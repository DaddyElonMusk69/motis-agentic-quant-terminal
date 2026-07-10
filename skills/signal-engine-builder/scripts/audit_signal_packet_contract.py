#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


FORBIDDEN_FIELDS = {
    "action",
    "confidence",
    "direction",
    "entry",
    "entry_price",
    "leverage",
    "margin",
    "notional_usd",
    "order_type",
    "position_size",
    "score",
    "side",
    "size",
    "sl",
    "sl_pct",
    "stop_loss",
    "take_profit",
    "tp",
    "tp_pct",
    "trade_action",
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit a Motis signal_packet.v2 JSON file for downstream consumer readiness.")
    parser.add_argument("--packet", required=True, help="Path to a signal packet JSON file.")
    parser.add_argument("--max-size-kb", type=float, default=64.0, help="Warn when packet JSON exceeds this size.")
    args = parser.parse_args()

    packet_path = Path(args.packet)
    if not packet_path.is_file():
        print(json.dumps({"packet": str(packet_path), "status": "fail", "errors": ["packet file not found"], "warnings": []}, indent=2))
        return 1
    try:
        packet = json.loads(packet_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"packet": str(packet_path), "status": "fail", "errors": [f"cannot read packet JSON: {exc}"], "warnings": []}, indent=2))
        return 1
    errors: list[str] = []
    warnings: list[str] = []

    _require(packet.get("schema_version") == "signal_packet.v2", "schema_version must be signal_packet.v2", errors)
    _require(bool(packet.get("asset")), "asset is required", errors)
    _require(bool(packet.get("timestamp")), "timestamp is required", errors)
    packet_ts = _parse_ts(packet.get("timestamp"), "timestamp", errors)

    _scan_forbidden(packet, path="", errors=errors)

    evidence = packet.get("evidence") if isinstance(packet.get("evidence"), dict) else {}
    if not evidence:
        errors.append("evidence object is required")
    _number_field(evidence, "reference_price", errors)
    _number_field(evidence, "trigger_candle_close", errors)
    signal_available_at = _parse_ts(evidence.get("signal_available_at"), "evidence.signal_available_at", errors)
    _parse_ts(evidence.get("signal_candle_open_ts"), "evidence.signal_candle_open_ts", warnings)
    _parse_ts(evidence.get("signal_candle_close_ts"), "evidence.signal_candle_close_ts", warnings)

    charts = packet.get("charts") if isinstance(packet.get("charts"), dict) else {}
    _audit_charts(charts=charts, signal_available_at=signal_available_at, packet_ts=packet_ts, errors=errors, warnings=warnings)

    size_kb = packet_path.stat().st_size / 1024
    if size_kb > args.max_size_kb:
        warnings.append(f"packet size {size_kb:.1f}KB exceeds {args.max_size_kb:.1f}KB")

    result = {
        "packet": str(packet_path),
        "status": "fail" if errors else "pass",
        "size_kb": round(size_kb, 3),
        "errors": errors,
        "warnings": warnings,
    }
    print(json.dumps(result, indent=2))
    return 1 if errors else 0


def _require(condition: bool, message: str, errors: list[str]) -> None:
    if not condition:
        errors.append(message)


def _number_field(evidence: dict[str, Any], key: str, errors: list[str]) -> None:
    value = evidence.get(key)
    if value is None:
        errors.append(f"evidence.{key} is required")
        return
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        errors.append(f"evidence.{key} must be numeric")
        return
    if numeric <= 0:
        errors.append(f"evidence.{key} must be positive")


def _scan_forbidden(value: Any, *, path: str, errors: list[str]) -> None:
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}" if path else str(key)
            if key in FORBIDDEN_FIELDS:
                errors.append(f"forbidden execution field present: {child_path}")
            _scan_forbidden(child, path=child_path, errors=errors)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _scan_forbidden(child, path=f"{path}[{index}]", errors=errors)


def _audit_charts(
    *,
    charts: dict[str, Any],
    signal_available_at: datetime | None,
    packet_ts: datetime | None,
    errors: list[str],
    warnings: list[str],
) -> None:
    for name, chart in charts.items():
        if not isinstance(chart, dict):
            warnings.append(f"charts.{name} is not an object")
            continue
        columns = chart.get("columns")
        if "candles" in chart or "rows" in chart:
            if not isinstance(columns, list) or not columns:
                errors.append(f"charts.{name}.columns is required for row arrays")
        rows = chart.get("candles") if "candles" in chart else chart.get("rows")
        if isinstance(rows, list) and rows and isinstance(rows[0], list) and isinstance(columns, list):
            if len(rows[0]) != len(columns):
                errors.append(f"charts.{name} first row length does not match columns length")
        if isinstance(rows, list) and name.lower() in {"2h", "4h", "8h", "12h", "1d"}:
            _audit_htf_rows(name=name, rows=rows, columns=columns, signal_available_at=signal_available_at, packet_ts=packet_ts, errors=errors)


def _audit_htf_rows(
    *,
    name: str,
    rows: list[Any],
    columns: Any,
    signal_available_at: datetime | None,
    packet_ts: datetime | None,
    errors: list[str],
) -> None:
    if not isinstance(columns, list):
        return
    aliases = {
        "open": ("open_ts", "open_time", "timestamp", "ts"),
        "close": ("close_ts", "close_time"),
        "partial_close": ("partial_close_ts", "partial_close_time"),
        "complete": ("complete", "is_completed"),
    }
    idx = {role: _first_index(columns, names) for role, names in aliases.items()}
    if idx["close"] is None or idx["complete"] is None:
        return
    for row_index, row in enumerate(rows[:5]):
        if not isinstance(row, list):
            continue
        close_ts = _parse_ts(row[idx["close"]], f"charts.{name}.candles[{row_index}].close", errors)
        complete = bool(row[idx["complete"]])
        if complete and close_ts and signal_available_at and close_ts > signal_available_at:
            errors.append(f"charts.{name}.candles[{row_index}] completed close is after signal_available_at")
        if not complete and packet_ts is not None and idx["open"] is not None:
            open_ts = _parse_ts(row[idx["open"]], f"charts.{name}.candles[{row_index}].open", errors)
            if open_ts and not (open_ts <= packet_ts):
                errors.append(f"charts.{name}.candles[{row_index}] forming open is after packet timestamp")


def _first_index(columns: list[Any], names: tuple[str, ...]) -> int | None:
    for name in names:
        if name in columns:
            return columns.index(name)
    return None


def _parse_ts(value: Any, label: str, issues: list[str]) -> datetime | None:
    if value in (None, ""):
        issues.append(f"{label} is missing")
        return None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        issues.append(f"{label} is not a parseable timestamp")
        return None


if __name__ == "__main__":
    raise SystemExit(main())
