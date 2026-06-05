from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_TP_LEVELS = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0]
DEFAULT_FORWARD_HOURS = 36


def run_stage2_capture_curve(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    tp_levels: list[float] | None = None,
    forward_hours: int = DEFAULT_FORWARD_HOURS,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    canonical_scores_path = promotion_root / "stage1a_canonical_full_cycle_scores.json"
    canonical_scores = _read_json(canonical_scores_path)
    match_set = [
        item
        for item in canonical_scores.get("match_set", [])
        if isinstance(item, dict) and item.get("signal_id")
    ]
    if not match_set:
        raise ValueError("Stage 2 requires a non-empty canonical Stage 1A MATCH set.")

    levels = tp_levels or DEFAULT_TP_LEVELS
    signals_by_id = _index_signals(signal_rows)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])

    per_signal = []
    for match in match_set:
        signal = _find_signal(signals_by_id, str(match["signal_id"]))
        if signal is None:
            raise ValueError(f"Canonical Stage 1A MATCH signal not found in signal rows: {match['signal_id']}")
        packet = _packet_from_signal(signal)
        direction = _match_direction(match)
        capture = _walk_signal_capture(
            signal_id=str(match["signal_id"]),
            sample_role=str(match.get("sample_role") or "full_cycle"),
            direction=direction,
            packet=packet,
            signal_timestamp=_coerce_datetime(packet.get("timestamp") or signal["timestamp"]),
            candles=candle_rows,
            tp_levels=levels,
            forward_hours=forward_hours,
        )
        per_signal.append(capture)

    result = _build_result(
        session=session,
        canonical_scores_path=canonical_scores_path,
        per_signal=per_signal,
        tp_levels=levels,
        forward_hours=forward_hours,
    )
    promotion_root.mkdir(parents=True, exist_ok=True)
    capture_path = promotion_root / "stage2_capture_curve.json"
    per_signal_path = promotion_root / "stage2_capture_per_signal.json"
    summary_path = promotion_root / "stage2_summary.md"
    capture_path.write_text(json.dumps(result, indent=2) + "\n")
    per_signal_path.write_text(json.dumps(per_signal, indent=2) + "\n")
    summary_path.write_text(_render_summary(result))
    return {
        **result,
        "capture_curve_path": str(capture_path),
        "per_signal_path": str(per_signal_path),
        "summary_path": str(summary_path),
    }


def get_reference_price(packet: dict[str, Any]) -> float:
    interactions = packet.get("interactions", {})
    if isinstance(interactions, list):
        for timeframe in packet.get("active_timeframes", []):
            for entry in interactions:
                if entry.get("timeframe") == timeframe and entry.get("market_price") is not None:
                    return float(entry["market_price"])
        for entry in interactions:
            if entry.get("market_price") is not None:
                return float(entry["market_price"])
    elif isinstance(interactions, dict):
        for timeframe in packet.get("active_timeframes", []):
            entries = interactions.get(timeframe, [])
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])
        for entries in interactions.values():
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])

    for timeframe in packet.get("active_timeframes", []):
        chart = packet.get("charts", {}).get(timeframe, {})
        forming = chart.get("latest_forming_candle")
        if forming:
            if isinstance(forming, dict) and forming.get("close") is not None:
                return float(forming["close"])
            columns = chart.get("columns", [])
            if isinstance(forming, list) and "close" in columns:
                return float(forming[columns.index("close")])
    for chart in packet.get("charts", {}).values():
        forming = chart.get("latest_forming_candle")
        if isinstance(forming, dict) and forming.get("close") is not None:
            return float(forming["close"])
        columns = chart.get("columns", [])
        if isinstance(forming, list) and "close" in columns:
            return float(forming[columns.index("close")])
    raise ValueError("Signal packet has no reference price.")


def _walk_signal_capture(
    *,
    signal_id: str,
    sample_role: str,
    direction: str,
    packet: dict[str, Any],
    signal_timestamp: datetime,
    candles: list[dict[str, Any]],
    tp_levels: list[float],
    forward_hours: int,
) -> dict[str, Any]:
    reference_price = get_reference_price(packet)
    cutoff = signal_timestamp + timedelta(hours=forward_hours)
    reached = {level: False for level in tp_levels}
    first_tp_reached: float | None = None

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_timestamp:
            continue
        if timestamp > cutoff:
            break
        for level in tp_levels:
            if reached[level]:
                continue
            target = reference_price * (1 + level / 100) if direction == "LONG" else reference_price * (1 - level / 100)
            hit = candle["high"] >= target if direction == "LONG" else candle["low"] <= target
            if hit:
                reached[level] = True
                if first_tp_reached is None:
                    first_tp_reached = level

    return {
        "signal_id": signal_id,
        "sample_role": sample_role,
        "direction": direction,
        "signal_ts": signal_timestamp.isoformat().replace("+00:00", "Z"),
        "reference_price": reference_price,
        "first_tp_reached": first_tp_reached,
        "tp_reached": {f"{level:.1f}": reached[level] for level in tp_levels},
    }


def _build_result(
    *,
    session: dict[str, Any],
    canonical_scores_path: Path,
    per_signal: list[dict[str, Any]],
    tp_levels: list[float],
    forward_hours: int,
) -> dict[str, Any]:
    results: dict[str, dict[str, dict[str, float | int]]] = {}
    roles = sorted({str(item["sample_role"]) for item in per_signal})
    for level in tp_levels:
        level_key = f"{level:.1f}"
        results[level_key] = {}
        for role in [*roles, "full_cycle"]:
            cohort = per_signal if role == "full_cycle" else [item for item in per_signal if item["sample_role"] == role]
            reached = sum(1 for item in cohort if item["tp_reached"][level_key])
            total = len(cohort)
            results[level_key][role] = {
                "reached": reached,
                "total": total,
                "rate": round(reached / total * 100, 1) if total else 0.0,
            }

    return {
        "schema_version": "0.1",
        "stage": "stage2_travel_capture_curve",
        "artifact_role": "stage2_capture_curve",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "canonical_stage1_scores_path": str(canonical_scores_path),
        "forward_hours": forward_hours,
        "tp_levels": tp_levels,
        "metrics": {
            "total_match_signals": len(per_signal),
            "slice_counts": {role: sum(1 for item in per_signal if item["sample_role"] == role) for role in roles},
        },
        "results": results,
        "per_signal": per_signal,
        "stage3_input": {
            "role": "tp_range_evidence",
            "description": "Use this capture curve to narrow Stage 3 TP/SL/management grids on the same frozen Stage 1A MATCH set.",
        },
    }


def _render_summary(result: dict[str, Any]) -> str:
    lines = [
        "# Stage 2 Travel Capture",
        "",
        f"Session: `{result['session_id']}`",
        f"Forward hours: {result['forward_hours']}",
        f"MATCH signals: {result['metrics']['total_match_signals']}",
        "",
        "| TP | Training | Walk-forward | Full cycle |",
        "| --- | ---: | ---: | ---: |",
    ]
    for level in result["tp_levels"]:
        key = f"{level:.1f}"
        rows = result["results"][key]
        lines.append(
            f"| {key}% | {_rate(rows, 'training')} | {_rate(rows, 'walk_forward_test')} | {_rate(rows, 'full_cycle')} |"
        )
    lines.append("")
    return "\n".join(lines)


def _rate(rows: dict[str, dict[str, Any]], role: str) -> str:
    row = rows.get(role, {"rate": 0.0, "reached": 0, "total": 0})
    return f"{row['rate']:.1f}% ({row['reached']}/{row['total']})"


def _session_artifact_root(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"Required Stage 1 canonical score artifact not found: {path}")
    return json.loads(path.read_text())


def _index_signals(signal_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for signal in signal_rows:
        signal_id = str(signal["signal_id"])
        indexed[signal_id] = signal
        indexed.setdefault(signal_id.split(":")[-1], signal)
    return indexed


def _find_signal(signals_by_id: dict[str, dict[str, Any]], signal_id: str) -> dict[str, Any] | None:
    return signals_by_id.get(signal_id) or signals_by_id.get(signal_id.split(":")[-1])


def _packet_from_signal(signal: dict[str, Any]) -> dict[str, Any]:
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    return {
        **payload,
        "signal_id": signal["signal_id"],
        "timestamp": payload.get("timestamp") or signal["timestamp"],
    }


def _match_direction(match: dict[str, Any]) -> str:
    direction = match.get("decision_direction") or match.get("ground_truth_direction")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Stage 2 requires LONG/SHORT MATCH directions, got {direction!r}.")
    return str(direction)


def _coerce_candle(candle: Any) -> dict[str, Any]:
    if isinstance(candle, dict):
        return {
            "timestamp": _coerce_datetime(candle.get("timestamp") or candle.get("ts")),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
        }
    return {
        "timestamp": _coerce_datetime(candle.timestamp),
        "high": float(candle.high if not isinstance(candle.high, Decimal) else str(candle.high)),
        "low": float(candle.low if not isinstance(candle.low, Decimal) else str(candle.low)),
    }


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("missing timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
