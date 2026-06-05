from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

SKILL_RELATIVE_ROOT = Path("artifacts") / "skills" / "agentic-quant-trading-development"


def materialize_stage0_workspace(
    *,
    workspace_root: Path,
    strategy_id: str,
    signal_set: dict[str, Any],
    signals: list[dict[str, Any]],
    candle_rows: list[dict[str, Any]],
    stage0_dir: Path | None = None,
) -> dict[str, str]:
    ensure_stage0_legacy_workspace_manifest(workspace_root)
    signal_engine_id = signal_set["signal_engine_id"]
    asset = signal_set["asset"]
    signal_set_id = signal_set["signal_set_id"]
    signal_set_dir = workspace_root / "dev" / "signals" / signal_engine_id / asset / signal_set_id
    candles_csv = workspace_root / "dev" / "data" / "raw" / asset / "5m" / "candles.csv"
    stage0_dir = stage0_dir or workspace_root / "dev" / "training_sessions" / strategy_id / "stage0" / signal_set_id
    packets_dir = stage0_dir / "scores" / "_scoreable_signal_subset" / "packets"

    signal_set_dir.mkdir(parents=True, exist_ok=True)
    packets_dir.mkdir(parents=True, exist_ok=True)
    candles_csv.parent.mkdir(parents=True, exist_ok=True)
    (stage0_dir / "scores").mkdir(parents=True, exist_ok=True)
    (stage0_dir / "summaries").mkdir(parents=True, exist_ok=True)

    (signal_set_dir / "manifest.json").write_text(json.dumps(signal_set["manifest"], indent=2))
    for stale_packet in packets_dir.glob("*.json"):
        stale_packet.unlink()
    for signal in signals:
        packet_filename = _packet_filename(signal)
        (packets_dir / packet_filename).write_text(json.dumps(signal["payload"], indent=2))

    _write_candles_csv(candles_csv, candle_rows)

    return {
        "signal_set_manifest": str(signal_set_dir / "manifest.json"),
        "signal_packets_dir": str(packets_dir),
        "candles_csv": str(candles_csv),
        "stage0_dir": str(stage0_dir),
    }


def ensure_stage0_legacy_workspace_manifest(workspace_root: Path) -> None:
    """Provide the workspace contract required by the canonical Stage 0 scripts."""
    manifest_path = workspace_root / "workspace_manifest.json"
    manifest = {
        "schema_version": "motis_workspace_manifest.v1",
        "name": "Motis Agentic Quant Terminal",
        "directories": {
            "dev": "dev",
            "live": "live",
            "artifacts": "artifacts",
        },
    }
    for directory in manifest["directories"].values():
        (workspace_root / directory).mkdir(parents=True, exist_ok=True)
    if not manifest_path.exists():
        manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")


def build_stage0_commands(
    *,
    workspace_root: Path,
    strategy_id: str,
    asset: str,
    signal_engine_id: str,
    signal_set_id: str,
    signal_packets_dir: str,
    candles_csv: str,
    forward_hours: int,
    vote_threshold: int,
    significance_threshold_pct: float,
    stage0_dir: Path | None = None,
) -> dict[str, list[str]]:
    stage0_dir = stage0_dir or workspace_root / "dev" / "training_sessions" / strategy_id / "stage0" / signal_set_id
    skill_root = workspace_root / SKILL_RELATIVE_ROOT
    return {
        "stage0a": [
            "python3",
            str(skill_root / "scripts" / "optimization" / "max_travel_distribution.py"),
            signal_packets_dir,
            "--candles",
            candles_csv,
            "--forward-hours",
            str(forward_hours),
            "--asset",
            asset,
            "--vote-threshold",
            str(vote_threshold),
            "--out",
            str(stage0_dir / "scores" / "travel_distribution.json"),
        ],
        "stage0b": [
            "python3",
            str(skill_root / "scripts" / "optimization" / "significance_threshold_calibration.py"),
            signal_packets_dir,
            "--candles",
            candles_csv,
            "--forward-hours",
            str(forward_hours),
            "--asset",
            asset,
            "--vote-threshold",
            str(vote_threshold),
            "--out",
            str(stage0_dir / "scores" / "threshold_calibration.json"),
        ],
        "stage0c": [
            "python3",
            str(skill_root / "scripts" / "optimization" / "signal_ground_truth.py"),
            signal_packets_dir,
            "--candles",
            candles_csv,
            "--forward-hours",
            str(forward_hours),
            "--significance-threshold",
            str(significance_threshold_pct),
            "--asset",
            asset,
            "--vote-threshold",
            str(vote_threshold),
            "--out",
            str(stage0_dir / "scores" / "ground_truth"),
        ],
    }


def _packet_filename(signal: dict[str, Any]) -> str:
    if ":" in signal["signal_id"]:
        return f"{signal['signal_id'].split(':')[-1]}.json"
    return f"{signal['signal_id']}.json"


def _write_candles_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    columns = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "ts": row["timestamp"],
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row.get("volume", 0),
                    "vol_ccy": row.get("vol_ccy", 0),
                    "vol_ccy_quote": row.get("vol_ccy_quote", 0),
                    "confirm": row.get("confirm", 1),
                }
            )


def read_parquet_candles_for_stage0(
    *,
    storage_uri: Path,
    window_start: str,
    window_end: str,
    forward_hours: int,
) -> list[dict[str, Any]]:
    start = _parse_datetime(window_start)
    end = _parse_datetime(window_end) + timedelta(hours=forward_hours)
    rows: list[dict[str, Any]] = []
    for file in sorted(storage_uri.glob("year=*/month=*/data.parquet")):
        for row in pq.read_table(file).to_pylist():
            timestamp = _parse_datetime(row["timestamp"])
            if start <= timestamp <= end:
                rows.append(_strip_partition_columns(row))
    return sorted(rows, key=lambda row: row["timestamp"])


def _strip_partition_columns(row: dict[str, Any]) -> dict[str, Any]:
    partition_columns = {"source", "type", "asset", "timeframe", "year", "month", "origin"}
    return {key: value for key, value in row.items() if key not in partition_columns}


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))
