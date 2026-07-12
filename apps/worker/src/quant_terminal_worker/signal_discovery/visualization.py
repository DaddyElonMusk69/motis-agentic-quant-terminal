from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime
from math import ceil
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow.parquet as pq

from quant_terminal_sdk.market_data_reader import MarketDataCandle

_EPISODE_COLUMNS = [
    "episode_id",
    "direction",
    "start_ts",
    "end_ts",
    "timestamp_count",
    "duration_minutes",
    "risk_pct",
    "entry_delay_minutes",
    "horizon_hours",
]


def build_atlas_visualization(
    *,
    artifact_root: str | Path,
    candles: Sequence[MarketDataCandle],
    risk_pct: float,
    window_start: datetime,
    window_end: datetime,
    max_candles: int = 4_000,
) -> dict[str, Any]:
    start = _as_utc(window_start)
    end = _as_utc(window_end)
    if risk_pct <= 0:
        raise ValueError("risk_pct must be positive")
    if start > end:
        raise ValueError("visualization window start must not follow end")
    if max_candles <= 0:
        raise ValueError("max_candles must be positive")
    episodes_path = Path(artifact_root) / "atlas" / "training_episodes.parquet"
    if not episodes_path.is_file():
        raise ValueError("training atlas episodes are unavailable")

    episode_rows = pq.read_table(
        episodes_path,
        columns=_EPISODE_COLUMNS,
        filters=[
            ("risk_pct", "=", float(risk_pct)),
            ("end_ts", ">=", start),
            ("start_ts", "<=", end),
        ],
    ).to_pylist()
    by_scenario: dict[tuple[int, float], list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(episode_rows, key=lambda value: value["start_ts"]):
        key = (int(row["entry_delay_minutes"]), float(row["horizon_hours"]))
        by_scenario[key].append(_episode_payload(row))

    source_candles = sorted(
        (
            candle
            for candle in candles
            if candle.confirm == 1 and start <= _as_utc(candle.timestamp) <= end
        ),
        key=lambda candle: candle.timestamp,
    )
    candle_rows, interval_minutes = _aggregate_candles(
        source_candles,
        max_candles=max_candles,
    )
    lanes = [
        {
            "entry_delay_minutes": entry_delay,
            "horizon_hours": horizon,
            "episodes": rows,
        }
        for (entry_delay, horizon), rows in sorted(
            by_scenario.items(),
            key=lambda item: (item[0][1], item[0][0]),
        )
    ]
    return {
        "risk_pct": float(risk_pct),
        "window_start": start,
        "window_end": end,
        "source_candle_count": len(source_candles),
        "candle_interval_minutes": interval_minutes,
        "downsampled": len(candle_rows) < len(source_candles),
        "candles": candle_rows,
        "lanes": lanes,
    }


def read_atlas_episode_detail(
    *,
    artifact_root: str | Path,
    risk_pct: float,
    episode_id: str,
) -> dict[str, Any]:
    atlas_root = Path(artifact_root) / "atlas"
    episodes_path = atlas_root / "training_episodes.parquet"
    labels_path = atlas_root / "training_timestamp_labels.parquet"
    if not episodes_path.is_file() or not labels_path.is_file():
        raise ValueError("training atlas detail artifacts are unavailable")
    episodes = pq.read_table(
        episodes_path,
        columns=_EPISODE_COLUMNS,
        filters=[
            ("risk_pct", "=", float(risk_pct)),
            ("episode_id", "=", episode_id),
        ],
    ).to_pylist()
    if len(episodes) != 1:
        raise ValueError("atlas episode was not found for the selected R")
    episode = episodes[0]
    labels = pq.read_table(
        labels_path,
        columns=["decision_ts", "entry_ts", "entry_price", "label", "long", "short"],
        filters=[
            ("risk_pct", "=", float(risk_pct)),
            ("scenario_entry_delay_minutes", "=", int(episode["entry_delay_minutes"])),
            ("scenario_horizon_hours", "=", float(episode["horizon_hours"])),
            ("decision_ts", "=", episode["start_ts"]),
        ],
    ).to_pylist()
    if len(labels) != 1:
        raise ValueError("atlas episode bucket-start label is unavailable")
    label = labels[0]
    direction = str(episode["direction"]).upper()
    path = label[direction.lower()]
    return {
        "episode": _episode_payload(episode),
        "snapshot": {
            "decision_ts": label["decision_ts"],
            "entry_ts": label.get("entry_ts"),
            "entry_price": label.get("entry_price"),
            "label": label.get("label"),
            "path": _path_payload(path),
        },
    }


def _aggregate_candles(
    candles: Sequence[MarketDataCandle],
    *,
    max_candles: int,
) -> tuple[list[dict[str, Any]], int]:
    if not candles:
        return [], 5
    if len(candles) > 1:
        source_interval = max(
            1,
            int(
                (_as_utc(candles[1].timestamp) - _as_utc(candles[0].timestamp))
                .total_seconds()
                // 60
            ),
        )
    else:
        source_interval = 5
    group_size = max(1, ceil(len(candles) / max_candles))
    rows = []
    for offset in range(0, len(candles), group_size):
        group = candles[offset : offset + group_size]
        rows.append(
            {
                "timestamp": _as_utc(group[0].timestamp),
                "open": float(group[0].open),
                "high": max(float(candle.high) for candle in group),
                "low": min(float(candle.low) for candle in group),
                "close": float(group[-1].close),
            }
        )
    return rows, source_interval * group_size


def _episode_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "episode_id": str(row["episode_id"]),
        "direction": str(row["direction"]).upper(),
        "start_ts": _as_utc(row["start_ts"]),
        "end_ts": _as_utc(row["end_ts"]),
        "timestamp_count": int(row["timestamp_count"]),
        "duration_minutes": int(row["duration_minutes"]),
        "risk_pct": float(row["risk_pct"]),
        "entry_delay_minutes": int(row["entry_delay_minutes"]),
        "horizon_hours": float(row["horizon_hours"]),
    }


def _path_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "direction": str(row["direction"]),
        "outcome": str(row["outcome"]),
        "target_price": float(row["target_price"]),
        "stop_price": float(row["stop_price"]),
        "first_touch_ts": (
            _as_utc(row["first_touch_ts"])
            if row.get("first_touch_ts") is not None
            else None
        ),
        "mfe_pct": float(row["mfe_pct"]),
        "mae_pct": float(row["mae_pct"]),
        "terminal_return_pct": float(row["terminal_return_pct"]),
    }


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
