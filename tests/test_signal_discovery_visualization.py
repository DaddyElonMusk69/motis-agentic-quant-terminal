from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_discovery.visualization import (
    build_atlas_visualization,
    read_atlas_episode_detail,
)


def test_visualization_returns_only_selected_r_and_overlapping_episodes(
    tmp_path: Path,
) -> None:
    artifact_root = _write_atlas(tmp_path)

    result = build_atlas_visualization(
        artifact_root=artifact_root,
        candles=_candles(count=12),
        risk_pct=1.0,
        window_start=_ts("2026-01-01T00:10:00Z"),
        window_end=_ts("2026-01-01T00:35:00Z"),
        max_candles=100,
    )

    assert result["risk_pct"] == 1.0
    assert len(result["lanes"]) == 1
    assert result["lanes"][0]["entry_delay_minutes"] == 5
    assert result["lanes"][0]["horizon_hours"] == 36.0
    assert [episode["episode_id"] for episode in result["lanes"][0]["episodes"]] == [
        "episode-000001",
        "episode-000002",
    ]
    assert [episode["direction"] for episode in result["lanes"][0]["episodes"]] == [
        "LONG",
        "SHORT",
    ]
    assert all(episode["risk_pct"] == 1.0 for episode in result["lanes"][0]["episodes"])


def test_visualization_downsamples_candles_with_ohlc_semantics(tmp_path: Path) -> None:
    artifact_root = _write_atlas(tmp_path)

    result = build_atlas_visualization(
        artifact_root=artifact_root,
        candles=_candles(count=6),
        risk_pct=1.0,
        window_start=_ts("2026-01-01T00:00:00Z"),
        window_end=_ts("2026-01-01T00:25:00Z"),
        max_candles=2,
    )

    assert result["downsampled"] is True
    assert result["source_candle_count"] == 6
    assert result["candle_interval_minutes"] == 15
    assert result["candles"] == [
        {
            "timestamp": _ts("2026-01-01T00:00:00Z"),
            "open": 100.0,
            "high": 103.0,
            "low": 99.0,
            "close": 102.5,
        },
        {
            "timestamp": _ts("2026-01-01T00:15:00Z"),
            "open": 103.0,
            "high": 106.0,
            "low": 102.0,
            "close": 105.5,
        },
    ]


def test_episode_detail_returns_bucket_start_directional_snapshot(tmp_path: Path) -> None:
    artifact_root = _write_atlas(tmp_path)

    detail = read_atlas_episode_detail(
        artifact_root=artifact_root,
        risk_pct=1.0,
        episode_id="episode-000001",
    )

    assert detail["episode"]["direction"] == "LONG"
    assert detail["snapshot"]["decision_ts"] == _ts("2026-01-01T00:00:00Z")
    assert detail["snapshot"]["entry_price"] == 100.0
    assert detail["snapshot"]["path"] == {
        "direction": "LONG",
        "outcome": "TP",
        "target_price": 102.0,
        "stop_price": 99.0,
        "first_touch_ts": _ts("2026-01-01T00:20:00Z"),
        "mfe_pct": 2.4,
        "mae_pct": 0.4,
        "terminal_return_pct": 2.1,
    }


def _write_atlas(tmp_path: Path) -> Path:
    artifact_root = tmp_path / "session"
    atlas_root = artifact_root / "atlas"
    atlas_root.mkdir(parents=True)
    episodes = [
        _episode("episode-000001", "LONG", "00:00", "00:20", 1.0),
        _episode("episode-000002", "SHORT", "00:30", "00:45", 1.0),
        _episode("episode-000003", "LONG", "00:15", "00:25", 1.5),
    ]
    labels = [
        {
            "decision_ts": _ts("2026-01-01T00:00:00Z"),
            "entry_ts": _ts("2026-01-01T00:05:00Z"),
            "entry_price": 100.0,
            "risk_pct": 1.0,
            "scenario_entry_delay_minutes": 5,
            "scenario_horizon_hours": 36.0,
            "label": "LONG",
            "long": {
                "direction": "LONG",
                "outcome": "TP",
                "target_price": 102.0,
                "stop_price": 99.0,
                "first_touch_ts": _ts("2026-01-01T00:20:00Z"),
                "mfe_pct": 2.4,
                "mae_pct": 0.4,
                "terminal_return_pct": 2.1,
            },
            "short": {
                "direction": "SHORT",
                "outcome": "SL",
                "target_price": 98.0,
                "stop_price": 101.0,
                "first_touch_ts": _ts("2026-01-01T00:10:00Z"),
                "mfe_pct": 0.4,
                "mae_pct": 2.4,
                "terminal_return_pct": -2.1,
            },
        }
    ]
    pq.write_table(pa.Table.from_pylist(episodes), atlas_root / "training_episodes.parquet")
    pq.write_table(
        pa.Table.from_pylist(labels),
        atlas_root / "training_timestamp_labels.parquet",
    )
    return artifact_root


def _episode(
    episode_id: str,
    direction: str,
    start: str,
    end: str,
    risk_pct: float,
) -> dict[str, object]:
    start_ts = _ts(f"2026-01-01T{start}:00Z")
    end_ts = _ts(f"2026-01-01T{end}:00Z")
    return {
        "episode_id": episode_id,
        "direction": direction,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "timestamp_count": int((end_ts - start_ts).total_seconds() // 300) + 1,
        "duration_minutes": int((end_ts - start_ts).total_seconds() // 60),
        "member_timestamps": [start_ts, end_ts],
        "risk_pct": risk_pct,
        "entry_delay_minutes": 5,
        "horizon_hours": 36.0,
    }


def _candles(*, count: int) -> list[MarketDataCandle]:
    start = _ts("2026-01-01T00:00:00Z")
    return [
        MarketDataCandle(
            timestamp=start + timedelta(minutes=index * 5),
            open=Decimal(100 + index),
            high=Decimal(101 + index),
            low=Decimal(99 + index),
            close=Decimal("100.5") + Decimal(index),
            volume=Decimal("100"),
            vol_ccy=Decimal("100"),
            vol_ccy_quote=Decimal("10000"),
            confirm=1,
        )
        for index in range(count)
    ]


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
