from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import sys

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dev.experiments.stage0.btc_compression_participation_research import (
    build_causal_features,
    forward_excursions,
    greedy_dedupe_indices,
    matched_random_indices,
)


def _frame(rows: list[dict[str, float]]) -> pd.DataFrame:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    return pd.DataFrame(
        [
            {
                "timestamp": start + timedelta(minutes=5 * index),
                "open": row["close"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "vol_ccy_quote": row.get("volume", 100.0),
                "sum_open_interest": row.get("oi", 1000.0),
                "sum_open_interest_value": row.get("oi_value", 100000.0),
                "count_toptrader_long_short_ratio": 1.0,
                "count_long_short_ratio": 1.0,
                "sum_taker_long_short_vol_ratio": 1.0,
            }
            for index, row in enumerate(rows)
        ]
    )


def test_causal_prior_range_excludes_trigger_candle() -> None:
    frame = _frame(
        [
            {"high": 101.0, "low": 99.0, "close": 100.0},
            {"high": 102.0, "low": 100.0, "close": 101.0},
            {"high": 103.0, "low": 101.0, "close": 102.0},
            {"high": 120.0, "low": 102.0, "close": 119.0},
        ]
    )

    features = build_causal_features(frame, compression_bars=3, stats_bars=3, min_stats_bars=2)

    assert features.loc[3, "prior_range_high"] == 103.0
    assert features.loc[3, "prior_range_low"] == 99.0
    assert bool(features.loc[3, "range_breakout"])


def test_forward_excursion_starts_after_event_candle() -> None:
    frame = _frame(
        [
            {"high": 130.0, "low": 70.0, "close": 100.0},
            {"high": 102.0, "low": 99.0, "close": 101.0},
            {"high": 103.0, "low": 98.0, "close": 100.0},
        ]
    )

    result = forward_excursions(frame, horizon_bars=2)

    assert result.loc[0, "up_mfe_pct"] == 3.0
    assert result.loc[0, "down_mfe_pct"] == 2.0
    assert result.loc[0, "max_abs_mfe_pct"] == 3.0


def test_greedy_dedupe_keeps_chronological_events() -> None:
    timestamps = pd.Series(pd.date_range("2025-01-01", periods=6, freq="1h", tz="UTC"))

    selected = greedy_dedupe_indices([1, 2, 4, 5], timestamps=timestamps, min_gap=timedelta(hours=3))

    assert selected == [1, 4]


def test_matched_random_indices_preserve_month_volatility_and_regime() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.date_range("2025-01-01", periods=12, freq="1D", tz="UTC"),
            "month": ["2025-01"] * 12,
            "volatility_bin": [0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2],
            "trend_regime": ["down", "down", "up", "up"] * 3,
        }
    )
    event_indices = [1, 6, 10]
    eligible = pd.Series(True, index=frame.index)

    samples = matched_random_indices(
        frame,
        event_indices=event_indices,
        eligible_mask=eligible,
        replicates=3,
        seed="unit-test",
        exclusion_bars=0,
    )

    assert samples == matched_random_indices(
        frame,
        event_indices=event_indices,
        eligible_mask=eligible,
        replicates=3,
        seed="unit-test",
        exclusion_bars=0,
    )
    for replicate in samples:
        assert len(replicate) == len(event_indices)
        for event_index, random_index in zip(event_indices, replicate):
            assert frame.loc[random_index, "month"] == frame.loc[event_index, "month"]
            assert frame.loc[random_index, "volatility_bin"] == frame.loc[event_index, "volatility_bin"]
            assert frame.loc[random_index, "trend_regime"] == frame.loc[event_index, "trend_regime"]


def test_matched_random_indices_fall_back_to_same_quarter_and_report_usage() -> None:
    frame = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2025-01-15T00:00:00Z", "2025-03-15T00:00:00Z"]),
            "month": ["2025-01", "2025-03"],
            "volatility_bin": [2, 2],
            "trend_regime": ["flat", "flat"],
        }
    )
    diagnostics: dict[str, int] = {}

    samples = matched_random_indices(
        frame,
        event_indices=[0],
        eligible_mask=pd.Series(True, index=frame.index),
        replicates=2,
        seed="quarter-fallback",
        exclusion_bars=0,
        diagnostics=diagnostics,
    )

    assert samples == [[1], [1]]
    assert diagnostics == {
        "same_month_event_count": 0,
        "same_quarter_fallback_event_count": 1,
    }
