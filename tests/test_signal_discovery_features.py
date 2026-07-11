from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import sin

from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_discovery.features import (
    build_causal_feature_rows,
    select_hard_negatives,
)


def test_causal_features_do_not_change_when_future_price_and_oi_are_extreme() -> None:
    decision_ts = _ts("2026-01-01T00:00:00Z")
    candles = _feature_candles(decision_ts=decision_ts)
    oi_rows = _oi_rows(decision_ts=decision_ts)
    decision_rows = [{"decision_ts": decision_ts, "label": "LONG"}]

    baseline = build_causal_feature_rows(
        candles=candles,
        decision_rows=decision_rows,
        walk_forward_start=_ts("2026-02-01T00:00:00Z"),
        oi_rows=oi_rows,
    )
    with_future_extremes = build_causal_feature_rows(
        candles=[
            *candles,
            _candle(
                decision_ts + timedelta(minutes=5),
                open_value="1000000",
                high="2000000",
                low="1",
                close="1500000",
                volume="999999999",
            ),
        ],
        decision_rows=decision_rows,
        walk_forward_start=_ts("2026-02-01T00:00:00Z"),
        oi_rows=[
            *oi_rows,
            {"timestamp": decision_ts + timedelta(minutes=5), "open_interest": "999999999"},
        ],
    )

    assert baseline == with_future_extremes
    row = baseline[0]
    assert row["source_candle_ts"] == decision_ts
    assert row["return_1h_pct"] is not None
    assert row["return_4h_pct"] is not None
    assert row["return_12h_pct"] is not None
    assert row["return_24h_pct"] is not None
    assert row["realized_volatility_4h_pct"] is not None
    assert row["realized_volatility_24h_pct"] is not None
    assert row["range_4h_pct"] is not None
    assert row["range_24h_pct"] is not None
    assert row["volume_zscore_7d"] is not None
    assert row["trend_slope_4h_pct_per_hour"] is not None
    assert row["trend_slope_24h_pct_per_hour"] is not None
    assert row["oi_change_1h_pct"] is not None
    assert row["oi_change_4h_pct"] is not None
    assert row["oi_change_12h_pct"] is not None


def test_causal_features_exclude_walk_forward_decisions() -> None:
    decision_ts = _ts("2026-01-01T00:00:00Z")
    walk_forward_start = _ts("2026-01-02T00:00:00Z")
    candles = _feature_candles(decision_ts=walk_forward_start)

    rows = build_causal_feature_rows(
        candles=candles,
        decision_rows=[
            {"decision_ts": decision_ts, "label": "LONG"},
            {"decision_ts": walk_forward_start, "label": "SHORT"},
        ],
        walk_forward_start=walk_forward_start,
    )

    assert [row["decision_ts"] for row in rows] == [decision_ts]


def test_hard_negatives_match_context_and_are_deterministic() -> None:
    positive_ts = _ts("2026-01-10T01:00:00Z")
    candidate_ts = _ts("2026-01-11T02:00:00Z")
    feature_rows = [
        _feature_row(positive_ts, label="LONG", volatility=1.0),
        _feature_row(candidate_ts, label="NEUTRAL", volatility=1.1),
        *[
            _feature_row(
                _ts(f"2026-02-{day:02d}T12:00:00Z"),
                label="NEUTRAL",
                volatility=float(day),
            )
            for day in range(2, 10)
        ],
    ]
    episodes = [
        {"episode_id": "episode-000001", "start_ts": positive_ts, "direction": "LONG"}
    ]

    first = select_hard_negatives(
        feature_rows=feature_rows,
        episodes=episodes,
        negatives_per_episode=1,
    )
    second = select_hard_negatives(
        feature_rows=list(reversed(feature_rows)),
        episodes=episodes,
        negatives_per_episode=1,
    )

    assert first == second
    assert len(first) == 1
    assert first[0]["decision_ts"] == candidate_ts
    assert first[0]["matched_episode_id"] == "episode-000001"
    assert first[0]["match_month"] == "2026-01"
    assert first[0]["utc_hour_block"] == 0
    assert first[0]["prior_volatility_quintile"] == 0


def _feature_candles(*, decision_ts: datetime) -> list[MarketDataCandle]:
    start = decision_ts - timedelta(days=8)
    candles: list[MarketDataCandle] = []
    count = int((decision_ts - start).total_seconds() // 300)
    for index in range(count + 1):
        timestamp = start + timedelta(minutes=5 * index)
        close = Decimal("100") + Decimal(index) * Decimal("0.001")
        wave = Decimal(str(sin(index / 17))) * Decimal("0.05")
        close += wave
        candles.append(
            _candle(
                timestamp,
                open_value=str(close - Decimal("0.01")),
                high=str(close + Decimal("0.08")),
                low=str(close - Decimal("0.08")),
                close=str(close),
                volume=str(Decimal("100") + Decimal(str(sin(index / 11))) * Decimal("5")),
            )
        )
    return candles


def _oi_rows(*, decision_ts: datetime) -> list[dict[str, object]]:
    start = decision_ts - timedelta(days=2)
    return [
        {
            "timestamp": start + timedelta(hours=index),
            "open_interest": str(Decimal("10000") + Decimal(index) * Decimal("10")),
        }
        for index in range(49)
    ]


def _feature_row(timestamp: datetime, *, label: str, volatility: float) -> dict[str, object]:
    return {
        "decision_ts": timestamp,
        "label": label,
        "realized_volatility_24h_pct": volatility,
    }


def _candle(
    timestamp: datetime,
    *,
    open_value: str,
    high: str,
    low: str,
    close: str,
    volume: str,
) -> MarketDataCandle:
    return MarketDataCandle(
        timestamp=timestamp,
        open=Decimal(open_value),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal(volume),
        vol_ccy=Decimal(volume),
        vol_ccy_quote=Decimal(volume) * Decimal(close),
        confirm=1,
    )


def _ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC)
