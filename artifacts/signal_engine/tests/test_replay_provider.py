from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from vegas.replay_provider import ReplayMarketStateProvider


def test_replay_provider_builds_hybrid_snapshot_from_completed_context_and_active_5m() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        timeframes=("2h",),
        context_bars=3,
        training_root=Path("dev/data"),
    )

    snapshot = provider.snapshot_at(datetime(2023, 5, 11, 5, 15, tzinfo=UTC))
    chart = snapshot.charts["2h"]

    assert snapshot.asset == "BTC"
    assert snapshot.mode == "replay"
    assert snapshot.timestamp == datetime(2023, 5, 11, 5, 15, tzinfo=UTC)
    assert [candle.ts for candle in chart.completed_context] == [
        datetime(2023, 5, 10, 22, 0, tzinfo=UTC),
        datetime(2023, 5, 11, 0, 0, tzinfo=UTC),
        datetime(2023, 5, 11, 2, 0, tzinfo=UTC),
    ]
    assert chart.active_candle.ts == datetime(2023, 5, 11, 4, 0, tzinfo=UTC)
    assert chart.active_candle.open == Decimal("27448.5")
    assert chart.active_candle.high == Decimal("27498")
    assert chart.active_candle.low == Decimal("27391")
    assert chart.active_candle.close == Decimal("27466.7")
    assert chart.active_candle.volume == Decimal("341054")
    assert chart.active_candle.vol_ccy == Decimal("3410.54")
    assert chart.active_candle.vol_ccy_quote == Decimal("93643091.273")
    assert chart.active_candle.confirm == 0
    assert len(chart.active_5m_path) == 16
    assert chart.active_5m_path[0].ts == datetime(2023, 5, 11, 4, 0, tzinfo=UTC)
    assert chart.active_5m_path[-1].ts == datetime(2023, 5, 11, 5, 15, tzinfo=UTC)
    assert len(chart.chart_candles) == 4
    assert chart.chart_candles[-1] == chart.active_candle


def test_replay_provider_rejects_timestamp_without_enough_completed_context() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        timeframes=("2h",),
        context_bars=10,
        training_root=Path("dev/data"),
    )

    try:
        provider.snapshot_at(datetime(2023, 5, 10, 16, 0, tzinfo=UTC))
    except ValueError as error:
        assert "Not enough completed 2h candles" in str(error)
    else:
        raise AssertionError("Expected insufficient context to raise ValueError")
