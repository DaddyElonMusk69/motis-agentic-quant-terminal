from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from vegas.bollinger_signal_engine import UniversalBollingerSignalEngine
from vegas.indicators import latest_bollinger_bands
from vegas.schemas import Candle, ChartSnapshot, MarketStateSnapshot


def candle(ts: datetime, close: str) -> Candle:
    price = Decimal(close)
    return Candle(
        ts=ts,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1"),
        vol_ccy=Decimal("1"),
        vol_ccy_quote=Decimal("1"),
        confirm=1,
    )


def chart(timeframe: str, closes: list[str], active_close: str | None = None) -> ChartSnapshot:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    completed = tuple(
        candle(start + timedelta(hours=index), close)
        for index, close in enumerate(closes)
    )
    active = candle(start + timedelta(hours=len(closes)), active_close or closes[-1])
    return ChartSnapshot(
        timeframe=timeframe,
        completed_context=completed[-5:],
        active_candle=active,
        chart_candles=(*completed[-5:], active),
        ema_source_candles=(*completed, active),
    )


def snapshot(charts: dict[str, ChartSnapshot]) -> MarketStateSnapshot:
    latest = next(iter(charts.values())).active_candle
    return MarketStateSnapshot(
        asset="TEST",
        timestamp=latest.ts,
        mode="replay",
        latest_5m_candle=latest,
        charts=charts,
    )


def test_latest_bollinger_bands_uses_sma_and_population_stddev() -> None:
    bands = latest_bollinger_bands(
        [Decimal("1"), Decimal("2"), Decimal("3"), Decimal("4"), Decimal("5")],
        period=5,
        stddev_multiplier=Decimal("2"),
    )

    assert bands.middle == Decimal("3")
    assert bands.upper == pytest.approx(Decimal("5.828427124746190097603377448"))
    assert bands.lower == pytest.approx(Decimal("0.1715728752538099023966225516"))


def test_bollinger_engine_emits_neutral_packet_when_vote_threshold_is_met() -> None:
    base_closes = [str(value) for value in range(81, 101)]
    charts = {
        timeframe: chart(timeframe, base_closes, "105.7662812973353981455581106")
        for timeframe in ("4h", "8h", "12h")
    }
    engine = UniversalBollingerSignalEngine(
        bb_period=20,
        bb_stddev=Decimal("2"),
        proximity_threshold=Decimal("1"),
        vote_threshold=3,
    )

    packet = engine.scan(snapshot(charts))

    assert packet is not None
    assert packet.asset == "TEST"
    assert set(packet.active_timeframes) == {"4h", "8h", "12h"}
    serialized = packet.to_dict()
    assert set(serialized) == {
        "schema_version",
        "asset",
        "timestamp",
        "active_timeframes",
        "interactions",
        "charts",
    }
    assert serialized["schema_version"] == "signal_packet.v2"
    interaction = next(
        interaction
        for interaction in serialized["interactions"]
        if interaction["timeframe"] == "4h"
    )
    assert set(interaction) == {
        "timeframe",
        "band",
        "band_upper_limit",
        "band_middle",
        "band_lower_limit",
        "market_price",
        "distance_pct",
        "band_width_pct",
        "percent_b",
    }
    assert interaction["band"] == "upper"
    assert "direction" not in serialized
    assert "proximity_threshold" not in serialized
    assert "bb_period" not in serialized


def test_bollinger_engine_returns_none_when_vote_threshold_is_not_met() -> None:
    charts = {
        "4h": chart("4h", [str(value) for value in range(81, 101)], "105.7662812973353981455581106"),
        "8h": chart("8h", [str(value) for value in range(81, 101)], "100"),
    }
    engine = UniversalBollingerSignalEngine(
        bb_period=20,
        proximity_threshold=Decimal("0.001"),
        vote_threshold=2,
    )

    assert engine.scan(snapshot(charts)) is None


def test_bollinger_engine_counts_one_vote_per_timeframe_with_multiple_interactions() -> None:
    flat_closes = ["100"] * 20
    charts = {
        "4h": chart("4h", flat_closes, "100"),
    }
    engine = UniversalBollingerSignalEngine(
        bb_period=20,
        proximity_threshold=Decimal("0.001"),
        vote_threshold=2,
        watched_bands=("upper", "lower"),
    )

    packet = engine.scan(snapshot(charts))

    assert packet is None


def test_bollinger_engine_requires_enough_source_candles() -> None:
    charts = {"4h": chart("4h", ["100"] * 18, "100")}
    engine = UniversalBollingerSignalEngine(
        bb_period=20,
        proximity_threshold=Decimal("1"),
        vote_threshold=1,
    )

    assert engine.scan(snapshot(charts)) is None
