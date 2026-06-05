from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from vegas.candle_store import rebuild_derived, write_candles
from vegas.live_provider import LiveMarketStateProvider
from vegas.schemas import Candle
from vegas.signal_engine import UniversalVegasSignalEngine


def make_5m_candles(start: datetime, count: int) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        price = Decimal("100") + Decimal(index)
        candles.append(
            Candle(
                ts=ts,
                open=price,
                high=price + Decimal("2"),
                low=price - Decimal("1"),
                close=price + Decimal("0.5"),
                volume=Decimal("10"),
                vol_ccy=Decimal("0.1"),
                vol_ccy_quote=Decimal("1000"),
                confirm=1,
            )
        )
    return candles


def test_live_provider_builds_replay_compatible_snapshot_for_any_asset(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 96)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    provider = LiveMarketStateProvider(
        asset=asset,
        timeframes=("2h",),
        context_bars=2,
        ema_warmup_bars=10,
        live_root=live_root,
    )
    snapshot = provider.snapshot_at(datetime(2026, 1, 1, 5, 15, tzinfo=UTC))
    chart = snapshot.charts["2h"]

    assert snapshot.asset == asset
    assert snapshot.mode == "live"
    assert chart.completed_context[-1].ts == datetime(2026, 1, 1, 2, 0, tzinfo=UTC)
    assert chart.active_candle.ts == datetime(2026, 1, 1, 4, 0, tzinfo=UTC)
    assert len(chart.active_5m_path) == 16
    assert chart.active_5m_path[0].ts == datetime(2026, 1, 1, 4, 0, tzinfo=UTC)
    assert chart.active_5m_path[-1].ts == datetime(2026, 1, 1, 5, 15, tzinfo=UTC)
    assert chart.active_candle.open == candles[48].open
    assert chart.active_candle.high == max(candle.high for candle in candles[48:64])
    assert chart.active_candle.low == min(candle.low for candle in candles[48:64])
    assert chart.active_candle.close == candles[63].close
    assert chart.chart_candles[-1] == chart.active_candle
    assert len(chart.ema_source_candles) == 3


def test_live_packet_serializes_like_replay_packet(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    provider = LiveMarketStateProvider(
        asset=asset,
        timeframes=("2h",),
        context_bars=3,
        ema_warmup_bars=676,
        live_root=live_root,
    )
    snapshot = provider.snapshot_at(provider.latest_timestamp())
    engine = UniversalVegasSignalEngine(proximity_threshold=Decimal("1"), vote_threshold=1)
    packet = engine.scan(snapshot)

    assert packet is not None
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
    assert "mode" not in serialized
    assert "proximity_threshold" not in serialized
    assert "vote_threshold" not in serialized
    assert "direction" not in serialized
    for chart in serialized["charts"].values():
        assert set(chart) == {"timeframe", "columns", "completed_candles", "latest_forming_candle"}
        assert all(isinstance(row, list) for row in chart["completed_candles"])
        assert isinstance(chart["latest_forming_candle"], list)
