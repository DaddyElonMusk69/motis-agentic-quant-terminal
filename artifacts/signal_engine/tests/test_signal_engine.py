from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from vegas.indicators import ema_series
from vegas.replay_provider import ReplayMarketStateProvider
from vegas.signal_engine import UniversalVegasSignalEngine


def test_ema_series_uses_standard_exponential_formula() -> None:
    values = [Decimal("10"), Decimal("12"), Decimal("14")]

    assert ema_series(values, period=3) == [
        Decimal("10"),
        Decimal("11.0"),
        Decimal("12.50"),
    ]


def test_signal_engine_emits_neutral_packet_when_configured_votes_are_met() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        context_bars=80,
        training_root=Path("dev/data"),
    )
    snapshot = provider.snapshot_at(datetime(2024, 6, 1, 12, 35, tzinfo=UTC))
    engine = UniversalVegasSignalEngine(proximity_threshold=Decimal("1"), vote_threshold=3)

    packet = engine.scan(snapshot)

    assert packet is not None
    assert packet.asset == "BTC"
    assert packet.timestamp == snapshot.timestamp
    assert packet.mode == "replay"
    assert packet.proximity_threshold == Decimal("1")
    assert packet.vote_threshold == 3
    assert packet.total_votes == 5
    assert set(packet.voting_timeframes) == {"2h", "4h", "8h", "12h", "1d"}
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
    assert serialized["active_timeframes"] == ["2h", "4h", "8h", "12h", "1d"]
    assert "mode" not in serialized
    assert "proximity_threshold" not in serialized
    assert "vote_threshold" not in serialized
    assert "total_votes" not in serialized
    for chart in packet.charts.values():
        assert set(chart.ema_values or {}) == {36, 43, 144, 169, 576, 676}
        assert set(chart.ema_distances or {}) == {36, 43, 144, 169, 576, 676}
        assert chart.active_5m_path
    for chart in serialized["charts"].values():
        assert set(chart) == {
            "timeframe",
            "columns",
            "completed_candles",
            "latest_forming_candle",
        }
        assert chart["columns"] == [
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vol_ccy",
            "vol_ccy_quote",
            "confirm",
        ]
        assert all(isinstance(row, list) for row in chart["completed_candles"])
        assert isinstance(chart["latest_forming_candle"], list)
        assert "completed_context" not in chart
        assert "active_candle" not in chart
        assert "chart_candles" not in chart
        assert "active_5m_path" not in chart
        assert "ema_values" not in chart
        assert "ema_distances" not in chart
        assert "ema_warmup_counts" not in chart
        assert "ema_validity" not in chart
    assert "direction" not in serialized
    assert "decision" not in serialized


def test_signal_engine_returns_none_when_configured_vote_threshold_is_not_met() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        context_bars=80,
        training_root=Path("dev/data"),
    )
    snapshot = provider.snapshot_at(datetime(2024, 6, 1, 12, 35, tzinfo=UTC))
    engine = UniversalVegasSignalEngine(proximity_threshold=Decimal("1"), vote_threshold=6)

    assert engine.scan(snapshot) is None


def test_signal_engine_marks_ema_validity_from_warmup_history() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        context_bars=80,
        ema_warmup_bars=676,
        training_root=Path("dev/data"),
    )
    early_snapshot = provider.snapshot_at(datetime(2024, 6, 1, 12, 35, tzinfo=UTC))
    late_snapshot = provider.snapshot_at(datetime(2025, 4, 1, 12, 35, tzinfo=UTC))
    engine = UniversalVegasSignalEngine(proximity_threshold=Decimal("1"), vote_threshold=1)

    early_packet = engine.scan(early_snapshot)
    late_packet = engine.scan(late_snapshot)

    assert early_packet is not None
    assert late_packet is not None
    assert early_packet.charts["1d"].ema_warmup_counts[676] < 676
    assert early_packet.charts["1d"].ema_validity[676] is False
    assert late_packet.charts["1d"].ema_warmup_counts[676] >= 676
    assert late_packet.charts["1d"].ema_validity[676] is True
    assert all(
        interaction.tunnel != "slow"
        for interaction in early_packet.interactions["1d"]
    )


def test_signal_packet_interactions_serialize_tunnel_ranges() -> None:
    provider = ReplayMarketStateProvider(
        asset="BTC",
        context_bars=80,
        ema_warmup_bars=676,
        training_root=Path("dev/data"),
    )
    snapshot = provider.snapshot_at(datetime(2024, 6, 2, 12, 15, tzinfo=UTC))
    engine = UniversalVegasSignalEngine(proximity_threshold=Decimal("0.002"), vote_threshold=3)

    packet = engine.scan(snapshot)

    assert packet is not None
    interaction = next(
        interaction
        for interaction in packet.to_dict()["interactions"]
        if interaction["timeframe"] == "2h"
    )
    assert set(interaction) == {
        "timeframe",
        "tunnel",
        "tunnel_upper_limit",
        "tunnel_lower_limit",
        "market_price",
        "distance_pct",
    }
    assert interaction["tunnel"] == "fast"
    assert Decimal(interaction["tunnel_upper_limit"]) >= Decimal(
        interaction["tunnel_lower_limit"]
    )
    assert "ema_period" not in interaction
    assert "ema_value" not in interaction
    assert "active_price" not in interaction
