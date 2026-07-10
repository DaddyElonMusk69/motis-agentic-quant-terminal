from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from math import sin
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.oi_compression_v1 import (
    CANDLE_COLUMNS,
    HTF_COLUMNS,
    OI_COLUMNS,
    _aligned_rows,
    _build_feature_cache,
    _event_features,
    _with_defaults,
    generate_oi_compression_packets,
    scan_oi_compression_latest,
)
from quant_terminal_worker.signal_engines.oi_trap_reversal_v1 import (
    scan_oi_trap_reversal_latest,
)
from quant_terminal_worker.signal_engines.oi_trap_reversal_eth_v1 import (
    scan_oi_trap_reversal_eth_latest,
)
from quant_terminal_worker.signal_engines.oi_flush_exhaustion_v1 import (
    scan_oi_flush_exhaustion_latest,
)


def test_oi_compression_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("oi_compression_v1")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/oi_compression_v1_base.py")


def test_oi_trap_reversal_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("oi_trap_reversal_v1")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/oi_trap_reversal_v1_base.py")


def test_oi_trap_reversal_eth_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("oi_trap_reversal_eth_v1")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/oi_trap_reversal_eth_v1_base.py")


def test_oi_flush_exhaustion_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("oi_flush_exhaustion_v1")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/oi_flush_exhaustion_v1_base.py")


def test_oi_compression_latest_emits_point_in_time_safe_packet(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(expanding_oi=True, count=700)

    packet = scan_oi_compression_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["event_type"] == "OI_COMPRESSION"
    assert Decimal(evidence["oi_change_2h_zscore"]) >= Decimal(evidence["oi_z_threshold"])
    assert Decimal(evidence["range_2h_percentile"]) <= Decimal(evidence["range_percentile_threshold"])
    assert evidence["reference_price"] == evidence["close"]
    assert evidence["trigger_candle_close"] == evidence["close"]
    assert "direction" not in packet
    assert "direction" not in evidence

    signal_ts = _parse_ts(packet["timestamp"])
    available_at = _parse_ts(evidence["signal_available_at"])
    assert available_at == signal_ts + timedelta(minutes=5)

    candle_chart = packet["charts"]["5m"]
    assert candle_chart["columns"] == CANDLE_COLUMNS
    candle_ts_index = CANDLE_COLUMNS.index("ts")
    candle_close_index = CANDLE_COLUMNS.index("c")
    assert all(isinstance(candle, list) for candle in candle_chart["candles"])
    for candle in candle_chart["candles"]:
        assert _parse_ts(candle[candle_ts_index]) <= signal_ts
        assert _decimal_places(candle[candle_close_index]) <= 8

    oi_chart = packet["charts"]["open_interest_5m"]
    assert oi_chart["columns"] == OI_COLUMNS
    oi_ts_index = OI_COLUMNS.index("ts")
    oi_value_index = OI_COLUMNS.index("oi_value")
    assert all(isinstance(oi_row, list) for oi_row in oi_chart["rows"])
    for oi_row in oi_chart["rows"]:
        assert _parse_ts(oi_row[oi_ts_index]) <= signal_ts
        assert _decimal_places(oi_row[oi_value_index]) <= 2

    for timeframe in ("2h", "8h", "1d"):
        chart = packet["charts"][timeframe]
        assert chart["source"] == "aggregated_confirmed_5m_up_to_signal"
        assert chart["columns"] == HTF_COLUMNS
        open_index = HTF_COLUMNS.index("open_ts")
        close_index = HTF_COLUMNS.index("close_ts")
        partial_close_index = HTF_COLUMNS.index("partial_close_ts")
        complete_index = HTF_COLUMNS.index("complete")
        range_index = HTF_COLUMNS.index("range_pct")
        for candle in chart["candles"]:
            assert isinstance(candle, list)
            assert len(candle) == len(HTF_COLUMNS)
            open_time = _parse_ts(candle[open_index])
            close_time = _parse_ts(candle[close_index])
            partial_close_time = _parse_ts(candle[partial_close_index])
            assert _decimal_places(candle[range_index]) <= 6
            if candle[complete_index]:
                assert close_time <= available_at
                assert partial_close_time == close_time
            else:
                assert open_time <= signal_ts < close_time
                assert partial_close_time == available_at
                assert close_time > available_at


def test_oi_compression_does_not_fire_without_oi_expansion(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(expanding_oi=False, count=700)

    packet = scan_oi_compression_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is None


def test_oi_compression_baseline_excludes_current_event_value() -> None:
    candles, oi_rows = _fixture_rows(expanding_oi=True, count=700)
    params = _with_defaults({})
    rows = _aligned_rows(raw_5m=candles, raw_oi=oi_rows)
    cache = _build_feature_cache(rows, params)
    index = len(rows) - 1

    features = _event_features(rows=rows, index=index, parameters=params, feature_cache=cache)

    assert features is not None
    prior_changes = [
        cache["oi_change_2h"][prior_index]
        for prior_index in range(index - int(params["stats_lookback_bars"]), index)
        if cache["oi_change_2h"][prior_index] is not None
    ]
    current_change = cache["oi_change_2h"][index]
    assert current_change is not None
    assert current_change not in prior_changes
    assert features["oi_change_2h_zscore"] == cache["oi_change_2h_zscore"][index]


def test_oi_compression_training_and_live_share_packet_builder(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(expanding_oi=True, count=700)
    latest_ts = candles[-1].timestamp
    parameters = {"dedupe_window_minutes": 0}

    packets, generated_count = generate_oi_compression_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        start=latest_ts,
        end=latest_ts,
        parameters=parameters,
    )
    live_packet = scan_oi_compression_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters=parameters,
    )

    assert generated_count == 1
    assert packets == [live_packet]


def test_oi_compression_training_respects_dedupe_seed(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(expanding_oi=True, count=700)
    latest_ts = candles[-1].timestamp

    packets, generated_count = generate_oi_compression_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        start=latest_ts,
        end=latest_ts,
        parameters={"_dedupe_seed_timestamp": _iso_z(latest_ts - timedelta(minutes=60))},
    )

    assert generated_count == 0
    assert packets == []


def test_oi_compression_base_strategy_returns_scoreable_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import oi_compression_v1_base as strategy

    candles, oi_rows = _fixture_rows(expanding_oi=True, count=700)
    packet = scan_oi_compression_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )
    assert packet is not None

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_compression_v1:BTC:test",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] in {"LONG", "SHORT"}
    assert decision["execution_profile"] == {}


def test_oi_trap_reversal_emits_failed_upside_packet(tmp_path: Path) -> None:
    candles, oi_rows = _trap_fixture_rows(side="UP", count=700)

    packet = scan_oi_trap_reversal_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["engine"] == "oi_trap_reversal_v1"
    assert evidence["event_type"] == "OI_TRAP_REVERSAL"
    assert evidence["event_subtype"] == "upside_breakout_failed"
    assert evidence["breakout_direction"] == "UP"
    assert evidence["trap_side"] == "LONGS_POTENTIALLY_TRAPPED"
    assert evidence["close_back_inside_range"] is True
    assert Decimal(evidence["breakout_distance_pct"]) >= Decimal(evidence["breakout_threshold_pct"])
    assert Decimal(evidence["rejection_wick_pct"]) >= Decimal(evidence["min_rejection_wick_pct"])
    assert Decimal(evidence["oi_change_30m_zscore"]) >= Decimal(evidence["oi_z_threshold"])
    assert Decimal(evidence["volume_ratio_2d_median"]) >= Decimal(evidence["min_volume_ratio"])
    assert evidence["reference_price"] == evidence["close"]
    assert evidence["trigger_candle_close"] == evidence["close"]
    assert "direction" not in packet
    assert "direction" not in evidence


def test_oi_trap_reversal_rejects_breakout_that_holds_outside_range(tmp_path: Path) -> None:
    candles, oi_rows = _trap_fixture_rows(side="UP", count=700)
    last = candles[-1]
    candles[-1] = MarketDataCandle(
        timestamp=last.timestamp,
        open=last.open,
        high=last.high,
        low=last.low,
        close=Decimal("100.45"),
        volume=last.volume,
        vol_ccy=last.vol_ccy,
        vol_ccy_quote=last.vol_ccy_quote,
        confirm=last.confirm,
    )

    packet = scan_oi_trap_reversal_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is None


def test_oi_trap_reversal_base_strategy_contrarian_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import oi_trap_reversal_v1_base as strategy

    upside_candles, upside_oi = _trap_fixture_rows(side="UP", count=700)
    downside_candles, downside_oi = _trap_fixture_rows(side="DOWN", count=700)
    upside_packet = scan_oi_trap_reversal_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=upside_candles,
        raw_oi=upside_oi,
        parameters={},
    )
    downside_packet = scan_oi_trap_reversal_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=downside_candles,
        raw_oi=downside_oi,
        parameters={},
    )
    assert upside_packet is not None
    assert downside_packet is not None

    upside_decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_trap_reversal_v1:BTC:up",
                "payload": upside_packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    downside_decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_trap_reversal_v1:BTC:down",
                "payload": downside_packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert upside_decision["action"] == "ENTER"
    assert upside_decision["direction"] == "SHORT"
    assert downside_decision["action"] == "ENTER"
    assert downside_decision["direction"] == "LONG"


def test_oi_trap_reversal_eth_emits_failed_upside_eth_only(tmp_path: Path) -> None:
    candles, oi_rows = _trap_fixture_rows(side="UP", count=700)

    packet = scan_oi_trap_reversal_eth_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert packet["asset"] == "ETH"
    assert evidence["engine"] == "oi_trap_reversal_eth_v1"
    assert evidence["parent_engine"] == "oi_trap_reversal_v1"
    assert evidence["event_type"] == "OI_TRAP_REVERSAL"
    assert evidence["event_subtype"] == "upside_breakout_failed"
    assert evidence["breakout_direction"] == "UP"
    assert evidence["trap_side"] == "LONGS_POTENTIALLY_TRAPPED"
    assert evidence["eth_specific_filter"] == "failed_upside_trap_only"
    assert evidence["reference_price"] == evidence["close"]
    assert evidence["trigger_candle_close"] == evidence["close"]
    assert "direction" not in packet
    assert "direction" not in evidence


def test_oi_trap_reversal_eth_rejects_downside_and_non_eth(tmp_path: Path) -> None:
    downside_candles, downside_oi = _trap_fixture_rows(side="DOWN", count=700)
    upside_candles, upside_oi = _trap_fixture_rows(side="UP", count=700)

    downside_packet = scan_oi_trap_reversal_eth_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=downside_candles,
        raw_oi=downside_oi,
        parameters={},
    )
    non_eth_packet = scan_oi_trap_reversal_eth_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=upside_candles,
        raw_oi=upside_oi,
        parameters={},
    )

    assert downside_packet is None
    assert non_eth_packet is None


def test_oi_trap_reversal_eth_base_strategy_short_only(tmp_path: Path) -> None:
    from quant_terminal_strategies import oi_trap_reversal_eth_v1_base as strategy

    candles, oi_rows = _trap_fixture_rows(side="UP", count=700)
    packet = scan_oi_trap_reversal_eth_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )
    assert packet is not None

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_trap_reversal_eth_v1:ETH:up",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    rejected_packet = {
        **packet,
        "evidence": {
            **packet["evidence"],
            "event_subtype": "downside_breakdown_failed",
            "breakout_direction": "DOWN",
        },
    }
    rejected_decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_trap_reversal_eth_v1:ETH:down",
                "payload": rejected_packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "SHORT"
    assert decision["execution_profile"] == {}
    assert rejected_decision["action"] == "SKIP"
    assert rejected_decision["direction"] == "FLAT"


def test_oi_flush_exhaustion_emits_downside_flush_packet(tmp_path: Path) -> None:
    candles, oi_rows = _flush_fixture_rows(side="DOWN", contracting_oi=True, count=700)

    packet = scan_oi_flush_exhaustion_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["engine"] == "oi_flush_exhaustion_v1"
    assert evidence["event_type"] == "OI_FLUSH_EXHAUSTION"
    assert evidence["event_subtype"] == "downside_flush_rejection"
    assert evidence["flush_direction"] == "DOWN"
    assert evidence["deleveraging_side"] == "LONG_DELEVERAGING_FLUSH"
    assert Decimal(evidence["oi_change_30m_zscore"]) <= Decimal(evidence["oi_drop_z_threshold"])
    assert Decimal(evidence["oi_change_30m_pct"]) <= Decimal(evidence["max_oi_change_30m_pct"])
    assert abs(Decimal(evidence["price_return_30m_pct"])) >= Decimal(evidence["min_abs_price_return_30m_pct"])
    assert Decimal(evidence["rejection_wick_pct"]) >= Decimal(evidence["min_rejection_wick_pct"])
    assert Decimal(evidence["volume_ratio_2d_median"]) >= Decimal(evidence["min_volume_ratio"])
    assert evidence["reference_price"] == evidence["close"]
    assert evidence["trigger_candle_close"] == evidence["close"]
    assert "direction" not in packet
    assert "direction" not in evidence


def test_oi_flush_exhaustion_emits_rich_oi_and_ema_context(tmp_path: Path) -> None:
    candles, oi_rows = _flush_fixture_rows(side="DOWN", contracting_oi=True, count=2305)
    ema_5m = _ema_fixture_rows(candles)
    ema_context_rows = {
        "2h": _ema_context_fixture_rows(candles, timeframe="2h"),
        "8h": _ema_context_fixture_rows(candles, timeframe="8h"),
        "1d": _ema_context_fixture_rows(candles, timeframe="1d"),
    }

    packet = scan_oi_flush_exhaustion_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        ema_5m_rows=ema_5m,
        ema_context_rows=ema_context_rows,
        parameters={"context_bars": 8, "oi_context_bars": 8, "oi_aggregate_context_bars": 4},
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["event_type"] == "OI_FLUSH_EXHAUSTION"
    assert evidence["oi_features"]["oi_return_4h_pct"] is not None
    assert evidence["oi_features"]["oi_return_8h_zscore_2d"] is not None
    assert evidence["oi_features"]["price_oi_divergence_8h"] is not None
    assert evidence["oi_features"]["volume_adjusted_oi_change_8h"] is not None
    assert evidence["ema_context_available"] is True
    assert set(("ema_5m", "ema_2h", "ema_8h", "ema_1d")).issubset(packet["charts"])
    assert set(("open_interest_2h", "open_interest_8h", "open_interest_1d")).issubset(packet["charts"])
    for timeframe in ("5m", "2h", "8h", "1d"):
        chart = packet["charts"][f"ema_{timeframe}"]
        assert chart["role"] == "ema_context"
        assert chart["columns"] == ["ts", "close", "ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"]
        assert chart["latest_ema_values"]["36"] is not None
        assert chart["latest_ema_distances"]["36"] is not None
    for timeframe in ("2h", "8h", "1d"):
        chart = packet["charts"][f"open_interest_{timeframe}"]
        assert chart["role"] == "positioning_context"
        assert 0 < len(chart["rows"]) <= 4
        assert all(row[2] is True for row in chart["rows"])
        assert all(_parse_ts(row[1]) <= _parse_ts(evidence["signal_available_at"]) for row in chart["rows"])
    assert len(__import__("json").dumps(packet)) < 140_000
    assert "direction" not in packet
    assert "direction" not in evidence


def test_oi_flush_exhaustion_rejects_without_oi_contraction(tmp_path: Path) -> None:
    candles, oi_rows = _flush_fixture_rows(side="DOWN", contracting_oi=False, count=700)

    packet = scan_oi_flush_exhaustion_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={},
    )

    assert packet is None


def test_oi_flush_exhaustion_base_strategy_reversal_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import oi_flush_exhaustion_v1_base as strategy

    downside_candles, downside_oi = _flush_fixture_rows(side="DOWN", contracting_oi=True, count=700)
    upside_candles, upside_oi = _flush_fixture_rows(side="UP", contracting_oi=True, count=700)
    downside_packet = scan_oi_flush_exhaustion_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=downside_candles,
        raw_oi=downside_oi,
        parameters={},
    )
    upside_packet = scan_oi_flush_exhaustion_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=upside_candles,
        raw_oi=upside_oi,
        parameters={},
    )
    assert downside_packet is not None
    assert upside_packet is not None

    downside_decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_flush_exhaustion_v1:BTC:down",
                "payload": downside_packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    upside_decision = strategy.decide(
        {
            "signal": {
                "signal_id": "oi_flush_exhaustion_v1:BTC:up",
                "payload": upside_packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert downside_decision["action"] == "ENTER"
    assert downside_decision["direction"] == "LONG"
    assert upside_decision["action"] == "ENTER"
    assert upside_decision["direction"] == "SHORT"


def _fixture_rows(*, expanding_oi: bool, count: int) -> tuple[list[MarketDataCandle], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    compression_start = count - 24
    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        if index >= compression_start:
            close = Decimal("100") + Decimal(index - compression_start) * Decimal("0.001")
            high = close + Decimal("0.01")
            low = close - Decimal("0.01")
        else:
            close = Decimal("100") + Decimal(str(sin(index / 17))) * Decimal("0.20")
            high = close + Decimal("0.70")
            low = close - Decimal("0.70")
        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=close,
                high=high,
                low=low,
                close=close,
                volume=Decimal("100"),
                vol_ccy=Decimal("100"),
                vol_ccy_quote=Decimal("10000"),
                confirm=1,
            )
        )
        if expanding_oi:
            oi_value = Decimal("10000") + Decimal(index) * Decimal("0.50") + Decimal(str(sin(index / 11))) * Decimal("5")
            if index >= compression_start:
                oi_value += Decimal(index - compression_start + 1) * Decimal("20")
        else:
            oi_value = Decimal("10000")
        oi_rows.append(
            {
                "timestamp": _iso_z(timestamp),
                "symbol": "BTCUSDT",
                "sum_open_interest": str(oi_value),
                "sum_open_interest_value": str(oi_value * close),
                "count_toptrader_long_short_ratio": "1.05",
                "sum_toptrader_long_short_ratio": "1.02",
                "count_long_short_ratio": "1.01",
                "sum_taker_long_short_vol_ratio": "1.03",
                "confirm": 1,
            }
        )
    return candles, oi_rows


def _trap_fixture_rows(*, side: str, count: int) -> tuple[list[MarketDataCandle], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    event_start = count - 6
    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        if index == count - 1 and side == "UP":
            open_value = Decimal("100.05")
            high = Decimal("100.65")
            low = Decimal("99.95")
            close = Decimal("100.08")
            volume = Decimal("180")
        elif index == count - 1 and side == "DOWN":
            open_value = Decimal("99.95")
            high = Decimal("100.05")
            low = Decimal("99.35")
            close = Decimal("99.96")
            volume = Decimal("180")
        else:
            drift = Decimal(str(sin(index / 13))) * Decimal("0.03")
            close = Decimal("100") + drift
            open_value = close
            high = close + Decimal("0.06")
            low = close - Decimal("0.06")
            volume = Decimal("100")
        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=open_value,
                high=high,
                low=low,
                close=close,
                volume=volume,
                vol_ccy=volume,
                vol_ccy_quote=volume * close,
                confirm=1,
            )
        )

        oi_value = Decimal("10000") + Decimal(index) * Decimal("0.2") + Decimal(str(sin(index / 17))) * Decimal("2")
        if index >= event_start:
            oi_value += Decimal(index - event_start + 1) * Decimal("80")
        oi_rows.append(
            {
                "timestamp": _iso_z(timestamp),
                "symbol": "BTCUSDT",
                "sum_open_interest": str(oi_value),
                "sum_open_interest_value": str(oi_value * close),
                "count_toptrader_long_short_ratio": "1.05",
                "sum_toptrader_long_short_ratio": "1.02",
                "count_long_short_ratio": "1.01",
                "sum_taker_long_short_vol_ratio": "1.03",
                "confirm": 1,
            }
        )
    return candles, oi_rows


def _flush_fixture_rows(
    *,
    side: str,
    contracting_oi: bool,
    count: int,
) -> tuple[list[MarketDataCandle], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    event_start = count - 6
    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        if index < event_start:
            close = Decimal("100") + Decimal(str(sin(index / 17))) * Decimal("0.05")
        elif side == "DOWN":
            close = Decimal("100") - Decimal(index - event_start + 1) * Decimal("0.12")
        else:
            close = Decimal("100") + Decimal(index - event_start + 1) * Decimal("0.12")

        if index == count - 1 and side == "DOWN":
            open_value = Decimal("99.65")
            high = Decimal("99.75")
            low = Decimal("98.95")
            close = Decimal("99.52")
            volume = Decimal("190")
        elif index == count - 1 and side == "UP":
            open_value = Decimal("100.35")
            high = Decimal("101.05")
            low = Decimal("100.25")
            close = Decimal("100.48")
            volume = Decimal("190")
        else:
            open_value = close
            high = close + Decimal("0.05")
            low = close - Decimal("0.05")
            volume = Decimal("100")

        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=open_value,
                high=high,
                low=low,
                close=close,
                volume=volume,
                vol_ccy=volume,
                vol_ccy_quote=volume * close,
                confirm=1,
            )
        )

        oi_value = Decimal("12000") + Decimal(str(sin(index / 19))) * Decimal("4")
        if contracting_oi and index >= event_start:
            oi_value -= Decimal(index - event_start + 1) * Decimal("80")
        oi_rows.append(
            {
                "timestamp": _iso_z(timestamp),
                "symbol": "BTCUSDT",
                "sum_open_interest": str(oi_value),
                "sum_open_interest_value": str(oi_value * close),
                "count_toptrader_long_short_ratio": "1.05",
                "sum_toptrader_long_short_ratio": "1.02",
                "count_long_short_ratio": "1.01",
                "sum_taker_long_short_vol_ratio": "1.03",
                "confirm": 1,
            }
        )
    return candles, oi_rows


def _ema_fixture_rows(candles: list[MarketDataCandle]) -> list[dict[str, Any]]:
    rows = []
    for candle in candles:
        close = candle.close
        row: dict[str, Any] = {
            "timestamp": _iso_z(candle.timestamp),
            "open": str(candle.open),
            "high": str(candle.high),
            "low": str(candle.low),
            "close": str(close),
            "volume": str(candle.volume),
            "vol_ccy": str(candle.vol_ccy),
            "vol_ccy_quote": str(candle.vol_ccy_quote),
            "confirm": 1,
        }
        for period in (36, 43, 144, 169, 576, 676):
            row[f"ema_{period}"] = str(close + Decimal(period) / Decimal("100000"))
            row[f"ema_warmup_count_{period}"] = period
        rows.append(row)
    return rows


def _ema_context_fixture_rows(candles: list[MarketDataCandle], *, timeframe: str) -> list[dict[str, Any]]:
    step = {"2h": 24, "8h": 96, "1d": 288}[timeframe]
    rows = []
    for index in range(0, len(candles), step):
        source = candles[min(index + step - 1, len(candles) - 1)]
        open_candle = candles[index]
        if index + step > len(candles):
            break
        close = source.close
        row: dict[str, Any] = {
            "timestamp": _iso_z(open_candle.timestamp),
            "open": str(open_candle.open),
            "high": str(max(candle.high for candle in candles[index : index + step])),
            "low": str(min(candle.low for candle in candles[index : index + step])),
            "close": str(close),
            "volume": str(sum((candle.volume for candle in candles[index : index + step]), Decimal("0"))),
            "vol_ccy": str(sum((candle.vol_ccy for candle in candles[index : index + step]), Decimal("0"))),
            "vol_ccy_quote": str(sum((candle.vol_ccy_quote for candle in candles[index : index + step]), Decimal("0"))),
            "confirm": 1,
        }
        for period in (36, 43, 144, 169, 576, 676):
            row[f"ema_{period}"] = str(close + Decimal(period) / Decimal("100000"))
            row[f"ema_warmup_count_{period}"] = period
        rows.append(row)
    return rows


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _decimal_places(value: Any) -> int:
    text = str(value)
    if "." not in text:
        return 0
    return len(text.rsplit(".", 1)[1])
