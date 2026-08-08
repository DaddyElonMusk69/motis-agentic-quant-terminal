from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.leverage_reset_reacceleration_v1 import (
    generate_leverage_reset_reacceleration_packets,
    scan_leverage_reset_reacceleration_latest,
)


def test_leverage_reset_reacceleration_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("leverage_reset_reacceleration_v1")
    validate_strategy_module(
        "packages/strategy_modules/src/quant_terminal_strategies/leverage_reset_reacceleration_v1_base.py"
    )


def test_leverage_reset_reacceleration_latest_emits_point_in_time_packet(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(mode="UP", count=820)

    packet = scan_leverage_reset_reacceleration_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=_test_parameters(),
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["engine"] == "leverage_reset_reacceleration_v1"
    assert evidence["event_type"] == "LEVERAGE_RESET_REACCELERATION"
    assert evidence["observed_reacceleration"] == "UP"
    assert evidence["reset_state"] == "downside_flush_leverage_cleared"
    assert evidence["reference_price"] == evidence["trigger_candle_close"]
    assert "direction" not in packet
    assert "direction" not in evidence

    signal_ts = _parse_ts(packet["timestamp"])
    available_at = _parse_ts(evidence["signal_available_at"])
    assert available_at == signal_ts + timedelta(minutes=5)
    assert packet["charts"]["futures_metrics_5m"]["rows"]
    assert packet["charts"]["premium_index_5m"]["rows"]
    assert packet["charts"]["funding_features_5m"]["rows"]
    for timeframe in ("1h", "4h", "12h"):
        assert timeframe in packet["charts"]
        assert f"futures_metrics_{timeframe}" in packet["charts"]


def test_leverage_reset_reacceleration_training_and_live_share_packet_builder(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(mode="UP", count=820)
    latest_ts = candles[-1].timestamp
    parameters = {**_test_parameters(), "dedupe_window_minutes": 0}

    packets, generated_count = generate_leverage_reset_reacceleration_packets(
        workspace_root=tmp_path,
        asset="SOL",
        instrument="SOL-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        start=latest_ts,
        end=latest_ts,
        parameters=parameters,
    )
    live_packet = scan_leverage_reset_reacceleration_latest(
        workspace_root=tmp_path,
        asset="SOL",
        instrument="SOL-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=parameters,
    )

    assert generated_count == 1
    assert packets == [live_packet]


def test_leverage_reset_reacceleration_strategy_maps_reacceleration_to_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import leverage_reset_reacceleration_v1_base as strategy

    up_candles, up_oi, up_metrics, up_premium, up_funding = _fixture_rows(mode="UP", count=820)
    down_candles, down_oi, down_metrics, down_premium, down_funding = _fixture_rows(mode="DOWN", count=820)

    up_packet = scan_leverage_reset_reacceleration_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=up_candles,
        raw_oi=up_oi,
        raw_metrics=up_metrics,
        raw_premium=up_premium,
        funding_features=up_funding,
        parameters=_test_parameters(),
    )
    down_packet = scan_leverage_reset_reacceleration_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=down_candles,
        raw_oi=down_oi,
        raw_metrics=down_metrics,
        raw_premium=down_premium,
        funding_features=down_funding,
        parameters=_test_parameters(),
    )
    assert up_packet is not None
    assert down_packet is not None

    up_decision = strategy.decide(
        {
            "signal": {"signal_id": "leverage_reset_reacceleration_v1:ETH:up", "payload": up_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    down_decision = strategy.decide(
        {
            "signal": {"signal_id": "leverage_reset_reacceleration_v1:ETH:down", "payload": down_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert up_decision["action"] == "ENTER"
    assert up_decision["direction"] == "LONG"
    assert down_decision["action"] == "ENTER"
    assert down_decision["direction"] == "SHORT"


def _fixture_rows(
    *,
    mode: str,
    count: int,
) -> tuple[list[MarketDataCandle], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    reset_index = count - 24
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    premium: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []

    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        available_at = timestamp + timedelta(minutes=5)
        drift = (index % 8) * 0.005
        open_value = 100.0 + drift
        high = open_value + 0.10
        low = open_value - 0.10
        close = open_value + 0.01
        if index == reset_index and mode == "UP":
            open_value, high, low, close = 100.0, 100.10, 98.40, 98.90
        elif index == reset_index and mode == "DOWN":
            open_value, high, low, close = 100.0, 101.60, 99.90, 101.10
        elif index > reset_index and mode == "UP":
            step = index - reset_index
            open_value = 98.90 + step * 0.035
            high = open_value + 0.18
            low = open_value - 0.06
            close = open_value + 0.12
        elif index > reset_index and mode == "DOWN":
            step = index - reset_index
            open_value = 101.10 - step * 0.035
            high = open_value + 0.06
            low = open_value - 0.18
            close = open_value - 0.12

        if index < reset_index:
            oi = 10000.0 + index * 0.1
        elif index == reset_index:
            oi = 9800.0
        else:
            oi = 9800.0 + (index - reset_index) * 13.0
        quote_volume = 10000.0
        if index == reset_index:
            quote_volume = 22000.0
        elif index > reset_index:
            quote_volume = 17000.0
        taker_ratio = 1.0
        if index > reset_index and mode == "UP":
            taker_ratio = 1.14
        elif index > reset_index and mode == "DOWN":
            taker_ratio = 0.86
        global_ratio = 1.01
        top_account_ratio = 1.03 if mode == "UP" else 0.97
        top_position_ratio = 1.06 if mode == "UP" else 0.94
        premium_close = 0.00005 if mode == "UP" else -0.00005

        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=Decimal(str(open_value)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=Decimal("100"),
                vol_ccy=Decimal("100"),
                vol_ccy_quote=Decimal(str(quote_volume)),
                confirm=1,
            )
        )
        oi_rows.append(
            {
                "timestamp": timestamp,
                "available_at": available_at,
                "sum_open_interest": oi,
                "sum_open_interest_value": oi * close,
                "confirm": 1,
            }
        )
        metrics.append(
            {
                "timestamp": timestamp,
                "interval_end": available_at,
                "available_at": available_at,
                "sum_open_interest": oi,
                "sum_open_interest_value": oi * close,
                "top_trader_account_long_short_ratio": top_account_ratio,
                "top_trader_position_long_short_ratio": top_position_ratio,
                "global_account_long_short_ratio": global_ratio,
                "taker_buy_sell_volume_ratio": taker_ratio,
                "confirm": 1,
            }
        )
        premium.append(
            {
                "timestamp": timestamp,
                "available_at": available_at,
                "premium_open": premium_close * 0.9,
                "premium_high": premium_close * 1.1,
                "premium_low": premium_close * 0.8,
                "premium_close": premium_close,
                "confirm": 1,
            }
        )
        funding.append(
            {
                "timestamp": timestamp,
                "available_at": available_at,
                "latest_funding_rate": 0.00002 if mode == "UP" else -0.00002,
                "annualized_funding_rate": 0.02 if mode == "UP" else -0.02,
                "funding_rate_zscore_7d": 0.2,
                "funding_signed_streak": 1 if mode == "UP" else -1,
                "minutes_to_expected_funding": 60,
            }
        )

    return candles, oi_rows, metrics, premium, funding


def _test_parameters() -> dict[str, Any]:
    return {
        "range_lookback_bars": 96,
        "range_window_bars": 96,
        "oi_change_window_bars": 48,
        "stats_lookback_bars": 240,
        "min_stats_bars": 120,
        "reset_lookback_bars": 48,
        "min_reset_age_bars": 2,
        "max_reset_age_bars": 48,
        "min_flush_extension_pct": 0.08,
        "min_flush_candle_range_pct": 0.35,
        "min_reset_oi_drop_1h_pct": 0.04,
        "min_reset_volume_ratio": 1.05,
        "min_reclaim_from_extreme_pct": 0.12,
        "min_oi_rebuild_1h_pct": 0.01,
        "min_volume_ratio": 1.01,
        "min_taker_imbalance_z": 0.15,
        "taker_buy_ratio_threshold": 1.02,
        "taker_sell_ratio_threshold": 0.98,
        "max_abs_funding_zscore": 2.1,
        "max_abs_premium_zscore": 2.1,
        "post_reset_global_ratio_low": 0.85,
        "post_reset_global_ratio_high": 1.18,
        "context_bars": 48,
        "context_timeframes": ["1h", "4h", "12h"],
    }


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
