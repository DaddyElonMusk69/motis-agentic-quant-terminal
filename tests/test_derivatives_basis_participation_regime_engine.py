from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.derivatives_basis_participation_regime_v1 import (
    generate_derivatives_basis_participation_regime_packets,
    scan_derivatives_basis_participation_regime_latest,
)


def test_derivatives_basis_participation_regime_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("derivatives_basis_participation_regime_v1")
    validate_strategy_module(
        "packages/strategy_modules/src/quant_terminal_strategies/derivatives_basis_participation_regime_v1_base.py"
    )


def test_derivatives_basis_participation_regime_latest_emits_continuation_packet(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(mode="CONTINUATION_UP", count=520)

    packet = scan_derivatives_basis_participation_regime_latest(
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
    assert evidence["engine"] == "derivatives_basis_participation_regime_v1"
    assert evidence["event_type"] == "DERIVATIVES_BASIS_PARTICIPATION_REGIME"
    assert evidence["selected_leaf"] == "healthy_continuation"
    assert evidence["observed_regime_bias"] == "UP"
    assert evidence["reference_price"] == evidence["trigger_candle_close"]
    assert "direction" not in packet
    assert "direction" not in evidence
    assert packet["charts"]["futures_metrics_5m"]["rows"]
    assert packet["charts"]["premium_index_5m"]["rows"]
    assert packet["charts"]["funding_features_5m"]["rows"]
    for timeframe in ("1h", "4h", "12h"):
        assert timeframe in packet["charts"]
        assert f"futures_metrics_{timeframe}" in packet["charts"]


def test_derivatives_basis_participation_regime_training_and_live_share_packet_builder(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(mode="CONTINUATION_UP", count=520)
    latest_ts = candles[-1].timestamp
    parameters = {**_test_parameters(), "dedupe_window_minutes": 0}

    packets, generated_count = generate_derivatives_basis_participation_regime_packets(
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
    live_packet = scan_derivatives_basis_participation_regime_latest(
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


def test_derivatives_basis_participation_regime_strategy_maps_bias_to_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import derivatives_basis_participation_regime_v1_base as strategy

    up_candles, up_oi, up_metrics, up_premium, up_funding = _fixture_rows(mode="CONTINUATION_UP", count=520)
    fade_candles, fade_oi, fade_metrics, fade_premium, fade_funding = _fixture_rows(mode="CROWDED_LONG_FADE", count=520)
    up_packet = scan_derivatives_basis_participation_regime_latest(
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
    fade_packet = scan_derivatives_basis_participation_regime_latest(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=fade_candles,
        raw_oi=fade_oi,
        raw_metrics=fade_metrics,
        raw_premium=fade_premium,
        funding_features=fade_funding,
        parameters=_test_parameters(),
    )
    assert up_packet is not None
    assert fade_packet is not None
    assert fade_packet["evidence"]["selected_leaf"] == "crowded_basis_fade"

    up_decision = strategy.decide({"signal": {"signal_id": "basis:ETH:up", "payload": up_packet}, "runtime_mode": "stage1", "parameters": {}})
    fade_decision = strategy.decide({"signal": {"signal_id": "basis:ETH:fade", "payload": fade_packet}, "runtime_mode": "stage1", "parameters": {}})

    assert up_decision["action"] == "ENTER"
    assert up_decision["direction"] == "LONG"
    assert fade_decision["action"] == "ENTER"
    assert fade_decision["direction"] == "SHORT"


def _fixture_rows(
    *,
    mode: str,
    count: int,
) -> tuple[list[MarketDataCandle], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    premium: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []
    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        available_at = timestamp + timedelta(minutes=5)
        recent = index >= count - 48
        last_hour = index >= count - 12
        open_value = 100.0 + (index % 10) * 0.002
        close = open_value + 0.005
        if mode == "CONTINUATION_UP" and recent:
            step = index - (count - 48)
            open_value = 100.0 + step * 0.015
            close = open_value + 0.035
        elif mode == "CROWDED_LONG_FADE" and last_hour:
            step = index - (count - 12)
            open_value = 100.20 - step * 0.03
            close = open_value - 0.04
        high = max(open_value, close) + 0.08
        low = min(open_value, close) - 0.08
        quote_volume = 11000.0 if recent else 10000.0
        oi = 10000.0 + index * 0.05
        if mode == "CONTINUATION_UP" and recent:
            oi += (index - (count - 48) + 1) * 6.0
        taker_ratio = 1.0
        funding_zscore = 0.2
        premium_close = 0.00005
        global_ratio = 1.02
        if mode == "CONTINUATION_UP" and recent:
            taker_ratio = 1.035
        elif mode == "CROWDED_LONG_FADE":
            funding_zscore = 2.2
            premium_close = 0.00035
            global_ratio = 1.18
            if last_hour:
                taker_ratio = 0.965

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
        oi_rows.append({"timestamp": timestamp, "available_at": available_at, "sum_open_interest": oi, "sum_open_interest_value": oi * close, "confirm": 1})
        metrics.append(
            {
                "timestamp": timestamp,
                "interval_end": available_at,
                "available_at": available_at,
                "sum_open_interest": oi,
                "sum_open_interest_value": oi * close,
                "top_trader_account_long_short_ratio": 1.04,
                "top_trader_position_long_short_ratio": 1.06,
                "global_account_long_short_ratio": global_ratio,
                "taker_buy_sell_volume_ratio": taker_ratio,
                "confirm": 1,
            }
        )
        premium.append({"timestamp": timestamp, "available_at": available_at, "premium_open": premium_close, "premium_high": premium_close, "premium_low": premium_close, "premium_close": premium_close, "confirm": 1})
        funding.append({"timestamp": timestamp, "available_at": available_at, "latest_funding_rate": 0.00005, "annualized_funding_rate": 0.05, "funding_rate_zscore_7d": funding_zscore, "funding_signed_streak": 1, "minutes_to_expected_funding": 60})
    return candles, oi_rows, metrics, premium, funding


def _test_parameters() -> dict[str, Any]:
    return {
        "range_lookback_bars": 96,
        "range_window_bars": 96,
        "oi_change_window_bars": 48,
        "stats_lookback_bars": 220,
        "min_stats_bars": 120,
        "min_trend_return_4h_pct": 0.15,
        "min_short_return_1h_pct": 0.02,
        "min_oi_change_4h_pct": 0.01,
        "min_volume_ratio": 0.90,
        "min_taker_imbalance_z": 0.02,
        "taker_buy_ratio_threshold": 1.005,
        "taker_sell_ratio_threshold": 0.995,
        "context_bars": 48,
        "context_timeframes": ["1h", "4h", "12h"],
    }
