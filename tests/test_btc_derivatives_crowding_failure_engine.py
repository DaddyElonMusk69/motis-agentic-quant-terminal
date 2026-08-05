from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.btc_derivatives_crowding_failure_v1 import (
    FUTURES_METRICS_COLUMNS,
    HTF_FUTURES_METRICS_COLUMNS,
    generate_derivatives_crowding_failure_packets,
    generate_training_signals,
    scan_derivatives_crowding_failure_latest,
)
from quant_terminal_worker.signal_engines.runtime import EngineTrainingContext


def test_btc_derivatives_crowding_failure_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("btc_derivatives_crowding_failure_v1")
    validate_strategy_module(
        "packages/strategy_modules/src/quant_terminal_strategies/btc_derivatives_crowding_failure_v1_base.py"
    )


def test_btc_derivatives_crowding_failure_latest_emits_rich_point_in_time_packet(tmp_path: Path) -> None:
    candles, metrics, premium, funding = _fixture_rows(failure="UP", count=760)

    packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=_test_parameters(),
    )

    assert packet is not None
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["engine"] == "btc_derivatives_crowding_failure_v1"
    assert evidence["event_type"] == "DERIVATIVES_CROWDING_FAILURE"
    assert evidence["event_subtype"] == "crowded_long_failure"
    assert evidence["crowding_state"] == "leveraged_longs_failed_to_extend"
    assert evidence["breakout_direction"] == "UP"
    assert evidence["reference_price"] == evidence["trigger_candle_close"]
    assert "direction" not in packet
    assert "direction" not in evidence

    signal_ts = _parse_ts(packet["timestamp"])
    available_at = _parse_ts(evidence["signal_available_at"])
    assert available_at == signal_ts + timedelta(minutes=5)

    futures_5m = packet["charts"]["futures_metrics_5m"]
    assert futures_5m["columns"] == FUTURES_METRICS_COLUMNS
    futures_available_index = FUTURES_METRICS_COLUMNS.index("available_at")
    assert all(_parse_ts(row[futures_available_index]) <= available_at for row in futures_5m["rows"])

    assert packet["charts"]["premium_index_5m"]["rows"]
    assert packet["charts"]["funding_features_5m"]["rows"]

    for timeframe in ("1h", "4h", "12h"):
        candle_chart = packet["charts"][timeframe]
        assert candle_chart["source"] == "aggregated_confirmed_5m_up_to_signal"
        _assert_htf_rows_point_in_time_safe(candle_chart["columns"], candle_chart["candles"], available_at, signal_ts)

        metric_chart = packet["charts"][f"futures_metrics_{timeframe}"]
        assert metric_chart["columns"] == HTF_FUTURES_METRICS_COLUMNS
        _assert_htf_rows_point_in_time_safe(metric_chart["columns"], metric_chart["rows"], available_at, signal_ts)


def test_btc_derivatives_crowding_failure_training_and_live_share_packet_builder(tmp_path: Path) -> None:
    candles, metrics, premium, funding = _fixture_rows(failure="UP", count=760)
    latest_ts = candles[-1].timestamp
    parameters = {**_test_parameters(), "dedupe_window_minutes": 0}

    packets, generated_count = generate_derivatives_crowding_failure_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        start=latest_ts,
        end=latest_ts,
        parameters=parameters,
    )
    live_packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=parameters,
    )

    assert generated_count == 1
    assert packets == [live_packet]


def test_btc_derivatives_crowding_failure_reports_aligned_scan_coverage_when_metrics_lag(
    tmp_path: Path,
) -> None:
    candles, metrics, premium, funding = _fixture_rows(failure="UP", count=760)
    latest_candle_ts = candles[-1].timestamp
    latest_aligned_ts = metrics[-2]["timestamp"]

    class Reader:
        @staticmethod
        def get_candles(**kwargs):
            return candles

        @staticmethod
        def get_rows(**kwargs):
            if kwargs["data_type"] == "futures_metrics":
                return metrics[:-1]
            if kwargs["data_type"] == "premium_index":
                return premium
            if kwargs["data_type"] == "funding_features":
                return funding
            return []

    output = generate_training_signals(
        EngineTrainingContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            signal_set={},
            signal_set_key="btc_derivatives_crowding_failure_v1:BTC:test",
            parameters={**_test_parameters(), "dedupe_window_minutes": 0},
            market_data_reader=Reader(),
            spec=None,
            workspace_root=tmp_path,
            repository=None,
            start=latest_candle_ts,
            end=latest_candle_ts,
            raw_candle_end=latest_candle_ts,
        )
    )

    assert output.result.scan_coverage_end_ts == latest_aligned_ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_btc_derivatives_crowding_failure_accepts_bnb_asset(tmp_path: Path) -> None:
    candles, metrics, premium, funding = _fixture_rows(failure="UP", count=760)

    packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BNB",
        instrument="BNB-USDT-SWAP",
        raw_5m=candles,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=_test_parameters(),
    )

    assert packet is not None
    assert packet["asset"] == "BNB"
    assert packet["instrument"] == "BNB-USDT-SWAP"


def test_btc_derivatives_crowding_failure_strategy_maps_failures_to_reversal_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import btc_derivatives_crowding_failure_v1_base as strategy

    up_candles, up_metrics, up_premium, up_funding = _fixture_rows(failure="UP", count=760)
    down_candles, down_metrics, down_premium, down_funding = _fixture_rows(failure="DOWN", count=760)

    up_packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=up_candles,
        raw_metrics=up_metrics,
        raw_premium=up_premium,
        funding_features=up_funding,
        parameters=_test_parameters(),
    )
    down_packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=down_candles,
        raw_metrics=down_metrics,
        raw_premium=down_premium,
        funding_features=down_funding,
        parameters=_test_parameters(),
    )
    assert up_packet is not None
    assert down_packet is not None

    up_decision = strategy.decide(
        {
            "signal": {"signal_id": "btc_derivatives_crowding_failure_v1:BTC:up", "payload": up_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    down_decision = strategy.decide(
        {
            "signal": {"signal_id": "btc_derivatives_crowding_failure_v1:BTC:down", "payload": down_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert up_decision["action"] == "ENTER"
    assert up_decision["direction"] == "SHORT"
    assert down_decision["action"] == "ENTER"
    assert down_decision["direction"] == "LONG"


def test_btc_derivatives_crowding_failure_rejects_uncrowded_failure(tmp_path: Path) -> None:
    candles, metrics, premium, funding = _fixture_rows(failure="UP", count=760, crowded=False)

    packet = scan_derivatives_crowding_failure_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        parameters=_test_parameters(),
    )

    assert packet is None


def _assert_htf_rows_point_in_time_safe(
    columns: list[str],
    rows: list[list[Any]],
    available_at: datetime,
    signal_ts: datetime,
) -> None:
    assert rows
    open_index = columns.index("open_ts")
    close_index = columns.index("close_ts")
    partial_close_index = columns.index("partial_close_ts")
    complete_index = columns.index("complete")
    for row in rows:
        open_time = _parse_ts(row[open_index])
        close_time = _parse_ts(row[close_index])
        partial_close_time = _parse_ts(row[partial_close_index])
        if row[complete_index]:
            assert close_time <= available_at
            assert partial_close_time == close_time
        else:
            assert open_time <= signal_ts < close_time
            assert partial_close_time == available_at
            assert close_time > available_at


def _fixture_rows(
    *,
    failure: str,
    count: int,
    crowded: bool = True,
) -> tuple[list[MarketDataCandle], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    metrics: list[dict[str, Any]] = []
    premium: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []

    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        available_at = timestamp + timedelta(minutes=5)
        base = 100 + (index % 8) * 0.01
        open_value = base
        high = base + 0.35
        low = base - 0.35
        close = base + 0.02
        if index == count - 1 and failure == "UP":
            open_value = 100.1
            high = 102.0
            low = 99.8
            close = 100.2
        elif index == count - 1 and failure == "DOWN":
            open_value = 100.1
            high = 100.4
            low = 98.0
            close = 100.2

        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=Decimal(str(open_value)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=Decimal("100"),
                vol_ccy=Decimal("100"),
                vol_ccy_quote=Decimal(str(10000 + (5000 if index >= count - 48 else 0))),
                confirm=1,
            )
        )

        oi = 10000 + index * 0.5
        if index >= count - 48:
            oi += (index - (count - 48) + 1) * 30
        if crowded and failure == "UP":
            global_ratio = 1.2 if index >= count - 48 else 1.0
            taker_ratio = 1.25 if index >= count - 48 else 1.0
            top_account_ratio = 0.98 if index >= count - 48 else 1.0
            top_position_ratio = 0.95 if index >= count - 48 else 1.0
            premium_close = 0.0008
            funding_rate = 0.00015
        elif crowded and failure == "DOWN":
            global_ratio = 0.8 if index >= count - 48 else 1.0
            taker_ratio = 0.75 if index >= count - 48 else 1.0
            top_account_ratio = 1.02 if index >= count - 48 else 1.0
            top_position_ratio = 1.05 if index >= count - 48 else 1.0
            premium_close = -0.0008
            funding_rate = -0.00015
        else:
            global_ratio = 1.0
            taker_ratio = 1.0
            top_account_ratio = 1.0
            top_position_ratio = 1.0
            premium_close = 0.0
            funding_rate = 0.0

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
                "latest_funding_rate": funding_rate,
                "annualized_funding_rate": funding_rate * 365 * 3,
                "funding_rate_zscore_7d": 1.5 if funding_rate > 0 else (-1.5 if funding_rate < 0 else 0.0),
                "funding_signed_streak": 3 if funding_rate > 0 else (-3 if funding_rate < 0 else 0),
                "minutes_to_expected_funding": 60,
            }
        )

    return candles, metrics, premium, funding


def _test_parameters() -> dict[str, Any]:
    return {
        "range_lookback_bars": 96,
        "oi_change_window_bars": 48,
        "stats_lookback_bars": 240,
        "min_stats_bars": 120,
        "context_timeframes": ["1h", "4h", "12h"],
    }


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
