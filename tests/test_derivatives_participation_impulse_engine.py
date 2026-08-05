from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.derivatives_participation_impulse_v1 import (
    FUTURES_METRICS_COLUMNS,
    HTF_FUTURES_METRICS_COLUMNS,
    generate_derivatives_participation_impulse_packets,
    generate_training_signals,
    scan_derivatives_participation_impulse_latest,
)
from quant_terminal_worker.signal_engines.runtime import EngineTrainingContext


def test_derivatives_participation_impulse_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec("derivatives_participation_impulse_v1")
    validate_strategy_module(
        "packages/strategy_modules/src/quant_terminal_strategies/derivatives_participation_impulse_v1_base.py"
    )


def test_derivatives_participation_impulse_latest_emits_point_in_time_packet(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(resolution="UP", count=820)

    packet = scan_derivatives_participation_impulse_latest(
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
    assert evidence["engine"] == "derivatives_participation_impulse_v1"
    assert evidence["event_type"] == "DERIVATIVES_PARTICIPATION_IMPULSE"
    assert evidence["selected_leaf"] in {"clean_participation_breakout", "absorption_resolution"}
    assert evidence["observed_resolution"] == "UP"
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

    for timeframe in ("1h", "4h"):
        candle_chart = packet["charts"][timeframe]
        assert candle_chart["source"] == "aggregated_confirmed_5m_up_to_signal"
        _assert_htf_rows_point_in_time_safe(candle_chart["columns"], candle_chart["candles"], available_at, signal_ts)

        metric_chart = packet["charts"][f"futures_metrics_{timeframe}"]
        assert metric_chart["columns"] == HTF_FUTURES_METRICS_COLUMNS
        _assert_htf_rows_point_in_time_safe(metric_chart["columns"], metric_chart["rows"], available_at, signal_ts)


def test_derivatives_participation_impulse_training_and_live_share_packet_builder(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(resolution="DOWN", count=820)
    latest_ts = candles[-1].timestamp
    parameters = {**_test_parameters(), "dedupe_window_minutes": 0}

    packets, generated_count = generate_derivatives_participation_impulse_packets(
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
    live_packet = scan_derivatives_participation_impulse_latest(
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


def test_derivatives_participation_impulse_reports_aligned_scan_coverage_when_metrics_lag(
    tmp_path: Path,
) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(resolution="UP", count=820)
    latest_candle_ts = candles[-1].timestamp
    latest_aligned_ts = metrics[-2]["timestamp"]

    class Reader:
        @staticmethod
        def get_candles(**kwargs):
            return candles

        @staticmethod
        def get_rows(**kwargs):
            if kwargs["data_type"] == "open_interest":
                return oi
            if kwargs["data_type"] == "futures_metrics":
                return metrics[:-1]
            if kwargs["data_type"] == "premium_index":
                return premium
            if kwargs["data_type"] == "funding_features":
                return funding
            return []

    output = generate_training_signals(
        EngineTrainingContext(
            asset="ETH",
            instrument="ETH-USDT-SWAP",
            signal_set={},
            signal_set_key="derivatives_participation_impulse_v1:ETH:test",
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


def test_derivatives_participation_impulse_strategy_maps_observed_resolution_to_direction(tmp_path: Path) -> None:
    from quant_terminal_strategies import derivatives_participation_impulse_v1_base as strategy

    up_candles, up_oi, up_metrics, up_premium, up_funding = _fixture_rows(resolution="UP", count=820)
    down_candles, down_oi, down_metrics, down_premium, down_funding = _fixture_rows(resolution="DOWN", count=820)

    up_packet = scan_derivatives_participation_impulse_latest(
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
    down_packet = scan_derivatives_participation_impulse_latest(
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
            "signal": {"signal_id": "derivatives_participation_impulse_v1:ETH:up", "payload": up_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    down_decision = strategy.decide(
        {
            "signal": {"signal_id": "derivatives_participation_impulse_v1:ETH:down", "payload": down_packet},
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert up_decision["action"] == "ENTER"
    assert up_decision["direction"] == "LONG"
    assert down_decision["action"] == "ENTER"
    assert down_decision["direction"] == "SHORT"


def test_derivatives_participation_impulse_rejects_overheated_funding(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(resolution="UP", count=820, funding_zscore=3.0)

    packet = scan_derivatives_participation_impulse_latest(
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

    assert packet is None


def test_derivatives_participation_impulse_generation_uses_packet_sink_chunks(tmp_path: Path) -> None:
    candles, oi, metrics, premium, funding = _fixture_rows(resolution="UP", count=900, repeated_impulses=True)
    start = candles[620].timestamp
    end = candles[-1].timestamp
    chunks: list[list[dict[str, Any]]] = []

    packets, generated_count = generate_derivatives_participation_impulse_packets(
        workspace_root=tmp_path,
        asset="ETH",
        instrument="ETH-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi,
        raw_metrics=metrics,
        raw_premium=premium,
        funding_features=funding,
        start=start,
        end=end,
        parameters={**_test_parameters(), "dedupe_window_minutes": 0, "context_timeframes": ["1h"]},
        packet_sink=chunks.append,
        packet_chunk_size=2,
    )

    assert packets == []
    assert generated_count >= 2
    assert chunks
    assert all(1 <= len(chunk) <= 2 for chunk in chunks)


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
    resolution: str,
    count: int,
    funding_zscore: float = 0.2,
    repeated_impulses: bool = False,
) -> tuple[list[MarketDataCandle], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, Any]] = []
    metrics: list[dict[str, Any]] = []
    premium: list[dict[str, Any]] = []
    funding: list[dict[str, Any]] = []
    impulse_indexes = {count - 1}
    if repeated_impulses:
        impulse_indexes.update(range(640, count, 37))

    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        available_at = timestamp + timedelta(minutes=5)
        base = 100 + (index % 6) * 0.01
        open_value = base
        high = base + 0.10
        low = base - 0.10
        close = base + 0.01
        is_impulse = index in impulse_indexes
        if is_impulse and resolution == "UP":
            open_value = 100.1
            high = 101.6
            low = 100.0
            close = 101.3
        elif is_impulse and resolution == "DOWN":
            open_value = 100.1
            high = 100.2
            low = 98.4
            close = 98.7

        quote_volume = 10000 + (6000 if index >= count - 48 or is_impulse else 0)
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

        oi = 10000 + index * 0.2
        if index >= count - 48:
            oi += (index - (count - 48) + 1) * 24
        if is_impulse:
            oi += 400
        if resolution == "UP":
            taker_ratio = 1.18 if index >= count - 48 or is_impulse else 1.0
        else:
            taker_ratio = 0.82 if index >= count - 48 or is_impulse else 1.0
        global_ratio = 1.01
        top_account_ratio = 1.03 if resolution == "UP" else 0.97
        top_position_ratio = 1.08 if resolution == "UP" else 0.92
        premium_close = 0.00025 if resolution == "UP" else -0.00025

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
                "latest_funding_rate": 0.00005 if resolution == "UP" else -0.00005,
                "annualized_funding_rate": 0.05 if resolution == "UP" else -0.05,
                "funding_rate_zscore_7d": funding_zscore,
                "funding_signed_streak": 1 if resolution == "UP" else -1,
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
        "breakout_threshold_pct": 0.02,
        "range_percentile_threshold": 0.80,
        "min_oi_z_threshold": 0.3,
        "min_oi_change_1h_pct": 0.01,
        "min_volume_ratio": 1.02,
        "min_taker_imbalance_z": 0.2,
        "context_bars": 48,
        "context_timeframes": ["1h", "4h"],
    }


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
