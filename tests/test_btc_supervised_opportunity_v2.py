from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
from pathlib import Path

from quant_terminal_sdk.engine_contracts import (
    validate_signal_packet,
    validate_strategy_module,
)
from quant_terminal_strategies.btc_supervised_opportunity_v2_base import decide
from quant_terminal_worker.signal_engines import btc_supervised_opportunity_v2 as engine


STRATEGY_PATH = (
    "packages/strategy_modules/src/quant_terminal_strategies/"
    "btc_supervised_opportunity_v2_base.py"
)


def test_rejected_candidate_is_not_registered_and_artifact_is_reproducible() -> None:
    registry = json.loads(Path("artifacts/signal_engine/engine_registry.json").read_text())
    assert engine.ENGINE_ID not in registry
    artifact = engine._load_verified_artifact(Path.cwd())
    assert artifact["model_version"] == "3.0"
    assert artifact["feature_schema_version"] == engine.FEATURE_SCHEMA_VERSION
    validate_strategy_module(STRATEGY_PATH)


def test_auxiliary_rows_must_be_available_before_decision_time() -> None:
    candles = _candles(3)
    rows = engine.build_feature_frame(
        raw_5m=candles,
        raw_oi=_oi_rows(3),
        raw_metrics=[
            {
                "timestamp": "2026-01-01T00:00:00Z",
                "available_at": "2026-01-01T00:05:00Z",
                "top_trader_account_long_short_ratio": 1.1,
                "top_trader_position_long_short_ratio": 1.2,
                "global_account_long_short_ratio": 1.0,
                "taker_buy_sell_volume_ratio": 0.9,
                "sum_open_interest": 1000,
                "sum_open_interest_value": 100000,
                "complete": True,
                "confirm": 1,
            },
            {
                "timestamp": "2026-01-01T00:05:00Z",
                "available_at": "2026-01-01T00:15:01Z",
                "top_trader_account_long_short_ratio": 9.9,
                "top_trader_position_long_short_ratio": 9.9,
                "global_account_long_short_ratio": 9.9,
                "taker_buy_sell_volume_ratio": 9.9,
                "sum_open_interest": 1000,
                "sum_open_interest_value": 100000,
                "complete": True,
                "confirm": 1,
            },
        ],
        raw_premium=_premium_rows(3),
        funding_features=_funding_rows(3),
    )

    assert rows.loc[0, "metrics_coverage"] == 1.0
    assert rows.loc[1, "metrics_coverage"] == 0.0
    assert rows.loc[1, "top_account_ratio"] != 9.9


def test_packet_is_neutral_and_strategy_reads_canonical_wrapper() -> None:
    rows = engine.build_feature_frame(
        raw_5m=_candles(20),
        raw_oi=_oi_rows(20),
        raw_metrics=_metrics_rows(20),
        raw_premium=_premium_rows(20),
        funding_features=_funding_rows(20),
    )
    packet = engine.build_packet(
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        frame=rows,
        index=len(rows) - 1,
        context_bars=4,
    )

    validate_signal_packet(packet)
    assert packet["evidence"]["reference_price"] == packet["evidence"]["trigger_candle_close"]
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]

    decision = decide(
        {
            "signal": {
                "signal_id": "unit-test",
                "signal_set_key": "unit",
                "signal_engine_id": engine.ENGINE_ID,
                "asset": "BTC",
                "instrument": "BTC-USDT-SWAP",
                "timestamp": packet["timestamp"],
                "payload_schema": "signal_packet.v2",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
            "raw_data": {},
        }
    )
    assert decision["action"] == "ENTER"
    assert decision["direction"] in {"LONG", "SHORT"}


def _candles(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        close = 100.0 + index * 0.1
        rows.append(
            {
                "timestamp": ts.isoformat(),
                "open": close - 0.05,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 10 + index,
                "vol_ccy_quote": 1000 + index,
                "confirm": 1,
            }
        )
    return rows


def _oi_rows(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "sum_open_interest": 1000 + index,
            "sum_open_interest_value": 100000 + index,
            "count_toptrader_long_short_ratio": 1.1,
            "sum_toptrader_long_short_ratio": 1.2,
            "count_long_short_ratio": 1.0,
            "sum_taker_long_short_vol_ratio": 0.9,
            "confirm": 1,
        }
        for index in range(count)
    ]


def _metrics_rows(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "available_at": (start + timedelta(minutes=5 * (index + 1))).isoformat(),
            "top_trader_account_long_short_ratio": 1.1,
            "top_trader_position_long_short_ratio": 1.2,
            "global_account_long_short_ratio": 1.0,
            "taker_buy_sell_volume_ratio": 0.9,
            "sum_open_interest": 1000 + index,
            "sum_open_interest_value": 100000 + index,
            "complete": True,
            "confirm": 1,
        }
        for index in range(count)
    ]


def _premium_rows(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "available_at": (start + timedelta(minutes=5 * (index + 1))).isoformat(),
            "premium_open": 0.0001,
            "premium_high": 0.0002,
            "premium_low": -0.0001,
            "premium_close": 0.00005,
            "sample_count": 60,
            "complete": True,
            "confirm": 1,
        }
        for index in range(count)
    ]


def _funding_rows(count: int) -> list[dict[str, object]]:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    return [
        {
            "timestamp": (start + timedelta(minutes=5 * index)).isoformat(),
            "available_at": (start + timedelta(minutes=5 * (index + 1))).isoformat(),
            "latest_funding_rate": 0.0001,
            "funding_rate_change": 0.0,
            "annualized_funding_rate": 0.10,
            "funding_carry_1d": 0.0003,
            "funding_carry_3d": 0.0009,
            "funding_carry_7d": 0.0021,
            "funding_rate_zscore_7d": 0.2,
            "funding_signed_streak": 3,
            "funding_event_age_minutes": index * 5,
            "minutes_to_expected_funding": max(0, 480 - index * 5),
            "funding_event_is_new": index == 0,
            "complete": True,
            "confirm": 1,
        }
        for index in range(count)
    ]
