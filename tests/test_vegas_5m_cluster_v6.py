from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import json
from pathlib import Path
from statistics import mean, pstdev
import subprocess
import sys

import pytest

from quant_terminal_sdk.engine_contracts import validate_signal_engine_spec, validate_signal_packet, validate_strategy_module
from quant_terminal_worker.signal_engines.runtime import EngineLiveScanContext, EngineTrainingContext, resolve_signal_engine
from quant_terminal_worker.signal_engines.vegas_5m_cluster_v6 import (
    BOLLINGER_COLUMNS,
    calculate_provisional_bollinger,
    generate_5m_cluster_packets,
    scan_5m_cluster_at,
    scan_5m_cluster_latest,
)


PARAMETERS = {
    "context_bars": 24,
    "context_timeframes": ["2h", "8h", "12h"],
    "dedupe_window_minutes": 120,
    "proximity_threshold": "0.002",
    "vote_threshold": 3,
}


class EmptyRepository:
    def list_signal_engines(self) -> list[dict[str, object]]:
        return []


def test_v6_registry_resolves_as_a_separate_contract_engine() -> None:
    validate_signal_engine_spec("vegas_5m_cluster_v6")
    resolved = resolve_signal_engine(
        "vegas_5m_cluster_v6",
        repository=EmptyRepository(),
        workspace_root=Path.cwd(),
    )

    assert resolved.spec.signal_engine_id == "vegas_5m_cluster_v6"
    assert resolved.spec.configuration_schema["default_parameters"]["context_timeframes"] == ["2h", "8h", "12h"]
    assert resolved.spec.code_ref["base_strategy_path"].endswith("vegas_ema_5m_hft_v6_base.py")
    required = {(item["data_type"], item["origin"], item["timeframe"]) for item in resolved.spec.required_data}
    assert ("feature_bollinger", "derived", "1d") in required
    assert ("feature_open_interest_regime", "derived", "15m") in required
    assert ("open_interest", "raw", "5m") in required
    assert ("open_interest", "derived", "15m") in required
    assert {("candles", "derived", timeframe) for timeframe in ("5m", "2h", "8h", "12h", "1d")} <= required
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_provisional_bollinger_uses_population_stddev_and_exact_twenty_close_window() -> None:
    completed_closes = list(range(1, 20))
    result = calculate_provisional_bollinger(completed_closes, forming_close=20)
    expected_mid = mean(range(1, 21))
    expected_std = pstdev(range(1, 21))

    assert result["bb_mid_20"] == pytest.approx(expected_mid)
    assert result["bb_upper_20_2"] == pytest.approx(expected_mid + 2 * expected_std)
    assert result["bb_lower_20_2"] == pytest.approx(expected_mid - 2 * expected_std)
    assert result["bb_position_pct"] == pytest.approx((20 - (expected_mid - 2 * expected_std)) / (4 * expected_std) * 100)
    assert result["bb_bandwidth_pct"] == pytest.approx(4 * expected_std / expected_mid * 100)
    assert result["bb_zscore"] == pytest.approx((20 - expected_mid) / expected_std)


def test_packet_timestamp_is_confirmed_close_and_context_is_point_in_time_safe() -> None:
    data = _fixture()
    packet = _scan(data)

    assert packet is not None
    validate_signal_packet(packet)
    assert packet["timestamp"] == "2026-01-21T06:05:00Z"
    assert packet["evidence"]["signal_candle_open_ts"] == "2026-01-21T06:00:00Z"
    assert packet["evidence"]["signal_candle_close_ts"] == packet["timestamp"]
    assert packet["evidence"]["signal_available_at"] == packet["timestamp"]
    assert packet["evidence"]["reference_price"] == packet["evidence"]["trigger_candle_close"] == "20"
    oi = packet["evidence"]["derived_features"]["open_interest_regime"]
    assert oi["timeframe"] == "15m"
    assert oi["timestamp"] == "2026-01-21T05:45:00Z"
    assert oi["available_at"] == "2026-01-21T06:00:00Z"
    assert oi["complete"] is True
    assert oi["values"]["oi_return_pct_24h"] == "3"
    assert set(packet["charts"]) == {"5m", "2h", "8h", "12h", "bollinger_1d"}
    assert "1d" not in packet["charts"]

    available_at = _ts(packet["timestamp"])
    for timeframe in ("2h", "8h", "12h"):
        chart = packet["charts"][timeframe]
        columns = chart["candle_columns"]
        complete_index = columns.index("is_completed")
        partial_index = columns.index("partial_close_timestamp")
        expected_index = columns.index("expected_close_timestamp")
        for row in chart["candles"]:
            if row[complete_index]:
                assert _ts(row[expected_index]) <= available_at
            else:
                assert _ts(row[partial_index]) == available_at

    bollinger = packet["charts"]["bollinger_1d"]
    assert bollinger["columns"] == BOLLINGER_COLUMNS
    available_index = BOLLINGER_COLUMNS.index("available_at")
    assert all(_ts(row[available_index]) <= available_at for row in bollinger["rows"])
    assert bollinger["rows"][-1][BOLLINGER_COLUMNS.index("complete")] is False
    assert bollinger["rows"][-1][BOLLINGER_COLUMNS.index("source_candle_count")] == 73
    assert bollinger["rows"][-1][BOLLINGER_COLUMNS.index("close")] == "20"


def test_v6_preserves_v5_trigger_votes_for_the_same_5m_row() -> None:
    from quant_terminal_worker.signal_engines.vegas_5m_cluster_v5 import scan_5m_cluster_latest as scan_v5

    data = _fixture()
    v6_packet = _scan(data)
    v5_packet = scan_v5(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        derived_rows=data["derived_rows"],
        raw_5m_rows=data["raw_5m_rows"],
        context_rows={
            "2h": data["context_rows"]["2h"],
            "8h": data["context_rows"]["8h"],
            "1d": data["context_rows"]["12h"],
        },
        parameters={**PARAMETERS, "context_timeframes": ["2h", "8h", "1d"]},
    )

    assert v6_packet is not None
    assert v5_packet is not None
    assert v6_packet["evidence"]["matched_periods"] == v5_packet["evidence"]["matched_periods"]
    assert v6_packet["evidence"]["matched_ema_count"] == v5_packet["evidence"]["matched_ema_count"]


def test_future_mutation_and_append_leave_trigger_dedupe_context_and_packet_bytes_unchanged() -> None:
    data = _fixture()
    original = _scan_at(data)
    mutated = deepcopy(data)
    future_open = datetime(2026, 1, 21, 6, 5, tzinfo=UTC)
    mutated["derived_rows"].append(_ema_row(future_open, close=999, trigger=False))
    mutated["raw_5m_rows"].append(_raw_row(future_open, close=999))
    for timeframe in ("2h", "8h", "12h"):
        mutated["context_rows"][timeframe].append(_ema_row(future_open, close=999, trigger=True))
    mutated["daily_rows"].append(_raw_row(future_open, close=999))
    mutated["bollinger_rows"].append(_bollinger_row(future_open, close=999))
    mutated["oi_feature_rows"].append(
        _oi_feature_row(
            future_open,
            available_at=future_open + timedelta(minutes=15),
            value=999,
        )
    )

    assert json.dumps(_scan_at(mutated), separators=(",", ":")) == json.dumps(
        original,
        separators=(",", ":"),
    )


def test_training_and_live_share_identical_as_of_packet_builder_and_extension_dedupe() -> None:
    data = _fixture(include_second_trigger=True)
    trigger_open = datetime(2026, 1, 21, 6, 0, tzinfo=UTC)
    first_packets, first_count = _generate(data, start=trigger_open, end=trigger_open)
    live_data = deepcopy(data)
    live_data["derived_rows"] = live_data["derived_rows"][:1]
    live_data["raw_5m_rows"] = [row for row in live_data["raw_5m_rows"] if _row_ts(row) <= trigger_open]
    live_packet = _scan(live_data)

    assert first_count == 1
    assert first_packets == [live_packet]

    second_open = trigger_open + timedelta(minutes=30)
    extension_parameters = {**PARAMETERS, "_dedupe_seed_timestamp": first_packets[0]["timestamp"]}
    extension_packets, extension_count = generate_5m_cluster_packets(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        start=second_open,
        end=second_open,
        parameters=extension_parameters,
        **data,
    )
    full_packets, full_count = _generate(data, start=trigger_open, end=second_open)
    assert extension_packets == []
    assert extension_count == 0
    assert full_count == 1
    assert full_packets == first_packets


def test_runtime_training_and_live_adapters_preserve_the_same_complete_packet() -> None:
    data = _fixture()
    reader = FixtureReader(data)
    resolved = resolve_signal_engine(
        "vegas_5m_cluster_v6",
        repository=EmptyRepository(),
        workspace_root=Path.cwd(),
    )
    trigger_open = datetime(2026, 1, 21, 6, 0, tzinfo=UTC)
    training = resolved.generate_training_signals(
        EngineTrainingContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            signal_set={},
            signal_set_key="vegas_5m_cluster_v6:BTC:test",
            parameters=PARAMETERS,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=Path.cwd(),
            repository=EmptyRepository(),
            start=trigger_open,
            end=trigger_open,
            raw_candle_end=trigger_open,
        )
    )
    live = resolved.scan_live_signal(
        EngineLiveScanContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            route={},
            parameters=PARAMETERS,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=Path.cwd(),
            repository=EmptyRepository(),
        )
    )

    assert live.signal is not None
    assert training.packets == [live.signal.to_mapping()]
    assert json.dumps(training.packets[0], separators=(",", ":")) == json.dumps(
        live.signal.to_mapping(),
        separators=(",", ":"),
    )


def test_representative_packet_passes_consumer_audit(tmp_path: Path) -> None:
    packet = _scan(_fixture())
    assert packet is not None
    packet_path = tmp_path / "v6-packet.json"
    packet_path.write_text(json.dumps(packet))

    result = subprocess.run(
        [
            sys.executable,
            "skills/signal-engine-builder/scripts/audit_signal_packet_contract.py",
            "--packet",
            str(packet_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout
    assert json.loads(result.stdout)["status"] == "pass"


def test_required_daily_and_bollinger_dependencies_block_generation() -> None:
    data = _fixture()
    missing_daily = {**data, "daily_rows": []}
    missing_bollinger = {**data, "bollinger_rows": []}
    missing_oi = {**data, "oi_feature_rows": []}

    with pytest.raises(ValueError, match="derived 1d candle"):
        _scan(missing_daily)
    with pytest.raises(ValueError, match="feature_bollinger"):
        _scan(missing_bollinger)
    with pytest.raises(ValueError, match="feature_open_interest_regime"):
        _scan(missing_oi)


def test_strategy_requires_12h_and_derived_features_are_diagnostic_only() -> None:
    from quant_terminal_strategies import vegas_ema_5m_hft_v6_base as strategy

    packet = _scan(_fixture())
    assert packet is not None
    context = _strategy_context(packet)
    baseline = strategy.decide(context)
    changed_packet = deepcopy(packet)
    for row in changed_packet["charts"]["bollinger_1d"]["rows"]:
        for column in ("bb_mid_20", "bb_upper_20_2", "bb_lower_20_2", "bb_position_pct", "bb_bandwidth_pct", "bb_zscore"):
            row[BOLLINGER_COLUMNS.index(column)] = "999999"
    changed = strategy.decide(_strategy_context(changed_packet))

    oi_values = changed_packet["evidence"]["derived_features"]["open_interest_regime"]["values"]
    for column in oi_values:
        oi_values[column] = "999999"
    changed_oi = strategy.decide(_strategy_context(changed_packet))

    assert (changed["action"], changed["direction"], changed["reason_code"]) == (
        baseline["action"], baseline["direction"], baseline["reason_code"]
    )
    assert changed["diagnostics"]["bollinger_1d"] != baseline["diagnostics"]["bollinger_1d"]
    assert (changed_oi["action"], changed_oi["direction"], changed_oi["reason_code"]) == (
        baseline["action"], baseline["direction"], baseline["reason_code"]
    )
    assert changed_oi["diagnostics"]["open_interest_regime"] != baseline["diagnostics"]["open_interest_regime"]

    missing_12h = deepcopy(packet)
    missing_12h["charts"].pop("12h")
    missing_12h["charts"]["1d"] = deepcopy(packet["charts"]["8h"])
    decision = strategy.decide(_strategy_context(missing_12h))
    assert decision["reason_code"] == "missing_required_5m_2h_8h_or_12h_context"


def _scan(data: dict[str, object]) -> dict[str, object] | None:
    return scan_5m_cluster_latest(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        parameters=PARAMETERS,
        **data,
    )


def _scan_at(data: dict[str, object]) -> dict[str, object] | None:
    return scan_5m_cluster_at(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        timestamp=datetime(2026, 1, 21, 6, 0, tzinfo=UTC),
        parameters=PARAMETERS,
        **data,
    )


def _generate(data: dict[str, object], *, start: datetime, end: datetime) -> tuple[list[dict[str, object]], int]:
    return generate_5m_cluster_packets(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        start=start,
        end=end,
        parameters=PARAMETERS,
        **data,
    )


def _fixture(*, include_second_trigger: bool = False) -> dict[str, object]:
    day = datetime(2026, 1, 21, tzinfo=UTC)
    trigger_open = day + timedelta(hours=6)
    raw_rows = [_raw_row(day + timedelta(minutes=5 * index), close=20) for index in range(73)]
    derived_rows = [_ema_row(trigger_open, close=20, trigger=True)]
    if include_second_trigger:
        second_open = trigger_open + timedelta(minutes=30)
        raw_rows.extend(_raw_row(trigger_open + timedelta(minutes=5 * index), close=20) for index in range(1, 7))
        derived_rows.append(_ema_row(second_open, close=20, trigger=True))

    daily_start = datetime(2026, 1, 1, tzinfo=UTC)
    daily_rows = [_raw_row(daily_start + timedelta(days=index), close=index + 1) for index in range(21)]
    bollinger_rows = [_bollinger_row(daily_start + timedelta(days=index), close=index + 1) for index in range(20)]
    context_rows = {
        timeframe: [
            _ema_row(daily_start, close=10, trigger=True),
            _ema_row(trigger_open - _delta(timeframe), close=19, trigger=True),
        ]
        for timeframe in ("2h", "8h", "12h")
    }
    return {
        "derived_rows": derived_rows,
        "raw_5m_rows": raw_rows,
        "context_rows": context_rows,
        "daily_rows": daily_rows,
        "bollinger_rows": bollinger_rows,
        "oi_feature_rows": [
            _oi_feature_row(
                trigger_open - timedelta(minutes=15),
                available_at=trigger_open,
                value=3,
            ),
            _oi_feature_row(
                trigger_open,
                available_at=trigger_open + timedelta(minutes=15),
                value=999,
            ),
        ],
    }


def _raw_row(timestamp: datetime, *, close: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": close,
        "high": close + 1,
        "low": close - 1,
        "close": close,
        "volume": 1,
        "vol_ccy": 1,
        "vol_ccy_quote": 1,
        "confirm": 1,
    }


def _ema_row(timestamp: datetime, *, close: float, trigger: bool) -> dict[str, object]:
    row = _raw_row(timestamp, close=close)
    for period in (36, 43, 144, 169, 576, 676):
        row[f"ema_{period}"] = close if trigger else close * 2
        row[f"ema_warmup_count_{period}"] = period
    return row


def _bollinger_row(timestamp: datetime, *, close: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "bb_mid_20": close,
        "bb_upper_20_2": close + 2,
        "bb_lower_20_2": close - 2,
        "bb_position_pct": 50,
        "bb_bandwidth_pct": 20,
        "bb_zscore": 0,
        "confirm": 1,
    }


def _oi_feature_row(timestamp: datetime, *, available_at: datetime, value: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "available_at": available_at,
        "complete": True,
        "source_window_start_ts": timestamp - timedelta(days=7),
        "source_window_end_ts": timestamp,
        "source_row_count": 681,
        "oi_return_pct_2h": value,
        "oi_return_pct_8h": value,
        "oi_return_pct_24h": value,
        "oi_change_2h_zscore_7d": value,
        "general_long_short_ratio": value,
        "taker_long_short_ratio_avg_2h": value,
    }


def _strategy_context(packet: dict[str, object]) -> dict[str, object]:
    return {
        "signal": {
            "signal_id": "vegas_5m_cluster_v6:BTC:test:20260121T060500Z",
            "signal_engine_id": "vegas_5m_cluster_v6",
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


def _row_ts(row: dict[str, object]) -> datetime:
    value = row["timestamp"]
    assert isinstance(value, datetime)
    return value


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _delta(timeframe: str) -> timedelta:
    return timedelta(hours=int(timeframe[:-1]))


class FixtureReader:
    def __init__(self, data: dict[str, object]) -> None:
        self.data = data

    def get_candles(self, **kwargs: object) -> list[dict[str, object]]:
        assert kwargs["timeframe"] == "5m"
        assert kwargs["origin"] == "raw"
        return list(self.data["raw_5m_rows"])

    def get_rows(self, **kwargs: object) -> list[dict[str, object]]:
        timeframe = str(kwargs["timeframe"])
        if kwargs.get("data_type") == "feature_bollinger":
            return list(self.data["bollinger_rows"])
        if kwargs.get("data_type") == "feature_open_interest_regime":
            return list(self.data["oi_feature_rows"])
        if timeframe == "5m":
            return list(self.data["derived_rows"])
        if timeframe == "1d":
            return list(self.data["daily_rows"])
        return list(self.data["context_rows"][timeframe])
