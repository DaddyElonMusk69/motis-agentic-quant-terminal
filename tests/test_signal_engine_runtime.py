from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from sqlalchemy import create_engine, insert

from quant_terminal_api.db.models import data_sources, market_data_refs, metadata
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_sdk.engine_contracts import (
    ContractValidationError,
    validate_signal_engine_spec,
    validate_signal_packet,
    validate_strategy_module,
)
from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.execution.live_signal_scan import scan_latest_live_signal
from quant_terminal_worker.execution.wake_runner import run_route_wake
from quant_terminal_worker.ingestion.legacy_signals import build_signal_set_key
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles
from quant_terminal_worker.signal_engines import vegas_ema
from quant_terminal_worker.signal_engines.vegas_ema_5m_cluster import generate_training_signals as generate_5m_cluster_training_signals
from quant_terminal_worker.signal_engines.vegas_ema_recursive import recursive_ema_update
from quant_terminal_worker.signal_engines.vegas_ema_recursive import generate_training_signals as generate_recursive_training_signals
from quant_terminal_worker.signal_engines.vegas_ema_recursive_features import generate_training_signals as generate_recursive_feature_training_signals
from quant_terminal_worker.signal_engines.runtime import (
    EngineTrainingContext,
    apply_fixed_engine_parameters,
    resolve_signal_engine,
)


def test_fixed_engine_parameters_override_runtime_values() -> None:
    class FakeSpec:
        configuration_schema = {
            "default_parameters": {"dedupe_window_minutes": 480},
            "fixed_parameters": {"dedupe_window_minutes": 240},
        }

    assert apply_fixed_engine_parameters(
        FakeSpec(),
        {"dedupe_window_minutes": 0, "context_bars": 96},
    ) == {
        "dedupe_window_minutes": 240,
        "context_bars": 96,
    }


def test_resolve_signal_engine_prefers_canonical_db_spec(tmp_path: Path):
    repository = _repository()
    _register_threshold_engine(repository)

    resolved = resolve_signal_engine(
        "threshold_reversal",
        version="0.1.0",
        repository=repository,
        workspace_root=tmp_path,
    )

    assert resolved.spec.signal_engine_id == "threshold_reversal"
    assert resolved.spec.runtime_entrypoint == "quant_terminal_engines.threshold_reversal:generate_training_signals"
    assert resolved.spec.live_scanner_entrypoint == "quant_terminal_engines.threshold_reversal:scan_live_signal"


def test_resolve_signal_engine_rejects_invalid_required_data(tmp_path: Path):
    repository = _repository()
    repository.register_signal_engine(
        {
            "signal_engine_id": "bad_engine",
            "name": "Bad Engine",
            "description": "",
            "version": "0.1.0",
            "code_ref": {},
            "supported_input_data_types": ["orderbook"],
            "required_data": [{"data_type": "orderbook", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_engines.threshold_reversal:scan_live_signal",
            "configuration_schema": {},
        }
    )

    with pytest.raises(ContractValidationError, match="unsupported required data type: orderbook"):
        resolve_signal_engine("bad_engine", repository=repository, workspace_root=tmp_path)


def test_vegas_vote1_resolves_as_separate_engine_with_vote_threshold_one(tmp_path: Path, monkeypatch):
    root, repository = _workspace_with_vegas_pool(
        tmp_path,
        signal_engine_id="vegas_ema_vote1",
        vote_threshold=1,
        include_manifest_vote_threshold=False,
    )
    _register_default_vegas_refs(
        repository,
        root=root,
        asset="AAVE",
        rows=[
            _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
            _candle_row("2026-06-01T00:05:00Z", open_=100, close=101),
        ],
    )
    calls = []

    def fake_generate_vegas_packets(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(vegas_ema, "generate_vegas_packets", fake_generate_vegas_packets)

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema_vote1",
        asset="AAVE",
        target_end="2026-06-01T00:05:00Z",
    )

    resolved = resolve_signal_engine("vegas_ema_vote1", repository=repository, workspace_root=root)
    assert resolved.spec.signal_engine_id == "vegas_ema_vote1"
    assert result["status"] == "no_new_signals"
    assert calls[0]["vote_threshold"] == 1


def test_vegas_vote1_resolves_from_artifact_registry(tmp_path: Path):
    repository = _repository()

    resolved = resolve_signal_engine("vegas_ema_vote1", repository=repository, workspace_root=Path.cwd())

    assert resolved.spec.signal_engine_id == "vegas_ema_vote1"
    assert resolved.spec.name == "Vegas 1 Vote"
    assert resolved.spec.configuration_schema["default_parameters"]["vote_threshold"] == 1
    assert resolved.spec.runtime_entrypoint == "quant_terminal_worker.signal_engines.vegas_ema:generate_training_signals"


def test_existing_vegas_engine_remains_vote_threshold_two(tmp_path: Path, monkeypatch):
    root, repository = _workspace_with_vegas_pool(tmp_path, signal_engine_id="vegas_ema", vote_threshold=2)
    _register_default_vegas_refs(
        repository,
        root=root,
        asset="AAVE",
        rows=[
            _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
            _candle_row("2026-06-01T00:05:00Z", open_=100, close=101),
        ],
    )
    calls = []

    monkeypatch.setattr(vegas_ema, "generate_vegas_packets", lambda **kwargs: calls.append(kwargs) or [])

    extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema",
        asset="AAVE",
        target_end="2026-06-01T00:05:00Z",
    )

    assert calls[0]["vote_threshold"] == 2


def test_generic_training_dispatch_extends_non_vegas_signal_pool_from_parquet(tmp_path: Path):
    root, repository = _workspace_with_threshold_pool(tmp_path)
    _register_candle_ref(
        repository,
        root=root,
        asset="SOL",
        timeframe="5m",
        origin="raw",
        rows=[
            _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
            _candle_row("2026-06-01T00:05:00Z", open_=100, close=103),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="threshold_reversal",
        asset="SOL",
        target_end="2026-06-01T00:05:00Z",
    )

    assert result["status"] == "extended"
    assert result["signal_engine_id"] == "threshold_reversal"
    assert result["appended_packet_count"] == 1
    signals = repository.list_signals(signal_set_key=build_signal_set_key("threshold_reversal", "SOL", "SOL-threshold_reversal-canonical"))
    assert signals[0]["payload"]["evidence"]["neutral_trigger"] == "lookback_move_exceeded"
    assert "direction" not in signals[0]["payload"]


def test_bollinger_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("bollinger")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/bollinger_base.py")


def test_bollinger_training_dispatch_extends_signal_pool_from_parquet(tmp_path: Path):
    root, repository = _workspace_with_bollinger_pool(tmp_path)
    _register_bollinger_refs(repository, root=root, asset="AAVE")

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="bollinger",
        asset="AAVE",
        target_end="2026-06-01T04:05:00Z",
    )

    assert result["status"] == "extended"
    assert result["signal_engine_id"] == "bollinger"
    assert result["appended_packet_count"] == 2
    signals = repository.list_signals(signal_set_key=build_signal_set_key("bollinger", "AAVE", "AAVE-bollinger-canonical"))
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert packet["evidence"]["pattern"] == "bollinger_band_proximity"
    assert packet["evidence"]["vote_threshold"] == 1
    assert packet["evidence"]["bb_period"] == 2
    assert packet["evidence"]["interactions"][0]["band"] == "upper"
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]


def test_bollinger_live_scan_scans_latest_parquet_candle_only(tmp_path: Path):
    root, repository = _workspace_with_bollinger_pool(tmp_path)
    _register_bollinger_refs(repository, root=root, asset="AAVE")
    route = {
        **_route(root),
        "route_id": "aave-live",
        "signal_engine_id": "bollinger",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "bollinger"
    assert signal["payload_schema"] == "signal_packet.v2"
    assert signal["payload"]["evidence"]["pattern"] == "bollinger_band_proximity"
    assert signal["payload"]["evidence"]["active_timeframes"] == ["4h"]


def test_recursive_vegas_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_ema_recursive")
    validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py")


def test_recursive_vegas_updates_completed_ema_with_active_candle():
    value = recursive_ema_update("100", 3, "110")

    assert value == 105


def test_recursive_vegas_training_dispatch_uses_enriched_ema_rows(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_pool(tmp_path)
    _register_recursive_vegas_refs(repository, root=root, asset="AAVE")

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema_recursive",
        asset="AAVE",
        target_end="2026-06-01T00:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_ema_recursive", "AAVE", "AAVE-vegas_ema_recursive-canonical"))
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert packet["evidence"]["ema_mode"] == "recursive_precomputed_completed_htf_plus_active_candle"
    assert packet["interactions"][0]["timeframe"] == "2h"
    assert "charts" in packet
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]


def test_recursive_vegas_live_scan_preserves_training_packet_shape(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_pool(tmp_path)
    _register_recursive_vegas_refs(repository, root=root, asset="AAVE")
    route = {
        **_route(root),
        "route_id": "aave-live",
        "signal_engine_id": "vegas_ema_recursive",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "active_bundle": {
            "execution_setup": {
                "engine_parameters": {
                    "timeframes": ["2h"],
                    "context_bars": 1,
                    "vote_threshold": 1,
                    "proximity_threshold": "0.01",
                }
            }
        },
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "vegas_ema_recursive"
    assert signal["payload"]["evidence"]["pattern"] == "vegas_ema_tunnel_proximity"
    assert signal["payload"]["interactions"][0]["timeframe"] == "2h"
    assert "charts" in signal["payload"]


def test_recursive_vegas_training_can_stream_packets_in_chunks(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_pool(tmp_path)
    _register_recursive_vegas_refs(repository, root=root, asset="AAVE")
    reader = MarketDataReader(repository=repository, workspace_root=root)
    streamed_chunks = []

    output = generate_recursive_training_signals(
        EngineTrainingContext(
            asset="AAVE",
            instrument="AAVE-USDT-SWAP",
            signal_set=repository.get_signal_set(build_signal_set_key("vegas_ema_recursive", "AAVE", "AAVE-vegas_ema_recursive-canonical")),
            signal_set_key=build_signal_set_key("vegas_ema_recursive", "AAVE", "AAVE-vegas_ema_recursive-canonical"),
            parameters={"timeframes": ["2h"], "context_bars": 1, "vote_threshold": 1, "proximity_threshold": "0.01"},
            market_data_reader=reader,
            spec=resolve_signal_engine("vegas_ema_recursive", repository=repository, workspace_root=root).spec,
            workspace_root=root,
            repository=repository,
            start=datetime.fromisoformat("2026-06-01T00:00:00+00:00"),
            end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
            raw_candle_end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
            packet_sink=lambda packets: streamed_chunks.append(list(packets)),
            packet_chunk_size=1,
        )
    )

    assert output.result.generated_packet_count == 1
    assert output.packets == []
    assert len(streamed_chunks) == 1


def test_recursive_vegas_features_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_ema_recursive_features")
    resolved = resolve_signal_engine("vegas_ema_recursive_features", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_recursive_features_base.py"
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_recursive_vegas_features_training_emits_compact_feature_windows(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_features_pool(tmp_path)
    _register_recursive_vegas_feature_refs(repository, root=root, asset="AAVE")
    refresh_calls = []
    original_refresh = repository.refresh_signal_set_coverage

    def capture_refresh(signal_set_key: str) -> None:
        refresh_calls.append(signal_set_key)
        original_refresh(signal_set_key)

    repository.refresh_signal_set_coverage = capture_refresh

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema_recursive_features",
        asset="AAVE",
        target_end="2026-06-01T00:05:00Z",
    )

    assert result["status"] == "extended"
    signal_set_key = build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical")
    assert refresh_calls == [signal_set_key]
    signals = repository.list_signals(signal_set_key=signal_set_key)
    refreshed_pool = repository.get_signal_set(signal_set_key)
    assert refreshed_pool["packet_count"] == 1
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert packet["evidence"]["pattern"] == "vegas_ema_5m_cluster_proximity"
    assert packet["evidence"]["ema_mode"] == "precomputed_5m_ema_cluster"
    assert packet["evidence"]["vote_threshold"] == 3
    assert packet["evidence"]["matched_periods"] == [36, 43, 144]
    assert set(packet["charts"]) == {"5m", "2h", "1d"}
    assert packet["evidence"]["features"]["5m"]["latest"]["base_candle"]["return_pct"] == 0.2
    assert packet["features"] == packet["evidence"]["features"]
    assert len(packet["evidence"]["features"]["5m"]["window"]) == 2
    assert len(packet["evidence"]["features"]["2h"]["window"]) == 1
    assert len(packet["evidence"]["features"]["1d"]["window"]) == 1
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]
    assert "direction" not in json.dumps(packet["evidence"]["features"])


def test_recursive_vegas_features_live_scan_preserves_training_feature_shape(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_features_pool(tmp_path)
    _register_recursive_vegas_feature_refs(repository, root=root, asset="AAVE")
    route = {
        **_route(root),
        "route_id": "aave-live",
        "signal_engine_id": "vegas_ema_recursive_features",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "active_bundle": {
            "execution_setup": {
                "engine_parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "context_timeframes": ["2h", "1d"],
                    "feature_timeframes": ["5m", "2h", "1d"],
                    "feature_window_bars": {"5m": 24, "2h": 12, "1d": 10},
                }
            }
        },
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "vegas_ema_recursive_features"
    assert signal["payload"]["evidence"]["pattern"] == "vegas_ema_5m_cluster_proximity"
    assert signal["payload"]["interactions"][0]["timeframe"] == "5m"
    assert set(signal["payload"]["charts"]) == {"5m", "2h", "1d"}
    assert set(signal["payload"]["evidence"]["features"]) == {"5m", "2h", "1d"}
    assert signal["payload"]["features"] == signal["payload"]["evidence"]["features"]


def test_recursive_vegas_features_matches_5m_cluster_signal_shape(tmp_path: Path):
    old_root, old_repository = _workspace_with_5m_cluster_pool(tmp_path)
    _register_5m_cluster_refs(old_repository, root=old_root, asset="AAVE")
    feature_root, feature_repository = _workspace_with_recursive_vegas_features_pool(tmp_path)
    _register_recursive_vegas_feature_refs(feature_repository, root=feature_root, asset="AAVE")

    old_reader = MarketDataReader(repository=old_repository, workspace_root=old_root)
    feature_reader = MarketDataReader(repository=feature_repository, workspace_root=feature_root)
    start = datetime.fromisoformat("2026-06-01T00:00:00+00:00")
    end = datetime.fromisoformat("2026-06-01T00:05:00+00:00")
    old_output = generate_5m_cluster_training_signals(
        EngineTrainingContext(
            asset="AAVE",
            instrument="AAVE-USDT-SWAP",
            signal_set=old_repository.get_signal_set(build_signal_set_key("vegas_ema_5m_cluster", "AAVE", "AAVE-vegas_ema_5m_cluster-canonical")),
            signal_set_key=build_signal_set_key("vegas_ema_5m_cluster", "AAVE", "AAVE-vegas_ema_5m_cluster-canonical"),
            parameters={"context_bars": 2, "vote_threshold": 3, "proximity_threshold": "0.002"},
            market_data_reader=old_reader,
            spec=resolve_signal_engine("vegas_ema_5m_cluster", repository=old_repository, workspace_root=old_root).spec,
            workspace_root=old_root,
            repository=old_repository,
            start=start,
            end=end,
            raw_candle_end=end,
        )
    )
    feature_output = generate_recursive_feature_training_signals(
        EngineTrainingContext(
            asset="AAVE",
            instrument="AAVE-USDT-SWAP",
            signal_set=feature_repository.get_signal_set(build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical")),
            signal_set_key=build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical"),
            parameters={"context_bars": 2, "vote_threshold": 3, "proximity_threshold": "0.002"},
            market_data_reader=feature_reader,
            spec=resolve_signal_engine("vegas_ema_recursive_features", repository=feature_repository, workspace_root=feature_root).spec,
            workspace_root=feature_root,
            repository=feature_repository,
            start=start,
            end=end,
            raw_candle_end=end,
        )
    )

    assert old_output.result.generated_packet_count == feature_output.result.generated_packet_count == 1
    old_packet = old_output.packets[0]
    feature_packet = feature_output.packets[0]
    assert feature_packet["timestamp"] == old_packet["timestamp"]
    assert feature_packet["interactions"] == old_packet["interactions"]
    assert feature_packet["evidence"]["matched_periods"] == old_packet["evidence"]["matched_periods"]
    assert feature_packet["charts"] == old_packet["charts"]
    assert feature_packet["features"]["5m"]["latest"]["base_candle"]
    assert feature_packet["features"]["5m"]["latest"]["volatility_range"]
    assert feature_packet["features"]["5m"]["latest"]["volume"]
    assert feature_packet["features"]["5m"]["latest"]["ema_vegas_structure"]
    assert feature_packet["features"]["5m"]["latest"]["bollinger"]
    assert feature_packet["features"]["5m"]["latest"]["regime_momentum"]
    assert feature_packet["features"]["5m"]["window"]


def test_recursive_vegas_features_missing_feature_data_is_explicit(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_features_pool(tmp_path)
    _register_5m_cluster_refs(repository, root=root, asset="AAVE")
    reader = MarketDataReader(repository=repository, workspace_root=root)

    with pytest.raises(ValueError, match="feature_base_candle.*AAVE 5m"):
        generate_recursive_feature_training_signals(
            EngineTrainingContext(
                asset="AAVE",
                instrument="AAVE-USDT-SWAP",
                signal_set=repository.get_signal_set(build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical")),
                signal_set_key=build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical"),
                parameters={"context_bars": 2, "vote_threshold": 3, "proximity_threshold": "0.002"},
                market_data_reader=reader,
                spec=resolve_signal_engine("vegas_ema_recursive_features", repository=repository, workspace_root=root).spec,
                workspace_root=root,
                repository=repository,
                start=datetime.fromisoformat("2026-06-01T00:00:00+00:00"),
                end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
                raw_candle_end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
            )
        )


def test_recursive_vegas_features_reuses_feature_timestamp_indexes():
    from quant_terminal_worker.signal_engines import vegas_ema_recursive_features as engine

    class CountingRow(dict):
        timestamp_reads = 0

        def __getitem__(self, key):
            if key == "timestamp":
                type(self).timestamp_reads += 1
            return super().__getitem__(key)

    rows = [
        CountingRow(timestamp=datetime(2026, 6, 1, 0, minute, tzinfo=UTC), feature_value=minute)
        for minute in range(20)
    ]
    feature_rows = {
        timeframe: {family: rows for family in engine.FEATURE_FAMILIES}
        for timeframe in ("5m", "2h", "1d")
    }

    indexed = engine._prepare_feature_indexes(feature_rows)
    index_build_reads = CountingRow.timestamp_reads
    CountingRow.timestamp_reads = 0
    engine._feature_snapshot(
        feature_rows=indexed,
        signal_timestamp=datetime(2026, 6, 1, 0, 19, tzinfo=UTC),
        parameters={},
    )
    first_snapshot_reads = CountingRow.timestamp_reads
    engine._feature_snapshot(
        feature_rows=indexed,
        signal_timestamp=datetime(2026, 6, 1, 0, 19, tzinfo=UTC),
        parameters={},
    )

    assert index_build_reads == 360
    assert first_snapshot_reads == 0
    assert CountingRow.timestamp_reads == first_snapshot_reads


def test_recursive_vegas_features_reuses_context_timestamp_indexes(monkeypatch):
    from quant_terminal_worker.signal_engines import vegas_ema_recursive_features as engine

    rows = [
        {
            "timestamp": datetime(2026, 6, 1, hour, tzinfo=UTC),
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 1,
            "vol_ccy": 1,
            "vol_ccy_quote": 1,
            "confirm": 1,
            "ema_36": 100,
            "ema_warmup_count_36": 36,
            "ema_43": 100,
            "ema_warmup_count_43": 43,
            "ema_144": 100,
            "ema_warmup_count_144": 144,
            "ema_169": 100,
            "ema_warmup_count_169": 169,
            "ema_576": 100,
            "ema_warmup_count_576": 576,
            "ema_676": 100,
            "ema_warmup_count_676": 676,
        }
        for hour in range(20)
    ]
    context_rows = {"2h": rows, "1d": rows}

    indexed = engine._prepare_context_rows(asset="AAVE", context_rows=context_rows, parameters={"context_timeframes": ["2h", "1d"]})
    timestamp_ids = {timeframe: id(indexed[timeframe]["timestamps"]) for timeframe in ("2h", "1d")}
    bisect_inputs: list[int] = []

    def counting_bisect_right(timestamps, timestamp):
        bisect_inputs.append(id(timestamps))
        return len(timestamps)

    monkeypatch.setattr(engine, "bisect_right", counting_bisect_right)

    engine._context_charts(
        rows_by_timeframe=indexed,
        signal_timestamp=datetime(2026, 6, 1, 19, tzinfo=UTC),
        context_bars=2,
        context_timeframes=["2h", "1d"],
    )
    engine._context_charts(
        rows_by_timeframe=indexed,
        signal_timestamp=datetime(2026, 6, 1, 19, tzinfo=UTC),
        context_bars=2,
        context_timeframes=["2h", "1d"],
    )

    assert bisect_inputs == [
        timestamp_ids["2h"],
        timestamp_ids["1d"],
        timestamp_ids["2h"],
        timestamp_ids["1d"],
    ]


def test_recursive_vegas_features_training_can_stream_packets_in_chunks(tmp_path: Path):
    root, repository = _workspace_with_recursive_vegas_features_pool(tmp_path)
    _register_recursive_vegas_feature_refs(repository, root=root, asset="AAVE")
    reader = MarketDataReader(repository=repository, workspace_root=root)
    streamed_chunks = []

    output = generate_recursive_feature_training_signals(
        EngineTrainingContext(
            asset="AAVE",
            instrument="AAVE-USDT-SWAP",
            signal_set=repository.get_signal_set(build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical")),
            signal_set_key=build_signal_set_key("vegas_ema_recursive_features", "AAVE", "AAVE-vegas_ema_recursive_features-canonical"),
            parameters={"context_bars": 2, "vote_threshold": 3, "proximity_threshold": "0.002"},
            market_data_reader=reader,
            spec=resolve_signal_engine("vegas_ema_recursive_features", repository=repository, workspace_root=root).spec,
            workspace_root=root,
            repository=repository,
            start=datetime.fromisoformat("2026-06-01T00:00:00+00:00"),
            end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
            raw_candle_end=datetime.fromisoformat("2026-06-01T00:05:00+00:00"),
            packet_sink=lambda packets: streamed_chunks.append(list(packets)),
            packet_chunk_size=1,
        )
    )

    assert output.result.generated_packet_count == 1
    assert output.packets == []
    assert streamed_chunks[0][0]["evidence"]["features"]["5m"]["latest"]["base_candle"]["return_pct"] == 0.2


def test_liquidity_sweep_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("liquidity_sweep_v1")
    resolved = resolve_signal_engine("liquidity_sweep_v1", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.required_data == [
        {
            "data_type": "candles",
            "origin": "raw",
            "timeframe": "5m",
            "lookback_bars": 20000,
            "freshness_tolerance_seconds": 300,
        }
    ]
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/liquidity_sweep_v1_base.py"
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_liquidity_sweep_training_detects_high_and_low_sweeps(tmp_path: Path):
    from quant_terminal_worker.signal_engines.liquidity_sweep_v1 import generate_training_signals

    root, repository = _workspace_with_liquidity_sweep_pool(tmp_path)
    rows = _liquidity_reference_rows()
    rows.append(_ohlcv_row("2026-06-04T00:00:00Z", open_=100, high=108, low=98, close=101))
    rows.extend(_liquidity_reference_rows(start="2026-06-04T12:05:00Z", count=144))
    rows.append(_ohlcv_row("2026-06-05T00:05:00Z", open_=100, high=102, low=87, close=99))
    _register_candle_ref(repository, root=root, asset="AAVE", timeframe="5m", origin="raw", rows=rows)
    reader = MarketDataReader(repository=repository, workspace_root=root)

    output = generate_training_signals(
        EngineTrainingContext(
            asset="AAVE",
            instrument="AAVE-USDT-SWAP",
            signal_set=repository.get_signal_set(build_signal_set_key("liquidity_sweep_v1", "AAVE", "AAVE-liquidity_sweep_v1-canonical")),
            signal_set_key=build_signal_set_key("liquidity_sweep_v1", "AAVE", "AAVE-liquidity_sweep_v1-canonical"),
            parameters={"reference_window_hours": 72, "atr_period": 14, "min_sweep_atr": 0.2, "cooldown_hours": 12},
            market_data_reader=reader,
            spec=resolve_signal_engine("liquidity_sweep_v1", repository=repository, workspace_root=root).spec,
            workspace_root=root,
            repository=repository,
            start=datetime.fromisoformat("2026-06-04T00:00:00+00:00"),
            end=datetime.fromisoformat("2026-06-05T00:05:00+00:00"),
            raw_candle_end=datetime.fromisoformat("2026-06-05T00:05:00+00:00"),
        )
    )

    assert output.result.generated_packet_count == 2
    assert [packet["evidence"]["event_type"] for packet in output.packets] == ["HIGH_SWEEP", "LOW_SWEEP"]
    high_packet = output.packets[0]
    validate_signal_packet(high_packet)
    assert high_packet["schema_version"] == "signal_packet.v2"
    assert high_packet["active_timeframes"] == ["5m"]
    assert high_packet["evidence"]["pattern"] == "liquidity_sweep_event"
    assert high_packet["evidence"]["reference_window_hours"] == 72
    assert high_packet["evidence"]["reference_level"] == "105"
    assert high_packet["evidence"]["trigger_price"] == "108"
    assert high_packet["evidence"]["atr_14"] == "10"
    assert high_packet["evidence"]["sweep_distance"] == "3"
    assert high_packet["evidence"]["sweep_distance_atr"] == "0.3"
    assert high_packet["charts"]["5m"]["role"] == "trigger_context"
    assert "direction" not in json.dumps(high_packet)
    assert "side" not in json.dumps(high_packet)


def test_liquidity_sweep_training_ignores_microscopic_breach_and_respects_cooldown(tmp_path: Path):
    root, repository = _workspace_with_liquidity_sweep_pool(tmp_path)
    rows = _liquidity_reference_rows()
    rows.append(_ohlcv_row("2026-06-04T00:00:00Z", open_=100, high=106, low=98, close=101))
    rows.append(_ohlcv_row("2026-06-04T00:05:00Z", open_=100, high=109, low=98, close=101))
    rows.append(_ohlcv_row("2026-06-04T06:00:00Z", open_=100, high=112, low=98, close=101))
    _register_candle_ref(repository, root=root, asset="AAVE", timeframe="5m", origin="raw", rows=rows)

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="liquidity_sweep_v1",
        asset="AAVE",
        target_end="2026-06-04T06:00:00Z",
    )

    assert result["status"] == "extended"
    assert result["appended_packet_count"] == 2
    signals = repository.list_signals(signal_set_key=build_signal_set_key("liquidity_sweep_v1", "AAVE", "AAVE-liquidity_sweep_v1-canonical"))
    assert [signal["timestamp"].replace(tzinfo=UTC) for signal in signals] == [
        datetime.fromisoformat("2026-06-04T00:05:00+00:00"),
        datetime.fromisoformat("2026-06-04T06:00:00+00:00"),
    ]
    assert [signal["payload"]["evidence"]["event_type"] for signal in signals] == ["HIGH_SWEEP", "HIGH_SWEEP"]


def test_liquidity_sweep_live_scan_scans_latest_confirmed_raw_5m_candle_only(tmp_path: Path):
    root, repository = _workspace_with_liquidity_sweep_pool(tmp_path)
    rows = _liquidity_reference_rows()
    rows.append(_ohlcv_row("2026-06-04T00:00:00Z", open_=100, high=108, low=98, close=101))
    _register_candle_ref(repository, root=root, asset="AAVE", timeframe="5m", origin="raw", rows=rows)
    route = {
        **_route(root),
        "route_id": "aave-lse-live",
        "signal_engine_id": "liquidity_sweep_v1",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "liquidity_sweep_v1"
    assert signal["payload"]["timestamp"] == "2026-06-04T00:00:00Z"
    assert signal["payload"]["evidence"]["event_type"] == "HIGH_SWEEP"

    root_no_signal, repository_no_signal = _workspace_with_liquidity_sweep_pool(tmp_path / "no-signal")
    rows_no_signal = _liquidity_reference_rows()
    rows_no_signal.append(_ohlcv_row("2026-06-04T00:00:00Z", open_=100, high=104, low=96, close=100))
    _register_candle_ref(repository_no_signal, root=root_no_signal, asset="AAVE", timeframe="5m", origin="raw", rows=rows_no_signal)
    no_signal = scan_latest_live_signal(route=route, repository=repository_no_signal, workspace_root=root_no_signal)
    assert no_signal is None


def test_liquidity_sweep_training_uses_bounded_reference_window(monkeypatch):
    from quant_terminal_sdk.market_data_reader import MarketDataCandle
    from quant_terminal_worker.signal_engines import liquidity_sweep_v1 as engine

    rows = [
        MarketDataCandle(
            timestamp=datetime(2026, 6, 1, tzinfo=UTC) + timedelta(minutes=5 * index),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=1,
            vol_ccy=1,
            vol_ccy_quote=1,
            confirm=1,
        )
        for index in range(1_000)
    ]
    observed_history_sizes = []

    def capture_scan_row(*, rows, index, history_start_index, **kwargs):
        del kwargs
        observed_history_sizes.append(index - history_start_index)
        return None

    monkeypatch.setattr(engine, "_scan_row", capture_scan_row)

    engine.generate_liquidity_sweep_packets(
        workspace_root=Path.cwd(),
        asset="AAVE",
        instrument="AAVE-USDT-SWAP",
        raw_5m=rows,
        start=rows[0].timestamp,
        end=rows[-1].timestamp,
        parameters={"reference_window_hours": 12, "atr_period": 14, "min_sweep_atr": 0.2, "cooldown_hours": 2},
    )

    assert max(observed_history_sizes) <= 144


def test_5m_cluster_vegas_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_ema_5m_cluster")
    resolved = resolve_signal_engine("vegas_ema_5m_cluster", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_base.py"
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v2_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_5m_cluster_v2")
    resolved = resolve_signal_engine("vegas_5m_cluster_v2", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_base.py"
    assert resolved.spec.configuration_schema["default_parameters"]["context_mode"] == "closed_htf_only"
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v3_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_5m_cluster_v3")
    resolved = resolve_signal_engine("vegas_5m_cluster_v3", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v3_base.py"
    assert resolved.spec.configuration_schema["default_parameters"]["context_mode"] == "closed_htf_only"
    assert resolved.spec.configuration_schema["default_parameters"]["context_timeframes"] == ["2h", "8h", "1d"]
    required_timeframes = {
        item["timeframe"]
        for item in resolved.spec.required_data
        if item["data_type"] == "candles" and item["origin"] == "derived"
    }
    assert {"5m", "2h", "8h", "1d"}.issubset(required_timeframes)
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v4_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_5m_cluster_v4")
    resolved = resolve_signal_engine("vegas_5m_cluster_v4", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v4_base.py"
    assert resolved.spec.configuration_schema["default_parameters"]["context_mode"] == "closed_htf_integrated_forming"
    assert resolved.spec.configuration_schema["default_parameters"]["context_timeframes"] == ["2h", "8h", "1d"]
    required_timeframes = {
        item["timeframe"]
        for item in resolved.spec.required_data
        if item["data_type"] == "candles" and item["origin"] == "derived"
    }
    assert {"5m", "2h", "8h", "1d"}.issubset(required_timeframes)
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v5_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_5m_cluster_v5")
    resolved = resolve_signal_engine("vegas_5m_cluster_v5", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v5_base.py"
    assert resolved.spec.configuration_schema["default_parameters"]["context_mode"] == "candles_only_integrated_forming"
    assert resolved.spec.configuration_schema["default_parameters"]["context_timeframes"] == ["2h", "8h", "1d"]
    required_timeframes = {
        item["timeframe"]
        for item in resolved.spec.required_data
        if item["data_type"] == "candles" and item["origin"] == "derived"
    }
    assert {"5m", "2h", "8h", "1d"}.issubset(required_timeframes)
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v5_oi_registry_entry_is_contract_compliant():
    validate_signal_engine_spec("vegas_5m_cluster_v5_oi")
    resolved = resolve_signal_engine("vegas_5m_cluster_v5_oi", repository=_repository(), workspace_root=Path.cwd())
    assert resolved.spec.code_ref["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v5_oi_base.py"
    assert resolved.spec.configuration_schema["default_parameters"]["context_mode"] == "candles_only_integrated_forming_with_oi_features"
    required_candle_timeframes = {
        item["timeframe"]
        for item in resolved.spec.required_data
        if item["data_type"] == "candles" and item["origin"] == "derived"
    }
    required_oi_timeframes = {
        item["timeframe"]
        for item in resolved.spec.required_data
        if item["data_type"] == "open_interest" and item["origin"] == "raw"
    }
    assert {"5m", "2h", "8h", "1d"}.issubset(required_candle_timeframes)
    assert required_oi_timeframes == {"5m"}
    validate_strategy_module(resolved.spec.code_ref["base_strategy_path"])


def test_5m_cluster_v2_context_uses_closed_htf_candles_only(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_v2_pool(tmp_path)
    asset = "AAVE"
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="raw",
        rows=[
            _ohlcv_row("2026-06-30T15:25:00Z", open_=90, high=91, low=89, close=90),
            _ohlcv_row("2026-07-01T01:25:00Z", open_=100, high=103, low=99, close=102),
            _ohlcv_row("2026-07-01T02:00:00Z", open_=102, high=106, low=101, close=105),
            _ohlcv_row("2026-07-01T02:05:00Z", open_=105, high=107, low=104, close=106),
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-07-01T02:05:00Z",
                close=100,
                ema_values={36: 100, 43: 100.1, 144: 99.9, 169: 102, 576: 102, 676: 102},
            )
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="2h",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-30T23:25:00Z",
                close=98,
                ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
            ),
            _ema_cluster_row(
                "2026-07-01T01:25:00Z",
                close=106,
                ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
            ),
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="1d",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-29T15:25:00Z",
                close=97,
                ema_values={36: 97, 43: 97, 144: 97, 169: 97, 576: 97, 676: 97},
            ),
            _ema_cluster_row(
                "2026-06-30T15:25:00Z",
                close=109,
                ema_values={36: 109, 43: 109, 144: 109, 169: 109, 576: 109, 676: 109},
            ),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_5m_cluster_v2",
        asset=asset,
        target_end="2026-07-01T02:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_5m_cluster_v2", asset, f"{asset}-vegas_5m_cluster_v2-canonical"))
    packet = signals[0]["payload"]
    assert packet["evidence"]["context_mode"] == "closed_htf_only"
    assert packet["charts"]["2h"]["completed_candles"][-1][0] == "2026-06-30T23:25:00Z"
    assert packet["charts"]["1d"]["completed_candles"][-1][0] == "2026-06-29T15:25:00Z"
    assert packet["charts"]["2h"]["latest_closed_at"] == "2026-07-01T01:25:00Z"
    assert packet["charts"]["1d"]["latest_closed_at"] == "2026-06-30T15:25:00Z"
    forming_context = packet["forming_context"]
    assert forming_context == packet["evidence"]["forming_context"]
    assert forming_context["2h"] == {
        "role": "forming_context",
        "timeframe": "2h",
        "source_timeframe": "5m",
        "source": "aggregated_confirmed_5m_up_to_signal",
        "is_completed": False,
        "open_timestamp": "2026-07-01T01:25:00Z",
        "partial_close_timestamp": "2026-07-01T02:05:00Z",
        "expected_close_timestamp": "2026-07-01T03:25:00Z",
        "source_candle_count": 3,
        "ohlcv": {
            "open": "100",
            "high": "107",
            "low": "99",
            "close": "106",
            "volume": "3.0",
            "vol_ccy": "3.0",
            "vol_ccy_quote": "3.0",
        },
    }
    assert forming_context["1d"]["open_timestamp"] == "2026-06-30T15:25:00Z"
    assert forming_context["1d"]["partial_close_timestamp"] == "2026-07-01T02:05:00Z"
    assert forming_context["1d"]["expected_close_timestamp"] == "2026-07-01T15:25:00Z"
    assert forming_context["1d"]["source_candle_count"] == 4
    assert forming_context["1d"]["ohlcv"]["open"] == "90"
    assert forming_context["1d"]["ohlcv"]["high"] == "107"
    assert forming_context["1d"]["ohlcv"]["low"] == "89"
    assert forming_context["1d"]["ohlcv"]["close"] == "106"


def test_5m_cluster_v2_forming_context_advances_after_latest_derived_htf_row():
    from quant_terminal_worker.signal_engines import vegas_5m_cluster_v2 as engine

    signal_timestamp = datetime.fromisoformat("2026-07-07T07:50:00+00:00")
    rows_by_timeframe = {
        "1d": [
            _ema_cluster_row(
                "2026-07-06T07:00:00Z",
                close=100,
                ema_values={36: 100, 43: 100, 144: 100, 169: 100, 576: 100, 676: 100},
            )
        ]
    }
    prepared_context_rows = {
        timeframe: engine._prepare_rows(rows)
        for timeframe, rows in rows_by_timeframe.items()
    }
    raw_rows = [
        _ohlcv_row("2026-07-07T07:00:00Z", open_=100, high=101, low=99, close=100),
        _ohlcv_row("2026-07-07T07:45:00Z", open_=100, high=104, low=98, close=103),
        _ohlcv_row("2026-07-07T07:50:00Z", open_=103, high=105, low=102, close=104),
    ]

    forming = engine._forming_context(
        rows_by_timeframe=prepared_context_rows,
        timestamps_by_timeframe=engine._prepare_context_timestamp_index(prepared_context_rows),
        raw_index=engine._prepare_raw_index(raw_rows),
        signal_timestamp=signal_timestamp,
        context_timeframes=["1d"],
    )

    assert forming["1d"]["open_timestamp"] == "2026-07-07T07:00:00Z"
    assert forming["1d"]["partial_close_timestamp"] == "2026-07-07T07:50:00Z"
    assert forming["1d"]["expected_close_timestamp"] == "2026-07-08T07:00:00Z"
    assert forming["1d"]["source_candle_count"] == 3
    assert forming["1d"]["ohlcv"]["open"] == "100"
    assert forming["1d"]["ohlcv"]["high"] == "105"
    assert forming["1d"]["ohlcv"]["low"] == "98"
    assert forming["1d"]["ohlcv"]["close"] == "104"


def test_5m_cluster_v3_emits_completed_and_forming_8h_context(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_v3_pool(tmp_path)
    asset = "AAVE"
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="raw",
        rows=[
            _ohlcv_row("2026-06-30T19:25:00Z", open_=92, high=94, low=91, close=93),
            _ohlcv_row("2026-07-01T01:25:00Z", open_=100, high=103, low=99, close=102),
            _ohlcv_row("2026-07-01T02:00:00Z", open_=102, high=106, low=101, close=105),
            _ohlcv_row("2026-07-01T02:05:00Z", open_=105, high=107, low=104, close=106),
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-07-01T02:05:00Z",
                close=100,
                ema_values={36: 100, 43: 100.1, 144: 99.9, 169: 102, 576: 102, 676: 102},
            )
        ],
    )
    for timeframe in ("2h", "8h"):
        _register_candle_ref(
            repository,
            root=root,
            asset=asset,
            timeframe=timeframe,
            origin="derived",
            rows=[
                _ema_cluster_row(
                    "2026-06-30T17:25:00Z",
                    close=98,
                    ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
                ),
                _ema_cluster_row(
                    "2026-07-01T01:25:00Z",
                    close=106,
                    ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
                ),
            ],
        )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="1d",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-29T01:25:00Z",
                close=98,
                ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
            ),
            _ema_cluster_row(
                "2026-06-30T01:25:00Z",
                close=106,
                ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
            ),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_5m_cluster_v3",
        asset=asset,
        target_end="2026-07-01T02:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_5m_cluster_v3", asset, f"{asset}-vegas_5m_cluster_v3-canonical"))
    packet = signals[0]["payload"]
    assert packet["evidence"]["context_timeframes"] == ["2h", "8h", "1d"]
    assert set(packet["charts"]) == {"5m", "2h", "8h", "1d"}
    assert set(packet["forming_context"]) == {"2h", "8h", "1d"}
    assert packet["charts"]["8h"]["completed_candles"][-1][0] == "2026-06-30T17:25:00Z"
    assert packet["charts"]["8h"]["latest_closed_at"] == "2026-07-01T01:25:00Z"
    assert packet["forming_context"]["8h"] == {
        "role": "forming_context",
        "timeframe": "8h",
        "source_timeframe": "5m",
        "source": "aggregated_confirmed_5m_up_to_signal",
        "is_completed": False,
        "open_timestamp": "2026-07-01T01:25:00Z",
        "partial_close_timestamp": "2026-07-01T02:05:00Z",
        "expected_close_timestamp": "2026-07-01T09:25:00Z",
        "source_candle_count": 3,
        "ohlcv": {
            "open": "100",
            "high": "107",
            "low": "99",
            "close": "106",
            "volume": "3.0",
            "vol_ccy": "3.0",
            "vol_ccy_quote": "3.0",
        },
    }
    assert packet["forming_context"]["8h"]["partial_close_timestamp"] == packet["timestamp"]
    assert packet["charts"]["8h"]["latest_closed_at"] <= packet["timestamp"]


def test_5m_cluster_v4_embeds_forming_context_as_latest_chart_candle(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_v4_pool(tmp_path)
    asset = "AAVE"
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="raw",
        rows=[
            _ohlcv_row("2026-06-30T19:25:00Z", open_=92, high=94, low=91, close=93),
            _ohlcv_row("2026-07-01T01:25:00Z", open_=100, high=103, low=99, close=102),
            _ohlcv_row("2026-07-01T02:00:00Z", open_=102, high=106, low=101, close=105),
            _ohlcv_row("2026-07-01T02:05:00Z", open_=105, high=107, low=104, close=106),
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-07-01T02:05:00Z",
                close=100,
                ema_values={36: 100, 43: 100.1, 144: 99.9, 169: 102, 576: 102, 676: 102},
            )
        ],
    )
    for timeframe in ("2h", "8h"):
        _register_candle_ref(
            repository,
            root=root,
            asset=asset,
            timeframe=timeframe,
            origin="derived",
            rows=[
                _ema_cluster_row(
                    "2026-06-30T17:25:00Z",
                    close=98,
                    ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
                ),
                _ema_cluster_row(
                    "2026-07-01T01:25:00Z",
                    close=106,
                    ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
                ),
            ],
        )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="1d",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-29T01:25:00Z",
                close=98,
                ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
            ),
            _ema_cluster_row(
                "2026-06-30T01:25:00Z",
                close=106,
                ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
            ),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_5m_cluster_v4",
        asset=asset,
        target_end="2026-07-01T02:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_5m_cluster_v4", asset, f"{asset}-vegas_5m_cluster_v4-canonical"))
    packet = signals[0]["payload"]
    assert "forming_context" not in packet
    assert "forming_context" not in packet["evidence"]
    assert packet["evidence"]["context_timeframes"] == ["2h", "8h", "1d"]
    assert set(packet["charts"]) == {"5m", "2h", "8h", "1d"}
    eight_hour = packet["charts"]["8h"]
    assert eight_hour["completed_candles"][-1][0] == "2026-06-30T17:25:00Z"
    assert eight_hour["latest_closed_at"] == "2026-07-01T01:25:00Z"
    assert eight_hour["candle_columns"][-4:] == [
        "is_completed",
        "source_candle_count",
        "partial_close_timestamp",
        "expected_close_timestamp",
    ]
    assert eight_hour["candles"][-1][0] == "2026-07-01T01:25:00Z"
    assert eight_hour["candles"][-1][1:5] == ["100", "107", "99", "106"]
    assert eight_hour["candles"][-1][-4:] == [False, 3, "2026-07-01T02:05:00Z", "2026-07-01T09:25:00Z"]
    assert eight_hour["latest_candle_is_completed"] is False
    assert eight_hour["latest_partial_close_timestamp"] == packet["timestamp"]
    assert packet["charts"]["1d"]["candles"][-1][-4:] == [False, 3, "2026-07-01T02:05:00Z", "2026-07-02T01:25:00Z"]
    from quant_terminal_strategies import vegas_ema_5m_hft_v4_base as strategy

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v4:AAVE:test:20260701T020500Z",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    assert decision["reason_code"] != "missing_required_5m_2h_8h_or_1d_context"
    assert decision["diagnostics"]["eight_hour_forming_source_candle_count"] == 3


def test_5m_cluster_v4_does_not_scan_rows_inside_dedupe_window(monkeypatch):
    from quant_terminal_worker.signal_engines import vegas_5m_cluster_v4 as engine

    periods = [36, 43, 144, 169, 576, 676]

    def ema_row(timestamp: datetime) -> dict[str, object]:
        row: dict[str, object] = {
            "timestamp": timestamp,
            "open": 100,
            "high": 101,
            "low": 99,
            "close": 100,
            "volume": 1,
            "vol_ccy": 1,
            "vol_ccy_quote": 1,
            "confirm": 1,
        }
        for period in periods:
            row[f"ema_{period}"] = 100
        return row

    start = datetime(2026, 7, 1, tzinfo=UTC)
    rows = [ema_row(start + timedelta(minutes=5 * index)) for index in range(30)]
    context_rows = {
        "2h": [ema_row(start)],
        "8h": [ema_row(start)],
        "1d": [ema_row(start)],
    }
    scanned_indexes: list[int] = []
    original_scan_row = engine._scan_row

    def capture_scan_row(**kwargs):
        scanned_indexes.append(kwargs["index"])
        return original_scan_row(**kwargs)

    monkeypatch.setattr(engine, "_scan_row", capture_scan_row)

    _, generated_count = engine.generate_5m_cluster_packets(
        workspace_root=Path.cwd(),
        asset="AAVE",
        instrument="AAVE-USDT-SWAP",
        derived_rows=rows,
        raw_5m_rows=rows,
        start=start,
        end=rows[-1]["timestamp"],
        parameters={
            "context_bars": 80,
            "vote_threshold": 3,
            "proximity_threshold": "0.002",
            "dedupe_window_minutes": 120,
            "context_timeframes": ["2h", "8h", "1d"],
        },
        context_rows=context_rows,
    )

    assert generated_count == 2
    assert scanned_indexes == [0, 24]


def test_5m_cluster_v5_emits_candles_only_without_legacy_completed_candles(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_v5_pool(tmp_path)
    asset = "AAVE"
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="raw",
        rows=[
            _ohlcv_row("2026-06-30T19:25:00Z", open_=92, high=94, low=91, close=93),
            _ohlcv_row("2026-07-01T01:25:00Z", open_=100, high=103, low=99, close=102),
            _ohlcv_row("2026-07-01T02:00:00Z", open_=102, high=106, low=101, close=105),
            _ohlcv_row("2026-07-01T02:05:00Z", open_=105, high=107, low=104, close=106),
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-07-01T02:05:00Z",
                close=100,
                ema_values={36: 100, 43: 100.1, 144: 99.9, 169: 102, 576: 102, 676: 102},
            )
        ],
    )
    for timeframe in ("2h", "8h"):
        _register_candle_ref(
            repository,
            root=root,
            asset=asset,
            timeframe=timeframe,
            origin="derived",
            rows=[
                _ema_cluster_row(
                    "2026-06-30T17:25:00Z",
                    close=98,
                    ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
                ),
                _ema_cluster_row(
                    "2026-07-01T01:25:00Z",
                    close=106,
                    ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
                ),
            ],
        )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="1d",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-29T01:25:00Z",
                close=98,
                ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
            ),
            _ema_cluster_row(
                "2026-06-30T01:25:00Z",
                close=106,
                ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
            ),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_5m_cluster_v5",
        asset=asset,
        target_end="2026-07-01T02:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_5m_cluster_v5", asset, f"{asset}-vegas_5m_cluster_v5-canonical"))
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert "forming_context" not in packet
    assert "forming_context" not in packet["evidence"]
    assert packet["evidence"]["context_mode"] == "candles_only_integrated_forming"
    assert set(packet["charts"]) == {"5m", "2h", "8h", "1d"}
    for chart in packet["charts"].values():
        assert "completed_candles" not in chart
        assert "candles" in chart
        assert chart["candle_columns"][-4:] == [
            "is_completed",
            "source_candle_count",
            "partial_close_timestamp",
            "expected_close_timestamp",
        ]

    assert packet["charts"]["5m"]["candles"][-1][-4:] == [True, None, "2026-07-01T02:10:00Z", "2026-07-01T02:10:00Z"]
    assert packet["charts"]["8h"]["candles"][-1][0] == "2026-07-01T01:25:00Z"
    assert packet["charts"]["8h"]["candles"][-1][-4:] == [False, 3, "2026-07-01T02:05:00Z", "2026-07-01T09:25:00Z"]
    assert packet["charts"]["1d"]["candles"][-1][-4:] == [False, 3, "2026-07-01T02:05:00Z", "2026-07-02T01:25:00Z"]

    from quant_terminal_strategies import vegas_ema_5m_hft_v5_base as strategy

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v5:AAVE:test:20260701T020500Z",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    assert decision["reason_code"] != "missing_required_5m_2h_8h_or_1d_context"
    assert decision["diagnostics"]["eight_hour_forming_source_candle_count"] == 3


def test_5m_cluster_v5_oi_emits_open_interest_context(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_v5_oi_pool(tmp_path)
    asset = "AAVE"
    oi_start = datetime(2026, 6, 23, 2, 5, tzinfo=UTC)
    oi_count = 2305
    raw_rows = [
        _ohlcv_row(
            (oi_start + timedelta(minutes=5 * index)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            open_=100 + index * 0.01,
            high=101 + index * 0.01,
            low=99 + index * 0.01,
            close=100 + index * 0.01,
        )
        for index in range(oi_count)
    ]
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", rows=raw_rows)
    _register_open_interest_ref(
        repository,
        root=root,
        asset=asset,
        rows=[
            _open_interest_row(
                (oi_start + timedelta(minutes=5 * index)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                oi=1000 + index * 10,
                oi_value=100000 + index * 1200,
                long_short_ratio=1.0 + index * 0.0001,
            )
            for index in range(oi_count)
        ],
    )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="5m",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-07-01T02:05:00Z",
                close=100,
                ema_values={36: 100, 43: 100.1, 144: 99.9, 169: 102, 576: 102, 676: 102},
            )
        ],
    )
    for timeframe in ("2h", "8h"):
        _register_candle_ref(
            repository,
            root=root,
            asset=asset,
            timeframe=timeframe,
            origin="derived",
            rows=[
                _ema_cluster_row(
                    "2026-06-30T17:25:00Z",
                    close=98,
                    ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
                ),
                _ema_cluster_row(
                    "2026-07-01T01:25:00Z",
                    close=106,
                    ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
                ),
            ],
        )
    _register_candle_ref(
        repository,
        root=root,
        asset=asset,
        timeframe="1d",
        origin="derived",
        rows=[
            _ema_cluster_row(
                "2026-06-29T01:25:00Z",
                close=98,
                ema_values={36: 98, 43: 98, 144: 98, 169: 98, 576: 98, 676: 98},
            ),
            _ema_cluster_row(
                "2026-06-30T01:25:00Z",
                close=106,
                ema_values={36: 106, 43: 106, 144: 106, 169: 106, 576: 106, 676: 106},
            ),
        ],
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_5m_cluster_v5_oi",
        asset=asset,
        target_end="2026-07-01T02:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_5m_cluster_v5_oi", asset, f"{asset}-vegas_5m_cluster_v5_oi-canonical"))
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert packet["evidence"]["engine"] == "vegas_5m_cluster_v5_oi"
    assert packet["evidence"]["parent_engine"] == "vegas_5m_cluster_v5"
    assert packet["evidence"]["context_mode"] == "candles_only_integrated_forming_with_oi_features"
    assert packet["evidence"]["reference_price"] == "100"
    assert packet["evidence"]["trigger_candle_close"] == "100"
    assert packet["evidence"]["signal_available_at"] == "2026-07-01T02:10:00Z"
    assert set(packet["charts"]) == {"5m", "2h", "8h", "1d", "open_interest_5m", "open_interest_2h", "open_interest_8h", "open_interest_1d"}
    oi_chart = packet["charts"]["open_interest_5m"]
    assert oi_chart["role"] == "positioning_context"
    assert oi_chart["columns"] == ["ts", "oi", "oi_value", "top_count_ls", "top_sum_ls", "ls", "taker_ls", "confirm"]
    assert len(oi_chart["rows"]) == 4
    assert oi_chart["rows"][-1] == ["2026-07-01T02:05:00Z", "24040", "2864800", "1.05", "1.02", "1.2304", "1.03", 1]
    assert packet["evidence"]["oi_latest_timestamp"] == "2026-07-01T02:05:00Z"
    assert packet["evidence"]["oi_change_1h_pct"] is not None
    oi_features = packet["evidence"]["oi_features"]
    for key in (
        "oi_return_4h_pct",
        "oi_return_8h_pct",
        "oi_return_24h_pct",
        "oi_return_3d_pct",
        "oi_return_8h_zscore_2d",
        "oi_return_8h_percentile_7d",
        "oi_value_trend_slope_24h",
        "price_oi_divergence_8h",
        "volume_adjusted_oi_change_8h",
        "long_short_ratio_trend_slope_24h",
    ):
        assert key in oi_features
        assert oi_features[key] is not None
    for timeframe in ("2h", "8h", "1d"):
        aggregate_chart = packet["charts"][f"open_interest_{timeframe}"]
        assert aggregate_chart["role"] == "positioning_context"
        assert aggregate_chart["timeframe"] == timeframe
        assert aggregate_chart["columns"] == [
            "open_ts",
            "close_ts",
            "complete",
            "source_row_count",
            "oi_open",
            "oi_high",
            "oi_low",
            "oi_close",
            "oi_return_pct",
            "oi_value_close",
            "oi_value_return_pct",
            "ls_avg",
            "ls_last",
            "top_count_ls_avg",
            "taker_ls_avg",
        ]
        assert 0 < len(aggregate_chart["rows"]) <= 4
        close_index = aggregate_chart["columns"].index("close_ts")
        complete_index = aggregate_chart["columns"].index("complete")
        assert all(row[complete_index] is True for row in aggregate_chart["rows"])
        assert all(row[close_index] <= packet["evidence"]["signal_available_at"] for row in aggregate_chart["rows"])
    assert len(json.dumps(packet)) < 120_000
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]

    from quant_terminal_strategies import vegas_ema_5m_hft_v5_oi_base as strategy

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v5_oi:AAVE:test:20260701T020500Z",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )
    assert decision["reason_code"] != "missing_required_5m_2h_8h_or_1d_context"
    assert decision["diagnostics"]["engine"] == "vegas_5m_cluster_v5_oi"
    assert decision["diagnostics"]["pattern"] == "vegas_ema_5m_cluster_proximity"
    assert decision["diagnostics"]["signal_available_at"] == packet["evidence"]["signal_available_at"]
    assert decision["diagnostics"]["reference_price"] == packet["evidence"]["reference_price"]
    assert decision["diagnostics"]["trigger_candle_close"] == packet["evidence"]["trigger_candle_close"]
    assert decision["diagnostics"]["ema_context_available"] is True
    assert decision["diagnostics"]["has_open_interest_2h"] is True
    assert decision["diagnostics"]["has_open_interest_8h"] is True
    assert decision["diagnostics"]["has_open_interest_1d"] is True
    assert "oi_latest_timestamp" in decision["diagnostics"]
    assert "oi_features" in decision["diagnostics"]


def test_5m_cluster_v5_strategy_rejects_legacy_completed_candles_shape():
    from quant_terminal_strategies import vegas_ema_5m_hft_v5_base as strategy

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "legacy-shape",
                "payload": {
                    "schema_version": "signal_packet.v2",
                    "asset": "AAVE",
                    "timestamp": "2026-07-01T02:05:00Z",
                    "charts": {
                        "5m": {"columns": ["timestamp", "open", "high", "low", "close"], "completed_candles": []},
                        "2h": {"columns": ["timestamp", "open", "high", "low", "close"], "completed_candles": []},
                        "8h": {"columns": ["timestamp", "open", "high", "low", "close"], "completed_candles": []},
                        "1d": {"columns": ["timestamp", "open", "high", "low", "close"], "completed_candles": []},
                    },
                    "evidence": {"matched_periods": [36, 43, 144]},
                },
            },
            "runtime_mode": "stage1",
            "parameters": {},
        }
    )

    assert decision["action"] == "SKIP"
    assert decision["reason_code"] == "missing_candles_only_chart_data"


def test_5m_cluster_vegas_training_dispatch_uses_derived_5m_ema_rows(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_pool(tmp_path)
    _register_5m_cluster_refs(repository, root=root, asset="AAVE")

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema_5m_cluster",
        asset="AAVE",
        target_end="2026-06-01T00:05:00Z",
    )

    assert result["status"] == "extended"
    signals = repository.list_signals(signal_set_key=build_signal_set_key("vegas_ema_5m_cluster", "AAVE", "AAVE-vegas_ema_5m_cluster-canonical"))
    packet = signals[0]["payload"]
    validate_signal_packet(packet)
    assert packet["evidence"]["pattern"] == "vegas_ema_5m_cluster_proximity"
    assert packet["evidence"]["ema_mode"] == "precomputed_5m_ema_cluster"
    assert packet["evidence"]["vote_threshold"] == 3
    assert packet["evidence"]["matched_ema_count"] == 3
    assert packet["evidence"]["matched_periods"] == [36, 43, 144]
    assert packet["evidence"]["trigger_timeframe"] == "5m"
    assert packet["evidence"]["context_timeframes"] == ["2h", "1d"]
    assert packet["active_timeframes"] == ["5m"]
    assert set(packet["charts"]) == {"5m", "2h", "1d"}
    assert packet["charts"]["5m"]["role"] == "trigger"
    assert packet["charts"]["2h"]["role"] == "context"
    assert packet["charts"]["1d"]["role"] == "context"
    assert "direction" not in packet
    assert "direction" not in packet["evidence"]


def test_5m_cluster_vegas_live_scan_reads_latest_derived_5m_row(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_pool(tmp_path)
    _register_5m_cluster_refs(repository, root=root, asset="AAVE")
    route = {
        **_route(root),
        "route_id": "aave-live",
        "signal_engine_id": "vegas_ema_5m_cluster",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "vegas_ema_5m_cluster"
    assert signal["payload_schema"] == "signal_packet.v2"
    assert signal["payload"]["evidence"]["active_timeframes"] == ["5m"]
    assert signal["payload"]["evidence"]["context_timeframes"] == ["2h", "1d"]
    assert signal["payload"]["evidence"]["matched_ema_count"] == 3
    assert set(signal["payload"]["charts"]) == {"5m", "2h", "1d"}
    assert signal["payload"]["interactions"][0]["timeframe"] == "5m"


def test_5m_cluster_vegas_live_scan_requires_three_near_ema_rails(tmp_path: Path):
    root, repository = _workspace_with_5m_cluster_pool(tmp_path)
    _register_5m_cluster_refs(
        repository,
        root=root,
        asset="AAVE",
        latest_ema_values={
            36: 100,
            43: 100,
            144: 102,
            169: 102,
            576: 102,
            676: 102,
        },
    )
    route = {
        **_route(root),
        "route_id": "aave-live",
        "signal_engine_id": "vegas_ema_5m_cluster",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
    }

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is None


def test_generic_live_scan_returns_non_vegas_packet_from_latest_parquet(tmp_path: Path):
    root, repository = _workspace_with_threshold_pool(tmp_path)
    _register_candle_ref(
        repository,
        root=root,
        asset="SOL",
        timeframe="5m",
        origin="raw",
        rows=[
            _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
            _candle_row("2026-06-01T00:05:00Z", open_=100, close=103),
        ],
    )
    route = _route(root)

    signal = scan_latest_live_signal(route=route, repository=repository, workspace_root=root)

    assert signal is not None
    assert signal["signal_engine_id"] == "threshold_reversal"
    assert signal["payload_schema"] == "signal_packet.v2"
    assert signal["payload"]["evidence"]["move_pct"] == 3.0


def test_non_vegas_live_wake_uses_generic_scanner_and_strategy_decide(tmp_path: Path):
    root, repository = _workspace_with_threshold_pool(tmp_path)
    _register_candle_ref(
        repository,
        root=root,
        asset="SOL",
        timeframe="5m",
        origin="raw",
        rows=[
            _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
            _candle_row("2026-06-01T00:05:00Z", open_=100, close=103),
        ],
    )
    bundle = _bundle(root)
    signal_set_key = build_signal_set_key("threshold_reversal", "SOL", "SOL-threshold_reversal-canonical")
    repository.create_stage1_research_session(
        {
            "session_id": "session-1",
            "source_universe_run_id": "universe-sol",
            "source_candidate_id": "candidate-sol",
            "signal_set_key": signal_set_key,
            "signal_engine_id": "threshold_reversal",
            "signal_engine_version": "0.1.0",
            "asset": "SOL",
            "signal_set_id": "SOL-threshold_reversal-canonical",
            "strategy_id": "threshold-strategy",
            "strategy_version": "v0.1",
            "train_start": "2026-06-01",
            "train_end": "2026-06-01",
            "walk_forward_start": "2026-06-01",
            "walk_forward_end": "2026-06-01",
            "artifact_root": "dev/training_sessions/threshold/stage1-sol",
            "status": "accepted",
            "manifest": {"signal_set_key": signal_set_key},
        }
    )
    extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="threshold_reversal",
        asset="SOL",
        target_end=None,
    )
    route = {
        **_route(root),
        "active_bundle_id": bundle["bundle_id"],
        "active_bundle": bundle,
        "enabled": True,
        "promoted": True,
        "data_warmed": True,
        "manually_armed": True,
        "blockers": [],
    }

    wake = run_route_wake(
        route_id="sol-live",
        repository=FakeWakeRepository(repository=repository, route=route, bundle=bundle),
        adapter=FakeAdapter(),
        workspace_root=root,
    )

    assert wake["branch"] == "entry_scan"
    assert wake["signal_scan_result"]["source"] == "canonical_signal_pool"
    assert wake["signal_scan_result"]["signal_set_key"] == signal_set_key
    assert wake["signal_scan_result"]["signal_engine_id"] == "threshold_reversal"
    assert wake["strategy_decision"]["direction"] == "LONG"
    assert wake["order_intents"][0]["action"] == "ENTER"


class FakeAdapter:
    def readiness_blockers(self):
        return []

    def snapshot(self, instrument):
        return {
            "instrument": instrument,
            "positions": [],
            "open_orders": [],
            "protection_orders": [],
            "balance": {"total_equity_usd": 1000},
            "recent_fills": [],
        }


class FakeWakeRepository:
    def __init__(self, *, repository: RuntimeRepository, route: dict[str, object], bundle: dict[str, object]) -> None:
        self.repository = repository
        self.route = route
        self.bundle = bundle
        self.wakes: list[dict[str, object]] = []

    def __getattr__(self, name: str):
        return getattr(self.repository, name)

    def get_deployment_route(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return {**self.route, "active_bundle": self.bundle}

    def get_execution_bundle(self, bundle_id):
        if bundle_id == self.bundle["bundle_id"]:
            return self.bundle
        return None

    def get_open_owner_state(self, route_id):
        return None

    def close_open_owner_states(self, route_id, *, instrument=None, reason):
        return []

    def record_wake_run(self, wake):
        self.wakes.append(wake)
        return wake

    def list_wake_runs(self, route_id, limit=25):
        return list(reversed(self.wakes))[:limit]


def _repository() -> RuntimeRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    with engine.begin() as connection:
        connection.execute(insert(data_sources).values(source_id="okx", name="OKX", source_type="exchange", config={}))
    return repository


def _workspace_with_threshold_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace"
    root.mkdir()
    repository = _repository()
    _register_threshold_engine(repository)
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("threshold_reversal", "SOL", "SOL-threshold_reversal-canonical"),
            "signal_set_id": "SOL-threshold_reversal-canonical",
            "signal_engine_id": "threshold_reversal",
            "signal_engine_version": "0.1.0",
            "asset": "SOL",
            "instrument": "SOL-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {"parameters": {"min_move_pct": 1.0}},
        }
    )
    return root, repository


def _workspace_with_bollinger_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-bollinger"
    root.mkdir()
    repository = _repository()
    _register_bollinger_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("bollinger", asset, f"{asset}-bollinger-canonical"),
            "signal_set_id": f"{asset}-bollinger-canonical",
            "signal_engine_id": "bollinger",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "timeframes": ["4h"],
                    "context_bars": 2,
                    "bb_period": 2,
                    "bb_stddev": "2",
                    "proximity_threshold": "0.03",
                    "vote_threshold": 1,
                    "dedupe_window_minutes": 120,
                }
            },
        }
    )
    return root, repository


def _workspace_with_recursive_vegas_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-recursive-vegas"
    root.mkdir()
    repository = _repository()
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema_recursive",
            "name": "Vegas EMA Recursive",
            "description": "",
            "version": "0.1",
            "code_ref": {
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "timeframes": ["2h"],
                    "context_bars": 1,
                    "vote_threshold": 1,
                    "proximity_threshold": "0.01",
                    "dedupe_window_minutes": 120,
                }
            },
        }
    )
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_ema_recursive", asset, f"{asset}-vegas_ema_recursive-canonical"),
            "signal_set_id": f"{asset}-vegas_ema_recursive-canonical",
            "signal_engine_id": "vegas_ema_recursive",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "timeframes": ["2h"],
                    "context_bars": 1,
                    "vote_threshold": 1,
                    "proximity_threshold": "0.01",
                }
            },
        }
    )
    return root, repository


def _workspace_with_recursive_vegas_features_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-recursive-vegas-features"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_engine(repository)
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema_recursive_features",
            "name": "Vegas EMA Recursive + Features",
            "description": "",
            "version": "0.1",
            "code_ref": {
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_recursive_features_base.py",
            },
            "supported_input_data_types": ["candles", "features"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {"data_type": "feature_base_candle", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_volatility_range", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_volume", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_ema_vegas_structure", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_bollinger", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_regime_momentum", "origin": "derived", "timeframe": "5m"},
                {"data_type": "feature_base_candle", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_volatility_range", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_volume", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_ema_vegas_structure", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_bollinger", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_regime_momentum", "origin": "derived", "timeframe": "2h"},
                {"data_type": "feature_base_candle", "origin": "derived", "timeframe": "1d"},
                {"data_type": "feature_volatility_range", "origin": "derived", "timeframe": "1d"},
                {"data_type": "feature_volume", "origin": "derived", "timeframe": "1d"},
                {"data_type": "feature_ema_vegas_structure", "origin": "derived", "timeframe": "1d"},
                {"data_type": "feature_bollinger", "origin": "derived", "timeframe": "1d"},
                {"data_type": "feature_regime_momentum", "origin": "derived", "timeframe": "1d"},
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 2,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_timeframes": ["2h", "1d"],
                    "feature_timeframes": ["5m", "2h", "1d"],
                    "feature_window_bars": {"5m": 24, "2h": 12, "1d": 10},
                }
            },
        }
    )
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_ema_recursive_features", asset, f"{asset}-vegas_ema_recursive_features-canonical"),
            "signal_set_id": f"{asset}-vegas_ema_recursive_features-canonical",
            "signal_engine_id": "vegas_ema_recursive_features",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "context_timeframes": ["2h", "1d"],
                    "feature_timeframes": ["5m", "2h", "1d"],
                    "feature_window_bars": {"5m": 24, "2h": 12, "1d": 10},
                }
            },
        }
    )
    return root, repository


def _register_recursive_vegas_feature_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
) -> None:
    _register_5m_cluster_refs(repository, root=root, asset=asset)
    base_rows = [
        _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
        _candle_row("2026-06-01T00:05:00Z", open_=100, close=100.2),
    ]
    feature_timeframes = ("5m", "2h", "1d")
    families = (
        "feature_base_candle",
        "feature_volatility_range",
        "feature_volume",
        "feature_ema_vegas_structure",
        "feature_bollinger",
        "feature_regime_momentum",
    )
    for timeframe in feature_timeframes:
        for data_type in families:
            rows = base_rows if timeframe == "5m" else base_rows[-1:]
            _register_feature_ref(repository, root=root, asset=asset, timeframe=timeframe, data_type=data_type, rows=rows)


def _register_feature_ref(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    timeframe: str,
    data_type: str,
    rows: list[dict[str, object]],
) -> None:
    storage_uri = root / ".data" / "market-data" / "origin=derived" / "source=okx" / f"type={data_type}" / f"asset={asset}" / f"timeframe={timeframe}"
    path = storage_uri / "year=2026" / "month=06" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    feature_rows = []
    for row in rows:
        if data_type == "feature_base_candle":
            feature_row = {
                "timestamp": row["timestamp"],
                "return_pct": 0.2,
                "true_range_pct": 0.4,
                "body_pct": 50,
                "upper_wick_pct": 25,
                "lower_wick_pct": 25,
                "close_location_pct": 80,
            }
        else:
            feature_row = {
                "timestamp": row["timestamp"],
                "feature_value": 1.0,
            }
        feature_rows.append(feature_row)
    pq.write_table(pa.Table.from_pylist(feature_rows), path)
    with repository.engine.begin() as connection:
        connection.execute(
            insert(market_data_refs).values(
                dataset_id=f"{asset}-{data_type}-{timeframe}",
                source_id="okx",
                asset=asset,
                instrument=f"{asset}-USDT-SWAP",
                data_type=data_type,
                timeframe=timeframe,
                data_origin="derived",
                start_ts=datetime.fromisoformat(str(rows[0]["timestamp"]).replace("Z", "+00:00")),
                end_ts=datetime.fromisoformat(str(rows[-1]["timestamp"]).replace("Z", "+00:00")),
                row_count=len(feature_rows),
                storage_backend="parquet",
                storage_uri=str(storage_uri),
                schema_descriptor={"feature_family": data_type.replace("feature_", "")},
                quality_status="feature_enriched",
                ingestion_version="test",
            )
        )


def _workspace_with_5m_cluster_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-5m-cluster-vegas"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_ema_5m_cluster", asset, f"{asset}-vegas_ema_5m_cluster-canonical"),
            "signal_set_id": f"{asset}-vegas_ema_5m_cluster-canonical",
            "signal_engine_id": "vegas_ema_5m_cluster",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                }
            },
        }
    )
    return root, repository


def _workspace_with_5m_cluster_v2_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-vegas-5m-cluster-v2"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_v2_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_5m_cluster_v2", asset, f"{asset}-vegas_5m_cluster_v2-canonical"),
            "signal_set_id": f"{asset}-vegas_5m_cluster_v2-canonical",
            "signal_engine_id": "vegas_5m_cluster_v2",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                    "context_mode": "closed_htf_only",
                }
            },
        }
    )
    return root, repository


def _workspace_with_5m_cluster_v3_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-vegas-5m-cluster-v3"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_v3_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_5m_cluster_v3", asset, f"{asset}-vegas_5m_cluster_v3-canonical"),
            "signal_set_id": f"{asset}-vegas_5m_cluster_v3-canonical",
            "signal_engine_id": "vegas_5m_cluster_v3",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                    "context_mode": "closed_htf_only",
                    "context_timeframes": ["2h", "8h", "1d"],
                }
            },
        }
    )
    return root, repository


def _workspace_with_5m_cluster_v4_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-vegas-5m-cluster-v4"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_v4_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_5m_cluster_v4", asset, f"{asset}-vegas_5m_cluster_v4-canonical"),
            "signal_set_id": f"{asset}-vegas_5m_cluster_v4-canonical",
            "signal_engine_id": "vegas_5m_cluster_v4",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                    "context_mode": "closed_htf_integrated_forming",
                    "context_timeframes": ["2h", "8h", "1d"],
                }
            },
        }
    )
    return root, repository


def _workspace_with_5m_cluster_v5_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-vegas-5m-cluster-v5"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_v5_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_5m_cluster_v5", asset, f"{asset}-vegas_5m_cluster_v5-canonical"),
            "signal_set_id": f"{asset}-vegas_5m_cluster_v5-canonical",
            "signal_engine_id": "vegas_5m_cluster_v5",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                    "context_mode": "candles_only_integrated_forming",
                    "context_timeframes": ["2h", "8h", "1d"],
                }
            },
        }
    )
    return root, repository


def _workspace_with_5m_cluster_v5_oi_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-vegas-5m-cluster-v5-oi"
    root.mkdir()
    repository = _repository()
    _register_5m_cluster_v5_oi_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("vegas_5m_cluster_v5_oi", asset, f"{asset}-vegas_5m_cluster_v5_oi-canonical"),
            "signal_set_id": f"{asset}-vegas_5m_cluster_v5_oi-canonical",
            "signal_engine_id": "vegas_5m_cluster_v5_oi",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "context_bars": 2,
                    "oi_context_bars": 4,
                    "oi_aggregate_context_bars": 4,
                    "vote_threshold": 3,
                    "proximity_threshold": "0.002",
                    "dedupe_window_minutes": 120,
                    "context_mode": "candles_only_integrated_forming_with_oi_features",
                    "context_timeframes": ["2h", "8h", "1d"],
                }
            },
        }
    )
    return root, repository


def _workspace_with_liquidity_sweep_pool(tmp_path: Path) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / "workspace-liquidity-sweep"
    root.mkdir(parents=True, exist_ok=True)
    repository = _repository()
    _register_liquidity_sweep_engine(repository)
    asset = "AAVE"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key("liquidity_sweep_v1", asset, f"{asset}-liquidity_sweep_v1-canonical"),
            "signal_set_id": f"{asset}-liquidity_sweep_v1-canonical",
            "signal_engine_id": "liquidity_sweep_v1",
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {
                "parameters": {
                    "reference_window_hours": 12,
                    "atr_period": 14,
                    "min_sweep_atr": 0.2,
                    "cooldown_hours": 2,
                    "context_bars": 144,
                }
            },
        }
    )
    return root, repository


def _register_threshold_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "threshold_reversal",
            "name": "Threshold Reversal",
            "description": "Contract proof engine",
            "version": "0.1.0",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_engines.threshold_reversal:scan_live_signal",
            "configuration_schema": {},
        }
    )


def _register_liquidity_sweep_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "liquidity_sweep_v1",
            "name": "Liquidity Sweep v1",
            "description": "Lean 5m liquidity sweep event engine.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/liquidity_sweep_v1.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/liquidity_sweep_v1_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {
                    "data_type": "candles",
                    "origin": "raw",
                    "timeframe": "5m",
                    "lookback_bars": 20000,
                    "freshness_tolerance_seconds": 300,
                }
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.liquidity_sweep_v1:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.liquidity_sweep_v1:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "reference_window_hours": 12,
                    "atr_period": 14,
                    "min_sweep_atr": 0.2,
                    "cooldown_hours": 2,
                    "context_bars": 144,
                }
            },
        }
    )


def _register_bollinger_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "bollinger",
            "name": "Bollinger Bands",
            "description": "Bollinger band proximity signal engine.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/bollinger.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/bollinger_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "4h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.bollinger:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.bollinger:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "timeframes": ["4h"],
                    "context_bars": 2,
                    "bb_period": 2,
                    "vote_threshold": 1,
                    "proximity_threshold": "0.03",
                }
            },
        }
    )


def _register_5m_cluster_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema_5m_cluster",
            "name": "Vegas 5m EMA Cluster",
            "description": "5m-only Vegas EMA rail cluster proximity engine.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_ema_5m_cluster.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_5m_cluster:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_5m_cluster:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_timeframes": ["2h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _register_5m_cluster_v2_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_5m_cluster_v2",
            "name": "Vegas 5m Cluster v2",
            "description": "5m Vegas EMA cluster engine with closed-only HTF context.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_5m_cluster_v2.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v2:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v2:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_mode": "closed_htf_only",
                    "context_timeframes": ["2h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _register_5m_cluster_v3_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_5m_cluster_v3",
            "name": "Vegas 5m Cluster v3",
            "description": "5m Vegas EMA cluster engine with closed-only 2h/8h/1d HTF context.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_5m_cluster_v3.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v3_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "8h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v3:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v3:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_mode": "closed_htf_only",
                    "context_timeframes": ["2h", "8h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _register_5m_cluster_v4_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_5m_cluster_v4",
            "name": "Vegas 5m Cluster v4",
            "description": "5m Vegas EMA cluster engine with integrated forming 2h/8h/1d HTF chart candles.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_5m_cluster_v4.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v4_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "8h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v4:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v4:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_mode": "closed_htf_integrated_forming",
                    "context_timeframes": ["2h", "8h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _register_5m_cluster_v5_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_5m_cluster_v5",
            "name": "Vegas 5m Cluster v5",
            "description": "5m Vegas EMA cluster engine with candles-only integrated forming 2h/8h/1d HTF chart candles.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_5m_cluster_v5.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v5_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "8h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v5:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v5:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_mode": "candles_only_integrated_forming",
                    "context_timeframes": ["2h", "8h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _register_5m_cluster_v5_oi_engine(repository: RuntimeRepository) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_5m_cluster_v5_oi",
            "name": "Vegas 5m Cluster v5 + OI",
            "description": "Vegas 5m Cluster v5 replica with confirmed 5m open-interest packet context and OI feature summaries.",
            "version": "0.1",
            "code_ref": {
                "path": "apps/worker/src/quant_terminal_worker/signal_engines/vegas_5m_cluster_v5_oi.py",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v5_oi_base.py",
            },
            "supported_input_data_types": ["candles", "open_interest"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {"data_type": "open_interest", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "5m",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "8h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "1d",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    "required_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v5_oi:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_5m_cluster_v5_oi:scan_live_signal",
            "configuration_schema": {
                "default_parameters": {
                    "context_bars": 80,
                    "oi_context_bars": 80,
                    "oi_aggregate_context_bars": 24,
                    "proximity_threshold": "0.002",
                    "vote_threshold": 3,
                    "dedupe_window_minutes": 120,
                    "ema_mode": "precomputed_5m_ema_cluster",
                    "context_mode": "candles_only_integrated_forming_with_oi_features",
                    "context_timeframes": ["2h", "8h", "1d"],
                    "ema_columns": ["ema_36", "ema_43", "ema_144", "ema_169", "ema_576", "ema_676"],
                }
            },
        }
    )


def _workspace_with_vegas_pool(
    tmp_path: Path,
    *,
    signal_engine_id: str,
    vote_threshold: int,
    include_manifest_vote_threshold: bool = True,
) -> tuple[Path, RuntimeRepository]:
    root = tmp_path / f"workspace-{signal_engine_id}"
    root.mkdir()
    repository = _repository()
    _register_vegas_engine(repository, signal_engine_id=signal_engine_id, vote_threshold=vote_threshold)
    asset = "AAVE"
    signal_set_id = f"{asset}-{signal_engine_id}-canonical"
    repository.upsert_signal_set(
        {
            "signal_set_key": build_signal_set_key(signal_engine_id, asset, signal_set_id),
            "signal_set_id": signal_set_id,
            "signal_engine_id": signal_engine_id,
            "signal_engine_version": "0.1",
            "asset": asset,
            "instrument": f"{asset}-USDT-SWAP",
            "start_ts": None,
            "end_ts": None,
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {"parameters": {"vote_threshold": vote_threshold} if include_manifest_vote_threshold else {}},
        }
    )
    return root, repository


def _register_vegas_engine(repository: RuntimeRepository, *, signal_engine_id: str, vote_threshold: int) -> None:
    repository.register_signal_engine(
        {
            "signal_engine_id": signal_engine_id,
            "name": f"Vegas EMA vote {vote_threshold}",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:scan_live_signal",
            "configuration_schema": {"default_parameters": {"vote_threshold": vote_threshold}},
        }
    )


def _register_default_vegas_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    rows: list[dict[str, object]],
) -> None:
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", rows=rows)
    for timeframe in ("2h", "4h", "8h", "12h", "1d"):
        _register_candle_ref(repository, root=root, asset=asset, timeframe=timeframe, origin="derived", rows=rows)


def _register_bollinger_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
) -> None:
    raw_rows = [
        _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
        _candle_row("2026-06-01T04:00:00Z", open_=100, close=104),
        _candle_row("2026-06-01T04:05:00Z", open_=104, close=105),
    ]
    derived_rows = [
        _candle_row("2026-05-31T16:00:00Z", open_=100, close=100),
        _candle_row("2026-05-31T20:00:00Z", open_=100, close=100),
        _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
    ]
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", rows=raw_rows)
    _register_candle_ref(repository, root=root, asset=asset, timeframe="4h", origin="derived", rows=derived_rows)


def _register_recursive_vegas_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
) -> None:
    raw_rows = [
        _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
        _candle_row("2026-06-01T00:05:00Z", open_=100, close=100),
    ]
    derived_rows = [
        _ema_candle_row("2026-05-31T22:00:00Z", close=100),
    ]
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", rows=raw_rows)
    _register_candle_ref(repository, root=root, asset=asset, timeframe="2h", origin="derived", rows=derived_rows)


def _register_5m_cluster_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    latest_ema_values: dict[int, float] | None = None,
) -> None:
    latest_ema_values = latest_ema_values or {
        36: 100,
        43: 100.1,
        144: 99.9,
        169: 102,
        576: 102,
        676: 102,
    }
    raw_rows = [
        _candle_row("2026-06-01T00:00:00Z", open_=100, close=100),
        _candle_row("2026-06-01T00:05:00Z", open_=100, close=100),
    ]
    derived_rows = [
        _ema_cluster_row("2026-06-01T00:00:00Z", close=100, ema_values={36: 105, 43: 105, 144: 105, 169: 105, 576: 105, 676: 105}),
        _ema_cluster_row("2026-06-01T00:05:00Z", close=100, ema_values=latest_ema_values),
    ]
    context_rows = [
        _ema_cluster_row("2026-05-31T00:00:00Z", close=95, ema_values={36: 94, 43: 94, 144: 93, 169: 93, 576: 92, 676: 92}),
        _ema_cluster_row("2026-06-01T00:00:00Z", close=100, ema_values={36: 99, 43: 99, 144: 98, 169: 98, 576: 97, 676: 97}),
    ]
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", rows=raw_rows)
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="derived", rows=derived_rows)
    _register_candle_ref(repository, root=root, asset=asset, timeframe="2h", origin="derived", rows=context_rows)
    _register_candle_ref(repository, root=root, asset=asset, timeframe="1d", origin="derived", rows=context_rows)


def _register_candle_ref(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    timeframe: str,
    origin: str,
    rows: list[dict[str, object]],
) -> None:
    storage_uri = root / ".data" / "market-data" / f"origin={origin}" / "source=okx" / "type=candles" / f"asset={asset}" / f"timeframe={timeframe}"
    path = storage_uri / "year=2026" / "month=06" / "data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    with repository.engine.begin() as connection:
        connection.execute(
            insert(market_data_refs).values(
                dataset_id=f"{asset}-{origin}-{timeframe}",
                source_id="okx",
                asset=asset,
                instrument=f"{asset}-USDT-SWAP",
                data_type="candles",
                timeframe=timeframe,
                data_origin=origin,
                start_ts=datetime.fromisoformat(str(rows[0]["timestamp"]).replace("Z", "+00:00")),
                end_ts=datetime.fromisoformat(str(rows[-1]["timestamp"]).replace("Z", "+00:00")),
                row_count=len(rows),
                storage_backend="parquet",
                storage_uri=str(storage_uri),
                schema_descriptor={},
                quality_status="ingested",
                ingestion_version="test",
            )
        )


def _register_open_interest_ref(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    rows: list[dict[str, object]],
) -> None:
    storage_uri = root / ".data" / "market-data" / "origin=raw" / "source=binance" / "type=open_interest" / f"asset={asset}" / "timeframe=5m"
    path = storage_uri / "year=2026" / "month=06" / "data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    with repository.engine.begin() as connection:
        connection.execute(
            insert(market_data_refs).values(
                dataset_id=f"{asset}-binance-open-interest-raw-5m",
                source_id="binance",
                asset=asset,
                instrument=f"{asset}USDT",
                data_type="open_interest",
                timeframe="5m",
                data_origin="raw",
                start_ts=datetime.fromisoformat(str(rows[0]["timestamp"]).replace("Z", "+00:00")),
                end_ts=datetime.fromisoformat(str(rows[-1]["timestamp"]).replace("Z", "+00:00")),
                row_count=len(rows),
                storage_backend="parquet",
                storage_uri=str(storage_uri),
                schema_descriptor={},
                quality_status="ingested",
                ingestion_version="test",
            )
        )


def _candle_row(timestamp: str, *, open_: float, close: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": open_,
        "high": max(open_, close),
        "low": min(open_, close),
        "close": close,
        "volume": 1.0,
        "vol_ccy": 1.0,
        "vol_ccy_quote": 1.0,
        "confirm": 1,
    }


def _ohlcv_row(timestamp: str, *, open_: float, high: float, low: float, close: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1.0,
        "vol_ccy": 1.0,
        "vol_ccy_quote": 1.0,
        "confirm": 1,
    }


def _open_interest_row(timestamp: str, *, oi: float, oi_value: float, long_short_ratio: float = 1.01) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "symbol": "AAVEUSDT",
        "sum_open_interest": oi,
        "sum_open_interest_value": oi_value,
        "count_toptrader_long_short_ratio": 1.05,
        "sum_toptrader_long_short_ratio": 1.02,
        "count_long_short_ratio": long_short_ratio,
        "sum_taker_long_short_vol_ratio": 1.03,
        "confirm": 1,
    }


def _liquidity_reference_rows(*, start: str = "2026-06-01T00:00:00Z", count: int = 864) -> list[dict[str, object]]:
    start_ts = datetime.fromisoformat(start.replace("Z", "+00:00"))
    return [
        _ohlcv_row(
            (start_ts.replace(tzinfo=UTC) + timedelta(minutes=5 * index)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            open_=100,
            high=105,
            low=95,
            close=100,
        )
        for index in range(count)
    ]


def _ema_candle_row(timestamp: str, *, close: float) -> dict[str, object]:
    row = _candle_row(timestamp, open_=close, close=close)
    for period in (36, 43, 144, 169, 576, 676):
        row[f"ema_{period}"] = close
        row[f"ema_warmup_count_{period}"] = period
    return row


def _ema_cluster_row(timestamp: str, *, close: float, ema_values: dict[int, float]) -> dict[str, object]:
    row = _candle_row(timestamp, open_=close, close=close)
    for period in (36, 43, 144, 169, 576, 676):
        row[f"ema_{period}"] = ema_values[period]
        row[f"ema_warmup_count_{period}"] = period
    return row


def _route(root: Path) -> dict[str, object]:
    return {
        "route_id": "sol-live",
        "active_bundle_id": "bundle-1",
        "signal_engine_id": "threshold_reversal",
        "signal_engine_version": "0.1.0",
        "asset": "SOL",
        "instrument": "SOL-USDT-SWAP",
        "account_mode": "live",
        "execution_adapter": "okx",
        "bundle_uri": str(root / "bundle"),
    }


def _bundle(root: Path) -> dict[str, object]:
    bundle_root = root / "bundle"
    bundle_root.mkdir()
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text(
        "def decide(context):\n"
        "    return {'action': 'ENTER', 'direction': 'LONG', 'reason_code': 'threshold_accept'}\n"
    )
    execution_setup = {
        "schema_version": "0.1",
        "forward_hours": 24,
        "hard_exit_after_hours": 24,
        "setup": {
            "final_tp_pct": 1.0,
            "initial_sl_pct": 0.5,
            "protection_enabled": False,
        },
    }
    (bundle_root / "execution_setup.json").write_text(json.dumps(execution_setup))
    return {
        "bundle_id": "bundle-1",
        "bundle_uri": str(bundle_root),
        "strategy_module_ref": str(strategy_path),
        "strategy_id": "threshold-strategy",
        "strategy_version": "v0.1",
        "signal_engine_id": "threshold_reversal",
        "signal_engine_version": "0.1.0",
        "asset": "SOL",
        "instrument": "SOL-USDT-SWAP",
        "source_stage1_session_id": "session-1",
        "execution_setup": execution_setup,
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 100},
        "evidence_refs": {},
        "content_hash": "hash",
        "status": "promoted",
    }
