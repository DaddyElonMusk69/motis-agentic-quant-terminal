import json
import importlib
from pathlib import Path

import pytest

from quant_terminal_sdk.engine_contracts import (
    ContractValidationError,
    LiveSignalScanResult,
    SignalEngineSpec,
    SignalPacket,
    TrainingSignalGenerationResult,
    validate_engine_registry_entry,
    validate_execution_bundle,
    validate_execution_bundle_contract,
    validate_signal_engine_spec,
    validate_signal_packet,
    validate_strategy_module,
)


def test_valid_signal_engine_spec_accepts_required_parquet_candles():
    spec = SignalEngineSpec.from_mapping(
        {
            "signal_engine_id": "breakout",
            "version": "0.1",
            "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "engines/breakout/generate.py",
            "live_scanner_entrypoint": "engines/breakout/live_scan.py",
        }
    )

    assert spec.signal_engine_id == "breakout"
    assert spec.required_data[0]["timeframe"] == "5m"
    assert validate_engine_registry_entry(spec.to_mapping()) == []


def test_engine_spec_requires_live_scanner():
    with pytest.raises(ContractValidationError, match="live_scanner_entrypoint is required"):
        SignalEngineSpec.from_mapping(
            {
                "signal_engine_id": "breakout",
                "version": "0.1",
                "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
                "output_envelope_version": "signal_packet.v2",
                "runtime_entrypoint": "engines/breakout/generate.py",
            }
        )


def test_required_data_rejects_unsupported_type():
    with pytest.raises(ContractValidationError, match="unsupported required data type: orderbook"):
        SignalEngineSpec.from_mapping(
            {
                "signal_engine_id": "breakout",
                "version": "0.1",
                "required_data": [{"data_type": "orderbook", "origin": "raw", "timeframe": "5m"}],
                "output_envelope_version": "signal_packet.v2",
                "runtime_entrypoint": "engines/breakout/generate.py",
                "live_scanner_entrypoint": "engines/breakout/live_scan.py",
            }
        )


def test_required_data_accepts_open_interest():
    spec = SignalEngineSpec.from_mapping(
        {
            "signal_engine_id": "oi_context",
            "version": "0.1",
            "required_data": [{"data_type": "open_interest", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "engines/oi_context/generate.py",
            "live_scanner_entrypoint": "engines/oi_context/live_scan.py",
        }
    )

    assert spec.required_data == [{"data_type": "open_interest", "origin": "raw", "timeframe": "5m"}]


def test_required_data_accepts_futures_metrics():
    spec = SignalEngineSpec.from_mapping(
        {
            "signal_engine_id": "metrics_engine",
            "version": "1.0",
            "required_data": [
                {"data_type": "futures_metrics", "origin": "raw", "timeframe": "5m"}
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "example:generate_training_signals",
            "live_scanner_entrypoint": "example:scan_live_signal",
        }
    )

    assert spec.required_data[0]["data_type"] == "futures_metrics"


@pytest.mark.parametrize("data_type", ["funding_features", "premium_index"])
def test_required_data_accepts_new_binance_derived_types(data_type):
    spec = SignalEngineSpec.from_mapping(
        {
            "signal_engine_id": f"{data_type}_engine",
            "version": "1.0",
            "required_data": [
                {"data_type": data_type, "origin": "derived", "timeframe": "5m"}
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "example:generate_training_signals",
            "live_scanner_entrypoint": "example:scan_live_signal",
        }
    )

    assert spec.required_data[0]["data_type"] == data_type

def test_signal_packet_rejects_directional_or_execution_fields():
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": "SOL",
        "timestamp": "2026-06-08T00:00:00Z",
        "direction": "LONG",
        "votes": [{"timeframe": "2h", "kind": "breakout"}],
    }

    with pytest.raises(ContractValidationError, match="forbidden signal packet field: direction"):
        validate_signal_packet(packet)


def test_signal_packet_rejects_directional_fields_inside_evidence():
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": "SOL",
        "timestamp": "2026-06-08T00:00:00Z",
        "evidence": {"pattern": "breakout", "direction": "LONG"},
    }

    with pytest.raises(ContractValidationError, match="forbidden signal packet field: evidence.direction"):
        validate_signal_packet(packet)


def test_signal_packet_accepts_neutral_evidence():
    packet = SignalPacket.from_mapping(
        {
            "schema_version": "signal_packet.v2",
            "asset": "SOL",
            "timestamp": "2026-06-08T00:00:00Z",
            "instrument": "SOL-USDT-SWAP",
            "active_timeframes": ["5m", "2h"],
            "evidence": {"breakout": True},
        }
    )

    assert packet.asset == "SOL"
    assert validate_signal_packet(packet.to_mapping()) == []


def test_strategy_module_requires_decide(tmp_path: Path):
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text("def helper():\n    return None\n")

    with pytest.raises(ContractValidationError, match="strategy module must expose callable decide"):
        validate_strategy_module(strategy_path)


def test_strategy_decide_return_shape_is_validated(tmp_path: Path):
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text(
        "def decide(context):\n"
        "    return {'action': 'ENTER', 'direction': 'FLAT', 'reason_code': 'bad'}\n"
    )

    with pytest.raises(ContractValidationError, match="entry decisions require LONG or SHORT direction"):
        validate_strategy_module(strategy_path)


def test_manage_position_return_shape_is_validated(tmp_path: Path):
    strategy_path = tmp_path / "strategy.py"
    strategy_path.write_text(
        "def decide(context):\n"
        "    return {'action': 'SKIP', 'direction': 'FLAT', 'reason_code': 'idle'}\n"
        "def manage_position(context):\n"
        "    return {'action': 'ENTER', 'reason_code': 'bad'}\n"
    )

    with pytest.raises(ContractValidationError, match="invalid manage_position action: ENTER"):
        validate_strategy_module(strategy_path)


def test_execution_bundle_requires_live_exit_policy_fields():
    bundle = {
        "bundle_id": "bundle-1",
        "execution_setup": {
            "schema_version": "0.1",
            "forward_hours": 24,
            "hard_exit_after_hours": 24,
            "setup": {"final_tp_pct": 2.0},
        },
    }

    with pytest.raises(ContractValidationError, match="execution setup missing initial_sl_pct"):
        validate_execution_bundle_contract(bundle)


def test_execution_bundle_accepts_side_specific_exit_policy_without_shared_fallback():
    bundle = {
        "bundle_id": "bundle-1",
        "execution_setup": {
            "schema_version": "0.1",
            "forward_hours": 24,
            "hard_exit_after_hours": 24,
            "setup": {
                "policy_mode": "side_specific",
                "side_policies": {
                    "LONG": {
                        "protection_enabled": False,
                        "final_tp_pct": 1.5,
                        "initial_sl_pct": 0.6,
                    },
                    "SHORT": {
                        "protection_enabled": True,
                        "final_tp_pct": 1.1,
                        "initial_sl_pct": 0.8,
                        "protect_trigger_pct": 0.5,
                        "trail_sl_pct": 0.3,
                    },
                },
            },
        },
    }

    assert validate_execution_bundle_contract(bundle) == []


def test_execution_bundle_rejects_incomplete_side_specific_exit_policy():
    bundle = {
        "bundle_id": "bundle-1",
        "execution_setup": {
            "schema_version": "0.1",
            "forward_hours": 24,
            "hard_exit_after_hours": 24,
            "setup": {
                "policy_mode": "side_specific",
                "side_policies": {
                    "LONG": {"protection_enabled": False, "final_tp_pct": 1.5, "initial_sl_pct": 0.6},
                    "SHORT": {"protection_enabled": False, "final_tp_pct": 1.1},
                },
            },
        },
    }

    with pytest.raises(ContractValidationError, match="SHORT execution setup missing initial_sl_pct"):
        validate_execution_bundle_contract(bundle)


def test_execution_bundle_rejects_zero_protected_sl():
    bundle = {
        "bundle_id": "bundle-1",
        "execution_setup": {
            "schema_version": "0.1",
            "forward_hours": 24,
            "hard_exit_after_hours": 24,
            "setup": {
                "protection_enabled": True,
                "final_tp_pct": 1.5,
                "initial_sl_pct": 0.6,
                "protect_trigger_pct": 0.5,
                "trail_sl_pct": 0,
            },
        },
    }

    with pytest.raises(ContractValidationError, match="positive trail_sl_pct"):
        validate_execution_bundle_contract(bundle)


def test_training_and_live_scan_result_contracts():
    training = TrainingSignalGenerationResult(
        status="appended",
        generated_packet_count=4,
        appended_packet_count=3,
        raw_candle_end_ts="2026-06-08T00:00:00Z",
        scan_coverage_end_ts="2026-06-08T00:00:00Z",
        packet_refs=["packets/a.json"],
    )
    live = LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket(
            schema_version="signal_packet.v2",
            asset="SOL",
            timestamp="2026-06-08T00:00:00Z",
            evidence={"kind": "breakout"},
        ),
    )

    assert training.appended_packet_count == 3
    assert live.signal is not None


def test_engine_contracts_export_from_sdk_root():
    import quant_terminal_sdk as sdk

    assert sdk.SignalEngineSpec is SignalEngineSpec
    assert sdk.SignalPacket is SignalPacket
    assert sdk.validate_execution_bundle is validate_execution_bundle
    assert sdk.validate_execution_bundle_contract is validate_execution_bundle_contract
    assert sdk.validate_signal_engine_spec is validate_signal_engine_spec


def test_vegas_registry_metadata_is_canonical_and_readable():
    registry = json.loads(Path("artifacts/signal_engine/engine_registry.json").read_text())
    vegas_spec = SignalEngineSpec.from_mapping(registry["vegas_ema"])

    assert vegas_spec.signal_engine_id == "vegas_ema"
    assert vegas_spec.output_envelope_version == "signal_packet.v2"
    assert vegas_spec.runtime_entrypoint == "quant_terminal_worker.signal_engines.vegas_ema:generate_training_signals"
    assert vegas_spec.live_scanner_entrypoint == "quant_terminal_worker.signal_engines.vegas_ema:scan_live_signal"
    assert validate_signal_engine_spec("vegas_ema") == []


def test_engine_strategy_template_pair_validates():
    template_root = Path("templates/engine_strategy_pair")
    registry_entry = json.loads((template_root / "engine_registry_entry.json").read_text())

    assert validate_engine_registry_entry(registry_entry) == []
    assert validate_signal_engine_spec(template_root / "engine_registry_entry.json") == []
    assert validate_strategy_module(template_root / "strategy.py") == []


def test_5m_vegas_hft_base_strategy_validates():
    assert validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_base.py") == []


def test_5m_vegas_hft_v3_base_strategy_validates():
    assert validate_strategy_module("packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_5m_hft_v3_base.py") == []


def test_5m_vegas_hft_base_enters_with_aligned_context():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_5m_cluster:ETH:test:20260608T060000Z",
                "payload": _cluster_payload(
                    matched_periods=[36, 43, 144],
                    five_minute_closes=[100, 100.2, 100.4, 100.7],
                    two_hour_closes=[98, 99, 100, 101],
                    one_day_closes=[90, 94, 98, 102],
                    ema_values={"36": "101.0", "43": "100.8", "144": "100.2", "169": "99.8", "576": "99.0", "676": "98.8"},
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "daily_1d_last_return_override"
    assert decision["diagnostics"]["matched_ema_count"] == 3


def test_5m_vegas_hft_base_accepts_top_level_packet_shape():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_5m_cluster:ETH:test:20260608T060000Z",
                **_cluster_payload(
                    matched_periods=[36, 43, 144],
                    five_minute_closes=[100, 100.2, 100.4, 100.7],
                    two_hour_closes=[98, 99, 100, 101],
                    one_day_closes=[90, 94, 98, 102],
                    ema_values={"36": "101.0", "43": "100.8", "144": "100.2", "169": "99.8", "576": "99.0", "676": "98.8"},
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "daily_1d_last_return_override"
    assert decision["diagnostics"]["matched_ema_count"] == 3


def test_5m_vegas_hft_base_uses_forming_htf_context_when_completed_context_is_flat():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    payload = _cluster_payload(
        matched_periods=[36, 43, 144],
        five_minute_closes=[100, 100.2, 100.4, 100.7],
        two_hour_closes=[100, 100.02, 100.01, 100.03],
        one_day_closes=[100, 100.1, 100.05, 100.08],
        ema_values={"36": "101.0", "43": "100.8", "144": "100.2", "169": "99.8", "576": "99.0", "676": "98.8"},
        forming_context={
            "2h": _forming_context(open_=100, high=101.5, low=99.8, close=101.0, source_candle_count=18),
            "1d": _forming_context(open_=100, high=102.5, low=99.5, close=102.0, source_candle_count=160),
        },
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v2:ETH:test:20260608T060000Z",
                "payload": payload,
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "daily_1d_forming_return_override"
    assert decision["diagnostics"]["local_direction"] == "LONG"
    assert decision["diagnostics"]["macro_direction"] == "LONG"
    assert decision["diagnostics"]["two_hour_forming_return_pct"] == pytest.approx(1.0)
    assert decision["diagnostics"]["one_day_forming_return_pct"] == pytest.approx(2.0)


def test_5m_vegas_hft_base_uses_forming_daily_override_when_context_conflicts():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    payload = _cluster_payload(
        matched_periods=[36, 43, 144],
        five_minute_closes=[100, 100.2, 100.4, 100.7],
        two_hour_closes=[98, 99, 100, 101],
        one_day_closes=[90, 94, 98, 102],
        ema_values={"36": "101.0", "43": "100.8", "144": "100.2", "169": "99.8", "576": "99.0", "676": "98.8"},
        forming_context={
            "2h": _forming_context(open_=100, high=100.2, low=98.7, close=99.0, source_candle_count=18),
            "1d": _forming_context(open_=100, high=100.4, low=97.5, close=98.0, source_candle_count=160),
        },
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v2:ETH:test:20260608T060000Z",
                "payload": payload,
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "SHORT"
    assert decision["reason_code"] == "daily_1d_forming_return_override"
    assert decision["diagnostics"]["local_completed_direction"] == "LONG"
    assert decision["diagnostics"]["local_forming_direction"] == "SHORT"
    assert decision["diagnostics"]["macro_completed_direction"] == "LONG"
    assert decision["diagnostics"]["macro_forming_direction"] == "SHORT"


def test_5m_vegas_hft_base_enters_on_thin_context_like_btc_seed():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v2:ETH:test:20260608T060000Z",
                "payload": _cluster_payload(
                    matched_periods=[36, 43, 144],
                    five_minute_closes=[100, 100.02, 100.01, 100.03],
                    two_hour_closes=[100, 100.01, 100.02, 100.01],
                    one_day_closes=[100, 100.2, 100.4, 100.6, 100.8, 101.0, 101.2, 101.4, 101.6],
                    ema_values={"36": "100.4", "43": "100.2", "144": "101.0", "169": "100.8", "576": "99.0", "676": "99.2"},
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "thin_context_follow_1d_2h_5m_evidence"


def test_5m_vegas_hft_base_skips_without_required_context():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_5m_cluster:ETH:test:20260608T060000Z",
                "payload": {"evidence": {"matched_periods": [36, 43, 144]}, "charts": {"5m": {}}},
            }
        }
    )

    assert decision["action"] == "SKIP"
    assert decision["direction"] == "FLAT"
    assert decision["reason_code"] == "missing_required_5m_2h_or_1d_context"


def test_5m_vegas_hft_v3_base_uses_8h_regime_context():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_v3_base")
    payload = _cluster_payload(
        matched_periods=[36, 43, 144],
        five_minute_closes=[100, 100.2, 100.4, 100.7],
        two_hour_closes=[100, 100.02, 100.01, 100.03],
        eight_hour_closes=[100, 101, 102, 103],
        one_day_closes=[100, 100.1, 100.05, 100.08],
        ema_values={"36": "101.0", "43": "100.8", "144": "100.2", "169": "99.8", "576": "99.0", "676": "98.8"},
        forming_context={
            "2h": _forming_context(open_=100, high=100.2, low=99.8, close=100.05, source_candle_count=18),
            "8h": _forming_context(open_=100, high=104, low=99.5, close=103.5, source_candle_count=48),
            "1d": _forming_context(open_=100, high=100.4, low=99.5, close=100.1, source_candle_count=160),
        },
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v3:ETH:test:20260608T060000Z",
                "payload": payload,
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["diagnostics"]["regime_direction"] == "LONG"
    assert decision["diagnostics"]["eight_hour_return_pct"] == pytest.approx(3.0)
    assert decision["diagnostics"]["eight_hour_forming_return_pct"] == pytest.approx(3.5)


def test_5m_vegas_hft_v3_base_uses_8h_last_return_override_when_daily_is_flat():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_v3_base")
    payload = _cluster_payload(
        matched_periods=[36, 43, 144],
        five_minute_closes=[100, 100.02, 100.01, 100.03],
        two_hour_closes=[100, 100.02, 100.01, 100.03],
        eight_hour_closes=[100, 100.1, 100.2, 101.0],
        one_day_closes=[100, 100.05, 100.04, 100.06],
        ema_values={"36": "100.4", "43": "100.2", "144": "101.0", "169": "100.8", "576": "99.0", "676": "99.2"},
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v3:ETH:test:20260608T060000Z",
                "payload": payload,
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "regime_8h_last_return_override"
    assert decision["diagnostics"]["daily_last_direction"] is None
    assert decision["diagnostics"]["eight_hour_last_direction"] == "LONG"


def test_5m_vegas_hft_v3_base_uses_forming_8h_override_before_daily_forms():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_5m_hft_v3_base")
    payload = _cluster_payload(
        matched_periods=[36, 43, 144],
        five_minute_closes=[100, 100.02, 100.01, 100.03],
        two_hour_closes=[100, 100.02, 100.01, 100.03],
        eight_hour_closes=[100, 100.05, 100.03, 100.04],
        one_day_closes=[100, 100.05, 100.04, 100.06],
        ema_values={"36": "100.4", "43": "100.2", "144": "101.0", "169": "100.8", "576": "99.0", "676": "99.2"},
        forming_context={
            "8h": _forming_context(open_=100, high=100.2, low=96.5, close=97.0, source_candle_count=48),
            "1d": _forming_context(open_=100, high=100.2, low=99.8, close=100.05, source_candle_count=90),
        },
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_5m_cluster_v3:ETH:test:20260608T060000Z",
                "payload": payload,
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "SHORT"
    assert decision["reason_code"] == "regime_8h_forming_return_override"
    assert decision["diagnostics"]["eight_hour_last_direction"] == "SHORT"
    assert decision["diagnostics"]["eight_hour_forming_source_candle_count"] == 48


def test_recursive_vegas_features_base_strategy_validates_and_registry_points_to_it():
    registry = json.loads(Path("artifacts/signal_engine/engine_registry.json").read_text())
    entry = registry["vegas_ema_recursive_features"]

    assert entry["code_ref"]["base_strategy_path"] == "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_recursive_features_base.py"
    assert validate_strategy_module(entry["code_ref"]["base_strategy_path"]) == []


def test_recursive_vegas_features_base_enters_long_with_aligned_features():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_recursive_features_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_recursive_features:ETH:test:20260608T060000Z",
                "payload": _recursive_feature_payload(
                    active_timeframes=["2h", "4h"],
                    fast_mid_gap=0.8,
                    mid_slow_gap=0.6,
                    ema_stack_state="bull_stack",
                    returns_5m=[0.03, 0.04, 0.05],
                    return_2h_12=0.8,
                    return_1d_48=2.4,
                    bb_position_5m=54,
                    atr_pct_5m=0.22,
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "feature_aligned_recursive_vegas_long"
    assert decision["diagnostics"]["feature_bias"] == "LONG"


def test_recursive_vegas_features_base_enters_long_with_5m_only_signal_packet():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_recursive_features_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_recursive_features:ETH:test:20260608T060000Z",
                "payload": _recursive_feature_payload(
                    active_timeframes=["5m"],
                    fast_mid_gap=0.8,
                    mid_slow_gap=0.6,
                    ema_stack_state="bull_stack",
                    returns_5m=[0.03, 0.04, 0.05],
                    return_2h_12=0.8,
                    return_1d_48=2.4,
                    bb_position_5m=54,
                    atr_pct_5m=0.22,
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "feature_aligned_recursive_vegas_long"
    assert decision["diagnostics"]["active_timeframe_count"] == 1


def test_recursive_vegas_features_base_enters_short_with_aligned_features():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_recursive_features_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_recursive_features:ETH:test:20260608T060000Z",
                "payload": _recursive_feature_payload(
                    active_timeframes=["2h", "4h"],
                    fast_mid_gap=-0.7,
                    mid_slow_gap=-0.5,
                    ema_stack_state="bear_stack",
                    returns_5m=[-0.03, -0.04, -0.05],
                    return_2h_12=-0.8,
                    return_1d_48=-2.1,
                    bb_position_5m=46,
                    atr_pct_5m=0.2,
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "SHORT"
    assert decision["reason_code"] == "feature_aligned_recursive_vegas_short"
    assert decision["diagnostics"]["feature_bias"] == "SHORT"


def test_recursive_vegas_features_base_uses_interactions_and_ema_chart_context_for_direction():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_recursive_features_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_recursive_features:ETH:test:20260608T060000Z",
                "payload": _recursive_feature_payload(
                    active_timeframes=["5m"],
                    fast_mid_gap=0.0,
                    mid_slow_gap=0.0,
                    ema_stack_state="mixed",
                    returns_5m=[-0.01, 0.0, 0.01],
                    return_2h_12=0.0,
                    return_1d_48=0.0,
                    bb_position_5m=53,
                    atr_pct_5m=0.22,
                    interaction_distances={"36": "0.0004", "43": "0.0006", "144": "0.0011"},
                    ema_values_5m={"36": "100.10", "43": "100.00", "144": "99.80", "169": "99.70", "576": "99.40", "676": "99.20"},
                    ema_validity_5m={"36": True, "43": True, "144": True, "169": True, "576": True, "676": True},
                    five_minute_closes=[99.6, 99.8, 100.1, 100.4],
                    two_hour_closes=[99.0, 99.2, 99.6, 100.0],
                    one_day_closes=[98.0, 98.4, 99.0, 100.0],
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "weighted_recursive_feature_votes_long"
    assert decision["diagnostics"]["weighted_vote_direction"] == "LONG"
    assert decision["diagnostics"]["interaction_direction"] == "LONG"
    assert decision["diagnostics"]["ema_chart_direction"] == "LONG"


def test_recursive_vegas_features_base_skips_overextended_or_too_volatile_signals():
    strategy = importlib.import_module("quant_terminal_strategies.vegas_ema_recursive_features_base")

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": "vegas_ema_recursive_features:ETH:test:20260608T060000Z",
                "payload": _recursive_feature_payload(
                    active_timeframes=["2h", "4h"],
                    fast_mid_gap=0.8,
                    mid_slow_gap=0.6,
                    ema_stack_state="bull_stack",
                    returns_5m=[0.2, 0.2, 0.2],
                    return_2h_12=1.4,
                    return_1d_48=3.0,
                    bb_position_5m=98,
                    atr_pct_5m=1.3,
                ),
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["action"] == "SKIP"
    assert decision["direction"] == "FLAT"
    assert decision["reason_code"] == "feature_context_overextended_or_volatile"


def test_liquidity_sweep_v1_base_strategy_uses_basic_reversal_seed_direction():
    strategy = importlib.import_module("quant_terminal_strategies.liquidity_sweep_v1_base")

    high_decision = strategy.decide({"signal": {"signal_id": "liquidity_sweep_v1:AAVE:test:high", "payload": _liquidity_sweep_payload("HIGH_SWEEP")}})
    low_decision = strategy.decide({"signal": {"signal_id": "liquidity_sweep_v1:AAVE:test:low", "payload": _liquidity_sweep_payload("LOW_SWEEP")}})

    assert high_decision["action"] == "ENTER"
    assert high_decision["direction"] == "SHORT"
    assert high_decision["reason_code"] == "high_sweep_reversal_seed_short"
    assert high_decision["diagnostics"]["directional_prior"] == "reversal"
    assert low_decision["action"] == "ENTER"
    assert low_decision["direction"] == "LONG"
    assert low_decision["reason_code"] == "low_sweep_reversal_seed_long"
    assert low_decision["diagnostics"]["directional_prior"] == "reversal"


def _liquidity_sweep_payload(event_type: str) -> dict[str, object]:
    return {
        "schema_version": "signal_packet.v2",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "timestamp": "2026-06-08T06:00:00Z",
        "active_timeframes": ["5m"],
        "evidence": {
            "pattern": "liquidity_sweep_event",
            "event_type": event_type,
            "reference_window_hours": 72,
            "reference_level": "105",
            "trigger_price": "108",
            "trigger_candle_close": "101",
            "atr_14": "10",
            "sweep_distance": "3",
            "sweep_distance_atr": "0.3",
            "close_location_pct": "60",
            "cooldown_hours": 12,
            "level_id": "sweep-20260608",
        },
        "charts": {"5m": {"role": "trigger_context"}},
    }


def test_current_aave_execution_bundle_validates_with_legacy_aliases():
    bundle_id = "aave-vegas_ema-aave-vegas_ema-strategy-v01-3bee1a88652e"

    assert validate_execution_bundle(bundle_id) == []


def _cluster_payload(
    *,
    matched_periods: list[int],
    five_minute_closes: list[float],
    two_hour_closes: list[float],
    one_day_closes: list[float],
    ema_values: dict[str, str],
    eight_hour_closes: list[float] | None = None,
    forming_context: dict[str, object] | None = None,
) -> dict[str, object]:
    columns = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
    payload: dict[str, object] = {
        "schema_version": "signal_packet.v2",
        "asset": "ETH",
        "instrument": "ETH-USDT-SWAP",
        "timestamp": "2026-06-08T06:00:00Z",
        "active_timeframes": ["5m"],
        "evidence": {
            "pattern": "vegas_ema_5m_cluster_proximity",
            "trigger_timeframe": "5m",
            "context_timeframes": ["2h", "1d"],
            "matched_ema_count": len(matched_periods),
            "matched_periods": matched_periods,
        },
        "charts": {
            "5m": {
                "role": "trigger",
                "columns": columns,
                "completed_candles": [_candle_row(index, close) for index, close in enumerate(five_minute_closes)],
                "ema_values": ema_values,
            },
            "2h": {
                "role": "context",
                "columns": columns,
                "completed_candles": [_candle_row(index, close) for index, close in enumerate(two_hour_closes)],
            },
            "1d": {
                "role": "context",
                "columns": columns,
                "completed_candles": [_candle_row(index, close) for index, close in enumerate(one_day_closes)],
            },
        },
    }
    if eight_hour_closes is not None:
        payload["evidence"]["context_timeframes"] = ["2h", "8h", "1d"]
        payload["charts"]["8h"] = {
            "role": "context",
            "columns": columns,
            "completed_candles": [_candle_row(index, close) for index, close in enumerate(eight_hour_closes)],
        }
    if forming_context is not None:
        payload["forming_context"] = forming_context
        payload["evidence"]["forming_context"] = forming_context
    return payload


def _candle_row(index: int, close: float) -> list[object]:
    return [f"2026-06-08T00:{index:02d}:00Z", str(close), str(close), str(close), str(close), "1", "1", "1", 1]


def _forming_context(*, open_: float, high: float, low: float, close: float, source_candle_count: int) -> dict[str, object]:
    return {
        "role": "forming_context",
        "source": "aggregated_confirmed_5m_up_to_signal",
        "is_completed": False,
        "source_candle_count": source_candle_count,
        "ohlcv": {
            "open": str(open_),
            "high": str(high),
            "low": str(low),
            "close": str(close),
            "volume": "1",
            "vol_ccy": "1",
            "vol_ccy_quote": "1",
        },
    }


def _recursive_feature_payload(
    *,
    active_timeframes: list[str],
    fast_mid_gap: float,
    mid_slow_gap: float,
    ema_stack_state: str,
    returns_5m: list[float],
    return_2h_12: float,
    return_1d_48: float,
    bb_position_5m: float,
    atr_pct_5m: float,
    interaction_distances: dict[str, str] | None = None,
    ema_values_5m: dict[str, str] | None = None,
    ema_validity_5m: dict[str, bool] | None = None,
    five_minute_closes: list[float] | None = None,
    two_hour_closes: list[float] | None = None,
    one_day_closes: list[float] | None = None,
) -> dict[str, object]:
    feature_window_5m = [
        {
            "timestamp": f"2026-06-08T00:{index:02d}:00Z",
            "base_candle": {"return_pct": value, "close_location_pct": 60},
            "volatility_range": {"atr_pct_14": atr_pct_5m, "rolling_range_pct_12": abs(value) * 10},
            "ema_vegas_structure": {
                "fast_mid_gap_pct": fast_mid_gap,
                "mid_slow_gap_pct": mid_slow_gap,
                "ema_stack_state": ema_stack_state,
            },
            "bollinger": {"bb_position_pct": bb_position_5m, "bb_bandwidth_pct": 1.2},
            "regime_momentum": {"return_pct_12": sum(returns_5m), "return_pct_48": sum(returns_5m)},
        }
        for index, value in enumerate(returns_5m)
    ]
    features = {
        "5m": {"latest": feature_window_5m[-1], "window": feature_window_5m, "window_bars": 24},
        "2h": {
            "latest": {
                "ema_vegas_structure": {
                    "fast_mid_gap_pct": fast_mid_gap,
                    "mid_slow_gap_pct": mid_slow_gap,
                    "ema_stack_state": ema_stack_state,
                },
                "regime_momentum": {"return_pct_12": return_2h_12, "return_pct_48": return_2h_12},
                "bollinger": {"bb_position_pct": 58 if return_2h_12 > 0 else 42},
            },
            "window": [],
            "window_bars": 12,
        },
        "1d": {
            "latest": {
                "ema_vegas_structure": {"ema_stack_state": ema_stack_state},
                "regime_momentum": {"return_pct_12": return_1d_48 / 2, "return_pct_48": return_1d_48},
                "bollinger": {"bb_position_pct": 62 if return_1d_48 > 0 else 38},
            },
            "window": [],
            "window_bars": 10,
        },
    }
    columns = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
    matched_periods = [int(period) for period in (interaction_distances or {}).keys()]
    market_price = str((five_minute_closes or [100])[-1])
    interactions = [
        {
            "timeframe": "5m",
            "tunnel": "fast" if int(period) in {36, 43} else "mid" if int(period) in {144, 169} else "slow",
            "period": int(period),
            "ema_value": str((ema_values_5m or {}).get(period, "100")),
            "market_price": market_price,
            "distance_pct": distance,
        }
        for period, distance in (interaction_distances or {}).items()
    ]
    charts = {
        "5m": {
            "role": "trigger",
            "columns": columns,
            "completed_candles": [_candle_row(index, close) for index, close in enumerate(five_minute_closes or [100, 100, 100])],
            "ema_values": ema_values_5m or {},
            "ema_distances": interaction_distances or {},
            "ema_validity": ema_validity_5m or {},
        },
        "2h": {
            "role": "context",
            "columns": columns,
            "completed_candles": [_candle_row(index, close) for index, close in enumerate(two_hour_closes or [100, 100, 100])],
        },
        "1d": {
            "role": "context",
            "columns": columns,
            "completed_candles": [_candle_row(index, close) for index, close in enumerate(one_day_closes or [100, 100, 100])],
        },
    }
    return {
        "schema_version": "signal_packet.v2",
        "asset": "ETH",
        "instrument": "ETH-USDT-SWAP",
        "timestamp": "2026-06-08T06:00:00Z",
        "active_timeframes": active_timeframes,
        "interactions": interactions,
        "charts": charts,
        "features": features,
        "evidence": {
            "pattern": "vegas_ema_tunnel_proximity_with_features",
            "active_timeframes": active_timeframes,
            "matched_ema_count": len(matched_periods),
            "matched_periods": matched_periods,
            "interactions": interactions,
            "charts": charts,
            "features": features,
        },
    }
