from sqlalchemy import create_engine, insert, select

from quant_terminal_api.db.models import decisions, metadata, signal_sets, signals
from quant_terminal_api.repositories.runtime import RuntimeRepository


def test_runtime_repository_registers_modules_and_persists_backtest_result():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    repository.register_signal_engine(
        {
            "signal_engine_id": "threshold_reversal",
            "name": "Threshold Reversal",
            "description": "Neutral move detector",
            "version": "0.1.0",
            "code_ref": {"package": "quant_terminal_engines"},
            "supported_input_data_types": ["candles"],
            "output_envelope_version": "threshold_reversal.v1",
            "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_signals",
            "configuration_schema": {"min_move_pct": "number"},
        }
    )
    repository.register_strategy(
        {
            "strategy_id": "directional_threshold",
            "name": "Directional Threshold",
            "description": "Stage 1A pilot",
            "version": "0.1.0",
            "code_ref": {"package": "quant_terminal_strategies"},
            "supported_signal_engine_ids": ["threshold_reversal"],
            "parameter_schema": {"long_threshold_pct": "number"},
            "decision_schema": {"direction": "LONG|SHORT|FLAT"},
            "execution_profile_schema": {},
            "test_suite_status": "passing",
        }
    )

    repository.persist_stage1_backtest(
        {
            "run_id": "bt-test-1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "template_id": "ad_hoc",
            "strategy_id": "directional_threshold",
            "strategy_version": "0.1.0",
            "signal_engine_id": "threshold_reversal",
            "signal_engine_version": "0.1.0",
            "dataset_refs": ["btc-raw-5m"],
            "parameters_hash": "hash-1",
            "signals": [
                {
                    "signal_id": "signal-1",
                    "signal_engine_id": "threshold_reversal",
                    "signal_engine_version": "0.1.0",
                    "asset": "BTC",
                    "instrument": "BTC-USDT-SWAP",
                    "timestamp": "2026-06-01T00:00:00Z",
                    "data_refs": ["btc-raw-5m"],
                    "payload_schema": "threshold_reversal.v1",
                    "payload": {"move_pct": 2.0},
                }
            ],
            "decisions": [
                {
                    "decision_id": "decision-1",
                    "strategy_id": "directional_threshold",
                    "strategy_version": "0.1.0",
                    "signal_id": "signal-1",
                    "action": "ENTER",
                    "direction": "LONG",
                    "confidence": 0.7,
                    "reason_code": "positive_move_threshold",
                    "execution_profile": {},
                    "diagnostics": {},
                }
            ],
            "score_summary": {
                "scoring_method": "stage1a_directional_agreement",
                "metrics": {"total": 1, "agreement_rate": 1.0},
                "records": [],
            },
        }
    )

    summary = repository.get_backtest_run("bt-test-1")

    assert summary["run_id"] == "bt-test-1"
    assert summary["status"] == "completed"
    assert summary["metrics"] == {"total": 1, "agreement_rate": 1.0}
    assert summary["decision_count"] == 1
    assert summary["signal_count"] == 1

    repository.persist_stage1_backtest(
        {
            "run_id": "bt-test-2",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "template_id": "ad_hoc",
            "strategy_id": "directional_threshold",
            "strategy_version": "0.1.0",
            "signal_engine_id": "threshold_reversal",
            "signal_engine_version": "0.1.0",
            "dataset_refs": ["btc-raw-5m"],
            "parameters_hash": "hash-1",
            "signals": [
                {
                    "signal_id": "signal-1",
                    "signal_engine_id": "threshold_reversal",
                    "signal_engine_version": "0.1.0",
                    "asset": "BTC",
                    "instrument": "BTC-USDT-SWAP",
                    "timestamp": "2026-06-01T00:00:00Z",
                    "data_refs": ["btc-raw-5m"],
                    "payload_schema": "threshold_reversal.v1",
                    "payload": {"move_pct": 2.0},
                }
            ],
            "decisions": [
                {
                    "decision_id": "decision-2",
                    "strategy_id": "directional_threshold",
                    "strategy_version": "0.1.0",
                    "signal_id": "signal-1",
                    "action": "ENTER",
                    "direction": "LONG",
                    "confidence": 0.7,
                    "reason_code": "positive_move_threshold",
                    "execution_profile": {},
                    "diagnostics": {},
                }
            ],
            "score_summary": {
                "scoring_method": "stage1a_directional_agreement",
                "metrics": {"total": 1, "agreement_rate": 1.0},
                "records": [],
            },
        }
    )

    second_summary = repository.get_backtest_run("bt-test-2")

    assert second_summary["signal_count"] == 1
    assert second_summary["decision_count"] == 1


def test_runtime_repository_persists_stage0_universe_runs_and_candidates():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "configuration_schema": {},
        }
    )
    repository.upsert_signal_set(
        {
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 340,
            "payload_schema": "signal_packet.v2",
            "source_path": "/legacy",
            "manifest": {},
        }
    )

    run = {
        "universe_run_id": "universe-march-may",
        "config_hash": "hash-123",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T11:55:00Z",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "validation_start": "2026-05-01",
        "validation_end": "2026-05-24",
        "locked_oos_start": "2026-05-25",
        "locked_oos_end": "2026-05-30",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {"total_candidates": 1},
    }
    candidate = {
        "candidate_id": "universe-march-may:vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
        "universe_run_id": "universe-march-may",
        "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "BTC",
        "signal_set_id": "2026-BTC-2h-dedupe-vote2",
        "packet_count": 340,
        "trigger_rate_pct": 86.2,
        "branch_path": "path_a",
        "acceptance_status": "accepted",
        "duplicate_status": "new",
        "existing_strategy_id": None,
        "last_error": {},
        "metrics": {"trigger_rate_pct": 86.2},
    }

    repository.create_stage0_universe(run, [candidate])

    stored_run = repository.get_stage0_universe_run_by_config_hash("hash-123")
    stored_candidates = repository.list_stage0_universe_candidates("universe-march-may")

    assert stored_run["universe_run_id"] == "universe-march-may"
    assert stored_run["train_start"].isoformat() == "2026-03-01"
    assert stored_run["train_end"].isoformat() == "2026-04-30"
    assert stored_run["validation_start"].isoformat() == "2026-05-01"
    assert stored_run["validation_end"].isoformat() == "2026-05-24"
    assert stored_run["locked_oos_start"].isoformat() == "2026-05-25"
    assert stored_run["locked_oos_end"].isoformat() == "2026-05-30"
    assert stored_run["summary"] == {"total_candidates": 1}
    assert stored_candidates[0]["acceptance_status"] == "accepted"
    assert stored_candidates[0]["last_error"] == {}
    assert stored_candidates[0]["metrics"] == {"trigger_rate_pct": 86.2}


def test_runtime_repository_surfaces_signal_set_scanned_coverage_from_manifest():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    repository.upsert_signal_set(
        {
            "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            "signal_set_id": "AAVE-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-05-15T11:10:00Z",
            "packet_count": 1199,
            "payload_schema": "signal_packet.v2",
            "source_path": "/signals",
            "manifest": {
                "scan_coverage": {
                    "start_ts": "2026-03-01T00:00:00Z",
                    "end_ts": "2026-06-01T11:55:00Z",
                }
            },
        }
    )

    signal_set = repository.get_signal_set("vegas_ema:AAVE:AAVE-vegas_ema-canonical")

    assert signal_set["packet_end_ts"].isoformat() == "2026-05-15T11:10:00+00:00"
    assert signal_set["coverage_end_ts"].isoformat() == "2026-06-01T11:55:00+00:00"


def test_runtime_repository_deletes_stage0_universe_with_linked_stage1_sessions():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "configuration_schema": {},
        }
    )
    repository.upsert_signal_set(
        {
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 340,
            "payload_schema": "signal_packet.v2",
            "source_path": "/legacy",
            "manifest": {},
        }
    )
    repository.create_stage0_universe(
        {
            "universe_run_id": "universe-march-may",
            "config_hash": "hash-123",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-05-30T11:55:00Z",
            "forward_hours": 36,
            "trigger_rate_threshold_pct": 85,
            "engine_filter": ["vegas_ema"],
            "status": "created",
            "summary": {"total_candidates": 1},
        },
        [
            {
                "candidate_id": "candidate-btc",
                "universe_run_id": "universe-march-may",
                "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "BTC",
                "signal_set_id": "2026-BTC-2h-dedupe-vote2",
                "packet_count": 340,
                "trigger_rate_pct": 86.2,
                "branch_path": "path_a",
                "acceptance_status": "accepted",
                "duplicate_status": "new",
                "existing_strategy_id": None,
                "last_error": {},
                "metrics": {"trigger_rate_pct": 86.2},
            }
        ],
    )
    repository.create_stage1_research_session(
        {
            "session_id": "stage1-btc",
            "source_universe_run_id": "universe-march-may",
            "source_candidate_id": "candidate-btc",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "strategy_id": "btc-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "validation_start": "2026-05-01",
            "validation_end": "2026-05-24",
            "locked_oos_start": "2026-05-25",
            "locked_oos_end": "2026-05-31",
            "artifact_root": "dev/training_sessions/btc-vegas-tunnel-v01/stage1-btc",
            "status": "draft",
            "manifest": {"session_id": "stage1-btc"},
        }
    )

    repository.delete_stage0_universe_run("universe-march-may")

    assert repository.get_stage0_universe_run("universe-march-may") is None
    assert repository.list_stage0_universe_candidates("universe-march-may") == []
    assert repository.list_stage1_research_sessions() == []


def test_runtime_repository_canonicalizes_existing_signal_pools_and_references():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    for signal_set_id, timestamp in [
        ("2025-SOL-2h-dedupe-vote2", "2026-05-16T05:05:00Z"),
        ("2026-SOL-2h-dedupe-vote2", "2026-05-26T13:50:00Z"),
    ]:
        signal_set_key = f"vegas_ema:SOL:{signal_set_id}"
        repository.upsert_signal_set(
            {
                "signal_set_key": signal_set_key,
                "signal_set_id": signal_set_id,
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "SOL",
                "instrument": "SOL-USDT-SWAP",
                "start_ts": None,
                "end_ts": None,
                "packet_count": 99,
                "payload_schema": "signal_packet.v2",
                "source_path": f"/legacy/{signal_set_id}",
                "manifest": {"signal_set_id": signal_set_id},
            }
        )
        repository.upsert_signal(
            {
                "signal_id": f"{signal_set_key}:sig",
                "signal_set_key": signal_set_key,
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "SOL",
                "instrument": "SOL-USDT-SWAP",
                "timestamp": timestamp,
                "data_refs": [],
                "payload_schema": "signal_packet.v2",
                "payload": {"timestamp": timestamp},
            }
        )
    repository.create_stage0_universe(
        {
            "universe_run_id": "universe-march-may",
            "config_hash": "hash-123",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-05-30T23:59:59Z",
            "forward_hours": 36,
            "trigger_rate_threshold_pct": 85,
            "engine_filter": ["vegas_ema"],
            "status": "created",
            "summary": {},
        },
        [
            {
                "candidate_id": "candidate-sol-2025",
                "universe_run_id": "universe-march-may",
                "signal_set_key": "vegas_ema:SOL:2025-SOL-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "SOL",
                "signal_set_id": "2025-SOL-2h-dedupe-vote2",
                "packet_count": 1,
                "trigger_rate_pct": 90,
                "branch_path": "path_a",
                "acceptance_status": "accepted",
                "duplicate_status": "new",
                "existing_strategy_id": None,
                "last_error": {},
                "metrics": {},
            },
            {
                "candidate_id": "candidate-sol-2026",
                "universe_run_id": "universe-march-may",
                "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "SOL",
                "signal_set_id": "2026-SOL-2h-dedupe-vote2",
                "packet_count": 1,
                "trigger_rate_pct": 91,
                "branch_path": "path_a",
                "acceptance_status": "accepted",
                "duplicate_status": "new",
                "existing_strategy_id": None,
                "last_error": {},
                "metrics": {},
            },
        ],
    )
    repository.create_stage1_research_session(
        {
            "session_id": "stage1-sol",
            "source_universe_run_id": "universe-march-may",
            "source_candidate_id": "candidate-sol-2026",
            "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "SOL",
            "signal_set_id": "2026-SOL-2h-dedupe-vote2",
            "strategy_id": "sol-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "validation_start": "2026-05-01",
            "validation_end": "2026-05-24",
            "locked_oos_start": "2026-05-25",
            "locked_oos_end": "2026-05-31",
            "artifact_root": "dev/training_sessions/sol-vegas-tunnel-v01/stage1-sol",
            "status": "draft",
            "manifest": {"session_id": "stage1-sol"},
        }
    )

    report = repository.canonicalize_signal_pools()

    assert report["canonical_pool_count"] == 1
    assert report["deleted_duplicate_stage0_candidate_count"] == 1
    assert report["duplicate_engine_asset_groups_after"] == 0
    assert report["remaining_metadata_mismatch_count"] == 0
    with engine.connect() as connection:
        stored_sets = connection.execute(select(signal_sets)).mappings().all()
        stored_signals = connection.execute(select(signals)).mappings().all()
    assert [row["signal_set_key"] for row in stored_sets] == ["vegas_ema:SOL:SOL-vegas_ema-canonical"]
    assert stored_sets[0]["packet_count"] == 2
    assert {row["signal_set_key"] for row in stored_signals} == {"vegas_ema:SOL:SOL-vegas_ema-canonical"}
    candidates = repository.list_stage0_universe_candidates("universe-march-may")
    assert len(candidates) == 1
    assert candidates[0]["signal_set_key"] == "vegas_ema:SOL:SOL-vegas_ema-canonical"
    session = repository.get_stage1_research_session("stage1-sol")
    assert session["signal_set_key"] == "vegas_ema:SOL:SOL-vegas_ema-canonical"
    assert session["source_candidate_id"] == "candidate-sol-2026"


def test_runtime_repository_refreshes_stage0_summary_and_marks_complete_or_superseded():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "configuration_schema": {},
        }
    )
    for asset in ["BTC", "ETH"]:
        repository.upsert_signal_set(
            {
                "signal_set_key": f"vegas_ema:{asset}:2026-{asset}-2h-dedupe-vote2",
                "signal_set_id": f"2026-{asset}-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": asset,
                "instrument": f"{asset}-USDT-SWAP",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-06-01T00:00:00Z",
                "packet_count": 100,
                "payload_schema": "signal_packet.v2",
                "source_path": "/legacy",
                "manifest": {},
            }
        )
    run = {
        "universe_run_id": "universe-march-may",
        "config_hash": "hash-123",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T11:55:00Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    repository.create_stage0_universe(
        run,
        [
            {
                "candidate_id": "candidate-btc",
                "universe_run_id": "universe-march-may",
                "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "BTC",
                "signal_set_id": "2026-BTC-2h-dedupe-vote2",
                "packet_count": 100,
                "trigger_rate_pct": 90,
                "branch_path": "path_a",
                "acceptance_status": "accepted",
                "duplicate_status": "new",
                "existing_strategy_id": None,
                "metrics": {"trigger_rate_pct": 90},
            },
            {
                "candidate_id": "candidate-eth",
                "universe_run_id": "universe-march-may",
                "signal_set_key": "vegas_ema:ETH:2026-ETH-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "ETH",
                "signal_set_id": "2026-ETH-2h-dedupe-vote2",
                "packet_count": 100,
                "trigger_rate_pct": 70,
                "branch_path": "path_b",
                "acceptance_status": "watchlist",
                "duplicate_status": "new",
                "existing_strategy_id": None,
                "metrics": {"trigger_rate_pct": 70},
            },
        ],
    )

    repository.refresh_stage0_universe_summary("universe-march-may")
    repository.supersede_stage0_universe_run("universe-march-may")

    completed_run = repository.get_stage0_universe_run("universe-march-may")

    assert completed_run["summary"] == {
        "total_candidates": 2,
        "accepted": 1,
        "watchlist": 1,
        "pending_stage0": 0,
        "failed": 0,
    }
    assert completed_run["status"] == "superseded"


def test_runtime_repository_creates_stage1_research_session():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    session = {
        "session_id": "stage1-aave-vegas-202606",
        "source_universe_run_id": "universe-march-may",
        "source_candidate_id": "candidate-aave",
        "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "validation_start": "2026-05-01",
        "validation_end": "2026-05-24",
        "locked_oos_start": "2026-05-25",
        "locked_oos_end": "2026-05-31",
        "artifact_root": "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas-202606",
        "status": "draft",
        "manifest": {"stage": "stage1a_directional_agreement"},
    }

    repository.create_stage1_research_session(session)
    sessions = repository.list_stage1_research_sessions()

    assert sessions[0]["session_id"] == "stage1-aave-vegas-202606"
    assert sessions[0]["source_candidate_id"] == "candidate-aave"
    assert sessions[0]["train_start"].isoformat() == "2026-03-01"
    assert sessions[0]["manifest"]["stage"] == "stage1a_directional_agreement"


def test_runtime_repository_existing_rnd_includes_stage1_sessions():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    repository.create_stage1_research_session(
        {
            "session_id": "stage1-aave-vegas-202606",
            "source_universe_run_id": "universe-a",
            "source_candidate_id": "candidate-aave",
            "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "signal_set_id": "AAVE-vegas_ema-canonical",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "validation_start": "2026-05-01",
            "validation_end": "2026-05-24",
            "locked_oos_start": "2026-05-25",
            "locked_oos_end": "2026-05-30",
            "artifact_root": "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave",
            "status": "draft",
            "manifest": {"stage": "stage1a_directional_agreement"},
        }
    )

    existing = repository.existing_rnd_by_signal_set()

    assert existing["vegas_ema:AAVE:AAVE-vegas_ema-canonical"] == {
        "strategy_id": "aave-vegas-tunnel-v01",
        "status": "draft",
        "run_id": "stage1-aave-vegas-202606",
    }


def test_runtime_repository_resolves_latest_stage1_pair_seed(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-old"
    frozen_strategy_path = artifact_root / "promotion/frozen_stage1a_strategy_module/strategy.py"
    frozen_strategy_path.parent.mkdir(parents=True)
    frozen_strategy_path.write_text("def decide(context):\n    return {'seed': 'frozen'}\n")
    session = {
        "session_id": "stage1-aave-old",
        "source_universe_run_id": "universe-march-may",
        "source_candidate_id": "candidate-aave",
        "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "train_start": "2026-01-01",
        "train_end": "2026-02-28",
        "validation_start": "2026-03-01",
        "validation_end": "2026-03-15",
        "locked_oos_start": "2026-03-16",
        "locked_oos_end": "2026-03-31",
        "artifact_root": str(artifact_root),
        "status": "stage1a_frozen",
        "manifest": {"stage": "stage1a_directional_agreement"},
    }

    repository.create_stage1_research_session(session)
    seed = repository.latest_stage1_strategy_seed(
        asset="AAVE",
        signal_engine_id="vegas_ema",
        strategy_id="aave-vegas-tunnel-v01",
    )

    assert seed == {
        "source_type": "latest_pair_frozen",
        "source_path": str(frozen_strategy_path),
        "source_version": "v0.1",
        "source_session_id": "stage1-aave-old",
    }


def test_runtime_repository_canonicalize_signal_pools_dedupes_duplicate_timestamps_and_updates_decisions():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    legacy_key = "vegas_ema:ZEC:2026-ZEC-2h-dedupe-vote2"
    canonical_key = "vegas_ema:ZEC:ZEC-vegas_ema-canonical"
    for signal_set_key, signal_set_id in (
        (legacy_key, "2026-ZEC-2h-dedupe-vote2"),
        (canonical_key, "ZEC-vegas_ema-canonical"),
    ):
        repository.upsert_signal_set(
            {
                "signal_set_key": signal_set_key,
                "signal_set_id": signal_set_id,
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "ZEC",
                "instrument": "ZEC-USDT-SWAP",
                "start_ts": "2026-03-04T13:25:00Z",
                "end_ts": "2026-03-04T15:25:00Z",
                "packet_count": 2,
                "payload_schema": "signal_packet.v2",
                "source_path": "/signals",
                "manifest": {},
            }
        )
    repository.upsert_signal(
        {
            "signal_id": "vegas_ema:ZEC:2026-ZEC-2h-dedupe-vote2:20260304T132500Z",
            "signal_set_key": legacy_key,
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ZEC",
            "instrument": "ZEC-USDT-SWAP",
            "timestamp": "2026-03-04T13:25:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {"timestamp": "2026-03-04T13:25:00Z", "source": "legacy"},
        }
    )
    repository.upsert_signal(
        {
            "signal_id": "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z",
            "signal_set_key": canonical_key,
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ZEC",
            "instrument": "ZEC-USDT-SWAP",
            "timestamp": "2026-03-04T13:25:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {"timestamp": "2026-03-04T13:25:00Z", "source": "canonical"},
        }
    )
    repository.upsert_signal(
        {
            "signal_id": "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T152500Z",
            "signal_set_key": canonical_key,
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ZEC",
            "instrument": "ZEC-USDT-SWAP",
            "timestamp": "2026-03-04T15:25:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {"timestamp": "2026-03-04T15:25:00Z", "source": "canonical"},
        }
    )
    with engine.begin() as connection:
        connection.execute(
            insert(decisions).values(
                decision_id="decision-1",
                stage_run_id="stage-run-1",
                signal_id="vegas_ema:ZEC:2026-ZEC-2h-dedupe-vote2:20260304T132500Z",
                strategy_id="zec-vegas",
                strategy_version="v0.1",
                action="ENTER",
                direction="LONG",
                confidence=0.8,
                reason_code="test",
                execution_profile={},
                diagnostics={},
            )
        )

    report = repository.canonicalize_signal_pools(dry_run=False)
    refreshed = repository.get_signal_set(canonical_key)
    signals_in_window = repository.list_signals_for_signal_set_window(
        signal_set_key=canonical_key,
        window_start="2026-03-04T00:00:00Z",
        window_end="2026-03-05T00:00:00Z",
    )

    assert report["deduped_signal_row_count"] == 1
    assert refreshed["packet_count"] == 2
    assert [signal["signal_id"] for signal in signals_in_window] == [
        "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z",
        "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T152500Z",
    ]
    with engine.connect() as connection:
        stored_decision = connection.execute(select(decisions.c.signal_id)).scalar_one()
        remaining_sets = connection.execute(select(signal_sets.c.signal_set_key)).scalars().all()
        remaining_signals = connection.execute(select(signals.c.signal_id)).scalars().all()
    assert stored_decision == "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z"
    assert remaining_sets == [canonical_key]
    assert sorted(remaining_signals) == [
        "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z",
        "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T152500Z",
    ]


def test_runtime_repository_window_queries_dedupe_duplicate_signal_rows():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    signal_set_key = "vegas_ema:AAVE:AAVE-vegas_ema-canonical"
    repository.upsert_signal_set(
        {
            "signal_set_key": signal_set_key,
            "signal_set_id": "AAVE-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "start_ts": "2026-05-01T00:00:00Z",
            "end_ts": "2026-05-02T00:00:00Z",
            "packet_count": 2,
            "payload_schema": "signal_packet.v2",
            "source_path": "/signals",
            "manifest": {},
        }
    )
    for signal_id in (
        "vegas_ema:AAVE:legacy:20260501T000000Z",
        "vegas_ema:AAVE:AAVE-vegas_ema-canonical:20260501T000000Z",
    ):
        repository.upsert_signal(
            {
                "signal_id": signal_id,
                "signal_set_key": signal_set_key,
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "AAVE",
                "instrument": "AAVE-USDT-SWAP",
                "timestamp": "2026-05-01T00:00:00Z",
                "data_refs": [],
                "payload_schema": "signal_packet.v2",
                "payload": {"timestamp": "2026-05-01T00:00:00Z"},
            }
        )

    counts = repository.signal_counts_by_signal_set_window(
        window_start="2026-05-01T00:00:00Z",
        window_end="2026-05-01T23:59:59Z",
        engine_ids=["vegas_ema"],
    )
    signals_in_window = repository.list_signals_for_signal_set_window(
        signal_set_key=signal_set_key,
        window_start="2026-05-01T00:00:00Z",
        window_end="2026-05-01T23:59:59Z",
    )

    assert counts == {signal_set_key: 1}
    assert [signal["signal_id"] for signal in signals_in_window] == [
        "vegas_ema:AAVE:AAVE-vegas_ema-canonical:20260501T000000Z"
    ]
