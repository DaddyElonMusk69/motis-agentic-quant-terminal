from datetime import UTC, datetime, timedelta

from sqlalchemy import create_engine, insert, select, update

from quant_terminal_api.db.models import decisions, deployment_routes, execution_bundles, metadata, owner_states, signal_sets, signals, stage1_research_sessions, wake_runs
from quant_terminal_api.repositories.runtime import RuntimeRepository


def test_runtime_repository_persists_signal_engine_required_data():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    required_data = [
        {
            "data_type": "candles",
            "origin": "raw",
            "timeframe": "5m",
            "lookback_bars": 20000,
            "freshness_tolerance_seconds": 300,
        },
        {
            "data_type": "candles",
            "origin": "derived",
            "timeframe": "2h",
            "lookback_bars": 676,
            "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        },
    ]

    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "required_data": required_data,
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "configuration_schema": {},
        }
    )

    assert repository.list_signal_engines()[0]["required_data"] == required_data


def test_runtime_repository_serializes_datetime_values_inside_wake_json_payloads():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    now = datetime(2026, 6, 6, 4, 12, 21, tzinfo=UTC)

    stored = repository.record_wake_run(
        {
            "wake_id": "wake-json-safe",
            "route_id": "route-1",
            "bundle_id": "bundle-1",
            "status": "completed",
            "branch": "position_management",
            "blockers": [],
            "exchange_snapshot": {"positions": [{"opened_at": now}]},
            "signal_scan_result": {"status": "skipped_position_open", "checked_at": now},
            "strategy_decision": {"action": "HOLD", "diagnostics": {"created_at": now}},
            "order_intents": [{"intent_id": "intent-1", "created_at": now}],
            "adapter_results": [{"checked_at": now}],
            "error": {},
            "completed_at": now,
        }
    )

    assert stored["strategy_decision"]["diagnostics"]["created_at"] == "2026-06-06T04:12:21Z"
    assert stored["exchange_snapshot"]["positions"][0]["opened_at"] == "2026-06-06T04:12:21Z"


def test_runtime_repository_records_live_signal_observation_by_engine_asset():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    timestamp = datetime(2026, 6, 23, 5, 0, tzinfo=UTC)

    stored = repository.record_live_signal_observation(
        {
            "signal_engine_id": "vegas_ema_5m_cluster",
            "signal_engine_version": "0.1",
            "asset": "btc",
            "instrument": "BTC-USDT-SWAP",
            "signal_id": "sig-live-1",
            "signal_timestamp": timestamp,
            "route_id": "btc-live",
            "bundle_id": "bundle-1",
            "payload_schema": "signal_packet.v2",
            "payload": {"schema_version": "signal_packet.v2", "timestamp": "2026-06-23T05:00:00Z"},
            "decision": {"action": "ENTER", "signal_id": "sig-live-1"},
            "scan_metadata": {"status": "fresh_signal"},
            "observed_at": timestamp,
        }
    )

    repository.record_live_signal_observation(
        {
            **stored,
            "decision": {"action": "SKIP", "signal_id": "sig-live-1"},
        }
    )

    page = repository.list_live_signal_observations(signal_engine_id="vegas_ema_5m_cluster", asset="BTC")

    assert page["total"] == 1
    assert page["observations"][0]["asset"] == "BTC"
    assert page["observations"][0]["route_id"] == "btc-live"
    assert page["observations"][0]["decision"]["action"] == "SKIP"
    assert len(repository.list_signals(signal_engine_id="vegas_ema_5m_cluster", asset="BTC")) == 0


def test_runtime_repository_serializes_datetime_inside_route_lifecycle_error():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = {
        "bundle_id": "arb-vegas-bundle",
        "asset": "ARB",
        "instrument": "ARB-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "arb-vegas",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-arb-vegas",
        "source_stage4_result_path": "stage4-vegas.json",
        "bundle_uri": "artifacts/execution_bundles/arb-vegas",
        "strategy_module_ref": "artifacts/execution_bundles/arb-vegas/strategy.py",
        "execution_setup": {},
        "risk_limits": {"max_notional_usd": 1000},
        "evidence_refs": {},
        "content_hash": "vegas",
        "status": "promoted",
    }
    route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(bundle),
        account_mode="live",
        execution_adapter="okx",
    )
    checked_at = datetime(2026, 6, 23, 7, 30, 0, tzinfo=UTC)

    updated = repository.update_deployment_route_gate(
        route["route_id"],
        last_lifecycle_error={
            "status": "blocked",
            "checked_at": checked_at,
            "nested": {"last_raw_candle_at": checked_at},
        },
    )

    assert updated["last_lifecycle_error"] == {
        "status": "blocked",
        "checked_at": "2026-06-23T07:30:00Z",
        "nested": {"last_raw_candle_at": "2026-06-23T07:30:00Z"},
    }


def test_runtime_repository_bulk_upserts_signals_and_ignores_duplicates():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    base_signal = {
        "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "data_refs": [],
        "payload_schema": "signal_packet.v2",
    }

    repository.upsert_signals(
        [
            {
                **base_signal,
                "signal_id": "signal-1",
                "timestamp": "2026-06-01T00:00:00Z",
                "payload": {"timestamp": "2026-06-01T00:00:00Z"},
            },
            {
                **base_signal,
                "signal_id": "signal-2",
                "timestamp": datetime(2026, 6, 1, 0, 5, tzinfo=UTC),
                "payload": {"timestamp": "2026-06-01T00:05:00Z"},
            },
            {
                **base_signal,
                "signal_id": "signal-1",
                "timestamp": "2026-06-01T00:00:00Z",
                "payload": {"timestamp": "duplicate"},
            },
        ]
    )

    rows = repository.list_signals(signal_set_key="vegas_ema:AAVE:AAVE-vegas_ema-canonical")

    assert [row["signal_id"] for row in rows] == ["signal-1", "signal-2"]
    assert rows[0]["payload"] == {"timestamp": "2026-06-01T00:00:00Z"}


def test_runtime_repository_enqueues_and_dedupes_active_jobs_by_scope():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    first = repository.enqueue_job(
        job_type="stage3_local_variants",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave"},
    )
    duplicate = repository.enqueue_job(
        job_type="stage3_local_variants",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave"},
    )

    assert first["job_id"] == duplicate["job_id"]
    assert first["status"] == "queued"
    assert duplicate["payload"] == {"session_id": "stage1-aave"}

    claimed = repository.claim_next_job(worker_id="worker-1")
    assert claimed["job_id"] == first["job_id"]
    assert claimed["status"] == "running"
    assert claimed["locked_by"] == "worker-1"

    repository.complete_job(claimed["job_id"], result={"ok": True})
    next_job = repository.enqueue_job(
        job_type="stage3_local_variants",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave", "rerun": True},
    )

    assert next_job["job_id"] != first["job_id"]
    assert next_job["status"] == "queued"


def test_runtime_repository_claims_specific_job_without_draining_queue():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    first = repository.enqueue_job(
        job_type="stage1_score",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave"},
    )
    second = repository.enqueue_job(
        job_type="signal_pool_extend",
        scope_key="signal_set:vegas_ema:BTC",
        payload={"asset": "BTC"},
    )

    claimed_second = repository.claim_job(job_id=second["job_id"], worker_id="celery-worker-1")
    claimed_first = repository.claim_next_job(worker_id="legacy-worker-1")

    assert claimed_second["job_id"] == second["job_id"]
    assert claimed_second["locked_by"] == "celery-worker-1"
    assert claimed_first["job_id"] == first["job_id"]
    assert claimed_first["locked_by"] == "legacy-worker-1"


def test_runtime_repository_requeues_expired_running_jobs_before_claiming():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    queued = repository.enqueue_job(
        job_type="signal_pool_extend",
        scope_key="signal_set:vegas_ema:BTC",
        payload={"asset": "BTC"},
    )
    expired = repository.claim_job(job_id=queued["job_id"], worker_id="dead-worker", lock_seconds=-1)

    reclaimed = repository.claim_next_job(worker_id="live-worker")

    assert expired["status"] == "running"
    assert reclaimed["job_id"] == queued["job_id"]
    assert reclaimed["status"] == "running"
    assert reclaimed["locked_by"] == "live-worker"


def test_runtime_repository_heartbeat_extends_running_lock():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    queued = repository.enqueue_job(
        job_type="signal_pool_extend",
        scope_key="signal_set:vegas_ema:BTC",
        payload={"asset": "BTC"},
    )
    claimed = repository.claim_job(job_id=queued["job_id"], worker_id="worker-1", lock_seconds=1)

    refreshed = repository.heartbeat_job(claimed["job_id"], current_step="chunk_1")

    assert refreshed["current_step"] == "chunk_1"
    assert refreshed["lock_expires_at"] > claimed["lock_expires_at"]


def test_runtime_repository_cancels_only_queued_jobs():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    queued = repository.enqueue_job(
        job_type="stage1_score",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave"},
    )
    cancelled = repository.cancel_job(queued["job_id"])
    assert cancelled["status"] == "cancelled"

    running = repository.enqueue_job(
        job_type="stage1_score",
        scope_key="stage1_session:stage1-eth",
        payload={"session_id": "stage1-eth"},
    )
    repository.claim_next_job(worker_id="worker-1")

    assert repository.cancel_job(running["job_id"]) is None


def test_runtime_repository_reports_worker_runtime_status():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    offline = repository.get_worker_runtime_status()
    assert offline["status"] == "offline"
    assert offline["active_worker_count"] == 0

    repository.record_worker_heartbeat("worker-1", status="idle")
    online = repository.get_worker_runtime_status()

    assert online["status"] == "online"
    assert online["online"] is True
    assert online["active_worker_count"] == 1
    assert online["workers"][0]["worker_id"] == "worker-1"


def test_runtime_repository_ignores_generic_celery_unknown_heartbeat():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    repository.record_worker_heartbeat("celery-unknown", status="idle")
    runtime = repository.get_worker_runtime_status()

    assert runtime["status"] == "offline"
    assert runtime["online"] is False
    assert runtime["active_worker_count"] == 0
    assert runtime["workers"] == []


def test_runtime_repository_ignores_generic_celery_unknown_when_real_worker_is_active():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    repository.record_worker_heartbeat("celery-unknown", status="idle")
    repository.record_worker_heartbeat("worker-1", status="idle")
    runtime = repository.get_worker_runtime_status()

    assert runtime["status"] == "online"
    assert runtime["active_worker_count"] == 1
    assert [worker["worker_id"] for worker in runtime["workers"]] == ["worker-1"]


def test_runtime_repository_counts_generic_celery_unknown_as_stale_after_cutoff():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    now = datetime.now(UTC)

    repository.record_worker_heartbeat(
        "celery-unknown",
        status="idle",
        started_at=now - timedelta(seconds=60),
    )

    runtime = repository.get_worker_runtime_status(stale_after_seconds=15)

    assert runtime["status"] == "offline"
    assert runtime["stale_worker_count"] == 0


def test_runtime_repository_job_heartbeat_refreshes_worker_runtime_status():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    job = repository.enqueue_job(
        job_type="stage1_score",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave"},
    )
    claimed = repository.claim_next_job(worker_id="worker-1")

    repository.heartbeat_job(claimed["job_id"], current_step="scoring")
    runtime = repository.get_worker_runtime_status()

    assert job["job_id"] == claimed["job_id"]
    assert runtime["status"] == "online"
    assert runtime["workers"][0]["status"] == "running"
    assert runtime["workers"][0]["current_job_id"] == job["job_id"]
    assert runtime["workers"][0]["current_step"] == "scoring"


def test_runtime_repository_closes_all_open_owner_states_for_route_instrument():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    base = {
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "account_mode": "live",
        "owner_strategy_id": "aave-strategy",
        "owner_strategy_version": "v0.1",
        "opened_from_signal_id": None,
        "status": "open",
        "position_state": {"direction": "LONG"},
    }
    repository.create_owner_state({**base, "owner_state_id": "owner-1", "position_instance_id": "pos-1"})
    repository.create_owner_state({**base, "owner_state_id": "owner-2", "position_instance_id": "pos-2"})
    repository.create_owner_state(
        {
            **base,
            "owner_state_id": "owner-other",
            "route_id": "eth-live",
            "asset": "ETH",
            "instrument": "ETH-USDT-SWAP",
            "position_instance_id": "pos-other",
        }
    )

    closed = repository.close_open_owner_states(
        "aave-live",
        instrument="AAVE-USDT-SWAP",
        reason="exchange_position_flat",
    )

    assert {row["owner_state_id"] for row in closed} == {"owner-1", "owner-2"}
    assert all(row["status"] == "closed" for row in closed)
    assert all(row["position_state"]["close_reason"] == "exchange_position_flat" for row in closed)
    assert repository.get_open_owner_state("aave-live") is None
    assert repository.get_open_owner_state("eth-live")["owner_state_id"] == "owner-other"


def test_runtime_repository_refreshes_signal_engine_required_data_on_reregistration():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    base_registration = {
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
    required_data = [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}]

    repository.register_signal_engine(base_registration)
    repository.register_signal_engine({**base_registration, "required_data": required_data})

    assert repository.list_signal_engines()[0]["required_data"] == required_data


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
        "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-30",
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
    assert stored_run["walk_forward_start"].isoformat() == "2026-05-25"
    assert stored_run["walk_forward_end"].isoformat() == "2026-05-30"
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
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
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
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
            "artifact_root": "dev/training_sessions/sol-vegas-tunnel-v01/stage1-sol",
            "status": "draft",
            "manifest": {
                "session_id": "stage1-sol",
                "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
                "signal_set_id": "2026-SOL-2h-dedupe-vote2",
                "stage0_candidate_id": "candidate-sol-2026",
            },
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
    assert session["signal_set_id"] == "SOL-vegas_ema-canonical"
    assert session["manifest"]["signal_set_key"] == "vegas_ema:SOL:SOL-vegas_ema-canonical"
    assert session["manifest"]["signal_set_id"] == "SOL-vegas_ema-canonical"
    assert session["source_candidate_id"] == "candidate-sol-2026"

    with engine.begin() as connection:
        connection.execute(
            update(stage1_research_sessions)
            .where(stage1_research_sessions.c.session_id == "stage1-sol")
            .values(
                manifest={
                    **session["manifest"],
                    "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
                    "signal_set_id": "2026-SOL-2h-dedupe-vote2",
                }
            )
        )
    repository.canonicalize_signal_pools()
    repaired_session = repository.get_stage1_research_session("stage1-sol")
    assert repaired_session["signal_set_key"] == "vegas_ema:SOL:SOL-vegas_ema-canonical"
    assert repaired_session["manifest"]["signal_set_key"] == "vegas_ema:SOL:SOL-vegas_ema-canonical"
    assert repaired_session["manifest"]["signal_set_id"] == "SOL-vegas_ema-canonical"


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
        "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-31",
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
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-30",
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
        "walk_forward_start": "2026-03-01",
        "walk_forward_end": "2026-03-31",
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


def test_runtime_repository_persists_execution_bundle_route_wake_and_signal_consumption():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = {
        "bundle_id": "aave-vegas_ema-strategy-abc123",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave",
        "source_stage4_result_path": "dev/training_sessions/aave/promotion/stage4_realized_expectancy.json",
        "bundle_uri": "artifacts/execution_bundles/aave-vegas_ema-strategy-abc123",
        "strategy_module_ref": "artifacts/execution_bundles/aave-vegas_ema-strategy-abc123/strategy.py",
        "execution_setup": {
            "stage4_candidate_id": "candidate-1",
            "sizing": {"margin_allocation_pct": 30, "leverage": 5},
        },
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 250},
        "evidence_refs": {"stage4_optimal": "stage4_optimal.json"},
        "content_hash": "abc123",
        "status": "promoted",
    }

    stored_bundle = repository.create_execution_bundle(bundle)
    route = repository.upsert_deployment_route_for_bundle(
        bundle=stored_bundle,
        account_mode="live",
        execution_adapter="okx",
    )
    wake = repository.record_wake_run(
        {
            "wake_id": "wake-1",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "status": "blocked",
            "branch": "route_gate",
            "blockers": route["blockers"],
            "exchange_snapshot": {},
            "signal_scan_result": {"status": "not_run"},
            "strategy_decision": {},
            "order_intents": [],
            "adapter_results": [],
            "error": {},
            "completed_at": "2026-06-05T00:00:00Z",
        }
    )
    assert stored_bundle["status"] == "promoted"
    assert route["route_id"] == "aave-live"
    assert route["enabled"] is False
    assert route["manually_armed"] is False
    assert route["cron_interval_minutes"] == 5
    assert route["exchange_account"] == "default"
    assert route["margin_allocation_pct"] == 30.0
    assert route["leverage"] == 5.0
    assert route["manual_sizing_enabled"] is False
    assert route["active_bundle_id"] == stored_bundle["bundle_id"]
    assert route["blockers"] == ["route_disabled", "data_not_warmed", "route_not_manually_armed"]
    enabled_route = repository.update_deployment_route_gate(
        route["route_id"],
        enabled=True,
        data_warmed=True,
        manually_armed=True,
    )
    assert enabled_route["blockers"] == []
    settings_route = repository.update_deployment_route_gate(
        route["route_id"],
        cron_interval_minutes=30,
        execution_adapter="okx",
        exchange_account="main-live-01",
        margin_allocation_pct=30.0,
        leverage=5.0,
    )
    assert settings_route["cron_interval_minutes"] == 30
    assert settings_route["exchange_account"] == "main-live-01"
    assert settings_route["margin_allocation_pct"] == 30.0
    assert settings_route["leverage"] == 5.0
    assert repository.list_wake_runs(route["route_id"])[0]["wake_id"] == "wake-1"

    updated_wake = repository.update_wake_execution_results(
        wake_id=wake["wake_id"],
        order_intents=[{"intent_id": "intent-1", "status": "submitted"}],
        adapter_results=[{"ordId": "order-1"}],
    )
    owner_state = repository.create_owner_state(
        {
            "owner_state_id": "owner-1",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "position_instance_id": "pos-1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "account_mode": "live",
            "owner_strategy_id": "aave-vegas-tunnel-v01",
            "owner_strategy_version": "v0.1",
            "opened_from_signal_id": "signal-1",
            "status": "open",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted"}]},
        }
    )
    appended_owner_state = repository.append_owner_state_leg(
        "owner-1",
        {"leg": 2, "status": "submitted", "entry_price": "101"},
    )
    closed_owner_state = repository.close_open_owner_state(route["route_id"], reason="exchange_position_flat")

    assert updated_wake["order_intents"] == [{"intent_id": "intent-1", "status": "submitted"}]
    assert updated_wake["adapter_results"] == [{"ordId": "order-1"}]
    assert owner_state["owner_state_id"] == "owner-1"
    assert owner_state["position_instance_id"] == "pos-1"
    assert appended_owner_state["position_state"]["legs"][-1]["leg"] == 2
    assert appended_owner_state["position_state"]["protection_refresh_required"] is True
    assert closed_owner_state["status"] == "closed"
    assert closed_owner_state["position_state"]["close_reason"] == "exchange_position_flat"
    assert repository.get_open_owner_state(route["route_id"]) is None


def test_runtime_repository_reuses_one_deployment_route_per_asset_account_exchange():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    first_bundle = {
        "bundle_id": "aave-vegas-bundle",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave-vegas",
        "source_stage4_result_path": "stage4-vegas.json",
        "bundle_uri": "artifacts/execution_bundles/aave-vegas",
        "strategy_module_ref": "artifacts/execution_bundles/aave-vegas/strategy.py",
        "execution_setup": {},
        "risk_limits": {"max_notional_usd": 1000},
        "evidence_refs": {},
        "content_hash": "vegas",
        "status": "promoted",
    }
    second_bundle = {
        **first_bundle,
        "bundle_id": "aave-bollinger-bundle",
        "signal_engine_id": "bollinger",
        "signal_engine_version": "0.2",
        "strategy_id": "aave-bollinger",
        "source_stage1_session_id": "stage1-aave-bollinger",
        "bundle_uri": "artifacts/execution_bundles/aave-bollinger",
        "strategy_module_ref": "artifacts/execution_bundles/aave-bollinger/strategy.py",
        "content_hash": "bollinger",
    }

    first_route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(first_bundle),
        account_mode="live",
        execution_adapter="okx",
    )
    second_route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(second_bundle),
        account_mode="live",
        execution_adapter="okx",
    )
    repository.update_deployment_route_gate(second_route["route_id"], exchange_account="main-live-01")
    third_bundle = {
        **first_bundle,
        "bundle_id": "aave-okx-alt-account-bundle",
        "source_stage1_session_id": "stage1-aave-alt",
        "bundle_uri": "artifacts/execution_bundles/aave-alt",
        "strategy_module_ref": "artifacts/execution_bundles/aave-alt/strategy.py",
        "content_hash": "alt",
    }
    third_route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(third_bundle),
        account_mode="live",
        execution_adapter="okx",
        exchange_account="default",
    )

    routes = repository.list_deployment_routes()
    assert first_route["route_id"] == "aave-live"
    assert second_route["route_id"] == "aave-live"
    assert third_route["route_id"] == "aave-live-okx-default"
    assert len(routes) == 2
    default_route = next(route for route in routes if route["exchange_account"] == "default")
    live_profile_route = next(route for route in routes if route["exchange_account"] == "main-live-01")
    assert default_route["active_bundle_id"] == "aave-okx-alt-account-bundle"
    assert live_profile_route["active_bundle_id"] == "aave-bollinger-bundle"
    assert live_profile_route["signal_engine_id"] == "bollinger"
    assert live_profile_route["strategy_id"] == "aave-bollinger"
    assert live_profile_route["cron_interval_minutes"] == 5


def test_runtime_repository_archives_deployment_routes_out_of_active_list():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = {
        "bundle_id": "aave-vegas-bundle",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave-vegas",
        "source_stage4_result_path": "stage4-vegas.json",
        "bundle_uri": "artifacts/execution_bundles/aave-vegas",
        "strategy_module_ref": "artifacts/execution_bundles/aave-vegas/strategy.py",
        "execution_setup": {},
        "risk_limits": {"max_notional_usd": 1000},
        "evidence_refs": {},
        "content_hash": "vegas",
        "status": "promoted",
    }
    route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(bundle),
        account_mode="live",
        execution_adapter="okx",
    )
    repository.update_deployment_route_gate(
        route["route_id"],
        enabled=True,
        manually_armed=True,
        scheduler_status="running",
        auto_submit_enabled=True,
        next_wake_at="2026-06-06T05:00:00Z",
    )

    archived = repository.archive_deployment_route(route["route_id"], archived_at="2026-06-06T06:00:00Z")

    assert repository.list_deployment_routes() == []
    archived_routes = repository.list_deployment_routes(include_archived=True)
    assert len(archived_routes) == 1
    assert archived_routes[0]["route_id"] == route["route_id"]
    assert archived_routes[0]["archived"] is True
    assert archived_routes[0]["archived_at"].isoformat() == "2026-06-06T06:00:00+00:00"
    assert archived["enabled"] is False
    assert archived["scheduler_status"] == "stopped"
    assert archived["auto_submit_enabled"] is False
    assert archived["next_wake_at"] is None


def test_runtime_repository_deletes_archived_strategy_route_and_bundle_history():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = {
        "bundle_id": "aave-vegas-bundle-delete",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave-vegas",
        "source_stage4_result_path": "stage4-vegas.json",
        "bundle_uri": "artifacts/execution_bundles/aave-vegas-delete",
        "strategy_module_ref": "artifacts/execution_bundles/aave-vegas-delete/strategy.py",
        "execution_setup": {},
        "risk_limits": {"max_notional_usd": 1000},
        "evidence_refs": {},
        "content_hash": "vegas-delete",
        "status": "promoted",
    }
    stored_bundle = repository.create_execution_bundle(bundle)
    route = repository.upsert_deployment_route_for_bundle(
        bundle=stored_bundle,
        account_mode="live",
        execution_adapter="okx",
    )
    repository.archive_deployment_route(route["route_id"], archived_at="2026-06-06T06:00:00Z")
    repository.record_wake_run(
        {
            "wake_id": "wake-delete-1",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "status": "completed",
            "branch": "entry_scan",
            "blockers": [],
            "exchange_snapshot": {},
            "signal_scan_result": {},
            "strategy_decision": {},
            "order_intents": [],
            "adapter_results": [],
            "error": {},
            "completed_at": "2026-06-06T06:05:00Z",
        }
    )
    repository.create_owner_state(
        {
            "owner_state_id": "owner-delete-1",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "position_instance_id": "pos-delete-1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "account_mode": "live",
            "owner_strategy_id": "aave-vegas",
            "owner_strategy_version": "v0.1",
            "opened_from_signal_id": "signal-1",
            "status": "open",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted"}]},
        }
    )

    summary = repository.delete_archived_strategy_route(route["route_id"])

    assert summary["route_id"] == route["route_id"]
    assert summary["bundle_id"] == stored_bundle["bundle_id"]
    assert summary["deleted_wake_count"] == 1
    assert summary["deleted_owner_state_count"] == 1
    with repository.engine.connect() as connection:
        assert connection.execute(
            select(deployment_routes.c.route_id).where(deployment_routes.c.route_id == route["route_id"])
        ).first() is None
        assert connection.execute(
            select(execution_bundles.c.bundle_id).where(execution_bundles.c.bundle_id == stored_bundle["bundle_id"])
        ).first() is None
        assert connection.execute(
            select(wake_runs.c.wake_id).where(wake_runs.c.route_id == route["route_id"])
        ).first() is None
        assert connection.execute(
            select(owner_states.c.owner_state_id).where(owner_states.c.route_id == route["route_id"])
        ).first() is None


def test_runtime_repository_paginates_wake_runs_and_counts_total():
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = {
        "bundle_id": "aave-vegas-bundle",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave-vegas",
        "source_stage4_result_path": "stage4-vegas.json",
        "bundle_uri": "artifacts/execution_bundles/aave-vegas",
        "strategy_module_ref": "artifacts/execution_bundles/aave-vegas/strategy.py",
        "execution_setup": {},
        "risk_limits": {"max_notional_usd": 1000},
        "evidence_refs": {},
        "content_hash": "vegas-page",
        "status": "promoted",
    }
    route = repository.upsert_deployment_route_for_bundle(
        bundle=repository.create_execution_bundle(bundle),
        account_mode="live",
        execution_adapter="okx",
    )
    for index in range(5):
        repository.record_wake_run(
            {
                "wake_id": f"wake-{index}",
                "route_id": route["route_id"],
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "entry_scan",
                "blockers": [],
                "exchange_snapshot": {},
                "signal_scan_result": {},
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": [],
                "error": {},
                "completed_at": f"2026-06-06T00:0{index}:00Z",
            }
        )

    page = repository.list_wake_run_page(route["route_id"], limit=2, offset=2)

    assert [wake["wake_id"] for wake in page["wakes"]] == ["wake-2", "wake-1"]
    assert page["total"] == 5
    assert page["limit"] == 2
    assert page["offset"] == 2
