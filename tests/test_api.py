import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient

from quant_terminal_api.main import create_app


class StubRuntimeRepository:
    def __init__(self):
        self.stage0_run = None
        self.universe_run = None
        self.universe_runs = {}
        self.universe_candidates_by_run = {}
        self.universe_candidates = []
        self.updated_candidate = None
        self.summary_refreshed = False
        self.stage1_sessions = []
        self.updated_stage1_session = None
        self.window_requests = []
        self.window_signals = None
        self.candle_ref = None

    def list_signal_engines(self):
        if hasattr(self, "signal_engines"):
            return self.signal_engines
        return [
            {
                "signal_engine_id": "vegas_ema",
                "name": "Vegas EMA Tunnel",
                "description": "Legacy engine",
                "version": "0.1",
                "code_ref": {
                    "path": "artifacts/signal_engine",
                    "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py",
                },
                "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
                "live_scanner_entrypoint": "artifacts/signal_engine/scripts/signals/scan_okx_live_signals.py",
                "signal_set_count": 1,
                "packet_count": 340,
            }
        ]

    def list_signal_sets(self, signal_engine_id=None):
        assert signal_engine_id in {"vegas_ema", None}
        return [
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
                "source_path": "/legacy/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2",
                "manifest": {"parameters": {"vote_threshold": 2}},
            }
        ]

    def list_signals(self, **kwargs):
        assert kwargs["signal_set_key"] == "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2"
        return [
            {
                "signal_id": "signal-1",
                "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "BTC",
                "instrument": "BTC-USDT-SWAP",
                "timestamp": "2026-03-02T01:05:00Z",
                "data_refs": ["dev/data/manifests/BTC.json"],
                "payload_schema": "signal_packet.v2",
                "payload": {"active_timeframes": ["2h"], "interactions": []},
            }
        ]

    def get_signal_set(self, signal_set_key):
        assert signal_set_key == "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2"
        return self.list_signal_sets("vegas_ema")[0]

    def create_strategy_development_run(self, run):
        self.stage0_run = run

    def list_strategy_development_runs(self):
        return [self.stage0_run] if self.stage0_run else []

    def get_stage0_universe_run_by_config_hash(self, config_hash):
        for run in self.universe_runs.values():
            if run["config_hash"] == config_hash:
                return run
        if self.universe_run and self.universe_run["config_hash"] == config_hash:
            return self.universe_run
        return None

    def list_stage0_universe_candidates(self, universe_run_id):
        if universe_run_id in self.universe_candidates_by_run:
            return self.universe_candidates_by_run[universe_run_id]
        return self.universe_candidates if self.universe_run and self.universe_run["universe_run_id"] == universe_run_id else []

    def create_stage0_universe(self, run, candidates):
        self.universe_run = run
        self.universe_candidates = candidates
        self.universe_runs[run["universe_run_id"]] = run
        self.universe_candidates_by_run[run["universe_run_id"]] = candidates

    def list_stage0_universe_runs(self):
        if self.universe_runs:
            return list(self.universe_runs.values())
        return [self.universe_run] if self.universe_run else []

    def get_stage0_universe_run(self, universe_run_id):
        if universe_run_id in self.universe_runs:
            return self.universe_runs[universe_run_id]
        return self.universe_run if self.universe_run and self.universe_run["universe_run_id"] == universe_run_id else None

    def get_stage0_universe_candidate(self, candidate_id):
        candidates = [
            candidate
            for rows in self.universe_candidates_by_run.values()
            for candidate in rows
        ] or self.universe_candidates
        for candidate in candidates:
            if candidate["candidate_id"] == candidate_id:
                return candidate
        return None

    def update_stage0_universe_candidate(self, candidate):
        self.updated_candidate = candidate
        self.universe_candidates = [
            candidate if row["candidate_id"] == candidate["candidate_id"] else row
            for row in self.universe_candidates
        ]

    def refresh_stage0_universe_summary(self, universe_run_id):
        self.summary_refreshed = True
        self.universe_run["summary"] = {
            "total_candidates": len(self.universe_candidates),
            "accepted": sum(
                1 for candidate in self.universe_candidates if candidate["acceptance_status"] == "accepted"
            ),
            "watchlist": sum(
                1 for candidate in self.universe_candidates if candidate["acceptance_status"] == "watchlist"
            ),
            "pending_stage0": sum(
                1 for candidate in self.universe_candidates if candidate["acceptance_status"] == "pending_stage0"
            ),
            "failed": sum(1 for candidate in self.universe_candidates if candidate.get("last_error")),
        }
        if self.universe_run["summary"]["pending_stage0"] == 0:
            self.universe_run["status"] = "completed"

    def mark_stage0_universe_candidate_error(self, candidate_id, error):
        self.universe_candidates = [
            {
                **candidate,
                "last_error": error,
            }
            if candidate["candidate_id"] == candidate_id
            else candidate
            for candidate in self.universe_candidates
        ]

    def supersede_stage0_universe_run(self, universe_run_id):
        if self.universe_run and self.universe_run["universe_run_id"] == universe_run_id:
            self.universe_run = {**self.universe_run, "status": "superseded"}

    def delete_stage0_universe_run(self, universe_run_id):
        if self.universe_run and self.universe_run["universe_run_id"] == universe_run_id:
            self.universe_run = None
            self.universe_candidates = []
            self.stage1_sessions = [
                session
                for session in self.stage1_sessions
                if session.get("source_universe_run_id") != universe_run_id
            ]

    def create_stage1_research_session(self, session):
        self.stage1_sessions.append(session)

    def list_stage1_research_sessions(self):
        return self.stage1_sessions

    def get_stage1_research_session(self, session_id):
        for session in self.stage1_sessions:
            if session["session_id"] == session_id:
                return session
        return None

    def latest_stage1_strategy_seed(self, *, asset, signal_engine_id, strategy_id):
        matching_sessions = [
            session
            for session in self.stage1_sessions
            if session["asset"] == asset
            and session["signal_engine_id"] == signal_engine_id
            and session["strategy_id"] == strategy_id
        ]
        if not matching_sessions:
            return None
        latest = matching_sessions[-1]
        artifact_root = Path(latest["artifact_root"])
        frozen_path = artifact_root / "promotion" / "frozen_stage1a_strategy_module" / "strategy.py"
        if frozen_path.exists():
            return {
                "source_type": "latest_pair_frozen",
                "source_path": str(frozen_path),
                "source_version": latest["strategy_version"],
                "source_session_id": latest["session_id"],
            }
        strategy_path = artifact_root / "strategy_module" / "strategy.py"
        if strategy_path.exists():
            return {
                "source_type": "latest_pair_draft",
                "source_path": str(strategy_path),
                "source_version": latest["strategy_version"],
                "source_session_id": latest["session_id"],
            }
        return None

    def update_stage1_research_session_state(self, *, session_id, status, manifest):
        self.updated_stage1_session = {"session_id": session_id, "status": status, "manifest": manifest}
        self.stage1_sessions = [
            {**session, "status": status, "manifest": manifest}
            if session["session_id"] == session_id
            else session
            for session in self.stage1_sessions
        ]

    def list_signals_for_signal_set_window(self, **kwargs):
        self.window_requests.append(kwargs)
        if self.window_signals is not None:
            return self.window_signals
        if getattr(self, "empty_signal_windows", False):
            return []
        return [
            {
                "signal_id": "sig-1",
                "signal_set_key": kwargs["signal_set_key"],
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "AAVE",
                "instrument": "AAVE-USDT-SWAP",
                "timestamp": "2026-04-20T00:00:00Z",
                "data_refs": [],
                "payload_schema": "signal_packet.v2",
                "payload": {"hidden_truth": "LONG"},
            }
        ]

    def stage0_metrics_by_signal_set(self):
        return {
            "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": {
                "trigger_rate_pct": 86,
                "total_valid_signals": 100,
                "triggered_signals": 86,
            }
        }

    def get_candle_ref(self, *, asset, timeframe, origin, data_type="candles"):
        return self.candle_ref

    def existing_rnd_by_signal_set(self):
        return {}

    def signal_counts_by_signal_set_window(self, **kwargs):
        return {"vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": 88}

    def split_signal_counts_by_signal_set(self, **kwargs):
        return {
            "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": {
                "train": 60,
                "walk_forward": 20,
            }
        }


def test_health_endpoint_reports_local_services():
    client = TestClient(create_app())

    response = client.get("/api/v1/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "services": {
            "api": "ready",
            "database": "configured",
            "worker": "configured",
        },
    }


def test_api_allows_local_vite_origin_for_browser_fetches():
    client = TestClient(create_app())

    response = client.options(
        "/api/v1/market-data/catalog",
        headers={
            "Origin": "http://127.0.0.1:5177",
            "Access-Control-Request-Method": "GET",
        },
    )

    assert response.status_code == 200
    assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:5177"


def test_default_walk_forward_templates_are_exposed():
    client = TestClient(create_app())

    response = client.get("/api/v1/walk-forward/templates")

    assert response.status_code == 200
    templates = response.json()["templates"]
    assert templates[0]["template_id"] == "rolling_90d_14d_14d_weekly"
    assert templates[0]["retrain_cadence"] == "7d"
    assert templates[0]["train_range"] == "90d"


def test_agent_task_preview_scopes_prompt_context():
    client = TestClient(create_app())

    response = client.post(
        "/api/v1/agent-tasks/preview",
        json={
            "task_id": "agent-stage1a-iter003",
            "cycle_id": "2026-06-btc-vegas",
            "stage": "stage1a",
            "strategy_id": "vegas_reclaim",
            "strategy_version": "0.1.0",
            "allowed_context_paths": ["agent_tasks/example/failure_clusters.json"],
            "forbidden_context_paths": ["agent_tasks/example/walk_forward_ground_truth.jsonl"],
        },
    )

    assert response.status_code == 200
    prompt = response.json()["prompt"]
    assert "failure_clusters.json" in prompt
    assert "walk_forward_ground_truth.jsonl" not in prompt


def test_signal_engine_catalog_endpoints_expose_sets_and_packets():
    client = TestClient(create_app(runtime_repository=StubRuntimeRepository()))

    engines_response = client.get("/api/v1/signal-engines")
    sets_response = client.get("/api/v1/signal-engines/vegas_ema/signal-sets")
    signals_response = client.get(
        "/api/v1/signals",
        params={"signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2"},
    )

    assert engines_response.status_code == 200
    assert engines_response.json()["engines"][0]["signal_engine_id"] == "vegas_ema"
    assert engines_response.json()["engines"][0]["packet_count"] == 340
    assert sets_response.status_code == 200
    assert sets_response.json()["signal_sets"][0]["manifest"]["parameters"]["vote_threshold"] == 2
    assert signals_response.status_code == 200
    assert signals_response.json()["signals"][0]["payload"] == {
        "active_timeframes": ["2h"],
        "interactions": [],
    }


def test_signal_pool_extend_endpoint_uses_local_extension_service():
    repository = StubRuntimeRepository()
    calls = []

    def extender(**kwargs):
        calls.append(kwargs)
        return {
            "status": "extended",
            "signal_engine_id": kwargs["signal_engine_id"],
            "asset": kwargs["asset"],
            "appended_packet_count": 3,
            "target_end_ts": kwargs["target_end"],
        }

    client = TestClient(create_app(runtime_repository=repository, signal_pool_extension_service=extender))

    response = client.post(
        "/api/v1/signal-engines/vegas_ema/signal-sets/AAVE/extend-local",
        json={"target_end": "2026-06-01T00:00:00Z"},
    )

    assert response.status_code == 200
    assert response.json()["appended_packet_count"] == 3
    assert calls[0]["repository"] is repository
    assert calls[0]["signal_engine_id"] == "vegas_ema"
    assert calls[0]["asset"] == "AAVE"
    assert calls[0]["target_end"] == "2026-06-01T00:00:00Z"


def test_signal_pool_extend_endpoint_reports_local_coverage_blocker():
    def extender(**kwargs):
        raise ValueError("Raw candle data only covers through 2026-05-15T00:00:00Z")

    client = TestClient(
        create_app(
            runtime_repository=StubRuntimeRepository(),
            signal_pool_extension_service=extender,
        )
    )

    response = client.post("/api/v1/signal-engines/vegas_ema/signal-sets/AAVE/extend-local", json={})

    assert response.status_code == 400
    assert response.json()["detail"] == "Raw candle data only covers through 2026-05-15T00:00:00Z"


def test_stage0_run_endpoint_creates_canonical_development_run():
    repository = StubRuntimeRepository()
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage0-runs",
        json={
            "run_id": "stage0-btc-vegas-2026-06",
            "strategy_id": "btc-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "forward_hours": 36,
            "significance_threshold_pct": 0.9,
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "stage0"
    assert payload["status"] == "created"
    assert payload["commands"]["stage0a"][1].endswith("max_travel_distribution.py")
    assert payload["commands"]["stage0b"][1].endswith("significance_threshold_calibration.py")
    assert payload["commands"]["stage0c"][1].endswith("signal_ground_truth.py")

    list_response = client.get("/api/v1/research/runs")
    assert list_response.status_code == 200
    assert list_response.json()["runs"][0]["run_id"] == "stage0-btc-vegas-2026-06"


def test_stage0_universe_endpoint_builds_candidates_and_allows_repeat_same_config():
    repository = StubRuntimeRepository()
    client = TestClient(create_app(runtime_repository=repository))
    request = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T11:55:00Z",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-30",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "engine_ids": ["vegas_ema"],
    }

    response = client.post("/api/v1/research/stage0-universe-runs", json=request)
    repeated_config_response = client.post(
        "/api/v1/research/stage0-universe-runs",
        json={**request, "universe_run_id": "different-id-same-config"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["run"]["status"] == "created"
    assert payload["run"]["train_start"] == "2026-03-01"
    assert payload["run"]["train_end"] == "2026-04-30"
    assert payload["run"]["walk_forward_start"] == "2026-05-25"
    assert payload["run"]["walk_forward_end"] == "2026-05-30"
    assert payload["candidates"][0]["acceptance_status"] == "accepted"
    assert payload["candidates"][0]["branch_path"] == "path_a"
    assert repeated_config_response.status_code == 200
    assert repeated_config_response.json()["run"]["universe_run_id"] == "different-id-same-config"
    assert repeated_config_response.json()["run"]["status"] == "created"


def test_stage0_universe_endpoint_blocks_duplicate_run_id():
    repository = StubRuntimeRepository()
    client = TestClient(create_app(runtime_repository=repository))
    request = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T11:55:00Z",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-30",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "engine_ids": ["vegas_ema"],
    }

    first_response = client.post("/api/v1/research/stage0-universe-runs", json=request)
    duplicate_response = client.post("/api/v1/research/stage0-universe-runs", json=request)

    assert first_response.status_code == 200
    assert duplicate_response.status_code == 409
    assert duplicate_response.json()["detail"] == "stage0 universe run id already exists"


def test_execute_stage0_candidate_endpoint_updates_candidate():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-1",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "packet_count": 100,
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {},
        }
    ]

    def fake_executor(universe_run, candidate):
        return {
            "candidate": {
                **candidate,
                "acceptance_status": "accepted",
                "branch_path": "path_a",
                "trigger_rate_pct": 90,
                "metrics": {"trigger_rate_pct": 90},
            },
            "artifact_root": "/tmp/stage0",
            "commands": {},
        }

    client = TestClient(create_app(runtime_repository=repository, stage0_executor=fake_executor))

    response = client.post(
        "/api/v1/research/stage0-universe-runs/universe-march-may-vegas/candidates/execute",
        json={"candidate_id": "candidate-1"},
    )

    assert response.status_code == 200
    assert response.json()["candidate"]["acceptance_status"] == "accepted"
    assert repository.updated_candidate["trigger_rate_pct"] == 90


def test_execute_stage0_candidate_batch_runs_pending_only_and_reports_partial_failures():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-pending-a",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "packet_count": 100,
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {},
        },
        {
            "candidate_id": "candidate-pending-b",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:ETH:2026-ETH-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ETH",
            "signal_set_id": "2026-ETH-2h-dedupe-vote2",
            "packet_count": 100,
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {},
        },
        {
            "candidate_id": "candidate-already-accepted",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "SOL",
            "signal_set_id": "2026-SOL-2h-dedupe-vote2",
            "packet_count": 100,
            "trigger_rate_pct": 91,
            "branch_path": "path_a",
            "acceptance_status": "accepted",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {"trigger_rate_pct": 91},
        },
    ]
    executed_ids = []

    def fake_executor(universe_run, candidate):
        executed_ids.append(candidate["candidate_id"])
        if candidate["candidate_id"] == "candidate-pending-b":
            raise RuntimeError("missing candle coverage")
        return {
            "candidate": {
                **candidate,
                "acceptance_status": "accepted",
                "branch_path": "path_a",
                "trigger_rate_pct": 90,
                "metrics": {"trigger_rate_pct": 90},
            },
            "artifact_root": "/tmp/stage0",
            "commands": {},
        }

    client = TestClient(create_app(runtime_repository=repository, stage0_executor=fake_executor))

    response = client.post(
        "/api/v1/research/stage0-universe-runs/universe-march-may-vegas/candidates/execute-batch",
        json={"limit": 10},
    )

    assert response.status_code == 200
    payload = response.json()
    assert executed_ids == ["candidate-pending-a", "candidate-pending-b"]
    assert payload["summary"]["requested"] == 2
    assert payload["summary"]["succeeded"] == 1
    assert payload["summary"]["failed"] == 1
    assert payload["summary"]["skipped"] == 1
    assert payload["results"][0]["candidate"]["candidate_id"] == "candidate-pending-a"
    assert payload["errors"][0]["candidate_id"] == "candidate-pending-b"
    assert payload["errors"][0]["detail"] == "missing candle coverage"
    assert repository.universe_candidates[0]["acceptance_status"] == "accepted"
    assert repository.universe_candidates[1]["acceptance_status"] == "pending_stage0"
    assert repository.universe_candidates[1]["last_error"]["detail"] == "missing candle coverage"
    assert repository.summary_refreshed is True


def test_stage0_universe_run_can_be_superseded():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage0-universe-runs/universe-march-may-vegas/supersede")

    assert response.status_code == 200
    assert response.json()["run"]["status"] == "superseded"
    assert repository.universe_run["status"] == "superseded"


def test_stage0_universe_run_can_be_deleted_when_no_stage1_sessions():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-btc",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "packet_count": 329,
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete("/api/v1/research/stage0-universe-runs/universe-march-may-vegas")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "universe_run_id": "universe-march-may-vegas",
        "deleted_stage1_session_count": 0,
        "deleted_stage1_session_ids": [],
    }
    assert repository.universe_run is None
    assert repository.universe_candidates == []


def test_stage0_universe_run_delete_removes_linked_stage1_sessions():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {},
    }
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "source_universe_run_id": "universe-march-may-vegas",
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
            "artifact_root": "/tmp/stage1",
            "status": "draft",
            "manifest": {},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete("/api/v1/research/stage0-universe-runs/universe-march-may-vegas")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "universe_run_id": "universe-march-may-vegas",
        "deleted_stage1_session_count": 1,
        "deleted_stage1_session_ids": ["stage1-aave"],
    }
    assert repository.universe_run is None
    assert repository.stage1_sessions == []


def test_stage1_session_endpoint_creates_draft_from_accepted_stage0_candidate():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "completed",
        "summary": {"accepted": 1, "pending_stage0": 0},
    }
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-aave",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
            "packet_count": 174,
            "trigger_rate_pct": 99.43,
            "branch_path": "path_a",
            "acceptance_status": "accepted",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {"artifact_root": "/tmp/stage0"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": "candidate-aave",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
        },
    )

    assert response.status_code == 200
    payload = response.json()["session"]
    assert payload["status"] == "draft"
    assert payload["source_candidate_id"] == "candidate-aave"
    assert payload["manifest"]["stage0_candidate_id"] == "candidate-aave"
    assert payload["artifact_root"].endswith("/dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas-tunnel-v01-aave-2026-03-01-2026-05-31-candidate-aave")


def test_stage1_session_id_includes_stage0_candidate_to_allow_repeat_windows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    base_strategy_path = tmp_path / "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py"
    base_strategy_path.parent.mkdir(parents=True)
    base_strategy_path.write_text("def decide(context):\n    return {'seed': 'engine-base'}\n")
    repository = StubRuntimeRepository()
    repository.universe_runs = {
        "batch-old": {**_queue_universe_run(), "universe_run_id": "batch-old"},
        "batch-new": {**_queue_universe_run(), "universe_run_id": "batch-new"},
    }
    repository.universe_candidates_by_run = {
        "batch-old": [_queue_candidate("batch-old:vegas_ema:AAVE:canonical", "AAVE", "accepted", 99.43)],
        "batch-new": [_queue_candidate("batch-new:vegas_ema:AAVE:canonical", "AAVE", "accepted", 99.43)],
    }
    client = TestClient(create_app(runtime_repository=repository))
    request = {
        "strategy_id": "aave-vegas_ema-strategy-v01",
        "strategy_version": "v0.1",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-30",
    }

    old_response = client.post(
        "/api/v1/research/stage1-sessions",
        json={**request, "source_candidate_id": "batch-old:vegas_ema:AAVE:canonical"},
    )
    new_response = client.post(
        "/api/v1/research/stage1-sessions",
        json={**request, "source_candidate_id": "batch-new:vegas_ema:AAVE:canonical"},
    )

    assert old_response.status_code == 200
    assert new_response.status_code == 200
    old_session = old_response.json()["session"]
    new_session = new_response.json()["session"]
    assert old_session["session_id"] != new_session["session_id"]
    assert old_session["source_candidate_id"] == "batch-old:vegas_ema:AAVE:canonical"
    assert new_session["source_candidate_id"] == "batch-new:vegas_ema:AAVE:canonical"
    assert len(repository.stage1_sessions) == 2


def test_stage1_session_endpoint_seeds_from_latest_pair_frozen_script(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    prior_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-old"
    frozen_strategy_path = prior_root / "promotion/frozen_stage1a_strategy_module/strategy.py"
    frozen_strategy_path.parent.mkdir(parents=True)
    frozen_strategy_path.write_text("def decide(context):\n    return {'seed': 'latest-frozen'}\n")
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave-old",
            "artifact_root": str(prior_root),
            "source_candidate_id": "candidate-prior",
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
            "walk_forward_end": "2026-03-15",
            "walk_forward_start": "2026-03-16",
            "walk_forward_end": "2026-03-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave-old"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": "candidate-aave",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.2",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
        },
    )

    assert response.status_code == 200
    session = response.json()["session"]
    strategy_path = Path(session["artifact_root"]) / "strategy_module" / "strategy.py"
    assert strategy_path.read_text() == frozen_strategy_path.read_text()
    assert session["seed_strategy_source_type"] == "latest_pair_frozen"
    assert session["seed_strategy_source_path"] == str(frozen_strategy_path)
    assert session["manifest"]["seed_strategy"]["source_session_id"] == "stage1-aave-old"


def test_stage1_session_endpoint_seeds_from_signal_engine_base_when_pair_has_no_prior_script(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    base_strategy_path = tmp_path / "engine_templates/vegas_ema/strategy.py"
    base_strategy_path.parent.mkdir(parents=True)
    base_strategy_path.write_text("def decide(context):\n    return {'seed': 'engine-base'}\n")
    repository.signal_engines = [
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA Tunnel",
            "description": "Legacy engine",
            "version": "0.1",
            "code_ref": {"base_strategy_path": str(base_strategy_path)},
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "live_scanner_entrypoint": "artifacts/signal_engine/scripts/signals/scan_okx_live_signals.py",
            "signal_set_count": 1,
            "packet_count": 340,
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": "candidate-aave",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
        },
    )

    assert response.status_code == 200
    session = response.json()["session"]
    strategy_path = Path(session["artifact_root"]) / "strategy_module" / "strategy.py"
    assert strategy_path.read_text() == base_strategy_path.read_text()
    assert session["seed_strategy_source_type"] == "engine_base"
    assert session["seed_strategy_source_path"] == str(base_strategy_path)
    assert session["manifest"]["seed_strategy"]["source_version"] == "0.1"


def test_stage1_session_endpoint_inherits_windows_from_stage0_batch():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "train_start": "2026-03-01",
        "train_end": "2026-04-15",
        "walk_forward_start": "2026-04-16",
        "walk_forward_end": "2026-05-10",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "completed",
        "summary": {"accepted": 1, "pending_stage0": 0},
    }
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-aave",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
            "packet_count": 174,
            "trigger_rate_pct": 99.43,
            "branch_path": "path_a",
            "acceptance_status": "accepted",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {"artifact_root": "/tmp/stage0"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": "candidate-aave",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
        },
    )

    assert response.status_code == 200
    payload = response.json()["session"]
    assert payload["train_start"] == "2026-03-01"
    assert payload["train_end"] == "2026-04-15"
    assert payload["walk_forward_start"] == "2026-04-16"
    assert payload["walk_forward_end"] == "2026-05-10"
    assert payload["manifest"]["walk_forward_window"] == {"start": "2026-04-16", "end": "2026-05-10"}


def test_stage1_session_endpoint_rejects_unaccepted_stage0_candidate():
    repository = StubRuntimeRepository()
    repository.universe_candidates = [
        {
            "candidate_id": "candidate-btc",
            "universe_run_id": "universe-march-may-vegas",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "packet_count": 329,
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "duplicate_status": "new",
            "existing_strategy_id": None,
            "last_error": {},
            "metrics": {},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": "candidate-btc",
            "strategy_id": "btc-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 1 sessions require an accepted Stage 0 candidate"


def test_stage1_iteration_endpoint_creates_iteration_bundle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    stage0_root = tmp_path / "dev/stage0/universe/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (ground_truth_root / "sig-1.json").write_text(
        '{"signal_id":"sig-1","natural_direction":"LONG","first_move_pct":1.5,"status":"triggered"}'
    )
    repository = StubRuntimeRepository()
    session = {
        "session_id": "stage1-aave",
        "source_universe_run_id": "universe-march-may-vegas",
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
        "stage0_artifact_root": str(stage0_root),
        "artifact_root": str(tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"),
        "status": "draft",
        "manifest": {
            "session_id": "stage1-aave",
            "stage": "stage1a_directional_agreement",
            "stage0_artifact_root": str(stage0_root),
        },
    }
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/iterations",
        json={"sample_method": "training", "bundle_role": "strategy_builder"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["iteration"]["iteration_id"] == "iter_001_v0.1"
    assert "hidden_truth" not in (tmp_path / payload["iteration"]["agent_prompt_path"]).read_text()
    assert (tmp_path / payload["iteration"]["signal_sample_path"]).exists()
    assert (tmp_path / payload["iteration"]["builder_prompt_path"]).exists()
    builder_sample = (tmp_path / payload["iteration"]["builder_training_sample_path"]).read_text()
    evaluator_sample = (tmp_path / payload["iteration"]["signal_sample_path"]).read_text()
    assert "natural_direction" in builder_sample
    assert "natural_direction" not in evaluator_sample


def test_stage1_iteration_endpoint_rejects_frozen_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "source_universe_run_id": "universe-march-may-vegas",
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
            "artifact_root": str(artifact_root),
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/iterations",
        json={"sample_method": "training", "bundle_role": "strategy_builder"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Stage 1 session is frozen"


def test_stage1_iteration_endpoint_uses_sample_method_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session = {
        "session_id": "stage1-aave",
        "source_universe_run_id": "universe-march-may-vegas",
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
        "artifact_root": str(tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"),
        "status": "draft",
        "manifest": {"session_id": "stage1-aave", "stage": "stage1a_directional_agreement"},
    }
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/iterations",
        json={"sample_method": "walk_forward_test", "bundle_role": "evaluator"},
    )

    assert response.status_code == 200
    assert repository.window_requests[-1]["window_start"] == "2026-05-25T00:00:00Z"
    assert repository.window_requests[-1]["window_end"] == "2026-05-31T23:59:59Z"


def test_stage1_iteration_endpoint_uses_training_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    stage0_root = tmp_path / "dev/stage0/universe/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (ground_truth_root / "sig-1.json").write_text(
        '{"signal_id":"sig-1","natural_direction":"LONG","first_move_pct":1.5,"status":"triggered"}'
    )
    repository = StubRuntimeRepository()
    session = {
        "session_id": "stage1-aave",
        "source_universe_run_id": "universe-march-may-vegas",
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
        "stage0_artifact_root": str(stage0_root),
        "artifact_root": str(tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"),
        "status": "draft",
        "manifest": {
            "session_id": "stage1-aave",
            "stage": "stage1a_directional_agreement",
            "stage0_artifact_root": str(stage0_root),
        },
    }
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/iterations",
        json={"sample_method": "training", "bundle_role": "strategy_builder"},
    )

    assert response.status_code == 200
    assert response.json()["iteration"]["sample_method"] == "training"
    assert repository.window_requests[-1]["window_start"] == "2026-03-01T00:00:00Z"
    assert repository.window_requests[-1]["window_end"] == "2026-04-30T23:59:59Z"


def test_stage1_iteration_endpoint_reports_empty_walk_forward_window(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.empty_signal_windows = True
    session = {
        "session_id": "stage1-aave",
        "source_universe_run_id": "universe-march-may-vegas",
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
        "artifact_root": str(tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"),
        "status": "draft",
        "manifest": {"session_id": "stage1-aave", "stage": "stage1a_directional_agreement"},
    }
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/iterations",
        json={"sample_method": "walk_forward_test", "bundle_role": "evaluator"},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "No walk-forward test signals found for Stage 1 session between 2026-05-25 and 2026-05-31"


def test_stage1_iterations_endpoint_lists_persisted_iterations(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "audits").mkdir()
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_001_v0.1", "sample_method": "training", "signal_count": 3})
    )
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "strategy_builder_prompt.md").write_text("builder")
    (iteration_root / "builder_training_sample.json").write_text("{}")
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps({"metrics": {"directional_agreement": 1.0, "matches": 3}})
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/stage1-sessions/stage1-aave/iterations")

    assert response.status_code == 200
    iteration = response.json()["iterations"][0]
    assert iteration["iteration_id"] == "iter_001_v0.1"
    assert iteration["has_training_score"] is True
    assert iteration["training_score"]["metrics"]["matches"] == 3
    assert iteration["manifest_path"].startswith("dev/training_sessions/")


def test_stage1_iteration_delete_endpoint_removes_iteration_folder(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    iteration_root.mkdir(parents=True)
    (iteration_root / "manifest.json").write_text(json.dumps({"iteration_id": "iter_001_v0.1"}))
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "session_id": "stage1-aave",
        "iteration_id": "iter_001_v0.1",
    }
    assert not iteration_root.exists()


def test_stage1_iteration_agent_prompt_prefers_failure_audit_prompt(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    iteration_root.mkdir(parents=True)
    (iteration_root / "agent_prompt.md").write_text("generic handoff")
    (iteration_root / "strategy_builder_prompt.md").write_text("builder prompt")
    (iteration_root / "agent_failure_audit_prompt.md").write_text("audit prompt")
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/agent-prompt")

    assert response.status_code == 200
    payload = response.json()
    assert payload["prompt_type"] == "failure_audit"
    assert payload["prompt"] == "audit prompt"
    assert payload["prompt_path"].endswith("agent_failure_audit_prompt.md")


def test_stage1_training_score_endpoint_scores_iteration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    packets_root = tmp_path / "packets"
    strategy_root = iteration_root / "strategy_module"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "api-score",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "LONG",
        "confidence": 0.7,
        "reason_code": "api_test",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "sig-1.json").write_text('{"signal_id":"sig-1","payload":{}}')
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "packet_path": str(packets_root / "sig-1.json")}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "ground_truth": {"natural_direction": "LONG"}}]})
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/score-training")

    assert response.status_code == 200
    result = response.json()["score"]
    assert result["metrics"]["directional_agreement"] == 1
    assert (tmp_path / result["scores_path"]).exists()
    assert (tmp_path / result["decisions_path"]).exists()


def test_stage1_walk_forward_score_endpoint_scores_iteration(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    stage0_root = tmp_path / "dev/stage0/aave"
    packets_root = tmp_path / "packets"
    strategy_root = artifact_root / "strategy_module"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    ground_truth_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (artifact_root / "manifest.json").write_text(json.dumps({"stage0_artifact_root": str(stage0_root)}))
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "api-walk-forward",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "SHORT",
        "confidence": 0.7,
        "reason_code": "api_walk_forward",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "20260501T000000Z.json").write_text('{"signal_id":"20260501T000000Z","payload":{}}')
    (ground_truth_root / "20260501T000000Z.json").write_text(
        '{"signal_id":"20260501T000000Z","natural_direction":"SHORT"}'
    )
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "signal_id": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2:20260501T000000Z",
                        "packet_path": str(packets_root / "20260501T000000Z.json"),
                    }
                ]
            }
        )
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/score-walk-forward")

    assert response.status_code == 200
    result = response.json()["score"]
    assert result["metrics"]["matches"] == 1
    assert result["scores_path"].endswith("scores/stage1a_walk_forward_scores.json")


def test_stage1_gate_endpoint_reports_blockers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_001_v0.1", "sample_method": "training", "signal_count": 1})
    )
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps({"metrics": {"directional_agreement": 1, "matches": 1, "passes_threshold": True}})
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/stage1-sessions/stage1-aave/gate")

    assert response.status_code == 200
    gate = response.json()["gate"]
    assert gate["ready_to_freeze"] is False
    assert gate["roles"]["training"]["status"] == "pass"
    assert gate["roles"]["walk_forward_test"]["status"] == "missing"
    assert len(gate["blockers"]) == 1


def test_stage1_canonical_endpoint_blocks_until_gate_passes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/canonical-stage1a")

    assert response.status_code == 400
    assert response.json()["detail"]["message"].startswith("Stage 1A canonical readout requires")


def test_stage1_canonical_endpoint_writes_readout_and_freezes_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    stage0_root = tmp_path / "dev/stage0/aave"
    packet_root = tmp_path / "dev/signals/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2/packets"
    strategy_root = artifact_root / "strategy_module"
    (stage0_root / "scores/ground_truth").mkdir(parents=True)
    packet_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (stage0_root / "scores/ground_truth/sig-1.json").write_text(
        json.dumps({"signal_id": "sig-1", "natural_direction": "LONG"})
    )
    (packet_root / "sig-1.json").write_text(json.dumps({"signal_id": "sig-1", "payload": {}}))
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "canonical-api",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "LONG",
        "confidence": 0.8,
        "reason_code": "api_canonical",
        "diagnostics": {},
    }
"""
    )
    role_files = {
        "training": "stage1a_directional_scores.json",
        "walk_forward_test": "stage1a_walk_forward_scores.json",
    }
    for index, (role, score_name) in enumerate(role_files.items(), start=1):
        iteration_root = artifact_root / "iterations" / f"iter_{index:03d}_v0.1"
        (iteration_root / "scores").mkdir(parents=True)
        (iteration_root / "decisions").mkdir()
        (iteration_root / "summaries").mkdir()
        (iteration_root / "manifest.json").write_text(
            json.dumps({"iteration_id": f"iter_{index:03d}_v0.1", "sample_method": role, "signal_count": 1})
        )
        (iteration_root / "signal_sample.json").write_text("{}")
        (iteration_root / "agent_prompt.md").write_text("prompt")
        (iteration_root / "scores" / score_name).write_text(
            json.dumps({"metrics": {"directional_agreement": 1, "matches": 1, "passes_threshold": True}})
        )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "stage0_artifact_root": str(stage0_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave", "stage0_artifact_root": str(stage0_root)},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/canonical-stage1a")

    assert response.status_code == 200
    readout = response.json()["canonical_readout"]
    assert readout["metrics"]["matches"] == 2
    assert readout["scores_path"].endswith("promotion/stage1a_canonical_full_cycle_scores.json")
    assert (tmp_path / readout["decisions_path"]).exists()
    assert repository.updated_stage1_session["status"] == "stage1a_frozen"
    assert len(repository.window_requests) == 2


def test_stage2_capture_endpoint_rejects_unfrozen_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage2/capture-curve")

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 2 requires a frozen canonical Stage 1A readout"


def test_stage2_capture_endpoint_writes_curve_from_canonical_match_set(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "metrics": {"matches": 1},
                "match_set": [
                    {
                        "signal_id": "sig-1",
                        "sample_role": "walk_forward_test",
                        "decision_direction": "LONG",
                        "ground_truth_direction": "LONG",
                    }
                ],
            }
        )
    )
    storage_uri = tmp_path / "data/market/source=okx/type=candles/asset=AAVE/timeframe=5m/origin=raw"
    parquet_path = storage_uri / "year=2026/month=05/data.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-05-01T00:05:00Z",
                    "open": 100,
                    "high": 101.1,
                    "low": 99.5,
                    "close": 101,
                    "volume": 1,
                    "confirm": 1,
                }
            ]
        ),
        parquet_path,
    )
    repository.candle_ref = {
        "dataset_id": "okx-aave-raw-5m",
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
    }
    repository.window_signals = [
        {
            "signal_id": "sig-1",
            "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "timestamp": "2026-05-01T00:00:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        }
    ]
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage2/capture-curve")

    assert response.status_code == 200
    stage2 = response.json()["stage2_capture"]
    assert stage2["metrics"]["total_match_signals"] == 1
    assert stage2["results"]["1.0"]["walk_forward_test"] == {"reached": 1, "total": 1, "rate": 100.0}
    assert stage2["capture_curve_path"].endswith("promotion/stage2_capture_curve.json")
    assert (tmp_path / stage2["capture_curve_path"]).exists()
    assert len(repository.window_requests) == 2


def test_stage3_grid_endpoint_rejects_until_stage2_complete(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage3/grid-search")

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 3 requires completed Stage 2 travel capture"


def test_stage3_grid_endpoint_writes_grid_and_stage4_candidates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps({"metrics": {"total_match_signals": 1}, "results": {}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text(
        json.dumps(
            [
                {
                    "signal_id": "sig-1",
                    "sample_role": "walk_forward_test",
                    "direction": "LONG",
                    "signal_ts": "2026-05-01T00:00:00Z",
                    "reference_price": 100,
                }
            ]
        )
    )
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    storage_uri = tmp_path / "data/market/source=okx/type=candles/asset=AAVE/timeframe=5m/origin=raw"
    parquet_path = storage_uri / "year=2026/month=05/data.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-05-01T00:05:00Z",
                    "open": 100,
                    "high": 102.5,
                    "low": 99.5,
                    "close": 102,
                    "volume": 1,
                    "confirm": 1,
                }
            ]
        ),
        parquet_path,
    )
    repository.candle_ref = {
        "dataset_id": "okx-aave-raw-5m",
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
    }
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage3/grid-search")

    assert response.status_code == 200
    stage3 = response.json()["stage3_grid"]
    assert stage3["total_signals"] == 1
    assert stage3["optimal"]["best"]["tp_count"] == 1
    assert stage3["stage4_candidates_path"].endswith("promotion/stage4_candidates.json")
    assert (tmp_path / stage3["stage4_candidates_path"]).exists()


def test_stage3_pyramid_endpoint_requires_grid_search(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps({"metrics": {"total_match_signals": 1}, "results": {}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage3/pyramid")

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 3 pyramid requires completed Stage 3 grid search"


def test_stage3_pyramid_endpoint_writes_pyramid_and_stage4_candidate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps({"metrics": {"total_match_signals": 1}, "results": {}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text(
        json.dumps(
            [
                {
                    "signal_id": "sig-1",
                    "sample_role": "walk_forward_test",
                    "direction": "LONG",
                    "signal_ts": "2026-05-01T00:00:00Z",
                    "reference_price": 100,
                }
            ]
        )
    )
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"total_signals": 1, "optimal": {"best": {"tp": 1.0, "sl": 1.0}}})
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 1.0, "sl": 1.0}}))
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps({"candidates": [{"candidate_id": "market_tp_1p0_sl_1p0", "setup": {"entry_model": "market"}}]})
    )
    (promotion_root / "stage3_summary.md").write_text("# Stage 3 Grid Search\n")
    storage_uri = tmp_path / "data/market/source=okx/type=candles/asset=AAVE/timeframe=5m/origin=raw"
    parquet_path = storage_uri / "year=2026/month=05/data.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-05-01T00:05:00Z",
                    "open": 100,
                    "high": 101.5,
                    "low": 99.8,
                    "close": 101.2,
                    "volume": 1,
                    "confirm": 1,
                },
                {
                    "timestamp": "2026-05-01T00:10:00Z",
                    "open": 101.2,
                    "high": 102.0,
                    "low": 100.4,
                    "close": 101.8,
                    "volume": 1,
                    "confirm": 1,
                },
            ]
        ),
        parquet_path,
    )
    repository.candle_ref = {
        "dataset_id": "okx-aave-raw-5m",
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
    }
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage3/pyramid")

    assert response.status_code == 200
    pyramid = response.json()["stage3_pyramid"]
    assert pyramid["baseline"]["pnl_pct"] == 5.0
    assert pyramid["optimal"]["best"]["comparison"] == "BETTER"
    assert pyramid["stage4_candidates_path"].endswith("promotion/stage4_candidates.json")
    candidates = json.loads((tmp_path / pyramid["stage4_candidates_path"]).read_text())["candidates"]
    assert candidates[-1]["setup"]["max_legs"] == 3


def test_stage4_endpoint_requires_stage3_pyramid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)
    (promotion_root / "stage4_candidates.json").write_text(json.dumps({"candidates": []}))
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage4/realized-expectancy")

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 4 requires completed Stage 3 pyramid"


def test_stage4_endpoint_writes_realized_expectancy_from_full_decision_set(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-enter",
                        "agent_direction": "LONG",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                        "sample_role": "training",
                    },
                    {
                        "signal_id": "sig-skip",
                        "agent_direction": "FLAT",
                        "decision_direction": "FLAT",
                        "agreement": "NEUTRAL",
                        "sample_role": "walk_forward_test",
                    },
                ]
            }
        )
    )
    (promotion_root / "stage3_pyramid_results.json").write_text(json.dumps({"total_signals": 1}))
    (promotion_root / "stage3_pyramid_optimal.json").write_text(json.dumps({"best": {"step_pct": 0.5}}))
    (promotion_root / "stage3_pyramid_summary.md").write_text("# Stage 3 Pyramid\n")
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "market_tp_1p0_sl_1p0",
                        "setup": {
                            "entry_model": "market",
                            "tp_pct": 1.0,
                            "sl_pct": 1.0,
                            "timeout_policy": "close_at_cutoff",
                        },
                    }
                ]
            }
        )
    )
    repository.window_signals = [
        {
            "signal_id": "sig-enter",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T00:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 100}],
                "active_timeframes": ["2h"],
            },
        },
        {
            "signal_id": "sig-skip",
            "timestamp": "2026-05-01T01:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T01:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 200}],
                "active_timeframes": ["2h"],
            },
        },
    ]
    storage_uri = tmp_path / "data/market/source=okx/type=candles/asset=AAVE/timeframe=5m/origin=raw"
    parquet_path = storage_uri / "year=2026/month=05/data.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-05-01T00:05:00Z",
                    "open": 100,
                    "high": 101.2,
                    "low": 99.8,
                    "close": 101,
                    "volume": 1,
                    "confirm": 1,
                },
                {
                    "timestamp": "2026-05-01T01:05:00Z",
                    "open": 200,
                    "high": 201,
                    "low": 199,
                    "close": 200,
                    "volume": 1,
                    "confirm": 1,
                },
            ]
        ),
        parquet_path,
    )
    repository.candle_ref = {
        "dataset_id": "okx-aave-raw-5m",
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
    }
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage4/realized-expectancy")

    assert response.status_code == 200
    stage4 = response.json()["stage4_realized_expectancy"]
    assert stage4["best_candidate"]["total_decisions"] == 2
    assert stage4["best_candidate"]["skipped_decisions"] == 1
    assert stage4["realized_expectancy_path"].endswith("promotion/stage4_realized_expectancy.json")
    assert (tmp_path / stage4["optimal_path"]).exists()


def test_stage1_score_endpoint_rejects_frozen_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    iteration_root.mkdir(parents=True)
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/score-training")

    assert response.status_code == 409
    assert response.json()["detail"] == "Stage 1 session is frozen"


def test_stage1_failure_audit_endpoint_generates_audit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    (iteration_root / "audits").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "packet_path": str(tmp_path / "sig-1.json")}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "ground_truth": {"natural_direction": "LONG"}}]})
    )
    (iteration_root / "decisions/stage1a_directional_decisions.json").write_text(
        json.dumps({"decisions": [{"signal_id": "sig-1", "direction": "FLAT", "trade_action": "SKIP"}]})
    )
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps(
            {
                "metrics": {"total": 1, "matches": 0, "mismatches": 0, "neutral": 1},
                "records": [
                    {
                        "signal_id": "sig-1",
                        "agreement": "NEUTRAL",
                        "ground_truth_direction": "LONG",
                        "decision_direction": "FLAT",
                    }
                ],
            }
        )
    )
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
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
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/generate-failure-audit")

    assert response.status_code == 200
    audit = response.json()["audit"]
    assert audit["metrics"]["failure_count"] == 1
    assert (tmp_path / audit["audit_json_path"]).exists()
    assert (tmp_path / audit["agent_prompt_path"]).exists()


def test_development_queue_marks_accepted_candidate_without_session_stage1_not_started(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [
        _queue_candidate("candidate-aave", "AAVE", "accepted", 14.43, total_records=1199, packet_count=174)
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["candidate_id"] == "candidate-aave"
    assert row["stage0_evaluated_signal_count"] == 1199
    assert row["packet_count"] == 174
    assert row["development_status"] == "stage1_not_started"
    assert row["next_action"]["type"] == "start_stage1"
    assert row["next_action"]["disabled"] is False


def test_development_queue_draft_session_missing_training_requests_training_bundle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    repository.stage1_sessions = [_queue_session(tmp_path, "candidate-aave", "AAVE", "draft")]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage1_in_progress"
    assert row["stage1_gate"]["roles"]["training"]["status"] == "missing"
    assert row["next_action"]["type"] == "create_training_bundle"


def test_development_queue_training_pass_requests_walk_forward_bundle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "draft")
    _write_stage1_score(session, "iter_001_v0.1", "training", "stage1a_directional_scores.json")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["stage1_gate"]["roles"]["walk_forward_test"]["status"] == "missing"
    assert row["next_action"]["type"] == "create_walk_forward_bundle"


def test_development_queue_training_and_walk_forward_pass_request_canonical_readout(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "draft")
    _write_stage1_score(session, "iter_001_v0.1", "training", "stage1a_directional_scores.json")
    _write_stage1_score(session, "iter_002_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["stage1_gate"]["ready_to_freeze"] is True
    assert row["next_action"]["type"] == "run_canonical_stage1a"


def test_development_queue_walk_forward_fail_blocks_same_cycle_training(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "draft")
    _write_stage1_score(session, "iter_001_v0.1", "training", "stage1a_directional_scores.json")
    _write_stage1_score(session, "iter_002_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json", passes=False)
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["stage1_gate"]["roles"]["walk_forward_test"]["status"] == "fail"
    assert row["next_action"]["type"] == "walk_forward_failed_new_cycle"
    assert row["next_action"]["disabled"] is True


def test_development_queue_canonical_readout_marks_stage1_frozen(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    _write_stage1_score(session, "iter_001_v0.1", "training", "stage1a_directional_scores.json")
    _write_stage1_score(session, "iter_002_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json")
    _write_stage1_score(session, "iter_003_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage1_frozen"
    assert row["current_stage"] == "stage2_ready"
    assert row["next_action"]["type"] == "run_stage2_capture_curve"
    assert row["stage1_gate"]["canonical_readout"]["exists"] is True


def test_development_queue_stage2_capture_complete_locks_stage3_until_runner_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    _write_stage1_score(session, "iter_001_v0.1", "training", "stage1a_directional_scores.json")
    _write_stage1_score(session, "iter_002_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps({"metrics": {"total_match_signals": 1}, "results": {}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage2_complete"
    assert row["current_stage"] == "stage3_ready"
    assert row["next_action"]["type"] == "run_stage3_grid_search"
    assert row["stage1_gate"]["stage2_capture"]["exists"] is True


def test_development_queue_stage3_grid_complete_requests_pyramid(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"metrics": {"total_match_signals": 1}}))
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"total_signals": 1, "optimal": {"best": {"tp": 2.5, "sl": 1.0}}})
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 2.5, "sl": 1.0}}))
    (promotion_root / "stage4_candidates.json").write_text(json.dumps({"candidates": [{"candidate_id": "market"}]}))
    (promotion_root / "stage3_summary.md").write_text("# Stage 3 Grid Search\n")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage3_grid_complete"
    assert row["current_stage"] == "stage3_pyramid_ready"
    assert row["next_action"]["type"] == "run_stage3_pyramid"
    assert row["stage1_gate"]["stage3_grid"]["exists"] is True
    assert row["stage1_gate"]["stage3_pyramid"]["exists"] is False


def test_development_queue_stage3_pyramid_complete_requests_stage4(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"metrics": {"total_match_signals": 1}}))
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"total_signals": 1, "optimal": {"best": {"tp": 2.5, "sl": 1.0}}})
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 2.5, "sl": 1.0}}))
    (promotion_root / "stage3_summary.md").write_text("# Stage 3 Grid Search\n")
    (promotion_root / "stage3_pyramid_results.json").write_text(
        json.dumps({"total_signals": 1, "tp_pct": 2.5, "sl_pct": 1.0, "baseline": {}, "results": []})
    )
    (promotion_root / "stage3_pyramid_optimal.json").write_text(json.dumps({"best": {"step_pct": 0.5, "pnl_pct": 12.0}}))
    (promotion_root / "stage4_candidates.json").write_text(json.dumps({"candidates": [{"candidate_id": "pyramid"}]}))
    (promotion_root / "stage3_pyramid_summary.md").write_text("# Stage 3 Pyramid\n")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage3_complete"
    assert row["current_stage"] == "stage4_ready"
    assert row["next_action"]["type"] == "run_stage4_realized_expectancy"
    assert row["stage1_gate"]["stage3_pyramid"]["exists"] is True


def test_development_queue_stage4_complete_marks_promotion_review_ready(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({"best_candidate_id": "market", "best_candidate": {"candidate_id": "market"}, "candidates": []})
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(json.dumps({"best": {"candidate_id": "market"}}))
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage4_complete"
    assert row["current_stage"] == "promotion_review_ready"
    assert row["next_action"]["type"] == "review_promotion"
    assert row["stage1_gate"]["stage4_realized_expectancy"]["exists"] is True


def test_development_queue_includes_watchlist_and_pending_as_non_startable(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [
        _queue_candidate("candidate-btc", "BTC", "pending_stage0", None),
        _queue_candidate("candidate-eth", "ETH", "watchlist", 72.4),
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    rows = {row["candidate_id"]: row for row in response.json()["queue"]}
    assert rows["candidate-btc"]["development_status"] == "stage0_pending"
    assert rows["candidate-btc"]["next_action"]["disabled"] is True
    assert rows["candidate-eth"]["development_status"] == "watchlist_not_startable"
    assert rows["candidate-eth"]["next_action"]["disabled"] is True


def _queue_universe_run():
    return {
        "universe_run_id": "universe-march-may-vegas",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "completed",
        "summary": {"accepted": 1, "pending_stage0": 0, "watchlist": 0},
    }


def _queue_candidate(candidate_id, asset, acceptance_status, trigger_rate_pct, *, total_records=None, packet_count=100):
    metrics = {"trigger_rate_pct": trigger_rate_pct} if trigger_rate_pct is not None else {}
    if total_records is not None:
        metrics["total_records"] = total_records
    return {
        "candidate_id": candidate_id,
        "universe_run_id": "universe-march-may-vegas",
        "signal_set_key": f"vegas_ema:{asset}:2026-{asset}-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": asset,
        "signal_set_id": f"2026-{asset}-2h-dedupe-vote2",
        "packet_count": packet_count,
        "trigger_rate_pct": trigger_rate_pct,
        "branch_path": "path_a" if acceptance_status == "accepted" else "path_b",
        "acceptance_status": acceptance_status,
        "duplicate_status": "new",
        "existing_strategy_id": None,
        "last_error": {},
        "metrics": metrics,
    }


def _queue_session(tmp_path, candidate_id, asset, status):
    artifact_root = f"dev/training_sessions/{asset.lower()}-vegas-tunnel-v01/stage1-{asset.lower()}"
    (tmp_path / artifact_root / "iterations").mkdir(parents=True)
    (tmp_path / artifact_root / "promotion").mkdir()
    return {
        "session_id": f"stage1-{asset.lower()}",
        "source_universe_run_id": "universe-march-may-vegas",
        "source_candidate_id": candidate_id,
        "signal_set_key": f"vegas_ema:{asset}:2026-{asset}-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": asset,
        "signal_set_id": f"2026-{asset}-2h-dedupe-vote2",
        "strategy_id": f"{asset.lower()}-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
        "walk_forward_end": "2026-05-31",
        "artifact_root": artifact_root,
        "status": status,
        "manifest": {"session_id": f"stage1-{asset.lower()}"},
    }


def _write_stage1_score(session, iteration_id, sample_method, score_filename, *, passes=True):
    iteration_root = Path(session["artifact_root"]) / "iterations" / iteration_id
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": iteration_id, "sample_method": sample_method, "signal_count": 1})
    )
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "scores" / score_filename).write_text(
        json.dumps({"metrics": {"directional_agreement": 1 if passes else 0.4, "matches": 1 if passes else 0, "passes_threshold": passes}})
    )
