import json
from datetime import timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select
from sqlalchemy.pool import StaticPool

import quant_terminal_api.main as api_main
from quant_terminal_api.db.models import deployment_routes, execution_bundles, metadata, owner_states, wake_runs
from quant_terminal_api.main import create_app
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_worker.execution.bundle_loader import load_strategy_module
from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy


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
        self.execution_bundles = []
        self.updated_stage1_session = None
        self.window_requests = []
        self.window_signals = None
        self.candle_ref = None
        self.signal_sets = []
        self.candle_refs = {}

    def list_signal_engines(self):
        if hasattr(self, "signal_engines"):
            return self.signal_engines
        return [
            {
                "signal_engine_id": "vegas_ema",
                "name": "Vegas EMA Tunnel",
                "description": "Legacy engine",
                "version": "0.1",
                "created_at": "2026-06-01T00:00:00Z",
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
        if self.signal_sets:
            if signal_engine_id is None:
                return list(self.signal_sets)
            return [signal_set for signal_set in self.signal_sets if signal_set["signal_engine_id"] == signal_engine_id]
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
        self.last_list_signals_kwargs = kwargs
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
        if signal_set_key == "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2":
            return self.list_signal_sets("vegas_ema")[0]
        for signal_set in self.signal_sets:
            if signal_set["signal_set_key"] == signal_set_key:
                return signal_set
        return None

    def register_signal_engine(self, registration):
        engines = list(self.list_signal_engines())
        stored = {
            "signal_engine_id": registration["signal_engine_id"],
            "name": registration["name"],
            "description": registration.get("description", ""),
            "version": registration["version"],
            "created_at": registration.get("created_at") or "2026-06-15T00:00:00Z",
            "code_ref": registration.get("code_ref", {}),
            "required_data": registration.get("required_data", []),
            "output_envelope_version": registration.get("output_envelope_version", "signal_packet.v2"),
            "runtime_entrypoint": registration.get("runtime_entrypoint"),
            "live_scanner_entrypoint": registration.get("live_scanner_entrypoint"),
            "configuration_schema": registration.get("configuration_schema", {}),
            "signal_set_count": 0,
            "packet_count": 0,
        }
        self.signal_engines = [
            stored if engine["signal_engine_id"] == stored["signal_engine_id"] else engine
            for engine in engines
            if engine["signal_engine_id"] != stored["signal_engine_id"]
        ]
        self.signal_engines.append(stored)

    def update_signal_engine(self, signal_engine_id, **values):
        engines = list(self.list_signal_engines())
        self.signal_engines = [
            {**engine, **values} if engine["signal_engine_id"] == signal_engine_id else engine
            for engine in engines
        ]
        return next((engine for engine in self.signal_engines if engine["signal_engine_id"] == signal_engine_id), None)

    def upsert_signal_set(self, registration):
        existing = [item for item in self.signal_sets if item["signal_set_key"] != registration["signal_set_key"]]
        self.signal_sets = [*existing, registration]

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

    def append_stage0_universe_candidates(self, universe_run_id, candidates):
        existing = list(self.universe_candidates_by_run.get(universe_run_id, self.universe_candidates))
        existing_keys = {candidate["signal_set_key"] for candidate in existing}
        appended = [candidate for candidate in candidates if candidate["signal_set_key"] not in existing_keys]
        merged = [*existing, *appended]
        self.universe_candidates = merged
        self.universe_candidates_by_run[universe_run_id] = merged

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

    def delete_stage1_research_session(self, session_id):
        self.stage1_sessions = [session for session in self.stage1_sessions if session["session_id"] != session_id]

    def list_execution_bundles_for_stage1_session(self, session_id):
        return [
            bundle
            for bundle in self.execution_bundles
            if bundle.get("source_stage1_session_id") == session_id
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
        if self.candle_refs:
            return self.candle_refs.get((asset.upper(), data_type, origin, timeframe))
        return self.candle_ref

    def get_data_ref(self, *, asset, timeframe, origin, data_type):
        return self.get_candle_ref(asset=asset, timeframe=timeframe, origin=origin, data_type=data_type)

    def existing_rnd_by_signal_set(self):
        return {}

    def signal_counts_by_signal_set_window(self, **kwargs):
        if hasattr(self, "window_signal_counts"):
            return self.window_signal_counts
        if self.signal_sets:
            return {
                signal_set["signal_set_key"]: int(signal_set.get("packet_count", 0) or 0)
                for signal_set in self.signal_sets
            }
        return {"vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": 88}

    def split_signal_counts_by_signal_set(self, **kwargs):
        if hasattr(self, "split_window_signal_counts"):
            return self.split_window_signal_counts
        if self.signal_sets:
            counts = {}
            for signal_set in self.signal_sets:
                packet_count = int(signal_set.get("packet_count", 0) or 0)
                counts[signal_set["signal_set_key"]] = {
                    "train": packet_count if packet_count > 0 else 0,
                    "walk_forward": packet_count if packet_count > 0 else 0,
                }
            return counts
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


def test_job_runtime_endpoint_reports_worker_status(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'jobs-runtime.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    repository.record_worker_heartbeat("worker-api-test", status="idle")
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/jobs/runtime")

    assert response.status_code == 200
    payload = response.json()["worker_runtime"]
    assert payload["status"] == "online"
    assert payload["active_worker_count"] == 1
    assert payload["workers"][0]["worker_id"] == "worker-api-test"


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
    vegas_engine = next(engine for engine in engines_response.json()["engines"] if engine["signal_engine_id"] == "vegas_ema")
    assert vegas_engine["packet_count"] == 340
    assert sets_response.status_code == 200
    assert sets_response.json()["signal_sets"][0]["manifest"]["parameters"]["vote_threshold"] == 2
    assert signals_response.status_code == 200
    assert signals_response.json()["signals"][0]["payload"] == {
        "active_timeframes": ["2h"],
        "interactions": [],
    }


def test_list_signals_accepts_descending_order_request():
    repository = StubRuntimeRepository()
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(
        "/api/v1/signals",
        params={
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "descending": "true",
        },
    )

    assert response.status_code == 200
    assert repository.last_list_signals_kwargs["descending"] is True


def test_signal_engine_catalog_includes_repo_registry_entries(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry_root = tmp_path / "artifacts" / "signal_engine"
    registry_root.mkdir(parents=True)
    (registry_root / "engine_registry.json").write_text(
        json.dumps(
            {
                "bollinger": {
                    "signal_engine_id": "bollinger",
                    "version": "0.1",
                    "name": "Bollinger Bands",
                    "description": "Contract registry engine",
                    "code_ref": {
                        "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/bollinger_base.py"
                    },
                    "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
                    "output_envelope_version": "signal_packet.v2",
                    "runtime_entrypoint": "quant_terminal_worker.signal_engines.bollinger:generate_training_signals",
                    "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.bollinger:scan_live_signal",
                    "configuration_schema": {},
                }
            }
        )
    )
    repository = StubRuntimeRepository()
    repository.signal_engines = []
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/signal-engines")

    assert response.status_code == 200
    engines = response.json()["engines"]
    assert engines[0]["signal_engine_id"] == "bollinger"
    assert engines[0]["signal_set_count"] == 0
    assert engines[0]["packet_count"] == 0


def test_signal_engine_catalog_orders_by_created_at_descending():
    repository = StubRuntimeRepository()
    repository.signal_engines = [
        {
            "signal_engine_id": "old_engine",
            "name": "Old Engine",
            "description": "Older",
            "version": "0.1",
            "created_at": "2026-06-01T00:00:00Z",
            "code_ref": {},
            "required_data": [],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "old:generate",
            "live_scanner_entrypoint": "old:scan",
            "configuration_schema": {},
            "signal_set_count": 0,
            "packet_count": 0,
        },
        {
            "signal_engine_id": "new_engine",
            "name": "New Engine",
            "description": "Newer",
            "version": "0.1",
            "created_at": "2026-06-16T00:00:00Z",
            "code_ref": {},
            "required_data": [],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "new:generate",
            "live_scanner_entrypoint": "new:scan",
            "configuration_schema": {},
            "signal_set_count": 0,
            "packet_count": 0,
        },
        {
            "signal_engine_id": "undated_engine",
            "name": "Undated Engine",
            "description": "Missing timestamp",
            "version": "0.1",
            "code_ref": {},
            "required_data": [],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "undated:generate",
            "live_scanner_entrypoint": "undated:scan",
            "configuration_schema": {},
            "signal_set_count": 0,
            "packet_count": 0,
        },
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/signal-engines")

    assert response.status_code == 200
    engines = response.json()["engines"]
    ordered_ids = [engine["signal_engine_id"] for engine in engines if engine["signal_engine_id"] in {"new_engine", "old_engine", "undated_engine"}]
    assert ordered_ids == ["new_engine", "old_engine", "undated_engine"]
    assert next(engine for engine in engines if engine["signal_engine_id"] == "new_engine")["created_at"] == "2026-06-16T00:00:00Z"


def test_signal_engine_rename_materializes_registry_engine(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry_root = tmp_path / "artifacts" / "signal_engine"
    registry_root.mkdir(parents=True)
    (registry_root / "engine_registry.json").write_text(
        json.dumps(
            {
                "bollinger": {
                    "signal_engine_id": "bollinger",
                    "version": "0.1",
                    "name": "Bollinger Bands",
                    "description": "Contract registry engine",
                    "code_ref": {},
                    "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
                    "output_envelope_version": "signal_packet.v2",
                    "runtime_entrypoint": "quant_terminal_worker.signal_engines.bollinger:generate_training_signals",
                    "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.bollinger:scan_live_signal",
                    "configuration_schema": {},
                }
            }
        )
    )
    repository = StubRuntimeRepository()
    repository.signal_engines = []
    client = TestClient(create_app(runtime_repository=repository))

    response = client.patch("/api/v1/signal-engines/bollinger", json={"name": "BB Mean Reversion"})

    assert response.status_code == 200
    assert response.json()["engine"]["name"] == "BB Mean Reversion"
    assert repository.list_signal_engines()[0]["signal_engine_id"] == "bollinger"
    assert repository.list_signal_engines()[0]["name"] == "BB Mean Reversion"


def test_signal_pool_create_requires_engine_data_refs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry_root = tmp_path / "artifacts" / "signal_engine"
    registry_root.mkdir(parents=True)
    (registry_root / "engine_registry.json").write_text(
        json.dumps(
            {
                "bollinger": {
                    "signal_engine_id": "bollinger",
                    "version": "0.1",
                    "name": "Bollinger Bands",
                    "description": "Contract registry engine",
                    "code_ref": {},
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
                    "configuration_schema": {"default_parameters": {"vote_threshold": 2}},
                }
            }
        )
    )
    repository = StubRuntimeRepository()
    repository.signal_engines = []
    repository.candle_refs = {
        ("AAVE", "candles", "raw", "5m"): {
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "dataset_id": "AAVE-raw-5m",
            "data_type": "candles",
            "data_origin": "raw",
            "timeframe": "5m",
        }
    }
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/signal-engines/bollinger/signal-sets", json={"asset": "AAVE"})

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing required local data for AAVE: derived candles 4h"


def test_signal_pool_create_adds_canonical_pool_for_data_asset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry_root = tmp_path / "artifacts" / "signal_engine"
    registry_root.mkdir(parents=True)
    (registry_root / "engine_registry.json").write_text(
        json.dumps(
            {
                "bollinger": {
                    "signal_engine_id": "bollinger",
                    "version": "0.1",
                    "name": "Bollinger Bands",
                    "description": "Contract registry engine",
                    "code_ref": {},
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
                    "configuration_schema": {"default_parameters": {"vote_threshold": 2}},
                }
            }
        )
    )
    repository = StubRuntimeRepository()
    repository.signal_engines = []
    repository.candle_refs = {
        ("AAVE", "candles", "raw", "5m"): {
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "dataset_id": "AAVE-raw-5m",
            "data_type": "candles",
            "data_origin": "raw",
            "timeframe": "5m",
        },
        ("AAVE", "candles", "derived", "4h"): {
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "dataset_id": "AAVE-derived-4h",
            "data_type": "candles",
            "data_origin": "derived",
            "timeframe": "4h",
        },
    }
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/signal-engines/bollinger/signal-sets", json={"asset": "AAVE"})

    assert response.status_code == 200
    signal_set = response.json()["signal_set"]
    assert signal_set["signal_set_key"] == "bollinger:AAVE:AAVE-bollinger-canonical"
    assert signal_set["signal_set_id"] == "AAVE-bollinger-canonical"
    assert signal_set["packet_count"] == 0
    assert signal_set["manifest"]["parameters"] == {"vote_threshold": 2}
    assert signal_set["manifest"]["data_refs"] == ["AAVE-raw-5m", "AAVE-derived-4h"]


def test_signal_pool_create_accepts_feature_required_data_refs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    registry_root = tmp_path / "artifacts" / "signal_engine"
    registry_root.mkdir(parents=True)
    (registry_root / "engine_registry.json").write_text(
        json.dumps(
            {
                "feature_engine": {
                    "signal_engine_id": "feature_engine",
                    "version": "0.1",
                    "name": "Feature Engine",
                    "description": "Feature-backed contract engine",
                    "code_ref": {},
                    "required_data": [
                        {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                        {"data_type": "feature_base_candle", "origin": "derived", "timeframe": "5m"},
                    ],
                    "output_envelope_version": "signal_packet.v2",
                    "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:generate_training_signals",
                    "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:scan_live_signal",
                    "configuration_schema": {"default_parameters": {"feature_timeframes": ["5m"]}},
                }
            }
        )
    )
    repository = StubRuntimeRepository()
    repository.signal_engines = []
    repository.candle_refs = {
        ("AAVE", "candles", "raw", "5m"): {
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "dataset_id": "AAVE-raw-5m",
            "data_type": "candles",
            "data_origin": "raw",
            "timeframe": "5m",
        },
        ("AAVE", "feature_base_candle", "derived", "5m"): {
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "dataset_id": "AAVE-feature-base-5m",
            "data_type": "feature_base_candle",
            "data_origin": "derived",
            "timeframe": "5m",
        },
    }
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/signal-engines/feature_engine/signal-sets", json={"asset": "AAVE"})

    assert response.status_code == 200
    signal_set = response.json()["signal_set"]
    assert signal_set["signal_set_key"] == "feature_engine:AAVE:AAVE-feature_engine-canonical"
    assert signal_set["manifest"]["data_refs"] == ["AAVE-raw-5m", "AAVE-feature-base-5m"]


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
        "name": "March-May Vegas Training Pool",
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
    assert payload["run"]["name"] == "March-May Vegas Training Pool"
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


def test_stage0_universe_run_delete_blocks_when_linked_session_has_execution_bundle():
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    repository.execution_bundles = [
        {
            "bundle_id": "bundle-aave",
            "source_stage1_session_id": "stage1-aave",
            "status": "promoted",
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete("/api/v1/research/stage0-universe-runs/universe-march-may-vegas")

    assert response.status_code == 409
    assert response.json()["detail"] == "Training pool has linked promoted execution bundles"
    assert repository.universe_run is not None
    assert repository.stage1_sessions[0]["session_id"] == "stage1-aave"


def test_stage0_universe_run_lists_appendable_assets_for_same_engine_with_generated_signals():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "walk_forward_start": "2026-05-01",
        "walk_forward_end": "2026-05-30",
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
    repository.signal_sets = [
        {
            "signal_set_key": "vegas_ema:ETH:2026-ETH-2h-dedupe-vote2",
            "signal_set_id": "2026-ETH-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ETH",
            "instrument": "ETH-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 280,
            "payload_schema": "signal_packet.v2",
            "source_path": "/legacy/vegas_ema/ETH/2026-ETH-2h-dedupe-vote2",
            "manifest": {"parameters": {"vote_threshold": 2}},
        },
        {
            "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
            "signal_set_id": "2026-SOL-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "SOL",
            "instrument": "SOL-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 0,
            "payload_schema": "signal_packet.v2",
            "source_path": "/legacy/vegas_ema/SOL/2026-SOL-2h-dedupe-vote2",
            "manifest": {"parameters": {"vote_threshold": 2}},
        },
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/stage0-universe-runs/universe-march-may-vegas/appendable-assets")

    assert response.status_code == 200
    assert response.json()["assets"] == ["ETH"]


def test_stage0_universe_run_appends_new_tickers_without_rebuilding_existing_candidates():
    repository = StubRuntimeRepository()
    repository.universe_run = {
        "universe_run_id": "universe-march-may-vegas",
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "walk_forward_start": "2026-05-01",
        "walk_forward_end": "2026-05-30",
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-30T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "config_hash": "hash",
        "engine_filter": ["vegas_ema"],
        "status": "created",
        "summary": {"total_candidates": 1, "pending_stage0": 1},
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
    repository.signal_sets = [
        {
            "signal_set_key": "vegas_ema:ETH:2026-ETH-2h-dedupe-vote2",
            "signal_set_id": "2026-ETH-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ETH",
            "instrument": "ETH-USDT-SWAP",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 280,
            "payload_schema": "signal_packet.v2",
            "source_path": "/legacy/vegas_ema/ETH/2026-ETH-2h-dedupe-vote2",
            "manifest": {"parameters": {"vote_threshold": 2}},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage0-universe-runs/universe-march-may-vegas/append-assets",
        json={"assets": ["ETH"]},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["added_candidate_count"] == 1
    assert {candidate["asset"] for candidate in payload["added_candidates"]} == {"ETH"}
    assert {candidate["asset"] for candidate in repository.universe_candidates} == {"BTC", "ETH"}
    assert repository.summary_refreshed is True


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


def test_stage1_session_endpoint_can_force_engine_base_over_latest_pair_seed(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.universe_run = _queue_universe_run()
    repository.universe_candidates = [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)]
    prior_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-old"
    frozen_strategy_path = prior_root / "promotion/frozen_stage1a_strategy_module/strategy.py"
    frozen_strategy_path.parent.mkdir(parents=True)
    frozen_strategy_path.write_text("def decide(context):\n    return {'seed': 'latest-frozen'}\n")
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
            "seed_strategy_preference": "engine_base",
        },
    )

    assert response.status_code == 200
    session = response.json()["session"]
    strategy_path = Path(session["artifact_root"]) / "strategy_module" / "strategy.py"
    assert strategy_path.read_text() == base_strategy_path.read_text()
    assert session["seed_strategy_source_type"] == "engine_base"
    assert session["seed_strategy_source_path"] == str(base_strategy_path)


def test_stage1_session_reset_endpoint_returns_candidate_to_clean_slate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    (artifact_root / "strategy_module").mkdir(parents=True)
    (artifact_root / "strategy_module" / "strategy.py").write_text("def decide(context):\n    return {}\n")
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "source_candidate_id": "candidate-aave",
            "source_universe_run_id": "universe-a",
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

    response = client.delete("/api/v1/research/stage1-sessions/stage1-aave")

    assert response.status_code == 200
    assert response.json() == {
        "status": "deleted",
        "session_id": "stage1-aave",
        "source_candidate_id": "candidate-aave",
    }
    assert repository.stage1_sessions == []
    assert not artifact_root.exists()


def test_stage1_session_reset_allows_frozen_unpromoted_session(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "canonical_stage1a_decisions.json").write_text("{}\n")
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "source_candidate_id": "candidate-aave",
            "source_universe_run_id": "universe-a",
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

    response = client.delete("/api/v1/research/stage1-sessions/stage1-aave")

    assert response.status_code == 200
    assert repository.stage1_sessions == []
    assert not artifact_root.exists()


def test_stage1_session_reset_blocks_promoted_execution_bundle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    artifact_root.mkdir(parents=True)
    repository.stage1_sessions = [
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "source_candidate_id": "candidate-aave",
            "source_universe_run_id": "universe-a",
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
    repository.execution_bundles = [
        {
            "bundle_id": "bundle-aave",
            "source_stage1_session_id": "stage1-aave",
            "status": "promoted",
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete("/api/v1/research/stage1-sessions/stage1-aave")

    assert response.status_code == 409
    assert response.json()["detail"] == "Stage 1 session has a promoted execution bundle"
    assert repository.stage1_sessions[0]["session_id"] == "stage1-aave"
    assert artifact_root.exists()


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


def test_stage1_training_score_endpoint_enqueues_job_with_real_repository(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    iteration_root.mkdir(parents=True)
    repository.create_stage1_research_session(
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "source_candidate_id": "candidate-aave",
            "source_universe_run_id": "universe-aave",
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
    )
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/score-training")

    assert response.status_code == 200
    payload = response.json()
    assert payload["accepted"] is True
    assert payload["job"]["status"] == "queued"
    assert payload["job"]["job_type"] == "stage1_score"
    assert payload["job"]["scope_key"] == "stage1_session:stage1-aave"
    assert payload["job"]["payload"]["iteration_id"] == "iter_001_v0.1"
    assert not (iteration_root / "scores" / "stage1a_directional_scores.json").exists()


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


def test_stage1_canonical_endpoint_allows_forced_freeze_below_gate(tmp_path, monkeypatch):
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
    for index, (role, score_name, passes) in enumerate(
        (
            ("training", "stage1a_directional_scores.json", True),
            ("walk_forward_test", "stage1a_walk_forward_scores.json", False),
        ),
        start=1,
    ):
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
            json.dumps(
                {
                    "metrics": {
                        "directional_agreement": 1.0 if passes else 0.4,
                        "matches": 1 if passes else 0,
                        "mismatches": 0 if passes else 1,
                        "neutral": 0,
                        "scoreable": 1,
                        "total": 1,
                        "promotion_threshold_pct": 55.0,
                        "passes_threshold": passes,
                    }
                }
            )
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

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/canonical-stage1a",
        json={"force": True},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["forced"] is True
    assert payload["gate"]["canonical_readout"]["exists"] is True
    assert repository.updated_stage1_session["status"] == "stage1a_frozen"


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


def test_stage1_iteration_detail_endpoint_returns_records_and_monthly_clusters(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_001_v0.1", "sample_method": "training", "signal_count": 3})
    )
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {"signal_id": "sig-1", "timestamp": "2026-03-02T00:00:00Z", "packet_path": "dev/signals/a.json"},
                    {"signal_id": "sig-2", "timestamp": "2026-03-18T00:00:00Z", "packet_path": "dev/signals/b.json"},
                    {"signal_id": "sig-3", "timestamp": "2026-04-02T00:00:00Z", "packet_path": "dev/signals/c.json"},
                ]
            }
        )
    )
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps(
            {
                "metrics": {
                    "total": 3,
                    "matches": 1,
                    "mismatches": 1,
                    "neutral": 1,
                    "scoreable": 2,
                    "directional_agreement": 0.5,
                    "promotion_threshold_pct": 55.0,
                    "passes_threshold": False,
                },
                "records": [
                    {
                        "signal_id": "sig-1",
                        "ground_truth_direction": "LONG",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                        "status": "CORRECT",
                        "confidence": 0.7,
                        "reason_code": "trend_match",
                    },
                    {
                        "signal_id": "sig-2",
                        "ground_truth_direction": "SHORT",
                        "decision_direction": "LONG",
                        "agreement": "MISMATCH",
                        "status": "INCORRECT",
                        "confidence": 0.6,
                        "reason_code": "wrong_side",
                    },
                    {
                        "signal_id": "sig-3",
                        "ground_truth_direction": "LONG",
                        "decision_direction": "FLAT",
                        "agreement": "NEUTRAL",
                        "status": "NEUTRAL",
                        "confidence": 0.4,
                        "reason_code": "skip",
                    },
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

    response = client.get("/api/v1/research/stage1-sessions/stage1-aave/iterations/iter_001_v0.1/details")

    assert response.status_code == 200
    detail = response.json()["detail"]
    assert detail["iteration_id"] == "iter_001_v0.1"
    assert detail["sample_role"] == "training"
    assert detail["metrics"]["matches"] == 1
    assert len(detail["records"]) == 3
    assert detail["records"][0]["timestamp"] == "2026-03-02T00:00:00Z"
    assert detail["monthly"][0]["month"] == "2026-03"
    assert detail["monthly"][0]["metrics"]["total"] == 2
    assert detail["monthly"][1]["month"] == "2026-04"
    assert detail["monthly"][1]["metrics"]["neutral"] == 1


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


def test_stage2_exit_policy_endpoint_writes_policy_and_updates_gate(tmp_path, monkeypatch):
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
        json.dumps(
            {
                "tp_levels": [0.5, 1.0, 1.5],
                "sl_levels": [0.3, 0.5, 0.8],
                "metrics": {"total_match_signals": 1},
                "results": {"0.5": {}, "1.0": {}, "1.5": {}},
                "sl_results": {"0.3": {}, "0.5": {}, "0.8": {}},
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage2/exit-policy",
        json={"lock_profit_pct": 1.0, "initial_sl_pct": 0.5, "protect_trigger_pct": 0.5, "trail_sl_pct": 0.5},
    )

    assert response.status_code == 200
    policy = response.json()["stage2_exit_policy"]
    assert policy["exists"] is True
    assert policy["policy"]["lock_profit_pct"] == 1.0
    assert policy["policy"]["initial_sl_pct"] == 0.5
    assert policy["policy"]["protect_trigger_pct"] == 0.5
    assert policy["policy"]["trail_sl_pct"] == 0.5
    assert (promotion_root / "stage2_exit_policy.json").exists()
    assert response.json()["gate"]["stage2_exit_policy"]["exists"] is True


def test_stage2_exit_policy_endpoint_writes_side_specific_handoff(tmp_path, monkeypatch):
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
        json.dumps(
            {
                "tp_levels": [0.5, 1.0, 1.5],
                "sl_levels": [0.3, 0.5, 0.8],
                "metrics": {"total_match_signals": 1},
                "results": {"0.5": {}, "1.0": {}, "1.5": {}},
                "sl_results": {"0.3": {}, "0.5": {}, "0.8": {}},
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage2/exit-policy",
        json={
            "side_policies": {
                "LONG": {"lock_profit_pct": 1.5, "initial_sl_pct": 0.5, "protect_trigger_pct": 1.0, "trail_sl_pct": 0.5},
                "SHORT": {"lock_profit_pct": 1.0, "initial_sl_pct": 0.8, "protect_trigger_pct": 0.5, "trail_sl_pct": 0.5},
            }
        },
    )

    assert response.status_code == 200
    policy = response.json()["stage2_exit_policy"]
    assert policy["policy_mode"] == "side_specific"
    assert policy["side_policies"]["LONG"]["lock_profit_pct"] == 1.5
    assert policy["side_policies"]["SHORT"]["initial_sl_pct"] == 0.8
    persisted = json.loads((promotion_root / "stage2_exit_policy.json").read_text())
    assert persisted["policy_mode"] == "side_specific"
    assert persisted["side_policies"]["LONG"]["protect_trigger_pct"] == 1.0
    assert persisted["side_policies"]["SHORT"]["lock_profit_pct"] == 1.0
    assert persisted["policy"] == persisted["side_policies"]["LONG"]


def test_stage2_exit_policy_endpoint_rejects_legacy_capture_without_sl_band(tmp_path, monkeypatch):
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
        json.dumps(
            {
                "tp_levels": [0.5, 1.0, 1.5],
                "metrics": {"total_match_signals": 1},
                "results": {"0.5": {}, "1.0": {}, "1.5": {}},
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage2/exit-policy",
        json={
            "side_policies": {
                "LONG": {"lock_profit_pct": 1.5, "initial_sl_pct": 0, "protect_trigger_pct": 1.0, "trail_sl_pct": 0.5},
                "SHORT": {"lock_profit_pct": 1.0, "initial_sl_pct": 0, "protect_trigger_pct": 0.5, "trail_sl_pct": 0.5},
            }
        },
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 2 exit policy requires a matched adverse SL band. Rerun Stage 2 capture to rebuild the SL curve."


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


def test_stage3_grid_endpoint_rejects_until_stage2_exit_policy_promoted(tmp_path, monkeypatch):
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
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"tp_levels": [1.0], "metrics": {"total_match_signals": 1}, "results": {"1.0": {}}}))
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
            "status": "stage1a_frozen",
            "manifest": {"session_id": "stage1-aave"},
        }
    ]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage1-sessions/stage1-aave/stage3/grid-search")

    assert response.status_code == 400
    assert response.json()["detail"] == "Stage 3 requires promoted Stage 2 exit policy"


def test_stage3_grid_endpoint_writes_grid_and_stage4_candidates(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    stage0_root = tmp_path / "dev/stage0/universe-1/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2"
    (stage0_root / "scores").mkdir(parents=True)
    (stage0_root / "scores" / "ground_truth_summary.json").write_text(
        json.dumps({"metrics": {"significance_threshold_pct": 1.0, "forward_hours": 36}})
    )
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {}\n")
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "match_set": [{"signal_id": "sig-1"}]})
    )
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps(
                {
                    "tp_levels": [0.5, 1.0],
                    "sl_levels": [0.5, 1.0],
                    "metrics": {"total_match_signals": 1, "total_trade_decisions": 1, "match_count": 1, "mismatch_count": 0},
                    "results": {},
                    "sl_results": {},
                "stage3_input": {
                    "tp_range_source": "stage2_trade_profile",
                    "recommended_tp_min_pct": 0.1,
                    "recommended_tp_max_pct": 1.0,
                    "sl_range_source": "stage2_matched_adverse_profile",
                    "recommended_sl_min_pct": 0.5,
                    "recommended_sl_max_pct": 1.0,
                },
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text(
        json.dumps(
            [
                {
                    "signal_id": "sig-1",
                    "sample_role": "walk_forward_test",
                    "decision_direction": "LONG",
                    "direction": "LONG",
                    "agreement": "MATCH",
                    "signal_ts": "2026-05-01T00:00:00Z",
                    "reference_price": 100,
                }
            ]
        )
    )
    (promotion_root / "stage2_exit_policy.json").write_text(
        json.dumps(
                {
                    "schema_version": "0.1",
                    "artifact_role": "stage2_exit_policy",
                    "policy": {"lock_profit_pct": 1.0, "initial_sl_pct": 0.5, "protect_trigger_pct": 0.5, "trail_sl_pct": 0.5},
                }
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
                "stage0_artifact_root": str(stage0_root),
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
        json.dumps(
            {
                "metrics": {"total_match_signals": 1, "total_trade_decisions": 1, "match_count": 1, "mismatch_count": 0},
                "results": {},
                "stage3_input": {
                    "tp_range_source": "stage2_trade_profile",
                    "recommended_tp_min_pct": 0.1,
                    "recommended_tp_max_pct": 1.0,
                },
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
    assert response.json()["detail"] == "Stage 3 pyramid requires completed Stage 3 policy test"


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
        json.dumps({"tp_levels": [0.5, 1.0], "metrics": {"total_match_signals": 1}, "results": {}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text(
        json.dumps(
            [
                {
                    "signal_id": "sig-1",
                    "sample_role": "walk_forward_test",
                    "decision_direction": "LONG",
                    "direction": "LONG",
                    "agreement": "MATCH",
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
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "numeric_exact_tp_1p0_sl_1p0",
                        "setup": {"entry_model": "market", "tp_pct": 1.0, "sl_pct": 1.0, "protect_trigger_pct": 0.5},
                    }
                ]
            }
        )
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
    assert pyramid["baseline"]["pnl_pct"] == 0.9
    assert pyramid["optimal"]["best"]["comparison"] == "BETTER"
    assert pyramid["stage4_candidates_path"].endswith("promotion/stage4_candidates.json")
    candidates = json.loads((tmp_path / pyramid["stage4_candidates_path"]).read_text())["candidates"]
    pyramid_candidates = [candidate for candidate in candidates if candidate["candidate_id"].startswith("pyramid_")]
    assert pyramid_candidates
    assert {candidate["setup"]["pyramid_step_pct"] for candidate in pyramid_candidates} <= {0.1, 0.2, 0.3, 0.4, 0.5}
    assert {candidate["setup"]["max_legs"] for candidate in pyramid_candidates} <= {2, 3}


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

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage4/realized-expectancy",
        json={"initial_capital_usdt": 1000, "margin_allocation_pct": 30, "leverage": 5},
    )

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

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage4/realized-expectancy",
        json={"initial_capital_usdt": 1000, "margin_allocation_pct": 30, "leverage": 5},
    )

    assert response.status_code == 200
    stage4 = response.json()["stage4_realized_expectancy"]
    assert stage4["best_candidate"]["total_decisions"] == 2
    assert stage4["best_candidate"]["skipped_decisions"] == 1
    assert stage4["best_candidate"]["account"]["initial_capital_usdt"] == 1000
    assert stage4["run_id"]
    assert stage4["realized_expectancy_path"].endswith("promotion/stage4_realized_expectancy.json")
    assert (tmp_path / stage4["optimal_path"]).exists()
    gate_stage4 = response.json()["gate"]["stage4_realized_expectancy"]
    assert gate_stage4["latest_run_id"] == stage4["run_id"]
    assert len(gate_stage4["stage4_runs"]) == 1


def test_stage4_run_delete_endpoint_restores_previous_latest(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session, signals, candles = _stage4b_api_fixture(tmp_path, repository)
    promotion_root = Path(session["artifact_root"]) / "promotion"
    first_index = json.loads((promotion_root / "stage4_runs" / "index.json").read_text())
    first_run_id = first_index["latest_run_id"]
    client = TestClient(create_app(runtime_repository=repository))

    second = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=2000,
        margin_allocation_pct=20,
        leverage=3,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    second_run_id = second["run_id"]
    assert second_run_id != first_run_id

    delete_response = client.delete(f"/api/v1/research/stage1-sessions/{session['session_id']}/stage4/runs/{second_run_id}")

    assert delete_response.status_code == 200
    delete_result = delete_response.json()["stage4_run_delete"]
    assert delete_result["deleted_run_id"] == second_run_id
    assert delete_result["latest_run_id"] == first_run_id
    assert delete_result["remaining_run_count"] == 1
    assert delete_response.json()["gate"]["stage4_realized_expectancy"]["latest_run_id"] == first_run_id
    latest = json.loads((promotion_root / "stage4_realized_expectancy.json").read_text())
    assert latest["run_id"] == first_run_id
    assert not (promotion_root / "stage4_runs" / second_run_id).exists()


def test_stage4b_timing_prompt_endpoint_writes_prompt_and_updates_gate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session, _signals, _candles = _stage4b_api_fixture(tmp_path, repository)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/stage4/timing-prompt")

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage4b_timing_prompt"]["prompt_type"] == "stage4b_timing_optimizer"
    assert "$stage4b-timing-optimizer" in payload["stage4b_timing_prompt"]["prompt"]
    assert payload["gate"]["stage4b_timing"]["prompt_exists"] is True
    assert payload["gate"]["stage4b_timing"]["overlay_exists"] is False


def test_stage4b_timing_replay_endpoint_applies_overlay_and_updates_gate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session, _signals, _candles = _stage4b_api_fixture(tmp_path, repository)
    promotion_root = Path(session["artifact_root"]) / "promotion"
    stage4 = json.loads((promotion_root / "stage4_realized_expectancy.json").read_text())
    timing_root = promotion_root / "stage4b_timing"
    timing_root.mkdir(parents=True)
    (timing_root / "timing_overlay.json").write_text(
        json.dumps(
            {
                "schema_version": "stage4b_timing_overlay.v1",
                "source_stage4_run_id": stage4["run_id"],
                "exclude_utc_hours": [1],
                "applies_to": "all",
                "rationale": "Skip 01 UTC for API coverage.",
            }
        )
    )
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/stage4/timing-replay")

    assert response.status_code == 200
    replay = response.json()["stage4b_timing"]
    assert replay["best_candidate"]["executed_trades"] == 1
    assert replay["best_candidate"]["skipped_timing_filter"] == 1
    gate_timing = response.json()["gate"]["stage4b_timing"]
    assert gate_timing["exists"] is True
    assert gate_timing["latest_run_id"] == replay["run_id"]
    assert gate_timing["best_candidate"]["skipped_timing_filter"] == 1


def test_stage4_candidate_detail_endpoint_reads_stage4b_timing_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session, _signals, _candles = _stage4b_api_fixture(tmp_path, repository)
    promotion_root = Path(session["artifact_root"]) / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=4.0,
        stage4a_total=50.0,
        stage4b_wf=5.0,
        stage4b_total=70.0,
    )
    timing_root = promotion_root / "stage4b_timing"
    (timing_root / "timing_trade_ledger.json").write_text(
        json.dumps(
            {
                "run_id": "stage4b-run",
                "candidates": [
                    {
                        "candidate_id": "stage4b-best",
                        "trades": [
                            {
                                "candidate_id": "stage4b-best",
                                "signal_id": "sig-1",
                                "entry_status": "filled",
                                "exit_status": "tp_hit",
                                "net_pnl_usdt": 12.5,
                            }
                        ],
                    }
                ],
            }
        )
    )
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(
        f"/api/v1/research/stage1-sessions/{session['session_id']}/stage4/candidates/stage4b-best/details?source=stage4b_timing"
    )

    assert response.status_code == 200
    detail = response.json()["detail"]
    assert detail["source"] == "stage4b_timing"
    assert detail["run_id"] == "stage4b-run"
    assert detail["candidate"]["candidate_id"] == "stage4b-best"
    assert detail["trade_count"] == 1
    assert detail["trades"][0]["exit_status"] == "tp_hit"


def _stage4b_api_fixture(tmp_path: Path, repository: StubRuntimeRepository):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)
    records = [
        {
            "signal_id": "sig-enter",
            "agent_direction": "LONG",
            "decision_direction": "LONG",
            "agreement": "MATCH",
            "sample_role": "training",
        },
        {
            "signal_id": "sig-timing",
            "agent_direction": "LONG",
            "decision_direction": "LONG",
            "agreement": "MATCH",
            "sample_role": "walk_forward_test",
        },
    ]
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(json.dumps({"records": records}))
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "market_tp_1p0_sl_1p0",
                        "setup": {
                            "entry_model": "market",
                            "tp_pct": 1.0,
                            "sl_pct": 5.0,
                            "timeout_policy": "close_at_cutoff",
                        },
                    }
                ]
            }
        )
    )
    signals = [
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
            "signal_id": "sig-timing",
            "timestamp": "2026-05-01T01:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T01:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 100}],
                "active_timeframes": ["2h"],
            },
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101, "volume": 1, "confirm": 1},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101, "volume": 1, "confirm": 1},
    ]
    run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session={
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "asset": "AAVE",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "signal_engine_id": "vegas_ema",
            "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-31",
        },
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    storage_uri = tmp_path / "data/market/source=okx/type=candles/asset=AAVE/timeframe=5m/origin=raw"
    parquet_path = storage_uri / "year=2026/month=05/data.parquet"
    parquet_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(candles), parquet_path)
    repository.candle_ref = {"dataset_id": "okx-aave-raw-5m", "storage_backend": "parquet", "storage_uri": str(storage_uri)}
    repository.window_signals = signals
    session = {
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
    repository.stage1_sessions = [session]
    return session, signals, candles


def _write_promotable_stage4_branches(
    promotion_root: Path,
    *,
    stage4a_wf: float,
    stage4a_total: float,
    stage4b_wf: float,
    stage4b_total: float,
    overlay_source_run_id: str = "stage4-run",
) -> None:
    promotion_root.mkdir(parents=True, exist_ok=True)
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True, exist_ok=True)
    (frozen_root / "strategy.py").write_text(
        "def decide(context):\n"
        "    return {'trade_action': 'ENTER', 'action': 'ENTER', 'direction': 'LONG', 'reason_code': 'base'}\n"
        "\n"
        "def manage_position(context):\n"
        "    return {'action': 'HOLD'}\n"
    )
    stage4a_best = {
        "candidate_id": "stage4a-best",
        "setup": {"tp_pct": 2.0, "sl_pct": 1.0, "initial_sl_pct": 1.0, "max_hold_hours": 36},
        "account": {"net_pnl_usdt": stage4a_total, "ending_equity_usdt": 1000 + stage4a_total},
        "slices": {"walk_forward_test": {"net_pnl_pct": stage4a_wf, "profit_factor": 1.4}},
    }
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps(
            {
                "run_id": "stage4-run",
                "best_candidate_id": "stage4a-best",
                "best_candidate": stage4a_best,
                "simulation_inputs": {"initial_capital_usdt": 1000, "margin_allocation_pct": 30, "leverage": 5},
                "cost_assumptions": {"fees_bps_per_side": 5},
                "slice_windows": [],
                "candidates": [stage4a_best],
            }
        )
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(json.dumps({"run_id": "stage4-run", "best": stage4a_best}))
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    timing_root = promotion_root / "stage4b_timing"
    timing_root.mkdir(parents=True, exist_ok=True)
    overlay = {
        "schema_version": "stage4b_timing_overlay.v1",
        "source_stage4_run_id": overlay_source_run_id,
        "source_stage4_candidate_id": "stage4a-best",
        "exclude_utc_hours": [1],
        "exclude_utc_weekdays": [],
        "applies_to": "all",
        "rationale": "Training-supported weak 01 UTC window.",
    }
    stage4b_best = {
        "candidate_id": "stage4b-best",
        "setup": {"tp_pct": 2.5, "sl_pct": 1.0, "initial_sl_pct": 1.0, "max_hold_hours": 36},
        "account": {"net_pnl_usdt": stage4b_total, "ending_equity_usdt": 1000 + stage4b_total},
        "slices": {"walk_forward_test": {"net_pnl_pct": stage4b_wf, "profit_factor": 1.8}},
        "skipped_timing_filter": 12,
    }
    (timing_root / "timing_overlay.json").write_text(json.dumps(overlay))
    (timing_root / "timing_replay.json").write_text(
        json.dumps(
            {
                "run_id": "stage4b-run",
                "best_candidate_id": "stage4b-best",
                "best_candidate": stage4b_best,
                "simulation_inputs": {"initial_capital_usdt": 1000, "margin_allocation_pct": 30, "leverage": 5},
                "cost_assumptions": {"fees_bps_per_side": 5},
                "overlay": overlay,
                "candidates": [stage4b_best],
            }
        )
    )
    (timing_root / "timing_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (timing_root / "timing_summary.md").write_text("# Stage 4B Timing Replay\n")


def test_portfolio_backtest_endpoint_writes_pool_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"

    # Stage 4 realized expectancy with candidate setup
    candidate_setup = {
        "candidate_id": "market",
        "entry_model": "market",
        "tp_pct": 2.0,
        "sl_pct": 1.0,
        "final_tp_pct": 2.0,
        "initial_sl_pct": 1.0,
        "protection_enabled": False,
        "timeout_policy": "close_at_cutoff",
        "max_hold_hours": 12.0,
        "leverage": 5.0,
    }
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps(
            {
                "run_id": "stage4-run",
                "asset": "AAVE",
                "best_candidate_id": "market",
                "best_candidate": {"candidate_id": "market", "account": {"initial_capital_usdt": 1000}, "setup": candidate_setup},
                "cost_assumptions": {"fees_bps_per_side": 5.0, "slippage_bps_per_side": 0.0},
            }
        )
    )
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps({"candidates": [{"candidate_id": "market", "setup": candidate_setup}]})
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(
        json.dumps({"run_id": "stage4-run", "candidates": [{"candidate_id": "market", "trades": []}]})
    )
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"records": [{"signal_id": "sig-1", "decision_direction": "LONG", "agreement": "MATCH"}]})
    )

    # Set up repository signals with proper packet structure
    repository.window_signals = [
        {
            "signal_id": "sig-1",
            "signal_set_key": session["signal_set_key"],
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "timestamp": "2026-04-20T00:00:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {
                "timestamp": "2026-04-20T00:00:00Z",
                "active_timeframes": ["5m"],
                "charts": {"5m": {"latest_forming_candle": {"close": 100.0}}},
            },
        }
    ]

    # Mock MarketDataReader to return simple candles
    from datetime import datetime as _dt, timedelta as _td
    _candle_start = _dt(2026, 4, 20, tzinfo=__import__("datetime").timezone.utc)
    _candles = []
    _price = 100.0
    for _i in range(300):
        _candles.append({
            "timestamp": _candle_start + _td(minutes=5 * _i),
            "open": _price,
            "high": _price * 1.01,
            "low": _price * 0.99,
            "close": _price * 1.002,
        })
        _price = _price * 1.002
    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: type("MockReader", (), {"get_candles": lambda self, **kw: _candles})(),
    )

    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        "/api/v1/research/stage0-universe-runs/universe-march-may-vegas/portfolio-backtest",
        json={"initial_capital_usdt": 1000, "margin_allocations_pct": {"AAVE": 30}},
    )

    assert response.status_code == 200
    result = response.json()["portfolio_backtest"]
    assert result["summary"]["eligible_asset_count"] == 1
    assert result["summary"]["executed_positions"] == 1
    assert result["portfolio_backtest_path"].endswith("dev/portfolio_backtests/universe-march-may-vegas/portfolio_backtest.json")
    assert (tmp_path / result["portfolio_backtest_path"]).exists()

    history_response = client.get("/api/v1/research/stage0-universe-runs/universe-march-may-vegas/portfolio-backtest/runs")
    assert history_response.status_code == 200
    history = history_response.json()["portfolio_backtest_runs"]
    assert history["latest_run_id"] == result["run_id"]
    assert [item["run_id"] for item in history["runs"]] == [result["run_id"]]

    run_response = client.get(f"/api/v1/research/stage0-universe-runs/universe-march-may-vegas/portfolio-backtest/runs/{result['run_id']}")
    assert run_response.status_code == 200
    assert run_response.json()["portfolio_backtest"]["run_id"] == result["run_id"]

    delete_response = client.delete(f"/api/v1/research/stage0-universe-runs/universe-march-may-vegas/portfolio-backtest/runs/{result['run_id']}")
    assert delete_response.status_code == 200
    assert delete_response.json()["portfolio_backtest_delete"]["remaining_run_count"] == 0
    assert not (tmp_path / result["portfolio_backtest_path"]).exists()


def test_portfolio_backtest_endpoint_rejects_without_stage4_assets(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(_queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen"))
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post("/api/v1/research/stage0-universe-runs/universe-march-may-vegas/portfolio-backtest")

    assert response.status_code == 400
    assert response.json()["detail"] == "Portfolio backtest requires at least one Stage 4-complete asset"


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


def test_development_queue_stage2_capture_complete_requests_exit_policy_promotion(tmp_path, monkeypatch):
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
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get("/api/v1/research/cycles/universe-march-may-vegas/development-queue")

    assert response.status_code == 200
    row = response.json()["queue"][0]
    assert row["development_status"] == "stage2_complete"
    assert row["current_stage"] == "stage2_policy_ready"
    assert row["next_action"]["type"] == "promote_stage2_exit_policy"
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
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
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


def test_promote_execution_bundle_creates_route_and_blocked_wake(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True, exist_ok=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {'trade_action': 'SKIP'}\n")
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps(
            {
                "best_candidate_id": "pyramid",
                "best_candidate": {"candidate_id": "pyramid", "net_expectancy_pct": 0.24},
                "simulation_inputs": {
                    "initial_capital_usdt": 1000,
                    "margin_allocation_pct": 30,
                    "leverage": 5,
                },
                "cost_assumptions": {"fees_bps_per_side": 5},
                "slice_windows": [],
                "candidates": [],
            }
        )
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(
        json.dumps({"best": {"candidate_id": "pyramid", "setup": {"tp_pct": 2.0, "sl_pct": 1.0}}})
    )
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    promote_response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")
    routes_response = client.get("/api/v1/trading/routes")
    settings_response = client.patch(
        "/api/v1/trading/routes/aave-live/settings",
        json={
            "cron_interval_minutes": 30,
            "execution_adapter": "okx",
            "exchange_account": "main-live-01",
            "margin_allocation_pct": 30,
            "leverage": 5,
            "manual_sizing_enabled": True,
        },
    )
    wake_response = client.post("/api/v1/trading/routes/aave-live/wake")

    assert promote_response.status_code == 200
    promoted = promote_response.json()
    assert promoted["bundle"]["status"] == "promoted"
    assert promoted["bundle"]["execution_setup"]["forward_hours"] == 36
    assert promoted["bundle"]["execution_setup"]["hard_exit_after_hours"] == 36
    assert promoted["bundle"]["execution_setup"]["sizing"]["margin_allocation_pct"] == 30
    assert promoted["bundle"]["execution_setup"]["sizing"]["leverage"] == 5
    manifest = json.loads((tmp_path / promoted["bundle"]["bundle_uri"] / "manifest.json").read_text())
    assert manifest["contract_version"] == "engine_strategy_contract.v1"
    assert manifest["signal_engine_spec"]["signal_engine_id"] == "vegas_ema"
    assert promoted["route"]["route_id"] == "aave-live"
    assert promoted["route"]["enabled"] is False
    assert promoted["route"]["margin_allocation_pct"] == 30
    assert promoted["route"]["leverage"] == 5
    assert promoted["route"]["manual_sizing_enabled"] is False
    assert promoted["route"]["blockers"] == ["route_disabled", "data_not_warmed", "route_not_manually_armed"]
    assert (tmp_path / promoted["bundle"]["bundle_uri"] / "bundle.json").exists()
    assert routes_response.status_code == 200
    assert routes_response.json()["routes"][0]["active_bundle_id"] == promoted["bundle"]["bundle_id"]
    assert settings_response.status_code == 200
    assert settings_response.json()["route"]["cron_interval_minutes"] == 30
    assert settings_response.json()["route"]["exchange_account"] == "main-live-01"
    assert settings_response.json()["route"]["margin_allocation_pct"] == 30
    assert settings_response.json()["route"]["leverage"] == 5
    assert settings_response.json()["route"]["manual_sizing_enabled"] is True
    assert wake_response.status_code == 200
    assert wake_response.json()["wake"]["status"] == "blocked"
    assert wake_response.json()["wake"]["branch"] == "route_gate"


def test_promote_execution_bundle_selects_stage4b_when_walk_forward_is_better(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-stage4b.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=8,
        stage4a_total=500,
        stage4b_wf=18,
        stage4b_total=450,
    )
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 200
    bundle = response.json()["bundle"]
    assert bundle["execution_setup"]["source"] == "stage4b_timing"
    assert bundle["execution_setup"]["stage4_candidate_id"] == "stage4b-best"
    assert bundle["execution_setup"]["sizing"]["source"] == "stage4b_timing"
    assert bundle["source_stage4_result_path"].endswith("promotion/stage4b_timing/timing_replay.json")
    assert bundle["evidence_refs"]["stage4b_timing_replay"].endswith("promotion/stage4b_timing/timing_replay.json")
    assert bundle["evidence_refs"]["stage4b_timing_overlay"].endswith("promotion/stage4b_timing/timing_overlay.json")
    assert "frozen_stage4b_timing_strategy_module" in bundle["evidence_refs"]["stage4b_timing_strategy"]
    strategy_path = Path(bundle["strategy_module_ref"])
    module = load_strategy_module(strategy_path)
    assert module.decide({"timestamp": "2026-05-01T00:00:00Z"})["trade_action"] == "ENTER"
    skipped = module.decide({"timestamp": "2026-05-01T01:00:00Z"})
    assert skipped["trade_action"] == "SKIP"
    assert skipped["direction"] == "FLAT"
    assert skipped["reason_code"] == "timing_filter_utc_window"


def test_promote_execution_bundle_prefers_highest_oos_protected_candidate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-protected-oos.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=30,
        stage4a_total=500,
        stage4b_wf=35,
        stage4b_total=650,
    )
    realized_path = promotion_root / "stage4_realized_expectancy.json"
    realized = json.loads(realized_path.read_text())
    protected = {
        "candidate_id": "stage4a-protected",
        "setup": {
            "tp_pct": 2.0,
            "sl_pct": 1.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": True,
            "protect_trigger_pct": 1.0,
            "trail_sl_pct": 0.25,
            "max_hold_hours": 36,
        },
        "account": {"net_pnl_usdt": 400, "ending_equity_usdt": 1400},
        "slices": {"walk_forward_test": {"net_pnl_pct": 22, "profit_factor": 1.6}},
    }
    realized["candidates"].append(protected)
    realized_path.write_text(json.dumps(realized))
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 200
    bundle = response.json()["bundle"]
    assert bundle["execution_setup"]["source"] == "stage4_realized_expectancy"
    assert bundle["execution_setup"]["stage4_candidate_id"] == "stage4a-protected"
    assert bundle["execution_setup"]["setup"]["protection_enabled"] is True
    assert bundle["execution_setup"]["promotion_selection"]["criterion"] == "protected_walk_forward_net_pnl_pct"


def test_stage1_gate_reports_resolved_promotion_candidate(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    repository = StubRuntimeRepository()
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=4,
        stage4a_total=500,
        stage4b_wf=12,
        stage4b_total=450,
    )
    repository.stage1_sessions = [session]
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(f"/api/v1/research/stage1-sessions/{session['session_id']}/gate")

    assert response.status_code == 200
    candidate = response.json()["gate"]["promotion_candidate"]
    assert candidate["source"] == "stage4b_timing"
    assert candidate["candidate_id"] == "stage4b-best"
    assert candidate["walk_forward_net_pnl_pct"] == 12
    assert candidate["overall_net_pnl_usdt"] == 450


def test_promote_execution_bundle_keeps_stage4a_when_stage4b_only_wins_total_pnl(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-stage4a-wf.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=14,
        stage4a_total=500,
        stage4b_wf=5,
        stage4b_total=900,
    )
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 200
    bundle = response.json()["bundle"]
    assert bundle["execution_setup"]["source"] == "stage4_realized_expectancy"
    assert bundle["execution_setup"]["stage4_candidate_id"] == "stage4a-best"
    assert "stage4b_timing_replay" not in bundle["evidence_refs"]


def test_promote_execution_bundle_ignores_stale_stage4b_overlay(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-stage4b-stale.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    _write_promotable_stage4_branches(
        promotion_root,
        stage4a_wf=6,
        stage4a_total=500,
        stage4b_wf=30,
        stage4b_total=900,
        overlay_source_run_id="older-stage4-run",
    )
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 200
    bundle = response.json()["bundle"]
    assert bundle["execution_setup"]["source"] == "stage4_realized_expectancy"
    assert bundle["execution_setup"]["stage4_candidate_id"] == "stage4a-best"


def test_promote_execution_bundle_rejects_missing_strategy_decide(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-invalid-strategy.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True, exist_ok=True)
    (frozen_root / "strategy.py").write_text("def helper(context):\n    return None\n")
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({"best_candidate_id": "fixed", "best_candidate": {"candidate_id": "fixed"}, "candidates": []})
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(
        json.dumps({"best": {"candidate_id": "fixed", "setup": {"tp_pct": 2.0, "sl_pct": 1.0}}})
    )
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 400
    assert response.json()["detail"] == "strategy module must expose callable decide(context)"


def test_promote_execution_bundle_rejects_invalid_execution_setup(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'execution-invalid-setup.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    _register_vegas_engine(repository)
    session = _queue_session(tmp_path, "candidate-aave", "AAVE", "stage1a_frozen")
    promotion_root = tmp_path / session["artifact_root"] / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True, exist_ok=True)
    (frozen_root / "strategy.py").write_text("def decide(context):\n    return {'trade_action': 'SKIP'}\n")
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({"best_candidate_id": "fixed", "best_candidate": {"candidate_id": "fixed"}, "candidates": []})
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(
        json.dumps({"best": {"candidate_id": "fixed", "setup": {"tp_pct": 2.0}}})
    )
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    repository.create_stage0_universe(_queue_universe_run(), [_queue_candidate("candidate-aave", "AAVE", "accepted", 91.2)])
    repository.create_stage1_research_session(session)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(f"/api/v1/research/stage1-sessions/{session['session_id']}/promote-execution-bundle")

    assert response.status_code == 400
    assert response.json()["detail"] == "execution setup missing initial_sl_pct"


def test_trading_route_archive_hides_route_and_lists_archived_strategy(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'archive.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
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
    client = TestClient(create_app(runtime_repository=repository))

    archive_response = client.post(f"/api/v1/trading/routes/{route['route_id']}/archive")
    active_response = client.get("/api/v1/trading/routes")
    archived_response = client.get("/api/v1/trading/routes/archived")
    direct_response = client.get(f"/api/v1/trading/routes/{route['route_id']}")

    assert archive_response.status_code == 200
    archived_route = archive_response.json()["route"]
    assert archived_route["archived"] is True
    assert archived_route["archived_at"] is not None
    assert archived_route["enabled"] is False
    assert archived_route["scheduler_status"] == "stopped"
    assert archived_route["auto_submit_enabled"] is False
    assert archived_route["next_wake_at"] is None
    assert active_response.status_code == 200
    assert active_response.json()["routes"] == []
    assert archived_response.status_code == 200
    assert archived_response.json()["routes"][0]["route_id"] == route["route_id"]
    assert direct_response.status_code == 200
    assert direct_response.json()["route"]["archived"] is True


def test_archived_strategy_delete_removes_route_bundle_history_and_artifacts(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'archive-delete.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
        account_mode="live",
        execution_adapter="okx",
    )
    repository.archive_deployment_route(route["route_id"], archived_at="2026-06-06T06:00:00Z")
    repository.record_wake_run(
        {
            "wake_id": "wake-delete-1",
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
            "completed_at": "2026-06-06T06:05:00Z",
        }
    )
    repository.create_owner_state(
        {
            "owner_state_id": "owner-delete-1",
            "route_id": route["route_id"],
            "bundle_id": bundle["bundle_id"],
            "position_instance_id": "pos-delete-1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "account_mode": "live",
            "owner_strategy_id": "aave-vegas-tunnel-v01",
            "owner_strategy_version": "v0.1",
            "opened_from_signal_id": "signal-delete-1",
            "status": "open",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted"}]},
        }
    )

    class FakeAdapter:
        def readiness_blockers(self):
            return []

        def snapshot(self, instrument):
            return {
                "instrument": instrument,
                "positions": [],
                "open_orders": [],
                "protection_orders": [],
            }

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeAdapter())
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete(f"/api/v1/trading/routes/{route['route_id']}/archived-strategy")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "deleted"
    assert payload["route_id"] == route["route_id"]
    assert payload["bundle_id"] == bundle["bundle_id"]
    assert payload["deleted_wake_count"] == 1
    assert payload["deleted_owner_state_count"] == 1
    assert payload["artifact_deleted"] is True
    assert repository.get_deployment_route(route["route_id"]) is None
    assert repository.get_execution_bundle(bundle["bundle_id"]) is None
    assert repository.list_wake_runs(route["route_id"]) == []
    with repository.engine.connect() as connection:
        assert connection.execute(
            select(owner_states.c.owner_state_id).where(owner_states.c.route_id == route["route_id"])
        ).first() is None
    assert not Path(bundle["bundle_uri"]).exists()


def test_archived_strategy_delete_blocks_when_exchange_exposure_exists(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'archive-delete-blocked.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
        account_mode="live",
        execution_adapter="okx",
    )
    repository.archive_deployment_route(route["route_id"], archived_at="2026-06-06T06:00:00Z")

    class FakeAdapter:
        def readiness_blockers(self):
            return []

        def snapshot(self, instrument):
            return {
                "instrument": instrument,
                "positions": [{"instId": instrument, "pos": "1"}],
                "open_orders": [],
                "protection_orders": [],
            }

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeAdapter())
    client = TestClient(create_app(runtime_repository=repository))

    response = client.delete(f"/api/v1/trading/routes/{route['route_id']}/archived-strategy")

    assert response.status_code == 409
    assert response.json()["detail"] == "Archived strategy still has live exchange exposure"
    assert repository.get_deployment_route(route["route_id"]) is not None
    assert repository.get_execution_bundle(bundle["bundle_id"]) is not None
    assert Path(bundle["bundle_uri"]).exists()


def test_trading_route_wakes_endpoint_paginates_history(tmp_path):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'wake-pagination.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
        account_mode="demo",
        execution_adapter="okx",
    )
    for index in range(4):
        repository.record_wake_run(
            {
                "wake_id": f"wake-page-{index}",
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
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(f"/api/v1/trading/routes/{route['route_id']}/wakes?limit=2&offset=1")

    assert response.status_code == 200
    payload = response.json()
    assert [wake["wake_id"] for wake in payload["wakes"]] == ["wake-page-2", "wake-page-1"]
    assert payload["total"] == 4
    assert payload["limit"] == 2
    assert payload["offset"] == 1


def test_trading_wake_auto_warms_required_market_data(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'warmup.db'}")
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
            "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
            "configuration_schema": {},
        }
    )
    bundle = _execution_bundle(tmp_path)
    stored_bundle = repository.create_execution_bundle(bundle)
    route = repository.upsert_deployment_route_for_bundle(
        bundle=stored_bundle,
        account_mode="demo",
        execution_adapter="okx",
    )
    repository.update_deployment_route_gate(route["route_id"], enabled=True, manually_armed=True)

    class FakeMarketDataRepository:
        def __init__(self):
            self.raw_ref = {
                "dataset_id": "aave-raw-5m",
                "asset": "AAVE",
                "instrument": "AAVE-USDT-SWAP",
                "data_type": "candles",
                "timeframe": "5m",
                "data_origin": "raw",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-06-01T00:00:00Z",
                "row_count": 100,
                "storage_uri": str(tmp_path / "market-data"),
            }

        def get_raw_candle_ref(self, asset, timeframe="5m"):
            return dict(self.raw_ref)

        def list_derived_refs_for_raw(self, registration):
            return []

    market_data_repository = FakeMarketDataRepository()

    fill_calls = []

    def fake_fill_service(*, registration, repository, adapter, as_of):
        fill_calls.append(registration["dataset_id"])
        latest_confirmed_start = as_of - timedelta(minutes=5)
        repository.raw_ref["end_ts"] = latest_confirmed_start.isoformat().replace("+00:00", "Z")
        return {
            "dataset_id": registration["dataset_id"],
            "status": "current",
            "rows_added": 0,
            "end_ts": repository.raw_ref["end_ts"],
            "derived_rebuilt": [],
        }

    signal_extension_calls = []

    def fake_signal_extender(*, workspace_root, repository, signal_engine_id, asset, target_end):
        signal_extension_calls.append((signal_engine_id, asset, target_end))
        return {
            "status": "noop",
            "signal_engine_id": signal_engine_id,
            "asset": asset,
            "appended_packet_count": 0,
        }

    class FakeOKXAdapter:
        def __init__(self, config):
            self.config = config

        def readiness_blockers(self):
            return []

        def snapshot(self, instrument):
            return {"instrument": instrument, "positions": [], "open_orders": []}

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeOKXAdapter({
        "backend": "okx_cli",
        "mode": route["account_mode"],
        "profile": route.get("exchange_account") if route.get("exchange_account") not in {None, "", "default"} else None,
    }))
    client = TestClient(
        create_app(
            runtime_repository=repository,
            market_data_repository=market_data_repository,
            market_data_fill_service=fake_fill_service,
            signal_pool_extension_service=fake_signal_extender,
            live_signal_scan_service=lambda **kwargs: None,
        )
    )

    response = client.post(f"/api/v1/trading/routes/{route['route_id']}/wake")

    assert response.status_code == 200
    payload = response.json()
    assert payload["warmup"]["status"] == "warmed"
    assert payload["route"]["data_warmed"] is True
    assert payload["wake"]["status"] == "completed"
    assert payload["wake"]["signal_scan_result"]["status"] == "no_fresh_signal"
    assert fill_calls == ["aave-raw-5m"]
    assert signal_extension_calls == [("vegas_ema", "AAVE", None)]


def test_submit_wake_orders_places_order_and_persists_owner_state(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'submit.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = _execution_bundle(tmp_path)
    stored_bundle = repository.create_execution_bundle(bundle)
    route = repository.upsert_deployment_route_for_bundle(
        bundle=stored_bundle,
        account_mode="demo",
        execution_adapter="okx",
    )
    repository.update_deployment_route_gate(route["route_id"], enabled=True, data_warmed=True, manually_armed=True)
    wake = repository.record_wake_run(
        {
            "wake_id": "wake-submit-1",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "status": "completed",
            "branch": "entry_scan",
            "blockers": [],
            "exchange_snapshot": {},
            "signal_scan_result": {"status": "evaluated", "signal_id": "signal-1"},
            "strategy_decision": {"action": "ENTER"},
            "order_intents": [
                {
                    "intent_id": "wake-submit-1:0",
                    "route_id": route["route_id"],
                    "asset": "AAVE",
                    "instrument": "AAVE-USDT-SWAP",
                    "signal_id": "signal-1",
                    "side": "buy",
                    "direction": "LONG",
                    "order_type": "market",
                    "quantity": "1.25",
                    "notional_usd": 10,
                    "trade_mode": "isolated",
                    "reduce_only": False,
                    "client_order_id": "motis-submit-1",
                    "status": "intent_only",
                }
            ],
            "adapter_results": [],
            "error": {},
            "completed_at": "2026-06-05T00:00:00Z",
        }
    )

    class FakeOKXAdapter:
        def __init__(self, config):
            self.config = config

        def place_swap_order(self, request):
            return {"ordId": "okx-order-1", "clOrdId": request.client_order_id}

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeOKXAdapter({
        "backend": "okx_cli",
        "mode": route["account_mode"],
        "profile": route.get("exchange_account") if route.get("exchange_account") not in {None, "", "default"} else None,
    }))
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        f"/api/v1/trading/routes/{route['route_id']}/wakes/{wake['wake_id']}/submit-orders",
        json={"confirm_live": False},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "submitted"
    assert payload["submitted_count"] == 1
    assert payload["wake"]["order_intents"][0]["status"] == "submitted"
    assert payload["wake"]["adapter_results"][0]["ordId"] == "okx-order-1"
    assert repository.get_open_owner_state(route["route_id"])["opened_from_signal_id"] == "signal-1"


def test_submit_wake_orders_uses_route_exchange_adapter_factory(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'submit-factory.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = _execution_bundle(tmp_path)
    stored_bundle = repository.create_execution_bundle(bundle)
    route = repository.upsert_deployment_route_for_bundle(
        bundle=stored_bundle,
        account_mode="demo",
        execution_adapter="okx",
    )
    route = repository.update_deployment_route_gate(route["route_id"], exchange_account="paper-profile")
    repository.update_deployment_route_gate(route["route_id"], enabled=True, data_warmed=True, manually_armed=True)
    wake = repository.record_wake_run(
        {
            "wake_id": "wake-submit-factory",
            "route_id": route["route_id"],
            "bundle_id": stored_bundle["bundle_id"],
            "status": "completed",
            "branch": "entry_scan",
            "blockers": [],
            "exchange_snapshot": {},
            "signal_scan_result": {"status": "evaluated", "signal_id": "signal-factory"},
            "strategy_decision": {"action": "ENTER"},
            "order_intents": [
                {
                    "intent_id": "wake-submit-factory:0",
                    "route_id": route["route_id"],
                    "asset": "AAVE",
                    "instrument": "AAVE-USDT-SWAP",
                    "signal_id": "signal-factory",
                    "side": "buy",
                    "direction": "LONG",
                    "order_type": "market",
                    "quantity": "1",
                    "notional_usd": 10,
                    "trade_mode": "isolated",
                    "reduce_only": False,
                    "client_order_id": "motis-submit-factory",
                    "status": "intent_only",
                }
            ],
            "adapter_results": [],
            "error": {},
            "completed_at": "2026-06-05T00:00:00Z",
        }
    )

    built_routes = []

    class FakeAdapter:
        def place_swap_order(self, request):
            return {"ordId": "factory-order-1", "clOrdId": request.client_order_id}

    def fake_build_exchange_adapter(route):
        built_routes.append(dict(route))
        return FakeAdapter()

    class ExplodingOKXAdapter:
        def __init__(self, config):
            raise AssertionError("submit-orders must use build_exchange_adapter")

    monkeypatch.setattr(api_main, "build_exchange_adapter", fake_build_exchange_adapter, raising=False)
    monkeypatch.setattr(api_main, "OKXAdapter", ExplodingOKXAdapter, raising=False)
    client = TestClient(create_app(runtime_repository=repository))

    response = client.post(
        f"/api/v1/trading/routes/{route['route_id']}/wakes/{wake['wake_id']}/submit-orders",
        json={"confirm_live": False},
    )

    assert response.status_code == 200
    assert response.json()["wake"]["adapter_results"][0]["ordId"] == "factory-order-1"
    assert built_routes[0]["execution_adapter"] == "okx"
    assert built_routes[0]["exchange_account"] == "paper-profile"


def test_trading_route_exchange_health_reports_connected_backend_adapter(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'exchange-health.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
        account_mode="live",
        execution_adapter="okx",
    )
    repository.update_deployment_route_gate(route["route_id"], exchange_account="live")

    class FakeOKXAdapter:
        def __init__(self, config):
            self.config = config

        def readiness_blockers(self):
            return []

        def snapshot(self, instrument):
            return {
                "instrument": instrument,
                "positions": [{"instId": instrument, "pos": "0.01"}],
                "open_orders": [],
                "protection_orders": [],
                "recent_fills": [{"instId": instrument, "ordId": "fill-1"}],
                "balance": {"data": [{"ccy": "USDT", "eqUsd": "100"}]},
            }

        def _cli_path(self):
            return "/opt/homebrew/bin/okx"

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeOKXAdapter({
        "backend": "okx_cli",
        "mode": route["account_mode"],
        "profile": route.get("exchange_account") if route.get("exchange_account") not in {None, "", "default"} else None,
    }))
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(f"/api/v1/trading/routes/{route['route_id']}/exchange-health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "connected"
    assert payload["connected"] is True
    assert payload["adapter"] == "okx"
    assert payload["account_mode"] == "live"
    assert payload["exchange_account"] == "live"
    assert payload["instrument"] == "AAVE-USDT-SWAP"
    assert payload["cli_path"].endswith("okx")
    assert payload["snapshot"]["position_count"] == 1
    assert payload["snapshot"]["recent_fill_count"] == 1
    assert payload["error"] is None


def test_trading_route_exchange_health_reports_sanitized_adapter_failure(tmp_path, monkeypatch):
    engine = create_engine(f"sqlite+pysqlite:///{tmp_path / 'exchange-health-failure.db'}")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    bundle = repository.create_execution_bundle(_execution_bundle(tmp_path))
    route = repository.upsert_deployment_route_for_bundle(
        bundle=bundle,
        account_mode="live",
        execution_adapter="okx",
    )

    class FakeOKXAdapter:
        def __init__(self, config):
            self.config = config

        def readiness_blockers(self):
            return []

        def snapshot(self, instrument):
            raise api_main.ExchangeAdapterError("Error: Not logged in\nsecret_key=should-not-leak")

        def _cli_path(self):
            return "/opt/homebrew/bin/okx"

    monkeypatch.setattr(api_main, "build_exchange_adapter", lambda route: FakeOKXAdapter({
        "backend": "okx_cli",
        "mode": route["account_mode"],
        "profile": route.get("exchange_account") if route.get("exchange_account") not in {None, "", "default"} else None,
    }))
    client = TestClient(create_app(runtime_repository=repository))

    response = client.get(f"/api/v1/trading/routes/{route['route_id']}/exchange-health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "disconnected"
    assert payload["connected"] is False
    assert payload["error"] == "Error: Not logged in"
    assert "secret" not in payload["error"]
    assert payload["snapshot"] == {}


def _execution_bundle(tmp_path):
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text("def decide(context):\n    return {'trade_action': 'SKIP'}\n")
    return {
        "bundle_id": "aave-vegas_ema-strategy-submit",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "source_stage1_session_id": "stage1-aave",
        "source_stage4_result_path": "dev/training_sessions/aave/promotion/stage4_realized_expectancy.json",
        "bundle_uri": str(bundle_root),
        "strategy_module_ref": str(strategy_path),
        "execution_setup": {"stage4_candidate_id": "candidate-1"},
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 250},
        "evidence_refs": {"stage4_optimal": "stage4_optimal.json"},
        "content_hash": "submit",
        "status": "promoted",
    }


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


def _register_vegas_engine(repository):
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA Tunnel",
            "description": "Legacy engine",
            "version": "0.1",
            "code_ref": {
                "path": "artifacts/signal_engine",
                "base_strategy_path": "packages/strategy_modules/src/quant_terminal_strategies/vegas_ema_base.py",
            },
            "supported_input_data_types": ["candles"],
            "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:scan_live_signal",
            "configuration_schema": {},
        }
    )


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
