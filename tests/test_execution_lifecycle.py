from __future__ import annotations

import json
from datetime import UTC, datetime

from quant_terminal_worker.execution.lifecycle import next_wake_at, run_route_lifecycle_cycle


class FakeRuntimeRepository:
    def __init__(self, bundle: dict) -> None:
        self.route = {
            "route_id": "aave-live",
            "active_bundle_id": "bundle-1",
            "active_bundle": bundle,
            "strategy_id": "aave-strategy",
            "strategy_version": "v0.1",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "account_mode": "live",
            "execution_adapter": "okx",
            "scheduler_status": "running",
            "cron_interval_minutes": 5,
            "auto_submit_enabled": False,
            "enabled": True,
            "promoted": True,
            "data_warmed": False,
            "manually_armed": True,
            "blockers": [],
        }
        self.engines = [
            {
                "signal_engine_id": "vegas_ema",
                "version": "0.1",
                "required_data": [{"data_type": "candles", "origin": "raw", "timeframe": "5m"}],
            }
        ]
        self.wakes = []
        self.stage1_sessions = {
            "stage1-aave": {
                "session_id": "stage1-aave",
                "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            }
        }
        self.signals = []
        self.latest_confirmed_candle_ts = datetime(2026, 6, 5, 0, 0, tzinfo=UTC)

    def get_deployment_route(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return dict(self.route)

    def list_signal_engines(self):
        return list(self.engines)

    def update_deployment_route_gate(self, route_id, **values):
        assert route_id == self.route["route_id"]
        self.route = {**self.route, **values}
        return dict(self.route)

    def record_wake_run(self, wake):
        self.wakes.append(wake)
        return wake

    def get_open_owner_state(self, route_id):
        return None

    def list_wake_runs(self, route_id, limit=25):
        return list(reversed(self.wakes))[:limit]

    def get_stage1_research_session(self, session_id):
        return self.stage1_sessions.get(session_id)

    def get_signal_set(self, signal_set_key):
        if signal_set_key == "vegas_ema:AAVE:AAVE-vegas_ema-canonical":
            return {"signal_set_key": signal_set_key}
        return None

    def list_signals(self, **kwargs):
        rows = list(self.signals)
        if kwargs.get("signal_set_key"):
            rows = [row for row in rows if row.get("signal_set_key") == kwargs["signal_set_key"]]
        rows = sorted(rows, key=lambda row: row["timestamp"], reverse=bool(kwargs.get("descending")))
        return rows[: kwargs.get("limit", len(rows))]

    def get_latest_confirmed_candle_timestamp(self, *, asset, timeframe, origin):
        return self.latest_confirmed_candle_ts


class FakeMarketDataRepository:
    def get_raw_candle_ref(self, asset, timeframe="5m"):
        return {
            "dataset_id": "aave-raw-5m",
            "asset": asset,
            "data_type": "candles",
            "timeframe": timeframe,
            "data_origin": "raw",
        }


class FakeAdapter:
    def __init__(self, *, positions=None):
        self.positions = positions or []

    def readiness_blockers(self):
        return []

    def snapshot(self, instrument):
        return {
            "instrument": instrument,
            "positions": self.positions,
            "open_orders": [],
            "protection_orders": [],
            "balance": {},
            "recent_fills": [],
        }


def test_next_wake_at_aligns_live_5m_routes_to_utc_candle_close_grace():
    route = {"account_mode": "live", "cron_interval_minutes": 5}

    assert next_wake_at(route, from_time=datetime(2026, 7, 7, 7, 4, 0, tzinfo=UTC)) == datetime(
        2026, 7, 7, 7, 5, 15, tzinfo=UTC
    )
    assert next_wake_at(route, from_time=datetime(2026, 7, 7, 7, 5, 5, tzinfo=UTC)) == datetime(
        2026, 7, 7, 7, 5, 15, tzinfo=UTC
    )
    assert next_wake_at(route, from_time=datetime(2026, 7, 7, 7, 5, 16, tzinfo=UTC)) == datetime(
        2026, 7, 7, 7, 10, 15, tzinfo=UTC
    )


def test_next_wake_at_keeps_non_live_routes_on_relative_interval():
    route = {"account_mode": "paper", "cron_interval_minutes": 5}
    assert next_wake_at(route, from_time=datetime(2026, 7, 7, 7, 4, 0, tzinfo=UTC)) == datetime(
        2026, 7, 7, 7, 9, 0, tzinfo=UTC
    )


def test_next_wake_at_uses_pinned_bundle_cadence_over_route_setting():
    route = {
        "account_mode": "live",
        "cron_interval_minutes": 30,
        "active_bundle": {
            "execution_setup": {
                "decision_cadence": {
                    "decision_interval_minutes": 5,
                    "wake_grace_seconds": 45,
                    "cursor_seed_timestamp": "2026-06-30T23:59:59Z",
                }
            }
        },
    }
    assert next_wake_at(route, from_time=datetime(2026, 7, 7, 7, 4, 0, tzinfo=UTC)) == datetime(
        2026, 7, 7, 7, 5, 45, tzinfo=UTC
    )


def test_lifecycle_does_not_block_live_wake_when_research_signal_extension_fails(tmp_path):
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text("def decide(context):\n    return {'action': 'SKIP', 'reason_code': 'test'}\n")
    execution_setup = {"setup": {"entry_model": "market"}}
    (bundle_root / "execution_setup.json").write_text(json.dumps(execution_setup))
    bundle = {
        "bundle_id": "bundle-1",
        "bundle_uri": str(bundle_root),
        "strategy_module_ref": str(strategy_path),
        "strategy_id": "aave-strategy",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "source_stage1_session_id": "stage1-aave",
        "execution_setup": execution_setup,
        "risk_limits": {},
        "evidence_refs": {},
        "content_hash": "hash",
        "status": "promoted",
    }
    runtime_repository = FakeRuntimeRepository(bundle)

    def signal_pool_extender(**kwargs):
        raise RuntimeError("research signal pool unavailable")

    result = run_route_lifecycle_cycle(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=FakeMarketDataRepository(),
        fill_service=lambda **kwargs: {"status": "filled"},
        signal_pool_extender=signal_pool_extender,
        live_signal_scanner=lambda **kwargs: None,
        adapter=FakeAdapter(),
        workspace_root=tmp_path,
    )

    assert result["signal_update"]["status"] == "blocked"
    assert result["signal_update"]["reason"] == "signal_update_failed"
    assert result["wake"]["status"] == "blocked"
    assert result["wake"]["branch"] == "entry_scan"
    assert result["wake"]["signal_scan_result"]["reason"] == "signal_update_failed"
    assert runtime_repository.route["last_wake_at"] is not None


def test_lifecycle_extends_live_canonical_signal_pool_after_warmup(tmp_path):
    bundle = _bundle(tmp_path)
    runtime_repository = FakeRuntimeRepository(bundle)
    calls = []

    def signal_pool_extender(**kwargs):
        calls.append(kwargs)
        return {"status": "completed", "appended": 0}

    result = run_route_lifecycle_cycle(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=FakeMarketDataRepository(),
        fill_service=lambda **kwargs: {"status": "filled"},
        signal_pool_extender=signal_pool_extender,
        live_signal_scanner=lambda **kwargs: (_ for _ in ()).throw(AssertionError("raw scanner must not run")),
        adapter=FakeAdapter(),
        workspace_root=tmp_path,
    )

    assert result["signal_update"]["status"] == "completed"
    assert calls
    assert calls[0]["repository"] is runtime_repository
    assert calls[0]["signal_engine_id"] == "vegas_ema"
    assert calls[0]["asset"] == "AAVE"
    assert calls[0]["target_end"] is None
    assert result["wake"]["signal_scan_result"]["status"] == "no_fresh_canonical_signal"


def test_lifecycle_extends_four_hour_variant_canonical_pool_after_warmup(tmp_path):
    bundle = _bundle(tmp_path)
    runtime_repository = FakeRuntimeRepository(bundle)
    runtime_repository.route["signal_engine_id"] = "compression_participation_release_4h_v1"
    runtime_repository.engines[0][
        "signal_engine_id"
    ] = "compression_participation_release_4h_v1"
    calls = []

    def signal_pool_extender(**kwargs):
        calls.append(kwargs)
        return {"status": "completed", "appended": 0}

    run_route_lifecycle_cycle(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=FakeMarketDataRepository(),
        fill_service=lambda **kwargs: {"status": "filled"},
        signal_pool_extender=signal_pool_extender,
        live_signal_scanner=lambda **kwargs: (_ for _ in ()).throw(
            AssertionError("raw scanner must not run")
        ),
        adapter=FakeAdapter(),
        workspace_root=tmp_path,
    )

    assert calls[0]["signal_engine_id"] == "compression_participation_release_4h_v1"
    assert calls[0]["asset"] == "AAVE"
    assert calls[0]["target_end"] is None


def test_lifecycle_signal_extension_failure_still_runs_position_management(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
    )
    runtime_repository = FakeRuntimeRepository(bundle)

    def signal_pool_extender(**kwargs):
        raise ValueError("research signal pool unavailable")

    result = run_route_lifecycle_cycle(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=FakeMarketDataRepository(),
        fill_service=lambda **kwargs: {"status": "filled"},
        signal_pool_extender=signal_pool_extender,
        live_signal_scanner=lambda **kwargs: (_ for _ in ()).throw(AssertionError("raw scanner must not run")),
        adapter=FakeAdapter(positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long"}]),
        workspace_root=tmp_path,
    )

    assert result["signal_update"]["reason"] == "signal_update_failed"
    assert result["wake"]["status"] == "completed"
    assert result["wake"]["branch"] == "position_management"
    assert result["wake"]["strategy_decision"]["reason_code"] == "managed"


def _bundle(tmp_path, strategy_source=None):
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text(strategy_source or "def decide(context):\n    return {'action': 'SKIP', 'reason_code': 'test'}\n")
    execution_setup = {"setup": {"entry_model": "market"}}
    (bundle_root / "execution_setup.json").write_text(json.dumps(execution_setup))
    return {
        "bundle_id": "bundle-1",
        "bundle_uri": str(bundle_root),
        "strategy_module_ref": str(strategy_path),
        "strategy_id": "aave-strategy",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "source_stage1_session_id": "stage1-aave",
        "execution_setup": execution_setup,
        "risk_limits": {},
        "evidence_refs": {},
        "content_hash": "hash",
        "status": "promoted",
    }
