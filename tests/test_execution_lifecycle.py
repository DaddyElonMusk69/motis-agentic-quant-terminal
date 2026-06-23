from __future__ import annotations

import json
from datetime import UTC, datetime

from quant_terminal_worker.execution.lifecycle import run_route_lifecycle_cycle


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
    def readiness_blockers(self):
        return []

    def snapshot(self, instrument):
        return {
            "instrument": instrument,
            "positions": [],
            "open_orders": [],
            "protection_orders": [],
            "balance": {},
            "recent_fills": [],
        }


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
        "execution_setup": execution_setup,
        "risk_limits": {},
        "evidence_refs": {},
        "content_hash": "hash",
        "status": "promoted",
    }
    runtime_repository = FakeRuntimeRepository(bundle)

    def signal_pool_extender(**kwargs):
        raise ValueError("research signal pool unavailable")

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

    assert result["signal_update"]["status"] == "skipped"
    assert result["signal_update"]["reason"] == "live_execution_uses_observation_log"
    assert result["wake"]["status"] == "completed"
    assert result["wake"]["branch"] == "idle"
    assert result["wake"]["signal_scan_result"]["status"] == "no_fresh_signal"
    assert runtime_repository.route["last_wake_at"] is not None
