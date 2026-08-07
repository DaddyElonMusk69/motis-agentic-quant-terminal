from __future__ import annotations

import json
from pathlib import Path
from datetime import UTC, datetime, timedelta

from quant_terminal_worker.execution import atr_source
from quant_terminal_worker.execution.wake_runner import run_route_wake


class FakeAdapter:
    def __init__(self, *, positions=None, open_orders=None, protection_orders=None, balance=None, recent_fills=None, snapshot_error=None):
        self.positions = positions or []
        self.open_orders = open_orders or []
        self.protection_orders = protection_orders or []
        self.balance = balance or {}
        self.recent_fills = recent_fills or []
        self.snapshot_error = snapshot_error
        self.cancelled_order_ids = []

    def readiness_blockers(self):
        return []

    def snapshot(self, instrument):
        if self.snapshot_error is not None:
            raise self.snapshot_error
        return {
            "instrument": instrument,
            "positions": self.positions,
            "open_orders": self.open_orders,
            "protection_orders": self.protection_orders,
            "balance": self.balance,
            "recent_fills": self.recent_fills,
        }

    def cancel_order(self, *, instrument, order_id=None, client_order_id=None):
        if not order_id and client_order_id:
            order_id = client_order_id
        self.cancelled_order_ids.append(order_id)
        return {
            "instrument": instrument,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "status": "cancel_requested",
        }


class FakeRepository:
    def __init__(self, *, route, bundle, signals=None, owner_state=None, latest_confirmed_candle_ts=None):
        self.route = route
        self.bundle = bundle
        self.signals = signals or []
        self.owner_state = owner_state
        self.latest_confirmed_candle_ts = latest_confirmed_candle_ts or datetime(2026, 6, 5, 0, 0, tzinfo=UTC)
        self.stage1_sessions = {
            "stage1-aave": {
                "session_id": "stage1-aave",
                "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            }
        }
        self.closed_all_owner_states = []
        self.wakes = []
        self.closed_owner_states = []
        self.updated_owner_states = []
        self.live_signal_observations = []

    def get_deployment_route(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return {**self.route, "active_bundle": self.bundle}

    def update_deployment_route_gate(self, route_id, **values):
        if route_id != self.route["route_id"]:
            return None
        self.route = {**self.route, **values}
        return {**self.route, "active_bundle": self.bundle}

    def get_open_owner_state(self, route_id):
        return self.owner_state

    def update_owner_state(self, owner_state_id, **changes):
        self.updated_owner_states.append((owner_state_id, changes))
        self.owner_state = {**(self.owner_state or {}), **changes}
        return self.owner_state

    def close_open_owner_state(self, route_id, reason):
        if not self.owner_state:
            return None
        self.owner_state = {
            **self.owner_state,
            "status": "closed",
            "closed_at": datetime.now(UTC),
            "position_state": {
                **(self.owner_state.get("position_state") or {}),
                "close_reason": reason,
            },
        }
        self.closed_owner_states.append(self.owner_state)
        return self.owner_state

    def close_open_owner_states(self, route_id, *, instrument=None, reason):
        if not self.owner_state:
            return []
        closed = self.close_open_owner_state(route_id, reason)
        self.closed_all_owner_states.append({"route_id": route_id, "instrument": instrument, "reason": reason})
        return [closed]

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

    def record_wake_run(self, wake):
        self.wakes.append(wake)
        return wake

    def record_live_signal_observation(self, observation):
        self.live_signal_observations.append(observation)
        return observation

    def has_live_entry_for_signal(self, *, route_id, signal_id):
        return any(
            wake.get("route_id") == route_id
            and wake.get("signal_scan_result", {}).get("signal_id") == signal_id
            and any(intent.get("action") in {"ENTER", "ENTER_LONG", "ENTER_SHORT"} for intent in wake.get("order_intents", []))
            for wake in self.wakes
        )


class FakeAtrMarketDataRepository:
    def get_ref(self, dataset_id):
        if dataset_id == "aave-atr-2h":
            return self._ref()
        return None

    def get_data_ref(self, *, asset, timeframe, origin, data_type):
        if (asset, timeframe, origin, data_type) == ("AAVE", "2h", "derived", "technical_indicator_atr"):
            return self._ref()
        return None

    def _ref(self):
        return {
            "dataset_id": "aave-atr-2h",
            "asset": "AAVE",
            "data_type": "technical_indicator_atr",
            "data_origin": "derived",
            "timeframe": "2h",
            "storage_backend": "parquet",
            "storage_uri": ".data/aave-atr-2h",
        }


def _patch_atr_rows(monkeypatch, *, atr_pct: float = 1.5):
    atr_source._cached_atr_series.cache_clear()
    monkeypatch.setattr(
        atr_source,
        "read_rows_from_ref",
        lambda *args, **kwargs: [
            {
                "timestamp": "2026-06-04T22:00:00Z",
                "available_at": "2026-06-05T00:00:00Z",
                "atr_pct": atr_pct,
                "warmup_complete": True,
                "confirm": 1,
            }
        ],
    )


def test_wake_blocks_before_exchange_when_route_gates_fail(tmp_path):
    bundle = _bundle(tmp_path)
    repository = FakeRepository(
        route={**_route(bundle), "enabled": False, "blockers": ["route_disabled"]},
        bundle=bundle,
    )
    adapter = FakeAdapter()

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "blocked"
    assert wake["branch"] == "route_gate"
    assert wake["blockers"] == ["route_disabled"]
    assert wake["exchange_snapshot"] == {}


def test_wake_routes_open_position_to_management_branch(tmp_path):
    bundle = _bundle(tmp_path, strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n")
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"owner_strategy_id": "aave-strategy"},
    )
    adapter = FakeAdapter(positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long"}])

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "completed"
    assert wake["branch"] == "position_management"
    assert wake["strategy_decision"]["action"] == "HOLD"
    assert wake["signal_scan_result"]["status"] == "skipped_position_open"


def test_wake_completes_manual_position_to_full_target_without_pyramiding(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={
            "sizing": {"margin_allocation_pct": 10, "leverage": 5},
            "setup": {"tp_pct": 2.0, "sl_pct": 1.0},
        },
    )
    owner_state = {
        "owner_state_id": "owner-manual",
        "position_instance_id": "manual-position",
        "bundle_id": bundle["bundle_id"],
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled"}]},
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1",
                "posSide": "net",
                "avgPx": "100",
                "markPx": "100",
                "notionalUsd": "100",
            }
        ],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "state": "live",
                "side": "sell",
                "sz": "1",
                "tpTriggerPx": "102",
                "slTriggerPx": "99",
            }
        ],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciliation = wake["strategy_decision"]["diagnostics"]["position_size_reconciliation"]
    intent = wake["order_intents"][0]
    assert wake["strategy_decision"]["action"] == "COMPLETE_POSITION"
    assert wake["strategy_decision"]["reason_code"] == "position_size_incomplete"
    assert reconciliation["current_margin_usd"] == 20
    assert reconciliation["target_margin_usd"] == 100
    assert reconciliation["missing_margin_usd"] == 80
    assert intent["action"] == "COMPLETE_POSITION"
    assert intent["side"] == "buy"
    assert intent["quantity"] == "80"
    assert intent["notional_usd"] == 400
    assert intent["target_currency"] == "margin"
    assert intent["position_instance_id"] == "manual-position"
    assert intent["sizing_source"] == "position_size_reconciliation"


def test_wake_does_not_complete_position_that_already_meets_target(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={"sizing": {"margin_allocation_pct": 10, "leverage": 5}, "setup": {"tp_pct": 2.0, "sl_pct": 1.0}},
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"position_instance_id": "position-1", "bundle_id": bundle["bundle_id"], "position_state": {}},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "5", "posSide": "net", "avgPx": "100", "markPx": "100", "notionalUsd": "500"}],
        protection_orders=[{"instId": "AAVE-USDT-SWAP", "state": "live", "side": "sell", "sz": "5", "tpTriggerPx": "102", "slTriggerPx": "99"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciliation = wake["strategy_decision"]["diagnostics"]["position_size_reconciliation"]
    assert wake["strategy_decision"]["action"] == "HOLD"
    assert "position_size_already_complete" in reconciliation["blockers"]
    assert wake["order_intents"] == []


def test_wake_does_not_complete_position_inside_ten_percent_tolerance(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={"sizing": {"margin_allocation_pct": 10, "leverage": 5}, "setup": {"tp_pct": 2.0, "sl_pct": 1.0}},
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"position_instance_id": "position-1", "bundle_id": bundle["bundle_id"], "position_state": {}},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "4.6", "posSide": "net", "avgPx": "100", "markPx": "100", "notionalUsd": "460"}],
        protection_orders=[{"instId": "AAVE-USDT-SWAP", "state": "live", "side": "sell", "sz": "4.6", "tpTriggerPx": "102", "slTriggerPx": "99"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciliation = wake["strategy_decision"]["diagnostics"]["position_size_reconciliation"]
    assert reconciliation["current_margin_usd"] == 92
    assert reconciliation["target_margin_usd"] == 100
    assert reconciliation["tolerance_usd"] == 10
    assert "position_size_already_complete" in reconciliation["blockers"]
    assert wake["order_intents"] == []


def test_wake_completes_position_below_ten_percent_tolerance(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={"sizing": {"margin_allocation_pct": 10, "leverage": 5}, "setup": {"tp_pct": 2.0, "sl_pct": 1.0}},
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"position_instance_id": "position-1", "bundle_id": bundle["bundle_id"], "position_state": {}},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "4.4", "posSide": "net", "avgPx": "100", "markPx": "100", "notionalUsd": "440"}],
        protection_orders=[{"instId": "AAVE-USDT-SWAP", "state": "live", "side": "sell", "sz": "4.4", "tpTriggerPx": "102", "slTriggerPx": "99"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciliation = wake["strategy_decision"]["diagnostics"]["position_size_reconciliation"]
    assert reconciliation["current_margin_usd"] == 88
    assert reconciliation["missing_margin_usd"] == 12
    assert wake["strategy_decision"]["action"] == "COMPLETE_POSITION"
    assert wake["order_intents"][0]["quantity"] == "12"


def test_wake_leaves_partial_position_to_configured_pyramid_rules(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={
            "sizing": {"margin_allocation_pct": 10, "leverage": 5},
            "setup": {"tp_pct": 2.0, "sl_pct": 1.0, "pyramid": {"step_pct": 1.0, "max_legs": 3}},
        },
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"position_instance_id": "position-1", "bundle_id": bundle["bundle_id"], "position_state": {}},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "net", "avgPx": "100", "markPx": "100", "notionalUsd": "100"}],
        protection_orders=[{"instId": "AAVE-USDT-SWAP", "state": "live", "side": "sell", "sz": "1", "tpTriggerPx": "102", "slTriggerPx": "99"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciliation = wake["strategy_decision"]["diagnostics"]["position_size_reconciliation"]
    assert wake["strategy_decision"]["action"] != "COMPLETE_POSITION"
    assert "pyramiding_configured" in reconciliation["blockers"]


def test_wake_adopts_manual_exchange_position_once(tmp_path):
    bundle = _bundle(
        tmp_path,
        strategy_source="def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n",
        setup={"setup": {"tp_pct": 2.0, "sl_pct": 1.0, "pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )

    class AdoptingRepository(FakeRepository):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.created_owner_states = []

        def create_owner_state(self, owner_state):
            self.created_owner_states.append(owner_state)
            self.owner_state = owner_state
            return owner_state

    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    repository = AdoptingRepository(route=route, bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "posId": "position-123",
                "pos": "2",
                "posSide": "long",
                "avgPx": "100",
                "markPx": "100",
                "notionalUsd": "500",
                "cTime": "1784479008367",
            }
        ],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    first_wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)
    run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert first_wake["branch"] == "position_management"
    assert len(repository.created_owner_states) == 1
    adopted = repository.created_owner_states[0]
    assert adopted["position_instance_id"] == "exchange-aave-live-position-123-1784479008367"
    assert adopted["position_state"]["adoption_source"] == "manual_exchange_position"
    assert adopted["position_state"]["inferred_pyramid_legs"] == 1
    assert adopted["position_state"]["pyramid_exposure_ambiguous"] is False
    assert adopted["position_state"]["legs"][0]["fill_source"] == "manual_exchange_adoption"


def test_wake_position_management_ignores_strategy_protection_override_and_uses_bundle_policy(tmp_path):
    strategy_source = (
        "def manage_position(context):\n"
        "    position = context['exchange_snapshot']['positions'][0]\n"
        "    entry = float(position['avgPx'])\n"
        "    size = position['pos']\n"
        "    return {\n"
        "        'action': 'UPDATE_PROTECTION',\n"
        "        'direction': 'LONG',\n"
        "        'quantity': size,\n"
        "        'tp': str(round(entry * 1.1, 2)),\n"
        "        'sl': str(round(entry * 0.9, 2)),\n"
        "        'reason_code': 'entry_price_protection_refresh',\n"
        "    }\n"
    )
    bundle = _bundle(tmp_path, strategy_source=strategy_source)
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"owner_strategy_id": "aave-strategy"},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1.5", "posSide": "net", "avgPx": "100"}],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "algo-1",
                "ordType": "oco",
                "state": "live",
                "side": "sell",
                "sz": "1.5",
                "tpTriggerPx": "105",
                "slTriggerPx": "95",
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "completed"
    assert wake["branch"] == "position_management"
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["tp"] == "102"
    assert wake["strategy_decision"]["sl"] == "99"
    assert wake["order_intents"] == [
        {
            "intent_id": f"{wake['wake_id']}:0",
            "route_id": "aave-live",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "signal_id": None,
            "action": "UPDATE_PROTECTION",
            "side": "sell",
            "direction": "LONG",
            "order_type": "market",
            "quantity": "1.5",
            "notional_usd": None,
            "trade_mode": "isolated",
            "target_currency": None,
            "leverage": 5,
            "price": None,
            "tp": "102",
            "sl": "99",
            "tp_pct": 2.0,
            "sl_pct": 1.0,
            "reduce_only": True,
                "client_order_id": wake["order_intents"][0]["client_order_id"],
                "status": "intent_only",
                "position_side": "net",
                "protection_phase": "initial",
                "initial_sl_pct": 1,
                "protection_enabled": False,
            }
        ]


def test_wake_position_management_defaults_to_bundle_tp_sl_when_strategy_has_no_manager(tmp_path):
    bundle = _bundle(tmp_path)
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={"owner_strategy_id": "aave-strategy"},
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "-0.3", "posSide": "short", "avgPx": "59.37"}],
        protection_orders=[],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "completed"
    assert wake["branch"] == "position_management"
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["reason_code"] == "bundle_protection_refresh"
    assert wake["strategy_decision"]["tp"] == "58.1826"
    assert wake["strategy_decision"]["sl"] == "59.9637"
    assert wake["order_intents"][0]["side"] == "buy"
    assert wake["order_intents"][0]["quantity"] == "0.3"
    assert wake["order_intents"][0]["tp"] == "58.1826"
    assert wake["order_intents"][0]["sl"] == "59.9637"


def test_wake_position_management_uses_short_side_split_policy_for_bundle_protection(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "policy_mode": "side_specific",
                "entry_model": "market",
                "final_tp_pct": 1.0,
                "initial_sl_pct": 0.5,
                "protection_enabled": False,
                "side_policies": {
                    "LONG": {
                        "protection_enabled": False,
                        "final_tp_pct": 1.0,
                        "lock_profit_pct": 1.0,
                        "initial_sl_pct": 0.5,
                    },
                    "SHORT": {
                        "protection_enabled": False,
                        "final_tp_pct": 3.0,
                        "lock_profit_pct": 3.0,
                        "initial_sl_pct": 1.25,
                    },
                },
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "-2", "posSide": "short", "avgPx": "100", "markPx": "99"}],
        protection_orders=[],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["direction"] == "SHORT"
    assert wake["strategy_decision"]["tp"] == "97"
    assert wake["strategy_decision"]["sl"] == "101.25"
    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    assert protection["policy_mode"] == "side_specific"
    assert protection["selected_side"] == "SHORT"
    assert protection["final_tp_pct"] == 3.0
    assert protection["initial_sl_pct"] == 1.25
    assert wake["order_intents"][0]["tp_pct"] == 3.0
    assert wake["order_intents"][0]["sl_pct"] == 1.25


def test_wake_position_management_resolves_atr_policy_for_live_protection_refresh(tmp_path, monkeypatch):
    _patch_atr_rows(monkeypatch, atr_pct=2.0)
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "entry_model": "market",
                "leverage": 5,
                "protection_enabled": True,
                "atr_source": {
                    "timeframe": "2h",
                    "tp_multiplier": 1.5,
                    "sl_multiplier": 0.5,
                    "protect_trigger_multiplier": 1.0,
                    "trail_sl_multiplier": 0.4,
                },
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "2",
                "posSide": "long",
                "avgPx": "100",
                "markPx": "103",
                "opened_at": "2026-06-05T00:00:00Z",
            }
        ],
        protection_orders=[],
    )

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
        market_data_repository=FakeAtrMarketDataRepository(),
        workspace_root=tmp_path,
    )

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    intent = wake["order_intents"][0]
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["tp"] == "103"
    assert wake["strategy_decision"]["sl"] == "100.8"
    assert protection["phase"] == "protected"
    assert protection["atr_source"]["dataset_id"] == "aave-atr-2h"
    assert protection["atr_source"]["base_atr_pct"] == 2.0
    assert protection["final_tp_pct"] == 3.0
    assert protection["initial_sl_pct"] == 1.0
    assert protection["protect_trigger_pct"] == 2.0
    assert protection["trail_sl_pct"] == 0.8
    assert intent["tp_pct"] == 3.0
    assert intent["sl_pct"] == 0.8
    assert intent["initial_sl_pct"] == 1
    assert intent["trail_sl_pct"] == 0.8
    assert intent["protect_trigger_pct"] == 2


def test_wake_position_management_uses_short_side_split_policy_for_protected_sl(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "policy_mode": "side_specific",
                "entry_model": "market",
                "final_tp_pct": 1.0,
                "initial_sl_pct": 0.5,
                "protection_enabled": False,
                "side_policies": {
                    "LONG": {
                        "protection_enabled": False,
                        "final_tp_pct": 1.0,
                        "lock_profit_pct": 1.0,
                        "initial_sl_pct": 0.5,
                    },
                    "SHORT": {
                        "protection_enabled": True,
                        "final_tp_pct": 3.0,
                        "lock_profit_pct": 3.0,
                        "initial_sl_pct": 1.25,
                        "protect_trigger_pct": 1.0,
                        "trail_sl_pct": 0.4,
                    },
                },
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "-2", "posSide": "short", "avgPx": "100", "markPx": "99"}],
        protection_orders=[],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["direction"] == "SHORT"
    assert protection["selected_side"] == "SHORT"
    assert protection["phase"] == "protected"
    assert protection["favorable_move_pct"] == 1
    assert wake["strategy_decision"]["tp"] == "97"
    assert wake["strategy_decision"]["sl"] == "99.6"
    assert wake["order_intents"][0]["tp_pct"] == 3.0
    assert wake["order_intents"][0]["sl_pct"] == 0.4


def test_wake_position_management_derives_protected_sl_from_mark_move(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "final_tp_pct": 2.0,
                "initial_sl_pct": 1.0,
                "protection_enabled": True,
                "protect_trigger_pct": 0.5,
                "trail_sl_pct": 0.2,
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1.5", "posSide": "long", "avgPx": "100", "markPx": "100.5", "notionalUsd": "500"}],
        protection_orders=[],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert protection["phase"] == "protected"
    assert protection["favorable_move_pct"] == 0.5
    assert wake["order_intents"][0]["tp"] == "102"
    assert wake["order_intents"][0]["sl"] == "100.2"


def test_wake_position_management_infers_protected_phase_from_live_sl_side_after_restart(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "final_tp_pct": 2.0,
                "initial_sl_pct": 1.0,
                "protection_enabled": True,
                "protect_trigger_pct": 0.5,
                "trail_sl_pct": 0.2,
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1.5", "posSide": "long", "avgPx": "100", "markPx": "100.1"}],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "algo-1",
                "state": "live",
                "side": "sell",
                "sz": "1.5",
                "tpTriggerPx": "102",
                "slTriggerPx": "100.2",
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    assert wake["strategy_decision"]["action"] == "HOLD"
    assert protection["phase"] == "protected"
    assert protection["sync_reason"] == "protection_already_synced"
    assert wake["order_intents"] == []


def test_wake_treats_full_position_protection_as_size_synced(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "final_tp_pct": 2.0,
                "initial_sl_pct": 1.0,
                "protection_enabled": False,
            }
        },
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={
            "owner_state_id": "owner-1",
            "position_instance_id": "pos-1",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled"}]},
        },
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "2", "posSide": "long", "avgPx": "100", "markPx": "100"}],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "algo-1",
                "state": "live",
                "side": "sell",
                "sz": "",
                "closeFraction": "1",
                "tpTriggerPx": "102",
                "slTriggerPx": "99",
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "HOLD"
    assert wake["strategy_decision"]["diagnostics"]["protection"]["sync_reason"] == "protection_already_synced"
    assert wake["order_intents"] == []


def test_wake_reversal_ignores_stale_opposite_side_protection_for_phase(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "policy_mode": "side_specific",
                "side_policies": {
                    "LONG": {"final_tp_pct": 3.7, "initial_sl_pct": 2.0, "protection_enabled": True, "protect_trigger_pct": 2.8, "trail_sl_pct": 1.4},
                    "SHORT": {"final_tp_pct": 4.6, "initial_sl_pct": 2.0, "protection_enabled": True, "protect_trigger_pct": 3.0, "trail_sl_pct": 1.5},
                },
            }
        },
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={
            "owner_state_id": "owner-old-long",
            "position_instance_id": "old-long",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled"}]},
        },
    )
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "-7.57", "posSide": "net", "avgPx": "1858.7158", "markPx": "1858.65"}],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "old-long-protection",
                "state": "live",
                "side": "sell",
                "sz": "5.36",
                "tpTriggerPx": "1947.74",
                "slTriggerPx": "1840.68",
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    intent = wake["order_intents"][0]
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["direction"] == "SHORT"
    assert protection["phase"] == "initial"
    assert protection["sync_reason"] == "live_protection_side_mismatch"
    assert protection["live_sl"] is None
    assert intent["side"] == "buy"
    assert intent["protection_phase"] == "initial"


def test_wake_reopened_position_rejects_same_side_protection_from_previous_episode(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={"setup": {"final_tp_pct": 2.0, "initial_sl_pct": 1.0, "protection_enabled": False}},
    )
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        owner_state={
            "owner_state_id": "owner-old",
            "position_instance_id": "old",
            "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled"}]},
        },
    )
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "2",
                "posSide": "net",
                "avgPx": "100",
                "markPx": "100",
                "cTime": "2000",
            }
        ],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "old-oco",
                "state": "live",
                "side": "sell",
                "sz": "2",
                "tpTriggerPx": "102",
                "slTriggerPx": "99",
                "cTime": "1000",
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    protection = wake["strategy_decision"]["diagnostics"]["protection"]
    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert protection["sync_reason"] == "live_protection_from_previous_position"
    assert protection["live_sl"] is None


def test_wake_position_management_refreshes_missing_protection_before_pyramiding(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"tp_pct": 2.0, "sl_pct": 1.0, "pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled", "entry_price": "100"}]},
    }
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1.5", "posSide": "long", "avgPx": "100", "markPx": "100.5", "notionalUsd": "500"}],
        protection_orders=[],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "UPDATE_PROTECTION"
    assert wake["strategy_decision"]["reason_code"] == "bundle_protection_refresh"
    assert wake["strategy_decision"]["diagnostics"]["pyramid"]["trigger_reached"] is True
    assert wake["order_intents"][0]["action"] == "UPDATE_PROTECTION"


def test_wake_position_management_context_includes_position_age_and_hard_gate(tmp_path):
    strategy_source = (
        "def manage_position(context):\n"
        "    pc = context['position_context']\n"
        "    return {\n"
        "        'action': 'HOLD',\n"
        "        'reason_code': 'managed',\n"
        "        'diagnostics': {\n"
        "            'age_hours': pc['age_hours'],\n"
        "            'hard_exit_after_hours': pc['hard_exit_after_hours'],\n"
        "            'size': pc['size'],\n"
        "            'entry_price': pc['entry_price'],\n"
        "        },\n"
        "    }\n"
    )
    opened_at = datetime.now(UTC) - timedelta(hours=2)
    bundle = _bundle(tmp_path, strategy_source=strategy_source, setup={"hard_exit_after_hours": 36})
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1.5",
                "posSide": "long",
                "avgPx": "100",
                "cTime": str(int(opened_at.timestamp() * 1000)),
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    diagnostics = wake["strategy_decision"]["diagnostics"]
    assert 1.9 <= diagnostics["age_hours"] <= 2.1
    assert diagnostics["hard_exit_after_hours"] == 36
    assert diagnostics["size"] == "1.5"
    assert diagnostics["entry_price"] == "100"


def test_wake_position_management_seeds_missing_entry_and_emits_long_pyramid(tmp_path):
    strategy_source = (
        "def manage_position(context):\n"
        "    return {'action': 'HOLD', 'reason_code': 'managed', 'diagnostics': {'pyramid': context['position_context']['pyramid']}}\n"
    )
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3, "sl_breakeven": True}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted"}]},
    }
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1.5",
                "posSide": "long",
                "avgPx": "100",
                "markPx": "100.5",
                "notionalUsd": "500",
            }
        ],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert wake["strategy_decision"]["reason_code"] == "pyramid_trigger_reached"
    assert repository.updated_owner_states[0][1]["position_state"]["legs"][0]["status"] == "filled"
    assert repository.updated_owner_states[0][1]["position_state"]["legs"][0]["entry_price"] == "100"
    assert repository.updated_owner_states[0][1]["position_state"]["legs"][0]["fill_source"] == "live_position"
    pyramid_context = wake["strategy_decision"]["diagnostics"]["pyramid"]
    assert pyramid_context["eligible"] is True
    assert pyramid_context["next_trigger_price"] == 100.5
    assert pyramid_context["filled_legs"] == 1
    assert pyramid_context["raw_legs"] == 1
    assert pyramid_context["inferred_legs"] == 1
    intent = wake["order_intents"][0]
    assert intent["action"] == "PYRAMID"
    assert intent["side"] == "buy"
    assert intent["position_instance_id"] == "pos-1"
    assert intent["pyramid_leg"] == 2
    assert intent["trigger_price"] == 100.5
    assert intent["last_leg_entry"] == 100
    assert intent["quantity"] == "100"
    assert intent["notional_usd"] == 500
    assert intent["target_currency"] == "margin"


def test_wake_position_management_reconciles_filled_pyramid_leg_and_uses_it_for_next_trigger(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {
            "direction": "LONG",
            "legs": [
                {"leg": 1, "status": "filled", "entry_price": "100", "client_order_id": "entry-1"},
                {"leg": 2, "status": "submitted", "client_order_id": "pyr-2", "quantity": "100"},
            ],
        },
    }
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "2.5", "posSide": "long", "avgPx": "100.6", "markPx": "101.606", "notionalUsd": "1000"}],
        recent_fills=[{"instId": "AAVE-USDT-SWAP", "clOrdId": "pyr-2", "ordId": "okx-pyr-2", "fillPx": "101", "fillSz": "1", "fillTime": "1780710000000"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciled = repository.updated_owner_states[0][1]["position_state"]["legs"][1]
    assert reconciled["status"] == "filled"
    assert reconciled["entry_price"] == "101"
    assert reconciled["exchange_order_id"] == "okx-pyr-2"
    assert wake["strategy_decision"]["diagnostics"]["pyramid"]["filled_legs"] == 2
    assert wake["strategy_decision"]["diagnostics"]["pyramid"]["last_leg_entry"] == 100.6
    assert wake["strategy_decision"]["diagnostics"]["pyramid"]["raw_legs"] == 2
    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert wake["order_intents"][0]["pyramid_leg"] == 3
    assert wake["order_intents"][0]["last_leg_entry"] == 100.6


def test_wake_position_management_blocks_pyramid_while_submitted_leg_is_working(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {
            "direction": "LONG",
            "legs": [
                {"leg": 1, "status": "filled", "entry_price": "100"},
                {"leg": 2, "status": "submitted", "client_order_id": "pyr-2"},
            ],
        },
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long", "avgPx": "100", "markPx": "200"}],
        open_orders=[{"instId": "AAVE-USDT-SWAP", "clOrdId": "pyr-2", "ordId": "okx-pyr-2", "reduceOnly": False, "age_minutes": 5}],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciled = repository.updated_owner_states[0][1]["position_state"]["legs"][1]
    assert reconciled["status"] == "working"
    assert reconciled["exchange_order_id"] == "okx-pyr-2"
    assert wake["strategy_decision"]["action"] == "HOLD"
    assert wake["order_intents"] == []
    assert "working_add_order_exists" in wake["strategy_decision"]["diagnostics"]["pyramid"]["blockers"]


def test_wake_position_management_allows_retry_after_submitted_pyramid_leg_is_cancelled(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {
            "direction": "LONG",
            "legs": [
                {"leg": 1, "status": "filled", "entry_price": "100"},
                {"leg": 2, "status": "submitted", "client_order_id": "pyr-2"},
            ],
        },
    }
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long", "avgPx": "100", "markPx": "100.5", "notionalUsd": "500"}],
        recent_fills=[{"instId": "AAVE-USDT-SWAP", "clOrdId": "pyr-2", "state": "canceled", "ordId": "okx-pyr-2"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    reconciled = repository.updated_owner_states[0][1]["position_state"]["legs"][1]
    assert reconciled["status"] == "cancelled"
    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert wake["order_intents"][0]["pyramid_leg"] == 2


def test_wake_position_management_emits_short_pyramid_when_mark_reaches_trigger(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "SHORT", "legs": [{"leg": 1, "status": "submitted", "entry_price": "100"}]},
    }
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "-1.5", "posSide": "short", "avgPx": "100", "markPx": "99.5", "notionalUsd": "500"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    intent = wake["order_intents"][0]
    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert intent["action"] == "PYRAMID"
    assert intent["side"] == "sell"
    assert intent["trigger_price"] == 99.5


def test_wake_position_management_infers_pyramid_leg_count_from_exchange_exposure(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    repository = FakeRepository(route=route, bundle=bundle, owner_state=None)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "10",
                "posSide": "long",
                "avgPx": "100",
                "markPx": "101",
                "notionalUsd": "1005",
            }
        ],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "algo-1",
                "state": "live",
                "side": "sell",
                "sz": "10",
                "tpTriggerPx": "102",
                "slTriggerPx": "99",
            }
        ],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    pyramid = wake["strategy_decision"]["diagnostics"]["pyramid"]
    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert pyramid["raw_legs"] == 2.01
    assert pyramid["inferred_legs"] == 2
    assert pyramid["next_trigger_price"] == 101
    assert wake["order_intents"][0]["pyramid_leg"] == 3
    assert wake["order_intents"][0]["quantity"] == "100"
    assert wake["order_intents"][0]["notional_usd"] == 500


def test_wake_position_management_blocks_pyramid_when_exchange_exposure_is_ambiguous(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    repository = FakeRepository(route=route, bundle=bundle, owner_state=None)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "7.5",
                "posSide": "long",
                "avgPx": "100",
                "markPx": "101",
                "notionalUsd": "750",
            }
        ],
        protection_orders=[
            {
                "instId": "AAVE-USDT-SWAP",
                "algoId": "algo-1",
                "state": "live",
                "side": "sell",
                "sz": "7.5",
                "tpTriggerPx": "102",
                "slTriggerPx": "99",
            }
        ],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    pyramid = wake["strategy_decision"]["diagnostics"]["pyramid"]
    assert wake["strategy_decision"]["action"] == "HOLD"
    assert pyramid["raw_legs"] == 1.5
    assert pyramid["inferred_legs"] is None
    assert "pyramid_exposure_ambiguous" in pyramid["blockers"]


def test_wake_position_management_can_pyramid_without_owner_state_when_exchange_exposure_is_clear(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    repository = FakeRepository(route=route, bundle=bundle, owner_state=None)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long", "avgPx": "100", "markPx": "100.5", "notionalUsd": "500"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert wake["strategy_decision"]["diagnostics"]["pyramid"]["eligible"] is True
    assert wake["order_intents"][0]["pyramid_leg"] == 2


def test_wake_position_management_ignores_stale_owner_bundle_for_pyramid_decision(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "old-bundle",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted", "entry_price": "100"}]},
    }
    route = {**_route(bundle), "margin_allocation_pct": 30.0, "leverage": 5.0, "manual_sizing_enabled": True}
    repository = FakeRepository(route=route, bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long", "avgPx": "100", "markPx": "100.5", "notionalUsd": "500"}],
        balance={"data": [{"ccy": "USDT", "totalEq": "1000"}]},
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "PYRAMID"
    assert "bundle_mismatch" not in wake["strategy_decision"]["diagnostics"]["pyramid"]["blockers"]


def test_wake_position_management_does_not_pyramid_when_fresh_add_order_exists(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'managed'}\n"
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={"setup": {"pyramid": {"step_pct": 0.5, "max_legs": 3}}},
    )
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted", "entry_price": "100"}]},
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        positions=[{"instId": "AAVE-USDT-SWAP", "pos": "1", "posSide": "long", "avgPx": "100", "markPx": "200"}],
        open_orders=[{"ordId": "add-1", "reduceOnly": False, "age_minutes": 5}],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "HOLD"
    assert wake["order_intents"] == []
    assert "working_add_order_exists" in wake["strategy_decision"]["diagnostics"]["pyramid"]["blockers"]


def test_wake_closes_open_owner_state_when_exchange_position_is_flat(tmp_path):
    bundle = _bundle(tmp_path)
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "submitted", "entry_price": "100"}]},
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(positions=[], open_orders=[])

    run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
        live_signal_scanner=lambda **kwargs: None,
    )

    assert repository.closed_owner_states[0]["status"] == "closed"
    assert repository.closed_owner_states[0]["position_state"]["close_reason"] == "exchange_position_flat"


def test_wake_no_position_closes_all_owner_states_and_cancels_stale_non_reduce_orders(tmp_path):
    bundle = _bundle(tmp_path)
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG"},
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(positions=[], open_orders=[{"ordId": "stale-add", "reduceOnly": False, "age_minutes": 45}])

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
        entry_order_ttl_minutes=30,
        live_signal_scanner=lambda **kwargs: None,
    )

    assert adapter.cancelled_order_ids == ["stale-add"]
    assert repository.closed_all_owner_states == [
        {"route_id": "aave-live", "instrument": "AAVE-USDT-SWAP", "reason": "exchange_position_flat"}
    ]
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "no_position_after_cleanup"


def test_wake_snapshot_failure_does_not_close_owner_state_or_cancel_orders(tmp_path):
    bundle = _bundle(tmp_path)
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-live",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "status": "open",
        "position_state": {"direction": "LONG"},
    }
    repository = FakeRepository(route=_route(bundle), bundle=bundle, owner_state=owner_state)
    adapter = FakeAdapter(
        open_orders=[{"ordId": "open-1", "reduceOnly": False, "age_minutes": 45}],
        snapshot_error=RuntimeError("okx unavailable"),
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "error"
    assert wake["error"]["message"] == "okx unavailable"
    assert adapter.cancelled_order_ids == []
    assert repository.closed_owner_states == []
    assert repository.closed_all_owner_states == []


def test_wake_forces_exit_when_stage0_forward_hours_gate_expires(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'still_valid'}\n"
    opened_at = datetime.now(UTC) - timedelta(hours=2)
    bundle = _bundle(tmp_path, strategy_source=strategy_source, setup={"forward_hours": 1})
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1.5",
                "posSide": "long",
                "avgPx": "100",
                "cTime": str(int(opened_at.timestamp() * 1000)),
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "EXIT"
    assert wake["strategy_decision"]["reason_code"] == "hard_time_gate_expired"
    assert wake["strategy_decision"]["quantity"] == "1.5"
    assert wake["strategy_decision"]["side"] == "sell"
    assert wake["strategy_decision"]["reduce_only"] is True
    assert wake["strategy_decision"]["diagnostics"]["hard_exit_after_hours"] == 1
    assert wake["order_intents"][0]["action"] == "EXIT"
    assert wake["order_intents"][0]["side"] == "sell"
    assert wake["order_intents"][0]["quantity"] == "1.5"
    assert wake["order_intents"][0]["notional_usd"] is None
    assert wake["order_intents"][0]["reduce_only"] is True


def test_wake_forces_exit_when_bundle_max_hold_hours_expires(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'still_valid'}\n"
    opened_at = datetime.now(UTC) - timedelta(hours=2)
    bundle = _bundle(tmp_path, strategy_source=strategy_source, setup={"setup": {"max_hold_hours": 1}})
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1.5",
                "posSide": "long",
                "avgPx": "100",
                "cTime": str(int(opened_at.timestamp() * 1000)),
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "EXIT"
    assert wake["strategy_decision"]["reason_code"] == "hard_time_gate_expired"
    assert wake["strategy_decision"]["diagnostics"]["hard_exit_after_hours"] == 1


def test_wake_does_not_force_exit_before_stage0_forward_hours_gate(tmp_path):
    strategy_source = "def manage_position(context):\n    return {'action': 'HOLD', 'reason_code': 'still_valid'}\n"
    opened_at = datetime.now(UTC) - timedelta(minutes=30)
    bundle = _bundle(tmp_path, strategy_source=strategy_source, setup={"hard_exit_after_hours": 1})
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        positions=[
            {
                "instId": "AAVE-USDT-SWAP",
                "pos": "1.5",
                "posSide": "long",
                "avgPx": "100",
                "cTime": str(int(opened_at.timestamp() * 1000)),
            }
        ],
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["strategy_decision"]["action"] == "HOLD"
    assert wake["strategy_decision"]["reason_code"] == "still_valid"
    assert wake["order_intents"] == []


def test_wake_exits_quietly_for_fresh_entry_order(tmp_path):
    bundle = _bundle(tmp_path)
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(open_orders=[{"ordId": "order-1", "reduceOnly": False, "age_minutes": 5}])

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "fresh_entry_order_exists"


def test_wake_cancels_stale_entry_order_and_stops_without_entry_scan(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(open_orders=[{"ordId": "stale-1", "reduceOnly": False, "age_minutes": 45}])

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
        entry_order_ttl_minutes=30,
        live_signal_scanner=lambda **kwargs: signal,
    )

    assert adapter.cancelled_order_ids == ["stale-1"]
    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["strategy_decision"] == {}
    assert wake["order_intents"] == []
    assert wake["signal_scan_result"]["status"] == "no_position_after_cleanup"


def test_wake_order_intent_uses_explicit_execution_sizing(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "entry_model": "market",
                "leverage": 5,
                "tp_pct": 2.0,
                "sl_pct": 1.0,
                "position_quantity": "1.25",
                "position_notional_usd": 10,
                "trade_mode": "isolated",
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[_signal("sig-1")])
    adapter = FakeAdapter()

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    assert wake["order_intents"][0]["quantity"] == "1.25"
    assert wake["order_intents"][0]["notional_usd"] == 10
    assert wake["order_intents"][0]["trade_mode"] == "isolated"


def test_wake_records_live_observation_without_canonical_signal_insert(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=FakeAdapter(),
    )

    assert wake["signal_scan_result"]["status"] == "fresh_signal"
    assert repository.signals == [signal]
    assert len(repository.live_signal_observations) == 1
    observation = repository.live_signal_observations[0]
    assert observation["signal_engine_id"] == signal["signal_engine_id"]
    assert observation["asset"] == signal["asset"]
    assert observation["route_id"] == "aave-live"
    assert observation["bundle_id"] == bundle["bundle_id"]
    assert observation["signal_id"] == signal["signal_id"]
    assert observation["payload"] == signal["payload"]
    assert observation["decision"]["signal_id"] == signal["signal_id"]


def test_wake_executes_stage4b_timing_strategy_wrapper(tmp_path):
    bundle = _bundle(tmp_path)
    bundle_root = Path(bundle["bundle_uri"])
    (bundle_root / "stage1a_base_strategy.py").write_text(
        "def decide(context):\n"
        "    return {'trade_action': 'ENTER', 'action': 'ENTER', 'direction': 'LONG', 'confidence': 0.7, 'reason_code': 'base_entry'}\n"
        "\n"
        "def manage_position(context):\n"
        "    return {'action': 'HOLD'}\n"
    )
    (bundle_root / "strategy.py").write_text(
        "from __future__ import annotations\n"
        "from datetime import UTC, datetime\n"
        "import importlib.util\n"
        "from pathlib import Path\n"
        "TIMING_OVERLAY = {'exclude_utc_hours': [1], 'exclude_utc_weekdays': [], 'applies_to': 'all'}\n"
        "_BASE_MODULE = None\n"
        "def _load_base_module():\n"
        "    global _BASE_MODULE\n"
        "    if _BASE_MODULE is not None:\n"
        "        return _BASE_MODULE\n"
        "    path = Path(__file__).with_name('stage1a_base_strategy.py')\n"
        "    spec = importlib.util.spec_from_file_location('stage4b_stage1a_base_strategy', path)\n"
        "    module = importlib.util.module_from_spec(spec)\n"
        "    spec.loader.exec_module(module)\n"
        "    _BASE_MODULE = module\n"
        "    return module\n"
        "def decide(context):\n"
        "    base = dict(_load_base_module().decide(context))\n"
        "    packet = context.get('packet') if isinstance(context.get('packet'), dict) else context\n"
        "    signal = context.get('signal') if isinstance(context.get('signal'), dict) else {}\n"
        "    payload = signal.get('payload') if isinstance(signal.get('payload'), dict) else {}\n"
        "    value = context.get('timestamp') or packet.get('timestamp') or signal.get('timestamp') or payload.get('timestamp')\n"
        "    timestamp = datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(UTC)\n"
        "    if timestamp.hour in set(TIMING_OVERLAY['exclude_utc_hours']):\n"
        "        return {**base, 'trade_action': 'SKIP', 'action': 'SKIP', 'direction': 'FLAT', 'reason_code': 'timing_filter_utc_window'}\n"
        "    return base\n"
        "def manage_position(context):\n"
        "    return _load_base_module().manage_position(context)\n"
    )
    tradable_signal = {**_signal("sig-enter"), "timestamp": "2026-06-05T00:00:00Z", "payload": {"timestamp": "2026-06-05T00:00:00Z"}}
    skipped_signal = {**_signal("sig-skip"), "timestamp": "2026-06-05T01:00:00Z", "payload": {"timestamp": "2026-06-05T01:00:00Z"}}
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[tradable_signal])

    enter_wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=FakeAdapter(),
    )
    repository.signals = [skipped_signal]
    repository.latest_confirmed_candle_ts = datetime(2026, 6, 5, 1, 0, tzinfo=UTC)
    skip_wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=FakeAdapter(),
    )

    assert enter_wake["strategy_decision"]["trade_action"] == "ENTER"
    assert enter_wake["order_intents"]
    assert skip_wake["strategy_decision"]["trade_action"] == "SKIP"
    assert skip_wake["strategy_decision"]["direction"] == "FLAT"
    assert skip_wake["strategy_decision"]["reason_code"] == "timing_filter_utc_window"
    assert skip_wake["order_intents"] == []


def test_wake_entry_intent_uses_short_side_split_policy(tmp_path):
    strategy_source = (
        "def decide(context):\n"
        "    return {'trade_action': 'ENTER', 'direction': 'SHORT', 'confidence': 0.7, 'reason_code': 'test_short_entry'}\n"
    )
    bundle = _bundle(
        tmp_path,
        strategy_source=strategy_source,
        setup={
            "setup": {
                "entry_model": "market",
                "position_quantity": "1.25",
                "policy_mode": "side_specific",
                "final_tp_pct": 1.0,
                "initial_sl_pct": 0.5,
                "side_policies": {
                    "LONG": {
                        "protection_enabled": False,
                        "final_tp_pct": 1.0,
                        "lock_profit_pct": 1.0,
                        "initial_sl_pct": 0.5,
                    },
                    "SHORT": {
                        "protection_enabled": False,
                        "final_tp_pct": 3.5,
                        "lock_profit_pct": 3.5,
                        "initial_sl_pct": 1.25,
                    },
                },
            }
        },
    )
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[_signal("sig-1")])
    adapter = FakeAdapter()

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    intent = wake["order_intents"][0]
    assert intent["direction"] == "SHORT"
    assert intent["side"] == "sell"
    assert intent["quantity"] == "1.25"
    assert intent["tp_pct"] == 3.5
    assert intent["sl_pct"] == 1.25


def test_wake_entry_intent_uses_route_margin_percent_and_leverage_per_pyramid_leg(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "sizing": {"margin_allocation_pct": 30, "leverage": 5},
            "setup": {
                "entry_model": "market",
                "tp_pct": 2.0,
                "sl_pct": 1.0,
                "pyramid": {"max_legs": 3},
            }
        },
    )
    route = {
        **_route(bundle),
        "margin_allocation_pct": 5.0,
        "leverage": 2.0,
        "manual_sizing_enabled": False,
    }
    repository = FakeRepository(route=route, bundle=bundle, signals=[_signal("sig-1")])
    adapter = FakeAdapter()
    adapter.snapshot = lambda instrument: {
        "instrument": instrument,
        "positions": [],
        "open_orders": [],
        "protection_orders": [],
        "balance": {"data": [{"ccy": "USDT", "totalEq": "1000", "availBal": "950"}]},
        "recent_fills": [],
    }

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    intent = wake["order_intents"][0]
    assert intent["quantity"] == "100"
    assert intent["notional_usd"] == 500
    assert intent["target_currency"] == "margin"
    assert intent["leverage"] == 5.0
    assert intent["sizing_source"] == "bundle_stage4_sizing"
    assert intent["account_equity_usd"] == 1000
    assert intent["margin_allocation_pct"] == 30.0
    assert intent["pyramid_max_legs"] == 3
    assert intent["margin_usd"] == 100


def test_wake_trades_canonical_signal_at_latest_confirmed_candle(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("canon-sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=FakeAdapter())

    scan = wake["signal_scan_result"]
    assert wake["branch"] == "entry_scan"
    assert scan["status"] == "fresh_signal"
    assert scan["source"] == "canonical_signal_pool"
    assert scan["signal_id"] == "canon-sig-1"
    assert scan["signal_set_key"] == "vegas_ema:AAVE:AAVE-vegas_ema-canonical"
    assert scan["signal_timestamp"] == "2026-06-05T00:00:00Z"
    assert scan["latest_confirmed_candle_ts"] == "2026-06-05T00:00:00Z"
    assert scan["freshness_seconds"] == 0
    assert scan["freshness_class"] == "fresh"
    assert wake["order_intents"]


def test_wake_entry_intent_resolves_atr_policy_from_signal_timestamp(tmp_path, monkeypatch):
    _patch_atr_rows(monkeypatch, atr_pct=1.5)
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {
                "entry_model": "market",
                "leverage": 5,
                "atr_source": {
                    "timeframe": "2h",
                    "tp_multiplier": 2.0,
                    "sl_multiplier": 0.5,
                },
            }
        },
    )
    signal = _signal("canon-atr-sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])
    adapter = FakeAdapter(
        balance={"data": [{"ccy": "USDT", "totalEq": "1000", "availBal": "950"}]},
    )

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
        market_data_repository=FakeAtrMarketDataRepository(),
        workspace_root=tmp_path,
    )

    intent = wake["order_intents"][0]
    assert wake["branch"] == "entry_scan"
    assert wake["signal_scan_result"]["status"] == "fresh_signal"
    assert intent["action"] == "ENTER"
    assert intent["signal_id"] == "canon-atr-sig-1"
    assert intent["tp_pct"] == 3.0
    assert intent["sl_pct"] == 0.75


def test_wake_trades_canonical_signal_up_to_ten_minutes_old_as_late(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("canon-sig-1")
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        signals=[signal],
        latest_confirmed_candle_ts=datetime(2026, 6, 5, 0, 10, tzinfo=UTC),
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=FakeAdapter())

    scan = wake["signal_scan_result"]
    assert scan["status"] == "fresh_signal"
    assert scan["freshness_seconds"] == 600
    assert scan["freshness_class"] == "late"
    assert wake["order_intents"]


def test_wake_rejects_stale_canonical_signal_older_than_ten_minutes(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("canon-sig-1")
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        signals=[signal],
        latest_confirmed_candle_ts=datetime(2026, 6, 5, 0, 10, 1, tzinfo=UTC),
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=FakeAdapter())

    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "late_stale_canonical_signal"
    assert wake["signal_scan_result"]["freshness_seconds"] == 601
    assert wake["order_intents"] == []


def test_wake_rejects_future_canonical_signal(tmp_path):
    bundle = _bundle(tmp_path)
    signal = {**_signal("future-sig"), "timestamp": "2026-06-05T00:05:00Z"}
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=FakeAdapter())

    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "future_canonical_signal"
    assert wake["order_intents"] == []


def test_wake_rejects_duplicate_canonical_entry_signal_id(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("canon-sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])
    repository.wakes.append(
        {
            "route_id": "aave-live",
            "signal_scan_result": {"signal_id": "canon-sig-1", "signal_timestamp": "2026-06-05T00:00:00Z"},
            "order_intents": [{"action": "ENTER"}],
        }
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=FakeAdapter())

    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "duplicate_canonical_signal"
    assert wake["order_intents"] == []


def test_wake_entry_intent_manual_sizing_override_takes_route_values(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "sizing": {"margin_allocation_pct": 30, "leverage": 5},
            "setup": {
                "entry_model": "market",
                "tp_pct": 2.0,
                "sl_pct": 1.0,
                "pyramid": {"max_legs": 2},
            },
        },
    )
    route = {
        **_route(bundle),
        "margin_allocation_pct": 20.0,
        "leverage": 3.0,
        "manual_sizing_enabled": True,
    }
    repository = FakeRepository(route=route, bundle=bundle, signals=[_signal("sig-1")])
    adapter = FakeAdapter()
    adapter.snapshot = lambda instrument: {
        "instrument": instrument,
        "positions": [],
        "open_orders": [],
        "protection_orders": [],
        "balance": {"data": [{"ccy": "USDT", "totalEq": "1000", "availBal": "950"}]},
        "recent_fills": [],
    }

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    intent = wake["order_intents"][0]
    assert intent["quantity"] == "100"
    assert intent["notional_usd"] == 300
    assert intent["leverage"] == 3.0
    assert intent["sizing_source"] == "manual_route_override"
    assert intent["margin_allocation_pct"] == 20.0


def test_wake_entry_sizing_reads_actual_okx_balance_list_shape(tmp_path):
    bundle = _bundle(tmp_path, setup={"sizing": {"margin_allocation_pct": 30, "leverage": 5}, "setup": {"pyramid": {"max_legs": 3}}})
    route = {
        **_route(bundle),
        "manual_sizing_enabled": False,
    }
    repository = FakeRepository(route=route, bundle=bundle, signals=[_signal("sig-1")])
    adapter = FakeAdapter()
    adapter.snapshot = lambda instrument: {
        "instrument": instrument,
        "positions": [],
        "open_orders": [],
        "protection_orders": [],
        "balance": [
            {
                "totalEq": "296.80156360597664",
                "details": [
                    {
                        "ccy": "USDT",
                        "availBal": "293.6569352993546",
                        "availEq": "293.6569352993546",
                        "eq": "296.86984349264094",
                        "eqUsd": "296.8015634286376",
                    }
                ],
            }
        ],
        "recent_fills": [],
    }

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    intent = wake["order_intents"][0]
    assert intent["account_equity_usd"] == 296.80156361
    assert intent["margin_usd"] == 29.68015636
    assert intent["notional_usd"] == 148.4007818


def test_wake_idles_when_canonical_signal_is_stale(tmp_path):
    bundle = _bundle(tmp_path)
    repository = FakeRepository(
        route=_route(bundle),
        bundle=bundle,
        signals=[_signal("old-historical-sig")],
        latest_confirmed_candle_ts=datetime(2026, 6, 5, 0, 11, tzinfo=UTC),
    )
    adapter = FakeAdapter()

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "late_stale_canonical_signal"
    assert wake["strategy_decision"] == {}
    assert wake["order_intents"] == []


def test_wake_skips_duplicate_canonical_signal_without_backlog_consumption(tmp_path):
    bundle = _bundle(tmp_path)
    signal = _signal("fresh-sig-1")
    repository = FakeRepository(route=_route(bundle), bundle=bundle, signals=[signal])
    repository.wakes.append(
        {
            "route_id": "aave-live",
            "signal_scan_result": {"signal_id": "fresh-sig-1", "signal_timestamp": "2026-06-05T00:00:00Z"},
            "order_intents": [{"action": "ENTER"}],
        }
    )
    adapter = FakeAdapter()

    wake = run_route_wake(
        route_id="aave-live",
        repository=repository,
        adapter=adapter,
    )

    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "duplicate_canonical_signal"
    assert wake["signal_scan_result"]["signal_id"] == "fresh-sig-1"
    assert wake["order_intents"] == []


def test_wake_triggers_consecutive_win_cooldown_from_exchange_close_fill(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "pause_rules": [{"type": "consecutive_wins", "consecutive_count": 2, "cooldown_hours": 6}],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    close_time_ms = datetime.fromtimestamp(int(close_time.timestamp() * 1000) / 1000, tz=UTC)
    route = {
        **_route(bundle),
        "risk_limits": {
            "max_notional_usd": 1000,
            "max_daily_loss_usd": 250,
            "pause_rule_state": {
                "consecutive_wins": {
                    "consecutive_wins": 1,
                    "last_processed_close_event_id": "tradeId:previous-win",
                }
            },
        },
    }
    repository = FakeRepository(
        route=route,
        bundle=bundle,
        owner_state={"owner_state_id": "owner-1", "position_state": {"direction": "LONG"}},
    )
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "close-win-2",
                "side": "sell",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "12.5",
                "fee": "-0.5",
            }
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["consecutive_wins"]
    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "skipped_pause_rule"
    assert wake["signal_scan_result"]["reason"] == "pause_rule_consecutive_wins"
    assert wake["order_intents"] == []
    assert pause_state["consecutive_wins"] == 2
    assert pause_state["last_processed_close_event_id"] == "tradeId:close-win-2"
    assert pause_state["last_close_net_pnl_usdt"] == 12.0
    assert pause_state["pause_until"] == (close_time_ms + timedelta(hours=6)).isoformat().replace("+00:00", "Z")


def test_wake_resets_consecutive_win_cooldown_after_exchange_loss_close_fill(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "pause_rules": [{"type": "consecutive_wins", "consecutive_count": 2, "cooldown_hours": 6}],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    route = {
        **_route(bundle),
        "risk_limits": {
            "max_notional_usd": 1000,
            "max_daily_loss_usd": 250,
            "pause_rule_state": {
                "consecutive_wins": {
                    "consecutive_wins": 2,
                    "pause_until": (datetime.now(UTC) + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
                    "last_processed_close_event_id": "tradeId:previous-win",
                }
            },
        },
    }
    repository = FakeRepository(
        route=route,
        bundle=bundle,
        owner_state={"owner_state_id": "owner-1", "position_state": {"direction": "SHORT"}},
    )
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "close-loss-1",
                "side": "buy",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "-8.0",
                "fee": "-0.2",
            }
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter, allow_entry_scan=False)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["consecutive_wins"]
    assert wake["status"] == "blocked"
    assert wake["signal_scan_result"]["status"] == "blocked"
    assert pause_state["consecutive_wins"] == 0
    assert pause_state["pause_until"] is None
    assert pause_state["last_processed_close_event_id"] == "tradeId:close-loss-1"
    assert pause_state["last_close_net_pnl_usdt"] == -8.2


def test_wake_triggers_consecutive_loss_cooldown_from_instrument_exchange_fills(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "pause_rules": [{"type": "consecutive_losses", "consecutive_count": 2, "cooldown_hours": 6}],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    close_time_ms = datetime.fromtimestamp(int(close_time.timestamp() * 1000) / 1000, tz=UTC)
    route = {
        **_route(bundle),
        "risk_limits": {
            "max_notional_usd": 1000,
            "max_daily_loss_usd": 250,
            "pause_rule_state": {
                "consecutive_losses": {
                    "consecutive_losses": 1,
                    "last_processed_close_event_id": "tradeId:previous-loss",
                }
            },
        },
    }
    repository = FakeRepository(route=route, bundle=bundle)
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "close-loss-2",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "-10.0",
                "fee": "-0.5",
            }
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["consecutive_losses"]
    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "skipped_pause_rule"
    assert wake["signal_scan_result"]["reason"] == "pause_rule_consecutive_losses"
    assert wake["order_intents"] == []
    assert pause_state["consecutive_losses"] == 2
    assert pause_state["last_processed_close_event_id"] == "tradeId:close-loss-2"
    assert pause_state["last_close_net_pnl_usdt"] == -10.5
    assert pause_state["pause_until"] == (close_time_ms + timedelta(hours=6)).isoformat().replace("+00:00", "Z")


def test_wake_resets_consecutive_loss_cooldown_after_exchange_win_close_fill(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "pause_rules": [{"type": "consecutive_losses", "consecutive_count": 2, "cooldown_hours": 6}],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    route = {
        **_route(bundle),
        "risk_limits": {
            "max_notional_usd": 1000,
            "max_daily_loss_usd": 250,
            "pause_rule_state": {
                "consecutive_losses": {
                    "consecutive_losses": 2,
                    "pause_until": (datetime.now(UTC) + timedelta(hours=2)).isoformat().replace("+00:00", "Z"),
                    "last_processed_close_event_id": "tradeId:previous-loss",
                }
            },
        },
    }
    repository = FakeRepository(route=route, bundle=bundle)
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "close-win-1",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "8.0",
                "fee": "-0.2",
            }
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter, allow_entry_scan=False)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["consecutive_losses"]
    assert wake["status"] == "blocked"
    assert wake["signal_scan_result"]["status"] == "blocked"
    assert pause_state["consecutive_losses"] == 0
    assert pause_state["pause_until"] is None
    assert pause_state["last_processed_close_event_id"] == "tradeId:close-win-1"
    assert pause_state["last_close_net_pnl_usdt"] == 7.8


def test_wake_triggers_profit_burst_pause_from_instrument_exchange_fills(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "sizing": {"initial_capital_usdt": 1000},
            "pause_rules": [
                {"type": "profit_burst", "profit_threshold_pct": 5, "lookback_hours": 24, "cooldown_hours": 6}
            ],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    close_time_ms = datetime.fromtimestamp(int(close_time.timestamp() * 1000) / 1000, tz=UTC)
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "aave-close-1",
                "fillTime": str(int((close_time - timedelta(hours=2)).timestamp() * 1000)),
                "realizedPnl": "30",
                "fee": "-1",
            },
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "aave-close-2",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "32",
                "fee": "-1",
            },
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["profit_burst"]
    assert wake["status"] == "completed"
    assert wake["branch"] == "idle"
    assert wake["signal_scan_result"]["status"] == "skipped_pause_rule"
    assert wake["signal_scan_result"]["reason"] == "pause_rule_profit_burst"
    assert wake["order_intents"] == []
    assert pause_state["window_net_pnl_usdt"] == 60.0
    assert pause_state["route_capital_usdt"] == 1000.0
    assert pause_state["profit_growth_pct"] == 6.0
    assert pause_state["last_trigger_close_event_id"] == "tradeId:aave-close-2"
    assert pause_state["pause_until"] == (close_time_ms + timedelta(hours=6)).isoformat().replace("+00:00", "Z")


def test_profit_burst_pause_ignores_other_instrument_fills(tmp_path):
    bundle = _bundle(
        tmp_path,
        setup={
            "setup": {"entry_model": "market", "leverage": 5, "tp_pct": 2.0, "sl_pct": 1.0},
            "sizing": {"initial_capital_usdt": 1000},
            "pause_rules": [
                {"type": "profit_burst", "profit_threshold_pct": 5, "lookback_hours": 24, "cooldown_hours": 6}
            ],
        },
    )
    close_time = datetime.now(UTC) - timedelta(minutes=5)
    repository = FakeRepository(route=_route(bundle), bundle=bundle)
    adapter = FakeAdapter(
        recent_fills=[
            {
                "instId": "AAVE-USDT-SWAP",
                "tradeId": "aave-close-1",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "41",
                "fee": "-1",
            },
            {
                "instId": "ETH-USDT-SWAP",
                "tradeId": "eth-close-1",
                "fillTime": str(int(close_time.timestamp() * 1000)),
                "realizedPnl": "500",
                "fee": "-1",
            },
        ]
    )

    wake = run_route_wake(route_id="aave-live", repository=repository, adapter=adapter, allow_entry_scan=False)

    pause_state = repository.route["risk_limits"]["pause_rule_state"]["profit_burst"]
    assert wake["status"] == "blocked"
    assert wake["signal_scan_result"]["status"] == "blocked"
    assert pause_state["window_net_pnl_usdt"] == 40.0
    assert pause_state["profit_growth_pct"] == 4.0
    assert pause_state["pause_until"] is None


def _route(bundle):
    return {
        "route_id": "aave-live",
        "active_bundle_id": bundle["bundle_id"],
        "strategy_id": bundle["strategy_id"],
        "strategy_version": bundle["strategy_version"],
        "signal_engine_id": bundle["signal_engine_id"],
        "signal_engine_version": bundle["signal_engine_version"],
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "account_mode": "live",
        "execution_adapter": "okx",
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 250},
        "promoted": True,
        "data_warmed": True,
        "manually_armed": True,
        "enabled": True,
        "blockers": [],
    }


def _bundle(tmp_path: Path, strategy_source: str | None = None, setup: dict | None = None):
    bundle_root = tmp_path / "bundle"
    bundle_root.mkdir()
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text(
        strategy_source
        or "def decide(context):\n"
        "    return {'trade_action': 'ENTER', 'direction': 'LONG', 'confidence': 0.7, 'reason_code': 'test_entry'}\n"
    )
    setup = setup or {
        "setup": {
            "entry_model": "market",
            "leverage": 5,
            "tp_pct": 2.0,
            "sl_pct": 1.0,
        }
    }
    (bundle_root / "execution_setup.json").write_text(json.dumps(setup))
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
        "execution_setup": setup,
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 250},
        "evidence_refs": {},
        "content_hash": "hash",
        "status": "promoted",
    }


def _signal(signal_id):
    return {
        "signal_id": signal_id,
        "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "timestamp": "2026-06-05T00:00:00Z",
        "data_refs": [],
        "payload_schema": "signal_packet.v2",
        "payload": {},
    }
