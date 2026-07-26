from __future__ import annotations

from quant_terminal_worker.adapters.exchange import ExchangeAdapterError
from quant_terminal_worker.execution.order_submission import submit_wake_order_intents


class FakeAdapter:
    def __init__(self):
        self.orders = []
        self.leverage_calls = []
        self.protection_cancel_calls = []
        self.protection_update_calls = []
        self.snapshot_response = None

    def set_swap_leverage(self, **kwargs):
        self.leverage_calls.append(kwargs)
        return {"status": "ok", **kwargs}

    def place_swap_order(self, request):
        self.orders.append(request)
        return {
            "ordId": f"order-{len(self.orders)}",
            "clOrdId": request.client_order_id,
            "sCode": "0",
        }

    def cancel_swap_protection_orders(self, instrument):
        self.protection_cancel_calls.append(instrument)
        return {"status": "cancelled", "instrument": instrument, "cancelled_count": 1}

    def ensure_swap_protection(self, request):
        self.protection_update_calls.append(request)
        return {"status": "amended", "instrument": request.inst_id}

    def snapshot(self, instrument):
        if self.snapshot_response is None:
            return {"instrument": instrument, "positions": [], "open_orders": [], "protection_orders": [], "balance": {}, "recent_fills": []}
        return self.snapshot_response


class FakeRepository:
    def __init__(self, *, route, wake, owner_state=None):
        self.route = route
        self.wake = wake
        self.open_owner_state = owner_state
        self.owner_states = []
        self.appended_legs = []
        self.updated_owner_states = []
        self.updated_wakes = []

    def get_deployment_route(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return self.route

    def get_wake_run(self, wake_id):
        if wake_id != self.wake["wake_id"]:
            return None
        return self.wake

    def update_wake_execution_results(self, *, wake_id, order_intents, adapter_results):
        assert wake_id == self.wake["wake_id"]
        self.wake = {
            **self.wake,
            "order_intents": order_intents,
            "adapter_results": adapter_results,
        }
        self.updated_wakes.append(self.wake)
        return self.wake

    def create_owner_state(self, owner_state):
        self.owner_states.append(owner_state)
        self.open_owner_state = owner_state
        return owner_state

    def get_open_owner_state(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return self.open_owner_state

    def append_owner_state_leg(self, owner_state_id, leg):
        self.appended_legs.append((owner_state_id, leg))
        state = dict(self.open_owner_state or {})
        position_state = dict(state.get("position_state") or {})
        legs = list(position_state.get("legs") or [])
        legs.append(leg)
        position_state["legs"] = legs
        position_state["protection_refresh_required"] = True
        state["position_state"] = position_state
        self.open_owner_state = state
        return state

    def update_owner_state(self, owner_state_id, **changes):
        self.updated_owner_states.append((owner_state_id, changes))
        state = {**(self.open_owner_state or {}), **changes}
        self.open_owner_state = state
        return state


def test_live_submission_requires_explicit_confirmation():
    repository = FakeRepository(route=_route(account_mode="live"), wake=_wake(route_id="aave-live"))
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-live",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "blocked"
    assert result["blockers"] == ["live_confirmation_required"]
    assert adapter.orders == []
    assert repository.owner_states == []


def test_submission_places_order_and_writes_owner_state_for_entry_intent():
    repository = FakeRepository(route=_route(account_mode="demo"), wake=_wake(quantity="1.5", notional_usd=10))
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.orders[0].inst_id == "AAVE-USDT-SWAP"
    assert adapter.orders[0].side == "buy"
    assert adapter.orders[0].size == "1.5"
    assert adapter.orders[0].trade_mode == "isolated"
    assert adapter.protection_cancel_calls == ["AAVE-USDT-SWAP"]
    assert repository.updated_wakes[0]["order_intents"][0]["status"] == "submitted"
    assert repository.updated_wakes[0]["adapter_results"][0]["order"]["ordId"] == "order-1"
    assert repository.updated_wakes[0]["adapter_results"][0]["protection_cancel"]["cancelled_count"] == 1
    assert repository.owner_states[0]["route_id"] == "aave-demo"
    assert repository.owner_states[0]["opened_from_signal_id"] == "sig-1"
    assert repository.owner_states[0]["status"] == "open"
    assert repository.owner_states[0]["position_instance_id"].startswith("pos-aave-demo-wake-1-")
    assert repository.owner_states[0]["position_state"]["direction"] == "LONG"
    assert repository.owner_states[0]["position_state"]["legs"][0]["leg"] == 1
    assert repository.owner_states[0]["position_state"]["legs"][0]["status"] == "submitted"
    assert repository.owner_states[0]["position_state"]["legs"][0]["exchange_order_id"] == "order-1"
    assert repository.owner_states[0]["position_state"]["legs"][0]["exchange_client_order_id"] == "motis-aave-vegas-wake-1"


def test_submission_appends_pyramid_leg_without_new_owner_state():
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-demo",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "account_mode": "demo",
        "owner_strategy_id": "aave-strategy",
        "owner_strategy_version": "v0.1",
        "opened_from_signal_id": "sig-1",
        "status": "open",
        "position_state": {
            "direction": "LONG",
            "legs": [{"leg": 1, "status": "submitted", "entry_price": "100"}],
        },
    }
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(
            action="PYRAMID",
            side="buy",
            quantity="2",
            notional_usd=10,
            target_currency="margin",
            leverage=5,
            position_instance_id="pos-1",
            pyramid_leg=2,
            trigger_price="100.5",
            last_leg_entry="100",
        ),
        owner_state=owner_state,
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.protection_cancel_calls == ["AAVE-USDT-SWAP"]
    assert adapter.orders[0].target_currency == "margin"
    assert adapter.leverage_calls[0]["leverage"] == "5"
    assert repository.owner_states == []
    assert repository.appended_legs[0][0] == "owner-1"
    assert repository.appended_legs[0][1]["leg"] == 2
    assert repository.appended_legs[0][1]["action"] == "PYRAMID"
    assert repository.appended_legs[0][1]["exchange_order_id"] == "order-1"
    assert repository.appended_legs[0][1]["exchange_client_order_id"] == "motis-aave-vegas-wake-1"
    assert repository.open_owner_state["position_state"]["protection_refresh_required"] is True


def test_submission_appends_position_completion_without_new_owner_state():
    owner_state = {
        "owner_state_id": "owner-1",
        "route_id": "aave-demo",
        "bundle_id": "bundle-1",
        "position_instance_id": "pos-1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "account_mode": "demo",
        "owner_strategy_id": "aave-strategy",
        "owner_strategy_version": "v0.1",
        "status": "open",
        "position_state": {"direction": "LONG", "legs": [{"leg": 1, "status": "filled"}]},
    }
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(
            action="COMPLETE_POSITION",
            side="buy",
            quantity="80",
            notional_usd=400,
            target_currency="margin",
            leverage=5,
            position_instance_id="pos-1",
        ),
        owner_state=owner_state,
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.orders[0].size == "80"
    assert adapter.orders[0].target_currency == "margin"
    assert repository.owner_states == []
    assert repository.appended_legs[0][1]["action"] == "COMPLETE_POSITION"


def test_submission_refuses_zero_quantity_intent():
    repository = FakeRepository(route=_route(account_mode="demo"), wake=_wake(quantity="0", notional_usd=10))
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "blocked"
    assert result["blockers"] == ["missing_order_quantity"]
    assert adapter.orders == []
    assert repository.owner_states == []


def test_submission_can_use_explicit_manual_size_override():
    repository = FakeRepository(route=_route(account_mode="demo"), wake=_wake(quantity="0"))
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
        quantity_override="0.5",
        notional_usd_override=10,
    )

    assert result["status"] == "submitted"
    assert adapter.orders[0].size == "0.5"
    assert repository.updated_wakes[0]["order_intents"][0]["quantity"] == "0.5"
    assert repository.updated_wakes[0]["order_intents"][0]["notional_usd"] == 10
    assert repository.updated_wakes[0]["order_intents"][0]["sizing_source"] == "manual_submit_override"


def test_submission_sets_leverage_and_places_margin_sized_order():
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(quantity="2", notional_usd=10, target_currency="margin", leverage=5),
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.leverage_calls == [
        {
            "inst_id": "AAVE-USDT-SWAP",
            "leverage": "5",
            "margin_mode": "isolated",
            "position_side": None,
        }
    ]
    assert adapter.orders[0].size == "2"
    assert adapter.orders[0].target_currency == "margin"
    assert repository.updated_wakes[0]["adapter_results"][0]["leverage"]["status"] == "ok"
    assert repository.updated_wakes[0]["adapter_results"][0]["order"]["ordId"] == "order-1"


def test_submission_passes_attached_tp_sl_to_adapter():
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(quantity="2", notional_usd=10, target_currency="margin", tp="1620", sl="1560"),
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.orders[0].tp_trigger_price == "1620"
    assert adapter.orders[0].sl_trigger_price == "1560"


def test_submission_places_post_fill_protection_from_entry_price_and_bundle_percentages():
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(quantity="2", notional_usd=10, target_currency="margin", side="sell", direction="SHORT"),
    )
    adapter = FakeAdapter()
    adapter.snapshot_response = {
        "instrument": "AAVE-USDT-SWAP",
        "positions": [{"instId": "AAVE-USDT-SWAP", "pos": "-0.3", "posSide": "short", "avgPx": "59.37"}],
        "open_orders": [],
        "protection_orders": [],
        "balance": {},
        "recent_fills": [],
    }

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.protection_update_calls[0].side == "buy"
    assert adapter.protection_update_calls[0].size == "0.3"
    assert adapter.protection_update_calls[0].tp_trigger_price == "58.06386"
    assert adapter.protection_update_calls[0].sl_trigger_price == "60.26055"
    assert repository.owner_states[0]["position_state"]["protection_refresh_required"] is False
    assert repository.updated_wakes[0]["adapter_results"][0]["post_fill_protection"]["status"] == "amended"


def test_submission_updates_protection_without_new_entry_order():
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(
            action="UPDATE_PROTECTION",
            side="sell",
            quantity="1.5",
            notional_usd=None,
            tp="1620",
            sl="1560",
            reduce_only=True,
        ),
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.orders == []
    assert adapter.protection_cancel_calls == []
    assert adapter.protection_update_calls[0].inst_id == "AAVE-USDT-SWAP"
    assert adapter.protection_update_calls[0].side == "sell"
    assert adapter.protection_update_calls[0].size == "1.5"
    assert adapter.protection_update_calls[0].tp_trigger_price == "1620"
    assert adapter.protection_update_calls[0].sl_trigger_price == "1560"
    assert adapter.protection_update_calls[0].tp_pct == 2.2
    assert adapter.protection_update_calls[0].sl_pct == 1.5
    assert repository.owner_states == []


def test_automatic_submission_failure_is_persisted_instead_of_left_pending():
    repository = FakeRepository(
        route=_route(account_mode="live"),
        wake=_wake(
            route_id="aave-live",
            action="UPDATE_PROTECTION",
            side="sell",
            quantity="1.5",
            notional_usd=None,
            tp="1620",
            sl="1560",
            reduce_only=True,
        ),
    )
    adapter = FakeAdapter()
    adapter.ensure_swap_protection = lambda request: (_ for _ in ()).throw(
        ExchangeAdapterError('[{"sCode":"51524","sMsg":"Unable to modify quantity"}]')
    )

    result = submit_wake_order_intents(
        route_id="aave-live",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=True,
    )

    assert result["status"] == "failed"
    assert result["error"]["code"] == "51524"
    stored_intent = repository.updated_wakes[0]["order_intents"][0]
    assert stored_intent["status"] == "submission_failed"
    assert stored_intent["submission_error"]["code"] == "51524"
    assert stored_intent["failed_at"].endswith("Z")


def test_submission_exit_cancels_protection_and_places_reduce_only_close_without_notional():
    repository = FakeRepository(
        route=_route(account_mode="demo"),
        wake=_wake(
            action="EXIT",
            side="sell",
            quantity="1.5",
            notional_usd=None,
            reduce_only=True,
        ),
    )
    adapter = FakeAdapter()

    result = submit_wake_order_intents(
        route_id="aave-demo",
        wake_id="wake-1",
        repository=repository,
        adapter=adapter,
        confirm_live=False,
    )

    assert result["status"] == "submitted"
    assert adapter.protection_cancel_calls == ["AAVE-USDT-SWAP"]
    assert adapter.orders[0].side == "sell"
    assert adapter.orders[0].size == "1.5"
    assert adapter.orders[0].reduce_only is True
    assert repository.owner_states == []


def _route(*, account_mode: str):
    return {
        "route_id": f"aave-{account_mode}",
        "active_bundle_id": "bundle-1",
        "strategy_id": "aave-strategy",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "account_mode": account_mode,
        "execution_adapter": "okx",
        "risk_limits": {"max_notional_usd": 1000, "max_daily_loss_usd": 250},
        "promoted": True,
        "data_warmed": True,
        "manually_armed": account_mode == "live",
        "enabled": True,
        "blockers": [],
    }


def _wake(
    *,
    route_id: str = "aave-demo",
    action: str = "ENTER",
    side: str = "buy",
    direction: str = "LONG",
    quantity: str = "1",
    notional_usd: float | None = None,
    target_currency: str | None = None,
    leverage: int | None = None,
    tp: str | None = None,
    sl: str | None = None,
    tp_pct: float | None = 2.2,
    sl_pct: float | None = 1.5,
    reduce_only: bool = False,
    position_instance_id: str | None = None,
    pyramid_leg: int | None = None,
    trigger_price: str | None = None,
    last_leg_entry: str | None = None,
):
    intent = {
        "intent_id": "wake-1:0",
        "route_id": route_id,
        "asset": "AAVE",
        "instrument": "AAVE-USDT-SWAP",
        "signal_id": "sig-1",
        "action": action,
        "side": side,
        "direction": direction,
        "order_type": "market",
        "quantity": quantity,
        "trade_mode": "isolated",
        "reduce_only": reduce_only,
        "client_order_id": "motis-aave-vegas-wake-1",
        "status": "intent_only",
    }
    if notional_usd is not None:
        intent["notional_usd"] = notional_usd
    if target_currency is not None:
        intent["target_currency"] = target_currency
    if leverage is not None:
        intent["leverage"] = leverage
    if tp is not None:
        intent["tp"] = tp
    if sl is not None:
        intent["sl"] = sl
    if tp_pct is not None:
        intent["tp_pct"] = tp_pct
    if sl_pct is not None:
        intent["sl_pct"] = sl_pct
    if position_instance_id is not None:
        intent["position_instance_id"] = position_instance_id
    if pyramid_leg is not None:
        intent["pyramid_leg"] = pyramid_leg
    if trigger_price is not None:
        intent["trigger_price"] = trigger_price
    if last_leg_entry is not None:
        intent["last_leg_entry"] = last_leg_entry
    return {
        "wake_id": "wake-1",
        "route_id": route_id,
        "bundle_id": "bundle-1",
        "status": "completed",
        "branch": "entry_scan",
        "blockers": [],
        "signal_scan_result": {"status": "evaluated", "signal_id": "sig-1"},
        "strategy_decision": {"action": "ENTER"},
        "order_intents": [intent],
        "adapter_results": [],
        "error": {},
    }
