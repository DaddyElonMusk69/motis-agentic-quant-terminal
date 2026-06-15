from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from quant_terminal_worker.adapters.exchange import SwapOrderRequest, SwapProtectionRequest


def submit_wake_order_intents(
    *,
    route_id: str,
    wake_id: str,
    repository: Any,
    adapter: Any,
    confirm_live: bool = False,
    quantity_override: str | None = None,
    notional_usd_override: float | None = None,
) -> dict[str, Any]:
    route = repository.get_deployment_route(route_id)
    if route is None:
        raise ValueError(f"deployment route not found: {route_id}")
    wake = repository.get_wake_run(wake_id)
    if wake is None:
        raise ValueError(f"wake run not found: {wake_id}")
    if wake.get("route_id") != route_id:
        return _blocked(["wake_route_mismatch"], wake=wake, route=route)

    blockers = _route_blockers(route)
    if route.get("account_mode") == "live" and not confirm_live:
        blockers.append("live_confirmation_required")
    if blockers:
        return _blocked(blockers, wake=wake, route=route)

    order_intents = [dict(intent) for intent in wake.get("order_intents") or []]
    submittable = [intent for intent in order_intents if intent.get("status") == "intent_only"]
    if not submittable:
        return _blocked(["no_submittable_order_intents"], wake=wake, route=route)

    submittable = [
        _with_manual_sizing_override(
            intent,
            quantity_override=quantity_override,
            notional_usd_override=notional_usd_override,
        )
        for intent in submittable
    ]
    validation_blockers = _validate_intents(route=route, intents=submittable)
    if validation_blockers:
        return _blocked(validation_blockers, wake=wake, route=route)

    adapter_results = list(wake.get("adapter_results") or [])
    submitted_intents: list[dict[str, Any]] = []
    for intent in submittable:
        result = _submit_intent(adapter=adapter, intent=intent)
        protection_refresh_required = False
        action = _intent_action(intent=intent, wake=wake)
        if action in {"ENTER", "ENTER_LONG", "ENTER_SHORT", "PYRAMID"} and not _truthy(intent.get("reduce_only")):
            post_fill_protection = _ensure_post_fill_protection(adapter=adapter, intent={**intent, "action": action})
            if post_fill_protection is None:
                protection_refresh_required = True
            else:
                result = {"order": result} if "order" not in result and "data" in result else dict(result)
                result["post_fill_protection"] = post_fill_protection
        submitted = {
            **intent,
            "status": "submitted",
            "submitted_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "adapter_result_index": len(adapter_results),
        }
        adapter_results.append(result)
        submitted_intents.append(submitted)
        _replace_intent(order_intents, submitted)

    stored_wake = repository.update_wake_execution_results(
        wake_id=wake_id,
        order_intents=order_intents,
        adapter_results=adapter_results,
    )
    for intent in submitted_intents:
        action = _intent_action(intent=intent, wake=wake)
        adapter_result = adapter_results[intent["adapter_result_index"]] if intent.get("adapter_result_index") is not None else {}
        if action in {"ENTER", "ENTER_LONG", "ENTER_SHORT"} and not _truthy(intent.get("reduce_only")):
            repository.create_owner_state(
                _owner_state(
                    route=route,
                    wake=wake,
                    intent={**intent, "action": action},
                    adapter_result=adapter_result,
                    protection_refresh_required=not bool(adapter_result.get("post_fill_protection")),
                )
            )
        elif action == "PYRAMID" and not _truthy(intent.get("reduce_only")):
            owner_state = repository.get_open_owner_state(route_id)
            if owner_state is not None:
                repository.append_owner_state_leg(
                    owner_state["owner_state_id"],
                    _owner_state_leg({**intent, "action": action}, adapter_result=adapter_result),
                )
    return {
        "status": "submitted",
        "submitted_count": len(submitted_intents),
        "blockers": [],
        "wake": stored_wake,
        "adapter_results": adapter_results,
    }


def _submit_intent(*, adapter: Any, intent: dict[str, Any]) -> dict[str, Any]:
    if _canonical_action(intent.get("action")) == "UPDATE_PROTECTION":
        updater = getattr(adapter, "ensure_swap_protection", None)
        if updater is None:
            raise ValueError("adapter does not support protection updates")
        return {
            "protection": updater(_swap_protection_request(intent)),
        }

    leverage_result = _set_leverage_if_requested(adapter=adapter, intent=intent)
    protection_cancel_result = _cancel_existing_protection_before_position_order(adapter=adapter, intent=intent)
    order_result = adapter.place_swap_order(_swap_order_request(intent))
    result: dict[str, Any] = {"order": order_result}
    if leverage_result is not None:
        result["leverage"] = leverage_result
    if protection_cancel_result is not None:
        result["protection_cancel"] = protection_cancel_result
    return result if len(result) > 1 else order_result


def _validate_intents(*, route: dict[str, Any], intents: list[dict[str, Any]]) -> list[str]:
    blockers: list[str] = []
    for intent in intents:
        if _numeric(intent.get("quantity")) <= 0:
            blockers.append("missing_order_quantity")
        notional = _numeric(intent.get("notional_usd"))
        action = _canonical_action(intent.get("action"))
        reduce_only = _truthy(intent.get("reduce_only"))
        if action != "UPDATE_PROTECTION" and not reduce_only and notional <= 0:
            blockers.append("missing_order_notional_usd")
    return list(dict.fromkeys(blockers))


def _intent_action(*, intent: dict[str, Any], wake: dict[str, Any]) -> str:
    decision = wake.get("strategy_decision") if isinstance(wake.get("strategy_decision"), dict) else {}
    return _canonical_action(intent.get("action") or decision.get("action") or decision.get("trade_action"))


def _with_manual_sizing_override(
    intent: dict[str, Any],
    *,
    quantity_override: str | None,
    notional_usd_override: float | None,
) -> dict[str, Any]:
    if quantity_override in (None, "") and notional_usd_override is None:
        return intent
    updated = dict(intent)
    if quantity_override not in (None, ""):
        updated["quantity"] = str(quantity_override)
    if notional_usd_override is not None:
        updated["notional_usd"] = notional_usd_override
    updated["sizing_source"] = "manual_submit_override"
    return updated


def _swap_order_request(intent: dict[str, Any]) -> SwapOrderRequest:
    return SwapOrderRequest(
        inst_id=str(intent["instrument"]),
        side=str(intent["side"]),
        order_type=str(intent.get("order_type") or "market"),
        size=str(intent["quantity"]),
        trade_mode=str(intent.get("trade_mode") or "isolated"),
        client_order_id=str(intent["client_order_id"]),
        position_side=intent.get("position_side"),
        price=str(intent["price"]) if intent.get("price") not in (None, "") else None,
        target_currency=intent.get("target_currency") or intent.get("tgt_ccy"),
        tp_trigger_price=str(intent["tp"]) if intent.get("tp") not in (None, "") else None,
        sl_trigger_price=str(intent["sl"]) if intent.get("sl") not in (None, "") else None,
        reduce_only=_truthy(intent.get("reduce_only")),
    )


def _swap_protection_request(intent: dict[str, Any]) -> SwapProtectionRequest:
    return SwapProtectionRequest(
        inst_id=str(intent["instrument"]),
        side=str(intent["side"]),
        size=str(intent["quantity"]),
        trade_mode=str(intent.get("trade_mode") or "isolated"),
        tp_trigger_price=str(intent["tp"]),
        sl_trigger_price=str(intent["sl"]),
        position_side=intent.get("position_side"),
    )


def _ensure_post_fill_protection(*, adapter: Any, intent: dict[str, Any]) -> dict[str, Any] | None:
    if intent.get("tp") not in (None, "") and intent.get("sl") not in (None, ""):
        return None
    snapshotter = getattr(adapter, "snapshot", None)
    updater = getattr(adapter, "ensure_swap_protection", None)
    if snapshotter is None or updater is None:
        return None
    snapshot = snapshotter(str(intent["instrument"]))
    position = _active_position(snapshot=snapshot, instrument=str(intent["instrument"]))
    if position is None:
        return None
    entry_price = _numeric(_first_present(position, "avgPx", "avg_price", "entry_price", "openAvgPx"))
    size = abs(_numeric(position.get("pos") or position.get("size") or position.get("sz")))
    tp_pct = _numeric(intent.get("tp_pct"))
    sl_pct = _numeric(intent.get("sl_pct"))
    if entry_price <= 0 or size <= 0 or tp_pct <= 0 or sl_pct <= 0:
        return None
    direction = _position_direction(position=position, fallback=str(intent.get("direction") or "LONG"))
    tp, sl = _protection_prices(entry_price=entry_price, direction=direction, tp_pct=tp_pct, sl_pct=sl_pct)
    return updater(
        SwapProtectionRequest(
            inst_id=str(intent["instrument"]),
            side="sell" if direction == "LONG" else "buy",
            size=_format_decimal(size),
            trade_mode=str(intent.get("trade_mode") or "isolated"),
            tp_trigger_price=tp,
            sl_trigger_price=sl,
            position_side=intent.get("position_side"),
        )
    )


def _cancel_existing_protection_before_position_order(*, adapter: Any, intent: dict[str, Any]) -> dict[str, Any] | None:
    if _canonical_action(intent.get("action")) not in {"ENTER", "ENTER_LONG", "ENTER_SHORT", "PYRAMID", "EXIT", "REDUCE"}:
        return None
    canceller = getattr(adapter, "cancel_swap_protection_orders", None)
    if canceller is None:
        return None
    return canceller(str(intent["instrument"]))


def _set_leverage_if_requested(*, adapter: Any, intent: dict[str, Any]) -> dict[str, Any] | None:
    leverage = intent.get("leverage")
    if leverage in (None, ""):
        return None
    if _numeric(leverage) <= 0:
        return None
    setter = getattr(adapter, "set_swap_leverage", None)
    if setter is None:
        return None
    return setter(
        inst_id=str(intent["instrument"]),
        leverage=_format_decimal(leverage),
        margin_mode=str(intent.get("trade_mode") or "isolated"),
        position_side=intent.get("position_side"),
    )


def _replace_intent(order_intents: list[dict[str, Any]], submitted: dict[str, Any]) -> None:
    for index, intent in enumerate(order_intents):
        if intent.get("intent_id") == submitted.get("intent_id"):
            order_intents[index] = submitted
            return


def _owner_state(
    *,
    route: dict[str, Any],
    wake: dict[str, Any],
    intent: dict[str, Any],
    adapter_result: dict[str, Any] | None = None,
    protection_refresh_required: bool = False,
) -> dict[str, Any]:
    position_instance_id = str(
        intent.get("position_instance_id")
        or f"pos-{route['route_id']}-{wake['wake_id']}-{str(intent.get('intent_id', '0')).split(':')[-1]}"
    )
    submitted_at = intent.get("submitted_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z")
    return {
        "owner_state_id": f"owner-{route['route_id']}-{wake['wake_id']}-{intent['intent_id']}",
        "route_id": route["route_id"],
        "bundle_id": wake.get("bundle_id") or route["active_bundle_id"],
        "position_instance_id": position_instance_id,
        "asset": route["asset"],
        "instrument": route["instrument"],
        "account_mode": route["account_mode"],
        "owner_strategy_id": route["strategy_id"],
        "owner_strategy_version": route["strategy_version"],
        "opened_from_signal_id": intent.get("signal_id"),
        "status": "open",
        "position_state": {
            "schema_version": "position_episode.v1",
            "position_instance_id": position_instance_id,
            "direction": str(intent.get("direction") or "LONG").upper(),
            "opened_wake_id": wake["wake_id"],
            "opened_from_signal_id": intent.get("signal_id"),
            "opened_bundle_id": wake.get("bundle_id") or route["active_bundle_id"],
            "pyramid_setup": {
                "max_legs": intent.get("pyramid_max_legs"),
                "margin_allocation_pct": intent.get("margin_allocation_pct"),
                "leverage": intent.get("leverage"),
            },
            "legs": [_owner_state_leg({**intent, "pyramid_leg": 1, "submitted_at": submitted_at}, adapter_result=adapter_result)],
            "protection_refresh_required": protection_refresh_required,
        },
    }


def _owner_state_leg(intent: dict[str, Any], *, adapter_result: dict[str, Any] | None = None) -> dict[str, Any]:
    order_result = _extract_order_result(adapter_result or {})
    leg = {
        "leg": int(_numeric(intent.get("pyramid_leg") or 1)),
        "action": _canonical_action(intent.get("action")),
        "status": "submitted",
        "intent_id": intent.get("intent_id"),
        "client_order_id": intent.get("client_order_id"),
        "submitted_at": intent.get("submitted_at") or datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "side": intent.get("side"),
        "direction": intent.get("direction"),
        "quantity": intent.get("quantity"),
        "target_currency": intent.get("target_currency"),
        "margin_usd": intent.get("margin_usd"),
        "notional_usd": intent.get("notional_usd"),
        "leverage": intent.get("leverage"),
        "trigger_price": intent.get("trigger_price"),
        "last_leg_entry": intent.get("last_leg_entry"),
        "entry_price": intent.get("entry_price"),
    }
    exchange_order_id = _first_result_value(order_result, "ordId", "order_id", "orderId")
    exchange_client_order_id = _first_result_value(order_result, "clOrdId", "exchange_client_order_id", "client_order_id")
    if exchange_order_id not in (None, ""):
        leg["exchange_order_id"] = exchange_order_id
    if exchange_client_order_id not in (None, ""):
        leg["exchange_client_order_id"] = exchange_client_order_id
    return leg


def _extract_order_result(adapter_result: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(adapter_result, dict):
        return {}
    order = adapter_result.get("order") if isinstance(adapter_result.get("order"), dict) else adapter_result
    if isinstance(order.get("data"), list) and order["data"] and isinstance(order["data"][0], dict):
        return {**order, **order["data"][0]}
    return order


def _first_result_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) not in (None, ""):
            return mapping[key]
    return None


def _route_blockers(route: dict[str, Any]) -> list[str]:
    blockers = list(route.get("blockers") or [])
    if blockers:
        return blockers
    if not route.get("enabled"):
        blockers.append("route_disabled")
    if not route.get("active_bundle_id"):
        blockers.append("missing_active_bundle")
    if not route.get("promoted"):
        blockers.append("route_not_promoted")
    if not route.get("data_warmed"):
        blockers.append("data_not_warmed")
    if route.get("account_mode") == "live" and not route.get("manually_armed"):
        blockers.append("route_not_manually_armed")
    return blockers


def _blocked(blockers: list[str], *, wake: dict[str, Any], route: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "blocked",
        "submitted_count": 0,
        "blockers": blockers,
        "wake": wake,
        "route": route,
        "adapter_results": wake.get("adapter_results") or [],
    }


def _numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _format_decimal(value: Any) -> str:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.12f}".rstrip("0").rstrip(".")


def _active_position(*, snapshot: dict[str, Any], instrument: str) -> dict[str, Any] | None:
    for position in snapshot.get("positions") or []:
        if str(position.get("instId") or position.get("instrument") or "") != instrument:
            continue
        if abs(_numeric(position.get("pos") or position.get("size") or position.get("sz"))) > 0:
            return position
    return None


def _position_direction(*, position: dict[str, Any], fallback: str) -> str:
    side = str(position.get("posSide") or position.get("position_side") or position.get("side") or "").lower()
    if side in {"short", "sell"}:
        return "SHORT"
    if side in {"long", "buy"}:
        return "LONG"
    raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
    if raw_size < 0:
        return "SHORT"
    return str(fallback or "LONG").upper()


def _protection_prices(*, entry_price: float, direction: str, tp_pct: float, sl_pct: float) -> tuple[str, str]:
    if direction == "SHORT":
        tp = entry_price * (1 - tp_pct / 100)
        sl = entry_price * (1 + sl_pct / 100)
    else:
        tp = entry_price * (1 + tp_pct / 100)
        sl = entry_price * (1 - sl_pct / 100)
    return _format_decimal(tp), _format_decimal(sl)


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) not in (None, ""):
            return mapping[key]
    return None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"true", "1", "yes"}


def _canonical_action(value: Any) -> str:
    action = str(value or "").upper()
    aliases = {
        "LONG": "ENTER_LONG",
        "SHORT": "ENTER_SHORT",
        "ENTER": "ENTER",
        "BUY": "ENTER_LONG",
        "SELL": "ENTER_SHORT",
    }
    return aliases.get(action, action)
