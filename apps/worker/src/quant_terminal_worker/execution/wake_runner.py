from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from quant_terminal_worker.execution.bundle_loader import load_execution_bundle
from quant_terminal_worker.execution.canonical_signal_admission import load_latest_canonical_entry_signal
from quant_terminal_worker.execution.live_signal_scan import scan_latest_live_signal


DEFAULT_ENTRY_ORDER_TTL_MINUTES = 30
PYRAMID_LEG_BUCKET_TOLERANCE = 0.35
POSITION_SIZE_RECONCILIATION_TOLERANCE_PCT = 10.0


def run_route_wake(
    *,
    route_id: str,
    repository: Any,
    adapter: Any,
    workspace_root: Path | None = None,
    entry_order_ttl_minutes: int = DEFAULT_ENTRY_ORDER_TTL_MINUTES,
    allow_entry_scan: bool = True,
    live_signal_scanner: Callable[..., dict[str, Any] | None] | None = None,
) -> dict[str, Any]:
    workspace = workspace_root or Path.cwd()
    route = repository.get_deployment_route(route_id)
    if route is None:
        raise ValueError(f"deployment route not found: {route_id}")

    started_at = datetime.now(UTC)
    wake_id = f"wake-{route_id}-{started_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    blockers = _route_blockers(route)
    if blockers:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": route.get("active_bundle_id"),
                "status": "blocked",
                "branch": "route_gate",
                "blockers": blockers,
                "exchange_snapshot": {},
                "signal_scan_result": {
                    "status": "not_run",
                    "reason": "route or adapter gates blocked wake",
                },
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": [],
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    adapter_blockers = list(adapter.readiness_blockers()) if hasattr(adapter, "readiness_blockers") else []
    if adapter_blockers:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": route.get("active_bundle_id"),
                "status": "blocked",
                "branch": "route_gate",
                "blockers": adapter_blockers,
                "exchange_snapshot": {},
                "signal_scan_result": {
                    "status": "not_run",
                    "reason": "adapter gates blocked wake",
                },
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": [],
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    bundle = route.get("active_bundle")
    if bundle is None and route.get("active_bundle_id"):
        bundle = repository.get_execution_bundle(route["active_bundle_id"])
    if bundle is None:
        return _record_wake(
            repository,
            _error_wake(
                wake_id=wake_id,
                route=route,
                message="active execution bundle not found",
            ),
        )

    runtime = load_execution_bundle(bundle, workspace_root=workspace)
    try:
        snapshot = adapter.snapshot(route["instrument"])
    except Exception as exc:  # pragma: no cover - defensive adapter boundary
        return _record_wake(
            repository,
            _error_wake(
                wake_id=wake_id,
                route=route,
                message=str(exc),
                bundle_id=bundle["bundle_id"],
            ),
        )

    positions = _active_positions(snapshot)
    adapter_results = []
    working_entry_orders = _working_entry_orders(snapshot)
    if positions:
        fresh_working_entry_orders = [
            order
            for order in working_entry_orders
            if _order_age_minutes(order) < entry_order_ttl_minutes
        ]
        fresh_order_ids = {id(order) for order in fresh_working_entry_orders}
        for order in working_entry_orders:
            if id(order) in fresh_order_ids:
                continue
            order_id = str(order.get("ordId") or order.get("order_id") or "")
            client_order_id = str(order.get("clOrdId") or order.get("client_order_id") or "")
            if hasattr(adapter, "cancel_order") and (order_id or client_order_id):
                adapter_results.append(
                    adapter.cancel_order(
                        instrument=route["instrument"],
                        order_id=order_id or None,
                        client_order_id=client_order_id or None,
                    )
                )
        owner_state = repository.get_open_owner_state(route_id)
        if owner_state is None:
            owner_state = _adopt_exchange_position(
                repository=repository,
                route=route,
                bundle=bundle,
                wake_id=wake_id,
                position=positions[0],
                snapshot=snapshot,
                execution_setup=runtime["execution_setup"],
                now=started_at,
            )
        else:
            owner_state = _reconcile_owner_state(
                repository=repository,
                owner_state=owner_state,
                position=positions[0],
                snapshot=snapshot,
            )
        decision = _run_position_management(
            runtime=runtime,
            route=route,
            snapshot=snapshot,
            owner_state=owner_state,
            position=positions[0],
            working_entry_orders=fresh_working_entry_orders,
            now=started_at,
        )
        position_intents = _normalize_strategy_order_intents(
            wake_id=wake_id,
            route=route,
            signal=None,
            decision=decision,
            execution_setup=runtime["execution_setup"],
            snapshot=snapshot,
        )
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "position_management",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {"status": "skipped_position_open"},
                "strategy_decision": decision,
                "order_intents": position_intents,
                "adapter_results": adapter_results,
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    fresh_orders = [
        order
        for order in working_entry_orders
        if _order_age_minutes(order) < entry_order_ttl_minutes
    ]
    if fresh_orders:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "idle",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {
                    "status": "fresh_entry_order_exists",
                    "order_count": len(fresh_orders),
                },
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": [],
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    for order in working_entry_orders:
        order_id = str(order.get("ordId") or order.get("order_id") or "")
        client_order_id = str(order.get("clOrdId") or order.get("client_order_id") or "")
        if hasattr(adapter, "cancel_order") and (order_id or client_order_id):
            adapter_results.append(
                adapter.cancel_order(
                    instrument=route["instrument"],
                    order_id=order_id or None,
                    client_order_id=client_order_id or None,
                )
            )
    owner_state_before_flat = repository.get_open_owner_state(route_id)
    if hasattr(repository, "close_open_owner_states"):
        repository.close_open_owner_states(route_id, instrument=route["instrument"], reason="exchange_position_flat")
    elif hasattr(repository, "close_open_owner_state"):
        repository.close_open_owner_state(route_id, reason="exchange_position_flat")
    route = _update_pause_rule_states(
        repository=repository,
        route=route,
        execution_setup=runtime["execution_setup"],
        snapshot=snapshot,
        owner_state=owner_state_before_flat,
        now=started_at,
    )

    if working_entry_orders:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "idle",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {
                    "status": "no_position_after_cleanup",
                    "cancelled_order_count": len(adapter_results),
                },
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    active_pause = _active_pause_rule(route=route, execution_setup=runtime["execution_setup"], now=started_at)
    if active_pause is not None:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "idle",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": active_pause,
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    if not allow_entry_scan:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "blocked",
                "branch": "entry_scan",
                "blockers": ["signal_update_failed"],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {"status": "blocked", "reason": "signal_update_failed"},
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {"message": "signal update failed before entry scan"},
                "completed_at": datetime.now(UTC),
            },
        )

    try:
        if route.get("account_mode") == "live":
            admission = load_latest_canonical_entry_signal(
                route=route,
                bundle=bundle,
                repository=repository,
                workspace_root=workspace,
            )
            signal = admission["signal"]
            signal_scan_result = admission["scan_result"]
        else:
            scanner = live_signal_scanner or scan_latest_live_signal
            signal = scanner(route=route, repository=repository, workspace_root=workspace)
            signal_scan_result = _fresh_signal_scan_result(signal) if signal is not None else {"status": "no_fresh_signal"}
    except ValueError as exc:
        failure_reason = "canonical_signal_admission_failed" if route.get("account_mode") == "live" else "live_signal_scan_failed"
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "blocked",
                "branch": "entry_scan",
                "blockers": [failure_reason],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {"status": "blocked", "reason": failure_reason},
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {"message": str(exc)},
                "completed_at": datetime.now(UTC),
            },
        )
    if signal is None:
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "idle",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": signal_scan_result,
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )
    if _has_live_entry_for_signal(repository=repository, route_id=route_id, signal_id=signal["signal_id"]) or (
        route.get("account_mode") == "live"
        and _processed_entry_signal_timestamp(repository=repository, route_id=route_id) >= _signal_timestamp(signal)
    ):
        return _record_wake(
            repository,
            {
                "wake_id": wake_id,
                "route_id": route_id,
                "bundle_id": bundle["bundle_id"],
                "status": "completed",
                "branch": "idle",
                "blockers": [],
                "exchange_snapshot": snapshot,
                "signal_scan_result": {
                    **signal_scan_result,
                    "status": "duplicate_canonical_signal" if route.get("account_mode") == "live" else "duplicate_live_signal",
                },
                "strategy_decision": {},
                "order_intents": [],
                "adapter_results": adapter_results,
                "error": {},
                "completed_at": datetime.now(UTC),
            },
        )

    decision = _run_entry_decision(runtime=runtime, route=route, signal=signal, snapshot=snapshot)
    _record_live_signal_observation(
        repository=repository,
        route=route,
        bundle=bundle,
        signal=signal,
        decision=decision,
        signal_scan_result=signal_scan_result,
    )
    order_intents = _normalize_strategy_order_intents(
        wake_id=wake_id,
        route=route,
        signal=signal,
        decision=decision,
        execution_setup=runtime["execution_setup"],
        snapshot=snapshot,
    )
    wake = {
        "wake_id": wake_id,
        "route_id": route_id,
        "bundle_id": bundle["bundle_id"],
        "status": "completed",
        "branch": "entry_scan",
        "blockers": [],
        "exchange_snapshot": snapshot,
        "signal_scan_result": signal_scan_result,
        "strategy_decision": decision,
        "order_intents": order_intents,
        "adapter_results": adapter_results,
        "error": {},
        "completed_at": datetime.now(UTC),
    }
    return _record_wake(repository, wake)


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


def _active_consecutive_win_pause(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    rule = _consecutive_win_pause_rule(execution_setup)
    if rule is None:
        return None
    state = _consecutive_win_pause_state(route)
    pause_until = _parse_optional_timestamp(state.get("pause_until"))
    if pause_until is None or now >= pause_until:
        return None
    return {
        "status": "skipped_pause_rule",
        "reason": "pause_rule_consecutive_wins",
        "pause_rule": rule,
        "pause_until": _iso_timestamp(pause_until),
        "consecutive_wins": int(state.get("consecutive_wins") or 0),
        "last_close_at": state.get("last_close_at"),
        "last_close_event_id": state.get("last_processed_close_event_id"),
    }


def _active_consecutive_loss_pause(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    rule = _consecutive_loss_pause_rule(execution_setup)
    if rule is None:
        return None
    state = _consecutive_loss_pause_state(route)
    pause_until = _parse_optional_timestamp(state.get("pause_until"))
    if pause_until is None or now >= pause_until:
        return None
    return {
        "status": "skipped_pause_rule",
        "reason": "pause_rule_consecutive_losses",
        "pause_rule": rule,
        "pause_until": _iso_timestamp(pause_until),
        "consecutive_losses": int(state.get("consecutive_losses") or 0),
        "last_close_at": state.get("last_close_at"),
        "last_close_event_id": state.get("last_processed_close_event_id"),
    }


def _active_profit_burst_pause(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    rule = _profit_burst_pause_rule(execution_setup)
    if rule is None:
        return None
    state = _profit_burst_pause_state(route)
    pause_until = _parse_optional_timestamp(state.get("pause_until"))
    if pause_until is None or now >= pause_until:
        return None
    return {
        "status": "skipped_pause_rule",
        "reason": "pause_rule_profit_burst",
        "pause_rule": rule,
        "pause_until": _iso_timestamp(pause_until),
        "profit_growth_pct": float(state.get("profit_growth_pct") or 0.0),
        "window_net_pnl_usdt": float(state.get("window_net_pnl_usdt") or 0.0),
        "route_capital_usdt": float(state.get("route_capital_usdt") or rule["route_capital_usdt"]),
        "latest_close_at": state.get("latest_close_at"),
        "latest_close_event_id": state.get("latest_close_event_id"),
    }


def _active_pause_rule(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    return (
        _active_consecutive_loss_pause(route=route, execution_setup=execution_setup, now=now)
        or _active_consecutive_win_pause(route=route, execution_setup=execution_setup, now=now)
        or _active_profit_burst_pause(
            route=route,
            execution_setup=execution_setup,
            now=now,
        )
    )


def _update_pause_rule_states(
    *,
    repository: Any,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    owner_state: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    route = _update_consecutive_loss_pause_state(
        repository=repository,
        route=route,
        execution_setup=execution_setup,
        snapshot=snapshot,
        now=now,
    )
    route = _update_consecutive_win_pause_state(
        repository=repository,
        route=route,
        execution_setup=execution_setup,
        snapshot=snapshot,
        owner_state=owner_state,
        now=now,
    )
    return _update_profit_burst_pause_state(
        repository=repository,
        route=route,
        execution_setup=execution_setup,
        snapshot=snapshot,
        now=now,
    )


def _update_consecutive_loss_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    rule = _consecutive_loss_pause_rule(execution_setup)
    if rule is None:
        return route
    close_events = _instrument_close_events(route=route, snapshot=snapshot, fallback_time=now)
    if not close_events:
        return route
    latest_event = close_events[-1]
    state = _consecutive_loss_pause_state(route)
    last_processed = state.get("last_processed_close_event_id")
    if latest_event["event_id"] and latest_event["event_id"] == last_processed:
        return route

    consecutive_losses = int(state.get("consecutive_losses") or 0)
    event_ids = [event["event_id"] for event in close_events]
    if last_processed in event_ids:
        new_events = close_events[event_ids.index(last_processed) + 1 :]
    elif last_processed:
        new_events = [latest_event]
    else:
        new_events = close_events

    pause_until = state.get("pause_until")
    for event in new_events:
        net_pnl = float(event["net_pnl_usdt"])
        if net_pnl < 0:
            consecutive_losses += 1
        else:
            consecutive_losses = 0
            pause_until = None

    latest_net_pnl = float(latest_event["net_pnl_usdt"])
    if latest_net_pnl < 0 and consecutive_losses >= int(rule["consecutive_count"]):
        pause_until = _iso_timestamp(latest_event["closed_at"] + timedelta(hours=int(rule["cooldown_hours"])))

    next_state = {
        **state,
        "type": "consecutive_losses",
        "consecutive_losses": consecutive_losses,
        "last_processed_close_event_id": latest_event["event_id"],
        "last_close_at": _iso_timestamp(latest_event["closed_at"]),
        "last_close_net_pnl_usdt": latest_net_pnl,
        "pause_until": pause_until,
        "rule": rule,
    }
    return _persist_consecutive_loss_pause_state(repository=repository, route=route, state=next_state)


def _update_consecutive_win_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    owner_state: dict[str, Any] | None,
    now: datetime,
) -> dict[str, Any]:
    rule = _consecutive_win_pause_rule(execution_setup)
    if rule is None or owner_state is None:
        return route
    close_event = _latest_exchange_close_event(route=route, snapshot=snapshot, owner_state=owner_state, fallback_time=now)
    if close_event is None:
        return route

    state = _consecutive_win_pause_state(route)
    if close_event["event_id"] and close_event["event_id"] == state.get("last_processed_close_event_id"):
        return route

    consecutive_wins = int(state.get("consecutive_wins") or 0)
    net_pnl = float(close_event["net_pnl_usdt"])
    pause_until = state.get("pause_until")
    if net_pnl > 0:
        consecutive_wins += 1
    else:
        consecutive_wins = 0
        pause_until = None
    if net_pnl > 0 and consecutive_wins >= int(rule["consecutive_count"]):
        pause_until = _iso_timestamp(close_event["closed_at"] + timedelta(hours=int(rule["cooldown_hours"])))

    next_state = {
        **state,
        "type": "consecutive_wins",
        "consecutive_wins": consecutive_wins,
        "last_processed_close_event_id": close_event["event_id"],
        "last_close_at": _iso_timestamp(close_event["closed_at"]),
        "last_close_net_pnl_usdt": net_pnl,
        "pause_until": pause_until,
        "rule": rule,
    }
    return _persist_consecutive_win_pause_state(repository=repository, route=route, state=next_state)


def _update_profit_burst_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    rule = _profit_burst_pause_rule(execution_setup)
    if rule is None:
        return route
    close_events = _instrument_close_events(route=route, snapshot=snapshot, fallback_time=now)
    if not close_events:
        return route
    latest_event = max(close_events, key=lambda event: event["closed_at"])
    state = _profit_burst_pause_state(route)
    if latest_event["event_id"] and latest_event["event_id"] == state.get("last_trigger_close_event_id"):
        return route

    window_start = latest_event["closed_at"] - timedelta(hours=int(rule["lookback_hours"]))
    window_events = [event for event in close_events if event["closed_at"] >= window_start]
    window_net_pnl = sum(float(event["net_pnl_usdt"]) for event in window_events)
    route_capital = float(rule["route_capital_usdt"])
    if route_capital <= 0:
        return route
    profit_growth_pct = window_net_pnl / route_capital * 100.0
    pause_until = state.get("pause_until")
    last_trigger_close_event_id = state.get("last_trigger_close_event_id")
    if profit_growth_pct >= float(rule["profit_threshold_pct"]):
        pause_until = _iso_timestamp(latest_event["closed_at"] + timedelta(hours=int(rule["cooldown_hours"])))
        last_trigger_close_event_id = latest_event["event_id"]

    next_state = {
        **state,
        "type": "profit_burst",
        "latest_close_event_id": latest_event["event_id"],
        "latest_close_at": _iso_timestamp(latest_event["closed_at"]),
        "last_trigger_close_event_id": last_trigger_close_event_id,
        "lookback_start": _iso_timestamp(window_start),
        "lookback_hours": int(rule["lookback_hours"]),
        "window_close_event_count": len(window_events),
        "window_net_pnl_usdt": window_net_pnl,
        "route_capital_usdt": route_capital,
        "profit_growth_pct": profit_growth_pct,
        "pause_until": pause_until,
        "rule": rule,
    }
    return _persist_profit_burst_pause_state(repository=repository, route=route, state=next_state)


def _persist_consecutive_win_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    next_risk_limits = {
        **risk_limits,
        "pause_rule_state": {
            **pause_state,
            "consecutive_wins": state,
        },
    }
    updater = getattr(repository, "update_deployment_route_gate", None)
    if not callable(updater):
        return {**route, "risk_limits": next_risk_limits}
    updated = updater(route["route_id"], risk_limits=next_risk_limits)
    return updated or {**route, "risk_limits": next_risk_limits}


def _persist_consecutive_loss_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    next_risk_limits = {
        **risk_limits,
        "pause_rule_state": {
            **pause_state,
            "consecutive_losses": state,
        },
    }
    updater = getattr(repository, "update_deployment_route_gate", None)
    if not callable(updater):
        return {**route, "risk_limits": next_risk_limits}
    updated = updater(route["route_id"], risk_limits=next_risk_limits)
    return updated or {**route, "risk_limits": next_risk_limits}


def _persist_profit_burst_pause_state(
    *,
    repository: Any,
    route: dict[str, Any],
    state: dict[str, Any],
) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    next_risk_limits = {
        **risk_limits,
        "pause_rule_state": {
            **pause_state,
            "profit_burst": state,
        },
    }
    updater = getattr(repository, "update_deployment_route_gate", None)
    if not callable(updater):
        return {**route, "risk_limits": next_risk_limits}
    updated = updater(route["route_id"], risk_limits=next_risk_limits)
    return updated or {**route, "risk_limits": next_risk_limits}


def _consecutive_loss_pause_rule(execution_setup: dict[str, Any]) -> dict[str, Any] | None:
    for rule in _raw_pause_rules(execution_setup):
        if not isinstance(rule, dict) or rule.get("type") != "consecutive_losses":
            continue
        consecutive_count = _positive_int(rule.get("consecutive_count"))
        cooldown_hours = _positive_int(rule.get("cooldown_hours"))
        if consecutive_count is None or cooldown_hours is None:
            continue
        return {
            "type": "consecutive_losses",
            "consecutive_count": consecutive_count,
            "cooldown_hours": cooldown_hours,
        }
    return None


def _consecutive_win_pause_rule(execution_setup: dict[str, Any]) -> dict[str, Any] | None:
    rules: list[Any] = []
    if isinstance(execution_setup.get("pause_rules"), list):
        rules.extend(execution_setup["pause_rules"])
    if isinstance(execution_setup.get("pause_rule"), dict):
        rules.append(execution_setup["pause_rule"])
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else {}
    if isinstance(setup.get("pause_rules"), list):
        rules.extend(setup["pause_rules"])
    if isinstance(setup.get("pause_rule"), dict):
        rules.append(setup["pause_rule"])
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "consecutive_wins":
            continue
        consecutive_count = _positive_int(rule.get("consecutive_count"))
        cooldown_hours = _positive_int(rule.get("cooldown_hours"))
        if consecutive_count is None or cooldown_hours is None:
            continue
        return {
            "type": "consecutive_wins",
            "consecutive_count": consecutive_count,
            "cooldown_hours": cooldown_hours,
        }
    return None


def _profit_burst_pause_rule(execution_setup: dict[str, Any]) -> dict[str, Any] | None:
    rules = _raw_pause_rules(execution_setup)
    route_capital = _route_capital_usdt(execution_setup)
    for rule in rules:
        if not isinstance(rule, dict) or rule.get("type") != "profit_burst":
            continue
        profit_threshold_pct = _positive_float(rule.get("profit_threshold_pct"))
        lookback_hours = _positive_int(rule.get("lookback_hours"))
        cooldown_hours = _positive_int(rule.get("cooldown_hours"))
        rule_capital = _positive_float(rule.get("route_capital_usdt")) or route_capital
        if profit_threshold_pct is None or lookback_hours is None or cooldown_hours is None or rule_capital is None:
            continue
        return {
            "type": "profit_burst",
            "profit_threshold_pct": profit_threshold_pct,
            "lookback_hours": lookback_hours,
            "cooldown_hours": cooldown_hours,
            "route_capital_usdt": rule_capital,
        }
    return None


def _raw_pause_rules(execution_setup: dict[str, Any]) -> list[Any]:
    rules: list[Any] = []
    if isinstance(execution_setup.get("pause_rules"), list):
        rules.extend(execution_setup["pause_rules"])
    if isinstance(execution_setup.get("pause_rule"), dict):
        rules.append(execution_setup["pause_rule"])
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else {}
    if isinstance(setup.get("pause_rules"), list):
        rules.extend(setup["pause_rules"])
    if isinstance(setup.get("pause_rule"), dict):
        rules.append(setup["pause_rule"])
    return rules


def _route_capital_usdt(execution_setup: dict[str, Any]) -> float | None:
    direct = _positive_float(execution_setup.get("route_capital_usdt"))
    if direct is not None:
        return direct
    sizing = execution_setup.get("sizing") if isinstance(execution_setup.get("sizing"), dict) else {}
    return _positive_float(sizing.get("route_capital_usdt")) or _positive_float(sizing.get("initial_capital_usdt"))


def _consecutive_win_pause_state(route: dict[str, Any]) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    state = pause_state.get("consecutive_wins") if isinstance(pause_state.get("consecutive_wins"), dict) else {}
    return dict(state)


def _profit_burst_pause_state(route: dict[str, Any]) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    state = pause_state.get("profit_burst") if isinstance(pause_state.get("profit_burst"), dict) else {}
    return dict(state)


def _consecutive_loss_pause_state(route: dict[str, Any]) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = risk_limits.get("pause_rule_state") if isinstance(risk_limits.get("pause_rule_state"), dict) else {}
    state = pause_state.get("consecutive_losses") if isinstance(pause_state.get("consecutive_losses"), dict) else {}
    return dict(state)


def _latest_exchange_close_event(
    *,
    route: dict[str, Any],
    snapshot: dict[str, Any],
    owner_state: dict[str, Any],
    fallback_time: datetime,
) -> dict[str, Any] | None:
    direction = _owner_state_direction(owner_state)
    close_side = "sell" if direction == "LONG" else "buy" if direction == "SHORT" else None
    candidates: list[dict[str, Any]] = []
    for fill in snapshot.get("recent_fills") or []:
        if not isinstance(fill, dict):
            continue
        instrument = fill.get("instId") or fill.get("instrument") or fill.get("symbol")
        if instrument and str(instrument) != str(route.get("instrument")):
            continue
        fill_side = str(fill.get("side") or "").lower()
        if close_side and fill_side and fill_side != close_side:
            continue
        net_pnl = _fill_net_pnl_usdt(fill)
        if net_pnl is None:
            continue
        closed_at = _parse_exchange_timestamp(
            fill.get("fillTime")
            or fill.get("fill_time")
            or fill.get("ts")
            or fill.get("uTime")
            or fill.get("updated_at")
            or fill.get("created_at"),
            fallback=fallback_time,
        )
        candidates.append(
            {
                "event_id": _exchange_fill_event_id(fill),
                "closed_at": closed_at,
                "net_pnl_usdt": net_pnl,
            }
        )
    if not candidates:
        return None
    return max(candidates, key=lambda candidate: candidate["closed_at"])


def _instrument_close_events(
    *,
    route: dict[str, Any],
    snapshot: dict[str, Any],
    fallback_time: datetime,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for fill in snapshot.get("recent_fills") or []:
        if not isinstance(fill, dict):
            continue
        instrument = fill.get("instId") or fill.get("instrument") or fill.get("symbol")
        if instrument and str(instrument) != str(route.get("instrument")):
            continue
        net_pnl = _fill_net_pnl_usdt(fill)
        if net_pnl is None:
            continue
        closed_at = _parse_exchange_timestamp(
            fill.get("fillTime")
            or fill.get("fill_time")
            or fill.get("ts")
            or fill.get("uTime")
            or fill.get("updated_at")
            or fill.get("created_at"),
            fallback=fallback_time,
        )
        event_id = _exchange_fill_event_id(fill)
        if event_id in seen:
            continue
        seen.add(event_id)
        events.append(
            {
                "event_id": event_id,
                "closed_at": closed_at,
                "net_pnl_usdt": net_pnl,
            }
        )
    return sorted(events, key=lambda event: event["closed_at"])


def _owner_state_direction(owner_state: dict[str, Any]) -> str | None:
    position_state = owner_state.get("position_state") if isinstance(owner_state.get("position_state"), dict) else {}
    direction = (
        position_state.get("direction")
        or owner_state.get("direction")
        or position_state.get("side")
        or owner_state.get("side")
    )
    if direction is None:
        return None
    direction_text = str(direction).upper()
    if direction_text in {"LONG", "BUY"}:
        return "LONG"
    if direction_text in {"SHORT", "SELL"}:
        return "SHORT"
    return None


def _fill_net_pnl_usdt(fill: dict[str, Any]) -> float | None:
    net_value = _first_numeric(fill, ("netPnl", "net_pnl", "netPnlUsd", "net_pnl_usd", "net_pnl_usdt"))
    if net_value is not None:
        return net_value
    pnl_value = _first_numeric(fill, ("realizedPnl", "realized_pnl", "pnl", "fillPnl", "fill_pnl"))
    if pnl_value is None:
        return None
    fee_value = _optional_numeric(fill.get("fee"))
    if fee_value is not None:
        return pnl_value + fee_value
    commission_value = _optional_numeric(fill.get("commission"))
    if commission_value is not None:
        return pnl_value - abs(commission_value)
    return pnl_value


def _exchange_fill_event_id(fill: dict[str, Any]) -> str:
    for key in ("billId", "bill_id", "tradeId", "trade_id", "ordId", "order_id", "clOrdId", "client_order_id"):
        value = fill.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    timestamp = fill.get("fillTime") or fill.get("fill_time") or fill.get("ts") or fill.get("uTime") or ""
    return f"fill:{timestamp}:{fill.get('side', '')}:{fill.get('pnl', fill.get('realizedPnl', ''))}"


def _record_wake(repository: Any, wake: dict[str, Any]) -> dict[str, Any]:
    return repository.record_wake_run(wake)


def _record_live_signal_observation(
    *,
    repository: Any,
    route: dict[str, Any],
    bundle: dict[str, Any],
    signal: dict[str, Any],
    decision: dict[str, Any],
    signal_scan_result: dict[str, Any],
) -> None:
    recorder = getattr(repository, "record_live_signal_observation", None)
    if not callable(recorder):
        return
    recorder(
        {
            "signal_engine_id": signal.get("signal_engine_id") or route.get("signal_engine_id"),
            "signal_engine_version": signal.get("signal_engine_version") or route.get("signal_engine_version") or "unknown",
            "asset": signal.get("asset") or route.get("asset"),
            "instrument": signal.get("instrument") or route.get("instrument"),
            "signal_id": signal["signal_id"],
            "signal_timestamp": signal.get("timestamp"),
            "route_id": route.get("route_id"),
            "bundle_id": bundle.get("bundle_id"),
            "payload_schema": signal.get("payload_schema", "signal_packet.v2"),
            "payload": signal.get("payload", {}),
            "decision": decision,
            "scan_metadata": signal_scan_result,
            "observed_at": datetime.now(UTC),
        }
    )


def _error_wake(
    *,
    wake_id: str,
    route: dict[str, Any],
    message: str,
    bundle_id: str | None = None,
) -> dict[str, Any]:
    return {
        "wake_id": wake_id,
        "route_id": route["route_id"],
        "bundle_id": bundle_id or route.get("active_bundle_id"),
        "status": "error",
        "branch": "error",
        "blockers": [],
        "exchange_snapshot": {},
        "signal_scan_result": {},
        "strategy_decision": {},
        "order_intents": [],
        "adapter_results": [],
        "error": {"message": message},
        "completed_at": datetime.now(UTC),
    }


def _active_positions(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    positions = snapshot.get("positions") or []
    return [position for position in positions if _numeric(position.get("pos") or position.get("size") or position.get("sz")) != 0]


def _working_entry_orders(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    orders = snapshot.get("open_orders") or []
    return [order for order in orders if not _truthy(order.get("reduceOnly") or order.get("reduce_only"))]


def _order_age_minutes(order: dict[str, Any]) -> float:
    if order.get("age_minutes") is not None:
        return float(order["age_minutes"])
    created = order.get("created_at") or order.get("cTime")
    if created is None:
        return 0.0
    try:
        if isinstance(created, (int, float)) or str(created).isdigit():
            created_at = datetime.fromtimestamp(float(created) / 1000, tz=UTC)
        else:
            created_at = datetime.fromisoformat(str(created).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return 0.0
    return max(0.0, (datetime.now(UTC) - created_at).total_seconds() / 60)


def _fresh_signal_scan_result(signal: dict[str, Any]) -> dict[str, Any]:
    return {
        "status": "fresh_signal",
        "signal_id": signal["signal_id"],
        "signal_timestamp": _iso_timestamp(signal.get("timestamp")),
        "signal_engine_id": signal.get("signal_engine_id"),
        "asset": signal.get("asset"),
        "source": "live_parquet_snapshot",
    }


def _iso_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        timestamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
        return timestamp.isoformat().replace("+00:00", "Z")
    return str(value)


def _has_live_entry_for_signal(*, repository: Any, route_id: str, signal_id: str) -> bool:
    if hasattr(repository, "has_live_entry_for_signal"):
        return bool(repository.has_live_entry_for_signal(route_id=route_id, signal_id=signal_id))
    wakes = repository.list_wake_runs(route_id, limit=25) if hasattr(repository, "list_wake_runs") else []
    for wake in wakes:
        scan = wake.get("signal_scan_result") if isinstance(wake.get("signal_scan_result"), dict) else {}
        if scan.get("signal_id") != signal_id:
            continue
        for intent in wake.get("order_intents") or []:
            if _canonical_action(intent.get("action")) in {"ENTER", "ENTER_LONG", "ENTER_SHORT"}:
                return True
    return False


def _processed_entry_signal_timestamp(*, repository: Any, route_id: str) -> datetime:
    latest = datetime.min.replace(tzinfo=UTC)
    wakes = repository.list_wake_runs(route_id, limit=100) if hasattr(repository, "list_wake_runs") else []
    for wake in wakes:
        if not any(_canonical_action(intent.get("action")) in {"ENTER", "ENTER_LONG", "ENTER_SHORT"} for intent in wake.get("order_intents") or []):
            continue
        scan = wake.get("signal_scan_result") if isinstance(wake.get("signal_scan_result"), dict) else {}
        timestamp = scan.get("signal_timestamp")
        if timestamp is None:
            continue
        latest = max(latest, _parse_timestamp(timestamp))
    return latest


def _signal_timestamp(signal: dict[str, Any]) -> datetime:
    return _parse_timestamp(signal.get("timestamp"))


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _parse_optional_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _parse_exchange_timestamp(value, fallback=datetime.min.replace(tzinfo=UTC))
    except (TypeError, ValueError):
        return None


def _parse_exchange_timestamp(value: Any, *, fallback: datetime) -> datetime:
    if value in (None, ""):
        return fallback
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    text = str(value)
    try:
        numeric = float(text)
    except ValueError:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    scale = 1000 if abs(numeric) >= 10_000_000_000 else 1
    return datetime.fromtimestamp(numeric / scale, tz=UTC)


def _run_entry_decision(
    *,
    runtime: dict[str, Any],
    route: dict[str, Any],
    signal: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    module = runtime["strategy_module"]
    if not hasattr(module, "decide"):
        return {
            "action": "SKIP",
            "trade_action": "SKIP",
            "direction": "FLAT",
            "reason_code": "strategy_missing_decide",
            "signal_id": signal["signal_id"],
            "order_intents": [],
        }
    raw = module.decide(
        {
            "signal": signal,
            "runtime_mode": route.get("account_mode", "live"),
            "execution_setup": runtime["execution_setup"],
            "exchange_snapshot": snapshot,
            "portfolio_state": snapshot,
        }
    )
    action = _canonical_action(raw.get("action") or raw.get("trade_action") or "SKIP")
    direction = raw.get("direction") or _direction_from_action(action) or "FLAT"
    return {
        **raw,
        "action": action,
        "trade_action": raw.get("trade_action", action),
        "direction": str(direction).upper(),
        "signal_id": raw.get("signal_id", signal["signal_id"]),
        "decision_id": raw.get("decision_id") or f"{route['route_id']}:{signal['signal_id']}",
    }


def _run_position_management(
    *,
    runtime: dict[str, Any],
    route: dict[str, Any],
    snapshot: dict[str, Any],
    owner_state: dict[str, Any] | None,
    position: dict[str, Any],
    working_entry_orders: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    module = runtime["strategy_module"]
    position_context = _position_context(
        position=position,
        execution_setup=runtime["execution_setup"],
        now=now,
        owner_state=owner_state,
        route=route,
        snapshot=snapshot,
        working_entry_orders=working_entry_orders,
    )
    hard_exit = _hard_time_gate_decision(route=route, position_context=position_context)
    if hard_exit is not None:
        return hard_exit
    if not hasattr(module, "manage_position"):
        decision = _default_protection_decision(
            route=route,
            position_context=position_context,
            snapshot=snapshot,
            execution_setup=runtime["execution_setup"],
        ) or {
            "decision_id": f"{route['route_id']}:position-management",
            "action": "HOLD",
            "reason_code": "strategy_missing_manage_position",
            "order_intents": [],
            "diagnostics": {"position_context": position_context},
        }
        return _apply_pyramid_management(
            route=route,
            decision=decision,
            position_context=position_context,
            owner_state=owner_state,
            execution_setup=runtime["execution_setup"],
            working_entry_orders=working_entry_orders,
        )
    raw = module.manage_position(
        {
            "runtime_mode": route.get("account_mode", "live"),
            "execution_setup": runtime["execution_setup"],
            "exchange_snapshot": snapshot,
            "owner_state": owner_state or {},
            "position_context": position_context,
            "portfolio_state": snapshot,
        }
    )
    action = _canonical_action(raw.get("action", "HOLD"))
    decision = {
        "decision_id": raw.get("decision_id") or f"{route['route_id']}:position-management",
        "action": action,
        "reason_code": raw.get("reason_code", "position_management"),
        "order_intents": raw.get("order_intents", []),
        "quantity": raw.get("quantity"),
        "notional_usd": raw.get("notional_usd"),
        "side": raw.get("side"),
        "direction": raw.get("direction"),
        "order_type": raw.get("order_type"),
        "price": raw.get("price"),
        "tp": raw.get("tp"),
        "sl": raw.get("sl"),
        "tp_pct": raw.get("tp_pct"),
        "sl_pct": raw.get("sl_pct"),
        "reduce_only": raw.get("reduce_only"),
        "diagnostics": raw.get("diagnostics", {}),
    }
    if action in {"EXIT", "REDUCE"}:
        return decision
    decision = _default_protection_decision(
        route=route,
        position_context=position_context,
        snapshot=snapshot,
        execution_setup=runtime["execution_setup"],
    ) or {**decision, "action": "HOLD"}
    return _apply_pyramid_management(
        route=route,
        decision=decision,
        position_context=position_context,
        owner_state=owner_state,
        execution_setup=runtime["execution_setup"],
        working_entry_orders=working_entry_orders,
    )


def _default_protection_decision(
    *,
    route: dict[str, Any],
    position_context: dict[str, Any],
    snapshot: dict[str, Any],
    execution_setup: dict[str, Any],
) -> dict[str, Any] | None:
    resolved = _resolve_bundle_protection(
        route=route,
        position_context=position_context,
        snapshot=snapshot,
        execution_setup=execution_setup,
    )
    if resolved is None:
        return None
    diagnostics = {"protection": resolved["diagnostics"]}
    if resolved["synced"]:
        return {
            "decision_id": f"{route['route_id']}:bundle-protection",
            "action": "HOLD",
            "reason_code": "bundle_protection_synced",
            "order_intents": [],
            "diagnostics": diagnostics,
        }
    return {
        "decision_id": f"{route['route_id']}:bundle-protection",
        "action": "UPDATE_PROTECTION",
        "reason_code": "bundle_protection_refresh",
        "order_intents": [],
        "quantity": resolved["quantity"],
        "notional_usd": None,
        "side": resolved["side"],
        "direction": resolved["direction"],
        "order_type": "market",
        "price": None,
        "tp": resolved["tp"],
        "sl": resolved["sl"],
        "tp_pct": resolved["tp_pct"],
        "sl_pct": resolved["sl_pct"],
        "reduce_only": True,
        "diagnostics": diagnostics,
    }


def _execution_setup_policy(
    *,
    execution_setup: dict[str, Any],
    direction: str,
) -> dict[str, Any]:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    direction = str(direction or "LONG").upper()
    policy_mode = setup.get("policy_mode") or execution_setup.get("policy_mode") or "shared"
    selected = setup
    if policy_mode == "side_specific":
        side_policies = setup.get("side_policies")
        if not isinstance(side_policies, dict):
            return {"blocker": "side_specific_execution_setup_missing_side_policies", "policy_mode": policy_mode, "selected_side": direction}
        side_policy = side_policies.get(direction)
        if not isinstance(side_policy, dict):
            return {"blocker": f"side_specific_execution_setup_missing_{direction.lower()}_policy", "policy_mode": policy_mode, "selected_side": direction}
        selected = side_policy

    final_tp_pct = _numeric(_first_present(selected, "final_tp_pct", "tp_pct", "lock_profit_pct"))
    initial_sl_pct = _numeric(_first_present(selected, "initial_sl_pct", "sl_pct"))
    protection_enabled_source = selected if "protection_enabled" in selected else setup
    protection_enabled = _truthy(protection_enabled_source.get("protection_enabled"))
    protect_trigger_pct = _numeric(selected.get("protect_trigger_pct"))
    trail_sl_pct = _numeric(selected.get("trail_sl_pct"))
    if final_tp_pct <= 0:
        return {"blocker": "execution_setup_missing_final_tp_pct", "policy_mode": policy_mode, "selected_side": direction}
    if initial_sl_pct <= 0:
        return {"blocker": "execution_setup_missing_initial_sl_pct", "policy_mode": policy_mode, "selected_side": direction}
    if protection_enabled and (protect_trigger_pct <= 0 or trail_sl_pct <= 0):
        return {"blocker": "protected_execution_setup_missing_protection_values", "policy_mode": policy_mode, "selected_side": direction}
    return {
        "policy_mode": policy_mode,
        "selected_side": direction if policy_mode == "side_specific" else "shared",
        "protection_enabled": protection_enabled,
        "final_tp_pct": final_tp_pct,
        "initial_sl_pct": initial_sl_pct,
        "protect_trigger_pct": protect_trigger_pct,
        "trail_sl_pct": trail_sl_pct,
        "max_hold_hours": _numeric(_first_present(selected, "max_hold_hours", "hard_exit_hours")),
    }


def _resolve_bundle_protection(
    *,
    route: dict[str, Any],
    position_context: dict[str, Any],
    snapshot: dict[str, Any],
    execution_setup: dict[str, Any],
) -> dict[str, Any] | None:
    direction = str(position_context.get("direction") or "LONG").upper()
    policy = _execution_setup_policy(execution_setup=execution_setup, direction=direction)
    if policy.get("blocker"):
        return None
    tp_pct = float(policy["final_tp_pct"])
    initial_sl_pct = float(policy["initial_sl_pct"])
    entry_price = _numeric(position_context.get("entry_price"))
    size = _numeric(position_context.get("size"))
    if tp_pct <= 0 or initial_sl_pct <= 0 or entry_price <= 0 or size <= 0:
        return None

    mark_price = _numeric(position_context.get("mark_price")) or _numeric(position_context.get("last_price"))
    protection_state = _protection_state(snapshot=snapshot, instrument=route["instrument"])
    expected_side = "sell" if direction == "LONG" else "buy"
    live_order = protection_state["orders"][0] if protection_state["has_single_live"] else None
    live_order_is_stale = False
    if live_order is not None:
        live_order_is_stale = (
            str(live_order.get("side") or "").lower() != expected_side
            or _protection_order_predates_position(live_order, position_context=position_context)
        )
        if live_order_is_stale:
            live_order = None
    live_sl = _numeric(_first_present(live_order or {}, "slTriggerPx", "sl", "sl_trigger_price"))
    live_tp = _numeric(_first_present(live_order or {}, "tpTriggerPx", "tp", "tp_trigger_price"))
    protection_enabled = bool(policy["protection_enabled"])
    protect_trigger_pct = float(policy["protect_trigger_pct"])
    trail_sl_pct = float(policy["trail_sl_pct"])
    favorable_move_pct = _favorable_move_pct(entry_price=entry_price, mark_price=mark_price, direction=direction)
    phase = "initial"
    if protection_enabled and protect_trigger_pct > 0 and trail_sl_pct > 0:
        if _live_sl_is_protected(entry_price=entry_price, live_sl=live_sl, direction=direction):
            phase = "protected"
        elif favorable_move_pct is not None and favorable_move_pct >= protect_trigger_pct:
            phase = "protected"
    selected_sl_pct = trail_sl_pct if phase == "protected" else initial_sl_pct
    tp = _take_profit_price(entry_price=entry_price, direction=direction, tp_pct=tp_pct)
    sl = (
        _protected_stop_price(entry_price=entry_price, direction=direction, trail_sl_pct=selected_sl_pct)
        if phase == "protected"
        else _initial_stop_price(entry_price=entry_price, direction=direction, sl_pct=selected_sl_pct)
    )
    synced = protection_state["has_single_live"] and not live_order_is_stale and _protection_matches(
        protection_state["orders"][0],
        side=expected_side,
        size=_format_decimal(size),
        tp=tp,
        sl=sl,
    )
    sync_reason = "protection_already_synced"
    if not synced:
        if protection_state["live_count"] == 0:
            sync_reason = "missing_live_protection"
        elif protection_state["live_count"] != 1:
            sync_reason = "live_protection_count_mismatch"
        elif str(protection_state["orders"][0].get("side") or "").lower() != expected_side:
            sync_reason = "live_protection_side_mismatch"
        elif _protection_order_predates_position(protection_state["orders"][0], position_context=position_context):
            sync_reason = "live_protection_from_previous_position"
        else:
            sync_reason = "live_protection_mismatch"

    return {
        "quantity": _format_decimal(size),
        "side": expected_side,
        "direction": direction,
        "tp": tp,
        "sl": sl,
        "tp_pct": tp_pct,
        "sl_pct": selected_sl_pct,
        "synced": synced,
        "diagnostics": {
            "entry_price": _format_decimal(entry_price),
            "mark_price": _format_decimal(mark_price) if mark_price > 0 else None,
            "policy_mode": policy["policy_mode"],
            "selected_side": policy["selected_side"],
            "phase": phase,
            "protection_enabled": protection_enabled,
            "favorable_move_pct": _rounded_number(favorable_move_pct) if favorable_move_pct is not None else None,
            "protect_trigger_pct": _rounded_number(protect_trigger_pct) if protect_trigger_pct > 0 else None,
            "trail_sl_pct": _rounded_number(trail_sl_pct) if trail_sl_pct > 0 else None,
            "initial_sl_pct": _rounded_number(initial_sl_pct),
            "final_tp_pct": _rounded_number(tp_pct),
            "derived_tp": tp,
            "derived_sl": sl,
            "live_tp": _format_decimal(live_tp) if live_tp > 0 else None,
            "live_sl": _format_decimal(live_sl) if live_sl > 0 else None,
            "live_protection_count": protection_state["live_count"],
            "sync_reason": sync_reason,
        },
    }


def _hard_time_gate_decision(
    *,
    route: dict[str, Any],
    position_context: dict[str, Any],
) -> dict[str, Any] | None:
    hard_exit_after_hours = _numeric(position_context.get("hard_exit_after_hours"))
    age_hours = _numeric(position_context.get("age_hours"))
    if hard_exit_after_hours <= 0 or age_hours < hard_exit_after_hours:
        return None
    direction = str(position_context.get("direction") or "LONG").upper()
    return {
        "decision_id": f"{route['route_id']}:position-management",
        "action": "EXIT",
        "reason_code": "hard_time_gate_expired",
        "order_intents": [],
        "quantity": position_context["size"],
        "notional_usd": None,
        "side": "sell" if direction == "LONG" else "buy",
        "direction": direction,
        "order_type": "market",
        "price": None,
        "reduce_only": True,
        "diagnostics": {
            "position_age_hours": age_hours,
            "hard_exit_after_hours": hard_exit_after_hours,
        },
    }


def _position_context(
    *,
    position: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
    owner_state: dict[str, Any] | None,
    route: dict[str, Any],
    snapshot: dict[str, Any],
    working_entry_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
    direction = _position_direction(position, raw_size=raw_size)
    opened_at = _position_opened_at(position, owner_state=owner_state)
    age_hours = None
    if opened_at is not None:
        age_hours = max(0.0, (now - opened_at).total_seconds() / 3600)
    mark_price = _numeric(_first_present(position, "markPx", "mark_price"))
    if mark_price <= 0:
        mark_price = _numeric(_first_present(position, "last", "lastPx", "last_price"))
    position_notional = _position_notional_usd(position=position, mark_price=mark_price)
    account_equity = _account_equity_usd(snapshot)
    return {
        "instrument": position.get("instId") or position.get("instrument"),
        "direction": direction,
        "side": "long" if direction == "LONG" else "short",
        "size": _format_decimal(abs(raw_size)),
        "raw_size": _format_decimal(raw_size),
        "entry_price": _first_present(position, "avgPx", "avg_price", "entry_price", "openAvgPx"),
        "mark_price": _first_present(position, "markPx", "mark_price"),
        "last_price": _first_present(position, "last", "lastPx", "last_price"),
        "position_notional_usd": _rounded_number(position_notional) if position_notional > 0 else None,
        "account_equity_usd": _rounded_number(account_equity) if account_equity > 0 else None,
        "position_instance_id": owner_state.get("position_instance_id") if owner_state else None,
        "opened_at": opened_at.isoformat().replace("+00:00", "Z") if opened_at else None,
        "age_hours": age_hours,
        "hard_exit_after_hours": _hard_exit_after_hours(execution_setup),
        "pyramid": _pyramid_context(
            execution_setup=execution_setup,
            owner_state=owner_state,
            position=position,
            snapshot=snapshot,
            position_context_base={
                "direction": direction,
                "entry_price": _first_present(position, "avgPx", "avg_price", "entry_price", "openAvgPx"),
                "mark_price": _first_present(position, "markPx", "mark_price"),
                "last_price": _first_present(position, "last", "lastPx", "last_price"),
            },
            route=route,
            working_entry_orders=working_entry_orders,
        ),
    }


def _reconcile_owner_state(
    *,
    repository: Any,
    owner_state: dict[str, Any] | None,
    position: dict[str, Any],
    snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    if owner_state is None:
        return None
    position_state = dict(owner_state.get("position_state") or {})
    legs = [dict(leg) for leg in position_state.get("legs") or []]
    if not legs:
        return owner_state
    open_orders = snapshot.get("open_orders") or []
    recent_fills = snapshot.get("recent_fills") or []
    changed = False
    for index, leg in enumerate(legs):
        status = str(leg.get("status") or "submitted").lower()
        if status in {"filled", "cancelled", "canceled", "failed", "rejected"}:
            continue
        fill = _matching_exchange_row(leg, recent_fills)
        if fill is not None:
            changed = _apply_fill_or_terminal_row(leg, fill) or changed
            continue
        open_order = _matching_exchange_row(leg, open_orders)
        if open_order is not None:
            before = dict(leg)
            if status != "working":
                leg["status"] = "working"
            _copy_exchange_ids(leg, open_order)
            changed = (leg != before) or changed
            continue
        if index == 0:
            entry_price = _first_present(position, "avgPx", "avg_price", "entry_price", "openAvgPx")
            if entry_price not in (None, ""):
                leg["status"] = "filled"
                leg["entry_price"] = str(entry_price)
                leg["fill_source"] = "live_position"
                changed = True
    if not changed:
        return owner_state
    position_state["legs"] = legs
    if hasattr(repository, "update_owner_state"):
        return repository.update_owner_state(owner_state["owner_state_id"], position_state=position_state)
    return {**owner_state, "position_state": position_state}


def _adopt_exchange_position(
    *,
    repository: Any,
    route: dict[str, Any],
    bundle: dict[str, Any],
    wake_id: str,
    position: dict[str, Any],
    snapshot: dict[str, Any],
    execution_setup: dict[str, Any],
    now: datetime,
) -> dict[str, Any] | None:
    creator = getattr(repository, "create_owner_state", None)
    if not callable(creator):
        return None

    raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
    size = abs(raw_size)
    entry_price = _numeric(_first_present(position, "avgPx", "avg_price", "entry_price", "openAvgPx"))
    direction = _position_direction(position, raw_size=raw_size)
    mark_price = _numeric(_first_present(position, "markPx", "mark_price", "last", "lastPx", "last_price"))
    position_notional = _position_notional_usd(position=position, mark_price=mark_price)
    sizing_policy = _route_sizing_policy(route=route, execution_setup=execution_setup)
    leverage = sizing_policy["leverage"]
    max_legs = _pyramid_max_legs(execution_setup)
    account_equity = _account_equity_usd(snapshot)
    margin_allocation_pct = sizing_policy["margin_allocation_pct"]
    per_leg_margin = (
        account_equity * margin_allocation_pct / 100 / max_legs
        if account_equity > 0 and margin_allocation_pct > 0
        else 0
    )
    current_margin = position_notional / leverage if position_notional > 0 and leverage > 0 else 0
    inferred_legs = (
        _infer_pyramid_legs(raw_legs=current_margin / per_leg_margin, max_legs=max_legs)
        if per_leg_margin > 0
        else None
    )
    recorded_leg_count = inferred_legs or 1
    opened_at = _position_opened_at(position, owner_state=None)
    exchange_position_id = str(_first_present(position, "posId", "position_id", "positionId") or wake_id)
    opened_at_key = str(int(opened_at.timestamp() * 1000)) if opened_at else "unknown-open-time"
    position_instance_id = f"exchange-{route['route_id']}-{exchange_position_id}-{opened_at_key}"
    observed_at = now.isoformat().replace("+00:00", "Z")
    legs = [
        {
            "leg": leg,
            "action": "ADOPT",
            "status": "filled",
            "side": "buy" if direction == "LONG" else "sell",
            "direction": direction,
            "quantity": _format_decimal(size / recorded_leg_count) if size > 0 else None,
            "notional_usd": _rounded_number(position_notional / recorded_leg_count) if position_notional > 0 else None,
            "margin_usd": _rounded_number(current_margin / recorded_leg_count) if current_margin > 0 else None,
            "leverage": _rounded_number(leverage) if leverage > 0 else None,
            "entry_price": _format_decimal(entry_price) if entry_price > 0 else None,
            "filled_at": opened_at.isoformat().replace("+00:00", "Z") if opened_at else observed_at,
            "fill_source": "manual_exchange_adoption",
            "exchange_position_id": exchange_position_id,
        }
        for leg in range(1, recorded_leg_count + 1)
    ]
    owner_state = {
        "owner_state_id": f"owner-{position_instance_id}",
        "route_id": route["route_id"],
        "bundle_id": bundle["bundle_id"],
        "position_instance_id": position_instance_id,
        "asset": route["asset"],
        "instrument": route["instrument"],
        "account_mode": route["account_mode"],
        "owner_strategy_id": route["strategy_id"],
        "owner_strategy_version": route["strategy_version"],
        "opened_from_signal_id": None,
        "status": "open",
        "position_state": {
            "schema_version": "position_episode.v1",
            "position_instance_id": position_instance_id,
            "direction": direction,
            "opened_wake_id": wake_id,
            "opened_from_signal_id": None,
            "opened_bundle_id": bundle["bundle_id"],
            "adoption_source": "manual_exchange_position",
            "adopted_at": observed_at,
            "exchange_position_id": exchange_position_id,
            "inferred_pyramid_legs": inferred_legs,
            "pyramid_exposure_ambiguous": inferred_legs is None,
            "pyramid_setup": {
                "max_legs": max_legs,
                "margin_allocation_pct": _rounded_number(margin_allocation_pct),
                "leverage": _rounded_number(leverage),
            },
            "legs": legs,
            "protection_refresh_required": True,
        },
    }
    return creator(owner_state)


def _matching_exchange_row(leg: dict[str, Any], rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    leg_ids = _exchange_identifiers(leg)
    if not leg_ids:
        return None
    for row in rows:
        if leg_ids.intersection(_exchange_identifiers(row)):
            return row
    return None


def _exchange_identifiers(row: dict[str, Any]) -> set[str]:
    identifiers = set()
    for key in (
        "client_order_id",
        "exchange_client_order_id",
        "clOrdId",
        "cl_order_id",
        "ordId",
        "order_id",
        "orderId",
        "exchange_order_id",
    ):
        value = row.get(key)
        if value not in (None, ""):
            identifiers.add(str(value))
    return identifiers


def _apply_fill_or_terminal_row(leg: dict[str, Any], row: dict[str, Any]) -> bool:
    status = _exchange_row_status(row)
    before = dict(leg)
    _copy_exchange_ids(leg, row)
    if status in {"cancelled", "canceled"}:
        leg["status"] = "cancelled"
        leg["cancelled_at"] = _exchange_time(row)
        return leg != before
    if status in {"failed", "rejected"}:
        leg["status"] = "failed"
        leg["failed_at"] = _exchange_time(row)
        return leg != before
    fill_price = _first_present(row, "fillPx", "fill_px", "avgPx", "avg_price", "px", "price")
    if fill_price not in (None, "") or status in {"filled", "partially_filled", "partially-filled"}:
        leg["status"] = "filled"
        if fill_price not in (None, ""):
            leg["entry_price"] = str(fill_price)
        fill_size = _first_present(row, "fillSz", "fill_size", "accFillSz", "acc_fill_sz", "sz", "size")
        if fill_size not in (None, ""):
            leg["filled_size"] = str(fill_size)
        fill_time = _exchange_time(row)
        if fill_time is not None:
            leg["filled_at"] = fill_time
        leg["fill_source"] = "exchange_fill"
    return leg != before


def _copy_exchange_ids(leg: dict[str, Any], row: dict[str, Any]) -> None:
    order_id = _first_present(row, "ordId", "order_id", "orderId", "exchange_order_id")
    client_order_id = _first_present(row, "clOrdId", "exchange_client_order_id", "client_order_id", "cl_order_id")
    if order_id not in (None, ""):
        leg["exchange_order_id"] = str(order_id)
    if client_order_id not in (None, ""):
        leg["exchange_client_order_id"] = str(client_order_id)


def _exchange_row_status(row: dict[str, Any]) -> str:
    return str(_first_present(row, "state", "status", "order_state") or "").lower()


def _exchange_time(row: dict[str, Any]) -> str | None:
    value = _first_present(row, "fillTime", "fill_time", "uTime", "updated_at", "cTime", "created_at")
    parsed = _parse_datetime(value)
    return parsed.isoformat().replace("+00:00", "Z") if parsed else str(value) if value not in (None, "") else None


def _apply_pyramid_management(
    *,
    route: dict[str, Any],
    decision: dict[str, Any],
    position_context: dict[str, Any],
    owner_state: dict[str, Any] | None,
    execution_setup: dict[str, Any],
    working_entry_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    pyramid_context = _pyramid_context(
        execution_setup=execution_setup,
        owner_state=owner_state,
        position={},
        snapshot={},
        position_context_base=position_context,
        route=route,
        working_entry_orders=working_entry_orders,
    )
    diagnostics = dict(decision.get("diagnostics") or {})
    diagnostics["pyramid"] = pyramid_context
    action = _canonical_action(decision.get("action"))
    if action in {"EXIT", "REDUCE", "PYRAMID", "COMPLETE_POSITION", "UPDATE_PROTECTION", "BLOCKED"}:
        return {**decision, "diagnostics": diagnostics}
    reconciliation = _position_size_reconciliation(
        execution_setup=execution_setup,
        position_context=position_context,
        route=route,
        working_entry_orders=working_entry_orders,
    )
    diagnostics["position_size_reconciliation"] = reconciliation
    if action == "HOLD" and reconciliation["applicable"]:
        direction = str(position_context.get("direction") or "LONG").upper()
        return {
            **decision,
            "action": "COMPLETE_POSITION",
            "reason_code": "position_size_incomplete",
            "order_intents": [],
            "quantity": None,
            "notional_usd": None,
            "side": "buy" if direction == "LONG" else "sell",
            "direction": direction,
            "order_type": "market",
            "price": None,
            "reduce_only": False,
            "position_instance_id": position_context.get("position_instance_id"),
            "completion_margin_usd": reconciliation["missing_margin_usd"],
            "completion_notional_usd": reconciliation["missing_notional_usd"],
            "completion_target_margin_usd": reconciliation["target_margin_usd"],
            "diagnostics": diagnostics,
        }
    if not pyramid_context["eligible"] or not pyramid_context["trigger_reached"]:
        return {**decision, "diagnostics": diagnostics}
    direction = str(position_context.get("direction") or pyramid_context.get("direction") or "LONG").upper()
    return {
        **decision,
        "action": "PYRAMID",
        "reason_code": "pyramid_trigger_reached",
        "order_intents": [],
        "quantity": None,
        "notional_usd": None,
        "side": "buy" if direction == "LONG" else "sell",
        "direction": direction,
        "order_type": "market",
        "price": None,
        "reduce_only": False,
        "position_instance_id": pyramid_context["position_instance_id"],
        "pyramid_leg": pyramid_context["next_leg"],
        "trigger_price": pyramid_context["trigger_price"],
        "last_leg_entry": pyramid_context["last_leg_entry"],
        "diagnostics": diagnostics,
    }


def _position_size_reconciliation(
    *,
    execution_setup: dict[str, Any],
    position_context: dict[str, Any],
    route: dict[str, Any] | None,
    working_entry_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    pyramid_setup = setup.get("pyramid") if isinstance(setup.get("pyramid"), dict) else {}
    step_pct = _numeric(pyramid_setup.get("step_pct") or setup.get("pyramid_step_pct"))
    max_legs = _pyramid_max_legs(execution_setup)
    sizing_policy = _route_sizing_policy(route=route, execution_setup=execution_setup)
    allocation_pct = sizing_policy["margin_allocation_pct"]
    leverage = sizing_policy["leverage"]
    account_equity = _numeric(position_context.get("account_equity_usd"))
    position_notional = _numeric(position_context.get("position_notional_usd"))
    current_margin = position_notional / leverage if position_notional > 0 and leverage > 0 else 0.0
    target_margin = account_equity * allocation_pct / 100 if account_equity > 0 and allocation_pct > 0 else 0.0
    missing_margin = max(0.0, target_margin - current_margin)
    missing_notional = missing_margin * leverage if leverage > 0 else 0.0
    tolerance = max(0.01, target_margin * POSITION_SIZE_RECONCILIATION_TOLERANCE_PCT / 100)
    pyramid_configured = max_legs > 1 and step_pct > 0
    blockers: list[str] = []
    if pyramid_configured:
        blockers.append("pyramiding_configured")
    if leverage <= 0:
        blockers.append("missing_route_leverage")
    if allocation_pct <= 0:
        blockers.append("missing_margin_allocation_pct")
    if account_equity <= 0:
        blockers.append("missing_account_equity")
    if position_notional <= 0:
        blockers.append("missing_position_notional")
    if working_entry_orders:
        blockers.append("working_add_order_exists")
    if missing_margin <= tolerance:
        blockers.append("position_size_already_complete")
    return {
        "applicable": not blockers,
        "pyramiding_configured": pyramid_configured,
        "max_legs": max_legs,
        "step_pct": _rounded_number(step_pct) if step_pct > 0 else None,
        "account_equity_usd": _rounded_number(account_equity) if account_equity > 0 else None,
        "allocation_pct": _rounded_number(allocation_pct) if allocation_pct > 0 else None,
        "leverage": _rounded_number(leverage) if leverage > 0 else None,
        "current_notional_usd": _rounded_number(position_notional) if position_notional > 0 else None,
        "current_margin_usd": _rounded_number(current_margin) if current_margin > 0 else None,
        "target_margin_usd": _rounded_number(target_margin) if target_margin > 0 else None,
        "missing_margin_usd": _rounded_number(missing_margin) if missing_margin > 0 else None,
        "missing_notional_usd": _rounded_number(missing_notional) if missing_notional > 0 else None,
        "tolerance_usd": _rounded_number(tolerance),
        "blockers": blockers,
    }


def _pyramid_context(
    *,
    execution_setup: dict[str, Any],
    owner_state: dict[str, Any] | None,
    position: dict[str, Any],
    snapshot: dict[str, Any],
    position_context_base: dict[str, Any],
    route: dict[str, Any] | None,
    working_entry_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    pyramid_setup = setup.get("pyramid") if isinstance(setup.get("pyramid"), dict) else {}
    step_pct = _numeric(pyramid_setup.get("step_pct") or setup.get("pyramid_step_pct"))
    max_legs = _pyramid_max_legs(execution_setup)
    sl_breakeven = _truthy(pyramid_setup.get("sl_breakeven") or setup.get("sl_breakeven"))
    direction = str(position_context_base.get("direction") or "LONG").upper()
    entry_price = _numeric(position_context_base.get("entry_price"))
    trigger_source = "mark"
    trigger_price = _numeric(position_context_base.get("mark_price") or _first_present(position, "markPx", "mark_price"))
    if trigger_price <= 0:
        trigger_source = "last"
        trigger_price = _numeric(position_context_base.get("last_price") or _first_present(position, "last", "lastPx", "last_price"))
    sizing_policy = _route_sizing_policy(route=route, execution_setup=execution_setup)
    route_leverage = sizing_policy["leverage"]
    margin_allocation_pct = sizing_policy["margin_allocation_pct"]
    account_equity = _account_equity_usd(snapshot) or _numeric(position_context_base.get("account_equity_usd"))
    position_notional = _numeric(position_context_base.get("position_notional_usd")) or _position_notional_usd(position=position, mark_price=trigger_price)
    current_margin = abs(position_notional) / route_leverage if route_leverage > 0 else 0.0
    per_leg_margin = account_equity * margin_allocation_pct / 100 / max_legs if account_equity > 0 and margin_allocation_pct > 0 and max_legs > 0 else 0.0
    raw_legs = current_margin / per_leg_margin if per_leg_margin > 0 else 0.0
    inferred_legs = _infer_pyramid_legs(raw_legs=raw_legs, max_legs=max_legs)
    active_leg_count = inferred_legs or 0
    next_trigger_price = None
    if entry_price > 0 and step_pct > 0 and inferred_legs is not None:
        if direction == "SHORT":
            next_trigger_price = entry_price * (1 - inferred_legs * step_pct / 100)
        else:
            next_trigger_price = entry_price * (1 + inferred_legs * step_pct / 100)
        next_trigger_price = _rounded_number(next_trigger_price)
    blockers: list[str] = []
    if step_pct <= 0:
        blockers.append("missing_pyramid_step_pct")
    if route_leverage <= 0:
        blockers.append("missing_route_leverage")
    if margin_allocation_pct <= 0:
        blockers.append("missing_margin_allocation_pct")
    if account_equity <= 0:
        blockers.append("missing_account_equity")
    if position_notional <= 0:
        blockers.append("missing_position_notional")
    if per_leg_margin <= 0:
        blockers.append("missing_per_leg_margin")
    owner_position_state = owner_state.get("position_state") if owner_state else {}
    if isinstance(owner_position_state, dict) and owner_position_state.get("pyramid_exposure_ambiguous"):
        blockers.append("pyramid_exposure_ambiguous")
    if raw_legs > 0 and inferred_legs is None:
        blockers.append("pyramid_exposure_ambiguous")
    if inferred_legs is not None and inferred_legs >= max_legs:
        blockers.append("max_legs_reached")
    if entry_price <= 0:
        blockers.append("missing_entry_price")
    if trigger_price <= 0:
        blockers.append("missing_trigger_price")
    if working_entry_orders:
        blockers.append("working_add_order_exists")
    trigger_reached = False
    if not blockers and next_trigger_price is not None:
        trigger_reached = trigger_price <= next_trigger_price if direction == "SHORT" else trigger_price >= next_trigger_price
    return {
        "step_pct": _rounded_number(step_pct) if step_pct > 0 else None,
        "max_legs": max_legs,
        "sl_breakeven": sl_breakeven,
        "bucket_tolerance": PYRAMID_LEG_BUCKET_TOLERANCE,
        "raw_legs": _rounded_number(raw_legs) if raw_legs > 0 else None,
        "inferred_legs": inferred_legs,
        "current_margin": _rounded_number(current_margin) if current_margin > 0 else None,
        "per_leg_margin": _rounded_number(per_leg_margin) if per_leg_margin > 0 else None,
        "position_notional_usd": _rounded_number(position_notional) if position_notional > 0 else None,
        "account_equity_usd": _rounded_number(account_equity) if account_equity > 0 else None,
        "filled_legs": inferred_legs or 0,
        "pending_legs": 0,
        "active_legs": active_leg_count,
        "next_leg": (inferred_legs + 1) if inferred_legs is not None else None,
        "last_leg_entry": _rounded_number(entry_price) if entry_price > 0 else None,
        "next_trigger_price": next_trigger_price,
        "trigger_price": _rounded_number(trigger_price) if trigger_price > 0 else None,
        "trigger_source": trigger_source if trigger_price > 0 else None,
        "trigger_reached": trigger_reached,
        "eligible": not blockers,
        "blockers": blockers,
        "position_instance_id": owner_state.get("position_instance_id") if owner_state else None,
        "direction": direction,
    }


def _position_direction(position: dict[str, Any], *, raw_size: float) -> str:
    side = str(position.get("posSide") or position.get("position_side") or position.get("side") or "").lower()
    if side in {"short", "sell"}:
        return "SHORT"
    if side in {"long", "buy"}:
        return "LONG"
    return "SHORT" if raw_size < 0 else "LONG"


def _position_opened_at(position: dict[str, Any], *, owner_state: dict[str, Any] | None) -> datetime | None:
    value = _first_present(position, "opened_at", "open_time", "cTime", "created_at", "uTime")
    if value is None and owner_state:
        value = _first_present(owner_state, "opened_at", "created_at")
    return _parse_datetime(value)


def _parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            return datetime.fromtimestamp(float(value) / 1000, tz=UTC)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _hard_exit_after_hours(execution_setup: dict[str, Any]) -> float | None:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else {}
    value = (
        execution_setup.get("hard_exit_after_hours")
        or execution_setup.get("forward_hours")
        or execution_setup.get("max_hold_hours")
        or setup.get("hard_exit_after_hours")
        or setup.get("forward_hours")
        or setup.get("max_hold_hours")
    )
    if value is None:
        return None
    return _numeric(value)


def _first_present(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if mapping.get(key) not in (None, ""):
            return mapping[key]
    return None


def _normalize_strategy_order_intents(
    *,
    wake_id: str,
    route: dict[str, Any],
    signal: dict[str, Any] | None,
    decision: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
 ) -> list[dict[str, Any]]:
    explicit_intents = decision.get("order_intents")
    if isinstance(explicit_intents, list) and explicit_intents:
        return [
            _coerce_order_intent(
                wake_id=wake_id,
                route=route,
                signal=signal,
                decision=decision,
                execution_setup=execution_setup,
                snapshot=snapshot,
                intent=intent,
                index=index,
            )
            for index, intent in enumerate(explicit_intents)
        ]
    action = _canonical_action(decision.get("action") or decision.get("trade_action") or "SKIP")
    if action not in {"ENTER", "ENTER_LONG", "ENTER_SHORT", "EXIT", "REDUCE", "PYRAMID", "COMPLETE_POSITION", "UPDATE_PROTECTION"}:
        return []
    return [
        _coerce_order_intent(
            wake_id=wake_id,
            route=route,
            signal=signal,
            decision=decision,
            execution_setup=execution_setup,
            snapshot=snapshot,
            intent={},
            index=0,
        )
    ]


def _coerce_order_intent(
    *,
    wake_id: str,
    route: dict[str, Any],
    signal: dict[str, Any] | None,
    decision: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    intent: dict[str, Any],
    index: int,
) -> dict[str, Any]:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    action = _canonical_action(intent.get("action") or decision.get("action") or decision.get("trade_action") or "SKIP")
    direction = str(intent.get("direction") or decision.get("direction") or _direction_from_action(action) or "LONG").upper()
    execution_policy = _execution_setup_policy(execution_setup=execution_setup, direction=direction)
    if execution_policy.get("blocker") and execution_policy.get("policy_mode") == "side_specific":
        raise ValueError(str(execution_policy["blocker"]))
    side = str(intent.get("side") or decision.get("side") or _side_for_action(action=action, direction=direction)).lower()
    client_order_id = f"motis-{route['route_id']}-{wake_id}-{index}"
    quantity = (
        intent.get("quantity")
        or intent.get("order_quantity")
        or decision.get("quantity")
        or decision.get("order_quantity")
        or setup.get("position_quantity")
        or setup.get("quantity")
        or "0"
    )
    notional_usd = (
        intent.get("notional_usd")
        or intent.get("position_notional_usd")
        or decision.get("notional_usd")
        or decision.get("position_notional_usd")
        or setup.get("position_notional_usd")
        or setup.get("notional_usd")
    )
    route_sizing = _route_margin_sizing(route=route, execution_setup=execution_setup, snapshot=snapshot, action=action)
    reconciliation = (
        decision.get("diagnostics", {}).get("position_size_reconciliation")
        if isinstance(decision.get("diagnostics"), dict)
        else None
    )
    if action == "COMPLETE_POSITION":
        quantity = decision.get("completion_margin_usd") or "0"
        notional_usd = decision.get("completion_notional_usd")
    elif route_sizing is not None:
        quantity = route_sizing["margin_usd"]
        notional_usd = route_sizing["notional_usd"]
    target_currency = (
        intent.get("target_currency")
        or intent.get("tgt_ccy")
        or decision.get("target_currency")
        or decision.get("tgt_ccy")
        or setup.get("target_currency")
        or setup.get("tgt_ccy")
    )
    leverage = intent.get("leverage") or decision.get("leverage") or setup.get("leverage")
    if action == "COMPLETE_POSITION":
        sizing_policy = _route_sizing_policy(route=route, execution_setup=execution_setup)
        target_currency = "margin"
        leverage = _rounded_number(sizing_policy["leverage"])
    elif route_sizing is not None:
        target_currency = "margin"
        leverage = route_sizing["leverage"]
    order_intent = {
        "intent_id": f"{wake_id}:{index}",
        "route_id": route["route_id"],
        "asset": route["asset"],
        "instrument": route["instrument"],
        "signal_id": signal["signal_id"] if signal else None,
        "action": action,
        "side": side,
        "direction": direction,
        "order_type": intent.get("order_type") or decision.get("order_type") or setup.get("entry_model", "market"),
        "quantity": str(quantity),
        "notional_usd": notional_usd,
        "trade_mode": intent.get("trade_mode") or decision.get("trade_mode") or setup.get("trade_mode", "isolated"),
        "target_currency": target_currency,
        "leverage": leverage,
        "price": intent.get("price") or decision.get("price"),
        "tp": intent.get("tp") or decision.get("tp"),
        "sl": intent.get("sl") or decision.get("sl"),
        "tp_pct": intent.get("tp_pct") or decision.get("tp_pct") or execution_policy.get("final_tp_pct"),
        "sl_pct": intent.get("sl_pct") or decision.get("sl_pct") or execution_policy.get("initial_sl_pct"),
        "reduce_only": _truthy(intent.get("reduce_only") if "reduce_only" in intent else _default_reduce_only(action)),
        "client_order_id": str(intent.get("client_order_id") or client_order_id)[:64],
        "status": intent.get("status") or "intent_only",
    }
    position_side = (
        intent.get("position_side")
        or decision.get("position_side")
        or _exchange_position_side(snapshot=snapshot, instrument=route["instrument"], direction=direction)
    )
    if position_side not in (None, ""):
        order_intent["position_side"] = str(position_side)
    protection_diagnostics = decision.get("diagnostics", {}).get("protection") if isinstance(decision.get("diagnostics"), dict) else None
    if action == "UPDATE_PROTECTION" and isinstance(protection_diagnostics, dict) and protection_diagnostics.get("phase"):
        order_intent["protection_phase"] = str(protection_diagnostics["phase"])
        for key in ("initial_sl_pct", "trail_sl_pct", "protect_trigger_pct", "protection_enabled"):
            if protection_diagnostics.get(key) is not None:
                order_intent[key] = protection_diagnostics[key]
    for key in ("position_instance_id", "pyramid_leg", "trigger_price", "last_leg_entry"):
        value = intent.get(key) if key in intent else decision.get(key)
        if value not in (None, ""):
            order_intent[key] = value
    if action == "COMPLETE_POSITION" and isinstance(reconciliation, dict):
        order_intent.update(
            {
                "sizing_source": "position_size_reconciliation",
                "account_equity_usd": reconciliation.get("account_equity_usd"),
                "margin_allocation_pct": reconciliation.get("allocation_pct"),
                "margin_usd": reconciliation.get("missing_margin_usd"),
                "completion_target_margin_usd": reconciliation.get("target_margin_usd"),
                "completion_current_margin_usd": reconciliation.get("current_margin_usd"),
            }
        )
    elif route_sizing is not None:
        order_intent.update(
            {
                "sizing_source": route_sizing["sizing_source"],
                "account_equity_usd": route_sizing["account_equity_usd"],
                "margin_allocation_pct": route_sizing["margin_allocation_pct"],
                "pyramid_max_legs": route_sizing["pyramid_max_legs"],
                "margin_usd": route_sizing["margin_usd"],
            }
        )
    return order_intent


def _route_margin_sizing(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any],
    snapshot: dict[str, Any],
    action: str,
) -> dict[str, Any] | None:
    if action not in {"ENTER", "ENTER_LONG", "ENTER_SHORT", "PYRAMID"}:
        return None
    sizing_policy = _route_sizing_policy(route=route, execution_setup=execution_setup)
    margin_allocation_pct = sizing_policy["margin_allocation_pct"]
    leverage = sizing_policy["leverage"]
    account_equity = _account_equity_usd(snapshot)
    if margin_allocation_pct <= 0 or leverage <= 0 or account_equity <= 0:
        return None
    max_legs = _pyramid_max_legs(execution_setup)
    margin_usd = account_equity * margin_allocation_pct / 100 / max_legs
    return {
        "account_equity_usd": _rounded_number(account_equity),
        "sizing_source": sizing_policy["source"],
        "margin_allocation_pct": _rounded_number(margin_allocation_pct),
        "leverage": _rounded_number(leverage),
        "pyramid_max_legs": max_legs,
        "margin_usd": _rounded_number(margin_usd),
        "notional_usd": _rounded_number(margin_usd * leverage),
    }


def _route_sizing_policy(*, route: dict[str, Any] | None, execution_setup: dict[str, Any]) -> dict[str, Any]:
    if route and _truthy(route.get("manual_sizing_enabled")):
        return {
            "source": "manual_route_override",
            "margin_allocation_pct": _numeric(route.get("margin_allocation_pct")),
            "leverage": _numeric(route.get("leverage")),
        }
    sizing = execution_setup.get("sizing") if isinstance(execution_setup.get("sizing"), dict) else {}
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    margin_allocation_pct = _numeric(
        sizing.get("margin_allocation_pct")
        or execution_setup.get("margin_allocation_pct")
        or setup.get("margin_allocation_pct")
    )
    leverage = _numeric(
        sizing.get("leverage")
        or execution_setup.get("leverage")
        or setup.get("leverage")
    )
    return {
        "source": "bundle_stage4_sizing",
        "margin_allocation_pct": margin_allocation_pct,
        "leverage": leverage,
    }


def _pyramid_max_legs(execution_setup: dict[str, Any]) -> int:
    setup = execution_setup.get("setup") if isinstance(execution_setup.get("setup"), dict) else execution_setup
    pyramid = setup.get("pyramid") if isinstance(setup.get("pyramid"), dict) else {}
    value = pyramid.get("max_legs") or setup.get("max_legs") or 1
    try:
        max_legs = int(value)
    except (TypeError, ValueError):
        max_legs = 1
    return max(1, max_legs)


def _account_equity_usd(snapshot: dict[str, Any]) -> float:
    balance = snapshot.get("balance") if isinstance(snapshot, dict) else None
    candidates: list[Any] = []
    balance_rows: list[dict[str, Any]] = []
    if isinstance(balance, dict):
        candidates.extend([balance.get("totalEq"), balance.get("eq"), balance.get("availEq"), balance.get("availBal")])
        data = balance.get("data")
        if isinstance(data, list):
            balance_rows.extend([row for row in data if isinstance(row, dict)])
    elif isinstance(balance, list):
        balance_rows.extend([row for row in balance if isinstance(row, dict)])
    for row in balance_rows:
        if row.get("ccy") in {None, "", "USDT", "USD"}:
            candidates.extend([row.get("totalEq"), row.get("eq"), row.get("availEq"), row.get("availBal")])
        details = row.get("details")
        if isinstance(details, list):
            for detail in details:
                if isinstance(detail, dict) and detail.get("ccy") in {None, "", "USDT", "USD"}:
                    candidates.extend([detail.get("eqUsd"), detail.get("eq"), detail.get("availEq"), detail.get("availBal")])
    for candidate in candidates:
        value = _numeric(candidate)
        if value > 0:
            return value
    return 0.0


def _exchange_position_side(*, snapshot: dict[str, Any], instrument: str, direction: str) -> str | None:
    positions = [
        position
        for position in snapshot.get("positions") or []
        if str(position.get("instId") or position.get("instrument") or "") == instrument
        and abs(_numeric(position.get("pos") or position.get("size") or position.get("sz"))) > 0
    ]
    direction = str(direction or "LONG").upper()
    for position in positions:
        raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
        if _position_direction(position, raw_size=raw_size) == direction:
            value = position.get("posSide") or position.get("position_side")
            return str(value) if value not in (None, "") else None
    return None


def _position_notional_usd(*, position: dict[str, Any], mark_price: float) -> float:
    explicit = _numeric(
        _first_present(
            position,
            "notionalUsd",
            "notional_usd",
            "notional",
            "posNotional",
            "position_notional_usd",
        )
    )
    if explicit > 0:
        return abs(explicit)
    size = abs(_numeric(position.get("pos") or position.get("size") or position.get("sz")))
    return size * mark_price if size > 0 and mark_price > 0 else 0.0


def _infer_pyramid_legs(*, raw_legs: float, max_legs: int) -> int | None:
    if raw_legs <= 0:
        return None
    for leg in range(1, max_legs + 1):
        if abs(raw_legs - leg) <= PYRAMID_LEG_BUCKET_TOLERANCE:
            return leg
    return None


def _favorable_move_pct(*, entry_price: float, mark_price: float, direction: str) -> float | None:
    if entry_price <= 0 or mark_price <= 0:
        return None
    if direction == "SHORT":
        return (entry_price - mark_price) / entry_price * 100
    return (mark_price - entry_price) / entry_price * 100


def _live_sl_is_protected(*, entry_price: float, live_sl: float, direction: str) -> bool:
    if entry_price <= 0 or live_sl <= 0:
        return False
    return live_sl < entry_price if direction == "SHORT" else live_sl > entry_price


def _protection_prices(*, entry_price: float, direction: str, tp_pct: float, sl_pct: float) -> tuple[str, str]:
    return (
        _take_profit_price(entry_price=entry_price, direction=direction, tp_pct=tp_pct),
        _initial_stop_price(entry_price=entry_price, direction=direction, sl_pct=sl_pct),
    )


def _take_profit_price(*, entry_price: float, direction: str, tp_pct: float) -> str:
    if direction == "SHORT":
        return _format_decimal(entry_price * (1 - tp_pct / 100))
    return _format_decimal(entry_price * (1 + tp_pct / 100))


def _initial_stop_price(*, entry_price: float, direction: str, sl_pct: float) -> str:
    if direction == "SHORT":
        return _format_decimal(entry_price * (1 + sl_pct / 100))
    return _format_decimal(entry_price * (1 - sl_pct / 100))


def _protected_stop_price(*, entry_price: float, direction: str, trail_sl_pct: float) -> str:
    if direction == "SHORT":
        return _format_decimal(entry_price * (1 - trail_sl_pct / 100))
    return _format_decimal(entry_price * (1 + trail_sl_pct / 100))


def _protection_state(*, snapshot: dict[str, Any], instrument: str) -> dict[str, Any]:
    orders = [
        order
        for order in snapshot.get("protection_orders") or []
        if str(order.get("instId") or order.get("instrument") or "") == instrument
        and str(order.get("state") or "").lower() in {"", "live"}
    ]
    return {"orders": orders, "live_count": len(orders), "has_single_live": len(orders) == 1}


def _protection_matches(order: dict[str, Any], *, side: str, size: str, tp: str, sl: str) -> bool:
    return (
        str(order.get("side") or "").lower() == side
        and (_protection_closes_entire_position(order) or _same_decimal(order.get("sz") or order.get("size"), size))
        and _same_decimal(order.get("tpTriggerPx") or order.get("tp"), tp)
        and _same_decimal(order.get("slTriggerPx") or order.get("sl"), sl)
    )


def _protection_closes_entire_position(order: dict[str, Any]) -> bool:
    try:
        if float(order.get("closeFraction") or 0) == 1:
            return True
    except (TypeError, ValueError):
        pass
    return (order.get("sz") or order.get("size")) in (None, "")


def _protection_order_predates_position(order: dict[str, Any], *, position_context: dict[str, Any]) -> bool:
    order_created_at = _parse_datetime(order.get("cTime") or order.get("created_at"))
    position_opened_at = _parse_datetime(position_context.get("opened_at"))
    return bool(order_created_at and position_opened_at and order_created_at < position_opened_at)


def _same_decimal(left: Any, right: Any) -> bool:
    try:
        return abs(float(left) - float(right)) < 1e-8
    except (TypeError, ValueError):
        return str(left) == str(right)


def _rounded_number(value: float) -> float | int:
    rounded = round(float(value), 8)
    return int(rounded) if rounded.is_integer() else rounded


def _canonical_action(value: Any) -> str:
    action = str(value or "SKIP").upper()
    aliases = {
        "LONG": "ENTER_LONG",
        "SHORT": "ENTER_SHORT",
        "ENTER": "ENTER",
        "BUY": "ENTER_LONG",
        "SELL": "ENTER_SHORT",
        "NO_TRADE": "SKIP",
        "WAIT": "SKIP",
    }
    return aliases.get(action, action)


def _direction_from_action(action: str) -> str | None:
    if action in {"ENTER_LONG", "PYRAMID"}:
        return "LONG"
    if action == "ENTER_SHORT":
        return "SHORT"
    return None


def _side_for_action(*, action: str, direction: str) -> str:
    if action in {"EXIT", "REDUCE", "UPDATE_PROTECTION"}:
        return "sell" if direction == "LONG" else "buy"
    return "buy" if direction == "LONG" else "sell"


def _default_reduce_only(action: str) -> bool:
    return action in {"EXIT", "REDUCE", "UPDATE_PROTECTION"}


def _numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _optional_numeric(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _first_numeric(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _optional_numeric(row.get(key))
        if value is not None:
            return value
    return None


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _positive_float(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _format_decimal(value: Any) -> str:
    numeric = _numeric(value)
    if numeric.is_integer():
        return str(int(numeric))
    return f"{numeric:.12f}".rstrip("0").rstrip(".")


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).lower() in {"true", "1", "yes"}
