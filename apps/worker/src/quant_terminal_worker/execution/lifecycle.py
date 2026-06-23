from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_worker.execution.data_warmup import warm_route_data
from quant_terminal_worker.execution.order_submission import submit_wake_order_intents
from quant_terminal_worker.execution.wake_runner import run_route_wake
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles


def run_route_lifecycle_cycle(
    *,
    route_id: str,
    runtime_repository: Any,
    market_data_repository: Any,
    fill_service: Any,
    signal_pool_extender: Any | None,
    live_signal_scanner: Any | None = None,
    adapter: Any,
    workspace_root: Path,
) -> dict[str, Any]:
    route = runtime_repository.get_deployment_route(route_id)
    if route is None:
        raise ValueError(f"deployment route not found: {route_id}")

    started_at = datetime.now(UTC)
    warmup = _warm_market_data(
        route=route,
        runtime_repository=runtime_repository,
        market_data_repository=market_data_repository,
        fill_service=fill_service,
        adapter=adapter,
        workspace_root=workspace_root,
    )
    if warmup.get("status") == "blocked":
        route_after_block = _record_route_cycle(
            runtime_repository=runtime_repository,
            route_id=route_id,
            wake=None,
            route=runtime_repository.get_deployment_route(route_id) or route,
            error={"stage": "data_warmup", "detail": warmup},
            completed_at=datetime.now(UTC),
        )
        return {
            "status": "blocked",
            "warmup": warmup,
            "signal_update": {"status": "not_run", "reason": "data_warmup_blocked"},
            "wake": None,
            "submission": {"status": "not_run"},
            "route": route_after_block,
        }

    route = runtime_repository.get_deployment_route(route_id) or route
    signal_update = _extend_signals(
        route=route,
        runtime_repository=runtime_repository,
        signal_pool_extender=signal_pool_extender,
        workspace_root=workspace_root,
    )
    wake = run_route_wake(
        route_id=route_id,
        repository=runtime_repository,
        adapter=adapter,
        workspace_root=workspace_root,
        allow_entry_scan=True,
        live_signal_scanner=live_signal_scanner,
    )
    submission = _submit_if_enabled(
        route=runtime_repository.get_deployment_route(route_id) or route,
        wake=wake,
        runtime_repository=runtime_repository,
        adapter=adapter,
    )
    completed_at = datetime.now(UTC)
    route_after_cycle = _record_route_cycle(
        runtime_repository=runtime_repository,
        route_id=route_id,
        wake=wake,
        route=runtime_repository.get_deployment_route(route_id) or route,
        error={} if wake.get("status") != "error" else wake.get("error", {}),
        completed_at=completed_at,
    )
    return {
        "status": wake.get("status", "completed"),
        "started_at": started_at,
        "completed_at": completed_at,
        "warmup": warmup,
        "signal_update": signal_update,
        "wake": wake,
        "submission": submission,
        "route": route_after_cycle,
    }


def next_wake_at(route: dict[str, Any], *, from_time: datetime | None = None) -> datetime:
    base = from_time or datetime.now(UTC)
    try:
        minutes = int(route.get("cron_interval_minutes") or 5)
    except (TypeError, ValueError):
        minutes = 5
    return base + timedelta(minutes=max(1, minutes))


def _warm_market_data(
    *,
    route: dict[str, Any],
    runtime_repository: Any,
    market_data_repository: Any,
    fill_service: Any,
    adapter: Any,
    workspace_root: Path,
) -> dict[str, Any]:
    non_data_blockers = [blocker for blocker in route.get("blockers", []) if blocker != "data_not_warmed"]
    if non_data_blockers:
        return {
            "status": "skipped",
            "route_id": route["route_id"],
            "reason": "route_blocked_before_data_warmup",
            "blockers": non_data_blockers,
        }
    if market_data_repository is None:
        return {
            "status": "blocked",
            "route_id": route["route_id"],
            "reason": "missing_market_data_repository",
        }
    return warm_route_data(
        route_id=route["route_id"],
        runtime_repository=runtime_repository,
        market_data_repository=market_data_repository,
        fill_service=fill_service,
        adapter=adapter,
        workspace_root=workspace_root,
    )


def _extend_signals(
    *,
    route: dict[str, Any],
    runtime_repository: Any,
    signal_pool_extender: Any | None,
    workspace_root: Path,
) -> dict[str, Any]:
    if route.get("account_mode") == "live":
        return {
            "status": "skipped",
            "reason": "live_execution_uses_observation_log",
        }
    if route.get("blockers"):
        return {
            "status": "skipped",
            "reason": "route_blocked_before_signal_update",
            "blockers": route.get("blockers", []),
        }
    service = signal_pool_extender or extend_signal_pool_from_local_candles
    try:
        return service(
            workspace_root=workspace_root,
            repository=runtime_repository,
            signal_engine_id=route["signal_engine_id"],
            asset=route["asset"],
            target_end=None,
        )
    except ValueError as exc:
        return {
            "status": "blocked",
            "reason": "signal_update_failed",
            "detail": str(exc),
        }


def _submit_if_enabled(
    *,
    route: dict[str, Any],
    wake: dict[str, Any],
    runtime_repository: Any,
    adapter: Any,
) -> dict[str, Any]:
    if not route.get("auto_submit_enabled"):
        return {"status": "skipped", "reason": "auto_submit_disabled"}
    if not wake.get("order_intents"):
        return {"status": "skipped", "reason": "no_order_intents"}
    return submit_wake_order_intents(
        route_id=route["route_id"],
        wake_id=wake["wake_id"],
        repository=runtime_repository,
        adapter=adapter,
        confirm_live=route.get("account_mode") == "live",
    )


def _record_route_cycle(
    *,
    runtime_repository: Any,
    route_id: str,
    wake: dict[str, Any] | None,
    route: dict[str, Any],
    error: dict[str, Any],
    completed_at: datetime,
) -> dict[str, Any] | None:
    return runtime_repository.update_deployment_route_gate(
        route_id,
        last_wake_at=completed_at,
        last_wake_id=wake.get("wake_id") if wake else route.get("last_wake_id"),
        next_wake_at=next_wake_at(route, from_time=completed_at) if route.get("scheduler_status") == "running" else None,
        last_lifecycle_error=error,
    )
