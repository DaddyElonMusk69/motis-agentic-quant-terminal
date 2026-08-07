from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_worker.execution.data_warmup import warm_route_data
from quant_terminal_worker.execution.order_submission import submit_wake_order_intents
from quant_terminal_worker.execution.wake_runner import run_route_wake
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles


DEFAULT_LIVE_CANDLE_CLOSE_GRACE_SECONDS = 15


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
    raw_fill_services: dict[str, Any] | None = None,
    raw_adapters: dict[str, Any] | None = None,
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
        raw_fill_services=raw_fill_services,
        raw_adapters=raw_adapters,
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
        market_data_repository=market_data_repository,
        workspace_root=workspace_root,
        allow_entry_scan=signal_update.get("status") != "blocked",
        live_signal_scanner=live_signal_scanner,
    )
    submission = _submit_if_enabled(
        route=runtime_repository.get_deployment_route(route_id) or route,
        wake=wake,
        runtime_repository=runtime_repository,
        adapter=adapter,
    )
    completed_at = datetime.now(UTC)
    lifecycle_error: dict[str, Any] = {}
    if wake.get("status") == "error":
        lifecycle_error = wake.get("error", {})
    elif submission.get("status") == "failed":
        lifecycle_error = {
            "stage": "order_submission",
            **(submission.get("error") or {}),
        }
    route_after_cycle = _record_route_cycle(
        runtime_repository=runtime_repository,
        route_id=route_id,
        wake=wake,
        route=runtime_repository.get_deployment_route(route_id) or route,
        error=lifecycle_error,
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
    base = _utc(from_time or datetime.now(UTC))
    try:
        minutes = int(route.get("cron_interval_minutes") or 5)
    except (TypeError, ValueError):
        minutes = 5
    minutes = max(1, minutes)
    if route.get("account_mode") == "live":
        return _next_aligned_candle_close_wake(
            base,
            interval_minutes=minutes,
            grace_seconds=_live_candle_close_grace_seconds(route),
        )
    return base + timedelta(minutes=minutes)


def _next_aligned_candle_close_wake(
    base: datetime,
    *,
    interval_minutes: int,
    grace_seconds: int,
) -> datetime:
    interval_seconds = max(1, interval_minutes) * 60
    day_start = base.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_seconds = int((base - day_start).total_seconds())
    next_boundary_offset = ((elapsed_seconds // interval_seconds) + 1) * interval_seconds
    current_boundary_offset = (elapsed_seconds // interval_seconds) * interval_seconds
    current_boundary_wake = day_start + timedelta(seconds=current_boundary_offset + grace_seconds)
    if base <= current_boundary_wake:
        return current_boundary_wake
    return day_start + timedelta(seconds=next_boundary_offset + grace_seconds)


def _live_candle_close_grace_seconds(route: dict[str, Any]) -> int:
    value = route.get("candle_close_grace_seconds")
    if value is None:
        value = route.get("live_candle_close_grace_seconds")
    try:
        return max(0, int(value if value is not None else DEFAULT_LIVE_CANDLE_CLOSE_GRACE_SECONDS))
    except (TypeError, ValueError):
        return DEFAULT_LIVE_CANDLE_CLOSE_GRACE_SECONDS


def _utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _warm_market_data(
    *,
    route: dict[str, Any],
    runtime_repository: Any,
    market_data_repository: Any,
    fill_service: Any,
    raw_fill_services: dict[str, Any] | None,
    raw_adapters: dict[str, Any] | None,
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
        raw_fill_services=raw_fill_services,
        raw_adapters=raw_adapters,
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
    except Exception as exc:
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
