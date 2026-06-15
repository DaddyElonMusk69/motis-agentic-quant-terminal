from __future__ import annotations

import threading
from datetime import UTC, datetime
from typing import Any, Callable

from quant_terminal_worker.execution.lifecycle import next_wake_at


class RouteLifecycleScheduler:
    def __init__(
        self,
        *,
        load_route: Callable[[str], dict[str, Any] | None],
        update_route: Callable[[str, dict[str, Any]], dict[str, Any] | None],
        run_cycle: Callable[[str], dict[str, Any]],
        list_routes: Callable[[], list[dict[str, Any]]] | None = None,
    ) -> None:
        self._load_route = load_route
        self._list_routes = list_routes
        self._update_route = update_route
        self._run_cycle = run_cycle
        self._timers: dict[str, threading.Timer] = {}
        self._lock = threading.Lock()

    def start(self, route_id: str, *, run_immediately: bool = True) -> dict[str, Any]:
        route = self._load_route(route_id)
        if route is None:
            raise ValueError(f"deployment route not found: {route_id}")
        now = datetime.now(UTC)
        updated = self._update_route(
            route_id,
            {
                "scheduler_status": "running",
                "next_wake_at": now if run_immediately else next_wake_at(route, from_time=now),
                "last_lifecycle_error": {},
            },
        )
        self._schedule(route_id, delay_seconds=0 if run_immediately else _interval_seconds(updated or route))
        return updated or route

    def stop(self, route_id: str) -> dict[str, Any]:
        self._cancel(route_id)
        route = self._update_route(
            route_id,
            {
                "scheduler_status": "stopped",
                "next_wake_at": None,
            },
        )
        if route is None:
            raise ValueError(f"deployment route not found: {route_id}")
        return route

    def is_scheduled(self, route_id: str) -> bool:
        with self._lock:
            return route_id in self._timers

    def resume_running(self) -> list[str]:
        if self._list_routes is None:
            return []
        resumed: list[str] = []
        now = datetime.now(UTC)
        for route in self._list_routes():
            if not _route_is_running(route):
                continue
            route_id = str(route["route_id"])
            self._schedule(route_id, delay_seconds=_resume_delay_seconds(route, now=now))
            resumed.append(route_id)
        return resumed

    def _schedule(self, route_id: str, *, delay_seconds: float) -> None:
        self._cancel(route_id)
        timer = threading.Timer(delay_seconds, self._run_and_reschedule, args=(route_id,))
        timer.daemon = True
        with self._lock:
            self._timers[route_id] = timer
        timer.start()

    def _cancel(self, route_id: str) -> None:
        with self._lock:
            timer = self._timers.pop(route_id, None)
        if timer is not None:
            timer.cancel()

    def _run_and_reschedule(self, route_id: str) -> None:
        with self._lock:
            self._timers.pop(route_id, None)
        route = self._load_route(route_id)
        if route is None or route.get("scheduler_status") != "running":
            return
        try:
            self._run_cycle(route_id)
        except Exception as exc:  # pragma: no cover - scheduler boundary
            self._update_route(
                route_id,
                {
                    "last_lifecycle_error": {
                        "message": str(exc),
                        "raised_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
                    },
                },
            )
        route = self._load_route(route_id)
        if route is None or route.get("scheduler_status") != "running":
            return
        self._schedule(route_id, delay_seconds=_interval_seconds(route))


def _interval_seconds(route: dict[str, Any]) -> int:
    try:
        minutes = int(route.get("cron_interval_minutes") or 5)
    except (TypeError, ValueError):
        minutes = 5
    return max(1, minutes) * 60


def _route_is_running(route: dict[str, Any]) -> bool:
    return route.get("scheduler_status") == "running"


def _resume_delay_seconds(route: dict[str, Any], *, now: datetime) -> float:
    next_wake = _parse_datetime(route.get("next_wake_at"))
    if next_wake is None:
        return _interval_seconds(route)
    return max(0.0, (next_wake - now).total_seconds())


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None
