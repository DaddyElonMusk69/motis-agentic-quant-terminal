from __future__ import annotations

from datetime import UTC, datetime
from typing import Any


DECISION_CADENCE_SCHEMA_VERSION = "execution_decision_cadence.v1"
DEFAULT_DECISION_INTERVAL_MINUTES = 5
DEFAULT_WAKE_GRACE_SECONDS = 15
DEFAULT_LATE_ARRIVAL_GRACE_SECONDS = 600


def build_decision_cadence_contract(
    *,
    cursor_seed_timestamp: Any,
    source_stage4_run_id: Any = None,
    source_stage4_candidate_id: Any = None,
) -> dict[str, Any]:
    return {
        "schema_version": DECISION_CADENCE_SCHEMA_VERSION,
        "canonical_timeframe": "5m",
        "decision_interval_minutes": DEFAULT_DECISION_INTERVAL_MINUTES,
        "wake_grace_seconds": DEFAULT_WAKE_GRACE_SECONDS,
        "late_arrival_grace_seconds": DEFAULT_LATE_ARRIVAL_GRACE_SECONDS,
        "timestamp_match": "exact_utc",
        "cursor_seed_timestamp": _iso_z(cursor_seed_timestamp),
        "source_stage4_run_id": str(source_stage4_run_id or "") or None,
        "source_stage4_candidate_id": str(source_stage4_candidate_id or "") or None,
        "blocked_opportunities_advance_cursor": True,
        "expired_opportunities_advance_cursor": True,
    }


def resolve_decision_cadence_contract(
    *,
    route: dict[str, Any],
    execution_setup: dict[str, Any] | None = None,
) -> dict[str, Any]:
    setup = execution_setup if isinstance(execution_setup, dict) else _route_execution_setup(route)
    raw = setup.get("decision_cadence") if isinstance(setup.get("decision_cadence"), dict) else {}
    seed = raw.get("cursor_seed_timestamp")
    if seed in (None, ""):
        seed = route.get("created_at") or datetime.min.replace(tzinfo=UTC)
    return {
        "schema_version": str(raw.get("schema_version") or DECISION_CADENCE_SCHEMA_VERSION),
        "canonical_timeframe": str(raw.get("canonical_timeframe") or "5m"),
        "decision_interval_minutes": _nonnegative_int(
            raw.get("decision_interval_minutes"),
            DEFAULT_DECISION_INTERVAL_MINUTES,
            minimum=1,
        ),
        "wake_grace_seconds": _nonnegative_int(
            raw.get("wake_grace_seconds"), DEFAULT_WAKE_GRACE_SECONDS
        ),
        "late_arrival_grace_seconds": _nonnegative_int(
            raw.get("late_arrival_grace_seconds"),
            DEFAULT_LATE_ARRIVAL_GRACE_SECONDS,
        ),
        "timestamp_match": "exact_utc",
        "cursor_seed_timestamp": _iso_z(seed),
        "source_stage4_run_id": raw.get("source_stage4_run_id"),
        "source_stage4_candidate_id": raw.get("source_stage4_candidate_id"),
        "blocked_opportunities_advance_cursor": True,
        "expired_opportunities_advance_cursor": True,
    }


def decision_cursor(route: dict[str, Any], *, contract: dict[str, Any]) -> datetime:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = (
        risk_limits.get("pause_rule_state")
        if isinstance(risk_limits.get("pause_rule_state"), dict)
        else {}
    )
    cadence_state = (
        pause_state.get("decision_cadence")
        if isinstance(pause_state.get("decision_cadence"), dict)
        else {}
    )
    value = cadence_state.get("last_processed_decision_ts") or contract["cursor_seed_timestamp"]
    return parse_utc_timestamp(value)


def persist_decision_cursor(
    *,
    repository: Any,
    route: dict[str, Any],
    contract: dict[str, Any],
    decision_timestamp: Any,
    outcome: str,
    signal_id: str | None,
) -> dict[str, Any]:
    timestamp = parse_utc_timestamp(decision_timestamp)
    current = decision_cursor(route, contract=contract)
    if timestamp < current:
        return route
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = (
        risk_limits.get("pause_rule_state")
        if isinstance(risk_limits.get("pause_rule_state"), dict)
        else {}
    )
    cadence_state = (
        pause_state.get("decision_cadence")
        if isinstance(pause_state.get("decision_cadence"), dict)
        else {}
    )
    next_risk_limits = {
        **risk_limits,
        "pause_rule_state": {
            **pause_state,
            "decision_cadence": {
                **cadence_state,
                "schema_version": DECISION_CADENCE_SCHEMA_VERSION,
                "contract": contract,
                "last_processed_decision_ts": _iso_z(timestamp),
                "last_outcome": outcome,
                "last_signal_id": signal_id,
                "updated_at": _iso_z(datetime.now(UTC)),
            },
        },
    }
    updater = getattr(repository, "update_deployment_route_gate", None)
    if not callable(updater):
        return {**route, "risk_limits": next_risk_limits}
    updated = updater(route["route_id"], risk_limits=next_risk_limits)
    return updated or {**route, "risk_limits": next_risk_limits}


def ensure_decision_cadence_state(
    *,
    repository: Any,
    route: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    risk_limits = route.get("risk_limits") if isinstance(route.get("risk_limits"), dict) else {}
    pause_state = (
        risk_limits.get("pause_rule_state")
        if isinstance(risk_limits.get("pause_rule_state"), dict)
        else {}
    )
    cadence_state = (
        pause_state.get("decision_cadence")
        if isinstance(pause_state.get("decision_cadence"), dict)
        else {}
    )
    if cadence_state.get("contract") == contract and cadence_state.get(
        "last_processed_decision_ts"
    ):
        return route
    return persist_decision_cursor(
        repository=repository,
        route=route,
        contract=contract,
        decision_timestamp=cadence_state.get("last_processed_decision_ts")
        or contract["cursor_seed_timestamp"],
        outcome=str(cadence_state.get("last_outcome") or "cadence_initialized"),
        signal_id=cadence_state.get("last_signal_id"),
    )


def parse_utc_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _route_execution_setup(route: dict[str, Any]) -> dict[str, Any]:
    bundle = route.get("active_bundle") if isinstance(route.get("active_bundle"), dict) else {}
    setup = bundle.get("execution_setup") if isinstance(bundle.get("execution_setup"), dict) else {}
    return setup


def _nonnegative_int(value: Any, fallback: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(value))
    except (TypeError, ValueError):
        return fallback


def _iso_z(value: Any) -> str:
    return parse_utc_timestamp(value).isoformat().replace("+00:00", "Z")
