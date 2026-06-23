from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_terminal_worker.ingestion.feature_enrichment import FEATURE_FAMILIES, enrich_feature_family_datasets


FEATURE_DATA_TYPE_TO_FAMILY = {family.data_type: key for key, family in FEATURE_FAMILIES.items()}


def warm_route_data(
    *,
    route_id: str,
    runtime_repository: Any,
    market_data_repository: Any,
    fill_service: Any,
    adapter: Any,
    feature_service: Any | None = None,
    workspace_root: Path | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    route = runtime_repository.get_deployment_route(route_id)
    if route is None:
        raise ValueError(f"deployment route not found: {route_id}")

    engine = _find_engine(
        runtime_repository.list_signal_engines(),
        signal_engine_id=route["signal_engine_id"],
        version=route.get("signal_engine_version"),
    )
    if engine is None:
        return _blocked(route=route, requirements=[_requirement_result({}, "blocked", "signal_engine_not_registered")])

    required_data = list(engine.get("required_data") or [])
    if not required_data:
        updated_route = runtime_repository.update_deployment_route_gate(route_id, data_warmed=True)
        return {
            "status": "warmed",
            "route_id": route_id,
            "asset": route["asset"],
            "signal_engine_id": route["signal_engine_id"],
            "requirements": [],
            "route": updated_route,
        }

    checked_at = as_of or datetime.now(UTC)
    requirement_results: list[dict[str, Any]] = []
    raw_results_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    raw_refs_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    blocked = False

    for requirement in required_data:
        data_type = requirement.get("data_type")
        if data_type != "candles" and data_type not in FEATURE_DATA_TYPE_TO_FAMILY:
            requirement_results.append(_requirement_result(requirement, "blocked", "unsupported_data_type"))
            blocked = True
            continue
        if data_type != "candles" or requirement.get("origin") != "raw":
            continue

        timeframe = requirement.get("timeframe") or "5m"
        raw_ref = market_data_repository.get_raw_candle_ref(route["asset"], timeframe)
        if raw_ref is None:
            requirement_results.append(_requirement_result(requirement, "blocked", "missing_raw_candle_ref"))
            blocked = True
            continue

        fill_result = _call_fill_service(
            fill_service=fill_service,
            registration=raw_ref,
            repository=market_data_repository,
            adapter=adapter,
            as_of=checked_at,
        )
        raw_refs_by_key[("candles", timeframe)] = raw_ref
        raw_results_by_key[("candles", timeframe)] = fill_result
        requirement_results.append(
            {
                **_requirement_result(requirement, fill_result.get("status", "unknown")),
                "dataset_id": raw_ref["dataset_id"],
                "fill_result": fill_result,
            }
        )

    for requirement in required_data:
        if requirement.get("data_type") != "candles" or requirement.get("origin") != "derived":
            continue

        source = requirement.get("source") or {}
        source_timeframe = source.get("timeframe") or "5m"
        source_key = (source.get("data_type") or "candles", source_timeframe)
        if source_key not in raw_results_by_key:
            requirement_results.append(_requirement_result(requirement, "blocked", "missing_source_raw_requirement"))
            blocked = True
            continue

        source_ref = market_data_repository.get_raw_candle_ref(route["asset"], source_timeframe)
        derived_refs = market_data_repository.list_derived_refs_for_raw(source_ref) if source_ref is not None else []
        matching_ref = next(
            (
                ref
                for ref in derived_refs
                if ref.get("data_type") == requirement.get("data_type")
                and ref.get("data_origin") == "derived"
                and ref.get("timeframe") == requirement.get("timeframe")
            ),
            None,
        )
        if matching_ref is None:
            requirement_results.append(_requirement_result(requirement, "blocked", "missing_derived_candle_ref"))
            blocked = True
            continue
        requirement_results.append(
            {
                **_requirement_result(requirement, "satisfied_by_raw_rebuild"),
                "dataset_id": matching_ref["dataset_id"],
                "source_timeframe": source_timeframe,
            }
        )

    data_freshness = _route_data_freshness(
        route=route,
        requirements=required_data,
        market_data_repository=market_data_repository,
        checked_at=checked_at,
    )
    if data_freshness.get("status") == "stale":
        retry_result = _retry_stale_raw_5m(
            route=route,
            requirements=required_data,
            raw_refs_by_key=raw_refs_by_key,
            market_data_repository=market_data_repository,
            fill_service=fill_service,
            adapter=adapter,
            checked_at=checked_at,
        )
        if retry_result is not None:
            data_freshness["retry_result"] = retry_result
            data_freshness = _route_data_freshness(
                route=route,
                requirements=required_data,
                market_data_repository=market_data_repository,
                checked_at=checked_at,
            ) | {"retry_result": retry_result}
        if data_freshness.get("status") == "stale":
            updated_route = runtime_repository.update_deployment_route_gate(route_id, data_warmed=False)
            return {
                "status": "blocked",
                "route_id": route_id,
                "asset": route["asset"],
                "signal_engine_id": route["signal_engine_id"],
                "reason": "market_data_stale",
                "requirements": requirement_results,
                "data_freshness": data_freshness,
                "route": updated_route or route,
            }

    for requirement in required_data:
        data_type = requirement.get("data_type")
        if data_type not in FEATURE_DATA_TYPE_TO_FAMILY:
            continue
        if requirement.get("origin") != "derived":
            requirement_results.append(_requirement_result(requirement, "blocked", "feature_data_must_be_derived"))
            blocked = True
            continue
        family = FEATURE_DATA_TYPE_TO_FAMILY[str(data_type)]
        service = feature_service or enrich_feature_family_datasets
        target_root = (workspace_root or Path(".")) / ".data" / "market-data"
        feature_result = service(
            repository=market_data_repository,
            asset=route["asset"],
            family=family,
            target_root=target_root,
        )
        matching_feature = next(
            (
                item
                for item in feature_result.get("features", [])
                if item.get("data_type") == data_type and item.get("timeframe") == requirement.get("timeframe")
            ),
            None,
        )
        if matching_feature is None:
            matching_ref = _get_ref(
                market_data_repository,
                asset=route["asset"],
                timeframe=requirement.get("timeframe"),
                origin="derived",
                data_type=str(data_type),
            )
            if matching_ref is not None:
                matching_feature = {
                    "dataset_id": matching_ref["dataset_id"],
                    "timeframe": matching_ref.get("timeframe"),
                    "row_count": matching_ref.get("row_count"),
                    "data_type": matching_ref.get("data_type"),
                }
        if matching_feature is None:
            requirement_results.append(
                {
                    **_requirement_result(requirement, "blocked", "feature_refresh_produced_no_matching_dataset"),
                    "feature_result": feature_result,
                }
            )
            blocked = True
            continue
        requirement_results.append(
            {
                **_requirement_result(requirement, "feature_enriched"),
                "dataset_id": matching_feature["dataset_id"],
                "family": family,
                "feature_result": feature_result,
            }
        )

    if blocked:
        return _blocked(route=route, requirements=requirement_results)

    updated_route = runtime_repository.update_deployment_route_gate(route_id, data_warmed=True)
    return {
        "status": "warmed",
        "route_id": route_id,
        "asset": route["asset"],
        "signal_engine_id": route["signal_engine_id"],
        "requirements": requirement_results,
        "data_freshness": data_freshness,
        "route": updated_route,
    }


def _find_engine(
    engines: list[dict[str, Any]],
    *,
    signal_engine_id: str,
    version: str | None,
) -> dict[str, Any] | None:
    matching = [engine for engine in engines if engine.get("signal_engine_id") == signal_engine_id]
    if version is not None:
        for engine in matching:
            if engine.get("version") == version:
                return engine
    return matching[0] if matching else None


def _requirement_result(
    requirement: dict[str, Any],
    status: str,
    reason: str | None = None,
) -> dict[str, Any]:
    result = {
        "data_type": requirement.get("data_type"),
        "origin": requirement.get("origin"),
        "timeframe": requirement.get("timeframe"),
        "status": status,
    }
    if reason is not None:
        result["reason"] = reason
    return result


def _get_ref(repository: Any, *, asset: str, timeframe: str | None, origin: str, data_type: str) -> dict[str, Any] | None:
    getter = getattr(repository, "get_candle_ref", None)
    if not callable(getter):
        return None
    return getter(asset=asset, timeframe=timeframe, origin=origin, data_type=data_type)


def _route_data_freshness(
    *,
    route: dict[str, Any],
    requirements: list[dict[str, Any]],
    market_data_repository: Any,
    checked_at: datetime,
) -> dict[str, Any]:
    cron_minutes = route.get("cron_interval_minutes")
    if cron_minutes is None:
        return {"status": "not_checked", "reason": "route_has_no_wake_interval"}
    try:
        wake_interval_seconds = max(60, int(cron_minutes) * 60)
    except (TypeError, ValueError):
        wake_interval_seconds = 300
    candle_interval_seconds = _timeframe_seconds("5m")
    grace_seconds = 90
    max_age_seconds = candle_interval_seconds + wake_interval_seconds + grace_seconds
    raw_required = _requires_candles(requirements, origin="raw", timeframe="5m")
    derived_required = _requires_candles(requirements, origin="derived", timeframe="5m")
    raw_ref = market_data_repository.get_raw_candle_ref(route["asset"], "5m") if raw_required else None
    derived_ref = _get_ref(
        market_data_repository,
        asset=route["asset"],
        timeframe="5m",
        origin="derived",
        data_type="candles",
    ) if derived_required else None
    raw_status = _freshness_ref_status(raw_ref, checked_at=checked_at, max_age_seconds=max_age_seconds)
    derived_status = _freshness_ref_status(derived_ref, checked_at=checked_at, max_age_seconds=max_age_seconds)

    status = "fresh"
    reason = None
    if raw_required and raw_status["status"] == "stale":
        status = "stale"
        reason = "raw_5m_stale"
    elif raw_required and raw_status["status"] == "missing":
        status = "stale"
        reason = "raw_5m_missing"
    elif derived_required and derived_status["status"] == "stale":
        status = "stale"
        reason = "derived_5m_stale"
    elif derived_required and derived_status["status"] == "missing":
        status = "stale"
        reason = "derived_5m_missing"
    elif derived_required and raw_status.get("timestamp") and derived_status.get("timestamp"):
        raw_ts = _coerce_datetime(raw_status["timestamp"])
        derived_ts = _coerce_datetime(derived_status["timestamp"])
        if derived_ts < raw_ts:
            status = "stale"
            reason = "derived_5m_lagging_raw"

    return {
        "status": status,
        "reason": reason,
        "checked_at": _to_iso(checked_at),
        "candle_interval_seconds": candle_interval_seconds,
        "wake_interval_seconds": wake_interval_seconds,
        "grace_seconds": grace_seconds,
        "max_age_seconds": max_age_seconds,
        "raw_5m": raw_status if raw_required else None,
        "derived_5m": derived_status if derived_required else None,
    }


def _retry_stale_raw_5m(
    *,
    route: dict[str, Any],
    requirements: list[dict[str, Any]],
    raw_refs_by_key: dict[tuple[str, str], dict[str, Any]],
    market_data_repository: Any,
    fill_service: Any,
    adapter: Any,
    checked_at: datetime,
) -> dict[str, Any] | None:
    if not _requires_candles(requirements, origin="raw", timeframe="5m"):
        return None
    raw_ref = raw_refs_by_key.get(("candles", "5m")) or market_data_repository.get_raw_candle_ref(route["asset"], "5m")
    if raw_ref is None:
        return None
    return _call_fill_service(
        fill_service=fill_service,
        registration=raw_ref,
        repository=market_data_repository,
        adapter=adapter,
        as_of=checked_at,
    )


def _requires_candles(requirements: list[dict[str, Any]], *, origin: str, timeframe: str) -> bool:
    return any(
        requirement.get("data_type") == "candles"
        and requirement.get("origin") == origin
        and (requirement.get("timeframe") or "5m") == timeframe
        for requirement in requirements
    )


def _freshness_ref_status(ref: dict[str, Any] | None, *, checked_at: datetime, max_age_seconds: int) -> dict[str, Any]:
    if ref is None:
        return {"status": "missing"}
    end_ts = ref.get("end_ts")
    if end_ts is None:
        return {
            "status": "unknown",
            "dataset_id": ref.get("dataset_id"),
            "reason": "missing_end_ts",
        }
    timestamp = _coerce_datetime(end_ts)
    age_seconds = max(0, int((checked_at - timestamp).total_seconds()))
    return {
        "status": "fresh" if age_seconds <= max_age_seconds else "stale",
        "dataset_id": ref.get("dataset_id"),
        "timestamp": _to_iso(timestamp),
        "age_seconds": age_seconds,
    }


def _call_fill_service(
    *,
    fill_service: Any,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime,
) -> dict[str, Any]:
    try:
        return fill_service(
            registration=registration,
            repository=repository,
            adapter=adapter,
            as_of=as_of,
        )
    except TypeError as exc:
        if "as_of" not in str(exc):
            raise
        return fill_service(
            registration=registration,
            repository=repository,
            adapter=adapter,
        )


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)


def _timeframe_seconds(timeframe: str) -> int:
    normalized = str(timeframe).strip().lower()
    if normalized.endswith("m"):
        return int(normalized[:-1]) * 60
    if normalized.endswith("h"):
        return int(normalized[:-1]) * 60 * 60
    if normalized.endswith("d"):
        return int(normalized[:-1]) * 24 * 60 * 60
    return 300


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _blocked(*, route: dict[str, Any], requirements: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "blocked",
        "route_id": route["route_id"],
        "asset": route["asset"],
        "signal_engine_id": route["signal_engine_id"],
        "requirements": requirements,
        "route": route,
    }
