from __future__ import annotations

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

    requirement_results: list[dict[str, Any]] = []
    raw_results_by_key: dict[tuple[str, str], dict[str, Any]] = {}
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

        fill_result = fill_service(
            registration=raw_ref,
            repository=market_data_repository,
            adapter=adapter,
        )
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


def _blocked(*, route: dict[str, Any], requirements: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "status": "blocked",
        "route_id": route["route_id"],
        "asset": route["asset"],
        "signal_engine_id": route["signal_engine_id"],
        "requirements": requirements,
        "route": route,
    }
