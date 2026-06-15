from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.signal_engines.runtime import EngineLiveScanContext, resolve_signal_engine


def scan_latest_live_signal(
    *,
    route: dict[str, Any],
    repository: Any,
    workspace_root: Path,
) -> dict[str, Any] | None:
    signal_engine_id = route["signal_engine_id"]
    asset = route["asset"].upper()
    instrument = route.get("instrument") or f"{asset}-USDT-SWAP"
    resolved = resolve_signal_engine(
        signal_engine_id,
        version=route.get("signal_engine_version"),
        repository=repository,
        workspace_root=workspace_root,
    )
    result = resolved.scan_live_signal(
        EngineLiveScanContext(
            asset=asset,
            instrument=instrument,
            route=route,
            parameters={**_spec_default_parameters(resolved.spec), **_engine_parameters(route)},
            market_data_reader=MarketDataReader(repository=repository, workspace_root=workspace_root),
            spec=resolved.spec,
            workspace_root=workspace_root,
            repository=repository,
        )
    )
    if result.status == "no_fresh_signal":
        return None
    if result.status == "blocked":
        raise ValueError(result.reason or "live signal scan blocked")
    if result.signal is None:
        raise ValueError("fresh live signal scan result is missing signal")
    payload = _restore_packet_audit_fields(result.signal.to_mapping())
    timestamp = _parse_timestamp(str(payload["timestamp"]))
    return {
        "signal_id": _build_live_signal_id(route=route, timestamp=timestamp),
        "signal_set_key": None,
        "signal_engine_id": resolved.spec.signal_engine_id,
        "signal_engine_version": resolved.spec.version,
        "asset": asset,
        "instrument": instrument,
        "timestamp": _iso_z(timestamp),
        "data_refs": [],
        "payload_schema": payload.get("schema_version", "signal_packet.v2"),
        "payload": payload,
    }


def _engine_parameters(route: dict[str, Any]) -> dict[str, Any]:
    bundle = route.get("active_bundle") if isinstance(route.get("active_bundle"), dict) else {}
    setup = bundle.get("execution_setup") if isinstance(bundle.get("execution_setup"), dict) else {}
    engine_parameters = setup.get("engine_parameters") if isinstance(setup.get("engine_parameters"), dict) else {}
    return dict(engine_parameters)


def _restore_packet_audit_fields(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    restored = dict(payload)
    for key in ("interactions", "charts", "features"):
        if key in evidence and key not in restored:
            restored[key] = evidence[key]
    return restored


def _spec_default_parameters(spec: Any) -> dict[str, Any]:
    configuration_schema = spec.configuration_schema if isinstance(spec.configuration_schema, dict) else {}
    defaults = configuration_schema.get("default_parameters")
    return dict(defaults) if isinstance(defaults, dict) else {}


def _build_live_signal_id(*, route: dict[str, Any], timestamp: datetime) -> str:
    return f"{route['signal_engine_id']}:{route['asset'].upper()}:live:{timestamp.strftime('%Y%m%dT%H%M%SZ')}"


def _parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
