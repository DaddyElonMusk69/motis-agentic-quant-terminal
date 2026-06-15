from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.ingestion.legacy_signals import build_signal_set_key
from quant_terminal_worker.signal_engines.runtime import EngineTrainingContext, resolve_signal_engine


SIGNAL_ENGINE_VERSION = "0.1"


def extend_signal_pool_from_local_candles(
    *,
    workspace_root: Path,
    repository: Any,
    signal_engine_id: str,
    asset: str,
    target_end: str | None = None,
    progress_callback: Any | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root)
    asset = asset.upper()
    signal_set_key = _canonical_signal_set_key(signal_engine_id, asset)
    signal_set = repository.get_signal_set(signal_set_key)
    if signal_set is None:
        raise ValueError(f"Canonical signal pool not found for {signal_engine_id}/{asset}")

    reader = MarketDataReader(repository=repository, workspace_root=root)
    raw_5m = reader.get_candles(asset=asset, timeframe="5m", origin="raw")
    if not raw_5m:
        raise ValueError(f"Raw candle data is empty for {asset}. Update local candle data first.")

    raw_candle_end = raw_5m[-1].timestamp
    requested_target = _parse_timestamp(target_end) if target_end else raw_candle_end
    if requested_target > raw_candle_end:
        raise ValueError(
            f"Raw candle data only covers through {_iso_z(raw_candle_end)}. "
            "Update local candle data first."
        )

    existing_signals = repository.list_signals(signal_set_key=signal_set_key, limit=1_000_000)
    existing_timestamps = {_parse_timestamp(signal["timestamp"]) for signal in existing_signals}
    previous_signal_end = max(existing_timestamps) if existing_timestamps else None
    previous_scan_coverage = _scan_coverage(signal_set)
    previous_scan_end = previous_scan_coverage.get("end_ts")
    scan_start = _scan_start(
        previous_signal_end=previous_signal_end,
        previous_scan_coverage=previous_scan_coverage,
        fallback_start=raw_5m[0].timestamp,
    )

    if previous_scan_end is not None and requested_target <= previous_scan_end:
        return _build_response(
            status="noop",
            signal_engine_id=signal_engine_id,
            asset=asset,
            signal_set=signal_set,
            raw_candle_end=raw_candle_end,
            previous_signal_end=previous_signal_end,
            scan_coverage_end=previous_scan_end,
            final_signal_end=previous_signal_end,
            existing_packet_count=len(existing_signals),
            generated_packet_count=0,
            appended_packet_count=0,
            final_packet_count=len(existing_signals),
            generated_artifact_root=None,
            import_result={"status": "skipped", "reason": "already_scanned_to_target"},
        )

    resolved = resolve_signal_engine(
        signal_engine_id,
        version=signal_set.get("signal_engine_version"),
        repository=repository,
        workspace_root=root,
    )
    parameters = _engine_parameters(signal_set, defaults=_spec_default_parameters(resolved.spec))
    stream_state = {
        "generated_packet_count": 0,
        "appended_packet_count": 0,
        "final_signal_end": previous_signal_end,
    }

    def packet_sink(packets: list[dict[str, Any]]) -> None:
        if not packets:
            return
        stream_state["generated_packet_count"] += len(packets)
        new_packets = [
            packet
            for packet in packets
            if _parse_timestamp(str(packet["timestamp"])) not in existing_timestamps
        ]
        if not new_packets:
            return
        _append_packets_to_signal_set(
            repository=repository,
            signal_set=signal_set,
            signal_set_key=signal_set_key,
            packets=new_packets,
        )
        for packet in new_packets:
            timestamp = _parse_timestamp(str(packet["timestamp"]))
            existing_timestamps.add(timestamp)
            if stream_state["final_signal_end"] is None or timestamp > stream_state["final_signal_end"]:
                stream_state["final_signal_end"] = timestamp
        stream_state["appended_packet_count"] += len(new_packets)
        if callable(progress_callback):
            progress_callback(f"packets {stream_state['appended_packet_count']} appended")

    training_output = resolved.generate_training_signals(
        EngineTrainingContext(
            asset=asset,
            instrument=signal_set.get("instrument") or f"{asset}-USDT-SWAP",
            signal_set=signal_set,
            signal_set_key=signal_set_key,
            parameters=parameters,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=root,
            repository=repository,
            start=scan_start,
            end=requested_target,
            raw_candle_end=raw_candle_end,
            packet_sink=packet_sink,
        )
    )
    generated_packets = list(training_output.packets)

    if generated_packets:
        packet_sink(generated_packets)
    import_result = {
        "status": "imported",
        "signal_engine_id": signal_engine_id,
        "signal_set_key": signal_set_key,
        "signal_sets": 1,
        "signals": stream_state["appended_packet_count"],
        "replace_existing": False,
        "source": "parquet_market_data",
        "mode": "chunked",
    }
    final_packet_count = len(existing_signals) + stream_state["appended_packet_count"]
    final_signal_end = stream_state["final_signal_end"]
    updated_manifest = _updated_manifest(
        signal_set=signal_set,
        target_end=requested_target,
        raw_candle_end=raw_candle_end,
    )
    repository.upsert_signal_set(
        {
            "signal_set_key": signal_set_key,
            "signal_set_id": signal_set["signal_set_id"],
            "signal_engine_id": signal_engine_id,
            "signal_engine_version": signal_set.get("signal_engine_version") or SIGNAL_ENGINE_VERSION,
            "asset": asset,
            "instrument": signal_set.get("instrument") or f"{asset}-USDT-SWAP",
            "start_ts": signal_set.get("start_ts") or (min(existing_timestamps) if existing_timestamps else None),
            "end_ts": final_signal_end,
            "packet_count": final_packet_count,
            "payload_schema": signal_set.get("payload_schema") or "signal_packet.v2",
            "source_path": signal_set.get("source_path") or "canonicalized:signals",
            "manifest": updated_manifest,
        }
    )
    repository.refresh_signal_set_coverage(signal_set_key)
    refreshed = repository.get_signal_set(signal_set_key)
    status = "extended" if stream_state["appended_packet_count"] else "no_new_signals"

    return _build_response(
        status=status,
        signal_engine_id=signal_engine_id,
        asset=asset,
        signal_set=refreshed or signal_set,
        raw_candle_end=raw_candle_end,
        previous_signal_end=previous_signal_end,
        scan_coverage_end=requested_target,
        final_signal_end=final_signal_end,
        existing_packet_count=len(existing_signals),
        generated_packet_count=stream_state["generated_packet_count"] or len(generated_packets),
        appended_packet_count=stream_state["appended_packet_count"],
        final_packet_count=(refreshed or {}).get("packet_count", final_packet_count),
        generated_artifact_root=None,
        import_result=import_result,
    )


def _append_packets_to_signal_set(
    *,
    repository: Any,
    signal_set: dict[str, Any],
    signal_set_key: str,
    packets: list[dict[str, Any]],
) -> dict[str, Any]:
    signal_engine_id = signal_set["signal_engine_id"]
    signal_set_id = signal_set["signal_set_id"]
    asset = signal_set["asset"]
    instrument = signal_set.get("instrument") or f"{asset}-USDT-SWAP"
    version = signal_set.get("signal_engine_version") or SIGNAL_ENGINE_VERSION
    rows = []
    for packet in packets:
        timestamp = _parse_timestamp(str(packet["timestamp"]))
        rows.append(
            {
                "signal_id": _build_signal_id(
                    signal_engine_id=signal_engine_id,
                    asset=asset,
                    signal_set_id=signal_set_id,
                    timestamp=timestamp,
                ),
                "signal_set_key": signal_set_key,
                "signal_engine_id": signal_engine_id,
                "signal_engine_version": version,
                "asset": asset,
                "instrument": instrument,
                "timestamp": packet["timestamp"],
                "data_refs": _packet_data_refs(signal_set),
                "payload_schema": packet.get("schema_version", "signal_packet.v2"),
                "payload": packet,
            }
        )
    bulk_upsert = getattr(repository, "upsert_signals", None)
    if callable(bulk_upsert):
        bulk_upsert(rows)
    else:
        for row in rows:
            repository.upsert_signal(row)
    return {
        "status": "imported",
        "signal_engine_id": signal_engine_id,
        "signal_set_key": signal_set_key,
        "signal_sets": 1,
        "signals": len(packets),
        "replace_existing": False,
        "source": "parquet_market_data",
    }

def _canonical_signal_set_key(signal_engine_id: str, asset: str) -> str:
    return build_signal_set_key(signal_engine_id, asset, f"{asset}-{signal_engine_id}-canonical")


def _engine_parameters(signal_set: dict[str, Any], *, defaults: dict[str, Any] | None = None) -> dict[str, Any]:
    manifest = signal_set.get("manifest") if isinstance(signal_set.get("manifest"), dict) else {}
    parameters = manifest.get("parameters") if isinstance(manifest.get("parameters"), dict) else {}
    base_defaults = {
        "timeframes": ["2h", "4h", "8h", "12h", "1d"],
        "context_bars": 80,
        "vote_threshold": 2,
        "proximity_threshold": "0.002",
        "dedupe_window_minutes": 120,
    }
    return {**base_defaults, **(defaults or {}), **parameters}


def _spec_default_parameters(spec: Any) -> dict[str, Any]:
    configuration_schema = spec.configuration_schema if isinstance(spec.configuration_schema, dict) else {}
    defaults = configuration_schema.get("default_parameters")
    return dict(defaults) if isinstance(defaults, dict) else {}


def _packet_data_refs(signal_set: dict[str, Any]) -> list[str]:
    manifest = signal_set.get("manifest") if isinstance(signal_set.get("manifest"), dict) else {}
    data_manifest = manifest.get("data_manifest")
    return [data_manifest] if data_manifest else []


def _updated_manifest(
    *,
    signal_set: dict[str, Any],
    target_end: datetime,
    raw_candle_end: datetime,
) -> dict[str, Any]:
    manifest = dict(signal_set.get("manifest") or {})
    scan_coverage = manifest.get("scan_coverage") if isinstance(manifest.get("scan_coverage"), dict) else {}
    start_ts = scan_coverage.get("start_ts") or _optional_iso(signal_set.get("start_ts"))
    manifest["scan_coverage"] = {
        "start_ts": start_ts,
        "end_ts": _iso_z(target_end),
        "source": "parquet_market_data",
        "raw_candle_end_ts": _iso_z(raw_candle_end),
    }
    return manifest


def _scan_coverage(signal_set: dict[str, Any]) -> dict[str, Any]:
    manifest = signal_set.get("manifest") if isinstance(signal_set.get("manifest"), dict) else {}
    scan_coverage = manifest.get("scan_coverage") if isinstance(manifest.get("scan_coverage"), dict) else {}
    value = scan_coverage.get("end_ts")
    return {
        "end_ts": _parse_timestamp(value) if value else None,
        "source": scan_coverage.get("source"),
    }


def _scan_start(
    *,
    previous_signal_end: datetime | None,
    previous_scan_coverage: dict[str, Any],
    fallback_start: datetime,
) -> datetime:
    previous_scan_end = previous_scan_coverage.get("end_ts")
    if (
        previous_scan_coverage.get("source") == "parquet_market_data"
        and isinstance(previous_scan_end, datetime)
    ):
        return previous_scan_end + timedelta(minutes=5)
    return previous_signal_end or fallback_start


def _final_signal_end(
    *,
    previous_signal_end: datetime | None,
    packets: list[dict[str, Any]],
) -> datetime | None:
    packet_end = max((_parse_timestamp(str(packet["timestamp"])) for packet in packets), default=None)
    if previous_signal_end is None:
        return packet_end
    if packet_end is None:
        return previous_signal_end
    return max(previous_signal_end, packet_end)


def _build_response(
    *,
    status: str,
    signal_engine_id: str,
    asset: str,
    signal_set: dict[str, Any],
    raw_candle_end: datetime,
    previous_signal_end: datetime | None,
    scan_coverage_end: datetime,
    final_signal_end: datetime | None,
    existing_packet_count: int,
    generated_packet_count: int,
    appended_packet_count: int,
    final_packet_count: int,
    generated_artifact_root: str | None,
    import_result: dict[str, Any],
) -> dict[str, Any]:
    return {
        "status": status,
        "signal_engine_id": signal_engine_id,
        "asset": asset,
        "signal_set_key": signal_set["signal_set_key"],
        "signal_set_id": signal_set["signal_set_id"],
        "raw_candle_end_ts": _iso_z(raw_candle_end),
        "previous_signal_end_ts": _optional_iso(previous_signal_end),
        "scan_coverage_end_ts": _iso_z(scan_coverage_end),
        "final_signal_end_ts": _optional_iso(final_signal_end),
        "target_end_ts": _iso_z(scan_coverage_end),
        "coverage_end_ts": _iso_z(scan_coverage_end),
        "previous_end_ts": _optional_iso(previous_signal_end),
        "final_end_ts": _optional_iso(final_signal_end),
        "existing_packet_count": existing_packet_count,
        "generated_packet_count": generated_packet_count,
        "appended_packet_count": appended_packet_count,
        "final_packet_count": final_packet_count,
        "generated_artifact_root": generated_artifact_root,
        "local_only": True,
        "source": "parquet_market_data",
        "import_result": import_result,
    }


def _build_signal_id(
    *,
    signal_engine_id: str,
    asset: str,
    signal_set_id: str,
    timestamp: datetime,
) -> str:
    return f"{signal_engine_id}:{asset}:{signal_set_id}:{timestamp.strftime('%Y%m%dT%H%M%SZ')}"


def _parse_timestamp(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _optional_iso(value: str | datetime | None) -> str | None:
    return _iso_z(_parse_timestamp(value)) if value is not None else None


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
