from __future__ import annotations

from bisect import bisect_left, insort
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.ingestion.legacy_signals import build_signal_set_key
from quant_terminal_worker.signal_engines.runtime import (
    EngineTrainingContext,
    apply_fixed_engine_parameters,
    resolve_signal_engine,
)


SIGNAL_ENGINE_VERSION = "0.1"
DEFAULT_MULTI_SOURCE_EXTENSION_OVERLAP_MINUTES = 24 * 60


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
    raw_ref = repository.get_candle_ref(asset=asset, timeframe="5m", origin="raw", data_type="candles")
    if raw_ref is None:
        raise ValueError(f"Raw candle data is missing for {asset}. Update local candle data first.")
    if not raw_ref.get("end_ts"):
        raise ValueError(f"Raw candle coverage end is missing for {asset}. Update local candle data first.")

    raw_candle_end = _parse_timestamp(raw_ref["end_ts"])
    raw_candle_start = _parse_timestamp(raw_ref.get("start_ts") or signal_set.get("start_ts") or raw_candle_end)
    requested_target = _parse_timestamp(target_end) if target_end else raw_candle_end
    if requested_target > raw_candle_end:
        raise ValueError(
            f"Raw candle data only covers through {_iso_z(raw_candle_end)}. "
            "Update local candle data first."
        )

    previous_signal_end = _parse_timestamp(signal_set["end_ts"]) if signal_set.get("end_ts") else None
    previous_scan_coverage = _scan_coverage(signal_set)
    previous_scan_end = previous_scan_coverage.get("end_ts")

    resolved = resolve_signal_engine(
        signal_engine_id,
        version=signal_set.get("signal_engine_version"),
        repository=repository,
        workspace_root=root,
    )
    parameters = apply_fixed_engine_parameters(
        resolved.spec,
        _engine_parameters(signal_set, defaults=_spec_default_parameters(resolved.spec)),
    )
    overlap = _parquet_extension_overlap(spec=resolved.spec, parameters=parameters)
    scan_start = _scan_start(
        previous_signal_end=previous_signal_end,
        previous_scan_coverage=previous_scan_coverage,
        fallback_start=raw_candle_start,
        overlap=overlap,
    )
    dedupe_window = timedelta(minutes=int(parameters.get("dedupe_window_minutes", 120)))
    existing_signals = _recent_signal_rows(
        repository=repository,
        signal_set_key=signal_set_key,
        scan_start=scan_start,
        target_end=requested_target,
        dedupe_window=dedupe_window,
    )
    existing_timestamps = {_parse_timestamp(signal["timestamp"]) for signal in existing_signals}
    admitted_timestamps = sorted(existing_timestamps)

    if previous_scan_end is not None and requested_target <= previous_scan_end and (
        overlap <= timedelta(0) or scan_start > requested_target
    ):
        return _build_response(
            status="noop",
            signal_engine_id=signal_engine_id,
            asset=asset,
            signal_set=signal_set,
            raw_candle_end=raw_candle_end,
            previous_signal_end=previous_signal_end,
            scan_coverage_end=previous_scan_end,
            final_signal_end=previous_signal_end,
            existing_packet_count=int(signal_set.get("packet_count") or 0),
            generated_packet_count=0,
            appended_packet_count=0,
            final_packet_count=int(signal_set.get("packet_count") or 0),
            generated_artifact_root=None,
            import_result={"status": "skipped", "reason": "already_scanned_to_target"},
        )

    generation_parameters = dict(parameters)
    dedupe_seed = _latest_admitted_before(admitted_timestamps, scan_start)
    if dedupe_seed is not None:
        generation_parameters["_dedupe_seed_timestamp"] = _iso_z(dedupe_seed)
    stream_state = {
        "generated_packet_count": 0,
        "appended_packet_count": 0,
        "final_signal_end": previous_signal_end,
    }

    def packet_sink(packets: list[dict[str, Any]]) -> None:
        if not packets:
            return
        stream_state["generated_packet_count"] += len(packets)
        new_packets = []
        for packet in sorted(packets, key=lambda item: _parse_timestamp(str(item["timestamp"]))):
            timestamp = _parse_timestamp(str(packet["timestamp"]))
            if timestamp in existing_timestamps:
                continue
            if _has_dedupe_conflict(timestamp, admitted_timestamps, dedupe_window):
                continue
            new_packets.append(packet)
            insort(admitted_timestamps, timestamp)
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
            parameters=generation_parameters,
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
    training_result = getattr(training_output, "result", None)
    reported_coverage = getattr(training_result, "scan_coverage_end_ts", None)
    if reported_coverage:
        scan_coverage_end = min(requested_target, _parse_timestamp(reported_coverage))
    elif isinstance(previous_scan_end, datetime):
        scan_coverage_end = previous_scan_end
    else:
        scan_coverage_end = requested_target
    generated_packets = list(training_output.packets)

    if generated_packets:
        packet_sink(generated_packets)
    existing_packet_count = int(signal_set.get("packet_count") or 0)
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
    final_packet_count = existing_packet_count + stream_state["appended_packet_count"]
    final_signal_end = stream_state["final_signal_end"]
    updated_manifest = _updated_manifest(
        signal_set=signal_set,
        target_end=scan_coverage_end,
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
        scan_coverage_end=scan_coverage_end,
        final_signal_end=final_signal_end,
        existing_packet_count=existing_packet_count,
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


def _parquet_extension_overlap(*, spec: Any, parameters: dict[str, Any]) -> timedelta:
    configured = parameters.get("parquet_extension_overlap_minutes")
    if configured not in (None, ""):
        return timedelta(minutes=max(0, int(configured)))
    required_data = getattr(spec, "required_data", None)
    if any(isinstance(item, dict) and item.get("data_type") != "candles" for item in required_data or []):
        return timedelta(minutes=DEFAULT_MULTI_SOURCE_EXTENSION_OVERLAP_MINUTES)
    return timedelta(0)


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
    overlap: timedelta = timedelta(0),
) -> datetime:
    previous_scan_end = previous_scan_coverage.get("end_ts")
    if (
        previous_scan_coverage.get("source") == "parquet_market_data"
        and isinstance(previous_scan_end, datetime)
    ):
        if overlap > timedelta(0):
            return max(fallback_start, previous_scan_end - overlap + timedelta(minutes=5))
        return previous_scan_end + timedelta(minutes=5)
    return previous_signal_end or fallback_start


def _recent_signal_rows(
    *,
    repository: Any,
    signal_set_key: str,
    scan_start: datetime,
    target_end: datetime,
    dedupe_window: timedelta,
) -> list[dict[str, Any]]:
    getter = getattr(repository, "list_signals_for_signal_set_window", None)
    if callable(getter):
        window_start = scan_start - dedupe_window if dedupe_window > timedelta(0) else scan_start
        return getter(
            signal_set_key=signal_set_key,
            window_start=_iso_z(window_start),
            window_end=_iso_z(target_end),
        )
    fallback = getattr(repository, "list_signals", None)
    if not callable(fallback):
        return []
    rows = fallback(signal_set_key=signal_set_key, limit=10_000, descending=True)
    return [
        row
        for row in rows
        if scan_start - dedupe_window <= _parse_timestamp(row["timestamp"]) <= target_end
    ]


def _latest_admitted_before(timestamps: list[datetime], timestamp: datetime) -> datetime | None:
    index = bisect_left(timestamps, timestamp)
    if index <= 0:
        return None
    return timestamps[index - 1]


def _has_dedupe_conflict(timestamp: datetime, admitted_timestamps: list[datetime], dedupe_window: timedelta) -> bool:
    if dedupe_window <= timedelta(0):
        return False
    index = bisect_left(admitted_timestamps, timestamp)
    for neighbor_index in (index - 1, index):
        if 0 <= neighbor_index < len(admitted_timestamps):
            if abs(timestamp - admitted_timestamps[neighbor_index]) < dedupe_window:
                return True
    return False


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
