from __future__ import annotations

import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataCandle, MarketDataReader
from quant_terminal_worker.ingestion.legacy_signals import build_signal_set_key


DEFAULT_TIMEFRAMES = ("2h", "4h", "8h", "12h", "1d")
SIGNAL_ENGINE_VERSION = "0.1"


def extend_signal_pool_from_local_candles(
    *,
    workspace_root: Path,
    repository: Any,
    signal_engine_id: str,
    asset: str,
    target_end: str | None = None,
) -> dict[str, Any]:
    root = Path(workspace_root)
    asset = asset.upper()
    signal_set_key = _canonical_signal_set_key(signal_engine_id, asset)
    signal_set = repository.get_signal_set(signal_set_key)
    if signal_set is None:
        raise ValueError(f"Canonical signal pool not found for {signal_engine_id}/{asset}")
    if signal_engine_id != "vegas_ema":
        raise ValueError(f"Parquet signal extension is not implemented for {signal_engine_id}")

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

    derived = {
        timeframe: reader.get_candles(asset=asset, timeframe=timeframe, origin="derived")
        for timeframe in DEFAULT_TIMEFRAMES
    }
    parameters = _engine_parameters(signal_set)
    generated_packets = _generate_vegas_packets(
        workspace_root=root,
        asset=asset,
        raw_5m=raw_5m,
        derived=derived,
        start=scan_start,
        end=requested_target,
        context_bars=int(parameters.get("context_bars", 80)),
        proximity_threshold=Decimal(str(parameters.get("proximity_threshold", "0.002"))),
        vote_threshold=int(parameters.get("vote_threshold", 2)),
        window_minutes=int(parameters.get("dedupe_window_minutes", 120)),
    )

    new_packets = [
        packet
        for packet in generated_packets
        if _parse_timestamp(str(packet["timestamp"])) not in existing_timestamps
    ]
    import_result = _append_packets_to_signal_set(
        repository=repository,
        signal_set=signal_set,
        signal_set_key=signal_set_key,
        packets=new_packets,
    )
    final_packet_count = len(existing_signals) + import_result["signals"]
    final_signal_end = _final_signal_end(
        previous_signal_end=previous_signal_end,
        packets=new_packets,
    )
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
    status = "extended" if new_packets else "no_new_signals"

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
        generated_packet_count=len(generated_packets),
        appended_packet_count=len(new_packets),
        final_packet_count=(refreshed or {}).get("packet_count", final_packet_count),
        generated_artifact_root=None,
        import_result=import_result,
    )


def _generate_vegas_packets(
    *,
    workspace_root: Path,
    asset: str,
    raw_5m: list[MarketDataCandle],
    derived: dict[str, list[MarketDataCandle]],
    start: datetime,
    end: datetime,
    context_bars: int,
    proximity_threshold: Decimal,
    vote_threshold: int,
    window_minutes: int,
) -> list[dict[str, Any]]:
    _ensure_vegas_path(workspace_root)
    from vegas.replay_provider import ReplayMarketStateProvider
    from vegas.signal_engine import UniversalVegasSignalEngine

    provider = ReplayMarketStateProvider(
        asset=asset,
        raw_5m=[_to_vegas_candle(candle) for candle in raw_5m],
        derived_candles={
            timeframe: [_to_vegas_candle(candle) for candle in candles]
            for timeframe, candles in derived.items()
        },
        context_bars=context_bars,
    )
    engine = UniversalVegasSignalEngine(
        proximity_threshold=proximity_threshold,
        vote_threshold=vote_threshold,
    )
    window = timedelta(minutes=window_minutes)
    packets: list[dict[str, Any]] = []
    last_emitted_at: datetime | None = None

    for candle in provider.raw_5m:
        if candle.ts < start:
            continue
        if candle.ts > end:
            break
        try:
            snapshot = provider.snapshot_at(candle.ts)
        except ValueError as error:
            if "Not enough completed" not in str(error):
                raise
            continue
        packet = engine.scan(snapshot)
        if packet is None:
            continue
        if last_emitted_at is not None and (candle.ts - last_emitted_at) < window:
            continue
        last_emitted_at = candle.ts
        packets.append(packet.to_dict())

    return packets


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
    for packet in packets:
        timestamp = _parse_timestamp(str(packet["timestamp"]))
        repository.upsert_signal(
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
    return {
        "status": "imported",
        "signal_engine_id": signal_engine_id,
        "signal_set_key": signal_set_key,
        "signal_sets": 1,
        "signals": len(packets),
        "replace_existing": False,
        "source": "parquet_market_data",
    }


def _to_vegas_candle(candle: MarketDataCandle) -> Any:
    from vegas.schemas import Candle

    return Candle(
        ts=candle.timestamp,
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
        vol_ccy=candle.vol_ccy,
        vol_ccy_quote=candle.vol_ccy_quote,
        confirm=candle.confirm,
    )


def _ensure_vegas_path(root: Path) -> None:
    src = root / "artifacts" / "signal_engine" / "src"
    if not src.exists():
        raise ValueError(f"Vegas signal engine source is missing: {src}")
    if str(src) not in sys.path:
        sys.path.insert(0, str(src))


def _canonical_signal_set_key(signal_engine_id: str, asset: str) -> str:
    return build_signal_set_key(signal_engine_id, asset, f"{asset}-{signal_engine_id}-canonical")


def _engine_parameters(signal_set: dict[str, Any]) -> dict[str, Any]:
    manifest = signal_set.get("manifest") if isinstance(signal_set.get("manifest"), dict) else {}
    parameters = manifest.get("parameters") if isinstance(manifest.get("parameters"), dict) else {}
    defaults = {
        "timeframes": list(DEFAULT_TIMEFRAMES),
        "context_bars": 80,
        "vote_threshold": 2,
        "proximity_threshold": "0.002",
        "dedupe_window_minutes": 120,
    }
    return {**defaults, **parameters}


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
