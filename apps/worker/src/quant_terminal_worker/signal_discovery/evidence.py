from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq


EVIDENCE_MANIFEST_SCHEMA = "signal_discovery_evidence_manifest.v1"
_GOOD_QUALITY_STATES = {
    "complete",
    "derived",
    "ema_enriched",
    "feature_enriched",
    "ingested",
    "ok",
    "ready",
    "updated",
    "valid",
}


def resolve_primary_label_ref(
    *,
    refs: Sequence[Mapping[str, Any]],
    asset: str,
    instrument: str,
    research_start: datetime,
    walk_forward_end: datetime,
    horizon_hours: Sequence[int | float],
    entry_delays_minutes: Sequence[int],
    preferred_dataset_id: str | None = None,
) -> dict[str, Any]:
    required_start = _as_utc(research_start)
    required_end = _as_utc(walk_forward_end) + timedelta(
        hours=max(float(value) for value in horizon_hours),
        minutes=max(int(value) for value in entry_delays_minutes),
    )
    candidates = [
        dict(ref)
        for ref in refs
        if str(ref.get("asset") or "").upper() == asset.upper()
        and str(ref.get("instrument") or "") == instrument
        and ref.get("storage_backend") == "parquet"
        and ref.get("data_type") == "candles"
        and ref.get("data_origin") == "raw"
        and ref.get("timeframe") == "5m"
        and _optional_timestamp(ref.get("start_ts")) is not None
        and _optional_timestamp(ref.get("end_ts")) is not None
        and _coerce_timestamp(ref["start_ts"]) <= required_start
        and _coerce_timestamp(ref["end_ts"]) >= required_end
    ]
    if preferred_dataset_id:
        preferred = next(
            (ref for ref in candidates if str(ref.get("dataset_id") or "") == preferred_dataset_id),
            None,
        )
        if preferred is not None:
            return preferred
    if not candidates:
        raise ValueError(
            "No canonical raw 5m candle dataset fully covers the research and "
            "walk-forward windows plus the configured forward horizon."
        )
    candidates.sort(
        key=lambda ref: (
            -int(ref.get("row_count") or 0),
            _coerce_timestamp(ref["start_ts"]),
            -_coerce_timestamp(ref["end_ts"]).timestamp(),
            str(ref.get("dataset_id") or ""),
        )
    )
    return candidates[0]


def select_baseline_oi_ref(
    *,
    refs: Sequence[Mapping[str, Any]],
    asset: str,
    research_end: datetime,
) -> dict[str, Any] | None:
    cutoff = _as_utc(research_end)
    candidates = [
        dict(ref)
        for ref in refs
        if str(ref.get("asset") or "").upper() == asset.upper()
        and ref.get("storage_backend") == "parquet"
        and ref.get("data_type") == "open_interest"
        and ref.get("data_origin") == "raw"
        and _optional_timestamp(ref.get("start_ts")) is not None
        and _coerce_timestamp(ref["start_ts"]) <= cutoff
    ]
    if not candidates:
        return None
    candidates.sort(
        key=lambda ref: (
            _timeframe_minutes(ref.get("timeframe")),
            -int(ref.get("row_count") or 0),
            str(ref.get("dataset_id") or ""),
        )
    )
    return candidates[0]


def build_evidence_manifest(
    *,
    workspace_root: str | Path,
    artifact_root: str | Path,
    session_id: str,
    asset: str,
    instrument: str,
    primary_dataset_id: str,
    research_start: datetime,
    research_end: datetime,
    refs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    workspace = Path(workspace_root).resolve()
    cutoff = _as_utc(research_end)
    research_start_utc = _as_utc(research_start)
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []

    for raw_ref in sorted(refs, key=lambda value: str(value.get("dataset_id") or "")):
        if str(raw_ref.get("asset") or "").upper() != asset.upper():
            continue
        dataset_id = str(raw_ref.get("dataset_id") or "")
        if raw_ref.get("storage_backend") != "parquet":
            excluded.append({"dataset_id": dataset_id, "reason": "storage_backend_not_parquet"})
            continue
        try:
            entry = _inventory_dataset(
                workspace_root=workspace,
                ref=raw_ref,
                research_start=research_start_utc,
                cutoff=cutoff,
            )
        except (KeyError, TypeError, ValueError, OSError, pa.ArrowException) as exc:
            excluded.append({"dataset_id": dataset_id, "reason": f"unreadable_parquet:{exc}"})
            continue
        if entry is None:
            excluded.append({"dataset_id": dataset_id, "reason": "no_parquet_shards"})
            continue
        included.append(entry)

    included_ids = {row["dataset_id"] for row in included}
    if primary_dataset_id not in included_ids:
        reason = next(
            (row["reason"] for row in excluded if row["dataset_id"] == primary_dataset_id),
            "not_registered_for_asset",
        )
        raise ValueError(f"primary label dataset is unavailable: {reason}")

    baseline_oi = select_baseline_oi_ref(
        refs=[ref for ref in refs if str(ref.get("dataset_id") or "") in included_ids],
        asset=asset,
        research_end=cutoff,
    )
    payload: dict[str, Any] = {
        "schema_version": EVIDENCE_MANIFEST_SCHEMA,
        "session_id": session_id,
        "asset": asset.upper(),
        "instrument": instrument,
        "primary_label_dataset_id": primary_dataset_id,
        "baseline_oi_dataset_id": (
            str(baseline_oi["dataset_id"]) if baseline_oi is not None else None
        ),
        "authorized_start": None,
        "authorized_end": _iso_z(cutoff),
        "warmup_policy": "all_available_history",
        "included_dataset_count": len(included),
        "excluded_dataset_count": len(excluded),
        "warning_count": sum(len(row["warnings"]) for row in included),
        "data_types": sorted({str(row["data_type"]) for row in included}),
        "timeframes": sorted({str(row["timeframe"]) for row in included if row.get("timeframe")}),
        "included_datasets": included,
        "excluded_datasets": excluded,
    }
    payload["manifest_hash"] = _payload_hash(payload)
    _write_json(Path(artifact_root) / "evidence" / "evidence_manifest.json", payload)
    return payload


def validate_evidence_manifest(
    *,
    workspace_root: str | Path,
    artifact_root: str | Path,
) -> dict[str, Any]:
    path = Path(artifact_root) / "evidence" / "evidence_manifest.json"
    if not path.is_file():
        raise ValueError("signal discovery evidence manifest does not exist")
    value = json.loads(path.read_text())
    if not isinstance(value, dict) or value.get("schema_version") != EVIDENCE_MANIFEST_SCHEMA:
        raise ValueError("invalid signal discovery evidence manifest")
    expected_hash = str(value.get("manifest_hash") or "")
    if (
        _payload_hash({key: item for key, item in value.items() if key != "manifest_hash"})
        != expected_hash
    ):
        raise ValueError("signal discovery evidence manifest hash mismatch")
    workspace = Path(workspace_root).resolve()
    cutoff = _coerce_timestamp(value["authorized_end"])
    for dataset in value.get("included_datasets") or []:
        expected_shards = {
            str(shard["path"]): shard for shard in dataset.get("parquet_shards") or []
        }
        storage_root = _resolve_path(workspace, dataset["storage_uri"])
        current_shards: dict[str, str] = {}
        for source_path in sorted(storage_root.glob("year=*/month=*/data.parquet")):
            timestamps, authorized_hash, _ = _authorized_shard(
                source_path,
                cutoff=cutoff,
            )
            if timestamps:
                current_shards[_portable_path(workspace, source_path)] = authorized_hash
        if set(current_shards) != set(expected_shards):
            raise ValueError(
                f"signal discovery evidence source drift detected: {dataset.get('dataset_id')}"
            )
        for shard_path, shard in expected_shards.items():
            if current_shards.get(shard_path) != shard.get("sha256"):
                raise ValueError(
                    f"signal discovery evidence source drift detected: {dataset.get('dataset_id')}"
                )
    return value


def _inventory_dataset(
    *,
    workspace_root: Path,
    ref: Mapping[str, Any],
    research_start: datetime,
    cutoff: datetime,
) -> dict[str, Any] | None:
    storage_uri = str(ref["storage_uri"])
    storage_root = _resolve_path(workspace_root, storage_uri)
    files = sorted(storage_root.glob("year=*/month=*/data.parquet"))
    if not files:
        return None
    shards: list[dict[str, Any]] = []
    authorized_timestamps: list[datetime] = []
    schema_columns: list[str] = []
    for file in files:
        eligible, authorized_hash, schema = _authorized_shard(file, cutoff=cutoff)
        if not schema_columns:
            schema_columns = list(schema.names)
        if not eligible:
            continue
        authorized_timestamps.extend(eligible)
        shards.append(
            {
                "path": _portable_path(workspace_root, file),
                "authorized_row_count": len(eligible),
                "first_authorized_ts": _iso_z(min(eligible)),
                "last_authorized_ts": _iso_z(max(eligible)),
                "size_bytes": file.stat().st_size,
                "sha256": authorized_hash,
            }
        )
    if not shards:
        return None
    first_ts = min(authorized_timestamps)
    last_ts = max(authorized_timestamps)
    warnings: list[str] = []
    registered_start = _optional_timestamp(ref.get("start_ts"))
    registered_end = _optional_timestamp(ref.get("end_ts"))
    if (
        first_ts > research_start
        or last_ts < cutoff
        or (registered_start is not None and registered_start > research_start)
        or (registered_end is not None and registered_end < cutoff)
    ):
        warnings.append("partial_research_coverage")
    quality_status = str(ref.get("quality_status") or "unknown")
    if quality_status.lower() not in _GOOD_QUALITY_STATES:
        warnings.append(f"quality_status:{quality_status}")
    return {
        "dataset_id": str(ref["dataset_id"]),
        "source_id": str(ref.get("source_id") or ""),
        "asset": str(ref.get("asset") or "").upper(),
        "instrument": str(ref.get("instrument") or ""),
        "data_type": str(ref.get("data_type") or ""),
        "timeframe": ref.get("timeframe"),
        "data_origin": str(ref.get("data_origin") or ""),
        "storage_backend": "parquet",
        "storage_uri": storage_uri,
        "ingestion_version": str(ref.get("ingestion_version") or ""),
        "quality_status": quality_status,
        "registered_start": _iso_optional(ref.get("start_ts")),
        "registered_end": _iso_optional(ref.get("end_ts")),
        "registered_row_count": ref.get("row_count"),
        "authorized_start": _iso_z(first_ts),
        "authorized_end": _iso_z(cutoff),
        "last_authorized_ts": _iso_z(last_ts),
        "authorized_row_count": len(authorized_timestamps),
        "schema_descriptor": _json_safe(ref.get("schema_descriptor") or {}),
        "schema_columns": schema_columns,
        "warnings": warnings,
        "parquet_shards": shards,
    }


def _timeframe_minutes(value: Any) -> int:
    text = str(value or "")
    if text.endswith("m") and text[:-1].isdigit():
        return int(text[:-1])
    if text.endswith("h") and text[:-1].isdigit():
        return int(text[:-1]) * 60
    if text.endswith("d") and text[:-1].isdigit():
        return int(text[:-1]) * 1440
    return 10**9


def _resolve_path(workspace_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else workspace_root / path


def _portable_path(workspace_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(workspace_root))
    except ValueError:
        return str(path.resolve())


def _authorized_shard(
    path: Path,
    *,
    cutoff: datetime,
) -> tuple[list[datetime], str, pa.Schema]:
    table = pq.ParquetFile(path).read()
    timestamp_column = (
        "timestamp"
        if "timestamp" in table.column_names
        else ("ts" if "ts" in table.column_names else None)
    )
    if timestamp_column is None:
        raise ValueError("missing timestamp or ts column")
    eligible_indices: list[int] = []
    timestamps: list[datetime] = []
    for index, raw_timestamp in enumerate(table.column(timestamp_column).to_pylist()):
        if raw_timestamp is None:
            continue
        timestamp = _coerce_timestamp(raw_timestamp)
        if timestamp <= cutoff:
            eligible_indices.append(index)
            timestamps.append(timestamp)
    authorized = table.take(pa.array(eligible_indices, type=pa.int64()))
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, authorized.schema) as writer:
        writer.write_table(authorized)
    digest = hashlib.sha256()
    digest.update(sink.getvalue().to_pybytes())
    return timestamps, digest.hexdigest(), table.schema


def _payload_hash(value: Mapping[str, Any]) -> str:
    normalized = _json_safe(value)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f"{path.name}.tmp")
    try:
        temp.write_text(
            json.dumps(_json_safe(value), indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        )
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, datetime):
        return _iso_z(value)
    if isinstance(value, Path):
        return str(value)
    return value


def _optional_timestamp(value: Any) -> datetime | None:
    return _coerce_timestamp(value) if value is not None else None


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        return _as_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    raise ValueError(f"invalid timestamp: {value!r}")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _iso_z(value: datetime) -> str:
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def _iso_optional(value: Any) -> str | None:
    return _iso_z(_coerce_timestamp(value)) if value is not None else None
