from __future__ import annotations

import hashlib
import json
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

import pyarrow as pa
import pyarrow.parquet as pq

_SESSION_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def discovery_artifact_root(*, workspace_root: str | Path, session_id: str) -> Path:
    if not _SESSION_ID_PATTERN.fullmatch(session_id):
        raise ValueError("session_id must be a path-safe identifier")
    return Path(workspace_root).resolve() / "dev" / "signal_discovery_sessions" / session_id


def materialize_training_atlas(
    *,
    artifact_root: str | Path,
    timestamp_labels: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    features: Sequence[Mapping[str, Any]],
    hard_negatives: Sequence[Mapping[str, Any]],
    r_feasibility: Mapping[str, Any],
) -> dict[str, Path]:
    root = Path(artifact_root)
    atlas_root = root / "atlas"
    paths = {
        "training_timestamp_labels": atlas_root / "training_timestamp_labels.parquet",
        "training_episodes": atlas_root / "training_episodes.parquet",
        "training_features": atlas_root / "training_features.parquet",
        "training_hard_negatives": atlas_root / "training_hard_negatives.parquet",
        "r_feasibility": atlas_root / "r_feasibility.json",
    }
    _atomic_write_parquet(paths["training_timestamp_labels"], timestamp_labels)
    _atomic_write_parquet(paths["training_episodes"], episodes)
    _atomic_write_parquet(paths["training_features"], features)
    _atomic_write_parquet(paths["training_hard_negatives"], hard_negatives)
    _atomic_write_json(paths["r_feasibility"], r_feasibility)
    return paths


def write_session_manifest(
    *,
    artifact_root: str | Path,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    payload = {
        **_normalize_for_json(manifest),
        "schema_version": "signal_discovery_session.v1",
    }
    _atomic_write_json(Path(artifact_root) / "manifest.json", payload)
    return payload


def materialize_walk_forward_atlas(
    *,
    artifact_root: str | Path,
    timestamp_labels: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    summary: Mapping[str, Any],
) -> dict[str, Path]:
    root = Path(artifact_root) / "walk_forward"
    paths = {
        "walk_forward_timestamp_labels": root / "walk_forward_timestamp_labels.parquet",
        "walk_forward_episodes": root / "walk_forward_episodes.parquet",
        "walk_forward_summary": root / "walk_forward_summary.json",
    }
    _atomic_write_parquet(paths["walk_forward_timestamp_labels"], timestamp_labels)
    _atomic_write_parquet(paths["walk_forward_episodes"], episodes)
    _atomic_write_json(paths["walk_forward_summary"], summary)
    return paths


def freeze_target_contract(
    *,
    artifact_root: str | Path,
    session_id: str,
    selected_target: Mapping[str, Any],
    source_data: Mapping[str, Any],
    splits: Mapping[str, Any],
) -> dict[str, Any]:
    root = Path(artifact_root)
    target_path = root / "target" / "frozen_target.json"
    payload = {
        "schema_version": "signal_discovery_target.v1",
        "target_version": 1,
        "session_id": session_id,
        "selected_target": _normalize_for_json(selected_target),
        "source_data": _normalize_for_json(source_data),
        "splits": _normalize_for_json(splits),
    }
    payload["config_hash"] = _sha256_payload(payload)

    if target_path.is_file():
        existing = _read_json_object(target_path)
        if existing != payload:
            raise ValueError("frozen target is immutable and cannot be changed")
        return existing

    feasibility_path = root / "atlas" / "r_feasibility.json"
    if not feasibility_path.is_file():
        raise ValueError("a materialized training atlas is required before target freeze")
    _validate_target_payload(
        selected_target=payload["selected_target"],
        source_data=payload["source_data"],
        splits=payload["splits"],
    )
    feasibility = _read_json_object(feasibility_path)
    configured_risks = {
        float(row["risk_pct"])
        for row in feasibility.get("r_summaries", ())
        if isinstance(row, Mapping) and row.get("risk_pct") is not None
    }
    selected_risk = float(payload["selected_target"]["selected_risk_pct"])
    if selected_risk not in configured_risks:
        raise ValueError("selected_risk_pct is not present in training R feasibility")

    _atomic_write_json(target_path, payload)
    return payload


def read_frozen_target(*, artifact_root: str | Path) -> dict[str, Any]:
    path = Path(artifact_root) / "target" / "frozen_target.json"
    if not path.is_file():
        raise ValueError("frozen target does not exist")
    return _read_json_object(path)


def _validate_target_payload(
    *,
    selected_target: Mapping[str, Any],
    source_data: Mapping[str, Any],
    splits: Mapping[str, Any],
) -> None:
    required_target = {
        "selected_risk_pct",
        "reward_multiple",
        "stop_multiple",
        "horizon_hours",
        "entry_delay_minutes",
        "entry_semantics",
    }
    missing_target = sorted(required_target - selected_target.keys())
    if missing_target:
        raise ValueError(f"selected target is missing fields: {', '.join(missing_target)}")
    if float(selected_target["selected_risk_pct"]) <= 0:
        raise ValueError("selected_risk_pct must be positive")
    if float(selected_target["reward_multiple"]) != 2.0:
        raise ValueError("Outcome-First v1 requires a 2R reward barrier")
    if float(selected_target["stop_multiple"]) != 1.0:
        raise ValueError("Outcome-First v1 requires a 1R stop barrier")
    if float(selected_target["horizon_hours"]) not in {36.0, 48.0}:
        raise ValueError("horizon_hours must be 36 or 48")
    if int(selected_target["entry_delay_minutes"]) < 0:
        raise ValueError("entry_delay_minutes must be nonnegative")
    if selected_target["entry_semantics"] != "next_5m_open":
        raise ValueError("entry_semantics must be next_5m_open")
    if source_data.get("storage_backend") != "parquet":
        raise ValueError("source data must use canonical Parquet storage")
    if not source_data.get("dataset_id"):
        raise ValueError("source data requires dataset_id")

    required_splits = {
        "research_start",
        "research_end",
        "walk_forward_start",
        "walk_forward_end",
    }
    missing_splits = sorted(required_splits - splits.keys())
    if missing_splits:
        raise ValueError(f"split contract is missing fields: {', '.join(missing_splits)}")
    research_start = _coerce_timestamp(splits["research_start"])
    research_end = _coerce_timestamp(splits["research_end"])
    walk_forward_start = _coerce_timestamp(splits["walk_forward_start"])
    walk_forward_end = _coerce_timestamp(splits["walk_forward_end"])
    if not research_start <= research_end < walk_forward_start <= walk_forward_end:
        raise ValueError("split boundaries must be ordered and non-overlapping")


def _atomic_write_parquet(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    normalized = [_normalize_for_arrow(row) for row in rows]
    try:
        pq.write_table(pa.Table.from_pylist(normalized), temp_path)
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    normalized = _normalize_for_json(value)
    try:
        temp_path.write_text(
            json.dumps(normalized, indent=2, sort_keys=True, ensure_ascii=True) + "\n"
        )
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _normalize_for_arrow(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_for_arrow(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_arrow(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _as_utc(value)
    return value


def _normalize_for_json(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _normalize_for_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize_for_json(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _as_utc(value).isoformat().replace("+00:00", "Z")
    return value


def _sha256_payload(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize_for_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _read_json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _as_utc(parsed)
    raise ValueError(f"invalid timestamp: {value!r}")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
