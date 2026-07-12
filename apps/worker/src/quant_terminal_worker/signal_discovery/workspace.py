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
_ATLAS_CHECKPOINT_SCHEMA = "signal_discovery_atlas_checkpoint.v1"


class TrainingAtlasWorkspace:
    def __init__(
        self,
        *,
        artifact_root: str | Path,
        run_identity: Mapping[str, Any],
    ) -> None:
        self.artifact_root = Path(artifact_root)
        self.atlas_root = self.artifact_root / "atlas"
        self.work_root = self.atlas_root / ".work"
        self.checkpoint_path = self.atlas_root / "checkpoint.json"
        self.run_identity = _normalize_for_json(dict(run_identity))
        self.run_fingerprint = _json_sha256(self.run_identity)
        if self.checkpoint_path.is_file():
            self._checkpoint = json.loads(self.checkpoint_path.read_text())
            self._validate_checkpoint()
        else:
            self._checkpoint = {
                "schema_version": _ATLAS_CHECKPOINT_SCHEMA,
                "run_fingerprint": self.run_fingerprint,
                "run_identity": self.run_identity,
                "completed_risks": [],
            }

    @property
    def completed_risk_values(self) -> tuple[float, ...]:
        return tuple(float(row["risk_pct"]) for row in self._ordered_entries())

    @property
    def completed_episode_count(self) -> int:
        return sum(int(row["episode_count"]) for row in self._ordered_entries())

    @property
    def completed_timestamp_label_count(self) -> int:
        return sum(
            int(row["timestamp_label_count"]) for row in self._ordered_entries()
        )

    @property
    def completed_hard_negative_count(self) -> int:
        return sum(int(row["hard_negative_count"]) for row in self._ordered_entries())

    @property
    def r_summaries(self) -> list[dict[str, Any]]:
        return [dict(row["r_summary"]) for row in self._ordered_entries()]

    @property
    def purged_decision_count(self) -> int:
        values = {
            int(row["purged_decision_count"]) for row in self._ordered_entries()
        }
        if len(values) > 1:
            raise ValueError("checkpoint has inconsistent purged decision counts")
        return next(iter(values), 0)

    def write_risk_partition(
        self,
        *,
        risk_index: int,
        risk_pct: float,
        timestamp_labels: Sequence[Mapping[str, Any]],
        episodes: Sequence[Mapping[str, Any]],
        hard_negatives: Sequence[Mapping[str, Any]],
        r_summary: Mapping[str, Any],
        purged_decision_count: int,
    ) -> None:
        if risk_index < 0:
            raise ValueError("risk_index must be nonnegative")
        if risk_pct <= 0:
            raise ValueError("risk_pct must be positive")
        if risk_pct in self.completed_risk_values:
            raise ValueError(f"risk {risk_pct} is already checkpointed")
        partition_root = self.work_root / f"risk-{risk_index:04d}"
        rows_by_name = {
            "timestamp_labels": timestamp_labels,
            "episodes": episodes,
            "hard_negatives": hard_negatives,
        }
        artifacts: dict[str, dict[str, Any]] = {}
        for name, rows in rows_by_name.items():
            path = partition_root / f"{name}.parquet"
            _atomic_write_parquet(path, rows)
            artifacts[name] = {
                "path": str(path.relative_to(self.atlas_root)),
                "sha256": _file_sha256(path),
                "row_count": len(rows),
            }
        entry = {
            "risk_index": risk_index,
            "risk_pct": float(risk_pct),
            "timestamp_label_count": len(timestamp_labels),
            "episode_count": len(episodes),
            "hard_negative_count": len(hard_negatives),
            "purged_decision_count": int(purged_decision_count),
            "r_summary": _normalize_for_json(r_summary),
            "artifacts": artifacts,
        }
        updated = {
            **self._checkpoint,
            "completed_risks": sorted(
                [*self._checkpoint["completed_risks"], entry],
                key=lambda row: int(row["risk_index"]),
            ),
        }
        _atomic_write_json(self.checkpoint_path, updated)
        self._checkpoint = updated

    def finalize(
        self,
        *,
        features: Sequence[Mapping[str, Any]],
        r_feasibility: Mapping[str, Any],
    ) -> dict[str, Path]:
        if not self._checkpoint["completed_risks"]:
            raise ValueError("cannot finalize an empty training atlas")
        paths = _training_atlas_paths(self.artifact_root)
        entries = self._ordered_entries()
        for name, output_key in (
            ("timestamp_labels", "training_timestamp_labels"),
            ("episodes", "training_episodes"),
            ("hard_negatives", "training_hard_negatives"),
        ):
            parts = [
                self.atlas_root / row["artifacts"][name]["path"] for row in entries
            ]
            _atomic_compact_parquet(paths[output_key], parts)
        _atomic_write_parquet(paths["training_features"], features)
        _atomic_write_json(paths["r_feasibility"], r_feasibility)
        return paths

    def _ordered_entries(self) -> list[dict[str, Any]]:
        return sorted(
            self._checkpoint["completed_risks"],
            key=lambda row: int(row["risk_index"]),
        )

    def _validate_checkpoint(self) -> None:
        if self._checkpoint.get("schema_version") != _ATLAS_CHECKPOINT_SCHEMA:
            raise ValueError("unsupported training atlas checkpoint schema")
        if self._checkpoint.get("run_fingerprint") != self.run_fingerprint:
            raise ValueError("training atlas checkpoint run identity changed")
        if self._checkpoint.get("run_identity") != self.run_identity:
            raise ValueError("training atlas checkpoint run identity changed")
        seen_indices: set[int] = set()
        seen_risks: set[float] = set()
        for entry in self._ordered_entries():
            risk_index = int(entry["risk_index"])
            risk_pct = float(entry["risk_pct"])
            if risk_index in seen_indices or risk_pct in seen_risks:
                raise ValueError("training atlas checkpoint contains duplicate risks")
            seen_indices.add(risk_index)
            seen_risks.add(risk_pct)
            for artifact in entry["artifacts"].values():
                path = (self.atlas_root / artifact["path"]).resolve()
                if not path.is_relative_to(self.atlas_root.resolve()) or not path.is_file():
                    raise ValueError("training atlas checkpoint part is missing")
                if _file_sha256(path) != artifact["sha256"]:
                    raise ValueError("training atlas checkpoint part fingerprint changed")


def _training_atlas_paths(artifact_root: str | Path) -> dict[str, Path]:
    atlas_root = Path(artifact_root) / "atlas"
    return {
        "training_timestamp_labels": atlas_root / "training_timestamp_labels.parquet",
        "training_episodes": atlas_root / "training_episodes.parquet",
        "training_features": atlas_root / "training_features.parquet",
        "training_hard_negatives": atlas_root / "training_hard_negatives.parquet",
        "r_feasibility": atlas_root / "r_feasibility.json",
    }


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
    paths = _training_atlas_paths(artifact_root)
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
    approved_brackets: Sequence[Mapping[str, Any]] | None = None,
    bracket_summary: Mapping[str, Any] | None = None,
) -> dict[str, Path]:
    root = Path(artifact_root) / "walk_forward"
    paths = {
        "walk_forward_timestamp_labels": root / "walk_forward_timestamp_labels.parquet",
        "walk_forward_episodes": root / "walk_forward_episodes.parquet",
        "walk_forward_summary": root / "walk_forward_summary.json",
    }
    if approved_brackets is not None:
        paths["walk_forward_brackets"] = root / "walk_forward_brackets.parquet"
    if bracket_summary is not None:
        paths["walk_forward_bracket_summary"] = root / "walk_forward_bracket_summary.json"
    _atomic_write_parquet(paths["walk_forward_timestamp_labels"], timestamp_labels)
    _atomic_write_parquet(paths["walk_forward_episodes"], episodes)
    _atomic_write_json(paths["walk_forward_summary"], summary)
    if approved_brackets is not None:
        _atomic_write_parquet(paths["walk_forward_brackets"], approved_brackets)
    if bracket_summary is not None:
        _atomic_write_json(paths["walk_forward_bracket_summary"], bracket_summary)
    return paths


def freeze_target_contract(
    *,
    artifact_root: str | Path,
    session_id: str,
    selected_target: Mapping[str, Any],
    source_data: Mapping[str, Any],
    splits: Mapping[str, Any],
    bracket_contract: Mapping[str, Any] | None = None,
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
        **(
            {"bracket_policy": _normalize_for_json(bracket_contract)}
            if bracket_contract is not None
            else {}
        ),
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
    if float(selected_target["reward_multiple"]) <= 0:
        raise ValueError("reward_multiple must be positive")
    if float(selected_target["stop_multiple"]) != 1.0:
        raise ValueError("Outcome-First v1 requires a 1R stop barrier")
    horizon_hours = float(selected_target["horizon_hours"])
    if horizon_hours <= 0 or not horizon_hours.is_integer():
        raise ValueError("horizon_hours must be positive whole hours")
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


def _atomic_compact_parquet(path: Path, parts: Sequence[Path]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    writer: pq.ParquetWriter | None = None
    try:
        parquet_files = [
            parquet_file
            for part in parts
            if (parquet_file := pq.ParquetFile(part)).metadata.num_rows > 0
        ]
        if parquet_files:
            schema = pa.unify_schemas(
                [parquet_file.schema_arrow for parquet_file in parquet_files],
                promote_options="permissive",
            )
            writer = pq.ParquetWriter(temp_path, schema)
        for parquet_file in parquet_files:
            for batch in parquet_file.iter_batches(batch_size=65_536):
                table = _cast_table_to_schema(pa.Table.from_batches([batch]), schema)
                writer.write_table(table)
        if writer is not None:
            writer.close()
            writer = None
        else:
            pq.write_table(pa.Table.from_pylist([]), temp_path)
        temp_path.replace(path)
    finally:
        if writer is not None:
            writer.close()
        temp_path.unlink(missing_ok=True)


def _cast_table_to_schema(table: pa.Table, schema: pa.Schema) -> pa.Table:
    columns = []
    for field in schema:
        if field.name in table.column_names:
            column = table.column(field.name)
            if column.type != field.type:
                column = column.cast(field.type)
        else:
            column = pa.chunked_array([pa.nulls(len(table), type=field.type)])
        columns.append(column)
    return pa.Table.from_arrays(columns, schema=schema)


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


def _json_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        _normalize_for_json(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
