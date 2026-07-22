from __future__ import annotations

import argparse
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from quant_terminal_worker.signal_discovery.supervised_training_data import (
    BRANCH_SPECS,
    FeatureTimeline,
    _atomic_write_text,
    _sha256,
    load_prepared_supervised_training_data,
)


TABULAR_FEATURE_SCHEMA_VERSION = "motis_supervised_tabular_features.v1"
TABULAR_INPUT_SCHEMA_VERSION = "motis_supervised_lightgbm_input.v1"


@dataclass(frozen=True)
class LagSpec:
    name: str
    steps: int


DEFAULT_BRANCH_LAGS: dict[str, tuple[LagSpec, ...]] = {
    "5m_micro": (LagSpec("1h", 12), LagSpec("1d", 288)),
    "15m_short": (LagSpec("4h", 16), LagSpec("7d", 672)),
    "1h_medium": (LagSpec("1d", 24), LagSpec("30d", 720)),
    "4h_long": (LagSpec("7d", 42), LagSpec("90d", 540)),
    "1d_regime": (LagSpec("30d", 30), LagSpec("365d", 365)),
    "funding_events": (LagSpec("7d", 21), LagSpec("90d", 270)),
}


def ordered_branch_names(timelines: Mapping[str, FeatureTimeline]) -> tuple[str, ...]:
    preferred = [spec.name for spec in BRANCH_SPECS if spec.name in timelines]
    extras = sorted(set(timelines) - set(preferred))
    return tuple(preferred + extras)


def tabular_feature_names(
    timelines: Mapping[str, FeatureTimeline],
    *,
    branch_lags: Mapping[str, Sequence[LagSpec]] = DEFAULT_BRANCH_LAGS,
) -> tuple[str, ...]:
    names: list[str] = []
    for branch_name in ordered_branch_names(timelines):
        timeline = timelines[branch_name]
        lags = tuple(branch_lags.get(branch_name) or ())
        for channel in timeline.channel_names:
            names.append(f"{branch_name}__{channel}__current")
        for lag in lags:
            for channel in timeline.channel_names:
                names.append(f"{branch_name}__{channel}__delta_{lag.name}")
        names.append(f"{branch_name}__age_minutes")
    if len(names) != len(set(names)):
        raise ValueError("tabular feature names must be unique")
    return tuple(names)


def build_tabular_feature_matrix(
    *,
    decision_ns: np.ndarray,
    timelines: Mapping[str, FeatureTimeline],
    branch_lags: Mapping[str, Sequence[LagSpec]] = DEFAULT_BRANCH_LAGS,
) -> np.ndarray:
    names = tabular_feature_names(timelines, branch_lags=branch_lags)
    output = np.empty((len(decision_ns), len(names)), dtype=np.float32)
    _fill_tabular_feature_matrix(
        output=output,
        decision_ns=decision_ns,
        timelines=timelines,
        branch_lags=branch_lags,
    )
    return output


def prepare_lightgbm_training_input(
    *,
    source_manifest_path: Path,
    output_root: Path | None = None,
) -> dict[str, Any]:
    labels, timelines, source_manifest = load_prepared_supervised_training_data(
        source_manifest_path
    )
    artifact_root = Path(source_manifest["artifact_root"])
    selected_root = (
        output_root.resolve()
        if output_root is not None
        else artifact_root / "training" / "supervised_lightgbm_input"
    )
    selected_root.mkdir(parents=True, exist_ok=True)
    feature_names = tabular_feature_names(timelines)
    matrix_path = selected_root / "features.npy"
    temporary_path = matrix_path.with_suffix(matrix_path.suffix + ".tmp")
    output = np.lib.format.open_memmap(
        temporary_path,
        mode="w+",
        dtype=np.float32,
        shape=(len(labels), len(feature_names)),
    )
    try:
        _fill_tabular_feature_matrix(
            output=output,
            decision_ns=labels["decision_ns"].to_numpy(dtype=np.int64),
            timelines=timelines,
            branch_lags=DEFAULT_BRANCH_LAGS,
            progress=True,
        )
        output.flush()
    finally:
        del output
    temporary_path.replace(matrix_path)

    feature_schema = {
        "schema_version": TABULAR_FEATURE_SCHEMA_VERSION,
        "source_feature_schema_version": source_manifest["feature_schema_version"],
        "branch_order": list(ordered_branch_names(timelines)),
        "branch_lags": {
            name: [{"name": lag.name, "steps": lag.steps} for lag in lags]
            for name, lags in DEFAULT_BRANCH_LAGS.items()
            if name in timelines
        },
        "operations": ["current", "current_minus_causal_lag", "source_age_minutes"],
        "missing_values": "native_nan",
        "normalization": "none_tree_native",
        "feature_names": list(feature_names),
    }
    canonical_schema = json.dumps(feature_schema, sort_keys=True, separators=(",", ":"))
    feature_schema["feature_schema_hash"] = hashlib.sha256(
        canonical_schema.encode("utf-8")
    ).hexdigest()
    manifest: dict[str, Any] = {
        "schema_version": TABULAR_INPUT_SCHEMA_VERSION,
        "created_from": str(source_manifest_path.resolve()),
        "artifact_root": str(artifact_root.resolve()),
        "session_id": source_manifest["session_id"],
        "target_config_hash": source_manifest["target_config_hash"],
        "source_manifest_hash": source_manifest["manifest_hash"],
        "source_labels": source_manifest["labels"],
        "splits": source_manifest["splits"],
        "target": source_manifest["target"],
        "matrix": {
            "path": str(matrix_path.relative_to(artifact_root)),
            "sha256": _sha256(matrix_path),
            "rows": int(len(labels)),
            "columns": int(len(feature_names)),
            "dtype": "float32",
        },
        "feature_schema": feature_schema,
        "walk_forward_inspected": False,
    }
    canonical_manifest = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    manifest["manifest_hash"] = hashlib.sha256(
        canonical_manifest.encode("utf-8")
    ).hexdigest()
    manifest_path = selected_root / "manifest.json"
    _atomic_write_text(manifest_path, json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        f"[lightgbm-input] rows={len(labels)} features={len(feature_names)} "
        f"size_mib={matrix_path.stat().st_size / (1024 ** 2):.1f} manifest={manifest_path}",
        flush=True,
    )
    return {"manifest": manifest, "manifest_path": str(manifest_path)}


def load_lightgbm_training_input(
    manifest_path: Path,
) -> tuple[pd.DataFrame, np.ndarray, dict[str, Any]]:
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != TABULAR_INPUT_SCHEMA_VERSION:
        raise ValueError("unsupported LightGBM training input manifest")
    source_path = Path(manifest["created_from"])
    labels, _, source_manifest = load_prepared_supervised_training_data(source_path)
    if source_manifest["manifest_hash"] != manifest["source_manifest_hash"]:
        raise ValueError("source supervised manifest changed after tabular preparation")
    artifact_root = Path(manifest["artifact_root"])
    matrix_path = artifact_root / manifest["matrix"]["path"]
    if _sha256(matrix_path) != manifest["matrix"]["sha256"]:
        raise ValueError("prepared LightGBM feature matrix hash changed")
    matrix = np.load(matrix_path, mmap_mode="r")
    expected_shape = (int(manifest["matrix"]["rows"]), int(manifest["matrix"]["columns"]))
    if matrix.shape != expected_shape or len(labels) != expected_shape[0]:
        raise ValueError("prepared LightGBM matrix shape does not match its manifest")
    if matrix.dtype != np.float32:
        raise ValueError("prepared LightGBM matrix must use float32")
    return labels, matrix, manifest


def _fill_tabular_feature_matrix(
    *,
    output: np.ndarray,
    decision_ns: np.ndarray,
    timelines: Mapping[str, FeatureTimeline],
    branch_lags: Mapping[str, Sequence[LagSpec]],
    progress: bool = False,
) -> None:
    decisions = np.asarray(decision_ns, dtype=np.int64)
    if decisions.ndim != 1 or np.any(np.diff(decisions) < 0):
        raise ValueError("tabular decisions must be a chronological vector")
    expected_columns = len(tabular_feature_names(timelines, branch_lags=branch_lags))
    if output.shape != (len(decisions), expected_columns):
        raise ValueError("tabular output shape does not match its feature schema")
    cursor = 0
    for branch_name in ordered_branch_names(timelines):
        timeline = timelines[branch_name]
        latest = np.searchsorted(timeline.available_ns, decisions, side="right") - 1
        lags = tuple(branch_lags.get(branch_name) or ())
        maximum_lag = max((lag.steps for lag in lags), default=0)
        if np.any(latest < maximum_lag):
            raise ValueError(f"{branch_name} lacks tabular lag history")
        if np.any(timeline.available_ns[latest] > decisions):
            raise ValueError(f"{branch_name} exposes future rows")
        current = np.asarray(timeline.values[latest], dtype=np.float32)
        width = current.shape[1]
        output[:, cursor : cursor + width] = _finite_or_nan(current)
        cursor += width
        for lag in lags:
            previous = np.asarray(timeline.values[latest - lag.steps], dtype=np.float32)
            output[:, cursor : cursor + width] = _finite_or_nan(current - previous)
            cursor += width
        output[:, cursor] = (decisions - timeline.available_ns[latest]) / (60 * 1_000_000_000)
        cursor += 1
        if progress:
            print(
                f"[lightgbm-input] branch={branch_name} columns={cursor}/{expected_columns}",
                flush=True,
            )
    if cursor != expected_columns:
        raise RuntimeError("tabular feature writer did not fill the declared schema")


def _finite_or_nan(values: np.ndarray) -> np.ndarray:
    return np.where(np.isfinite(values), values, np.nan).astype(np.float32, copy=False)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Prepare causal tabular LightGBM features.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)
    result = prepare_lightgbm_training_input(
        source_manifest_path=args.manifest.resolve(),
        output_root=args.output_dir,
    )
    print(
        json.dumps(
            {
                "manifest_path": result["manifest_path"],
                "manifest_hash": result["manifest"]["manifest_hash"],
            },
            indent=2,
            sort_keys=True,
        ),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
