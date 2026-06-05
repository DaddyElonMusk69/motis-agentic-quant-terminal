#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path


ITERATION_RE = re.compile(r"^iter_\d{3}_.+$")
REQUIRED_DIRS = [
    "decisions",
    "scores",
    "audits",
    "summaries",
    "source_artifacts",
    "source_artifacts/strategy_skill_snapshot",
]
REQUIRED_FILES = ["manifest.json", "handoff.md", "signal_sample.json"]
REQUIRED_MANIFEST_FIELDS = [
    "schema_version",
    "iteration_id",
    "session_id",
    "stage",
    "asset",
    "strategy_id",
    "strategy_version",
    "signal_engine_id",
    "signal_family",
    "signal_set_id",
    "sample_method",
    "sample_size",
    "contamination_controls",
    "strategy_skill_snapshot",
    "status",
]


def main() -> int:
    iteration = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    errors: list[str] = []
    if not ITERATION_RE.match(iteration.name):
        errors.append("iteration directory must match iter_NNN_<strategy_version>")
    for child in REQUIRED_DIRS:
        if not (iteration / child).is_dir():
            errors.append(f"missing {child}/")
    for file_name in REQUIRED_FILES:
        if not (iteration / file_name).exists():
            errors.append(f"missing {file_name}")

    manifest = {}
    sample = {}
    try:
        manifest = json.loads((iteration / "manifest.json").read_text())
        for field in REQUIRED_MANIFEST_FIELDS:
            if field not in manifest:
                errors.append(f"manifest missing {field}")
    except Exception as exc:
        errors.append(f"invalid manifest.json: {exc}")

    try:
        sample = json.loads((iteration / "signal_sample.json").read_text())
        packet_paths = sample.get("packet_paths", [])
        if not isinstance(packet_paths, list):
            errors.append("signal_sample packet_paths must be a list")
        elif manifest and manifest.get("sample_size") != len(packet_paths) and packet_paths:
            errors.append("manifest sample_size does not match signal_sample packet_paths length")
    except Exception as exc:
        errors.append(f"invalid signal_sample.json: {exc}")

    controls = manifest.get("contamination_controls", {}) if isinstance(manifest, dict) else {}
    for key in (
        "ground_truth_hidden",
        "future_candles_hidden",
        "prior_iteration_results_hidden",
        "proposed_fixes_hidden",
    ):
        if controls.get(key) is not True:
            errors.append(f"contamination_controls.{key} must be true")

    snapshot_meta = manifest.get("strategy_skill_snapshot", {}) if isinstance(manifest, dict) else {}
    if not isinstance(snapshot_meta, dict):
        errors.append("manifest strategy_skill_snapshot must be an object")
    else:
        snapshot_path = snapshot_meta.get("path")
        snapshot_manifest_path = snapshot_meta.get("manifest_path")
        if snapshot_path != "source_artifacts/strategy_skill_snapshot":
            errors.append("strategy_skill_snapshot.path must equal source_artifacts/strategy_skill_snapshot")
        if snapshot_manifest_path != "source_artifacts/strategy_skill_snapshot_manifest.json":
            errors.append(
                "strategy_skill_snapshot.manifest_path must equal source_artifacts/strategy_skill_snapshot_manifest.json"
            )
        if not (iteration / "source_artifacts" / "strategy_skill_snapshot_manifest.json").exists():
            errors.append("missing source_artifacts/strategy_skill_snapshot_manifest.json")

    result = {
        "iteration": str(iteration.resolve()),
        "valid": not errors,
        "errors": errors,
    }
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
