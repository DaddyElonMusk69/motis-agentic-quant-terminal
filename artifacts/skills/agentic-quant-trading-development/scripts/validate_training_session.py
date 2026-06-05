#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


REQUIRED_DIRS = [
    "inputs",
    "iterations",
    "promotion",
]
REQUIRED_FIELDS = [
    "session_id",
    "asset",
    "strategy_id",
    "strategy_version",
    "signal_engine_id",
    "signal_family",
    "signal_set_id",
    "stage",
    "iteration_mode",
    "active_iteration",
    "iteration_count",
    "data_manifest",
    "signal_set_manifest",
    "stage0_manifest",
    "inputs",
    "outputs",
    "scoring",
    "status",
]


def main() -> int:
    session = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    missing_dirs = [name for name in REQUIRED_DIRS if not (session / name).is_dir()]
    manifest_path = session / "manifest.json"
    missing_fields: list[str] = []
    manifest_error = None
    if not manifest_path.exists():
        manifest_error = "manifest.json missing"
    else:
        try:
            manifest = json.loads(manifest_path.read_text())
            missing_fields = [field for field in REQUIRED_FIELDS if field not in manifest]
            stage0_manifest = manifest.get("stage0_manifest")
            if stage0_manifest:
                workspace = session
                for _ in range(4):
                    workspace = workspace.parent
                if not (workspace / str(stage0_manifest)).exists():
                    manifest_error = f"stage0_manifest missing: {stage0_manifest}"
        except json.JSONDecodeError as exc:
            manifest_error = str(exc)

    iteration_errors: list[str] = []
    iterations_dir = session / "iterations"
    if iterations_dir.is_dir():
        for iteration in sorted(path for path in iterations_dir.iterdir() if path.is_dir()):
            if not (iteration / "manifest.json").exists():
                iteration_errors.append(f"{iteration.name}: missing manifest.json")
            for child in ["decisions", "scores", "audits", "summaries", "source_artifacts"]:
                if not (iteration / child).is_dir():
                    iteration_errors.append(f"{iteration.name}: missing {child}/")
            if not (iteration / "source_artifacts" / "strategy_skill_snapshot").is_dir():
                iteration_errors.append(f"{iteration.name}: missing source_artifacts/strategy_skill_snapshot/")
            if not (iteration / "source_artifacts" / "strategy_skill_snapshot_manifest.json").exists():
                iteration_errors.append(f"{iteration.name}: missing source_artifacts/strategy_skill_snapshot_manifest.json")
            for file_name in ["handoff.md", "signal_sample.json"]:
                if not (iteration / file_name).exists():
                    iteration_errors.append(f"{iteration.name}: missing {file_name}")

    result = {
        "session": str(session.resolve()),
        "valid": not missing_dirs and not missing_fields and not iteration_errors and manifest_error is None,
        "missing_dirs": missing_dirs,
        "missing_fields": missing_fields,
        "iteration_errors": iteration_errors,
        "manifest_error": manifest_error,
    }
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
