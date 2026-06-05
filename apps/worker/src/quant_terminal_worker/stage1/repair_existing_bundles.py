from __future__ import annotations

import argparse
import json
from pathlib import Path

from quant_terminal_worker.stage1.workspace import repair_stage1_iteration_bundle


def main() -> int:
    parser = argparse.ArgumentParser(description="Repair existing Stage 1 bundle artifacts in-place.")
    parser.add_argument(
        "--workspace-root",
        default=".",
        help="Workspace root containing dev/training_sessions.",
    )
    args = parser.parse_args()
    workspace_root = Path(args.workspace_root).resolve()
    iterations = sorted((workspace_root / "dev" / "training_sessions").glob("**/iterations/iter_*"))
    repaired = []
    for iteration_root in iterations:
        if not iteration_root.is_dir():
            continue
        if not (iteration_root / "signal_sample.json").exists():
            continue
        repaired.append(
            repair_stage1_iteration_bundle(
                workspace_root=workspace_root,
                iteration_root=iteration_root,
            )
        )
    print(json.dumps({"workspace_root": str(workspace_root), "repaired_iterations": repaired}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
