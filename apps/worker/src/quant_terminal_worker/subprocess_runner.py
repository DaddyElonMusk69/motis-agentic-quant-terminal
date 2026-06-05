from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


def run_entrypoint_subprocess(
    *,
    entrypoint: str,
    payload: dict[str, Any],
    extra_python_paths: list[Path] | None = None,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    src_root = Path(__file__).resolve().parents[1]
    repo_root = Path.cwd()
    python_paths = [
        *(str(path) for path in extra_python_paths or []),
        str(src_root),
        str(repo_root / "packages" / "strategy_sdk" / "src"),
        str(repo_root / "packages" / "engine_sdk" / "src"),
        str(repo_root / "packages" / "strategy_modules" / "src"),
    ]
    existing_pythonpath = os.environ.get("PYTHONPATH")
    if existing_pythonpath:
        python_paths.append(existing_pythonpath)

    env = {
        **os.environ,
        "PYTHONPATH": os.pathsep.join(python_paths),
    }

    completed = subprocess.run(
        [sys.executable, "-m", "quant_terminal_worker.subprocess_entrypoint", entrypoint],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
        check=False,
        env=env,
    )

    if completed.returncode != 0:
        raise RuntimeError(completed.stderr.strip() or "worker subprocess failed")

    return json.loads(completed.stdout)
