#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


REQUIRED_PATHS = [
    "workspace_manifest.json",
    "dev/data/raw",
    "dev/data/derived",
    "dev/data/manifests",
    "dev/signals",
    "dev/training_sessions",
    "live/data/raw",
    "live/data/derived",
    "live/data/state",
    "live/signals",
    "live/router/state/open_position_owner",
    "live/router/state/position_reviews",
    "live/router/state/wake_router",
    "live/router/logs",
    "live/router/prompts",
    "artifacts/signal_engine",
    "artifacts/skills/agentic-quant-trading-development/SKILL.md",
    "artifacts/skills/strategies",
]


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    missing = [path for path in REQUIRED_PATHS if not (root / path).exists()]
    manifest_path = root / "workspace_manifest.json"
    manifest_error = None
    if manifest_path.exists():
        try:
            json.loads(manifest_path.read_text())
        except json.JSONDecodeError as exc:
            manifest_error = str(exc)

    result = {
        "root": str(root.resolve()),
        "valid": not missing and manifest_error is None,
        "missing": missing,
        "manifest_error": manifest_error,
    }
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

