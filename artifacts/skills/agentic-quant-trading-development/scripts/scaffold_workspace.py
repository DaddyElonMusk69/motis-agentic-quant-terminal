#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


DIRS = [
    "dev/data/raw",
    "dev/data/derived",
    "dev/data/manifests",
    "dev/signals",
    "dev/training_sessions",
    "dev/notebooks_or_scratch",
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
    "artifacts/skills/strategies",
    "artifacts/docs/architecture",
    "artifacts/docs/operations",
    "artifacts/docs/archive",
]


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    root.mkdir(parents=True, exist_ok=True)
    for rel in DIRS:
        path = root / rel
        path.mkdir(parents=True, exist_ok=True)
        keep = path / ".gitkeep"
        keep.touch(exist_ok=True)

    manifest = root / "workspace_manifest.json"
    if not manifest.exists():
        manifest.write_text(
            json.dumps(
                {
                    "workspace_name": root.name,
                    "schema_version": "0.1",
                    "purpose": "Agentic quant trading development and live execution scaffold",
                    "canonical_skill": "artifacts/skills/agentic-quant-trading-development/SKILL.md",
                    "signal_engine_root": "artifacts/signal_engine",
                    "strategy_skill_root": "artifacts/skills/strategies",
                },
                indent=2,
            )
            + "\n"
        )

    print(
        json.dumps(
            {
                "root": str(root.resolve()),
                "scaffolded": True,
                "next_steps": [
                    "install this skill at artifacts/skills/agentic-quant-trading-development",
                    "install or copy the deterministic signal engine at artifacts/signal_engine",
                    "install strategy skills at artifacts/skills/strategies",
                    "run validate_workspace.py after artifacts are installed",
                ],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
