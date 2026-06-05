from __future__ import annotations

import json
from pathlib import Path


MANIFEST_NAME = "workspace_manifest.json"


def find_workspace_root(start: str | Path | None = None) -> Path:
    """Find the Motis workspace root by requiring the new scaffold manifest."""
    current = Path(start).resolve() if start is not None else Path.cwd().resolve()
    if current.is_file():
        current = current.parent

    for candidate in (current, *current.parents):
        manifest_path = candidate / MANIFEST_NAME
        if not manifest_path.exists():
            continue
        manifest = json.loads(manifest_path.read_text())
        required = ("dev", "live", "artifacts")
        if all((candidate / name).is_dir() for name in required) and all(
            name in manifest.get("directories", {}) for name in required
        ):
            return candidate

    raise FileNotFoundError(
        f"Could not find {MANIFEST_NAME}; run from inside the Motis Agentic Quant Terminal workspace"
    )


def signal_engine_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "artifacts" / "signal_engine"


def dev_data_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "dev" / "data"


def dev_signals_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "dev" / "signals"


def dev_training_sessions_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "dev" / "training_sessions"


def live_data_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "live" / "data"


def live_signals_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "live" / "signals"


def live_router_root(workspace_root: str | Path | None = None) -> Path:
    root = Path(workspace_root) if workspace_root is not None else find_workspace_root()
    return root / "live" / "router"
