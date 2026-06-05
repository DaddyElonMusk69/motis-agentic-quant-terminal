from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def infer_signal_engine_id(
    signal_engine_id: str | None,
    signal_family: str | None = None,
    *,
    signals_root: str | Path | None = None,
) -> str:
    for candidate in (signal_engine_id, signal_family):
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    if signals_root is not None:
        name = Path(signals_root).name.strip()
        if name:
            return name
    return ""


def load_engine_registry(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        raise ValueError(f"engine registry missing: {path}")
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"engine registry is not valid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"engine registry must be a JSON object: {path}")

    registry: dict[str, dict[str, Any]] = {}
    for engine_id, entry in payload.items():
        if not isinstance(entry, dict):
            raise ValueError(f"engine registry entry must be an object: {engine_id}")
        registry[str(engine_id)] = entry
    return registry
