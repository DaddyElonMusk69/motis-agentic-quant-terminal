#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path


def count_files(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*") if item.is_file() and item.name != ".gitkeep")


def main() -> int:
    session = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    manifest_path = session / "manifest.json"
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    summary = {
        "session": str(session.resolve()),
        "manifest": manifest,
        "file_counts": {
            "inputs": count_files(session / "inputs"),
            "iterations": count_files(session / "iterations"),
            "promotion": count_files(session / "promotion"),
        },
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
