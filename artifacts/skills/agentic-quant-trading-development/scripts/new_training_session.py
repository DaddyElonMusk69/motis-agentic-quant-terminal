#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


def metadata_value(value: str):
    if not value:
        return ""
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a canonical training session folder.")
    parser.add_argument("root", help="Workspace root")
    parser.add_argument("--asset", required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--strategy-version", required=True)
    parser.add_argument("--signal-engine-id", required=True)
    parser.add_argument("--signal-family", required=True)
    parser.add_argument("--signal-set-id", required=True)
    parser.add_argument("--stage", required=True)
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--branch-path", default="", choices=["", "path_a", "path_b", "both"])
    parser.add_argument("--forward-hours", type=int, default=36)
    parser.add_argument("--threshold-pct", type=float)
    parser.add_argument("--walk-forward-month", default="")
    parser.add_argument("--train-window", default="")
    parser.add_argument("--validation-window", default="")
    parser.add_argument("--locked-oos-window", default="")
    parser.add_argument("--universe-manifest-path", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    session = root / "dev" / "training_sessions" / args.strategy_id / args.session_id
    for child in [
        "inputs",
        "iterations",
        "promotion",
    ]:
        (session / child).mkdir(parents=True, exist_ok=True)
        (session / child / ".gitkeep").touch(exist_ok=True)

    manifest = {
        "session_id": args.session_id,
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asset": args.asset.upper(),
        "strategy_id": args.strategy_id,
        "strategy_version": args.strategy_version,
        "signal_engine_id": args.signal_engine_id,
        "signal_family": args.signal_family,
        "signal_set_id": args.signal_set_id,
        "stage": args.stage,
        "iteration_mode": True,
        "active_iteration": "",
        "iteration_count": 0,
        "branch_path": args.branch_path,
        "forward_hours": args.forward_hours,
        "threshold_pct": args.threshold_pct,
        "walk_forward_month": args.walk_forward_month,
        "train_window": metadata_value(args.train_window),
        "validation_window": metadata_value(args.validation_window),
        "locked_oos_window": metadata_value(args.locked_oos_window),
        "universe_manifest_path": args.universe_manifest_path,
        "data_manifest": f"dev/data/manifests/{args.asset.upper()}.json",
        "signal_set_manifest": f"dev/signals/{args.signal_engine_id}/{args.asset.upper()}/{args.signal_set_id}/manifest.json",
        "stage0_manifest": f"dev/training_sessions/{args.strategy_id}/stage0/{args.signal_set_id}/manifest.json",
        "inputs": {
            "signal_packets": f"dev/signals/{args.signal_engine_id}/{args.asset.upper()}/{args.signal_set_id}/packets",
            "strategy_skill": f"artifacts/skills/strategies/{args.strategy_id}",
            "ground_truth": f"dev/training_sessions/{args.strategy_id}/stage0/{args.signal_set_id}/scores/ground_truth/",
        },
        "outputs": {
            "iterations": "iterations/",
            "promotion": "promotion/",
        },
        "scoring": {
            "method": "",
            "promotion_gate": "",
        },
        "status": "created",
    }
    (session / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"session": str(session), "created": True}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
