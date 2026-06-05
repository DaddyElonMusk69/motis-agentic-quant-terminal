#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def rel(path: Path, root: Path) -> str:
    return path.resolve().relative_to(root.resolve()).as_posix()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a canonical Stage 0 manifest.")
    parser.add_argument("root", help="Workspace root")
    parser.add_argument("--asset", required=True)
    parser.add_argument("--strategy-id", required=True)
    parser.add_argument("--signal-engine-id", required=True)
    parser.add_argument("--signal-family", required=True)
    parser.add_argument("--signal-set-id", required=True)
    parser.add_argument("--forward-hours", type=int, required=True)
    parser.add_argument("--threshold-pct", type=float, required=True)
    parser.add_argument("--scoreable-signal-end")
    parser.add_argument("--scoreable-outcome-end")
    parser.add_argument("--status", default="scored")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    stage0 = root / "dev" / "training_sessions" / args.strategy_id / "stage0" / args.signal_set_id
    scores = stage0 / "scores"
    summaries = stage0 / "summaries"
    ground_truth_dir = scores / "ground_truth"

    travel = load_json(scores / "travel_distribution.json")
    calibration = load_json(scores / "threshold_calibration.json")
    gt_summary = load_json(scores / "ground_truth_summary.json")
    gt_distribution = load_json(ground_truth_dir / "distribution.json")

    gt_metrics = gt_summary.get("metrics", {})
    if not isinstance(gt_metrics, dict):
        gt_metrics = {}

    status_counts = gt_metrics.get("status_counts", {})
    if not isinstance(status_counts, dict):
        status_counts = {}

    total_records = (
        gt_metrics.get("total_records")
        or gt_summary.get("total_records")
        or gt_summary.get("total_signals")
        or gt_distribution.get("total_signals")
        or len([p for p in ground_truth_dir.glob("*.json") if p.name not in {"index.json", "distribution.json"}])
    )
    direction_split = gt_distribution.get("direction_split", {})
    if direction_split:
        triggered = sum(int(v) for v in direction_split.values())
    else:
        triggered = (
            gt_metrics.get("triggered_records")
            or status_counts.get("triggered")
            or gt_summary.get("triggered")
            or 0
        )

    if gt_metrics.get("trigger_rate_pct") is not None:
        trigger_rate_pct = float(gt_metrics["trigger_rate_pct"])
    else:
        trigger_rate_pct = round(triggered / total_records * 100, 2) if total_records else 0.0

    branch_path = gt_metrics.get("branch_path") or ("path_a" if trigger_rate_pct >= 80 else "path_b")
    branch_decision = gt_metrics.get(
        "branch_decision",
        "rich_pool_go_to_stage1a"
        if branch_path == "path_a"
        else "sparse_pool_go_to_stage1b_then_stage1a",
    )

    manifest = {
        "schema_version": "0.1",
        "created_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "asset": args.asset.upper(),
        "strategy_id": args.strategy_id,
        "signal_engine_id": args.signal_engine_id,
        "signal_family": args.signal_family,
        "signal_set_id": args.signal_set_id,
        "data_manifest": f"dev/data/manifests/{args.asset.upper()}.json",
        "signal_set_manifest": f"dev/signals/{args.signal_engine_id}/{args.asset.upper()}/{args.signal_set_id}/manifest.json",
        "forward_hours": args.forward_hours,
        "threshold_pct": args.threshold_pct,
        "scoring_window": {
            "scoreable_signal_end": args.scoreable_signal_end or "",
            "scoreable_outcome_end": args.scoreable_outcome_end or "",
        },
        "outputs": {
            "travel_distribution": "scores/travel_distribution.json",
            "threshold_calibration": "scores/threshold_calibration.json",
            "ground_truth_summary": "scores/ground_truth_summary.json",
            "ground_truth_records": "scores/ground_truth/",
            "scoreable_signal_subset": "scores/_scoreable_signal_subset/packets/",
            "summaries": "summaries/",
        },
        "status": args.status,
        "metrics": {
            "total_records": total_records,
            "triggered_records": triggered,
            "trigger_rate_pct": trigger_rate_pct,
            "branch_path": branch_path,
            "branch_decision": branch_decision,
            "travel_distribution_available": bool(travel),
            "threshold_calibration_available": bool(calibration),
            "ground_truth_available": ground_truth_dir.is_dir(),
        },
    }

    stage0.mkdir(parents=True, exist_ok=True)
    scores.mkdir(parents=True, exist_ok=True)
    summaries.mkdir(parents=True, exist_ok=True)
    manifest_path = stage0 / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"manifest": rel(manifest_path, root), "metrics": manifest["metrics"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
