#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any


REQUIRED_FILES = [
    "manifest.json",
    "stage0_branch_decisions.json",
    "tradable_universe.json",
    "watchlist_universe.json",
    "summaries/monthly_universe.md",
]


def load_json(path: Path, errors: list[str]) -> dict[str, Any]:
    if not path.exists():
        errors.append(f"missing file: {path.name}")
        return {}
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        errors.append(f"invalid JSON: {path.name}: {exc}")
        return {}
    if not isinstance(payload, dict):
        errors.append(f"JSON root must be object: {path.name}")
        return {}
    return payload


def require_fields(record: dict[str, Any], fields: list[str], label: str, errors: list[str]) -> None:
    for field in fields:
        if record.get(field) in (None, ""):
            errors.append(f"{label} missing {field}")


def require_present(record: dict[str, Any], fields: list[str], label: str, errors: list[str]) -> None:
    for field in fields:
        if field not in record:
            errors.append(f"{label} missing {field}")


def resolve_link(path_value: str, workspace_root: Path) -> Path:
    path = Path(path_value)
    if path.is_absolute():
        return path
    root_candidate = workspace_root / path
    if root_candidate.exists():
        return root_candidate
    return Path.cwd() / path


def main() -> int:
    universe_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    workspace_root = universe_dir.parents[2] if len(universe_dir.parents) >= 3 else universe_dir
    errors: list[str] = []
    for required in REQUIRED_FILES:
        if not (universe_dir / required).exists():
            errors.append(f"missing required file: {required}")

    manifest = load_json(universe_dir / "manifest.json", errors)
    branch = load_json(universe_dir / "stage0_branch_decisions.json", errors)
    tradable = load_json(universe_dir / "tradable_universe.json", errors)
    watchlist = load_json(universe_dir / "watchlist_universe.json", errors)

    month = manifest.get("walk_forward_month")
    if not month:
        errors.append("manifest missing walk_forward_month")
    for name, payload in [
        ("stage0_branch_decisions.json", branch),
        ("tradable_universe.json", tradable),
        ("watchlist_universe.json", watchlist),
    ]:
        if payload and payload.get("walk_forward_month") != month:
            errors.append(f"{name} walk_forward_month does not match manifest")

    decisions = branch.get("decisions", [])
    if not isinstance(decisions, list):
        errors.append("stage0_branch_decisions.json decisions must be a list")
        decisions = []

    decision_keys: set[tuple[str, str, str]] = set()
    required_decision_fields = [
        "asset",
        "strategy_id",
        "signal_engine_id",
        "signal_set_id",
        "stage0_manifest_path",
        "total_valid_signals",
        "triggered_signals",
        "trigger_rate_pct",
        "branch_path",
        "path_a_threshold_pct",
        "threshold_pct",
        "forward_hours",
    ]
    for index, decision in enumerate(decisions):
        if not isinstance(decision, dict):
            errors.append(f"decision {index} must be an object")
            continue
        require_fields(decision, required_decision_fields, f"decision {index}", errors)
        if decision.get("branch_path") not in {"path_a", "path_b"}:
            errors.append(f"decision {index} branch_path must be path_a or path_b")
        if decision.get("stage0_manifest_path"):
            stage0_path = resolve_link(str(decision["stage0_manifest_path"]), workspace_root)
            if not stage0_path.exists():
                errors.append(f"decision {index} stage0 manifest path does not exist: {decision['stage0_manifest_path']}")
        decision_keys.add(
            (
                str(decision.get("asset") or ""),
                str(decision.get("strategy_id") or ""),
                str(decision.get("signal_set_id") or ""),
            )
        )

    def validate_assets(payload: dict[str, Any], expected_path: str, label: str) -> None:
        assets = payload.get("assets", [])
        if not isinstance(assets, list):
            errors.append(f"{label} assets must be a list")
            return
        for index, asset in enumerate(assets):
            if not isinstance(asset, dict):
                errors.append(f"{label} asset {index} must be an object")
                continue
            require_fields(
                asset,
                [
                    "asset",
                    "strategy_id",
                    "signal_engine_id",
                    "signal_set_id",
                    "stage0_manifest_path",
                    "branch_path",
                    "trigger_rate_pct",
                    "strategy_training_status",
                    "notes",
                ],
                f"{label} asset {index}",
                errors,
            )
            require_present(
                asset,
                ["strategy_version", "latest_training_session", "latest_training_date"],
                f"{label} asset {index}",
                errors,
            )
            if asset.get("branch_path") != expected_path:
                errors.append(f"{label} asset {index} has branch_path {asset.get('branch_path')}, expected {expected_path}")
            key = (
                str(asset.get("asset") or ""),
                str(asset.get("strategy_id") or ""),
                str(asset.get("signal_set_id") or ""),
            )
            if key not in decision_keys:
                errors.append(f"{label} asset {index} is not present in branch decisions")
            if asset.get("strategy_training_status") not in {"retrained_for_month", "stale", "missing_strategy"}:
                errors.append(f"{label} asset {index} has invalid strategy_training_status")
        strategy_ids = [str(asset.get("strategy_id") or "") for asset in assets if isinstance(asset, dict)]
        if len(strategy_ids) != len(set(strategy_ids)):
            errors.append(f"{label} contains duplicate strategy_id rows")

    validate_assets(tradable, "path_a", "tradable")
    validate_assets(watchlist, "path_b", "watchlist")

    result = {
        "universe_dir": str(universe_dir.resolve()),
        "valid": not errors,
        "errors": errors,
    }
    print(json.dumps(result, indent=2))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
