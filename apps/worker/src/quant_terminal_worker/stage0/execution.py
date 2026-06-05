from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from quant_terminal_worker.stage0.workspace import (
    build_stage0_commands,
    materialize_stage0_workspace,
)

Stage0Runner = Callable[[list[str]], None]


def execute_stage0_candidate(
    *,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    signal_set: dict[str, Any],
    signals: list[dict[str, Any]],
    candle_rows: list[dict[str, Any]],
    runner: Stage0Runner | None = None,
) -> dict[str, Any]:
    run_command = runner or _run_subprocess
    stage0_dir = (
        workspace_root
        / "dev"
        / "stage0"
        / universe_run["universe_run_id"]
        / candidate["signal_engine_id"]
        / candidate["asset"]
        / candidate["signal_set_id"]
    )
    materialized = materialize_stage0_workspace(
        workspace_root=workspace_root,
        strategy_id=universe_run["universe_run_id"],
        signal_set=signal_set,
        signals=signals,
        candle_rows=candle_rows,
        stage0_dir=stage0_dir,
    )
    vote_threshold = int(signal_set.get("manifest", {}).get("parameters", {}).get("vote_threshold", 0))
    initial_threshold = float(candidate.get("metrics", {}).get("significance_threshold_pct", 0.9))
    commands = build_stage0_commands(
        workspace_root=workspace_root,
        strategy_id=universe_run["universe_run_id"],
        asset=candidate["asset"],
        signal_engine_id=candidate["signal_engine_id"],
        signal_set_id=candidate["signal_set_id"],
        signal_packets_dir=materialized["signal_packets_dir"],
        candles_csv=materialized["candles_csv"],
        forward_hours=int(universe_run["forward_hours"]),
        vote_threshold=vote_threshold,
        significance_threshold_pct=initial_threshold,
        stage0_dir=stage0_dir,
    )

    run_command(commands["stage0a"])
    run_command(commands["stage0b"])
    chosen_threshold = _read_chosen_threshold(stage0_dir)
    commands = {
        **commands,
        "stage0c": build_stage0_commands(
            workspace_root=workspace_root,
            strategy_id=universe_run["universe_run_id"],
            asset=candidate["asset"],
            signal_engine_id=candidate["signal_engine_id"],
            signal_set_id=candidate["signal_set_id"],
            signal_packets_dir=materialized["signal_packets_dir"],
            candles_csv=materialized["candles_csv"],
            forward_hours=int(universe_run["forward_hours"]),
            vote_threshold=vote_threshold,
            significance_threshold_pct=chosen_threshold,
            stage0_dir=stage0_dir,
        )["stage0c"],
    }
    run_command(commands["stage0c"])

    summary = json.loads((stage0_dir / "scores" / "ground_truth_summary.json").read_text())
    travel_distribution = _read_json_if_exists(stage0_dir / "scores" / "travel_distribution.json")
    threshold_calibration = _read_json_if_exists(stage0_dir / "scores" / "threshold_calibration.json")
    metrics = {
        **summary.get("metrics", {}),
        "significance_threshold_pct": chosen_threshold,
        "artifact_root": str(stage0_dir),
        "travel_distribution": travel_distribution.get("distribution", {}),
        "travel_mean_pct": travel_distribution.get("mean"),
        "stable_threshold_range": threshold_calibration.get("stable_range", []),
    }
    trigger_rate_pct = metrics.get("trigger_rate_pct")
    accepted = (
        trigger_rate_pct is not None
        and float(trigger_rate_pct) >= float(universe_run["trigger_rate_threshold_pct"])
    )
    updated_candidate = {
        **candidate,
        "trigger_rate_pct": trigger_rate_pct,
        "branch_path": "path_a" if accepted else "path_b",
        "acceptance_status": "accepted" if accepted else "watchlist",
        "last_error": {},
        "metrics": metrics,
    }
    return {
        "candidate": updated_candidate,
        "commands": commands,
        "artifact_root": str(stage0_dir),
    }


def _run_subprocess(command: list[str]) -> None:
    subprocess.run(command, check=True, cwd=Path.cwd())


def _read_chosen_threshold(stage0_dir: Path) -> float:
    calibration_path = stage0_dir / "scores" / "threshold_calibration.json"
    calibration = json.loads(calibration_path.read_text())
    return float(calibration["chosen_threshold_pct"])


def _read_json_if_exists(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())
