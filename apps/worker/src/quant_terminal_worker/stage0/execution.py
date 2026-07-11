from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any, Callable

from quant_terminal_worker.stage0.information import run_stage0_information_gate
from quant_terminal_worker.stage0.workspace import (
    build_stage0_commands,
    materialize_stage0_workspace,
)

Stage0Runner = Callable[[list[str]], None]
Stage0InformationEvaluator = Callable[..., dict[str, Any]]


def execute_stage0_candidate(
    *,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    signal_set: dict[str, Any],
    signals: list[dict[str, Any]],
    candle_rows: list[dict[str, Any]],
    label_mode: str = "threshold_first_hit",
    runner: Stage0Runner | None = None,
) -> dict[str, Any]:
    run_command = runner or _run_subprocess
    stage0_dir = _stage0_candidate_dir(workspace_root=workspace_root, universe_run=universe_run, candidate=candidate)
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
        label_mode=label_mode,
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
            label_mode=label_mode,
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
        "label_mode": label_mode,
        "travel_distribution": travel_distribution.get("distribution", {}),
        "travel_mean_pct": travel_distribution.get("mean"),
        "stable_threshold_range": threshold_calibration.get("stable_range", []),
    }
    existing_information = _existing_stage0_information(candidate)
    if existing_information is not None:
        metrics["stage0_information"] = existing_information
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


def execute_stage0_information_gate(
    *,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    signal_set: dict[str, Any],
    signals: list[dict[str, Any]],
    candle_rows: list[dict[str, Any]],
    information_gate_evaluator: Stage0InformationEvaluator | None = None,
) -> dict[str, Any]:
    evaluate_information = information_gate_evaluator or run_stage0_information_gate
    stage0_dir = _stage0_candidate_dir(workspace_root=workspace_root, universe_run=universe_run, candidate=candidate)
    materialize_stage0_workspace(
        workspace_root=workspace_root,
        strategy_id=universe_run["universe_run_id"],
        signal_set=signal_set,
        signals=signals,
        candle_rows=candle_rows,
        stage0_dir=stage0_dir,
    )
    information_gate = evaluate_information(
        universe_run=universe_run,
        candidate=candidate,
        signals=signals,
        candle_rows=candle_rows,
    )
    _write_information_artifacts(stage0_dir=stage0_dir, information_gate=information_gate)
    updated_candidate = {
        **candidate,
        "last_error": {},
        "metrics": {
            **(candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}),
            "stage0_information": _stage0_information_metrics(information_gate),
        },
    }
    return {
        "candidate": updated_candidate,
        "commands": {},
        "artifact_root": str(stage0_dir),
        "information_gate": information_gate,
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


def _stage0_candidate_dir(*, workspace_root: Path, universe_run: dict[str, Any], candidate: dict[str, Any]) -> Path:
    return (
        workspace_root
        / "dev"
        / "stage0"
        / universe_run["universe_run_id"]
        / candidate["signal_engine_id"]
        / candidate["asset"]
        / candidate["signal_set_id"]
    )


def _existing_stage0_information(candidate: dict[str, Any]) -> dict[str, Any] | None:
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    information = metrics.get("stage0_information") if isinstance(metrics.get("stage0_information"), dict) else None
    return information


def _write_information_artifacts(*, stage0_dir: Path, information_gate: dict[str, Any]) -> None:
    scores_dir = stage0_dir / "scores"
    scores_dir.mkdir(parents=True, exist_ok=True)
    (scores_dir / "event_information.json").write_text(json.dumps(information_gate, indent=2, sort_keys=True))
    (scores_dir / "random_baseline.json").write_text(
        json.dumps(information_gate.get("random_baseline", {}), indent=2, sort_keys=True)
    )
    (scores_dir / "information_summary.json").write_text(
        json.dumps(_stage0_information_metrics(information_gate), indent=2, sort_keys=True)
    )


def _stage0_information_metrics(information_gate: dict[str, Any]) -> dict[str, Any]:
    summary = information_gate.get("summary_metrics") if isinstance(information_gate.get("summary_metrics"), dict) else {}
    train = (information_gate.get("splits") or {}).get("train", {}).get("score", {}) if isinstance(information_gate.get("splits"), dict) else {}
    walk_forward = (information_gate.get("splits") or {}).get("walk_forward", {}).get("score", {}) if isinstance(information_gate.get("splits"), dict) else {}
    monthly = information_gate.get("monthly_stability") if isinstance(information_gate.get("monthly_stability"), dict) else {}
    return {
        "schema_version": information_gate.get("schema_version", "stage0_information_gate.v1"),
        "status": information_gate.get("status"),
        "decision_reason": information_gate.get("decision_reason"),
        "train_event_count": summary.get("train_event_count", train.get("event_count")),
        "walk_forward_event_count": summary.get("walk_forward_event_count", walk_forward.get("event_count")),
        "train_median_lift_pct": summary.get("train_median_lift_pct", train.get("median_lift_pct")),
        "walk_forward_median_lift_pct": summary.get("walk_forward_median_lift_pct", walk_forward.get("median_lift_pct")),
        "train_empirical_p_value": summary.get("train_empirical_p_value", train.get("empirical_p_value")),
        "walk_forward_empirical_p_value": summary.get("walk_forward_empirical_p_value", walk_forward.get("empirical_p_value")),
        "train_q_value": summary.get("train_q_value", train.get("q_value")),
        "monthly_positive_lift_months": summary.get("monthly_positive_lift_months", monthly.get("positive_lift_months")),
        "monthly_eligible_months": summary.get("monthly_eligible_months", monthly.get("eligible_months")),
    }
