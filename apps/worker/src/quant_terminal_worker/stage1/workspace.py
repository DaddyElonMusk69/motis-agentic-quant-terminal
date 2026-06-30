from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage1.scoring import SAMPLE_ROLE_ARTIFACTS


STARTER_STRATEGY = '''from __future__ import annotations

from quant_terminal_strategies.vegas_ema_base import decide

'''


def materialize_stage1_session_workspace(
    *,
    workspace_root: Path,
    session: dict[str, Any],
) -> dict[str, str]:
    artifact_root = Path(session["artifact_root"])
    if not artifact_root.is_absolute():
        artifact_root = workspace_root / artifact_root
    artifact_root.mkdir(parents=True, exist_ok=True)
    for folder in ("inputs", "iterations", "promotion"):
        (artifact_root / folder).mkdir(exist_ok=True)

    strategy_module_dir = artifact_root / "strategy_module"
    strategy_module_dir.mkdir(exist_ok=True)
    (strategy_module_dir / "__init__.py").write_text("")
    strategy_path = strategy_module_dir / "strategy.py"
    if not strategy_path.exists():
        seed_path_value = session.get("seed_strategy_source_path")
        if seed_path_value:
            seed_path = Path(seed_path_value)
            if not seed_path.is_absolute():
                seed_path = workspace_root / seed_path
            if not seed_path.is_file():
                raise ValueError(f"Seed strategy source not found: {seed_path}")
            strategy_path.write_text(seed_path.read_text())
        else:
            strategy_path.write_text(STARTER_STRATEGY)

    manifest = dict(session["manifest"])
    if session.get("seed_strategy_source_type") or session.get("seed_strategy_source_path"):
        manifest["seed_strategy"] = {
            "source_type": session.get("seed_strategy_source_type", "unknown"),
            "source_path": session.get("seed_strategy_source_path"),
            "source_version": session.get("seed_strategy_source_version"),
        }
    (artifact_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return {
        "artifact_root": str(artifact_root),
        "manifest_path": str(artifact_root / "manifest.json"),
        "strategy_path": str(strategy_path),
        "strategy_entrypoint": "strategy_module.strategy:decide",
    }


def create_stage1_iteration_workspace(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signals: list[dict[str, Any]],
    sample_method: str,
    bundle_role: str = "evaluator",
) -> dict[str, str]:
    materialize_stage1_session_workspace(workspace_root=workspace_root, session=session)
    artifact_root = _artifact_root(workspace_root, session)
    iteration_id = _next_iteration_id(artifact_root, session["strategy_version"])
    iteration_root = artifact_root / "iterations" / iteration_id
    for folder in ("decisions", "scores", "audits", "summaries", "source_artifacts"):
        (iteration_root / folder).mkdir(parents=True, exist_ok=True)

    snapshot_dir = iteration_root / "source_artifacts" / "strategy_module_snapshot"
    _copy_strategy_snapshot(artifact_root / "strategy_module", snapshot_dir)

    selected_signals = _all_window_signals(signals)
    sample = {
        "schema_version": "0.1",
        "sample_method": sample_method,
        "signal_count": len(selected_signals),
        "signals": [
            {
                "signal_id": signal["signal_id"],
                "timestamp": _iso_timestamp(signal["timestamp"]),
                "packet_path": _packet_path(workspace_root, session, signal),
                "packet": _packet_payload(signal),
            }
            for signal in selected_signals
        ],
        "selection_notes": {
            "ordering": "all signals in the selected Stage 1 window, emitted chronologically",
            "ground_truth_hidden": True,
            "future_candles_hidden": True,
        },
    }
    manifest = {
        "schema_version": "0.2",
        "iteration_id": iteration_id,
        "session_id": session["session_id"],
        "stage": "stage1a_directional_agreement",
        "asset": session["asset"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "signal_engine_id": session["signal_engine_id"],
        "signal_family": session["signal_engine_id"],
        "signal_set_id": session["signal_set_id"],
        "sample_method": sample_method,
        "signal_count": len(selected_signals),
        "contamination_controls": {
            "ground_truth_hidden": True,
            "future_candles_hidden": True,
            "prior_iteration_results_hidden": True,
            "proposed_fixes_hidden": True,
        },
        "handoff_path": "handoff.md",
        "signal_sample_path": "signal_sample.json",
        "strategy_module_snapshot": {
            "path": "source_artifacts/strategy_module_snapshot",
        },
        "outputs": {
            "decisions": "decisions/",
            "scores": "scores/",
            "audit": "audits/failure_audit.md",
            "summary": "summaries/iteration_summary.md",
        },
        "status": "created",
    }
    handoff = _render_handoff(session=session, iteration_id=iteration_id, sample=sample, iteration_root=iteration_root)
    evaluator_prompt = _render_evaluator_prompt(
        session=session,
        iteration_id=iteration_id,
        sample=sample,
        iteration_root=iteration_root,
        strategy_path=artifact_root / "strategy_module" / "strategy.py",
        snapshot_dir=snapshot_dir,
    )
    (iteration_root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    (iteration_root / "signal_sample.json").write_text(json.dumps(sample, indent=2) + "\n")
    (iteration_root / "handoff.md").write_text(handoff)
    (iteration_root / "agent_prompt.md").write_text(evaluator_prompt)
    result = {
        "iteration_id": iteration_id,
        "iteration_root": str(iteration_root),
        "manifest_path": str(iteration_root / "manifest.json"),
        "handoff_path": str(iteration_root / "handoff.md"),
        "signal_sample_path": str(iteration_root / "signal_sample.json"),
        "agent_prompt_path": str(iteration_root / "agent_prompt.md"),
        "strategy_snapshot_path": str(snapshot_dir),
        "sample_method": sample_method,
        "signal_count": len(selected_signals),
        "bundle_role": bundle_role,
    }
    if bundle_role == "strategy_builder":
        builder_sample = _build_training_sample(
            workspace_root=workspace_root,
            session=session,
            sample=sample,
            selected_signals=selected_signals,
        )
        builder_prompt = _render_strategy_builder_prompt(
            session=session,
            iteration_id=iteration_id,
            sample=sample,
            builder_sample=builder_sample,
            iteration_root=iteration_root,
            strategy_path=artifact_root / "strategy_module" / "strategy.py",
            snapshot_dir=snapshot_dir,
        )
        (iteration_root / "builder_training_sample.json").write_text(json.dumps(builder_sample, indent=2) + "\n")
        (iteration_root / "strategy_builder_prompt.md").write_text(builder_prompt)
        result["builder_prompt_path"] = str(iteration_root / "strategy_builder_prompt.md")
        result["builder_training_sample_path"] = str(iteration_root / "builder_training_sample.json")
    return result


def list_stage1_iterations(*, workspace_root: Path, session: dict[str, Any]) -> list[dict[str, Any]]:
    artifact_root = _artifact_root(workspace_root, session)
    iterations_root = artifact_root / "iterations"
    if not iterations_root.is_dir():
        return []
    return [
        _summarize_iteration(iteration_root=path)
        for path in sorted(iterations_root.glob("iter_*"), key=lambda item: item.name)
        if path.is_dir()
    ]


def read_stage1_iteration_detail(*, workspace_root: Path, session: dict[str, Any], iteration_id: str) -> dict[str, Any]:
    artifact_root = _artifact_root(workspace_root, session)
    iteration_root = artifact_root / "iterations" / iteration_id
    if not iteration_root.is_dir():
        raise FileNotFoundError(f"Stage 1 iteration not found: {iteration_id}")

    summary = _summarize_iteration(iteration_root=iteration_root)
    sample_role = summary.get("sample_method") if summary.get("sample_method") in SAMPLE_ROLE_ARTIFACTS else "training"
    score_path = iteration_root / "scores" / SAMPLE_ROLE_ARTIFACTS[sample_role]["scores"]
    score_payload = _read_json_if_exists(score_path)
    if score_payload is None:
        raise ValueError(f"Stage 1 iteration has not been scored for {sample_role}")

    sample_payload = json.loads((iteration_root / "signal_sample.json").read_text())
    signal_items = sample_payload.get("signals", []) if isinstance(sample_payload, dict) else []
    sample_by_signal_id = {
        str(item.get("signal_id")): item
        for item in signal_items
        if isinstance(item, dict) and item.get("signal_id")
    }

    records = []
    monthly_groups: dict[str, list[dict[str, Any]]] = {}
    for record in score_payload.get("records", []):
        if not isinstance(record, dict):
            continue
        sample_item = sample_by_signal_id.get(str(record.get("signal_id")), {})
        timestamp = sample_item.get("timestamp")
        detailed_record = {
            **record,
            "timestamp": timestamp,
            "packet_path": sample_item.get("packet_path"),
        }
        records.append(detailed_record)
        month_key = timestamp[:7] if isinstance(timestamp, str) and len(timestamp) >= 7 else "unknown"
        monthly_groups.setdefault(month_key, []).append(detailed_record)

    monthly = [
        {
            "month": month,
            "metrics": _detail_metrics(items),
        }
        for month, items in sorted(monthly_groups.items(), key=lambda item: item[0])
    ]

    return {
        "iteration_id": summary["iteration_id"],
        "sample_role": sample_role,
        "bundle_role": summary.get("bundle_role"),
        "signal_count": summary.get("signal_count", len(records)),
        "metrics": score_payload.get("metrics", {}),
        "records": records,
        "monthly": monthly,
        "score_path": str(score_path),
        "signal_sample_path": summary.get("signal_sample_path"),
    }


def read_stage4_candidate_detail(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candidate_id: str,
    source: str = "stage4_realized_expectancy",
) -> dict[str, Any]:
    artifact_root = _artifact_root(workspace_root, session)
    promotion_root = artifact_root / "promotion"
    if source == "stage4_realized_expectancy":
        realized_path = promotion_root / "stage4_realized_expectancy.json"
        ledger_path = promotion_root / "stage4_trade_ledger.json"
    elif source == "stage4b_timing":
        timing_root = promotion_root / "stage4b_timing"
        realized_path = timing_root / "timing_replay.json"
        ledger_path = timing_root / "timing_trade_ledger.json"
    else:
        raise ValueError(f"Unsupported Stage 4 detail source: {source}")
    realized = _read_json_if_exists(realized_path)
    ledger = _read_json_if_exists(ledger_path)
    if realized is None or ledger is None:
        raise FileNotFoundError(f"Stage 4 detail is not available for this session: {source}")

    candidate = next(
        (
            row
            for row in realized.get("candidates", [])
            if isinstance(row, dict) and str(row.get("candidate_id")) == candidate_id
        ),
        None,
    )
    if candidate is None:
        raise FileNotFoundError(f"Stage 4 candidate not found: {candidate_id}")

    ledger_candidate = next(
        (
            row
            for row in ledger.get("candidates", [])
            if isinstance(row, dict) and str(row.get("candidate_id")) == candidate_id
        ),
        None,
    )
    trades = ledger_candidate.get("trades", []) if isinstance(ledger_candidate, dict) else []
    if not isinstance(trades, list):
        trades = []

    return {
        "session_id": session["session_id"],
        "source": source,
        "run_id": realized.get("run_id"),
        "created_at": realized.get("created_at"),
        "candidate": candidate,
        "trade_count": len(trades),
        "trades": trades,
    }


def repair_stage1_iteration_bundle(*, workspace_root: Path, iteration_root: Path) -> dict[str, Any]:
    artifact_root = iteration_root.parent.parent
    session_manifest = _read_json_if_exists(artifact_root / "manifest.json") or {}
    iteration_manifest = _read_json_if_exists(iteration_root / "manifest.json") or {}
    session = {
        "session_id": session_manifest.get("session_id", artifact_root.name),
        "strategy_id": session_manifest.get("strategy_id", "unknown"),
        "strategy_version": session_manifest.get("strategy_version", iteration_manifest.get("iteration_id", "unknown").split("_")[-1]),
        "asset": session_manifest.get("asset", _asset_from_signal_set_key(session_manifest.get("signal_set_key"))),
        "signal_engine_id": session_manifest.get("signal_engine_id", "unknown"),
        "signal_set_id": session_manifest.get("signal_set_id", _signal_set_id_from_signal_set_key(session_manifest.get("signal_set_key"))),
        "signal_set_key": session_manifest.get("signal_set_key"),
        "artifact_root": str(artifact_root),
    }
    sample_path = iteration_root / "signal_sample.json"
    sample = json.loads(sample_path.read_text())
    repaired_signals = _repair_sample_signals(
        workspace_root=workspace_root,
        session=session,
        signal_items=sample.get("signals", []),
    )
    repaired_sample = {
        "schema_version": sample.get("schema_version", "0.1"),
        "sample_method": sample.get("sample_method", iteration_manifest.get("sample_method")),
        "signal_count": len(repaired_signals),
        "signals": repaired_signals,
        "selection_notes": sample.get("selection_notes")
        or {
            "ordering": "all signals in the selected Stage 1 window, emitted chronologically",
            "ground_truth_hidden": True,
            "future_candles_hidden": True,
        },
    }
    sample_path.write_text(json.dumps(repaired_sample, indent=2) + "\n")

    strategy_path = artifact_root / "strategy_module" / "strategy.py"
    snapshot_dir = iteration_root / "source_artifacts" / "strategy_module_snapshot"
    handoff = _render_handoff(
        session=session,
        iteration_id=iteration_manifest.get("iteration_id", iteration_root.name),
        sample=repaired_sample,
        iteration_root=iteration_root,
    )
    prompt = _render_evaluator_prompt(
        session=session,
        iteration_id=iteration_manifest.get("iteration_id", iteration_root.name),
        sample=repaired_sample,
        iteration_root=iteration_root,
        strategy_path=strategy_path,
        snapshot_dir=snapshot_dir,
    )
    (iteration_root / "handoff.md").write_text(handoff)
    (iteration_root / "agent_prompt.md").write_text(prompt)

    builder_sample_path = iteration_root / "builder_training_sample.json"
    if builder_sample_path.exists():
        builder_sample = json.loads(builder_sample_path.read_text())
        repaired_builder_signals = _repair_sample_signals(
            workspace_root=workspace_root,
            session=session,
            signal_items=builder_sample.get("signals", []),
        )
        repaired_builder_sample = {
            **builder_sample,
            "signal_count": len(repaired_builder_signals),
            "signals": repaired_builder_signals,
        }
        builder_sample_path.write_text(json.dumps(repaired_builder_sample, indent=2) + "\n")
        builder_prompt = _render_strategy_builder_prompt(
            session=session,
            iteration_id=iteration_manifest.get("iteration_id", iteration_root.name),
            sample=repaired_sample,
            builder_sample=repaired_builder_sample,
            iteration_root=iteration_root,
            strategy_path=strategy_path,
            snapshot_dir=snapshot_dir,
        )
        (iteration_root / "strategy_builder_prompt.md").write_text(builder_prompt)

    return {
        "iteration_root": str(iteration_root),
        "signal_count": len(repaired_signals),
        "builder_signal_count": len(repaired_builder_signals) if builder_sample_path.exists() else None,
    }


def build_stage1_gate_summary(*, workspace_root: Path, session: dict[str, Any]) -> dict[str, Any]:
    artifact_root = _artifact_root(workspace_root, session)
    iterations = list_stage1_iterations(workspace_root=workspace_root, session=session)
    latest_scores = _latest_role_scores(iterations)
    roles = {
        role: _role_gate_state(role=role, score=latest_scores.get(role))
        for role in ("training", "walk_forward_test")
    }
    blockers = [
        role_state["blocker"]
        for role_state in roles.values()
        if role_state.get("blocker")
    ]
    ready_to_freeze = not blockers
    canonical = _canonical_readout_state(artifact_root)
    stage2_capture = _stage2_capture_state(artifact_root)
    stage2_exit_policy = _stage2_exit_policy_state(artifact_root)
    stage3_grid = _stage3_grid_state(artifact_root)
    stage3_pyramid = _stage3_pyramid_state(artifact_root)
    stage4_realized_expectancy = _stage4_realized_expectancy_state(artifact_root)
    stage4b_timing = _stage4b_timing_state(artifact_root)
    promotion_candidate = _promotion_candidate_state(artifact_root)
    status = "canonical_complete" if canonical["exists"] else "ready_to_freeze" if ready_to_freeze else "blocked"
    if session.get("status") == "stage1a_frozen" and canonical["exists"]:
        status = "stage1a_frozen"
    return {
        "session_id": session["session_id"],
        "status": status,
        "ready_to_freeze": ready_to_freeze,
        "blockers": blockers,
        "roles": roles,
        "canonical_readout": canonical,
        "stage2_capture": stage2_capture,
        "stage2_exit_policy": stage2_exit_policy,
        "stage3_grid": stage3_grid,
        "stage3_pyramid": stage3_pyramid,
        "stage4_realized_expectancy": stage4_realized_expectancy,
        "stage4b_timing": stage4b_timing,
        "promotion_candidate": promotion_candidate,
        "downstream_contract": {
            "stage2_stage3": "Use the MATCH subset from promotion/stage1a_canonical_full_cycle_scores.json.",
            "stage4": "Use the full decision set from promotion/stage1a_canonical_full_cycle_decisions.json.",
        },
    }


def _artifact_root(workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _next_iteration_id(artifact_root: Path, strategy_version: str) -> str:
    iterations_root = artifact_root / "iterations"
    existing = [path.name for path in iterations_root.glob("iter_*") if path.is_dir()]
    next_number = len(existing) + 1
    return f"iter_{next_number:03d}_{strategy_version}"


def _latest_role_scores(iterations: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for iteration in iterations:
        for role, score in (iteration.get("scores") or {}).items():
            latest[role] = {
                **score,
                "iteration_id": iteration["iteration_id"],
                "sample_method": iteration.get("sample_method"),
            }
    return latest


def _role_gate_state(*, role: str, score: dict[str, Any] | None) -> dict[str, Any]:
    label = SAMPLE_ROLE_ARTIFACTS[role]["title"]
    if score is None:
        return {
            "role": role,
            "label": label,
            "status": "missing",
            "blocker": f"{label} has not been scored.",
            "score": None,
        }
    metrics = score.get("metrics", {})
    passed = bool(metrics.get("passes_threshold"))
    return {
        "role": role,
        "label": label,
        "status": "pass" if passed else "fail",
        "blocker": None if passed else f"{label} is below the Stage 1A agreement gate.",
        "score": score,
    }


def _canonical_readout_state(artifact_root: Path) -> dict[str, Any]:
    scores_path = artifact_root / "promotion" / "stage1a_canonical_full_cycle_scores.json"
    decisions_path = artifact_root / "promotion" / "stage1a_canonical_full_cycle_decisions.json"
    summary_path = artifact_root / "promotion" / "stage1a_canonical_full_cycle_summary.md"
    frozen_strategy_path = artifact_root / "promotion" / "frozen_stage1a_strategy_module" / "strategy.py"
    scores = _read_json_if_exists(scores_path)
    return {
        "exists": scores is not None and decisions_path.exists() and frozen_strategy_path.exists(),
        "scores_path": str(scores_path) if scores_path.exists() else None,
        "decisions_path": str(decisions_path) if decisions_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "frozen_strategy_path": str(frozen_strategy_path) if frozen_strategy_path.exists() else None,
        "metrics": scores.get("metrics", {}) if scores else {},
        "slice_metrics": scores.get("slice_metrics", {}) if scores else {},
        "match_count": len(scores.get("match_set", [])) if scores else 0,
    }


def _stage2_capture_state(artifact_root: Path) -> dict[str, Any]:
    capture_path = artifact_root / "promotion" / "stage2_capture_curve.json"
    per_signal_path = artifact_root / "promotion" / "stage2_capture_per_signal.json"
    stage3_inputs_path = artifact_root / "promotion" / "stage3_trade_inputs.json"
    summary_path = artifact_root / "promotion" / "stage2_summary.md"
    capture = _read_json_if_exists(capture_path)
    return {
        "exists": capture is not None and per_signal_path.exists() and stage3_inputs_path.exists() and summary_path.exists(),
        "capture_curve_path": str(capture_path) if capture_path.exists() else None,
        "per_signal_path": str(per_signal_path) if per_signal_path.exists() else None,
        "stage3_trade_inputs_path": str(stage3_inputs_path) if stage3_inputs_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "metrics": capture.get("metrics", {}) if capture else {},
        "results": capture.get("results", {}) if capture else {},
        "sl_results": capture.get("sl_results", {}) if capture else {},
        "side_splits": capture.get("side_splits", {}) if capture else {},
        "cohorts": capture.get("cohorts", {}) if capture else {},
        "stage3_input": capture.get("stage3_input", {}) if capture else {},
        "tp_levels": capture.get("tp_levels", []) if capture else [],
        "sl_levels": capture.get("sl_levels", []) if capture else [],
        "total_trade_decisions": (capture.get("metrics") or {}).get("total_trade_decisions", 0) if capture else 0,
        "match_count": (capture.get("metrics") or {}).get("match_count", 0) if capture else 0,
        "mismatch_count": (capture.get("metrics") or {}).get("mismatch_count", 0) if capture else 0,
        "recommended_tp_min_pct": (capture.get("stage3_input") or {}).get("recommended_tp_min_pct") if capture else None,
        "recommended_tp_max_pct": (capture.get("stage3_input") or {}).get("recommended_tp_max_pct") if capture else None,
        "recommended_sl_min_pct": (capture.get("stage3_input") or {}).get("recommended_sl_min_pct") if capture else None,
        "recommended_sl_max_pct": (capture.get("stage3_input") or {}).get("recommended_sl_max_pct") if capture else None,
    }


def _stage2_exit_policy_state(artifact_root: Path) -> dict[str, Any]:
    policy_path = artifact_root / "promotion" / "stage2_exit_policy.json"
    policy = _read_json_if_exists(policy_path)
    return {
        "exists": policy is not None,
        "policy_path": str(policy_path) if policy_path.exists() else None,
        "policy": policy.get("policy", {}) if policy else {},
        "policy_mode": policy.get("policy_mode", "shared") if policy else None,
        "side_policies": policy.get("side_policies", {}) if policy else {},
        "created_at": policy.get("created_at") if policy else None,
    }


def _stage3_grid_state(artifact_root: Path) -> dict[str, Any]:
    grid_path = artifact_root / "promotion" / "stage3_grid_results.json"
    optimal_path = artifact_root / "promotion" / "stage3_optimal.json"
    candidates_path = artifact_root / "promotion" / "stage4_candidates.json"
    summary_path = artifact_root / "promotion" / "stage3_summary.md"
    grid = _read_json_if_exists(grid_path)
    optimal = _read_json_if_exists(optimal_path)
    best = None
    if optimal:
        best = optimal.get("best")
    if best is None and grid:
        best = (grid.get("optimal") or {}).get("best")
    fixed_result = _compact_stage3_result(grid.get("fixed_sl_baseline_result", {}) if grid else {})
    exact_result = _compact_stage3_result(grid.get("exact_protection_result", {}) if grid else {})
    exact_policy_result = _compact_stage3_result(grid.get("exact_policy_result", {}) if grid else {})
    shortlist = [_compact_stage3_result(item) for item in grid.get("stage3c_shortlist", [])] if grid else []
    top_5 = [_compact_stage3_result(item) for item in (optimal.get("top_5") if optimal else (grid.get("optimal") or {}).get("top_5") if grid else []) or []]
    fixed_complete = bool(grid and grid.get("fixed_sl_complete")) or bool(grid and grid.get("fixed_sl_baseline_result"))
    exact_complete = bool(grid and grid.get("exact_protection_complete")) or bool(
        grid and (grid.get("exact_protection_result") or grid.get("exact_policy_result"))
    )
    local_complete = bool(grid and grid.get("local_variants_complete")) or bool(
        grid is not None and optimal is not None and candidates_path.exists() and summary_path.exists()
    )
    return {
        "exists": local_complete,
        "fixed_sl_complete": fixed_complete,
        "exact_protection_complete": exact_complete,
        "local_variants_complete": local_complete,
        "grid_results_path": str(grid_path) if grid_path.exists() else None,
        "optimal_path": str(optimal_path) if optimal_path.exists() else None,
        "stage4_candidates_path": str(candidates_path) if candidates_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "total_signals": grid.get("total_signals", 0) if grid else 0,
        "total_executable_decisions": grid.get("total_executable_decisions", grid.get("total_signals", 0)) if grid else 0,
        "forward_hours": grid.get("forward_hours") if grid else None,
        "leverage": grid.get("leverage") if grid else None,
        "tp_range_source": grid.get("tp_range_source") if grid else None,
        "tp_values": grid.get("tp_values", []) if grid else [],
        "sl_values": grid.get("sl_values", []) if grid else [],
        "fees_bps_per_side": grid.get("fees_bps_per_side") if grid else None,
        "stage0_risk_policy": grid.get("stage0_risk_policy", {}) if grid else {},
        "stage2_exit_policy": grid.get("stage2_exit_policy", {}) if grid else {},
        "fixed_sl_baseline_result": fixed_result,
        "exact_protection_result": exact_result,
        "exact_policy_result": exact_policy_result,
        "stage3c_total_combinations_tested": grid.get("stage3c_total_combinations_tested", 0) if grid else 0,
        "stage3c_value_ranges": grid.get("stage3c_value_ranges", {}) if grid else {},
        "stage3c_shortlist": shortlist,
        "best": _compact_stage3_result(best or {}),
        "top_5": top_5,
    }


def _stage3_pyramid_state(artifact_root: Path) -> dict[str, Any]:
    results_path = artifact_root / "promotion" / "stage3_pyramid_results.json"
    optimal_path = artifact_root / "promotion" / "stage3_pyramid_optimal.json"
    candidates_path = artifact_root / "promotion" / "stage4_candidates.json"
    summary_path = artifact_root / "promotion" / "stage3_pyramid_summary.md"
    results = _read_json_if_exists(results_path)
    optimal = _read_json_if_exists(optimal_path)
    best = optimal.get("best") if optimal else None
    return {
        "exists": results is not None and optimal is not None and candidates_path.exists() and summary_path.exists(),
        "results_path": str(results_path) if results_path.exists() else None,
        "optimal_path": str(optimal_path) if optimal_path.exists() else None,
        "stage4_candidates_path": str(candidates_path) if candidates_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "total_signals": results.get("total_signals", 0) if results else 0,
        "tp_pct": results.get("tp_pct") if results else None,
        "sl_pct": results.get("sl_pct") if results else None,
        "max_legs": results.get("max_legs") if results else None,
        "sl_breakeven": results.get("sl_breakeven") if results else None,
        "baseline": _compact_stage3_result(results.get("baseline", {}) if results else {}),
        "best": _compact_stage3_result(best or {}),
        "results": [_compact_stage3_result(item) for item in results.get("results", [])] if results else [],
    }


def _stage4_realized_expectancy_state(artifact_root: Path) -> dict[str, Any]:
    realized_path = artifact_root / "promotion" / "stage4_realized_expectancy.json"
    ledger_path = artifact_root / "promotion" / "stage4_trade_ledger.json"
    optimal_path = artifact_root / "promotion" / "stage4_optimal.json"
    summary_path = artifact_root / "promotion" / "stage4_summary.md"
    run_index_path = artifact_root / "promotion" / "stage4_runs" / "index.json"
    realized = _read_json_if_exists(realized_path)
    optimal = _read_json_if_exists(optimal_path)
    run_index = _read_json_if_exists(run_index_path) or {}
    best = None
    if optimal:
        best = optimal.get("best")
    if best is None and realized:
        best = realized.get("best_candidate")
    if realized:
        best = _stage4_display_best_candidate(realized, best or {})
    return {
        "exists": realized is not None and ledger_path.exists() and optimal is not None and summary_path.exists(),
        "realized_expectancy_path": str(realized_path) if realized_path.exists() else None,
        "trade_ledger_path": str(ledger_path) if ledger_path.exists() else None,
        "optimal_path": str(optimal_path) if optimal_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "best_candidate_id": best.get("candidate_id") if best else realized.get("best_candidate_id") if realized else None,
        "best_candidate": _compact_stage4_candidate(best or {}),
        "candidates": [_compact_stage4_candidate(item) for item in realized.get("candidates", [])] if realized else [],
        "latest_run_id": realized.get("run_id") if realized else run_index.get("latest_run_id"),
        "latest_simulation_inputs": realized.get("simulation_inputs", {}) if realized else {},
        "latest_account": (best or {}).get("account", {}) if best else {},
        "stage4_runs": _stage4_run_history_rows(run_index),
    }


def _stage4_run_history_rows(run_index: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for run in run_index.get("runs", []):
        if not isinstance(run, dict):
            continue
        realized_path = Path(str(run.get("realized_expectancy_path") or ""))
        realized = _read_json_if_exists(realized_path) if realized_path.is_file() else None
        if not realized:
            rows.append(run)
            continue
        stored_best = run.get("best_candidate") or realized.get("best_candidate") or {}
        best = _stage4_display_best_candidate(realized, stored_best)
        rows.append(
            {
                **run,
                "best_candidate_id": best.get("candidate_id") or run.get("best_candidate_id"),
                "best_candidate": _compact_stage4_candidate(best),
                "account": best.get("account") or run.get("account"),
            }
        )
    return rows


def _stage4_display_best_candidate(realized: dict[str, Any], stored_best: dict[str, Any]) -> dict[str, Any]:
    candidates = _stage4_candidate_rows(realized, stored_best)
    protected_eligible = [candidate for candidate in candidates if _candidate_has_protected_sl(candidate) and _wf_net_pnl_pct(candidate) > 0]
    if protected_eligible:
        selected = max(
            protected_eligible,
            key=lambda candidate: (
                _wf_net_pnl_pct(candidate),
                _wf_profit_factor(candidate),
                _overall_net_pnl_usdt(candidate),
            ),
        )
        return {**selected, "selection_mode": "protected_walk_forward_net_pnl_pct"}
    return stored_best


def _stage4b_timing_state(artifact_root: Path) -> dict[str, Any]:
    timing_root = artifact_root / "promotion" / "stage4b_timing"
    prompt_path = timing_root / "timing_optimizer_prompt.md"
    context_path = timing_root / "timing_context.json"
    overlay_path = timing_root / "timing_overlay.json"
    replay_path = timing_root / "timing_replay.json"
    ledger_path = timing_root / "timing_trade_ledger.json"
    summary_path = timing_root / "timing_summary.md"
    run_index_path = timing_root / "stage4b_runs" / "index.json"
    replay = _read_json_if_exists(replay_path)
    overlay = _read_json_if_exists(overlay_path)
    run_index = _read_json_if_exists(run_index_path) or {}
    best = replay.get("best_candidate") if replay else {}
    return {
        "exists": replay is not None and ledger_path.exists() and summary_path.exists(),
        "prompt_exists": prompt_path.exists(),
        "context_exists": context_path.exists(),
        "overlay_exists": overlay_path.exists(),
        "prompt_path": str(prompt_path) if prompt_path.exists() else None,
        "context_path": str(context_path) if context_path.exists() else None,
        "overlay_path": str(overlay_path) if overlay_path.exists() else None,
        "overlay_profile": _stage4b_overlay_profile(overlay),
        "timing_replay_path": str(replay_path) if replay_path.exists() else None,
        "timing_trade_ledger_path": str(ledger_path) if ledger_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "latest_run_id": replay.get("run_id") if replay else run_index.get("latest_run_id"),
        "best_candidate_id": replay.get("best_candidate_id") if replay else None,
        "best_candidate": _compact_stage4_candidate(best or {}),
        "candidates": [_compact_stage4_candidate(item) for item in replay.get("candidates", [])] if replay else [],
        "latest_account": (best or {}).get("account", {}) if best else {},
        "stage4b_runs": run_index.get("runs", []),
    }


def _stage4b_overlay_profile(overlay: dict[str, Any] | None) -> dict[str, Any] | None:
    if not overlay:
        return None
    return {
        "exclude_utc_hours": overlay.get("exclude_utc_hours") or [],
        "exclude_utc_weekdays": overlay.get("exclude_utc_weekdays") or [],
        "applies_to": overlay.get("applies_to") or "all",
    }


def _promotion_candidate_state(artifact_root: Path) -> dict[str, Any]:
    promotion_root = artifact_root / "promotion"
    stage4_path = promotion_root / "stage4_realized_expectancy.json"
    optimal_path = promotion_root / "stage4_optimal.json"
    if not stage4_path.is_file() or not optimal_path.is_file():
        return {"exists": False, "source": None, "candidate_id": None}
    stage4 = _read_json_if_exists(stage4_path) or {}
    optimal = _read_json_if_exists(optimal_path) or {}
    stage4_best = optimal.get("best") or stage4.get("best_candidate") or {}
    candidates = [_promotion_candidate_item("stage4_realized_expectancy", "Stage 4A", row, timing_skips=0) for row in _stage4_candidate_rows(stage4, stage4_best)]
    timing_root = promotion_root / "stage4b_timing"
    replay = _read_json_if_exists(timing_root / "timing_replay.json")
    overlay = _read_json_if_exists(timing_root / "timing_overlay.json")
    if replay and overlay and str(overlay.get("source_stage4_run_id") or "") == str(stage4.get("run_id") or ""):
        candidates.extend(_promotion_candidate_item("stage4b_timing", "Stage 4B Timing", row, timing_skips=row.get("skipped_timing_filter", 0)) for row in _stage4_candidate_rows(replay, replay.get("best_candidate") or {}))
    protected_eligible = [candidate for candidate in candidates if _candidate_has_protected_sl(candidate.get("raw_candidate") or {}) and candidate["walk_forward_net_pnl_pct"] > 0]
    if protected_eligible:
        selected = max(protected_eligible, key=_promotion_candidate_rank)
        return _public_promotion_candidate({**selected, "criterion": "protected_walk_forward_net_pnl_pct"})
    eligible = [candidate for candidate in candidates if candidate["walk_forward_net_pnl_pct"] > 0]
    if eligible:
        return _public_promotion_candidate(max(eligible, key=_promotion_candidate_rank))
    selected = max(candidates, key=lambda item: (item["overall_net_pnl_usdt"], item["source"] == "stage4_realized_expectancy"))
    return _public_promotion_candidate({**selected, "warning": "weak_walk_forward_fallback"})


def _stage4_candidate_rows(payload: dict[str, Any], fallback_best: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [row for row in payload.get("candidates", []) if isinstance(row, dict) and row.get("candidate_id")]
    return rows or ([fallback_best] if fallback_best.get("candidate_id") else [])


def _promotion_candidate_item(source: str, label: str, candidate: dict[str, Any], *, timing_skips: int | None) -> dict[str, Any]:
    return {
        "exists": True,
        "source": source,
        "label": label,
        "candidate_id": candidate.get("candidate_id"),
        "best_candidate": _compact_stage4_candidate(candidate),
        "walk_forward_net_pnl_pct": _wf_net_pnl_pct(candidate),
        "walk_forward_profit_factor": _wf_profit_factor(candidate),
        "overall_net_pnl_usdt": _overall_net_pnl_usdt(candidate),
        "timing_skips": timing_skips or 0,
        "warning": None,
        "raw_candidate": candidate,
    }


def _public_promotion_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in candidate.items() if key != "raw_candidate"}


def _promotion_candidate_rank(candidate: dict[str, Any]) -> tuple[float, float, float, bool]:
    return (
        candidate["walk_forward_net_pnl_pct"],
        candidate["walk_forward_profit_factor"],
        candidate["overall_net_pnl_usdt"],
        candidate["source"] == "stage4_realized_expectancy",
    )


def _candidate_has_protected_sl(candidate: dict[str, Any]) -> bool:
    setup = candidate.get("setup") if isinstance(candidate.get("setup"), dict) else candidate
    if bool(setup.get("protection_enabled")):
        return True
    side_policies = setup.get("side_policies") if isinstance(setup.get("side_policies"), dict) else {}
    return any(isinstance(policy, dict) and bool(policy.get("protection_enabled")) for policy in side_policies.values())


def _wf_net_pnl_pct(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("net_pnl_pct"), 0.0)


def _wf_profit_factor(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("profit_factor"), 0.0)


def _overall_net_pnl_usdt(best: dict[str, Any]) -> float:
    account = best.get("account") or {}
    return _float_or_default(account.get("net_pnl_usdt"), 0.0)


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _compact_stage3_result(result: Any) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    heavy_keys = {"outcomes", "trades", "ledger", "signals", "records", "decisions"}
    return {
        key: value
        for key, value in result.items()
        if key not in heavy_keys
    }


def _compact_stage4_candidate(candidate: Any) -> dict[str, Any]:
    if not isinstance(candidate, dict):
        return {}
    heavy_keys = {"trades", "ledger", "trade_ledger", "executions", "decisions"}
    return {
        key: value
        for key, value in candidate.items()
        if key not in heavy_keys
    }


def _copy_strategy_snapshot(source_dir: Path, snapshot_dir: Path) -> None:
    if snapshot_dir.exists():
        shutil.rmtree(snapshot_dir)
    shutil.copytree(source_dir, snapshot_dir)


def _summarize_iteration(*, iteration_root: Path) -> dict[str, Any]:
    manifest = _read_json_if_exists(iteration_root / "manifest.json") or {}
    audit_path = iteration_root / "audits" / "failure_audit.json"
    audit = _read_json_if_exists(audit_path)
    role_scores = _role_scores(iteration_root)
    training_score = role_scores.get("training")
    return {
        "iteration_id": manifest.get("iteration_id", iteration_root.name),
        "iteration_root": str(iteration_root),
        "sample_method": manifest.get("sample_method"),
        "signal_count": manifest.get("signal_count", manifest.get("sample_size")),
        "status": manifest.get("status", "created"),
        "bundle_role": "strategy_builder" if (iteration_root / "strategy_builder_prompt.md").exists() else "evaluator",
        "manifest_path": str(iteration_root / "manifest.json"),
        "signal_sample_path": str(iteration_root / "signal_sample.json"),
        "agent_prompt_path": str(iteration_root / "agent_prompt.md"),
        "builder_prompt_path": str(iteration_root / "strategy_builder_prompt.md")
        if (iteration_root / "strategy_builder_prompt.md").exists()
        else None,
        "builder_training_sample_path": str(iteration_root / "builder_training_sample.json")
        if (iteration_root / "builder_training_sample.json").exists()
        else None,
        "scores": role_scores,
        "has_training_score": training_score is not None,
        "training_score": training_score,
        "has_failure_audit": audit is not None,
        "failure_audit": {
            "audit_json_path": str(audit_path),
            "audit_md_path": str(iteration_root / "audits" / "failure_audit.md"),
            "agent_prompt_path": str(iteration_root / "agent_failure_audit_prompt.md"),
            "metrics": audit.get("metrics", {}),
        }
        if audit is not None
        else None,
    }


def _role_scores(iteration_root: Path) -> dict[str, dict[str, Any]]:
    scores = {}
    for role, artifacts in SAMPLE_ROLE_ARTIFACTS.items():
        score_path = iteration_root / "scores" / artifacts["scores"]
        score = _read_json_if_exists(score_path)
        if score is None:
            continue
        scores[role] = {
            "scores_path": str(score_path),
            "decisions_path": str(iteration_root / "decisions" / artifacts["decisions"]),
            "summary_path": str(iteration_root / "summaries" / artifacts["summary"]),
            "metrics": score.get("metrics", {}),
        }
    return scores


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else None


def _detail_metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    matches = sum(1 for record in records if record.get("agreement") == "MATCH")
    mismatches = sum(1 for record in records if record.get("agreement") == "MISMATCH")
    neutral = sum(1 for record in records if record.get("agreement") == "NEUTRAL")
    scoreable = matches + mismatches
    directional_agreement = round(matches / scoreable, 6) if scoreable else 0
    threshold = 55.0
    return {
        "total": len(records),
        "matches": matches,
        "mismatches": mismatches,
        "neutral": neutral,
        "scoreable": scoreable,
        "directional_agreement": directional_agreement,
        "promotion_threshold_pct": threshold,
        "passes_threshold": directional_agreement * 100 >= threshold,
    }


def _all_window_signals(signals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    selected: dict[str, dict[str, Any]] = {}
    for signal in sorted(signals, key=lambda item: (_iso_timestamp(item["timestamp"]), item["signal_id"])):
        timestamp = _iso_timestamp(signal["timestamp"])
        current = selected.get(timestamp)
        if current is None or _signal_rank(signal) > _signal_rank(current):
            selected[timestamp] = signal
    return list(selected.values())


def _packet_path(workspace_root: Path, session: dict[str, Any], signal: dict[str, Any]) -> str:
    signal_set_key = signal.get("signal_set_key", "")
    if signal_set_key.count(":") >= 2:
        _, asset, signal_set_id = signal_set_key.split(":", 2)
    else:
        asset = session["asset"]
        signal_set_id = session["signal_set_id"]
    packet_name = signal["signal_id"].split(":")[-1]
    preferred = (
        workspace_root
        / "dev"
        / "signals"
        / signal.get("signal_engine_id", session["signal_engine_id"])
        / asset
        / signal_set_id
        / "packets"
        / f"{packet_name}.json"
    )
    if preferred.exists():
        return str(preferred)
    fallback = _discover_existing_packet_path(
        workspace_root=workspace_root,
        signal_engine_id=signal.get("signal_engine_id", session["signal_engine_id"]),
        asset=asset,
        packet_name=packet_name,
    )
    return str(fallback or preferred)


def _packet_payload(signal: dict[str, Any]) -> dict[str, Any]:
    payload = signal.get("payload")
    if not isinstance(payload, dict):
        return {}
    blocked_keys = {
        "future_ground_truth",
        "ground_truth",
        "natural_direction",
        "first_move_direction",
        "first_move_pct",
    }
    return {
        key: value
        for key, value in payload.items()
        if key not in blocked_keys
    }


def _render_handoff(
    *,
    session: dict[str, Any],
    iteration_id: str,
    sample: dict[str, Any],
    iteration_root: Path,
) -> str:
    return f"""# Stage 1A Evaluator Handoff

Session: {session['session_id']}
Iteration: {iteration_id}
Iteration root: {iteration_root}
Strategy snapshot: {iteration_root / "source_artifacts" / "strategy_module_snapshot"}
Stage: stage1a_directional_agreement
Signal set: {session['signal_set_id']}
Signal count: {sample['signal_count']}

Process every entry in signal_sample.json sequentially, one at a time, using that file as the checklist.

Rules:
- Use only the embedded `packet` JSON in signal_sample.json and the strategy module snapshot.
- Treat `packet_path` fields as debug metadata only; use the embedded packet JSON as the evaluated input.
- Evaluate only the listed signal_sample.json entries.
- Preserve the listed order exactly.
- Do not scan any signal folder for additional packets.
- Do not use future outcomes.
- Do not use ground truth, future candles, previous scores, proposed fixes, filenames, neighboring signals, scripts, formulas, or batch heuristics.
- A decision is valid only after the embedded packet JSON has been read in full and evaluated directly against the strategy snapshot.

Output JSON only under {iteration_root / "decisions" / "stage1a_directional_decisions.json"}.
"""


def _build_training_sample(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    sample: dict[str, Any],
    selected_signals: list[dict[str, Any]],
) -> dict[str, Any]:
    ground_truth_records = _load_ground_truth_records(workspace_root=workspace_root, session=session)
    builder_signals = []
    missing_labels = []
    for item, signal in zip(sample["signals"], selected_signals, strict=True):
        signal_id = item["signal_id"]
        ground_truth = _find_ground_truth_record(ground_truth_records, signal_id)
        if ground_truth is None:
            missing_labels.append(signal_id)
            continue
        builder_signals.append(
            {
                **item,
                "ground_truth": _training_ground_truth_view(ground_truth),
                "payload_diagnostics": _payload_diagnostics(signal.get("payload", {})),
            }
        )
    if missing_labels:
        joined = ", ".join(missing_labels[:5])
        suffix = "..." if len(missing_labels) > 5 else ""
        raise ValueError(f"Missing Stage 0 training labels for selected signals: {joined}{suffix}")
    return {
        "schema_version": "0.1",
        "session_id": session["session_id"],
        "iteration_id": sample.get("iteration_id"),
        "sample_method": sample["sample_method"],
        "signal_count": len(builder_signals),
        "ground_truth_visible": True,
        "allowed_use": "strategy_builder_training_only",
        "forbidden_use": ["walk_forward_test", "evaluator_handoff"],
        "signals": builder_signals,
        "notes": {
            "label_source": "Stage 0 natural direction records inside the training window",
            "walk_forward_hidden": True,
        },
    }


def _find_ground_truth_record(records: dict[str, dict[str, Any]], signal_id: str) -> dict[str, Any] | None:
    for key in _signal_label_keys(signal_id):
        if key in records:
            return records[key]
    return None


def _signal_label_keys(signal_id: str) -> list[str]:
    keys = [signal_id]
    if ":" in signal_id:
        keys.append(signal_id.split(":")[-1])
    return keys


def _repair_sample_signals(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_signals = []
    signal_set_key = session.get("signal_set_key") or _session_signal_set_key(session)
    for item in signal_items:
        normalized_signals.append(
            {
                **item,
                "signal_id": item["signal_id"],
                "timestamp": item["timestamp"],
                "payload": item.get("packet", {}),
                "signal_engine_id": session["signal_engine_id"],
                "signal_set_key": signal_set_key,
            }
        )
    selected = _all_window_signals(normalized_signals)
    return [
        {
            **{key: value for key, value in signal.items() if key not in {"payload", "signal_engine_id", "signal_set_key"}},
            "signal_id": signal["signal_id"],
            "timestamp": _iso_timestamp(signal["timestamp"]),
            "packet_path": _packet_path(workspace_root, session, signal),
            "packet": _packet_payload({"payload": signal.get("payload", {})}),
        }
        for signal in selected
    ]


def _signal_rank(signal: dict[str, Any]) -> tuple[int, int, str]:
    signal_id = str(signal.get("signal_id") or "")
    canonical_signal_id = _canonical_signal_id(signal)
    return (
        1 if signal_id == canonical_signal_id else 0,
        1 if _signal_id_matches_signal_set(signal) else 0,
        signal_id,
    )


def _canonical_signal_id(signal: dict[str, Any]) -> str:
    signal_set_key = str(signal.get("signal_set_key") or "")
    if signal_set_key.count(":") < 2:
        return str(signal.get("signal_id") or "")
    signal_engine_id, asset, signal_set_id = signal_set_key.split(":", 2)
    timestamp = _iso_timestamp(signal["timestamp"]).replace("-", "").replace(":", "")
    return f"{signal_engine_id}:{asset}:{signal_set_id}:{timestamp}"


def _signal_id_matches_signal_set(signal: dict[str, Any]) -> bool:
    signal_id = str(signal.get("signal_id") or "")
    signal_set_key = str(signal.get("signal_set_key") or "")
    if signal_set_key.count(":") < 2:
        return False
    _, _, signal_set_id = signal_set_key.split(":", 2)
    return f":{signal_set_id}:" in signal_id


def _discover_existing_packet_path(
    *,
    workspace_root: Path,
    signal_engine_id: str,
    asset: str,
    packet_name: str,
) -> Path | None:
    packets_root = workspace_root / "dev" / "signals" / signal_engine_id / asset
    if not packets_root.is_dir():
        return None
    matches = sorted(packets_root.glob(f"*/packets/{packet_name}.json"))
    return matches[0] if matches else None


def _session_signal_set_key(session: dict[str, Any]) -> str:
    return f"{session['signal_engine_id']}:{session['asset']}:{session['signal_set_id']}"


def _asset_from_signal_set_key(signal_set_key: Any) -> str:
    if isinstance(signal_set_key, str) and signal_set_key.count(":") >= 2:
        return signal_set_key.split(":", 2)[1]
    return "unknown"


def _signal_set_id_from_signal_set_key(signal_set_key: Any) -> str:
    if isinstance(signal_set_key, str) and signal_set_key.count(":") >= 2:
        return signal_set_key.split(":", 2)[2]
    return "unknown"


def _load_ground_truth_records(*, workspace_root: Path, session: dict[str, Any]) -> dict[str, dict[str, Any]]:
    stage0_root_value = session.get("stage0_artifact_root") or session.get("manifest", {}).get("stage0_artifact_root")
    if not stage0_root_value:
        raise ValueError("Stage 1 builder bundle requires stage0_artifact_root")
    stage0_root = Path(stage0_root_value)
    if not stage0_root.is_absolute():
        stage0_root = workspace_root / stage0_root
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    if not ground_truth_root.is_dir():
        raise ValueError(f"Stage 0 ground truth directory not found: {ground_truth_root}")
    records: dict[str, dict[str, Any]] = {}
    for path in ground_truth_root.glob("*.json"):
        if path.name == "distribution.json":
            continue
        payload = json.loads(path.read_text())
        payloads = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            signal_id = str(item.get("signal_id") or path.stem)
            records[signal_id] = item
    return records


def _training_ground_truth_view(record: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "signal_id",
        "natural_direction",
        "first_move_pct",
        "max_travel_pct",
        "opposite_max_pct",
        "first_move_hours",
        "reversed",
        "status",
        "significance_threshold_pct",
    )
    return {field: record[field] for field in fields if field in record}


def _payload_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        key: payload[key]
        for key in (
            "signal_type",
            "direction_hint",
            "votes",
            "vote_count",
            "timeframe",
            "features",
            "diagnostics",
        )
        if key in payload
    }


def _render_evaluator_prompt(
    *,
    session: dict[str, Any],
    iteration_id: str,
    sample: dict[str, Any],
    iteration_root: Path,
    strategy_path: Path,
    snapshot_dir: Path,
) -> str:
    return f"""You are the evaluator/backtester for a Stage 1A strategy-development iteration.

Read:
- {iteration_root / "handoff.md"}
- {iteration_root / "signal_sample.json"}
- {snapshot_dir}

Session: {session['session_id']}
Iteration: {iteration_id}
Strategy: {session['strategy_id']}@{session['strategy_version']}
Session strategy file: {strategy_path}

Your task is to evaluate exactly {sample['signal_count']} neutral signal packets and produce JSON decisions.
Use only the embedded packet JSON in signal_sample.json and the strategy module snapshot. Do not use future outcomes, ground truth, score files, proposed fixes, global signal folders, or any packets not listed in signal_sample.json.
"""


def _render_strategy_builder_prompt(
    *,
    session: dict[str, Any],
    iteration_id: str,
    sample: dict[str, Any],
    builder_sample: dict[str, Any],
    iteration_root: Path,
    strategy_path: Path,
    snapshot_dir: Path,
) -> str:
    return f"""You are the strategy-builder agent for a Stage 1A deterministic strategy-script iteration.

Session: {session['session_id']}
Iteration: {iteration_id}
Strategy: {session['strategy_id']}@{session['strategy_version']}
Stage: Stage 1A directional agreement
Iteration root: {iteration_root}
Session strategy file to edit: {strategy_path}
Read-only strategy snapshot for this iteration: {snapshot_dir}

Read:
- {iteration_root / "manifest.json"}
- {iteration_root / "builder_training_sample.json"}
- {iteration_root / "signal_sample.json"}
- {strategy_path}
- {snapshot_dir}

Task:
Edit only {strategy_path} so deterministic `decide(...)` improves Stage 1A directional agreement on the training sample.

Training sample:
- sample method: {sample['sample_method']}
- signal count: {builder_sample['signal_count']}
- Use training-window natural_direction labels in builder_training_sample.json.
- Inspect embedded training packet JSON in builder_training_sample.json and signal_sample.json.
- Treat {snapshot_dir} as read-only evidence of what this iteration started from.

Rules:
- Return a deterministic StrategyDecision-compatible object with confidence, reason_code, and diagnostics.
- Stage 1A is direction-only. Do not add Stage 1B entry gates, expected-travel filters, TP/SL logic, live execution, randomness, network access, or exchange calls.
- Do not use validation, walk-forward, locked OOS, live state, or future candles.
- Do not modify signal packets, Stage 0 evidence, sample files, evaluator handoff files, or the read-only snapshot.
- Do not claim promotion readiness from this training bundle.
- New Stage 1 bundles automatically snapshot the current session strategy file into their own source_artifacts/strategy_module_snapshot folder.
- After editing, the user should rerun Score on this iteration.

After editing, summarize the changed deterministic rules and the training failure patterns they target in {iteration_root / "summaries" / "iteration_summary.md"}.
"""


def _iso_timestamp(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)
