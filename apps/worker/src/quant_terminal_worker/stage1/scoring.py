from __future__ import annotations

from datetime import UTC, datetime
import importlib.util
import json
from pathlib import Path
import shutil
import sys
from typing import Any


PROMOTION_THRESHOLD_PCT = 55.0


SAMPLE_ROLE_ARTIFACTS = {
    "recent_regime_train": {
        "decisions": "stage1a_directional_decisions.json",
        "scores": "stage1a_directional_scores.json",
        "summary": "iteration_summary.md",
        "title": "Stage 1A Training Score",
    },
    "forward_validation": {
        "decisions": "stage1a_forward_validation_decisions.json",
        "scores": "stage1a_forward_validation_scores.json",
        "summary": "forward_validation_summary.md",
        "title": "Stage 1A Forward Validation Score",
    },
    "locked_recent_oos": {
        "decisions": "stage1a_locked_oos_decisions.json",
        "scores": "stage1a_locked_oos_scores.json",
        "summary": "locked_oos_summary.md",
        "title": "Stage 1A Locked OOS Score",
    },
}


def run_stage1a_training_score(*, iteration_root: Path) -> dict[str, Any]:
    return run_stage1a_score(iteration_root=iteration_root, sample_role="recent_regime_train")


def run_stage1a_score(*, iteration_root: Path, sample_role: str) -> dict[str, Any]:
    if sample_role not in SAMPLE_ROLE_ARTIFACTS:
        raise ValueError(f"Unsupported Stage 1A sample role: {sample_role}")
    iteration_root = iteration_root.resolve()
    sample = json.loads((iteration_root / "signal_sample.json").read_text())
    training_sample = _read_optional_json(iteration_root / "builder_training_sample.json") or {}
    decide = _load_decide(_strategy_path_for_iteration(iteration_root))
    labels = _labels_for_iteration(iteration_root=iteration_root, training_sample=training_sample)

    decisions: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    for item in sample.get("signals", []):
        signal = _load_signal_packet(item, iteration_root=iteration_root)
        signal["signal_id"] = item["signal_id"]
        decision = _normalize_decision(
            decide(
                {
                    "signal": signal,
                    "runtime_mode": "backtest",
                    "parameters": {},
                    "raw_data": {"packet_path": item.get("packet_path")},
                }
            ),
            signal_id=item["signal_id"],
        )
        truth = _label_for_signal(labels, item["signal_id"])
        decisions.append(decision)
        records.append(_score_record(decision, truth))

    metrics = _metrics(records)
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    decisions_artifact = {
        "schema_version": "0.1",
        "stage": "stage1a_directional_agreement",
        "created_at": created_at,
        "decision_source": "strategy_module/strategy.py",
        "decisions": decisions,
    }
    score_artifact = {
        "schema_version": "0.1",
        "session_id": training_sample.get("session_id", ""),
        "asset": _infer_asset(sample),
        "strategy_id": _first_value(decisions, "strategy_id"),
        "strategy_version": _first_value(decisions, "strategy_version"),
        "signal_engine_id": "",
        "signal_family": "",
        "signal_set_id": "",
        "stage": "stage1a_directional_agreement",
        "sample_role": sample_role,
        "scoring_method": "stage1a_directional_agreement",
        "created_at": created_at,
        "metrics": metrics,
        "records": records,
    }

    artifacts = SAMPLE_ROLE_ARTIFACTS[sample_role]
    decisions_path = iteration_root / "decisions" / artifacts["decisions"]
    scores_path = iteration_root / "scores" / artifacts["scores"]
    summary_path = iteration_root / "summaries" / artifacts["summary"]
    decisions_path.write_text(json.dumps(decisions_artifact, indent=2) + "\n")
    scores_path.write_text(json.dumps(score_artifact, indent=2) + "\n")
    summary_path.write_text(_render_summary(metrics, title=artifacts["title"]))
    return {
        "decisions_path": str(decisions_path),
        "scores_path": str(scores_path),
        "summary_path": str(summary_path),
        "metrics": metrics,
    }


def run_stage1a_canonical_full_cycle(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signals_by_role: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    unsupported = sorted(set(signals_by_role) - set(SAMPLE_ROLE_ARTIFACTS))
    if unsupported:
        raise ValueError(f"Unsupported canonical Stage 1A sample roles: {', '.join(unsupported)}")

    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)
    frozen_strategy_root = promotion_root / "frozen_stage1a_strategy_module"
    _copy_frozen_strategy_snapshot(artifact_root / "strategy_module", frozen_strategy_root)

    decide = _load_decide(frozen_strategy_root / "strategy.py")
    labels = _labels_for_stage0_root(_stage0_root_for_session(workspace_root=workspace_root, session=session))
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")

    decisions: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    slice_metrics: dict[str, dict[str, Any]] = {}
    for sample_role in ("recent_regime_train", "forward_validation", "locked_recent_oos"):
        role_records: list[dict[str, Any]] = []
        for signal in signals_by_role.get(sample_role, []):
            item = _canonical_signal_item(workspace_root=workspace_root, session=session, signal=signal)
            packet = _load_signal_packet(item, iteration_root=None)
            packet["signal_id"] = item["signal_id"]
            decision = _normalize_decision(
                decide(
                    {
                        "signal": packet,
                        "runtime_mode": "backtest",
                        "parameters": {},
                        "raw_data": {"packet_path": item.get("packet_path"), "sample_role": sample_role},
                    }
                ),
                signal_id=item["signal_id"],
            )
            decision["sample_role"] = sample_role
            truth = _label_for_signal(labels, item["signal_id"])
            record = _score_record(decision, truth)
            record["sample_role"] = sample_role
            decisions.append(decision)
            records.append(record)
            role_records.append(record)
        slice_metrics[sample_role] = _metrics(role_records)

    metrics = _metrics(records)
    match_set = [
        {
            "signal_id": record["signal_id"],
            "sample_role": record["sample_role"],
            "decision_direction": record["decision_direction"],
            "ground_truth_direction": record["ground_truth_direction"],
        }
        for record in records
        if record["agreement"] == "MATCH"
    ]
    decisions_artifact = {
        "schema_version": "0.1",
        "stage": "stage1a_directional_agreement",
        "artifact_role": "canonical_full_cycle_decisions",
        "created_at": created_at,
        "session_id": session["session_id"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "signal_engine_id": session["signal_engine_id"],
        "signal_set_id": session["signal_set_id"],
        "decision_source": "promotion/frozen_stage1a_strategy_module/strategy.py",
        "decisions": decisions,
    }
    score_artifact = {
        "schema_version": "0.1",
        "stage": "stage1a_directional_agreement",
        "artifact_role": "canonical_full_cycle_scores",
        "created_at": created_at,
        "session_id": session["session_id"],
        "asset": session["asset"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "signal_engine_id": session["signal_engine_id"],
        "signal_set_id": session["signal_set_id"],
        "scoring_method": "stage1a_canonical_full_cycle_directional_agreement",
        "metrics": metrics,
        "slice_metrics": slice_metrics,
        "match_set": match_set,
        "records": records,
        "stage2_stage3_input": {
            "role": "canonical_match_set",
            "description": "Stage 2 and Stage 3 must use this MATCH subset from the frozen full-cycle readout.",
        },
        "stage4_input": {
            "role": "canonical_full_decision_set",
            "description": "Stage 4 must use the full decision set from this same frozen readout.",
        },
    }

    decisions_path = promotion_root / "stage1a_canonical_full_cycle_decisions.json"
    scores_path = promotion_root / "stage1a_canonical_full_cycle_scores.json"
    summary_path = promotion_root / "stage1a_canonical_full_cycle_summary.md"
    decisions_path.write_text(json.dumps(decisions_artifact, indent=2) + "\n")
    scores_path.write_text(json.dumps(score_artifact, indent=2) + "\n")
    summary_path.write_text(_render_canonical_summary(score_artifact))
    return {
        "decisions_path": str(decisions_path),
        "scores_path": str(scores_path),
        "summary_path": str(summary_path),
        "frozen_strategy_path": str(frozen_strategy_root / "strategy.py"),
        "metrics": metrics,
        "slice_metrics": slice_metrics,
        "match_count": len(match_set),
    }


def generate_stage1a_failure_audit(*, iteration_root: Path, sample_role: str = "recent_regime_train") -> dict[str, Any]:
    if sample_role not in SAMPLE_ROLE_ARTIFACTS:
        raise ValueError(f"Unsupported Stage 1A sample role: {sample_role}")
    iteration_root = iteration_root.resolve()
    sample = json.loads((iteration_root / "signal_sample.json").read_text())
    training_sample = _read_optional_json(iteration_root / "builder_training_sample.json") or {}
    artifacts = SAMPLE_ROLE_ARTIFACTS[sample_role]
    decisions = json.loads((iteration_root / "decisions" / artifacts["decisions"]).read_text())
    scores = json.loads((iteration_root / "scores" / artifacts["scores"]).read_text())

    sample_by_id = {item["signal_id"]: item for item in sample.get("signals", [])}
    labels_by_id = _audit_labels_for_iteration(iteration_root=iteration_root, training_sample=training_sample)
    decisions_by_id = {item["signal_id"]: item for item in decisions.get("decisions", [])}
    failure_cases = []
    protected_cases = []
    for record in scores.get("records", []):
        case = _audit_case(
            record=record,
            sample=sample_by_id.get(record["signal_id"], {}),
            decision=decisions_by_id.get(record["signal_id"], {}),
            ground_truth=labels_by_id.get(record["signal_id"], {}),
        )
        if record["agreement"] in {"MISMATCH", "NEUTRAL"}:
            failure_cases.append(case)
        elif record["agreement"] == "MATCH":
            protected_cases.append(case)

    metrics = {
        "total": scores.get("metrics", {}).get("total", len(scores.get("records", []))),
        "failure_count": len(failure_cases),
        "mismatch_count": sum(1 for case in failure_cases if case["agreement"] == "MISMATCH"),
        "neutral_count": sum(1 for case in failure_cases if case["agreement"] == "NEUTRAL"),
        "protected_count": len(protected_cases),
    }
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    audit = {
        "schema_version": "0.1",
        "stage": "stage1a_directional_agreement",
        "sample_role": sample_role,
        "sample_title": artifacts["title"],
        "agent_handoff_policy": _audit_handoff_policy(sample_role),
        "created_at": created_at,
        "iteration_root": str(iteration_root),
        "session_strategy_path": str(iteration_root.parents[1] / "strategy_module" / "strategy.py"),
        "strategy_snapshot_path": str(iteration_root / "source_artifacts" / "strategy_module_snapshot"),
        "metrics": metrics,
        "failure_cluster": _failure_cluster(metrics),
        "failure_cases": failure_cases,
        "protected_cases": protected_cases[:10],
        "required_update_shape": {
            "layer": "Stage 1A directional classification only",
            "proposed_skill_change": "Identify recurring packet evidence that should reclassify direction or turn neutral reads into LONG/SHORT calls.",
            "regression_risk": "Do not break protected MATCH cases while correcting failures.",
            "retest_plan": "Rerun Stage 1A training score, then validate on forward and locked OOS samples before promotion.",
        },
    }
    audit_json_path = iteration_root / "audits" / "failure_audit.json"
    audit_md_path = iteration_root / "audits" / "failure_audit.md"
    prompt_path = iteration_root / "agent_failure_audit_prompt.md"
    audit_json_path.write_text(json.dumps(audit, indent=2) + "\n")
    audit_md_path.write_text(_render_failure_audit_md(audit))
    prompt_path.write_text(_render_failure_audit_prompt(audit))
    return {
        "audit_json_path": str(audit_json_path),
        "audit_md_path": str(audit_md_path),
        "agent_prompt_path": str(prompt_path),
        "sample_role": sample_role,
        "agent_handoff_policy": audit["agent_handoff_policy"],
        "metrics": metrics,
    }


def _load_decide(strategy_path: Path):
    if not strategy_path.exists():
        raise ValueError(f"Strategy module not found: {strategy_path}")
    module_name = f"_stage1_strategy_{abs(hash(strategy_path))}"
    spec = importlib.util.spec_from_file_location(module_name, strategy_path)
    if spec is None or spec.loader is None:
        raise ValueError(f"Unable to load strategy module: {strategy_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    decide = getattr(module, "decide", None)
    if not callable(decide):
        raise ValueError("Strategy module must expose callable decide(context)")
    return decide


def _audit_case(
    *,
    record: dict[str, Any],
    sample: dict[str, Any],
    decision: dict[str, Any],
    ground_truth: dict[str, Any],
) -> dict[str, Any]:
    return {
        "signal_id": record["signal_id"],
        "agreement": record["agreement"],
        "ground_truth_direction": record.get("ground_truth_direction"),
        "decision_direction": record.get("decision_direction"),
        "trade_action": decision.get("trade_action") or decision.get("action"),
        "confidence": decision.get("confidence"),
        "reason_code": decision.get("reason_code"),
        "packet_path": sample.get("packet_path"),
        "ground_truth": ground_truth,
        "diagnostics": decision.get("diagnostics", {}),
    }


def _audit_labels_for_iteration(*, iteration_root: Path, training_sample: dict[str, Any]) -> dict[str, dict[str, Any]]:
    training_labels = {
        item["signal_id"]: item.get("ground_truth", {})
        for item in training_sample.get("signals", [])
        if item.get("signal_id")
    }
    if training_labels:
        return training_labels
    stage0_labels = _labels_for_stage0_root(_stage0_root_for_iteration(iteration_root))
    return {
        signal_id: {"natural_direction": direction}
        for signal_id, direction in stage0_labels.items()
    }


def _audit_handoff_policy(sample_role: str) -> str:
    if sample_role == "recent_regime_train":
        return "direct_strategy_revision"
    if sample_role == "forward_validation":
        return "return_to_training"
    if sample_role == "locked_recent_oos":
        return "postmortem_only"
    return "unknown"


def _failure_cluster(metrics: dict[str, Any]) -> str:
    if metrics["neutral_count"] and not metrics["mismatch_count"]:
        return "neutral_no_scoreable_direction"
    if metrics["mismatch_count"] and not metrics["neutral_count"]:
        return "directional_mismatch"
    if metrics["failure_count"]:
        return "mixed_mismatch_and_neutral"
    return "no_failures"


def _render_failure_audit_md(audit: dict[str, Any]) -> str:
    failure_lines = "\n".join(
        f"- {case['signal_id']}: {case['agreement']} truth={case['ground_truth_direction']} decision={case['decision_direction']} reason={case.get('reason_code')}"
        for case in audit["failure_cases"]
    ) or "- None"
    protected_lines = "\n".join(
        f"- {case['signal_id']}: truth={case['ground_truth_direction']} decision={case['decision_direction']} reason={case.get('reason_code')}"
        for case in audit["protected_cases"]
    ) or "- None"
    metrics = audit["metrics"]
    return f"""# Stage 1A Failure Audit

Sample: {audit.get('sample_title', audit.get('sample_role', 'unknown'))}
Agent handoff policy: {audit.get('agent_handoff_policy', 'unknown')}
Failure cluster: {audit['failure_cluster']}

- Total: {metrics['total']}
- Failures: {metrics['failure_count']}
- Mismatches: {metrics['mismatch_count']}
- Neutral: {metrics['neutral_count']}
- Protected matches: {metrics['protected_count']}

## Failure Ledger

{failure_lines}

## Protected Cases

{protected_lines}

## Required Update Shape

- Layer: {audit['required_update_shape']['layer']}
- Proposed skill change: {audit['required_update_shape']['proposed_skill_change']}
- Regression risk: {audit['required_update_shape']['regression_risk']}
- Retest plan: {audit['required_update_shape']['retest_plan']}
"""


def _render_failure_audit_prompt(audit: dict[str, Any]) -> str:
    failure_ids = ", ".join(case["signal_id"] for case in audit["failure_cases"]) or "none"
    protected_ids = ", ".join(case["signal_id"] for case in audit["protected_cases"][:10]) or "none"
    iteration_root = Path(audit["iteration_root"])
    strategy_path = audit["session_strategy_path"]
    policy = audit.get("agent_handoff_policy")
    if policy == "return_to_training":
        return f"""You are reviewing a failed Stage 1A forward-validation slice.

Read:
- {iteration_root / "audits" / "failure_audit.json"}
- {iteration_root / "audits" / "failure_audit.md"}
- {iteration_root / "signal_sample.json"}
- {strategy_path}

Read-only strategy snapshot for this validation iteration:
- {audit["strategy_snapshot_path"]}

Current failure cluster: {audit['failure_cluster']}
Failure cases: {failure_ids}
Protected cases that should not regress: {protected_ids}

Task:
Diagnose why the current pair-specific strategy failed validation and produce guidance for the next training iteration.

Rules:
- Do not edit the strategy directly against validation labels.
- Do not tune to exact validation timestamps or signal ids.
- Use this validation audit only to propose general pattern hypotheses.
- The user should create a new training bundle and apply any strategy changes there before re-validating.
"""
    if policy == "postmortem_only":
        return f"""You are reviewing a failed locked-OOS Stage 1A slice.

Read:
- {iteration_root / "audits" / "failure_audit.json"}
- {iteration_root / "audits" / "failure_audit.md"}
- {iteration_root / "signal_sample.json"}

Current failure cluster: {audit['failure_cluster']}
Failure cases: {failure_ids}
Protected cases: {protected_ids}

Task:
Write a postmortem only. Explain the failure modes and whether the strategy should be rejected, retrained in a fresh cycle, or held for review.

Rules:
- Do not edit the strategy based on locked OOS.
- Do not create a revision prompt for this same cycle.
- Do not tune to exact OOS timestamps, signal ids, or labels.
- Locked OOS is a promotion gate, not an optimization set.
"""
    return f"""You are the strategy-builder agent for the next Stage 1A iteration.

Read:
- {iteration_root / "audits" / "failure_audit.json"}
- {iteration_root / "audits" / "failure_audit.md"}
- {iteration_root / "builder_training_sample.json"}
- {iteration_root / "signal_sample.json"}
- {strategy_path}

Read-only strategy snapshot for this failed iteration:
- {audit["strategy_snapshot_path"]}

Session strategy file to edit:
- {strategy_path}

Current failure cluster: {audit['failure_cluster']}
Failure cases: {failure_ids}
Protected cases that should not regress: {protected_ids}

Task:
Make the smallest possible Stage 1A direction-only update to {strategy_path} that addresses repeated failure evidence in the audit.

Rules:
- Stage 1A is directional agreement only: choose LONG or SHORT when the signal is scoreable.
- Do not add Stage 1B entry gates, expected-travel filters, trade-management logic, live execution, randomness, or exchange calls.
- Do not encode exact training timestamps or signal ids as strategy rules.
- Preserve protected MATCH cases unless the audit evidence proves the old read was wrong.
- Do not edit the read-only strategy snapshot; it is only evidence of what failed.
- New Stage 1 bundles automatically snapshot the current session strategy file into their own source_artifacts/strategy_module_snapshot folder.
- After editing, the user should rerun Score on this iteration before creating validation or OOS bundles.
"""


def _strategy_path_for_iteration(iteration_root: Path) -> Path:
    iteration_strategy = iteration_root / "strategy_module" / "strategy.py"
    if iteration_strategy.exists():
        return iteration_strategy
    return iteration_root.parents[1] / "strategy_module" / "strategy.py"


def _training_labels(training_sample: dict[str, Any]) -> dict[str, str]:
    labels = {}
    for item in training_sample.get("signals", []):
        direction = item.get("ground_truth", {}).get("natural_direction")
        if direction in {"LONG", "SHORT"}:
            labels[item["signal_id"]] = direction
    return labels


def _label_for_signal(labels: dict[str, str], signal_id: str) -> str | None:
    if signal_id in labels:
        return labels[signal_id]
    if ":" in signal_id:
        return labels.get(signal_id.split(":")[-1])
    return None


def _labels_for_iteration(*, iteration_root: Path, training_sample: dict[str, Any]) -> dict[str, str]:
    labels = _training_labels(training_sample)
    if labels:
        return labels
    stage0_root = _stage0_root_for_iteration(iteration_root)
    if stage0_root is None:
        raise ValueError("Stage 1A scoring requires builder_training_sample labels or session stage0_artifact_root")
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    if not ground_truth_root.is_dir():
        raise ValueError(f"Stage 0 ground truth directory not found: {ground_truth_root}")
    labels = {}
    for path in ground_truth_root.glob("*.json"):
        if path.name == "distribution.json":
            continue
        payload = json.loads(path.read_text())
        payloads = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            direction = item.get("natural_direction")
            if direction in {"LONG", "SHORT"}:
                signal_id = str(item.get("signal_id") or path.stem)
                labels[signal_id] = direction
    return labels


def _stage0_root_for_iteration(iteration_root: Path) -> Path | None:
    manifest_path = iteration_root.parents[1] / "manifest.json"
    manifest = _read_optional_json(manifest_path) or {}
    stage0_root = manifest.get("stage0_artifact_root")
    return Path(stage0_root) if stage0_root else None


def _session_artifact_root(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _stage0_root_for_session(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    stage0_root_value = session.get("stage0_artifact_root") or session.get("manifest", {}).get("stage0_artifact_root")
    if not stage0_root_value:
        raise ValueError("Canonical Stage 1A scoring requires session stage0_artifact_root")
    stage0_root = Path(stage0_root_value)
    return stage0_root if stage0_root.is_absolute() else workspace_root / stage0_root


def _labels_for_stage0_root(stage0_root: Path) -> dict[str, str]:
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    if not ground_truth_root.is_dir():
        raise ValueError(f"Stage 0 ground truth directory not found: {ground_truth_root}")
    labels = {}
    for path in ground_truth_root.glob("*.json"):
        if path.name == "distribution.json":
            continue
        payload = json.loads(path.read_text())
        payloads = payload if isinstance(payload, list) else [payload]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            direction = item.get("natural_direction")
            if direction in {"LONG", "SHORT"}:
                signal_id = str(item.get("signal_id") or path.stem)
                labels[signal_id] = direction
    return labels


def _canonical_signal_item(*, workspace_root: Path, session: dict[str, Any], signal: dict[str, Any]) -> dict[str, str]:
    signal_id = str(signal["signal_id"])
    signal_set_key = str(signal.get("signal_set_key") or session["signal_set_key"])
    if signal_set_key.count(":") >= 2:
        _, asset, signal_set_id = signal_set_key.split(":", 2)
    else:
        asset = session["asset"]
        signal_set_id = session["signal_set_id"]
    packet_name = signal_id.split(":")[-1]
    item = {
        "signal_id": signal_id,
        "packet_path": str(
            workspace_root
            / "dev"
            / "signals"
            / str(signal.get("signal_engine_id") or session["signal_engine_id"])
            / asset
            / signal_set_id
            / "packets"
            / f"{packet_name}.json"
        ),
    }
    if isinstance(signal.get("payload"), dict):
        item["packet"] = signal["payload"]
    return item


def _copy_frozen_strategy_snapshot(source_dir: Path, frozen_strategy_root: Path) -> None:
    if not (source_dir / "strategy.py").exists():
        raise ValueError(f"Session strategy module not found: {source_dir / 'strategy.py'}")
    if frozen_strategy_root.exists():
        shutil.rmtree(frozen_strategy_root)
    shutil.copytree(source_dir, frozen_strategy_root)


def _read_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else None


def _load_signal_packet(item: dict[str, Any], *, iteration_root: Path | None = None) -> dict[str, Any]:
    embedded_packet = item.get("packet")
    if isinstance(embedded_packet, dict) and embedded_packet:
        return dict(embedded_packet)

    packet_path_value = item.get("packet_path")
    if packet_path_value:
        packet_path = Path(packet_path_value)
        if packet_path.is_file():
            packet = json.loads(packet_path.read_text())
            if not isinstance(packet, dict):
                raise ValueError(f"Signal packet must be a JSON object: {packet_path}")
            return packet

    fallback_path = _stage0_subset_packet_path(item=item, iteration_root=iteration_root)
    if fallback_path is not None and fallback_path.is_file():
        packet = json.loads(fallback_path.read_text())
        if not isinstance(packet, dict):
            raise ValueError(f"Signal packet must be a JSON object: {fallback_path}")
        return packet

    packet_ref = packet_path_value or _packet_name_for_item(item)
    raise ValueError(f"Signal packet artifact not found for {item.get('signal_id')}: {packet_ref}")


def _stage0_subset_packet_path(*, item: dict[str, Any], iteration_root: Path | None) -> Path | None:
    if iteration_root is None:
        return None
    stage0_root = _stage0_root_for_iteration(iteration_root)
    if stage0_root is None:
        return None
    packet_name = _packet_name_for_item(item)
    return stage0_root / "scores" / "_scoreable_signal_subset" / "packets" / packet_name


def _packet_name_for_item(item: dict[str, Any]) -> str:
    packet_path = item.get("packet_path")
    if packet_path:
        return Path(str(packet_path)).name
    signal_id = str(item.get("signal_id") or "")
    packet_stem = signal_id.split(":")[-1]
    return f"{packet_stem}.json"


def _normalize_decision(decision: Any, *, signal_id: str) -> dict[str, Any]:
    if not isinstance(decision, dict):
        raise ValueError("Strategy decide(context) must return a dict for v1 scoring")
    trade_action = decision.get("trade_action") or decision.get("action")
    direction = decision.get("direction")
    if trade_action not in {"ENTER", "SKIP"}:
        raise ValueError(f"Invalid trade_action for {signal_id}: {trade_action}")
    if direction not in {"LONG", "SHORT", "FLAT"}:
        raise ValueError(f"Invalid direction for {signal_id}: {direction}")
    return {
        "decision_id": str(decision.get("decision_id") or f"stage1a-{signal_id}"),
        "strategy_id": str(decision.get("strategy_id") or ""),
        "strategy_version": str(decision.get("strategy_version") or ""),
        "signal_id": signal_id,
        "trade_action": trade_action,
        "action": trade_action,
        "direction": direction,
        "confidence": float(decision.get("confidence", 0)),
        "reason_code": str(decision.get("reason_code") or ""),
        "execution_profile": decision.get("execution_profile", {}),
        "diagnostics": decision.get("diagnostics", {}),
    }


def _score_record(decision: dict[str, Any], truth: str | None) -> dict[str, Any]:
    direction = decision["direction"]
    if decision["trade_action"] == "SKIP" or direction == "FLAT":
        agreement = "NEUTRAL"
        status = "NEUTRAL"
    elif truth and direction == truth:
        agreement = "MATCH"
        status = "CORRECT"
    else:
        agreement = "MISMATCH"
        status = "INCORRECT"
    return {
        "signal_id": decision["signal_id"],
        "ground_truth_direction": truth,
        "agent_direction": direction if direction in {"LONG", "SHORT"} else None,
        "decision_direction": direction,
        "confidence": decision["confidence"],
        "agreement": agreement,
        "status": status,
        "reason_code": decision["reason_code"],
    }


def _metrics(records: list[dict[str, Any]]) -> dict[str, Any]:
    matches = sum(1 for record in records if record["agreement"] == "MATCH")
    mismatches = sum(1 for record in records if record["agreement"] == "MISMATCH")
    neutral = sum(1 for record in records if record["agreement"] == "NEUTRAL")
    scoreable = matches + mismatches
    directional_agreement = round(matches / scoreable, 6) if scoreable else 0
    return {
        "total": len(records),
        "matches": matches,
        "mismatches": mismatches,
        "neutral": neutral,
        "scoreable": scoreable,
        "directional_agreement": directional_agreement,
        "promotion_threshold_pct": PROMOTION_THRESHOLD_PCT,
        "passes_threshold": directional_agreement * 100 >= PROMOTION_THRESHOLD_PCT,
    }


def _render_summary(metrics: dict[str, Any], *, title: str) -> str:
    agreement_pct = metrics["directional_agreement"] * 100
    decision = "continue to validation" if metrics["passes_threshold"] else "audit failures before next edit"
    return f"""# {title}

Directional agreement: {agreement_pct:.2f}%

- Total signals: {metrics['total']}
- Scoreable signals: {metrics['scoreable']}
- Matches: {metrics['matches']}
- Mismatches: {metrics['mismatches']}
- Neutral: {metrics['neutral']}
- Promotion threshold: {metrics['promotion_threshold_pct']:.2f}%
- Decision: {decision}
"""


def _render_canonical_summary(score_artifact: dict[str, Any]) -> str:
    metrics = score_artifact["metrics"]
    slice_lines = "\n".join(
        f"- {role}: {role_metrics['directional_agreement'] * 100:.2f}% agreement, "
        f"{role_metrics['matches']} match / {role_metrics['mismatches']} mismatch / {role_metrics['neutral']} neutral"
        for role, role_metrics in score_artifact["slice_metrics"].items()
    )
    return f"""# Canonical Stage 1A Full-Cycle Readout

Session: {score_artifact['session_id']}
Strategy: {score_artifact['strategy_id']}@{score_artifact['strategy_version']}

Directional agreement: {metrics['directional_agreement'] * 100:.2f}%

- Total signals: {metrics['total']}
- Scoreable signals: {metrics['scoreable']}
- Matches: {metrics['matches']}
- Mismatches: {metrics['mismatches']}
- Neutral: {metrics['neutral']}
- Promotion threshold: {metrics['promotion_threshold_pct']:.2f}%

## Slices

{slice_lines}

## Downstream Contract

- Stage 2 and Stage 3 use the MATCH subset in `stage1a_canonical_full_cycle_scores.json`.
- Stage 4 uses the full decision set in `stage1a_canonical_full_cycle_decisions.json`.
"""


def _first_value(items: list[dict[str, Any]], key: str) -> str:
    for item in items:
        if item.get(key):
            return str(item[key])
    return ""


def _infer_asset(sample: dict[str, Any]) -> str:
    signals = sample.get("signals", [])
    if not signals:
        return ""
    parts = str(signals[0].get("signal_id", "")).split(":")
    return parts[1] if len(parts) >= 2 else ""
