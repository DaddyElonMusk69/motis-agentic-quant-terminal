#!/usr/bin/env python3
"""Stage 1A: score agent directional decisions against Stage 0 ground truth."""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


VALID_DIRECTIONS = {"LONG", "SHORT"}
SKIP_NAMES = {
    "index.json",
    "summary.json",
    "ground_truth_summary.json",
    "distribution.json",
    "stage1a_directional_scores.json",
}


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def normalize_direction(value: Any) -> str | None:
    if value is None:
        return None
    direction = str(value).strip().upper()
    if direction in VALID_DIRECTIONS:
        return direction
    if direction in {"NEUTRAL", "SKIP", "NONE", "NO_TRADE", "NO TRADE"}:
        return "NEUTRAL"
    return None


def extract_decision_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("decisions", "records", "results"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    if "signal_id" in payload:
        return [payload]
    return []


def load_decisions(path: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if path.is_file():
        payload = load_json(path)
        metadata = payload if isinstance(payload, dict) else {}
        return extract_decision_list(payload), metadata

    decisions: list[dict[str, Any]] = []
    metadata: dict[str, Any] = {}
    for file_path in sorted(path.glob("*.json")):
        if file_path.name in SKIP_NAMES:
            continue
        payload = load_json(file_path)
        entries = extract_decision_list(payload)
        decisions.extend(entries)
    return decisions, metadata


def maybe_load_expected_signal_ids(decisions_path: Path) -> list[str] | None:
    candidate_roots: list[Path] = []
    if decisions_path.is_file():
        candidate_roots.append(decisions_path.parent.parent)
    else:
        candidate_roots.append(decisions_path.parent)

    for iteration_root in candidate_roots:
        sample_path = iteration_root / "signal_sample.json"
        if not sample_path.exists():
            continue
        payload = load_json(sample_path)
        if not isinstance(payload, dict):
            continue
        packet_paths = payload.get("packet_paths")
        if not isinstance(packet_paths, list):
            continue
        expected = [Path(str(packet_path)).stem for packet_path in packet_paths]
        return expected
    return None


def validate_sample_alignment(expected_signal_ids: list[str], decisions: list[dict[str, Any]]) -> list[str]:
    actual_signal_ids = [str(decision.get("signal_id", "")).strip() for decision in decisions]
    errors: list[str] = []
    if len(actual_signal_ids) != len(expected_signal_ids):
        errors.append(
            f"decision count {len(actual_signal_ids)} does not match frozen sample size {len(expected_signal_ids)}"
        )

    for index, expected_signal_id in enumerate(expected_signal_ids):
        if index >= len(actual_signal_ids):
            break
        actual_signal_id = actual_signal_ids[index]
        if actual_signal_id != expected_signal_id:
            errors.append(
                f"slot {index + 1} expected {expected_signal_id} but got {actual_signal_id or '<missing>'}"
            )

    return errors


def load_ground_truth(ground_truth_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for file_path in sorted(ground_truth_dir.glob("*.json")):
        if file_path.name in SKIP_NAMES:
            continue
        payload = load_json(file_path)
        if not isinstance(payload, dict):
            continue
        signal_id = str(payload.get("signal_id") or file_path.stem)
        records[signal_id] = payload
    return records


def infer_metadata(decision_metadata: dict[str, Any], args: argparse.Namespace) -> dict[str, Any]:
    fields = [
        "session_id",
        "iteration_id",
        "asset",
        "strategy_id",
        "strategy_version",
        "signal_engine_id",
        "signal_family",
        "signal_set_id",
    ]
    inferred: dict[str, Any] = {}
    for field in fields:
        value = getattr(args, field)
        if value is None:
            value = decision_metadata.get(field)
        if value is not None:
            inferred[field] = value
    return inferred


def bump_counter(bucket: dict[str, dict[str, int]], key: str, agreement: str) -> None:
    item = bucket.setdefault(key, {"total": 0, "match": 0, "mismatch": 0, "neutral": 0})
    item["total"] += 1
    if agreement == "MATCH":
        item["match"] += 1
    elif agreement == "MISMATCH":
        item["mismatch"] += 1
    else:
        item["neutral"] += 1


def score_decisions(
    decisions: list[dict[str, Any]],
    ground_truth: dict[str, dict[str, Any]],
    min_confidence: float | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    records: list[dict[str, Any]] = []
    missing_ground_truth: list[str] = []
    duplicate_decisions: list[str] = []
    seen: set[str] = set()

    by_ground_truth_direction: dict[str, dict[str, int]] = {}
    by_agent_direction: dict[str, dict[str, int]] = {}
    by_regime: dict[str, dict[str, int]] = {}
    by_source_batch: dict[str, dict[str, int]] = {}

    for decision in decisions:
        signal_id = str(decision.get("signal_id", "")).strip()
        if not signal_id:
            continue
        if signal_id in seen:
            duplicate_decisions.append(signal_id)
        seen.add(signal_id)

        gt = ground_truth.get(signal_id)
        if gt is None:
            missing_ground_truth.append(signal_id)
            continue

        gt_direction = normalize_direction(gt.get("natural_direction"))
        agent_direction = normalize_direction(
            decision.get("direction")
            or decision.get("agent_direction")
            or decision.get("decision")
            or decision.get("verdict")
        )
        confidence = decision.get("confidence")
        confidence_value: float | None
        try:
            confidence_value = float(confidence) if confidence is not None else None
        except (TypeError, ValueError):
            confidence_value = None

        if min_confidence is not None and (
            confidence_value is None or confidence_value < min_confidence
        ):
            agent_direction = "NEUTRAL"

        if gt_direction not in VALID_DIRECTIONS:
            agreement = "NEUTRAL"
            status = "UNSCORED"
        elif agent_direction not in VALID_DIRECTIONS:
            agreement = "NEUTRAL"
            status = "NEUTRAL"
        elif agent_direction == gt_direction:
            agreement = "MATCH"
            status = "CORRECT"
        else:
            agreement = "MISMATCH"
            status = "INCORRECT"

        record = {
            "signal_id": signal_id,
            "ground_truth_direction": gt_direction,
            "agent_direction": agent_direction,
            "confidence": confidence_value,
            "agreement": agreement,
            "status": status,
            "gt_travel_pct": gt.get("max_travel_pct"),
            "first_move_pct": gt.get("first_move_pct"),
            "opposite_max_pct": gt.get("opposite_max_pct"),
            "ground_truth_status": gt.get("status"),
        }
        for optional_key in ("regime", "source_batch", "batch", "reasoning"):
            if optional_key in decision:
                record[optional_key] = decision[optional_key]
        records.append(record)

        if gt_direction in VALID_DIRECTIONS:
            bump_counter(by_ground_truth_direction, gt_direction, agreement)
        if agent_direction in VALID_DIRECTIONS:
            bump_counter(by_agent_direction, agent_direction, agreement)
        if decision.get("regime"):
            bump_counter(by_regime, str(decision["regime"]), agreement)
        source_batch = decision.get("source_batch") or decision.get("batch")
        if source_batch:
            bump_counter(by_source_batch, str(source_batch), agreement)

    match = sum(1 for record in records if record["agreement"] == "MATCH")
    mismatch = sum(1 for record in records if record["agreement"] == "MISMATCH")
    neutral = sum(1 for record in records if record["agreement"] == "NEUTRAL")
    scoreable = match + mismatch
    directional_agreement = round(match / scoreable, 4) if scoreable else 0.0

    metrics: dict[str, Any] = {
        "total_decisions": len(decisions),
        "scored_records": len(records),
        "scoreable": scoreable,
        "match": match,
        "mismatch": mismatch,
        "neutral": neutral,
        "directional_agreement": directional_agreement,
        "directional_agreement_pct": round(directional_agreement * 100, 2),
        "by_ground_truth_direction": by_ground_truth_direction,
        "by_agent_direction": by_agent_direction,
        "missing_ground_truth_count": len(missing_ground_truth),
        "duplicate_decision_count": len(duplicate_decisions),
    }
    if by_regime:
        metrics["by_regime"] = by_regime
    if by_source_batch:
        metrics["by_source_batch"] = by_source_batch
    if missing_ground_truth:
        metrics["missing_ground_truth"] = sorted(set(missing_ground_truth))
    if duplicate_decisions:
        metrics["duplicate_decisions"] = sorted(set(duplicate_decisions))

    return records, metrics


def write_summary(path: Path, output: dict[str, Any]) -> None:
    metrics = output["metrics"]
    lines = [
        "# Stage 1A Directional Score",
        "",
        f"- Total decisions: {metrics['total_decisions']}",
        f"- Scoreable decisions: {metrics['scoreable']}",
        f"- Match: {metrics['match']}",
        f"- Mismatch: {metrics['mismatch']}",
        f"- Neutral: {metrics['neutral']}",
        f"- Directional agreement: {metrics['directional_agreement_pct']}%",
        f"- Passed stage gate: {metrics['passed_stage_gate']}",
        "",
    ]
    if metrics.get("missing_ground_truth_count"):
        lines.append(f"- Missing ground truth: {metrics['missing_ground_truth_count']}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score Stage 1A directional decisions against Stage 0 ground truth."
    )
    parser.add_argument("--decisions", required=True, type=Path, help="Decision JSON file or directory")
    parser.add_argument("--ground-truth-dir", required=True, type=Path, help="Stage 0 ground_truth directory")
    parser.add_argument("--out", required=True, type=Path, help="Output score JSON path")
    parser.add_argument("--summary-out", type=Path, help="Optional markdown summary path")
    parser.add_argument("--min-confidence", type=float, help="Treat decisions below this confidence as NEUTRAL")
    parser.add_argument("--promotion-threshold-pct", type=float, default=55.0)
    parser.add_argument("--session-id")
    parser.add_argument("--iteration-id")
    parser.add_argument("--asset")
    parser.add_argument("--strategy-id")
    parser.add_argument("--strategy-version")
    parser.add_argument("--signal-engine-id")
    parser.add_argument("--signal-family")
    parser.add_argument("--signal-set-id")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    decisions, decision_metadata = load_decisions(args.decisions)
    expected_signal_ids = maybe_load_expected_signal_ids(args.decisions)
    if expected_signal_ids is not None:
        alignment_errors = validate_sample_alignment(expected_signal_ids, decisions)
        if alignment_errors:
            raise SystemExit(
                "Decision batch does not match frozen signal_sample.json:\n- "
                + "\n- ".join(alignment_errors)
            )
    ground_truth = load_ground_truth(args.ground_truth_dir)
    records, metrics = score_decisions(decisions, ground_truth, args.min_confidence)
    metrics["promotion_threshold_pct"] = args.promotion_threshold_pct
    metrics["passed_stage_gate"] = (
        metrics["directional_agreement_pct"] >= args.promotion_threshold_pct
        if metrics["scoreable"]
        else False
    )

    output = {
        "schema_version": "0.1",
        **infer_metadata(decision_metadata, args),
        "stage": "stage1a_directional_agreement",
        "scoring_method": "natural_direction_agreement",
        "created_at": utc_now(),
        "inputs": {
            "decisions": str(args.decisions),
            "ground_truth_dir": str(args.ground_truth_dir),
            "min_confidence": args.min_confidence,
        },
        "metrics": metrics,
        "records": records,
    }

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(output, indent=2) + "\n")
    if args.summary_out:
        write_summary(args.summary_out, output)

    print(json.dumps({"out": str(args.out), "metrics": metrics}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
