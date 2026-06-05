from __future__ import annotations

import argparse
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "signal_id": {"type": "string"},
        "trade_action": {"type": "string", "enum": ["ENTER", "SKIP"]},
        "direction": {"type": "string", "enum": ["LONG", "SHORT"]},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "expected_travel": {"type": "string", "enum": ["high", "low"]},
        "entry_gate": {"type": "string", "enum": ["pass", "fail"]},
        "gate_reason_code": {"type": "string"},
        "travel_conviction": {"type": "number", "minimum": 0, "maximum": 1},
        "reasoning": {"type": "string"},
    },
    "required": [
        "signal_id",
        "trade_action",
        "direction",
        "confidence",
        "expected_travel",
        "entry_gate",
        "gate_reason_code",
        "reasoning",
    ],
}


def select_evenly_spaced(items: list[Any], count: int) -> list[Any]:
    if count <= 0:
        return []
    if count >= len(items):
        return list(items)
    if count == 1:
        return [items[len(items) // 2]]

    max_index = len(items) - 1
    indexes: list[int] = []
    for position in range(count):
        raw_index = round(position * max_index / (count - 1))
        if indexes and raw_index <= indexes[-1]:
            raw_index = indexes[-1] + 1
        indexes.append(min(raw_index, max_index))
    return [items[index] for index in indexes]


def build_balanced_sample(entries: list[dict[str, Any]], sample_size: int) -> list[dict[str, Any]]:
    ordered = sorted(entries, key=lambda entry: entry["signal_id"])
    triggered = [entry for entry in ordered if entry["gt_trigger"]]
    no_trigger = [entry for entry in ordered if not entry["gt_trigger"]]

    target_triggered = min(len(triggered), sample_size // 2)
    target_no_trigger = min(len(no_trigger), sample_size // 2)
    remaining = sample_size - target_triggered - target_no_trigger

    while remaining > 0:
        triggered_remaining = len(triggered) - target_triggered
        no_trigger_remaining = len(no_trigger) - target_no_trigger
        if triggered_remaining >= no_trigger_remaining and triggered_remaining > 0:
            target_triggered += 1
        elif no_trigger_remaining > 0:
            target_no_trigger += 1
        else:
            break
        remaining -= 1

    sample = select_evenly_spaced(triggered, target_triggered)
    sample.extend(select_evenly_spaced(no_trigger, target_no_trigger))
    return sorted(sample, key=lambda entry: entry["signal_id"])


def load_ground_truth_entries(ground_truth_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for path in sorted(ground_truth_dir.glob("*.json")):
        if path.name in {"distribution.json", "summary.json", "index.json"}:
            continue
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict):
            continue
        gt_direction = payload.get("natural_direction")
        gt_trigger = payload.get("status") != "no_trigger" and gt_direction in {"LONG", "SHORT"}
        entries.append(
            {
                "signal_id": payload["signal_id"],
                "gt_trigger": gt_trigger,
                "gt_direction": gt_direction if gt_trigger else None,
            }
        )
    return entries


def resolve_strategy_skill_id(skill_file: Path) -> str:
    snapshot_manifest = skill_file.parent.parent / "strategy_skill_snapshot_manifest.json"
    if skill_file.parent.name == "strategy_skill_snapshot" and snapshot_manifest.exists():
        manifest = json.loads(snapshot_manifest.read_text())
        strategy_id = manifest.get("strategy_id")
        if isinstance(strategy_id, str) and strategy_id:
            return strategy_id
    return skill_file.parent.name


def normalize_stage1b_decision(payload: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}

    if "signal_id" in payload:
        normalized["signal_id"] = str(payload["signal_id"]).strip()

    if "direction" not in payload:
        raise ValueError("Stage 1B decision missing direction")
    direction = str(payload["direction"]).upper()
    if direction not in {"LONG", "SHORT"}:
        raise ValueError(f"invalid direction={direction}")
    normalized["direction"] = direction

    if "confidence" not in payload:
        raise ValueError("Stage 1B decision missing confidence")
    confidence = float(payload["confidence"])
    if confidence < 0 or confidence > 1:
        raise ValueError(f"confidence outside 0..1: {confidence}")
    normalized["confidence"] = confidence

    expected_travel = None
    if "expected_travel" in payload:
        expected_travel = str(payload["expected_travel"]).lower()
        if expected_travel not in {"high", "low"}:
            raise ValueError(f"invalid expected_travel={expected_travel}")

    trade_action = None
    if "trade_action" in payload:
        trade_action = str(payload["trade_action"]).upper()
        if trade_action not in {"ENTER", "SKIP"}:
            raise ValueError(f"invalid trade_action={trade_action}")

    if trade_action is None:
        if expected_travel == "high":
            trade_action = "ENTER"
        elif expected_travel == "low":
            trade_action = "SKIP"
        else:
            raise ValueError("Stage 1B decision missing trade_action and expected_travel")

    inferred_expected_travel = "high" if trade_action == "ENTER" else "low"
    if expected_travel is None:
        expected_travel = inferred_expected_travel
    elif expected_travel != inferred_expected_travel:
        raise ValueError(f"expected_travel={expected_travel} conflicts with trade_action={trade_action}")

    entry_gate = None
    if "entry_gate" in payload:
        entry_gate = str(payload["entry_gate"]).lower()
        if entry_gate not in {"pass", "fail"}:
            raise ValueError(f"invalid entry_gate={entry_gate}")

    inferred_entry_gate = "pass" if trade_action == "ENTER" else "fail"
    if entry_gate is None:
        entry_gate = inferred_entry_gate
    elif entry_gate != inferred_entry_gate:
        raise ValueError(f"entry_gate={entry_gate} conflicts with trade_action={trade_action}")

    gate_reason_code = payload.get("gate_reason_code")
    if gate_reason_code is None:
        gate_reason_code = f"legacy_expected_travel_{expected_travel}"

    if "reasoning" not in payload:
        raise ValueError("Stage 1B decision missing reasoning")
    reasoning = str(payload["reasoning"]).strip()
    if not reasoning:
        raise ValueError("Stage 1B decision reasoning is empty")

    normalized["trade_action"] = trade_action
    normalized["expected_travel"] = expected_travel
    normalized["entry_gate"] = entry_gate
    normalized["gate_reason_code"] = str(gate_reason_code).strip()
    if not normalized["gate_reason_code"]:
        raise ValueError("Stage 1B decision gate_reason_code is empty")
    if "travel_conviction" in payload:
        normalized["travel_conviction"] = float(payload["travel_conviction"])
    normalized["reasoning"] = reasoning
    return normalized


def classify_outcome(trade_action: str, gt_trigger: bool) -> str:
    action = str(trade_action).upper()
    if action == "ENTER" and gt_trigger:
        return "TP"
    if action == "ENTER" and not gt_trigger:
        return "FP"
    if action == "SKIP" and gt_trigger:
        return "FN"
    if action != "SKIP":
        raise ValueError(f"invalid trade_action={trade_action}")
    return "TN"


def summarize_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    tp = sum(1 for result in results if result["outcome"] == "TP")
    fp = sum(1 for result in results if result["outcome"] == "FP")
    tn = sum(1 for result in results if result["outcome"] == "TN")
    fn = sum(1 for result in results if result["outcome"] == "FN")

    precision = round((tp / (tp + fp) * 100), 1) if (tp + fp) else 0.0
    recall = round((tp / (tp + fn) * 100), 1) if (tp + fn) else 0.0
    direction_matches = sum(
        1
        for result in results
        if result["outcome"] == "TP" and result["direction"] == result["gt_direction"]
    )
    directional = round((direction_matches / tp * 100), 1) if tp else 0.0

    return {
        "signals": len(results),
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "directional": directional,
    }


def build_stage1b_prompt(
    signal_file: Path,
    skill_file: Path,
    threshold_pct: float,
    forward_hours: int,
) -> str:
    signal_id = signal_file.stem
    return f"""You are evaluating a trading signal to determine whether the strategy skill permits a live entry. The calibrated travel threshold is {threshold_pct}% within {forward_hours}h.

Read the signal packet at {signal_file}.
Read the strategy skill at {skill_file}.

`trade_action` is the live entry gate:
- ENTER means the signal passes the strategy skill's entry gate and should be traded.
- SKIP means the signal fails the entry gate. Do not enter even if a directional bias exists.
- expected_travel is supporting metadata only: ENTER uses high, SKIP uses low.

Output ONLY a JSON object:
{{
  "signal_id": "{signal_id}",
  "trade_action": "ENTER" or "SKIP",
  "direction": "LONG" or "SHORT",
  "confidence": 0.0 to 1.0,
  "expected_travel": "high" or "low",
  "entry_gate": "pass" or "fail",
  "gate_reason_code": "short_snake_case_reason",
  "reasoning": "Concise packet-grounded reason for the entry gate decision"
}}

Do not add commentary outside the JSON. Do not override the skill's rules with your own market knowledge.
"""


def parse_agent_output(raw_output: str) -> dict[str, Any]:
    candidate = raw_output.strip()
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        start = candidate.find("{")
        end = candidate.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        parsed = json.loads(candidate[start : end + 1])

    return normalize_stage1b_decision(parsed)


def evaluate_signal(
    repo_root: Path,
    signal_file: Path,
    skill_file: Path,
    threshold_pct: float,
    forward_hours: int,
    retries: int,
) -> dict[str, Any]:
    prompt = build_stage1b_prompt(signal_file, skill_file, threshold_pct, forward_hours)
    last_error: Exception | None = None

    for _ in range(retries + 1):
        with tempfile.TemporaryDirectory(prefix="stage1b_") as tmpdir:
            tmp_path = Path(tmpdir)
            schema_path = tmp_path / "schema.json"
            output_path = tmp_path / "last_message.json"
            schema_path.write_text(json.dumps(OUTPUT_SCHEMA))

            command = [
                "codex",
                "exec",
                "--sandbox",
                "read-only",
                "--skip-git-repo-check",
                "--ephemeral",
                "-C",
                str(repo_root),
                "--output-schema",
                str(schema_path),
                "-o",
                str(output_path),
                prompt,
            ]
            completed = subprocess.run(command, capture_output=True, text=True, check=False)
            if completed.returncode != 0:
                last_error = RuntimeError(completed.stderr.strip() or completed.stdout.strip())
                continue
            try:
                return parse_agent_output(output_path.read_text())
            except Exception as exc:  # pragma: no cover - exercised via integration
                last_error = exc

    if last_error is None:
        last_error = RuntimeError(f"Unable to evaluate {signal_file.name}")
    raise last_error


def run_stage1b(
    signal_dir: Path,
    ground_truth_dir: Path,
    skill_file: Path,
    output_dir: Path,
    sample_size: int,
    threshold_pct: float,
    forward_hours: int,
    repo_root: Path,
    retries: int,
    reuse_existing: bool,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    sample = build_balanced_sample(load_ground_truth_entries(ground_truth_dir), sample_size)
    (output_dir / "sample.json").write_text(json.dumps(sample, indent=2))

    session = output_dir.name
    strategy_skill = resolve_strategy_skill_id(skill_file)
    results: list[dict[str, Any]] = []
    for entry in sample:
        signal_id = entry["signal_id"]
        result_path = output_dir / f"{signal_id}.json"
        if reuse_existing and result_path.exists():
            result_payload = json.loads(result_path.read_text())
        else:
            decision = evaluate_signal(
                repo_root=repo_root,
                signal_file=signal_dir / f"{signal_id}.json",
                skill_file=skill_file,
                threshold_pct=threshold_pct,
                forward_hours=forward_hours,
                retries=retries,
            )
            if "signal_id" in decision and decision["signal_id"] != signal_id:
                raise ValueError(f"{result_path.name}: decision signal_id does not match sample signal_id")
            result_payload = {
                "session": session,
                "strategy_skill": strategy_skill,
                **decision,
                "signal_id": signal_id,
                "gt_trigger": entry["gt_trigger"],
                "gt_direction": entry["gt_direction"],
                "outcome": classify_outcome(decision["trade_action"], entry["gt_trigger"]),
            }
            result_path.write_text(json.dumps(result_payload, indent=2))
        normalized_decision = normalize_stage1b_decision(result_payload)
        result_payload.update(normalized_decision)
        result_payload["outcome"] = classify_outcome(result_payload["trade_action"], entry["gt_trigger"])
        results.append(result_payload)

    summary = {
        "session": session,
        "strategy_skill": strategy_skill,
        "threshold_pct": threshold_pct,
        "forward_hours": forward_hours,
        **summarize_results(results),
    }
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Stage 1B screening evaluation with Codex.")
    parser.add_argument("signal_dir", type=Path)
    parser.add_argument("ground_truth_dir", type=Path)
    parser.add_argument("skill_file", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--sample-size", type=int, default=30)
    parser.add_argument("--threshold-pct", type=float, required=True)
    parser.add_argument("--forward-hours", type=int, default=36)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--reuse-existing", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    summary = run_stage1b(
        signal_dir=args.signal_dir,
        ground_truth_dir=args.ground_truth_dir,
        skill_file=args.skill_file,
        output_dir=args.output_dir,
        sample_size=args.sample_size,
        threshold_pct=args.threshold_pct,
        forward_hours=args.forward_hours,
        repo_root=args.repo_root,
        retries=args.retries,
        reuse_existing=args.reuse_existing,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
