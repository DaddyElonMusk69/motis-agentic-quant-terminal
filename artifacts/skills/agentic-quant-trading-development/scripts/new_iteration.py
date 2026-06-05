#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create a canonical agentic evaluation iteration.")
    parser.add_argument("session", help="Training session directory")
    parser.add_argument("--iteration-id", required=True, help="Example: iter_001_v0.16")
    parser.add_argument("--sample-method", required=True)
    parser.add_argument("--sample-size", type=int, required=True)
    parser.add_argument("--packet-path", action="append", default=[], help="Ordered sampled packet path. Repeat for each packet.")
    return parser.parse_args()


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text())


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n")


def workspace_root_from_session(session: Path) -> Path:
    workspace = session
    for _ in range(4):
        workspace = workspace.parent
    return workspace


def build_file_hash_index(root: Path) -> list[dict[str, str]]:
    file_hashes: list[dict[str, str]] = []
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        relative = path.relative_to(root).as_posix()
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        file_hashes.append({"path": relative, "sha256": digest})
    return file_hashes


def build_output_contract(stage: object, strategy_id: object, strategy_version: object) -> str:
    stage_name = str(stage)
    if "stage1b" in stage_name.lower():
        return f"""
For Stage 1B, `trade_action` is the live entry gate and the scoring source of truth.
Use `ENTER` only when the packet passes the strategy skill's entry gate. Use `SKIP` when
the gate fails, even if a directional bias exists. `expected_travel` is supporting metadata
only and must agree with the action. Preserve signal order exactly.

Return JSON only:

{{
  "stage": "{stage_name}",
  "strategy_skill": "{strategy_id}",
  "strategy_version": "{strategy_version}",
  "decisions": [
    {{
      "signal_id": "...",
      "trade_action": "ENTER",
      "direction": "LONG",
      "confidence": 0.72,
      "expected_travel": "high",
      "entry_gate": "pass",
      "gate_reason_code": "accepted_reclaim_with_room",
      "reasoning": "Concise packet-grounded reason"
    }}
  ]
}}
"""

    return """
Return JSON only. Preserve signal order exactly.
"""


def main() -> int:
    args = parse_args()
    session = Path(args.session)
    session_manifest_path = session / "manifest.json"
    if not session_manifest_path.exists():
        raise SystemExit(f"Missing session manifest: {session_manifest_path}")
    session_manifest = load_json(session_manifest_path)

    iteration = session / "iterations" / args.iteration_id
    if iteration.exists():
        raise SystemExit(f"Iteration already exists: {iteration}")
    for child in ["decisions", "scores", "audits", "summaries", "source_artifacts"]:
        (iteration / child).mkdir(parents=True, exist_ok=True)
        (iteration / child / ".gitkeep").touch(exist_ok=True)

    workspace = workspace_root_from_session(session)
    strategy_source = workspace / "artifacts" / "skills" / "strategies" / str(session_manifest["strategy_id"])
    strategy_skill_file = strategy_source / "SKILL.md"
    if not strategy_skill_file.exists():
        raise SystemExit(f"Missing strategy skill: {strategy_skill_file}")

    snapshot_root = iteration / "source_artifacts" / "strategy_skill_snapshot"
    shutil.copytree(strategy_source, snapshot_root)

    now = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    packet_paths = args.packet_path
    sample_size = args.sample_size
    if packet_paths and len(packet_paths) != sample_size:
        raise SystemExit("--sample-size must match number of --packet-path values when packet paths are provided")

    snapshot_manifest = {
        "captured_at": now,
        "strategy_id": session_manifest["strategy_id"],
        "strategy_version": session_manifest["strategy_version"],
        "source_path": strategy_source.relative_to(workspace).as_posix(),
        "snapshot_path": snapshot_root.relative_to(iteration).as_posix(),
        "files": build_file_hash_index(snapshot_root),
    }
    write_json(iteration / "source_artifacts" / "strategy_skill_snapshot_manifest.json", snapshot_manifest)

    iteration_manifest = {
        "schema_version": "0.2",
        "iteration_id": args.iteration_id,
        "created_at": now,
        "session_id": session_manifest["session_id"],
        "stage": session_manifest["stage"],
        "asset": session_manifest["asset"],
        "strategy_id": session_manifest["strategy_id"],
        "strategy_version": session_manifest["strategy_version"],
        "signal_engine_id": session_manifest.get("signal_engine_id", session_manifest.get("signal_family", "")),
        "signal_family": session_manifest.get("signal_family", ""),
        "signal_set_id": session_manifest["signal_set_id"],
        "sample_method": args.sample_method,
        "sample_size": sample_size,
        "contamination_controls": {
            "ground_truth_hidden": True,
            "future_candles_hidden": True,
            "prior_iteration_results_hidden": True,
            "proposed_fixes_hidden": True,
        },
        "handoff_path": "handoff.md",
        "signal_sample_path": "signal_sample.json",
        "strategy_skill_snapshot": {
            "path": "source_artifacts/strategy_skill_snapshot",
            "manifest_path": "source_artifacts/strategy_skill_snapshot_manifest.json",
        },
        "outputs": {
            "decisions": "decisions/",
            "scores": "scores/",
            "audit": "audits/failure_audit.md",
            "summary": "summaries/iteration_summary.md",
        },
        "status": "created",
    }
    write_json(iteration / "manifest.json", iteration_manifest)

    signal_sample = {
        "schema_version": "0.1",
        "iteration_id": args.iteration_id,
        "sample_method": args.sample_method,
        "sample_size": sample_size,
        "signal_engine_id": session_manifest.get("signal_engine_id", session_manifest.get("signal_family", "")),
        "signal_family": session_manifest.get("signal_family", ""),
        "asset": session_manifest["asset"],
        "signal_set_id": session_manifest["signal_set_id"],
        "packet_paths": packet_paths,
    }
    write_json(iteration / "signal_sample.json", signal_sample)

    output_contract = build_output_contract(
        session_manifest["stage"],
        session_manifest["strategy_id"],
        session_manifest["strategy_version"],
    )

    handoff = f"""# Evaluator Handoff

Session: {session_manifest["session_id"]}
Iteration: {args.iteration_id}
Stage: {session_manifest["stage"]}
Asset: {session_manifest["asset"]}
Strategy skill snapshot: source_artifacts/strategy_skill_snapshot
Strategy version: {session_manifest["strategy_version"]}
Signal engine: {session_manifest.get("signal_engine_id", session_manifest.get("signal_family", ""))}
Signal family: {session_manifest.get("signal_family", "")}
Signal set: {session_manifest["signal_set_id"]}
Sample size: {sample_size}

## Task

Evaluate the sampled neutral signal packets using only the strategy skill snapshot for this iteration.

You are the Backtester role: your job is unbiased evaluation of the assigned sample only,
not strategy improvement, sampling, scoring, or execution.

## Process Protocol

Process signals sequentially, one at a time. Read a single packet, evaluate it fully
against the strategy skill snapshot, record the decision, then proceed to the next packet. Do not
load all packets at once. This mirrors live execution, where signals arrive individually.

Use signal_sample.json as the checklist. Preserve its order exactly. For each listed path,
open and read the full signal packet and apply the strategy skill snapshot as if the packet
had arrived live. Do not use scratch notes, abbreviated packet summaries, or shortcut
evaluations. Keep only the final decision object for each packet, then assemble the final
JSON after all listed packets are complete.

Forbidden shortcut rule: do not use scripts, formulas, batch heuristics, neighboring
signals, filenames, timestamps, prior scores, or any other estimate to approximate a
decision. If a packet has not been opened, read in full, and evaluated directly against the
strategy snapshot, no decision may be recorded for it.

## Inputs

- Strategy skill path: source_artifacts/strategy_skill_snapshot
- Signal sample path: signal_sample.json
- Output path: decisions/

## Sample Boundary

- Evaluate only the packet paths listed in signal_sample.json.
- Preserve the listed order exactly.
- Do not scan the signal directory for more packets.
- Do not replace or expand the sample.

## Contamination Rules

- Do not use ground truth.
- Do not use future candles.
- Do not use prior iteration scores, failures, audits, or proposed fixes.
- Do not use execution setup unless the requested stage is execution setup.

## Output Contract

{output_contract}
"""
    (iteration / "handoff.md").write_text(handoff)

    session_manifest["active_iteration"] = args.iteration_id
    session_manifest["iteration_count"] = int(session_manifest.get("iteration_count", 0)) + 1
    write_json(session_manifest_path, session_manifest)

    print(json.dumps({"iteration": str(iteration), "created": True}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
