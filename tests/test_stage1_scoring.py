import json
from pathlib import Path

from quant_terminal_worker.stage1.scoring import (
    generate_stage1a_failure_audit,
    run_stage1a_canonical_full_cycle,
    run_stage1a_score,
    run_stage1a_training_score,
)


def test_run_stage1a_training_score_executes_strategy_and_writes_artifacts(tmp_path: Path):
    iteration_root = tmp_path / "iteration"
    packets_root = tmp_path / "packets"
    strategy_root = iteration_root / "strategy_module"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    signal_id = context["signal"]["signal_id"]
    if signal_id.endswith("short"):
        direction = "SHORT"
    else:
        direction = "LONG"
    return {
        "decision_id": f"test-{signal_id}",
        "strategy_id": "unit-strategy",
        "strategy_version": "v0.1",
        "signal_id": signal_id,
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": direction,
        "confidence": 0.8,
        "reason_code": "unit_rule",
        "diagnostics": {"rule": "suffix"},
    }
"""
    )
    (packets_root / "sig-long.json").write_text(
        json.dumps({"signal_id": "sig-long", "asset": "AAVE", "payload": {"charts": {}}})
    )
    (packets_root / "sig-short.json").write_text(
        json.dumps({"signal_id": "sig-short", "asset": "AAVE", "payload": {"charts": {}}})
    )
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {"signal_id": "sig-long", "packet_path": str(packets_root / "sig-long.json")},
                    {"signal_id": "sig-short", "packet_path": str(packets_root / "sig-short.json")},
                ]
            }
        )
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps(
            {
                "session_id": "stage1-aave",
                "signals": [
                    {"signal_id": "sig-long", "ground_truth": {"natural_direction": "LONG"}},
                    {"signal_id": "sig-short", "ground_truth": {"natural_direction": "LONG"}},
                ],
            }
        )
    )

    result = run_stage1a_training_score(iteration_root=iteration_root)

    decisions = json.loads((iteration_root / "decisions/stage1a_directional_decisions.json").read_text())
    scores = json.loads((iteration_root / "scores/stage1a_directional_scores.json").read_text())
    summary = (iteration_root / "summaries/iteration_summary.md").read_text()
    assert result["metrics"]["directional_agreement"] == 0.5
    assert decisions["decisions"][0]["direction"] == "LONG"
    assert scores["records"][0]["agreement"] == "MATCH"
    assert scores["records"][1]["agreement"] == "MISMATCH"
    assert scores["metrics"]["matches"] == 1
    assert "Directional agreement: 50.00%" in summary


def test_run_stage1a_training_score_marks_skip_as_neutral(tmp_path: Path):
    iteration_root = tmp_path / "iteration"
    packets_root = tmp_path / "packets"
    strategy_root = iteration_root / "strategy_module"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "skip",
        "strategy_id": "unit-strategy",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "SKIP",
        "action": "SKIP",
        "direction": "FLAT",
        "confidence": 0.3,
        "reason_code": "no_direction",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "sig-flat.json").write_text(json.dumps({"signal_id": "sig-flat", "payload": {}}))
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-flat", "packet_path": str(packets_root / "sig-flat.json")}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-flat", "ground_truth": {"natural_direction": "SHORT"}}]})
    )

    result = run_stage1a_training_score(iteration_root=iteration_root)

    assert result["metrics"]["neutral"] == 1
    assert result["metrics"]["directional_agreement"] == 0
    scores = json.loads((iteration_root / "scores/stage1a_directional_scores.json").read_text())
    assert scores["records"][0]["agreement"] == "NEUTRAL"


def test_run_stage1a_training_score_uses_session_strategy_when_iteration_copy_is_absent(tmp_path: Path):
    session_root = tmp_path / "dev/training_sessions/aave/stage1-aave"
    iteration_root = session_root / "iterations" / "iter_001_v0.1"
    packets_root = tmp_path / "packets"
    strategy_root = session_root / "strategy_module"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "session-strategy",
        "strategy_id": "unit-strategy",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "SHORT",
        "confidence": 0.8,
        "reason_code": "session_level",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "sig-short.json").write_text(json.dumps({"signal_id": "sig-short", "payload": {}}))
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-short", "packet_path": str(packets_root / "sig-short.json")}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-short", "ground_truth": {"natural_direction": "SHORT"}}]})
    )

    result = run_stage1a_training_score(iteration_root=iteration_root)

    assert result["metrics"]["matches"] == 1


def test_generate_stage1a_failure_audit_writes_failure_ledger_and_prompt(tmp_path: Path):
    iteration_root = tmp_path / "iteration"
    packets_root = tmp_path / "packets"
    (iteration_root / "audits").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    packets_root.mkdir()
    (packets_root / "sig-miss.json").write_text(json.dumps({"signal_id": "sig-miss", "payload": {"setup": "miss"}}))
    (packets_root / "sig-flat.json").write_text(json.dumps({"signal_id": "sig-flat", "payload": {"setup": "flat"}}))
    (packets_root / "sig-win.json").write_text(json.dumps({"signal_id": "sig-win", "payload": {"setup": "win"}}))
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {"signal_id": "sig-miss", "packet_path": str(packets_root / "sig-miss.json")},
                    {"signal_id": "sig-flat", "packet_path": str(packets_root / "sig-flat.json")},
                    {"signal_id": "sig-win", "packet_path": str(packets_root / "sig-win.json")},
                ]
            }
        )
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {"signal_id": "sig-miss", "ground_truth": {"natural_direction": "SHORT", "first_move_pct": 2.2}},
                    {"signal_id": "sig-flat", "ground_truth": {"natural_direction": "LONG", "first_move_pct": 1.4}},
                    {"signal_id": "sig-win", "ground_truth": {"natural_direction": "LONG", "first_move_pct": 1.9}},
                ]
            }
        )
    )
    (iteration_root / "decisions/stage1a_directional_decisions.json").write_text(
        json.dumps(
            {
                "decisions": [
                    {"signal_id": "sig-miss", "direction": "LONG", "trade_action": "ENTER", "reason_code": "wrong"},
                    {"signal_id": "sig-flat", "direction": "FLAT", "trade_action": "SKIP", "reason_code": "skip"},
                    {"signal_id": "sig-win", "direction": "LONG", "trade_action": "ENTER", "reason_code": "right"},
                ]
            }
        )
    )
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps(
            {
                "metrics": {"total": 3, "matches": 1, "mismatches": 1, "neutral": 1},
                "records": [
                    {"signal_id": "sig-miss", "agreement": "MISMATCH", "ground_truth_direction": "SHORT", "decision_direction": "LONG"},
                    {"signal_id": "sig-flat", "agreement": "NEUTRAL", "ground_truth_direction": "LONG", "decision_direction": "FLAT"},
                    {"signal_id": "sig-win", "agreement": "MATCH", "ground_truth_direction": "LONG", "decision_direction": "LONG"},
                ],
            }
        )
    )

    result = generate_stage1a_failure_audit(iteration_root=iteration_root)

    audit = json.loads((iteration_root / "audits/failure_audit.json").read_text())
    prompt = (iteration_root / "agent_failure_audit_prompt.md").read_text()
    markdown = (iteration_root / "audits/failure_audit.md").read_text()
    assert result["metrics"]["failure_count"] == 2
    assert audit["failure_cases"][0]["signal_id"] == "sig-miss"
    assert audit["protected_cases"][0]["signal_id"] == "sig-win"
    assert "smallest possible Stage 1A direction-only update" in prompt
    assert "Do not add Stage 1B entry gates" in prompt
    assert str(iteration_root / "audits" / "failure_audit.json") in prompt
    assert str(iteration_root.parents[1] / "strategy_module" / "strategy.py") in prompt
    assert "New Stage 1 bundles automatically snapshot the current session strategy file" in prompt
    assert "sig-flat" in markdown


def test_generate_stage1a_failure_audit_writes_walk_forward_diagnostic_prompt(tmp_path: Path):
    iteration_root = tmp_path / "dev/training_sessions/aave/stage1-aave/iterations/iter_002_v0.1"
    stage0_root = tmp_path / "dev/stage0/aave"
    packets_root = tmp_path / "packets"
    (iteration_root / "audits").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    packets_root.mkdir()
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (iteration_root.parents[1] / "manifest.json").write_text(json.dumps({"stage0_artifact_root": str(stage0_root)}))
    (packets_root / "sig-val.json").write_text(json.dumps({"signal_id": "sig-val", "payload": {"setup": "walk-forward"}}))
    (ground_truth_root / "sig-val.json").write_text(json.dumps({"signal_id": "sig-val", "natural_direction": "SHORT"}))
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-val", "packet_path": str(packets_root / "sig-val.json")}]})
    )
    (iteration_root / "decisions/stage1a_walk_forward_decisions.json").write_text(
        json.dumps({"decisions": [{"signal_id": "sig-val", "direction": "LONG", "trade_action": "ENTER", "reason_code": "bad_walk_forward"}]})
    )
    (iteration_root / "scores/stage1a_walk_forward_scores.json").write_text(
        json.dumps(
            {
                "metrics": {"total": 1, "matches": 0, "mismatches": 1, "neutral": 0},
                "records": [
                    {"signal_id": "sig-val", "agreement": "MISMATCH", "ground_truth_direction": "SHORT", "decision_direction": "LONG"}
                ],
            }
        )
    )

    result = generate_stage1a_failure_audit(iteration_root=iteration_root, sample_role="walk_forward_test")

    audit = json.loads((iteration_root / "audits/failure_audit.json").read_text())
    prompt = (iteration_root / "agent_failure_audit_prompt.md").read_text()
    assert result["sample_role"] == "walk_forward_test"
    assert audit["sample_role"] == "walk_forward_test"
    assert audit["agent_handoff_policy"] == "postmortem_only"
    assert "Do not edit the strategy based on walk-forward test evidence" in prompt
    assert "postmortem only" in prompt.lower()


def test_generate_stage1a_failure_audit_writes_walk_forward_postmortem_prompt(tmp_path: Path):
    iteration_root = tmp_path / "dev/training_sessions/aave/stage1-aave/iterations/iter_003_v0.1"
    stage0_root = tmp_path / "dev/stage0/aave"
    packets_root = tmp_path / "packets"
    (iteration_root / "audits").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "scores").mkdir()
    packets_root.mkdir()
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (iteration_root.parents[1] / "manifest.json").write_text(json.dumps({"stage0_artifact_root": str(stage0_root)}))
    (packets_root / "sig-oos.json").write_text(json.dumps({"signal_id": "sig-oos", "payload": {"setup": "oos"}}))
    (ground_truth_root / "sig-oos.json").write_text(json.dumps({"signal_id": "sig-oos", "natural_direction": "LONG"}))
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-oos", "packet_path": str(packets_root / "sig-oos.json")}]})
    )
    (iteration_root / "decisions/stage1a_walk_forward_decisions.json").write_text(
        json.dumps({"decisions": [{"signal_id": "sig-oos", "direction": "SHORT", "trade_action": "ENTER", "reason_code": "bad_oos"}]})
    )
    (iteration_root / "scores/stage1a_walk_forward_scores.json").write_text(
        json.dumps(
            {
                "metrics": {"total": 1, "matches": 0, "mismatches": 1, "neutral": 0},
                "records": [
                    {"signal_id": "sig-oos", "agreement": "MISMATCH", "ground_truth_direction": "LONG", "decision_direction": "SHORT"}
                ],
            }
        )
    )

    result = generate_stage1a_failure_audit(iteration_root=iteration_root, sample_role="walk_forward_test")

    audit = json.loads((iteration_root / "audits/failure_audit.json").read_text())
    prompt = (iteration_root / "agent_failure_audit_prompt.md").read_text()
    assert result["sample_role"] == "walk_forward_test"
    assert audit["agent_handoff_policy"] == "postmortem_only"
    assert "postmortem only" in prompt.lower()
    assert "Do not edit" in prompt


def test_run_stage1a_score_writes_walk_forward_test_artifacts_and_loads_stage0_labels(tmp_path: Path):
    session_root = tmp_path / "dev/training_sessions/aave/stage1-aave"
    iteration_root = session_root / "iterations" / "iter_001_v0.1"
    stage0_root = tmp_path / "dev/stage0/aave"
    packets_root = tmp_path / "packets"
    strategy_root = session_root / "strategy_module"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    packets_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    ground_truth_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (session_root / "manifest.json").write_text(json.dumps({"stage0_artifact_root": str(stage0_root)}))
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "walk-forward-strategy",
        "strategy_id": "unit-strategy",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "SHORT",
        "confidence": 0.8,
        "reason_code": "walk_forward_rule",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "20260501T000000Z.json").write_text(json.dumps({"signal_id": "20260501T000000Z", "payload": {}}))
    full_signal_id = "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2:20260501T000000Z"
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": full_signal_id, "packet_path": str(packets_root / "20260501T000000Z.json")}]})
    )
    (ground_truth_root / "20260501T000000Z.json").write_text(
        json.dumps({"signal_id": "20260501T000000Z", "natural_direction": "SHORT"})
    )

    result = run_stage1a_score(iteration_root=iteration_root, sample_role="walk_forward_test")

    assert result["metrics"]["matches"] == 1
    assert result["scores_path"].endswith("scores/stage1a_walk_forward_scores.json")
    assert result["decisions_path"].endswith("decisions/stage1a_walk_forward_decisions.json")
    assert (iteration_root / "summaries/walk_forward_summary.md").exists()


def test_run_stage1a_score_falls_back_to_stage0_subset_packets_when_canonical_path_is_missing(tmp_path: Path):
    session_root = tmp_path / "dev/training_sessions/zec/stage1-zec"
    iteration_root = session_root / "iterations" / "iter_001_v0.1"
    stage0_root = tmp_path / "dev/stage0/zec"
    subset_packets_root = stage0_root / "scores" / "_scoreable_signal_subset" / "packets"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    strategy_root = session_root / "strategy_module"
    subset_packets_root.mkdir(parents=True)
    ground_truth_root.mkdir(parents=True)
    strategy_root.mkdir(parents=True)
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "summaries").mkdir()
    (session_root / "manifest.json").write_text(json.dumps({"stage0_artifact_root": str(stage0_root)}))
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "decision_id": "zec-decision",
        "strategy_id": "unit-strategy",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "LONG",
        "confidence": 0.8,
        "reason_code": "fallback_packet",
        "diagnostics": {},
    }
"""
    )
    full_signal_id = "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z"
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "signal_id": full_signal_id,
                        "packet_path": str(
                            tmp_path / "dev/signals/vegas_ema/ZEC/ZEC-vegas_ema-canonical/packets/20260304T132500Z.json"
                        ),
                    }
                ]
            }
        )
    )
    (subset_packets_root / "20260304T132500Z.json").write_text(
        json.dumps({"schema_version": "signal_packet.v2", "asset": "ZEC", "timestamp": "2026-03-04T13:25:00Z"})
    )
    (ground_truth_root / "20260304T132500Z.json").write_text(
        json.dumps({"signal_id": "20260304T132500Z", "natural_direction": "LONG"})
    )

    result = run_stage1a_score(iteration_root=iteration_root, sample_role="training")

    assert result["metrics"]["matches"] == 1
    decisions = json.loads((iteration_root / "decisions/stage1a_directional_decisions.json").read_text())
    assert decisions["decisions"][0]["signal_id"] == full_signal_id


def test_run_stage1a_canonical_full_cycle_writes_frozen_readout_for_downstream_stages(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    strategy_root = artifact_root / "strategy_module"
    packets_root = tmp_path / "dev/signals/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2/packets"
    stage0_root = tmp_path / "dev/stage0/aave"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    strategy_root.mkdir(parents=True)
    packets_root.mkdir(parents=True)
    ground_truth_root.mkdir(parents=True)
    (strategy_root / "__init__.py").write_text("")
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    signal_id = context["signal"]["signal_id"]
    direction = "SHORT" if signal_id.endswith("short") else "LONG"
    return {
        "decision_id": f"canonical-{signal_id}",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "signal_id": signal_id,
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": direction,
        "confidence": 0.8,
        "reason_code": "canonical_test",
        "diagnostics": {},
    }
"""
    )
    for packet_name in ("sig-train-long", "sig-walk-forward-short"):
        (packets_root / f"{packet_name}.json").write_text(json.dumps({"signal_id": packet_name, "payload": {}}))
    (ground_truth_root / "sig-train-long.json").write_text(
        json.dumps({"signal_id": "sig-train-long", "natural_direction": "LONG"})
    )
    (ground_truth_root / "sig-walk-forward-short.json").write_text(
        json.dumps({"signal_id": "sig-walk-forward-short", "natural_direction": "SHORT"})
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "stage0_artifact_root": str(stage0_root),
        "source_candidate_id": "candidate-aave",
        "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_engine_version": "0.1",
        "asset": "AAVE",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "manifest": {"stage0_artifact_root": str(stage0_root)},
    }

    result = run_stage1a_canonical_full_cycle(
        workspace_root=tmp_path,
        session=session,
        signals_by_role={
            "training": [{"signal_id": "sig-train-long", "signal_set_key": session["signal_set_key"]}],
            "walk_forward_test": [{"signal_id": "sig-walk-forward-short", "signal_set_key": session["signal_set_key"]}],
        },
    )

    scores = json.loads((artifact_root / "promotion/stage1a_canonical_full_cycle_scores.json").read_text())
    decisions = json.loads((artifact_root / "promotion/stage1a_canonical_full_cycle_decisions.json").read_text())
    assert result["metrics"]["matches"] == 2
    assert result["match_count"] == 2
    assert scores["stage2_stage3_input"]["role"] == "canonical_match_set"
    assert scores["stage4_input"]["role"] == "canonical_full_decision_set"
    assert scores["slice_metrics"]["walk_forward_test"]["matches"] == 1
    assert decisions["decisions"][1]["sample_role"] == "walk_forward_test"
    assert (artifact_root / "promotion/frozen_stage1a_strategy_module/strategy.py").exists()
