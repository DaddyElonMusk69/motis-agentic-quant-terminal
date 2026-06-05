import json
from pathlib import Path

from quant_terminal_worker.stage1.workspace import (
    build_stage1_gate_summary,
    create_stage1_iteration_workspace,
    list_stage1_iterations,
    materialize_stage1_session_workspace,
    repair_stage1_iteration_bundle,
)


def test_materialize_stage1_session_workspace_writes_manifest_and_starter_strategy(tmp_path: Path):
    session = {
        "session_id": "stage1-aave-vegas",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "AAVE",
        "artifact_root": str(tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas"),
        "manifest": {
            "session_id": "stage1-aave-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "aave-vegas-tunnel-v01",
        },
    }

    result = materialize_stage1_session_workspace(workspace_root=tmp_path, session=session)

    artifact_root = Path(session["artifact_root"])
    assert (artifact_root / "manifest.json").exists()
    assert (artifact_root / "inputs").is_dir()
    assert (artifact_root / "iterations").is_dir()
    assert (artifact_root / "promotion").is_dir()
    assert (artifact_root / "strategy_module" / "strategy.py").exists()
    assert json.loads((artifact_root / "manifest.json").read_text())["session_id"] == "stage1-aave-vegas"
    assert result["strategy_entrypoint"].endswith("strategy_module.strategy:decide")


def test_materialize_stage1_session_workspace_copies_seed_strategy_source(tmp_path: Path):
    seed_path = tmp_path / "engine_base" / "strategy.py"
    seed_path.parent.mkdir()
    seed_path.write_text("def decide(context):\n    return {'seed': 'engine-base'}\n")
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas"
    session = {
        "session_id": "stage1-aave-vegas",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "AAVE",
        "artifact_root": str(artifact_root),
        "seed_strategy_source_type": "engine_base",
        "seed_strategy_source_path": str(seed_path),
        "seed_strategy_source_version": "0.1",
        "manifest": {
            "session_id": "stage1-aave-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "aave-vegas-tunnel-v01",
        },
    }

    materialize_stage1_session_workspace(workspace_root=tmp_path, session=session)

    strategy_path = artifact_root / "strategy_module" / "strategy.py"
    manifest = json.loads((artifact_root / "manifest.json").read_text())
    assert strategy_path.read_text() == seed_path.read_text()
    assert manifest["seed_strategy"]["source_type"] == "engine_base"
    assert manifest["seed_strategy"]["source_path"] == str(seed_path)


def test_create_stage1_iteration_workspace_writes_handoff_sample_and_snapshot(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas"
    session = {
        "session_id": "stage1-aave-vegas",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "AAVE",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "artifact_root": str(artifact_root),
        "manifest": {
            "session_id": "stage1-aave-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "aave-vegas-tunnel-v01",
        },
    }
    materialize_stage1_session_workspace(workspace_root=tmp_path, session=session)
    signals = [
        {
            "signal_id": "sig-old",
            "timestamp": "2026-03-02T00:00:00Z",
            "payload": {"future_ground_truth": "LONG"},
        },
        {
            "signal_id": "sig-new",
            "timestamp": "2026-04-25T00:00:00Z",
            "payload": {"future_ground_truth": "SHORT"},
        },
        {
            "signal_id": "sig-latest",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {"future_ground_truth": "LONG"},
        },
    ]

    result = create_stage1_iteration_workspace(
        workspace_root=tmp_path,
        session=session,
        signals=signals,
        sample_method="training",
    )

    iteration_root = Path(result["iteration_root"])
    sample = json.loads((iteration_root / "signal_sample.json").read_text())
    handoff = (iteration_root / "handoff.md").read_text()
    prompt = (iteration_root / "agent_prompt.md").read_text()

    assert result["iteration_id"] == "iter_001_v0.1"
    assert (iteration_root / "decisions").is_dir()
    assert (iteration_root / "scores").is_dir()
    assert (iteration_root / "audits").is_dir()
    assert (iteration_root / "summaries").is_dir()
    assert (iteration_root / "source_artifacts/strategy_module_snapshot/strategy.py").exists()
    assert [item["signal_id"] for item in sample["signals"]] == ["sig-old", "sig-new", "sig-latest"]
    assert sample["signals"][0]["packet"] == {}
    assert sample["sample_method"] == "training"
    assert sample["signal_count"] == 3
    assert "future_ground_truth" not in json.dumps(sample)
    assert "future_ground_truth" not in handoff
    assert "future_ground_truth" not in prompt
    assert "Do not use future outcomes" in handoff
    assert "embedded `packet` JSON" in handoff
    assert "packet paths" not in handoff.lower()
    assert "signal folder" in handoff
    assert "source_artifacts/strategy_module_snapshot" in prompt
    assert str(iteration_root / "signal_sample.json") in prompt
    assert "embedded packet JSON" in prompt
    assert "listed packet paths" not in prompt
    assert str(artifact_root / "strategy_module" / "strategy.py") in prompt


def test_create_stage1_iteration_workspace_dedupes_duplicate_signal_timestamps_and_prefers_canonical_ids(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/zec-vegas-tunnel-v01/stage1-zec-vegas"
    legacy_packet_dir = tmp_path / "dev/signals/vegas_ema/ZEC/2026-ZEC-2h-dedupe-vote2/packets"
    legacy_packet_dir.mkdir(parents=True)
    (legacy_packet_dir / "20260304T132500Z.json").write_text('{"signal_id":"20260304T132500Z"}')
    session = {
        "session_id": "stage1-zec-vegas",
        "strategy_id": "zec-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "ZEC",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "ZEC-vegas_ema-canonical",
        "artifact_root": str(artifact_root),
        "manifest": {
            "session_id": "stage1-zec-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "zec-vegas-tunnel-v01",
        },
    }
    materialize_stage1_session_workspace(workspace_root=tmp_path, session=session)
    signals = [
        {
            "signal_id": "vegas_ema:ZEC:2026-ZEC-2h-dedupe-vote2:20260304T132500Z",
            "signal_set_key": "vegas_ema:ZEC:ZEC-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "timestamp": "2026-03-04T13:25:00Z",
            "payload": {"variant": "legacy"},
        },
        {
            "signal_id": "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z",
            "signal_set_key": "vegas_ema:ZEC:ZEC-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "timestamp": "2026-03-04T13:25:00Z",
            "payload": {"variant": "canonical"},
        },
    ]

    result = create_stage1_iteration_workspace(
        workspace_root=tmp_path,
        session=session,
        signals=signals,
        sample_method="training",
    )

    sample = json.loads((Path(result["iteration_root"]) / "signal_sample.json").read_text())

    assert sample["signal_count"] == 1
    assert sample["signals"][0]["signal_id"] == "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z"
    assert sample["signals"][0]["packet"] == {"variant": "canonical"}
    assert sample["signals"][0]["packet_path"].endswith("2026-ZEC-2h-dedupe-vote2/packets/20260304T132500Z.json")


def test_repair_stage1_iteration_bundle_rewrites_stale_samples_and_handoff(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/zec-vegas-tunnel-v01/stage1-zec-vegas"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    snapshot_dir = iteration_root / "source_artifacts" / "strategy_module_snapshot"
    snapshot_dir.mkdir(parents=True)
    (snapshot_dir / "strategy.py").write_text("def decide(context):\n    return {}\n")
    strategy_path = artifact_root / "strategy_module" / "strategy.py"
    strategy_path.parent.mkdir(parents=True)
    strategy_path.write_text("def decide(context):\n    return {}\n")
    (artifact_root / "manifest.json").write_text(
        json.dumps(
            {
                "session_id": "stage1-zec-vegas",
                "strategy_id": "zec-vegas-tunnel-v01",
                "strategy_version": "v0.1",
                "asset": "ZEC",
                "signal_engine_id": "vegas_ema",
                "signal_set_id": "ZEC-vegas_ema-canonical",
                "signal_set_key": "vegas_ema:ZEC:ZEC-vegas_ema-canonical",
            }
        )
    )
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_001_v0.1", "sample_method": "training", "signal_count": 2})
    )
    legacy_packet_dir = tmp_path / "dev/signals/vegas_ema/ZEC/2026-ZEC-2h-dedupe-vote2/packets"
    legacy_packet_dir.mkdir(parents=True)
    (legacy_packet_dir / "20260304T132500Z.json").write_text('{"signal_id":"20260304T132500Z"}')
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "sample_method": "training",
                "signal_count": 2,
                "signals": [
                    {
                        "signal_id": "vegas_ema:ZEC:2026-ZEC-2h-dedupe-vote2:20260304T132500Z",
                        "timestamp": "2026-03-04T13:25:00Z",
                        "packet_path": str(tmp_path / "missing.json"),
                        "packet": {"variant": "legacy"},
                    },
                    {
                        "signal_id": "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z",
                        "timestamp": "2026-03-04T13:25:00Z",
                        "packet_path": str(tmp_path / "missing-too.json"),
                        "packet": {"variant": "canonical"},
                    },
                ],
            }
        )
    )
    (iteration_root / "handoff.md").write_text("Signal packet paths:\n- bad\n")
    (iteration_root / "agent_prompt.md").write_text("old prompt")

    result = repair_stage1_iteration_bundle(workspace_root=tmp_path, iteration_root=iteration_root)

    sample = json.loads((iteration_root / "signal_sample.json").read_text())
    handoff = (iteration_root / "handoff.md").read_text()

    assert result["signal_count"] == 1
    assert sample["signal_count"] == 1
    assert sample["signals"][0]["signal_id"] == "vegas_ema:ZEC:ZEC-vegas_ema-canonical:20260304T132500Z"
    assert sample["signals"][0]["packet_path"].endswith("2026-ZEC-2h-dedupe-vote2/packets/20260304T132500Z.json")
    assert "Use only the embedded `packet` JSON" in handoff
    assert "Signal packet paths" not in handoff


def test_create_stage1_iteration_workspace_writes_builder_bundle_with_training_labels(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas"
    stage0_root = tmp_path / "dev/stage0/universe/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (ground_truth_root / "sig-old.json").write_text(
        json.dumps(
            {
                "signal_id": "sig-old",
                "natural_direction": "LONG",
                "first_move_pct": 1.2,
                "max_travel_pct": 2.4,
                "opposite_max_pct": 0.3,
                "first_move_hours": 8.0,
                "status": "triggered",
            }
        )
    )
    (ground_truth_root / "sig-new.json").write_text(
        json.dumps(
            {
                "signal_id": "sig-new",
                "natural_direction": "SHORT",
                "first_move_pct": 0.9,
                "max_travel_pct": 1.7,
                "opposite_max_pct": 0.4,
                "first_move_hours": 12.0,
                "status": "triggered",
            }
        )
    )
    session = {
        "session_id": "stage1-aave-vegas",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "AAVE",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "stage0_artifact_root": str(stage0_root),
        "artifact_root": str(artifact_root),
        "manifest": {
            "session_id": "stage1-aave-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "aave-vegas-tunnel-v01",
            "stage0_artifact_root": str(stage0_root),
        },
    }
    signals = [
        {
            "signal_id": "sig-old",
            "timestamp": "2026-03-02T00:00:00Z",
            "payload": {"schema_version": "signal_packet.v2", "asset": "AAVE", "timestamp": "2026-03-02T00:00:00Z"},
        },
        {
            "signal_id": "sig-new",
            "timestamp": "2026-04-25T00:00:00Z",
            "payload": {"schema_version": "signal_packet.v2", "asset": "AAVE", "timestamp": "2026-04-25T00:00:00Z"},
        },
    ]

    result = create_stage1_iteration_workspace(
        workspace_root=tmp_path,
        session=session,
        signals=signals,
        sample_method="training",
        bundle_role="strategy_builder",
    )

    iteration_root = Path(result["iteration_root"])
    builder_sample = json.loads((iteration_root / "builder_training_sample.json").read_text())
    prompt = (iteration_root / "strategy_builder_prompt.md").read_text()
    evaluator_sample = json.loads((iteration_root / "signal_sample.json").read_text())

    assert result["builder_prompt_path"].endswith("strategy_builder_prompt.md")
    assert result["builder_training_sample_path"].endswith("builder_training_sample.json")
    assert builder_sample["ground_truth_visible"] is True
    assert builder_sample["signals"][0]["ground_truth"]["natural_direction"] == "LONG"
    assert builder_sample["signals"][1]["ground_truth"]["natural_direction"] == "SHORT"
    assert f"Edit {artifact_root / 'strategy_module' / 'strategy.py'}" in prompt
    assert "New Stage 1 bundles automatically snapshot the current session strategy file" in prompt
    assert str(iteration_root / "builder_training_sample.json") in prompt
    assert "embedded training packet JSON" in prompt
    assert "training packet paths" not in prompt
    assert "walk-forward" in prompt.lower()
    assert "walk-forward" in prompt
    assert evaluator_sample["signals"][0]["packet"]["schema_version"] == "signal_packet.v2"
    assert "natural_direction" not in json.dumps(evaluator_sample)


def test_create_stage1_iteration_workspace_writes_training_builder_prompt(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave-vegas"
    stage0_root = tmp_path / "dev/stage0/universe/vegas_ema/AAVE/2026-AAVE-2h-dedupe-vote2"
    ground_truth_root = stage0_root / "scores" / "ground_truth"
    ground_truth_root.mkdir(parents=True)
    (ground_truth_root / "sig-a.json").write_text(json.dumps({"signal_id": "sig-a", "natural_direction": "LONG"}))
    (ground_truth_root / "sig-b.json").write_text(json.dumps({"signal_id": "sig-b", "natural_direction": "SHORT"}))
    session = {
        "session_id": "stage1-aave-vegas",
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "asset": "AAVE",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
        "stage0_artifact_root": str(stage0_root),
        "artifact_root": str(artifact_root),
        "manifest": {
            "session_id": "stage1-aave-vegas",
            "stage": "stage1a_directional_agreement",
            "strategy_id": "aave-vegas-tunnel-v01",
            "stage0_artifact_root": str(stage0_root),
        },
    }
    signals = [
        {"signal_id": "sig-a", "timestamp": "2026-03-02T00:00:00Z", "payload": {"schema_version": "signal_packet.v2"}},
        {"signal_id": "sig-b", "timestamp": "2026-05-10T00:00:00Z", "payload": {"schema_version": "signal_packet.v2"}},
    ]

    result = create_stage1_iteration_workspace(
        workspace_root=tmp_path,
        session=session,
        signals=signals,
        sample_method="training",
        bundle_role="strategy_builder",
    )

    iteration_root = Path(result["iteration_root"])
    prompt = (iteration_root / "strategy_builder_prompt.md").read_text()

    assert "training-window natural_direction labels" in prompt
    assert "Do not use walk-forward labels, packets, score files, or future candles" in prompt
    assert "click Score on this iteration, then create the walk-forward test bundle" in prompt


def test_list_stage1_iterations_reports_bundle_score_and_audit_state(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    (iteration_root / "decisions").mkdir(parents=True)
    (iteration_root / "scores").mkdir()
    (iteration_root / "audits").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps(
            {
                "iteration_id": "iter_001_v0.1",
                "sample_method": "training",
                "signal_count": 3,
                "status": "created",
            }
        )
    )
    (iteration_root / "strategy_builder_prompt.md").write_text("builder")
    (iteration_root / "builder_training_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("evaluator")
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "scores/stage1a_directional_scores.json").write_text(
        json.dumps({"metrics": {"directional_agreement": 1.0, "matches": 3, "mismatches": 0, "neutral": 0}})
    )
    (iteration_root / "audits/failure_audit.json").write_text(
        json.dumps({"metrics": {"failure_count": 0, "protected_count": 3}})
    )
    session = {"artifact_root": str(artifact_root)}

    iterations = list_stage1_iterations(workspace_root=tmp_path, session=session)

    assert len(iterations) == 1
    assert iterations[0]["iteration_id"] == "iter_001_v0.1"
    assert iterations[0]["bundle_role"] == "strategy_builder"
    assert iterations[0]["has_training_score"] is True
    assert iterations[0]["has_failure_audit"] is True
    assert iterations[0]["training_score"]["metrics"]["matches"] == 3
    assert iterations[0]["failure_audit"]["metrics"]["protected_count"] == 3


def test_build_stage1_gate_summary_blocks_until_all_slice_scores_pass(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    for iteration_id, role, score_name, passes in (
        ("iter_001_v0.1", "training", "stage1a_directional_scores.json", True),
        ("iter_002_v0.1", "walk_forward_test", "stage1a_walk_forward_scores.json", False),
    ):
        iteration_root = artifact_root / "iterations" / iteration_id
        (iteration_root / "scores").mkdir(parents=True)
        (iteration_root / "decisions").mkdir()
        (iteration_root / "summaries").mkdir()
        (iteration_root / "manifest.json").write_text(
            json.dumps({"iteration_id": iteration_id, "sample_method": role, "signal_count": 3})
        )
        (iteration_root / "signal_sample.json").write_text("{}")
        (iteration_root / "agent_prompt.md").write_text("prompt")
        (iteration_root / "scores" / score_name).write_text(
            json.dumps(
                {
                    "metrics": {
                        "directional_agreement": 0.8 if passes else 0.4,
                        "matches": 2 if passes else 1,
                        "mismatches": 1,
                        "neutral": 0,
                        "passes_threshold": passes,
                    }
                }
            )
        )
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "draft"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["ready_to_freeze"] is False
    assert gate["roles"]["training"]["status"] == "pass"
    assert gate["roles"]["walk_forward_test"]["status"] == "fail"
    assert "Walk-Forward" in gate["blockers"][0]


def test_build_stage1_gate_summary_reports_missing_walk_forward_checkpoint(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_003_v0.1"
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_003_v0.1", "sample_method": "training", "signal_count": 12})
    )
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "strategy_builder_prompt.md").write_text("builder")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "draft"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["ready_to_freeze"] is False
    assert gate["roles"]["training"]["status"] == "missing"
    assert gate["roles"]["walk_forward_test"]["status"] == "missing"


def test_build_stage1_gate_summary_reports_canonical_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    role_files = {
        "training": "stage1a_directional_scores.json",
        "walk_forward_test": "stage1a_walk_forward_scores.json",
    }
    for index, (role, score_name) in enumerate(role_files.items(), start=1):
        iteration_root = artifact_root / "iterations" / f"iter_{index:03d}_v0.1"
        (iteration_root / "scores").mkdir(parents=True)
        (iteration_root / "decisions").mkdir()
        (iteration_root / "summaries").mkdir()
        (iteration_root / "manifest.json").write_text(
            json.dumps({"iteration_id": f"iter_{index:03d}_v0.1", "sample_method": role, "signal_count": 3})
        )
        (iteration_root / "signal_sample.json").write_text("{}")
        (iteration_root / "agent_prompt.md").write_text("prompt")
        (iteration_root / "scores" / score_name).write_text(
            json.dumps({"metrics": {"directional_agreement": 0.8, "matches": 2, "passes_threshold": True}})
        )
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 6}, "slice_metrics": {}, "match_set": [{"signal_id": "sig"}]})
    )
    (frozen_root / "strategy.py").write_text("def decide(context): return {}")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "stage1a_frozen"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["status"] == "stage1a_frozen"
    assert gate["ready_to_freeze"] is True
    assert gate["canonical_readout"]["exists"] is True
    assert gate["canonical_readout"]["match_count"] == 1
    assert gate["stage2_capture"]["exists"] is False


def test_build_stage1_gate_summary_reports_stage2_capture_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "slice_metrics": {}, "match_set": [{"signal_id": "sig"}]})
    )
    (frozen_root / "strategy.py").write_text("def decide(context): return {}")
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps({"metrics": {"total_match_signals": 1}, "results": {"1.0": {"full_cycle": {"rate": 100.0}}}})
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "stage1a_frozen"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["stage2_capture"]["exists"] is True
    assert gate["stage2_capture"]["capture_curve_path"].endswith("promotion/stage2_capture_curve.json")
    assert gate["stage2_capture"]["metrics"]["total_match_signals"] == 1
    assert gate["stage3_grid"]["exists"] is False


def test_build_stage1_gate_summary_reports_stage3_grid_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "slice_metrics": {}, "match_set": [{"signal_id": "sig"}]})
    )
    (frozen_root / "strategy.py").write_text("def decide(context): return {}")
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"metrics": {"total_match_signals": 1}}))
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"total_signals": 1, "optimal": {"best": {"tp": 2.5, "sl": 1.0}}})
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 2.5, "sl": 1.0}}))
    (promotion_root / "stage4_candidates.json").write_text(json.dumps({"candidates": [{"candidate_id": "market"}]}))
    (promotion_root / "stage3_summary.md").write_text("# Stage 3 Grid Search\n")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "stage1a_frozen"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["stage3_grid"]["exists"] is True
    assert gate["stage3_grid"]["grid_results_path"].endswith("promotion/stage3_grid_results.json")
    assert gate["stage3_grid"]["best"]["tp"] == 2.5
    assert gate["stage3_pyramid"]["exists"] is False


def test_build_stage1_gate_summary_reports_stage3_pyramid_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    frozen_root = promotion_root / "frozen_stage1a_strategy_module"
    frozen_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_decisions.json").write_text("{}")
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({"metrics": {"matches": 1}, "slice_metrics": {}, "match_set": [{"signal_id": "sig"}]})
    )
    (frozen_root / "strategy.py").write_text("def decide(context): return {}")
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"metrics": {"total_match_signals": 1}}))
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"total_signals": 1, "optimal": {"best": {"tp": 2.5, "sl": 1.0}}})
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 2.5, "sl": 1.0}}))
    (promotion_root / "stage3_summary.md").write_text("# Stage 3 Grid Search\n")
    (promotion_root / "stage3_pyramid_results.json").write_text(
        json.dumps({"total_signals": 1, "tp_pct": 2.5, "sl_pct": 1.0, "baseline": {}, "results": []})
    )
    (promotion_root / "stage3_pyramid_optimal.json").write_text(json.dumps({"best": {"step_pct": 0.5, "pnl_pct": 12.0}}))
    (promotion_root / "stage4_candidates.json").write_text(json.dumps({"candidates": [{"candidate_id": "pyramid"}]}))
    (promotion_root / "stage3_pyramid_summary.md").write_text("# Stage 3 Pyramid\n")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "stage1a_frozen"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["stage3_pyramid"]["exists"] is True
    assert gate["stage3_pyramid"]["results_path"].endswith("promotion/stage3_pyramid_results.json")
    assert gate["stage3_pyramid"]["best"]["step_pct"] == 0.5
    assert gate["stage4_realized_expectancy"]["exists"] is False


def test_build_stage1_gate_summary_reports_stage4_realized_expectancy_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({"best_candidate_id": "market", "best_candidate": {"net_expectancy_pct": 1.2}, "candidates": []})
    )
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(json.dumps({"best": {"candidate_id": "market", "net_expectancy_pct": 1.2}}))
    (promotion_root / "stage4_summary.md").write_text("# Stage 4 Realized Expectancy\n")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "stage1a_frozen"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["stage4_realized_expectancy"]["exists"] is True
    assert gate["stage4_realized_expectancy"]["best_candidate_id"] == "market"
    assert gate["stage4_realized_expectancy"]["best_candidate"]["net_expectancy_pct"] == 1.2
