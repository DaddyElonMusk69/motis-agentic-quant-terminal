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
        sample_method="recent_regime_train",
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
    assert sample["sample_method"] == "recent_regime_train"
    assert sample["signal_count"] == 3
    assert "future_ground_truth" not in json.dumps(sample)
    assert "future_ground_truth" not in handoff
    assert "future_ground_truth" not in prompt
    assert "Do not use future outcomes" in handoff
    assert "embedded `packet` JSON" in handoff
    assert "packet paths" not in handoff.lower()
    assert "global packet folders" in handoff
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
        sample_method="recent_regime_train",
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
        json.dumps({"iteration_id": "iter_001_v0.1", "sample_method": "recent_regime_train", "signal_count": 2})
    )
    legacy_packet_dir = tmp_path / "dev/signals/vegas_ema/ZEC/2026-ZEC-2h-dedupe-vote2/packets"
    legacy_packet_dir.mkdir(parents=True)
    (legacy_packet_dir / "20260304T132500Z.json").write_text('{"signal_id":"20260304T132500Z"}')
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "sample_method": "recent_regime_train",
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
        sample_method="recent_regime_train",
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
    assert "validation" in prompt.lower()
    assert "locked OOS" in prompt
    assert evaluator_sample["signals"][0]["packet"]["schema_version"] == "signal_packet.v2"
    assert "natural_direction" not in json.dumps(evaluator_sample)


def test_create_stage1_iteration_workspace_writes_final_refit_bundle_prompt(tmp_path: Path):
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
        sample_method="final_refit_ab",
        bundle_role="strategy_builder",
    )

    iteration_root = Path(result["iteration_root"])
    prompt = (iteration_root / "strategy_builder_prompt.md").read_text()

    assert "Training + Forward Validation final refit" in prompt
    assert "Locked OOS labels and packets remain hidden" in prompt
    assert "After editing" in prompt and "create the locked OOS evaluator bundle" in prompt


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
                "sample_method": "recent_regime_train",
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
        ("iter_001_v0.1", "recent_regime_train", "stage1a_directional_scores.json", True),
        ("iter_002_v0.1", "forward_validation", "stage1a_forward_validation_scores.json", False),
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
    assert gate["roles"]["recent_regime_train"]["status"] == "pass"
    assert gate["roles"]["forward_validation"]["status"] == "fail"
    assert gate["roles"]["locked_recent_oos"]["status"] == "missing"
    assert "Locked OOS" in gate["blockers"][1]
    assert gate["final_refit"]["exists"] is False


def test_build_stage1_gate_summary_reports_final_refit_checkpoint(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_003_v0.1"
    (iteration_root / "scores").mkdir(parents=True)
    (iteration_root / "decisions").mkdir()
    (iteration_root / "summaries").mkdir()
    (iteration_root / "manifest.json").write_text(
        json.dumps({"iteration_id": "iter_003_v0.1", "sample_method": "final_refit_ab", "signal_count": 12})
    )
    (iteration_root / "signal_sample.json").write_text("{}")
    (iteration_root / "agent_prompt.md").write_text("prompt")
    (iteration_root / "strategy_builder_prompt.md").write_text("builder")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root), "status": "draft"}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["final_refit"]["exists"] is True
    assert gate["final_refit"]["iteration_id"] == "iter_003_v0.1"
    assert gate["final_refit"]["signal_count"] == 12


def test_build_stage1_gate_summary_reports_canonical_complete(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    role_files = {
        "recent_regime_train": "stage1a_directional_scores.json",
        "forward_validation": "stage1a_forward_validation_scores.json",
        "locked_recent_oos": "stage1a_locked_oos_scores.json",
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
