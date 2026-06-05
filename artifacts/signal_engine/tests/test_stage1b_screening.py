from vegas.stage1b_screening import (
    build_balanced_sample,
    classify_outcome,
    normalize_stage1b_decision,
    resolve_strategy_skill_id,
    select_evenly_spaced,
    summarize_results,
)


def test_select_evenly_spaced_picks_range_anchors_and_middle() -> None:
    assert select_evenly_spaced(["a", "b", "c", "d", "e"], 3) == ["a", "c", "e"]


def test_build_balanced_sample_preserves_equal_trigger_mix() -> None:
    entries = [
        {"signal_id": "20260101T000000Z", "gt_trigger": True, "gt_direction": "LONG"},
        {"signal_id": "20260102T000000Z", "gt_trigger": False, "gt_direction": None},
        {"signal_id": "20260103T000000Z", "gt_trigger": True, "gt_direction": "SHORT"},
        {"signal_id": "20260104T000000Z", "gt_trigger": False, "gt_direction": None},
        {"signal_id": "20260105T000000Z", "gt_trigger": True, "gt_direction": "LONG"},
        {"signal_id": "20260106T000000Z", "gt_trigger": False, "gt_direction": None},
        {"signal_id": "20260107T000000Z", "gt_trigger": True, "gt_direction": "SHORT"},
        {"signal_id": "20260108T000000Z", "gt_trigger": False, "gt_direction": None},
    ]

    sample = build_balanced_sample(entries, sample_size=4)

    assert [entry["signal_id"] for entry in sample] == [
        "20260101T000000Z",
        "20260102T000000Z",
        "20260107T000000Z",
        "20260108T000000Z",
    ]
    assert sum(1 for entry in sample if entry["gt_trigger"]) == 2
    assert sum(1 for entry in sample if not entry["gt_trigger"]) == 2


def test_summarize_results_computes_screening_metrics_and_directional_hit_rate() -> None:
    results = [
        {"outcome": "TP", "direction": "LONG", "gt_direction": "LONG"},
        {"outcome": "TP", "direction": "SHORT", "gt_direction": "LONG"},
        {"outcome": "FP", "direction": "SHORT", "gt_direction": None},
        {"outcome": "TN", "direction": "LONG", "gt_direction": None},
        {"outcome": "FN", "direction": "SHORT", "gt_direction": "SHORT"},
    ]

    summary = summarize_results(results)

    assert summary == {
        "signals": 5,
        "tp": 2,
        "fp": 1,
        "tn": 1,
        "fn": 1,
        "precision": 66.7,
        "recall": 66.7,
        "directional": 50.0,
    }


def test_classify_outcome_uses_trade_action_as_primary_gate() -> None:
    assert classify_outcome("ENTER", True) == "TP"
    assert classify_outcome("ENTER", False) == "FP"
    assert classify_outcome("SKIP", False) == "TN"
    assert classify_outcome("SKIP", True) == "FN"


def test_normalize_stage1b_decision_accepts_new_entry_gate_contract() -> None:
    decision = normalize_stage1b_decision(
        {
            "signal_id": "20260101T000000Z",
            "trade_action": "enter",
            "direction": "long",
            "confidence": "0.72",
            "expected_travel": "HIGH",
            "entry_gate": "pass",
            "gate_reason_code": "accepted_reclaim_with_room",
            "reasoning": "Accepted reclaim has room before resistance.",
        }
    )

    assert decision == {
        "signal_id": "20260101T000000Z",
        "trade_action": "ENTER",
        "direction": "LONG",
        "confidence": 0.72,
        "expected_travel": "high",
        "entry_gate": "pass",
        "gate_reason_code": "accepted_reclaim_with_room",
        "reasoning": "Accepted reclaim has room before resistance.",
    }


def test_normalize_stage1b_decision_maps_legacy_expected_travel() -> None:
    decision = normalize_stage1b_decision(
        {
            "direction": "SHORT",
            "confidence": 0.61,
            "expected_travel": "low",
            "travel_conviction": 0.58,
            "reasoning": "Rejected control and no room.",
        }
    )

    assert decision["trade_action"] == "SKIP"
    assert decision["entry_gate"] == "fail"
    assert decision["gate_reason_code"] == "legacy_expected_travel_low"
    assert decision["travel_conviction"] == 0.58


def test_normalize_stage1b_decision_rejects_mixed_gate_contract() -> None:
    try:
        normalize_stage1b_decision(
            {
                "trade_action": "ENTER",
                "direction": "LONG",
                "confidence": 0.72,
                "expected_travel": "low",
                "entry_gate": "pass",
                "gate_reason_code": "mixed_contract",
                "reasoning": "Contradictory action and travel.",
            }
        )
    except ValueError as exc:
        assert "expected_travel=low conflicts with trade_action=ENTER" in str(exc)
    else:
        raise AssertionError("mixed Stage 1B contract should fail")


def test_resolve_strategy_skill_id_reads_snapshot_manifest(tmp_path) -> None:
    snapshot = tmp_path / "source_artifacts" / "strategy_skill_snapshot"
    snapshot.mkdir(parents=True)
    skill_file = snapshot / "SKILL.md"
    skill_file.write_text("# Snapshot\n")
    (tmp_path / "source_artifacts" / "strategy_skill_snapshot_manifest.json").write_text(
        '{"strategy_id": "eth-vegas-tunnel-v02"}'
    )

    assert resolve_strategy_skill_id(skill_file) == "eth-vegas-tunnel-v02"
