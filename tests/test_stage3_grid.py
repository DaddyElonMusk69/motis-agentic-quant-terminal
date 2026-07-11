import json
from pathlib import Path

import pytest

import quant_terminal_worker.stage3.grid_search as stage3_grid
from quant_terminal_worker.stage3.grid_search import (
    _build_trade_candle_windows,
    run_stage3_exact_protection,
    run_stage3_fixed_sl_baseline,
    run_stage3_grid_search,
    run_stage3_local_variants,
)


def test_stage3_substeps_write_artifacts_incrementally(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.5, initial_sl_pct=0.8, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-protected",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            }
        ],
    )
    session = _session(artifact_root, stage0_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 100.8, "close": 101.0},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.0, "high": 101.6, "low": 100.4, "close": 101.5},
    ]

    fixed = run_stage3_fixed_sl_baseline(workspace_root=tmp_path, session=session, candles=candles)

    assert fixed["fixed_sl_complete"] is True
    assert fixed["exact_protection_complete"] is False
    assert fixed["local_variants_complete"] is False
    assert fixed["fixed_sl_baseline_result"]["protection_enabled"] is False
    assert (promotion_root / "stage3_grid_results.json").exists()
    assert not (promotion_root / "stage3_optimal.json").exists()
    assert not (promotion_root / "stage4_candidates.json").exists()

    exact = run_stage3_exact_protection(workspace_root=tmp_path, session=session, candles=candles)

    assert exact["fixed_sl_complete"] is True
    assert exact["exact_protection_complete"] is True
    assert exact["local_variants_complete"] is False
    assert exact["exact_protection_result"]["protection_enabled"] is True
    assert not (promotion_root / "stage3_optimal.json").exists()
    assert not (promotion_root / "stage4_candidates.json").exists()

    variants = run_stage3_local_variants(workspace_root=tmp_path, session=session, candles=candles, shortlist_size=2)

    assert variants["fixed_sl_complete"] is True
    assert variants["exact_protection_complete"] is True
    assert variants["local_variants_complete"] is True
    assert variants["stage3c_shortlist"] == variants["optimal"]["top_5"]
    assert variants["stage4_candidates"]["candidates"]
    assert (promotion_root / "stage3_optimal.json").exists()
    assert (promotion_root / "stage4_candidates.json").exists()


def test_stage3_local_variants_requires_exact_protection_substep(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.5, initial_sl_pct=0.8, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            }
        ],
    )

    with pytest.raises(ValueError, match="Stage 3B exact protection"):
        run_stage3_local_variants(workspace_root=tmp_path, session=_session(artifact_root, stage0_root), candles=[])


def test_stage3_fixed_target_preserves_tp_sl_across_protection_variants(
    tmp_path: Path,
):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    fixed_target_path = stage0_root / "scores/fixed_target_contract.json"
    fixed_target_path.parent.mkdir(parents=True, exist_ok=True)
    fixed_target_path.write_text(
        json.dumps(
            {
                "schema_version": "signal_discovery_target.v1",
                "config_hash": "frozen-btc-target",
                "selected_target": {
                    "selected_risk_pct": 1.0,
                    "reward_multiple": 2.0,
                    "stop_multiple": 1.0,
                    "horizon_hours": 36,
                    "entry_delay_minutes": 5,
                    "entry_semantics": "next_5m_open",
                },
            }
        )
    )
    (stage0_root / "scores/ground_truth_summary.json").write_text(
        json.dumps(
            {
                "label_contract": "fixed_r_first_touch.v1",
                "fixed_target_contract_path": str(fixed_target_path),
                "metrics": {"total_records": 2},
            }
        )
    )
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5, 2.0, 2.5])
    fixed_policy = {
        "lock_profit_pct": 2.0,
        "initial_sl_pct": 1.0,
        "protect_trigger_pct": 1.0,
        "trail_sl_pct": 1.0,
    }
    (promotion_root / "stage2_exit_policy.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "artifact_role": "stage2_exit_policy",
                "policy_source": "signal_discovery_fixed_target",
                "policy_mode": "shared",
                "policy": fixed_policy,
                "side_policies": {"LONG": fixed_policy, "SHORT": fixed_policy},
                "fixed_target": {
                    "config_hash": "frozen-btc-target",
                    "target_pct": 2.0,
                    "stop_pct": 1.0,
                    "forward_hours": 36,
                },
            }
        )
    )
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "train-fixed",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "wf-fixed",
                "sample_role": "walk_forward_test",
                "decision_direction": "SHORT",
                "direction": "SHORT",
                "agreement": "MATCH",
                "signal_ts": "2026-05-03T00:00:00Z",
                "reference_price": 100,
            },
        ],
    )
    candles = [
        {
            "timestamp": "2026-05-01T00:05:00Z",
            "open": 100,
            "high": 102.1,
            "low": 99.5,
            "close": 101.8,
        },
        {
            "timestamp": "2026-05-03T00:05:00Z",
            "open": 100,
            "high": 100.5,
            "low": 97.9,
            "close": 98.2,
        },
    ]

    result = run_stage3_grid_search(
        workspace_root=tmp_path,
        session=_session(artifact_root, stage0_root),
        candles=candles,
        tp_values=[0.5, 1.0, 1.5, 2.0, 2.5],
        forward_hours=1,
        shortlist_size=20,
        leverage=1,
        fees_bps_per_side=0,
    )

    assert result["policy_source"] == "signal_discovery_fixed_target"
    assert result["forward_hours"] == 36
    assert result["tp_values"] == [2.0]
    assert result["sl_values"] == [1.0]
    assert result["fixed_sl_baseline_result"]["final_tp_pct"] == 2.0
    assert result["fixed_sl_baseline_result"]["initial_sl_pct"] == 1.0
    assert result["local_variant_results"]
    assert all(row["protection_enabled"] for row in result["local_variant_results"])
    assert {row["final_tp_pct"] for row in result["results"]} == {2.0}
    assert {row["initial_sl_pct"] for row in result["results"]} == {1.0}
    assert {row["initial_sl_multiplier"] for row in result["results"]} == {1.0}
    assert {
        candidate["setup"]["tp_pct"]
        for candidate in result["stage4_candidates"]["candidates"]
    } == {2.0}
    assert {
        candidate["setup"]["sl_pct"]
        for candidate in result["stage4_candidates"]["candidates"]
    } == {1.0}


def test_run_stage3_policy_test_scores_fixed_baseline_exact_policy_and_shortlists_stage4_candidates(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.5, initial_sl_pct=0.7, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-protected",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "sig-initial-sl",
                "sample_role": "walk_forward_test",
                "decision_direction": "SHORT",
                "direction": "SHORT",
                "agreement": "MISMATCH",
                "signal_ts": "2026-05-01T01:00:00Z",
                "reference_price": 200,
            },
        ],
    )
    session = _session(artifact_root, stage0_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 100.8, "close": 101.0},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.0, "high": 101.1, "low": 100.4, "close": 100.6},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 200, "high": 202.5, "low": 199.6, "close": 202.0},
    ]

    result = run_stage3_grid_search(workspace_root=tmp_path, session=session, candles=candles, shortlist_size=2)

    assert result["stage3_mode"] == "numerical_exit_policy"
    assert result["total_executable_decisions"] == 2
    assert result["stage0_risk_policy"] == {"initial_sl_pct": 0.7, "stage0_meaningful_move_threshold_pct": 1.0, "hard_exit_hours": 2}
    fixed = result["fixed_sl_baseline_result"]
    assert fixed["stage3_step"] == "fixed_sl_baseline"
    assert fixed["protection_enabled"] is False
    assert fixed["protected_sl_count"] == 0
    assert fixed["initial_sl_count"] == 1
    exact = result["exact_protection_result"]
    assert result["exact_policy_result"] == exact
    assert exact["stage3_step"] == "exact_protection_policy"
    assert exact["protection_enabled"] is True
    assert exact["protected_sl_count"] == 1
    assert exact["initial_sl_count"] == 1
    assert exact["agreement_split"]["MATCH"]["protected_sl_count"] == 1
    assert exact["agreement_split"]["MISMATCH"]["initial_sl_count"] == 1
    assert exact["net_pnl_pct"] < exact["gross_pnl_pct"]
    assert result["optimal"]["best"]["stage3_mode"] == "numerical_exit_policy"
    assert result["stage3c_shortlist"] == result["optimal"]["top_5"]
    assert result["stage4_candidates"]["candidates"][0]["setup"]["tp_pct"] == result["optimal"]["best"]["tp"]
    assert isinstance(result["stage4_candidates"]["candidates"][0]["setup"]["protection_enabled"], bool)
    assert result["stage4_candidates"]["candidates"][0]["setup"]["exit_policy_type"] == "numerical_stage2_policy"
    candidate = result["stage4_candidates"]["candidates"][0]
    assert candidate["selection_diagnostics"]["stage3_ranking"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
    assert "walk_forward_net_pnl_pct" in candidate["selection_diagnostics"]["stage3_ranking"]
    assert "full_cycle_net_pnl_pct" in candidate["selection_diagnostics"]["stage3_ranking"]
    assert (promotion_root / "stage3_grid_results.json").exists()
    assert (promotion_root / "stage3_optimal.json").exists()
    assert (promotion_root / "stage4_candidates.json").exists()
    assert "Stage 3 Numerical Exit Policy" in (promotion_root / "stage3_summary.md").read_text()


def test_stage3_local_variants_use_only_adjacent_stage2_levels_and_sl_multipliers(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=0.8, forward_hours=3)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5, 2.0])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.0, initial_sl_pct=0.8, protect_trigger_pct=1.0, trail_sl_pct=1.0)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-match",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "sig-mismatch",
                "sample_role": "walk_forward_test",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MISMATCH",
                "signal_ts": "2026-05-01T01:00:00Z",
                "reference_price": 100,
            },
        ],
    )
    session = _session(artifact_root, stage0_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101.0},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 100, "high": 100.2, "low": 99.0, "close": 99.2},
    ]

    result = run_stage3_grid_search(workspace_root=tmp_path, session=session, candles=candles, shortlist_size=5)

    variants = result["local_variant_results"]
    assert variants
    fixed_variants = [row for row in variants if not row["protection_enabled"]]
    protected_variants = [row for row in variants if row["protection_enabled"]]
    assert fixed_variants
    assert protected_variants
    assert {row["stage3_step"] for row in fixed_variants} == {"fixed_sl_variant"}
    assert {row["protect_trigger_pct"] for row in fixed_variants} == {None}
    assert {row["trail_sl_pct"] for row in fixed_variants} == {None}
    assert {row["final_tp_pct"] for row in variants} <= {0.5, 1.0, 1.5}
    assert {row["protect_trigger_pct"] for row in protected_variants} <= {0.5, 1.0, 1.5}
    assert {row["trail_sl_pct"] for row in protected_variants} <= {0.5, 1.0, 1.5}
    assert {row["initial_sl_multiplier"] for row in variants} == {0.75, 1.0, 1.25}
    assert {round(row["initial_sl_pct"], 4) for row in variants} == {0.6, 0.8, 1.0}
    assert result["optimal"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
    assert result["optimal"]["best"]["agreement_split"]["MISMATCH"]["total"] == 1
    assert result["stage3c_total_combinations_tested"] == len(variants)
    assert result["stage3c_value_ranges"]["initial_sl_multipliers"] == [0.75, 1.0, 1.25]


def test_stage3_ranking_prefers_walk_forward_when_full_cycle_is_viable(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=1)
    _write_stage2_capture(promotion_root, tp_levels=[1.0, 3.0])
    _write_stage2_policy(promotion_root, lock_profit_pct=3.0, initial_sl_pct=1.0, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "train-a",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "wf-a",
                "sample_role": "walk_forward_test",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-02T00:00:00Z",
                "reference_price": 100,
            },
        ],
    )
    session = _session(artifact_root, stage0_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 103.1, "low": 99.8, "close": 102.5},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 101.1, "low": 99.8, "close": 100.8},
    ]

    result = run_stage3_grid_search(
        workspace_root=tmp_path,
        session=session,
        candles=candles,
        shortlist_size=5,
        leverage=1,
        fees_bps_per_side=0,
    )

    best = result["optimal"]["best"]
    assert result["optimal"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
    assert best["ranking_diagnostics"]["walk_forward_viable"] is True
    assert best["ranking_diagnostics"]["full_cycle_viable"] is True
    assert best["slice_split"]["walk_forward_test"]["net_pnl_pct"] >= 1.0
    assert best["final_tp_pct"] == 1.0


def test_stage3_uses_side_specific_stage2_policy_by_trade_direction(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage2_side_policy(
        promotion_root,
        long_policy={"lock_profit_pct": 1.5, "initial_sl_pct": 0.5, "protect_trigger_pct": 1.0, "trail_sl_pct": 0.5},
        short_policy={"lock_profit_pct": 0.5, "initial_sl_pct": 1.0, "protect_trigger_pct": 0.5, "trail_sl_pct": 0.5},
    )
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-long",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "sig-short",
                "sample_role": "training",
                "decision_direction": "SHORT",
                "direction": "SHORT",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T03:00:00Z",
                "reference_price": 200,
            },
        ],
    )
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.6, "close": 100.9},
        {"timestamp": "2026-05-01T03:05:00Z", "open": 200, "high": 200.2, "low": 198.8, "close": 199.0},
    ]

    result = run_stage3_fixed_sl_baseline(workspace_root=tmp_path, session=_session(artifact_root, stage0_root), candles=candles)

    fixed = result["fixed_sl_baseline_result"]
    assert fixed["policy_mode"] == "side_specific"
    assert fixed["side_policies"]["LONG"]["final_tp_pct"] == 1.5
    assert fixed["side_policies"]["SHORT"]["final_tp_pct"] == 0.5
    assert fixed["side_split"]["LONG"]["time_exit_count"] == 1
    assert fixed["side_split"]["SHORT"]["tp_count"] == 1
    assert {row["outcome"] for row in fixed["outcomes"]} == {"TIME_EXIT", "TP"}


def test_stage3c_side_specific_variants_are_paired_not_cartesian(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage2_side_policy(
        promotion_root,
        long_policy={"lock_profit_pct": 1.0, "initial_sl_pct": 0.8, "protect_trigger_pct": 1.0, "trail_sl_pct": 0.5},
        short_policy={"lock_profit_pct": 1.5, "initial_sl_pct": 1.0, "protect_trigger_pct": 1.0, "trail_sl_pct": 0.5},
    )
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-long",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            },
            {
                "signal_id": "sig-short",
                "sample_role": "walk_forward_test",
                "decision_direction": "SHORT",
                "direction": "SHORT",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T01:00:00Z",
                "reference_price": 200,
            },
        ],
    )
    session = _session(artifact_root, stage0_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101.0},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 200, "high": 200.2, "low": 197.0, "close": 198.0},
    ]

    result = run_stage3_grid_search(workspace_root=tmp_path, session=session, candles=candles, shortlist_size=5)

    variants = result["local_variant_results"]
    assert variants
    assert result["policy_mode"] == "side_specific"
    assert result["stage3c_total_combinations_tested"] == len(variants)
    assert len(variants) <= 61
    assert all(row["policy_mode"] == "side_specific" for row in variants)
    assert all(set(row["side_policies"]) == {"LONG", "SHORT"} for row in variants)
    assert any(row["side_policies"]["LONG"]["final_tp_pct"] != row["side_policies"]["SHORT"]["final_tp_pct"] for row in variants)


def test_stage3_trade_candle_windows_trim_to_post_signal_hard_exit_range():
    trades = [
        {
            "signal_id": "sig-window",
            "sample_role": "training",
            "direction": "LONG",
            "decision_direction": "LONG",
            "agreement": "MATCH",
            "signal_ts": "2026-05-01T00:10:00Z",
            "reference_price": 100,
        }
    ]
    candles = [
        {"timestamp": "2026-05-01T00:00:00Z", "open": 100, "high": 100, "low": 100, "close": 100},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 100, "high": 100, "low": 100, "close": 100},
        {"timestamp": "2026-05-01T00:15:00Z", "open": 100, "high": 101, "low": 99, "close": 100},
        {"timestamp": "2026-05-01T01:10:00Z", "open": 100, "high": 101, "low": 99, "close": 100},
        {"timestamp": "2026-05-01T01:15:00Z", "open": 100, "high": 101, "low": 99, "close": 100},
    ]

    windows = _build_trade_candle_windows(trades=trades, candles=candles, hard_exit_hours=1)

    assert [[row["timestamp"].isoformat().replace("+00:00", "Z") for row in window] for window in windows] == [
        ["2026-05-01T00:15:00Z", "2026-05-01T01:10:00Z"]
    ]


def test_stage3_policy_scoring_uses_precomputed_trade_windows(monkeypatch):
    trades = [
        {
            "signal_id": "sig-window",
            "sample_role": "training",
            "direction": "LONG",
            "decision_direction": "LONG",
            "agreement": "MATCH",
            "signal_ts": "2026-05-01T00:10:00Z",
            "reference_price": 100,
        }
    ]
    full_candles = [
        {"timestamp": "2026-05-01T00:00:00Z", "open": 100, "high": 100, "low": 100, "close": 100},
        {"timestamp": "2026-05-01T00:15:00Z", "open": 100, "high": 101, "low": 99, "close": 100},
    ]
    window = [full_candles[1]]
    observed_candles = []

    def fake_simulate_policy_trade(**kwargs):
        observed_candles.append(kwargs["candles"])
        return {
            "signal_id": kwargs["trade"]["signal_id"],
            "sample_role": kwargs["trade"]["sample_role"],
            "direction": kwargs["trade"]["direction"],
            "agreement": kwargs["trade"]["agreement"],
            "outcome": "TIME_EXIT",
            "gross_pnl_pct": 0.0,
            "fees_pct": 0.0,
            "net_pnl_pct": 0.0,
        }

    monkeypatch.setattr(stage3_grid, "_simulate_policy_trade", fake_simulate_policy_trade)

    stage3_grid._score_policy_config(
        config={
            "config_id": "fixed",
            "stage3_step": "fixed_sl_variant",
            "policy_mode": "shared",
            "protection_enabled": False,
            "final_tp_pct": 1.0,
            "initial_sl_pct": 1.0,
            "protect_trigger_pct": None,
            "trail_sl_pct": None,
            "hard_exit_hours": 1,
        },
        trades=trades,
        candles=full_candles,
        trade_candle_windows=[window],
        leverage=1,
        fees_bps_per_side=0,
    )

    assert observed_candles == [window]


def test_stage3_policy_test_refuses_missing_stage2_exit_policy(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[1.0])
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            }
        ],
    )

    with pytest.raises(ValueError, match="promoted Stage 2 exit policy"):
        run_stage3_grid_search(workspace_root=tmp_path, session=_session(artifact_root, stage0_root), candles=[])


def test_stage3_policy_test_refuses_missing_stage0_risk_policy(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage2_capture(promotion_root, tp_levels=[1.0])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.0, initial_sl_pct=1.0, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            }
        ],
    )

    with pytest.raises(ValueError, match="Stage 0 ground truth summary"):
        run_stage3_grid_search(workspace_root=tmp_path, session=_session(artifact_root, stage0_root), candles=[])


def test_stage3_policy_rerun_clears_pyramid_and_stage4_artifacts(tmp_path: Path):
    artifact_root, stage0_root = _stage3_workspace(tmp_path)
    promotion_root = artifact_root / "promotion"
    _write_stage0_summary(stage0_root, significance_threshold_pct=1.0, forward_hours=2)
    _write_stage2_capture(promotion_root, tp_levels=[1.0])
    _write_stage2_policy(promotion_root, lock_profit_pct=1.0, initial_sl_pct=1.0, protect_trigger_pct=1.0, trail_sl_pct=0.5)
    _write_trade_inputs(
        promotion_root,
        [
            {
                "signal_id": "sig-long",
                "sample_role": "training",
                "decision_direction": "LONG",
                "direction": "LONG",
                "agreement": "MATCH",
                "signal_ts": "2026-05-01T00:00:00Z",
                "reference_price": 100,
            }
        ],
    )
    for artifact in [
        "stage3_pyramid_results.json",
        "stage3_pyramid_optimal.json",
        "stage4_realized_expectancy.json",
        "stage4_trade_ledger.json",
        "stage4_optimal.json",
        "stage4_summary.md",
    ]:
        (promotion_root / artifact).write_text("{}")
    stage4_run_dir = promotion_root / "stage4_runs" / "old-run"
    stage4_run_dir.mkdir(parents=True)
    (stage4_run_dir / "stage4_realized_expectancy.json").write_text("{}")
    session = _session(artifact_root, stage0_root)
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.8, "close": 101}]

    result = run_stage3_grid_search(workspace_root=tmp_path, session=session, candles=candles)

    assert result["stage4_candidates"]["candidates"]
    assert not (promotion_root / "stage3_pyramid_results.json").exists()
    assert not (promotion_root / "stage4_realized_expectancy.json").exists()
    assert not (promotion_root / "stage4_runs").exists()


def _stage3_workspace(tmp_path: Path) -> tuple[Path, Path]:
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    stage0_root = tmp_path / "dev/stage0/universe-1/vegas_ema/AAVE/AAVE-vegas_ema-canonical"
    return artifact_root, stage0_root


def _session(artifact_root: Path, stage0_root: Path) -> dict:
    return {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "stage0_artifact_root": str(stage0_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }


def _write_stage0_summary(stage0_root: Path, *, significance_threshold_pct: float, forward_hours: int) -> None:
    scores_root = stage0_root / "scores"
    scores_root.mkdir(parents=True)
    (scores_root / "ground_truth_summary.json").write_text(
        json.dumps(
            {
                "metrics": {
                    "significance_threshold_pct": significance_threshold_pct,
                    "forward_hours": forward_hours,
                }
            }
        )
    )


def _write_stage2_capture(promotion_root: Path, *, tp_levels: list[float]) -> None:
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps(
            {
                "tp_levels": tp_levels,
                "metrics": {"total_trade_decisions": 1, "match_count": 1, "mismatch_count": 0},
                "stage3_input": {
                    "tp_range_source": "stage2_trade_profile",
                    "recommended_tp_min_pct": min(tp_levels),
                    "recommended_tp_max_pct": max(tp_levels),
                },
            }
        )
    )


def _write_stage2_policy(
    promotion_root: Path,
    *,
    lock_profit_pct: float,
    initial_sl_pct: float,
    protect_trigger_pct: float,
    trail_sl_pct: float,
) -> None:
    (promotion_root / "stage2_exit_policy.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "artifact_role": "stage2_exit_policy",
                "policy": {
                    "lock_profit_pct": lock_profit_pct,
                    "initial_sl_pct": initial_sl_pct,
                    "protect_trigger_pct": protect_trigger_pct,
                    "trail_sl_pct": trail_sl_pct,
                },
            }
        )
    )


def _write_stage2_side_policy(
    promotion_root: Path,
    *,
    long_policy: dict[str, float],
    short_policy: dict[str, float],
) -> None:
    (promotion_root / "stage2_exit_policy.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "artifact_role": "stage2_exit_policy",
                "policy_mode": "side_specific",
                "policy": long_policy,
                "side_policies": {
                    "LONG": long_policy,
                    "SHORT": short_policy,
                },
            }
        )
    )


def _write_trade_inputs(promotion_root: Path, rows: list[dict]) -> None:
    (promotion_root / "stage3_trade_inputs.json").write_text(json.dumps(rows))
