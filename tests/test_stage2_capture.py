import json
from pathlib import Path

from quant_terminal_worker.stage2.capture_curve import run_stage2_capture_curve


def test_stage2_fixed_target_preserves_frozen_policy_and_writes_exit_handoff(
    tmp_path: Path,
):
    artifact_root = tmp_path / "dev/training_sessions/btc-outcome-first/stage1-btc"
    promotion_root = artifact_root / "promotion"
    stage0_root = tmp_path / "dev/signal_discovery_sessions/btc/handoff/stage0"
    promotion_root.mkdir(parents=True)
    (stage0_root / "scores").mkdir(parents=True)
    (stage0_root / "scores/ground_truth_summary.json").write_text(
        json.dumps(
            {
                "label_contract": "fixed_r_first_touch.v1",
                "fixed_target_contract_path": str(
                    stage0_root / "scores/fixed_target_contract.json"
                ),
                "metrics": {"total_records": 2},
            }
        )
    )
    (stage0_root / "scores/fixed_target_contract.json").write_text(
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
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-fixed",
                        "sample_role": "training",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                    }
                ]
            }
        )
    )
    session = {
        "session_id": "stage1-btc-fixed",
        "artifact_root": str(artifact_root),
        "stage0_artifact_root": str(stage0_root),
        "asset": "BTC",
        "strategy_id": "btc-outcome-first",
        "strategy_version": "v0.1",
        "signal_engine_id": "fixture-engine",
        "signal_set_id": "BTC-fixture-engine-canonical",
    }

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=[
            {
                "signal_id": "sig-fixed",
                "timestamp": "2026-05-01T00:00:00Z",
                "payload": {
                    "timestamp": "2026-05-01T00:00:00Z",
                    "evidence": {"reference_price": 100.0},
                },
            }
        ],
        candles=[
            {
                "timestamp": "2026-05-01T00:05:00Z",
                "open": 100,
                "high": 101.2,
                "low": 99.5,
                "close": 101.0,
            }
        ],
        tp_levels=[0.5, 1.0, 1.5, 2.0, 2.5],
        forward_hours=1,
    )

    assert result["policy_source"] == "signal_discovery_fixed_target"
    assert result["forward_hours"] == 36
    assert result["fixed_target"] == {
        "config_hash": "frozen-btc-target",
        "target_pct": 2.0,
        "stop_pct": 1.0,
        "forward_hours": 36,
        "entry_delay_minutes": 5,
        "entry_semantics": "next_5m_open",
    }
    assert result["stage3_input"]["recommended_tp_min_pct"] == 2.0
    assert result["stage3_input"]["recommended_tp_max_pct"] == 2.0
    assert result["stage3_input"]["recommended_sl_min_pct"] == 1.0
    assert result["stage3_input"]["recommended_sl_max_pct"] == 1.0
    policy = json.loads((promotion_root / "stage2_exit_policy.json").read_text())
    assert policy["policy_source"] == "signal_discovery_fixed_target"
    assert policy["policy_mode"] == "shared"
    assert policy["policy"]["lock_profit_pct"] == 2.0
    assert policy["policy"]["initial_sl_pct"] == 1.0
    assert policy["source"]["fixed_target_contract_path"] == str(
        stage0_root / "scores/fixed_target_contract.json"
    )


def test_run_stage2_capture_curve_reads_liquidity_sweep_reference_price(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-lse/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-lse-high",
                        "sample_role": "training",
                        "decision_direction": "SHORT",
                        "agreement": "MATCH",
                    }
                ]
            }
        )
    )
    session = {
        "session_id": "stage1-aave-lse",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-lse",
        "strategy_version": "v0.1",
        "signal_engine_id": "liquidity_sweep_v1",
        "signal_set_id": "AAVE-liquidity_sweep_v1-canonical",
    }
    signal_rows = [
        {
            "signal_id": "sig-lse-high",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {
                "schema_version": "signal_packet.v2",
                "active_timeframes": ["5m"],
                "evidence": {
                    "pattern": "liquidity_sweep_event",
                    "event_type": "HIGH_SWEEP",
                    "trigger_candle_close": "100",
                    "trigger_price": "101",
                },
            },
        }
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 100.2, "low": 99.2, "close": 99.5},
    ]

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signal_rows,
        candles=candles,
        tp_levels=[0.5],
        forward_hours=1,
    )

    assert result["per_signal"][0]["reference_price"] == 100.0
    assert result["per_signal"][0]["first_tp_reached"] == 0.5


def test_run_stage2_capture_curve_scores_match_set_by_slice(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "session_id": "stage1-aave",
                "match_set": [
                    {
                        "signal_id": "sig-long",
                        "sample_role": "training",
                        "decision_direction": "LONG",
                        "ground_truth_direction": "LONG",
                    },
                    {
                        "signal_id": "sig-short",
                        "sample_role": "walk_forward_test",
                        "decision_direction": "SHORT",
                        "ground_truth_direction": "SHORT",
                    },
                ],
            }
        )
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }
    signal_rows = [
        {
            "signal_id": "sig-long",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        },
        {
            "signal_id": "sig-short",
            "timestamp": "2026-05-02T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": [{"timeframe": "2h", "market_price": 200}]},
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101, "high": 102.1, "low": 100.5, "close": 102},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 200, "high": 201, "low": 198.4, "close": 199},
        {"timestamp": "2026-05-02T00:10:00Z", "open": 199, "high": 200, "low": 197.0, "close": 198},
    ]

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signal_rows,
        candles=candles,
        tp_levels=[0.5, 1.0, 1.5, 2.0],
        forward_hours=36,
    )

    assert result["metrics"]["total_match_signals"] == 2
    assert result["results"]["1.0"]["full_cycle"] == {"reached": 2, "total": 2, "rate": 100.0}
    assert result["results"]["1.5"]["training"] == {"reached": 1, "total": 1, "rate": 100.0}
    assert result["results"]["2.0"]["walk_forward_test"] == {"reached": 0, "total": 1, "rate": 0.0}
    assert result["side_splits"]["LONG"]["count"] == 1
    assert result["side_splits"]["SHORT"]["count"] == 1
    assert result["side_splits"]["LONG"]["results"]["2.0"]["full_cycle"] == {"reached": 1, "total": 1, "rate": 100.0}
    assert result["side_splits"]["SHORT"]["results"]["2.0"]["full_cycle"] == {"reached": 0, "total": 1, "rate": 0.0}
    assert result["side_splits"]["LONG"]["sl_results"]["0.3"]["full_cycle"] == {"hit": 0, "total": 1, "rate": 0.0}
    assert result["side_splits"]["SHORT"]["sl_results"]["0.5"]["full_cycle"] == {"hit": 1, "total": 1, "rate": 100.0}
    assert result["per_signal"][0]["first_tp_reached"] == 0.5
    assert (promotion_root / "stage2_capture_curve.json").exists()
    assert (promotion_root / "stage2_capture_per_signal.json").exists()
    assert "Stage 2 Travel Capture" in (promotion_root / "stage2_summary.md").read_text()


def test_stage2_recommends_tp_from_training_and_reports_walk_forward_guardrails(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {"signal_id": "train-1", "sample_role": "training", "decision_direction": "LONG", "agreement": "MATCH"},
                    {"signal_id": "train-2", "sample_role": "training", "decision_direction": "LONG", "agreement": "MATCH"},
                    {"signal_id": "wf-1", "sample_role": "walk_forward_test", "decision_direction": "LONG", "agreement": "MATCH"},
                ]
            }
        )
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }
    signal_rows = [
        {
            "signal_id": "train-1",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}},
        },
        {
            "signal_id": "train-2",
            "timestamp": "2026-05-01T01:00:00Z",
            "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}},
        },
        {
            "signal_id": "wf-1",
            "timestamp": "2026-05-02T00:00:00Z",
            "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}},
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.1, "low": 99.8, "close": 100.8},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 100, "high": 101.2, "low": 99.7, "close": 101.0},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 100.6, "low": 99.6, "close": 100.4},
    ]

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signal_rows,
        candles=candles,
        tp_levels=[0.5, 1.0],
        forward_hours=1,
    )

    assert result["stage3_input"]["tp_range_source"] == "stage2_training_match_profile_with_walk_forward_guardrail"
    assert result["stage3_input"]["recommended_tp_max_pct"] == 1.0
    assert result["stage3_input"]["walk_forward_guardrail"]["0.5"]["status"] == "pass"
    assert result["stage3_input"]["walk_forward_guardrail"]["1.0"]["status"] == "fail"
    assert result["stage3_input"]["walk_forward_guardrail"]["1.0"]["reason"] == "walk_forward_capture_below_guardrail"
    assert result["stage3_input"]["selection_notes"]["primary_source"] == "training"


def test_run_stage2_capture_curve_profiles_match_decisions_and_writes_stage3_all_trade_inputs(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    stage0_root = tmp_path / "dev/stage0/stage0-aave"
    promotion_root.mkdir(parents=True)
    (stage0_root / "scores").mkdir(parents=True)
    (stage0_root / "scores" / "ground_truth_summary.json").write_text(
        json.dumps({"metrics": {"significance_threshold_pct": 0.8, "forward_hours": 1}})
    )
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "session_id": "stage1-aave",
                "records": [
                    {
                        "signal_id": "sig-match-long",
                        "sample_role": "training",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                    },
                    {
                        "signal_id": "sig-mismatch-long",
                        "sample_role": "walk_forward_test",
                        "decision_direction": "LONG",
                        "agreement": "MISMATCH",
                    },
                    {
                        "signal_id": "sig-flat",
                        "sample_role": "training",
                        "decision_direction": "FLAT",
                        "agreement": "NEUTRAL",
                    },
                ],
            }
        )
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
        "stage0_artifact_root": str(stage0_root),
    }
    signal_rows = [
        {
            "signal_id": "sig-match-long",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        },
        {
            "signal_id": "sig-mismatch-long",
            "timestamp": "2026-05-02T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        },
        {
            "signal_id": "sig-flat",
            "timestamp": "2026-05-03T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 100.6, "low": 99.7, "close": 100.4},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 100.4, "high": 100.8, "low": 100.1, "close": 100.7},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 100.3, "low": 98.8, "close": 99.0},
        {"timestamp": "2026-05-02T00:10:00Z", "open": 99, "high": 99.4, "low": 98.5, "close": 98.7},
    ]

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signal_rows,
        candles=candles,
        forward_hours=1,
    )

    assert result["tp_levels"][:5] == [0.1, 0.2, 0.3, 0.4, 0.5]
    assert result["tp_levels"][-1] == 5.0
    assert result["metrics"]["total_trade_decisions"] == 2
    assert result["metrics"]["match_count"] == 1
    assert result["metrics"]["mismatch_count"] == 1
    assert result["metrics"]["stage2_profiled_match_count"] == 1
    assert {row["signal_id"] for row in result["per_signal"]} == {"sig-match-long"}
    assert result["per_signal"][0]["agreement"] == "MATCH"
    assert result["per_signal"][0]["max_favorable_excursion_pct"] == 0.8
    assert result["per_signal"][0]["max_adverse_excursion_pct"] == 0.3
    assert result["sl_levels"][:5] == [0.1, 0.2, 0.3, 0.4, 0.5]
    assert result["sl_levels"][-1] == 0.8
    assert result["sl_results"]["0.2"]["full_cycle"] == {"hit": 1, "total": 1, "rate": 100.0}
    assert result["sl_results"]["0.3"]["full_cycle"] == {"hit": 1, "total": 1, "rate": 100.0}
    assert result["sl_results"]["0.4"]["full_cycle"] == {"hit": 0, "total": 1, "rate": 0.0}
    assert result["per_signal"][0]["time_to_first_tp_minutes"] == 5.0
    assert result["cohorts"]["MATCH"]["0.4"] == {"reached": 1, "total": 1, "rate": 100.0}
    assert result["cohorts"]["MISMATCH"]["0.4"] == {"reached": 0, "total": 0, "rate": 0.0}
    assert result["stage3_input"]["recommended_tp_min_pct"] == 0.1
    assert result["stage3_input"]["recommended_tp_max_pct"] == 0.8
    assert result["stage3_input"]["sl_range_source"] == "stage2_matched_adverse_profile"
    assert result["stage3_input"]["recommended_sl_min_pct"] == 0.1
    assert result["stage3_input"]["recommended_sl_max_pct"] == 0.8
    stage3_inputs = json.loads((promotion_root / "stage3_trade_inputs.json").read_text())
    assert {row["signal_id"] for row in stage3_inputs} == {"sig-match-long", "sig-mismatch-long"}
    assert {row["agreement"] for row in stage3_inputs} == {"MATCH", "MISMATCH"}


def test_stage2_rerun_clears_downstream_stage3_and_stage4_artifacts(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-long",
                        "sample_role": "training",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                    }
                ]
            }
        )
    )
    for artifact in [
        "stage3_grid_results.json",
        "stage3_optimal.json",
        "stage3_pyramid_results.json",
        "stage3_pyramid_optimal.json",
        "stage4_candidates.json",
        "stage4_realized_expectancy.json",
        "stage4_trade_ledger.json",
        "stage4_optimal.json",
        "stage4_summary.md",
    ]:
        (promotion_root / artifact).write_text("{}")
    stage4_run_dir = promotion_root / "stage4_runs" / "old-run"
    stage4_run_dir.mkdir(parents=True)
    (stage4_run_dir / "stage4_realized_expectancy.json").write_text("{}")
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root)}
    signal_rows = [
        {
            "signal_id": "sig-long",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {"active_timeframes": ["2h"], "interactions": {"2h": [{"market_price": 100}]}},
        }
    ]
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 100.5, "low": 99.8, "close": 100.4}]

    run_stage2_capture_curve(workspace_root=tmp_path, session=session, signal_rows=signal_rows, candles=candles)

    assert not (promotion_root / "stage3_grid_results.json").exists()
    assert not (promotion_root / "stage3_pyramid_results.json").exists()
    assert not (promotion_root / "stage4_realized_expectancy.json").exists()
    assert not (promotion_root / "stage4_runs").exists()
