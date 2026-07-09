import json
from pathlib import Path

import pytest

import quant_terminal_worker.stage3.pyramid as stage3_pyramid
from quant_terminal_worker.stage3.pyramid import run_stage3_pyramid


def test_run_stage3_pyramid_tests_stage3_candidate_shortlist_with_tight_intraday_steps(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    _write_trade_inputs(promotion_root)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage4_candidates(
        promotion_root,
        [
            {
                "candidate_id": "numeric_exact_tp_1p5_sl_1",
                "setup": {
                    "entry_model": "market",
                    "tp_pct": 1.5,
                    "sl_pct": 1.0,
                    "protect_trigger_pct": 1.0,
                    "trail_sl_pct": 0.5,
                    "exit_policy_type": "numerical_stage2_policy",
                    "timeout_policy": "close_at_cutoff",
                },
            }
        ],
    )
    session = _session(artifact_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.1, "low": 99.8, "close": 101.0},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.0, "high": 102.7, "low": 100.9, "close": 102.0},
    ]

    result = run_stage3_pyramid(workspace_root=tmp_path, session=session, candles=candles, shortlist_size=5)

    assert result["stage3_mode"] == "numerical_exit_policy_pyramid"
    assert result["total_signals"] == 1
    assert {row["max_legs"] for row in result["results"]} == {1, 2, 3}
    assert {row["step_pct"] for row in result["results"] if row["step_pct"] is not None} == {0.1, 0.2, 0.3, 0.4, 0.5}
    assert result["baseline"]["max_legs"] == 1
    assert result["optimal"]["best"]["max_legs"] in {2, 3}
    pyramid_candidates = [
        candidate
        for candidate in result["stage4_candidates"]["candidates"]
        if candidate["candidate_id"].startswith("pyramid_")
    ]
    assert pyramid_candidates
    assert all(candidate["setup"]["max_legs"] > 1 for candidate in pyramid_candidates)
    assert pyramid_candidates[0]["setup"]["protect_trigger_pct"] == 1.0
    assert (promotion_root / "stage3_pyramid_results.json").exists()
    assert (promotion_root / "stage3_pyramid_optimal.json").exists()
    assert "Stage 3 Pyramiding" in (promotion_root / "stage3_pyramid_summary.md").read_text()


def test_stage3_pyramid_reads_each_non_pyramid_stage4_candidate(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    _write_trade_inputs(promotion_root)
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5])
    _write_stage4_candidates(
        promotion_root,
        [
            {"candidate_id": "numeric_a", "setup": {"entry_model": "market", "tp_pct": 1.0, "sl_pct": 1.0, "protect_trigger_pct": 0.5}},
            {"candidate_id": "numeric_b", "setup": {"entry_model": "market", "tp_pct": 1.5, "sl_pct": 1.0, "protect_trigger_pct": 1.0}},
        ],
    )
    session = _session(artifact_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 100.7, "low": 99.8, "close": 100.6},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 100.6, "high": 102.0, "low": 100.3, "close": 101.8},
    ]

    result = run_stage3_pyramid(workspace_root=tmp_path, session=session, candles=candles, steps=[0.5], shortlist_size=10)

    pyramid_candidates = [
        candidate
        for candidate in result["stage4_candidates"]["candidates"]
        if candidate["candidate_id"].startswith("pyramid_")
    ]
    assert {candidate["source_candidate_id"] for candidate in pyramid_candidates} == {"numeric_a", "numeric_b"}


def test_stage3_pyramid_scores_side_specific_candidate_by_trade_direction(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage3_trade_inputs.json").write_text(
        json.dumps(
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
                    "signal_ts": "2026-05-01T02:00:00Z",
                    "reference_price": 200,
                },
            ]
        )
    )
    _write_stage2_capture(promotion_root, tp_levels=[0.5, 1.0, 1.5, 2.0])
    _write_stage4_candidates(
        promotion_root,
        [
            {
                "candidate_id": "numeric_side_specific",
                "setup": {
                    "entry_model": "market",
                    "policy_mode": "side_specific",
                    "tp_pct": 1.0,
                    "sl_pct": 1.0,
                    "timeout_policy": "close_at_cutoff",
                    "side_policies": {
                        "LONG": {"final_tp_pct": 2.0, "initial_sl_pct": 0.5, "protection_enabled": False},
                        "SHORT": {"final_tp_pct": 0.5, "initial_sl_pct": 1.0, "protection_enabled": False},
                    },
                },
            }
        ],
    )
    session = _session(artifact_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.4, "close": 100.9},
        {"timestamp": "2026-05-01T02:05:00Z", "open": 200, "high": 200.2, "low": 198.8, "close": 199.0},
    ]

    result = run_stage3_pyramid(workspace_root=tmp_path, session=session, candles=candles, steps=[0.5], shortlist_size=1, fees_bps_per_side=0)

    assert result["baseline"]["pnl_pct"] == 0.0
    pyramid_candidates = [
        candidate
        for candidate in result["stage4_candidates"]["candidates"]
        if candidate["candidate_id"].startswith("pyramid_")
    ]
    assert pyramid_candidates
    assert pyramid_candidates[0]["setup"]["policy_mode"] == "side_specific"
    assert pyramid_candidates[0]["setup"]["side_policies"]["SHORT"]["final_tp_pct"] == 0.5


def test_stage3_pyramid_scoring_uses_precomputed_trade_windows(monkeypatch):
    trades = [
        {
            "signal_id": "sig-window",
            "sample_role": "training",
            "decision_direction": "LONG",
            "direction": "LONG",
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

    def fake_simulate_pyramid_trade(**kwargs):
        observed_candles.append(kwargs["candles"])
        return {"pnl_pct": 0.0, "legs_filled": 1, "wins": 0, "losses": 0}

    monkeypatch.setattr(stage3_pyramid, "simulate_pyramid_trade", fake_simulate_pyramid_trade)

    stage3_pyramid._score_pyramid_setup(
        trades=trades,
        candles=full_candles,
        trade_candle_windows=[window],
        tp_pct=1.0,
        sl_pct=1.0,
        step_pct=0.5,
        max_legs=2,
        sl_breakeven=False,
        forward_hours=1,
        leverage=1,
        fees_bps_per_side=0,
    )

    assert observed_candles == [window]


def test_stage3_pyramid_refuses_missing_stage3_candidate_shortlist(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    _write_trade_inputs(promotion_root)
    session = _session(artifact_root)

    with pytest.raises(ValueError, match="Stage 4 candidate shortlist"):
        run_stage3_pyramid(workspace_root=tmp_path, session=session, candles=[])


def test_stage3_pyramid_rerun_replaces_old_pyramid_candidates_and_clears_stage4(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    _write_trade_inputs(promotion_root)
    _write_stage2_capture(promotion_root, tp_levels=[0.5])
    _write_stage4_candidates(
        promotion_root,
        [
            {"candidate_id": "numeric_a", "setup": {"entry_model": "market", "tp_pct": 1.0, "sl_pct": 1.0, "protect_trigger_pct": 0.5}},
            {"candidate_id": "pyramid_stale", "setup": {"entry_model": "market", "tp_pct": 9.9, "sl_pct": 9.9, "max_legs": 3}},
        ],
    )
    (promotion_root / "stage4_realized_expectancy.json").write_text("{}")
    stage4_run_dir = promotion_root / "stage4_runs" / "old-run"
    stage4_run_dir.mkdir(parents=True)
    (stage4_run_dir / "stage4_realized_expectancy.json").write_text("{}")
    session = _session(artifact_root)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 100.7, "low": 99.8, "close": 100.6},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 100.6, "high": 102.0, "low": 100.3, "close": 101.8},
    ]

    result = run_stage3_pyramid(workspace_root=tmp_path, session=session, candles=candles, steps=[0.5])

    candidate_ids = [candidate["candidate_id"] for candidate in result["stage4_candidates"]["candidates"]]
    assert "pyramid_stale" not in candidate_ids
    assert any(candidate_id.startswith("pyramid_") for candidate_id in candidate_ids)
    assert not (promotion_root / "stage4_realized_expectancy.json").exists()
    assert not (promotion_root / "stage4_runs").exists()


def _session(artifact_root: Path) -> dict:
    return {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }


def _write_trade_inputs(promotion_root: Path) -> None:
    (promotion_root / "stage3_trade_inputs.json").write_text(
        json.dumps(
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
            ]
        )
    )


def _write_stage2_capture(promotion_root: Path, *, tp_levels: list[float]) -> None:
    (promotion_root / "stage2_capture_curve.json").write_text(json.dumps({"tp_levels": tp_levels}))


def _write_stage4_candidates(promotion_root: Path, candidates: list[dict]) -> None:
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "schema_version": "0.2",
                "artifact_role": "stage4_candidates",
                "source_stage": "stage3_conditional_execution_setup",
                "stage3_mode": "numerical_exit_policy",
                "candidates": candidates,
            }
        )
    )
