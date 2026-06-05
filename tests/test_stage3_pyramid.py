import json
from pathlib import Path

from quant_terminal_worker.stage3.pyramid import run_stage3_pyramid


def test_run_stage3_pyramid_compares_steps_against_one_leg_baseline(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage2_capture_per_signal.json").write_text(
        json.dumps(
            [
                {
                    "signal_id": "sig-long",
                    "sample_role": "training",
                    "direction": "LONG",
                    "signal_ts": "2026-05-01T00:00:00Z",
                    "reference_price": 100,
                }
            ]
        )
    )
    (promotion_root / "stage3_optimal.json").write_text(json.dumps({"best": {"tp": 1.0, "sl": 1.0}}))
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps({"candidates": [{"candidate_id": "market_tp_1p0_sl_1p0", "setup": {"entry_model": "market"}}]})
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
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.8, "close": 101.2},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.2, "high": 102.0, "low": 100.4, "close": 101.8},
    ]

    result = run_stage3_pyramid(
        workspace_root=tmp_path,
        session=session,
        candles=candles,
        steps=[0.5],
        max_legs=2,
        leverage=5,
    )

    assert result["total_signals"] == 1
    assert result["baseline"]["pnl_pct"] == 5.0
    best = result["optimal"]["best"]
    assert best["step_pct"] == 0.5
    assert best["pnl_pct"] == 10.0
    assert best["delta_vs_baseline_pct"] == 5.0
    assert best["avg_legs_per_signal"] == 2.0
    assert result["stage4_candidates"]["candidates"][-1]["setup"]["pyramid_step_pct"] == 0.5
    assert (promotion_root / "stage3_pyramid_results.json").exists()
    assert (promotion_root / "stage3_pyramid_optimal.json").exists()
    assert "Stage 3 Pyramid" in (promotion_root / "stage3_pyramid_summary.md").read_text()
