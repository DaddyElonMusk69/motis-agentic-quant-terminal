import json
from pathlib import Path

from quant_terminal_worker.stage3.grid_search import run_stage3_grid_search


def test_run_stage3_grid_search_scores_tp_sl_grid_and_shortlists_stage4_candidates(tmp_path: Path):
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
                },
                {
                    "signal_id": "sig-short",
                    "sample_role": "walk_forward_test",
                    "direction": "SHORT",
                    "signal_ts": "2026-05-01T01:00:00Z",
                    "reference_price": 200,
                },
            ]
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
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 102.2, "low": 99.4, "close": 101.5},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 200, "high": 201.2, "low": 195.5, "close": 198.5},
    ]

    result = run_stage3_grid_search(
        workspace_root=tmp_path,
        session=session,
        candles=candles,
        tp_values=[1.0, 2.0],
        sl_values=[0.5, 1.0],
        forward_hours=36,
        leverage=5,
        shortlist_size=2,
    )

    assert result["total_signals"] == 2
    best = result["optimal"]["best"]
    assert best["tp"] == 2.0
    assert best["sl"] == 0.5
    assert best["tp_count"] == 2
    assert best["sl_count"] == 0
    assert best["wr"] == 100.0
    assert best["expectancy"] == 2.0
    assert best["slice_split"]["training"]["tp_count"] == 1
    assert best["slice_split"]["walk_forward_test"]["tp_count"] == 1
    assert best["side_split"]["LONG"]["tp_count"] == 1
    assert best["side_split"]["SHORT"]["tp_count"] == 1
    assert result["stage4_candidates"]["candidates"][0]["setup"]["tp_pct"] == 2.0
    assert (promotion_root / "stage3_grid_results.json").exists()
    assert (promotion_root / "stage3_optimal.json").exists()
    assert (promotion_root / "stage4_candidates.json").exists()
    assert "Stage 3 Grid Search" in (promotion_root / "stage3_summary.md").read_text()
