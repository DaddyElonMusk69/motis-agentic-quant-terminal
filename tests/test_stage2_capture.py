import json
from pathlib import Path

from quant_terminal_worker.stage2.capture_curve import run_stage2_capture_curve


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
    assert result["per_signal"][0]["first_tp_reached"] == 0.5
    assert (promotion_root / "stage2_capture_curve.json").exists()
    assert (promotion_root / "stage2_capture_per_signal.json").exists()
    assert "Stage 2 Travel Capture" in (promotion_root / "stage2_summary.md").read_text()
