import json
from pathlib import Path

from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy


def test_run_stage4_realized_expectancy_scores_full_decision_set(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-enter",
                        "agent_direction": "LONG",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                        "sample_role": "training",
                    },
                    {
                        "signal_id": "sig-skip",
                        "agent_direction": "FLAT",
                        "decision_direction": "FLAT",
                        "agreement": "NEUTRAL",
                        "sample_role": "walk_forward_test",
                    },
                ]
            }
        )
    )
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "market_tp_1p0_sl_1p0",
                        "setup": {
                            "entry_model": "market",
                            "tp_pct": 1.0,
                            "sl_pct": 1.0,
                            "timeout_policy": "close_at_cutoff",
                        },
                    }
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
    signals = [
        {
            "signal_id": "sig-enter",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T00:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 100}],
                "active_timeframes": ["2h"],
            },
        },
        {
            "signal_id": "sig-skip",
            "timestamp": "2026-05-01T01:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T01:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 200}],
                "active_timeframes": ["2h"],
            },
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 200, "high": 201, "low": 199, "close": 200},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    best = result["best_candidate"]
    assert best["candidate_id"] == "market_tp_1p0_sl_1p0"
    assert best["total_decisions"] == 2
    assert best["executed_trades"] == 1
    assert best["skipped_decisions"] == 1
    assert best["tp_hits"] == 1
    assert best["net_expectancy_pct"] == 2.5
    assert result["ledger"]["candidates"][0]["trades"][1]["entry_status"] == "SKIPPED"
    assert (promotion_root / "stage4_realized_expectancy.json").exists()
    assert (promotion_root / "stage4_trade_ledger.json").exists()
    assert (promotion_root / "stage4_optimal.json").exists()
    assert "Stage 4 Realized Expectancy" in (promotion_root / "stage4_summary.md").read_text()
