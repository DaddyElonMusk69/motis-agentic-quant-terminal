from quant_terminal_worker.backtests.stage1 import run_stage1_backtest


def test_stage1_backtest_runs_engine_and_strategy_subprocesses():
    result = run_stage1_backtest(
        {
            "run_id": "bt-test-1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_refs": ["btc-raw-5m"],
            "rows": [
                {"timestamp": "2026-06-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-06-01T00:05:00Z", "open": 100, "close": 101},
                {"timestamp": "2026-06-01T00:10:00Z", "open": 101, "close": 103},
                {"timestamp": "2026-06-01T00:15:00Z", "open": 103, "close": 97},
            ],
            "signal_engine": {
                "signal_engine_id": "threshold_reversal",
                "version": "0.1.0",
                "runtime_entrypoint": "quant_terminal_engines.threshold_reversal:generate_signals",
                "parameters": {"min_move_pct": 2.0},
            },
            "strategy": {
                "strategy_id": "directional_threshold",
                "version": "0.1.0",
                "runtime_entrypoint": "quant_terminal_strategies.directional_threshold:decide",
                "parameters": {"long_threshold_pct": 1.0, "short_threshold_pct": -1.0},
            },
            "ground_truth": {
                "threshold_reversal-BTC-20260601T001000Z": "LONG",
                "threshold_reversal-BTC-20260601T001500Z": "SHORT",
            },
        }
    )

    assert [signal["signal_id"] for signal in result["signals"]] == [
        "threshold_reversal-BTC-20260601T001000Z",
        "threshold_reversal-BTC-20260601T001500Z",
    ]
    assert [decision["direction"] for decision in result["decisions"]] == ["LONG", "SHORT"]
    assert result["score_summary"] == {
        "scoring_method": "stage1a_directional_agreement",
        "metrics": {
            "total": 2,
            "matched": 2,
            "mismatched": 0,
            "skipped": 0,
            "agreement_rate": 1.0,
        },
        "records": [
            {
                "signal_id": "threshold_reversal-BTC-20260601T001000Z",
                "ground_truth_direction": "LONG",
                "decision_direction": "LONG",
                "agreement": "MATCH",
                "status": "CORRECT",
            },
            {
                "signal_id": "threshold_reversal-BTC-20260601T001500Z",
                "ground_truth_direction": "SHORT",
                "decision_direction": "SHORT",
                "agreement": "MATCH",
                "status": "CORRECT",
            },
        ],
    }
