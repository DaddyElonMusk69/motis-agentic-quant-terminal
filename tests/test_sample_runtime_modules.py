from quant_terminal_engines.threshold_reversal import generate_signals
from quant_terminal_strategies.directional_threshold import decide


def test_sample_signal_engine_emits_neutral_signal_envelopes():
    result = generate_signals(
        {
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_refs": ["btc-raw-5m"],
            "rows": [
                {"timestamp": "2026-06-01T00:00:00Z", "open": 100, "close": 100},
                {"timestamp": "2026-06-01T00:05:00Z", "open": 100, "close": 101},
                {"timestamp": "2026-06-01T00:10:00Z", "open": 101, "close": 103},
            ],
            "parameters": {"min_move_pct": 1.5},
        }
    )

    assert result["signals"] == [
        {
            "signal_id": "threshold_reversal-BTC-20260601T001000Z",
            "signal_engine_id": "threshold_reversal",
            "signal_engine_version": "0.1.0",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "timestamp": "2026-06-01T00:10:00Z",
            "data_refs": ["btc-raw-5m"],
            "payload_schema": "threshold_reversal.v1",
            "payload": {
                "move_pct": 3.0,
                "lookback_open": 100.0,
                "current_close": 103.0,
                "neutral_trigger": "lookback_move_exceeded",
            },
        }
    ]


def test_sample_strategy_makes_deterministic_stage1a_direction_decision():
    decision = decide(
        {
            "signal": {
                "signal_id": "signal-1",
                "payload": {"move_pct": 2.4},
            },
            "runtime_mode": "backtest",
            "parameters": {"long_threshold_pct": 1.0, "short_threshold_pct": -1.0},
        }
    )

    assert decision == {
        "decision_id": "directional_threshold-0.1.0-signal-1",
        "strategy_id": "directional_threshold",
        "strategy_version": "0.1.0",
        "signal_id": "signal-1",
        "action": "ENTER",
        "direction": "LONG",
        "confidence": 0.74,
        "reason_code": "positive_move_threshold",
        "execution_profile": {},
        "diagnostics": {"move_pct": 2.4, "runtime_mode": "backtest"},
    }
