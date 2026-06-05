from quant_terminal_strategies.vegas_ema_base import decide


def test_vegas_ema_base_enters_with_macro_pressure_and_2h_confirmation():
    decision = decide(
        {
            "signal": {
                "signal_id": "vegas-signal-1",
                "asset": "AAVE",
                "signal_engine_id": "vegas_ema",
                "payload": {
                    "active_timeframes": ["2h", "4h"],
                    "charts": {
                        "1d": {
                            "trend": "bullish",
                            "range_position_pct": 62,
                            "mature_leg": False,
                        },
                        "2h": {
                            "structure": "healthy_pullback",
                            "confirmation": "support_reclaim",
                        },
                    },
                },
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["trade_action"] == "ENTER"
    assert decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["reason_code"] == "bullish_pressure_with_2h_confirmation"


def test_vegas_ema_base_skips_when_required_timeframes_are_missing():
    decision = decide(
        {
            "signal": {
                "signal_id": "vegas-signal-2",
                "asset": "AAVE",
                "signal_engine_id": "vegas_ema",
                "payload": {"charts": {"1d": {"trend": "bullish"}}},
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["trade_action"] == "SKIP"
    assert decision["direction"] == "FLAT"
    assert decision["reason_code"] == "missing_required_2h_or_1d_context"


def test_vegas_ema_base_reads_root_level_legacy_packet_charts():
    decision = decide(
        {
            "signal": {
                "signal_id": "vegas-root-packet",
                "asset": "AAVE",
                "signal_engine_id": "vegas_ema",
                "charts": {
                    "1d": {
                        "columns": ["ts", "open", "high", "low", "close"],
                        "completed_candles": [
                            ["2026-04-26T00:00:00Z", "95", "98", "94", "97"],
                            ["2026-04-27T00:00:00Z", "97", "99", "96", "98"],
                            ["2026-04-28T00:00:00Z", "98", "99", "95", "96"],
                        ],
                    },
                    "2h": {
                        "columns": ["ts", "open", "high", "low", "close"],
                        "completed_candles": [
                            ["2026-04-29T00:00:00Z", "96", "97", "95", "96"],
                            ["2026-04-29T02:00:00Z", "96", "98", "96", "97.5"],
                            ["2026-04-29T04:00:00Z", "97.5", "98", "97", "97.8"],
                        ],
                    },
                },
                "active_timeframes": ["2h", "12h"],
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["trade_action"] == "ENTER"
    assert decision["direction"] == "SHORT"
    assert decision["reason_code"] == "daily_softness_after_anchor_push"


def test_vegas_ema_base_stage1a_emits_direction_when_only_candles_exist():
    decision = decide(
        {
            "signal": {
                "signal_id": "vegas-candle-only",
                "asset": "AAVE",
                "signal_engine_id": "vegas_ema",
                "charts": {
                    "1d": {
                        "columns": ["ts", "open", "high", "low", "close"],
                        "completed_candles": [
                            ["2026-04-25T00:00:00Z", "95", "97", "94", "95"],
                            ["2026-04-26T00:00:00Z", "95", "99", "95", "98"],
                            ["2026-04-27T00:00:00Z", "98", "100", "97", "99"],
                        ],
                    },
                    "2h": {
                        "columns": ["ts", "open", "high", "low", "close"],
                        "completed_candles": [
                            ["2026-04-29T00:00:00Z", "96", "97", "95", "96.3"],
                            ["2026-04-29T02:00:00Z", "96.3", "97", "96", "96.5"],
                            ["2026-04-29T04:00:00Z", "96.5", "97", "96", "96.8"],
                        ],
                    },
                },
            },
            "runtime_mode": "backtest",
        }
    )

    assert decision["trade_action"] == "ENTER"
    assert decision["direction"] in {"LONG", "SHORT"}
    assert decision["direction"] != "FLAT"
