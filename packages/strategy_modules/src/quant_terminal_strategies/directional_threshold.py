from __future__ import annotations

from typing import Any


STRATEGY_ID = "directional_threshold"
STRATEGY_VERSION = "0.1.0"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context["signal"]
    parameters = context.get("parameters", {})
    move_pct = float(signal.get("payload", {}).get("move_pct", 0.0))
    long_threshold = float(parameters.get("long_threshold_pct", 1.0))
    short_threshold = float(parameters.get("short_threshold_pct", -1.0))

    if move_pct >= long_threshold:
        action = "ENTER"
        direction = "LONG"
        reason_code = "positive_move_threshold"
    elif move_pct <= short_threshold:
        action = "ENTER"
        direction = "SHORT"
        reason_code = "negative_move_threshold"
    else:
        action = "SKIP"
        direction = "FLAT"
        reason_code = "inside_threshold"

    confidence = min(1.0, round(0.5 + abs(move_pct) / 10, 2))
    return {
        "decision_id": f"{STRATEGY_ID}-{STRATEGY_VERSION}-{signal['signal_id']}",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "signal_id": signal["signal_id"],
        "action": action,
        "direction": direction,
        "confidence": confidence,
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": {
            "move_pct": move_pct,
            "runtime_mode": context.get("runtime_mode", "backtest"),
        },
    }
