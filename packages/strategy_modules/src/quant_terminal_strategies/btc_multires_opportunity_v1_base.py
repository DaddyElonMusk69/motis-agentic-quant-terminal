from __future__ import annotations

from typing import Any


STRATEGY_ID = "btc_multires_opportunity_v1_base"
STRATEGY_VERSION = "v1.0"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    signal_id = str(signal.get("signal_id") or "unknown")
    if payload.get("schema_version") != "signal_packet.v2":
        return _decision(signal_id, "BLOCKED", "FLAT", "invalid_packet_schema", evidence)
    if evidence.get("event_type") != "MULTIRES_OPPORTUNITY_STATE":
        return _decision(signal_id, "BLOCKED", "FLAT", "missing_multires_event", evidence)

    direction_value = (
        _float(evidence.get("price_return_4h_pct"))
        + 0.35 * _float(evidence.get("price_return_24h_pct"))
        + 0.10 * (_float(evidence.get("taker_long_short_ratio"), default=1.0) - 1.0) * 100.0
    )
    direction = "LONG" if direction_value >= 0 else "SHORT"
    return _decision(
        signal_id,
        "ENTER",
        direction,
        f"multires_opportunity_{direction.lower()}",
        evidence,
    )


def manage_position(context: dict[str, Any]) -> dict[str, Any]:
    position = (
        context.get("position_context")
        if isinstance(context.get("position_context"), dict)
        else {}
    )
    if position.get("hard_exit_expired") is True:
        return {"action": "EXIT", "reason_code": "hard_exit_expired"}
    return {"action": "HOLD", "reason_code": "mechanical_policy"}


def _decision(
    signal_id: str,
    action: str,
    direction: str,
    reason_code: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "decision_id": f"{STRATEGY_ID}-{STRATEGY_VERSION}-{signal_id}",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "signal_id": signal_id,
        "action": action,
        "trade_action": action,
        "direction": direction,
        "confidence": 0.55 if action == "ENTER" else 0.0,
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": {
            "event_type": evidence.get("event_type"),
            "signal_available_at": evidence.get("signal_available_at"),
            "price_return_4h_pct": evidence.get("price_return_4h_pct"),
            "price_return_24h_pct": evidence.get("price_return_24h_pct"),
            "oi_change_4h_pct": evidence.get("oi_change_4h_pct"),
            "taker_long_short_ratio": evidence.get("taker_long_short_ratio"),
        },
    }


def _float(value: Any, *, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default
