from __future__ import annotations

from typing import Any


STRATEGY_ID = "leverage_reset_reacceleration_v1_base"
STRATEGY_VERSION = "v0.1"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    signal_id = str(signal.get("signal_id") or "unknown")

    if payload.get("schema_version") != "signal_packet.v2":
        return _decision(signal_id=signal_id, action="BLOCKED", direction="FLAT", reason_code="invalid_packet_schema", diagnostics={})
    if evidence.get("event_type") != "LEVERAGE_RESET_REACCELERATION":
        return _decision(
            signal_id=signal_id,
            action="BLOCKED",
            direction="FLAT",
            reason_code="missing_leverage_reset_reacceleration_event",
            diagnostics={},
        )

    required = (
        "event_subtype",
        "reset_state",
        "observed_reacceleration",
        "reset_age_bars",
        "reset_flush_extension_pct",
        "reset_oi_drop_1h_pct",
        "reclaim_from_extreme_pct",
        "oi_rebuild_1h_pct",
        "volume_ratio_2d_median",
        "taker_imbalance_zscore",
        "funding_rate_zscore_7d",
        "premium_zscore",
    )
    if any(key not in evidence for key in required):
        return _decision(
            signal_id=signal_id,
            action="BLOCKED",
            direction="FLAT",
            reason_code="missing_required_leverage_reset_evidence",
            diagnostics=_diagnostics(evidence=evidence, payload=payload, directional_basis=None),
        )

    observed = str(evidence.get("observed_reacceleration") or "").upper()
    if observed == "UP":
        direction = "LONG"
        directional_basis = "downside_leverage_flush_reset_then_upside_reacceleration"
    elif observed == "DOWN":
        direction = "SHORT"
        directional_basis = "upside_leverage_flush_reset_then_downside_reacceleration"
    else:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            reason_code="reacceleration_unresolved",
            diagnostics=_diagnostics(evidence=evidence, payload=payload, directional_basis="unknown_reacceleration"),
        )

    return _decision(
        signal_id=signal_id,
        action="ENTER",
        direction=direction,
        reason_code=f"leverage_reset_reacceleration_seed_{direction.lower()}",
        diagnostics=_diagnostics(evidence=evidence, payload=payload, directional_basis=directional_basis),
    )


def manage_position(context: dict[str, Any]) -> dict[str, Any]:
    position_context = context.get("position_context") if isinstance(context.get("position_context"), dict) else {}
    if position_context.get("hard_exit_expired") is True:
        return {"action": "EXIT", "reason_code": "hard_exit_expired"}
    return {"action": "HOLD", "reason_code": "mechanical_policy"}


def _diagnostics(
    *,
    evidence: dict[str, Any],
    payload: dict[str, Any],
    directional_basis: str | None,
) -> dict[str, Any]:
    return {
        "pattern": evidence.get("pattern"),
        "event_type": evidence.get("event_type"),
        "event_subtype": evidence.get("event_subtype"),
        "reset_state": evidence.get("reset_state"),
        "observed_reacceleration": evidence.get("observed_reacceleration"),
        "directional_basis": directional_basis,
        "signal_available_at": evidence.get("signal_available_at"),
        "reset_age_bars": evidence.get("reset_age_bars"),
        "reset_flush_extension_pct": evidence.get("reset_flush_extension_pct"),
        "reset_candle_range_pct": evidence.get("reset_candle_range_pct"),
        "reset_oi_drop_1h_pct": evidence.get("reset_oi_drop_1h_pct"),
        "reclaim_from_extreme_pct": evidence.get("reclaim_from_extreme_pct"),
        "oi_rebuild_1h_pct": evidence.get("oi_rebuild_1h_pct"),
        "volume_ratio_2d_median": evidence.get("volume_ratio_2d_median"),
        "taker_buy_sell_volume_ratio": evidence.get("taker_buy_sell_volume_ratio"),
        "taker_imbalance_zscore": evidence.get("taker_imbalance_zscore"),
        "global_account_long_short_ratio": evidence.get("global_account_long_short_ratio"),
        "top_trader_position_vs_global_long_share_gap": evidence.get("top_trader_position_vs_global_long_share_gap"),
        "premium_zscore": evidence.get("premium_zscore"),
        "funding_rate_zscore_7d": evidence.get("funding_rate_zscore_7d"),
        "active_timeframes": payload.get("active_timeframes") or [],
    }


def _decision(
    *,
    signal_id: str,
    action: str,
    direction: str,
    reason_code: str,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "decision_id": f"{STRATEGY_ID}-{STRATEGY_VERSION}-{signal_id}",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "signal_id": signal_id,
        "action": action,
        "trade_action": action,
        "direction": direction,
        "confidence": 0.5 if action == "ENTER" else 0.2,
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": diagnostics,
    }
