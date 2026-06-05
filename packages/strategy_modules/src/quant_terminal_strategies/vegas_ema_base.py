from __future__ import annotations

from typing import Any


STRATEGY_ID = "vegas_ema_base"
STRATEGY_VERSION = "v0.1"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context["signal"]
    payload = signal.get("payload", {})
    charts = payload.get("charts") or signal.get("charts", {})
    one_day = charts.get("1d")
    two_hour = charts.get("2h")
    signal_id = signal["signal_id"]

    if not isinstance(one_day, dict) or not isinstance(two_hour, dict):
        return _decision(
            signal_id=signal_id,
            trade_action="SKIP",
            direction="FLAT",
            confidence=0.2,
            reason_code="missing_required_2h_or_1d_context",
            diagnostics={"has_1d": isinstance(one_day, dict), "has_2h": isinstance(two_hour, dict)},
        )

    macro_direction = _macro_direction(one_day)
    local_direction = _local_direction(two_hour)
    candle_direction = _candle_direction(charts)
    if macro_direction is None:
        macro_direction = candle_direction["macro_direction"]
    if local_direction is None:
        local_direction = candle_direction["local_direction"]
    if macro_direction is None and local_direction is None:
        return _decision(
            signal_id=signal_id,
            trade_action="SKIP",
            direction="FLAT",
            confidence=0.25,
            reason_code="unreadable_macro_and_2h_context",
            diagnostics={"one_day": one_day, "two_hour": two_hour},
        )

    stretched = _is_stretched(one_day, macro_direction)
    mature_leg = bool(one_day.get("mature_leg", False))
    if macro_direction and local_direction and macro_direction != local_direction:
        if stretched and mature_leg:
            direction = local_direction
            reason_code = "mature_macro_leg_with_2h_reversal_proof"
            confidence = 0.68
        else:
            return _decision(
                signal_id=signal_id,
                trade_action="SKIP",
                direction="FLAT",
                confidence=0.35,
                reason_code="macro_2h_conflict_defer_to_daily_pressure",
                diagnostics={
                    "macro_direction": macro_direction,
                    "local_direction": local_direction,
                    "stretched": stretched,
                    "mature_leg": mature_leg,
                    **candle_direction,
                },
            )
    else:
        direction = local_direction or macro_direction
        reason_code = _direction_reason(direction=direction, candle_direction=candle_direction)
        confidence = 0.72 if macro_direction == local_direction else 0.58

    return _decision(
        signal_id=signal_id,
        trade_action="ENTER",
        direction=direction or "FLAT",
        confidence=confidence,
        reason_code=reason_code,
        diagnostics={
            "macro_direction": macro_direction,
            "local_direction": local_direction,
            "range_position_pct": one_day.get("range_position_pct"),
            "mature_leg": mature_leg,
            "active_timeframes": payload.get("active_timeframes", signal.get("active_timeframes", [])),
            "runtime_mode": context.get("runtime_mode", "backtest"),
            **candle_direction,
        },
    )


def _macro_direction(one_day: dict[str, Any]) -> str | None:
    trend = str(one_day.get("trend", one_day.get("pressure", ""))).lower()
    if trend in {"bullish", "up", "uptrend", "long"}:
        return "LONG"
    if trend in {"bearish", "down", "downtrend", "short"}:
        return "SHORT"
    return None


def _local_direction(two_hour: dict[str, Any]) -> str | None:
    combined = " ".join(
        str(two_hour.get(key, ""))
        for key in ("structure", "confirmation", "trigger", "pressure")
    ).lower()
    if any(term in combined for term in ("support", "reclaim", "healthy_pullback", "bullish", "higher_low")):
        return "LONG"
    if any(term in combined for term in ("rejection", "failed", "bearish", "rollover", "lower_high")):
        return "SHORT"
    return None


def _is_stretched(one_day: dict[str, Any], macro_direction: str | None) -> bool:
    position = one_day.get("range_position_pct")
    if position is None:
        return False
    position_pct = float(position)
    if macro_direction == "LONG":
        return position_pct >= 80
    if macro_direction == "SHORT":
        return position_pct <= 20
    return False


def _candle_direction(charts: dict[str, Any]) -> dict[str, Any]:
    daily = _chart_stats(charts.get("1d", {}), lookback=10)
    anchor = _chart_stats(charts.get("2h", {}), lookback=6)
    macro_direction = _direction_from_return(daily["return_pct"], threshold_pct=0.75)
    local_direction = _direction_from_return(anchor["return_pct"], threshold_pct=0.35)
    if daily["last_return_pct"] is not None and daily["last_return_pct"] < -0.75 and local_direction == "LONG":
        local_direction = "SHORT"
    if daily["range_position_pct"] is not None:
        if daily["range_position_pct"] <= 30 and daily["last_return_pct"] < 0:
            macro_direction = "SHORT"
        if daily["range_position_pct"] >= 70 and daily["last_return_pct"] > 0:
            macro_direction = "LONG"
    return {
        "macro_direction": macro_direction,
        "local_direction": local_direction,
        "daily_return_pct": daily["return_pct"],
        "daily_last_return_pct": daily["last_return_pct"],
        "daily_range_position_pct": daily["range_position_pct"],
        "anchor_return_pct": anchor["return_pct"],
        "anchor_last_return_pct": anchor["last_return_pct"],
        "anchor_range_position_pct": anchor["range_position_pct"],
    }


def _chart_stats(chart: dict[str, Any], *, lookback: int) -> dict[str, float | None]:
    candles = chart.get("completed_candles", [])
    closes = [_close(candle, chart.get("columns", [])) for candle in candles]
    closes = [value for value in closes if value is not None]
    if len(closes) < 2:
        return {"return_pct": None, "last_return_pct": None, "range_position_pct": None}
    start_index = max(0, len(closes) - lookback - 1)
    return_pct = _pct_change(closes[start_index], closes[-1])
    last_return_pct = _pct_change(closes[-2], closes[-1])
    window = closes[-min(len(closes), lookback + 1) :]
    low = min(window)
    high = max(window)
    range_position_pct = ((closes[-1] - low) / (high - low) * 100) if high > low else 50.0
    return {
        "return_pct": return_pct,
        "last_return_pct": last_return_pct,
        "range_position_pct": range_position_pct,
    }


def _close(candle: list[Any], columns: list[str]) -> float | None:
    try:
        close_index = columns.index("close") if "close" in columns else 4
        return float(candle[close_index])
    except (IndexError, TypeError, ValueError):
        return None


def _pct_change(start: float | None, end: float | None) -> float | None:
    if start in (None, 0) or end is None:
        return None
    return (end / start - 1) * 100


def _direction_from_return(value: float | None, *, threshold_pct: float) -> str | None:
    if value is None:
        return None
    if value >= threshold_pct:
        return "LONG"
    if value <= -threshold_pct:
        return "SHORT"
    return None


def _direction_reason(*, direction: str | None, candle_direction: dict[str, Any]) -> str:
    if direction == "SHORT" and candle_direction["daily_last_return_pct"] is not None:
        if candle_direction["daily_last_return_pct"] < 0 and candle_direction["anchor_return_pct"] is not None:
            if candle_direction["anchor_return_pct"] > 0:
                return "daily_softness_after_anchor_push"
        return "bearish_pressure_with_2h_confirmation"
    if direction == "LONG":
        return "bullish_pressure_with_2h_confirmation"
    return "unresolved_direction"


def _decision(
    *,
    signal_id: str,
    trade_action: str,
    direction: str,
    confidence: float,
    reason_code: str,
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    return {
        "decision_id": f"{STRATEGY_ID}-{STRATEGY_VERSION}-{signal_id}",
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "signal_id": signal_id,
        "trade_action": trade_action,
        "action": trade_action,
        "direction": direction,
        "confidence": confidence,
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": diagnostics,
    }
