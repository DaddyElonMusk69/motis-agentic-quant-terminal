from __future__ import annotations

from typing import Any


STRATEGY_ID = "vegas_ema_5m_hft_base"
STRATEGY_VERSION = "v0.1"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    if not evidence and isinstance(signal.get("evidence"), dict):
        evidence = signal.get("evidence")
    charts = payload.get("charts") if isinstance(payload.get("charts"), dict) else {}
    if not charts and isinstance(signal.get("charts"), dict):
        charts = signal.get("charts")
    signal_id = str(signal.get("signal_id", "unknown"))

    five_minute = charts.get("5m")
    two_hour = charts.get("2h")
    one_day = charts.get("1d")
    if not all(isinstance(chart, dict) for chart in (five_minute, two_hour, one_day)):
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.2,
            reason_code="missing_required_5m_2h_or_1d_context",
            diagnostics={
                "has_5m": isinstance(five_minute, dict),
                "has_2h": isinstance(two_hour, dict),
                "has_1d": isinstance(one_day, dict),
            },
        )

    matched_periods = _matched_periods(evidence)
    if len(matched_periods) < 3:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.25,
            reason_code="insufficient_5m_cluster_votes",
            diagnostics={"matched_ema_count": len(matched_periods), "matched_periods": matched_periods},
        )

    five_minute_stats = _chart_stats(five_minute, lookback=12)
    two_hour_stats = _chart_stats(two_hour, lookback=6)
    one_day_stats = _chart_stats(one_day, lookback=8)
    micro_direction = _ema_stack_direction(five_minute) or _direction_from_return(five_minute_stats["return_pct"], threshold_pct=0.08)
    local_direction = _direction_from_return(two_hour_stats["return_pct"], threshold_pct=0.25)
    macro_direction = _direction_from_return(one_day_stats["return_pct"], threshold_pct=0.5)

    directions = [direction for direction in (micro_direction, local_direction, macro_direction) if direction]
    if len(directions) < 2:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.3,
            reason_code="insufficient_directional_context",
            diagnostics=_diagnostics(
                matched_periods=matched_periods,
                micro_direction=micro_direction,
                local_direction=local_direction,
                macro_direction=macro_direction,
                five_minute_stats=five_minute_stats,
                two_hour_stats=two_hour_stats,
                one_day_stats=one_day_stats,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    long_votes = directions.count("LONG")
    short_votes = directions.count("SHORT")
    if long_votes == short_votes:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.35,
            reason_code="mixed_5m_2h_1d_directional_context",
            diagnostics=_diagnostics(
                matched_periods=matched_periods,
                micro_direction=micro_direction,
                local_direction=local_direction,
                macro_direction=macro_direction,
                five_minute_stats=five_minute_stats,
                two_hour_stats=two_hour_stats,
                one_day_stats=one_day_stats,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    direction = "LONG" if long_votes > short_votes else "SHORT"
    confidence = _confidence(primary_votes=max(long_votes, short_votes), total_votes=len(directions), matched_count=len(matched_periods))
    return _decision(
        signal_id=signal_id,
        action="ENTER",
        direction=direction,
        confidence=confidence,
        reason_code="aligned_5m_cluster_with_2h_1d_context",
        diagnostics=_diagnostics(
            matched_periods=matched_periods,
            micro_direction=micro_direction,
            local_direction=local_direction,
            macro_direction=macro_direction,
            five_minute_stats=five_minute_stats,
            two_hour_stats=two_hour_stats,
            one_day_stats=one_day_stats,
            runtime_mode=context.get("runtime_mode", "backtest"),
        ),
    )


def manage_position(context: dict[str, Any]) -> dict[str, Any]:
    position_context = context.get("position_context") if isinstance(context.get("position_context"), dict) else {}
    if position_context.get("hard_exit_expired") is True:
        return {"action": "EXIT", "reason_code": "hard_exit_expired"}
    return {"action": "HOLD", "reason_code": "mechanical_policy"}


def _matched_periods(evidence: dict[str, Any]) -> list[int]:
    raw = evidence.get("matched_periods") if isinstance(evidence.get("matched_periods"), list) else []
    periods: list[int] = []
    for item in raw:
        try:
            periods.append(int(item))
        except (TypeError, ValueError):
            continue
    return periods


def _ema_stack_direction(chart: dict[str, Any]) -> str | None:
    ema_values = chart.get("ema_values") if isinstance(chart.get("ema_values"), dict) else {}
    try:
        fast = (float(ema_values["36"]) + float(ema_values["43"])) / 2
        mid = (float(ema_values["144"]) + float(ema_values["169"])) / 2
        slow = (float(ema_values["576"]) + float(ema_values["676"])) / 2
    except (KeyError, TypeError, ValueError):
        return None
    if fast > mid > slow:
        return "LONG"
    if fast < mid < slow:
        return "SHORT"
    return None


def _chart_stats(chart: dict[str, Any], *, lookback: int) -> dict[str, float | None]:
    candles = chart.get("completed_candles") if isinstance(chart.get("completed_candles"), list) else []
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


def _confidence(*, primary_votes: int, total_votes: int, matched_count: int) -> float:
    context_score = primary_votes / max(total_votes, 1)
    cluster_bonus = min(0.12, max(0, matched_count - 3) * 0.03)
    return round(min(0.86, 0.48 + context_score * 0.28 + cluster_bonus), 2)


def _diagnostics(
    *,
    matched_periods: list[int],
    micro_direction: str | None,
    local_direction: str | None,
    macro_direction: str | None,
    five_minute_stats: dict[str, float | None],
    two_hour_stats: dict[str, float | None],
    one_day_stats: dict[str, float | None],
    runtime_mode: Any,
) -> dict[str, Any]:
    return {
        "matched_ema_count": len(matched_periods),
        "matched_periods": matched_periods,
        "micro_direction": micro_direction,
        "local_direction": local_direction,
        "macro_direction": macro_direction,
        "five_minute_return_pct": five_minute_stats["return_pct"],
        "two_hour_return_pct": two_hour_stats["return_pct"],
        "one_day_return_pct": one_day_stats["return_pct"],
        "runtime_mode": runtime_mode,
    }


def _decision(
    *,
    signal_id: str,
    action: str,
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
        "action": action,
        "trade_action": action,
        "direction": direction,
        "confidence": confidence,
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": diagnostics,
    }
