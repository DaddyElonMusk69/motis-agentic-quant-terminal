from __future__ import annotations

from typing import Any


STRATEGY_ID = "vegas_ema_recursive_features_base"
STRATEGY_VERSION = "v0.1"


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else signal
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    features = payload.get("features") if isinstance(payload.get("features"), dict) else evidence.get("features")
    charts = payload.get("charts") if isinstance(payload.get("charts"), dict) else {}
    signal_id = str(signal.get("signal_id", "unknown"))

    if not isinstance(features, dict):
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.2,
            reason_code="missing_feature_payload",
            diagnostics={"has_features": False},
        )

    five_minute = _latest(features, "5m")
    two_hour = _latest(features, "2h")
    one_day = _latest(features, "1d")
    if not all(isinstance(item, dict) and item for item in (five_minute, two_hour, one_day)):
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.22,
            reason_code="missing_required_5m_2h_or_1d_features",
            diagnostics={
                "has_5m": bool(five_minute),
                "has_2h": bool(two_hour),
                "has_1d": bool(one_day),
            },
        )

    feature_bias = _feature_bias(five_minute=five_minute, two_hour=two_hour, one_day=one_day)
    chart_bias = _chart_bias(charts)
    active_timeframes = list(evidence.get("active_timeframes") or payload.get("active_timeframes") or [])
    vote_count = len(active_timeframes)
    if vote_count < 2:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.28,
            reason_code="insufficient_recursive_vegas_votes",
            diagnostics=_diagnostics(
                feature_bias=feature_bias,
                chart_bias=chart_bias,
                active_timeframes=active_timeframes,
                five_minute=five_minute,
                two_hour=two_hour,
                one_day=one_day,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    if _is_overextended_or_volatile(five_minute=five_minute, feature_bias=feature_bias):
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.34,
            reason_code="feature_context_overextended_or_volatile",
            diagnostics=_diagnostics(
                feature_bias=feature_bias,
                chart_bias=chart_bias,
                active_timeframes=active_timeframes,
                five_minute=five_minute,
                two_hour=two_hour,
                one_day=one_day,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    if feature_bias not in {"LONG", "SHORT"}:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.3,
            reason_code="feature_bias_unresolved",
            diagnostics=_diagnostics(
                feature_bias=feature_bias,
                chart_bias=chart_bias,
                active_timeframes=active_timeframes,
                five_minute=five_minute,
                two_hour=two_hour,
                one_day=one_day,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    if chart_bias in {"LONG", "SHORT"} and chart_bias != feature_bias:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.35,
            reason_code="feature_chart_directional_conflict",
            diagnostics=_diagnostics(
                feature_bias=feature_bias,
                chart_bias=chart_bias,
                active_timeframes=active_timeframes,
                five_minute=five_minute,
                two_hour=two_hour,
                one_day=one_day,
                runtime_mode=context.get("runtime_mode", "backtest"),
            ),
        )

    return _decision(
        signal_id=signal_id,
        action="ENTER",
        direction=feature_bias,
        confidence=_confidence(feature_bias=feature_bias, chart_bias=chart_bias, vote_count=vote_count),
        reason_code=f"feature_aligned_recursive_vegas_{feature_bias.lower()}",
        diagnostics=_diagnostics(
            feature_bias=feature_bias,
            chart_bias=chart_bias,
            active_timeframes=active_timeframes,
            five_minute=five_minute,
            two_hour=two_hour,
            one_day=one_day,
            runtime_mode=context.get("runtime_mode", "backtest"),
        ),
    )


def manage_position(context: dict[str, Any]) -> dict[str, Any]:
    position_context = context.get("position_context") if isinstance(context.get("position_context"), dict) else {}
    if position_context.get("hard_exit_expired") is True:
        return {"action": "EXIT", "reason_code": "hard_exit_expired"}
    return {"action": "HOLD", "reason_code": "mechanical_policy"}


def _feature_bias(*, five_minute: dict[str, Any], two_hour: dict[str, Any], one_day: dict[str, Any]) -> str | None:
    votes = [
        _ema_structure_direction(two_hour) or _ema_structure_direction(five_minute),
        _momentum_direction(two_hour, threshold_pct=0.25),
        _momentum_direction(one_day, threshold_pct=0.75),
        _micro_flow_direction(five_minute),
    ]
    long_votes = votes.count("LONG")
    short_votes = votes.count("SHORT")
    if long_votes >= 3 and long_votes > short_votes:
        return "LONG"
    if short_votes >= 3 and short_votes > long_votes:
        return "SHORT"
    return None


def _ema_structure_direction(row: dict[str, Any]) -> str | None:
    ema = _family(row, "ema_vegas_structure")
    stack = str(ema.get("ema_stack_state", "")).lower()
    fast_mid = _number(ema.get("fast_mid_gap_pct"))
    mid_slow = _number(ema.get("mid_slow_gap_pct"))
    if stack == "bull_stack" or (fast_mid is not None and mid_slow is not None and fast_mid > 0 and mid_slow > 0):
        return "LONG"
    if stack == "bear_stack" or (fast_mid is not None and mid_slow is not None and fast_mid < 0 and mid_slow < 0):
        return "SHORT"
    return None


def _momentum_direction(row: dict[str, Any], *, threshold_pct: float) -> str | None:
    regime = _family(row, "regime_momentum")
    values = [
        _number(regime.get("return_pct_12")),
        _number(regime.get("return_pct_48")),
    ]
    values = [value for value in values if value is not None]
    if not values:
        return None
    average = sum(values) / len(values)
    if average >= threshold_pct:
        return "LONG"
    if average <= -threshold_pct:
        return "SHORT"
    return None


def _micro_flow_direction(row: dict[str, Any]) -> str | None:
    base = _family(row, "base_candle")
    value = _number(base.get("return_pct"))
    if value is None:
        return None
    if value >= 0.02:
        return "LONG"
    if value <= -0.02:
        return "SHORT"
    return None


def _chart_bias(charts: dict[str, Any]) -> str | None:
    if not isinstance(charts, dict):
        return None
    directions = [
        _chart_direction(charts.get("2h"), threshold_pct=0.25),
        _chart_direction(charts.get("1d"), threshold_pct=0.75),
    ]
    if directions.count("LONG") >= 2:
        return "LONG"
    if directions.count("SHORT") >= 2:
        return "SHORT"
    return None


def _chart_direction(chart: Any, *, threshold_pct: float) -> str | None:
    if not isinstance(chart, dict):
        return None
    candles = chart.get("completed_candles") if isinstance(chart.get("completed_candles"), list) else []
    closes = [_close(candle, chart.get("columns", [])) for candle in candles]
    closes = [close for close in closes if close is not None]
    if len(closes) < 2:
        return None
    change = _pct_change(closes[0], closes[-1])
    if change is None:
        return None
    if change >= threshold_pct:
        return "LONG"
    if change <= -threshold_pct:
        return "SHORT"
    return None


def _is_overextended_or_volatile(*, five_minute: dict[str, Any], feature_bias: str | None) -> bool:
    volatility = _family(five_minute, "volatility_range")
    bollinger = _family(five_minute, "bollinger")
    atr_pct = _number(volatility.get("atr_pct_14"))
    bb_position = _number(bollinger.get("bb_position_pct"))
    if atr_pct is not None and atr_pct >= 1.0:
        return True
    if feature_bias == "LONG" and bb_position is not None and bb_position >= 95:
        return True
    if feature_bias == "SHORT" and bb_position is not None and bb_position <= 5:
        return True
    return False


def _latest(features: dict[str, Any], timeframe: str) -> dict[str, Any]:
    frame = features.get(timeframe)
    if not isinstance(frame, dict):
        return {}
    latest = frame.get("latest")
    if isinstance(latest, dict) and latest:
        return latest
    window = frame.get("window")
    if isinstance(window, list) and window and isinstance(window[-1], dict):
        return window[-1]
    return {}


def _family(row: dict[str, Any], key: str) -> dict[str, Any]:
    value = row.get(key)
    return value if isinstance(value, dict) else {}


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


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _confidence(*, feature_bias: str, chart_bias: str | None, vote_count: int) -> float:
    base = 0.6
    if chart_bias == feature_bias:
        base += 0.08
    base += min(0.08, max(0, vote_count - 2) * 0.03)
    return round(min(0.82, base), 2)


def _diagnostics(
    *,
    feature_bias: str | None,
    chart_bias: str | None,
    active_timeframes: list[Any],
    five_minute: dict[str, Any],
    two_hour: dict[str, Any],
    one_day: dict[str, Any],
    runtime_mode: Any,
) -> dict[str, Any]:
    return {
        "feature_bias": feature_bias,
        "chart_bias": chart_bias,
        "active_timeframes": active_timeframes,
        "active_timeframe_count": len(active_timeframes),
        "five_minute_return_pct": _number(_family(five_minute, "base_candle").get("return_pct")),
        "five_minute_atr_pct_14": _number(_family(five_minute, "volatility_range").get("atr_pct_14")),
        "five_minute_bb_position_pct": _number(_family(five_minute, "bollinger").get("bb_position_pct")),
        "two_hour_momentum_pct": _number(_family(two_hour, "regime_momentum").get("return_pct_12")),
        "one_day_momentum_pct": _number(_family(one_day, "regime_momentum").get("return_pct_48")),
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
