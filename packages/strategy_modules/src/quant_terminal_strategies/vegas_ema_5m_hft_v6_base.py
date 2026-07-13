from __future__ import annotations

from copy import deepcopy
from typing import Any

from quant_terminal_strategies import vegas_ema_5m_hft_v5_base as v5


STRATEGY_ID = "vegas_ema_5m_hft_v6_base"
STRATEGY_VERSION = "v0.1"
BOLLINGER_VALUE_COLUMNS = (
    "bb_mid_20",
    "bb_upper_20_2",
    "bb_lower_20_2",
    "bb_position_pct",
    "bb_bandwidth_pct",
    "bb_zscore",
)


def decide(context: dict[str, Any]) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    charts = payload.get("charts") if isinstance(payload.get("charts"), dict) else {}
    signal_id = str(signal.get("signal_id", "unknown"))
    required = {timeframe: charts.get(timeframe) for timeframe in ("5m", "2h", "8h", "12h")}
    if not all(isinstance(chart, dict) for chart in required.values()):
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.2,
            reason_code="missing_required_5m_2h_8h_or_12h_context",
            diagnostics={f"has_{timeframe}": isinstance(chart, dict) for timeframe, chart in required.items()},
        )

    invalid = [timeframe for timeframe, chart in required.items() if not v5._is_candles_only_chart(chart)]
    if invalid:
        return _decision(
            signal_id=signal_id,
            action="SKIP",
            direction="FLAT",
            confidence=0.2,
            reason_code="missing_candles_only_chart_data",
            diagnostics={"invalid_timeframes": invalid},
        )

    # V5's decision tree is reused with 12h installed at its macro-context input.
    # The Bollinger chart is deliberately excluded from that directional call.
    adapted_context = deepcopy(context)
    adapted_payload = adapted_context["signal"]["payload"]
    adapted_charts = adapted_payload["charts"]
    adapted_charts["1d"] = deepcopy(adapted_charts["12h"])
    decision = v5.decide(adapted_context)
    translated = _translate_v5_macro_names(decision)
    translated["strategy_id"] = STRATEGY_ID
    translated["strategy_version"] = STRATEGY_VERSION
    translated["decision_id"] = f"{STRATEGY_ID}-{STRATEGY_VERSION}-{signal_id}"
    diagnostics = translated.get("diagnostics") if isinstance(translated.get("diagnostics"), dict) else {}
    translated["diagnostics"] = {
        **diagnostics,
        "bollinger_1d": _bollinger_diagnostics(charts.get("bollinger_1d")),
        "open_interest_regime": _open_interest_diagnostics(payload),
    }
    return translated


def manage_position(context: dict[str, Any]) -> dict[str, Any]:
    return v5.manage_position(context)


def _bollinger_diagnostics(chart: Any) -> dict[str, Any]:
    if not isinstance(chart, dict):
        return {"available": False, "latest_completed": None, "forming": None}
    columns = chart.get("columns") if isinstance(chart.get("columns"), list) else []
    rows = chart.get("rows") if isinstance(chart.get("rows"), list) else []
    completed: dict[str, Any] | None = None
    forming: dict[str, Any] | None = None
    for row in rows:
        if not isinstance(row, list):
            continue
        values = {column: _value(row, columns, column) for column in BOLLINGER_VALUE_COLUMNS}
        values.update(
            {
                "open_ts": _value(row, columns, "open_ts"),
                "available_at": _value(row, columns, "available_at"),
                "source_candle_count": _value(row, columns, "source_candle_count"),
            }
        )
        if _value(row, columns, "complete") is True:
            completed = values
        else:
            forming = values
    return {
        "available": bool(completed or forming),
        "latest_completed": completed,
        "forming": forming,
    }


def _open_interest_diagnostics(payload: dict[str, Any]) -> dict[str, Any]:
    evidence = payload.get("evidence") if isinstance(payload.get("evidence"), dict) else {}
    features = evidence.get("derived_features") if isinstance(evidence.get("derived_features"), dict) else {}
    snapshot = features.get("open_interest_regime")
    if not isinstance(snapshot, dict):
        return {"available": False, "snapshot": None}
    return {"available": True, "snapshot": deepcopy(snapshot)}


def _translate_v5_macro_names(value: Any) -> Any:
    if isinstance(value, dict):
        return {_translate_name(str(key)): _translate_v5_macro_names(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_translate_v5_macro_names(item) for item in value]
    if isinstance(value, str):
        return _translate_name(value)
    return value


def _translate_name(value: str) -> str:
    translated = value.replace("one_day", "twelve_hour")
    translated = translated.replace("daily_1d", "macro_12h")
    translated = translated.replace("daily", "twelve_hour")
    return translated.replace("1d", "12h")


def _value(row: list[Any], columns: list[Any], column: str) -> Any:
    try:
        return row[columns.index(column)]
    except (ValueError, IndexError):
        return None


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
