from __future__ import annotations

from datetime import datetime
import math
from typing import Any


FEATURE_SPEC_VERSION = "vegas_5m_cluster_v6.stage1a_features.v1"
STAGE1A_CONTEXT_SCHEMA_VERSION = "vegas_5m_cluster_v6.stage1a_context.v2"
EMA_PERIODS = ("36", "43", "144", "169", "576", "676")
TIMEFRAMES = ("5m", "2h", "8h", "12h")

STAGE1A_CONTEXT_FEATURE_NAMES = (
    "5m_return_12_pct",
    "5m_return_48_pct",
    "5m_return_288_pct",
    "5m_realized_volatility_48_pct",
    "5m_realized_volatility_288_pct",
    "5m_range_position_48_pct",
    "5m_range_position_288_pct",
    "5m_volume_zscore_48",
    "5m_trend_efficiency_48",
    "5m_trend_efficiency_288",
    *(
        name
        for timeframe in ("2h", "8h", "12h")
        for name in (
            f"{timeframe}_completed_return_pct",
            f"{timeframe}_realized_volatility_12_pct",
            f"{timeframe}_range_position_12_pct",
            f"{timeframe}_trend_efficiency_12",
            f"{timeframe}_ema_36_slope_3_pct",
            f"{timeframe}_ema_576_slope_3_pct",
        )
    ),
    "1d_return_3_pct",
    "1d_return_10_pct",
    "1d_return_20_pct",
    "1d_realized_volatility_20_pct",
)

OI_FEATURE_NAMES = (
    "oi_return_pct_2h",
    "oi_return_pct_8h",
    "oi_return_pct_24h",
    "oi_change_2h_zscore_7d",
    "oi_general_long_short_ratio",
    "oi_taker_long_short_ratio_avg_2h",
)

BOLLINGER_FEATURE_NAMES = (
    "bb_last_position_pct",
    "bb_forming_position_pct",
    "bb_last_zscore",
    "bb_forming_zscore",
    "bb_last_bandwidth_pct",
    "bb_forming_bandwidth_pct",
    "bb_position_delta",
    "bb_zscore_delta",
    "bb_bandwidth_delta",
)

DEFAULT_ACTIVE_FEATURE_NAMES = (
    "matched_ema_count",
    "matched_period_36",
    "matched_period_43",
    "matched_period_144",
    "matched_period_169",
    "matched_period_576",
    "matched_period_676",
    "5m_last_return_pct",
    "5m_last_range_pct",
    "5m_last_close_location_pct",
    "5m_recent_range_pos_12_pct",
    "5m_ema_stack_spread_pct",
    "2h_last_return_pct",
    "2h_forming_return_pct",
    "2h_recent_range_pos_12_pct",
    "2h_ema_stack_spread_pct",
    "8h_last_return_pct",
    "8h_forming_return_pct",
    "8h_recent_range_pos_12_pct",
    "8h_ema_stack_spread_pct",
    "12h_last_return_pct",
    "12h_forming_return_pct",
    "12h_recent_range_pos_12_pct",
    "12h_ema_stack_spread_pct",
    "bb_last_position_pct",
    "bb_forming_position_pct",
    "bb_last_zscore",
    "bb_forming_zscore",
    "bb_last_bandwidth_pct",
    "bb_forming_bandwidth_pct",
    "bb_position_delta",
)


def extract_signal_features(signal: dict[str, Any]) -> dict[str, float | None]:
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else signal
    return extract_packet_features(payload if isinstance(payload, dict) else {})


def extract_packet_features(packet: dict[str, Any]) -> dict[str, float | None]:
    as_of = _parse_timestamp(packet.get("timestamp"))
    evidence = packet.get("evidence") if isinstance(packet.get("evidence"), dict) else {}
    charts = packet.get("charts") if isinstance(packet.get("charts"), dict) else {}
    matched_periods = {
        str(value)
        for value in evidence.get("matched_periods", [])
        if value not in (None, "")
    }

    features: dict[str, float | None] = {
        "matched_ema_count": _number(evidence.get("matched_ema_count")),
    }
    for period in EMA_PERIODS:
        features[f"matched_period_{period}"] = float(period in matched_periods)
    for timeframe in TIMEFRAMES:
        chart = charts.get(timeframe)
        features.update(
            _extract_chart_features(
                chart if isinstance(chart, dict) else {},
                timeframe=timeframe,
                as_of=as_of,
            )
        )
    features.update(
        _extract_bollinger_features(
            charts.get("bollinger_1d") if isinstance(charts.get("bollinger_1d"), dict) else {},
            as_of=as_of,
        )
    )
    features.update(_extract_oi_features(evidence, as_of=as_of))
    features.update(_extract_stage1a_context_features(evidence, as_of=as_of))
    return features


def observed_feature_names() -> tuple[str, ...]:
    feature_names = ["matched_ema_count"]
    feature_names.extend(f"matched_period_{period}" for period in EMA_PERIODS)
    for timeframe in TIMEFRAMES:
        feature_names.extend(_chart_feature_names(timeframe))
    feature_names.extend(BOLLINGER_FEATURE_NAMES)
    feature_names.extend(OI_FEATURE_NAMES)
    feature_names.extend(STAGE1A_CONTEXT_FEATURE_NAMES)
    return tuple(feature_names)


def _extract_chart_features(
    chart: dict[str, Any],
    *,
    timeframe: str,
    as_of: datetime | None,
) -> dict[str, float | None]:
    result = {name: None for name in _chart_feature_names(timeframe)}
    columns = _columns(chart)
    rows = chart.get("candles") if isinstance(chart.get("candles"), list) else []
    completed = [
        row
        for row in rows
        if isinstance(row, list)
        and _row_value(row, columns, "is_completed") is True
        and _row_available(row, columns, as_of=as_of, completed=True)
    ]
    forming = [
        row
        for row in rows
        if isinstance(row, list)
        and _row_value(row, columns, "is_completed") is False
        and _row_available(row, columns, as_of=as_of, completed=False)
    ]
    if not completed:
        return result

    latest = completed[-1]
    previous = completed[-2] if len(completed) > 1 else None
    latest_close = _number(_row_value(latest, columns, "close"))
    result.update(
        {
            f"{timeframe}_last_return_pct": _return_pct(latest, columns),
            f"{timeframe}_prev_return_pct": _return_pct(previous, columns),
            f"{timeframe}_last_range_pct": _range_pct(latest, columns),
            f"{timeframe}_last_close_location_pct": _close_location_pct(latest, columns),
            f"{timeframe}_recent_range_pos_6_pct": _recent_range_position_pct(
                completed, columns, 6
            ),
            f"{timeframe}_recent_range_pos_12_pct": _recent_range_position_pct(
                completed, columns, 12
            ),
        }
    )
    if forming:
        latest_forming = forming[-1]
        result[f"{timeframe}_forming_return_pct"] = _return_pct(latest_forming, columns)
        result[f"{timeframe}_forming_close_location_pct"] = _close_location_pct(
            latest_forming, columns
        )
        result[f"{timeframe}_forming_source_candle_count"] = _number(
            _row_value(latest_forming, columns, "source_candle_count")
        )

    ema_values = chart.get("ema_values") if isinstance(chart.get("ema_values"), dict) else {}
    ema_distances = (
        chart.get("ema_distances") if isinstance(chart.get("ema_distances"), dict) else {}
    )
    for period in EMA_PERIODS:
        ema = _number(ema_values.get(period))
        result[f"{timeframe}_ema_distance_{period}"] = _number(ema_distances.get(period))
        if latest_close is not None and ema not in (None, 0):
            result[f"{timeframe}_price_ema_{period}_distance_pct"] = (
                latest_close / ema - 1.0
            ) * 100.0
    ema_36 = _number(ema_values.get("36"))
    ema_576 = _number(ema_values.get("576"))
    if ema_36 is not None and ema_576 not in (None, 0):
        result[f"{timeframe}_ema_stack_spread_pct"] = (ema_36 / ema_576 - 1.0) * 100.0
    return result


def _extract_bollinger_features(
    chart: dict[str, Any], *, as_of: datetime | None
) -> dict[str, float | None]:
    result = {name: None for name in BOLLINGER_FEATURE_NAMES}
    columns = chart.get("columns") if isinstance(chart.get("columns"), list) else []
    rows = chart.get("rows") if isinstance(chart.get("rows"), list) else []
    available_rows = [
        row
        for row in rows
        if isinstance(row, list) and _timestamp_not_after(_row_value(row, columns, "available_at"), as_of)
    ]
    completed = [row for row in available_rows if _row_value(row, columns, "complete") is True]
    forming = [row for row in available_rows if _row_value(row, columns, "complete") is False]
    latest_completed = completed[-1] if completed else None
    latest_forming = forming[-1] if forming else None

    mappings = {
        "position_pct": "bb_position_pct",
        "zscore": "bb_zscore",
        "bandwidth_pct": "bb_bandwidth_pct",
    }
    for suffix, column in mappings.items():
        result[f"bb_last_{suffix}"] = _number(_row_value(latest_completed, columns, column))
        result[f"bb_forming_{suffix}"] = _number(_row_value(latest_forming, columns, column))
    for suffix in ("position", "zscore", "bandwidth"):
        completed_value = result[f"bb_last_{suffix}_pct"] if suffix != "zscore" else result["bb_last_zscore"]
        forming_value = result[f"bb_forming_{suffix}_pct"] if suffix != "zscore" else result["bb_forming_zscore"]
        result[f"bb_{suffix}_delta"] = (
            forming_value - completed_value
            if forming_value is not None and completed_value is not None
            else None
        )
    return result


def _extract_oi_features(
    evidence: dict[str, Any], *, as_of: datetime | None
) -> dict[str, float | None]:
    result = {name: None for name in OI_FEATURE_NAMES}
    derived = (
        evidence.get("derived_features")
        if isinstance(evidence.get("derived_features"), dict)
        else {}
    )
    snapshot = (
        derived.get("open_interest_regime")
        if isinstance(derived.get("open_interest_regime"), dict)
        else {}
    )
    if not _timestamp_not_after(snapshot.get("available_at"), as_of):
        return result
    values = snapshot.get("values") if isinstance(snapshot.get("values"), dict) else {}
    source_names = {
        "oi_return_pct_2h": "oi_return_pct_2h",
        "oi_return_pct_8h": "oi_return_pct_8h",
        "oi_return_pct_24h": "oi_return_pct_24h",
        "oi_change_2h_zscore_7d": "oi_change_2h_zscore_7d",
        "oi_general_long_short_ratio": "general_long_short_ratio",
        "oi_taker_long_short_ratio_avg_2h": "taker_long_short_ratio_avg_2h",
    }
    for feature_name, source_name in source_names.items():
        result[feature_name] = _number(values.get(source_name))
    return result


def _extract_stage1a_context_features(
    evidence: dict[str, Any], *, as_of: datetime | None
) -> dict[str, float | None]:
    result = {name: None for name in STAGE1A_CONTEXT_FEATURE_NAMES}
    derived = evidence.get("derived_features") if isinstance(evidence.get("derived_features"), dict) else {}
    snapshot = derived.get("stage1a_context_v2") if isinstance(derived.get("stage1a_context_v2"), dict) else {}
    available_at = _parse_timestamp(snapshot.get("available_at"))
    if (
        snapshot.get("schema_version") != STAGE1A_CONTEXT_SCHEMA_VERSION
        or snapshot.get("complete") is not True
        or as_of is None
        or available_at is None
        or as_of.tzinfo is None
        or available_at.tzinfo is None
        or available_at > as_of
    ):
        return result
    values = snapshot.get("values") if isinstance(snapshot.get("values"), dict) else {}
    for name in STAGE1A_CONTEXT_FEATURE_NAMES:
        result[name] = _number(values.get(name))
    return result


def _chart_feature_names(timeframe: str) -> tuple[str, ...]:
    names = [
        f"{timeframe}_last_return_pct",
        f"{timeframe}_prev_return_pct",
        f"{timeframe}_last_range_pct",
        f"{timeframe}_last_close_location_pct",
        f"{timeframe}_recent_range_pos_6_pct",
        f"{timeframe}_recent_range_pos_12_pct",
        f"{timeframe}_forming_return_pct",
        f"{timeframe}_forming_close_location_pct",
        f"{timeframe}_forming_source_candle_count",
        f"{timeframe}_ema_stack_spread_pct",
    ]
    for period in EMA_PERIODS:
        names.append(f"{timeframe}_ema_distance_{period}")
        names.append(f"{timeframe}_price_ema_{period}_distance_pct")
    return tuple(names)


def _columns(chart: dict[str, Any]) -> list[Any]:
    columns = chart.get("columns")
    if isinstance(columns, list):
        return columns
    candle_columns = chart.get("candle_columns")
    return candle_columns if isinstance(candle_columns, list) else []


def _row_available(
    row: list[Any],
    columns: list[Any],
    *,
    as_of: datetime | None,
    completed: bool,
) -> bool:
    column = "expected_close_timestamp" if completed else "partial_close_timestamp"
    value = _row_value(row, columns, column)
    if value in (None, ""):
        value = _row_value(row, columns, "ts")
    return _timestamp_not_after(value, as_of)


def _timestamp_not_after(value: Any, as_of: datetime | None) -> bool:
    if as_of is None or value in (None, ""):
        return True
    timestamp = _parse_timestamp(value)
    return timestamp is None or timestamp <= as_of


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _row_value(row: list[Any] | None, columns: list[Any], name: str) -> Any:
    if row is None:
        return None
    try:
        return row[columns.index(name)]
    except (ValueError, IndexError):
        return None


def _number(value: Any) -> float | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _return_pct(row: list[Any] | None, columns: list[Any]) -> float | None:
    open_ = _number(_row_value(row, columns, "open"))
    close = _number(_row_value(row, columns, "close"))
    if open_ in (None, 0) or close is None:
        return None
    return (close / open_ - 1.0) * 100.0


def _range_pct(row: list[Any] | None, columns: list[Any]) -> float | None:
    close = _number(_row_value(row, columns, "close"))
    high = _number(_row_value(row, columns, "high"))
    low = _number(_row_value(row, columns, "low"))
    if close in (None, 0) or high is None or low is None:
        return None
    return (high - low) / close * 100.0


def _close_location_pct(row: list[Any] | None, columns: list[Any]) -> float | None:
    close = _number(_row_value(row, columns, "close"))
    high = _number(_row_value(row, columns, "high"))
    low = _number(_row_value(row, columns, "low"))
    if close is None or high is None or low is None or high == low:
        return None
    return (close - low) / (high - low) * 100.0


def _recent_range_position_pct(
    rows: list[list[Any]], columns: list[Any], lookback: int
) -> float | None:
    closes = [_number(_row_value(row, columns, "close")) for row in rows[-lookback:]]
    values = [value for value in closes if value is not None]
    if not values:
        return None
    low = min(values)
    high = max(values)
    if high == low:
        return 50.0
    return (values[-1] - low) / (high - low) * 100.0
