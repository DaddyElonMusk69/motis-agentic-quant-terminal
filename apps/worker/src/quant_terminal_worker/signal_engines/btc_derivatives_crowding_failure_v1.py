from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import (
    LiveSignalScanResult,
    SignalPacket,
    TrainingSignalGenerationResult,
    validate_signal_packet,
)
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.oi_compression_v2 import (
    BASE_TIMEFRAME_DELTA,
    CANDLE_COLUMNS,
    HTF_COLUMNS,
    _aggregate_htf_row,
    _candle_chart_row,
    _context_timeframes,
    _feature_to_str,
    _htf_source_lookback_bars,
    _iso_z,
    _number_to_str,
    _optional_seed_timestamp,
    _pct_change_series,
    _price_to_str,
    _rolling_prior_median_ratio,
    _rolling_prior_zscore,
    _utc,
)
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


ENGINE_ID = "btc_derivatives_crowding_failure_v1"
DEFAULT_RANGE_LOOKBACK_BARS = 96
DEFAULT_OI_CHANGE_WINDOW_BARS = 48
DEFAULT_STATS_LOOKBACK_BARS = 576
DEFAULT_MIN_STATS_BARS = 200
DEFAULT_OI_Z_THRESHOLD = 0.6
DEFAULT_MIN_OI_CHANGE_4H_PCT = 0.10
DEFAULT_BREAKOUT_THRESHOLD_PCT = 0.05
DEFAULT_MIN_REJECTION_WICK_PCT = 15.0
DEFAULT_MIN_VOLUME_RATIO = 1.05
DEFAULT_GLOBAL_LONG_RATIO_THRESHOLD = 1.05
DEFAULT_GLOBAL_SHORT_RATIO_THRESHOLD = 0.95
DEFAULT_TAKER_BUY_RATIO_THRESHOLD = 1.05
DEFAULT_TAKER_SELL_RATIO_THRESHOLD = 0.95
DEFAULT_MAX_TOP_POSITION_LONG_SHARE_GAP = 0.02
DEFAULT_MIN_TOP_POSITION_LONG_SHARE_GAP = -0.02
DEFAULT_MIN_CROWDING_SCORE = 4
DEFAULT_DEDUPE_WINDOW_MINUTES = 180
DEFAULT_CONTEXT_BARS = 96
DEFAULT_CONTEXT_TIMEFRAMES = ("1h", "4h", "12h")

FUTURES_METRICS_COLUMNS = [
    "ts",
    "oi",
    "oi_value",
    "top_account_ls",
    "top_position_ls",
    "global_ls",
    "taker_buy_sell_ratio",
    "top_account_vs_global_long_share_gap",
    "top_position_vs_global_long_share_gap",
    "available_at",
    "confirm",
]
HTF_FUTURES_METRICS_COLUMNS = [
    "open_ts",
    "close_ts",
    "partial_close_ts",
    "complete",
    "count",
    "oi_change_pct",
    "oi_value_change_pct",
    "global_ls_avg",
    "global_ls_last",
    "taker_buy_sell_ratio_avg",
    "taker_buy_sell_ratio_last",
    "top_account_ls_avg",
    "top_account_ls_last",
    "top_position_ls_avg",
    "top_position_ls_last",
    "top_account_vs_global_gap_avg",
    "top_account_vs_global_gap_last",
    "top_position_vs_global_gap_avg",
    "top_position_vs_global_gap_last",
]
PREMIUM_COLUMNS = [
    "ts",
    "premium_open",
    "premium_high",
    "premium_low",
    "premium_close",
    "available_at",
]
FUNDING_COLUMNS = [
    "ts",
    "latest_funding_rate",
    "annualized_funding_rate",
    "funding_rate_zscore_7d",
    "funding_signed_streak",
    "minutes_to_expected_funding",
    "available_at",
]


def generate_training_signals(context: EngineTrainingContext) -> EngineTrainingOutput:
    params = _with_defaults(context.parameters)
    warmup_start = _scan_warmup_start(start=context.start, parameters=params)
    raw_5m = context.market_data_reader.get_candles(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        start=warmup_start,
        end=context.end,
    )
    raw_metrics = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="futures_metrics",
        start=warmup_start,
        end=context.end,
    )
    raw_premium = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="premium_index",
        start=warmup_start,
        end=context.end,
    )
    funding_features = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="derived",
        data_type="funding_features",
        start=warmup_start,
        end=context.end,
    )
    scan_coverage_end = _actual_scan_coverage_end(
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
        start=context.start,
        end=context.end,
        parameters=params,
    )
    packets, generated_packet_count = generate_derivatives_crowding_failure_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
        start=context.start,
        end=context.end,
        parameters=params,
        packet_sink=context.packet_sink,
        packet_chunk_size=context.packet_chunk_size,
    )
    return EngineTrainingOutput(
        result=TrainingSignalGenerationResult(
            status="appended" if generated_packet_count else "noop",
            generated_packet_count=generated_packet_count,
            appended_packet_count=0,
            raw_candle_end_ts=_iso_z(context.raw_candle_end),
            scan_coverage_end_ts=_iso_z(scan_coverage_end) if scan_coverage_end is not None else None,
            packet_refs=[],
        ),
        packets=packets,
    )


def scan_live_signal(context: EngineLiveScanContext) -> LiveSignalScanResult:
    params = _with_defaults(context.parameters)
    latest_end = _latest_required_ref_end(
        repository=context.repository,
        asset=context.asset,
    )
    warmup_start = _scan_warmup_start(start=latest_end, parameters=params) if latest_end else None
    raw_5m = context.market_data_reader.get_candles(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        start=warmup_start,
        end=latest_end,
    )
    raw_metrics = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="futures_metrics",
        start=warmup_start,
        end=latest_end,
    )
    raw_premium = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="premium_index",
        start=warmup_start,
        end=latest_end,
    )
    funding_features = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="derived",
        data_type="funding_features",
        start=warmup_start,
        end=latest_end,
    )
    packet = scan_derivatives_crowding_failure_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
        parameters=params,
    )
    if packet is None:
        return LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest_confirmed_derivatives_state_did_not_trigger",
        )
    return LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket.from_mapping(packet),
    )


def generate_derivatives_crowding_failure_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]] | None,
    funding_features: list[dict[str, Any]] | None,
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
    packet_sink: Any | None = None,
    packet_chunk_size: int = 500,
) -> tuple[list[dict[str, Any]], int]:
    del workspace_root
    _require_supported_asset(asset)
    params = _with_defaults(parameters)
    aligned_rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium or [],
        funding_features=funding_features or [],
    )
    if not aligned_rows:
        return [], 0
    aligned_rows = _trim_rows_for_scan(rows=aligned_rows, start=start, end=end, parameters=params)
    if not aligned_rows:
        return [], 0
    feature_cache = _build_feature_cache(aligned_rows, params)

    timestamps = [row["timestamp"] for row in aligned_rows]
    start_index = max(0, bisect_left(timestamps, _utc(start)))
    end_index = bisect_right(timestamps, _utc(end)) - 1
    if end_index < start_index:
        return [], 0

    packets: list[dict[str, Any]] = []
    buffered_packets: list[dict[str, Any]] = []
    generated_packet_count = 0
    last_emitted_at = _optional_seed_timestamp(params)
    dedupe_window = timedelta(minutes=int(params["dedupe_window_minutes"]))

    for index in range(start_index, end_index + 1):
        timestamp = aligned_rows[index]["timestamp"]
        if last_emitted_at is not None and timestamp - last_emitted_at < dedupe_window:
            continue
        packet = _scan_index(
            asset=asset,
            instrument=instrument,
            rows=aligned_rows,
            index=index,
            parameters=params,
            feature_cache=feature_cache,
        )
        if packet is None:
            continue
        last_emitted_at = timestamp
        generated_packet_count += 1
        if callable(packet_sink):
            buffered_packets.append(packet)
            if len(buffered_packets) >= max(1, int(packet_chunk_size)):
                packet_sink(buffered_packets)
                buffered_packets = []
        else:
            packets.append(packet)

    if callable(packet_sink) and buffered_packets:
        packet_sink(buffered_packets)

    return packets, generated_packet_count


def scan_derivatives_crowding_failure_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]] | None,
    funding_features: list[dict[str, Any]] | None,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    _require_supported_asset(asset)
    rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium or [],
        funding_features=funding_features or [],
    )
    if not rows:
        return None
    params = _with_defaults(parameters)
    latest_timestamp = rows[-1]["timestamp"]
    rows = _trim_rows_for_scan(rows=rows, start=latest_timestamp, end=latest_timestamp, parameters=params)
    if not rows:
        return None
    return _scan_index(
        asset=asset,
        instrument=instrument,
        rows=rows,
        index=len(rows) - 1,
        parameters=params,
        feature_cache=_build_feature_cache(rows, params),
    )


def _scan_index(
    *,
    asset: str,
    instrument: str,
    rows: list[dict[str, Any]],
    index: int,
    parameters: dict[str, Any],
    feature_cache: dict[str, list[float | None]],
) -> dict[str, Any] | None:
    features = _event_features(rows=rows, index=index, parameters=parameters, feature_cache=feature_cache)
    if features is None:
        return None
    if not features["thresholds_met"]:
        return None

    row = rows[index]
    context_bars = int(parameters["context_bars"])
    context_start = max(0, index - context_bars + 1)
    context_rows = rows[context_start : index + 1]
    signal_open_ts = row["timestamp"]
    signal_close_ts = signal_open_ts + BASE_TIMEFRAME_DELTA
    signal_available_at = signal_close_ts
    numeric_features = {
        key: value
        for key, value in features.items()
        if isinstance(value, (float, int)) and not isinstance(value, bool)
    }
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset.upper(),
        "instrument": instrument,
        "timestamp": _iso_z(signal_open_ts),
        "active_timeframes": ["5m", *_context_timeframes(parameters)],
        "evidence": {
            "engine": ENGINE_ID,
            "pattern": "derivatives_crowding_failure",
            "event_type": "DERIVATIVES_CROWDING_FAILURE",
            "event_subtype": str(features["event_subtype"]),
            "crowding_state": str(features["crowding_state"]),
            "breakout_direction": str(features["breakout_direction"]),
            "failure_type": str(features["failure_type"]),
            "trapped_participants": str(features["trapped_participants"]),
            "trigger_timeframe": "5m",
            "signal_candle_open_ts": _iso_z(signal_open_ts),
            "signal_candle_close_ts": _iso_z(signal_close_ts),
            "signal_available_at": _iso_z(signal_available_at),
            "range_lookback_bars": int(parameters["range_lookback_bars"]),
            "oi_change_window_bars": int(parameters["oi_change_window_bars"]),
            "stats_lookback_bars": int(parameters["stats_lookback_bars"]),
            "min_stats_bars": int(parameters["min_stats_bars"]),
            "breakout_threshold_pct": _number_to_str(parameters["breakout_threshold_pct"], places=6),
            "min_rejection_wick_pct": _number_to_str(parameters["min_rejection_wick_pct"], places=6),
            "oi_z_threshold": _number_to_str(parameters["oi_z_threshold"], places=6),
            "min_oi_change_4h_pct": _number_to_str(parameters["min_oi_change_4h_pct"], places=6),
            "min_volume_ratio": _number_to_str(parameters["min_volume_ratio"], places=6),
            "min_crowding_score": int(parameters["min_crowding_score"]),
            "dedupe_window_minutes": int(parameters["dedupe_window_minutes"]),
            "close_back_inside_range": bool(features["close_back_inside_range"]),
            "reference_price": _price_to_str(row["close"]),
            "trigger_candle_close": _price_to_str(row["close"]),
            **{key: _feature_to_str(key, value) for key, value in numeric_features.items()},
        },
        "charts": {
            "5m": {
                "role": "trigger_context",
                "timeframe": "5m",
                "columns": CANDLE_COLUMNS,
                "candles": [_candle_chart_row(item) for item in context_rows],
            },
            "futures_metrics_5m": {
                "role": "derivatives_positioning_context",
                "timeframe": "5m",
                "columns": FUTURES_METRICS_COLUMNS,
                "rows": [_futures_metrics_row(item) for item in context_rows],
            },
            "premium_index_5m": {
                "role": "premium_context",
                "timeframe": "5m",
                "columns": PREMIUM_COLUMNS,
                "rows": [_premium_row(item) for item in context_rows if item.get("premium_close") is not None],
            },
            "funding_features_5m": {
                "role": "funding_context",
                "timeframe": "5m",
                "columns": FUNDING_COLUMNS,
                "rows": [_funding_row(item) for item in context_rows if item.get("latest_funding_rate") is not None],
            },
        },
    }
    context_timeframes = _context_timeframes(parameters)
    htf_source_start = max(
        0,
        index - _htf_source_lookback_bars(context_bars=context_bars, context_timeframes=context_timeframes) + 1,
    )
    htf_source_rows = rows[htf_source_start : index + 1]
    packet["charts"].update(
        _bounded_candle_htf_charts(
            rows=htf_source_rows,
            signal_open_ts=signal_open_ts,
            signal_available_at=signal_available_at,
            context_bars=context_bars,
            context_timeframes=context_timeframes,
        )
    )
    packet["charts"].update(
        _futures_metrics_htf_charts(
            rows=htf_source_rows,
            signal_open_ts=signal_open_ts,
            signal_available_at=signal_available_at,
            context_bars=context_bars,
            context_timeframes=context_timeframes,
        )
    )
    validate_signal_packet(packet)
    return packet


def _event_features(
    *,
    rows: list[dict[str, Any]],
    index: int,
    parameters: dict[str, Any],
    feature_cache: dict[str, list[float | None]],
) -> dict[str, Any] | None:
    min_stats = int(parameters["min_stats_bars"])
    range_lookback = int(parameters["range_lookback_bars"])
    if index < max(min_stats, range_lookback):
        return None

    row = rows[index]
    prior_range = rows[index - range_lookback : index]
    prior_high = max(item["high"] for item in prior_range)
    prior_low = min(item["low"] for item in prior_range)
    prior_span = prior_high - prior_low
    if prior_high <= 0 or prior_low <= 0 or prior_span <= 0 or row["close"] <= 0:
        return None

    oi_zscore = feature_cache["oi_change_4h_zscore"][index]
    volume_ratio = feature_cache["volume_ratio_2d_median"][index]
    oi_change_4h = feature_cache["oi_change_4h"][index]
    if oi_zscore is None or volume_ratio is None or oi_change_4h is None:
        return None

    upside_breakout_pct = max(0.0, (row["high"] - prior_high) / prior_high * 100.0)
    downside_breakout_pct = max(0.0, (prior_low - row["low"]) / prior_low * 100.0)
    close_back_inside_upside = row["close"] < prior_high
    close_back_inside_downside = row["close"] > prior_low
    upper_wick_pct, lower_wick_pct = _wick_share_pct(row)
    close_position_in_prior_range_pct = (row["close"] - prior_low) / prior_span * 100.0

    top_account_gap = _long_share_gap(
        top_ratio=row.get("top_trader_account_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    top_position_gap = _long_share_gap(
        top_ratio=row.get("top_trader_position_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )

    long_score = _crowded_long_score(
        row=row,
        top_position_gap=top_position_gap,
        top_account_gap=top_account_gap,
        oi_zscore=oi_zscore,
        oi_change_4h_pct=oi_change_4h * 100.0,
        volume_ratio=volume_ratio,
        parameters=parameters,
    )
    short_score = _crowded_short_score(
        row=row,
        top_position_gap=top_position_gap,
        top_account_gap=top_account_gap,
        oi_zscore=oi_zscore,
        oi_change_4h_pct=oi_change_4h * 100.0,
        volume_ratio=volume_ratio,
        parameters=parameters,
    )

    min_score = int(parameters["min_crowding_score"])
    upside_failure = (
        upside_breakout_pct >= float(parameters["breakout_threshold_pct"])
        and close_back_inside_upside
        and upper_wick_pct >= float(parameters["min_rejection_wick_pct"])
        and long_score >= min_score
    )
    downside_failure = (
        downside_breakout_pct >= float(parameters["breakout_threshold_pct"])
        and close_back_inside_downside
        and lower_wick_pct >= float(parameters["min_rejection_wick_pct"])
        and short_score >= min_score
    )
    if upside_failure and downside_failure:
        return None
    if not upside_failure and not downside_failure:
        return None

    if upside_failure:
        event_subtype = "crowded_long_failure"
        crowding_state = "leveraged_longs_failed_to_extend"
        breakout_direction = "UP"
        failure_type = "failed_upside_breakout"
        trapped_participants = "leveraged_longs"
        breakout_distance_pct = upside_breakout_pct
        rejection_wick_pct = upper_wick_pct
        crowding_score = long_score
        close_back_inside_range = close_back_inside_upside
    else:
        event_subtype = "crowded_short_failure"
        crowding_state = "leveraged_shorts_failed_to_extend"
        breakout_direction = "DOWN"
        failure_type = "failed_downside_breakdown"
        trapped_participants = "leveraged_shorts"
        breakout_distance_pct = downside_breakout_pct
        rejection_wick_pct = lower_wick_pct
        crowding_score = short_score
        close_back_inside_range = close_back_inside_downside

    return {
        "thresholds_met": True,
        "close": row["close"],
        "event_subtype": event_subtype,
        "crowding_state": crowding_state,
        "breakout_direction": breakout_direction,
        "failure_type": failure_type,
        "trapped_participants": trapped_participants,
        "prior_range_high": prior_high,
        "prior_range_low": prior_low,
        "prior_range_pct": prior_span / row["close"] * 100.0,
        "breakout_distance_pct": breakout_distance_pct,
        "close_back_inside_range": close_back_inside_range,
        "upper_wick_pct": upper_wick_pct,
        "lower_wick_pct": lower_wick_pct,
        "rejection_wick_pct": rejection_wick_pct,
        "close_position_in_prior_range_pct": close_position_in_prior_range_pct,
        "crowding_score": crowding_score,
        "crowded_long_score": long_score,
        "crowded_short_score": short_score,
        "oi_change_1h_pct": (feature_cache["oi_change_1h"][index] or 0.0) * 100.0,
        "oi_change_4h_pct": oi_change_4h * 100.0,
        "oi_change_12h_pct": (feature_cache["oi_change_12h"][index] or 0.0) * 100.0,
        "oi_change_4h_zscore": oi_zscore,
        "oi_value_change_4h_pct": (feature_cache["oi_value_change_4h"][index] or 0.0) * 100.0,
        "price_return_1h_pct": (feature_cache["price_return_1h"][index] or 0.0) * 100.0,
        "price_return_4h_pct": (feature_cache["price_return_4h"][index] or 0.0) * 100.0,
        "price_return_12h_pct": (feature_cache["price_return_12h"][index] or 0.0) * 100.0,
        "volume_ratio_2d_median": volume_ratio,
        "global_account_long_short_ratio": row["global_account_long_short_ratio"],
        "taker_buy_sell_volume_ratio": row["taker_buy_sell_volume_ratio"],
        "top_trader_account_long_short_ratio": row["top_trader_account_long_short_ratio"],
        "top_trader_position_long_short_ratio": row["top_trader_position_long_short_ratio"],
        "top_trader_account_vs_global_long_share_gap": top_account_gap or 0.0,
        "top_trader_position_vs_global_long_share_gap": top_position_gap or 0.0,
        "premium_close": row.get("premium_close") or 0.0,
        "latest_funding_rate": row.get("latest_funding_rate") or 0.0,
        "annualized_funding_rate": row.get("annualized_funding_rate") or 0.0,
        "funding_rate_zscore_7d": row.get("funding_rate_zscore_7d") or 0.0,
    }


def _crowded_long_score(
    *,
    row: dict[str, Any],
    top_position_gap: float | None,
    top_account_gap: float | None,
    oi_zscore: float,
    oi_change_4h_pct: float,
    volume_ratio: float,
    parameters: dict[str, Any],
) -> int:
    score = 0
    global_crowded = row["global_account_long_short_ratio"] >= float(parameters["global_long_ratio_threshold"])
    if global_crowded:
        score += 1
    if row["taker_buy_sell_volume_ratio"] >= float(parameters["taker_buy_ratio_threshold"]):
        score += 1
    if global_crowded and top_position_gap is not None and top_position_gap <= float(parameters["max_top_position_long_share_gap"]):
        score += 1
    elif global_crowded and top_account_gap is not None and top_account_gap <= float(parameters["max_top_position_long_share_gap"]):
        score += 1
    if oi_zscore >= float(parameters["oi_z_threshold"]):
        score += 1
    if oi_change_4h_pct >= float(parameters["min_oi_change_4h_pct"]):
        score += 1
    if volume_ratio >= float(parameters["min_volume_ratio"]):
        score += 1
    if (row.get("premium_close") or 0.0) > 0 or (row.get("latest_funding_rate") or 0.0) > 0:
        score += 1
    return score


def _crowded_short_score(
    *,
    row: dict[str, Any],
    top_position_gap: float | None,
    top_account_gap: float | None,
    oi_zscore: float,
    oi_change_4h_pct: float,
    volume_ratio: float,
    parameters: dict[str, Any],
) -> int:
    score = 0
    global_crowded = row["global_account_long_short_ratio"] <= float(parameters["global_short_ratio_threshold"])
    if global_crowded:
        score += 1
    if row["taker_buy_sell_volume_ratio"] <= float(parameters["taker_sell_ratio_threshold"]):
        score += 1
    if global_crowded and top_position_gap is not None and top_position_gap >= float(parameters["min_top_position_long_share_gap"]):
        score += 1
    elif global_crowded and top_account_gap is not None and top_account_gap >= float(parameters["min_top_position_long_share_gap"]):
        score += 1
    if oi_zscore >= float(parameters["oi_z_threshold"]):
        score += 1
    if oi_change_4h_pct >= float(parameters["min_oi_change_4h_pct"]):
        score += 1
    if volume_ratio >= float(parameters["min_volume_ratio"]):
        score += 1
    if (row.get("premium_close") or 0.0) < 0 or (row.get("latest_funding_rate") or 0.0) < 0:
        score += 1
    return score


def _build_feature_cache(rows: list[dict[str, Any]], parameters: dict[str, Any]) -> dict[str, list[float | None]]:
    oi_window = int(parameters["oi_change_window_bars"])
    stats_lookback = int(parameters["stats_lookback_bars"])
    min_stats = int(parameters["min_stats_bars"])
    oi_values = [row["sum_open_interest"] for row in rows]
    oi_value_values = [row["sum_open_interest_value"] for row in rows]
    close_values = [row["close"] for row in rows]
    cache: dict[str, list[float | None]] = {
        "oi_change_1h": _pct_change_series(oi_values, 12),
        "oi_change_4h": _pct_change_series(oi_values, oi_window),
        "oi_change_12h": _pct_change_series(oi_values, 144),
        "oi_value_change_4h": _pct_change_series(oi_value_values, oi_window),
        "price_return_1h": _pct_change_series(close_values, 12),
        "price_return_4h": _pct_change_series(close_values, 48),
        "price_return_12h": _pct_change_series(close_values, 144),
    }
    cache["oi_change_4h_zscore"] = _rolling_prior_zscore(
        cache["oi_change_4h"],
        lookback=stats_lookback,
        min_count=min_stats,
    )
    cache["volume_ratio_2d_median"] = _rolling_prior_median_ratio(
        [row["vol_ccy_quote"] for row in rows],
        lookback=stats_lookback,
        min_count=min_stats,
    )
    return cache


def _aligned_rows(
    *,
    raw_5m: list[MarketDataCandle],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]],
    funding_features: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candle_rows = {
        _utc(candle.timestamp): {
            "timestamp": _utc(candle.timestamp),
            "open": float(candle.open),
            "high": float(candle.high),
            "low": float(candle.low),
            "close": float(candle.close),
            "volume": float(candle.volume),
            "vol_ccy": float(candle.vol_ccy),
            "vol_ccy_quote": float(candle.vol_ccy_quote),
            "confirm": int(candle.confirm),
        }
        for candle in raw_5m
        if int(candle.confirm) == 1
    }
    metric_rows: dict[datetime, dict[str, Any]] = {}
    for item in raw_metrics:
        if int(item.get("confirm", 1)) != 1:
            continue
        timestamp = _utc(item.get("timestamp") or item.get("ts"))
        available_at = _source_available_at(item, timestamp)
        if available_at > timestamp + BASE_TIMEFRAME_DELTA:
            continue
        metric = {
            "metric_available_at": available_at,
            "sum_open_interest": _float(item.get("sum_open_interest")),
            "sum_open_interest_value": _float(item.get("sum_open_interest_value")),
            "top_trader_account_long_short_ratio": _float(item.get("top_trader_account_long_short_ratio")),
            "top_trader_position_long_short_ratio": _float(item.get("top_trader_position_long_short_ratio")),
            "global_account_long_short_ratio": _float(item.get("global_account_long_short_ratio")),
            "taker_buy_sell_volume_ratio": _float(item.get("taker_buy_sell_volume_ratio")),
            "metrics_confirm": int(item.get("confirm", 1)),
        }
        if (
            metric["sum_open_interest"] <= 0
            or metric["sum_open_interest_value"] <= 0
            or metric["global_account_long_short_ratio"] <= 0
            or metric["taker_buy_sell_volume_ratio"] <= 0
        ):
            continue
        metric_rows[timestamp] = metric

    premium_rows: dict[datetime, dict[str, Any]] = {}
    for item in raw_premium:
        if int(item.get("confirm", 1)) != 1:
            continue
        timestamp = _utc(item.get("timestamp") or item.get("ts"))
        available_at = _source_available_at(item, timestamp)
        if available_at <= timestamp + BASE_TIMEFRAME_DELTA:
            premium_rows[timestamp] = {
                "premium_available_at": available_at,
                "premium_open": _optional_float(item.get("premium_open")),
                "premium_high": _optional_float(item.get("premium_high")),
                "premium_low": _optional_float(item.get("premium_low")),
                "premium_close": _optional_float(item.get("premium_close")),
            }

    funding_rows: dict[datetime, dict[str, Any]] = {}
    for item in funding_features:
        timestamp = _utc(item.get("timestamp") or item.get("ts"))
        available_at = _source_available_at(item, timestamp)
        if available_at <= timestamp + BASE_TIMEFRAME_DELTA:
            funding_rows[timestamp] = {
                "funding_available_at": available_at,
                "latest_funding_rate": _optional_float(item.get("latest_funding_rate")),
                "annualized_funding_rate": _optional_float(item.get("annualized_funding_rate")),
                "funding_rate_zscore_7d": _optional_float(item.get("funding_rate_zscore_7d")),
                "funding_signed_streak": _optional_float(item.get("funding_signed_streak")),
                "minutes_to_expected_funding": _optional_float(item.get("minutes_to_expected_funding")),
            }

    rows = []
    for timestamp in sorted(set(candle_rows) & set(metric_rows)):
        rows.append(
            {
                **candle_rows[timestamp],
                **metric_rows[timestamp],
                **premium_rows.get(timestamp, {}),
                **funding_rows.get(timestamp, {}),
            }
        )
    return rows


def _trim_rows_for_scan(
    *,
    rows: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
) -> list[dict[str, Any]]:
    warmup_start = _scan_warmup_start(start=start, parameters=parameters)
    scan_end = _utc(end)
    timestamps = [row["timestamp"] for row in rows]
    start_index = max(0, bisect_left(timestamps, warmup_start))
    end_index = bisect_right(timestamps, scan_end)
    return rows[start_index:end_index]


def _actual_scan_coverage_end(
    *,
    raw_5m: list[MarketDataCandle],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]],
    funding_features: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
) -> datetime | None:
    aligned_rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
    )
    if not aligned_rows:
        return None
    scan_rows = _trim_rows_for_scan(rows=aligned_rows, start=start, end=end, parameters=parameters)
    if scan_rows:
        return scan_rows[-1]["timestamp"]
    timestamps = [row["timestamp"] for row in aligned_rows]
    end_index = bisect_right(timestamps, _utc(end)) - 1
    if end_index >= 0:
        return timestamps[end_index]
    return None


def _scan_warmup_start(*, start: datetime, parameters: dict[str, Any]) -> datetime:
    context_timeframes = _context_timeframes(parameters)
    max_feature_bars = max(
        int(parameters["stats_lookback_bars"]) + int(parameters["oi_change_window_bars"]) + 2,
        int(parameters["range_lookback_bars"]) + 2,
        int(parameters["context_bars"]),
        _htf_source_lookback_bars(
            context_bars=int(parameters["context_bars"]),
            context_timeframes=context_timeframes,
        ),
    )
    return _utc(start) - max_feature_bars * BASE_TIMEFRAME_DELTA


def _bounded_candle_htf_charts(
    *,
    rows: list[dict[str, Any]],
    signal_open_ts: datetime,
    signal_available_at: datetime,
    context_bars: int,
    context_timeframes: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    charts: dict[str, dict[str, Any]] = {}
    for timeframe in context_timeframes:
        delta = _timeframe_delta(timeframe)
        chart_rows = []
        for bucket_open, source_rows in _recent_bucket_rows(
            rows=rows,
            signal_open_ts=signal_open_ts,
            timeframe=timeframe,
            context_bars=context_bars,
        ):
            bucket_close = bucket_open + delta
            if bucket_close <= signal_available_at:
                expected_count = int(delta / BASE_TIMEFRAME_DELTA)
                if len(source_rows) != expected_count:
                    continue
                chart_rows.append(_aggregate_htf_row(timeframe, bucket_open, bucket_close, source_rows, complete=True))
            elif bucket_open <= signal_open_ts < bucket_close:
                chart_rows.append(
                    _aggregate_htf_row(
                        timeframe,
                        bucket_open,
                        bucket_close,
                        source_rows,
                        complete=False,
                        partial_close_time=signal_available_at,
                    )
                )
        if chart_rows:
            charts[timeframe] = {
                "role": "context",
                "timeframe": timeframe,
                "source": "aggregated_confirmed_5m_up_to_signal",
                "columns": HTF_COLUMNS,
                "candles": chart_rows[-_htf_context_limit(timeframe=timeframe, context_bars=context_bars) :],
            }
    return charts


def _futures_metrics_htf_charts(
    *,
    rows: list[dict[str, Any]],
    signal_open_ts: datetime,
    signal_available_at: datetime,
    context_bars: int,
    context_timeframes: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    charts: dict[str, dict[str, Any]] = {}
    for timeframe in context_timeframes:
        delta = _timeframe_delta(timeframe)
        chart_rows = []
        for bucket_open, source_rows in _recent_bucket_rows(
            rows=rows,
            signal_open_ts=signal_open_ts,
            timeframe=timeframe,
            context_bars=context_bars,
        ):
            bucket_close = bucket_open + delta
            if bucket_close <= signal_available_at:
                expected_count = int(delta / BASE_TIMEFRAME_DELTA)
                if len(source_rows) != expected_count:
                    continue
                chart_rows.append(
                    _aggregate_futures_metrics_row(bucket_open, bucket_close, source_rows, complete=True)
                )
            elif bucket_open <= signal_open_ts < bucket_close:
                chart_rows.append(
                    _aggregate_futures_metrics_row(
                        bucket_open,
                        bucket_close,
                        source_rows,
                        complete=False,
                        partial_close_time=signal_available_at,
                    )
                )
        if chart_rows:
            charts[f"futures_metrics_{timeframe}"] = {
                "role": "higher_timeframe_derivatives_positioning_context",
                "timeframe": timeframe,
                "source": "aggregated_confirmed_5m_futures_metrics_up_to_signal",
                "columns": HTF_FUTURES_METRICS_COLUMNS,
                "rows": chart_rows[-_htf_context_limit(timeframe=timeframe, context_bars=context_bars) :],
            }
    return charts


def _recent_bucket_rows(
    *,
    rows: list[dict[str, Any]],
    signal_open_ts: datetime,
    timeframe: str,
    context_bars: int,
) -> list[tuple[datetime, list[dict[str, Any]]]]:
    delta = _timeframe_delta(timeframe)
    signal_bucket_open = _bucket_open(signal_open_ts, delta)
    limit = _htf_context_limit(timeframe=timeframe, context_bars=context_bars)
    earliest_bucket_open = signal_bucket_open - (delta * limit)
    bucket_rows: dict[datetime, list[dict[str, Any]]] = {}
    for row in reversed(rows):
        timestamp = row["timestamp"]
        if timestamp < earliest_bucket_open:
            break
        bucket_open = _bucket_open(timestamp, delta)
        if bucket_open > signal_open_ts:
            continue
        bucket_rows.setdefault(bucket_open, []).append(row)
    return [(bucket_open, list(reversed(source_rows))) for bucket_open, source_rows in sorted(bucket_rows.items())]


def _aggregate_futures_metrics_row(
    bucket_open: datetime,
    bucket_close: datetime,
    rows: list[dict[str, Any]],
    *,
    complete: bool,
    partial_close_time: datetime | None = None,
) -> list[Any]:
    first = rows[0]
    last = rows[-1]
    top_account_gaps = [
        _long_share_gap(
            top_ratio=row.get("top_trader_account_long_short_ratio"),
            global_ratio=row.get("global_account_long_short_ratio"),
        )
        for row in rows
    ]
    top_position_gaps = [
        _long_share_gap(
            top_ratio=row.get("top_trader_position_long_short_ratio"),
            global_ratio=row.get("global_account_long_short_ratio"),
        )
        for row in rows
    ]
    return [
        _iso_z(bucket_open),
        _iso_z(bucket_close),
        _iso_z(partial_close_time or bucket_close),
        complete,
        len(rows),
        _pct_to_str((last["sum_open_interest"] / first["sum_open_interest"] - 1.0) * 100.0 if first["sum_open_interest"] else 0.0),
        _pct_to_str(
            (last["sum_open_interest_value"] / first["sum_open_interest_value"] - 1.0) * 100.0
            if first["sum_open_interest_value"]
            else 0.0
        ),
        _number_to_str(_avg([row["global_account_long_short_ratio"] for row in rows]), places=6),
        _number_to_str(last["global_account_long_short_ratio"], places=6),
        _number_to_str(_avg([row["taker_buy_sell_volume_ratio"] for row in rows]), places=6),
        _number_to_str(last["taker_buy_sell_volume_ratio"], places=6),
        _number_to_str(_avg([row["top_trader_account_long_short_ratio"] for row in rows]), places=6),
        _number_to_str(last["top_trader_account_long_short_ratio"], places=6),
        _number_to_str(_avg([row["top_trader_position_long_short_ratio"] for row in rows]), places=6),
        _number_to_str(last["top_trader_position_long_short_ratio"], places=6),
        _optional_number_to_str(_avg_optional(top_account_gaps), places=6),
        _optional_number_to_str(top_account_gaps[-1], places=6),
        _optional_number_to_str(_avg_optional(top_position_gaps), places=6),
        _optional_number_to_str(top_position_gaps[-1], places=6),
    ]


def _futures_metrics_row(row: dict[str, Any]) -> list[Any]:
    top_account_gap = _long_share_gap(
        top_ratio=row.get("top_trader_account_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    top_position_gap = _long_share_gap(
        top_ratio=row.get("top_trader_position_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    return [
        _iso_z(row["timestamp"]),
        _number_to_str(row["sum_open_interest"], places=8),
        _number_to_str(row["sum_open_interest_value"], places=2),
        _number_to_str(row["top_trader_account_long_short_ratio"], places=6),
        _number_to_str(row["top_trader_position_long_short_ratio"], places=6),
        _number_to_str(row["global_account_long_short_ratio"], places=6),
        _number_to_str(row["taker_buy_sell_volume_ratio"], places=6),
        _optional_number_to_str(top_account_gap, places=6),
        _optional_number_to_str(top_position_gap, places=6),
        _iso_z(row["metric_available_at"]),
        int(row.get("metrics_confirm", 1)),
    ]


def _premium_row(row: dict[str, Any]) -> list[Any]:
    return [
        _iso_z(row["timestamp"]),
        _optional_number_to_str(row.get("premium_open"), places=10),
        _optional_number_to_str(row.get("premium_high"), places=10),
        _optional_number_to_str(row.get("premium_low"), places=10),
        _optional_number_to_str(row.get("premium_close"), places=10),
        _iso_z(row.get("premium_available_at") or row["timestamp"] + BASE_TIMEFRAME_DELTA),
    ]


def _funding_row(row: dict[str, Any]) -> list[Any]:
    return [
        _iso_z(row["timestamp"]),
        _optional_number_to_str(row.get("latest_funding_rate"), places=10),
        _optional_number_to_str(row.get("annualized_funding_rate"), places=6),
        _optional_number_to_str(row.get("funding_rate_zscore_7d"), places=6),
        _optional_number_to_str(row.get("funding_signed_streak"), places=0),
        _optional_number_to_str(row.get("minutes_to_expected_funding"), places=0),
        _iso_z(row.get("funding_available_at") or row["timestamp"] + BASE_TIMEFRAME_DELTA),
    ]


def _wick_share_pct(row: dict[str, Any]) -> tuple[float, float]:
    candle_range = row["high"] - row["low"]
    if candle_range <= 0:
        return 0.0, 0.0
    upper_wick = row["high"] - max(row["open"], row["close"])
    lower_wick = min(row["open"], row["close"]) - row["low"]
    return max(0.0, upper_wick / candle_range * 100.0), max(0.0, lower_wick / candle_range * 100.0)


def _long_share_gap(*, top_ratio: Any, global_ratio: Any) -> float | None:
    top_share = _long_share(top_ratio)
    global_share = _long_share(global_ratio)
    if top_share is None or global_share is None:
        return None
    return top_share - global_share


def _long_share(value: Any) -> float | None:
    ratio = _optional_float(value)
    if ratio is None or ratio < 0:
        return None
    return ratio / (1.0 + ratio)


def _with_defaults(parameters: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(parameters or {})
    merged.setdefault("range_lookback_bars", DEFAULT_RANGE_LOOKBACK_BARS)
    merged.setdefault("oi_change_window_bars", DEFAULT_OI_CHANGE_WINDOW_BARS)
    merged.setdefault("stats_lookback_bars", DEFAULT_STATS_LOOKBACK_BARS)
    merged.setdefault("min_stats_bars", DEFAULT_MIN_STATS_BARS)
    merged.setdefault("oi_z_threshold", DEFAULT_OI_Z_THRESHOLD)
    merged.setdefault("min_oi_change_4h_pct", DEFAULT_MIN_OI_CHANGE_4H_PCT)
    merged.setdefault("breakout_threshold_pct", DEFAULT_BREAKOUT_THRESHOLD_PCT)
    merged.setdefault("min_rejection_wick_pct", DEFAULT_MIN_REJECTION_WICK_PCT)
    merged.setdefault("min_volume_ratio", DEFAULT_MIN_VOLUME_RATIO)
    merged.setdefault("global_long_ratio_threshold", DEFAULT_GLOBAL_LONG_RATIO_THRESHOLD)
    merged.setdefault("global_short_ratio_threshold", DEFAULT_GLOBAL_SHORT_RATIO_THRESHOLD)
    merged.setdefault("taker_buy_ratio_threshold", DEFAULT_TAKER_BUY_RATIO_THRESHOLD)
    merged.setdefault("taker_sell_ratio_threshold", DEFAULT_TAKER_SELL_RATIO_THRESHOLD)
    merged.setdefault("max_top_position_long_share_gap", DEFAULT_MAX_TOP_POSITION_LONG_SHARE_GAP)
    merged.setdefault("min_top_position_long_share_gap", DEFAULT_MIN_TOP_POSITION_LONG_SHARE_GAP)
    merged.setdefault("min_crowding_score", DEFAULT_MIN_CROWDING_SCORE)
    merged.setdefault("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)
    merged.setdefault("context_bars", DEFAULT_CONTEXT_BARS)
    merged.setdefault("context_timeframes", list(DEFAULT_CONTEXT_TIMEFRAMES))
    return merged


def _context_timeframes_from_defaults() -> list[str]:
    return list(DEFAULT_CONTEXT_TIMEFRAMES)


def _timeframe_delta(timeframe: str) -> timedelta:
    value = timeframe.lower().strip()
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _bucket_open(timestamp: datetime, delta: timedelta) -> datetime:
    seconds = int(delta.total_seconds())
    epoch = int(_utc(timestamp).timestamp())
    return datetime.fromtimestamp(epoch // seconds * seconds, tz=UTC)


def _htf_context_limit(*, timeframe: str, context_bars: int) -> int:
    normalized = timeframe.lower().strip()
    if normalized == "1h":
        return min(context_bars, 24)
    if normalized == "4h":
        return min(context_bars, 18)
    if normalized == "12h":
        return min(context_bars, 12)
    return context_bars


def _source_available_at(row: dict[str, Any], timestamp: datetime) -> datetime:
    raw = row.get("available_at") or row.get("interval_end")
    if raw:
        return _utc(raw)
    return timestamp + BASE_TIMEFRAME_DELTA


def _latest_required_ref_end(*, repository: Any, asset: str) -> datetime | None:
    getter = getattr(repository, "get_candle_ref", None)
    if not callable(getter):
        return None
    end_times: list[datetime] = []
    for origin, data_type in (("raw", "candles"), ("raw", "futures_metrics")):
        ref = getter(asset=asset.upper(), timeframe="5m", origin=origin, data_type=data_type)
        if not isinstance(ref, dict) or not ref.get("end_ts"):
            return None
        end_times.append(_utc(ref["end_ts"]))
    return min(end_times) if end_times else None


def _optional_number_to_str(value: Any | None, *, places: int) -> str | None:
    if value is None:
        return None
    return _number_to_str(value, places=places)


def _pct_to_str(value: Any) -> str:
    return _number_to_str(value, places=6)


def _avg(values: list[float]) -> float:
    numeric = [float(value) for value in values]
    return sum(numeric) / len(numeric) if numeric else 0.0


def _avg_optional(values: list[float | None]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    return sum(numeric) / len(numeric) if numeric else None


def _float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _require_supported_asset(asset: str) -> None:
    if not asset or not asset.strip():
        raise ValueError(f"{ENGINE_ID} requires an asset")
