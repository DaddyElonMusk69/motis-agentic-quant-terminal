from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import (
    LiveSignalScanResult,
    SignalPacket,
    TrainingSignalGenerationResult,
    validate_signal_packet,
)
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.btc_derivatives_crowding_failure_v1 import (
    FUNDING_COLUMNS,
    FUTURES_METRICS_COLUMNS,
    HTF_FUTURES_METRICS_COLUMNS,
    PREMIUM_COLUMNS,
    _bounded_candle_htf_charts,
    _funding_row,
    _futures_metrics_htf_charts,
    _futures_metrics_row,
    _long_share_gap,
    _premium_row,
    _source_available_at,
)
from quant_terminal_worker.signal_engines.oi_compression_v2 import (
    BASE_TIMEFRAME_DELTA,
    CANDLE_COLUMNS,
    _candle_chart_row,
    _context_timeframes,
    _feature_to_str,
    _iso_z,
    _number_to_str,
    _optional_seed_timestamp,
    _pct_change_series,
    _price_to_str,
    _rolling_prior_median_ratio,
    _rolling_prior_percentile,
    _rolling_prior_zscore,
    _utc,
)
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


ENGINE_ID = "derivatives_participation_impulse_v1"
DEFAULT_RANGE_LOOKBACK_BARS = 96
DEFAULT_RANGE_WINDOW_BARS = 96
DEFAULT_OI_CHANGE_WINDOW_BARS = 48
DEFAULT_STATS_LOOKBACK_BARS = 576
DEFAULT_MIN_STATS_BARS = 200
DEFAULT_BREAKOUT_THRESHOLD_PCT = 0.04
DEFAULT_RANGE_PERCENTILE_THRESHOLD = 0.55
DEFAULT_MIN_OI_Z_THRESHOLD = 0.65
DEFAULT_MIN_OI_CHANGE_1H_PCT = 0.02
DEFAULT_MIN_VOLUME_RATIO = 1.05
DEFAULT_MIN_TAKER_IMBALANCE_Z = 0.35
DEFAULT_TAKER_BUY_RATIO_THRESHOLD = 1.03
DEFAULT_TAKER_SELL_RATIO_THRESHOLD = 0.97
DEFAULT_NEUTRAL_GLOBAL_RATIO_LOW = 0.92
DEFAULT_NEUTRAL_GLOBAL_RATIO_HIGH = 1.08
DEFAULT_MAX_ABS_FUNDING_ZSCORE = 2.25
DEFAULT_MAX_ABS_PREMIUM_ZSCORE = 2.25
DEFAULT_MIN_TREND_RETURN_4H_PCT = 0.25
DEFAULT_DEDUPE_WINDOW_MINUTES = 120
DEFAULT_CONTEXT_BARS = 48
DEFAULT_CONTEXT_TIMEFRAMES = ("1h", "4h")


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
    raw_oi = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="open_interest",
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
        raw_oi=raw_oi,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
        start=context.start,
        end=context.end,
        parameters=params,
    )
    packets, generated_packet_count = generate_derivatives_participation_impulse_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_oi=raw_oi,
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
    latest_end = _latest_required_ref_end(repository=context.repository, asset=context.asset)
    warmup_start = _scan_warmup_start(start=latest_end, parameters=params) if latest_end else None
    raw_5m = context.market_data_reader.get_candles(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        start=warmup_start,
        end=latest_end,
    )
    raw_oi = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="open_interest",
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
    packet = scan_derivatives_participation_impulse_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_oi=raw_oi,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium,
        funding_features=funding_features,
        parameters=params,
    )
    if packet is None:
        return LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest_confirmed_derivatives_participation_state_did_not_trigger",
        )
    return LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket.from_mapping(packet),
    )


def generate_derivatives_participation_impulse_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
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
    params = _with_defaults(parameters)
    aligned_rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_oi=raw_oi,
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


def scan_derivatives_participation_impulse_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]] | None,
    funding_features: list[dict[str, Any]] | None,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    params = _with_defaults(parameters)
    rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_oi=raw_oi,
        raw_metrics=raw_metrics,
        raw_premium=raw_premium or [],
        funding_features=funding_features or [],
    )
    if not rows:
        return None
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
            "pattern": "derivatives_participation_impulse",
            "event_type": "DERIVATIVES_PARTICIPATION_IMPULSE",
            "selected_leaf": str(features["selected_leaf"]),
            "participation_state": str(features["participation_state"]),
            "observed_resolution": str(features["observed_resolution"]),
            "trigger_timeframe": "5m",
            "signal_candle_open_ts": _iso_z(signal_open_ts),
            "signal_candle_close_ts": _iso_z(signal_close_ts),
            "signal_available_at": _iso_z(signal_available_at),
            "range_lookback_bars": int(parameters["range_lookback_bars"]),
            "range_window_bars": int(parameters["range_window_bars"]),
            "oi_change_window_bars": int(parameters["oi_change_window_bars"]),
            "stats_lookback_bars": int(parameters["stats_lookback_bars"]),
            "min_stats_bars": int(parameters["min_stats_bars"]),
            "breakout_threshold_pct": _number_to_str(parameters["breakout_threshold_pct"], places=6),
            "range_percentile_threshold": _number_to_str(parameters["range_percentile_threshold"], places=6),
            "min_oi_z_threshold": _number_to_str(parameters["min_oi_z_threshold"], places=6),
            "min_volume_ratio": _number_to_str(parameters["min_volume_ratio"], places=6),
            "min_taker_imbalance_z": _number_to_str(parameters["min_taker_imbalance_z"], places=6),
            "dedupe_window_minutes": int(parameters["dedupe_window_minutes"]),
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
                "role": "derivatives_participation_context",
                "timeframe": "5m",
                "columns": FUTURES_METRICS_COLUMNS,
                "rows": [_futures_metrics_row(item) for item in context_rows],
            },
            "premium_index_5m": {
                "role": "premium_overheat_context",
                "timeframe": "5m",
                "columns": PREMIUM_COLUMNS,
                "rows": [_premium_row(item) for item in context_rows if item.get("premium_close") is not None],
            },
            "funding_features_5m": {
                "role": "funding_overheat_context",
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

    oi_zscore = feature_cache["oi_change_zscore"][index]
    volume_ratio = feature_cache["volume_ratio_2d_median"][index]
    range_percentile = feature_cache["range_percentile"][index]
    taker_zscore = feature_cache["taker_imbalance_zscore"][index]
    premium_zscore = feature_cache["premium_zscore"][index]
    if oi_zscore is None or volume_ratio is None or range_percentile is None or taker_zscore is None:
        return None

    oi_change_1h_pct = (feature_cache["oi_change_1h"][index] or 0.0) * 100.0
    oi_change_4h_pct = (feature_cache["oi_change_4h"][index] or 0.0) * 100.0
    price_return_1h_pct = (feature_cache["price_return_1h"][index] or 0.0) * 100.0
    price_return_4h_pct = (feature_cache["price_return_4h"][index] or 0.0) * 100.0
    price_return_12h_pct = (feature_cache["price_return_12h"][index] or 0.0) * 100.0

    upside_breakout_pct = max(0.0, (row["close"] - prior_high) / prior_high * 100.0)
    downside_breakout_pct = max(0.0, (prior_low - row["close"]) / prior_low * 100.0)
    top_account_gap = _long_share_gap(
        top_ratio=row.get("top_trader_account_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    top_position_gap = _long_share_gap(
        top_ratio=row.get("top_trader_position_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    funding_zscore = row.get("funding_rate_zscore_7d") or 0.0
    premium_zscore = premium_zscore or 0.0
    overheated = (
        abs(float(funding_zscore)) > float(parameters["max_abs_funding_zscore"])
        or abs(float(premium_zscore)) > float(parameters["max_abs_premium_zscore"])
    )
    if overheated:
        return None

    common_impulse = (
        oi_zscore >= float(parameters["min_oi_z_threshold"])
        and oi_change_1h_pct >= float(parameters["min_oi_change_1h_pct"])
        and volume_ratio >= float(parameters["min_volume_ratio"])
    )
    compressed = range_percentile <= float(parameters["range_percentile_threshold"])
    neutral_positioning = (
        float(parameters["neutral_global_ratio_low"])
        <= row["global_account_long_short_ratio"]
        <= float(parameters["neutral_global_ratio_high"])
    )

    up_flow = (
        row["taker_buy_sell_volume_ratio"] >= float(parameters["taker_buy_ratio_threshold"])
        and taker_zscore >= float(parameters["min_taker_imbalance_z"])
    )
    down_flow = (
        row["taker_buy_sell_volume_ratio"] <= float(parameters["taker_sell_ratio_threshold"])
        and taker_zscore <= -float(parameters["min_taker_imbalance_z"])
    )

    up_breakout = upside_breakout_pct >= float(parameters["breakout_threshold_pct"])
    down_breakout = downside_breakout_pct >= float(parameters["breakout_threshold_pct"])
    if up_breakout and down_breakout:
        return None

    selected_leaf: str | None = None
    observed_resolution: str | None = None
    participation_state: str | None = None

    if compressed and oi_change_4h_pct > oi_change_1h_pct and common_impulse and up_breakout and up_flow:
        selected_leaf = "absorption_resolution"
        observed_resolution = "UP"
        participation_state = "compressed_oi_build_resolved_up"
    elif compressed and oi_change_4h_pct > oi_change_1h_pct and common_impulse and down_breakout and down_flow:
        selected_leaf = "absorption_resolution"
        observed_resolution = "DOWN"
        participation_state = "compressed_oi_build_resolved_down"
    elif compressed and common_impulse and up_breakout and up_flow:
        selected_leaf = "clean_participation_breakout"
        observed_resolution = "UP"
        participation_state = "fresh_leverage_confirmed_upside_resolution"
    elif compressed and common_impulse and down_breakout and down_flow:
        selected_leaf = "clean_participation_breakout"
        observed_resolution = "DOWN"
        participation_state = "fresh_leverage_confirmed_downside_resolution"
    elif common_impulse and up_flow and price_return_4h_pct >= float(parameters["min_trend_return_4h_pct"]) and price_return_1h_pct > 0:
        selected_leaf = "pullback_reacceleration"
        observed_resolution = "UP"
        participation_state = "trend_participation_reaccelerating_up"
    elif common_impulse and down_flow and price_return_4h_pct <= -float(parameters["min_trend_return_4h_pct"]) and price_return_1h_pct < 0:
        selected_leaf = "pullback_reacceleration"
        observed_resolution = "DOWN"
        participation_state = "trend_participation_reaccelerating_down"
    elif compressed and neutral_positioning and common_impulse and up_flow and price_return_1h_pct > 0:
        selected_leaf = "neutral_positioning_expansion"
        observed_resolution = "UP"
        participation_state = "early_participation_from_neutral_positioning_up"
    elif compressed and neutral_positioning and common_impulse and down_flow and price_return_1h_pct < 0:
        selected_leaf = "neutral_positioning_expansion"
        observed_resolution = "DOWN"
        participation_state = "early_participation_from_neutral_positioning_down"

    if selected_leaf is None or observed_resolution is None or participation_state is None:
        return None

    close_position_in_prior_range_pct = (row["close"] - prior_low) / prior_span * 100.0
    return {
        "selected_leaf": selected_leaf,
        "participation_state": participation_state,
        "observed_resolution": observed_resolution,
        "prior_range_high": prior_high,
        "prior_range_low": prior_low,
        "prior_range_pct": prior_span / row["close"] * 100.0,
        "range_percentile": range_percentile,
        "upside_breakout_pct": upside_breakout_pct,
        "downside_breakout_pct": downside_breakout_pct,
        "close_position_in_prior_range_pct": close_position_in_prior_range_pct,
        "oi_change_1h_pct": oi_change_1h_pct,
        "oi_change_4h_pct": oi_change_4h_pct,
        "oi_change_12h_pct": (feature_cache["oi_change_12h"][index] or 0.0) * 100.0,
        "oi_change_zscore": oi_zscore,
        "oi_value_change_4h_pct": (feature_cache["oi_value_change_4h"][index] or 0.0) * 100.0,
        "price_return_1h_pct": price_return_1h_pct,
        "price_return_4h_pct": price_return_4h_pct,
        "price_return_12h_pct": price_return_12h_pct,
        "volume_ratio_2d_median": volume_ratio,
        "taker_buy_sell_volume_ratio": row["taker_buy_sell_volume_ratio"],
        "taker_imbalance_zscore": taker_zscore,
        "global_account_long_short_ratio": row["global_account_long_short_ratio"],
        "top_trader_account_long_short_ratio": row["top_trader_account_long_short_ratio"],
        "top_trader_position_long_short_ratio": row["top_trader_position_long_short_ratio"],
        "top_trader_account_vs_global_long_share_gap": top_account_gap or 0.0,
        "top_trader_position_vs_global_long_share_gap": top_position_gap or 0.0,
        "premium_close": row.get("premium_close") or 0.0,
        "premium_zscore": premium_zscore,
        "latest_funding_rate": row.get("latest_funding_rate") or 0.0,
        "annualized_funding_rate": row.get("annualized_funding_rate") or 0.0,
        "funding_rate_zscore_7d": funding_zscore,
    }


def _build_feature_cache(rows: list[dict[str, Any]], parameters: dict[str, Any]) -> dict[str, list[float | None]]:
    oi_window = int(parameters["oi_change_window_bars"])
    range_window = int(parameters["range_window_bars"])
    stats_lookback = int(parameters["stats_lookback_bars"])
    min_stats = int(parameters["min_stats_bars"])
    oi_values = [row["sum_open_interest"] for row in rows]
    oi_value_values = [row["sum_open_interest_value"] for row in rows]
    close_values = [row["close"] for row in rows]
    taker_imbalances = [row["taker_buy_sell_volume_ratio"] - 1.0 for row in rows]
    premium_values = [row.get("premium_close") for row in rows]
    cache: dict[str, list[float | None]] = {
        "oi_change_1h": _pct_change_series(oi_values, 12),
        "oi_change_4h": _pct_change_series(oi_values, oi_window),
        "oi_change_12h": _pct_change_series(oi_values, 144),
        "oi_value_change_4h": _pct_change_series(oi_value_values, oi_window),
        "price_return_1h": _pct_change_series(close_values, 12),
        "price_return_4h": _pct_change_series(close_values, 48),
        "price_return_12h": _pct_change_series(close_values, 144),
        "range_pct": [_range_pct(rows, index, range_window) for index in range(len(rows))],
    }
    cache["oi_change_zscore"] = _rolling_prior_zscore(
        cache["oi_change_4h"],
        lookback=stats_lookback,
        min_count=min_stats,
    )
    cache["range_percentile"] = _rolling_prior_percentile(
        cache["range_pct"],
        lookback=stats_lookback,
        min_count=min_stats,
    )
    cache["volume_ratio_2d_median"] = _rolling_prior_median_ratio(
        [row["vol_ccy_quote"] for row in rows],
        lookback=stats_lookback,
        min_count=min_stats,
    )
    cache["taker_imbalance_zscore"] = _rolling_prior_zscore(
        taker_imbalances,
        lookback=stats_lookback,
        min_count=min_stats,
    )
    cache["premium_zscore"] = _rolling_prior_zscore(
        premium_values,
        lookback=stats_lookback,
        min_count=min_stats,
    )
    return cache


def _aligned_rows(
    *,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
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
    oi_rows: dict[datetime, dict[str, Any]] = {}
    for item in raw_oi:
        if int(item.get("confirm", 1)) != 1:
            continue
        timestamp = _utc(item.get("timestamp") or item.get("ts"))
        available_at = _source_available_at(item, timestamp)
        if available_at > timestamp + BASE_TIMEFRAME_DELTA:
            continue
        oi = {
            "oi_available_at": available_at,
            "sum_open_interest": _float(item.get("sum_open_interest")),
            "sum_open_interest_value": _float(item.get("sum_open_interest_value")),
            "oi_confirm": int(item.get("confirm", 1)),
        }
        if oi["sum_open_interest"] <= 0 or oi["sum_open_interest_value"] <= 0:
            continue
        oi_rows[timestamp] = oi

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
            "metrics_sum_open_interest": _float(item.get("sum_open_interest")),
            "metrics_sum_open_interest_value": _float(item.get("sum_open_interest_value")),
            "top_trader_account_long_short_ratio": _float(item.get("top_trader_account_long_short_ratio")),
            "top_trader_position_long_short_ratio": _float(item.get("top_trader_position_long_short_ratio")),
            "global_account_long_short_ratio": _float(item.get("global_account_long_short_ratio")),
            "taker_buy_sell_volume_ratio": _float(item.get("taker_buy_sell_volume_ratio")),
            "metrics_confirm": int(item.get("confirm", 1)),
        }
        if (
            metric["global_account_long_short_ratio"] <= 0
            or metric["taker_buy_sell_volume_ratio"] <= 0
            or metric["top_trader_account_long_short_ratio"] <= 0
            or metric["top_trader_position_long_short_ratio"] <= 0
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
    for timestamp in sorted(set(candle_rows) & set(oi_rows) & set(metric_rows)):
        metric = dict(metric_rows[timestamp])
        rows.append(
            {
                **candle_rows[timestamp],
                **metric,
                **oi_rows[timestamp],
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
    raw_oi: list[dict[str, Any]],
    raw_metrics: list[dict[str, Any]],
    raw_premium: list[dict[str, Any]],
    funding_features: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
) -> datetime | None:
    aligned_rows = _aligned_rows(
        raw_5m=raw_5m,
        raw_oi=raw_oi,
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
        int(parameters["stats_lookback_bars"]) + max(int(parameters["oi_change_window_bars"]), 144) + 2,
        int(parameters["stats_lookback_bars"]) + int(parameters["range_window_bars"]) + 2,
        int(parameters["range_lookback_bars"]) + 2,
        int(parameters["context_bars"]),
        _htf_source_lookback_bars(
            context_bars=int(parameters["context_bars"]),
            context_timeframes=context_timeframes,
        ),
    )
    return _utc(start) - max_feature_bars * BASE_TIMEFRAME_DELTA


def _htf_source_lookback_bars(*, context_bars: int, context_timeframes: tuple[str, ...]) -> int:
    lookback = int(context_bars)
    for timeframe in context_timeframes:
        delta = _timeframe_delta(timeframe)
        base_rows_per_bucket = int(delta / BASE_TIMEFRAME_DELTA)
        lookback = max(
            lookback,
            base_rows_per_bucket * (_htf_context_limit(timeframe=timeframe, context_bars=context_bars) + 1),
        )
    return lookback


def _htf_context_limit(*, timeframe: str, context_bars: int) -> int:
    normalized = timeframe.lower().strip()
    if normalized == "1h":
        return min(context_bars, 12)
    if normalized == "4h":
        return min(context_bars, 8)
    if normalized == "12h":
        return min(context_bars, 4)
    return min(context_bars, 8)


def _timeframe_delta(timeframe: str) -> timedelta:
    value = timeframe.lower().strip()
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _range_pct(rows: list[dict[str, Any]], index: int, window: int) -> float | None:
    if index < window:
        return None
    source = rows[index - window : index]
    high = max(row["high"] for row in source)
    low = min(row["low"] for row in source)
    close = rows[index]["close"]
    return (high - low) / close if close > 0 else None


def _with_defaults(parameters: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(parameters or {})
    merged.setdefault("range_lookback_bars", DEFAULT_RANGE_LOOKBACK_BARS)
    merged.setdefault("range_window_bars", DEFAULT_RANGE_WINDOW_BARS)
    merged.setdefault("oi_change_window_bars", DEFAULT_OI_CHANGE_WINDOW_BARS)
    merged.setdefault("stats_lookback_bars", DEFAULT_STATS_LOOKBACK_BARS)
    merged.setdefault("min_stats_bars", DEFAULT_MIN_STATS_BARS)
    merged.setdefault("breakout_threshold_pct", DEFAULT_BREAKOUT_THRESHOLD_PCT)
    merged.setdefault("range_percentile_threshold", DEFAULT_RANGE_PERCENTILE_THRESHOLD)
    merged.setdefault("min_oi_z_threshold", DEFAULT_MIN_OI_Z_THRESHOLD)
    merged.setdefault("min_oi_change_1h_pct", DEFAULT_MIN_OI_CHANGE_1H_PCT)
    merged.setdefault("min_volume_ratio", DEFAULT_MIN_VOLUME_RATIO)
    merged.setdefault("min_taker_imbalance_z", DEFAULT_MIN_TAKER_IMBALANCE_Z)
    merged.setdefault("taker_buy_ratio_threshold", DEFAULT_TAKER_BUY_RATIO_THRESHOLD)
    merged.setdefault("taker_sell_ratio_threshold", DEFAULT_TAKER_SELL_RATIO_THRESHOLD)
    merged.setdefault("neutral_global_ratio_low", DEFAULT_NEUTRAL_GLOBAL_RATIO_LOW)
    merged.setdefault("neutral_global_ratio_high", DEFAULT_NEUTRAL_GLOBAL_RATIO_HIGH)
    merged.setdefault("max_abs_funding_zscore", DEFAULT_MAX_ABS_FUNDING_ZSCORE)
    merged.setdefault("max_abs_premium_zscore", DEFAULT_MAX_ABS_PREMIUM_ZSCORE)
    merged.setdefault("min_trend_return_4h_pct", DEFAULT_MIN_TREND_RETURN_4H_PCT)
    merged.setdefault("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)
    merged.setdefault("context_bars", DEFAULT_CONTEXT_BARS)
    merged.setdefault("context_timeframes", list(DEFAULT_CONTEXT_TIMEFRAMES))
    return merged


def _latest_required_ref_end(*, repository: Any, asset: str) -> datetime | None:
    getter = getattr(repository, "get_candle_ref", None)
    if not callable(getter):
        return None
    end_times: list[datetime] = []
    for origin, data_type in (
        ("raw", "candles"),
        ("raw", "open_interest"),
        ("raw", "futures_metrics"),
        ("raw", "premium_index"),
        ("derived", "funding_features"),
    ):
        ref = getter(asset=asset.upper(), timeframe="5m", origin=origin, data_type=data_type)
        if not isinstance(ref, dict) or not ref.get("end_ts"):
            return None
        end_times.append(_utc(ref["end_ts"]))
    return min(end_times) if end_times else None


def _float(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    return float(value)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
