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
    PREMIUM_COLUMNS,
    _bounded_candle_htf_charts,
    _funding_row,
    _futures_metrics_htf_charts,
    _futures_metrics_row,
    _premium_row,
)
from quant_terminal_worker.signal_engines.derivatives_participation_impulse_v1 import (
    _actual_scan_coverage_end,
    _aligned_rows,
    _build_feature_cache,
    _latest_required_ref_end,
    _scan_warmup_start,
    _trim_rows_for_scan,
)
from quant_terminal_worker.signal_engines.leverage_reset_reacceleration_v1 import (
    _htf_source_lookback_bars,
    _long_share_gap,
    _optional_seed_timestamp,
    _utc,
)
from quant_terminal_worker.signal_engines.oi_compression_v2 import (
    BASE_TIMEFRAME_DELTA,
    CANDLE_COLUMNS,
    _candle_chart_row,
    _context_timeframes,
    _feature_to_str,
    _iso_z,
    _number_to_str,
    _price_to_str,
)
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


ENGINE_ID = "derivatives_basis_participation_regime_v1"
DEFAULT_RANGE_LOOKBACK_BARS = 96
DEFAULT_RANGE_WINDOW_BARS = 96
DEFAULT_OI_CHANGE_WINDOW_BARS = 48
DEFAULT_STATS_LOOKBACK_BARS = 576
DEFAULT_MIN_STATS_BARS = 160
DEFAULT_MIN_TREND_RETURN_4H_PCT = 0.20
DEFAULT_MIN_SHORT_RETURN_1H_PCT = 0.03
DEFAULT_MIN_OI_CHANGE_4H_PCT = 0.02
DEFAULT_MIN_VOLUME_RATIO = 0.90
DEFAULT_MIN_TAKER_IMBALANCE_Z = 0.03
DEFAULT_TAKER_BUY_RATIO_THRESHOLD = 1.005
DEFAULT_TAKER_SELL_RATIO_THRESHOLD = 0.995
DEFAULT_MAX_HEALTHY_FUNDING_ZSCORE = 1.75
DEFAULT_MAX_HEALTHY_PREMIUM_ZSCORE = 1.75
DEFAULT_EXTREME_FUNDING_ZSCORE = 1.85
DEFAULT_EXTREME_PREMIUM_ZSCORE = 1.85
DEFAULT_CROWDED_GLOBAL_LONG_RATIO = 1.15
DEFAULT_CROWDED_GLOBAL_SHORT_RATIO = 0.87
DEFAULT_HEALTHY_GLOBAL_RATIO_LOW = 0.78
DEFAULT_HEALTHY_GLOBAL_RATIO_HIGH = 1.28
DEFAULT_MAX_FADE_OI_CHANGE_4H_PCT = 0.08
DEFAULT_MIN_FADE_REVERSAL_1H_PCT = 0.04
DEFAULT_MIN_STRESS_PRICE_RETURN_4H_PCT = 0.45
DEFAULT_MIN_STRESS_OI_CHANGE_4H_PCT = 0.08
DEFAULT_DEDUPE_WINDOW_MINUTES = 120
DEFAULT_CONTEXT_BARS = 72
DEFAULT_CONTEXT_TIMEFRAMES = ("1h", "4h", "12h")


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
    packets, generated_packet_count = generate_derivatives_basis_participation_regime_packets(
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
    raw_5m = context.market_data_reader.get_candles(asset=context.asset, timeframe="5m", origin="raw", start=warmup_start, end=latest_end)
    raw_oi = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="raw", data_type="open_interest", start=warmup_start, end=latest_end)
    raw_metrics = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="raw", data_type="futures_metrics", start=warmup_start, end=latest_end)
    raw_premium = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="raw", data_type="premium_index", start=warmup_start, end=latest_end)
    funding_features = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="derived", data_type="funding_features", start=warmup_start, end=latest_end)
    packet = scan_derivatives_basis_participation_regime_latest(
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
            reason="latest_confirmed_derivatives_basis_participation_regime_did_not_trigger",
        )
    return LiveSignalScanResult(status="fresh_signal", source="live_parquet_snapshot", signal=SignalPacket.from_mapping(packet))


def generate_derivatives_basis_participation_regime_packets(
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
    aligned_rows = _aligned_rows(raw_5m=raw_5m, raw_oi=raw_oi, raw_metrics=raw_metrics, raw_premium=raw_premium or [], funding_features=funding_features or [])
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
        packet = _scan_index(asset=asset, instrument=instrument, rows=aligned_rows, index=index, parameters=params, feature_cache=feature_cache)
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


def scan_derivatives_basis_participation_regime_latest(
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
    rows = _aligned_rows(raw_5m=raw_5m, raw_oi=raw_oi, raw_metrics=raw_metrics, raw_premium=raw_premium or [], funding_features=funding_features or [])
    if not rows:
        return None
    latest_timestamp = rows[-1]["timestamp"]
    rows = _trim_rows_for_scan(rows=rows, start=latest_timestamp, end=latest_timestamp, parameters=params)
    if not rows:
        return None
    return _scan_index(asset=asset, instrument=instrument, rows=rows, index=len(rows) - 1, parameters=params, feature_cache=_build_feature_cache(rows, params))


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
    context_rows = rows[max(0, index - context_bars + 1) : index + 1]
    signal_open_ts = row["timestamp"]
    signal_close_ts = signal_open_ts + BASE_TIMEFRAME_DELTA
    signal_available_at = signal_close_ts
    numeric_features = {key: value for key, value in features.items() if isinstance(value, (float, int)) and not isinstance(value, bool)}
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset.upper(),
        "instrument": instrument,
        "timestamp": _iso_z(signal_open_ts),
        "active_timeframes": ["5m", *_context_timeframes(parameters)],
        "evidence": {
            "engine": ENGINE_ID,
            "pattern": "derivatives_basis_participation_regime",
            "event_type": "DERIVATIVES_BASIS_PARTICIPATION_REGIME",
            "selected_leaf": str(features["selected_leaf"]),
            "regime_state": str(features["regime_state"]),
            "observed_regime_bias": str(features["observed_regime_bias"]),
            "basis_state": str(features["basis_state"]),
            "positioning_state": str(features["positioning_state"]),
            "trigger_timeframe": "5m",
            "signal_candle_open_ts": _iso_z(signal_open_ts),
            "signal_candle_close_ts": _iso_z(signal_close_ts),
            "signal_available_at": _iso_z(signal_available_at),
            "dedupe_window_minutes": int(parameters["dedupe_window_minutes"]),
            "min_trend_return_4h_pct": _number_to_str(parameters["min_trend_return_4h_pct"], places=6),
            "min_oi_change_4h_pct": _number_to_str(parameters["min_oi_change_4h_pct"], places=6),
            "min_volume_ratio": _number_to_str(parameters["min_volume_ratio"], places=6),
            "reference_price": _price_to_str(row["close"]),
            "trigger_candle_close": _price_to_str(row["close"]),
            **{key: _feature_to_str(key, value) for key, value in numeric_features.items()},
        },
        "charts": {
            "5m": {"role": "trigger_context", "timeframe": "5m", "columns": CANDLE_COLUMNS, "candles": [_candle_chart_row(item) for item in context_rows]},
            "futures_metrics_5m": {"role": "basis_participation_context", "timeframe": "5m", "columns": FUTURES_METRICS_COLUMNS, "rows": [_futures_metrics_row(item) for item in context_rows]},
            "premium_index_5m": {"role": "basis_context", "timeframe": "5m", "columns": PREMIUM_COLUMNS, "rows": [_premium_row(item) for item in context_rows if item.get("premium_close") is not None]},
            "funding_features_5m": {"role": "funding_context", "timeframe": "5m", "columns": FUNDING_COLUMNS, "rows": [_funding_row(item) for item in context_rows if item.get("latest_funding_rate") is not None]},
        },
    }
    context_timeframes = _context_timeframes(parameters)
    htf_source_rows = rows[max(0, index - _htf_source_lookback_bars(context_bars=context_bars, context_timeframes=context_timeframes) + 1) : index + 1]
    packet["charts"].update(_bounded_candle_htf_charts(rows=htf_source_rows, signal_open_ts=signal_open_ts, signal_available_at=signal_available_at, context_bars=context_bars, context_timeframes=context_timeframes))
    packet["charts"].update(_futures_metrics_htf_charts(rows=htf_source_rows, signal_open_ts=signal_open_ts, signal_available_at=signal_available_at, context_bars=context_bars, context_timeframes=context_timeframes))
    validate_signal_packet(packet)
    return packet


def _event_features(
    *,
    rows: list[dict[str, Any]],
    index: int,
    parameters: dict[str, Any],
    feature_cache: dict[str, list[float | None]],
) -> dict[str, Any] | None:
    if index < max(int(parameters["min_stats_bars"]), 144):
        return None
    row = rows[index]
    price_return_1h_pct = (feature_cache["price_return_1h"][index] or 0.0) * 100.0
    price_return_4h_pct = (feature_cache["price_return_4h"][index] or 0.0) * 100.0
    price_return_12h_pct = (feature_cache["price_return_12h"][index] or 0.0) * 100.0
    oi_change_1h_pct = (feature_cache["oi_change_1h"][index] or 0.0) * 100.0
    oi_change_4h_pct = (feature_cache["oi_change_4h"][index] or 0.0) * 100.0
    oi_change_12h_pct = (feature_cache["oi_change_12h"][index] or 0.0) * 100.0
    volume_ratio = feature_cache["volume_ratio_2d_median"][index]
    taker_zscore = feature_cache["taker_imbalance_zscore"][index]
    premium_zscore = feature_cache["premium_zscore"][index] or 0.0
    funding_zscore = row.get("funding_rate_zscore_7d") or 0.0
    if volume_ratio is None or taker_zscore is None:
        return None
    top_account_gap = _long_share_gap(top_ratio=row.get("top_trader_account_long_short_ratio"), global_ratio=row.get("global_account_long_short_ratio"))
    top_position_gap = _long_share_gap(top_ratio=row.get("top_trader_position_long_short_ratio"), global_ratio=row.get("global_account_long_short_ratio"))

    common = {
        "price_return_1h_pct": price_return_1h_pct,
        "price_return_4h_pct": price_return_4h_pct,
        "price_return_12h_pct": price_return_12h_pct,
        "oi_change_1h_pct": oi_change_1h_pct,
        "oi_change_4h_pct": oi_change_4h_pct,
        "oi_change_12h_pct": oi_change_12h_pct,
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
    selected = _healthy_continuation(row=row, parameters=parameters, common=common)
    if selected is None:
        selected = _crowded_fade(row=row, parameters=parameters, common=common)
    if selected is None:
        selected = _leverage_stress_continuation(row=row, parameters=parameters, common=common)
    return {**selected, **common} if selected is not None else None


def _healthy_continuation(*, row: dict[str, Any], parameters: dict[str, Any], common: dict[str, Any]) -> dict[str, Any] | None:
    if abs(float(common["funding_rate_zscore_7d"])) > float(parameters["max_healthy_funding_zscore"]):
        return None
    if abs(float(common["premium_zscore"])) > float(parameters["max_healthy_premium_zscore"]):
        return None
    if not float(parameters["healthy_global_ratio_low"]) <= row["global_account_long_short_ratio"] <= float(parameters["healthy_global_ratio_high"]):
        return None
    if common["oi_change_4h_pct"] < float(parameters["min_oi_change_4h_pct"]):
        return None
    if common["volume_ratio_2d_median"] < float(parameters["min_volume_ratio"]):
        return None
    if common["price_return_4h_pct"] >= float(parameters["min_trend_return_4h_pct"]) and common["price_return_1h_pct"] >= float(parameters["min_short_return_1h_pct"]):
        if row["taker_buy_sell_volume_ratio"] >= float(parameters["taker_buy_ratio_threshold"]) and common["taker_imbalance_zscore"] >= float(parameters["min_taker_imbalance_z"]):
            return {"selected_leaf": "healthy_continuation", "regime_state": "price_volume_oi_participation_up", "observed_regime_bias": "UP", "basis_state": "constructive_not_overheated", "positioning_state": "participation_not_extreme"}
    if common["price_return_4h_pct"] <= -float(parameters["min_trend_return_4h_pct"]) and common["price_return_1h_pct"] <= -float(parameters["min_short_return_1h_pct"]):
        if row["taker_buy_sell_volume_ratio"] <= float(parameters["taker_sell_ratio_threshold"]) and common["taker_imbalance_zscore"] <= -float(parameters["min_taker_imbalance_z"]):
            return {"selected_leaf": "healthy_continuation", "regime_state": "price_volume_oi_participation_down", "observed_regime_bias": "DOWN", "basis_state": "constructive_not_overheated", "positioning_state": "participation_not_extreme"}
    return None


def _crowded_fade(*, row: dict[str, Any], parameters: dict[str, Any], common: dict[str, Any]) -> dict[str, Any] | None:
    if common["volume_ratio_2d_median"] < float(parameters["min_volume_ratio"]):
        return None
    oi_stalled = common["oi_change_4h_pct"] <= float(parameters["max_fade_oi_change_4h_pct"])
    crowded_long = (
        common["funding_rate_zscore_7d"] >= float(parameters["extreme_funding_zscore"])
        or common["premium_zscore"] >= float(parameters["extreme_premium_zscore"])
        or row["global_account_long_short_ratio"] >= float(parameters["crowded_global_long_ratio"])
    )
    if crowded_long and oi_stalled and common["price_return_1h_pct"] <= -float(parameters["min_fade_reversal_1h_pct"]) and common["taker_imbalance_zscore"] <= -float(parameters["min_taker_imbalance_z"]):
        return {"selected_leaf": "crowded_basis_fade", "regime_state": "overheated_longs_losing_efficiency", "observed_regime_bias": "DOWN", "basis_state": "positive_dislocation_fading", "positioning_state": "long_crowding"}
    crowded_short = (
        common["funding_rate_zscore_7d"] <= -float(parameters["extreme_funding_zscore"])
        or common["premium_zscore"] <= -float(parameters["extreme_premium_zscore"])
        or row["global_account_long_short_ratio"] <= float(parameters["crowded_global_short_ratio"])
    )
    if crowded_short and oi_stalled and common["price_return_1h_pct"] >= float(parameters["min_fade_reversal_1h_pct"]) and common["taker_imbalance_zscore"] >= float(parameters["min_taker_imbalance_z"]):
        return {"selected_leaf": "crowded_basis_fade", "regime_state": "overheated_shorts_losing_efficiency", "observed_regime_bias": "UP", "basis_state": "negative_dislocation_fading", "positioning_state": "short_crowding"}
    return None


def _leverage_stress_continuation(*, row: dict[str, Any], parameters: dict[str, Any], common: dict[str, Any]) -> dict[str, Any] | None:
    if common["oi_change_4h_pct"] < float(parameters["min_stress_oi_change_4h_pct"]):
        return None
    if common["volume_ratio_2d_median"] < float(parameters["min_volume_ratio"]):
        return None
    if common["price_return_4h_pct"] <= -float(parameters["min_stress_price_return_4h_pct"]) and common["taker_imbalance_zscore"] <= -float(parameters["min_taker_imbalance_z"]):
        return {"selected_leaf": "leverage_stress_continuation", "regime_state": "risk_off_derivatives_pressure_down", "observed_regime_bias": "DOWN", "basis_state": "stress_aligned", "positioning_state": "leverage_expanding"}
    if common["price_return_4h_pct"] >= float(parameters["min_stress_price_return_4h_pct"]) and common["taker_imbalance_zscore"] >= float(parameters["min_taker_imbalance_z"]):
        return {"selected_leaf": "leverage_stress_continuation", "regime_state": "squeeze_derivatives_pressure_up", "observed_regime_bias": "UP", "basis_state": "stress_aligned", "positioning_state": "leverage_expanding"}
    return None


def _with_defaults(parameters: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(parameters or {})
    merged.setdefault("range_lookback_bars", DEFAULT_RANGE_LOOKBACK_BARS)
    merged.setdefault("range_window_bars", DEFAULT_RANGE_WINDOW_BARS)
    merged.setdefault("oi_change_window_bars", DEFAULT_OI_CHANGE_WINDOW_BARS)
    merged.setdefault("stats_lookback_bars", DEFAULT_STATS_LOOKBACK_BARS)
    merged.setdefault("min_stats_bars", DEFAULT_MIN_STATS_BARS)
    merged.setdefault("min_trend_return_4h_pct", DEFAULT_MIN_TREND_RETURN_4H_PCT)
    merged.setdefault("min_short_return_1h_pct", DEFAULT_MIN_SHORT_RETURN_1H_PCT)
    merged.setdefault("min_oi_change_4h_pct", DEFAULT_MIN_OI_CHANGE_4H_PCT)
    merged.setdefault("min_volume_ratio", DEFAULT_MIN_VOLUME_RATIO)
    merged.setdefault("min_taker_imbalance_z", DEFAULT_MIN_TAKER_IMBALANCE_Z)
    merged.setdefault("taker_buy_ratio_threshold", DEFAULT_TAKER_BUY_RATIO_THRESHOLD)
    merged.setdefault("taker_sell_ratio_threshold", DEFAULT_TAKER_SELL_RATIO_THRESHOLD)
    merged.setdefault("max_healthy_funding_zscore", DEFAULT_MAX_HEALTHY_FUNDING_ZSCORE)
    merged.setdefault("max_healthy_premium_zscore", DEFAULT_MAX_HEALTHY_PREMIUM_ZSCORE)
    merged.setdefault("extreme_funding_zscore", DEFAULT_EXTREME_FUNDING_ZSCORE)
    merged.setdefault("extreme_premium_zscore", DEFAULT_EXTREME_PREMIUM_ZSCORE)
    merged.setdefault("crowded_global_long_ratio", DEFAULT_CROWDED_GLOBAL_LONG_RATIO)
    merged.setdefault("crowded_global_short_ratio", DEFAULT_CROWDED_GLOBAL_SHORT_RATIO)
    merged.setdefault("healthy_global_ratio_low", DEFAULT_HEALTHY_GLOBAL_RATIO_LOW)
    merged.setdefault("healthy_global_ratio_high", DEFAULT_HEALTHY_GLOBAL_RATIO_HIGH)
    merged.setdefault("max_fade_oi_change_4h_pct", DEFAULT_MAX_FADE_OI_CHANGE_4H_PCT)
    merged.setdefault("min_fade_reversal_1h_pct", DEFAULT_MIN_FADE_REVERSAL_1H_PCT)
    merged.setdefault("min_stress_price_return_4h_pct", DEFAULT_MIN_STRESS_PRICE_RETURN_4H_PCT)
    merged.setdefault("min_stress_oi_change_4h_pct", DEFAULT_MIN_STRESS_OI_CHANGE_4H_PCT)
    merged.setdefault("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)
    merged.setdefault("context_bars", DEFAULT_CONTEXT_BARS)
    merged.setdefault("context_timeframes", list(DEFAULT_CONTEXT_TIMEFRAMES))
    return merged
