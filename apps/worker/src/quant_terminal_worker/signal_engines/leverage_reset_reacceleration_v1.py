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
from quant_terminal_worker.signal_engines.btc_derivatives_crowding_failure_v1 import (
    FUNDING_COLUMNS,
    FUTURES_METRICS_COLUMNS,
    HTF_FUTURES_METRICS_COLUMNS,
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


ENGINE_ID = "leverage_reset_reacceleration_v1"
DEFAULT_RANGE_LOOKBACK_BARS = 96
DEFAULT_RANGE_WINDOW_BARS = 96
DEFAULT_OI_CHANGE_WINDOW_BARS = 48
DEFAULT_STATS_LOOKBACK_BARS = 576
DEFAULT_MIN_STATS_BARS = 200
DEFAULT_RESET_LOOKBACK_BARS = 48
DEFAULT_MIN_RESET_AGE_BARS = 2
DEFAULT_MAX_RESET_AGE_BARS = 48
DEFAULT_MIN_FLUSH_EXTENSION_PCT = 0.12
DEFAULT_MIN_FLUSH_CANDLE_RANGE_PCT = 0.45
DEFAULT_MIN_RESET_OI_DROP_1H_PCT = 0.06
DEFAULT_MIN_RESET_VOLUME_RATIO = 1.15
DEFAULT_MIN_RECLAIM_FROM_EXTREME_PCT = 0.18
DEFAULT_MIN_OI_REBUILD_1H_PCT = 0.02
DEFAULT_MIN_VOLUME_RATIO = 1.00
DEFAULT_MIN_TAKER_IMBALANCE_Z = 0.20
DEFAULT_TAKER_BUY_RATIO_THRESHOLD = 1.02
DEFAULT_TAKER_SELL_RATIO_THRESHOLD = 0.98
DEFAULT_MAX_ABS_FUNDING_ZSCORE = 2.10
DEFAULT_MAX_ABS_PREMIUM_ZSCORE = 2.10
DEFAULT_POST_RESET_GLOBAL_RATIO_LOW = 0.85
DEFAULT_POST_RESET_GLOBAL_RATIO_HIGH = 1.18
DEFAULT_DEDUPE_WINDOW_MINUTES = 180
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
    packets, generated_packet_count = generate_leverage_reset_reacceleration_packets(
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
    packet = scan_leverage_reset_reacceleration_latest(
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
            reason="latest_confirmed_leverage_reset_reacceleration_state_did_not_trigger",
        )
    return LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket.from_mapping(packet),
    )


def generate_leverage_reset_reacceleration_packets(
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


def scan_leverage_reset_reacceleration_latest(
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
            "pattern": "leverage_reset_reacceleration",
            "event_type": "LEVERAGE_RESET_REACCELERATION",
            "event_subtype": str(features["event_subtype"]),
            "reset_state": str(features["reset_state"]),
            "observed_reacceleration": str(features["observed_reacceleration"]),
            "trigger_timeframe": "5m",
            "signal_candle_open_ts": _iso_z(signal_open_ts),
            "signal_candle_close_ts": _iso_z(signal_close_ts),
            "signal_available_at": _iso_z(signal_available_at),
            "range_lookback_bars": int(parameters["range_lookback_bars"]),
            "reset_lookback_bars": int(parameters["reset_lookback_bars"]),
            "stats_lookback_bars": int(parameters["stats_lookback_bars"]),
            "min_stats_bars": int(parameters["min_stats_bars"]),
            "min_flush_extension_pct": _number_to_str(parameters["min_flush_extension_pct"], places=6),
            "min_flush_candle_range_pct": _number_to_str(parameters["min_flush_candle_range_pct"], places=6),
            "min_reset_oi_drop_1h_pct": _number_to_str(parameters["min_reset_oi_drop_1h_pct"], places=6),
            "min_reclaim_from_extreme_pct": _number_to_str(parameters["min_reclaim_from_extreme_pct"], places=6),
            "min_oi_rebuild_1h_pct": _number_to_str(parameters["min_oi_rebuild_1h_pct"], places=6),
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
                "role": "post_reset_derivatives_context",
                "timeframe": "5m",
                "columns": FUTURES_METRICS_COLUMNS,
                "rows": [_futures_metrics_row(item) for item in context_rows],
            },
            "premium_index_5m": {
                "role": "post_reset_premium_context",
                "timeframe": "5m",
                "columns": PREMIUM_COLUMNS,
                "rows": [_premium_row(item) for item in context_rows if item.get("premium_close") is not None],
            },
            "funding_features_5m": {
                "role": "post_reset_funding_context",
                "timeframe": "5m",
                "columns": FUNDING_COLUMNS,
                "rows": [_funding_row(item) for item in context_rows if item.get("latest_funding_rate") is not None],
            },
        },
    }
    context_timeframes = _context_timeframes(parameters)
    htf_source_start = max(0, index - _htf_source_lookback_bars(context_bars=context_bars, context_timeframes=context_timeframes) + 1)
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
    reset_lookback = int(parameters["reset_lookback_bars"])
    if index < max(min_stats, range_lookback, reset_lookback, 144):
        return None

    row = rows[index]
    volume_ratio = feature_cache["volume_ratio_2d_median"][index]
    taker_zscore = feature_cache["taker_imbalance_zscore"][index]
    premium_zscore = feature_cache["premium_zscore"][index]
    oi_rebuild_1h_pct = (feature_cache["oi_change_1h"][index] or 0.0) * 100.0
    oi_change_4h_pct = (feature_cache["oi_change_4h"][index] or 0.0) * 100.0
    price_return_1h_pct = (feature_cache["price_return_1h"][index] or 0.0) * 100.0
    price_return_4h_pct = (feature_cache["price_return_4h"][index] or 0.0) * 100.0
    if volume_ratio is None or taker_zscore is None:
        return None
    funding_zscore = row.get("funding_rate_zscore_7d") or 0.0
    premium_zscore = premium_zscore or 0.0
    if abs(float(funding_zscore)) > float(parameters["max_abs_funding_zscore"]):
        return None
    if abs(float(premium_zscore)) > float(parameters["max_abs_premium_zscore"]):
        return None
    if not (
        float(parameters["post_reset_global_ratio_low"])
        <= row["global_account_long_short_ratio"]
        <= float(parameters["post_reset_global_ratio_high"])
    ):
        return None
    if oi_rebuild_1h_pct < float(parameters["min_oi_rebuild_1h_pct"]):
        return None
    if volume_ratio < float(parameters["min_volume_ratio"]):
        return None

    long_anchor = _best_reset_anchor(rows=rows, index=index, parameters=parameters, feature_cache=feature_cache, mode="downside")
    short_anchor = _best_reset_anchor(rows=rows, index=index, parameters=parameters, feature_cache=feature_cache, mode="upside")

    selected: dict[str, Any] | None = None
    if long_anchor is not None:
        reclaim_pct = (row["close"] - long_anchor["extreme_price"]) / long_anchor["extreme_price"] * 100.0
        if (
            reclaim_pct >= float(parameters["min_reclaim_from_extreme_pct"])
            and price_return_1h_pct > 0
            and row["taker_buy_sell_volume_ratio"] >= float(parameters["taker_buy_ratio_threshold"])
            and taker_zscore >= float(parameters["min_taker_imbalance_z"])
        ):
            selected = {
                **long_anchor,
                "event_subtype": "downside_leverage_reset_reaccelerating_up",
                "reset_state": "downside_flush_leverage_cleared",
                "observed_reacceleration": "UP",
                "reclaim_from_extreme_pct": reclaim_pct,
            }

    if short_anchor is not None:
        reclaim_pct = (short_anchor["extreme_price"] - row["close"]) / short_anchor["extreme_price"] * 100.0
        if (
            reclaim_pct >= float(parameters["min_reclaim_from_extreme_pct"])
            and price_return_1h_pct < 0
            and row["taker_buy_sell_volume_ratio"] <= float(parameters["taker_sell_ratio_threshold"])
            and taker_zscore <= -float(parameters["min_taker_imbalance_z"])
        ):
            short_selected = {
                **short_anchor,
                "event_subtype": "upside_leverage_reset_reaccelerating_down",
                "reset_state": "upside_flush_leverage_cleared",
                "observed_reacceleration": "DOWN",
                "reclaim_from_extreme_pct": reclaim_pct,
            }
            if selected is None or short_selected["reset_score"] > selected["reset_score"]:
                selected = short_selected

    if selected is None:
        return None
    anchor_row = rows[int(selected["reset_index"])]
    top_account_gap = _long_share_gap(
        top_ratio=row.get("top_trader_account_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    top_position_gap = _long_share_gap(
        top_ratio=row.get("top_trader_position_long_short_ratio"),
        global_ratio=row.get("global_account_long_short_ratio"),
    )
    return {
        "event_subtype": selected["event_subtype"],
        "reset_state": selected["reset_state"],
        "observed_reacceleration": selected["observed_reacceleration"],
        "reset_age_bars": index - int(selected["reset_index"]),
        "reset_score": selected["reset_score"],
        "reset_flush_extension_pct": selected["flush_extension_pct"],
        "reset_candle_range_pct": selected["flush_candle_range_pct"],
        "reset_oi_drop_1h_pct": selected["reset_oi_drop_1h_pct"],
        "reset_volume_ratio": selected["reset_volume_ratio"],
        "reclaim_from_extreme_pct": selected["reclaim_from_extreme_pct"],
        "reset_extreme_price": selected["extreme_price"],
        "reset_candle_close": anchor_row["close"],
        "oi_rebuild_1h_pct": oi_rebuild_1h_pct,
        "oi_change_4h_pct": oi_change_4h_pct,
        "price_return_1h_pct": price_return_1h_pct,
        "price_return_4h_pct": price_return_4h_pct,
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


def _best_reset_anchor(
    *,
    rows: list[dict[str, Any]],
    index: int,
    parameters: dict[str, Any],
    feature_cache: dict[str, list[float | None]],
    mode: str,
) -> dict[str, Any] | None:
    min_age = int(parameters["min_reset_age_bars"])
    max_age = int(parameters["max_reset_age_bars"])
    range_lookback = int(parameters["range_lookback_bars"])
    start = max(range_lookback, index - max_age)
    end = max(range_lookback, index - min_age)
    best: dict[str, Any] | None = None
    for anchor_index in range(start, end + 1):
        anchor = rows[anchor_index]
        prior = rows[anchor_index - range_lookback : anchor_index]
        if not prior:
            continue
        prior_high = max(item["high"] for item in prior)
        prior_low = min(item["low"] for item in prior)
        if prior_high <= 0 or prior_low <= 0 or anchor["close"] <= 0:
            continue
        if mode == "downside":
            flush_extension_pct = max(0.0, (prior_low - anchor["low"]) / prior_low * 100.0)
            extreme_price = anchor["low"]
            price_impulse_ok = (feature_cache["price_return_1h"][anchor_index] or 0.0) < 0
        else:
            flush_extension_pct = max(0.0, (anchor["high"] - prior_high) / prior_high * 100.0)
            extreme_price = anchor["high"]
            price_impulse_ok = (feature_cache["price_return_1h"][anchor_index] or 0.0) > 0
        flush_candle_range_pct = (anchor["high"] - anchor["low"]) / anchor["close"] * 100.0
        reset_oi_drop_1h_pct = (feature_cache["oi_change_1h"][anchor_index] or 0.0) * 100.0
        reset_volume_ratio = feature_cache["volume_ratio_2d_median"][anchor_index] or 0.0
        if not price_impulse_ok:
            continue
        if flush_extension_pct < float(parameters["min_flush_extension_pct"]):
            continue
        if flush_candle_range_pct < float(parameters["min_flush_candle_range_pct"]):
            continue
        if reset_oi_drop_1h_pct > -float(parameters["min_reset_oi_drop_1h_pct"]):
            continue
        if reset_volume_ratio < float(parameters["min_reset_volume_ratio"]):
            continue
        reset_score = (
            flush_extension_pct
            + flush_candle_range_pct
            + abs(reset_oi_drop_1h_pct)
            + reset_volume_ratio
        )
        candidate = {
            "reset_index": anchor_index,
            "reset_score": reset_score,
            "flush_extension_pct": flush_extension_pct,
            "flush_candle_range_pct": flush_candle_range_pct,
            "reset_oi_drop_1h_pct": reset_oi_drop_1h_pct,
            "reset_volume_ratio": reset_volume_ratio,
            "extreme_price": extreme_price,
        }
        if best is None or candidate["reset_score"] > best["reset_score"]:
            best = candidate
    return best


def _with_defaults(parameters: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(parameters or {})
    merged.setdefault("range_lookback_bars", DEFAULT_RANGE_LOOKBACK_BARS)
    merged.setdefault("range_window_bars", DEFAULT_RANGE_WINDOW_BARS)
    merged.setdefault("oi_change_window_bars", DEFAULT_OI_CHANGE_WINDOW_BARS)
    merged.setdefault("stats_lookback_bars", DEFAULT_STATS_LOOKBACK_BARS)
    merged.setdefault("min_stats_bars", DEFAULT_MIN_STATS_BARS)
    merged.setdefault("reset_lookback_bars", DEFAULT_RESET_LOOKBACK_BARS)
    merged.setdefault("min_reset_age_bars", DEFAULT_MIN_RESET_AGE_BARS)
    merged.setdefault("max_reset_age_bars", DEFAULT_MAX_RESET_AGE_BARS)
    merged.setdefault("min_flush_extension_pct", DEFAULT_MIN_FLUSH_EXTENSION_PCT)
    merged.setdefault("min_flush_candle_range_pct", DEFAULT_MIN_FLUSH_CANDLE_RANGE_PCT)
    merged.setdefault("min_reset_oi_drop_1h_pct", DEFAULT_MIN_RESET_OI_DROP_1H_PCT)
    merged.setdefault("min_reset_volume_ratio", DEFAULT_MIN_RESET_VOLUME_RATIO)
    merged.setdefault("min_reclaim_from_extreme_pct", DEFAULT_MIN_RECLAIM_FROM_EXTREME_PCT)
    merged.setdefault("min_oi_rebuild_1h_pct", DEFAULT_MIN_OI_REBUILD_1H_PCT)
    merged.setdefault("min_volume_ratio", DEFAULT_MIN_VOLUME_RATIO)
    merged.setdefault("min_taker_imbalance_z", DEFAULT_MIN_TAKER_IMBALANCE_Z)
    merged.setdefault("taker_buy_ratio_threshold", DEFAULT_TAKER_BUY_RATIO_THRESHOLD)
    merged.setdefault("taker_sell_ratio_threshold", DEFAULT_TAKER_SELL_RATIO_THRESHOLD)
    merged.setdefault("max_abs_funding_zscore", DEFAULT_MAX_ABS_FUNDING_ZSCORE)
    merged.setdefault("max_abs_premium_zscore", DEFAULT_MAX_ABS_PREMIUM_ZSCORE)
    merged.setdefault("post_reset_global_ratio_low", DEFAULT_POST_RESET_GLOBAL_RATIO_LOW)
    merged.setdefault("post_reset_global_ratio_high", DEFAULT_POST_RESET_GLOBAL_RATIO_HIGH)
    merged.setdefault("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)
    merged.setdefault("context_bars", DEFAULT_CONTEXT_BARS)
    merged.setdefault("context_timeframes", list(DEFAULT_CONTEXT_TIMEFRAMES))
    return merged


def _htf_source_lookback_bars(*, context_bars: int, context_timeframes: tuple[str, ...]) -> int:
    lookback = int(context_bars)
    for timeframe in context_timeframes:
        delta = _timeframe_delta(timeframe)
        base_rows_per_bucket = int(delta / BASE_TIMEFRAME_DELTA)
        lookback = max(lookback, base_rows_per_bucket * (_htf_context_limit(timeframe=timeframe, context_bars=context_bars) + 1))
    return lookback


def _htf_context_limit(*, timeframe: str, context_bars: int) -> int:
    normalized = timeframe.lower().strip()
    if normalized == "1h":
        return min(context_bars, 24)
    if normalized == "4h":
        return min(context_bars, 18)
    if normalized == "12h":
        return min(context_bars, 12)
    return context_bars


def _timeframe_delta(timeframe: str) -> timedelta:
    value = timeframe.lower().strip()
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _optional_seed_timestamp(parameters: dict[str, Any]) -> datetime | None:
    value = parameters.get("_dedupe_seed_timestamp")
    return _utc(value) if value else None


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _long_share_gap(*, top_ratio: float | None, global_ratio: float | None) -> float | None:
    if top_ratio is None or global_ratio is None or top_ratio <= 0 or global_ratio <= 0:
        return None
    top_long_share = top_ratio / (1.0 + top_ratio)
    global_long_share = global_ratio / (1.0 + global_ratio)
    return top_long_share - global_long_share
