from __future__ import annotations

from bisect import bisect_right
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_sdk.engine_contracts import (
    LiveSignalScanResult,
    SignalPacket,
    TrainingSignalGenerationResult,
    validate_signal_packet,
)
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


FeatureRows = dict[str, dict[str, list[dict[str, Any]]]]
FeatureRowIndex = dict[str, dict[str, dict[str, Any]]]
ContextRowIndex = dict[str, dict[str, Any]]

EMA_PERIODS = (36, 43, 144, 169, 576, 676)
EMA_TUNNELS: dict[str, tuple[int, int]] = {
    "fast": (36, 43),
    "mid": (144, 169),
    "slow": (576, 676),
}
CANDLE_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
DEFAULT_CONTEXT_BARS = 80
DEFAULT_PROXIMITY_THRESHOLD = Decimal("0.002")
DEFAULT_VOTE_THRESHOLD = 3
DEFAULT_DEDUPE_WINDOW_MINUTES = 120
DEFAULT_CONTEXT_TIMEFRAMES = ("2h", "1d")
FEATURE_FAMILIES: dict[str, str] = {
    "base_candle": "feature_base_candle",
    "volatility_range": "feature_volatility_range",
    "volume": "feature_volume",
    "ema_vegas_structure": "feature_ema_vegas_structure",
    "bollinger": "feature_bollinger",
    "regime_momentum": "feature_regime_momentum",
}
DEFAULT_FEATURE_TIMEFRAMES = ("5m", "2h", "1d")
DEFAULT_FEATURE_WINDOW_BARS = {"5m": 24, "2h": 12, "1d": 10}


def generate_training_signals(context: EngineTrainingContext) -> EngineTrainingOutput:
    raw_5m = context.market_data_reader.get_candles(asset=context.asset, timeframe="5m", origin="raw")
    if not raw_5m:
        raise ValueError(f"Raw candle data is empty for {context.asset}. Update local candle data first.")
    derived_rows = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="derived")
    context_rows = {
        timeframe: context.market_data_reader.get_rows(asset=context.asset, timeframe=timeframe, origin="derived")
        for timeframe in _context_timeframes(context.parameters)
    }
    feature_rows = _load_feature_rows(context=context)
    packets, generated_packet_count = generate_recursive_feature_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        derived_rows=derived_rows,
        start=context.start,
        end=context.end,
        parameters=context.parameters,
        context_rows=context_rows,
        feature_rows=feature_rows,
        packet_sink=context.packet_sink,
        packet_chunk_size=context.packet_chunk_size,
    )
    return EngineTrainingOutput(
        result=TrainingSignalGenerationResult(
            status="appended" if generated_packet_count else "noop",
            generated_packet_count=generated_packet_count,
            appended_packet_count=0,
            raw_candle_end_ts=_iso_z(context.raw_candle_end),
            scan_coverage_end_ts=_iso_z(context.end),
            packet_refs=[],
        ),
        packets=packets,
    )


def scan_live_signal(context: EngineLiveScanContext) -> LiveSignalScanResult:
    derived_rows = context.market_data_reader.get_rows(asset=context.asset, timeframe="5m", origin="derived")
    context_rows = {
        timeframe: context.market_data_reader.get_rows(asset=context.asset, timeframe=timeframe, origin="derived")
        for timeframe in _context_timeframes(context.parameters)
    }
    feature_rows = _load_feature_rows(context=context)
    packet = scan_recursive_features_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        derived_rows=derived_rows,
        context_rows=context_rows,
        feature_rows=feature_rows,
        parameters=context.parameters,
    )
    if packet is None:
        return LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest_confirmed_candle_did_not_trigger",
        )
    return LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket.from_mapping(packet),
    )


def generate_recursive_feature_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
    context_rows: dict[str, list[dict[str, Any]]] | ContextRowIndex | None = None,
    feature_rows: FeatureRows | FeatureRowIndex | None = None,
    packet_sink: Any | None = None,
    packet_chunk_size: int = 500,
) -> tuple[list[dict[str, Any]], int]:
    del workspace_root
    rows = _prepare_rows(derived_rows)
    if not rows:
        raise ValueError(f"Vegas recursive features requires derived EMA candle rows for {asset} 5m.")
    prepared_context_rows = _prepare_context_rows(asset=asset, context_rows=context_rows, parameters=parameters)
    prepared_feature_rows = _prepare_feature_indexes(feature_rows or {})
    _validate_feature_rows(asset=asset, feature_rows=prepared_feature_rows, parameters=parameters)
    window = timedelta(minutes=int(parameters.get("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)))
    packets: list[dict[str, Any]] = []
    buffered_packets: list[dict[str, Any]] = []
    generated_packet_count = 0
    last_emitted_at: datetime | None = None

    for index, row in enumerate(rows):
        timestamp = row["timestamp"]
        if timestamp < start:
            continue
        if timestamp > end:
            break
        packet = _scan_row(
            asset=asset,
            instrument=instrument,
            rows=rows,
            context_rows=prepared_context_rows,
            index=index,
            parameters=parameters,
            feature_rows=prepared_feature_rows,
        )
        if packet is None:
            continue
        if last_emitted_at is not None and (timestamp - last_emitted_at) < window:
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


def scan_recursive_features_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    context_rows: dict[str, list[dict[str, Any]]] | ContextRowIndex | None = None,
    feature_rows: FeatureRows | FeatureRowIndex | None = None,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    rows = _prepare_rows(derived_rows)
    if not rows:
        raise ValueError(f"Vegas recursive features requires derived EMA candle rows for {asset} 5m.")
    prepared_context_rows = _prepare_context_rows(asset=asset, context_rows=context_rows, parameters=parameters)
    prepared_feature_rows = _prepare_feature_indexes(feature_rows or {})
    _validate_feature_rows(asset=asset, feature_rows=prepared_feature_rows, parameters=parameters)
    return _scan_row(
        asset=asset,
        instrument=instrument,
        rows=rows,
        context_rows=prepared_context_rows,
        index=len(rows) - 1,
        parameters=parameters,
        feature_rows=prepared_feature_rows,
    )


def scan_recursive_features_at(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    context_rows: dict[str, list[dict[str, Any]]] | ContextRowIndex | None = None,
    feature_rows: FeatureRows | FeatureRowIndex | None = None,
    timestamp: datetime,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    rows = _prepare_rows(derived_rows)
    if not rows:
        raise ValueError(f"Vegas recursive features requires derived EMA candle rows for {asset} 5m.")
    timestamps = [row["timestamp"] for row in rows]
    index = bisect_right(timestamps, _utc(timestamp)) - 1
    if index < 0:
        return None
    prepared_context_rows = _prepare_context_rows(asset=asset, context_rows=context_rows, parameters=parameters)
    prepared_feature_rows = _prepare_feature_indexes(feature_rows or {})
    _validate_feature_rows(asset=asset, feature_rows=prepared_feature_rows, parameters=parameters)
    return _scan_row(
        asset=asset,
        instrument=instrument,
        rows=rows,
        context_rows=prepared_context_rows,
        index=index,
        parameters=parameters,
        feature_rows=prepared_feature_rows,
    )


def _scan_row(
    *,
    asset: str,
    instrument: str,
    rows: list[dict[str, Any]],
    context_rows: ContextRowIndex,
    index: int,
    parameters: dict[str, Any],
    feature_rows: FeatureRowIndex,
) -> dict[str, Any] | None:
    row = rows[index]
    timestamp = row["timestamp"]
    close = _decimal(row.get("close"))
    if close == 0:
        raise ValueError("Cannot scan Vegas 5m EMA distance with zero close.")

    proximity_threshold = Decimal(str(parameters.get("proximity_threshold", DEFAULT_PROXIMITY_THRESHOLD)))
    vote_threshold = int(parameters.get("cluster_vote_threshold", parameters.get("vote_threshold", DEFAULT_VOTE_THRESHOLD)))
    ema_values: dict[int, Decimal] = {}
    ema_distances: dict[int, Decimal] = {}
    ema_validity: dict[int, bool] = {}
    interactions: list[dict[str, Any]] = []
    matched_periods: list[int] = []

    for tunnel, periods in EMA_TUNNELS.items():
        for period in periods:
            ema_value = _ema_value(row, period)
            distance_pct = abs(close - ema_value) / close
            is_valid = _ema_is_valid(row, period)
            ema_values[period] = ema_value
            ema_distances[period] = distance_pct
            ema_validity[period] = is_valid
            if not is_valid or distance_pct > proximity_threshold:
                continue
            matched_periods.append(period)
            interactions.append(
                {
                    "timeframe": "5m",
                    "tunnel": tunnel,
                    "period": period,
                    "ema_value": str(ema_value),
                    "market_price": str(close),
                    "distance_pct": str(distance_pct),
                }
            )

    if len(matched_periods) < vote_threshold:
        return None

    context_bars = int(parameters.get("context_bars", DEFAULT_CONTEXT_BARS))
    trigger_context_rows = rows[max(0, index - context_bars + 1) : index + 1]
    context_timeframes = list(_context_timeframes(parameters))
    charts = {
        "5m": {
            "role": "trigger",
            "timeframe": "5m",
            "columns": CANDLE_COLUMNS,
            "completed_candles": [_row_to_packet_row(context_row) for context_row in trigger_context_rows],
            "ema_mode": "precomputed_5m_ema_cluster",
            "ema_values": {str(period): str(value) for period, value in ema_values.items()},
            "ema_distances": {str(period): str(value) for period, value in ema_distances.items()},
            "ema_validity": {str(period): valid for period, valid in ema_validity.items()},
        }
    }
    charts.update(
        _context_charts(
            rows_by_timeframe=context_rows,
            signal_timestamp=timestamp,
            context_bars=context_bars,
            context_timeframes=context_timeframes,
        )
    )
    features = _feature_snapshot(
        feature_rows=feature_rows,
        signal_timestamp=timestamp,
        parameters=parameters,
    )
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset,
        "instrument": instrument,
        "timestamp": _iso_z(timestamp),
        "active_timeframes": ["5m"],
        "interactions": interactions,
        "charts": charts,
        "features": features,
        "evidence": {
            "pattern": "vegas_ema_5m_cluster_proximity",
            "ema_mode": "precomputed_5m_ema_cluster",
            "timeframe": "5m",
            "trigger_timeframe": "5m",
            "context_timeframes": context_timeframes,
            "proximity_threshold": str(proximity_threshold),
            "vote_threshold": vote_threshold,
            "matched_ema_count": len(matched_periods),
            "matched_periods": matched_periods,
            "active_timeframes": ["5m"],
            "interactions": interactions,
            "charts": charts,
            "features": features,
        },
    }
    validate_signal_packet(packet)
    return packet


def _ema_value(row: dict[str, Any], period: int) -> Decimal:
    for key in (f"ema_{period}", f"ema{period}"):
        if row.get(key) not in (None, ""):
            return _decimal(row[key])
    raise ValueError(
        f"Vegas recursive features requires derived candle column ema_{period}. "
        "Prepare EMA-enriched 5m data before using vegas_ema_recursive_features."
    )


def _ema_is_valid(row: dict[str, Any], period: int) -> bool:
    for key in (f"ema_warmup_count_{period}", f"ema_{period}_warmup_count", "ema_warmup_count"):
        if row.get(key) not in (None, ""):
            return int(row[key]) >= period
    return True


def _prepare_context_rows(
    *,
    asset: str,
    context_rows: dict[str, list[dict[str, Any]]] | ContextRowIndex | None,
    parameters: dict[str, Any],
) -> ContextRowIndex:
    rows_by_timeframe: ContextRowIndex = {}
    source = context_rows or {}
    for timeframe in _context_timeframes(parameters):
        value = source.get(timeframe) or {}
        if _is_context_index(value):
            rows = list(value["rows"])
            timestamps = list(value["timestamps"])
        else:
            rows = _prepare_rows(value if isinstance(value, list) else [])
            timestamps = [row["timestamp"] for row in rows]
        if not rows:
            raise ValueError(f"Vegas recursive features requires derived EMA context rows for {asset} {timeframe}.")
        rows_by_timeframe[timeframe] = {"rows": rows, "timestamps": timestamps}
    return rows_by_timeframe


def _context_charts(
    *,
    rows_by_timeframe: ContextRowIndex,
    signal_timestamp: datetime,
    context_bars: int,
    context_timeframes: list[str],
) -> dict[str, dict[str, Any]]:
    charts: dict[str, dict[str, Any]] = {}
    for timeframe in context_timeframes:
        context_index = rows_by_timeframe.get(timeframe) or {}
        rows = _context_index_rows(context_index)
        timestamps = context_index.get("timestamps") if _is_context_index(context_index) else [row["timestamp"] for row in rows]
        index = bisect_right(timestamps, _utc(signal_timestamp)) - 1
        if index < 0:
            continue
        context_rows = rows[max(0, index - context_bars + 1) : index + 1]
        latest_row = rows[index]
        ema_values, ema_distances, ema_validity = _ema_snapshot(latest_row)
        charts[timeframe] = {
            "role": "context",
            "timeframe": timeframe,
            "columns": CANDLE_COLUMNS,
            "completed_candles": [_row_to_packet_row(context_row) for context_row in context_rows],
            "ema_mode": "precomputed_context_ema",
            "ema_values": {str(period): str(value) for period, value in ema_values.items()},
            "ema_distances": {str(period): str(value) for period, value in ema_distances.items()},
            "ema_validity": {str(period): valid for period, valid in ema_validity.items()},
        }
    return charts


def _ema_snapshot(row: dict[str, Any]) -> tuple[dict[int, Decimal], dict[int, Decimal], dict[int, bool]]:
    close = _decimal(row.get("close"))
    if close == 0:
        raise ValueError("Cannot scan Vegas EMA context distance with zero close.")
    ema_values: dict[int, Decimal] = {}
    ema_distances: dict[int, Decimal] = {}
    ema_validity: dict[int, bool] = {}
    for period in EMA_PERIODS:
        ema_value = _ema_value(row, period)
        ema_values[period] = ema_value
        ema_distances[period] = abs(close - ema_value) / close
        ema_validity[period] = _ema_is_valid(row, period)
    return ema_values, ema_distances, ema_validity


def _feature_snapshot(
    *,
    feature_rows: FeatureRowIndex,
    signal_timestamp: datetime,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    snapshots: dict[str, Any] = {}
    window_bars = _feature_window_bars(parameters)
    feature_families = _feature_families(parameters)
    for timeframe in _feature_timeframes(parameters):
        family_rows = feature_rows.get(timeframe) or {}
        family_windows: dict[str, list[dict[str, Any]]] = {}
        latest: dict[str, dict[str, Any]] = {}
        missing: list[dict[str, Any]] = []
        for family, data_type in feature_families.items():
            feature_index = family_rows.get(family) or {}
            window = _rows_until(feature_index=feature_index, timestamp=signal_timestamp, limit=window_bars[timeframe])
            serialized_window = [_serialize_feature_row(row) for row in window]
            family_windows[family] = serialized_window
            if serialized_window:
                latest[family] = serialized_window[-1]
            else:
                first_timestamp = _first_feature_timestamp(feature_index)
                missing.append(
                    {
                        "family": family,
                        "data_type": data_type,
                        "reason": "no_feature_rows_at_or_before_signal_timestamp",
                        "signal_timestamp": _iso_z(signal_timestamp),
                        "first_feature_timestamp": _iso_z(first_timestamp) if first_timestamp else None,
                    }
                )
        snapshot: dict[str, Any] = {
            "latest": latest,
            "window": _merge_family_windows(family_windows),
            "window_bars": window_bars[timeframe],
        }
        if missing:
            snapshot["missing_feature_families"] = missing
        snapshots[timeframe] = snapshot
    return snapshots


def _context_index_rows(value: Any) -> list[dict[str, Any]]:
    if _is_context_index(value):
        return list(value["rows"])
    if isinstance(value, list):
        return value
    return []


def _is_context_index(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("rows"), list) and isinstance(value.get("timestamps"), list)


def _load_feature_rows(context: EngineTrainingContext | EngineLiveScanContext) -> FeatureRows:
    rows: FeatureRows = {}
    for timeframe in _feature_timeframes(context.parameters):
        rows[timeframe] = {}
        for family, data_type in _feature_families(context.parameters).items():
            try:
                raw_rows = context.market_data_reader.get_rows(
                    asset=context.asset,
                    timeframe=timeframe,
                    origin="derived",
                    data_type=data_type,
                )
            except ValueError as exc:
                raise ValueError(
                    f"Vegas recursive features requires derived {data_type} rows for "
                    f"{context.asset} {timeframe} ({family})."
                ) from exc
            prepared_rows = _prepare_rows(raw_rows)
            if not prepared_rows:
                raise ValueError(
                    f"Vegas recursive features requires non-empty derived {data_type} rows for "
                    f"{context.asset} {timeframe} ({family})."
                )
            rows[timeframe][family] = prepared_rows
    return rows


def _validate_feature_rows(
    *,
    asset: str,
    feature_rows: FeatureRows | FeatureRowIndex,
    parameters: dict[str, Any],
) -> None:
    for timeframe in _feature_timeframes(parameters):
        family_rows = feature_rows.get(timeframe) or {}
        for family, data_type in _feature_families(parameters).items():
            if not _feature_index_rows(family_rows.get(family)):
                raise ValueError(
                    f"Vegas recursive features requires non-empty derived {data_type} rows for "
                    f"{asset} {timeframe} ({family})."
                )


def _prepare_feature_indexes(feature_rows: FeatureRows | FeatureRowIndex) -> FeatureRowIndex:
    indexed: FeatureRowIndex = {}
    for timeframe, family_rows in feature_rows.items():
        indexed[timeframe] = {}
        for family, value in family_rows.items():
            if _is_feature_index(value):
                rows = list(value["rows"])
                timestamps = list(value["timestamps"])
            else:
                rows = list(value)
                timestamps = [row["timestamp"] for row in rows]
            indexed[timeframe][family] = {"rows": rows, "timestamps": timestamps}
    return indexed


def _rows_until(*, feature_index: dict[str, Any], timestamp: datetime, limit: int) -> list[dict[str, Any]]:
    rows = _feature_index_rows(feature_index)
    timestamps = feature_index.get("timestamps") if _is_feature_index(feature_index) else [row["timestamp"] for row in rows]
    end = bisect_right(timestamps, _utc(timestamp))
    return rows[max(0, end - limit) : end]


def _feature_index_rows(value: Any) -> list[dict[str, Any]]:
    if _is_feature_index(value):
        return list(value["rows"])
    if isinstance(value, list):
        return value
    return []


def _first_feature_timestamp(value: Any) -> datetime | None:
    if _is_feature_index(value):
        timestamps = value.get("timestamps") or []
        return timestamps[0] if timestamps else None
    rows = _feature_index_rows(value)
    return rows[0]["timestamp"] if rows else None


def _is_feature_index(value: Any) -> bool:
    return isinstance(value, dict) and isinstance(value.get("rows"), list) and isinstance(value.get("timestamps"), list)


def _merge_family_windows(family_windows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    rows_by_timestamp: dict[str, dict[str, Any]] = {}
    for family, rows in family_windows.items():
        for row in rows:
            timestamp = str(row["timestamp"])
            rows_by_timestamp.setdefault(timestamp, {"timestamp": timestamp})
            rows_by_timestamp[timestamp][family] = {key: value for key, value in row.items() if key != "timestamp"}
    return [rows_by_timestamp[timestamp] for timestamp in sorted(rows_by_timestamp)]


def _serialize_feature_row(row: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in row.items():
        if key == "ts":
            continue
        if key == "timestamp":
            serialized[key] = _iso_z(value)
        else:
            serialized[key] = value
    return serialized


def _feature_families(parameters: dict[str, Any]) -> dict[str, str]:
    configured = parameters.get("feature_families")
    if configured is None:
        return dict(FEATURE_FAMILIES)
    if isinstance(configured, dict):
        unknown = set(configured) - set(FEATURE_FAMILIES)
        if unknown:
            raise ValueError(f"Unknown Vegas feature families: {sorted(unknown)}")
        return {str(family): str(data_type) for family, data_type in configured.items()}
    families = tuple(str(item) for item in configured)
    unknown = set(families) - set(FEATURE_FAMILIES)
    if unknown:
        raise ValueError(f"Unknown Vegas feature families: {sorted(unknown)}")
    return {family: FEATURE_FAMILIES[family] for family in families}


def _feature_timeframes(parameters: dict[str, Any]) -> tuple[str, ...]:
    value = parameters.get("feature_timeframes", DEFAULT_FEATURE_TIMEFRAMES)
    return tuple(str(item) for item in value)


def _feature_window_bars(parameters: dict[str, Any]) -> dict[str, int]:
    value = parameters.get("feature_window_bars")
    configured = value if isinstance(value, dict) else {}
    return {
        timeframe: max(1, int(configured.get(timeframe, DEFAULT_FEATURE_WINDOW_BARS.get(timeframe, 10))))
        for timeframe in _feature_timeframes(parameters)
    }


def _context_timeframes(parameters: dict[str, Any]) -> tuple[str, ...]:
    value = parameters.get("context_timeframes", DEFAULT_CONTEXT_TIMEFRAMES)
    return tuple(str(item) for item in value)


def _prepare_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared = []
    for row in rows:
        prepared.append({**row, "timestamp": _utc(row.get("timestamp") or row.get("ts"))})
    return sorted(prepared, key=lambda row: row["timestamp"])


def _row_to_packet_row(row: dict[str, Any]) -> list[Any]:
    timestamp = _utc(row["timestamp"]).isoformat().replace("+00:00", "Z")
    return [
        timestamp,
        str(_decimal(row.get("open", 0))),
        str(_decimal(row.get("high", 0))),
        str(_decimal(row.get("low", 0))),
        str(_decimal(row.get("close", 0))),
        str(_decimal(row.get("volume", 0))),
        str(_decimal(row.get("vol_ccy", row.get("volCcy", 0)))),
        str(_decimal(row.get("vol_ccy_quote", row.get("volCcyQuote", 0)))),
        int(row.get("confirm", 1)),
    ]


def _decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def _utc(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _iso_z(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")
