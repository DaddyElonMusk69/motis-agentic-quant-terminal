from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from statistics import mean, pstdev
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
from quant_terminal_worker.signal_engines.vegas_ema_5m_cluster import (
    CANDLE_COLUMNS,
    DEFAULT_CONTEXT_BARS,
    DEFAULT_DEDUPE_WINDOW_MINUTES,
    DEFAULT_PROXIMITY_THRESHOLD,
    DEFAULT_VOTE_THRESHOLD,
    EMA_TUNNELS,
    _decimal,
    _ema_is_valid,
    _ema_snapshot,
    _ema_value,
    _iso_z,
    _optional_seed_timestamp,
    _prepare_rows,
    _row_to_packet_row,
    _utc,
)


ENGINE_ID = "vegas_5m_cluster_v6"
V6_CONTEXT_MODE = "candles_integrated_forming_bollinger_oi_and_stage1a_context_v2"
DEFAULT_CONTEXT_TIMEFRAMES = ("2h", "8h", "12h")
STAGE1A_CONTEXT_SCHEMA_VERSION = "vegas_5m_cluster_v6.stage1a_context.v2"
STAGE1A_CONTEXT_VALUE_NAMES = (
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
        for timeframe in DEFAULT_CONTEXT_TIMEFRAMES
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
FORMING_CANDLE_METADATA_COLUMNS = [
    "is_completed",
    "source_candle_count",
    "partial_close_timestamp",
    "expected_close_timestamp",
]
PACKET_CANDLE_COLUMNS = CANDLE_COLUMNS + FORMING_CANDLE_METADATA_COLUMNS
BOLLINGER_VALUE_COLUMNS = [
    "bb_mid_20",
    "bb_upper_20_2",
    "bb_lower_20_2",
    "bb_position_pct",
    "bb_bandwidth_pct",
    "bb_zscore",
]
BOLLINGER_COLUMNS = [
    "open_ts",
    "available_at",
    "complete",
    "source_candle_count",
    "close",
    *BOLLINGER_VALUE_COLUMNS,
]
OI_FEATURE_VALUE_COLUMNS = [
    "oi_return_pct_2h",
    "oi_return_pct_8h",
    "oi_return_pct_24h",
    "oi_change_2h_zscore_7d",
    "general_long_short_ratio",
    "taker_long_short_ratio_avg_2h",
]
OI_FEATURE_METADATA_COLUMNS = [
    "available_at",
    "complete",
    "source_window_start_ts",
    "source_window_end_ts",
    "source_row_count",
]


@dataclass(frozen=True, slots=True)
class _PreparedData:
    trigger_rows: list[dict[str, Any]]
    trigger_timestamps: list[datetime]
    raw_rows: list[dict[str, Any]]
    raw_timestamps: list[datetime]
    raw_by_timestamp: dict[datetime, dict[str, Any]]
    context_rows: dict[str, list[dict[str, Any]]]
    context_timestamps: dict[str, list[datetime]]
    daily_rows: list[dict[str, Any]]
    daily_timestamps: list[datetime]
    daily_by_timestamp: dict[datetime, dict[str, Any]]
    bollinger_pairs: list[tuple[dict[str, Any], dict[str, Any]]]
    bollinger_available_at: list[datetime]
    oi_feature_rows: list[dict[str, Any]]
    oi_feature_available_at: list[datetime]


@dataclass(frozen=True, slots=True)
class _V6SignalPacket(SignalPacket):
    raw_packet: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> _V6SignalPacket:
        base = SignalPacket.from_mapping(value)
        return cls(
            schema_version=base.schema_version,
            asset=base.asset,
            timestamp=base.timestamp,
            evidence=base.evidence,
            instrument=base.instrument,
            active_timeframes=base.active_timeframes,
            raw_packet=dict(value),
        )

    def to_mapping(self) -> dict[str, Any]:
        return dict(self.raw_packet)


def generate_training_signals(context: EngineTrainingContext) -> EngineTrainingOutput:
    parameters = _with_v6_defaults(context.parameters)
    data = _read_required_data(context.market_data_reader, asset=context.asset, parameters=parameters)
    packets, generated_packet_count = generate_5m_cluster_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        start=context.start,
        end=context.end,
        parameters=parameters,
        packet_sink=context.packet_sink,
        packet_chunk_size=context.packet_chunk_size,
        **data,
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
    parameters = _with_v6_defaults(context.parameters)
    data = _read_required_data(context.market_data_reader, asset=context.asset, parameters=parameters)
    packet = scan_5m_cluster_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        parameters=parameters,
        **data,
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
        signal=_V6SignalPacket.from_mapping(packet),
    )


def generate_5m_cluster_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    raw_5m_rows: list[Any],
    context_rows: dict[str, list[dict[str, Any]]],
    daily_rows: list[dict[str, Any]],
    bollinger_rows: list[dict[str, Any]],
    oi_feature_rows: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
    packet_sink: Any | None = None,
    packet_chunk_size: int = 500,
) -> tuple[list[dict[str, Any]], int]:
    del workspace_root
    parameters = _with_v6_defaults(parameters)
    prepared = _prepare_data(
        asset=asset,
        derived_rows=derived_rows,
        raw_5m_rows=raw_5m_rows,
        context_rows=context_rows,
        daily_rows=daily_rows,
        bollinger_rows=bollinger_rows,
        oi_feature_rows=oi_feature_rows,
        parameters=parameters,
    )
    window = timedelta(minutes=int(parameters.get("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES)))
    start = _utc(start)
    end = _utc(end)
    last_emitted_at = _optional_seed_timestamp(parameters)
    packets: list[dict[str, Any]] = []
    buffered: list[dict[str, Any]] = []
    count = 0

    for index, row in enumerate(prepared.trigger_rows):
        signal_open = row["timestamp"]
        if signal_open < start:
            continue
        if signal_open > end:
            break
        signal_available_at = signal_open + timedelta(minutes=5)
        if last_emitted_at is not None and signal_available_at - last_emitted_at < window:
            continue
        packet = _build_packet_at_index(
            prepared=prepared,
            index=index,
            asset=asset,
            instrument=instrument,
            parameters=parameters,
        )
        if packet is None:
            continue
        last_emitted_at = signal_available_at
        count += 1
        if callable(packet_sink):
            buffered.append(packet)
            if len(buffered) >= max(1, int(packet_chunk_size)):
                packet_sink(buffered)
                buffered = []
        else:
            packets.append(packet)

    if callable(packet_sink) and buffered:
        packet_sink(buffered)
    return packets, count


def scan_5m_cluster_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    raw_5m_rows: list[Any],
    context_rows: dict[str, list[dict[str, Any]]],
    daily_rows: list[dict[str, Any]],
    bollinger_rows: list[dict[str, Any]],
    oi_feature_rows: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    parameters = _with_v6_defaults(parameters)
    prepared = _prepare_data(
        asset=asset,
        derived_rows=derived_rows,
        raw_5m_rows=raw_5m_rows,
        context_rows=context_rows,
        daily_rows=daily_rows,
        bollinger_rows=bollinger_rows,
        oi_feature_rows=oi_feature_rows,
        parameters=parameters,
    )
    for index in range(len(prepared.trigger_rows) - 1, -1, -1):
        if prepared.trigger_rows[index]["timestamp"] in prepared.raw_by_timestamp:
            return _build_packet_at_index(
                prepared=prepared,
                index=index,
                asset=asset,
                instrument=instrument,
                parameters=parameters,
            )
    return None


def scan_5m_cluster_at(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    derived_rows: list[dict[str, Any]],
    raw_5m_rows: list[Any],
    context_rows: dict[str, list[dict[str, Any]]],
    daily_rows: list[dict[str, Any]],
    bollinger_rows: list[dict[str, Any]],
    oi_feature_rows: list[dict[str, Any]],
    timestamp: datetime,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    parameters = _with_v6_defaults(parameters)
    prepared = _prepare_data(
        asset=asset,
        derived_rows=derived_rows,
        raw_5m_rows=raw_5m_rows,
        context_rows=context_rows,
        daily_rows=daily_rows,
        bollinger_rows=bollinger_rows,
        oi_feature_rows=oi_feature_rows,
        parameters=parameters,
    )
    index = bisect_right(prepared.trigger_timestamps, _utc(timestamp)) - 1
    if index < 0:
        return None
    return _build_packet_at_index(
        prepared=prepared,
        index=index,
        asset=asset,
        instrument=instrument,
        parameters=parameters,
    )


def calculate_provisional_bollinger(
    completed_closes: list[Any],
    *,
    forming_close: Any,
) -> dict[str, float | None]:
    if len(completed_closes) != 19:
        raise ValueError("Provisional 1d Bollinger requires exactly 19 completed closes.")
    closes = [float(value) for value in completed_closes] + [float(forming_close)]
    middle = mean(closes)
    standard_deviation = pstdev(closes)
    upper = middle + 2 * standard_deviation
    lower = middle - 2 * standard_deviation
    width = upper - lower
    return {
        "bb_mid_20": middle,
        "bb_upper_20_2": upper,
        "bb_lower_20_2": lower,
        "bb_position_pct": ((closes[-1] - lower) / width * 100) if width else None,
        "bb_bandwidth_pct": (width / middle * 100) if middle else None,
        "bb_zscore": ((closes[-1] - middle) / standard_deviation) if standard_deviation else None,
    }


def _read_required_data(reader: Any, *, asset: str, parameters: dict[str, Any]) -> dict[str, Any]:
    raw_5m_rows = reader.get_candles(asset=asset, timeframe="5m", origin="raw")
    if not raw_5m_rows:
        raise ValueError(f"Raw candle data is empty for {asset}. Update local candle data first.")
    derived_rows = reader.get_rows(asset=asset, timeframe="5m", origin="derived")
    context_rows = {
        timeframe: reader.get_rows(asset=asset, timeframe=timeframe, origin="derived")
        for timeframe in _context_timeframes(parameters)
    }
    daily_rows = reader.get_rows(asset=asset, timeframe="1d", origin="derived")
    bollinger_rows = reader.get_rows(
        asset=asset,
        timeframe="1d",
        origin="derived",
        data_type="feature_bollinger",
    )
    oi_feature_rows = reader.get_rows(
        asset=asset,
        timeframe="15m",
        origin="derived",
        data_type="feature_open_interest_regime",
    )
    return {
        "derived_rows": derived_rows,
        "raw_5m_rows": raw_5m_rows,
        "context_rows": context_rows,
        "daily_rows": daily_rows,
        "bollinger_rows": bollinger_rows,
        "oi_feature_rows": oi_feature_rows,
    }


def _prepare_data(
    *,
    asset: str,
    derived_rows: list[dict[str, Any]],
    raw_5m_rows: list[Any],
    context_rows: dict[str, list[dict[str, Any]]],
    daily_rows: list[dict[str, Any]],
    bollinger_rows: list[dict[str, Any]],
    oi_feature_rows: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> _PreparedData:
    trigger_rows = _prepare_rows(derived_rows)
    if not trigger_rows:
        raise ValueError(f"Vegas 5m Cluster v6 requires derived EMA candle rows for {asset} 5m.")
    raw_rows = sorted(
        (_raw_row_to_mapping(row) for row in raw_5m_rows if _row_confirmed(row)),
        key=lambda row: row["timestamp"],
    )
    if not raw_rows:
        raise ValueError(f"Raw candle data is empty for {asset}. Update local candle data first.")

    prepared_context: dict[str, list[dict[str, Any]]] = {}
    for timeframe in _context_timeframes(parameters):
        rows = _prepare_rows((context_rows or {}).get(timeframe) or [])
        if not rows:
            raise ValueError(f"Vegas 5m Cluster v6 requires derived {timeframe} candle context for {asset}.")
        prepared_context[timeframe] = rows

    prepared_daily = _prepare_rows(daily_rows)
    if not prepared_daily:
        raise ValueError(f"Vegas 5m Cluster v6 requires derived 1d candle data for {asset}.")
    prepared_bollinger = _prepare_feature_rows(bollinger_rows)
    if not prepared_bollinger:
        raise ValueError(f"Vegas 5m Cluster v6 requires derived feature_bollinger 1d data for {asset}.")
    missing_columns = [column for column in BOLLINGER_VALUE_COLUMNS if any(column not in row for row in prepared_bollinger)]
    if missing_columns:
        raise ValueError(
            "Vegas 5m Cluster v6 feature_bollinger 1d data is missing required columns: "
            + ", ".join(sorted(set(missing_columns)))
        )
    prepared_oi_features = _prepare_oi_feature_rows(oi_feature_rows)
    if not prepared_oi_features:
        raise ValueError(f"Vegas 5m Cluster v6 requires derived feature_open_interest_regime 15m data for {asset}.")
    missing_oi_columns = [
        column
        for column in [*OI_FEATURE_VALUE_COLUMNS, *OI_FEATURE_METADATA_COLUMNS]
        if any(column not in row for row in prepared_oi_features)
    ]
    if missing_oi_columns:
        raise ValueError(
            "Vegas 5m Cluster v6 feature_open_interest_regime 15m data is missing required columns: "
            + ", ".join(sorted(set(missing_oi_columns)))
        )

    raw_by_timestamp = {row["timestamp"]: row for row in raw_rows}
    daily_by_timestamp = {row["timestamp"]: row for row in prepared_daily}
    bollinger_pairs = [
        (feature, daily_by_timestamp[feature["timestamp"]])
        for feature in prepared_bollinger
        if feature["timestamp"] in daily_by_timestamp
    ]
    return _PreparedData(
        trigger_rows=trigger_rows,
        trigger_timestamps=[row["timestamp"] for row in trigger_rows],
        raw_rows=raw_rows,
        raw_timestamps=[row["timestamp"] for row in raw_rows],
        raw_by_timestamp=raw_by_timestamp,
        context_rows=prepared_context,
        context_timestamps={key: [row["timestamp"] for row in rows] for key, rows in prepared_context.items()},
        daily_rows=prepared_daily,
        daily_timestamps=[row["timestamp"] for row in prepared_daily],
        daily_by_timestamp=daily_by_timestamp,
        bollinger_pairs=bollinger_pairs,
        bollinger_available_at=[source["timestamp"] + timedelta(days=1) for _, source in bollinger_pairs],
        oi_feature_rows=prepared_oi_features,
        oi_feature_available_at=[row["available_at"] for row in prepared_oi_features],
    )


def _build_packet_at_index(
    *,
    prepared: _PreparedData,
    index: int,
    asset: str,
    instrument: str,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    row = prepared.trigger_rows[index]
    signal_open = row["timestamp"]
    signal_available_at = signal_open + timedelta(minutes=5)
    trigger_candle = prepared.raw_by_timestamp.get(signal_open)
    if trigger_candle is None:
        return None
    trigger_close = _decimal(trigger_candle.get("close"))
    derived_close = _decimal(row.get("close"))
    if trigger_close <= 0 or derived_close <= 0:
        raise ValueError("Vegas 5m Cluster v6 requires a positive trigger reference price.")

    trigger = _trigger_state(row=row, close=derived_close, parameters=parameters)
    if trigger is None:
        return None
    context_bars = int(parameters.get("context_bars", DEFAULT_CONTEXT_BARS))
    trigger_context = [
        context_row
        for context_row in prepared.trigger_rows[max(0, index - context_bars + 1) : index + 1]
        if context_row["timestamp"] + timedelta(minutes=5) <= signal_available_at
    ]
    charts: dict[str, Any] = {
        "5m": {
            "role": "trigger",
            "timeframe": "5m",
            "columns": PACKET_CANDLE_COLUMNS,
            "candle_columns": PACKET_CANDLE_COLUMNS,
            "candles": [_completed_candle_row(item, timeframe="5m") for item in trigger_context],
            "ema_mode": "precomputed_5m_ema_cluster",
            "ema_values": trigger["ema_values"],
            "ema_distances": trigger["ema_distances"],
            "ema_validity": trigger["ema_validity"],
        }
    }
    for timeframe in _context_timeframes(parameters):
        charts[timeframe] = _context_chart(
            prepared=prepared,
            timeframe=timeframe,
            signal_available_at=signal_available_at,
            context_bars=context_bars,
        )
    charts["bollinger_1d"] = _bollinger_chart(
        prepared=prepared,
        signal_available_at=signal_available_at,
        context_bars=int(parameters.get("bollinger_context_bars", 20)),
    )
    oi_feature = _open_interest_regime_snapshot(
        prepared=prepared,
        asset=asset,
        signal_available_at=signal_available_at,
    )
    stage1a_context = _stage1a_context_snapshot(
        prepared=prepared,
        signal_available_at=signal_available_at,
    )

    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset,
        "instrument": instrument,
        "timestamp": _iso_z(signal_available_at),
        "active_timeframes": ["5m"],
        "interactions": trigger["interactions"],
        "charts": charts,
        "evidence": {
            "engine": ENGINE_ID,
            "pattern": "vegas_ema_5m_cluster_proximity",
            "ema_mode": "precomputed_5m_ema_cluster",
            "context_mode": V6_CONTEXT_MODE,
            "timeframe": "5m",
            "trigger_timeframe": "5m",
            "context_timeframes": list(_context_timeframes(parameters)),
            "proximity_threshold": trigger["proximity_threshold"],
            "vote_threshold": trigger["vote_threshold"],
            "matched_ema_count": len(trigger["matched_periods"]),
            "matched_periods": trigger["matched_periods"],
            "active_timeframes": ["5m"],
            "interactions": trigger["interactions"],
            "reference_price": str(trigger_close),
            "trigger_candle_close": str(trigger_close),
            "signal_candle_open_ts": _iso_z(signal_open),
            "signal_candle_close_ts": _iso_z(signal_available_at),
            "signal_available_at": _iso_z(signal_available_at),
            "derived_features": {
                "open_interest_regime": oi_feature,
                "stage1a_context_v2": stage1a_context,
            },
        },
    }
    validate_signal_packet(packet)
    return packet


def _trigger_state(*, row: dict[str, Any], close: Decimal, parameters: dict[str, Any]) -> dict[str, Any] | None:
    proximity = _decimal(parameters.get("proximity_threshold", DEFAULT_PROXIMITY_THRESHOLD))
    vote_threshold = int(parameters.get("cluster_vote_threshold", parameters.get("vote_threshold", DEFAULT_VOTE_THRESHOLD)))
    ema_values: dict[str, str] = {}
    ema_distances: dict[str, str] = {}
    ema_validity: dict[str, bool] = {}
    matched_periods: list[int] = []
    interactions: list[dict[str, Any]] = []
    for tunnel, periods in EMA_TUNNELS.items():
        for period in periods:
            value = _ema_value(row, period)
            distance = abs(close - value) / close
            valid = _ema_is_valid(row, period)
            ema_values[str(period)] = str(value)
            ema_distances[str(period)] = str(distance)
            ema_validity[str(period)] = valid
            if not valid or distance > proximity:
                continue
            matched_periods.append(period)
            interactions.append(
                {
                    "timeframe": "5m",
                    "tunnel": tunnel,
                    "period": period,
                    "ema_value": str(value),
                    "market_price": str(close),
                    "distance_pct": str(distance),
                }
            )
    if len(matched_periods) < vote_threshold:
        return None
    return {
        "proximity_threshold": str(proximity),
        "vote_threshold": vote_threshold,
        "ema_values": ema_values,
        "ema_distances": ema_distances,
        "ema_validity": ema_validity,
        "matched_periods": matched_periods,
        "interactions": interactions,
    }


def _context_chart(
    *,
    prepared: _PreparedData,
    timeframe: str,
    signal_available_at: datetime,
    context_bars: int,
) -> dict[str, Any]:
    rows = prepared.context_rows[timeframe]
    timestamps = prepared.context_timestamps[timeframe]
    delta = _timeframe_delta(timeframe)
    completed_index = bisect_right(timestamps, signal_available_at - delta) - 1
    completed = rows[max(0, completed_index - context_bars + 1) : completed_index + 1] if completed_index >= 0 else []
    candles = [_completed_candle_row(row, timeframe=timeframe) for row in completed]
    forming = _forming_candle(
        raw_rows=prepared.raw_rows,
        raw_timestamps=prepared.raw_timestamps,
        anchor=timestamps[0],
        timeframe=timeframe,
        signal_available_at=signal_available_at,
    )
    if forming is not None:
        candles.append(_forming_candle_row(forming))
    latest_completed = completed[-1] if completed else None
    if latest_completed is not None:
        ema_values, ema_distances, ema_validity = _ema_snapshot(latest_completed)
    else:
        ema_values, ema_distances, ema_validity = {}, {}, {}
    if latest_completed is not None:
        latest_open = latest_completed["timestamp"]
        latest_close = latest_open + delta
    elif forming is not None:
        latest_open = forming["open_timestamp_raw"]
        latest_close = latest_open + delta
    else:
        latest_open = None
        latest_close = None
    return {
        "role": "context",
        "timeframe": timeframe,
        "context_mode": V6_CONTEXT_MODE,
        "columns": PACKET_CANDLE_COLUMNS,
        "candle_columns": PACKET_CANDLE_COLUMNS,
        "candles": candles,
        "latest_opened_at": _iso_z(latest_open) if latest_open is not None else None,
        "latest_closed_at": _iso_z(latest_close) if latest_close is not None else None,
        "latest_candle_is_completed": forming is None,
        "latest_partial_close_timestamp": forming["partial_close_timestamp"] if forming else None,
        "latest_expected_close_timestamp": forming["expected_close_timestamp"] if forming else None,
        "ema_mode": "precomputed_context_ema",
        "ema_values": {str(period): str(value) for period, value in ema_values.items()},
        "ema_distances": {str(period): str(value) for period, value in ema_distances.items()},
        "ema_validity": {str(period): valid for period, valid in ema_validity.items()},
    }


def _bollinger_chart(
    *,
    prepared: _PreparedData,
    signal_available_at: datetime,
    context_bars: int,
) -> dict[str, Any]:
    completed_index = bisect_right(prepared.bollinger_available_at, signal_available_at)
    selected_pairs = prepared.bollinger_pairs[max(0, completed_index - max(1, context_bars)) : completed_index]
    completed_rows: list[list[Any]] = []
    for feature, source in selected_pairs:
        available_at = source["timestamp"] + timedelta(days=1)
        completed_rows.append(
            [
                _iso_z(source["timestamp"]),
                _iso_z(available_at),
                True,
                288,
                str(_decimal(source.get("close"))),
                *[_packet_number(feature.get(column)) for column in BOLLINGER_VALUE_COLUMNS],
            ]
        )
    rows = completed_rows

    forming = _forming_candle(
        raw_rows=prepared.raw_rows,
        raw_timestamps=prepared.raw_timestamps,
        anchor=prepared.daily_timestamps[0],
        timeframe="1d",
        signal_available_at=signal_available_at,
    )
    if forming is not None:
        daily_end = bisect_left(prepared.daily_timestamps, forming["open_timestamp_raw"])
        completed_daily = [
            row
            for row in prepared.daily_rows[max(0, daily_end - 19) : daily_end]
            if row["timestamp"] + timedelta(days=1) <= signal_available_at
        ]
        if len(completed_daily) >= 19:
            values = calculate_provisional_bollinger(
                [row["close"] for row in completed_daily[-19:]],
                forming_close=forming["ohlcv"]["close"],
            )
            rows.append(
                [
                    forming["open_timestamp"],
                    _iso_z(signal_available_at),
                    False,
                    forming["source_candle_count"],
                    str(forming["ohlcv"]["close"]),
                    *[_packet_number(values[column]) for column in BOLLINGER_VALUE_COLUMNS],
                ]
            )
    return {
        "role": "mean_reversion_context",
        "timeframe": "1d",
        "source": "derived_completed_plus_causal_forming",
        "columns": BOLLINGER_COLUMNS,
        "rows": rows,
    }


def _forming_candle(
    *,
    raw_rows: list[dict[str, Any]],
    raw_timestamps: list[datetime],
    anchor: datetime,
    timeframe: str,
    signal_available_at: datetime,
) -> dict[str, Any] | None:
    delta = _timeframe_delta(timeframe)
    open_ts = _aligned_bucket_open(anchor=anchor, signal_timestamp=signal_available_at, timeframe_delta=delta)
    if open_ts is None:
        return None
    expected_close = open_ts + delta
    if signal_available_at >= expected_close:
        return None
    start_index = bisect_left(raw_timestamps, open_ts)
    latest_confirmed_open = signal_available_at - timedelta(minutes=5)
    end_index = bisect_right(raw_timestamps, latest_confirmed_open)
    source_rows = raw_rows[start_index:end_index]
    if not source_rows:
        return None
    return {
        "open_timestamp_raw": open_ts,
        "open_timestamp": _iso_z(open_ts),
        "partial_close_timestamp": _iso_z(signal_available_at),
        "expected_close_timestamp": _iso_z(expected_close),
        "source_candle_count": len(source_rows),
        "ohlcv": _aggregate_raw_ohlcv(source_rows),
    }


def _completed_candle_row(row: dict[str, Any], *, timeframe: str) -> list[Any]:
    close_ts = row["timestamp"] + _timeframe_delta(timeframe)
    return _row_to_packet_row(row) + [True, None, _iso_z(close_ts), _iso_z(close_ts)]


def _forming_candle_row(forming: dict[str, Any]) -> list[Any]:
    ohlcv = forming["ohlcv"]
    return [
        forming["open_timestamp"],
        str(ohlcv["open"]),
        str(ohlcv["high"]),
        str(ohlcv["low"]),
        str(ohlcv["close"]),
        str(ohlcv["volume"]),
        str(ohlcv["vol_ccy"]),
        str(ohlcv["vol_ccy_quote"]),
        0,
        False,
        forming["source_candle_count"],
        forming["partial_close_timestamp"],
        forming["expected_close_timestamp"],
    ]


def _aggregate_raw_ohlcv(rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    return {
        "open": _decimal(rows[0].get("open", 0)),
        "high": max(_decimal(row.get("high", 0)) for row in rows),
        "low": min(_decimal(row.get("low", 0)) for row in rows),
        "close": _decimal(rows[-1].get("close", 0)),
        "volume": sum((_decimal(row.get("volume", 0)) for row in rows), Decimal(0)),
        "vol_ccy": sum((_decimal(row.get("vol_ccy", row.get("volCcy", 0))) for row in rows), Decimal(0)),
        "vol_ccy_quote": sum(
            (_decimal(row.get("vol_ccy_quote", row.get("volCcyQuote", 0))) for row in rows),
            Decimal(0),
        ),
    }


def _prepare_feature_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared = [
        {**row, "timestamp": _utc(row.get("timestamp") or row.get("ts"))}
        for row in rows
        if int(row.get("confirm", 1)) == 1
    ]
    return sorted(prepared, key=lambda row: row["timestamp"])


def _prepare_oi_feature_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    for row in rows:
        if row.get("complete") is not True:
            continue
        available_at = row.get("available_at")
        if available_at in (None, ""):
            prepared.append({**row, "timestamp": _utc(row.get("timestamp") or row.get("ts"))})
            continue
        prepared.append(
            {
                **row,
                "timestamp": _utc(row.get("timestamp") or row.get("ts")),
                "available_at": _utc(available_at),
            }
        )
    return sorted(prepared, key=lambda row: (row.get("available_at") or row["timestamp"], row["timestamp"]))


def _open_interest_regime_snapshot(
    *,
    prepared: _PreparedData,
    asset: str,
    signal_available_at: datetime,
) -> dict[str, Any] | None:
    index = bisect_right(prepared.oi_feature_available_at, signal_available_at) - 1
    if index < 0:
        return None
    row = prepared.oi_feature_rows[index]
    return {
        "data_type": "feature_open_interest_regime",
        "source": "binance",
        "source_instrument": f"{asset.upper()}USDT",
        "timeframe": "15m",
        "timestamp": _iso_z(row["timestamp"]),
        "available_at": _iso_z(row["available_at"]),
        "complete": True,
        "source_window_start_ts": _iso_z(_utc(row["source_window_start_ts"])),
        "source_window_end_ts": _iso_z(_utc(row["source_window_end_ts"])),
        "source_row_count": int(row["source_row_count"]),
        "values": {
            column: _packet_number(row.get(column))
            for column in OI_FEATURE_VALUE_COLUMNS
        },
    }


def _stage1a_context_snapshot(
    *,
    prepared: _PreparedData,
    signal_available_at: datetime,
) -> dict[str, Any]:
    five_minute_rows = _completed_rows_as_of(
        rows=prepared.raw_rows,
        timestamps=prepared.raw_timestamps,
        timeframe="5m",
        signal_available_at=signal_available_at,
        max_rows=289,
    )
    daily_rows = _completed_rows_as_of(
        rows=prepared.daily_rows,
        timestamps=prepared.daily_timestamps,
        timeframe="1d",
        signal_available_at=signal_available_at,
        max_rows=21,
    )
    rows_by_timeframe = {
        timeframe: _completed_rows_as_of(
            rows=prepared.context_rows.get(timeframe, []),
            timestamps=prepared.context_timestamps.get(timeframe, []),
            timeframe=timeframe,
            signal_available_at=signal_available_at,
            max_rows=80,
        )
        for timeframe in DEFAULT_CONTEXT_TIMEFRAMES
    }

    values: dict[str, Any] = {name: None for name in STAGE1A_CONTEXT_VALUE_NAMES}
    five_minute_delta = _timeframe_delta("5m")
    for lookback in (12, 48, 288):
        values[f"5m_return_{lookback}_pct"] = _window_return_pct(
            five_minute_rows,
            lookback=lookback,
            delta=five_minute_delta,
        )
    for lookback in (48, 288):
        values[f"5m_realized_volatility_{lookback}_pct"] = _realized_volatility_pct(
            five_minute_rows,
            lookback=lookback,
            delta=five_minute_delta,
        )
        values[f"5m_range_position_{lookback}_pct"] = _range_position_pct(
            five_minute_rows,
            lookback=lookback,
            delta=five_minute_delta,
        )
        values[f"5m_trend_efficiency_{lookback}"] = _trend_efficiency(
            five_minute_rows,
            lookback=lookback,
            delta=five_minute_delta,
        )
    values["5m_volume_zscore_48"] = _volume_zscore(
        five_minute_rows,
        lookback=48,
        delta=five_minute_delta,
    )

    for timeframe, rows in rows_by_timeframe.items():
        delta = _timeframe_delta(timeframe)
        values[f"{timeframe}_completed_return_pct"] = _candle_return_pct(rows[-1]) if rows else None
        values[f"{timeframe}_realized_volatility_12_pct"] = _realized_volatility_pct(
            rows,
            lookback=12,
            delta=delta,
        )
        values[f"{timeframe}_range_position_12_pct"] = _range_position_pct(
            rows,
            lookback=12,
            delta=delta,
        )
        values[f"{timeframe}_trend_efficiency_12"] = _trend_efficiency(
            rows,
            lookback=12,
            delta=delta,
        )
        for period in (36, 576):
            values[f"{timeframe}_ema_{period}_slope_3_pct"] = _ema_slope_pct(
                rows,
                period=period,
                lookback=3,
                delta=delta,
            )

    daily_delta = _timeframe_delta("1d")
    for lookback in (3, 10, 20):
        values[f"1d_return_{lookback}_pct"] = _window_return_pct(
            daily_rows,
            lookback=lookback,
            delta=daily_delta,
        )
    values["1d_realized_volatility_20_pct"] = _realized_volatility_pct(
        daily_rows,
        lookback=20,
        delta=daily_delta,
    )

    source_windows = {
        "5m": _source_window_metadata(five_minute_rows, timeframe="5m", required_rows=289),
        **{
            timeframe: _source_window_metadata(rows, timeframe=timeframe, required_rows=13)
            for timeframe, rows in rows_by_timeframe.items()
        },
        "1d": _source_window_metadata(daily_rows, timeframe="1d", required_rows=21),
    }
    return {
        "schema_version": STAGE1A_CONTEXT_SCHEMA_VERSION,
        "available_at": _iso_z(signal_available_at),
        "complete": True,
        "source_windows": source_windows,
        "values": {name: _packet_number(values[name]) for name in STAGE1A_CONTEXT_VALUE_NAMES},
    }


def _completed_rows_as_of(
    *,
    rows: list[dict[str, Any]],
    timestamps: list[datetime],
    timeframe: str,
    signal_available_at: datetime,
    max_rows: int,
) -> list[dict[str, Any]]:
    if not rows or not timestamps:
        return []
    end = bisect_right(timestamps, signal_available_at - _timeframe_delta(timeframe))
    return rows[max(0, end - max_rows) : end]


def _source_window_metadata(
    rows: list[dict[str, Any]],
    *,
    timeframe: str,
    required_rows: int,
) -> dict[str, Any]:
    delta = _timeframe_delta(timeframe)
    contiguous_row_count = _contiguous_tail_count(rows, delta=delta)
    return {
        "timeframe": timeframe,
        "selected_row_count": len(rows),
        "contiguous_tail_row_count": contiguous_row_count,
        "required_row_count": required_rows,
        "warmup_complete": contiguous_row_count >= required_rows,
        "latest_available_at": _iso_z(rows[-1]["timestamp"] + delta) if rows else None,
    }


def _window_return_pct(
    rows: list[dict[str, Any]],
    *,
    lookback: int,
    delta: timedelta,
) -> Decimal | None:
    selected = _contiguous_tail(rows, required_rows=lookback + 1, delta=delta)
    if not selected:
        return None
    start = _decimal(selected[0].get("close"))
    end = _decimal(selected[-1].get("close"))
    if start == 0:
        return None
    return (end / start - 1) * Decimal(100)


def _realized_volatility_pct(
    rows: list[dict[str, Any]],
    *,
    lookback: int,
    delta: timedelta,
) -> float | None:
    selected = _contiguous_tail(rows, required_rows=lookback + 1, delta=delta)
    if not selected:
        return None
    closes = [_decimal(row.get("close")) for row in selected]
    if any(value == 0 for value in closes[:-1]):
        return None
    returns = [float((current / previous - 1) * Decimal(100)) for previous, current in zip(closes, closes[1:])]
    return pstdev(returns)


def _range_position_pct(
    rows: list[dict[str, Any]],
    *,
    lookback: int,
    delta: timedelta,
) -> Decimal | None:
    selected = _contiguous_tail(rows, required_rows=lookback, delta=delta)
    if not selected:
        return None
    low = min(_decimal(row.get("low")) for row in selected)
    high = max(_decimal(row.get("high")) for row in selected)
    close = _decimal(selected[-1].get("close"))
    if high == low:
        return Decimal(50)
    return (close - low) / (high - low) * Decimal(100)


def _volume_zscore(
    rows: list[dict[str, Any]],
    *,
    lookback: int,
    delta: timedelta,
) -> float | None:
    selected = _contiguous_tail(rows, required_rows=lookback, delta=delta)
    if not selected:
        return None
    volumes = [float(_decimal(row.get("volume", 0))) for row in selected]
    standard_deviation = pstdev(volumes)
    if standard_deviation == 0:
        return None
    return (volumes[-1] - mean(volumes)) / standard_deviation


def _trend_efficiency(
    rows: list[dict[str, Any]],
    *,
    lookback: int,
    delta: timedelta,
) -> Decimal | None:
    selected = _contiguous_tail(rows, required_rows=lookback + 1, delta=delta)
    if not selected:
        return None
    closes = [_decimal(row.get("close")) for row in selected]
    path = sum((abs(current - previous) for previous, current in zip(closes, closes[1:])), Decimal(0))
    if path == 0:
        return Decimal(0)
    return abs(closes[-1] - closes[0]) / path


def _candle_return_pct(row: dict[str, Any]) -> Decimal | None:
    open_ = _decimal(row.get("open"))
    close = _decimal(row.get("close"))
    if open_ == 0:
        return None
    return (close / open_ - 1) * Decimal(100)


def _ema_slope_pct(
    rows: list[dict[str, Any]],
    *,
    period: int,
    lookback: int,
    delta: timedelta,
) -> Decimal | None:
    selected = _contiguous_tail(rows, required_rows=lookback + 1, delta=delta)
    if not selected or not _ema_is_valid(selected[0], period) or not _ema_is_valid(selected[-1], period):
        return None
    start = _ema_value(selected[0], period)
    end = _ema_value(selected[-1], period)
    if start == 0:
        return None
    return (end / start - 1) * Decimal(100)


def _contiguous_tail(
    rows: list[dict[str, Any]],
    *,
    required_rows: int,
    delta: timedelta,
) -> list[dict[str, Any]]:
    if required_rows <= 0 or len(rows) < required_rows:
        return []
    selected = rows[-required_rows:]
    if any(current["timestamp"] - previous["timestamp"] != delta for previous, current in zip(selected, selected[1:])):
        return []
    return selected


def _contiguous_tail_count(rows: list[dict[str, Any]], *, delta: timedelta) -> int:
    if not rows:
        return 0
    count = 1
    for previous, current in zip(reversed(rows[:-1]), reversed(rows[1:])):
        if current["timestamp"] - previous["timestamp"] != delta:
            break
        count += 1
    return count


def _raw_row_to_mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, dict):
        return {**row, "timestamp": _utc(row.get("timestamp") or row.get("ts"))}
    return {
        "timestamp": _utc(getattr(row, "timestamp")),
        "open": getattr(row, "open"),
        "high": getattr(row, "high"),
        "low": getattr(row, "low"),
        "close": getattr(row, "close"),
        "volume": getattr(row, "volume", 0),
        "vol_ccy": getattr(row, "vol_ccy", 0),
        "vol_ccy_quote": getattr(row, "vol_ccy_quote", 0),
        "confirm": getattr(row, "confirm", 1),
    }


def _row_confirmed(row: Any) -> bool:
    return int(row.get("confirm", 1) if isinstance(row, dict) else getattr(row, "confirm", 1)) == 1


def _aligned_bucket_open(*, anchor: datetime, signal_timestamp: datetime, timeframe_delta: timedelta) -> datetime | None:
    if signal_timestamp < anchor:
        return None
    return anchor + ((signal_timestamp - anchor) // timeframe_delta) * timeframe_delta


def _timeframe_delta(timeframe: str) -> timedelta:
    value = timeframe.strip().lower()
    if value.endswith("m"):
        return timedelta(minutes=int(value[:-1]))
    if value.endswith("h"):
        return timedelta(hours=int(value[:-1]))
    if value.endswith("d"):
        return timedelta(days=int(value[:-1]))
    raise ValueError(f"Unsupported timeframe for point-in-time context: {timeframe}")


def _packet_number(value: Any) -> str | None:
    return None if value is None else str(value)


def _context_timeframes(parameters: dict[str, Any]) -> tuple[str, ...]:
    configured = parameters.get("context_timeframes")
    return tuple(str(item) for item in configured) if configured else DEFAULT_CONTEXT_TIMEFRAMES


def _with_v6_defaults(parameters: dict[str, Any] | None) -> dict[str, Any]:
    merged = dict(parameters or {})
    merged.setdefault("context_mode", V6_CONTEXT_MODE)
    merged.setdefault("context_timeframes", list(DEFAULT_CONTEXT_TIMEFRAMES))
    return merged
