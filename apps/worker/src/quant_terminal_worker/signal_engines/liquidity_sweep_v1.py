from __future__ import annotations

from bisect import bisect_left
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
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


DEFAULT_REFERENCE_WINDOW_HOURS = 12
DEFAULT_ATR_PERIOD = 14
DEFAULT_MIN_SWEEP_ATR = Decimal("0.2")
DEFAULT_COOLDOWN_HOURS = 2
DEFAULT_CONTEXT_BARS = 144
CANDLE_COLUMNS = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]


def generate_training_signals(context: EngineTrainingContext) -> EngineTrainingOutput:
    raw_5m = context.market_data_reader.get_candles(asset=context.asset, timeframe="5m", origin="raw")
    packets, generated_packet_count = generate_liquidity_sweep_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        start=context.start,
        end=context.end,
        parameters=context.parameters,
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
    raw_5m = context.market_data_reader.get_candles(asset=context.asset, timeframe="5m", origin="raw")
    if not raw_5m:
        raise ValueError(f"Raw candle data is empty for {context.asset}. Update local candle data first.")
    packet = scan_liquidity_sweep_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
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


def generate_liquidity_sweep_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
    packet_sink: Any | None = None,
    packet_chunk_size: int = 500,
) -> tuple[list[dict[str, Any]], int]:
    del workspace_root
    rows = _prepare_rows(raw_5m)
    if not rows:
        raise ValueError(f"Liquidity sweep engine requires raw 5m candle rows for {asset}.")
    timestamps = [row["timestamp"] for row in rows]

    packets: list[dict[str, Any]] = []
    buffered_packets: list[dict[str, Any]] = []
    generated_packet_count = 0
    last_emitted_at = _optional_seed_timestamp(parameters)
    cooldown = timedelta(hours=int(parameters.get("cooldown_hours", DEFAULT_COOLDOWN_HOURS)))
    reference_window_hours = int(parameters.get("reference_window_hours", DEFAULT_REFERENCE_WINDOW_HOURS))

    for index, row in enumerate(rows):
        timestamp = row["timestamp"]
        if timestamp < start:
            continue
        if timestamp > end:
            break
        history_start_index = bisect_left(
            timestamps,
            timestamp - timedelta(hours=reference_window_hours),
            0,
            index,
        )
        packet = _scan_row(
            asset=asset,
            instrument=instrument,
            rows=rows,
            index=index,
            history_start_index=history_start_index,
            parameters=parameters,
        )
        if packet is None:
            continue
        if last_emitted_at is not None and (timestamp - last_emitted_at) < cooldown:
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


def _optional_seed_timestamp(parameters: dict[str, Any]) -> datetime | None:
    value = parameters.get("_dedupe_seed_timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC)
    if isinstance(value, str) and value:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return None


def scan_liquidity_sweep_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    del workspace_root
    rows = _prepare_rows(raw_5m)
    if not rows:
        raise ValueError(f"Liquidity sweep engine requires raw 5m candle rows for {asset}.")
    timestamps = [row["timestamp"] for row in rows]
    index = len(rows) - 1
    reference_window_hours = int(parameters.get("reference_window_hours", DEFAULT_REFERENCE_WINDOW_HOURS))
    history_start_index = bisect_left(
        timestamps,
        rows[index]["timestamp"] - timedelta(hours=reference_window_hours),
        0,
        index,
    )
    return _scan_row(
        asset=asset,
        instrument=instrument,
        rows=rows,
        index=index,
        history_start_index=history_start_index,
        parameters=parameters,
    )


def _scan_row(
    *,
    asset: str,
    instrument: str,
    rows: list[dict[str, Any]],
    index: int,
    history_start_index: int,
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    row = rows[index]
    timestamp = row["timestamp"]
    reference_window_hours = int(parameters.get("reference_window_hours", DEFAULT_REFERENCE_WINDOW_HOURS))
    atr_period = int(parameters.get("atr_period", DEFAULT_ATR_PERIOD))
    min_sweep_atr = Decimal(str(parameters.get("min_sweep_atr", DEFAULT_MIN_SWEEP_ATR)))
    context_bars = int(parameters.get("context_bars", DEFAULT_CONTEXT_BARS))

    window_start = timestamp - timedelta(hours=reference_window_hours)
    history = rows[history_start_index:index]
    if len(history) < atr_period:
        return None

    atr = _atr(history[-atr_period:])
    if atr <= 0:
        return None

    reference_high = max(previous["high"] for previous in history)
    reference_low = min(previous["low"] for previous in history)
    current_high = row["high"]
    current_low = row["low"]
    current_close = row["close"]

    high_distance = current_high - reference_high
    low_distance = reference_low - current_low
    threshold = min_sweep_atr * atr

    high_triggered = current_high > reference_high and high_distance >= threshold
    low_triggered = current_low < reference_low and low_distance >= threshold
    if high_triggered and low_triggered:
        return None
    if not high_triggered and not low_triggered:
        return None

    event_type = "HIGH_SWEEP" if high_triggered else "LOW_SWEEP"
    trigger_price = current_high if high_triggered else current_low
    sweep_distance = high_distance if high_triggered else low_distance
    close_location_pct = _close_location_pct(current_low, current_high, current_close)
    chart_rows = rows[max(0, index - context_bars + 1) : index + 1]
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset,
        "instrument": instrument,
        "timestamp": _iso_z(timestamp),
        "active_timeframes": ["5m"],
        "evidence": {
            "pattern": "liquidity_sweep_event",
            "event_type": event_type,
            "reference_window_hours": reference_window_hours,
            "reference_level": _decimal_to_str(reference_high if high_triggered else reference_low),
            "reference_start_ts": _iso_z(window_start),
            "reference_end_ts": _iso_z(history[-1]["timestamp"]),
            "trigger_price": _decimal_to_str(trigger_price),
            "trigger_candle_close": _decimal_to_str(current_close),
            "atr_14": _decimal_to_str(atr),
            "sweep_distance": _decimal_to_str(sweep_distance),
            "sweep_distance_atr": _decimal_to_str(sweep_distance / atr),
            "close_location_pct": _decimal_to_str(close_location_pct),
            "cooldown_hours": int(parameters.get("cooldown_hours", DEFAULT_COOLDOWN_HOURS)),
            "level_id": f"{event_type.lower()}-{timestamp.strftime('%Y%m%dT%H%M%SZ')}",
        },
        "charts": {
            "5m": {
                "role": "trigger_context",
                "columns": list(CANDLE_COLUMNS),
                "completed_candles": [_chart_row(candle) for candle in chart_rows],
            }
        },
    }
    validate_signal_packet(packet)
    return packet


def _prepare_rows(raw_5m: list[MarketDataCandle]) -> list[dict[str, Any]]:
    rows = [
        {
            "timestamp": candle.timestamp.astimezone(UTC) if candle.timestamp.tzinfo else candle.timestamp.replace(tzinfo=UTC),
            "open": Decimal(str(candle.open)),
            "high": Decimal(str(candle.high)),
            "low": Decimal(str(candle.low)),
            "close": Decimal(str(candle.close)),
            "volume": Decimal(str(candle.volume)),
            "vol_ccy": Decimal(str(candle.vol_ccy)),
            "vol_ccy_quote": Decimal(str(candle.vol_ccy_quote)),
            "confirm": int(candle.confirm),
        }
        for candle in raw_5m
        if int(candle.confirm) == 1
    ]
    rows.sort(key=lambda row: row["timestamp"])
    return rows


def _atr(rows: list[dict[str, Any]]) -> Decimal:
    tr_values: list[Decimal] = []
    previous_close: Decimal | None = None
    for row in rows:
        high = row["high"]
        low = row["low"]
        close = row["close"]
        if previous_close is None:
            tr = high - low
        else:
            tr = max(high - low, abs(high - previous_close), abs(low - previous_close))
        tr_values.append(tr)
        previous_close = close
    if not tr_values:
        return Decimal("0")
    return sum(tr_values) / Decimal(len(tr_values))


def _close_location_pct(low: Decimal, high: Decimal, close: Decimal) -> Decimal:
    span = high - low
    if span <= 0:
        return Decimal("50")
    return (close - low) / span * Decimal("100")


def _chart_row(row: dict[str, Any]) -> list[str]:
    return [
        _iso_z(row["timestamp"]),
        _decimal_to_str(row["open"]),
        _decimal_to_str(row["high"]),
        _decimal_to_str(row["low"]),
        _decimal_to_str(row["close"]),
        _decimal_to_str(row["volume"]),
        _decimal_to_str(row["vol_ccy"]),
        _decimal_to_str(row["vol_ccy_quote"]),
        str(int(row["confirm"])),
    ]


def _decimal_to_str(value: Decimal | int | float) -> str:
    decimal_value = value if isinstance(value, Decimal) else Decimal(str(value))
    normalized = decimal_value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized.quantize(Decimal(1)), "f")
    return format(normalized, "f").rstrip("0").rstrip(".")


def _iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
