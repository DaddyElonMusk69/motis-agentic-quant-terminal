from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from quant_terminal_sdk.engine_contracts import (
    LiveSignalScanResult,
    SignalPacket,
    TrainingSignalGenerationResult,
    validate_signal_packet,
)
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.multires_tcn_runtime import (
    BRANCH_CONFIG,
    align_market_rows,
    build_sequence_stores,
    load_model_artifact,
    ready_decision_indices,
    score_decisions,
)
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    EngineTrainingOutput,
)


ENGINE_ID = "btc_multires_opportunity_v1"
MODEL_RELATIVE_PATH = Path("artifacts/signal_engine/models/btc_multires_opportunity_v1.pt")
MODEL_SHA256 = "b17fb7aa7985755a4f3ce74c45fd6fcfb62c2b227709fcb331670a6d798dcea2"
DEFAULT_DEDUPE_WINDOW_MINUTES = 480
DEFAULT_CONTEXT_BARS = 96
BASE_INTERVAL = timedelta(minutes=5)
CANDLE_COLUMNS = ["open_ts", "close_ts", "o", "h", "l", "c", "quote_volume", "complete"]
OI_COLUMNS = [
    "open_ts",
    "close_ts",
    "contract_oi",
    "notional_oi",
    "toptrader_position_ratio",
    "general_ratio",
    "taker_ratio",
    "complete",
]


def generate_training_signals(context: EngineTrainingContext) -> EngineTrainingOutput:
    raw_5m = context.market_data_reader.get_candles(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
    )
    raw_metrics = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="futures_metrics",
    )
    packets, generated_count, coverage_end = generate_multires_packets(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_oi=raw_metrics,
        start=context.start,
        end=context.end,
        parameters=context.parameters,
        packet_sink=context.packet_sink,
        packet_chunk_size=context.packet_chunk_size,
    )
    return EngineTrainingOutput(
        result=TrainingSignalGenerationResult(
            status="appended" if generated_count else "noop",
            generated_packet_count=generated_count,
            appended_packet_count=0,
            raw_candle_end_ts=_iso_z(context.raw_candle_end),
            scan_coverage_end_ts=_iso_z(coverage_end) if coverage_end is not None else None,
            packet_refs=[],
        ),
        packets=packets,
    )


def scan_live_signal(context: EngineLiveScanContext) -> LiveSignalScanResult:
    raw_5m = context.market_data_reader.get_candles(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
    )
    raw_metrics = context.market_data_reader.get_rows(
        asset=context.asset,
        timeframe="5m",
        origin="raw",
        data_type="futures_metrics",
    )
    packet = scan_multires_latest(
        workspace_root=context.workspace_root,
        asset=context.asset,
        instrument=context.instrument,
        raw_5m=raw_5m,
        raw_oi=raw_metrics,
        parameters=context.parameters,
    )
    if packet is None:
        return LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest_confirmed_multires_state_did_not_trigger",
        )
    return LiveSignalScanResult(
        status="fresh_signal",
        source="live_parquet_snapshot",
        signal=SignalPacket.from_mapping(packet),
    )


def generate_multires_packets(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
    start: datetime,
    end: datetime,
    parameters: dict[str, Any],
    packet_sink: Any | None = None,
    packet_chunk_size: int = 500,
) -> tuple[list[dict[str, Any]], int, datetime | None]:
    _require_btc(asset)
    frame = align_market_rows(raw_5m=raw_5m, raw_oi=raw_oi)
    if frame.empty:
        return [], 0, None
    stores = build_sequence_stores(frame)
    indices = ready_decision_indices(
        source=frame,
        stores=stores,
        start=_timestamp(start),
        end=_timestamp(end),
    )
    coverage_end = min(_timestamp(end), frame.index[-1])
    if not len(indices):
        return [], 0, coverage_end.to_pydatetime()
    model, artifact = _load_verified_model(workspace_root)
    source_ns = frame.index.astype("int64").to_numpy(dtype=np.int64)
    scores = score_decisions(
        model=model,
        decision_ns=source_ns[indices] + int(pd.Timedelta(minutes=5).value),
        stores=stores,
    )
    threshold = float(artifact["score_threshold"])
    dedupe = timedelta(
        minutes=int(parameters.get("dedupe_window_minutes", DEFAULT_DEDUPE_WINDOW_MINUTES))
    )
    last_emitted = _seed_timestamp(parameters)
    context_bars = int(parameters.get("context_bars", DEFAULT_CONTEXT_BARS))
    packets = []
    buffer = []
    generated_count = 0
    for index, score in zip(indices, scores):
        if float(score) < threshold:
            continue
        packet_timestamp = frame.index[index].to_pydatetime()
        if last_emitted is not None and packet_timestamp - last_emitted < dedupe:
            continue
        packet = build_packet(
            asset=asset,
            instrument=instrument,
            frame=frame,
            index=int(index),
            context_bars=context_bars,
        )
        generated_count += 1
        last_emitted = packet_timestamp
        if callable(packet_sink):
            buffer.append(packet)
            if len(buffer) >= max(1, int(packet_chunk_size)):
                packet_sink(buffer)
                buffer = []
        else:
            packets.append(packet)
    if buffer and callable(packet_sink):
        packet_sink(buffer)
    return packets, generated_count, coverage_end.to_pydatetime()


def scan_multires_latest(
    *,
    workspace_root: Path,
    asset: str,
    instrument: str,
    raw_5m: list[MarketDataCandle],
    raw_oi: list[dict[str, Any]],
    parameters: dict[str, Any],
) -> dict[str, Any] | None:
    _require_btc(asset)
    frame = align_market_rows(raw_5m=raw_5m, raw_oi=raw_oi)
    if frame.empty:
        return None
    stores = build_sequence_stores(frame)
    latest = len(frame) - 1
    decision_ns = int(frame.index[latest].value + pd.Timedelta(minutes=5).value)
    if not all(store.has_history(decision_ns) for store in stores.values()):
        return None
    model, artifact = _load_verified_model(workspace_root)
    score = float(
        score_decisions(
            model=model,
            decision_ns=np.asarray([decision_ns], dtype=np.int64),
            stores=stores,
            batch_size=1,
        )[0]
    )
    if score < float(artifact["score_threshold"]):
        return None
    return build_packet(
        asset=asset,
        instrument=instrument,
        frame=frame,
        index=latest,
        context_bars=int(parameters.get("context_bars", DEFAULT_CONTEXT_BARS)),
    )


def build_packet(
    *,
    asset: str,
    instrument: str,
    frame: pd.DataFrame,
    index: int,
    context_bars: int,
) -> dict[str, Any]:
    row = frame.iloc[index]
    source_open = frame.index[index]
    available_at = source_open + pd.Timedelta(minutes=5)
    context = frame.iloc[max(0, index - context_bars + 1) : index + 1]
    evidence = {
        "engine": ENGINE_ID,
        "pattern": "multiresolution_opportunity_state",
        "event_type": "MULTIRES_OPPORTUNITY_STATE",
        "trigger_timeframe": "5m",
        "model_version": "1.0",
        "model_artifact_sha256": MODEL_SHA256,
        "model_context_timeframes": list(BRANCH_CONFIG),
        "training_weighting": "capped_proportional_48h",
        "training_objective": "signal_level_opportunity_state",
        "signal_candle_open_ts": _iso_z(source_open.to_pydatetime()),
        "signal_candle_close_ts": _iso_z(available_at.to_pydatetime()),
        "signal_available_at": _iso_z(available_at.to_pydatetime()),
        "dedupe_window_minutes": DEFAULT_DEDUPE_WINDOW_MINUTES,
        "reference_price": _number(row["close"], 8),
        "trigger_candle_close": _number(row["close"], 8),
        **_neutral_features(frame=frame, index=index),
    }
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": asset.upper(),
        "instrument": instrument,
        "timestamp": _iso_z(source_open.to_pydatetime()),
        "active_timeframes": list(BRANCH_CONFIG),
        "evidence": evidence,
        "charts": {
            "5m": {
                "role": "trigger_context",
                "timeframe": "5m",
                "columns": CANDLE_COLUMNS,
                "candles": [_candle_row(timestamp, item) for timestamp, item in context.iterrows()],
            },
            "open_interest_5m": {
                "role": "positioning_context",
                "timeframe": "5m",
                "columns": OI_COLUMNS,
                "rows": [_oi_row(timestamp, item) for timestamp, item in context.iterrows()],
            },
        },
    }
    validate_signal_packet(packet)
    return packet


def _neutral_features(*, frame: pd.DataFrame, index: int) -> dict[str, str]:
    close = frame["close"].to_numpy(dtype=float)
    oi = frame["sum_open_interest"].to_numpy(dtype=float)
    quote_volume = frame["vol_ccy_quote"].to_numpy(dtype=float)
    return {
        "price_return_4h_pct": _number(_change_pct(close, index, 48), 6),
        "price_return_24h_pct": _number(_change_pct(close, index, 288), 6),
        "oi_change_4h_pct": _number(_change_pct(oi, index, 48), 6),
        "oi_change_24h_pct": _number(_change_pct(oi, index, 288), 6),
        "quote_volume_ratio_4h": _number(_median_ratio(quote_volume, index, 48), 6),
        "toptrader_position_ratio": _number(
            frame.iloc[index]["sum_toptrader_long_short_ratio"], 6
        ),
        "general_long_short_ratio": _number(frame.iloc[index]["count_long_short_ratio"], 6),
        "taker_long_short_ratio": _number(
            frame.iloc[index]["sum_taker_long_short_vol_ratio"], 6
        ),
    }


def _change_pct(values: np.ndarray, index: int, bars: int) -> float:
    if index < bars or not np.isfinite(values[index - bars]) or values[index - bars] == 0:
        return 0.0
    return (values[index] / values[index - bars] - 1.0) * 100.0


def _median_ratio(values: np.ndarray, index: int, bars: int) -> float:
    start = max(0, index - bars + 1)
    median = float(np.nanmedian(values[start : index + 1]))
    return float(values[index] / median) if median else 0.0


def _candle_row(timestamp: pd.Timestamp, row: pd.Series) -> list[Any]:
    return [
        _iso_z(timestamp.to_pydatetime()),
        _iso_z((timestamp + pd.Timedelta(minutes=5)).to_pydatetime()),
        _number(row["open"], 8),
        _number(row["high"], 8),
        _number(row["low"], 8),
        _number(row["close"], 8),
        _number(row["vol_ccy_quote"], 4),
        True,
    ]


def _oi_row(timestamp: pd.Timestamp, row: pd.Series) -> list[Any]:
    return [
        _iso_z(timestamp.to_pydatetime()),
        _iso_z((timestamp + pd.Timedelta(minutes=5)).to_pydatetime()),
        _number(row["sum_open_interest"], 4),
        _number(row["sum_open_interest_value"], 4),
        _number(row["sum_toptrader_long_short_ratio"], 6),
        _number(row["count_long_short_ratio"], 6),
        _number(row["sum_taker_long_short_vol_ratio"], 6),
        True,
    ]


def _load_verified_model(workspace_root: Path) -> tuple[Any, dict[str, Any]]:
    path = workspace_root / MODEL_RELATIVE_PATH
    if not path.is_file():
        path = Path.cwd() / MODEL_RELATIVE_PATH
    if not path.is_file():
        raise ValueError(f"multi-resolution model artifact is missing: {path}")
    if hashlib.sha256(path.read_bytes()).hexdigest() != MODEL_SHA256:
        raise ValueError("multi-resolution model artifact hash mismatch")
    return load_model_artifact(path.resolve())


def _require_btc(asset: str) -> None:
    if asset.upper() != "BTC":
        raise ValueError(f"{ENGINE_ID} is trained only for BTC")


def _seed_timestamp(parameters: dict[str, Any]) -> datetime | None:
    value = parameters.get("_dedupe_seed_timestamp")
    return _timestamp(value).to_pydatetime() if value else None


def _timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    return timestamp.tz_convert("UTC") if timestamp.tz else timestamp.tz_localize("UTC")


def _number(value: Any, places: int) -> str:
    number = float(value)
    return f"{number:.{places}f}"


def _iso_z(value: datetime) -> str:
    timestamp = value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
