from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from quant_terminal_sdk.market_data import MarketDataReference
from quant_terminal_sdk.parquet_store import write_candles


class OKXCandleAdapter(Protocol):
    def market_candles(
        self,
        inst_id: str,
        *,
        bar: str,
        limit: int,
        after: str | None = None,
    ) -> dict[str, Any]:
        ...


@dataclass(frozen=True, slots=True)
class CandleIngestionResult:
    reference: MarketDataReference
    path: Path
    row_count: int
    registration: dict[str, Any]


def normalize_okx_candle(candle: list[Any] | dict[str, Any]) -> dict[str, Any]:
    if isinstance(candle, dict):
        timestamp = candle.get("ts") or candle.get("timestamp")
        open_price = candle.get("o") or candle.get("open")
        high = candle.get("h") or candle.get("high")
        low = candle.get("l") or candle.get("low")
        close = candle.get("c") or candle.get("close")
        volume = candle.get("vol") or candle.get("volume")
        vol_ccy = candle.get("volCcy") or candle.get("vol_ccy")
        vol_ccy_quote = candle.get("volCcyQuote") or candle.get("vol_ccy_quote")
        confirm = candle.get("confirm")
    else:
        timestamp, open_price, high, low, close, volume = candle[:6]
        vol_ccy = candle[6] if len(candle) > 6 else None
        vol_ccy_quote = candle[7] if len(candle) > 7 else None
        confirm = candle[8] if len(candle) > 8 else None

    row = {
        "timestamp": _timestamp_ms_to_iso(str(timestamp)),
        "open": float(open_price),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume),
    }
    if vol_ccy is not None:
        row["vol_ccy"] = float(vol_ccy)
    if vol_ccy_quote is not None:
        row["vol_ccy_quote"] = float(vol_ccy_quote)
    if confirm is not None:
        row["confirm"] = int(confirm)
    return row


def ingest_okx_candles(
    *,
    adapter: OKXCandleAdapter,
    root: Path,
    inst_id: str,
    asset: str,
    timeframe: str,
    year: int,
    month: int,
    limit: int,
    ingestion_version: str,
) -> CandleIngestionResult:
    payload = adapter.market_candles(inst_id, bar=timeframe, limit=limit)
    rows = [normalize_okx_candle(candle) for candle in payload.get("data", [])]
    if not rows:
        raise ValueError("OKX candle ingestion returned no rows")

    dataset_id = f"okx-{inst_id}-candles-{timeframe}-{year:04d}-{month:02d}-{ingestion_version}"
    reference = MarketDataReference(
        dataset_id=dataset_id,
        source_id="okx",
        asset=asset,
        instrument=inst_id,
        data_type="candles",
        timeframe=timeframe,
        storage_backend="parquet",
    )
    path = write_candles(root=root, reference=reference, year=year, month=month, rows=rows)
    registration = {
        "dataset_id": reference.dataset_id,
        "source_id": reference.source_id,
        "asset": reference.asset,
        "instrument": reference.instrument,
        "data_type": reference.data_type,
        "timeframe": reference.timeframe,
        "data_origin": "raw",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "storage_backend": reference.storage_backend,
        "storage_uri": str(path),
        "schema_descriptor": {
            "columns": ["timestamp", "open", "high", "low", "close", "volume"],
            "format": "parquet",
        },
        "quality_status": "ingested",
        "ingestion_version": ingestion_version,
    }
    return CandleIngestionResult(
        reference=reference,
        path=path,
        row_count=len(rows),
        registration=registration,
    )


def _timestamp_ms_to_iso(value: str) -> str:
    timestamp = datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    return timestamp.replace(microsecond=0).isoformat().replace("+00:00", "Z")
