from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

import pyarrow.parquet as pq


@dataclass(frozen=True, slots=True)
class MarketDataCandle:
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vol_ccy: Decimal
    vol_ccy_quote: Decimal
    confirm: int


class CandleRefRepository(Protocol):
    def get_candle_ref(
        self,
        *,
        asset: str,
        timeframe: str,
        origin: str,
        data_type: str = "candles",
    ) -> dict[str, Any] | None:
        ...


class MarketDataReader:
    def __init__(self, *, repository: CandleRefRepository, workspace_root: str | Path) -> None:
        self.repository = repository
        self.workspace_root = Path(workspace_root)

    def get_candles(
        self,
        *,
        asset: str,
        timeframe: str,
        origin: str,
        data_type: str = "candles",
        start: str | datetime | None = None,
        end: str | datetime | None = None,
        confirmed_only: bool = True,
    ) -> list[MarketDataCandle]:
        ref = self.repository.get_candle_ref(
            asset=asset.upper(),
            timeframe=timeframe,
            origin=origin,
            data_type=data_type,
        )
        if ref is None:
            raise ValueError(
                f"Canonical {origin} {data_type} data is missing for {asset.upper()} {timeframe}."
            )
        return read_candles_from_ref(
            ref,
            workspace_root=self.workspace_root,
            start=start,
            end=end,
            confirmed_only=confirmed_only,
        )


def read_candles_from_ref(
    ref: dict[str, Any],
    *,
    workspace_root: str | Path,
    start: str | datetime | None = None,
    end: str | datetime | None = None,
    confirmed_only: bool = True,
) -> list[MarketDataCandle]:
    if ref.get("storage_backend") != "parquet":
        raise ValueError(f"Canonical market data ref is not parquet-backed: {ref.get('dataset_id')}")

    storage_uri = _resolve_storage_uri(workspace_root=Path(workspace_root), value=ref["storage_uri"])
    start_ts = _coerce_optional_datetime(start)
    end_ts = _coerce_optional_datetime(end)
    rows_by_timestamp: dict[datetime, MarketDataCandle] = {}

    for file in sorted(storage_uri.glob("year=*/month=*/data.parquet")):
        for row in pq.read_table(file).to_pylist():
            candle = _coerce_candle(row)
            if confirmed_only and candle.confirm != 1:
                continue
            if start_ts is not None and candle.timestamp < start_ts:
                continue
            if end_ts is not None and candle.timestamp > end_ts:
                continue
            rows_by_timestamp[candle.timestamp] = candle

    return [rows_by_timestamp[timestamp] for timestamp in sorted(rows_by_timestamp)]


def _resolve_storage_uri(*, workspace_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else workspace_root / path


def _coerce_candle(row: dict[str, Any]) -> MarketDataCandle:
    return MarketDataCandle(
        timestamp=_coerce_datetime(row.get("timestamp") or row.get("ts")),
        open=_coerce_decimal(row["open"]),
        high=_coerce_decimal(row["high"]),
        low=_coerce_decimal(row["low"]),
        close=_coerce_decimal(row["close"]),
        volume=_coerce_decimal(row.get("volume", 0)),
        vol_ccy=_coerce_decimal(row.get("vol_ccy", row.get("volCcy", 0))),
        vol_ccy_quote=_coerce_decimal(row.get("vol_ccy_quote", row.get("volCcyQuote", 0))),
        confirm=int(row.get("confirm", 1)),
    )


def _coerce_optional_datetime(value: str | datetime | None) -> datetime | None:
    return _coerce_datetime(value) if value is not None else None


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("missing candle timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _coerce_decimal(value: Any) -> Decimal:
    return Decimal(str(value))
