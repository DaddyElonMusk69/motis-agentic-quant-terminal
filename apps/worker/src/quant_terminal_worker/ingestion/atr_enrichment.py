from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from typing import Any

from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_worker.ingestion.binance_funding import (
    _coerce_datetime,
    _dedupe_sort_rows,
    _read_dataset_rows,
    _timeframe_delta,
    _to_iso,
    _write_dataset_rows,
)


DATA_TYPE = "technical_indicator_atr"
SCHEMA_VERSION = "technical-indicator-atr.v1"
DEFAULT_START_DATE = date(2023, 1, 1)
DEFAULT_SOURCE_TIMEFRAME = "5m"
DEFAULT_TIMEFRAMES = ("1h", "2h", "4h")
DEFAULT_PERIOD = 14
ATR_COLUMNS = [
    "timestamp",
    "interval_end",
    "available_at",
    "symbol",
    "timeframe",
    "period",
    "method",
    "source_timeframe",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "true_range",
    "atr",
    "atr_pct",
    "warmup_complete",
    "complete",
    "confirm",
]


def build_atr_rows(
    *,
    raw_rows: list[dict[str, Any]],
    timeframe: str,
    period: int = DEFAULT_PERIOD,
    source_timeframe: str = DEFAULT_SOURCE_TIMEFRAME,
    start: datetime | None = None,
    end: datetime | None = None,
) -> list[dict[str, Any]]:
    if period <= 0:
        raise ValueError("period must be positive")
    htf_rows = _build_complete_candle_buckets(
        raw_rows=raw_rows,
        timeframe=timeframe,
        source_timeframe=source_timeframe,
    )
    if not htf_rows:
        return []

    true_ranges: list[float] = []
    atr_values: list[float | None] = []
    previous_close: float | None = None
    previous_atr: float | None = None
    for index, row in enumerate(htf_rows):
        high = float(row["high"])
        low = float(row["low"])
        true_range = high - low if previous_close is None else max(
            high - low,
            abs(high - previous_close),
            abs(low - previous_close),
        )
        true_ranges.append(true_range)
        if index == period - 1:
            previous_atr = sum(true_ranges[:period]) / period
            atr_values.append(previous_atr)
        elif index >= period and previous_atr is not None:
            previous_atr = ((previous_atr * (period - 1)) + true_range) / period
            atr_values.append(previous_atr)
        else:
            atr_values.append(None)
        previous_close = float(row["close"])

    start_dt = _coerce_datetime(start) if start is not None else None
    end_dt = _coerce_datetime(end) if end is not None else None
    output: list[dict[str, Any]] = []
    for row, true_range, atr in zip(htf_rows, true_ranges, atr_values):
        timestamp = _coerce_datetime(row["timestamp"])
        if start_dt is not None and timestamp < start_dt:
            continue
        if end_dt is not None and timestamp > end_dt:
            continue
        interval_end = _coerce_datetime(row["interval_end"])
        close = float(row["close"])
        output.append(
            {
                "timestamp": _to_iso(timestamp),
                "interval_end": _to_iso(interval_end),
                "available_at": _to_iso(interval_end),
                "symbol": str(row.get("symbol") or "").upper(),
                "timeframe": timeframe,
                "period": period,
                "method": "wilder",
                "source_timeframe": source_timeframe,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": close,
                "volume": float(row.get("volume") or 0.0),
                "true_range": true_range,
                "atr": atr,
                "atr_pct": (atr / close * 100.0) if atr is not None and close else None,
                "warmup_complete": atr is not None,
                "complete": True,
                "confirm": 1,
            }
        )
    return output


def enrich_atr_datasets(
    *,
    repository: Any,
    asset: str | None = None,
    timeframes: tuple[str, ...] = DEFAULT_TIMEFRAMES,
    period: int = DEFAULT_PERIOD,
    source_timeframe: str = DEFAULT_SOURCE_TIMEFRAME,
    start_date: str | date = DEFAULT_START_DATE,
    as_of: datetime | None = None,
    target_root: Path = Path(".data/market-data"),
) -> dict[str, Any]:
    requested_asset = asset.upper() if asset else None
    refs = [
        row
        for row in repository.list_refs()
        if row.get("data_type") == "candles"
        and row.get("data_origin") == "raw"
        and row.get("timeframe") == source_timeframe
        and (requested_asset is None or row.get("asset") == requested_asset)
    ]
    results: list[dict[str, Any]] = []
    for registration in refs:
        for timeframe in timeframes:
            results.append(
                enrich_atr_dataset(
                    source_registration=registration,
                    repository=repository,
                    timeframe=timeframe,
                    period=period,
                    source_timeframe=source_timeframe,
                    start_date=start_date,
                    as_of=as_of,
                    target_root=target_root,
                )
            )
    enriched = [item for item in results if item.get("status") == "enriched"]
    return {
        "status": "enriched" if enriched else "noop",
        "asset": requested_asset,
        "data_type": DATA_TYPE,
        "period": period,
        "timeframes": list(timeframes),
        "dataset_count": len(enriched),
        "datasets": results,
    }


def enrich_atr_dataset(
    *,
    source_registration: dict[str, Any],
    repository: Any,
    timeframe: str,
    period: int,
    source_timeframe: str,
    start_date: str | date,
    as_of: datetime | None,
    target_root: Path,
) -> dict[str, Any]:
    raw_rows = _read_dataset_rows(Path(source_registration["storage_uri"]))
    start = datetime.combine(_coerce_date(start_date), datetime.min.time(), tzinfo=UTC)
    target = _coerce_datetime(as_of) if as_of is not None else None
    rows = build_atr_rows(
        raw_rows=raw_rows,
        timeframe=timeframe,
        period=period,
        source_timeframe=source_timeframe,
        start=start,
        end=target,
    )
    if not rows:
        return {
            "dataset_id": _dataset_id(source_registration, timeframe=timeframe, period=period),
            "status": "skipped",
            "reason": "empty_atr_window",
        }
    storage_uri = _storage_uri(
        target_root=target_root,
        source_id=str(source_registration.get("source_id") or "local"),
        asset=str(source_registration["asset"]),
        timeframe=timeframe,
    )
    _write_dataset_rows(storage_uri, rows)
    registration = _registration(
        source_registration=source_registration,
        storage_uri=storage_uri,
        rows=rows,
        timeframe=timeframe,
        period=period,
        source_timeframe=source_timeframe,
    )
    repository.upsert_ref(registration)
    return _summary(registration, status="enriched")


def _build_complete_candle_buckets(
    *,
    raw_rows: list[dict[str, Any]],
    timeframe: str,
    source_timeframe: str,
) -> list[dict[str, Any]]:
    source_delta = _timeframe_delta(source_timeframe)
    target_delta = _timeframe_delta(timeframe)
    source_seconds = int(source_delta.total_seconds())
    target_seconds = int(target_delta.total_seconds())
    if source_seconds <= 0 or target_seconds < source_seconds or target_seconds % source_seconds:
        raise ValueError(f"cannot derive {timeframe} from {source_timeframe}")
    expected_count = target_seconds // source_seconds
    buckets: dict[datetime, list[dict[str, Any]]] = defaultdict(list)
    for row in _dedupe_sort_rows(raw_rows):
        if int(row.get("confirm", 1)) != 1:
            continue
        timestamp = _coerce_datetime(row["timestamp"])
        bucket_start = _floor_to_timeframe(timestamp, target_delta)
        buckets[bucket_start].append(row)

    output: list[dict[str, Any]] = []
    for bucket_start in sorted(buckets):
        bucket = sorted(buckets[bucket_start], key=lambda item: item["timestamp"])
        if len(bucket) != expected_count:
            continue
        expected_timestamps = {
            _to_iso(bucket_start + source_delta * index)
            for index in range(expected_count)
        }
        actual_timestamps = {_to_iso(_coerce_datetime(item["timestamp"])) for item in bucket}
        if actual_timestamps != expected_timestamps:
            continue
        row = {
            "timestamp": _to_iso(bucket_start),
            "interval_end": _to_iso(bucket_start + target_delta),
            "symbol": str(bucket[-1].get("symbol") or "").upper(),
            "open": float(bucket[0]["open"]),
            "high": max(float(item["high"]) for item in bucket),
            "low": min(float(item["low"]) for item in bucket),
            "close": float(bucket[-1]["close"]),
            "volume": sum(float(item.get("volume") or item.get("vol") or 0.0) for item in bucket),
        }
        output.append(row)
    return output


def _floor_to_timeframe(value: datetime, timeframe_delta: timedelta) -> datetime:
    value = _coerce_datetime(value)
    seconds = int(timeframe_delta.total_seconds())
    epoch_seconds = int(value.timestamp())
    floored = epoch_seconds - (epoch_seconds % seconds)
    return datetime.fromtimestamp(floored, tz=UTC)


def _registration(
    *,
    source_registration: dict[str, Any],
    storage_uri: Path,
    rows: list[dict[str, Any]],
    timeframe: str,
    period: int,
    source_timeframe: str,
) -> dict[str, Any]:
    source_id = str(source_registration.get("source_id") or "local")
    return {
        "dataset_id": _dataset_id(source_registration, timeframe=timeframe, period=period),
        "source_id": source_id,
        "asset": str(source_registration["asset"]).upper(),
        "instrument": str(source_registration["instrument"]).upper(),
        "data_type": DATA_TYPE,
        "timeframe": timeframe,
        "data_origin": "derived",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {
            "schema_version": SCHEMA_VERSION,
            "columns": ATR_COLUMNS,
            "format": "parquet",
            "derived_from_dataset_id": source_registration["dataset_id"],
            "source": {
                "data_type": "candles",
                "origin": "raw",
                "timeframe": source_timeframe,
            },
            "indicator": "atr",
            "method": "wilder",
            "period": period,
            "timestamp_semantics": "higher_timeframe_interval_start",
            "availability_semantics": "interval_end",
            "atr_pct_semantics": "percentage_points_of_close",
            "quality": {
                "complete_row_count": len(rows),
                "warmup_complete_row_count": sum(1 for row in rows if row.get("warmup_complete")),
            },
        },
        "quality_status": "atr_enriched",
        "ingestion_version": SCHEMA_VERSION,
    }


def _dataset_id(source_registration: dict[str, Any], *, timeframe: str, period: int) -> str:
    source_id = str(source_registration.get("source_id") or "local").lower()
    return f"{str(source_registration['asset']).lower()}-{source_id}-{DATA_TYPE}-derived-{timeframe}-wilder-{period}"


def _storage_uri(*, target_root: Path, source_id: str, asset: str, timeframe: str) -> Path:
    return (
        target_root
        / "origin=derived"
        / f"source={source_id.lower()}"
        / f"type={DATA_TYPE}"
        / f"asset={asset.upper()}"
        / f"timeframe={timeframe}"
    )


def _summary(registration: dict[str, Any], *, status: str) -> dict[str, Any]:
    return {
        "dataset_id": registration["dataset_id"],
        "status": status,
        "asset": registration["asset"],
        "timeframe": registration["timeframe"],
        "period": registration["schema_descriptor"]["period"],
        "row_count": registration["row_count"],
        "start_ts": registration["start_ts"],
        "end_ts": registration["end_ts"],
        "storage_uri": registration["storage_uri"],
        "data_type": registration["data_type"],
    }


def _coerce_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build causal ATR indicator datasets from local 5m candles.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--asset", action="append", dest="assets")
    parser.add_argument("--timeframe", action="append", dest="timeframes")
    parser.add_argument("--period", type=int, default=DEFAULT_PERIOD)
    parser.add_argument("--source-timeframe", default=DEFAULT_SOURCE_TIMEFRAME)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--target-root", type=Path, default=Path(".data/market-data"))
    args = parser.parse_args()

    repository = PostgresMarketDataRepository(args.database_url)
    assets = args.assets or [None]
    timeframes = tuple(args.timeframes or DEFAULT_TIMEFRAMES)
    results = [
        enrich_atr_datasets(
            repository=repository,
            asset=asset,
            timeframes=timeframes,
            period=args.period,
            source_timeframe=args.source_timeframe,
            start_date=args.start_date,
            target_root=args.target_root,
        )
        for asset in assets
    ]
    print(json.dumps({"results": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
