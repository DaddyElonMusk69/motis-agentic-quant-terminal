from __future__ import annotations

from collections import defaultdict
import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
import zipfile

import pyarrow as pa

from quant_terminal_sdk.parquet_store import read_candles, write_parquet_table_atomically


FUNDING_RAW_COLUMNS = [
    "timestamp",
    "symbol",
    "funding_rate",
    "funding_interval_hours",
    "mark_price",
    "confirm",
]


def normalize_funding_row(row: dict[str, Any]) -> dict[str, Any]:
    interval_hours = _optional_int(
        row.get("funding_interval_hours")
        or row.get("fundingIntervalHours")
        or row.get("funding_interval")
        or row.get("fundingInterval")
        or 8
    )
    if interval_hours is None or interval_hours <= 0:
        interval_hours = 8
    timestamp_value = row.get("timestamp") or row.get("calc_time") or row.get("fundingTime")
    normalized: dict[str, Any] = {
        "timestamp": _to_iso(
            _floor_to_hour_interval_boundary(_coerce_datetime(timestamp_value), interval_hours)
        ),
        "symbol": str(row.get("symbol") or "").upper(),
        "funding_rate": _optional_float(row.get("last_funding_rate") or row.get("fundingRate")),
        "funding_interval_hours": interval_hours,
        "confirm": int(row.get("confirm", 1)),
    }
    mark_price = row.get("mark_price") or row.get("markPrice")
    if mark_price not in (None, ""):
        normalized["mark_price"] = _optional_float(mark_price)
    return {key: value for key, value in normalized.items() if value is not None}


def import_binance_funding_history(
    *,
    source_dir: Path,
    target_root: Path,
    repository: Any,
    asset: str,
    symbol: str,
    ingestion_version: str,
    min_timestamp: datetime | None = None,
    timeframe: str = "8h",
) -> dict[str, Any]:
    rows = _read_zip_csv_rows(source_dir=source_dir, symbol=symbol)
    if min_timestamp is not None:
        min_dt = _coerce_datetime(min_timestamp)
        rows = [row for row in rows if _coerce_datetime(row["timestamp"]) >= min_dt]
    if not rows:
        raise ValueError(f"no Binance funding rows found in {source_dir}")

    raw_storage_uri = _storage_uri(target_root=target_root, asset=asset, timeframe=timeframe)
    _write_dataset_rows(raw_storage_uri, rows)
    raw_registration = _registration(
        dataset_id=f"{asset.lower()}-binance-funding-raw-{timeframe}",
        asset=asset,
        symbol=symbol,
        timeframe=timeframe,
        storage_uri=raw_storage_uri,
        rows=rows,
        ingestion_version=ingestion_version,
        schema_descriptor={
            "columns": FUNDING_RAW_COLUMNS,
            "format": "parquet",
            "source_format": "binance_funding_rate_zip_csv",
        },
    )

    _upsert_data_source(repository)
    repository.upsert_ref(raw_registration)
    return {
        "status": "imported",
        "raw": _summary(raw_registration),
    }


def fill_raw_funding_dataset(
    *,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    if registration["data_type"] != "funding" or registration["data_origin"] != "raw":
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_market_data_only",
        }

    end_ts = _coerce_datetime(registration["end_ts"])
    from_ts = end_ts + _timeframe_delta(registration["timeframe"])
    target = as_of or datetime.now(UTC)
    if from_ts > target:
        result = {
            "dataset_id": registration["dataset_id"],
            "status": "current",
            "rows_added": 0,
            "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
            "end_ts": _to_iso(end_ts),
            "row_count": registration["row_count"],
        }
        return _attach_funding_feature_rebuild(
            result=result,
            registration=registration,
            repository=repository,
            rows=_read_dataset_rows(Path(registration["storage_uri"])),
            as_of=target,
        )

    fetched_rows = _fetch_missing_rows(
        adapter=adapter,
        symbol=registration["instrument"],
        timeframe=registration["timeframe"],
        from_ts=from_ts,
        target=target,
        limit=limit,
    )
    storage_uri = Path(registration["storage_uri"])
    existing_rows = _read_dataset_rows(storage_uri)
    existing_timestamps = {row["timestamp"] for row in existing_rows}
    new_rows = [row for row in fetched_rows if row["timestamp"] not in existing_timestamps]

    if not new_rows:
        result = {
            "dataset_id": registration["dataset_id"],
            "status": "no_new_rows",
            "rows_added": 0,
            "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
            "end_ts": _to_iso(end_ts),
            "row_count": registration["row_count"],
            "from_ts": _to_iso(from_ts),
            "to_ts": _to_iso(target),
            "source": "binance_cli",
            "reason": "source_returned_no_new_rows",
        }
        return _attach_funding_feature_rebuild(
            result=result,
            registration=registration,
            repository=repository,
            rows=existing_rows,
            as_of=target,
        )

    merged_rows = _dedupe_sort_rows([*existing_rows, *new_rows])
    _write_dataset_rows(storage_uri, merged_rows)
    updated_registration = {
        **registration,
        "start_ts": merged_rows[0]["timestamp"],
        "end_ts": merged_rows[-1]["timestamp"],
        "row_count": len(merged_rows),
        "quality_status": "updated",
    }
    repository.update_ref(updated_registration)
    result = {
        "dataset_id": registration["dataset_id"],
        "status": "filled",
        "rows_added": len(new_rows),
        "start_ts": merged_rows[0]["timestamp"],
        "end_ts": merged_rows[-1]["timestamp"],
        "row_count": len(merged_rows),
        "from_ts": _to_iso(from_ts),
        "to_ts": _to_iso(target),
        "source": "binance_cli",
    }
    return _attach_funding_feature_rebuild(
        result=result,
        registration=updated_registration,
        repository=repository,
        rows=merged_rows,
        as_of=target,
    )


def _attach_funding_feature_rebuild(
    *,
    result: dict[str, Any],
    registration: dict[str, Any],
    repository: Any,
    rows: list[dict[str, Any]],
    as_of: datetime,
) -> dict[str, Any]:
    from quant_terminal_worker.ingestion.funding_feature_enrichment import (
        rebuild_registered_funding_features,
    )

    rebuilt = rebuild_registered_funding_features(
        repository=repository,
        source_registration=registration,
        source_rows=rows,
        as_of=as_of,
    )
    return {**result, **({"derived_rebuilt": rebuilt} if rebuilt else {})}


def _read_zip_csv_rows(*, source_dir: Path, symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(source_dir.rglob(f"{symbol}-fundingRate-*.zip")):
        with zipfile.ZipFile(path) as archive:
            csv_name = next((name for name in archive.namelist() if name.endswith(".csv")), None)
            if csv_name is None:
                continue
            with archive.open(csv_name) as handle:
                text = (line.decode("utf-8") for line in handle)
                rows.extend(
                    {**normalize_funding_row(row), "symbol": symbol.upper()}
                    for row in csv.DictReader(text)
                )
    return _dedupe_sort_rows(rows)


def _fetch_missing_rows(
    *,
    adapter: Any,
    symbol: str,
    timeframe: str,
    from_ts: datetime,
    target: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cursor = from_ts
    step = _timeframe_delta(timeframe)
    while cursor <= target:
        payload = adapter.funding_rate_history(
            symbol=symbol,
            limit=limit,
            start_time_ms=_to_epoch_ms(cursor),
            end_time_ms=_to_epoch_ms(target),
        )
        batch = [
            row
            for row in (normalize_funding_row(item) for item in payload)
            if cursor <= _coerce_datetime(row["timestamp"]) <= target
        ]
        batch = _dedupe_sort_rows(batch)
        if not batch:
            break
        rows.extend(batch)
        next_cursor = _coerce_datetime(batch[-1]["timestamp"]) + step
        if next_cursor <= cursor:
            break
        cursor = next_cursor
        if len(batch) < limit:
            break
    return _dedupe_sort_rows(rows)


def _storage_uri(*, target_root: Path, asset: str, timeframe: str) -> Path:
    return (
        target_root
        / "origin=raw"
        / "source=binance"
        / "type=funding"
        / f"asset={asset.upper()}"
        / f"timeframe={timeframe}"
    )


def _registration(
    *,
    dataset_id: str,
    asset: str,
    symbol: str,
    timeframe: str,
    storage_uri: Path,
    rows: list[dict[str, Any]],
    ingestion_version: str,
    schema_descriptor: dict[str, Any],
) -> dict[str, Any]:
    return {
        "dataset_id": dataset_id,
        "source_id": "binance",
        "asset": asset.upper(),
        "instrument": symbol.upper(),
        "data_type": "funding",
        "timeframe": timeframe,
        "data_origin": "raw",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": schema_descriptor,
        "quality_status": "ingested",
        "ingestion_version": ingestion_version,
    }


def _upsert_data_source(repository: Any) -> None:
    upsert = getattr(repository, "upsert_data_source", None)
    if callable(upsert):
        upsert("binance", "Binance", "cex")


def _read_dataset_rows(storage_uri: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(storage_uri.glob("year=*/month=*/data.parquet")):
        rows.extend(read_candles(path))
    return rows


def _write_dataset_rows(storage_uri: Path, rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in _dedupe_sort_rows(rows):
        timestamp = _coerce_datetime(row["timestamp"])
        grouped[(timestamp.year, timestamp.month)].append(row)
    for (year, month), month_rows in sorted(grouped.items()):
        path = storage_uri / f"year={year:04d}" / f"month={month:02d}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        write_parquet_table_atomically(pa.Table.from_pylist(month_rows), path)


def _dedupe_sort_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_timestamp: dict[str, dict[str, Any]] = {}
    for row in rows:
        timestamp = _to_iso(_coerce_datetime(row["timestamp"]))
        by_timestamp[timestamp] = {**row, "timestamp": timestamp}
    return [by_timestamp[timestamp] for timestamp in sorted(by_timestamp)]


def _summary(registration: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": registration["dataset_id"],
        "timeframe": registration["timeframe"],
        "row_count": registration["row_count"],
        "start_ts": registration["start_ts"],
        "end_ts": registration["end_ts"],
    }


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        return datetime.fromtimestamp(int(value) / 1000, tz=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _floor_to_hour_interval_boundary(value: datetime, interval_hours: int) -> datetime:
    hours = value.hour - (value.hour % interval_hours)
    return value.replace(hour=hours, minute=0, second=0, microsecond=0)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _timeframe_delta(timeframe: str | None) -> timedelta:
    if timeframe is None:
        return timedelta(0)
    normalized = timeframe.strip()
    if normalized.endswith("m"):
        return timedelta(minutes=int(normalized[:-1]))
    if normalized.endswith("h"):
        return timedelta(hours=int(normalized[:-1]))
    if normalized.endswith("d"):
        return timedelta(days=int(normalized[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")


def _to_epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
