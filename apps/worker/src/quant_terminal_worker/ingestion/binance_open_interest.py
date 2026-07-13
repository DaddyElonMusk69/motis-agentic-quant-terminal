from __future__ import annotations

from collections import defaultdict
import csv
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
import zipfile

import pyarrow as pa

from quant_terminal_sdk.parquet_store import read_candles, write_parquet_table_atomically


OI_RAW_COLUMNS = [
    "timestamp",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "cmc_circulating_supply",
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
    "confirm",
]

RATIO_FIELDS = [
    "count_toptrader_long_short_ratio",
    "sum_toptrader_long_short_ratio",
    "count_long_short_ratio",
    "sum_taker_long_short_vol_ratio",
]


def normalize_open_interest_row(row: dict[str, Any]) -> dict[str, Any]:
    timestamp_value = row.get("timestamp") or row.get("create_time")
    normalized: dict[str, Any] = {
        "timestamp": _to_iso(_floor_to_5m_boundary(_coerce_datetime(timestamp_value))),
        "symbol": str(row.get("symbol") or "").upper(),
        "sum_open_interest": _optional_float(row.get("sum_open_interest") or row.get("sumOpenInterest")),
        "sum_open_interest_value": _optional_float(row.get("sum_open_interest_value") or row.get("sumOpenInterestValue")),
        "confirm": int(row.get("confirm", 1)),
    }
    cmc_supply = row.get("cmc_circulating_supply") or row.get("CMCCirculatingSupply")
    if cmc_supply not in (None, ""):
        normalized["cmc_circulating_supply"] = _optional_float(cmc_supply)
    for field in RATIO_FIELDS:
        value = row.get(field)
        if value not in (None, ""):
            normalized[field] = _optional_float(value)
    return {key: value for key, value in normalized.items() if value is not None}


def import_binance_open_interest_history(
    *,
    source_dir: Path,
    target_root: Path,
    repository: Any,
    asset: str,
    symbol: str,
    ingestion_version: str,
    derived_timeframes: Iterable[str] = ("15m", "1h", "2h", "4h", "1d"),
    min_timestamp: datetime | None = None,
) -> dict[str, Any]:
    rows = _read_zip_csv_rows(source_dir=source_dir, symbol=symbol)
    if min_timestamp is not None:
        min_dt = _coerce_datetime(min_timestamp)
        rows = [row for row in rows if _coerce_datetime(row["timestamp"]) >= min_dt]
    if not rows:
        raise ValueError(f"no Binance open-interest rows found in {source_dir}")

    raw_storage_uri = _storage_uri(target_root=target_root, origin="raw", asset=asset, timeframe="5m")
    _write_dataset_rows(raw_storage_uri, rows)
    raw_registration = _registration(
        dataset_id=f"{asset.lower()}-binance-open_interest-raw-5m",
        asset=asset,
        symbol=symbol,
        timeframe="5m",
        origin="raw",
        storage_uri=raw_storage_uri,
        rows=rows,
        ingestion_version=ingestion_version,
        schema_descriptor={
            "columns": OI_RAW_COLUMNS,
            "format": "parquet",
            "source_format": "binance_metrics_zip_csv",
        },
    )

    _upsert_data_source(repository)
    repository.upsert_ref(raw_registration)

    derived_results = []
    for timeframe in derived_timeframes:
        derived_rows = derive_open_interest_rows(
            raw_rows=rows,
            raw_timeframe="5m",
            derived_timeframe=timeframe,
        )
        if not derived_rows:
            continue
        derived_storage_uri = _storage_uri(target_root=target_root, origin="derived", asset=asset, timeframe=timeframe)
        _write_dataset_rows(derived_storage_uri, derived_rows)
        derived_registration = _registration(
            dataset_id=f"{asset.lower()}-binance-open_interest-derived-{timeframe}",
            asset=asset,
            symbol=symbol,
            timeframe=timeframe,
            origin="derived",
            storage_uri=derived_storage_uri,
            rows=derived_rows,
            ingestion_version=ingestion_version,
            schema_descriptor={
                "format": "parquet",
                "origin": "derived",
                "derived_from_dataset_id": raw_registration["dataset_id"],
                "source": {"data_type": "open_interest", "origin": "raw", "timeframe": "5m"},
            },
        )
        repository.upsert_ref(derived_registration)
        derived_results.append(_summary(derived_registration))

    return {
        "status": "imported",
        "raw": _summary(raw_registration),
        "derived": derived_results,
    }


def fill_raw_open_interest_dataset(
    *,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    if registration["data_type"] != "open_interest" or registration["data_origin"] != "raw":
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_market_data_only",
        }

    end_ts = _coerce_datetime(registration["end_ts"])
    from_ts = end_ts + _timeframe_delta(registration["timeframe"])
    target = as_of or datetime.now(UTC)
    if from_ts > target:
        return {
            "dataset_id": registration["dataset_id"],
            "status": "current",
            "rows_added": 0,
            "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
            "end_ts": _to_iso(end_ts),
            "row_count": registration["row_count"],
        }

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
        return {
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
    derived_rebuilt = _rebuild_derived_refs(
        raw_registration=updated_registration,
        raw_rows=merged_rows,
        repository=repository,
    )
    return {
        "dataset_id": registration["dataset_id"],
        "status": "filled",
        "rows_added": len(new_rows),
        "start_ts": merged_rows[0]["timestamp"],
        "end_ts": merged_rows[-1]["timestamp"],
        "row_count": len(merged_rows),
        "from_ts": _to_iso(from_ts),
        "to_ts": _to_iso(target),
        "source": "binance_cli",
        "derived_rebuilt": derived_rebuilt,
    }


def derive_open_interest_rows(
    *,
    raw_rows: list[dict[str, Any]],
    raw_timeframe: str | None,
    derived_timeframe: str | None,
) -> list[dict[str, Any]]:
    if raw_timeframe is None or derived_timeframe is None:
        return []
    raw_seconds = int(_timeframe_delta(raw_timeframe).total_seconds())
    derived_seconds = int(_timeframe_delta(derived_timeframe).total_seconds())
    if derived_seconds < raw_seconds or derived_seconds % raw_seconds != 0:
        return []
    if derived_seconds == raw_seconds:
        return [dict(row) for row in _dedupe_sort_rows(raw_rows)]

    bucket_size = derived_seconds // raw_seconds
    rows = _dedupe_sort_rows(raw_rows)
    derived_rows: list[dict[str, Any]] = []
    buckets: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        timestamp = _coerce_datetime(row["timestamp"])
        bucket_epoch = int(timestamp.timestamp()) // derived_seconds * derived_seconds
        buckets[bucket_epoch].append(row)
    for bucket_epoch, bucket in sorted(buckets.items()):
        bucket = _dedupe_sort_rows(bucket)
        if len(bucket) != bucket_size:
            continue
        if not _bucket_is_contiguous(bucket, bucket_epoch=bucket_epoch, raw_seconds=raw_seconds):
            continue
        derived_rows.append(_aggregate_oi_bucket(bucket))
    return derived_rows


def _read_zip_csv_rows(*, source_dir: Path, symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(source_dir.rglob(f"{symbol}-metrics-*.zip")):
        with zipfile.ZipFile(path) as archive:
            csv_name = next((name for name in archive.namelist() if name.endswith(".csv")), None)
            if csv_name is None:
                continue
            with archive.open(csv_name) as handle:
                text = (line.decode("utf-8") for line in handle)
                rows.extend(normalize_open_interest_row(row) for row in csv.DictReader(text))
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
    payload = adapter.open_interest_statistics(
        symbol=symbol,
        period=timeframe,
        limit=limit,
        start_time_ms=_to_epoch_ms(from_ts),
        end_time_ms=_to_epoch_ms(target),
    )
    rows = [normalize_open_interest_row(row) for row in payload]
    return [
        row
        for row in _dedupe_sort_rows(rows)
        if from_ts <= _coerce_datetime(row["timestamp"]) <= target
    ]


def _rebuild_derived_refs(
    *,
    raw_registration: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    repository: Any,
) -> list[dict[str, Any]]:
    from quant_terminal_worker.ingestion.open_interest_feature_enrichment import (
        rebuild_registered_open_interest_feature_datasets,
    )

    rebuilt: list[dict[str, Any]] = []
    for derived_registration in repository.list_derived_refs_for_raw(raw_registration):
        if derived_registration.get("data_type") != "open_interest":
            continue
        timeframe = derived_registration["timeframe"]
        derived_rows = derive_open_interest_rows(
            raw_rows=raw_rows,
            raw_timeframe=raw_registration["timeframe"],
            derived_timeframe=timeframe,
        )
        if not derived_rows:
            continue
        _write_dataset_rows(Path(derived_registration["storage_uri"]), derived_rows)
        updated_registration = {
            **derived_registration,
            "start_ts": derived_rows[0]["timestamp"],
            "end_ts": derived_rows[-1]["timestamp"],
            "row_count": len(derived_rows),
            "quality_status": "derived",
            "schema_descriptor": {
                **(derived_registration.get("schema_descriptor") or {}),
                "origin": "derived",
                "derived_from_dataset_id": raw_registration["dataset_id"],
            },
        }
        repository.update_ref(updated_registration)
        result = _summary(updated_registration)
        features_rebuilt = rebuild_registered_open_interest_feature_datasets(
            repository=repository,
            source_registration=updated_registration,
            source_rows=derived_rows,
        )
        if features_rebuilt:
            result["features_rebuilt"] = features_rebuilt
        rebuilt.append(result)
    return rebuilt


def _aggregate_oi_bucket(bucket: list[dict[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {
        "timestamp": bucket[0]["timestamp"],
        "symbol": bucket[-1].get("symbol"),
        "confirm": 1 if all(int(item.get("confirm", 1)) == 1 for item in bucket) else 0,
    }
    for field in ("sum_open_interest", "sum_open_interest_value"):
        values = [_optional_float(item.get(field)) for item in bucket if item.get(field) not in (None, "")]
        if not values:
            continue
        result[f"{field}_first"] = values[0]
        result[f"{field}_last"] = values[-1]
        result[f"{field}_min"] = min(values)
        result[f"{field}_max"] = max(values)
        result[f"{field}_change"] = values[-1] - values[0]
    for field in RATIO_FIELDS:
        values = [_optional_float(item.get(field)) for item in bucket if item.get(field) not in (None, "")]
        if not values:
            continue
        result[f"{field}_avg"] = sum(values) / len(values)
        result[f"{field}_last"] = values[-1]
    return result


def _bucket_is_contiguous(bucket: list[dict[str, Any]], *, bucket_epoch: int, raw_seconds: int) -> bool:
    timestamps = [_coerce_datetime(row["timestamp"]) for row in bucket]
    if int(timestamps[0].timestamp()) != bucket_epoch:
        return False
    return all(
        int((right - left).total_seconds()) == raw_seconds
        for left, right in zip(timestamps, timestamps[1:])
    )


def _storage_uri(*, target_root: Path, origin: str, asset: str, timeframe: str) -> Path:
    return (
        target_root
        / f"origin={origin}"
        / "source=binance"
        / "type=open_interest"
        / f"asset={asset.upper()}"
        / f"timeframe={timeframe}"
    )


def _registration(
    *,
    dataset_id: str,
    asset: str,
    symbol: str,
    timeframe: str,
    origin: str,
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
        "data_type": "open_interest",
        "timeframe": timeframe,
        "data_origin": origin,
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": schema_descriptor,
        "quality_status": "ingested" if origin == "raw" else "derived",
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


def _dedupe_sort_rows(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
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


def _floor_to_5m_boundary(value: datetime) -> datetime:
    return value.replace(minute=value.minute - (value.minute % 5), second=0, microsecond=0)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


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
