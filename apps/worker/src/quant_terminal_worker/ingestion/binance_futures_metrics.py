from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Iterable
import zipfile

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.binance_open_interest import (
    _coerce_datetime,
    _dedupe_sort_rows,
    _read_dataset_rows,
    _to_epoch_ms,
    _to_iso,
    _write_dataset_rows,
)


DATA_TYPE = "futures_metrics"
TIMEFRAME = "5m"
SCHEMA_VERSION = "binance-futures-metrics.v1"
BASE_INTERVAL = timedelta(minutes=5)
DERIVED_TIMEFRAMES = ("15m", "1h", "2h", "4h", "8h", "12h", "1d")
ARCHIVE_BASE_URL = "https://data.binance.vision/data/futures/um/daily/metrics"
FUTURES_METRICS_COLUMNS = [
    "timestamp",
    "interval_end",
    "available_at",
    "symbol",
    "sum_open_interest",
    "sum_open_interest_value",
    "top_trader_account_long_short_ratio",
    "top_trader_position_long_short_ratio",
    "global_account_long_short_ratio",
    "taker_buy_sell_volume_ratio",
    "complete",
    "confirm",
    "ingest_source",
]
METRIC_COLUMNS = FUTURES_METRICS_COLUMNS[4:10]
RATIO_COLUMNS = [
    "top_trader_account_long_short_ratio",
    "top_trader_position_long_short_ratio",
    "global_account_long_short_ratio",
    "taker_buy_sell_volume_ratio",
]
DERIVED_FUTURES_METRICS_COLUMNS = [
    "timestamp",
    "interval_end",
    "available_at",
    "symbol",
    *(f"{column}_{stat}" for column in RATIO_COLUMNS for stat in ("avg", "last")),
    "top_trader_account_vs_global_long_share_gap_avg",
    "top_trader_account_vs_global_long_share_gap_last",
    "top_trader_position_vs_global_long_share_gap_avg",
    "top_trader_position_vs_global_long_share_gap_last",
    "source_row_count",
    "complete",
    "confirm",
    "ingest_source",
]


def normalize_archive_futures_metrics_row(row: dict[str, Any]) -> dict[str, Any]:
    interval_start = _floor_to_5m_boundary(
        _coerce_datetime(row.get("create_time") or row.get("timestamp"))
    )
    interval_end = interval_start + BASE_INTERVAL
    normalized: dict[str, Any] = {
        "timestamp": _to_iso(interval_start),
        "interval_end": _to_iso(interval_end),
        "available_at": _to_iso(interval_end),
        "symbol": str(row.get("symbol") or "").upper(),
        "sum_open_interest": _optional_float(row.get("sum_open_interest")),
        "sum_open_interest_value": _optional_float(row.get("sum_open_interest_value")),
        "top_trader_account_long_short_ratio": _optional_float(
            row.get("count_toptrader_long_short_ratio")
        ),
        "top_trader_position_long_short_ratio": _optional_float(
            row.get("sum_toptrader_long_short_ratio")
        ),
        "global_account_long_short_ratio": _optional_float(
            row.get("count_long_short_ratio")
        ),
        "taker_buy_sell_volume_ratio": _optional_float(
            row.get("sum_taker_long_short_vol_ratio")
        ),
        "ingest_source": "archive",
    }
    complete = all(normalized.get(column) is not None for column in METRIC_COLUMNS)
    normalized["complete"] = complete
    normalized["confirm"] = 1 if complete else 0
    return normalized


def import_binance_futures_metrics_history(
    *,
    source_dir: Path,
    target_root: Path,
    repository: Any,
    asset: str,
    symbol: str,
    ingestion_version: str = SCHEMA_VERSION,
    min_timestamp: datetime | None = None,
    derived_timeframes: Iterable[str] = DERIVED_TIMEFRAMES,
) -> dict[str, Any]:
    archive_rows = _read_archive_rows(source_dir=source_dir, symbol=symbol)
    if min_timestamp is not None:
        minimum = _coerce_datetime(min_timestamp)
        archive_rows = [
            row for row in archive_rows if _coerce_datetime(row["timestamp"]) >= minimum
        ]
    if not archive_rows:
        raise ValueError(f"no Binance futures metrics rows found in {source_dir}")

    storage_uri = _storage_uri(target_root=target_root, asset=asset)
    existing_rows = _read_dataset_rows(storage_uri) if storage_uri.exists() else []
    if min_timestamp is not None:
        minimum = _coerce_datetime(min_timestamp)
        existing_rows = [
            row for row in existing_rows if _coerce_datetime(row["timestamp"]) >= minimum
        ]
    # Archive rows are authoritative when they overlap provisional REST rows.
    rows = _dedupe_sort_rows([*existing_rows, *archive_rows])
    _write_dataset_rows(storage_uri, rows)
    registration = _registration(
        asset=asset,
        symbol=symbol,
        storage_uri=storage_uri,
        rows=rows,
        ingestion_version=ingestion_version,
    )
    _upsert_data_source(repository)
    repository.upsert_ref(registration)
    result = _summary(registration, status="imported")
    result["derived"] = _build_derived_datasets(
        raw_registration=registration,
        raw_rows=rows,
        repository=repository,
        target_root=target_root,
        timeframes=derived_timeframes,
    )
    return result


def derive_futures_metrics_rows(
    *,
    raw_rows: list[dict[str, Any]],
    raw_timeframe: str | None,
    derived_timeframe: str | None,
) -> list[dict[str, Any]]:
    if raw_timeframe is None or derived_timeframe is None:
        return []
    raw_delta = _timeframe_delta(raw_timeframe)
    derived_delta = _timeframe_delta(derived_timeframe)
    raw_seconds = int(raw_delta.total_seconds())
    derived_seconds = int(derived_delta.total_seconds())
    if derived_seconds < raw_seconds or derived_seconds % raw_seconds != 0:
        return []
    if derived_seconds == raw_seconds:
        return [dict(row) for row in _dedupe_sort_rows(raw_rows)]

    bucket_size = derived_seconds // raw_seconds
    buckets: dict[int, list[dict[str, Any]]] = {}
    for row in _dedupe_sort_rows(raw_rows):
        timestamp = _coerce_datetime(row["timestamp"])
        bucket_epoch = int(timestamp.timestamp()) // derived_seconds * derived_seconds
        buckets.setdefault(bucket_epoch, []).append(row)

    output: list[dict[str, Any]] = []
    for bucket_epoch, bucket in sorted(buckets.items()):
        bucket = _dedupe_sort_rows(bucket)
        if len(bucket) != bucket_size or not _bucket_is_complete(
            bucket,
            bucket_epoch=bucket_epoch,
            raw_seconds=raw_seconds,
        ):
            continue
        bucket_start = datetime.fromtimestamp(bucket_epoch, tz=UTC)
        interval_end = bucket_start + derived_delta
        row: dict[str, Any] = {
            "timestamp": _to_iso(bucket_start),
            "interval_end": _to_iso(interval_end),
            "available_at": _to_iso(interval_end),
            "symbol": str(bucket[-1].get("symbol") or "").upper(),
            "source_row_count": len(bucket),
            "complete": True,
            "confirm": 1,
            "ingest_source": "derived",
        }
        for column in RATIO_COLUMNS:
            values = [float(item[column]) for item in bucket]
            row[f"{column}_avg"] = sum(values) / len(values)
            row[f"{column}_last"] = values[-1]
        account_gaps = [
            _long_share(float(item["top_trader_account_long_short_ratio"]))
            - _long_share(float(item["global_account_long_short_ratio"]))
            for item in bucket
        ]
        position_gaps = [
            _long_share(float(item["top_trader_position_long_short_ratio"]))
            - _long_share(float(item["global_account_long_short_ratio"]))
            for item in bucket
        ]
        row["top_trader_account_vs_global_long_share_gap_avg"] = sum(
            account_gaps
        ) / len(account_gaps)
        row["top_trader_account_vs_global_long_share_gap_last"] = account_gaps[-1]
        row["top_trader_position_vs_global_long_share_gap_avg"] = sum(
            position_gaps
        ) / len(position_gaps)
        row["top_trader_position_vs_global_long_share_gap_last"] = position_gaps[-1]
        output.append(row)
    return output


def build_live_futures_metrics_rows(
    *,
    adapter: Any,
    symbol: str,
    from_ts: datetime,
    target: datetime,
    limit: int = 500,
) -> list[dict[str, Any]]:
    start = _floor_to_5m_boundary(_coerce_datetime(from_ts))
    end = _floor_to_5m_boundary(_coerce_datetime(target))
    if start > end:
        return []

    snapshot_start = start + BASE_INTERVAL
    snapshot_end = end + BASE_INTERVAL
    common_snapshot_kwargs = {
        "symbol": symbol,
        "period": TIMEFRAME,
        "limit": limit,
        "start_time_ms": _to_epoch_ms(snapshot_start),
        "end_time_ms": _to_epoch_ms(snapshot_end),
    }
    taker_kwargs = {
        "symbol": symbol,
        "period": TIMEFRAME,
        "limit": limit,
        "start_time_ms": _to_epoch_ms(start),
        "end_time_ms": _to_epoch_ms(end),
    }
    oi_rows = adapter.open_interest_statistics(**common_snapshot_kwargs)
    account_rows = adapter.top_trader_account_ratio(**common_snapshot_kwargs)
    position_rows = adapter.top_trader_position_ratio(**common_snapshot_kwargs)
    global_rows = adapter.global_account_ratio(**common_snapshot_kwargs)
    taker_rows = adapter.taker_buy_sell_volume(**taker_kwargs)

    oi_by_start = _snapshot_index(oi_rows)
    account_by_start = _snapshot_index(account_rows)
    position_by_start = _snapshot_index(position_rows)
    global_by_start = _snapshot_index(global_rows)
    taker_by_start = _interval_start_index(taker_rows)
    output: list[dict[str, Any]] = []
    current = start
    while current <= end:
        payloads = (
            oi_by_start.get(current),
            account_by_start.get(current),
            position_by_start.get(current),
            global_by_start.get(current),
            taker_by_start.get(current),
        )
        if any(payload is None for payload in payloads):
            break
        oi, account, position, global_ratio, taker = payloads
        assert oi is not None
        assert account is not None
        assert position is not None
        assert global_ratio is not None
        assert taker is not None
        values = {
            "sum_open_interest": _optional_float(oi.get("sumOpenInterest")),
            "sum_open_interest_value": _optional_float(oi.get("sumOpenInterestValue")),
            "top_trader_account_long_short_ratio": _optional_float(
                account.get("longShortRatio")
            ),
            "top_trader_position_long_short_ratio": _optional_float(
                position.get("longShortRatio")
            ),
            "global_account_long_short_ratio": _optional_float(
                global_ratio.get("longShortRatio")
            ),
            "taker_buy_sell_volume_ratio": _optional_float(taker.get("buySellRatio")),
        }
        if any(value is None for value in values.values()):
            break
        interval_end = current + BASE_INTERVAL
        output.append(
            {
                "timestamp": _to_iso(current),
                "interval_end": _to_iso(interval_end),
                "available_at": _to_iso(interval_end),
                "symbol": str(oi.get("symbol") or symbol).upper(),
                **values,
                "complete": True,
                "confirm": 1,
                "ingest_source": "rest",
            }
        )
        current += BASE_INTERVAL
    return output


def fill_raw_futures_metrics_dataset(
    *,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    if (
        registration.get("data_type") != DATA_TYPE
        or registration.get("data_origin") != "raw"
    ):
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_futures_metrics_only",
        }

    end_ts = _coerce_datetime(registration["end_ts"])
    from_ts = end_ts + BASE_INTERVAL
    now = _coerce_datetime(as_of or datetime.now(UTC))
    target = _floor_to_5m_boundary(now) - BASE_INTERVAL
    if from_ts > target:
        return {
            "dataset_id": registration["dataset_id"],
            "status": "current",
            "rows_added": 0,
            "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
            "end_ts": _to_iso(end_ts),
            "row_count": int(registration["row_count"]),
        }

    fetched: list[dict[str, Any]] = []
    page_start = from_ts
    while page_start <= target:
        page_target = min(target, page_start + BASE_INTERVAL * (limit - 1))
        page_rows = build_live_futures_metrics_rows(
            adapter=adapter,
            symbol=str(registration["instrument"]),
            from_ts=page_start,
            target=page_target,
            limit=limit,
        )
        fetched.extend(page_rows)
        expected = int((page_target - page_start) / BASE_INTERVAL) + 1
        if len(page_rows) != expected:
            break
        page_start = page_target + BASE_INTERVAL

    storage_uri = Path(registration["storage_uri"])
    new_rows = fetched
    if not new_rows:
        return {
            "dataset_id": registration["dataset_id"],
            "status": "no_new_rows",
            "rows_added": 0,
            "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
            "end_ts": _to_iso(end_ts),
            "row_count": int(registration["row_count"]),
            "from_ts": _to_iso(from_ts),
            "to_ts": _to_iso(target),
            "source": "binance_cli",
            "reason": "source_returned_no_contiguous_complete_rows",
        }

    _append_dataset_rows(storage_uri, new_rows)
    quality = _increment_quality_summary(
        registration.get("schema_descriptor"),
        added_complete_rows=len(new_rows),
    )
    updated = {
        **registration,
        "end_ts": new_rows[-1]["timestamp"],
        "row_count": int(registration["row_count"]) + len(new_rows),
        "quality_status": (
            "source_gaps"
            if quality["missing_interval_count"] or quality["incomplete_row_count"]
            else "updated"
        ),
        "schema_descriptor": {
            **(registration.get("schema_descriptor") or {}),
            "quality": quality,
        },
    }
    repository.update_ref(updated)
    derived_rebuilt = _rebuild_derived_refs(
        raw_registration=updated,
        raw_rows=_read_dataset_rows(storage_uri),
        repository=repository,
    )
    return {
        "dataset_id": registration["dataset_id"],
        "status": "filled",
        "rows_added": len(new_rows),
        "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
        "end_ts": new_rows[-1]["timestamp"],
        "row_count": int(registration["row_count"]) + len(new_rows),
        "from_ts": _to_iso(from_ts),
        "to_ts": _to_iso(target),
        "source": "binance_cli",
        "derived_rebuilt": derived_rebuilt,
    }


def download_binance_futures_metrics_archives(
    *,
    source_dir: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    workers: int = 12,
    base_url: str = ARCHIVE_BASE_URL,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    source_dir.mkdir(parents=True, exist_ok=True)
    days: list[date] = []
    current = start_date
    while current <= end_date:
        days.append(current)
        current += timedelta(days=1)
    downloaded = 0
    existing = 0
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _download_archive_day,
                source_dir=source_dir,
                symbol=symbol,
                day=day,
                base_url=base_url,
            ): day
            for day in days
        }
        for future in as_completed(futures):
            day = futures[future]
            try:
                status = future.result()
            except (OSError, zipfile.BadZipFile) as exc:
                failures.append({"date": day.isoformat(), "error": str(exc)})
                continue
            if status == "downloaded":
                downloaded += 1
            else:
                existing += 1
    if failures:
        first = failures[0]
        raise RuntimeError(
            f"failed to download {len(failures)} Binance metrics archives; "
            f"first failure {first['date']}: {first['error']}"
        )
    return {
        "status": "downloaded",
        "symbol": symbol.upper(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "file_count": len(days),
        "downloaded_count": downloaded,
        "existing_count": existing,
    }


def _download_archive_day(
    *,
    source_dir: Path,
    symbol: str,
    day: date,
    base_url: str,
) -> str:
    filename = f"{symbol.upper()}-metrics-{day.isoformat()}.zip"
    target = source_dir / filename
    if _valid_archive(target):
        return "existing"
    url = f"{base_url.rstrip('/')}/{symbol.upper()}/{filename}"
    temporary = target.with_suffix(".zip.part")
    curl = shutil.which("curl")
    if curl is None:
        raise OSError("curl is required to download Binance archive files")
    for attempt in range(3):
        try:
            completed = subprocess.run(
                [curl, "-fsS", "--max-time", "30", "-o", str(temporary), url],
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                raise OSError(completed.stderr.strip() or f"curl failed for {url}")
            if not _valid_archive(temporary):
                raise zipfile.BadZipFile(f"invalid metrics archive: {url}")
            temporary.replace(target)
            return "downloaded"
        except (OSError, zipfile.BadZipFile):
            temporary.unlink(missing_ok=True)
            if attempt == 2:
                raise
            time.sleep(0.5 * (2**attempt))
    raise AssertionError("unreachable")


def _valid_archive(path: Path) -> bool:
    if not path.is_file():
        return False
    try:
        with zipfile.ZipFile(path) as archive:
            return any(name.endswith(".csv") for name in archive.namelist())
    except zipfile.BadZipFile:
        return False


def _read_archive_rows(*, source_dir: Path, symbol: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(source_dir.rglob(f"{symbol.upper()}-metrics-*.zip")):
        with zipfile.ZipFile(path) as archive:
            csv_name = next(
                (name for name in archive.namelist() if name.endswith(".csv")), None
            )
            if csv_name is None:
                continue
            with archive.open(csv_name) as handle:
                text = (line.decode("utf-8") for line in handle)
                rows.extend(
                    normalize_archive_futures_metrics_row(row)
                    for row in csv.DictReader(text)
                )
    return _dedupe_sort_rows(rows)


def _snapshot_index(rows: Iterable[dict[str, Any]]) -> dict[datetime, dict[str, Any]]:
    return {
        _floor_to_5m_boundary(_coerce_datetime(row["timestamp"])) - BASE_INTERVAL: row
        for row in rows
        if row.get("timestamp") not in (None, "")
    }


def _interval_start_index(
    rows: Iterable[dict[str, Any]],
) -> dict[datetime, dict[str, Any]]:
    return {
        _floor_to_5m_boundary(_coerce_datetime(row["timestamp"])): row
        for row in rows
        if row.get("timestamp") not in (None, "")
    }


def _registration(
    *,
    asset: str,
    symbol: str,
    storage_uri: Path,
    rows: list[dict[str, Any]],
    ingestion_version: str,
) -> dict[str, Any]:
    quality = _quality_summary(rows)
    return {
        "dataset_id": f"{asset.lower()}-binance-futures_metrics-raw-{TIMEFRAME}",
        "source_id": "binance",
        "asset": asset.upper(),
        "instrument": symbol.upper(),
        "data_type": DATA_TYPE,
        "timeframe": TIMEFRAME,
        "data_origin": "raw",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {
            "schema_version": SCHEMA_VERSION,
            "columns": FUTURES_METRICS_COLUMNS,
            "format": "parquet",
            "source_format": "binance_metrics_zip_csv_and_rest",
            "timestamp_semantics": "interval_start",
            "availability_semantics": "interval_end",
            "quality": quality,
        },
        "quality_status": (
            "source_gaps"
            if quality["missing_interval_count"] or quality["incomplete_row_count"]
            else "ingested"
        ),
        "ingestion_version": ingestion_version,
    }


def _derived_registration(
    *,
    raw_registration: dict[str, Any],
    timeframe: str,
    storage_uri: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    quality = {
        "complete_row_count": len(rows),
        "incomplete_row_count": 0,
    }
    return {
        "dataset_id": (
            f"{str(raw_registration['asset']).lower()}-binance-"
            f"{DATA_TYPE}-derived-{timeframe}"
        ),
        "source_id": "binance",
        "asset": str(raw_registration["asset"]).upper(),
        "instrument": str(raw_registration["instrument"]).upper(),
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
            "columns": DERIVED_FUTURES_METRICS_COLUMNS,
            "format": "parquet",
            "origin": "derived",
            "derived_from_dataset_id": raw_registration["dataset_id"],
            "source": {
                "data_type": DATA_TYPE,
                "origin": "raw",
                "timeframe": TIMEFRAME,
            },
            "aggregation": {
                **{column: ["avg", "last"] for column in RATIO_COLUMNS},
                "top_vs_global": "long_share_gap_avg_and_last",
            },
            "top_vs_global_semantics": (
                "top-trader long share minus all-account long share; "
                "global is not a retail-only cohort"
            ),
            "timestamp_semantics": "interval_start",
            "availability_semantics": "interval_end",
            "quality": quality,
        },
        "quality_status": "derived",
        "ingestion_version": raw_registration["ingestion_version"],
    }


def _build_derived_datasets(
    *,
    raw_registration: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    repository: Any,
    target_root: Path,
    timeframes: Iterable[str],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for timeframe in timeframes:
        rows = derive_futures_metrics_rows(
            raw_rows=raw_rows,
            raw_timeframe=TIMEFRAME,
            derived_timeframe=timeframe,
        )
        if not rows:
            continue
        storage_uri = _derived_storage_uri(
            target_root=target_root,
            asset=str(raw_registration["asset"]),
            timeframe=timeframe,
        )
        _write_dataset_rows(storage_uri, rows)
        registration = _derived_registration(
            raw_registration=raw_registration,
            timeframe=timeframe,
            storage_uri=storage_uri,
            rows=rows,
        )
        repository.upsert_ref(registration)
        results.append(_summary(registration, status="derived"))
    return results


def _rebuild_derived_refs(
    *,
    raw_registration: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    repository: Any,
) -> list[dict[str, Any]]:
    list_refs = getattr(repository, "list_derived_refs_for_raw", None)
    if not callable(list_refs):
        return []
    results: list[dict[str, Any]] = []
    for registration in list_refs(raw_registration):
        if registration.get("data_type") != DATA_TYPE:
            continue
        rows = derive_futures_metrics_rows(
            raw_rows=raw_rows,
            raw_timeframe=TIMEFRAME,
            derived_timeframe=str(registration["timeframe"]),
        )
        if not rows:
            continue
        _write_dataset_rows(Path(registration["storage_uri"]), rows)
        updated = _derived_registration(
            raw_registration=raw_registration,
            timeframe=str(registration["timeframe"]),
            storage_uri=Path(registration["storage_uri"]),
            rows=rows,
        )
        repository.update_ref(updated)
        results.append(_summary(updated, status="rebuilt"))
    return results


def _storage_uri(*, target_root: Path, asset: str) -> Path:
    return (
        target_root
        / "origin=raw"
        / "source=binance"
        / f"type={DATA_TYPE}"
        / f"asset={asset.upper()}"
        / f"timeframe={TIMEFRAME}"
    )


def _derived_storage_uri(*, target_root: Path, asset: str, timeframe: str) -> Path:
    return (
        target_root
        / "origin=derived"
        / "source=binance"
        / f"type={DATA_TYPE}"
        / f"asset={asset.upper()}"
        / f"timeframe={timeframe}"
    )


def _upsert_data_source(repository: Any) -> None:
    upsert = getattr(repository, "upsert_data_source", None)
    if callable(upsert):
        upsert("binance", "Binance", "cex")


def _summary(registration: dict[str, Any], *, status: str) -> dict[str, Any]:
    return {
        "dataset_id": registration["dataset_id"],
        "status": status,
        "timeframe": registration["timeframe"],
        "row_count": registration["row_count"],
        "start_ts": registration["start_ts"],
        "end_ts": registration["end_ts"],
        "storage_uri": registration["storage_uri"],
    }


def _quality_summary(rows: list[dict[str, Any]]) -> dict[str, int]:
    timestamps = [_coerce_datetime(row["timestamp"]) for row in rows]
    missing_intervals = sum(
        max(0, int((right - left) / BASE_INTERVAL) - 1)
        for left, right in zip(timestamps, timestamps[1:])
    )
    incomplete_rows = sum(
        1
        for row in rows
        if not bool(row.get("complete", False)) or int(row.get("confirm", 0)) != 1
    )
    return {
        "missing_interval_count": missing_intervals,
        "incomplete_row_count": incomplete_rows,
        "complete_row_count": len(rows) - incomplete_rows,
    }


def _increment_quality_summary(
    schema_descriptor: Any,
    *,
    added_complete_rows: int,
) -> dict[str, int]:
    descriptor = schema_descriptor if isinstance(schema_descriptor, dict) else {}
    prior = descriptor.get("quality") if isinstance(descriptor.get("quality"), dict) else {}
    return {
        "missing_interval_count": int(prior.get("missing_interval_count") or 0),
        "incomplete_row_count": int(prior.get("incomplete_row_count") or 0),
        "complete_row_count": int(prior.get("complete_row_count") or 0)
        + added_complete_rows,
    }


def _append_dataset_rows(storage_uri: Path, rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in rows:
        timestamp = _coerce_datetime(row["timestamp"])
        grouped.setdefault((timestamp.year, timestamp.month), []).append(row)
    for (year, month), additions in sorted(grouped.items()):
        path = storage_uri / f"year={year:04d}" / f"month={month:02d}" / "data.parquet"
        existing = read_candles(path) if path.is_file() else []
        _write_dataset_rows(
            storage_uri,
            _dedupe_sort_rows([*existing, *additions]),
        )


def _bucket_is_complete(
    bucket: list[dict[str, Any]],
    *,
    bucket_epoch: int,
    raw_seconds: int,
) -> bool:
    timestamps = [_coerce_datetime(row["timestamp"]) for row in bucket]
    if int(timestamps[0].timestamp()) != bucket_epoch:
        return False
    if any(
        not bool(row.get("complete", False)) or int(row.get("confirm", 0)) != 1
        for row in bucket
    ):
        return False
    return all(
        int((right - left).total_seconds()) == raw_seconds
        for left, right in zip(timestamps, timestamps[1:])
    )


def _long_share(long_short_ratio: float) -> float:
    if long_short_ratio < 0:
        raise ValueError("long/short ratio cannot be negative")
    return long_short_ratio / (1.0 + long_short_ratio)


def _floor_to_5m_boundary(value: datetime) -> datetime:
    return value.replace(
        minute=value.minute - value.minute % 5,
        second=0,
        microsecond=0,
    )


def _timeframe_delta(timeframe: str) -> timedelta:
    normalized = timeframe.strip()
    if normalized.endswith("m"):
        return timedelta(minutes=int(normalized[:-1]))
    if normalized.endswith("h"):
        return timedelta(hours=int(normalized[:-1]))
    if normalized.endswith("d"):
        return timedelta(days=int(normalized[:-1]))
    raise ValueError(f"unsupported timeframe: {timeframe}")


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill and refresh canonical Binance USD-M futures metrics."
    )
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(".data/downloads/binance/futures_metrics/BTCUSDT"),
    )
    parser.add_argument("--target-root", type=Path, default=Path(".data/market-data"))
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workers", type=int, default=12)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--refresh-live", action="store_true")
    parser.add_argument("--binance-cli-path")
    parser.add_argument("--binance-profile")
    args = parser.parse_args()

    from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
    from quant_terminal_worker.adapters.binance import BinanceCLIAdapter

    download_result = None
    if not args.skip_download:
        download_result = download_binance_futures_metrics_archives(
            source_dir=args.source_dir,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            workers=args.workers,
        )
    repository = PostgresMarketDataRepository(args.database_url)
    imported = import_binance_futures_metrics_history(
        source_dir=args.source_dir,
        target_root=args.target_root,
        repository=repository,
        asset=args.asset,
        symbol=args.symbol,
        min_timestamp=datetime.combine(args.start_date, datetime.min.time(), tzinfo=UTC),
    )
    live_result = None
    if args.refresh_live:
        registration = repository.get_ref(imported["dataset_id"])
        assert registration is not None
        live_result = fill_raw_futures_metrics_dataset(
            registration=registration,
            repository=repository,
            adapter=BinanceCLIAdapter(
                {
                    "cli_path": args.binance_cli_path,
                    "profile": args.binance_profile,
                }
            ),
        )
    print(
        json.dumps(
            {"download": download_result, "import": imported, "live": live_result},
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
