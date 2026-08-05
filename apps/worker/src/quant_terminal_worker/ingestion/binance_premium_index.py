from __future__ import annotations

import argparse
import calendar
from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import shutil
import subprocess
import time
from typing import Any, Iterable, Sequence
import zipfile

import pyarrow.parquet as pq

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.binance_open_interest import (
    _coerce_datetime,
    _dedupe_sort_rows,
    _read_dataset_rows,
    _to_epoch_ms,
    _to_iso,
    _write_dataset_rows,
)


DATA_TYPE = "premium_index"
TIMEFRAME = "5m"
SCHEMA_VERSION = "binance-premium-index.v1"
BASE_INTERVAL = timedelta(minutes=5)
DERIVED_TIMEFRAMES = ("15m", "1h", "2h", "4h", "8h", "12h", "1d")
MONTHLY_ARCHIVE_BASE_URL = (
    "https://data.binance.vision/data/futures/um/monthly/premiumIndexKlines"
)
DAILY_ARCHIVE_BASE_URL = (
    "https://data.binance.vision/data/futures/um/daily/premiumIndexKlines"
)
ARCHIVE_COLUMNS = [
    "open_time",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "close_time",
    "quote_volume",
    "count",
    "taker_buy_volume",
    "taker_buy_quote_volume",
    "ignore",
]
PREMIUM_INDEX_COLUMNS = [
    "timestamp",
    "interval_end",
    "available_at",
    "symbol",
    "premium_open",
    "premium_high",
    "premium_low",
    "premium_close",
    "sample_count",
    "complete",
    "confirm",
    "ingest_source",
]
DERIVED_PREMIUM_INDEX_COLUMNS = [*PREMIUM_INDEX_COLUMNS, "source_row_count"]


def normalize_archive_premium_index_row(
    row: dict[str, Any],
    *,
    symbol: str,
) -> dict[str, Any]:
    return _normalize_premium_index_values(
        open_time=row.get("open_time"),
        premium_open=row.get("open"),
        premium_high=row.get("high"),
        premium_low=row.get("low"),
        premium_close=row.get("close"),
        close_time=row.get("close_time"),
        sample_count=row.get("count"),
        symbol=symbol,
        ingest_source="archive",
    )


def normalize_live_premium_index_row(
    row: Sequence[Any],
    *,
    symbol: str,
) -> dict[str, Any]:
    if len(row) < 9:
        raise ValueError("Binance premium-index kline row has fewer than 9 fields")
    return _normalize_premium_index_values(
        open_time=row[0],
        premium_open=row[1],
        premium_high=row[2],
        premium_low=row[3],
        premium_close=row[4],
        close_time=row[6],
        sample_count=row[8],
        symbol=symbol,
        ingest_source="rest",
    )


def import_binance_premium_index_history(
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
            row
            for row in archive_rows
            if _coerce_datetime(row["timestamp"]) >= minimum
        ]
    if not archive_rows:
        raise ValueError(f"no Binance premium-index rows found in {source_dir}")

    storage_uri = _storage_uri(target_root=target_root, asset=asset)
    existing_rows = _read_dataset_rows(storage_uri) if storage_uri.exists() else []
    if min_timestamp is not None:
        minimum = _coerce_datetime(min_timestamp)
        existing_rows = [
            row
            for row in existing_rows
            if _coerce_datetime(row["timestamp"]) >= minimum
        ]
    # Published archive rows are authoritative over provisional REST rows.
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


def derive_premium_index_rows(
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
        if len(bucket) != bucket_size:
            continue
        if not _bucket_is_complete(
            bucket,
            bucket_epoch=bucket_epoch,
            raw_seconds=raw_seconds,
        ):
            continue
        bucket_start = datetime.fromtimestamp(bucket_epoch, tz=UTC)
        interval_end = bucket_start + derived_delta
        output.append(
            {
                "timestamp": _to_iso(bucket_start),
                "interval_end": _to_iso(interval_end),
                "available_at": _to_iso(interval_end),
                "symbol": str(bucket[-1].get("symbol") or "").upper(),
                "premium_open": float(bucket[0]["premium_open"]),
                "premium_high": max(float(row["premium_high"]) for row in bucket),
                "premium_low": min(float(row["premium_low"]) for row in bucket),
                "premium_close": float(bucket[-1]["premium_close"]),
                "sample_count": sum(
                    int(row.get("sample_count") or 0) for row in bucket
                ),
                "complete": True,
                "confirm": 1,
                "ingest_source": "derived",
                "source_row_count": len(bucket),
            }
        )
    return output


def build_live_premium_index_rows(
    *,
    adapter: Any,
    symbol: str,
    from_ts: datetime,
    target: datetime,
    as_of: datetime,
    limit: int = 1000,
    latest_window: bool = False,
) -> list[dict[str, Any]]:
    start = _floor_to_5m_boundary(_coerce_datetime(from_ts))
    end = _floor_to_5m_boundary(_coerce_datetime(target))
    observed_at = _coerce_datetime(as_of)
    if start > end:
        return []

    request_kwargs: dict[str, Any] = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": limit,
    }
    if not latest_window:
        request_kwargs.update(
            {
                "start_time_ms": _to_epoch_ms(start),
                "end_time_ms": _to_epoch_ms(end + BASE_INTERVAL) - 1,
            }
        )
    payload = adapter.premium_index_klines(**request_kwargs)
    by_timestamp: dict[datetime, dict[str, Any]] = {}
    for item in payload:
        row = normalize_live_premium_index_row(item, symbol=symbol)
        timestamp = _coerce_datetime(row["timestamp"])
        interval_end = _coerce_datetime(row["interval_end"])
        if start <= timestamp <= end and interval_end <= observed_at:
            by_timestamp[timestamp] = row

    output: list[dict[str, Any]] = []
    current = start
    while current <= end:
        row = by_timestamp.get(current)
        if row is None:
            break
        output.append(row)
        current += BASE_INTERVAL
    return output


def fill_raw_premium_index_dataset(
    *,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime | None = None,
    limit: int = 1000,
    archive_source_dir: Path | None = None,
    archive_downloader: Any | None = None,
) -> dict[str, Any]:
    if (
        registration.get("data_type") != DATA_TYPE
        or registration.get("data_origin") != "raw"
    ):
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_premium_index_only",
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

    symbol = str(registration["instrument"]).upper()
    fetched: list[dict[str, Any]] = []
    page_start = from_ts
    archive_row_count = 0
    rolling_window = BASE_INTERVAL * (limit - 1)
    if target - page_start > rolling_window:
        source_dir = archive_source_dir or (
            Path(".data/downloads/binance/premium_index") / symbol
        )
        # Daily archives arrive with a delay. Leave the latest two UTC dates to
        # the rolling endpoint so an unpublished archive cannot block refresh.
        archive_end_date = (now - timedelta(days=2)).date()
        if archive_end_date >= page_start.date():
            downloader = archive_downloader or download_binance_premium_index_archives
            downloader(
                source_dir=source_dir,
                symbol=symbol,
                start_date=page_start.date(),
                end_date=archive_end_date,
            )
            archive_candidates = _read_archive_rows(
                source_dir=source_dir,
                symbol=symbol,
                min_timestamp=page_start,
                max_timestamp=target,
            )
            archive_rows = _contiguous_rows_from(
                rows=archive_candidates,
                start=page_start,
            )
            fetched.extend(archive_rows)
            archive_row_count = len(archive_rows)
            if archive_rows:
                page_start = _coerce_datetime(archive_rows[-1]["timestamp"]) + BASE_INTERVAL

    live_row_count = 0
    if page_start <= target:
        live_rows = build_live_premium_index_rows(
            adapter=adapter,
            symbol=symbol,
            from_ts=page_start,
            target=target,
            as_of=now,
            limit=limit,
            latest_window=True,
        )
        fetched.extend(live_rows)
        live_row_count = len(live_rows)

    if not fetched:
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
            "reason": "source_returned_no_contiguous_closed_rows",
        }

    storage_uri = Path(registration["storage_uri"])
    _append_dataset_rows(storage_uri, fetched)
    stored_row_count = _dataset_row_count(storage_uri)
    previous_row_count = int(registration["row_count"])
    if stored_row_count < previous_row_count:
        raise RuntimeError(
            f"stored premium-index row count regressed for {registration['dataset_id']}: "
            f"{stored_row_count} < {previous_row_count}"
        )
    registered_rows_added = stored_row_count - previous_row_count
    quality = _increment_quality_summary(
        registration.get("schema_descriptor"),
        new_rows=fetched[-registered_rows_added:] if registered_rows_added else [],
    )
    updated = {
        **registration,
        "end_ts": fetched[-1]["timestamp"],
        "row_count": stored_row_count,
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
    derived_rebuilt = _refresh_derived_refs_incrementally(
        raw_registration=updated,
        raw_storage_uri=storage_uri,
        first_changed_ts=_coerce_datetime(fetched[0]["timestamp"]),
        last_changed_ts=_coerce_datetime(fetched[-1]["timestamp"]),
        repository=repository,
    )
    final_end_ts = _coerce_datetime(fetched[-1]["timestamp"])
    status = "filled" if final_end_ts >= target else "partial_filled"
    result = {
        "dataset_id": registration["dataset_id"],
        "status": status,
        "rows_added": registered_rows_added,
        "start_ts": _to_iso(_coerce_datetime(registration["start_ts"])),
        "end_ts": fetched[-1]["timestamp"],
        "row_count": stored_row_count,
        "from_ts": _to_iso(from_ts),
        "to_ts": _to_iso(target),
        "source": "binance_archive_and_cli" if archive_row_count else "binance_cli",
        "archive_rows_added": archive_row_count,
        "live_rows_added": live_row_count,
        "derived_rebuilt": derived_rebuilt,
    }
    if status == "partial_filled":
        result["reason"] = "source_returned_partial_contiguous_rows"
        result["next_from_ts"] = _to_iso(final_end_ts + BASE_INTERVAL)
    return result


def _contiguous_rows_from(*, rows: list[dict[str, Any]], start: datetime) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    current = _floor_to_5m_boundary(_coerce_datetime(start))
    rows_by_timestamp = {
        _coerce_datetime(row["timestamp"]): row
        for row in _dedupe_sort_rows(rows)
    }
    while True:
        row = rows_by_timestamp.get(current)
        if row is None:
            break
        output.append(row)
        current += BASE_INTERVAL
    return output


def repair_premium_index_gaps(
    *,
    registration: dict[str, Any],
    repository: Any,
    adapter: Any,
    as_of: datetime | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    if (
        registration.get("data_type") != DATA_TYPE
        or registration.get("data_origin") != "raw"
    ):
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "repair_supported_for_raw_premium_index_only",
        }

    storage_uri = Path(registration["storage_uri"])
    existing = _dedupe_sort_rows(_read_dataset_rows(storage_uri))
    gaps = _missing_ranges(existing)
    if not gaps:
        return {
            "dataset_id": registration["dataset_id"],
            "status": "current",
            "gap_runs": 0,
            "rows_added": 0,
            "remaining_missing_intervals": 0,
            "row_count": len(existing),
        }

    now = _coerce_datetime(as_of or datetime.now(UTC))
    additions: list[dict[str, Any]] = []
    for gap_start, gap_end in gaps:
        page_start = gap_start
        while page_start <= gap_end:
            page_target = min(gap_end, page_start + BASE_INTERVAL * (limit - 1))
            page_rows = build_live_premium_index_rows(
                adapter=adapter,
                symbol=str(registration["instrument"]),
                from_ts=page_start,
                target=page_target,
                as_of=now,
                limit=limit,
            )
            additions.extend(page_rows)
            expected = int((page_target - page_start) / BASE_INTERVAL) + 1
            if len(page_rows) != expected:
                break
            page_start = page_target + BASE_INTERVAL

    merged = _dedupe_sort_rows([*existing, *additions])
    if additions:
        _append_dataset_rows(storage_uri, additions)
    quality = _quality_summary(merged)
    updated = {
        **registration,
        "start_ts": merged[0]["timestamp"],
        "end_ts": merged[-1]["timestamp"],
        "row_count": len(merged),
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
        raw_rows=merged,
        repository=repository,
    )
    return {
        "dataset_id": registration["dataset_id"],
        "status": "repaired" if additions else "no_new_rows",
        "gap_runs": len(gaps),
        "rows_added": len(merged) - len(existing),
        "remaining_missing_intervals": quality["missing_interval_count"],
        "row_count": len(merged),
        "derived_rebuilt": derived_rebuilt,
    }


def download_binance_premium_index_archives(
    *,
    source_dir: Path,
    symbol: str,
    start_date: date,
    end_date: date,
    workers: int = 8,
) -> dict[str, Any]:
    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date")
    source_dir.mkdir(parents=True, exist_ok=True)
    specs = _archive_specs(start_date=start_date, end_date=end_date)
    downloaded = 0
    existing = 0
    failures: list[dict[str, str]] = []
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        futures = {
            pool.submit(
                _download_archive,
                source_dir=source_dir,
                symbol=symbol,
                archive_type=archive_type,
                period=period,
            ): (archive_type, period)
            for archive_type, period in specs
        }
        for future in as_completed(futures):
            archive_type, period = futures[future]
            try:
                status = future.result()
            except (OSError, zipfile.BadZipFile) as exc:
                failures.append(
                    {
                        "archive": f"{archive_type}:{period}",
                        "error": str(exc),
                    }
                )
                continue
            if status == "downloaded":
                downloaded += 1
            else:
                existing += 1
    if failures:
        first = failures[0]
        raise RuntimeError(
            f"failed to download {len(failures)} Binance premium-index archives; "
            f"first failure {first['archive']}: {first['error']}"
        )
    return {
        "status": "downloaded",
        "symbol": symbol.upper(),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "file_count": len(specs),
        "downloaded_count": downloaded,
        "existing_count": existing,
        "monthly_count": sum(1 for kind, _ in specs if kind == "monthly"),
        "daily_count": sum(1 for kind, _ in specs if kind == "daily"),
    }


def _normalize_premium_index_values(
    *,
    open_time: Any,
    premium_open: Any,
    premium_high: Any,
    premium_low: Any,
    premium_close: Any,
    close_time: Any,
    sample_count: Any,
    symbol: str,
    ingest_source: str,
) -> dict[str, Any]:
    interval_start = _coerce_datetime(open_time)
    expected_end = interval_start + BASE_INTERVAL
    source_end = _coerce_datetime(int(close_time) + 1)
    if interval_start != _floor_to_5m_boundary(interval_start):
        raise ValueError(f"misaligned premium-index open time: {open_time}")
    if source_end != expected_end:
        raise ValueError(
            f"unexpected premium-index close time for {open_time}: {close_time}"
        )
    values = {
        "premium_open": _optional_float(premium_open),
        "premium_high": _optional_float(premium_high),
        "premium_low": _optional_float(premium_low),
        "premium_close": _optional_float(premium_close),
    }
    complete = all(value is not None for value in values.values())
    return {
        "timestamp": _to_iso(interval_start),
        "interval_end": _to_iso(expected_end),
        "available_at": _to_iso(expected_end),
        "symbol": symbol.upper(),
        **values,
        "sample_count": _optional_int(sample_count),
        "complete": complete,
        "confirm": 1 if complete else 0,
        "ingest_source": ingest_source,
    }


def _archive_specs(*, start_date: date, end_date: date) -> list[tuple[str, str]]:
    specs: list[tuple[str, str]] = []
    current = start_date
    while current <= end_date:
        month_end = date(
            current.year,
            current.month,
            calendar.monthrange(current.year, current.month)[1],
        )
        if current.day == 1 and month_end <= end_date:
            specs.append(("monthly", current.strftime("%Y-%m")))
            current = month_end + timedelta(days=1)
        else:
            specs.append(("daily", current.isoformat()))
            current += timedelta(days=1)
    return specs


def _download_archive(
    *,
    source_dir: Path,
    symbol: str,
    archive_type: str,
    period: str,
) -> str:
    filename = f"{symbol.upper()}-{TIMEFRAME}-{period}.zip"
    target = source_dir / archive_type / filename
    if _valid_archive(target):
        return "existing"
    base_url = (
        MONTHLY_ARCHIVE_BASE_URL
        if archive_type == "monthly"
        else DAILY_ARCHIVE_BASE_URL
    )
    url = f"{base_url}/{symbol.upper()}/{TIMEFRAME}/{filename}"
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(".zip.part")
    curl = shutil.which("curl")
    if curl is None:
        raise OSError("curl is required to download Binance archive files")
    for attempt in range(3):
        try:
            completed = subprocess.run(
                [curl, "-fsS", "--max-time", "60", "-o", str(temporary), url],
                capture_output=True,
                text=True,
                check=False,
            )
            if completed.returncode != 0:
                raise OSError(completed.stderr.strip() or f"curl failed for {url}")
            if not _valid_archive(temporary):
                raise zipfile.BadZipFile(f"invalid premium-index archive: {url}")
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


def _read_archive_rows(
    *,
    source_dir: Path,
    symbol: str,
    min_timestamp: datetime | None = None,
    max_timestamp: datetime | None = None,
) -> list[dict[str, Any]]:
    minimum = _coerce_datetime(min_timestamp) if min_timestamp is not None else None
    maximum = _coerce_datetime(max_timestamp) if max_timestamp is not None else None
    rows: list[dict[str, Any]] = []
    pattern = f"{symbol.upper()}-{TIMEFRAME}-*.zip"
    for path in sorted(source_dir.rglob(pattern)):
        archive_range = _premium_archive_range(path, symbol=symbol)
        if archive_range is not None:
            archive_start, archive_end = archive_range
            if minimum is not None and archive_end < minimum.date():
                continue
            if maximum is not None and archive_start > maximum.date():
                continue
        with zipfile.ZipFile(path) as archive:
            csv_name = next(
                (name for name in archive.namelist() if name.endswith(".csv")), None
            )
            if csv_name is None:
                continue
            with archive.open(csv_name) as handle:
                reader = csv.reader(line.decode("utf-8") for line in handle)
                first = next(reader, None)
                if first is None:
                    continue
                if first and first[0] != "open_time":
                    _append_archive_row_in_range(
                        rows,
                        values=first,
                        symbol=symbol,
                        minimum=minimum,
                        maximum=maximum,
                    )
                for values in reader:
                    _append_archive_row_in_range(
                        rows,
                        values=values,
                        symbol=symbol,
                        minimum=minimum,
                        maximum=maximum,
                    )
    return _dedupe_sort_rows(rows)


def _append_archive_row_in_range(
    rows: list[dict[str, Any]],
    *,
    values: Sequence[Any],
    symbol: str,
    minimum: datetime | None,
    maximum: datetime | None,
) -> None:
    row = normalize_archive_premium_index_row(
        dict(zip(ARCHIVE_COLUMNS, values)),
        symbol=symbol,
    )
    timestamp = _coerce_datetime(row["timestamp"])
    if minimum is not None and timestamp < minimum:
        return
    if maximum is not None and timestamp > maximum:
        return
    rows.append(row)


def _premium_archive_range(path: Path, *, symbol: str) -> tuple[date, date] | None:
    prefix = f"{symbol.upper()}-{TIMEFRAME}-"
    name = path.name
    if not name.startswith(prefix) or not name.endswith(".zip"):
        return None
    period = name[len(prefix) : -4]
    try:
        if len(period) == 7:
            start = date.fromisoformat(f"{period}-01")
            return (
                start,
                date(start.year, start.month, calendar.monthrange(start.year, start.month)[1]),
            )
        day = date.fromisoformat(period)
        return day, day
    except ValueError:
        return None


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
        "dataset_id": f"{asset.lower()}-binance-{DATA_TYPE}-raw-{TIMEFRAME}",
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
            "columns": PREMIUM_INDEX_COLUMNS,
            "format": "parquet",
            "source_format": "binance_premium_index_kline_zip_csv_and_rest",
            "timestamp_semantics": "interval_start",
            "availability_semantics": "interval_end",
            "units": "dimensionless_ratio",
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
            "columns": DERIVED_PREMIUM_INDEX_COLUMNS,
            "format": "parquet",
            "origin": "derived",
            "derived_from_dataset_id": raw_registration["dataset_id"],
            "source": {
                "data_type": DATA_TYPE,
                "origin": "raw",
                "timeframe": TIMEFRAME,
            },
            "aggregation": {
                "premium_open": "first",
                "premium_high": "max",
                "premium_low": "min",
                "premium_close": "last",
                "sample_count": "sum",
            },
            "timestamp_semantics": "interval_start",
            "availability_semantics": "interval_end",
            "units": "dimensionless_ratio",
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
        rows = derive_premium_index_rows(
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
        rows = derive_premium_index_rows(
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


def _refresh_derived_refs_incrementally(
    *,
    raw_registration: dict[str, Any],
    raw_storage_uri: Path,
    first_changed_ts: datetime,
    last_changed_ts: datetime,
    repository: Any,
) -> list[dict[str, Any]]:
    list_refs = getattr(repository, "list_derived_refs_for_raw", None)
    if not callable(list_refs):
        return []
    registrations = [
        registration
        for registration in list_refs(raw_registration)
        if registration.get("data_type") == DATA_TYPE
    ]
    if not registrations:
        return []

    starts = {
        str(registration["timeframe"]): _floor_to_timeframe_boundary(
            first_changed_ts,
            str(registration["timeframe"]),
        )
        for registration in registrations
    }
    raw_rows = _read_dataset_rows_between(
        raw_storage_uri,
        start=min(starts.values()),
        end=last_changed_ts,
    )
    results: list[dict[str, Any]] = []
    for registration in registrations:
        timeframe = str(registration["timeframe"])
        rebuild_start = starts[timeframe]
        rows = [
            row
            for row in derive_premium_index_rows(
                raw_rows=raw_rows,
                raw_timeframe=TIMEFRAME,
                derived_timeframe=timeframe,
            )
            if _coerce_datetime(row["timestamp"]) >= rebuild_start
        ]
        if not rows:
            continue
        storage_uri = Path(registration["storage_uri"])
        _append_dataset_rows(storage_uri, rows)
        stored_row_count = _dataset_row_count(storage_uri)
        prior_end = _coerce_datetime(registration["end_ts"])
        final_end = max(prior_end, _coerce_datetime(rows[-1]["timestamp"]))
        descriptor = registration.get("schema_descriptor") or {}
        updated = {
            **registration,
            "end_ts": _to_iso(final_end),
            "row_count": stored_row_count,
            "schema_descriptor": {
                **descriptor,
                "quality": {
                    "complete_row_count": stored_row_count,
                    "incomplete_row_count": 0,
                },
            },
            "quality_status": "derived",
        }
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
    reduced_samples = sum(
        1
        for row in rows
        if row.get("sample_count") is not None and int(row["sample_count"]) < 60
    )
    return {
        "missing_interval_count": missing_intervals,
        "incomplete_row_count": incomplete_rows,
        "complete_row_count": len(rows) - incomplete_rows,
        "reduced_sample_count": reduced_samples,
    }


def _missing_ranges(
    rows: list[dict[str, Any]],
) -> list[tuple[datetime, datetime]]:
    timestamps = [_coerce_datetime(row["timestamp"]) for row in rows]
    return [
        (left + BASE_INTERVAL, right - BASE_INTERVAL)
        for left, right in zip(timestamps, timestamps[1:])
        if right - left > BASE_INTERVAL
    ]


def _increment_quality_summary(
    schema_descriptor: Any,
    *,
    new_rows: list[dict[str, Any]],
) -> dict[str, int]:
    descriptor = schema_descriptor if isinstance(schema_descriptor, dict) else {}
    prior = descriptor.get("quality") if isinstance(descriptor.get("quality"), dict) else {}
    added_incomplete = sum(
        1
        for row in new_rows
        if not bool(row.get("complete", False)) or int(row.get("confirm", 0)) != 1
    )
    added_reduced = sum(
        1
        for row in new_rows
        if row.get("sample_count") is not None and int(row["sample_count"]) < 60
    )
    return {
        "missing_interval_count": int(prior.get("missing_interval_count") or 0),
        "incomplete_row_count": int(prior.get("incomplete_row_count") or 0)
        + added_incomplete,
        "complete_row_count": int(prior.get("complete_row_count") or 0)
        + len(new_rows)
        - added_incomplete,
        "reduced_sample_count": int(prior.get("reduced_sample_count") or 0)
        + added_reduced,
    }


def _append_dataset_rows(storage_uri: Path, rows: list[dict[str, Any]]) -> int:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for row in rows:
        timestamp = _coerce_datetime(row["timestamp"])
        grouped.setdefault((timestamp.year, timestamp.month), []).append(row)
    rows_added = 0
    for (year, month), additions in sorted(grouped.items()):
        path = storage_uri / f"year={year:04d}" / f"month={month:02d}" / "data.parquet"
        existing = read_candles(path) if path.is_file() else []
        merged = _dedupe_sort_rows([*existing, *additions])
        rows_added += len(merged) - len(existing)
        _write_dataset_rows(
            storage_uri,
            merged,
        )
    return rows_added


def _read_dataset_rows_between(
    storage_uri: Path,
    *,
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    minimum = _coerce_datetime(start)
    maximum = _coerce_datetime(end)
    rows: list[dict[str, Any]] = []
    current = datetime(minimum.year, minimum.month, 1, tzinfo=UTC)
    while current <= maximum:
        path = (
            storage_uri
            / f"year={current.year:04d}"
            / f"month={current.month:02d}"
            / "data.parquet"
        )
        if path.is_file():
            rows.extend(
                row
                for row in read_candles(path)
                if minimum <= _coerce_datetime(row["timestamp"]) <= maximum
            )
        current = (
            datetime(current.year + 1, 1, 1, tzinfo=UTC)
            if current.month == 12
            else datetime(current.year, current.month + 1, 1, tzinfo=UTC)
        )
    return _dedupe_sort_rows(rows)


def _dataset_row_count(storage_uri: Path) -> int:
    return sum(
        int(pq.ParquetFile(path).metadata.num_rows)
        for path in storage_uri.glob("year=*/month=*/data.parquet")
    )


def _floor_to_timeframe_boundary(value: datetime, timeframe: str) -> datetime:
    timestamp = _coerce_datetime(value)
    seconds = int(_timeframe_delta(timeframe).total_seconds())
    bucket_epoch = int(timestamp.timestamp()) // seconds * seconds
    return datetime.fromtimestamp(bucket_epoch, tz=UTC)


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
        "quality": registration["schema_descriptor"]["quality"],
    }


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


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill and refresh canonical Binance USD-M premium-index klines."
    )
    parser.add_argument("--asset", default="BTC")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path(".data/downloads/binance/premium_index/BTCUSDT"),
    )
    parser.add_argument("--target-root", type=Path, default=Path(".data/market-data"))
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--skip-download", action="store_true")
    parser.add_argument("--repair-gaps", action="store_true")
    parser.add_argument("--refresh-live", action="store_true")
    parser.add_argument("--binance-cli-path")
    parser.add_argument("--binance-profile")
    args = parser.parse_args()

    from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
    from quant_terminal_worker.adapters.binance import BinanceCLIAdapter

    download_result = None
    if not args.skip_download:
        download_result = download_binance_premium_index_archives(
            source_dir=args.source_dir,
            symbol=args.symbol,
            start_date=args.start_date,
            end_date=args.end_date,
            workers=args.workers,
        )
    repository = PostgresMarketDataRepository(args.database_url)
    imported = import_binance_premium_index_history(
        source_dir=args.source_dir,
        target_root=args.target_root,
        repository=repository,
        asset=args.asset,
        symbol=args.symbol,
        min_timestamp=datetime.combine(args.start_date, datetime.min.time(), tzinfo=UTC),
    )
    repair_result = None
    if args.repair_gaps:
        registration = repository.get_ref(imported["dataset_id"])
        assert registration is not None
        repair_result = repair_premium_index_gaps(
            registration=registration,
            repository=repository,
            adapter=BinanceCLIAdapter(
                {
                    "cli_path": args.binance_cli_path,
                    "profile": args.binance_profile,
                }
            ),
        )
    live_result = None
    if args.refresh_live:
        registration = repository.get_ref(imported["dataset_id"])
        assert registration is not None
        live_result = fill_raw_premium_index_dataset(
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
            {
                "download": download_result,
                "import": imported,
                "repair": repair_result,
                "live": live_result,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
