from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.okx_candles import OKXCandleAdapter, normalize_okx_candle


class MarketDataRefRepository(Protocol):
    def update_ref(self, registration: dict[str, Any]) -> None:
        ...

    def list_derived_refs_for_raw(self, registration: dict[str, Any]) -> list[dict[str, Any]]:
        ...


def fill_raw_candle_dataset(
    *,
    registration: dict[str, Any],
    repository: MarketDataRefRepository,
    adapter: OKXCandleAdapter,
    as_of: datetime | None = None,
    limit: int = 300,
) -> dict[str, Any]:
    if registration["data_type"] != "candles" or registration["data_origin"] != "raw":
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_candles_only",
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
        instrument=registration["instrument"],
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
            "source": "okx_cli",
            "reason": "source_returned_no_new_rows",
        }

    merged_rows = sorted(existing_rows + new_rows, key=lambda row: row["timestamp"])
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
        "source": "okx_cli",
        "derived_rebuilt": derived_rebuilt,
    }


def _read_dataset_rows(storage_uri: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted(storage_uri.glob("year=*/month=*/data.parquet")):
        rows.extend(read_candles(path))
    return rows


def _fetch_missing_rows(
    *,
    adapter: OKXCandleAdapter,
    instrument: str,
    timeframe: str,
    from_ts: datetime,
    target: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    rows_by_timestamp: dict[str, dict[str, Any]] = {}
    cursor: str | None = None

    while True:
        payload = adapter.market_candles(
            instrument,
            bar=timeframe,
            limit=limit,
            after=cursor,
        )
        normalized_rows = [normalize_okx_candle(candle) for candle in payload.get("data", [])]
        if not normalized_rows:
            break

        for row in normalized_rows:
            timestamp = _coerce_datetime(row["timestamp"])
            if from_ts <= timestamp <= target:
                rows_by_timestamp[row["timestamp"]] = row

        oldest_ts = min(_coerce_datetime(row["timestamp"]) for row in normalized_rows)
        if oldest_ts <= from_ts:
            break
        cursor = str(_to_epoch_ms(oldest_ts))

    return sorted(rows_by_timestamp.values(), key=lambda row: row["timestamp"])


def _write_dataset_rows(storage_uri: Path, rows: list[dict[str, Any]]) -> None:
    grouped: dict[tuple[int, int], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        timestamp = _coerce_datetime(row["timestamp"])
        grouped[(timestamp.year, timestamp.month)].append(row)

    for (year, month), month_rows in sorted(grouped.items()):
        path = storage_uri / f"year={year:04d}" / f"month={month:02d}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(month_rows), path)


def _rebuild_derived_refs(
    *,
    raw_registration: dict[str, Any],
    raw_rows: list[dict[str, Any]],
    repository: MarketDataRefRepository,
) -> list[dict[str, Any]]:
    rebuilt: list[dict[str, Any]] = []
    for derived_registration in repository.list_derived_refs_for_raw(raw_registration):
        timeframe = derived_registration["timeframe"]
        derived_rows = _derive_candles(
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
            "quality_status": "rebuilt",
            "schema_descriptor": {
                **derived_registration.get("schema_descriptor", {}),
                "origin": "derived",
                "derived_from_dataset_id": raw_registration["dataset_id"],
            },
        }
        repository.update_ref(updated_registration)
        rebuilt.append(
            {
                "dataset_id": derived_registration["dataset_id"],
                "timeframe": timeframe,
                "row_count": len(derived_rows),
                "start_ts": derived_rows[0]["timestamp"],
                "end_ts": derived_rows[-1]["timestamp"],
            }
        )
    return rebuilt


def _derive_candles(
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
        return [dict(row) for row in raw_rows]

    bucket_size = derived_seconds // raw_seconds
    rows = sorted(raw_rows, key=lambda row: row["timestamp"])
    derived_rows: list[dict[str, Any]] = []
    for index in range(0, len(rows), bucket_size):
        bucket = rows[index : index + bucket_size]
        if len(bucket) != bucket_size:
            continue
        derived_rows.append(_aggregate_bucket(bucket))
    return derived_rows


def _aggregate_bucket(bucket: list[dict[str, Any]]) -> dict[str, Any]:
    row = {
        "timestamp": bucket[0]["timestamp"],
        "open": float(bucket[0]["open"]),
        "high": max(float(item["high"]) for item in bucket),
        "low": min(float(item["low"]) for item in bucket),
        "close": float(bucket[-1]["close"]),
        "volume": sum(float(item["volume"]) for item in bucket),
    }
    if all("vol_ccy" in item for item in bucket):
        row["vol_ccy"] = sum(float(item["vol_ccy"]) for item in bucket)
    if all("vol_ccy_quote" in item for item in bucket):
        row["vol_ccy_quote"] = sum(float(item["vol_ccy_quote"]) for item in bucket)
    if all("confirm" in item for item in bucket):
        row["confirm"] = min(int(item["confirm"]) for item in bucket)
    return row


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_epoch_ms(value: datetime) -> int:
    return int(value.astimezone(UTC).timestamp() * 1000)


def _timeframe_delta(timeframe: str | None) -> timedelta:
    if timeframe is None:
        return timedelta(0)
    if timeframe.endswith("m"):
        return timedelta(minutes=int(timeframe[:-1]))
    if timeframe.endswith("h"):
        return timedelta(hours=int(timeframe[:-1]))
    if timeframe.endswith("d"):
        return timedelta(days=int(timeframe[:-1]))
    raise ValueError(f"Unsupported timeframe: {timeframe}")
