from __future__ import annotations

import argparse
from bisect import bisect_right
from datetime import UTC, date, datetime, timedelta
import json
import math
from pathlib import Path
from typing import Any

from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_worker.ingestion.binance_funding import (
    _coerce_datetime,
    _dedupe_sort_rows,
    _read_dataset_rows,
    _to_iso,
    _write_dataset_rows,
)


DATA_TYPE = "funding_features"
TIMEFRAME = "5m"
SCHEMA_VERSION = "binance-funding-features.v1"
BASE_INTERVAL = timedelta(minutes=5)
DEFAULT_START_DATE = date(2023, 1, 1)
FUNDING_FEATURE_COLUMNS = [
    "timestamp",
    "interval_end",
    "available_at",
    "symbol",
    "source_event_timestamp",
    "source_previous_event_timestamp",
    "latest_funding_rate",
    "funding_rate_change",
    "annualized_funding_rate",
    "funding_interval_hours",
    "funding_event_age_minutes",
    "minutes_to_expected_funding",
    "funding_event_is_new",
    "funding_carry_1d",
    "funding_carry_3d",
    "funding_carry_7d",
    "funding_event_count_1d",
    "funding_event_count_3d",
    "funding_event_count_7d",
    "funding_rate_mean_7d",
    "funding_rate_std_7d",
    "funding_rate_zscore_7d",
    "funding_signed_streak",
    "complete",
    "confirm",
]


def build_funding_feature_rows(
    *,
    raw_rows: list[dict[str, Any]],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    events = [
        row
        for row in _dedupe_sort_rows(raw_rows)
        if row.get("funding_rate") not in (None, "")
        and int(row.get("confirm", 1)) == 1
    ]
    if not events:
        return []

    event_times = [_coerce_datetime(row["timestamp"]) for row in events]
    event_seconds = [int(value.timestamp()) for value in event_times]
    rates = [float(row["funding_rate"]) for row in events]
    prefix = [0.0]
    prefix_squares = [0.0]
    for rate in rates:
        prefix.append(prefix[-1] + rate)
        prefix_squares.append(prefix_squares[-1] + rate * rate)

    states = [
        _event_state(
            events=events,
            event_times=event_times,
            event_seconds=event_seconds,
            rates=rates,
            prefix=prefix,
            prefix_squares=prefix_squares,
            index=index,
        )
        for index in range(len(events))
    ]

    cursor = _floor_to_5m(max(_coerce_datetime(start), event_times[0]))
    target = _floor_to_5m(_coerce_datetime(end))
    if cursor > target:
        return []
    event_index = bisect_right(event_times, cursor) - 1
    if event_index < 0:
        cursor = _floor_to_5m(event_times[0])
        event_index = 0

    output: list[dict[str, Any]] = []
    while cursor <= target:
        while (
            event_index + 1 < len(event_times)
            and event_times[event_index + 1] <= cursor
        ):
            event_index += 1
        event_time = event_times[event_index]
        event = events[event_index]
        interval_hours = int(event.get("funding_interval_hours") or 8)
        expected_next = event_time + timedelta(hours=interval_hours)
        interval_end = cursor + BASE_INTERVAL
        output.append(
            {
                "timestamp": _to_iso(cursor),
                "interval_end": _to_iso(interval_end),
                "available_at": _to_iso(interval_end),
                "symbol": str(event.get("symbol") or "").upper(),
                **states[event_index],
                "funding_event_age_minutes": int(
                    (cursor - event_time).total_seconds() // 60
                ),
                "minutes_to_expected_funding": max(
                    0,
                    int((expected_next - cursor).total_seconds() // 60),
                ),
                "funding_event_is_new": cursor == event_time,
                "complete": True,
                "confirm": 1,
            }
        )
        cursor += BASE_INTERVAL
    return output


def enrich_funding_feature_datasets(
    *,
    repository: Any,
    asset: str | None = None,
    start_date: str | date = DEFAULT_START_DATE,
    as_of: datetime | None = None,
    target_root: Path = Path(".data/market-data"),
) -> dict[str, Any]:
    requested_asset = asset.upper() if asset else None
    refs = [
        row
        for row in repository.list_refs()
        if row.get("data_type") == "funding"
        and row.get("data_origin") == "raw"
        and (requested_asset is None or row.get("asset") == requested_asset)
    ]
    results = [
        enrich_funding_feature_dataset(
            source_registration=registration,
            repository=repository,
            start_date=start_date,
            as_of=as_of,
            target_root=target_root,
        )
        for registration in refs
    ]
    return {
        "status": "enriched" if results else "noop",
        "asset": requested_asset,
        "dataset_count": len(results),
        "datasets": results,
    }


def enrich_funding_feature_dataset(
    *,
    source_registration: dict[str, Any],
    repository: Any,
    start_date: str | date,
    as_of: datetime | None,
    target_root: Path,
) -> dict[str, Any]:
    raw_rows = _read_dataset_rows(Path(source_registration["storage_uri"]))
    start = datetime.combine(_coerce_date(start_date), datetime.min.time(), tzinfo=UTC)
    now = _coerce_datetime(as_of or datetime.now(UTC))
    target = _floor_to_5m(now) - BASE_INTERVAL
    rows = build_funding_feature_rows(raw_rows=raw_rows, start=start, end=target)
    if not rows:
        return {
            "dataset_id": _dataset_id(source_registration),
            "status": "skipped",
            "reason": "empty_funding_feature_window",
        }
    storage_uri = _storage_uri(
        target_root=target_root,
        asset=str(source_registration["asset"]),
    )
    _write_dataset_rows(storage_uri, rows)
    registration = _registration(
        source_registration=source_registration,
        storage_uri=storage_uri,
        rows=rows,
    )
    repository.upsert_ref(registration)
    return _summary(registration, status="enriched")


def rebuild_registered_funding_features(
    *,
    repository: Any,
    source_registration: dict[str, Any],
    source_rows: list[dict[str, Any]],
    as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    list_refs = getattr(repository, "list_refs", None)
    if not callable(list_refs):
        return []
    refs = [
        row
        for row in list_refs()
        if row.get("data_type") == DATA_TYPE
        and row.get("data_origin") == "derived"
        and (row.get("schema_descriptor") or {}).get("derived_from_dataset_id")
        == source_registration["dataset_id"]
    ]
    rebuilt: list[dict[str, Any]] = []
    now = _coerce_datetime(as_of or datetime.now(UTC))
    target = _floor_to_5m(now) - BASE_INTERVAL
    for registration in refs:
        start = _coerce_datetime(registration["start_ts"])
        rows = build_funding_feature_rows(
            raw_rows=source_rows,
            start=start,
            end=target,
        )
        if not rows:
            continue
        _write_dataset_rows(Path(registration["storage_uri"]), rows)
        updated = _registration(
            source_registration=source_registration,
            storage_uri=Path(registration["storage_uri"]),
            rows=rows,
        )
        repository.update_ref(updated)
        rebuilt.append(_summary(updated, status="rebuilt"))
    return rebuilt


def _event_state(
    *,
    events: list[dict[str, Any]],
    event_times: list[datetime],
    event_seconds: list[int],
    rates: list[float],
    prefix: list[float],
    prefix_squares: list[float],
    index: int,
) -> dict[str, Any]:
    rate = rates[index]
    one_day = _window_stats(
        event_seconds=event_seconds,
        rates=rates,
        prefix=prefix,
        prefix_squares=prefix_squares,
        index=index,
        seconds=86400,
    )
    three_day = _window_stats(
        event_seconds=event_seconds,
        rates=rates,
        prefix=prefix,
        prefix_squares=prefix_squares,
        index=index,
        seconds=3 * 86400,
    )
    seven_day = _window_stats(
        event_seconds=event_seconds,
        rates=rates,
        prefix=prefix,
        prefix_squares=prefix_squares,
        index=index,
        seconds=7 * 86400,
    )
    interval_hours = int(events[index].get("funding_interval_hours") or 8)
    previous_rate = rates[index - 1] if index else rate
    return {
        "source_event_timestamp": _to_iso(event_times[index]),
        "source_previous_event_timestamp": (
            _to_iso(event_times[index - 1]) if index else None
        ),
        "latest_funding_rate": rate,
        "funding_rate_change": rate - previous_rate,
        "annualized_funding_rate": rate * (24.0 / interval_hours) * 365.0,
        "funding_interval_hours": interval_hours,
        "funding_carry_1d": one_day["sum"],
        "funding_carry_3d": three_day["sum"],
        "funding_carry_7d": seven_day["sum"],
        "funding_event_count_1d": one_day["count"],
        "funding_event_count_3d": three_day["count"],
        "funding_event_count_7d": seven_day["count"],
        "funding_rate_mean_7d": seven_day["mean"],
        "funding_rate_std_7d": seven_day["std"],
        "funding_rate_zscore_7d": (
            (rate - seven_day["mean"]) / seven_day["std"]
            if seven_day["std"] > 1e-15
            else 0.0
        ),
        "funding_signed_streak": _signed_streak(rates, index),
    }


def _window_stats(
    *,
    event_seconds: list[int],
    rates: list[float],
    prefix: list[float],
    prefix_squares: list[float],
    index: int,
    seconds: int,
) -> dict[str, float | int]:
    left = bisect_right(event_seconds, event_seconds[index] - seconds, 0, index + 1)
    count = index - left + 1
    total = prefix[index + 1] - prefix[left]
    square_total = prefix_squares[index + 1] - prefix_squares[left]
    average = total / count
    variance = max(0.0, square_total / count - average * average)
    return {
        "sum": total,
        "count": count,
        "mean": average,
        "std": math.sqrt(variance),
    }


def _signed_streak(rates: list[float], index: int) -> int:
    sign = 1 if rates[index] > 0 else -1 if rates[index] < 0 else 0
    if sign == 0:
        return 0
    count = 1
    cursor = index - 1
    while cursor >= 0:
        prior_sign = 1 if rates[cursor] > 0 else -1 if rates[cursor] < 0 else 0
        if prior_sign != sign:
            break
        count += 1
        cursor -= 1
    return sign * count


def _registration(
    *,
    source_registration: dict[str, Any],
    storage_uri: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "dataset_id": _dataset_id(source_registration),
        "source_id": "binance",
        "asset": str(source_registration["asset"]).upper(),
        "instrument": str(source_registration["instrument"]).upper(),
        "data_type": DATA_TYPE,
        "timeframe": TIMEFRAME,
        "data_origin": "derived",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {
            "schema_version": SCHEMA_VERSION,
            "columns": FUNDING_FEATURE_COLUMNS,
            "format": "parquet",
            "derived_from_dataset_id": source_registration["dataset_id"],
            "source": {
                "data_type": "funding",
                "origin": "raw",
                "timeframe": source_registration["timeframe"],
            },
            "timestamp_semantics": "five_minute_decision_interval_start",
            "availability_semantics": "interval_end",
            "carry_semantics": "sum_of_settled_funding_rates_in_trailing_window",
            "quality": {
                "complete_row_count": len(rows),
                "incomplete_row_count": 0,
            },
        },
        "quality_status": "feature_enriched",
        "ingestion_version": SCHEMA_VERSION,
    }


def _dataset_id(source_registration: dict[str, Any]) -> str:
    return f"{str(source_registration['asset']).lower()}-binance-{DATA_TYPE}-derived-{TIMEFRAME}"


def _storage_uri(*, target_root: Path, asset: str) -> Path:
    return (
        target_root
        / "origin=derived"
        / "source=binance"
        / f"type={DATA_TYPE}"
        / f"asset={asset.upper()}"
        / f"timeframe={TIMEFRAME}"
    )


def _summary(registration: dict[str, Any], *, status: str) -> dict[str, Any]:
    return {
        "dataset_id": registration["dataset_id"],
        "status": status,
        "asset": registration["asset"],
        "timeframe": registration["timeframe"],
        "row_count": registration["row_count"],
        "start_ts": registration["start_ts"],
        "end_ts": registration["end_ts"],
        "storage_uri": registration["storage_uri"],
    }


def _floor_to_5m(value: datetime) -> datetime:
    value = _coerce_datetime(value)
    return value.replace(
        minute=value.minute - value.minute % 5,
        second=0,
        microsecond=0,
    )


def _coerce_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build causal Binance funding feature datasets."
    )
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--asset", action="append", dest="assets")
    parser.add_argument("--start-date", default=DEFAULT_START_DATE.isoformat())
    parser.add_argument("--target-root", type=Path, default=Path(".data/market-data"))
    args = parser.parse_args()

    repository = PostgresMarketDataRepository(args.database_url)
    assets = args.assets or [None]
    results = [
        enrich_funding_feature_datasets(
            repository=repository,
            asset=asset,
            start_date=args.start_date,
            target_root=args.target_root,
        )
        for asset in assets
    ]
    print(json.dumps({"results": results}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
