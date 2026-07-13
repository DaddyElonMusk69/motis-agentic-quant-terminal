from __future__ import annotations

import argparse
from bisect import bisect_left
from collections import deque
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import Any, Protocol

from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_worker.ingestion.binance_open_interest import _read_dataset_rows, _write_dataset_rows


FEATURE_FAMILY = "open_interest_regime"
FEATURE_DATA_TYPE = "feature_open_interest_regime"
FEATURE_TIMEFRAME = "15m"
OI_FEATURE_COLUMNS = (
    "oi_return_pct_2h",
    "oi_return_pct_8h",
    "oi_return_pct_24h",
    "oi_change_2h_zscore_7d",
    "general_long_short_ratio",
    "taker_long_short_ratio_avg_2h",
)
FEATURE_METADATA_COLUMNS = (
    "available_at",
    "complete",
    "source_window_start_ts",
    "source_window_end_ts",
    "source_row_count",
)
HORIZON_BARS = {"2h": 8, "8h": 32, "24h": 96}
ZSCORE_BASELINE = timedelta(days=7)
MAX_SOURCE_LOOKBACK = ZSCORE_BASELINE + timedelta(hours=2)


class FeatureRefRepository(Protocol):
    def list_refs(self) -> list[dict[str, Any]]:
        ...

    def upsert_ref(self, registration: dict[str, Any]) -> None:
        ...


def build_open_interest_regime_rows(
    rows: list[dict[str, Any]],
    *,
    timeframe: str = FEATURE_TIMEFRAME,
) -> list[dict[str, Any]]:
    if timeframe != FEATURE_TIMEFRAME:
        raise ValueError(f"Open-interest regime features require {FEATURE_TIMEFRAME} source rows.")
    sorted_rows = sorted(
        (row for row in rows if int(row.get("confirm", 1)) == 1),
        key=lambda row: _coerce_datetime(row["timestamp"]),
    )
    timestamps = [_coerce_datetime(row["timestamp"]) for row in sorted_rows]
    by_timestamp = {timestamp: row for timestamp, row in zip(timestamps, sorted_rows)}
    oi_returns_2h = [
        _return_at_horizon(
            row=row,
            timestamp=timestamp,
            by_timestamp=by_timestamp,
            horizon=timedelta(hours=2),
        )
        for row, timestamp in zip(sorted_rows, timestamps)
    ]
    prior_returns: deque[tuple[datetime, float]] = deque()
    prior_sum = 0.0
    prior_sum_squares = 0.0
    enriched: list[dict[str, Any]] = []

    for index, (row, timestamp) in enumerate(zip(sorted_rows, timestamps)):
        baseline_start = timestamp - ZSCORE_BASELINE
        while prior_returns and prior_returns[0][0] < baseline_start:
            _, removed = prior_returns.popleft()
            prior_sum -= removed
            prior_sum_squares -= removed * removed
        current_return_2h = oi_returns_2h[index]
        zscore = _zscore_against_prior(
            current_return_2h,
            count=len(prior_returns),
            total=prior_sum,
            total_squares=prior_sum_squares,
        )
        source_start_index = bisect_left(timestamps, timestamp - MAX_SOURCE_LOOKBACK)
        enriched.append(
            {
                "timestamp": _to_iso(timestamp),
                "oi_return_pct_2h": current_return_2h,
                "oi_return_pct_8h": _return_at_horizon(
                    row=row,
                    timestamp=timestamp,
                    by_timestamp=by_timestamp,
                    horizon=timedelta(hours=8),
                ),
                "oi_return_pct_24h": _return_at_horizon(
                    row=row,
                    timestamp=timestamp,
                    by_timestamp=by_timestamp,
                    horizon=timedelta(hours=24),
                ),
                "oi_change_2h_zscore_7d": zscore,
                "general_long_short_ratio": _optional_float(row.get("count_long_short_ratio_last")),
                "taker_long_short_ratio_avg_2h": _taker_average_2h(
                    timestamp=timestamp,
                    by_timestamp=by_timestamp,
                ),
                "available_at": _to_iso(timestamp + timedelta(minutes=15)),
                "complete": True,
                "source_window_start_ts": _to_iso(timestamps[source_start_index]),
                "source_window_end_ts": _to_iso(timestamp),
                "source_row_count": index - source_start_index + 1,
            }
        )
        if current_return_2h is not None:
            prior_returns.append((timestamp, current_return_2h))
            prior_sum += current_return_2h
            prior_sum_squares += current_return_2h * current_return_2h
    return enriched


def enrich_open_interest_regime_datasets(
    *,
    repository: FeatureRefRepository,
    asset: str,
    start_date: str | date = "2023-01-01",
    target_root: Path = Path(".data/market-data"),
) -> dict[str, Any]:
    source_refs = [
        ref
        for ref in repository.list_refs()
        if ref.get("asset") == asset.upper()
        and ref.get("data_type") == "open_interest"
        and ref.get("data_origin") == "derived"
        and ref.get("timeframe") == FEATURE_TIMEFRAME
    ]
    features = [
        enrich_open_interest_regime_dataset(
            source_registration=source_ref,
            repository=repository,
            start_date=start_date,
            target_root=target_root,
        )
        for source_ref in source_refs
    ]
    enriched = [item for item in features if item["status"] == "enriched"]
    return {
        "status": "enriched" if enriched else "noop",
        "asset": asset.upper(),
        "family": FEATURE_FAMILY,
        "data_type": FEATURE_DATA_TYPE,
        "dataset_count": len(source_refs),
        "feature_count": len(enriched),
        "features": enriched,
    }


def enrich_open_interest_regime_dataset(
    *,
    source_registration: dict[str, Any],
    repository: Any,
    start_date: str | date,
    target_root: Path,
) -> dict[str, Any]:
    source_rows = _read_dataset_rows(Path(source_registration["storage_uri"]))
    feature_rows = build_open_interest_regime_rows(
        source_rows,
        timeframe=str(source_registration["timeframe"]),
    )
    start = _coerce_date(start_date)
    feature_rows = [row for row in feature_rows if _coerce_datetime(row["timestamp"]).date() >= start]
    if not feature_rows:
        return {
            "dataset_id": _feature_dataset_id(source_registration),
            "status": "skipped",
            "reason": "empty_source_after_start_date",
        }
    storage_uri = _feature_storage_uri(target_root=target_root, source_registration=source_registration)
    _write_dataset_rows(storage_uri, feature_rows)
    registration = _feature_registration(
        source_registration=source_registration,
        storage_uri=storage_uri,
        rows=feature_rows,
    )
    repository.upsert_ref(registration)
    return _summary(registration, status="enriched")


def rebuild_registered_open_interest_feature_datasets(
    *,
    repository: Any,
    source_registration: dict[str, Any],
    source_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if source_registration.get("timeframe") != FEATURE_TIMEFRAME:
        return []
    list_feature_refs = getattr(repository, "list_feature_refs_for_derived", None)
    if not callable(list_feature_refs):
        return []
    feature_refs = [
        ref
        for ref in list_feature_refs(source_registration)
        if ref.get("data_type") == FEATURE_DATA_TYPE
    ]
    if not feature_refs:
        return []
    all_feature_rows = build_open_interest_regime_rows(
        source_rows,
        timeframe=str(source_registration["timeframe"]),
    )
    rebuilt: list[dict[str, Any]] = []
    for feature_ref in feature_refs:
        start_ts = _coerce_datetime(feature_ref["start_ts"])
        feature_rows = [row for row in all_feature_rows if _coerce_datetime(row["timestamp"]) >= start_ts]
        if not feature_rows:
            continue
        storage_uri = Path(feature_ref["storage_uri"])
        _write_dataset_rows(storage_uri, feature_rows)
        generated = _feature_registration(
            source_registration=source_registration,
            storage_uri=storage_uri,
            rows=feature_rows,
        )
        updated = {
            **feature_ref,
            "start_ts": generated["start_ts"],
            "end_ts": generated["end_ts"],
            "row_count": generated["row_count"],
            "schema_descriptor": generated["schema_descriptor"],
            "quality_status": generated["quality_status"],
            "ingestion_version": generated["ingestion_version"],
        }
        repository.update_ref(updated)
        rebuilt.append(_summary(updated))
    return rebuilt


def _return_at_horizon(
    *,
    row: dict[str, Any],
    timestamp: datetime,
    by_timestamp: dict[datetime, dict[str, Any]],
    horizon: timedelta,
) -> float | None:
    current = _optional_float(row.get("sum_open_interest_last"))
    previous_row = by_timestamp.get(timestamp - horizon)
    previous = _optional_float(previous_row.get("sum_open_interest_last")) if previous_row else None
    if current is None or previous in (None, 0):
        return None
    return (current / previous - 1) * 100


def _taker_average_2h(
    *,
    timestamp: datetime,
    by_timestamp: dict[datetime, dict[str, Any]],
) -> float | None:
    rows = [by_timestamp.get(timestamp - timedelta(minutes=15 * offset)) for offset in range(8)]
    if any(row is None for row in rows):
        return None
    values = [_optional_float(row.get("sum_taker_long_short_vol_ratio_avg")) for row in rows if row]
    if len(values) != 8 or any(value is None for value in values):
        return None
    return mean(float(value) for value in values if value is not None)


def _zscore_against_prior(
    value: float | None,
    *,
    count: int,
    total: float,
    total_squares: float,
) -> float | None:
    expected_count = int(ZSCORE_BASELINE / timedelta(minutes=15))
    if value is None or count != expected_count:
        return None
    baseline_mean = total / count
    variance = max(0.0, total_squares / count - baseline_mean * baseline_mean)
    standard_deviation = variance**0.5
    if standard_deviation == 0:
        return None
    return (value - baseline_mean) / standard_deviation


def _feature_registration(
    *,
    source_registration: dict[str, Any],
    storage_uri: Path,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "dataset_id": _feature_dataset_id(source_registration),
        "source_id": source_registration["source_id"],
        "asset": source_registration["asset"],
        "instrument": source_registration["instrument"],
        "data_type": FEATURE_DATA_TYPE,
        "timeframe": FEATURE_TIMEFRAME,
        "data_origin": "derived",
        "start_ts": rows[0]["timestamp"],
        "end_ts": rows[-1]["timestamp"],
        "row_count": len(rows),
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {
            "columns": ["timestamp", *OI_FEATURE_COLUMNS, *FEATURE_METADATA_COLUMNS],
            "feature_family": FEATURE_FAMILY,
            "label": "Open Interest Regime",
            "source_dataset_id": source_registration["dataset_id"],
            "source_data_type": source_registration["data_type"],
            "source_timeframe": source_registration["timeframe"],
        },
        "quality_status": "feature_enriched",
        "ingestion_version": "open_interest_feature_enrichment.v1",
    }


def _feature_dataset_id(source_registration: dict[str, Any]) -> str:
    return f"{source_registration['asset']}-{FEATURE_DATA_TYPE}-{FEATURE_TIMEFRAME}"


def _feature_storage_uri(*, target_root: Path, source_registration: dict[str, Any]) -> Path:
    return (
        target_root
        / "origin=derived"
        / f"source={source_registration['source_id']}"
        / f"type={FEATURE_DATA_TYPE}"
        / f"asset={source_registration['asset']}"
        / f"timeframe={FEATURE_TIMEFRAME}"
    )


def _summary(registration: dict[str, Any], *, status: str | None = None) -> dict[str, Any]:
    result = {
        "dataset_id": registration["dataset_id"],
        "data_type": registration["data_type"],
        "timeframe": registration["timeframe"],
        "row_count": registration["row_count"],
        "start_ts": registration["start_ts"],
        "end_ts": registration["end_ts"],
    }
    if status is not None:
        result["status"] = status
    return result


def _coerce_date(value: str | date) -> date:
    return value if isinstance(value, date) else date.fromisoformat(value)


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _optional_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build causal 15m open-interest regime features.")
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--asset", required=True)
    parser.add_argument("--start-date", default="2023-01-01")
    parser.add_argument("--target-root", default=Path(".data/market-data"), type=Path)
    args = parser.parse_args()
    result = enrich_open_interest_regime_datasets(
        repository=PostgresMarketDataRepository(args.database_url),
        asset=args.asset,
        start_date=args.start_date,
        target_root=args.target_root,
    )
    print(result)


if __name__ == "__main__":
    main()
