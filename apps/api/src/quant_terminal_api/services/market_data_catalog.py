from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


def build_catalog(rows: list[dict[str, Any]]) -> dict[str, Any]:
    assets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    data_types: set[str] = set()

    for row in rows:
        data_types.add(row["data_type"])
        assets[row["asset"]].append(_serialize_dataset(row))

    return {
        "summary": {
            "assets": len(assets),
            "datasets": len(rows),
            "data_types": sorted(data_types),
        },
        "assets": [
            {
                "asset": asset,
                "datasets": sorted(
                    datasets,
                    key=lambda item: (
                        item["data_type"],
                        item["data_origin"],
                        item["timeframe"] or "",
                    ),
                ),
            }
            for asset, datasets in sorted(assets.items())
        ],
    }


def build_refresh_plan(
    registration: dict[str, Any],
    *,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if registration["data_type"] == "candles" and registration["data_origin"] != "raw":
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_candles_only",
        }
    if registration["data_origin"] != "raw" or registration["data_type"] not in {"candles", "open_interest"}:
        return {
            "dataset_id": registration["dataset_id"],
            "status": "blocked",
            "reason": "refresh_supported_for_raw_market_data_only",
        }

    end_ts = _coerce_datetime(registration["end_ts"])
    start = end_ts + _timeframe_delta(registration["timeframe"])
    target = as_of or datetime.now(UTC)
    return {
        "dataset_id": registration["dataset_id"],
        "status": "planned",
        "asset": registration["asset"],
        "instrument": registration["instrument"],
        "data_type": registration["data_type"],
        "timeframe": registration["timeframe"],
        "from_ts": _to_iso(start),
        "to_ts": _to_iso(target),
        "source": "okx_cli",
    }


def read_parquet_candles(storage_uri: Path, *, limit: int = 200) -> list[dict[str, Any]]:
    files = sorted(storage_uri.glob("year=*/month=*/data.parquet"))
    rows: list[dict[str, Any]] = []
    for file in files:
        rows.extend(pq.read_table(file).to_pylist())
        if len(rows) >= limit:
            break

    partition_columns = {"source", "type", "asset", "timeframe", "year", "month", "origin"}
    return [
        {key: value for key, value in row.items() if key not in partition_columns}
        for row in rows[:limit]
    ]


def _serialize_dataset(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "dataset_id": row["dataset_id"],
        "asset": row["asset"],
        "instrument": row["instrument"],
        "data_type": row["data_type"],
        "timeframe": row["timeframe"],
        "data_origin": row["data_origin"],
        "start_ts": _to_iso(_coerce_datetime(row["start_ts"])) if row.get("start_ts") else None,
        "end_ts": _to_iso(_coerce_datetime(row["end_ts"])) if row.get("end_ts") else None,
        "row_count": row["row_count"],
        "storage_backend": row["storage_backend"],
        "storage_uri": row["storage_uri"],
        "schema_descriptor": row.get("schema_descriptor") if isinstance(row.get("schema_descriptor"), dict) else {},
        "quality_status": row["quality_status"],
        "ingestion_version": row["ingestion_version"],
    }


def _coerce_datetime(value: datetime | str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
