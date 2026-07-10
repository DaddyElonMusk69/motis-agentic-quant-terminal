from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_api.services.market_data_catalog import (
    build_catalog,
    build_refresh_plan,
    read_parquet_candles,
)


def test_build_catalog_groups_multiple_data_types_by_asset():
    rows = [
        {
            "dataset_id": "btc-candles",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 5, 1, tzinfo=UTC),
            "end_ts": datetime(2026, 5, 31, tzinfo=UTC),
            "row_count": 100,
            "storage_backend": "parquet",
            "storage_uri": ".data/market-data/origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m",
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        {
            "dataset_id": "btc-open-interest",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "open_interest",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 5, 1, tzinfo=UTC),
            "end_ts": datetime(2026, 5, 30, tzinfo=UTC),
            "row_count": 90,
            "storage_backend": "parquet",
            "storage_uri": ".data/market-data/type=open_interest/asset=BTC",
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
    ]

    catalog = build_catalog(rows)

    assert catalog["summary"] == {"assets": 1, "datasets": 2, "data_types": ["candles", "open_interest"]}
    assert catalog["assets"][0]["asset"] == "BTC"
    assert [dataset["data_type"] for dataset in catalog["assets"][0]["datasets"]] == [
        "candles",
        "open_interest",
    ]


def test_build_refresh_plan_only_allows_raw_candle_datasets():
    registration = {
        "dataset_id": "btc-candles",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "data_type": "candles",
        "timeframe": "5m",
        "data_origin": "raw",
        "end_ts": datetime(2026, 5, 31, 23, 55, tzinfo=UTC),
    }

    plan = build_refresh_plan(registration, as_of=datetime(2026, 6, 3, 12, 0, tzinfo=UTC))

    assert plan == {
        "dataset_id": "btc-candles",
        "status": "planned",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "data_type": "candles",
        "timeframe": "5m",
        "from_ts": "2026-06-01T00:00:00Z",
        "to_ts": "2026-06-03T12:00:00Z",
        "source": "okx_cli",
    }

def test_read_parquet_candles_returns_limited_rows(tmp_path: Path):
    partition = tmp_path / "year=2026" / "month=06"
    partition.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"timestamp": "2026-06-01T00:00:00Z", "open": 100.0, "close": 101.0},
                {"timestamp": "2026-06-01T00:05:00Z", "open": 101.0, "close": 102.0},
            ]
        ),
        partition / "data.parquet",
    )

    rows = read_parquet_candles(tmp_path, limit=1)

    assert rows == [{"timestamp": "2026-06-01T00:00:00Z", "open": 100.0, "close": 101.0}]
