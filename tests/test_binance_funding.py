from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_worker.ingestion.binance_funding import (
    fill_raw_funding_dataset,
    import_binance_funding_history,
    normalize_funding_row,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted_sources = []
        self.upserted = []
        self.updated = []

    def upsert_data_source(self, source_id, name, source_type):
        self.upserted_sources.append({"source_id": source_id, "name": name, "source_type": source_type})

    def upsert_ref(self, registration):
        self.upserted.append(registration)

    def update_ref(self, registration):
        self.updated.append(registration)


def test_normalize_funding_row_accepts_archive_csv_fields():
    row = normalize_funding_row(
        {
            "calc_time": "1780272000001",
            "funding_interval_hours": "8",
            "last_funding_rate": "0.00005703",
        }
    )

    assert row == {
        "timestamp": "2026-06-01T00:00:00Z",
        "symbol": "",
        "funding_rate": 0.00005703,
        "funding_interval_hours": 8,
        "confirm": 1,
    }


def test_normalize_funding_row_accepts_live_cli_fields():
    row = normalize_funding_row(
        {
            "symbol": "BTCUSDT",
            "fundingTime": 1783958400004,
            "fundingRate": "0.00010000",
            "markPrice": "62603.79932609",
        }
    )

    assert row == {
        "timestamp": "2026-07-13T16:00:00Z",
        "symbol": "BTCUSDT",
        "funding_rate": 0.0001,
        "funding_interval_hours": 8,
        "mark_price": 62603.79932609,
        "confirm": 1,
    }


def test_import_binance_funding_history_writes_monthly_parquet_and_registers_ref(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-fundingRate-2026-06.zip",
        "BTCUSDT-fundingRate-2026-06.csv",
        [
            "calc_time,funding_interval_hours,last_funding_rate",
            "1780272000001,8,0.00005703",
            "1780300800001,8,0.00004438",
        ],
    )
    repository = FakeRepository()

    result = import_binance_funding_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        ingestion_version="binance-funding-v1",
    )

    assert result["status"] == "imported"
    assert result["raw"]["dataset_id"] == "btc-binance-funding-raw-8h"
    assert result["raw"]["row_count"] == 2
    assert repository.upserted_sources == [{"source_id": "binance", "name": "Binance", "source_type": "cex"}]
    assert [item["dataset_id"] for item in repository.upserted] == ["btc-binance-funding-raw-8h"]

    raw_path = tmp_path / ".data/market-data/origin=raw/source=binance/type=funding/asset=BTC/timeframe=8h/year=2026/month=06/data.parquet"
    rows = pq.read_table(raw_path).to_pylist()
    assert [row["timestamp"] for row in rows] == [
        "2026-06-01T00:00:00Z",
        "2026-06-01T08:00:00Z",
    ]
    assert rows[0]["funding_rate"] == 0.00005703


def test_import_binance_funding_history_can_ignore_rows_before_min_timestamp(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-fundingRate-2026-06.zip",
        "BTCUSDT-fundingRate-2026-06.csv",
        [
            "calc_time,funding_interval_hours,last_funding_rate",
            "1780272000001,8,0.00005703",
            "1780300800001,8,0.00004438",
        ],
    )
    repository = FakeRepository()

    result = import_binance_funding_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        ingestion_version="binance-funding-v1",
        min_timestamp=datetime(2026, 6, 1, 8, tzinfo=UTC),
    )

    assert result["raw"]["row_count"] == 1
    assert repository.upserted[0]["start_ts"] == "2026-06-01T08:00:00Z"


def test_fill_raw_funding_dataset_appends_live_cli_rows(tmp_path: Path):
    storage_uri = tmp_path / "origin=raw/source=binance/type=funding/asset=BTC/timeframe=8h"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-06-30T16:00:00Z",
                    "symbol": "BTCUSDT",
                    "funding_rate": 0.0001,
                    "funding_interval_hours": 8,
                    "confirm": 1,
                }
            ]
        ),
        month_path,
    )
    repository = FakeRepository()

    class FakeAdapter:
        def funding_rate_history(self, **kwargs):
            assert kwargs["symbol"] == "BTCUSDT"
            return [
                {
                    "symbol": "BTCUSDT",
                    "fundingTime": 1782864000000,
                    "fundingRate": "0.0002",
                    "markPrice": "62000.0",
                }
            ]

    result = fill_raw_funding_dataset(
        registration={
            "dataset_id": "btc-binance-funding-raw-8h",
            "source_id": "binance",
            "asset": "BTC",
            "instrument": "BTCUSDT",
            "data_type": "funding",
            "timeframe": "8h",
            "data_origin": "raw",
            "start_ts": "2026-06-30T16:00:00Z",
            "end_ts": "2026-06-30T16:00:00Z",
            "row_count": 1,
            "storage_uri": str(storage_uri),
        },
        repository=repository,
        adapter=FakeAdapter(),
        as_of=datetime(2026, 7, 1, 8, tzinfo=UTC),
    )

    assert result == {
        "dataset_id": "btc-binance-funding-raw-8h",
        "status": "filled",
        "rows_added": 1,
        "start_ts": "2026-06-30T16:00:00Z",
        "end_ts": "2026-07-01T00:00:00Z",
        "row_count": 2,
        "from_ts": "2026-07-01T00:00:00Z",
        "to_ts": "2026-07-01T08:00:00Z",
        "source": "binance_cli",
    }
    assert repository.updated[0]["row_count"] == 2


def _write_zip(path: Path, name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, "\n".join(lines) + "\n")
