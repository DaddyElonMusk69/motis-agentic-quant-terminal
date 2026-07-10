from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import zipfile

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_worker.ingestion.binance_open_interest import (
    derive_open_interest_rows,
    fill_raw_open_interest_dataset,
    import_binance_open_interest_history,
    normalize_open_interest_row,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted_sources = []
        self.upserted = []
        self.updated = []
        self.derived_refs = []

    def upsert_data_source(self, source_id, name, source_type):
        self.upserted_sources.append({"source_id": source_id, "name": name, "source_type": source_type})

    def upsert_ref(self, registration):
        self.upserted.append(registration)

    def update_ref(self, registration):
        self.updated.append(registration)

    def list_derived_refs_for_raw(self, registration):
        return self.derived_refs


def test_normalize_open_interest_row_accepts_downloaded_csv_fields():
    row = normalize_open_interest_row(
        {
            "create_time": "2021-12-01 00:05:00",
            "symbol": "SOLUSDT",
            "sum_open_interest": "1164342.00000000",
            "sum_open_interest_value": "242826336.55645758",
            "count_toptrader_long_short_ratio": "6.227905227529146",
            "sum_toptrader_long_short_ratio": "0.9987935993",
            "count_long_short_ratio": "6.57619477006312",
            "sum_taker_long_short_vol_ratio": "0.7346938776",
        }
    )

    assert row == {
        "timestamp": "2021-12-01T00:05:00Z",
        "symbol": "SOLUSDT",
        "sum_open_interest": 1164342.0,
        "sum_open_interest_value": 242826336.55645758,
        "count_toptrader_long_short_ratio": 6.227905227529146,
        "sum_toptrader_long_short_ratio": 0.9987935993,
        "count_long_short_ratio": 6.57619477006312,
        "sum_taker_long_short_vol_ratio": 0.7346938776,
        "confirm": 1,
    }


def test_normalize_open_interest_row_snaps_downloaded_rows_to_5m_boundary():
    row = normalize_open_interest_row(
        {
            "create_time": "2024-03-04 06:00:01",
            "symbol": "ETHUSDT",
            "sum_open_interest": "775814.79300000",
            "sum_open_interest_value": "2690546466.92330700",
        }
    )

    assert row["timestamp"] == "2024-03-04T06:00:00Z"


def test_import_binance_open_interest_history_writes_monthly_parquet_and_registers_refs(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "SOLUSDT-metrics-2021-12-01.zip",
        "SOLUSDT-metrics-2021-12-01.csv",
            [
                "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio",
                "2021-12-01 00:00:00,SOLUSDT,100,200,1.0,1.1,1.2,1.3",
                "2021-12-01 00:10:00,SOLUSDT,120,240,1.2,1.3,1.4,1.5",
                "2021-12-01 00:05:00,SOLUSDT,110,220,1.1,1.2,1.3,1.4",
                "2021-12-01 00:05:00,SOLUSDT,111,222,1.1,1.2,1.3,1.4",
        ],
    )
    repository = FakeRepository()

    result = import_binance_open_interest_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="SOL",
        symbol="SOLUSDT",
        ingestion_version="binance-metrics-v1",
        derived_timeframes=("15m",),
    )

    assert result["status"] == "imported"
    assert result["raw"]["dataset_id"] == "sol-binance-open_interest-raw-5m"
    assert result["raw"]["row_count"] == 3
    assert result["derived"][0]["dataset_id"] == "sol-binance-open_interest-derived-15m"
    assert repository.upserted_sources == [{"source_id": "binance", "name": "Binance", "source_type": "cex"}]
    assert [item["dataset_id"] for item in repository.upserted] == [
        "sol-binance-open_interest-raw-5m",
        "sol-binance-open_interest-derived-15m",
    ]

    raw_path = tmp_path / ".data/market-data/origin=raw/source=binance/type=open_interest/asset=SOL/timeframe=5m/year=2021/month=12/data.parquet"
    rows = pq.read_table(raw_path).to_pylist()
    assert [row["timestamp"] for row in rows] == [
        "2021-12-01T00:00:00Z",
        "2021-12-01T00:05:00Z",
        "2021-12-01T00:10:00Z",
    ]
    assert rows[1]["sum_open_interest"] == 111.0


def test_import_binance_open_interest_history_discovers_nested_year_folders(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    nested_dir = source_dir / "ETHUSDT_2023"
    nested_dir.mkdir(parents=True)
    _write_zip(
        nested_dir / "ETHUSDT-metrics-2023-01-01.zip",
        "ETHUSDT-metrics-2023-01-01.csv",
        [
            "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio",
            "2023-01-01 00:00:00,ETHUSDT,100,200,1.0,1.1,1.2,1.3",
        ],
    )
    repository = FakeRepository()

    result = import_binance_open_interest_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="ETH",
        symbol="ETHUSDT",
        ingestion_version="binance-metrics-v1",
        derived_timeframes=(),
    )

    assert result["raw"]["dataset_id"] == "eth-binance-open_interest-raw-5m"
    assert result["raw"]["row_count"] == 1
    assert repository.upserted[0]["start_ts"] == "2023-01-01T00:00:00Z"


def test_import_binance_open_interest_history_can_ignore_supplemental_rows_before_min_timestamp(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    supplemental_dir = source_dir / "missing" / "BTCUSDT"
    main_dir = source_dir / "BTCUSDT_2023"
    supplemental_dir.mkdir(parents=True)
    main_dir.mkdir(parents=True)
    header = "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio"
    _write_zip(
        supplemental_dir / "BTCUSDT-metrics-2021-01-20.zip",
        "BTCUSDT-metrics-2021-01-20.csv",
        [
            header,
            "2021-01-20 00:00:00,BTCUSDT,100,200,1.0,1.1,1.2,1.3",
        ],
    )
    _write_zip(
        main_dir / "BTCUSDT-metrics-2023-01-01.zip",
        "BTCUSDT-metrics-2023-01-01.csv",
        [
            header,
            "2023-01-01 00:00:00,BTCUSDT,300,400,1.0,1.1,1.2,1.3",
        ],
    )
    repository = FakeRepository()

    result = import_binance_open_interest_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        ingestion_version="binance-metrics-v1",
        derived_timeframes=(),
        min_timestamp=datetime(2023, 1, 1, tzinfo=UTC),
    )

    assert result["raw"]["row_count"] == 1
    assert result["raw"]["start_ts"] == "2023-01-01T00:00:00Z"


def test_derive_open_interest_rows_uses_oi_specific_bucket_fields():
    rows = [
        _oi_row("2026-06-01T00:00:00Z", oi=100, value=1000, ratio=1.0),
        _oi_row("2026-06-01T00:05:00Z", oi=105, value=1200, ratio=1.2),
        _oi_row("2026-06-01T00:10:00Z", oi=95, value=900, ratio=1.4),
        _oi_row("2026-06-01T00:15:00Z", oi=110, value=1300, ratio=1.6),
    ]

    derived = derive_open_interest_rows(raw_rows=rows, raw_timeframe="5m", derived_timeframe="15m")

    assert derived == [
        {
            "timestamp": "2026-06-01T00:00:00Z",
            "symbol": "SOLUSDT",
            "sum_open_interest_first": 100.0,
            "sum_open_interest_last": 95.0,
            "sum_open_interest_min": 95.0,
            "sum_open_interest_max": 105.0,
            "sum_open_interest_change": -5.0,
            "sum_open_interest_value_first": 1000.0,
            "sum_open_interest_value_last": 900.0,
            "sum_open_interest_value_min": 900.0,
            "sum_open_interest_value_max": 1200.0,
            "sum_open_interest_value_change": -100.0,
            "sum_taker_long_short_vol_ratio_avg": 1.2,
            "sum_taker_long_short_vol_ratio_last": 1.4,
            "confirm": 1,
        }
    ]


def test_derive_open_interest_rows_skips_bucket_with_missing_5m_timestamp():
    rows = [
        _oi_row("2026-06-01T00:00:00Z", oi=100, value=1000, ratio=1.0),
        _oi_row("2026-06-01T00:05:00Z", oi=105, value=1200, ratio=1.2),
        _oi_row("2026-06-01T00:20:00Z", oi=95, value=900, ratio=1.4),
    ]

    assert derive_open_interest_rows(raw_rows=rows, raw_timeframe="5m", derived_timeframe="15m") == []


def test_fill_raw_open_interest_dataset_appends_live_cli_rows_and_rebuilds_derived(tmp_path: Path):
    storage_uri = tmp_path / "origin=raw/source=binance/type=open_interest/asset=SOL/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist([_oi_row("2026-06-01T00:00:00Z", oi=100, value=1000, ratio=1.0)]),
        month_path,
    )
    derived_uri = tmp_path / "origin=derived/source=binance/type=open_interest/asset=SOL/timeframe=15m"
    repository = FakeRepository()
    repository.derived_refs = [
        {
            "dataset_id": "sol-binance-open_interest-derived-15m",
            "source_id": "binance",
            "asset": "SOL",
            "instrument": "SOLUSDT",
            "data_type": "open_interest",
            "timeframe": "15m",
            "data_origin": "derived",
            "storage_backend": "parquet",
            "storage_uri": str(derived_uri),
            "schema_descriptor": {},
            "quality_status": "ingested",
            "ingestion_version": "binance-metrics-v1",
        }
    ]

    class FakeBinanceAdapter:
        def __init__(self) -> None:
            self.calls = []

        def open_interest_statistics(self, *, symbol, period, limit, start_time_ms=None, end_time_ms=None):
            self.calls.append(
                {
                    "symbol": symbol,
                    "period": period,
                    "limit": limit,
                    "start_time_ms": start_time_ms,
                    "end_time_ms": end_time_ms,
                }
            )
            return [
                {"symbol": "SOLUSDT", "sumOpenInterest": "105", "sumOpenInterestValue": "1050", "timestamp": 1780272300000},
                {"symbol": "SOLUSDT", "sumOpenInterest": "110", "sumOpenInterestValue": "1100", "timestamp": 1780272600000},
            ]

    result = fill_raw_open_interest_dataset(
        registration={
            "dataset_id": "sol-binance-open_interest-raw-5m",
            "source_id": "binance",
            "asset": "SOL",
            "instrument": "SOLUSDT",
            "data_type": "open_interest",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {},
            "quality_status": "ingested",
            "ingestion_version": "binance-metrics-v1",
        },
        repository=repository,
        adapter=FakeBinanceAdapter(),
        as_of=datetime(2026, 6, 1, 0, 10, tzinfo=UTC),
        limit=1000,
    )

    assert result["status"] == "filled"
    assert result["rows_added"] == 2
    assert result["end_ts"] == "2026-06-01T00:10:00Z"
    assert result["derived_rebuilt"][0]["dataset_id"] == "sol-binance-open_interest-derived-15m"
    assert repository.updated[-1]["quality_status"] == "derived"


def _write_zip(path: Path, name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, "\n".join(lines))


def _oi_row(timestamp: str, *, oi: float, value: float, ratio: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "symbol": "SOLUSDT",
        "sum_open_interest": float(oi),
        "sum_open_interest_value": float(value),
        "sum_taker_long_short_vol_ratio": float(ratio),
        "confirm": 1,
    }
