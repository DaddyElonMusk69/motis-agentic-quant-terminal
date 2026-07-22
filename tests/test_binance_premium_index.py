from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
import zipfile

import pyarrow.parquet as pq

from quant_terminal_worker.ingestion.binance_premium_index import (
    _archive_specs,
    build_live_premium_index_rows,
    derive_premium_index_rows,
    fill_raw_premium_index_dataset,
    import_binance_premium_index_history,
    normalize_archive_premium_index_row,
    normalize_live_premium_index_row,
    repair_premium_index_gaps,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted_sources = []
        self.upserted = []
        self.updated = []

    def upsert_data_source(self, source_id, name, source_type):
        self.upserted_sources.append(
            {"source_id": source_id, "name": name, "source_type": source_type}
        )

    def upsert_ref(self, registration):
        self.upserted.append(registration)

    def update_ref(self, registration):
        self.updated.append(registration)

    def list_derived_refs_for_raw(self, registration):
        return [
            row
            for row in self.upserted
            if row.get("data_origin") == "derived"
            and row.get("data_type") == registration.get("data_type")
            and row.get("asset") == registration.get("asset")
        ]


def test_normalize_archive_premium_index_row_keeps_meaningful_fields():
    row = normalize_archive_premium_index_row(
        {
            "open_time": "1672531200000",
            "open": "-0.00028637",
            "high": "-0.00024141",
            "low": "-0.00046708",
            "close": "-0.00030046",
            "volume": "0",
            "close_time": "1672531499999",
            "quote_volume": "0",
            "count": "59",
        },
        symbol="BTCUSDT",
    )

    assert row == {
        "timestamp": "2023-01-01T00:00:00Z",
        "interval_end": "2023-01-01T00:05:00Z",
        "available_at": "2023-01-01T00:05:00Z",
        "symbol": "BTCUSDT",
        "premium_open": -0.00028637,
        "premium_high": -0.00024141,
        "premium_low": -0.00046708,
        "premium_close": -0.00030046,
        "sample_count": 59,
        "complete": True,
        "confirm": 1,
        "ingest_source": "archive",
    }


def test_normalize_live_premium_index_row_accepts_cli_array():
    row = normalize_live_premium_index_row(
        _kline(1672531200000, count=60),
        symbol="BTCUSDT",
    )

    assert row["timestamp"] == "2023-01-01T00:00:00Z"
    assert row["premium_close"] == -0.0003
    assert row["sample_count"] == 60
    assert row["ingest_source"] == "rest"


def test_archive_specs_use_monthly_files_then_daily_partial_month():
    specs = _archive_specs(
        start_date=date(2023, 1, 1),
        end_date=date(2023, 2, 3),
    )

    assert specs == [
        ("monthly", "2023-01"),
        ("daily", "2023-02-01"),
        ("daily", "2023-02-02"),
        ("daily", "2023-02-03"),
    ]


def test_derive_premium_index_rows_preserves_ohlc_and_availability():
    rows = [
        _premium_row(
            "2026-06-01T00:00:00Z",
            premium_open=1.0,
            premium_high=3.0,
            premium_low=0.0,
            premium_close=2.0,
            sample_count=60,
        ),
        _premium_row(
            "2026-06-01T00:05:00Z",
            premium_open=2.0,
            premium_high=5.0,
            premium_low=1.0,
            premium_close=4.0,
            sample_count=58,
        ),
        _premium_row(
            "2026-06-01T00:10:00Z",
            premium_open=4.0,
            premium_high=6.0,
            premium_low=2.0,
            premium_close=3.0,
            sample_count=60,
        ),
    ]

    derived = derive_premium_index_rows(
        raw_rows=rows,
        raw_timeframe="5m",
        derived_timeframe="15m",
    )

    assert derived == [
        {
            "timestamp": "2026-06-01T00:00:00Z",
            "interval_end": "2026-06-01T00:15:00Z",
            "available_at": "2026-06-01T00:15:00Z",
            "symbol": "BTCUSDT",
            "premium_open": 1.0,
            "premium_high": 6.0,
            "premium_low": 0.0,
            "premium_close": 3.0,
            "sample_count": 178,
            "complete": True,
            "confirm": 1,
            "ingest_source": "derived",
            "source_row_count": 3,
        }
    ]


def test_derive_premium_index_rows_skips_missing_or_incomplete_bucket():
    missing = [
        _premium_row("2026-06-01T00:00:00Z"),
        _premium_row("2026-06-01T00:10:00Z"),
    ]
    incomplete = [
        _premium_row("2026-06-01T00:00:00Z"),
        _premium_row("2026-06-01T00:05:00Z", complete=False),
        _premium_row("2026-06-01T00:10:00Z"),
    ]

    assert derive_premium_index_rows(
        raw_rows=missing,
        raw_timeframe="5m",
        derived_timeframe="15m",
    ) == []
    assert derive_premium_index_rows(
        raw_rows=incomplete,
        raw_timeframe="5m",
        derived_timeframe="15m",
    ) == []


def test_import_writes_monthly_parquet_and_registers_premium_index(tmp_path: Path):
    source_dir = tmp_path / "downloads" / "monthly"
    source_dir.mkdir(parents=True)
    _write_zip(
        source_dir / "BTCUSDT-5m-2023-01.zip",
        "BTCUSDT-5m-2023-01.csv",
        [
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
            "1672531200000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672531499999,0,60,0,0,0",
            "1672531500000,-0.00030,-0.00010,-0.00050,-0.00020,0,1672531799999,0,58,0,0,0",
        ],
    )
    repository = FakeRepository()

    result = import_binance_premium_index_history(
        source_dir=tmp_path / "downloads",
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
    )

    assert result["dataset_id"] == "btc-binance-premium_index-raw-5m"
    assert result["row_count"] == 2
    assert result["quality"] == {
        "missing_interval_count": 0,
        "incomplete_row_count": 0,
        "complete_row_count": 2,
        "reduced_sample_count": 1,
    }
    registration = repository.upserted[0]
    assert registration["data_type"] == "premium_index"
    assert registration["schema_descriptor"]["units"] == "dimensionless_ratio"
    path = (
        tmp_path
        / ".data/market-data/origin=raw/source=binance/type=premium_index/asset=BTC/timeframe=5m/year=2023/month=01/data.parquet"
    )
    rows = pq.read_table(path).to_pylist()
    assert [row["premium_close"] for row in rows] == [-0.0003, -0.0002]


def test_import_registers_requested_derived_timeframe(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-5m-2023-01.zip",
        "BTCUSDT-5m-2023-01.csv",
        [
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
            "1672531200000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672531499999,0,60,0,0,0",
            "1672531500000,-0.00030,-0.00010,-0.00050,-0.00020,0,1672531799999,0,60,0,0,0",
            "1672531800000,-0.00020,0.00010,-0.00030,0.00005,0,1672532099999,0,60,0,0,0",
        ],
    )
    repository = FakeRepository()

    result = import_binance_premium_index_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        derived_timeframes=("15m",),
    )

    assert [row["dataset_id"] for row in result["derived"]] == [
        "btc-binance-premium_index-derived-15m"
    ]
    assert [row["data_origin"] for row in repository.upserted] == ["raw", "derived"]
    path = (
        tmp_path
        / ".data/market-data/origin=derived/source=binance/type=premium_index/asset=BTC/timeframe=15m/year=2023/month=01/data.parquet"
    )
    rows = pq.read_table(path).to_pylist()
    assert rows[0]["premium_open"] == -0.00028
    assert rows[0]["premium_high"] == 0.0001
    assert rows[0]["premium_low"] == -0.0005
    assert rows[0]["premium_close"] == 0.00005
    assert rows[0]["available_at"] == "2023-01-01T00:15:00Z"


def test_fill_rebuilds_registered_derived_timeframe(tmp_path: Path):
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-5m-2023-01.zip",
        "BTCUSDT-5m-2023-01.csv",
        [
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
            "1672531200000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672531499999,0,60,0,0,0",
            "1672531500000,-0.00030,-0.00010,-0.00050,-0.00020,0,1672531799999,0,60,0,0,0",
            "1672531800000,-0.00020,0.00010,-0.00030,0.00005,0,1672532099999,0,60,0,0,0",
        ],
    )
    repository = FakeRepository()
    import_binance_premium_index_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        derived_timeframes=("15m",),
    )

    class Adapter:
        def premium_index_klines(self, **kwargs):
            return [
                _kline(1672532100000),
                _kline(1672532400000),
                _kline(1672532700000),
            ]

    result = fill_raw_premium_index_dataset(
        registration=repository.upserted[0],
        repository=repository,
        adapter=Adapter(),
        as_of=datetime(2023, 1, 1, 0, 31, tzinfo=UTC),
    )

    assert [row["dataset_id"] for row in result["derived_rebuilt"]] == [
        "btc-binance-premium_index-derived-15m"
    ]
    derived_path = (
        tmp_path
        / ".data/market-data/origin=derived/source=binance/type=premium_index/asset=BTC/timeframe=15m/year=2023/month=01/data.parquet"
    )
    rows = pq.read_table(derived_path).to_pylist()
    assert [row["timestamp"] for row in rows] == [
        "2023-01-01T00:00:00Z",
        "2023-01-01T00:15:00Z",
    ]
    assert repository.updated[-1]["row_count"] == 2


def test_build_live_rows_excludes_forming_bar_and_stops_at_gap():
    class Adapter:
        def premium_index_klines(self, **kwargs):
            assert kwargs["interval"] == "5m"
            return [
                _kline(1672531500000),
                _kline(1672531800000, count=58),
                _kline(1672532400000),
                _kline(1672532700000, count=20),
            ]

    rows = build_live_premium_index_rows(
        adapter=Adapter(),
        symbol="BTCUSDT",
        from_ts=datetime(2023, 1, 1, 0, 5, tzinfo=UTC),
        target=datetime(2023, 1, 1, 0, 25, tzinfo=UTC),
        as_of=datetime(2023, 1, 1, 0, 27, tzinfo=UTC),
    )

    assert [row["timestamp"] for row in rows] == [
        "2023-01-01T00:05:00Z",
        "2023-01-01T00:10:00Z",
    ]
    assert rows[-1]["sample_count"] == 58


def test_fill_appends_only_closed_contiguous_rows(tmp_path: Path):
    repository = FakeRepository()
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-5m-2023-01.zip",
        "BTCUSDT-5m-2023-01.csv",
        [
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
            "1672531200000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672531499999,0,60,0,0,0",
        ],
    )
    imported = import_binance_premium_index_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
    )
    registration = repository.upserted[0]

    class Adapter:
        def premium_index_klines(self, **kwargs):
            return [_kline(1672531500000), _kline(1672531800000)]

    result = fill_raw_premium_index_dataset(
        registration=registration,
        repository=repository,
        adapter=Adapter(),
        as_of=datetime(2023, 1, 1, 0, 16, tzinfo=UTC),
    )

    assert imported["row_count"] == 1
    assert result["status"] == "filled"
    assert result["rows_added"] == 2
    assert result["end_ts"] == "2023-01-01T00:10:00Z"
    assert repository.updated[0]["row_count"] == 3


def test_repair_fills_internal_archive_gap(tmp_path: Path):
    repository = FakeRepository()
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-5m-2023-01.zip",
        "BTCUSDT-5m-2023-01.csv",
        [
            "open_time,open,high,low,close,volume,close_time,quote_volume,count,taker_buy_volume,taker_buy_quote_volume,ignore",
            "1672531200000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672531499999,0,60,0,0,0",
            "1672531800000,-0.00028,-0.00020,-0.00040,-0.00030,0,1672532099999,0,60,0,0,0",
        ],
    )
    import_binance_premium_index_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
    )
    registration = repository.upserted[0]

    class Adapter:
        def premium_index_klines(self, **kwargs):
            return [_kline(1672531500000)]

    result = repair_premium_index_gaps(
        registration=registration,
        repository=repository,
        adapter=Adapter(),
        as_of=datetime(2023, 1, 2, tzinfo=UTC),
    )

    assert result == {
        "dataset_id": "btc-binance-premium_index-raw-5m",
        "status": "repaired",
        "gap_runs": 1,
        "rows_added": 1,
        "remaining_missing_intervals": 0,
        "row_count": 3,
        "derived_rebuilt": [],
    }
    assert repository.updated[-1]["schema_descriptor"]["quality"][
        "missing_interval_count"
    ] == 0


def _kline(open_time: int, *, count: int = 60) -> list[object]:
    return [
        open_time,
        "-0.00028",
        "-0.00020",
        "-0.00040",
        "-0.00030",
        "0",
        open_time + 299999,
        "0",
        count,
        "0",
        "0",
        "0",
    ]


def _premium_row(
    timestamp: str,
    *,
    premium_open: float = 1.0,
    premium_high: float = 2.0,
    premium_low: float = 0.0,
    premium_close: float = 1.5,
    sample_count: int = 60,
    complete: bool = True,
) -> dict[str, object]:
    start = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    interval_end = start.replace(tzinfo=UTC) + timedelta(minutes=5)
    return {
        "timestamp": timestamp,
        "interval_end": interval_end.isoformat().replace("+00:00", "Z"),
        "available_at": interval_end.isoformat().replace("+00:00", "Z"),
        "symbol": "BTCUSDT",
        "premium_open": premium_open,
        "premium_high": premium_high,
        "premium_low": premium_low,
        "premium_close": premium_close,
        "sample_count": sample_count,
        "complete": complete,
        "confirm": 1 if complete else 0,
        "ingest_source": "archive",
    }


def _write_zip(path: Path, csv_name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(csv_name, "\n".join(lines) + "\n")
