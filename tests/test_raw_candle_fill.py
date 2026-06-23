from datetime import UTC, datetime, timedelta
from pathlib import Path

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.raw_candle_fill import fill_raw_candle_dataset


class FakeRepository:
    def __init__(self) -> None:
        self.updated_registration = None
        self.derived_refs = []
        self.updated_registrations = []

    def update_ref(self, registration):
        self.updated_registration = registration
        self.updated_registrations.append(registration)

    def list_derived_refs_for_raw(self, registration):
        return self.derived_refs


class FakeOKXAdapter:
    def __init__(self) -> None:
        self.calls = []

    def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None):
        self.calls.append({"inst_id": inst_id, "bar": bar, "limit": limit, "after": after})
        return {
            "code": "0",
            "data": [
                ["1780272300000", "101", "106", "100", "104", "8.75", "0.875", "910", "1"],
                ["1780272600000", "104", "108", "103", "107", "6.0", "0.6", "642", "1"],
            ],
        }


class EmptyOKXAdapter:
    def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None):
        return {"code": "0", "data": []}


def test_fill_raw_candle_dataset_merges_tail_rows_and_updates_registration(tmp_path: Path):
    storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    _write_parquet(
        month_path,
        [
            {
                "timestamp": "2026-06-01T00:00:00Z",
                "open": 100.0,
                "high": 105.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 12.5,
            }
        ],
    )
    registration = {
        "dataset_id": "btc-raw-5m",
        "source_id": "okx",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "data_type": "candles",
        "timeframe": "5m",
        "data_origin": "raw",
        "start_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        "end_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
        "row_count": 1,
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
        "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
        "quality_status": "ingested",
        "ingestion_version": "legacy",
    }

    repository = FakeRepository()
    adapter = FakeOKXAdapter()
    result = fill_raw_candle_dataset(
        registration=registration,
        repository=repository,
        adapter=adapter,
        as_of=datetime(2026, 6, 1, 0, 10, tzinfo=UTC),
    )

    assert result["status"] == "filled"
    assert result["rows_added"] == 2
    assert result["row_count"] == 3
    assert result["start_ts"] == "2026-06-01T00:00:00Z"
    assert result["end_ts"] == "2026-06-01T00:10:00Z"
    assert adapter.calls == [
        {
            "inst_id": "BTC-USDT-SWAP",
            "bar": "5m",
            "limit": 300,
            "after": None,
        }
    ]
    assert repository.updated_registration["dataset_id"] == "btc-raw-5m"
    assert repository.updated_registration["row_count"] == 3
    assert repository.updated_registration["quality_status"] == "updated"
    assert [row["timestamp"] for row in read_candles(month_path)] == [
        "2026-06-01T00:00:00Z",
        "2026-06-01T00:05:00Z",
        "2026-06-01T00:10:00Z",
    ]


def test_fill_raw_candle_dataset_rebuilds_matching_derived_candle_datasets(tmp_path: Path):
    raw_storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    raw_month_path = raw_storage_uri / "year=2026/month=06/data.parquet"
    raw_month_path.parent.mkdir(parents=True)
    _write_parquet(
        raw_month_path,
        [
            _row("2026-06-01T00:00:00Z", 100, 105, 99, 101, 12.5),
            _row("2026-06-01T00:05:00Z", 101, 106, 100, 104, 8.75),
        ],
    )
    derived_storage_uri = tmp_path / "origin=derived/source=okx/type=candles/asset=BTC/timeframe=10m"
    derived_month_path = derived_storage_uri / "year=2026/month=06/data.parquet"
    derived_month_path.parent.mkdir(parents=True)
    _write_parquet(derived_month_path, [_row("2026-06-01T00:00:00Z", 1, 1, 1, 1, 1)])
    registration = {
        "dataset_id": "btc-raw-5m",
        "source_id": "okx",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "data_type": "candles",
        "timeframe": "5m",
        "data_origin": "raw",
        "start_ts": datetime(2026, 6, 1, 0, 5, tzinfo=UTC),
        "end_ts": datetime(2026, 6, 1, 0, 5, tzinfo=UTC),
        "row_count": 2,
        "storage_backend": "parquet",
        "storage_uri": str(raw_storage_uri),
        "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
        "quality_status": "ingested",
        "ingestion_version": "legacy",
    }
    derived_registration = {
        **registration,
        "dataset_id": "btc-derived-10m",
        "timeframe": "10m",
        "data_origin": "derived",
        "row_count": 1,
        "storage_uri": str(derived_storage_uri),
    }
    repository = FakeRepository()
    repository.derived_refs = [derived_registration]

    result = fill_raw_candle_dataset(
        registration=registration,
        repository=repository,
        adapter=FakeOKXAdapter(),
        as_of=datetime(2026, 6, 1, 0, 10, tzinfo=UTC),
    )

    assert result["status"] == "filled"
    assert result["derived_rebuilt"] == [
        {
            "dataset_id": "btc-derived-10m",
            "timeframe": "10m",
            "row_count": 1,
            "start_ts": "2026-06-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
        }
    ]
    rebuilt_row = read_candles(derived_month_path)[0]
    assert {
        key: rebuilt_row[key]
        for key in ("timestamp", "open", "high", "low", "close", "volume")
    } == {
        "timestamp": "2026-06-01T00:00:00Z",
        "open": 100.0,
        "high": 106.0,
        "low": 99.0,
        "close": 104.0,
        "volume": 21.25,
    }
    assert rebuilt_row["ema_676"] == 104.0
    assert rebuilt_row["ema_warmup_count_676"] == 1
    assert repository.updated_registrations[-1]["dataset_id"] == "btc-derived-10m"
    assert repository.updated_registrations[-1]["quality_status"] == "ema_enriched"
    assert repository.updated_registrations[-1]["schema_descriptor"]["ema"]["periods"] == [36, 43, 144, 169, 576, 676]


def test_fill_raw_candle_dataset_pages_back_from_latest_until_gap_is_covered(tmp_path: Path):
    class PagedOKXAdapter:
        def __init__(self) -> None:
            self.calls = []

        def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None):
            self.calls.append({"inst_id": inst_id, "bar": bar, "limit": limit, "after": after})
            if after is None:
                return {
                    "code": "0",
                    "data": [
                        ["1780273200000", "104", "108", "103", "107", "6.0", "0.6", "642", "1"],
                        ["1780272900000", "101", "106", "100", "104", "8.75", "0.875", "910", "1"],
                    ],
                }
            return {
                "code": "0",
                "data": [
                    ["1780272600000", "100", "105", "99", "101", "12.5", "1.25", "1262.5", "1"],
                    ["1780272300000", "99", "101", "98", "100", "3.0", "0.3", "300", "1"],
                ],
            }

    storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    _write_parquet(month_path, [_row("2026-06-01T00:00:00Z", 98, 100, 97, 99, 1)])

    result = fill_raw_candle_dataset(
        registration={
            "dataset_id": "btc-raw-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        repository=FakeRepository(),
        adapter=PagedOKXAdapter(),
        as_of=datetime(2026, 6, 1, 0, 20, tzinfo=UTC),
        limit=2,
    )

    assert result["status"] == "filled"
    assert result["rows_added"] == 4
    assert result["end_ts"] == "2026-06-01T00:20:00Z"


def test_fill_raw_candle_dataset_ignores_unconfirmed_source_candles(tmp_path: Path):
    class FormingOKXAdapter:
        def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None):
            return {
                "code": "0",
                "data": [
                    ["1780272300000", "101", "106", "100", "104", "8.75", "0.875", "910", "0"],
                ],
            }

    storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    _write_parquet(month_path, [_row("2026-06-01T00:00:00Z", 100, 105, 99, 101, 12.5)])
    repository = FakeRepository()

    result = fill_raw_candle_dataset(
        registration={
            "dataset_id": "btc-raw-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        repository=repository,
        adapter=FormingOKXAdapter(),
        as_of=datetime(2026, 6, 1, 0, 5, tzinfo=UTC),
    )

    assert result["status"] == "no_new_rows"
    assert result["end_ts"] == "2026-06-01T00:00:00Z"
    assert repository.updated_registration is None
    assert [row["timestamp"] for row in read_candles(month_path)] == ["2026-06-01T00:00:00Z"]


def test_fill_raw_candle_dataset_appends_confirmed_rows_only_from_mixed_payload(tmp_path: Path):
    class MixedOKXAdapter:
        def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None):
            return {
                "code": "0",
                "data": [
                    ["1780272600000", "104", "108", "103", "107", "6.0", "0.6", "642", "0"],
                    ["1780272300000", "101", "106", "100", "104", "8.75", "0.875", "910", "1"],
                ],
            }

    storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    _write_parquet(month_path, [_row("2026-06-01T00:00:00Z", 100, 105, 99, 101, 12.5)])
    repository = FakeRepository()

    result = fill_raw_candle_dataset(
        registration={
            "dataset_id": "btc-raw-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 1, 0, 0, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        repository=repository,
        adapter=MixedOKXAdapter(),
        as_of=datetime(2026, 6, 1, 0, 10, tzinfo=UTC),
    )

    assert result["status"] == "filled"
    assert result["rows_added"] == 1
    assert result["end_ts"] == "2026-06-01T00:05:00Z"
    assert repository.updated_registration["end_ts"] == "2026-06-01T00:05:00Z"
    assert [row["timestamp"] for row in read_candles(month_path)] == [
        "2026-06-01T00:00:00Z",
        "2026-06-01T00:05:00Z",
    ]


def test_fill_raw_candle_dataset_reports_no_new_rows_when_source_returns_gap(tmp_path: Path):
    storage_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    month_path = storage_uri / "year=2026/month=06/data.parquet"
    month_path.parent.mkdir(parents=True)
    _write_parquet(month_path, [_row("2026-06-02T23:55:00Z", 100, 105, 99, 101, 12.5)])

    result = fill_raw_candle_dataset(
        registration={
            "dataset_id": "btc-raw-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 6, 2, 23, 55, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 2, 23, 55, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        repository=FakeRepository(),
        adapter=EmptyOKXAdapter(),
        as_of=datetime(2026, 6, 3, 1, 0, tzinfo=UTC),
    )

    assert result == {
        "dataset_id": "btc-raw-5m",
        "status": "no_new_rows",
        "rows_added": 0,
        "start_ts": "2026-06-02T23:55:00Z",
        "end_ts": "2026-06-02T23:55:00Z",
        "row_count": 1,
        "from_ts": "2026-06-03T00:00:00Z",
        "to_ts": "2026-06-03T01:00:00Z",
        "source": "okx_cli",
        "reason": "source_returned_no_new_rows",
    }


def test_fill_raw_candle_dataset_blocks_non_raw_candle_datasets(tmp_path: Path):
    result = fill_raw_candle_dataset(
        registration={
            "dataset_id": "btc-derived-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "derived",
            "end_ts": datetime(2026, 6, 1, tzinfo=UTC),
            "row_count": 1,
            "storage_backend": "parquet",
            "storage_uri": str(tmp_path),
            "schema_descriptor": {},
            "quality_status": "ingested",
            "ingestion_version": "legacy",
        },
        repository=FakeRepository(),
        adapter=FakeOKXAdapter(),
        as_of=datetime(2026, 6, 1, 0, 10, tzinfo=UTC),
    )

    assert result == {
        "dataset_id": "btc-derived-5m",
        "status": "blocked",
        "reason": "refresh_supported_for_raw_candles_only",
    }


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    pq.write_table(pa.Table.from_pylist(rows), path)


def _row(timestamp: str, open_: float, high: float, low: float, close: float, volume: float):
    return {
        "timestamp": timestamp,
        "open": float(open_),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "volume": float(volume),
    }
