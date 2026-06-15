from __future__ import annotations

from datetime import UTC
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_sdk.market_data_reader import MarketDataReader


class FakeRepository:
    def __init__(self, ref):
        self.ref = ref
        self.calls = []

    def get_candle_ref(self, *, asset: str, timeframe: str, origin: str, data_type: str = "candles"):
        self.calls.append(
            {"asset": asset, "timeframe": timeframe, "origin": origin, "data_type": data_type}
        )
        return self.ref


def test_market_data_reader_reads_partitioned_parquet_sorted_deduped_and_utc(tmp_path: Path):
    storage_uri = tmp_path / ".data" / "market-data" / "origin=raw" / "source=okx" / "type=candles" / "asset=BTC" / "timeframe=5m"
    month_path = storage_uri / "year=2026" / "month=06" / "data.parquet"
    month_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                _row("2026-06-01T00:10:00Z", close=110, confirm=1),
                _row("2026-06-01T00:05:00Z", close=105, confirm=1),
                _row("2026-06-01T00:05:00Z", close=106, confirm=1),
                _row("2026-06-01T00:15:00Z", close=115, confirm=0),
            ]
        ),
        month_path,
    )
    reader = MarketDataReader(
        repository=FakeRepository(
            {
                "dataset_id": "btc-raw-5m",
                "storage_backend": "parquet",
                "storage_uri": str(storage_uri),
            }
        ),
        workspace_root=tmp_path,
    )

    candles = reader.get_candles(
        asset="btc",
        timeframe="5m",
        origin="raw",
        start="2026-06-01T00:04:00Z",
        end="2026-06-01T00:15:00Z",
    )

    assert [candle.timestamp.isoformat() for candle in candles] == [
        "2026-06-01T00:05:00+00:00",
        "2026-06-01T00:10:00+00:00",
    ]
    assert candles[0].timestamp.tzinfo is UTC
    assert str(candles[0].close) == "106"
    assert str(candles[1].close) == "110"


def test_market_data_reader_preserves_extra_parquet_columns_for_engine_rows(tmp_path: Path):
    storage_uri = tmp_path / ".data" / "market-data" / "origin=derived" / "source=okx" / "type=candles" / "asset=BTC" / "timeframe=2h"
    month_path = storage_uri / "year=2026" / "month=06" / "data.parquet"
    month_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {**_row("2026-06-01T00:00:00Z", close=100, confirm=1), "ema_676": "99.5"},
                {**_row("2026-06-01T02:00:00Z", close=102, confirm=0), "ema_676": "100.0"},
            ]
        ),
        month_path,
    )
    reader = MarketDataReader(
        repository=FakeRepository(
            {
                "dataset_id": "btc-derived-2h",
                "storage_backend": "parquet",
                "storage_uri": str(storage_uri),
            }
        ),
        workspace_root=tmp_path,
    )

    rows = reader.get_rows(asset="btc", timeframe="2h", origin="derived")

    assert len(rows) == 1
    assert rows[0]["timestamp"].tzinfo is UTC
    assert rows[0]["ema_676"] == "99.5"


def test_market_data_reader_reports_corrupt_parquet_shard(tmp_path: Path):
    storage_uri = tmp_path / ".data" / "market-data" / "origin=raw" / "source=okx" / "type=candles" / "asset=BNB" / "timeframe=5m"
    month_path = storage_uri / "year=2026" / "month=04" / "data.parquet"
    month_path.parent.mkdir(parents=True)
    month_path.write_bytes(b"bad!")
    reader = MarketDataReader(
        repository=FakeRepository(
            {
                "dataset_id": "bnb-raw-5m",
                "storage_backend": "parquet",
                "storage_uri": str(storage_uri),
            }
        ),
        workspace_root=tmp_path,
    )

    with pytest.raises(ValueError, match="Corrupt parquet shard for bnb-raw-5m"):
        reader.get_candles(asset="bnb", timeframe="5m", origin="raw")


def _row(timestamp: str, *, close: int, confirm: int) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 1,
        "vol_ccy": 1,
        "vol_ccy_quote": 1,
        "confirm": confirm,
    }
