from pathlib import Path

import pytest

from quant_terminal_sdk.market_data import MarketDataReference
from quant_terminal_sdk.parquet_store import read_candles, write_candles


def test_write_and_read_candles_from_partitioned_parquet(tmp_path: Path):
    reference = MarketDataReference(
        dataset_id="okx-btc-5m",
        source_id="okx",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        data_type="candles",
        timeframe="5m",
        storage_backend="parquet",
    )
    rows = [
        {
            "timestamp": "2026-06-01T00:00:00Z",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 12.5,
        }
    ]

    path = write_candles(root=tmp_path, reference=reference, year=2026, month=6, rows=rows)

    assert path.exists()
    assert read_candles(path) == rows


def test_write_candles_preserves_existing_shard_when_temp_write_fails(tmp_path: Path, monkeypatch):
    reference = MarketDataReference(
        dataset_id="okx-btc-5m",
        source_id="okx",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        data_type="candles",
        timeframe="5m",
        storage_backend="parquet",
    )
    original_rows = [
        {
            "timestamp": "2026-06-01T00:00:00Z",
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.0,
            "volume": 12.5,
        }
    ]
    path = write_candles(root=tmp_path, reference=reference, year=2026, month=6, rows=original_rows)

    import quant_terminal_sdk.parquet_store as parquet_store

    def fail_write(table, where, *args, **kwargs):
        Path(where).write_bytes(b"bad!")
        raise OSError("simulated interrupted parquet write")

    monkeypatch.setattr(parquet_store.pq, "write_table", fail_write)

    with pytest.raises(OSError, match="simulated interrupted parquet write"):
        write_candles(
            root=tmp_path,
            reference=reference,
            year=2026,
            month=6,
            rows=[
                {
                    "timestamp": "2026-06-01T00:05:00Z",
                    "open": 101.0,
                    "high": 103.0,
                    "low": 100.0,
                    "close": 102.0,
                    "volume": 9.0,
                }
            ],
        )

    assert read_candles(path) == original_rows
    assert list(path.parent.glob("*.tmp")) == []
