from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_worker.ingestion.atr_enrichment import (
    build_atr_rows,
    enrich_atr_dataset,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted = []

    def upsert_ref(self, registration):
        self.upserted.append(registration)


def test_build_atr_rows_uses_complete_closed_buckets_and_wilder_average():
    raw_rows = []
    cursor = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    for index in range(36):
        # Three complete 1h candles from 5m source rows.
        hour = index // 12
        raw_rows.append(
            {
                "timestamp": (cursor + timedelta(minutes=5 * index)).isoformat().replace("+00:00", "Z"),
                "symbol": "BTCUSDT",
                "open": 100 + hour,
                "high": 110 + hour,
                "low": 90 + hour,
                "close": 100 + hour,
                "volume": 1,
                "confirm": 1,
            }
        )
    raw_rows.append(
        {
            "timestamp": "2026-06-01T03:00:00Z",
            "symbol": "BTCUSDT",
            "open": 103,
            "high": 113,
            "low": 93,
            "close": 103,
            "volume": 1,
            "confirm": 1,
        }
    )

    rows = build_atr_rows(raw_rows=raw_rows, timeframe="1h", period=3)

    assert [row["timestamp"] for row in rows] == [
        "2026-06-01T00:00:00Z",
        "2026-06-01T01:00:00Z",
        "2026-06-01T02:00:00Z",
    ]
    assert rows[0]["atr"] is None
    assert rows[0]["warmup_complete"] is False
    assert rows[2]["true_range"] == pytest.approx(20.0)
    assert rows[2]["atr"] == pytest.approx(20.0)
    assert rows[2]["atr_pct"] == pytest.approx(20.0 / 102.0 * 100.0)
    assert rows[2]["available_at"] == "2026-06-01T03:00:00Z"


def test_enrich_atr_dataset_writes_and_registers_standalone_ref(tmp_path: Path):
    source_uri = tmp_path / "origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    source_path = source_uri / "year=2026/month=06/data.parquet"
    source_path.parent.mkdir(parents=True)
    rows = []
    cursor = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    for index in range(36):
        rows.append(
            {
                "timestamp": (cursor + timedelta(minutes=5 * index)).isoformat().replace("+00:00", "Z"),
                "symbol": "BTC-USDT-SWAP",
                "open": 100 + index / 12,
                "high": 110 + index / 12,
                "low": 90 + index / 12,
                "close": 100 + index / 12,
                "volume": 1,
                "confirm": 1,
            }
        )
    pq.write_table(pa.Table.from_pylist(rows), source_path)
    repository = FakeRepository()

    result = enrich_atr_dataset(
        source_registration={
            "dataset_id": "btc-okx-candles-raw-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_uri": str(source_uri),
        },
        repository=repository,
        timeframe="1h",
        period=3,
        source_timeframe="5m",
        start_date="2026-06-01",
        as_of=datetime(2026, 6, 1, 3, 0, tzinfo=UTC),
        target_root=tmp_path / ".data" / "market-data",
    )

    assert result["dataset_id"] == "btc-okx-technical_indicator_atr-derived-1h-wilder-3"
    assert result["row_count"] == 3
    registration = repository.upserted[0]
    assert registration["data_type"] == "technical_indicator_atr"
    assert registration["quality_status"] == "atr_enriched"
    assert registration["schema_descriptor"]["availability_semantics"] == "interval_end"
    assert registration["schema_descriptor"]["period"] == 3
    path = (
        tmp_path
        / ".data/market-data/origin=derived/source=okx/type=technical_indicator_atr/asset=BTC/timeframe=1h/year=2026/month=06/data.parquet"
    )
    written = pq.ParquetFile(path).read().to_pylist()
    assert written[-1]["timestamp"] == "2026-06-01T02:00:00Z"
    assert written[-1]["warmup_complete"] is True
