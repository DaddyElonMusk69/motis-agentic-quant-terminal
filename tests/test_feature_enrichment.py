from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.feature_enrichment import (
    FEATURE_FAMILIES,
    build_feature_rows,
    enrich_feature_family_datasets,
)


class FakeRepository:
    def __init__(self) -> None:
        self.refs = []
        self.upserted = []

    def list_refs(self):
        return self.refs

    def upsert_ref(self, registration):
        self.upserted.append(registration)


def test_build_feature_rows_computes_base_candle_and_bollinger_features():
    rows = [_row(f"2026-06-01T00:{minute:02d}:00Z", close=100 + minute, volume=10 + minute) for minute in range(25)]

    base_rows = build_feature_rows(rows, family="base_candle")
    bollinger_rows = build_feature_rows(rows, family="bollinger")

    assert base_rows[-1]["return_pct"] > 0
    assert base_rows[-1]["body_pct"] > 0
    assert "close_location_pct" in base_rows[-1]
    assert bollinger_rows[-1]["bb_mid_20"] is not None
    assert bollinger_rows[-1]["bb_upper_20_2"] is not None
    assert bollinger_rows[-1]["bb_position_pct"] is not None
    assert bollinger_rows[-1]["bb_bandwidth_pct"] is not None


def test_enrich_feature_family_datasets_writes_feature_refs_for_timeframes(tmp_path: Path):
    source_storage = tmp_path / "origin=derived/source=okx/type=candles/asset=BTC/timeframe=5m"
    path = source_storage / "year=2026/month=06/data.parquet"
    path.parent.mkdir(parents=True)
    _write_parquet(path, [_row(f"2026-06-01T00:{minute:02d}:00Z", close=100 + minute, volume=10 + minute) for minute in range(25)])
    repository = FakeRepository()
    repository.refs = [
        {
            "dataset_id": "btc-derived-5m",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "derived",
            "row_count": 25,
            "storage_backend": "parquet",
            "storage_uri": str(source_storage),
            "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
            "quality_status": "ema_enriched",
            "ingestion_version": "test",
        }
    ]

    result = enrich_feature_family_datasets(
        repository=repository,
        asset="BTC",
        family="bollinger",
        timeframes=("5m",),
        start_date="2025-01-01",
        target_root=tmp_path / "features",
    )

    assert result["status"] == "enriched"
    assert result["family"] == "bollinger"
    assert result["feature_count"] == 1
    assert repository.upserted[0]["data_type"] == FEATURE_FAMILIES["bollinger"].data_type
    assert repository.upserted[0]["schema_descriptor"]["feature_family"] == "bollinger"
    feature_path = Path(repository.upserted[0]["storage_uri"]) / "year=2026/month=06/data.parquet"
    written_rows = read_candles(feature_path)
    assert written_rows[-1]["bb_mid_20"] is not None


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    pq.write_table(pa.Table.from_pylist(rows), path)


def _row(timestamp: str, *, close: float, volume: float = 1.0) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": volume,
        "vol_ccy_quote": volume * close,
        "confirm": 1,
    }
