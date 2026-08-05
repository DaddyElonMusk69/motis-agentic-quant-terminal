from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.feature_enrichment import (
    FEATURE_FAMILIES,
    TIMEFRAMES,
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


def test_default_feature_timeframes_cover_v6_context() -> None:
    assert TIMEFRAMES == ("5m", "2h", "8h", "12h", "1d")


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


def test_build_feature_rows_records_causal_availability_and_source_window() -> None:
    rows = [
        _row(f"2026-06-{1 + index // 12:02d}T{(index % 12) * 2:02d}:00:00Z", close=100 + index, volume=10 + index)
        for index in range(50)
    ]

    feature_rows = build_feature_rows(rows, family="volume", timeframe="2h")

    assert feature_rows[-1]["available_at"] == "2026-06-05T04:00:00Z"
    assert feature_rows[-1]["complete"] is True
    assert feature_rows[-1]["source_row_count"] == 48
    assert feature_rows[-1]["source_window_start_ts"] == rows[-48]["timestamp"]
    assert feature_rows[-1]["source_window_end_ts"] == rows[-1]["timestamp"]


def test_build_feature_rows_computes_saty_daily_atr_levels_causally() -> None:
    rows = [
        _row(f"2026-06-{day:02d}T00:00:00Z", close=100 + day, volume=10 + day)
        for day in range(1, 17)
    ]

    feature_rows = build_feature_rows(rows, family="saty_atr_levels", timeframe="5m")
    latest = feature_rows[-1]
    previous = rows[-2]

    assert latest["anchor_timeframe"] == "1d"
    assert latest["previous_period_close"] == previous["close"]
    assert latest["atr_14"] is not None
    assert latest["upper_trigger"] == latest["previous_period_close"] + latest["atr_14"] * 0.236
    assert latest["lower_1000"] == latest["previous_period_close"] - latest["atr_14"]
    assert latest["upper_1000"] == latest["previous_period_close"] + latest["atr_14"]
    assert latest["current_range_pct_of_atr"] is not None
    assert latest["ribbon_state"] in {"bullish", "bearish", "mixed"}
    assert latest["available_at"] == "2026-06-16T00:05:00Z"
    assert latest["source_window_end_ts"] == rows[-1]["timestamp"]


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
    assert "available_at" in repository.upserted[0]["schema_descriptor"]["columns"]
    feature_path = Path(repository.upserted[0]["storage_uri"]) / "year=2026/month=06/data.parquet"
    written_rows = read_candles(feature_path)
    assert written_rows[-1]["bb_mid_20"] is not None
    assert written_rows[-1]["available_at"] == "2026-06-01T00:29:00Z"


def test_enrich_saty_atr_levels_uses_only_5m_source_candles(tmp_path: Path):
    repository = FakeRepository()
    for timeframe in ("5m", "2h"):
        source_storage = tmp_path / f"origin=derived/source=okx/type=candles/asset=BTC/timeframe={timeframe}"
        path = source_storage / "year=2026/month=06/data.parquet"
        path.parent.mkdir(parents=True)
        rows = [
            _row(f"2026-06-{day:02d}T00:00:00Z", close=100 + day, volume=10 + day)
            for day in range(1, 17)
        ]
        _write_parquet(path, rows)
        repository.refs.append(
            {
                "dataset_id": f"btc-derived-{timeframe}",
                "source_id": "okx",
                "asset": "BTC",
                "instrument": "BTC-USDT-SWAP",
                "data_type": "candles",
                "timeframe": timeframe,
                "data_origin": "derived",
                "row_count": len(rows),
                "storage_backend": "parquet",
                "storage_uri": str(source_storage),
                "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
                "quality_status": "ema_enriched",
                "ingestion_version": "test",
            }
        )

    result = enrich_feature_family_datasets(
        repository=repository,
        asset="BTC",
        family="saty_atr_levels",
        start_date="2025-01-01",
        target_root=tmp_path / "features",
    )

    assert result["status"] == "enriched"
    assert result["feature_count"] == 1
    assert repository.upserted[0]["dataset_id"] == "BTC-feature_saty_atr_levels-5m"
    assert repository.upserted[0]["schema_descriptor"]["feature_family"] == "saty_atr_levels"
    assert repository.upserted[0]["timeframe"] == "5m"


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
