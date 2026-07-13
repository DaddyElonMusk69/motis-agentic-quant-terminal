from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_sdk.parquet_store import read_candles
from quant_terminal_worker.ingestion.open_interest_feature_enrichment import (
    OI_FEATURE_COLUMNS,
    build_open_interest_regime_rows,
    enrich_open_interest_regime_dataset,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted: list[dict[str, object]] = []

    def upsert_ref(self, registration: dict[str, object]) -> None:
        self.upserted.append(registration)


def test_build_open_interest_regime_rows_computes_minimum_causal_features() -> None:
    rows = [_oi_aggregate_row(index) for index in range(681)]

    features = build_open_interest_regime_rows(rows, timeframe="15m")
    latest = features[-1]

    assert latest["oi_return_pct_2h"] == pytest.approx((1680 / 1672 - 1) * 100)
    assert latest["oi_return_pct_8h"] == pytest.approx((1680 / 1648 - 1) * 100)
    assert latest["oi_return_pct_24h"] == pytest.approx((1680 / 1584 - 1) * 100)
    assert latest["general_long_short_ratio"] == pytest.approx(1.68)
    assert latest["taker_long_short_ratio_avg_2h"] == pytest.approx(
        sum(0.993 + index / 1000 for index in range(673, 681)) / 8
    )
    assert latest["oi_change_2h_zscore_7d"] is not None
    assert latest["available_at"] == "2026-01-08T02:15:00Z"
    assert latest["complete"] is True
    assert latest["source_window_start_ts"] == rows[0]["timestamp"]
    assert latest["source_window_end_ts"] == rows[-1]["timestamp"]
    assert latest["source_row_count"] == 681


def test_oi_zscore_baseline_excludes_current_observation() -> None:
    rows = [_oi_aggregate_row(index) for index in range(681)]
    rows[-1]["sum_open_interest_last"] = 5000.0

    features = build_open_interest_regime_rows(rows, timeframe="15m")
    latest = features[-1]
    prior = [row["oi_return_pct_2h"] for row in features[8:-1]]
    prior_values = [float(value) for value in prior if value is not None][-672:]
    expected_mean = sum(prior_values) / len(prior_values)
    expected_variance = sum((value - expected_mean) ** 2 for value in prior_values) / len(prior_values)
    expected = (float(latest["oi_return_pct_2h"]) - expected_mean) / expected_variance**0.5

    assert len(prior_values) == 672
    assert latest["oi_change_2h_zscore_7d"] == pytest.approx(expected)


def test_enrich_open_interest_regime_dataset_writes_registered_parquet(tmp_path: Path) -> None:
    source_storage = tmp_path / "origin=derived/source=binance/type=open_interest/asset=BTC/timeframe=15m"
    path = source_storage / "year=2026/month=01/data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([_oi_aggregate_row(index) for index in range(681)]), path)
    source_registration = {
        "dataset_id": "btc-binance-open_interest-derived-15m",
        "source_id": "binance",
        "asset": "BTC",
        "instrument": "BTCUSDT",
        "data_type": "open_interest",
        "timeframe": "15m",
        "data_origin": "derived",
        "storage_backend": "parquet",
        "storage_uri": str(source_storage),
        "schema_descriptor": {},
        "quality_status": "derived",
        "ingestion_version": "binance-metrics-v1",
    }
    repository = FakeRepository()

    result = enrich_open_interest_regime_dataset(
        source_registration=source_registration,
        repository=repository,
        start_date="2026-01-01",
        target_root=tmp_path / "features",
    )

    assert result["status"] == "enriched"
    registration = repository.upserted[0]
    assert registration["dataset_id"] == "BTC-feature_open_interest_regime-15m"
    assert registration["data_type"] == "feature_open_interest_regime"
    assert registration["timeframe"] == "15m"
    columns = registration["schema_descriptor"]["columns"]
    assert columns == [
        "timestamp",
        *OI_FEATURE_COLUMNS,
        "available_at",
        "complete",
        "source_window_start_ts",
        "source_window_end_ts",
        "source_row_count",
    ]
    feature_path = Path(str(registration["storage_uri"])) / "year=2026/month=01/data.parquet"
    written = read_candles(feature_path)
    assert written[-1]["available_at"] == "2026-01-08T02:15:00Z"


def _oi_aggregate_row(index: int) -> dict[str, object]:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * index)
    oi = 1000.0 + index
    return {
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "symbol": "BTCUSDT",
        "sum_open_interest_last": oi,
        "count_long_short_ratio_last": 1.0 + index / 1000,
        "sum_taker_long_short_vol_ratio_avg": 0.993 + index / 1000,
        "confirm": 1,
    }
