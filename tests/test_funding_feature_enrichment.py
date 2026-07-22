from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_worker.ingestion.funding_feature_enrichment import (
    build_funding_feature_rows,
    enrich_funding_feature_dataset,
    rebuild_registered_funding_features,
)


class FakeRepository:
    def __init__(self) -> None:
        self.refs = []
        self.upserted = []
        self.updated = []

    def list_refs(self):
        return self.refs

    def upsert_ref(self, registration):
        self.upserted.append(registration)
        self.refs = [
            row
            for row in self.refs
            if row.get("dataset_id") != registration["dataset_id"]
        ] + [registration]

    def update_ref(self, registration):
        self.updated.append(registration)
        self.refs = [
            row
            for row in self.refs
            if row.get("dataset_id") != registration["dataset_id"]
        ] + [registration]


def test_build_funding_features_uses_only_latest_settled_event():
    rows = build_funding_feature_rows(
        raw_rows=[
            _funding_row("2026-06-01T00:00:00Z", 0.0001),
            _funding_row("2026-06-01T08:00:00Z", 0.0002),
            _funding_row("2026-06-01T16:00:00Z", -0.0001),
        ],
        start=datetime(2026, 6, 1, 15, 55, tzinfo=UTC),
        end=datetime(2026, 6, 1, 16, 5, tzinfo=UTC),
    )

    assert [row["timestamp"] for row in rows] == [
        "2026-06-01T15:55:00Z",
        "2026-06-01T16:00:00Z",
        "2026-06-01T16:05:00Z",
    ]
    assert rows[0]["source_event_timestamp"] == "2026-06-01T08:00:00Z"
    assert rows[0]["latest_funding_rate"] == 0.0002
    assert rows[1]["source_event_timestamp"] == "2026-06-01T16:00:00Z"
    assert rows[1]["latest_funding_rate"] == -0.0001
    assert rows[1]["funding_rate_change"] == pytest.approx(-0.0003)
    assert rows[1]["funding_carry_1d"] == pytest.approx(0.0002)
    assert rows[1]["funding_event_count_1d"] == 3
    assert rows[1]["funding_signed_streak"] == -1
    assert rows[1]["funding_event_age_minutes"] == 0
    assert rows[1]["minutes_to_expected_funding"] == 480
    assert rows[1]["funding_event_is_new"] is True
    assert rows[2]["funding_event_age_minutes"] == 5
    assert rows[2]["minutes_to_expected_funding"] == 475
    assert rows[2]["funding_event_is_new"] is False
    assert rows[2]["available_at"] == "2026-06-01T16:10:00Z"


def test_funding_signed_streak_is_directional():
    rows = build_funding_feature_rows(
        raw_rows=[
            _funding_row("2026-06-01T00:00:00Z", -0.0001),
            _funding_row("2026-06-01T08:00:00Z", -0.0002),
            _funding_row("2026-06-01T16:00:00Z", -0.0003),
        ],
        start=datetime(2026, 6, 1, 16, 0, tzinfo=UTC),
        end=datetime(2026, 6, 1, 16, 0, tzinfo=UTC),
    )

    assert rows[0]["funding_signed_streak"] == -3
    assert rows[0]["annualized_funding_rate"] == pytest.approx(-0.3285)


def test_enrich_funding_features_writes_and_registers_dataset(tmp_path: Path):
    source_uri = tmp_path / "origin=raw/source=binance/type=funding/asset=BTC/timeframe=8h"
    source_path = source_uri / "year=2026/month=06/data.parquet"
    source_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                _funding_row("2026-06-01T00:00:00Z", 0.0001),
                _funding_row("2026-06-01T08:00:00Z", 0.0002),
            ]
        ),
        source_path,
    )
    source_registration = _source_registration(source_uri)
    repository = FakeRepository()

    result = enrich_funding_feature_dataset(
        source_registration=source_registration,
        repository=repository,
        start_date="2026-06-01",
        as_of=datetime(2026, 6, 1, 8, 11, tzinfo=UTC),
        target_root=tmp_path / ".data" / "market-data",
    )

    assert result["dataset_id"] == "btc-binance-funding_features-derived-5m"
    assert result["row_count"] == 98
    registration = repository.upserted[0]
    assert registration["data_type"] == "funding_features"
    assert registration["schema_descriptor"]["availability_semantics"] == "interval_end"
    path = (
        tmp_path
        / ".data/market-data/origin=derived/source=binance/type=funding_features/asset=BTC/timeframe=5m/year=2026/month=06/data.parquet"
    )
    rows = pq.read_table(path).to_pylist()
    assert rows[-1]["timestamp"] == "2026-06-01T08:05:00Z"
    assert rows[-1]["source_event_timestamp"] == "2026-06-01T08:00:00Z"


def test_rebuild_extends_registered_funding_features(tmp_path: Path):
    source_uri = tmp_path / "origin=raw/source=binance/type=funding/asset=BTC/timeframe=8h"
    target_uri = tmp_path / "origin=derived/source=binance/type=funding_features/asset=BTC/timeframe=5m"
    source_registration = _source_registration(source_uri)
    repository = FakeRepository()
    repository.refs = [
        {
            "dataset_id": "btc-binance-funding_features-derived-5m",
            "source_id": "binance",
            "asset": "BTC",
            "instrument": "BTCUSDT",
            "data_type": "funding_features",
            "timeframe": "5m",
            "data_origin": "derived",
            "start_ts": "2026-06-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "row_count": 1,
            "storage_uri": str(target_uri),
            "schema_descriptor": {
                "derived_from_dataset_id": source_registration["dataset_id"]
            },
        }
    ]

    result = rebuild_registered_funding_features(
        repository=repository,
        source_registration=source_registration,
        source_rows=[_funding_row("2026-06-01T00:00:00Z", 0.0001)],
        as_of=datetime(2026, 6, 1, 0, 16, tzinfo=UTC),
    )

    assert result[0]["row_count"] == 3
    assert result[0]["end_ts"] == "2026-06-01T00:10:00Z"
    assert repository.updated[0]["quality_status"] == "feature_enriched"


def _funding_row(timestamp: str, rate: float) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "symbol": "BTCUSDT",
        "funding_rate": rate,
        "funding_interval_hours": 8,
        "confirm": 1,
    }


def _source_registration(storage_uri: Path) -> dict[str, object]:
    return {
        "dataset_id": "btc-binance-funding-raw-8h",
        "source_id": "binance",
        "asset": "BTC",
        "instrument": "BTCUSDT",
        "data_type": "funding",
        "timeframe": "8h",
        "data_origin": "raw",
        "start_ts": "2026-06-01T00:00:00Z",
        "end_ts": "2026-06-01T08:00:00Z",
        "row_count": 2,
        "storage_uri": str(storage_uri),
        "ingestion_version": "binance-funding-v1",
    }
