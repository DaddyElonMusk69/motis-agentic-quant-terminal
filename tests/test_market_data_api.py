from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient
import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_api import main as api_main
from quant_terminal_api.main import create_app


class FakeMarketDataRepository:
    def __init__(self):
        self.updated_registration = None
        self.upserted_registration = None

    def list_refs(self):
        return [
            {
                "dataset_id": "btc-raw-5m",
                "asset": "BTC",
                "instrument": "BTC-USDT-SWAP",
                "data_type": "candles",
                "timeframe": "5m",
                "data_origin": "raw",
                "start_ts": datetime(2026, 5, 1, tzinfo=UTC),
                "end_ts": datetime(2026, 5, 31, tzinfo=UTC),
                "row_count": 100,
                "storage_backend": "parquet",
                "storage_uri": ".data/market-data",
                "schema_descriptor": {"columns": ["timestamp", "open", "high", "low", "close", "volume"]},
                "quality_status": "ingested",
                "ingestion_version": "legacy",
            }
        ]

    def get_ref(self, dataset_id: str):
        if dataset_id == "btc-raw-5m":
            return self.list_refs()[0]
        if dataset_id == "btc-derived-5m":
            return {**self.list_refs()[0], "dataset_id": "btc-derived-5m", "data_origin": "derived"}
        if dataset_id == "sol-binance-open_interest-raw-5m":
            return {
                "dataset_id": "sol-binance-open_interest-raw-5m",
                "asset": "SOL",
                "instrument": "SOLUSDT",
                "data_type": "open_interest",
                "timeframe": "5m",
                "data_origin": "raw",
                "start_ts": datetime(2026, 5, 1, tzinfo=UTC),
                "end_ts": datetime(2026, 5, 31, tzinfo=UTC),
                "row_count": 100,
                "storage_backend": "parquet",
                "storage_uri": ".data/market-data",
                "schema_descriptor": {"columns": ["timestamp", "symbol", "sum_open_interest"]},
                "quality_status": "ingested",
                "ingestion_version": "binance-metrics-v1",
            }
        if dataset_id == "sol-binance-open_interest-derived-15m":
            return {
                **self.get_ref("sol-binance-open_interest-raw-5m"),
                "dataset_id": "sol-binance-open_interest-derived-15m",
                "timeframe": "15m",
                "data_origin": "derived",
            }
        if dataset_id == "btc-binance-funding-raw-8h":
            return {
                "dataset_id": "btc-binance-funding-raw-8h",
                "asset": "BTC",
                "instrument": "BTCUSDT",
                "data_type": "funding",
                "timeframe": "8h",
                "data_origin": "raw",
                "start_ts": datetime(2026, 5, 1, tzinfo=UTC),
                "end_ts": datetime(2026, 6, 30, 16, tzinfo=UTC),
                "row_count": 100,
                "storage_backend": "parquet",
                "storage_uri": ".data/market-data",
                "schema_descriptor": {"columns": ["timestamp", "symbol", "funding_rate", "funding_interval_hours"]},
                "quality_status": "ingested",
                "ingestion_version": "binance-funding-v1",
            }
        return None

    def update_ref(self, registration):
        self.updated_registration = registration

    def upsert_ref(self, registration):
        self.upserted_registration = registration


def fake_fill_service(*, registration, repository, adapter):
    assert registration["dataset_id"] == "btc-raw-5m"
    repository.update_ref({**registration, "row_count": 101, "quality_status": "updated"})
    return {
        "dataset_id": "btc-raw-5m",
        "status": "filled",
        "rows_added": 1,
        "row_count": 101,
        "end_ts": "2026-06-01T00:05:00Z",
    }


def test_market_data_catalog_endpoint_uses_repository():
    client = TestClient(create_app(market_data_repository=FakeMarketDataRepository()))

    response = client.get("/api/v1/market-data/catalog")

    assert response.status_code == 200
    assert response.json()["summary"] == {"assets": 1, "datasets": 1, "data_types": ["candles"]}
    assert response.json()["assets"][0]["datasets"][0]["schema_descriptor"]["columns"] == ["timestamp", "open", "high", "low", "close", "volume"]


def test_market_data_ema_refresh_endpoint_queues_worker_job():
    class FakeRuntimeRepository:
        def enqueue_job(self, *, job_type, scope_key, payload, current_step):
            return {
                "job_id": "job-ema-btc",
                "job_type": job_type,
                "scope_key": scope_key,
                "status": "queued",
                "payload": payload,
                "result": {},
                "error": {},
                "current_step": current_step,
            }

    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            runtime_repository=FakeRuntimeRepository(),
        )
    )

    response = client.post("/api/v1/market-data/assets/btc/ema/refresh")

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert response.json()["job"]["job_type"] == "market_data_ema_refresh"
    assert response.json()["job"]["payload"] == {"asset": "BTC"}


def test_market_data_atr_refresh_endpoint_queues_worker_job():
    class FakeRuntimeRepository:
        def enqueue_job(self, *, job_type, scope_key, payload, current_step):
            return {
                "job_id": "job-atr-btc",
                "job_type": job_type,
                "scope_key": scope_key,
                "status": "queued",
                "payload": payload,
                "result": {},
                "error": {},
                "current_step": current_step,
            }

    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            runtime_repository=FakeRuntimeRepository(),
        )
    )

    response = client.post("/api/v1/market-data/assets/btc/atr/refresh")

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert response.json()["job"]["job_type"] == "market_data_atr_refresh"
    assert response.json()["job"]["scope_key"] == "asset:BTC:atr"
    assert response.json()["job"]["payload"] == {"asset": "BTC", "timeframes": ["1h", "2h", "4h"], "period": 14}


def test_market_data_feature_refresh_endpoint_queues_worker_job():
    class FakeRuntimeRepository:
        def enqueue_job(self, *, job_type, scope_key, payload, current_step):
            return {
                "job_id": "job-feature-btc-bollinger",
                "job_type": job_type,
                "scope_key": scope_key,
                "status": "queued",
                "payload": payload,
                "result": {},
                "error": {},
                "current_step": current_step,
            }

    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            runtime_repository=FakeRuntimeRepository(),
        )
    )

    response = client.post("/api/v1/market-data/assets/btc/features/bollinger/refresh")

    assert response.status_code == 200
    assert response.json()["accepted"] is True
    assert response.json()["job"]["job_type"] == "market_data_feature_refresh"
    assert response.json()["job"]["scope_key"] == "asset:BTC:feature:bollinger"
    assert response.json()["job"]["payload"] == {"asset": "BTC", "family": "bollinger"}


def test_market_data_rows_endpoint_reads_partitioned_atr_parquet(tmp_path: Path):
    storage_uri = tmp_path / "origin=derived/source=okx/type=technical_indicator_atr/asset=BTC/timeframe=1h"
    data_path = storage_uri / "year=2026/month=08/data.parquet"
    data_path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "timestamp": "2026-08-01T00:00:00Z",
                    "interval_end": "2026-08-01T01:00:00Z",
                    "available_at": "2026-08-01T01:00:00Z",
                    "symbol": "BTCUSDT",
                    "timeframe": "1h",
                    "period": 14,
                    "method": "wilder",
                    "source_timeframe": "5m",
                    "open": 1.0,
                    "high": 2.0,
                    "low": 0.5,
                    "close": 1.5,
                    "volume": 10.0,
                    "true_range": 1.5,
                    "atr": 1.5,
                    "atr_pct": 100.0,
                    "warmup_complete": True,
                    "complete": True,
                    "confirm": 1,
                }
            ]
        ),
        data_path,
    )

    class AtrMarketDataRepository(FakeMarketDataRepository):
        def get_ref(self, dataset_id: str):
            if dataset_id == "btc-atr-1h":
                return {
                    "dataset_id": "btc-atr-1h",
                    "asset": "BTC",
                    "instrument": "BTCUSDT",
                    "data_type": "technical_indicator_atr",
                    "timeframe": "1h",
                    "data_origin": "derived",
                    "start_ts": datetime(2026, 8, 1, tzinfo=UTC),
                    "end_ts": datetime(2026, 8, 1, 1, tzinfo=UTC),
                    "row_count": 1,
                    "storage_backend": "parquet",
                    "storage_uri": str(storage_uri),
                    "schema_descriptor": {"columns": ["timestamp", "interval_end", "available_at", "timeframe", "period"]},
                    "quality_status": "atr_enriched",
                    "ingestion_version": "technical-indicator-atr.v1",
                }
            return super().get_ref(dataset_id)

    client = TestClient(create_app(market_data_repository=AtrMarketDataRepository()))

    response = client.get("/api/v1/market-data/btc-atr-1h/rows?limit=5")

    assert response.status_code == 200
    assert response.json()["rows"][0]["timeframe"] == "1h"
    assert response.json()["rows"][0]["atr"] == 1.5


def test_market_data_open_interest_feature_refresh_uses_specialized_fallback(monkeypatch):
    calls = []

    def fake_enrich_open_interest_regime_datasets(**kwargs):
        calls.append(kwargs)
        return {
            "status": "enriched",
            "asset": kwargs["asset"],
            "family": "open_interest_regime",
            "feature_count": 1,
            "features": [],
        }

    monkeypatch.setattr(api_main, "enrich_open_interest_regime_datasets", fake_enrich_open_interest_regime_datasets)
    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            runtime_repository=object(),
        )
    )

    response = client.post("/api/v1/market-data/assets/btc/features/open_interest_regime/refresh")

    assert response.status_code == 200
    assert response.json()["family"] == "open_interest_regime"
    assert calls[0]["asset"] == "BTC"
    assert "family" not in calls[0]


def test_market_data_refresh_endpoint_fills_dataset():
    repository = FakeMarketDataRepository()
    client = TestClient(
        create_app(
            market_data_repository=repository,
            market_data_fill_service=fake_fill_service,
        )
    )

    response = client.post("/api/v1/market-data/btc-raw-5m/refresh")

    assert response.status_code == 200
    assert response.json()["dataset_id"] == "btc-raw-5m"
    assert response.json()["status"] == "filled"
    assert response.json()["rows_added"] == 1
    assert repository.updated_registration["row_count"] == 101


def test_market_data_refresh_endpoint_blocks_derived_dataset_before_fill_service():
    def failing_fill_service(*, registration, repository, adapter):
        raise AssertionError("fill service should not be called for derived datasets")

    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            market_data_fill_service=failing_fill_service,
        )
    )

    response = client.post("/api/v1/market-data/btc-derived-5m/refresh")

    assert response.status_code == 200
    assert response.json() == {
        "dataset_id": "btc-derived-5m",
        "status": "blocked",
        "reason": "refresh_supported_for_raw_candles_only",
    }


def test_market_data_refresh_endpoint_fills_raw_open_interest_dataset():
    repository = FakeMarketDataRepository()

    def fake_oi_fill_service(*, registration, repository, adapter):
        assert registration["dataset_id"] == "sol-binance-open_interest-raw-5m"
        assert registration["data_type"] == "open_interest"
        repository.update_ref({**registration, "row_count": 101, "quality_status": "updated"})
        return {
            "dataset_id": registration["dataset_id"],
            "status": "filled",
            "rows_added": 1,
            "row_count": 101,
            "end_ts": "2026-06-01T00:05:00Z",
            "source": "binance_cli",
        }

    client = TestClient(
        create_app(
            market_data_repository=repository,
            market_data_fill_service=fake_oi_fill_service,
        )
    )

    response = client.post("/api/v1/market-data/sol-binance-open_interest-raw-5m/refresh")

    assert response.status_code == 200
    assert response.json()["dataset_id"] == "sol-binance-open_interest-raw-5m"
    assert response.json()["status"] == "filled"
    assert repository.updated_registration["row_count"] == 101

def test_market_data_refresh_endpoint_blocks_derived_open_interest_dataset_before_fill_service():
    def failing_fill_service(*, registration, repository, adapter):
        raise AssertionError("fill service should not be called for derived open interest datasets")

    client = TestClient(
        create_app(
            market_data_repository=FakeMarketDataRepository(),
            market_data_fill_service=failing_fill_service,
        )
    )

    response = client.post("/api/v1/market-data/sol-binance-open_interest-derived-15m/refresh")

    assert response.status_code == 200
    assert response.json() == {
        "dataset_id": "sol-binance-open_interest-derived-15m",
        "status": "blocked",
        "reason": "refresh_supported_for_raw_market_data_only",
    }


def test_market_data_refresh_endpoint_fills_raw_funding_dataset():
    repository = FakeMarketDataRepository()

    def fake_funding_fill_service(*, registration, repository, adapter):
        assert registration["dataset_id"] == "btc-binance-funding-raw-8h"
        assert registration["data_type"] == "funding"
        repository.update_ref({**registration, "row_count": 101, "quality_status": "updated"})
        return {
            "dataset_id": registration["dataset_id"],
            "status": "filled",
            "rows_added": 1,
            "row_count": 101,
            "end_ts": "2026-07-01T00:00:00Z",
            "source": "binance_cli",
        }

    client = TestClient(
        create_app(
            market_data_repository=repository,
            market_data_fill_service=fake_funding_fill_service,
        )
    )

    response = client.post("/api/v1/market-data/btc-binance-funding-raw-8h/refresh")

    assert response.status_code == 200
    assert response.json()["dataset_id"] == "btc-binance-funding-raw-8h"
    assert response.json()["status"] == "filled"
    assert repository.updated_registration["row_count"] == 101
