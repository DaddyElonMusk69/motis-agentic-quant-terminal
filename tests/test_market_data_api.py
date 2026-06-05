from datetime import UTC, datetime

from fastapi.testclient import TestClient

from quant_terminal_api.main import create_app


class FakeMarketDataRepository:
    def __init__(self):
        self.updated_registration = None

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
                "quality_status": "ingested",
                "ingestion_version": "legacy",
            }
        ]

    def get_ref(self, dataset_id: str):
        if dataset_id == "btc-raw-5m":
            return self.list_refs()[0]
        if dataset_id == "btc-derived-5m":
            return {**self.list_refs()[0], "dataset_id": "btc-derived-5m", "data_origin": "derived"}
        return None

    def update_ref(self, registration):
        self.updated_registration = registration


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
