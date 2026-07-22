from __future__ import annotations

from pathlib import Path

from quant_terminal_worker import jobs


class FakeMarketDataRepository:
    def get_ref(self, dataset_id: str):
        assert dataset_id == "sol-binance-open_interest-raw-5m"
        return {
            "dataset_id": dataset_id,
            "source_id": "binance",
            "asset": "SOL",
            "instrument": "SOLUSDT",
            "data_type": "open_interest",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_uri": ".data/market-data/sol/oi",
        }


class FakeFundingMarketDataRepository:
    def get_ref(self, dataset_id: str):
        assert dataset_id == "btc-binance-funding-raw-8h"
        return {
            "dataset_id": dataset_id,
            "source_id": "binance",
            "asset": "BTC",
            "instrument": "BTCUSDT",
            "data_type": "funding",
            "timeframe": "8h",
            "data_origin": "raw",
            "storage_uri": ".data/market-data/btc/funding",
        }


class FakeFuturesMetricsMarketDataRepository:
    def get_ref(self, dataset_id: str):
        assert dataset_id == "btc-binance-futures_metrics-raw-5m"
        return {
            "dataset_id": dataset_id,
            "source_id": "binance",
            "asset": "BTC",
            "instrument": "BTCUSDT",
            "data_type": "futures_metrics",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_uri": ".data/market-data/btc/futures_metrics",
        }


class FakePremiumIndexMarketDataRepository:
    def get_ref(self, dataset_id: str):
        assert dataset_id == "btc-binance-premium_index-raw-5m"
        return {
            "dataset_id": dataset_id,
            "source_id": "binance",
            "asset": "BTC",
            "instrument": "BTCUSDT",
            "data_type": "premium_index",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_uri": ".data/market-data/btc/premium_index",
        }


def test_market_data_refresh_job_routes_open_interest_to_binance_fill(monkeypatch):
    calls = []

    class FakeBinanceAdapter:
        def __init__(self, config):
            self.config = config

    def fake_fill_raw_open_interest_dataset(*, registration, repository, adapter):
        calls.append({"registration": registration, "repository": repository, "adapter": adapter})
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    monkeypatch.setattr(jobs, "BinanceCLIAdapter", FakeBinanceAdapter)
    monkeypatch.setattr(jobs, "fill_raw_open_interest_dataset", fake_fill_raw_open_interest_dataset)

    market_repository = FakeMarketDataRepository()
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=market_repository,
        workspace_root=Path("."),
        job={
            "job_id": "job-oi",
            "job_type": "market_data_refresh",
            "payload": {
                "dataset_id": "sol-binance-open_interest-raw-5m",
                "binance_cli_path": "/opt/homebrew/bin/binance-cli",
                "binance_profile": "research",
            },
        },
    )

    assert result == {"dataset_id": "sol-binance-open_interest-raw-5m", "status": "current", "rows_added": 0}
    assert calls[0]["registration"]["data_type"] == "open_interest"
    assert calls[0]["adapter"].config == {
        "cli_path": "/opt/homebrew/bin/binance-cli",
        "profile": "research",
    }


def test_market_data_feature_refresh_routes_open_interest_regime_to_specialized_service(monkeypatch):
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

    monkeypatch.setattr(jobs, "enrich_open_interest_regime_datasets", fake_enrich_open_interest_regime_datasets)

    market_repository = FakeMarketDataRepository()
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=market_repository,
        workspace_root=Path("/workspace"),
        job={
            "job_id": "job-oi-features",
            "job_type": "market_data_feature_refresh",
            "payload": {"asset": "btc", "family": "open_interest_regime"},
        },
    )

    assert result["family"] == "open_interest_regime"
    assert calls[0]["repository"] is market_repository
    assert calls[0]["asset"] == "BTC"
    assert calls[0]["target_root"] == Path("/workspace/.data/market-data")
    assert "family" not in calls[0]


def test_market_data_refresh_job_routes_funding_to_binance_fill(monkeypatch):
    calls = []

    class FakeBinanceAdapter:
        def __init__(self, config):
            self.config = config

    def fake_fill_raw_funding_dataset(*, registration, repository, adapter):
        calls.append({"registration": registration, "repository": repository, "adapter": adapter})
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    monkeypatch.setattr(jobs, "BinanceCLIAdapter", FakeBinanceAdapter)
    monkeypatch.setattr(jobs, "fill_raw_funding_dataset", fake_fill_raw_funding_dataset)

    market_repository = FakeFundingMarketDataRepository()
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=market_repository,
        workspace_root=Path("."),
        job={
            "job_id": "job-funding",
            "job_type": "market_data_refresh",
            "payload": {
                "dataset_id": "btc-binance-funding-raw-8h",
                "binance_cli_path": "/opt/homebrew/bin/binance-cli",
                "binance_profile": "research",
            },
        },
    )

    assert result == {"dataset_id": "btc-binance-funding-raw-8h", "status": "current", "rows_added": 0}
    assert calls[0]["registration"]["data_type"] == "funding"
    assert calls[0]["adapter"].config == {
        "cli_path": "/opt/homebrew/bin/binance-cli",
        "profile": "research",
    }


def test_market_data_refresh_job_routes_futures_metrics_to_binance_fill(monkeypatch):
    calls = []

    class FakeBinanceAdapter:
        def __init__(self, config):
            self.config = config

    def fake_fill(*, registration, repository, adapter):
        calls.append({"registration": registration, "repository": repository, "adapter": adapter})
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    monkeypatch.setattr(jobs, "BinanceCLIAdapter", FakeBinanceAdapter)
    monkeypatch.setattr(jobs, "fill_raw_futures_metrics_dataset", fake_fill)
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=FakeFuturesMetricsMarketDataRepository(),
        workspace_root=Path("."),
        job={
            "job_id": "job-futures-metrics",
            "job_type": "market_data_refresh",
            "payload": {"dataset_id": "btc-binance-futures_metrics-raw-5m"},
        },
    )

    assert result["status"] == "current"
    assert calls[0]["registration"]["data_type"] == "futures_metrics"


def test_market_data_refresh_job_routes_premium_index_to_binance_fill(monkeypatch):
    calls = []

    class FakeBinanceAdapter:
        def __init__(self, config):
            self.config = config

    def fake_fill(*, registration, repository, adapter):
        calls.append({"registration": registration, "adapter": adapter})
        return {"dataset_id": registration["dataset_id"], "status": "current"}

    monkeypatch.setattr(jobs, "BinanceCLIAdapter", FakeBinanceAdapter)
    monkeypatch.setattr(jobs, "fill_raw_premium_index_dataset", fake_fill)
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=FakePremiumIndexMarketDataRepository(),
        workspace_root=Path("."),
        job={
            "job_id": "job-premium-index",
            "job_type": "market_data_refresh",
            "payload": {"dataset_id": "btc-binance-premium_index-raw-5m"},
        },
    )

    assert result["status"] == "current"
    assert calls[0]["registration"]["data_type"] == "premium_index"
