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


class FakeRuntimeRepository:
    def __init__(self, session: dict | None = None) -> None:
        self.session = session or {
            "session_id": "stage1-btc-test",
            "asset": "BTC",
            "signal_set_key": "engine:BTC:set",
            "train_start": "2025-01-01",
            "train_end": "2025-01-31",
            "walk_forward_start": "2025-02-01",
            "walk_forward_end": "2025-02-28",
        }
        self.heartbeats: list[tuple[str, str | None]] = []

    def get_stage1_research_session(self, session_id: str):
        return self.session if session_id == self.session["session_id"] else None

    def heartbeat_job(self, job_id: str, current_step: str | None = None):
        self.heartbeats.append((job_id, current_step))

    def list_signals_for_signal_set_window(self, **kwargs):
        return []


def test_stage3_policy_job_passes_market_data_repository_to_atr_enabled_runner(monkeypatch):
    calls = []

    def fake_stage2_raw_candles(*args, **kwargs):
        return [{"timestamp": "2025-01-01T00:00:00Z"}]

    def fake_run_stage3_local_variants(**kwargs):
        calls.append(kwargs)
        return {"stage3c_atr_combinations_tested": 16}

    monkeypatch.setattr(jobs, "_stage2_raw_candles", fake_stage2_raw_candles)
    monkeypatch.setattr(jobs, "run_stage3_local_variants", fake_run_stage3_local_variants)

    runtime_repository = FakeRuntimeRepository()
    market_repository = object()
    result = jobs.execute_job(
        repository=runtime_repository,
        market_data_repository=market_repository,
        workspace_root=Path("/workspace"),
        job={
            "job_id": "job-stage3c",
            "job_type": "stage3_policy_step",
            "payload": {"session_id": "stage1-btc-test", "step": "local_variants"},
        },
    )

    assert result["stage3_grid"] == {"stage3c_atr_combinations_tested": 16}
    assert calls[0]["market_data_repository"] is market_repository
    assert runtime_repository.heartbeats == [("job-stage3c", "stage3_local_variants")]


def test_stage4_realized_expectancy_job_passes_market_data_repository_to_atr_enabled_runner(monkeypatch):
    calls = []

    def fake_stage2_raw_candles(*args, **kwargs):
        return [{"timestamp": "2025-01-01T00:00:00Z"}]

    def fake_run_stage4_realized_expectancy(**kwargs):
        calls.append(kwargs)
        return {"status": "complete"}

    monkeypatch.setattr(jobs, "_stage2_raw_candles", fake_stage2_raw_candles)
    monkeypatch.setattr(jobs, "run_stage4_realized_expectancy", fake_run_stage4_realized_expectancy)

    runtime_repository = FakeRuntimeRepository()
    market_repository = object()
    result = jobs.execute_job(
        repository=runtime_repository,
        market_data_repository=market_repository,
        workspace_root=Path("/workspace"),
        job={
            "job_id": "job-stage4",
            "job_type": "stage4_realized_expectancy",
            "payload": {
                "session_id": "stage1-btc-test",
                "initial_capital_usdt": 10000,
                "margin_allocation_pct": 10,
                "leverage": 5,
            },
        },
    )

    assert result["stage4_realized_expectancy"] == {"status": "complete"}
    assert calls[0]["market_data_repository"] is market_repository
    assert runtime_repository.heartbeats == [("job-stage4", "stage4_realized_expectancy")]


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


def test_market_data_atr_refresh_routes_to_atr_enrichment(monkeypatch):
    calls = []

    def fake_enrich_atr_datasets(**kwargs):
        calls.append(kwargs)
        return {
            "status": "enriched",
            "asset": kwargs["asset"],
            "data_type": "technical_indicator_atr",
            "dataset_count": 3,
            "datasets": [],
        }

    monkeypatch.setattr(jobs, "enrich_atr_datasets", fake_enrich_atr_datasets)

    market_repository = FakeMarketDataRepository()
    result = jobs.execute_job(
        repository=object(),
        market_data_repository=market_repository,
        workspace_root=Path("/workspace"),
        job={
            "job_id": "job-atr-btc",
            "job_type": "market_data_atr_refresh",
            "payload": {"asset": "btc"},
        },
    )

    assert result["data_type"] == "technical_indicator_atr"
    assert calls[0]["repository"] is market_repository
    assert calls[0]["asset"] == "BTC"
    assert calls[0]["timeframes"] == ("1h", "2h", "4h")
    assert calls[0]["period"] == 14
    assert calls[0]["target_root"] == Path("/workspace/.data/market-data")


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
