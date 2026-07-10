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

    result = jobs.execute_job(
        repository=object(),
        market_data_repository=FakeMarketDataRepository(),
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
