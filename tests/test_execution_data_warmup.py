from __future__ import annotations

from datetime import UTC, datetime, timedelta

from quant_terminal_worker.execution.data_warmup import warm_route_data


class FakeRuntimeRepository:
    def __init__(self) -> None:
        self.route = {
            "route_id": "aave-live",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "data_warmed": False,
        }
        self.engines = [
            {
                "signal_engine_id": "vegas_ema",
                "version": "0.1",
                "required_data": [
                    {
                        "data_type": "candles",
                        "origin": "raw",
                        "timeframe": "5m",
                    },
                    {
                        "data_type": "candles",
                        "origin": "derived",
                        "timeframe": "2h",
                        "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                    },
                    {
                        "data_type": "feature_bollinger",
                        "origin": "derived",
                        "timeframe": "2h",
                        "source": {"data_type": "candles", "origin": "derived", "timeframe": "2h"},
                    },
                ],
            }
        ]
        self.gate_updates = []

    def get_deployment_route(self, route_id):
        if route_id != self.route["route_id"]:
            return None
        return dict(self.route)

    def list_signal_engines(self):
        return list(self.engines)

    def update_deployment_route_gate(self, route_id, **values):
        assert route_id == self.route["route_id"]
        self.route = {**self.route, **values}
        self.gate_updates.append(values)
        return dict(self.route)


class FakeMarketDataRepository:
    def __init__(self) -> None:
        self.raw_ref = {
            "dataset_id": "aave-raw-5m",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "start_ts": datetime(2026, 3, 1, tzinfo=UTC),
            "end_ts": datetime(2026, 6, 1, tzinfo=UTC),
            "row_count": 100,
            "storage_uri": ".data/market-data/aave/5m",
        }
        self.derived_ref = {
            **self.raw_ref,
            "dataset_id": "aave-derived-2h",
            "timeframe": "2h",
            "data_origin": "derived",
        }
        self.feature_ref = {
            **self.raw_ref,
            "dataset_id": "AAVE-feature_bollinger-2h",
            "data_type": "feature_bollinger",
            "timeframe": "2h",
            "data_origin": "derived",
        }

    def get_raw_candle_ref(self, asset, timeframe="5m"):
        if asset == "AAVE" and timeframe == "5m":
            return dict(self.raw_ref)
        return None

    def list_derived_refs_for_raw(self, registration):
        assert registration["dataset_id"] == "aave-raw-5m"
        return [dict(self.derived_ref)]

    def get_candle_ref(self, *, asset, timeframe, origin, data_type="candles"):
        if (
            asset == "AAVE"
            and timeframe == "5m"
            and origin == "raw"
            and data_type == "candles"
        ):
            return dict(self.raw_ref)
        if (
            asset == "AAVE"
            and timeframe == self.derived_ref["timeframe"]
            and origin == "derived"
            and data_type == "candles"
        ):
            return dict(self.derived_ref)
        if (
            self.feature_ref is not None
            and asset == "AAVE"
            and timeframe == "2h"
            and origin == "derived"
            and data_type == "feature_bollinger"
        ):
            return dict(self.feature_ref)
        return None


class FakeAdapter:
    pass


def test_warm_route_data_fills_raw_requirement_and_marks_route_warmed():
    runtime_repository = FakeRuntimeRepository()
    market_repository = FakeMarketDataRepository()
    fill_calls = []

    def fill_service(*, registration, repository, adapter):
        fill_calls.append({"registration": registration, "repository": repository, "adapter": adapter})
        return {
            "dataset_id": registration["dataset_id"],
            "status": "filled",
            "rows_added": 12,
            "derived_rebuilt": [{"dataset_id": "aave-derived-2h", "timeframe": "2h"}],
            "end_ts": "2026-06-05T00:00:00Z",
        }

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        adapter=FakeAdapter(),
        feature_service=lambda **kwargs: {
            "status": "enriched",
            "family": kwargs["family"],
            "feature_count": 1,
            "features": [{"dataset_id": "AAVE-feature_bollinger-2h", "timeframe": "2h", "row_count": 100}],
        },
    )

    assert result["status"] == "warmed"
    assert result["route_id"] == "aave-live"
    assert result["requirements"][0]["status"] == "filled"
    assert result["requirements"][1]["status"] == "satisfied_by_raw_rebuild"
    assert result["requirements"][2]["status"] == "feature_enriched"
    assert result["requirements"][2]["dataset_id"] == "AAVE-feature_bollinger-2h"
    assert fill_calls[0]["registration"]["dataset_id"] == "aave-raw-5m"
    assert fill_calls[0]["repository"] is market_repository
    assert runtime_repository.gate_updates == [{"data_warmed": True}]


def test_warm_route_data_blocks_when_required_raw_ref_is_missing():
    runtime_repository = FakeRuntimeRepository()

    class MissingMarketDataRepository:
        def get_raw_candle_ref(self, asset, timeframe="5m"):
            return None

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=MissingMarketDataRepository(),
        fill_service=lambda **kwargs: {"status": "filled"},
        adapter=FakeAdapter(),
        feature_service=lambda **kwargs: {"status": "enriched", "feature_count": 0, "features": []},
    )

    assert result["status"] == "blocked"
    assert result["requirements"][0]["reason"] == "missing_raw_candle_ref"
    assert runtime_repository.gate_updates == []


def test_warm_route_data_blocks_when_required_feature_cannot_be_built():
    runtime_repository = FakeRuntimeRepository()
    market_repository = FakeMarketDataRepository()
    market_repository.feature_ref = None

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=lambda **kwargs: {"status": "filled"},
        adapter=FakeAdapter(),
        feature_service=lambda **kwargs: {
            "status": "noop",
            "family": kwargs["family"],
            "feature_count": 0,
            "features": [],
            "skipped": [{"reason": "empty_source_after_start_date"}],
        },
    )

    assert result["status"] == "blocked"
    feature_requirement = result["requirements"][2]
    assert feature_requirement["data_type"] == "feature_bollinger"
    assert feature_requirement["reason"] == "feature_refresh_produced_no_matching_dataset"
    assert runtime_repository.gate_updates == []


def test_warm_route_data_reports_fresh_5m_candle_status():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        {
            "data_type": "candles",
            "origin": "derived",
            "timeframe": "5m",
            "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        },
    ]
    market_repository = FakeMarketDataRepository()
    as_of = datetime(2026, 6, 1, 0, 5, tzinfo=UTC)
    latest = as_of - timedelta(minutes=5)
    market_repository.raw_ref["end_ts"] = latest
    market_repository.derived_ref["timeframe"] = "5m"
    market_repository.derived_ref["end_ts"] = latest

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=lambda **kwargs: {"status": "current", "rows_added": 0, "end_ts": latest.isoformat()},
        adapter=FakeAdapter(),
        as_of=as_of,
    )

    assert result["status"] == "warmed"
    assert result["data_freshness"]["status"] == "fresh"
    assert result["data_freshness"]["raw_5m"]["status"] == "fresh"
    assert result["data_freshness"]["derived_5m"]["status"] == "fresh"
    assert result["data_freshness"]["candle_interval_seconds"] == 300
    assert result["data_freshness"]["max_age_seconds"] == 690


def test_warm_route_data_accepts_latest_confirmed_5m_candle_start_timestamp():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        {
            "data_type": "candles",
            "origin": "derived",
            "timeframe": "5m",
            "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        },
    ]
    market_repository = FakeMarketDataRepository()
    as_of = datetime(2026, 6, 1, 0, 8, 17, tzinfo=UTC)
    latest_confirmed_start = datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    market_repository.raw_ref["end_ts"] = latest_confirmed_start
    market_repository.derived_ref["timeframe"] = "5m"
    market_repository.derived_ref["end_ts"] = latest_confirmed_start

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=lambda **kwargs: {
            "status": "current",
            "rows_added": 0,
            "end_ts": latest_confirmed_start.isoformat(),
        },
        adapter=FakeAdapter(),
        as_of=as_of,
    )

    assert result["status"] == "warmed"
    assert result["data_freshness"]["status"] == "fresh"
    assert result["data_freshness"]["raw_5m"]["age_seconds"] == 497


def test_warm_route_data_blocks_when_latest_5m_candle_is_stale_after_retry():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    market_repository = FakeMarketDataRepository()
    as_of = datetime(2026, 6, 1, 0, 30, tzinfo=UTC)
    stale = as_of - timedelta(minutes=20)
    market_repository.raw_ref["end_ts"] = stale
    fill_calls = []

    def fill_service(*, registration, repository, adapter):
        fill_calls.append(registration["dataset_id"])
        return {"status": "no_new_rows", "rows_added": 0, "end_ts": stale.isoformat()}

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        adapter=FakeAdapter(),
        feature_service=lambda **kwargs: {
            "status": "enriched",
            "family": kwargs["family"],
            "feature_count": 1,
            "features": [{"dataset_id": "AAVE-feature_bollinger-2h", "timeframe": "2h", "row_count": 100}],
        },
        as_of=as_of,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "market_data_stale"
    assert result["data_freshness"]["status"] == "stale"
    assert result["data_freshness"]["raw_5m"]["age_seconds"] == 1200
    assert fill_calls == ["aave-raw-5m", "aave-raw-5m"]
    assert runtime_repository.gate_updates == [{"data_warmed": False}]
