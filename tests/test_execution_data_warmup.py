from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

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
        self.open_interest_ref = {
            **self.raw_ref,
            "dataset_id": "aave-binance-open_interest-raw-5m",
            "instrument": "AAVEUSDT",
            "data_type": "open_interest",
            "data_origin": "raw",
        }
        self.futures_metrics_ref = {
            **self.raw_ref,
            "dataset_id": "aave-binance-futures_metrics-raw-5m",
            "instrument": "AAVEUSDT",
            "data_type": "futures_metrics",
            "data_origin": "raw",
        }
        self.premium_index_ref = {
            **self.raw_ref,
            "dataset_id": "aave-binance-premium_index-raw-5m",
            "instrument": "AAVEUSDT",
            "data_type": "premium_index",
            "data_origin": "raw",
        }
        self.funding_ref = {
            **self.raw_ref,
            "dataset_id": "aave-binance-funding-raw-8h",
            "instrument": "AAVEUSDT",
            "data_type": "funding",
            "data_origin": "raw",
            "timeframe": "8h",
        }
        self.funding_features_ref = {
            **self.raw_ref,
            "dataset_id": "aave-binance-funding_features-derived-5m",
            "instrument": "AAVEUSDT",
            "data_type": "funding_features",
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
        if (
            asset == "AAVE"
            and timeframe == "5m"
            and origin == "raw"
            and data_type == "open_interest"
        ):
            return dict(self.open_interest_ref)
        if (
            asset == "AAVE"
            and timeframe == "5m"
            and origin == "raw"
            and data_type == "futures_metrics"
        ):
            return dict(self.futures_metrics_ref)
        if (
            asset == "AAVE"
            and timeframe == "5m"
            and origin == "raw"
            and data_type == "premium_index"
        ):
            return dict(self.premium_index_ref)
        if (
            asset == "AAVE"
            and timeframe == "8h"
            and origin == "raw"
            and data_type == "funding"
        ):
            return dict(self.funding_ref)
        if (
            asset == "AAVE"
            and timeframe == "5m"
            and origin == "derived"
            and data_type == "funding_features"
        ):
            return dict(self.funding_features_ref)
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


def test_warm_route_data_fills_required_open_interest_with_data_type_service():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        {"data_type": "open_interest", "origin": "raw", "timeframe": "5m"},
    ]
    market_repository = FakeMarketDataRepository()
    candle_adapter = object()
    oi_adapter = object()
    fill_calls = []

    def candle_fill_service(*, registration, repository, adapter):
        fill_calls.append(("candles", registration["dataset_id"], adapter))
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    def oi_fill_service(*, registration, repository, adapter):
        fill_calls.append(("open_interest", registration["dataset_id"], adapter))
        return {"dataset_id": registration["dataset_id"], "status": "filled", "rows_added": 12}

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=candle_fill_service,
        raw_fill_services={
            "candles": candle_fill_service,
            "open_interest": oi_fill_service,
        },
        raw_adapters={
            "candles": candle_adapter,
            "open_interest": oi_adapter,
        },
        adapter=candle_adapter,
    )

    assert result["status"] == "warmed"
    assert fill_calls == [
        ("candles", "aave-raw-5m", candle_adapter),
        ("open_interest", "aave-binance-open_interest-raw-5m", oi_adapter),
    ]
    assert [item["status"] for item in result["requirements"]] == ["current", "filled"]
    assert runtime_repository.gate_updates == [{"data_warmed": True}]


def test_warm_route_data_supports_derivatives_engine_data_channels():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
        {"data_type": "futures_metrics", "origin": "raw", "timeframe": "5m"},
        {"data_type": "premium_index", "origin": "raw", "timeframe": "5m"},
        {"data_type": "funding_features", "origin": "derived", "timeframe": "5m"},
    ]
    market_repository = FakeMarketDataRepository()
    fill_calls = []

    def fill_service(*, registration, repository, adapter):
        fill_calls.append(registration["data_type"])
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        raw_fill_services={
            "candles": fill_service,
            "futures_metrics": fill_service,
            "premium_index": fill_service,
            "funding": fill_service,
        },
        adapter=FakeAdapter(),
    )

    assert result["status"] == "warmed"
    assert fill_calls == ["candles", "futures_metrics", "premium_index", "funding"]
    assert [item["status"] for item in result["requirements"]] == [
        "current",
        "current",
        "current",
        "refreshed_from_raw_dependency",
    ]
    assert result["requirements"][3]["dataset_id"] == "aave-binance-funding_features-derived-5m"
    assert result["requirements"][3]["source_dataset_id"] == "aave-binance-funding-raw-8h"
    assert runtime_repository.gate_updates == [{"data_warmed": True}]


def test_warm_route_data_blocks_when_required_derivatives_ref_remains_stale():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "premium_index", "origin": "raw", "timeframe": "5m"},
        {"data_type": "funding_features", "origin": "derived", "timeframe": "5m"},
    ]
    market_repository = FakeMarketDataRepository()
    as_of = datetime(2026, 6, 1, 0, 30, tzinfo=UTC)
    stale = as_of - timedelta(minutes=20)
    stale_funding = as_of - timedelta(hours=11)
    market_repository.premium_index_ref["end_ts"] = stale
    market_repository.funding_ref["end_ts"] = stale_funding
    market_repository.funding_features_ref["end_ts"] = stale
    fill_calls = []

    def fill_service(*, registration, repository, adapter):
        fill_calls.append(registration["dataset_id"])
        return {"dataset_id": registration["dataset_id"], "status": "no_new_rows", "rows_added": 0}

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        raw_fill_services={
            "premium_index": fill_service,
            "funding": fill_service,
        },
        adapter=FakeAdapter(),
        as_of=as_of,
    )

    assert result["status"] == "blocked"
    assert result["reason"] == "market_data_stale"
    assert result["data_freshness"]["status"] == "stale"
    assert result["data_freshness"]["reason"] == "raw_premium_index_5m_stale"
    assert [
        (item["data_type"], item["origin"], item["status"])
        for item in result["data_freshness"]["required_refs"]
    ] == [
        ("premium_index", "raw", "stale"),
        ("funding_features", "derived", "stale"),
    ]
    assert result["data_freshness"]["dependency_refs"][0]["data_type"] == "funding"
    assert result["data_freshness"]["dependency_refs"][0]["status"] == "stale"
    assert fill_calls == ["aave-binance-premium_index-raw-5m", "aave-binance-funding-raw-8h"]
    assert runtime_repository.gate_updates == [{"data_warmed": False}]


def test_warm_route_data_blocks_cleanly_when_required_raw_fill_raises():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "futures_metrics", "origin": "raw", "timeframe": "5m"}
    ]
    market_repository = FakeMarketDataRepository()

    def fill_service(**kwargs):
        raise RuntimeError("binance cli failed")

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        raw_fill_services={"futures_metrics": fill_service},
        adapter=FakeAdapter(),
        as_of=datetime(2026, 6, 1, 0, 30, tzinfo=UTC),
    )

    assert result["status"] == "blocked"
    requirement = result["requirements"][0]
    assert requirement["status"] == "blocked"
    assert requirement["reason"] == "raw_fill_failed"
    assert requirement["dataset_id"] == "aave-binance-futures_metrics-raw-5m"
    assert requirement["error"] == {"type": "RuntimeError", "message": "binance cli failed"}
    assert runtime_repository.gate_updates == [{"data_warmed": False}]


def test_warm_route_data_blocks_cleanly_when_refreshable_dependency_fill_raises():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.route["cron_interval_minutes"] = 5
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "funding_features", "origin": "derived", "timeframe": "5m"}
    ]
    market_repository = FakeMarketDataRepository()

    def fill_service(**kwargs):
        raise RuntimeError("funding cli failed")

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        raw_fill_services={"funding": fill_service},
        adapter=FakeAdapter(),
        as_of=datetime(2026, 6, 1, 0, 30, tzinfo=UTC),
    )

    assert result["status"] == "blocked"
    requirement = result["requirements"][0]
    assert requirement["status"] == "blocked"
    assert requirement["reason"] == "raw_dependency_fill_failed"
    assert requirement["source_dataset_id"] == "aave-binance-funding-raw-8h"
    assert requirement["error"] == {"type": "RuntimeError", "message": "funding cli failed"}
    assert runtime_repository.gate_updates == [{"data_warmed": False}]


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


def test_warm_route_data_can_build_technical_indicator_atr_from_raw_5m_source():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "technical_indicator_atr", "origin": "derived", "timeframe": "2h"}
    ]
    market_repository = FakeMarketDataRepository()
    calls = []

    def atr_service(**kwargs):
        calls.append(kwargs)
        return {
            "status": "enriched",
            "asset": kwargs["asset"],
            "data_type": "technical_indicator_atr",
            "dataset_count": 1,
            "datasets": [
                {
                    "dataset_id": "AAVE-technical_indicator_atr-derived-2h-wilder-14",
                    "data_type": "technical_indicator_atr",
                    "timeframe": "2h",
                    "row_count": 100,
                }
            ],
        }

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=lambda **kwargs: {"dataset_id": kwargs["registration"]["dataset_id"], "status": "current", "rows_added": 0},
        adapter=FakeAdapter(),
        atr_service=atr_service,
    )

    assert result["status"] == "warmed"
    assert result["requirements"][0]["status"] == "atr_enriched"
    assert result["requirements"][0]["dataset_id"] == "AAVE-technical_indicator_atr-derived-2h-wilder-14"
    assert calls[0]["asset"] == "AAVE"
    assert calls[0]["timeframes"] == ("2h",)
    assert calls[0]["target_root"] == Path(".") / ".data" / "market-data"


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


def test_warm_route_data_fills_raw_open_interest_requirement():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.engines[0]["required_data"] = [
        {"data_type": "open_interest", "origin": "raw", "timeframe": "5m"}
    ]
    market_repository = FakeMarketDataRepository()
    fill_calls = []

    def fill_service(*, registration, repository, adapter):
        fill_calls.append(registration["dataset_id"])
        return {"dataset_id": registration["dataset_id"], "status": "current", "rows_added": 0}

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=fill_service,
        adapter=FakeAdapter(),
    )

    assert result["status"] == "warmed"
    assert result["requirements"] == [
        {
            "data_type": "open_interest",
            "origin": "raw",
            "timeframe": "5m",
            "status": "current",
            "dataset_id": "aave-binance-open_interest-raw-5m",
            "fill_result": {"dataset_id": "aave-binance-open_interest-raw-5m", "status": "current", "rows_added": 0},
        }
    ]
    assert fill_calls == ["aave-binance-open_interest-raw-5m"]


def test_warm_route_data_enriches_open_interest_regime_with_specialized_service():
    runtime_repository = FakeRuntimeRepository()
    runtime_repository.engines[0]["required_data"] = [
        {
            "data_type": "feature_open_interest_regime",
            "origin": "derived",
            "timeframe": "15m",
        }
    ]
    market_repository = FakeMarketDataRepository()
    oi_feature_ref = {
        **market_repository.open_interest_ref,
        "dataset_id": "AAVE-feature_open_interest_regime-15m",
        "data_type": "feature_open_interest_regime",
        "timeframe": "15m",
        "data_origin": "derived",
    }
    original_get_ref = market_repository.get_candle_ref

    def get_ref(**kwargs):
        if (
            kwargs["asset"] == "AAVE"
            and kwargs["timeframe"] == "15m"
            and kwargs["origin"] == "derived"
            and kwargs["data_type"] == "feature_open_interest_regime"
        ):
            return dict(oi_feature_ref)
        return original_get_ref(**kwargs)

    market_repository.get_candle_ref = get_ref
    service_calls = []

    def oi_feature_service(**kwargs):
        service_calls.append(kwargs)
        return {
            "status": "enriched",
            "family": "open_interest_regime",
            "feature_count": 1,
            "features": [
                {
                    "dataset_id": "AAVE-feature_open_interest_regime-15m",
                    "data_type": "feature_open_interest_regime",
                    "timeframe": "15m",
                    "row_count": 100,
                }
            ],
        }

    result = warm_route_data(
        route_id="aave-live",
        runtime_repository=runtime_repository,
        market_data_repository=market_repository,
        fill_service=lambda **kwargs: {"status": "current"},
        adapter=FakeAdapter(),
        open_interest_feature_service=oi_feature_service,
    )

    assert result["status"] == "warmed"
    assert result["requirements"][0]["status"] == "feature_enriched"
    assert result["requirements"][0]["family"] == "open_interest_regime"
    assert service_calls[0]["asset"] == "AAVE"
    assert "family" not in service_calls[0]
