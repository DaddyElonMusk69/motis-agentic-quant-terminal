from __future__ import annotations

import json

import pytest

import quant_terminal_worker.adapters.binance as binance_adapter
from quant_terminal_worker.adapters.binance import BinanceCLIAdapter, BinanceCLIError


def test_binance_cli_adapter_fetches_usds_open_interest_statistics():
    calls = []

    def runner(command, *, timeout_seconds):
        calls.append({"command": command, "timeout_seconds": timeout_seconds})
        return json.dumps(
            [
                {
                    "symbol": "SOLUSDT",
                    "sumOpenInterest": "9418572.34000000",
                    "sumOpenInterestValue": "734466788.06595920",
                    "CMCCirculatingSupply": "581839026.56873320",
                    "timestamp": 1783589400000,
                }
            ]
        )

    adapter = BinanceCLIAdapter(config={"cli_path": "/opt/homebrew/bin/binance-cli"}, command_runner=runner)

    rows = adapter.open_interest_statistics(
        symbol="SOLUSDT",
        period="5m",
        limit=2,
        start_time_ms=1783589100000,
        end_time_ms=1783589700000,
    )

    assert rows == [
        {
            "symbol": "SOLUSDT",
            "sumOpenInterest": "9418572.34000000",
            "sumOpenInterestValue": "734466788.06595920",
            "CMCCirculatingSupply": "581839026.56873320",
            "timestamp": 1783589400000,
        }
    ]
    assert calls == [
        {
            "command": [
                "/opt/homebrew/bin/binance-cli",
                "futures-usds",
                "open-interest-statistics",
                "--symbol",
                "SOLUSDT",
                "--period",
                "5m",
                "--limit",
                "2",
                "--start-time",
                "1783589100000",
                "--end-time",
                "1783589700000",
            ],
            "timeout_seconds": 30,
        }
    ]


def test_binance_cli_adapter_fetches_futures_metrics_ratio_histories():
    calls = []

    def runner(command, *, timeout_seconds):
        calls.append(command)
        return json.dumps([{"longShortRatio": "1.2345", "timestamp": 1783814700000}])

    adapter = BinanceCLIAdapter(
        config={"cli_path": "/opt/homebrew/bin/binance-cli"},
        command_runner=runner,
    )

    assert adapter.top_trader_account_ratio(
        symbol="BTCUSDT", period="5m", limit=2
    )[0]["longShortRatio"] == "1.2345"
    adapter.top_trader_position_ratio(symbol="BTCUSDT", period="5m", limit=2)
    adapter.global_account_ratio(symbol="BTCUSDT", period="5m", limit=2)
    adapter.taker_buy_sell_volume(symbol="BTCUSDT", period="5m", limit=2)

    assert [command[2] for command in calls] == [
        "top-trader-long-short-ratio-accounts",
        "top-trader-long-short-ratio-positions",
        "long-short-ratio",
        "taker-buy-sell-volume",
    ]


def test_binance_cli_adapter_ignores_profile_warning_before_json():
    def runner(command, *, timeout_seconds):
        return (
            "There is no active profile found, using public endpoint mode.\n"
            + json.dumps([{"longShortRatio": "1.2345", "timestamp": 1783814700000}])
        )

    adapter = BinanceCLIAdapter(
        config={"cli_path": "/opt/homebrew/bin/binance-cli"},
        command_runner=runner,
    )

    rows = adapter.top_trader_position_ratio(symbol="BTCUSDT", period="5m", limit=2)

    assert rows == [{"longShortRatio": "1.2345", "timestamp": 1783814700000}]


def test_binance_cli_adapter_uses_rest_fallback_after_non_json_cli_failure(monkeypatch):
    def runner(command, *, timeout_seconds):
        return "Request failed after 3 retries\n"

    rest_calls = []

    def fake_rest(**kwargs):
        rest_calls.append(kwargs)
        return [{"longShortRatio": "1.2345", "timestamp": 1783814700000}]

    monkeypatch.setattr(binance_adapter, "_request_futures_data_json", fake_rest)
    adapter = BinanceCLIAdapter(
        config={"cli_path": "/opt/homebrew/bin/binance-cli", "api_key": "redacted"},
        command_runner=runner,
    )

    rows = adapter.global_account_ratio(
        symbol="BTCUSDT",
        period="5m",
        limit=2,
        start_time_ms=1783814400000,
        end_time_ms=1783815000000,
    )

    assert rows == [{"longShortRatio": "1.2345", "timestamp": 1783814700000}]
    assert rest_calls == [
        {
            "rest_path": "globalLongShortAccountRatio",
            "params": {
                "symbol": "BTCUSDT",
                "period": "5m",
                "limit": 2,
                "startTime": 1783814400000,
                "endTime": 1783815000000,
            },
            "timeout_seconds": 30,
            "api_key": "redacted",
        }
    ]


def test_binance_cli_adapter_uses_rest_fallback_after_cli_command_failure(monkeypatch):
    def runner(command, *, timeout_seconds):
        raise BinanceCLIError("Request failed after 3 retries")

    rest_calls = []

    def fake_rest(**kwargs):
        rest_calls.append(kwargs)
        return [{"symbol": "SOLUSDT", "sumOpenInterest": "10", "sumOpenInterestValue": "20", "timestamp": 1783814700000}]

    monkeypatch.setattr(binance_adapter, "_request_futures_data_json", fake_rest)
    adapter = BinanceCLIAdapter(config={"cli_path": "/opt/homebrew/bin/binance-cli"}, command_runner=runner)

    rows = adapter.open_interest_statistics(symbol="SOLUSDT", period="5m", limit=2)

    assert rows == [{"symbol": "SOLUSDT", "sumOpenInterest": "10", "sumOpenInterestValue": "20", "timestamp": 1783814700000}]
    assert rest_calls[0]["rest_path"] == "openInterestHist"


def test_binance_cli_adapter_reports_cli_and_rest_fallback_failure(monkeypatch):
    def runner(command, *, timeout_seconds):
        return "Request failed after 3 retries\n"

    def fake_rest(**kwargs):
        raise BinanceCLIError("HTTP 418: rate limited")

    monkeypatch.setattr(binance_adapter, "_request_futures_data_json", fake_rest)
    adapter = BinanceCLIAdapter(
        config={"cli_path": "/opt/homebrew/bin/binance-cli"},
        command_runner=runner,
    )

    with pytest.raises(BinanceCLIError, match="Request failed after 3 retries.*REST fallback.*rate limited"):
        adapter.global_account_ratio(symbol="BTCUSDT", period="5m", limit=2)


def test_binance_cli_adapter_fetches_premium_index_klines():
    calls = []

    def runner(command, *, timeout_seconds):
        calls.append({"command": command, "timeout_seconds": timeout_seconds})
        return json.dumps(
            [
                [
                    1783987200000,
                    "-0.00021261",
                    "-0.00021261",
                    "-0.00065238",
                    "-0.00052606",
                    "0",
                    1783987499999,
                    "0",
                    60,
                    "0",
                    "0",
                    "0",
                ]
            ]
        )

    adapter = BinanceCLIAdapter(
        config={"cli_path": "/opt/homebrew/bin/binance-cli", "profile": "research"},
        command_runner=runner,
    )
    rows = adapter.premium_index_klines(
        symbol="BTCUSDT",
        interval="5m",
        limit=3,
        start_time_ms=1783987200000,
        end_time_ms=1783988099999,
    )

    assert rows[0][4] == "-0.00052606"
    assert calls == [
        {
            "command": [
                "/opt/homebrew/bin/binance-cli",
                "futures-usds",
                "premium-index-kline-data",
                "--symbol",
                "BTCUSDT",
                "--interval",
                "5m",
                "--limit",
                "3",
                "--start-time",
                "1783987200000",
                "--end-time",
                "1783988099999",
                "--profile",
                "research",
            ],
            "timeout_seconds": 60,
        }
    ]


def test_binance_cli_adapter_uses_rest_fallback_for_premium_index(monkeypatch):
    def runner(command, *, timeout_seconds):
        return "Request failed after 3 retries\n"

    rest_calls = []

    def fake_rest(**kwargs):
        rest_calls.append(kwargs)
        return [[1783987200000, "-0.1", "-0.1", "-0.2", "-0.15", "0", 1783987499999, "0", 60, "0", "0", "0"]]

    monkeypatch.setattr(binance_adapter, "_request_futures_api_json", fake_rest)
    adapter = BinanceCLIAdapter(config={"cli_path": "/opt/homebrew/bin/binance-cli"}, command_runner=runner)

    rows = adapter.premium_index_klines(
        symbol="ETHUSDT",
        interval="5m",
        limit=3,
        start_time_ms=1783987200000,
        end_time_ms=1783988099999,
    )

    assert rows[0][4] == "-0.15"
    assert rest_calls == [
        {
            "rest_path": "premiumIndexKlines",
            "params": {
                "symbol": "ETHUSDT",
                "interval": "5m",
                "limit": 3,
                "startTime": 1783987200000,
                "endTime": 1783988099999,
            },
            "timeout_seconds": 60,
            "api_key": None,
        }
    ]


def test_binance_rest_transport_uses_curl_and_parses_json(monkeypatch):
    calls = []

    class Completed:
        returncode = 0
        stdout = '[{"timestamp": 1783814700000}]'
        stderr = ""

    monkeypatch.setattr(binance_adapter.shutil, "which", lambda name: "/usr/bin/curl")

    def fake_run(command, **kwargs):
        calls.append((command, kwargs))
        return Completed()

    monkeypatch.setattr(binance_adapter.subprocess, "run", fake_run)

    payload = binance_adapter._request_binance_json(
        base_url="https://fapi.binance.com/futures/data",
        rest_path="openInterestHist",
        params={"symbol": "SOLUSDT", "period": "5m", "limit": 2},
        timeout_seconds=30,
        api_key=None,
    )

    assert payload == [{"timestamp": 1783814700000}]
    assert calls[0][0][-1] == (
        "https://fapi.binance.com/futures/data/openInterestHist?"
        "symbol=SOLUSDT&period=5m&limit=2"
    )
    assert calls[0][1] == {
        "capture_output": True,
        "text": True,
        "check": False,
    }
