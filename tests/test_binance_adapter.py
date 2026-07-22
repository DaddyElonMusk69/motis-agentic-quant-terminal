from __future__ import annotations

import json

from quant_terminal_worker.adapters.binance import BinanceCLIAdapter


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
