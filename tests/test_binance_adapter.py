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
