import json
from pathlib import Path

import pytest

from quant_terminal_worker.adapters.okx import OKXAdapter, OKXCLIError, SwapOrderRequest


def test_okx_adapter_reports_missing_live_credentials():
    adapter = OKXAdapter(config={"backend": "env_credentials"})

    assert adapter.adapter_id == "okx"
    assert adapter.readiness_blockers() == [
        "missing_okx_api_key",
        "missing_okx_api_secret",
        "missing_okx_passphrase",
    ]


def test_okx_adapter_is_ready_with_required_credentials():
    adapter = OKXAdapter(
        config={
            "backend": "env_credentials",
            "api_key": "key",
            "api_secret": "secret",
            "passphrase": "passphrase",
        }
    )

    assert adapter.readiness_blockers() == []


def test_okx_cli_adapter_builds_market_candles_command():
    adapter = OKXAdapter(
        config={
            "backend": "okx_cli",
            "cli_path": "/opt/homebrew/bin/okx",
            "profile": "motis",
            "mode": "live",
        }
    )

    command = adapter.build_command("market", "candles", ["BTC-USDT-SWAP", "--bar", "5m"])

    assert command == [
        "/opt/homebrew/bin/okx",
        "--profile",
        "motis",
        "--live",
        "--json",
        "market",
        "candles",
        "BTC-USDT-SWAP",
        "--bar",
        "5m",
    ]


def test_okx_cli_adapter_runs_json_command(tmp_path: Path):
    cli = tmp_path / "okx"
    cli.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "print(json.dumps({'argv': __import__('sys').argv[1:], 'data': [{'close': '100'}]}))",
            ]
        )
    )
    cli.chmod(0o755)
    adapter = OKXAdapter(config={"backend": "okx_cli", "cli_path": str(cli), "mode": "demo"})

    result = adapter.market_candles("BTC-USDT-SWAP", bar="5m", limit=2)

    assert result["data"] == [{"close": "100"}]
    assert result["argv"] == [
        "--demo",
        "--json",
        "market",
        "candles",
        "BTC-USDT-SWAP",
        "--bar",
        "5m",
        "--limit",
        "2",
    ]


def test_okx_cli_adapter_wraps_market_candle_array_output(tmp_path: Path):
    cli = tmp_path / "okx"
    cli.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "print(json.dumps([['1780272000000', '100', '105', '99', '101', '12.5']]))",
            ]
        )
    )
    cli.chmod(0o755)
    adapter = OKXAdapter(config={"backend": "okx_cli", "cli_path": str(cli), "mode": "demo"})

    result = adapter.market_candles("BTC-USDT-SWAP", bar="5m", limit=2)

    assert result == {
        "code": "0",
        "data": [["1780272000000", "100", "105", "99", "101", "12.5"]],
    }


def test_okx_cli_adapter_raises_on_nonzero_exit(tmp_path: Path):
    cli = tmp_path / "okx"
    cli.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import sys",
                "sys.stderr.write('order rejected')",
                "raise SystemExit(2)",
            ]
        )
    )
    cli.chmod(0o755)
    adapter = OKXAdapter(config={"backend": "okx_cli", "cli_path": str(cli), "mode": "live"})

    with pytest.raises(OKXCLIError, match="order rejected"):
        adapter.place_swap_order(
            SwapOrderRequest(
                inst_id="BTC-USDT-SWAP",
                side="buy",
                order_type="market",
                size="1",
                trade_mode="cross",
                client_order_id="route-decision-1",
            )
        )


def test_okx_cli_adapter_places_swap_order_with_client_order_id(tmp_path: Path):
    cli = tmp_path / "okx"
    cli.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, sys",
                "print(json.dumps({'argv': sys.argv[1:], 'ordId': '123'}))",
            ]
        )
    )
    cli.chmod(0o755)
    adapter = OKXAdapter(config={"backend": "okx_cli", "cli_path": str(cli), "mode": "live"})

    result = adapter.place_swap_order(
        SwapOrderRequest(
            inst_id="BTC-USDT-SWAP",
            side="buy",
            order_type="market",
            size="1",
            trade_mode="cross",
            client_order_id="route-decision-1",
            position_side="long",
        )
    )

    assert result["ordId"] == "123"
    assert result["argv"] == [
        "--live",
        "--json",
        "swap",
        "place",
        "--instId",
        "BTC-USDT-SWAP",
        "--side",
        "buy",
        "--ordType",
        "market",
        "--sz",
        "1",
        "--tdMode",
        "cross",
        "--clOrdId",
        "route-decision-1",
        "--posSide",
        "long",
    ]


def test_okx_cli_adapter_rejects_non_json_output(tmp_path: Path):
    cli = tmp_path / "okx"
    cli.write_text("#!/usr/bin/env python3\nprint('not json')\n")
    cli.chmod(0o755)
    adapter = OKXAdapter(config={"backend": "okx_cli", "cli_path": str(cli), "mode": "demo"})

    with pytest.raises(OKXCLIError, match="non-JSON"):
        adapter.market_candles("BTC-USDT-SWAP", bar="5m", limit=2)
