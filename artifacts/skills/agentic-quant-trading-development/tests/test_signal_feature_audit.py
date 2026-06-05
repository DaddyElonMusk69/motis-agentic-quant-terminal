from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "analysis"
    / "signal_feature_audit.py"
)
SPEC = importlib.util.spec_from_file_location("signal_feature_audit", MODULE_PATH)
assert SPEC is not None
signal_feature_audit = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(signal_feature_audit)


def test_v2_row_candles_are_decoded_for_feature_extraction() -> None:
    packet = {
        "schema_version": "signal_packet.v2",
        "charts": {
            "2h": {
                "columns": [
                    "timestamp",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vol_ccy",
                    "vol_ccy_quote",
                    "confirm",
                ],
                "completed_candles": [
                    ["2026-01-01T00:00:00Z", "100", "110", "90", "105", "1", "0", "0", 1],
                    ["2026-01-01T02:00:00Z", "105", "115", "95", "112", "1", "0", "0", 1],
                ],
                "latest_forming_candle": [
                    "2026-01-01T04:00:00Z",
                    "112",
                    "118",
                    "108",
                    "116",
                    "1",
                    "0",
                    "0",
                    0,
                ],
            }
        },
    }

    completed = signal_feature_audit.completed_candles(packet, "2h")
    candles = signal_feature_audit.chart_candles(packet, "2h")

    assert completed[0]["open"] == "100"
    assert completed[1]["close"] == "112"
    assert candles[-1]["confirm"] == 0
    assert round(signal_feature_audit.forming_body_pct(packet, "2h"), 4) == 3.5714


def test_v2_flat_interactions_provide_reference_price_and_summary() -> None:
    packet = {
        "schema_version": "signal_packet.v2",
        "interactions": [
            {
                "timeframe": "2h",
                "tunnel": "fast",
                "type": "touch",
                "market_price": "123.45",
            },
            {
                "timeframe": "1d",
                "band": "slow",
                "type": "above",
                "market_price": "123.45",
            },
        ],
        "charts": {},
    }

    assert signal_feature_audit.reference_price(packet, ["2h"]) == 123.45
    assert signal_feature_audit.interactions_summary(packet) == "1d:slow|2h:fast"
