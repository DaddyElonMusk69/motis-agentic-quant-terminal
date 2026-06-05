from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "analysis"
    / "stage1b_gate_feature_pack.py"
)
SPEC = importlib.util.spec_from_file_location("stage1b_gate_feature_pack", MODULE_PATH)
assert SPEC is not None
gate_pack = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(gate_pack)


def test_interaction_summary_counts_support_and_resistance() -> None:
    packet = {
        "interactions": [
            {
                "timeframe": "2h",
                "tunnel": "fast",
                "tunnel_upper_limit": "100",
                "tunnel_lower_limit": "90",
                "distance_pct": "0.01",
            },
            {
                "timeframe": "8h",
                "tunnel": "mid",
                "tunnel_upper_limit": "120",
                "tunnel_lower_limit": "110",
                "distance_pct": "0.02",
            },
        ]
    }

    summary = gate_pack.interaction_summary(packet, price=105.0)

    assert summary["support_like_interactions"] == 1
    assert summary["resistance_like_interactions"] == 1
    assert summary["net_support_minus_resistance"] == 0
    assert summary["min_interaction_distance_pct"] == 1.0


def test_direction_counts_and_chase_math() -> None:
    candles = [
        {"open": "100", "close": "101"},
        {"open": "101", "close": "102"},
        {"open": "102", "close": "103"},
        {"open": "103", "close": "104"},
        {"open": "104", "close": "105"},
    ]

    up, down = gate_pack.direction_counts(candles, 5)

    assert up == 5
    assert down == 0
    assert up - down > 3
