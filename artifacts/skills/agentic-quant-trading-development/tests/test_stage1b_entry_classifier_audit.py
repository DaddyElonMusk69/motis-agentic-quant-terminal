from __future__ import annotations

import importlib.util
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "analysis"
    / "stage1b_entry_classifier_audit.py"
)
SPEC = importlib.util.spec_from_file_location("stage1b_entry_classifier_audit", MODULE_PATH)
assert SPEC is not None
audit = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(audit)


def test_interaction_features_count_support_and_resistance() -> None:
    packet = {
        "interactions": [
            {
                "timeframe": "2h",
                "tunnel": "fast",
                "tunnel_upper_limit": "100",
                "tunnel_lower_limit": "90",
                "market_price": "105",
                "distance_pct": "0.01",
            },
            {
                "timeframe": "8h",
                "tunnel": "mid",
                "tunnel_upper_limit": "120",
                "tunnel_lower_limit": "110",
                "market_price": "105",
                "distance_pct": "0.02",
            },
        ]
    }

    features = audit.interaction_features(packet, price=105.0, direction="LONG")

    assert features["interaction_count"] == 2
    assert features["support_like_interactions"] == 1
    assert features["resistance_like_interactions"] == 1
    assert features["net_support_minus_resistance"] == 0
    assert features["min_interaction_distance_pct"] == 1.0


def test_threshold_scan_can_find_enter_filter_that_blocks_fp() -> None:
    rows = [
        {"trade_action": "ENTER", "classification": "TP", "room_pct": 3.0},
        {"trade_action": "ENTER", "classification": "TP", "room_pct": 2.5},
        {"trade_action": "ENTER", "classification": "FP", "room_pct": 0.5},
        {"trade_action": "SKIP", "classification": "FN", "room_pct": 4.0},
    ]

    rules = audit.scan_threshold_rules(rows, ["room_pct"])

    assert rules
    best = rules[0]
    assert best["feature"] == "room_pct"
    assert best["operator"] == ">="
    assert best["blocked_fp"] == 1
    assert best["blocked_tp"] == 0


def test_feature_keys_exclude_ground_truth_leakage() -> None:
    rows = [
        {
            "gt_opposite_max_pct": 4.0,
            "2h_room_long_20_pct": 2.0,
            "trade_action": "ENTER",
            "classification": "TP",
        }
    ]

    keys = [
        key
        for key in rows[0].keys()
        if not key.startswith("gt_")
        and audit.re.search(r"(pos|room|edge|body|distance|support|resistance|last5_net)", key)
    ]

    assert keys == ["2h_room_long_20_pct"]


def test_skip_rescue_scan_counts_rescued_tp_and_added_fp() -> None:
    rows = [
        {"trade_action": "ENTER", "classification": "TP", "room_pct": 1.0},
        {"trade_action": "SKIP", "classification": "FN", "room_pct": 4.0},
        {"trade_action": "SKIP", "classification": "TN", "room_pct": 0.2},
    ]

    rules = audit.scan_skip_rescue_rules(rows, ["room_pct"])

    assert rules
    best = rules[0]
    assert best["rescued_tp"] == 1
    assert best["added_fp"] == 0
