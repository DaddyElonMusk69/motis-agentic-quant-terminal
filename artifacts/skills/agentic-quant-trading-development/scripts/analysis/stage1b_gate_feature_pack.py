#!/usr/bin/env python3
"""Build packet-observable Stage 1B gate features for evaluator handoffs."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def row_to_mapping(row: Any, columns: list[str]) -> dict[str, Any]:
    if isinstance(row, dict):
        return row
    if isinstance(row, list):
        return dict(zip(columns, row))
    raise TypeError(f"Unsupported candle row type: {type(row).__name__}")


def chart_columns(packet: dict[str, Any], timeframe: str) -> list[str]:
    chart = packet.get("charts", {}).get(timeframe, {})
    columns = chart.get("columns")
    return [str(column) for column in columns] if isinstance(columns, list) else []


def chart_candles(packet: dict[str, Any], timeframe: str, include_forming: bool = True) -> list[dict[str, Any]]:
    chart = packet.get("charts", {}).get(timeframe, {})
    columns = chart_columns(packet, timeframe)
    candles = [row_to_mapping(row, columns) for row in chart.get("completed_candles", [])]
    forming = chart.get("latest_forming_candle")
    if include_forming and forming:
        candles.append(row_to_mapping(forming, columns))
    return candles


def candle_float(candle: dict[str, Any], key: str) -> float:
    return float(candle[key])


def pct_change(start: float, end: float) -> float:
    return 0.0 if start == 0 else (end - start) / start * 100.0


def reference_price(packet: dict[str, Any], timeframes: list[str]) -> float:
    interactions = packet.get("interactions", {})
    records: list[dict[str, Any]] = []
    if isinstance(interactions, list):
        records = [item for item in interactions if isinstance(item, dict)]
    elif isinstance(interactions, dict):
        for value in interactions.values():
            records.extend(item for item in value if isinstance(item, dict))
    for record in records:
        if record.get("market_price") is not None:
            return float(record["market_price"])
    for timeframe in timeframes:
        candles = chart_candles(packet, timeframe, include_forming=True)
        if candles:
            return candle_float(candles[-1], "close")
    raise ValueError("Packet has no usable reference price")


def recent_range(candles: list[dict[str, Any]], lookback: int) -> tuple[float, float]:
    window = candles[-lookback:]
    if not window:
        raise ValueError("No candles for recent range")
    return (
        min(candle_float(candle, "low") for candle in window),
        max(candle_float(candle, "high") for candle in window),
    )


def direction_counts(candles: list[dict[str, Any]], lookback: int) -> tuple[int, int]:
    up = 0
    down = 0
    for candle in candles[-lookback:]:
        open_ = candle_float(candle, "open")
        close = candle_float(candle, "close")
        if close > open_:
            up += 1
        elif close < open_:
            down += 1
    return up, down


def body_pct(candle: dict[str, Any]) -> float:
    return pct_change(candle_float(candle, "open"), candle_float(candle, "close"))


def max_body_for_direction(candles: list[dict[str, Any]], lookback: int, direction: str) -> float:
    values: list[float] = []
    for candle in candles[-lookback:]:
        value = body_pct(candle)
        if direction == "LONG" and value > 0:
            values.append(value)
        elif direction == "SHORT" and value < 0:
            values.append(abs(value))
    return max(values) if values else 0.0


def normalized_interactions(packet: dict[str, Any]) -> list[dict[str, Any]]:
    interactions = packet.get("interactions", {})
    if isinstance(interactions, list):
        return [item for item in interactions if isinstance(item, dict)]
    if isinstance(interactions, dict):
        flattened: list[dict[str, Any]] = []
        for timeframe, records in interactions.items():
            for record in records:
                if isinstance(record, dict):
                    item = dict(record)
                    item.setdefault("timeframe", timeframe)
                    flattened.append(item)
        return flattened
    return []


def interaction_summary(packet: dict[str, Any], price: float) -> dict[str, Any]:
    support_like = 0
    resistance_like = 0
    distances: list[float] = []
    labels: list[str] = []
    for interaction in normalized_interactions(packet):
        timeframe = str(interaction.get("timeframe", ""))
        tunnel = str(interaction.get("tunnel") or interaction.get("band") or interaction.get("type") or "")
        labels.append(f"{timeframe}:{tunnel}")
        if interaction.get("distance_pct") is not None:
            distances.append(abs(float(interaction["distance_pct"]) * 100.0))
        lower = interaction.get("tunnel_lower_limit")
        upper = interaction.get("tunnel_upper_limit")
        if lower is None or upper is None:
            continue
        lower_f = float(lower)
        upper_f = float(upper)
        midpoint = (lower_f + upper_f) / 2.0
        if price >= upper_f:
            support_like += 1
        elif price <= lower_f:
            resistance_like += 1
        elif price >= midpoint:
            support_like += 1
        else:
            resistance_like += 1
    return {
        "interaction_count": len(labels),
        "interaction_labels": labels,
        "min_interaction_distance_pct": round(min(distances), 6) if distances else None,
        "support_like_interactions": support_like,
        "resistance_like_interactions": resistance_like,
        "net_support_minus_resistance": support_like - resistance_like,
    }


def signal_id_from_path(path: str) -> str:
    return Path(path).stem


def packet_feature(packet_path: Path, lookback: int) -> dict[str, Any]:
    packet = load_json(packet_path)
    signal_id = signal_id_from_path(packet_path.name)
    price = reference_price(packet, ["2h", "1d"])
    completed_2h = chart_candles(packet, "2h", include_forming=False)
    completed_1d = chart_candles(packet, "1d", include_forming=False)
    forming_2h = chart_candles(packet, "2h", include_forming=True)[-1]
    low_2h, high_2h = recent_range(completed_2h, lookback)
    low_1d, high_1d = recent_range(completed_1d, lookback)
    up5, down5 = direction_counts(completed_2h, 5)
    range_2h = high_2h - low_2h
    range_1d = high_1d - low_1d
    from_lower = max(0.0, (price - low_2h) / price * 100.0)
    from_upper = max(0.0, (high_2h - price) / price * 100.0)
    midpoint_2h = (low_2h + high_2h) / 2.0
    return {
        "signal_id": signal_id,
        "packet_path": packet_path.as_posix(),
        "reference_price": round(price, 6),
        "lookback_completed_2h": lookback,
        "two_h_range_low": round(low_2h, 6),
        "two_h_range_high": round(high_2h, 6),
        "two_h_range_midpoint": round(midpoint_2h, 6),
        "two_h_position_pct": None if range_2h == 0 else round((price - low_2h) / range_2h * 100.0, 6),
        "two_h_room_long_pct": round(max(0.0, (high_2h - price) / price * 100.0), 6),
        "two_h_room_short_pct": round(max(0.0, (price - low_2h) / price * 100.0), 6),
        "two_h_from_lower_edge_pct": round(from_lower, 6),
        "two_h_from_upper_edge_pct": round(from_upper, 6),
        "two_h_near_lower_edge_1p2": from_lower <= 1.2,
        "two_h_near_upper_edge_1p2": from_upper <= 1.2,
        "two_h_last5_up": up5,
        "two_h_last5_down": down5,
        "two_h_last5_net_long": up5 - down5,
        "two_h_last5_net_short": down5 - up5,
        "two_h_late_chase_long": up5 - down5 > 3,
        "two_h_late_chase_short": down5 - up5 > 3,
        "two_h_forming_body_pct": round(body_pct(forming_2h), 6),
        "two_h_max_up_body10_pct": round(max_body_for_direction(completed_2h, 10, "LONG"), 6),
        "two_h_max_down_body10_pct": round(max_body_for_direction(completed_2h, 10, "SHORT"), 6),
        "one_d_position_pct": None if range_1d == 0 else round((price - low_1d) / range_1d * 100.0, 6),
        **interaction_summary(packet, price),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Stage 1B deterministic gate features for a signal sample.")
    parser.add_argument("--sample", required=True, type=Path, help="signal_sample.json")
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--lookback", type=int, default=20)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sample = load_json(args.sample)
    packet_paths = [Path(path) for path in sample.get("packet_paths", [])]
    features = [packet_feature(path, args.lookback) for path in packet_paths]
    output = {
        "schema_version": "stage1b_gate_features.v1",
        "source_sample": args.sample.as_posix(),
        "lookback_completed_2h": args.lookback,
        "feature_notes": {
            "two_h_late_chase_long": "True when the last five completed 2h candles strongly align LONG.",
            "two_h_late_chase_short": "True when the last five completed 2h candles strongly align SHORT.",
            "two_h_near_lower_edge_1p2": "True when price is within 1.2% of the lower edge of the recent completed 2h range.",
        },
        "features": features,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(output, indent=2) + "\n")
    print(json.dumps({"features": len(features), "out": args.out.as_posix()}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
