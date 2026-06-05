#!/usr/bin/env python3
"""Extract neutral packet features and join them to Stage 0 and Stage 1A results."""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any


SKIP_NAMES = {
    "index.json",
    "summary.json",
    "latest_scan.json",
    "manifest.json",
    "ground_truth_summary.json",
    "distribution.json",
}


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
    if isinstance(columns, list):
        return [str(column) for column in columns]
    return []


def pct_change(start: float, end: float) -> float:
    if start == 0:
        return 0.0
    return (end - start) / start * 100.0


def candle_float(candle: dict[str, Any], key: str) -> float:
    return float(candle[key])


def completed_candles(packet: dict[str, Any], timeframe: str) -> list[dict[str, Any]]:
    chart = packet.get("charts", {}).get(timeframe, {})
    columns = chart_columns(packet, timeframe)
    return [
        row_to_mapping(row, columns)
        for row in chart.get("completed_candles", [])
    ]


def chart_candles(packet: dict[str, Any], timeframe: str) -> list[dict[str, Any]]:
    chart = packet.get("charts", {}).get(timeframe, {})
    columns = chart_columns(packet, timeframe)
    candles = [
        row_to_mapping(row, columns)
        for row in chart.get("completed_candles", [])
    ]
    forming = chart.get("latest_forming_candle")
    if forming:
        candles.append(row_to_mapping(forming, columns))
    return candles


def recent_position_pct(candles: list[dict[str, Any]], price: float, lookback: int) -> float | None:
    window = candles[-lookback:]
    if not window:
        return None
    high = max(candle_float(candle, "high") for candle in window)
    low = min(candle_float(candle, "low") for candle in window)
    if high == low:
        return None
    return (price - low) / (high - low) * 100.0


def direction_counts(candles: list[dict[str, Any]], lookback: int) -> tuple[int, int]:
    up = 0
    down = 0
    for candle in candles[-lookback:]:
        close = candle_float(candle, "close")
        open_ = candle_float(candle, "open")
        if close > open_:
            up += 1
        elif close < open_:
            down += 1
    return up, down


def high_low_step_counts(candles: list[dict[str, Any]], lookback: int) -> tuple[int, int]:
    window = candles[-lookback:]
    higher_highs = 0
    lower_lows = 0
    for prev, curr in zip(window, window[1:]):
        if candle_float(curr, "high") > candle_float(prev, "high"):
            higher_highs += 1
        if candle_float(curr, "low") < candle_float(prev, "low"):
            lower_lows += 1
    return higher_highs, lower_lows


def max_body_pct(candles: list[dict[str, Any]], lookback: int, side: str) -> float:
    values: list[float] = []
    for candle in candles[-lookback:]:
        open_ = candle_float(candle, "open")
        close = candle_float(candle, "close")
        if side == "bull" and close > open_:
            values.append(pct_change(open_, close))
        elif side == "bear" and close < open_:
            values.append(pct_change(close, open_))
    return max(values) if values else 0.0


def drawdown_from_high(candles: list[dict[str, Any]], price: float, lookback: int) -> float | None:
    window = candles[-lookback:]
    if not window:
        return None
    high = max(candle_float(candle, "high") for candle in window)
    if high == 0:
        return None
    return (high - price) / high * 100.0


def bounce_from_low(candles: list[dict[str, Any]], price: float, lookback: int) -> float | None:
    window = candles[-lookback:]
    if not window:
        return None
    low = min(candle_float(candle, "low") for candle in window)
    if low == 0:
        return None
    return (price - low) / low * 100.0


def forming_body_pct(packet: dict[str, Any], timeframe: str) -> float | None:
    forming = packet.get("charts", {}).get(timeframe, {}).get("latest_forming_candle")
    if not forming:
        return None
    forming_candle = row_to_mapping(forming, chart_columns(packet, timeframe))
    return pct_change(candle_float(forming_candle, "open"), candle_float(forming_candle, "close"))


def interactions_summary(packet: dict[str, Any]) -> str:
    parts: list[str] = []
    raw_interactions = packet.get("interactions", {})
    if isinstance(raw_interactions, list):
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for interaction in raw_interactions:
            if isinstance(interaction, dict):
                grouped[str(interaction.get("timeframe", ""))].append(interaction)
        interaction_items = grouped.items()
    else:
        interaction_items = raw_interactions.items()

    for timeframe, interactions in sorted(interaction_items):
        labels: list[str] = []
        for interaction in interactions:
            label = interaction.get("tunnel") or interaction.get("band") or interaction.get("type")
            labels.append(str(label))
        parts.append(f"{timeframe}:{','.join(labels)}")
    return "|".join(parts)


def reference_price(packet: dict[str, Any], timeframes: list[str]) -> float | None:
    raw_interactions = packet.get("interactions", {})
    if isinstance(raw_interactions, list):
        for interaction in raw_interactions:
            if isinstance(interaction, dict) and interaction.get("market_price") is not None:
                return float(interaction["market_price"])
    else:
        for interactions in raw_interactions.values():
            if interactions and interactions[0].get("market_price") is not None:
                return float(interactions[0]["market_price"])
    for timeframe in timeframes:
        chart = packet.get("charts", {}).get(timeframe, {})
        forming = chart.get("latest_forming_candle")
        if forming:
            forming_candle = row_to_mapping(forming, chart_columns(packet, timeframe))
            if forming_candle.get("close") is not None:
                return float(forming_candle["close"])
    return None


def round_or_blank(value: float | None, digits: int = 4) -> float | str:
    return round(value, digits) if value is not None else ""


def median_or_none(values: list[float]) -> float | None:
    return round(statistics.median(values), 4) if values else None


def load_ground_truth(ground_truth_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for path in sorted(ground_truth_dir.glob("*.json")):
        if path.name in SKIP_NAMES:
            continue
        payload = load_json(path)
        if isinstance(payload, dict):
            records[str(payload.get("signal_id") or path.stem)] = payload
    return records


def load_stage1_scores(score_path: Path | None) -> dict[str, dict[str, Any]]:
    if score_path is None:
        return {}
    payload = load_json(score_path)
    records = payload.get("records", []) if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        return {}
    return {
        str(record["signal_id"]): record
        for record in records
        if isinstance(record, dict) and record.get("signal_id")
    }


def add_timeframe_features(
    row: dict[str, Any],
    packet: dict[str, Any],
    timeframe: str,
    price: float,
    lookback: int,
    prefix: str,
) -> None:
    completed = completed_candles(packet, timeframe)
    all_candles = chart_candles(packet, timeframe)
    window = completed[-lookback:]
    up_count, down_count = direction_counts(completed, min(5, lookback))
    higher_highs, lower_lows = high_low_step_counts(completed, lookback)

    row[f"{prefix}_dir{lookback}_pct"] = (
        round(pct_change(candle_float(window[0], "close"), candle_float(window[-1], "close")), 4)
        if len(window) >= 2
        else ""
    )
    row[f"{prefix}_pos{lookback}_pct"] = round_or_blank(
        recent_position_pct(completed, price, lookback)
    )
    row[f"{prefix}_hh{lookback}"] = higher_highs
    row[f"{prefix}_ll{lookback}"] = lower_lows
    row[f"{prefix}_bias"] = (
        "bullish" if higher_highs > lower_lows else "bearish" if lower_lows > higher_highs else "flat"
    )
    row[f"{prefix}_last5_label"] = f"{up_count}up/{down_count}down"
    row[f"{prefix}_mixed_last5"] = up_count in (2, 3) and down_count in (2, 3)
    row[f"{prefix}_decisive_bear_last5"] = down_count >= 4
    row[f"{prefix}_decisive_bull_last5"] = up_count >= 4
    row[f"{prefix}_forming_body_pct"] = round_or_blank(forming_body_pct(packet, timeframe))
    row[f"{prefix}_max_bear_body10_pct"] = round(max_body_pct(completed, 10, "bear"), 4)
    row[f"{prefix}_max_bull_body10_pct"] = round(max_body_pct(completed, 10, "bull"), 4)
    row[f"{prefix}_drawdown{lookback}_pct"] = round_or_blank(
        drawdown_from_high(all_candles, price, lookback)
    )
    row[f"{prefix}_bounce{lookback}_pct"] = round_or_blank(
        bounce_from_low(all_candles, price, lookback)
    )


def build_row(
    packet_path: Path,
    ground_truth: dict[str, dict[str, Any]],
    stage1_scores: dict[str, dict[str, Any]],
    primary_tf: str,
    anchor_tf: str,
    lookback: int,
) -> dict[str, Any]:
    packet = load_json(packet_path)
    signal_id = str(packet.get("signal_id") or packet_path.stem)
    timeframes = list(packet.get("charts", {}).keys()) or [anchor_tf, primary_tf]
    price = reference_price(packet, timeframes)
    if price is None:
        raise ValueError(f"No reference price for {signal_id}")

    gt = ground_truth.get(signal_id, {})
    score = stage1_scores.get(signal_id, {})
    row: dict[str, Any] = {
        "signal_id": signal_id,
        "timestamp": packet.get("timestamp", ""),
        "active_timeframes": ",".join(packet.get("active_timeframes", [])),
        "interactions": interactions_summary(packet),
        "reference_price": round(price, 6),
        "natural_direction": gt.get("natural_direction"),
        "gt_status": gt.get("status", ""),
        "gt_first_move_pct": gt.get("first_move_pct", ""),
        "gt_max_travel_pct": gt.get("max_travel_pct", ""),
        "gt_opposite_max_pct": gt.get("opposite_max_pct", ""),
        "gt_reversed": gt.get("reversed", ""),
        "agent_direction": score.get("agent_direction", ""),
        "agreement": score.get("agreement", ""),
        "score_status": score.get("status", ""),
        "confidence": score.get("confidence", ""),
        "regime": score.get("regime", ""),
    }

    add_timeframe_features(row, packet, primary_tf, price, lookback, primary_tf.replace("-", "_"))
    add_timeframe_features(row, packet, anchor_tf, price, lookback, anchor_tf.replace("-", "_"))

    primary_bias = row[f"{primary_tf.replace('-', '_')}_bias"]
    anchor_prefix = anchor_tf.replace("-", "_")
    row["primary_bull_anchor_mixed"] = (
        primary_bias == "bullish" and bool(row[f"{anchor_prefix}_mixed_last5"])
    )
    row["primary_bear_anchor_mixed"] = (
        primary_bias == "bearish" and bool(row[f"{anchor_prefix}_mixed_last5"])
    )
    row["primary_anchor_conflict"] = (
        primary_bias == "bullish" and bool(row[f"{anchor_prefix}_decisive_bear_last5"])
    ) or (
        primary_bias == "bearish" and bool(row[f"{anchor_prefix}_decisive_bull_last5"])
    )
    return row


def summarize_group(rows: list[dict[str, Any]], numeric_keys: list[str]) -> dict[str, Any]:
    summary: dict[str, Any] = {"count": len(rows)}
    directions = sorted({str(row.get("natural_direction")) for row in rows})
    agreements = sorted({str(row.get("agreement")) for row in rows if row.get("agreement")})
    for label, key, values in [
        ("direction", "natural_direction", directions),
        ("agreement", "agreement", agreements),
    ]:
        for value in values:
            subset = [row for row in rows if str(row.get(key)) == value]
            item: dict[str, Any] = {"count": len(subset)}
            for numeric_key in numeric_keys:
                vals = [float(row[numeric_key]) for row in subset if row.get(numeric_key) not in ("", None)]
                item[f"median_{numeric_key}"] = median_or_none(vals)
            summary[f"{label}:{value}"] = item
    return summary


def boolean_clusters(rows: list[dict[str, Any]], numeric_keys: list[str]) -> dict[str, Any]:
    bool_keys = [
        key
        for key in rows[0].keys()
        if rows and isinstance(rows[0].get(key), bool)
    ] if rows else []
    clusters: dict[str, Any] = {}
    for key in bool_keys:
        subset = [row for row in rows if row.get(key) is True]
        if subset:
            clusters[key] = summarize_group(subset, numeric_keys)
    return clusters


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys()) if rows else []
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build neutral feature audit tables for directional failure analysis."
    )
    parser.add_argument("--signal-dir", required=True, type=Path)
    parser.add_argument("--ground-truth-dir", required=True, type=Path)
    parser.add_argument("--stage1-score", type=Path, help="Optional Stage 1A score JSON")
    parser.add_argument("--primary-tf", default="1d")
    parser.add_argument("--anchor-tf", default="2h")
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ground_truth = load_ground_truth(args.ground_truth_dir)
    stage1_scores = load_stage1_scores(args.stage1_score)
    rows = [
        build_row(
            packet_path=packet_path,
            ground_truth=ground_truth,
            stage1_scores=stage1_scores,
            primary_tf=args.primary_tf,
            anchor_tf=args.anchor_tf,
            lookback=args.lookback,
        )
        for packet_path in sorted(args.signal_dir.glob("*.json"))
        if packet_path.name not in SKIP_NAMES
    ]

    numeric_keys = [
        key
        for key in rows[0].keys()
        if rows and key.endswith("_pct") and key not in {"reference_price"}
    ] if rows else []
    write_csv(args.out_csv, rows)

    mismatches = [row for row in rows if row.get("agreement") == "MISMATCH"]
    matches = [row for row in rows if row.get("agreement") == "MATCH"]
    by_direction = defaultdict(int)
    by_agreement = defaultdict(int)
    for row in rows:
        by_direction[str(row.get("natural_direction"))] += 1
        if row.get("agreement"):
            by_agreement[str(row.get("agreement"))] += 1

    summary = {
        "total_rows": len(rows),
        "primary_tf": args.primary_tf,
        "anchor_tf": args.anchor_tf,
        "lookback": args.lookback,
        "direction_counts": dict(sorted(by_direction.items())),
        "agreement_counts": dict(sorted(by_agreement.items())),
        "all_rows": summarize_group(rows, numeric_keys),
        "matches": summarize_group(matches, numeric_keys),
        "mismatches": summarize_group(mismatches, numeric_keys),
        "boolean_clusters": boolean_clusters(rows, numeric_keys),
        "mismatch_examples": [
            {
                "signal_id": row["signal_id"],
                "natural_direction": row["natural_direction"],
                "agent_direction": row.get("agent_direction"),
                "regime": row.get("regime"),
                "interactions": row["interactions"],
            }
            for row in mismatches[:50]
        ],
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({
        "csv": str(args.out_csv),
        "summary": str(args.out_json),
        "total_rows": len(rows),
        "mismatches": len(mismatches),
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
