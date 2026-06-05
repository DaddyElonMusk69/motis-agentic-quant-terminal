#!/usr/bin/env python3
"""Deterministic Stage 1B entry-gate feature audit and simple rule scan."""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
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
    return [str(column) for column in columns] if isinstance(columns, list) else []


def chart_candles(packet: dict[str, Any], timeframe: str, include_forming: bool = True) -> list[dict[str, Any]]:
    chart = packet.get("charts", {}).get(timeframe, {})
    columns = chart_columns(packet, timeframe)
    candles = [
        row_to_mapping(row, columns)
        for row in chart.get("completed_candles", [])
    ]
    forming = chart.get("latest_forming_candle")
    if include_forming and forming:
        candles.append(row_to_mapping(forming, columns))
    return candles


def candle_float(candle: dict[str, Any], key: str) -> float:
    return float(candle[key])


def pct_change(start: float, end: float) -> float:
    return 0.0 if start == 0 else (end - start) / start * 100.0


def reference_price(packet: dict[str, Any], timeframes: list[str]) -> float | None:
    interactions = packet.get("interactions", {})
    if isinstance(interactions, list):
        for interaction in interactions:
            if isinstance(interaction, dict) and interaction.get("market_price") is not None:
                return float(interaction["market_price"])
    elif isinstance(interactions, dict):
        for records in interactions.values():
            if records and records[0].get("market_price") is not None:
                return float(records[0]["market_price"])
    for timeframe in timeframes:
        candles = chart_candles(packet, timeframe, include_forming=True)
        if candles:
            return candle_float(candles[-1], "close")
    return None


def recent_range(candles: list[dict[str, Any]], lookback: int) -> tuple[float, float] | None:
    window = candles[-lookback:]
    if not window:
        return None
    return (
        min(candle_float(candle, "low") for candle in window),
        max(candle_float(candle, "high") for candle in window),
    )


def range_position_pct(candles: list[dict[str, Any]], price: float, lookback: int) -> float | None:
    bounds = recent_range(candles, lookback)
    if not bounds:
        return None
    low, high = bounds
    return None if high == low else (price - low) / (high - low) * 100.0


def room_to_range_edge_pct(candles: list[dict[str, Any]], price: float, lookback: int, direction: str) -> float | None:
    bounds = recent_range(candles, lookback)
    if not bounds or price == 0:
        return None
    low, high = bounds
    if direction == "LONG":
        return max(0.0, (high - price) / price * 100.0)
    return max(0.0, (price - low) / price * 100.0)


def distance_from_range_edge_pct(candles: list[dict[str, Any]], price: float, lookback: int, direction: str) -> float | None:
    bounds = recent_range(candles, lookback)
    if not bounds or price == 0:
        return None
    low, high = bounds
    if direction == "LONG":
        return max(0.0, (price - low) / price * 100.0)
    return max(0.0, (high - price) / price * 100.0)


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


def abs_body_pct(candle: dict[str, Any]) -> float:
    return abs(body_pct(candle))


def max_body_pct(candles: list[dict[str, Any]], lookback: int, direction: str) -> float:
    values: list[float] = []
    for candle in candles[-lookback:]:
        value = body_pct(candle)
        if direction == "LONG" and value > 0:
            values.append(value)
        elif direction == "SHORT" and value < 0:
            values.append(abs(value))
    return max(values) if values else 0.0


def latest_forming_body_pct(packet: dict[str, Any], timeframe: str) -> float | None:
    chart = packet.get("charts", {}).get(timeframe, {})
    forming = chart.get("latest_forming_candle")
    if not forming:
        return None
    return body_pct(row_to_mapping(forming, chart_columns(packet, timeframe)))


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


def interaction_features(packet: dict[str, Any], price: float, direction: str) -> dict[str, Any]:
    interactions = normalized_interactions(packet)
    distances: list[float] = []
    support_like = 0
    resistance_like = 0
    labels: list[str] = []
    for interaction in interactions:
        timeframe = str(interaction.get("timeframe", ""))
        tunnel = str(interaction.get("tunnel") or interaction.get("band") or interaction.get("type") or "")
        labels.append(f"{timeframe}:{tunnel}")
        if interaction.get("distance_pct") is not None:
            distances.append(abs(float(interaction["distance_pct"]) * 100.0))
        lower = interaction.get("tunnel_lower_limit")
        upper = interaction.get("tunnel_upper_limit")
        if lower is not None and upper is not None:
            lower_f = float(lower)
            upper_f = float(upper)
            mid = (lower_f + upper_f) / 2.0
            if price >= upper_f:
                support_like += 1
            elif price <= lower_f:
                resistance_like += 1
            elif direction == "LONG" and price >= mid:
                support_like += 1
            elif direction == "SHORT" and price <= mid:
                resistance_like += 1
    return {
        "interaction_count": len(interactions),
        "interaction_labels": "|".join(labels),
        "min_interaction_distance_pct": min(distances) if distances else None,
        "support_like_interactions": support_like,
        "resistance_like_interactions": resistance_like,
        "net_support_minus_resistance": support_like - resistance_like,
    }


def load_ground_truth(ground_truth_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for path in sorted(ground_truth_dir.glob("*.json")):
        if path.name in SKIP_NAMES:
            continue
        payload = load_json(path)
        records[str(payload.get("signal_id") or path.stem)] = payload
    return records


def load_scores(score_paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in score_paths:
        payload = load_json(path)
        session_id = str(payload.get("session_id") or path.parents[3].name)
        iteration_id = str(payload.get("iteration_id") or path.parents[1].name)
        strategy_version = str(payload.get("strategy_version") or "")
        for record in payload.get("per_signal", []):
            item = dict(record)
            item["source_score_path"] = path.as_posix()
            item["session_id"] = session_id
            item["iteration_id"] = iteration_id
            item["strategy_version"] = strategy_version
            rows.append(item)
    return rows


def gt_triggered(gt: dict[str, Any]) -> bool:
    return gt.get("status") != "no_trigger" and gt.get("natural_direction") in {"LONG", "SHORT"}


def build_feature_row(
    score_row: dict[str, Any],
    packet_dir: Path,
    ground_truth: dict[str, dict[str, Any]],
    primary_tf: str,
    anchor_tf: str,
    lookback: int,
) -> dict[str, Any]:
    signal_id = str(score_row["signal_id"])
    packet = load_json(packet_dir / f"{signal_id}.json")
    price = reference_price(packet, [anchor_tf, primary_tf])
    if price is None:
        raise ValueError(f"No reference price for {signal_id}")
    direction = str(score_row["direction"]).upper()
    gt = ground_truth.get(signal_id, {})
    triggered = gt_triggered(gt)

    row: dict[str, Any] = {
        "session_id": score_row["session_id"],
        "iteration_id": score_row["iteration_id"],
        "strategy_version": score_row["strategy_version"],
        "signal_id": signal_id,
        "timestamp": packet.get("timestamp", ""),
        "gate_reason_code": score_row.get("gate_reason_code", ""),
        "trade_action": score_row.get("trade_action", ""),
        "direction": direction,
        "classification": score_row.get("classification", ""),
        "is_enter": score_row.get("trade_action") == "ENTER",
        "is_tp": score_row.get("classification") == "TP",
        "is_fp": score_row.get("classification") == "FP",
        "is_fn": score_row.get("classification") == "FN",
        "is_tn": score_row.get("classification") == "TN",
        "gt_trigger": triggered,
        "gt_direction": gt.get("natural_direction") if triggered else "",
        "gt_status": gt.get("status", ""),
        "gt_max_travel_pct": gt.get("max_travel_pct", ""),
        "gt_opposite_max_pct": gt.get("opposite_max_pct", ""),
        "reference_price": round(price, 6),
        "active_timeframes": ",".join(packet.get("active_timeframes", [])),
        "reasoning": score_row.get("reasoning", ""),
    }

    for timeframe in [primary_tf, anchor_tf]:
        prefix = timeframe.replace("-", "_")
        completed = chart_candles(packet, timeframe, include_forming=False)
        all_candles = chart_candles(packet, timeframe, include_forming=True)
        up5, down5 = direction_counts(completed, 5)
        row[f"{prefix}_pos{lookback}_pct"] = range_position_pct(completed, price, lookback)
        row[f"{prefix}_room_{direction.lower()}_{lookback}_pct"] = room_to_range_edge_pct(
            completed, price, lookback, direction
        )
        row[f"{prefix}_from_opposite_edge_{direction.lower()}_{lookback}_pct"] = distance_from_range_edge_pct(
            completed, price, lookback, direction
        )
        row[f"{prefix}_last5_up"] = up5
        row[f"{prefix}_last5_down"] = down5
        row[f"{prefix}_last5_net_for_direction"] = (up5 - down5) if direction == "LONG" else (down5 - up5)
        row[f"{prefix}_forming_body_pct"] = latest_forming_body_pct(packet, timeframe)
        row[f"{prefix}_forming_body_for_direction_pct"] = (
            row[f"{prefix}_forming_body_pct"]
            if direction == "LONG"
            else -row[f"{prefix}_forming_body_pct"]
            if row[f"{prefix}_forming_body_pct"] is not None
            else None
        )
        row[f"{prefix}_max_body_for_direction10_pct"] = max_body_pct(completed, 10, direction)
        if all_candles:
            row[f"{prefix}_latest_abs_body_pct"] = abs_body_pct(all_candles[-1])

    row.update(interaction_features(packet, price, direction))
    return row


def numeric_values(rows: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        value = row.get(key)
        if isinstance(value, bool) or value in ("", None):
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(number):
            values.append(number)
    return values


def metric(tp: int, fp: int, fn: int) -> dict[str, Any]:
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "precision_pct": round(precision * 100, 2),
        "recall_pct": round(recall * 100, 2),
        "f1": round(f1, 4),
        "passed_gate": precision >= 0.70 and recall >= 0.50,
    }


def score_subset(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tp = sum(1 for row in rows if row.get("classification") == "TP")
    fp = sum(1 for row in rows if row.get("classification") == "FP")
    fn = sum(1 for row in rows if row.get("classification") == "FN")
    tn = sum(1 for row in rows if row.get("classification") == "TN")
    out = metric(tp, fp, fn)
    out["tn"] = tn
    out["total"] = len(rows)
    return out


def scan_threshold_rules(rows: list[dict[str, Any]], feature_keys: list[str]) -> list[dict[str, Any]]:
    enter_rows = [row for row in rows if row.get("trade_action") == "ENTER"]
    total_fn = sum(1 for row in rows if row.get("classification") == "FN")
    rules: list[dict[str, Any]] = []
    for feature in feature_keys:
        values = sorted(set(round(value, 6) for value in numeric_values(enter_rows, feature)))
        if len(values) < 2:
            continue
        thresholds = values
        for op in [">=", "<="]:
            for threshold in thresholds:
                kept: list[dict[str, Any]] = []
                blocked_tp = 0
                blocked_fp = 0
                for row in enter_rows:
                    value = row.get(feature)
                    if value in ("", None):
                        keep = False
                    else:
                        keep = float(value) >= threshold if op == ">=" else float(value) <= threshold
                    if keep:
                        kept.append(row)
                    elif row.get("classification") == "TP":
                        blocked_tp += 1
                    elif row.get("classification") == "FP":
                        blocked_fp += 1
                tp = sum(1 for row in kept if row.get("classification") == "TP")
                fp = sum(1 for row in kept if row.get("classification") == "FP")
                out = metric(tp, fp, total_fn + blocked_tp)
                out.update(
                    {
                        "feature": feature,
                        "operator": op,
                        "threshold": threshold,
                        "kept_enters": len(kept),
                        "blocked_tp": blocked_tp,
                        "blocked_fp": blocked_fp,
                        "scope": "entered_only",
                    }
                )
                if blocked_fp or out["passed_gate"]:
                    rules.append(out)
    return sorted(rules, key=lambda item: (item["passed_gate"], item["f1"], item["precision_pct"]), reverse=True)


def scan_skip_rescue_rules(rows: list[dict[str, Any]], feature_keys: list[str]) -> list[dict[str, Any]]:
    skip_rows = [row for row in rows if row.get("trade_action") == "SKIP"]
    base_tp = sum(1 for row in rows if row.get("classification") == "TP")
    base_fp = sum(1 for row in rows if row.get("classification") == "FP")
    base_fn = sum(1 for row in rows if row.get("classification") == "FN")
    rules: list[dict[str, Any]] = []
    for feature in feature_keys:
        values = sorted(set(round(value, 6) for value in numeric_values(skip_rows, feature)))
        if len(values) < 2:
            continue
        for op in [">=", "<="]:
            for threshold in values:
                rescued_tp = 0
                added_fp = 0
                matched = 0
                for row in skip_rows:
                    value = row.get(feature)
                    if value in ("", None):
                        keep = False
                    else:
                        keep = float(value) >= threshold if op == ">=" else float(value) <= threshold
                    if not keep:
                        continue
                    matched += 1
                    if row.get("classification") == "FN":
                        rescued_tp += 1
                    elif row.get("classification") == "TN":
                        added_fp += 1
                if not rescued_tp:
                    continue
                out = metric(base_tp + rescued_tp, base_fp + added_fp, base_fn - rescued_tp)
                out.update(
                    {
                        "feature": feature,
                        "operator": op,
                        "threshold": threshold,
                        "matched_skips": matched,
                        "rescued_tp": rescued_tp,
                        "added_fp": added_fp,
                        "scope": "skips_only",
                    }
                )
                rules.append(out)
    return sorted(rules, key=lambda item: (item["passed_gate"], item["f1"], -item["added_fp"]), reverse=True)


def summarize_by_family(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row.get("gate_reason_code") or "unknown")].append(row)
    summary = []
    for family, group in sorted(grouped.items()):
        item = {"family": family, **score_subset(group)}
        for feature in [
            "2h_pos20_pct",
            "2h_room_long_20_pct",
            "2h_room_short_20_pct",
            "1d_pos20_pct",
            "min_interaction_distance_pct",
            "net_support_minus_resistance",
        ]:
            vals = numeric_values(group, feature)
            if vals:
                item[f"median_{feature}"] = round(statistics.median(vals), 4)
        summary.append(item)
    return summary


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit Stage 1B entry gate features and simple classifiers.")
    parser.add_argument("--packet-dir", required=True, type=Path)
    parser.add_argument("--ground-truth-dir", required=True, type=Path)
    parser.add_argument("--score", action="append", required=True, type=Path)
    parser.add_argument("--primary-tf", default="1d")
    parser.add_argument("--anchor-tf", default="2h")
    parser.add_argument("--lookback", type=int, default=20)
    parser.add_argument("--out-csv", required=True, type=Path)
    parser.add_argument("--out-json", required=True, type=Path)
    parser.add_argument("--out-md", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ground_truth = load_ground_truth(args.ground_truth_dir)
    score_rows = load_scores(args.score)
    rows = [
        build_feature_row(
            score_row=row,
            packet_dir=args.packet_dir,
            ground_truth=ground_truth,
            primary_tf=args.primary_tf,
            anchor_tf=args.anchor_tf,
            lookback=args.lookback,
        )
        for row in score_rows
    ]
    write_csv(args.out_csv, rows)

    feature_keys = [
        key
        for key in rows[0].keys()
        if rows
        and not key.startswith("gt_")
        and re.search(r"(pos|room|edge|body|distance|support|resistance|last5_net)", key)
    ]
    rules = scan_threshold_rules(rows, feature_keys)
    rescue_rules = scan_skip_rescue_rules(rows, feature_keys)
    by_run: dict[str, Any] = {}
    for session_id in sorted({str(row["session_id"]) for row in rows}):
        by_run[session_id] = score_subset([row for row in rows if row["session_id"] == session_id])
    summary = {
        "total_rows": len(rows),
        "source_scores": [path.as_posix() for path in args.score],
        "by_run": by_run,
        "by_family": summarize_by_family(rows),
        "top_rules": rules[:50],
        "top_skip_rescue_rules": rescue_rules[:50],
    }
    args.out_json.parent.mkdir(parents=True, exist_ok=True)
    args.out_json.write_text(json.dumps(summary, indent=2) + "\n")

    md = [
        "# Stage 1B Entry Classifier Audit",
        "",
        "## Runs",
        "",
        "| Run | Total | TP | FP | TN | FN | Precision | Recall | Gate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|",
    ]
    for run, item in by_run.items():
        md.append(
            f"| `{run}` | {item['total']} | {item['tp']} | {item['fp']} | {item['tn']} | {item['fn']} | "
            f"{item['precision_pct']:.2f}% | {item['recall_pct']:.2f}% | {item['passed_gate']} |"
        )
    md += [
        "",
        "## Family Summary",
        "",
        "| Family | Total | TP | FP | TN | FN | Precision | Recall |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for item in summary["by_family"]:
        md.append(
            f"| `{item['family']}` | {item['total']} | {item['tp']} | {item['fp']} | {item['tn']} | {item['fn']} | "
            f"{item['precision_pct']:.2f}% | {item['recall_pct']:.2f}% |"
        )
    md += [
        "",
        "## Top Single-Feature Entry Filters",
        "",
        "These rules only filter existing ENTER decisions. They cannot recover FNs; use them to identify stable FP separators before any strategy wording update.",
        "",
        "| Rule | Kept Enters | Blocked TP | Blocked FP | Precision | Recall | Gate |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for item in rules[:20]:
        md.append(
            f"| `{item['feature']} {item['operator']} {item['threshold']}` | {item['kept_enters']} | "
            f"{item['blocked_tp']} | {item['blocked_fp']} | {item['precision_pct']:.2f}% | "
            f"{item['recall_pct']:.2f}% | {item['passed_gate']} |"
        )
    md += [
        "",
        "## Top Single-Feature SKIP Rescue Rules",
        "",
        "These rules turn matching SKIP decisions into hypothetical ENTER decisions. They are diagnostic only and must be checked for overfit before promotion.",
        "",
        "| Rule | Matched Skips | Rescued TP | Added FP | Precision | Recall | Gate |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for item in rescue_rules[:20]:
        md.append(
            f"| `{item['feature']} {item['operator']} {item['threshold']}` | {item['matched_skips']} | "
            f"{item['rescued_tp']} | {item['added_fp']} | {item['precision_pct']:.2f}% | "
            f"{item['recall_pct']:.2f}% | {item['passed_gate']} |"
        )
    args.out_md.parent.mkdir(parents=True, exist_ok=True)
    args.out_md.write_text("\n".join(md) + "\n")

    print(json.dumps({"rows": len(rows), "rules": len(rules), "out_json": str(args.out_json)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
