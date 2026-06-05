#!/usr/bin/env python3
"""Stage 0b: Compute signal ground truth using a calibrated threshold.

For each signal, determines natural direction, first_move_pct, max_travel_pct,
and whether the signal reversed. Outputs per-signal JSON + directional
travel distribution.
"""

import csv
import json
import sys
import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
SRC = WORKSPACE_ROOT / "artifacts" / "signal_engine" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.workspace import find_workspace_root


WORKSPACE_ROOT = find_workspace_root(WORKSPACE_ROOT)


def infer_signal_engine_id(signal_dir: Path) -> str:
    if signal_dir.name == "packets" and len(signal_dir.parents) >= 3:
        return signal_dir.parents[2].name
    return signal_dir.parent.parent.name if len(signal_dir.parents) >= 2 else ""


def infer_signal_family(signal_dir: Path) -> str:
    signal_engine_id = infer_signal_engine_id(signal_dir)
    return signal_engine_id if signal_engine_id else ""

def parse_ts(ts_str: str) -> datetime:
    ts_str = ts_str.strip()
    if ts_str.isdigit():
        return datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def load_candles(csv_path: str, start: datetime, end: datetime) -> list:
    start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
    end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
    candles = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_ts(row["ts"])
            if ts < start:
                continue
            if ts > end:
                break
            candles.append({
                "ts": ts,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
    return candles


def get_reference_price(sig_data: dict) -> float | None:
    interactions = sig_data.get("interactions", {})
    if isinstance(interactions, dict):
        for tf in interactions:
            entries = interactions[tf]
            if entries:
                mp = entries[0].get("market_price")
                if mp:
                    return float(mp)
    elif isinstance(interactions, list):
        for entry in interactions:
            if not isinstance(entry, dict):
                continue
            mp = entry.get("market_price")
            if mp:
                return float(mp)
    for tf in ["2h", "4h", "8h", "12h", "1d"]:
        lfc = sig_data.get("charts", {}).get(tf, {}).get("latest_forming_candle", {})
        if lfc and lfc.get("close"):
            return float(lfc["close"])
    return None


def analyze_signal(candles: list, signal_ts: datetime, ref_price: float,
                   forward_hours: int, threshold_pct: float) -> dict:
    cutoff = signal_ts + timedelta(hours=forward_hours)
    threshold_abs = threshold_pct / 100.0

    first_idx = None
    for i, c in enumerate(candles):
        if c["ts"] > signal_ts:
            first_idx = i
            break
    if first_idx is None:
        return {"natural_direction": None, "first_move_pct": 0, "max_travel_pct": 0,
                "opposite_max_pct": 0, "reversed": False, "status": "no_candles"}

    long_target = ref_price * (1 + threshold_abs)
    short_target = ref_price * (1 - threshold_abs)

    long_hit_idx = None
    short_hit_idx = None
    for i in range(first_idx, len(candles)):
        c = candles[i]
        if c["ts"] > cutoff:
            break
        if long_hit_idx is None and c["high"] >= long_target:
            long_hit_idx = i
        if short_hit_idx is None and c["low"] <= short_target:
            short_hit_idx = i
        if long_hit_idx is not None and short_hit_idx is not None:
            break

    if long_hit_idx is None and short_hit_idx is None:
        return {"natural_direction": None, "first_move_pct": 0, "max_travel_pct": 0,
                "opposite_max_pct": 0, "reversed": False, "status": "no_trigger"}

    # Determine natural direction
    if long_hit_idx is not None and short_hit_idx is None:
        natural_direction = "LONG"
        start_idx = long_hit_idx
    elif short_hit_idx is not None and long_hit_idx is None:
        natural_direction = "SHORT"
        start_idx = short_hit_idx
    else:
        # Both hit — first one wins
        if long_hit_idx < short_hit_idx:
            natural_direction = "LONG"
            start_idx = long_hit_idx
        else:
            natural_direction = "SHORT"
            start_idx = short_hit_idx

    # Walk from start_idx: track max in natural direction + max in opposite + reversal
    max_natural = float('-inf')
    max_opposite = float('-inf')
    reversed_ = False

    for i in range(start_idx, len(candles)):
        c = candles[i]
        if c["ts"] > cutoff:
            break

        if natural_direction == "LONG":
            move = (c["high"] - ref_price) / ref_price * 100
            opp = (ref_price - c["low"]) / ref_price * 100
            if move > max_natural:
                max_natural = move
            if opp > max_opposite:
                max_opposite = opp
            if opp >= threshold_pct and not reversed_:
                reversed_ = True
        else:
            move = (ref_price - c["low"]) / ref_price * 100
            opp = (c["high"] - ref_price) / ref_price * 100
            if move > max_natural:
                max_natural = move
            if opp > max_opposite:
                max_opposite = opp
            if opp >= threshold_pct and not reversed_:
                reversed_ = True

    # first_move_pct = max_natural at the point of first reversal, or max_natural if never reversed
    # Simplified: record max_natural as first_move_pct (it's the first significant move magnitude)
    first_move_pct = max_natural if max_natural != float('-inf') else threshold_pct

    return {
        "natural_direction": natural_direction,
        "first_move_pct": round(first_move_pct, 4),
        "max_travel_pct": round(max_natural if max_natural != float('-inf') else 0, 4),
        "opposite_max_pct": round(max_opposite if max_opposite != float('-inf') else 0, 4),
        "reversed": reversed_,
        "status": "ok",
    }


def percentiles(values: list, *pcts) -> dict:
    if not values:
        return {}
    vals = sorted(values)
    n = len(vals)
    result = {}
    for p in pcts:
        idx = int(n * p / 100)
        idx = min(idx, n - 1)
        result[f"p{p}"] = round(vals[idx], 4)
    return result


def main():
    parser = argparse.ArgumentParser(description="Stage 0b: signal ground truth")
    parser.add_argument("signal_dir", help="Directory of signal JSON files")
    parser.add_argument("--candles", required=True, help="Path to 5m candles CSV")
    parser.add_argument("--forward-hours", type=int, default=36)
    parser.add_argument("--significance-threshold", type=float, required=True,
                        help="Calibrated significance threshold pct (e.g. 1.8)")
    parser.add_argument("--out", required=True, help="Canonical output directory for per-signal JSON + distribution")
    parser.add_argument("--asset", default="UNKNOWN")
    parser.add_argument("--vote-threshold", type=int, default=0)
    args = parser.parse_args()

    signal_dir = Path(args.signal_dir)
    candles_path = Path(args.candles)
    forward_hours = args.forward_hours
    threshold = args.significance_threshold

    # Discover signal files
    signal_files = sorted(signal_dir.glob("*.json"))
    signal_files = [f for f in signal_files if f.name not in ("index.json", "summary.json")]
    total = len(signal_files)
    print(f"Found {total} signal files")

    # Parse timestamps
    signal_records = []
    for sf in signal_files:
        dt_str = sf.stem.replace("Z", "")
        try:
            ts = datetime.strptime(dt_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            signal_records.append((sf, ts))
        except ValueError:
            continue

    earliest = min(ts for _, ts in signal_records)
    latest = max(ts for _, ts in signal_records) + timedelta(hours=forward_hours)

    print(f"Loading candles...")
    candles = load_candles(str(candles_path), earliest, latest)
    print(f"Loaded {len(candles):,} candles")

    # Output directory
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\nComputing ground truth for {len(signal_records)} signals (threshold={threshold}%)...")
    report_every = max(1, len(signal_records) // 10)

    results = []
    longs = []
    shorts = []
    no_trigger = 0
    reversed_count = 0

    for idx, (sf, sig_ts) in enumerate(signal_records):
        try:
            with open(sf) as f:
                sig_data = json.load(f)
        except Exception:
            continue

        ref_price = get_reference_price(sig_data)
        if ref_price is None:
            continue

        r = analyze_signal(candles, sig_ts, ref_price, forward_hours, threshold)

        record = {
            "signal_id": sf.stem,
            "reference_price": ref_price,
            "significance_threshold_pct": threshold,
            "natural_direction": r["natural_direction"],
            "first_move_pct": r["first_move_pct"],
            "max_travel_pct": r["max_travel_pct"],
            "opposite_max_pct": r["opposite_max_pct"],
            "reversed": r["reversed"],
            "status": r["status"],
        }
        results.append(record)

        # Write per-signal file
        with open(out_dir / f"{sf.stem}.json", "w") as f:
            json.dump(record, f, indent=2)

        if r["natural_direction"] == "LONG":
            longs.append(r["first_move_pct"])
        elif r["natural_direction"] == "SHORT":
            shorts.append(r["first_move_pct"])
        else:
            no_trigger += 1

        if r["reversed"]:
            reversed_count += 1

        if (idx + 1) % report_every == 0:
            print(f"  {idx+1}/{len(signal_records)} done...")

    # ── Distribution ──────────────────────────────────────────────────
    all_travels = longs + shorts
    total_valid = len(all_travels)
    long_count = len(longs)
    short_count = len(shorts)

    pcts_all = percentiles(all_travels, 10, 25, 50, 75, 90, 95)
    pcts_long = percentiles(longs, 10, 25, 50, 75, 90, 95)
    pcts_short = percentiles(shorts, 10, 25, 50, 75, 90, 95)

    print(f"\n{'='*60}")
    print(f"GROUND TRUTH — {args.asset} ({total} signals, threshold={threshold}%)")
    print(f"{'='*60}")
    print(f"\n  Total signals:           {len(results)}")
    print(f"  With direction:          {total_valid} ({total_valid/len(results)*100:.1f}%)")
    print(f"    LONG:                  {long_count}")
    print(f"    SHORT:                 {short_count}")
    print(f"  No trigger (never hit):  {no_trigger} ({no_trigger/len(results)*100:.1f}%)")
    print(f"  Reversed:                {reversed_count} ({reversed_count/total_valid*100:.1f}% of triggered)")

    print(f"\nDirectional Travel Distribution:")
    print(f"{'':>8s}  {'LONG':>8s}  {'SHORT':>8s}  {'ALL':>8s}")
    print(f"{'':->40s}")
    for pct_name in ["p10", "p25", "p50", "p75", "p90", "p95"]:
        lv = pcts_long.get(pct_name, 0)
        sv = pcts_short.get(pct_name, 0)
        av = pcts_all.get(pct_name, 0)
        print(f"  {pct_name.upper():6s}: {lv:>7.2f}%  {sv:>7.2f}%  {av:>7.2f}%")

    # TP suggestions
    p25_all = pcts_all.get("p25", 0)
    p50_all = pcts_all.get("p50", 0)
    print(f"\nTP Suggestions (based on first_move_pct percentiles):")
    print(f"  Conservative (P25): {p25_all:.2f}% — {int(total_valid * 0.75)} signals reach this")
    print(f"  Balanced (P50):     {p50_all:.2f}% — {total_valid // 2} signals reach this")

    # Save distribution
    trigger_rate_pct = round(total_valid / len(results) * 100, 2) if results else 0
    branch_path = "path_a" if trigger_rate_pct >= 80 else "path_b"
    branch_decision = (
        "rich_pool_go_to_stage1a"
        if branch_path == "path_a"
        else "sparse_pool_go_to_stage1b_then_stage1a"
    )
    dist = {
        "asset": args.asset,
        "vote_threshold": args.vote_threshold,
        "significance_threshold_pct": threshold,
        "calibration_source": "sensitivity_scan",
        "total_signals": len(results),
        "forward_hours": forward_hours,
        "direction_split": {"LONG": long_count, "SHORT": short_count},
        "no_trigger": no_trigger,
        "reversed_count": reversed_count,
        "reversal_rate_pct": round(reversed_count / total_valid * 100, 1) if total_valid else 0,
        "travel_distribution": {
            "LONG": pcts_long,
            "SHORT": pcts_short,
            "ALL": pcts_all,
        },
    }

    dist_path = out_dir / "distribution.json"
    with open(dist_path, "w") as f:
        json.dump(dist, f, indent=2)

    summary = {
        "schema_version": "0.1",
        "asset": args.asset,
        "signal_engine_id": infer_signal_engine_id(signal_dir),
        "signal_family": infer_signal_family(signal_dir),
        "stage": "stage0c_ground_truth",
        "scoring_method": "first_significant_move_natural_direction",
        "created_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "metrics": {
            "total_records": len(results),
            "status_counts": {
                "triggered": total_valid,
                "no_trigger": no_trigger,
            },
            "direction_counts": {
                "LONG": long_count,
                "SHORT": short_count,
            },
            "trigger_rate_pct": trigger_rate_pct,
            "significance_threshold_pct": threshold,
            "forward_hours": forward_hours,
            "branch_path": branch_path,
            "branch_decision": branch_decision,
            "reversed_count": reversed_count,
            "reversal_rate_pct": round(reversed_count / total_valid * 100, 1) if total_valid else 0,
        },
        "records_path": "scores/ground_truth/",
    }
    summary_path = out_dir.parent / "ground_truth_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    # Also write index
    index = [r["signal_id"] for r in results]
    with open(out_dir / "index.json", "w") as f:
        json.dump(index, f)

    print(f"\nPer-signal files: {out_dir}/")
    print(f"Distribution:      {dist_path}")
    print(f"Summary:           {summary_path}")
    print(f"Signals written:   {len(results)}")


if __name__ == "__main__":
    main()
