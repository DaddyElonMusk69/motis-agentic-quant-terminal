#!/usr/bin/env python3
"""Stage 0a: Calibrate the significance threshold for natural direction.

Scans a range of thresholds (e.g., 0.2% to 2.0%) and measures:
  - Direction split: LONG/SHORT ratio. Near 50/50 = random assignment.
  - Reversal rate: % of signals that reverse past threshold in opposite direction.
  - Travel adequacy: P25 of first_move_pct at this threshold.

The stable range is where reversal < 15% AND travel P25 >= 1.0%.
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
    """
    For a given threshold, determine:
    - natural_direction: which side hit threshold first (or None if neither)
    - first_move_pct: how far it went before reversing past threshold the other way
    - reversed: did it reverse past threshold before window end?
    """
    cutoff = signal_ts + timedelta(hours=forward_hours)
    threshold_abs = threshold_pct / 100.0

    # Find first candle after signal
    first_idx = None
    for i, c in enumerate(candles):
        if c["ts"] > signal_ts:
            first_idx = i
            break
    if first_idx is None:
        return {"natural_direction": None, "first_move_pct": 0, "reversed": False,
                "status": "no_candles"}

    long_target = ref_price * (1 + threshold_abs)
    short_target = ref_price * (1 - threshold_abs)

    long_hit_ts = None
    short_hit_ts = None

    for i in range(first_idx, len(candles)):
        c = candles[i]
        if c["ts"] > cutoff:
            break
        if long_hit_ts is None and c["high"] >= long_target:
            long_hit_ts = c["ts"]
        if short_hit_ts is None and c["low"] <= short_target:
            short_hit_ts = c["ts"]
        if long_hit_ts is not None and short_hit_ts is not None:
            break

    # Neither hit
    if long_hit_ts is None and short_hit_ts is None:
        return {"natural_direction": None, "first_move_pct": 0, "reversed": False,
                "status": "no_trigger"}

    # Determine natural direction
    if long_hit_ts is not None and short_hit_ts is None:
        natural_direction = "LONG"
        first_hit_idx = None
        for i, c in enumerate(candles):
            if c["ts"] == long_hit_ts:
                first_hit_idx = i
                break
    elif short_hit_ts is not None and long_hit_ts is None:
        natural_direction = "SHORT"
        first_hit_idx = None
        for i, c in enumerate(candles):
            if c["ts"] == short_hit_ts:
                first_hit_idx = i
                break
    else:
        # Both hit in same candle — check which one was closer to ref
        # Or use the one that was hit first (closer to signal_ts)
        if long_hit_ts < short_hit_ts:
            natural_direction = "LONG"
            first_hit_idx = None
            for i, c in enumerate(candles):
                if c["ts"] == long_hit_ts:
                    first_hit_idx = i
                    break
        else:
            natural_direction = "SHORT"
            first_hit_idx = None
            for i, c in enumerate(candles):
                if c["ts"] == short_hit_ts:
                    first_hit_idx = i
                    break

    if first_hit_idx is None:
        return {"natural_direction": None, "first_move_pct": 0, "reversed": False,
                "status": "index_error"}

    # Now compute first_move_pct: how far in natural direction before reversing
    # past threshold the other way
    if natural_direction == "LONG":
        peak = ref_price
        reversal_threshold = short_target
        reversed_ = False
        for i in range(first_hit_idx, len(candles)):
            c = candles[i]
            if c["ts"] > cutoff:
                break
            if c["high"] > peak:
                peak = c["high"]
            if c["low"] <= reversal_threshold:
                reversed_ = True
                break
        first_move_pct = (peak - ref_price) / ref_price * 100
    else:
        trough = ref_price
        reversal_threshold = long_target
        reversed_ = False
        for i in range(first_hit_idx, len(candles)):
            c = candles[i]
            if c["ts"] > cutoff:
                break
            if c["low"] < trough:
                trough = c["low"]
            if c["high"] >= reversal_threshold:
                reversed_ = True
                break
        first_move_pct = (ref_price - trough) / ref_price * 100

    return {
        "natural_direction": natural_direction,
        "first_move_pct": round(first_move_pct, 4),
        "reversed": reversed_,
        "status": "ok"
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
    parser = argparse.ArgumentParser(description="Stage 0a: significance threshold calibration")
    parser.add_argument("signal_dir", help="Directory of signal JSON files")
    parser.add_argument("--candles", required=True, help="Path to 5m candles CSV")
    parser.add_argument("--forward-hours", type=int, default=36)
    parser.add_argument("--threshold-range", nargs=3, type=float,
                        default=[0.2, 2.0, 0.1],
                        help="Start, end, step for threshold scan (pct)")
    parser.add_argument("--out", required=True, help="Canonical output JSON path")
    parser.add_argument("--asset", default="UNKNOWN")
    parser.add_argument("--vote-threshold", type=int, default=0)
    args = parser.parse_args()

    threshold_start, threshold_end, threshold_step = args.threshold_range
    signal_dir = Path(args.signal_dir)
    candles_path = Path(args.candles)
    forward_hours = args.forward_hours

    # Discover signal files
    signal_files = sorted(signal_dir.glob("*.json"))
    signal_files = [f for f in signal_files if f.name not in ("index.json", "summary.json")]
    total = len(signal_files)
    print(f"Found {total} signal files")

    if total == 0:
        print("No signal files found.")
        sys.exit(1)

    # Parse all signal timestamps
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
    print(f"Loaded {len(candles):,} candles. Signal range: {earliest} → {latest}")

    # Pre-load all signal data
    print(f"Loading {len(signal_records)} signal packets...")
    signal_data = []
    for sf, sig_ts in signal_records:
        try:
            with open(sf) as f:
                sd = json.load(f)
            ref_price = get_reference_price(sd)
            if ref_price is None:
                continue
            signal_data.append({
                "signal_id": sf.stem,
                "signal_ts": sig_ts,
                "ref_price": ref_price,
            })
        except Exception:
            continue
    print(f"Loaded {len(signal_data)} signals with valid reference prices")

    # Generate thresholds
    thresholds = []
    t = threshold_start
    while t <= threshold_end + 1e-9:
        thresholds.append(round(t, 2))
        t += threshold_step

    print(f"\nScanning {len(thresholds)} thresholds ({threshold_start}% → {threshold_end}%)...")
    print(f"{'Threshold':>10s}  {'Split(L/S)':>12s}  {'Rev%':>6s}  {'TravelP25':>10s}  {'TravelP50':>10s}  {'NoTrig%':>8s}")
    print("-" * 72)

    results = []
    for thresh_idx, threshold in enumerate(thresholds):
        longs = 0
        shorts = 0
        reversed_count = 0
        no_trigger = 0
        travel_pcts = []

        for sd in signal_data:
            r = analyze_signal(candles, sd["signal_ts"], sd["ref_price"],
                              forward_hours, threshold)

            if r["status"] == "no_trigger":
                no_trigger += 1
                continue

            if r["natural_direction"] == "LONG":
                longs += 1
            elif r["natural_direction"] == "SHORT":
                shorts += 1
            else:
                no_trigger += 1
                continue

            if r["reversed"]:
                reversed_count += 1
            travel_pcts.append(r["first_move_pct"])

        total_valid = longs + shorts
        if total_valid == 0:
            continue

        split_str = f"{longs}/{shorts}" if total_valid > 0 else "N/A"
        rev_pct = reversed_count / total_valid * 100 if total_valid > 0 else 0
        no_trig_pct = no_trigger / len(signal_data) * 100
        pcts = percentiles(travel_pcts, 25, 50, 75)

        travel_p25 = pcts.get("p25", 0)
        travel_p50 = pcts.get("p50", 0)

        # Stability markers
        flags = []
        if rev_pct < 15:
            flags.append("✓rev")
        else:
            flags.append(" ✗rev")
        if travel_p25 >= 1.0:
            flags.append("✓travel")
        else:
            flags.append(" ✗travel")
        if 35 <= (longs / total_valid * 100) <= 65:
            flags.append("~split")
        else:
            flags.append("✓split")

        flag_str = " ".join(flags)
        print(f"{threshold:>8.1f}%  {split_str:>12s}  {rev_pct:>5.1f}%  {travel_p25:>8.2f}%  {travel_p50:>8.2f}%  {no_trig_pct:>7.1f}%  {flag_str}")

        results.append({
            "threshold_pct": threshold,
            "long_count": longs,
            "short_count": shorts,
            "total_valid": total_valid,
            "no_trigger": no_trigger,
            "no_trigger_pct": round(no_trig_pct, 1),
            "reversal_rate_pct": round(rev_pct, 1),
            "travel_p25": round(travel_p25, 2),
            "travel_p50": round(travel_p50, 2),
            "reversal_ok": rev_pct < 15,
            "travel_ok": travel_p25 >= 1.0,
            "split_ok": not (35 <= (longs / total_valid * 100) <= 65 if total_valid > 0 else True),
        })

    # ── Find stable range ────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"STABLE RANGE ANALYSIS")
    print(f"{'='*70}")

    stable = [r for r in results if r["reversal_ok"] and r["travel_ok"]]
    if stable:
        print(f"\nThresholds passing both reversal < 15% AND travel P25 >= 1.0%:")
        for r in stable:
            split_val = r["long_count"] / r["total_valid"] * 100 if r["total_valid"] > 0 else 0
            print(f"  {r['threshold_pct']:.1f}%  rev={r['reversal_rate_pct']:.1f}%  "
                  f"travel_p25={r['travel_p25']:.2f}%  split={split_val:.0f}/{100-split_val:.0f}")

        if len(stable) >= 1:
            mid = stable[len(stable) // 2]
            chosen = mid["threshold_pct"]
            low = stable[0]["threshold_pct"]
            high = stable[-1]["threshold_pct"]
            print(f"\n  Stable range: {low:.1f}% – {high:.1f}%")
            print(f"  Chosen threshold (midpoint): {chosen:.1f}%")
        else:
            chosen = stable[0]["threshold_pct"]
            print(f"\n  Only one stable threshold: {chosen:.1f}%")
    else:
        print("\nNo threshold passes both criteria!")
        # Find the best compromise
        best = min(results, key=lambda r: (r["reversal_rate_pct"] if not r["reversal_ok"] else 0)
                                          + (abs(1.0 - r["travel_p25"]) * 10 if not r["travel_ok"] else 0))
        chosen = best["threshold_pct"]
        print(f"  Best compromise: {chosen:.1f}% (rev={best['reversal_rate_pct']:.1f}%, "
              f"travel_p25={best['travel_p25']:.2f}%)")

    # ── Save ──────────────────────────────────────────────────────────
    out_path = args.out

    out = {
        "asset": args.asset,
        "vote_threshold": args.vote_threshold,
        "total_signals": len(signal_data),
        "forward_hours": forward_hours,
        "threshold_range": [threshold_start, threshold_end, threshold_step],
        "stable_range": [low if stable else chosen, high if stable else chosen],
        "chosen_threshold_pct": chosen,
        "scan_results": results,
    }

    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
