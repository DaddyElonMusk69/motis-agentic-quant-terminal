#!/usr/bin/env python3
"""Stage 0: Travel distribution for a signal set.

Walks every signal forward through 5m candles (default 36h) and records
max_favorable_pct — the largest absolute price move in either direction.
Outputs a percentile distribution for picking a travel threshold.
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
    """Extract reference price from signal packet."""
    # Try interactions first. Support both legacy {timeframe: [...]} and
    # canonical [{timeframe: "...", market_price: "..."}] packet shapes.
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

    # Fallback to latest_forming_candle.close
    for tf in ["2h", "4h", "8h", "12h", "1d"]:
        lfc = sig_data.get("charts", {}).get(tf, {}).get("latest_forming_candle", {})
        if lfc and lfc.get("close"):
            return float(lfc["close"])

    return None


def compute_max_travel(candles: list, signal_ts: datetime, ref_price: float,
                       forward_hours: int) -> float:
    """Walk forward from signal_ts and return max_favorable_pct (abs move)."""
    cutoff = signal_ts + timedelta(hours=forward_hours)

    # Find first candle after signal
    first_idx = None
    for i, c in enumerate(candles):
        if c["ts"] > signal_ts:
            first_idx = i
            break
    if first_idx is None:
        return 0.0

    max_high = ref_price
    max_low = ref_price

    for i in range(first_idx, len(candles)):
        c = candles[i]
        if c["ts"] > cutoff:
            break
        if c["high"] > max_high:
            max_high = c["high"]
        if c["low"] < max_low:
            max_low = c["low"]

    up_pct = (max_high - ref_price) / ref_price * 100
    down_pct = (ref_price - max_low) / ref_price * 100

    return max(up_pct, down_pct)


def percentiles(values: list, *pcts) -> dict:
    """Compute percentiles from a sorted list."""
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
    parser = argparse.ArgumentParser(description="Stage 0: travel distribution")
    parser.add_argument("signal_dir", help="Directory of signal JSON files")
    parser.add_argument("--candles", required=True, help="Path to 5m candles CSV")
    parser.add_argument("--forward-hours", type=int, default=36)
    parser.add_argument("--out", required=True, help="Canonical output JSON path")
    parser.add_argument("--asset", default="UNKNOWN", help="Asset name")
    parser.add_argument("--vote-threshold", type=int, default=0, help="Vote threshold used")
    args = parser.parse_args()

    signal_dir = Path(args.signal_dir)
    candles_path = Path(args.candles)
    forward_hours = args.forward_hours

    # Discover all signal files
    signal_files = sorted(signal_dir.glob("*.json"))
    # Filter out index.json and summary.json
    signal_files = [f for f in signal_files if f.name not in ("index.json", "summary.json")]
    total = len(signal_files)
    print(f"Found {total} signal files in {signal_dir}")

    if total == 0:
        print("No signal files found. Aborting.")
        sys.exit(1)

    # Parse all signal timestamps
    signal_timestamps = []
    for sf in signal_files:
        # filename: YYYYMMDDTHHMMSSZ.json
        dt_str = sf.stem.replace("Z", "")
        try:
            ts = datetime.strptime(dt_str, "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
            signal_timestamps.append((sf, ts))
        except ValueError:
            print(f"  SKIP: cannot parse timestamp from {sf.name}")
            continue

    if not signal_timestamps:
        print("No valid signal timestamps found. Aborting.")
        sys.exit(1)

    earliest = min(ts for _, ts in signal_timestamps)
    latest = max(ts for _, ts in signal_timestamps) + timedelta(hours=forward_hours)

    print(f"Signal range: {earliest} → {latest}")
    print(f"Loading candles from {candles_path}...")
    candles = load_candles(str(candles_path), earliest, latest)
    print(f"Loaded {len(candles):,} candles. Processing {len(signal_timestamps)} signals...")

    travel_pcts = []
    errors = 0
    skipped = 0
    report_every = max(1, len(signal_timestamps) // 20)

    for idx, (sf, sig_ts) in enumerate(signal_timestamps):
        try:
            with open(sf) as f:
                sig_data = json.load(f)
        except (json.JSONDecodeError, OSError):
            skipped += 1
            continue

        ref_price = get_reference_price(sig_data)
        if ref_price is None:
            errors += 1
            continue

        max_travel = compute_max_travel(candles, sig_ts, ref_price, forward_hours)
        travel_pcts.append(round(max_travel, 4))

        if (idx + 1) % report_every == 0:
            print(f"  {idx+1}/{len(signal_timestamps)} done...")

    # ── Distribution ──────────────────────────────────────────────────
    print(f"\nProcessed: {len(travel_pcts)} signals ({errors} ref-price errors, {skipped} skipped)")

    pcts = percentiles(travel_pcts, 10, 20, 25, 30, 40, 50, 60, 70, 75, 80, 90, 95)

    print(f"\n{'='*60}")
    print(f"TRAVEL DISTRIBUTION — {args.asset} ({total} signals, {forward_hours}h window)")
    print(f"{'='*60}")
    print(f"\n  Total signals:   {len(travel_pcts)}")
    print(f"  Mean travel:     {sum(travel_pcts)/len(travel_pcts):.2f}%")
    print(f"  Zero travel:     {travel_pcts.count(0.0)} ({travel_pcts.count(0.0)/len(travel_pcts)*100:.1f}%)")
    print(f"\nPercentile distribution:")
    for pct_name in sorted(pcts.keys(), key=lambda x: int(x[1:])):
        val = pcts[pct_name]
        bar = "█" * int(val * 5)
        print(f"  {pct_name.upper():6s}: {val:6.2f}% {bar}")
    print(f"\n{'─'*60}")
    print(f"Suggested thresholds:")
    for pct_name in ["p50", "p60", "p70", "p75"]:
        val = pcts[pct_name]
        pct_num = int(pct_name[1:])
        count_above = sum(1 for t in travel_pcts if t >= val)
        print(f"  P{pct_num} ({val:.2f}%): {count_above}/{len(travel_pcts)} signals reach this")

    # ── Save ──────────────────────────────────────────────────────────
    out_path = args.out

    # Safety: truncate travel_pcts list if too large for JSON
    output_travel_list = travel_pcts[:1000] if len(travel_pcts) > 1000 else travel_pcts

    out = {
        "asset": args.asset,
        "vote_threshold": args.vote_threshold,
        "total_signals": len(travel_pcts),
        "forward_hours": forward_hours,
        "distribution": pcts,
        "mean": round(sum(travel_pcts) / len(travel_pcts), 4) if travel_pcts else 0,
        "zero_travel_count": travel_pcts.count(0.0),
        "all_travels": output_travel_list,
    }

    with open(out_path, "w") as f:
        json.dump(out, f, indent=2)
    print(f"\nSaved: {out_path}")


if __name__ == "__main__":
    main()
