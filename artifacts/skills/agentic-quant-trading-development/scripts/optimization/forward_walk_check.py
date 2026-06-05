#!/usr/bin/env python3
"""Forward-walk correctness check for Stage 1 backtesting.

Determines whether a directional prediction was correct by checking
if price EVER reached the favorable threshold within the 36-hour window.
Uses max_favorable_pct (best price excursion in predicted direction),
not terminal close at 36h. This matches how a trader thinks:
"Did my thesis ever play out?" not "Where did price end up?"
"""

import csv
import json
import sys
from datetime import datetime, timedelta, timezone

CANDLES_PATH = "dev/data/raw/BTC/5m/candles.csv"
FORWARD_HOURS = 36
MIN_MOVE_PCT = 0.005  # 0.5% minimum move to count as directional


def parse_candle_timestamp(ts_str: str) -> datetime:
    """Parse candle timestamp. Handles both ISO and ms epoch formats."""
    ts_str = ts_str.strip()
    if ts_str.isdigit():
        # Millisecond epoch
        return datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
    # ISO format
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def load_candles_in_range(csv_path: str, start: datetime, end: datetime) -> list:
    """Load 5m candles between start and end (inclusive)."""
    # start and end should be timezone-aware UTC
    start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
    end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
    
    candles = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts = parse_candle_timestamp(row["ts"])
            if ts < start:
                continue
            if ts > end:
                break
            candles.append({
                "ts": ts.isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            })
    return candles


def check_correctness(
    signal_ts: str,
    reference_price: float,
    predicted_direction: str,
    csv_path: str = CANDLES_PATH,
    forward_hours: int = FORWARD_HOURS,
    min_move_pct: float = MIN_MOVE_PCT,
) -> dict:
    """
    Check if directional prediction was correct.
    
    Returns dict with:
        correct: bool
        status: CORRECT | INCORRECT | NEUTRAL
        outcome_price: float
        outcome_time: str
        max_favorable_pct: float
        max_adverse_pct: float
        candles_available: int
    """
    # Parse signal timestamp
    signal_dt = parse_candle_timestamp(signal_ts)
    end_dt = signal_dt + timedelta(hours=forward_hours)
    
    candles = load_candles_in_range(csv_path, signal_dt, end_dt)
    
    if not candles:
        return {
            "correct": False,
            "status": "NO_DATA",
            "terminal_price": reference_price,
            "terminal_time": end_dt.isoformat(),
            "max_favorable_pct": 0,
            "max_adverse_pct": 0,
            "candles_available": 0,
        }
    
    # Track max favorable and adverse moves
    max_fav = 0.0
    max_adv = 0.0
    
    for c in candles:
        pct = (c["close"] - reference_price) / reference_price
        if predicted_direction == "LONG":
            if pct > max_fav:
                max_fav = pct
            if pct < max_adv:
                max_adv = pct
        else:  # SHORT
            if -pct > max_fav:
                max_fav = -pct
            if -pct < max_adv:
                max_adv = -pct
    
    # Terminal reference (for record-keeping, not for correctness)
    last_candle = candles[-1]
    terminal_price = last_candle["close"]
    terminal_time = last_candle["ts"]
    pct_change = (terminal_price - reference_price) / reference_price
    
    # Determine correctness using max favorable/adverse within the window.
    # max_fav is the best excursion in the predicted direction.
    # max_adv is the worst excursion against the predicted direction.
    # A prediction is CORRECT if price EVER reached the favorable threshold.
    if max_fav >= min_move_pct:
        status = "CORRECT"
    elif max_adv <= -min_move_pct:
        status = "INCORRECT"
    else:
        status = "NEUTRAL"
    
    return {
        "correct": status == "CORRECT",
        "status": status,
        "evaluation_method": "max_favorable",
        "terminal_price": round(terminal_price, 2),
        "terminal_time": terminal_time,
        "reference_price": reference_price,
        "pct_change": round(pct_change * 100, 4),
        "max_favorable_pct": round(max_fav * 100, 4),
        "max_adverse_pct": round(max_adv * 100, 4),
        "candles_available": len(candles),
        "forward_hours": forward_hours,
        "min_move_pct": min_move_pct * 100,
    }


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 forward_walk_check.py <signal_ts> <reference_price> <direction> [candles_path]")
        print("Example: python3 forward_walk_check.py 20250101T235000Z 93500.00 LONG")
        print("Example: python3 forward_walk_check.py 20250102T085500Z 3437.99 LONG dev/data/raw/ETH/5m/candles.csv")
        sys.exit(1)
    
    candles_path = sys.argv[4] if len(sys.argv) > 4 else CANDLES_PATH
    
    result = check_correctness(
        signal_ts=sys.argv[1],
        reference_price=float(sys.argv[2]),
        predicted_direction=sys.argv[3],
        csv_path=candles_path,
    )
    print(json.dumps(result, indent=2))
