"""Stage 3: TP/SL grid search on Stage 2 MATCH signals.

Walks 5m candles from signal timestamp. For each (TP%, SL%) pair, determines
whether TP or SL was hit first within the forward window.

Within-candle tiebreaker: candle body direction determines precedence.
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List, Dict


FORWARD_HOURS = 36
LEVERAGE = 5


@dataclass
class TradeInput:
    signal_id: str
    direction: str
    entry_price: float
    signal_ts: datetime


def parse_ts(ts_str: str) -> datetime:
    ts_str = ts_str.strip()
    if ts_str.isdigit():
        return datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def load_inputs(match_signals_file: str, signal_dir: str) -> List[TradeInput]:
    """Load trade inputs from Stage 2 match signals."""
    with open(match_signals_file) as f:
        signals = json.load(f)

    inputs = []
    for sig in signals:
        sid = sig['signal_id']
        # Get signal timestamp from signal file
        sf = f'{signal_dir}/{sid}.json'
        with open(sf) as f:
            data = json.load(f)
        ts = parse_ts(data['timestamp'])

        inputs.append(TradeInput(
            signal_id=sid,
            direction=sig.get('natural_direction', sig.get('direction', 'LONG')),
            entry_price=sig['reference_price'],
            signal_ts=ts,
        ))
    return inputs


def load_candles(csv_path: str, start: datetime, end: datetime) -> List[dict]:
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
                "ts": ts.isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "ts_dt": ts,
            })
    return candles


def simulate_trade(trade: TradeInput, candles: List[dict],
                   tp_pct: float, sl_pct: float) -> str:
    """Simulate one trade. Returns 'TP', 'SL', or 'NEITHER'."""
    entry = trade.entry_price
    direction = trade.direction
    cutoff = trade.signal_ts + timedelta(hours=FORWARD_HOURS)

    for c in candles:
        if c['ts_dt'] <= trade.signal_ts:
            continue
        if c['ts_dt'] > cutoff:
            break

        if direction == 'LONG':
            tp_price = entry * (1 + tp_pct / 100)
            sl_price = entry * (1 - sl_pct / 100)
            tp_hit = c['high'] >= tp_price
            sl_hit = c['low'] <= sl_price
        else:
            tp_price = entry * (1 - tp_pct / 100)
            sl_price = entry * (1 + sl_pct / 100)
            tp_hit = c['low'] <= tp_price
            sl_hit = c['high'] >= sl_price

        if tp_hit and sl_hit:
            # Tiebreaker: candle body direction
            body = c['close'] - c['open']
            if direction == 'LONG':
                return 'TP' if body >= 0 else 'SL'
            else:
                return 'TP' if body <= 0 else 'SL'
        elif tp_hit:
            return 'TP'
        elif sl_hit:
            return 'SL'

    return 'NEITHER'


def grid_search(inputs: List[TradeInput], candles: List[dict],
                tp_range: tuple, sl_range: tuple) -> List[Dict]:
    """Run full grid search."""
    tp_start, tp_end, tp_step = tp_range
    sl_start, sl_end, sl_step = sl_range

    results = []
    tp = tp_start
    while tp <= tp_end + 1e-9:
        sl = sl_start
        while sl <= sl_end + 1e-9:
            tp_count = 0
            sl_count = 0
            neither = 0

            for trade in inputs:
                outcome = simulate_trade(trade, candles, tp, sl)
                if outcome == 'TP':
                    tp_count += 1
                elif outcome == 'SL':
                    sl_count += 1
                else:
                    neither += 1

            total = len(inputs)
            wr = tp_count / total * 100 if total > 0 else 0
            exp = (tp_count * tp - sl_count * sl) / total if total > 0 else 0
            pf = (tp_count * tp) / (sl_count * sl) if sl_count * sl > 0 else (999 if tp_count > 0 else 0)

            # PnL with leverage
            pnl_pct = tp_count * tp * LEVERAGE - sl_count * sl * LEVERAGE

            results.append({
                'tp': round(tp, 1),
                'sl': round(sl, 1),
                'tp_count': tp_count,
                'sl_count': sl_count,
                'neither': neither,
                'wr': round(wr, 1),
                'expectancy': round(exp, 2),
                'profit_factor': round(pf, 2),
                'pnl_pct': round(pnl_pct, 1),
                'rr_ratio': round(tp / sl, 1) if sl > 0 else 0,
            })

            sl += sl_step
        tp += tp_step

    return results


def print_top(results: List[Dict], sort_by: str = 'pnl_pct', top_n: int = 15):
    print(f"\n{'TP%':>5s} {'SL%':>5s} {'WR%':>6s} {'TP#':>5s} {'SL#':>5s} "
          f"{'Exp%':>6s} {'PF':>6s} {'PnL%':>7s} {'R:R':>5s}")
    print("-" * 58)
    sorted_results = sorted(results, key=lambda r: r[sort_by], reverse=True)
    for r in sorted_results[:top_n]:
        print(f"{r['tp']:>4.1f}% {r['sl']:>4.1f}% {r['wr']:>5.1f}% {r['tp_count']:>5d} "
              f"{r['sl_count']:>5d} {r['expectancy']:>5.2f}% {r['profit_factor']:>5.2f} "
              f"{r['pnl_pct']:>6.1f}% {r['rr_ratio']:>4.1f}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--match-signals', required=True, help='JSON file with MATCH signal list')
    p.add_argument('--signal-dir', required=True)
    p.add_argument('--candles', required=True)
    p.add_argument('--tp-range', nargs=3, type=float, default=[1.0, 3.5, 0.3])
    p.add_argument('--sl-range', nargs=3, type=float, default=[0.3, 1.5, 0.1])
    p.add_argument('--leverage', type=int, default=5)
    p.add_argument('--out', required=True)
    args = p.parse_args()

    global LEVERAGE
    LEVERAGE = args.leverage

    inputs = load_inputs(args.match_signals, args.signal_dir)
    print(f"Loaded {len(inputs)} trade inputs")

    tss = [t.signal_ts for t in inputs]
    earliest = min(tss); latest = max(tss)
    print(f"Date range: {earliest.date()} to {latest.date()}")

    candles = load_candles(args.candles, earliest, latest + timedelta(hours=FORWARD_HOURS))
    print(f"Loaded {len(candles)} candles")

    tp_range = tuple(args.tp_range)
    sl_range = tuple(args.sl_range)
    print(f"Grid: TP {tp_range}, SL {sl_range}")

    results = grid_search(inputs, candles, tp_range, sl_range)
    print(f"Grid points: {len(results)}")

    print_top(results, 'pnl_pct', 20)
    print()
    print_top(results, 'wr', 10)

    os.makedirs(args.out, exist_ok=True)
    with open(f'{args.out}/grid_results.json', 'w') as f:
        json.dump({
            'total_signals': len(inputs),
            'leverage': LEVERAGE,
            'tp_range': list(tp_range),
            'sl_range': list(sl_range),
            'results': results,
        }, f, indent=2)

    top5 = sorted(results, key=lambda r: r['pnl_pct'], reverse=True)[:5]
    with open(f'{args.out}/optimal.json', 'w') as f:
        json.dump({
            'criterion': 'max_pnl',
            'top_5': top5,
            'best': top5[0] if top5 else None,
        }, f, indent=2)

    print(f"\nSaved to {args.out}/")


if __name__ == '__main__':
    main()
