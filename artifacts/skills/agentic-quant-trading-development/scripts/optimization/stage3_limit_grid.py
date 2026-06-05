"""Stage 3 limit grid: adds mechanical limit-offset dimension.

LONG:  entry = reference_price * (1 - offset%)  — wait for dip
SHORT: entry = reference_price * (1 + offset%)  — wait for pop

Limit must be filled first before TP/SL simulation begins.
Unfilled signals = 0 P&L (no risk taken).

Grid: TP%, SL%, limit_offset% — 3D search.
"""

import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import List

FORWARD_HOURS = 36


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


def load_inputs(json_file: str, signal_dir: str) -> List[TradeInput]:
    with open(json_file) as f:
        signals = json.load(f)
    inputs = []
    for sig in signals:
        sid = sig['signal_id']
        sf = f'{signal_dir}/{sid}.json'
        with open(sf) as fh:
            data = json.load(fh)
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


def simulate_limit_trade(trade: TradeInput, candles: List[dict],
                         tp_pct: float, sl_pct: float,
                         limit_offset: float) -> str:
    """Returns 'TP', 'SL', 'UNFILLED', or 'NEITHER' (filled but no hit)."""
    entry = trade.entry_price
    direction = trade.direction
    cutoff = trade.signal_ts + timedelta(hours=FORWARD_HOURS)

    # Determine limit price
    if direction == 'LONG':
        limit_price = entry * (1 - limit_offset / 100)
    else:
        limit_price = entry * (1 + limit_offset / 100)

    limit_filled = False
    fill_index = None

    # First pass: find limit fill
    for i, c in enumerate(candles):
        if c['ts_dt'] <= trade.signal_ts:
            continue
        if c['ts_dt'] > cutoff:
            break

        if direction == 'LONG':
            if c['low'] <= limit_price:
                limit_filled = True
                fill_index = i
                break
        else:
            if c['high'] >= limit_price:
                limit_filled = True
                fill_index = i
                break

    if not limit_filled:
        return 'UNFILLED'

    # Second pass: from fill candle onward, check TP/SL
    # Entry is at limit_price from fill
    for j in range(fill_index, len(candles)):
        c = candles[j]
        if c['ts_dt'] > cutoff:
            break

        if direction == 'LONG':
            tp_price = limit_price * (1 + tp_pct / 100)
            sl_price = limit_price * (1 - sl_pct / 100)
            tp_hit = c['high'] >= tp_price
            sl_hit = c['low'] <= sl_price
        else:
            tp_price = limit_price * (1 - tp_pct / 100)
            sl_price = limit_price * (1 + sl_pct / 100)
            tp_hit = c['low'] <= tp_price
            sl_hit = c['high'] >= sl_price

        if tp_hit and sl_hit:
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
                tp_range, sl_range, limit_range, leverage: int):
    tp_start, tp_end, tp_step = tp_range
    sl_start, sl_end, sl_step = sl_range
    lim_start, lim_end, lim_step = limit_range

    results = []
    total = len(inputs)

    tp = tp_start
    while tp <= tp_end + 1e-9:
        sl = sl_start
        while sl <= sl_end + 1e-9:
            lim = lim_start
            while lim <= lim_end + 1e-9:
                tp_count = 0; sl_count = 0; neither = 0; unfilled = 0

                for trade in inputs:
                    outcome = simulate_limit_trade(trade, candles, tp, sl, lim)
                    if outcome == 'TP': tp_count += 1
                    elif outcome == 'SL': sl_count += 1
                    elif outcome == 'UNFILLED': unfilled += 1
                    else: neither += 1

                filled = total - unfilled
                wr = tp_count / filled * 100 if filled > 0 else 0

                # PnL: unfilled = 0, TP profit based on TP% of limit entry
                pnl = tp_count * tp * leverage - sl_count * sl * leverage
                pf = (tp_count * tp) / (sl_count * sl) if sl_count * sl > 0 else (999 if tp_count > 0 else 0)

                results.append({
                    'tp': round(tp, 1), 'sl': round(sl, 1), 'limit': round(lim, 1),
                    'tp_count': tp_count, 'sl_count': sl_count, 'neither': neither,
                    'unfilled': unfilled, 'filled': filled, 'fill_rate': round(filled/total*100, 1),
                    'wr': round(wr, 1), 'profit_factor': round(pf, 2),
                    'pnl_pct': round(pnl, 1), 'rr_ratio': round(tp/sl, 1) if sl > 0 else 0,
                })

                lim += lim_step
            sl += sl_step
        tp += tp_step

    return results


def print_best(results, sort_by='pnl_pct', top_n=15):
    print(f"\nBest by {sort_by}:")
    print(f"{'TP%':>5s} {'SL%':>5s} {'Lim%':>5s} {'WR%':>6s} {'TP#':>5s} {'SL#':>5s} "
          f"{'Fill':>5s} {'PnL%':>7s} {'PF':>6s}")
    print("-" * 58)
    for r in sorted(results, key=lambda x: x[sort_by], reverse=True)[:top_n]:
        print(f"{r['tp']:>4.1f}% {r['sl']:>4.1f}% {r['limit']:>4.1f}% {r['wr']:>5.1f}% "
              f"{r['tp_count']:>5d} {r['sl_count']:>5d} {r['fill_rate']:>4.1f}% "
              f"{r['pnl_pct']:>6.1f}% {r['profit_factor']:>5.2f}")


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--signals', required=True)
    p.add_argument('--signal-dir', required=True)
    p.add_argument('--candles', required=True)
    p.add_argument('--tp-range', nargs=3, type=float, default=[1.5, 3.5, 0.2])
    p.add_argument('--sl-range', nargs=3, type=float, default=[0.5, 1.5, 0.2])
    p.add_argument('--limit-range', nargs=3, type=float, default=[0.0, 0.8, 0.1])
    p.add_argument('--leverage', type=int, default=5)
    p.add_argument('--out', required=True)
    args = p.parse_args()

    inputs = load_inputs(args.signals, args.signal_dir)
    print(f"Loaded {len(inputs)} inputs")
    tss = [t.signal_ts for t in inputs]
    candles = load_candles(args.candles, min(tss), max(tss) + timedelta(hours=FORWARD_HOURS))
    print(f"Loaded {len(candles)} candles")

    tp_range = tuple(args.tp_range); sl_range = tuple(args.sl_range)
    limit_range = tuple(args.limit_range)
    print(f"Grid: TP{tp_range} SL{sl_range} Limit{limit_range}")

    results = grid_search(inputs, candles, tp_range, sl_range, limit_range, args.leverage)
    print(f"Grid points: {len(results)}")

    print_best(results, 'pnl_pct', 20)
    print()
    print_best(results, 'wr', 10)

    # Market-only (limit=0.0) baseline
    market = [r for r in results if r['limit'] == 0.0]
    if market:
        best_market = max(market, key=lambda r: r['pnl_pct'])
        print(f"\nMarket baseline (limit=0%): TP={best_market['tp']}% SL={best_market['sl']}% "
              f"WR={best_market['wr']}% PnL={best_market['pnl_pct']}%")
        best_limit = max(results, key=lambda r: r['pnl_pct'])
        print(f"Best with limit: TP={best_limit['tp']}% SL={best_limit['sl']}% "
              f"Limit={best_limit['limit']}% WR={best_limit['wr']}% Fill={best_limit['fill_rate']}% "
              f"PnL={best_limit['pnl_pct']}%")

    os.makedirs(args.out, exist_ok=True)
    with open(f'{args.out}/limit_grid_results.json', 'w') as f:
        json.dump({'total': len(inputs), 'leverage': args.leverage, 'results': results}, f, indent=2)
    with open(f'{args.out}/limit_optimal.json', 'w') as f:
        top5 = sorted(results, key=lambda r: r['pnl_pct'], reverse=True)[:5]
        best_limit_pnl = max(results, key=lambda r: r['pnl_pct'])
        best_market_pnl = max(market, key=lambda r: r['pnl_pct']) if market else None
        json.dump({'top5': top5, 'best_with_limit': best_limit_pnl,
                   'best_market': best_market_pnl}, f, indent=2)
    print(f"\nSaved to {args.out}/")


if __name__ == '__main__':
    main()
