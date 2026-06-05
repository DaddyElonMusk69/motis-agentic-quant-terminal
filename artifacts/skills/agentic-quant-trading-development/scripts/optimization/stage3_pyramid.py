"""Pyramid: add legs as price moves favorably, SL fixed at original entry."""

import csv, json, os
from datetime import datetime, timedelta, timezone
from typing import List

FORWARD_HOURS = 36
LEVERAGE = 5


def parse_ts(ts_str: str) -> datetime:
    ts_str = ts_str.strip()
    if ts_str.isdigit():
        return datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
    ts_str = ts_str.replace("Z", "+00:00")
    return datetime.fromisoformat(ts_str)


def load_inputs(json_file: str, signal_dir: str):
    with open(json_file) as f:
        signals = json.load(f)
    inputs = []
    for sig in signals:
        sf = f'{signal_dir}/{sig["signal_id"]}.json'
        with open(sf) as fh:
            data = json.load(fh)
        inputs.append({
            'signal_id': sig['signal_id'],
            'direction': sig.get('natural_direction', sig.get('direction')),
            'entry_price': sig['reference_price'],
            'signal_ts': parse_ts(data['timestamp']),
        })
    return inputs


def load_candles(csv_path, start, end):
    start = start.replace(tzinfo=timezone.utc) if start.tzinfo is None else start
    end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
    candles = []
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            ts = parse_ts(row["ts"])
            if ts < start: continue
            if ts > end: break
            candles.append({"ts": ts, "O": float(row["open"]), "H": float(row["high"]),
                            "L": float(row["low"]), "C": float(row["close"])})
    return candles


def simulate(trade, candles, tp_pct, sl_pct, step_pct, max_legs, sl_breakeven=False):
    """Returns (margin_pnl, legs_filled, wins, losses)."""
    d = trade['direction']
    ref = trade['entry_price']
    cutoff = trade['signal_ts'] + timedelta(hours=FORWARD_HOURS)

    if d == 'LONG':
        sl_price = ref * (1 - sl_pct / 100)
    else:
        sl_price = ref * (1 + sl_pct / 100)

    # Active legs: (leg_num, entry, tp)
    active = [(1, ref, ref * (1 + tp_pct / 100) if d == 'LONG' else ref * (1 - tp_pct / 100))]
    entries = [ref]
    legs_filled = 1
    wins = 0
    losses = 0
    pnl = 0.0

    for c in candles:
        if c['ts'] <= trade['signal_ts']: continue
        if c['ts'] > cutoff: break

        # New leg entry?
        if legs_filled < max_legs:
            if d == 'LONG':
                next_e = entries[-1] * (1 + step_pct / 100)
                if c['H'] >= next_e:
                    legs_filled += 1
                    entries.append(next_e)
                    active.append((legs_filled, next_e, next_e * (1 + tp_pct / 100)))
                    # Move SL to breakeven: average entry of all filled legs
                    if sl_breakeven:
                        avg = sum(entries) / len(entries)
                        sl_price = avg
            else:
                next_e = entries[-1] * (1 - step_pct / 100)
                if c['L'] <= next_e:
                    legs_filled += 1
                    entries.append(next_e)
                    active.append((legs_filled, next_e, next_e * (1 - tp_pct / 100)))
                    if sl_breakeven:
                        avg = sum(entries) / len(entries)
                        sl_price = avg

        # Check TP/SL per leg
        closed = []
        for leg, entry, tp in active:
            if d == 'LONG':
                tp_hit = c['H'] >= tp
                sl_hit = c['L'] <= sl_price
            else:
                tp_hit = c['L'] <= tp
                sl_hit = c['H'] >= sl_price

            if tp_hit and sl_hit:
                body = c['C'] - c['O']
                closed.append((leg, 'TP' if (d == 'LONG' and body >= 0) or (d == 'SHORT' and body <= 0) else 'SL'))
            elif tp_hit:
                closed.append((leg, 'TP'))
            elif sl_hit:
                closed.append((leg, 'SL'))

        for leg, outcome in closed:
            if outcome == 'TP':
                pnl += tp_pct * LEVERAGE
                wins += 1
            else:
                entry = entries[leg - 1]
                loss = abs(entry - sl_price) / entry * 100
                pnl += -loss * LEVERAGE
                losses += 1

        active = [(n, e, t) for n, e, t in active if n not in {c[0] for c in closed}]
        if not active:
            break

    return pnl, legs_filled, wins, losses


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('--signals', required=True)
    p.add_argument('--signal-dir', required=True)
    p.add_argument('--candles', required=True)
    p.add_argument('--tp', type=float, required=True)
    p.add_argument('--sl', type=float, required=True)
    p.add_argument('--steps', type=float, nargs='+', default=[0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5])
    p.add_argument('--max-legs', type=int, default=3)
    p.add_argument('--sl-breakeven', action='store_true',
                   help='Move stop to average filled entry after a new leg is added')
    p.add_argument('--out', required=True)
    args = p.parse_args()

    inputs = load_inputs(args.signals, args.signal_dir)
    print(f"Signals: {len(inputs)}")
    tss = [i['signal_ts'] for i in inputs]
    candles = load_candles(args.candles, min(tss), max(tss) + timedelta(hours=FORWARD_HOURS))
    print(f"Candles: {len(candles)}")

    # Baseline
    bp = 0; bw = 0; bl = 0
    for t in inputs:
        pnl, _, w, l_ = simulate(t, candles, args.tp, args.sl, 999, 1)
        bp += pnl; bw += w; bl += l_
    print(f"\nBaseline 1-leg ({args.tp}%/{args.sl}%): PnL=+{bp:.0f}% W={bw} L={bl}")

    records = []
    for step in args.steps:
        tp_ = 0; lg = 0; wi = 0; lo = 0
        for t in inputs:
            pnl, legs, w, l_ = simulate(
                t, candles, args.tp, args.sl, step, args.max_legs, sl_breakeven=args.sl_breakeven
            )
            tp_ += pnl; lg += legs; wi += w; lo += l_
        vs = tp_ - bp
        better = "BETTER" if tp_ > bp else ("worse" if tp_ < bp else "same")
        records.append({
            "step_pct": step,
            "pnl_pct": round(tp_, 4),
            "delta_vs_baseline_pct": round(vs, 4),
            "avg_legs_per_signal": round(lg / len(inputs), 4) if inputs else 0,
            "wins": wi,
            "losses": lo,
            "comparison": better,
        })
        print(f"  Step {step:.1f}%: PnL=+{tp_:.0f}% ({vs:+.0f} vs base) Lg={lg/len(inputs):.1f}/sig W={wi} L={lo}  {better}")

    os.makedirs(args.out, exist_ok=True)
    with open(f'{args.out}/pyramid_results.json', 'w') as f:
        json.dump({
            "total_signals": len(inputs),
            "tp_pct": args.tp,
            "sl_pct": args.sl,
            "max_legs": args.max_legs,
            "sl_breakeven": args.sl_breakeven,
            "baseline": {
                "pnl_pct": round(bp, 4),
                "wins": bw,
                "losses": bl,
            },
            "results": records,
        }, f, indent=2)
    best = max(records, key=lambda r: r["pnl_pct"]) if records else None
    with open(f'{args.out}/pyramid_optimal.json', 'w') as f:
        json.dump({"criterion": "max_pnl", "best": best}, f, indent=2)
    print(f"\nSaved to {args.out}/")


if __name__ == '__main__':
    main()
