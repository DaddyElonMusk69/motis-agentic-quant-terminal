"""Stage 2: TP Capture Curve — mechanical walk on MATCH signals."""
import json, os, sys
from datetime import datetime, timedelta, timezone

FORWARD_HOURS = 36
CANDLES_CSV = None  # set from --candles


def get_reference_price(packet):
    interactions = packet.get("interactions", {})
    if isinstance(interactions, list):
        for timeframe in packet.get("active_timeframes", []):
            for entry in interactions:
                if entry.get("timeframe") == timeframe and entry.get("market_price") is not None:
                    return float(entry["market_price"])
        for entry in interactions:
            if entry.get("market_price") is not None:
                return float(entry["market_price"])
    else:
        for timeframe in packet.get("active_timeframes", []):
            entries = interactions.get(timeframe, [])
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])
        for entries in interactions.values():
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])

    for timeframe in packet.get("active_timeframes", []):
        chart = packet.get("charts", {}).get(timeframe, {})
        forming = chart.get("latest_forming_candle")
        if forming:
            if isinstance(forming, dict) and forming.get("close") is not None:
                return float(forming["close"])
            columns = chart.get("columns", [])
            if isinstance(forming, list) and "close" in columns:
                return float(forming[columns.index("close")])
    for chart in packet.get("charts", {}).values():
        forming = chart.get("latest_forming_candle")
        if isinstance(forming, dict) and forming.get("close") is not None:
            return float(forming["close"])
        columns = chart.get("columns", [])
        if isinstance(forming, list) and "close" in columns:
            return float(forming[columns.index("close")])
    raise ValueError("packet has no reference price")


def load_score_records(path):
    with open(path) as f:
        payload = json.load(f)
    if isinstance(payload, dict):
        return payload.get("records", payload.get("decisions", []))
    return payload


def load_signals(match_inputs, gt_dir, signal_dir):
    """Load all MATCH signals with reference prices and natural directions."""
    signals = []
    seen = set()
    for match_input in match_inputs:
        if os.path.isfile(match_input):
            records = load_score_records(match_input)
        else:
            records = []
            for fname in os.listdir(match_input):
                if not fname.endswith('.json'):
                    continue
                if fname in ('summary.json', 'index.json', 'summary_batch2.json'):
                    continue
                with open(f'{match_input}/{fname}') as f:
                    record = json.load(f)
                record.setdefault("signal_id", fname.replace(".json", ""))
                records.append(record)

        for r in records:
            if not isinstance(r, dict):
                continue
            sid = str(r.get("signal_id", ""))
            if not sid:
                continue
            if r.get('agreement') != 'MATCH':
                continue
            if sid in seen:
                continue
            seen.add(sid)

            with open(f'{gt_dir}/{sid}.json') as f:
                gt = json.load(f)

            sf = f'{signal_dir}/{sid}.json'
            with open(sf) as f:
                sig = json.load(f)
            ref_price = get_reference_price(sig)

            signals.append({
                'signal_id': sid,
                'natural_direction': gt['natural_direction'],
                'reference_price': ref_price,
                'signal_ts': sig['timestamp'],
            })
    return signals


def load_candles(csv_path, earliest_ts, latest_ts):
    """Load 5m candles in the date range."""
    start = earliest_ts.replace(tzinfo=timezone.utc) if earliest_ts.tzinfo is None else earliest_ts
    end = latest_ts.replace(tzinfo=timezone.utc) if latest_ts.tzinfo is None else latest_ts
    end = end + timedelta(hours=FORWARD_HOURS)

    candles = []
    with open(csv_path) as f:
        header = f.readline().strip().split(',')
        ts_idx = header.index('ts')
        o_idx = header.index('open')
        h_idx = header.index('high')
        l_idx = header.index('low')
        c_idx = header.index('close')

        for line in f:
            parts = line.strip().split(',')
            ts_str = parts[ts_idx]
            ts = datetime.fromisoformat(ts_str).replace(tzinfo=timezone.utc)
            if ts < start:
                continue
            if ts > end:
                break
            candles.append({
                'ts': ts,
                'open': float(parts[o_idx]),
                'high': float(parts[h_idx]),
                'low': float(parts[l_idx]),
                'close': float(parts[c_idx]),
            })
    return candles


def walk_capture(signals, candles, tp_levels):
    """For each signal, check which TP levels are reached within the forward window."""
    results = {tp: {'reached': 0, 'total': 0} for tp in tp_levels}
    per_signal = []

    for sig in signals:
        ref_price = sig['reference_price']
        nd = sig['natural_direction']
        ts = datetime.fromisoformat(sig['signal_ts']).replace(tzinfo=timezone.utc)
        cutoff = ts + timedelta(hours=FORWARD_HOURS)

        # Find first candle after signal
        reached = {tp: False for tp in tp_levels}
        first_reached = None  # first TP level reached

        for c in candles:
            if c['ts'] <= ts:
                continue
            if c['ts'] > cutoff:
                break

            if nd == 'LONG':
                for tp in tp_levels:
                    target = ref_price * (1 + tp / 100)
                    if c['high'] >= target and not reached[tp]:
                        reached[tp] = True
                        if first_reached is None:
                            first_reached = tp
            else:
                for tp in tp_levels:
                    target = ref_price * (1 - tp / 100)
                    if c['low'] <= target and not reached[tp]:
                        reached[tp] = True
                        if first_reached is None:
                            first_reached = tp

        for tp in tp_levels:
            results[tp]['total'] += 1
            if reached[tp]:
                results[tp]['reached'] += 1

        per_signal.append({
            'signal_id': sig['signal_id'],
            'natural_direction': nd,
            'reference_price': ref_price,
            'first_tp_reached': first_reached,
            'tp_reached': {tp: reached[tp] for tp in tp_levels},
        })

    return results, per_signal


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument('match_inputs', nargs='+', help='Stage 1A score JSON files or directories with per-signal MATCH results')
    p.add_argument('--candles', required=True)
    p.add_argument('--signal-dir', required=True)
    p.add_argument('--gt-dir', default='dev/training_sessions/btc-vegas-tunnel-v01/stage0/2026-BTC-2h-dedupe-vote2/scores/ground_truth')
    p.add_argument('--forward-hours', type=int, default=36)
    p.add_argument('--out', required=True)
    args = p.parse_args()

    global FORWARD_HOURS
    FORWARD_HOURS = args.forward_hours

    signals = load_signals(args.match_inputs, args.gt_dir, args.signal_dir)
    print(f"Loaded {len(signals)} MATCH signals")

    # Find date range
    tss = [datetime.fromisoformat(s['signal_ts']).replace(tzinfo=timezone.utc)
           for s in signals]
    earliest = min(tss)
    latest = max(tss)
    print(f"Date range: {earliest.date()} to {latest.date()}")

    candles = load_candles(args.candles, earliest, latest)
    print(f"Loaded {len(candles)} 5m candles")

    tp_levels = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0]
    results, per_signal = walk_capture(signals, candles, tp_levels)

    print(f"\n{'TP %':>6s}  {'Reached':>8s}  {'Total':>6s}  {'Rate':>7s}")
    print("-" * 32)
    for tp in tp_levels:
        r = results[tp]
        rate = r['reached'] / r['total'] * 100 if r['total'] > 0 else 0
        print(f"{tp:>5.1f}%  {r['reached']:>8d}  {r['total']:>6d}  {rate:>6.1f}%")

    # Save
    out_path = args.out
    out_is_file = out_path.endswith(".json")
    out_dir = os.path.dirname(out_path) if out_is_file else out_path
    os.makedirs(out_dir or ".", exist_ok=True)
    summary = {
        'total_signals': len(signals),
        'forward_hours': FORWARD_HOURS,
        'tp_levels': tp_levels,
        'results': {f"{tp:.1f}": {'reached': results[tp]['reached'], 'total': results[tp]['total'],
                                   'rate': round(results[tp]['reached']/results[tp]['total']*100, 1)}
                    for tp in tp_levels},
    }
    summary_path = out_path if out_is_file else f'{args.out}/capture_curve.json'
    per_signal_path = f'{out_dir}/stage2_capture_per_signal.json' if out_is_file else f'{args.out}/per_signal.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    with open(per_signal_path, 'w') as f:
        json.dump(per_signal, f, indent=2)
    print(f"\nSaved: {summary_path}")


if __name__ == '__main__':
    main()
