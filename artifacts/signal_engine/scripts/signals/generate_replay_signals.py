#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.replay_provider import DEFAULT_TIMEFRAMES, ReplayMarketStateProvider
from vegas.signal_engine import UniversalVegasSignalEngine
from vegas.workspace import dev_data_root, dev_signals_root, find_workspace_root


WORKSPACE_ROOT = find_workspace_root(SIGNAL_ENGINE_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate neutral Vegas replay signal packets.")
    parser.add_argument("--asset", required=True, help="Canonical asset, e.g. BTC")
    parser.add_argument("--start", required=True, help="UTC ISO start timestamp")
    parser.add_argument("--end", required=True, help="UTC ISO end timestamp")
    parser.add_argument("--out", help="Output JSONL path. Defaults under dev/signals/vegas_ema.")
    parser.add_argument("--data-root", default=str(dev_data_root(WORKSPACE_ROOT)))
    parser.add_argument("--context-bars", type=int, default=80)
    parser.add_argument("--proximity-threshold", default="0.002", help="0.002 means 0.2%%")
    parser.add_argument("--vote-threshold", type=int, default=3)
    parser.add_argument("--timeframes", nargs="*", default=list(DEFAULT_TIMEFRAMES))
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def main() -> int:
    args = parse_args()
    start = parse_timestamp(args.start)
    end = parse_timestamp(args.end)
    provider = ReplayMarketStateProvider(
        asset=args.asset,
        timeframes=args.timeframes,
        context_bars=args.context_bars,
        training_root=args.data_root,
    )
    engine = UniversalVegasSignalEngine(
        proximity_threshold=Decimal(args.proximity_threshold),
        vote_threshold=args.vote_threshold,
    )

    output_path = Path(args.out) if args.out else (
        dev_signals_root(WORKSPACE_ROOT)
        / "vegas_ema"
        / args.asset.upper()
        / f"{start.strftime('%Y%m%dT%H%M%SZ')}_{end.strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scanned = 0
    skipped_insufficient_context = 0
    emitted = 0

    with output_path.open("w") as handle:
        for candle in provider.raw_5m:
            if candle.ts < start:
                continue
            if candle.ts > end:
                break
            try:
                snapshot = provider.snapshot_at(candle.ts)
            except ValueError as error:
                if "Not enough completed" not in str(error):
                    raise
                skipped_insufficient_context += 1
                continue

            packet = engine.scan(snapshot)
            scanned += 1
            if packet is None:
                continue
            handle.write(json.dumps(packet.to_dict(), separators=(",", ":")) + "\n")
            emitted += 1

    print(
        json.dumps(
            {
                "asset": args.asset.upper(),
                "start": start.isoformat().replace("+00:00", "Z"),
                "end": end.isoformat().replace("+00:00", "Z"),
                "scanned": scanned,
                "skipped_insufficient_context": skipped_insufficient_context,
                "emitted": emitted,
                "proximity_threshold": args.proximity_threshold,
                "vote_threshold": args.vote_threshold,
                "out": str(output_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
