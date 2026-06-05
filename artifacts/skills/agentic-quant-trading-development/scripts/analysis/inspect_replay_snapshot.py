#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
SRC = WORKSPACE_ROOT / "artifacts" / "signal_engine" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.replay_provider import DEFAULT_TIMEFRAMES, ReplayMarketStateProvider
from vegas.workspace import dev_data_root, find_workspace_root


WORKSPACE_ROOT = find_workspace_root(WORKSPACE_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect a live-like replay market snapshot.")
    parser.add_argument("--asset", required=True, help="Canonical asset, e.g. BTC")
    parser.add_argument("--timestamp", required=True, help="UTC ISO timestamp, e.g. 2023-05-11T05:15:00Z")
    parser.add_argument("--context-bars", type=int, default=80)
    parser.add_argument("--timeframes", nargs="*", default=list(DEFAULT_TIMEFRAMES))
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def main() -> int:
    args = parse_args()
    provider = ReplayMarketStateProvider(
        asset=args.asset,
        timeframes=args.timeframes,
        context_bars=args.context_bars,
        training_root=dev_data_root(WORKSPACE_ROOT),
    )
    snapshot = provider.snapshot_at(parse_timestamp(args.timestamp))
    print(json.dumps(snapshot.to_dict(), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
