#!/usr/bin/env python3
"""Generate deduplicated replay signals for a date range and persist as individual JSON files.

Dedup: 2h rolling window. A new signal is kept only if at least 2 hours have passed
since the last emitted signal.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.replay_provider import DEFAULT_TIMEFRAMES, ReplayMarketStateProvider  # noqa: E402
from vegas.packet_format import write_signal_packet  # noqa: E402
from vegas.signal_engine import UniversalVegasSignalEngine  # noqa: E402
from vegas.workspace import dev_data_root, dev_signals_root, find_workspace_root  # noqa: E402


WORKSPACE_ROOT = find_workspace_root(SIGNAL_ENGINE_ROOT)
SIGNAL_ENGINE_ID = "vegas_ema"
DEFAULT_MARKET_DATA_TIMEFRAMES = ("2h", "4h", "8h", "12h", "1d")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate deduplicated replay signals as individual JSON files."
    )
    parser.add_argument("--asset", required=True, help="Canonical asset, e.g. BTC")
    parser.add_argument("--start", required=True, help="UTC ISO start timestamp")
    parser.add_argument("--end", required=True, help="UTC ISO end timestamp")
    parser.add_argument(
        "--out-dir", help="Output directory for individual signal JSON files. Defaults under dev/signals/vegas_ema."
    )
    parser.add_argument(
        "--data-root",
        default=str(dev_data_root(WORKSPACE_ROOT)),
        help="Legacy CSV data root. Used only when --database-url is omitted.",
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Load canonical candles from market_data_refs/Parquet instead of legacy CSV data.",
    )
    parser.add_argument(
        "--workspace-root",
        default=str(WORKSPACE_ROOT),
        help="Workspace root for resolving relative Parquet storage_uri values.",
    )
    parser.add_argument("--context-bars", type=int, default=80)
    parser.add_argument("--proximity-threshold", default="0.002", help="0.002 means 0.2%%")
    parser.add_argument("--vote-threshold", type=int, default=3)
    parser.add_argument("--window-minutes", type=int, default=120, help="Dedup rolling window in minutes")
    return parser.parse_args()


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def default_signal_set_id(start: datetime, asset: str, window_minutes: int, vote_threshold: int) -> str:
    if window_minutes % 1440 == 0:
        dedupe = f"{window_minutes // 1440}d"
    elif window_minutes % 60 == 0:
        dedupe = f"{window_minutes // 60}h"
    else:
        dedupe = f"{window_minutes}m"
    return f"{start.year}-{asset.upper()}-{dedupe}-dedupe-vote{vote_threshold}"


def write_manifest(
    signal_set_dir: Path,
    args: argparse.Namespace,
    start: datetime,
    end: datetime,
    packet_count: int,
) -> None:
    signal_set_id = signal_set_dir.name
    manifest = {
        "schema_version": "0.1",
        "signal_set_id": signal_set_id,
        "signal_engine_id": SIGNAL_ENGINE_ID,
        "signal_family": "vegas_ema",
        "asset": args.asset.upper(),
        "signal_engine_version": "0.1",
        "data_manifest": f"dev/data/manifests/{args.asset.upper()}.json",
        "parameters": {
            "proximity_threshold": args.proximity_threshold,
            "vote_threshold": args.vote_threshold,
            "timeframes": list(DEFAULT_TIMEFRAMES),
            "context_bars": args.context_bars,
            "dedupe_window_minutes": args.window_minutes,
        },
        "packet_count": packet_count,
        "start_ts": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_ts": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "packets_path": "packets/",
        "packet_filename_format": "YYYYMMDDTHHMMSSZ.json",
    }
    (signal_set_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")


def build_provider(args: argparse.Namespace) -> ReplayMarketStateProvider:
    if not args.database_url:
        return ReplayMarketStateProvider(
            asset=args.asset,
            context_bars=args.context_bars,
            training_root=args.data_root,
        )

    from quant_terminal_api.repositories.runtime import RuntimeRepository
    from quant_terminal_sdk.market_data_reader import MarketDataReader
    from vegas.schemas import Candle

    reader = MarketDataReader(
        repository=RuntimeRepository(args.database_url),
        workspace_root=Path(args.workspace_root),
    )

    def to_vegas_candle(candle) -> Candle:
        return Candle(
            ts=candle.timestamp,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            vol_ccy=candle.vol_ccy,
            vol_ccy_quote=candle.vol_ccy_quote,
            confirm=candle.confirm,
        )

    raw_5m = [
        to_vegas_candle(candle)
        for candle in reader.get_candles(asset=args.asset, timeframe="5m", origin="raw")
    ]
    derived = {
        timeframe: [
            to_vegas_candle(candle)
            for candle in reader.get_candles(asset=args.asset, timeframe=timeframe, origin="derived")
        ]
        for timeframe in DEFAULT_MARKET_DATA_TIMEFRAMES
    }
    return ReplayMarketStateProvider(
        asset=args.asset,
        context_bars=args.context_bars,
        raw_5m=raw_5m,
        derived_candles=derived,
    )


def main() -> int:
    args = parse_args()
    start = parse_timestamp(args.start)
    end = parse_timestamp(args.end)
    window = timedelta(minutes=args.window_minutes)

    provider = build_provider(args)
    engine = UniversalVegasSignalEngine(
        proximity_threshold=Decimal(args.proximity_threshold),
        vote_threshold=args.vote_threshold,
    )

    canonical_default = args.out_dir is None
    signal_set_id = default_signal_set_id(start, args.asset, args.window_minutes, args.vote_threshold)
    out_dir = Path(args.out_dir) if args.out_dir else (
        dev_signals_root(WORKSPACE_ROOT) / SIGNAL_ENGINE_ID / args.asset.upper() / signal_set_id / "packets"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    scanned = 0
    skipped_insufficient_context = 0
    raw_signals = 0
    emitted = 0
    last_emitted_at: datetime | None = None

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

        raw_signals += 1

        # 2h rolling window dedup
        if last_emitted_at is not None and (candle.ts - last_emitted_at) < window:
            continue

        # Write as individual JSON file
        signal_id = f"{candle.ts.strftime('%Y%m%dT%H%M%S')}Z"
        file_path = out_dir / f"{signal_id}.json"
        write_signal_packet(file_path, packet.to_dict())

        last_emitted_at = candle.ts
        emitted += 1

    if canonical_default or out_dir.name == "packets":
        write_manifest(out_dir.parent, args, start, end, emitted)

    summary = {
        "asset": args.asset.upper(),
        "start": start.isoformat().replace("+00:00", "Z"),
        "end": end.isoformat().replace("+00:00", "Z"),
        "scanned": scanned,
        "skipped_insufficient_context": skipped_insufficient_context,
        "raw_signals": raw_signals,
        "dedup_emitted": emitted,
        "proximity_threshold": args.proximity_threshold,
        "vote_threshold": args.vote_threshold,
        "window_minutes": args.window_minutes,
        "out_dir": str(out_dir),
        "manifest": str(out_dir.parent / "manifest.json") if (canonical_default or out_dir.name == "packets") else None,
    }
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
