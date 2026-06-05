#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.candle_store import (
    DERIVED_TIMEFRAMES,
    asset_to_okx_swap,
    candle_summary,
    dedupe_sort_candles,
    derived_path,
    rebuild_derived,
    raw_5m_path,
    seed_live_from_training,
    write_candles,
    write_metadata,
)
from vegas.replay_provider import load_candles
from vegas.schemas import Candle
from vegas.workspace import dev_data_root, find_workspace_root, live_data_root


WORKSPACE_ROOT = find_workspace_root(SIGNAL_ENGINE_ROOT)


PAGE_LIMIT = 300
REQUEST_RETRIES = 2
SUBPROCESS_TIMEOUT_SECONDS = 8
RATE_LIMIT_SLEEP = 0.25
DEFAULT_BACKFILL_DAYS = 760
DEFAULT_FRESH_CACHE_MINUTES = 15


@dataclass(frozen=True)
class OkxCandle:
    ts_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    vol_ccy: str
    vol_ccy_quote: str
    confirm: str

    def to_candle(self) -> Candle:
        return Candle(
            ts=datetime.fromtimestamp(self.ts_ms / 1000, UTC),
            open=Decimal(self.open),
            high=Decimal(self.high),
            low=Decimal(self.low),
            close=Decimal(self.close),
            volume=Decimal(self.volume),
            vol_ccy=Decimal(self.vol_ccy),
            vol_ccy_quote=Decimal(self.vol_ccy_quote),
            confirm=int(self.confirm),
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Update local OKX live 5m cache and derived candles.")
    parser.add_argument("--asset", required=True, help="Standalone asset ticker, e.g. BTC")
    parser.add_argument("--inst-id", help="OKX instrument override, e.g. BTC-USDT-SWAP")
    parser.add_argument("--live-root", default=str(live_data_root(WORKSPACE_ROOT)))
    parser.add_argument("--training-root", default=str(dev_data_root(WORKSPACE_ROOT)))
    parser.add_argument("--backfill-days", type=int, default=DEFAULT_BACKFILL_DAYS)
    parser.add_argument("--fresh-cache-minutes", type=int, default=DEFAULT_FRESH_CACHE_MINUTES)
    parser.add_argument("--skip-fetch", action="store_true", help="Only seed/rebuild local data.")
    return parser.parse_args()


def fetch_okx_page(inst_id: str, after: int | None = None) -> list[OkxCandle]:
    cmd = ["okx", "market", "candles", inst_id, "--bar", "5m", "--limit", str(PAGE_LIMIT), "--json"]
    if after is not None:
        cmd.extend(["--after", str(after)])

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            result = subprocess.run(
                cmd,
                check=True,
                capture_output=True,
                text=True,
                timeout=SUBPROCESS_TIMEOUT_SECONDS,
            )
            payload = json.loads(result.stdout)
            if not payload:
                return []
            payload.reverse()
            return [
                OkxCandle(
                    ts_ms=int(item[0]),
                    open=item[1],
                    high=item[2],
                    low=item[3],
                    close=item[4],
                    volume=item[5],
                    vol_ccy=item[6],
                    vol_ccy_quote=item[7],
                    confirm=item[8],
                )
                for item in payload
            ]
        except (subprocess.SubprocessError, json.JSONDecodeError) as error:
            last_error = error
            if attempt == REQUEST_RETRIES:
                break
            time.sleep(min(2**attempt, 8))

    raise RuntimeError(f"OKX candle fetch failed after {REQUEST_RETRIES} attempts: {last_error}")


def fetch_since(inst_id: str, start_ms: int | None) -> list[Candle]:
    fetched: list[Candle] = []
    after = None
    while True:
        page = fetch_okx_page(inst_id, after=after)
        if not page:
            break

        confirmed = [item for item in page if item.confirm == "1"]
        for item in confirmed:
            if start_ms is None or item.ts_ms > start_ms:
                fetched.append(item.to_candle())

        oldest = page[0].ts_ms
        if start_ms is not None and oldest <= start_ms:
            break
        after = oldest - 1
        time.sleep(RATE_LIMIT_SLEEP)
    return fetched


def cache_is_fresh(candles: list[Candle], fresh_minutes: int) -> bool:
    if not candles:
        return False
    max_age = timedelta(minutes=fresh_minutes)
    return datetime.now(UTC) - candles[-1].ts <= max_age


def derived_cache_complete(live_root: Path, asset: str) -> bool:
    return all(derived_path(live_root, asset, timeframe).exists() for timeframe in DERIVED_TIMEFRAMES)


def existing_derived_summary(live_root: Path, asset: str) -> dict[str, dict[str, object]]:
    summaries: dict[str, dict[str, object]] = {}
    for timeframe in DERIVED_TIMEFRAMES:
        path = derived_path(live_root, asset, timeframe)
        if path.exists():
            summaries[timeframe] = candle_summary(load_candles(path), path)
    return summaries


def main() -> int:
    args = parse_args()
    asset = args.asset.upper()
    inst_id = args.inst_id or asset_to_okx_swap(asset)
    live_root = Path(args.live_root)
    training_root = Path(args.training_root)
    raw_path = raw_5m_path(live_root, asset)

    seeded = seed_live_from_training(asset, live_root, training_root)
    existing = load_candles(raw_path) if raw_path.exists() else []
    fetch_status = "skipped" if args.skip_fetch else "not_attempted"
    fetch_error: str | None = None
    should_rebuild = True

    has_derived_cache = derived_cache_complete(live_root, asset)

    if existing and not args.skip_fetch and has_derived_cache and cache_is_fresh(
        existing,
        args.fresh_cache_minutes,
    ):
        fetch_status = "fresh_cache"
        should_rebuild = False
    elif not existing and not args.skip_fetch:
        target_start = datetime.now(UTC) - timedelta(days=args.backfill_days)
        start_ms = int(target_start.timestamp() * 1000)
        existing = fetch_since(inst_id, start_ms)
        fetch_status = "fetched"
    elif not args.skip_fetch:
        latest_ms = int(existing[-1].ts.timestamp() * 1000) if existing else None
        try:
            fetched = fetch_since(inst_id, latest_ms)
            existing = dedupe_sort_candles(existing + fetched)
            fetch_status = "fetched" if fetched else "no_new_candles"
        except RuntimeError as error:
            fetch_error = str(error)
            fetch_status = "cache_fallback"
            should_rebuild = not has_derived_cache

    if not existing:
        raise SystemExit(f"No live candles available for {asset}; seed or fetch failed")

    if should_rebuild:
        write_candles(raw_path, dedupe_sort_candles([c for c in existing if c.confirm == 1]))
        derived_summary = rebuild_derived(live_root, asset)
    else:
        derived_summary = existing_derived_summary(live_root, asset)
    write_metadata(live_root, asset, inst_id, "okx", derived_summary)

    raw_candles = load_candles(raw_path)
    print(
        json.dumps(
            {
                "asset": asset,
                "inst_id": inst_id,
                "seeded_from_training": seeded,
                "fetch_status": fetch_status,
                "fetch_error": fetch_error,
                "raw_rows": len(raw_candles),
                "raw_start": raw_candles[0].ts.isoformat().replace("+00:00", "Z"),
                "raw_end": raw_candles[-1].ts.isoformat().replace("+00:00", "Z"),
                "live_root": str(live_root),
                "derived": derived_summary,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
