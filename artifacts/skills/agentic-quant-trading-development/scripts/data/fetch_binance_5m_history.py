#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
BINANCE_URL = "https://fapi.binance.com/fapi/v1/klines"
PAGE_LIMIT = 1500
RATE_LIMIT_SLEEP = 0.35
REQUEST_TIMEOUT_SECONDS = 20
REQUEST_RETRIES = 4
CHECKPOINT_EVERY_PAGES = 10


@dataclass(frozen=True)
class Candle:
    ts_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    vol_ccy_quote: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extend Binance 5m history backward to a target timestamp.")
    parser.add_argument("--symbol", required=True, help="Binance symbol, e.g. ETHUSDT")
    parser.add_argument("--target-start-ms", required=True, type=int, help="Fetch until earliest candle is at or before this UTC ms timestamp.")
    parser.add_argument("--out", required=True, help="Output CSV path.")
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore any existing output file and fetch a new history from the latest candles backward.",
    )
    return parser.parse_args()


def fetch_page(symbol: str, end_time_ms: int | None = None) -> list[Candle]:
    params = {
        "symbol": symbol,
        "interval": "5m",
        "limit": PAGE_LIMIT,
    }
    if end_time_ms is not None:
        params["endTime"] = str(end_time_ms)

    last_error: Exception | None = None
    for attempt in range(1, REQUEST_RETRIES + 1):
        try:
            response = requests.get(BINANCE_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            raw = response.json()
            if not raw:
                return []

            return [
                Candle(
                    ts_ms=int(item[0]),
                    open=item[1],
                    high=item[2],
                    low=item[3],
                    close=item[4],
                    volume=item[5],
                    vol_ccy_quote=item[7],
                )
                for item in raw
            ]
        except (requests.RequestException, ValueError) as error:
            last_error = error
            if attempt == REQUEST_RETRIES:
                break
            time.sleep(min(2**attempt, 8))

    raise RuntimeError(f"Binance fetch failed after {REQUEST_RETRIES} attempts: {last_error}")


def load_existing(path: Path) -> list[Candle]:
    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candles.append(
                Candle(
                    ts_ms=int(row["ts"]),
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    vol_ccy_quote=row.get("vol_ccy_quote", row.get("vol_ccy", "0")),
                )
            )
    candles.sort(key=lambda candle: candle.ts_ms)
    return candles


def write_candles(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"])
        for candle in candles:
            writer.writerow(
                [
                    candle.ts_ms,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.vol_ccy_quote,
                    "0.0",
                    1,
                ]
            )


def main() -> int:
    args = parse_args()
    out_path = Path(args.out)
    existing: list[Candle] = []
    if out_path.exists() and not args.fresh:
        existing = load_existing(out_path)

    if existing and existing[0].ts_ms <= args.target_start_ms:
        print(
            json.dumps(
                {
                    "status": "already_satisfied",
                    "rows": len(existing),
                    "earliest_ts_ms": existing[0].ts_ms,
                    "latest_ts_ms": existing[-1].ts_ms,
                    "target_start_ms": args.target_start_ms,
                },
                indent=2,
            )
        )
        return 0

    all_candles = existing
    request_count = 0
    end_time_ms = all_candles[0].ts_ms - 1 if all_candles else None

    while not all_candles or all_candles[0].ts_ms > args.target_start_ms:
        page = fetch_page(args.symbol, end_time_ms=end_time_ms)
        if not page:
            break

        page.sort(key=lambda candle: candle.ts_ms)
        existing_ts = {candle.ts_ms for candle in all_candles}
        new_page = [candle for candle in page if candle.ts_ms not in existing_ts]
        if not new_page:
            break

        all_candles = sorted(new_page + all_candles, key=lambda candle: candle.ts_ms)
        end_time_ms = all_candles[0].ts_ms - 1
        request_count += 1
        if request_count % CHECKPOINT_EVERY_PAGES == 0:
            write_candles(out_path, all_candles)
        time.sleep(RATE_LIMIT_SLEEP)

    write_candles(out_path, all_candles)

    print(
        json.dumps(
            {
                "status": "ok",
                "symbol": args.symbol,
                "rows": len(all_candles),
                "earliest_ts_ms": all_candles[0].ts_ms,
                "latest_ts_ms": all_candles[-1].ts_ms,
                "target_start_ms": args.target_start_ms,
                "requests": request_count,
                "out": str(out_path),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
