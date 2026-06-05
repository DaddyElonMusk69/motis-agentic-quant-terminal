#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
OKX_HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
OKX_FALLBACK_URL = "https://www.okx.com/api/v5/market/candles"
RATE_LIMIT_BURST = 18
RATE_LIMIT_SLEEP = 2.1
PAGE_LIMIT = 300


@dataclass(frozen=True)
class Candle:
    ts_ms: int
    open: str
    high: str
    low: str
    close: str
    volume: str
    vol_ccy: str
    vol_ccy_quote: str
    confirm: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extend OKX 5m history backward to a target timestamp.")
    parser.add_argument("--inst-id", required=True, help="OKX instrument id, e.g. ETH-USDT-SWAP")
    parser.add_argument("--target-start-ms", required=True, type=int, help="Fetch until earliest candle is at or before this UTC ms timestamp.")
    parser.add_argument("--out", required=True, help="Output CSV path.")
    return parser.parse_args()


def fetch_candles_page(inst_id: str, after: int | None = None) -> list[Candle]:
    params = {"instId": inst_id, "bar": "5m", "limit": str(PAGE_LIMIT)}
    if after is not None:
        params["after"] = str(after)

    for url in (OKX_HISTORY_URL, OKX_FALLBACK_URL):
        response = requests.get(url, params=params, timeout=20)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            continue
        raw = payload.get("data", [])
        if not raw:
            return []
        raw.reverse()
        return [
            Candle(
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
            for item in raw
        ]

    return []


def load_existing(path: Path) -> list[Candle]:
    if not path.exists():
        return []

    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            ts_raw = row["ts"]
            if ts_raw.endswith("Z"):
                ts_ms = int(datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                ts_ms = int(ts_raw)

            candles.append(
                Candle(
                    ts_ms=ts_ms,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    vol_ccy=row.get("vol_ccy", row.get("volCcy", "0")),
                    vol_ccy_quote=row.get("vol_ccy_quote", row.get("volCcyQuote", "0")),
                    confirm=row["confirm"],
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
            ts = datetime.fromtimestamp(candle.ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            writer.writerow(
                [
                    ts,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                    candle.vol_ccy,
                    candle.vol_ccy_quote,
                    candle.confirm,
                ]
            )


def main() -> int:
    args = parse_args()
    out_path = Path(args.out)
    existing = load_existing(out_path)

    if not existing:
        raise SystemExit(f"No existing seed file found at {out_path}")

    earliest = existing[0].ts_ms
    if earliest <= args.target_start_ms:
        print(
            json.dumps(
                {
                    "status": "already_satisfied",
                    "earliest_ts_ms": earliest,
                    "target_start_ms": args.target_start_ms,
                    "rows": len(existing),
                },
                indent=2,
            )
        )
        return 0

    all_candles = existing
    request_count = 0
    after = earliest - 1

    while all_candles and all_candles[0].ts_ms > args.target_start_ms:
        page = fetch_candles_page(args.inst_id, after=after)
        if not page:
            break

        existing_ts = {candle.ts_ms for candle in all_candles}
        new_page = [candle for candle in page if candle.ts_ms not in existing_ts]
        if not new_page:
            break

        all_candles = sorted(new_page + all_candles, key=lambda candle: candle.ts_ms)
        after = all_candles[0].ts_ms - 1
        request_count += 1

        if request_count % RATE_LIMIT_BURST == 0:
            time.sleep(RATE_LIMIT_SLEEP)

    write_candles(out_path, all_candles)

    print(
        json.dumps(
            {
                "status": "ok",
                "inst_id": args.inst_id,
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
