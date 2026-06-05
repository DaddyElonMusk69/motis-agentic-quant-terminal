#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests


HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
PAGE_LIMIT = 300
RATE_LIMIT_BURST = 18
RATE_LIMIT_SLEEP = 2.1
COLUMNS = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]


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
    parser = argparse.ArgumentParser(description="Append confirmed OKX 5m candles forward to a target timestamp.")
    parser.add_argument("--inst-id", required=True, help="OKX instrument id, e.g. ZEC-USDT-SWAP")
    parser.add_argument("--target-end", required=True, help="Fetch until latest candle is at or after this UTC ISO timestamp.")
    parser.add_argument("--out", required=True, help="Canonical raw 5m CSV path.")
    return parser.parse_args()


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def format_ts_ms(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def ts_to_ms(value: str) -> int:
    if value.isdigit():
        return int(value)
    return int(parse_ts(value).timestamp() * 1000)


def load_existing(path: Path) -> list[Candle]:
    if not path.exists():
        return []
    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candles.append(
                Candle(
                    ts_ms=ts_to_ms(row["ts"]),
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                    vol_ccy=row.get("vol_ccy", row.get("volCcy", "0")),
                    vol_ccy_quote=row.get("vol_ccy_quote", row.get("volCcyQuote", "0")),
                    confirm=str(row.get("confirm", "1")),
                )
            )
    return sorted(dedupe_candles(candles), key=lambda candle: candle.ts_ms)


def dedupe_candles(candles: list[Candle]) -> list[Candle]:
    by_ts: dict[int, Candle] = {}
    for candle in candles:
        by_ts[candle.ts_ms] = candle
    return sorted(by_ts.values(), key=lambda candle: candle.ts_ms)


def write_candles(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    candles = dedupe_candles(candles)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        for candle in candles:
            writer.writerow(
                [
                    format_ts_ms(candle.ts_ms),
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


def parse_okx_rows(rows: list[list[Any]]) -> list[Candle]:
    candles = [
        Candle(
            ts_ms=int(row[0]),
            open=str(row[1]),
            high=str(row[2]),
            low=str(row[3]),
            close=str(row[4]),
            volume=str(row[5]),
            vol_ccy=str(row[6]),
            vol_ccy_quote=str(row[7]),
            confirm=str(row[8]),
        )
        for row in rows
    ]
    return sorted(candles, key=lambda candle: candle.ts_ms)


def fetch_page(inst_id: str, after: int | None = None, before: int | None = None, retries: int = 4) -> list[Candle]:
    params = {"instId": inst_id, "bar": "5m", "limit": str(PAGE_LIMIT)}
    if after is not None and before is not None:
        raise ValueError("use either after or before, not both")
    if after is not None:
        params["after"] = str(after)
    if before is not None:
        params["before"] = str(before)

    for attempt in range(retries):
        for url in (CANDLES_URL, HISTORY_URL):
            try:
                response = requests.get(url, params=params, timeout=30)
                response.raise_for_status()
                payload = response.json()
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                return []

            if payload.get("code") != "0":
                continue
            rows = payload.get("data") or []
            if not rows:
                continue
            return parse_okx_rows(rows)

        if attempt < retries - 1:
            time.sleep(min(2**attempt, 8))

    return []


def append_forward(existing: list[Candle], fetched: list[Candle], target_end_ms: int) -> tuple[list[Candle], int]:
    if not existing:
        combined = [candle for candle in fetched if candle.confirm == "1" and candle.ts_ms <= target_end_ms]
        return dedupe_candles(combined), len(combined)

    latest = existing[-1].ts_ms
    new_candles = [
        candle
        for candle in fetched
        if candle.confirm == "1" and latest < candle.ts_ms <= target_end_ms
    ]
    return dedupe_candles(existing + new_candles), len(new_candles)


def has_5m_gaps(candles: list[Candle]) -> bool:
    if len(candles) < 2:
        return False
    expected_step_ms = 5 * 60 * 1000
    ordered = sorted(candles, key=lambda candle: candle.ts_ms)
    return any(
        right.ts_ms - left.ts_ms != expected_step_ms
        for left, right in zip(ordered, ordered[1:])
    )


def contiguous_anchor_before_target(candles: list[Candle], target_end_ms: int) -> int:
    """Return the last timestamp before a gap that must be repaired for target_end."""
    if not candles:
        return 0
    expected_step_ms = 5 * 60 * 1000
    ordered = sorted(candles, key=lambda candle: candle.ts_ms)
    anchor = ordered[0].ts_ms
    for left, right in zip(ordered, ordered[1:]):
        if left.ts_ms >= target_end_ms:
            return target_end_ms
        if right.ts_ms - left.ts_ms != expected_step_ms:
            if left.ts_ms < target_end_ms:
                return left.ts_ms
        anchor = right.ts_ms
    return min(anchor, target_end_ms)


def has_gap_between(candles: list[Candle], start_ms: int, end_ms: int) -> bool:
    bounded = [candle for candle in candles if start_ms <= candle.ts_ms <= end_ms]
    if not bounded:
        return True
    if bounded[-1].ts_ms < end_ms:
        return True
    return has_5m_gaps(bounded)


def main() -> int:
    args = parse_args()
    out_path = Path(args.out)
    target_end_ms = int(parse_ts(args.target_end).timestamp() * 1000)

    existing = load_existing(out_path)
    if not existing:
        raise SystemExit(f"No existing seed file found at {out_path}")

    repair_anchor = contiguous_anchor_before_target(existing, target_end_ms)
    if existing[-1].ts_ms >= target_end_ms and not has_gap_between(existing, repair_anchor, target_end_ms):
        print(
            json.dumps(
                {
                    "status": "already_satisfied",
                    "inst_id": args.inst_id,
                    "rows": len(existing),
                    "added": 0,
                    "start_ts": format_ts_ms(existing[0].ts_ms),
                    "end_ts": format_ts_ms(existing[-1].ts_ms),
                    "target_end": args.target_end,
                    "requests": 0,
                    "out": str(out_path),
                },
                indent=2,
            )
        )
        return 0

    request_count = 0
    original_latest = repair_anchor
    current = existing
    cursor = target_end_ms + 1
    new_candles: list[Candle] = []

    while True:
        # OKX's `after` parameter pages backward from the cursor. For forward repair,
        # collect the whole bounded missing segment by walking backward from target_end
        # until the fetched page bridges to the existing tail.
        page = fetch_page(args.inst_id, after=cursor)
        request_count += 1
        if not page:
            break

        bounded_page = [
            candle
            for candle in page
            if candle.confirm == "1" and original_latest < candle.ts_ms <= target_end_ms
        ]
        new_candles.extend(bounded_page)
        oldest = min(candle.ts_ms for candle in page)
        newest = max(candle.ts_ms for candle in page)
        if oldest <= original_latest + (5 * 60 * 1000):
            break
        if newest <= original_latest:
            break
        cursor = oldest - 1

        if request_count % RATE_LIMIT_BURST == 0:
            time.sleep(RATE_LIMIT_SLEEP)

    current = dedupe_candles(existing + new_candles)
    write_candles(out_path, current)
    gap_after_original_tail = has_gap_between(current, original_latest, target_end_ms)

    print(
        json.dumps(
            {
                "status": "ok" if current[-1].ts_ms >= target_end_ms and not gap_after_original_tail else "incomplete",
                "inst_id": args.inst_id,
                "rows": len(current),
                "added": len(dedupe_candles(new_candles)),
                "start_ts": format_ts_ms(current[0].ts_ms),
                "end_ts": format_ts_ms(current[-1].ts_ms),
                "target_end": args.target_end,
                "gap_after_original_tail": gap_after_original_tail,
                "requests": request_count,
                "out": str(out_path),
            },
            indent=2,
        )
    )
    return 0 if current[-1].ts_ms >= target_end_ms and not gap_after_original_tail else 1


if __name__ == "__main__":
    raise SystemExit(main())
