#!/usr/bin/env python3
"""Bulk-fetch OKX 5m candles with progress feedback. Creates seed if needed.

Uses history-candles endpoint (primary) which supports full historical range.
Falls back to candles endpoint for recent data.
"""
from __future__ import annotations

import csv
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
PAGE_LIMIT = 300
BURST = 18

COLUMNS = ["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]


def fetch_page(inst_id: str, after: int | None = None, retries: int = 3) -> list[dict]:
    """Fetch one page of candles with retry logic.

    Uses CLI's routing logic: history-candles when after > 2 days ago,
    candles otherwise. Retries on empty responses (rate limits).
    """
    params = {"instId": inst_id, "bar": "5m", "limit": str(PAGE_LIMIT)}
    if after is not None:
        params["after"] = str(after)

    # Determine primary endpoint (matching OKX CLI logic)
    use_history = after is not None and after < (int(time.time() * 1000) - 172800000)  # 2 days
    primary = HISTORY_URL if use_history else CANDLES_URL
    fallback = CANDLES_URL if use_history else HISTORY_URL

    for attempt in range(retries):
        for url in (primary, fallback):
            try:
                resp = requests.get(url, params=params, timeout=30)
                resp.raise_for_status()
                payload = resp.json()
                if payload.get("code") != "0":
                    if attempt < retries - 1:
                        time.sleep(1)
                        continue
                    break
                raw = payload.get("data", [])
                if not raw:
                    if attempt < retries - 1:
                        time.sleep(1)
                        continue
                    break
                # API returns newest first, reverse to chronological
                raw.reverse()
                return [
                    {
                        "ts": int(row[0]),
                        "open": row[1],
                        "high": row[2],
                        "low": row[3],
                        "close": row[4],
                        "volume": row[5],
                        "vol_ccy": row[6],
                        "vol_ccy_quote": row[7],
                        "confirm": row[8],
                    }
                    for row in raw
                ]
            except Exception:
                if attempt < retries - 1:
                    time.sleep(1)
                    continue
                break
        # If we got here, both endpoints failed — retry after delay
        if attempt < retries - 1:
            time.sleep(2)

    return []


def ts_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_earliest_ts(path: Path) -> tuple[int, int]:
    """Return (earliest_ts_ms, row_count) from existing CSV. Only reads first data row."""
    row_count = 0
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row_count == 0:
                ts_str = row["ts"]
                if ts_str.endswith("Z"):
                    earliest_ms = int(datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000)
                else:
                    earliest_ms = int(ts_str)
            row_count += 1
    return earliest_ms, row_count


def append_csv(path: Path, candles: list[dict]):
    """Append candles to CSV file. Assumes candles are in chronological order and all older than existing."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with path.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(COLUMNS)
            for c in candles:
                w.writerow([ts_to_iso(c["ts"]), c["open"], c["high"], c["low"],
                           c["close"], c["volume"], c["vol_ccy"], c["vol_ccy_quote"], c["confirm"]])
        return

    # Read existing, prepend new, rewrite — only done at save checkpoints
    existing = []
    with path.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ts_str = row["ts"]
            if ts_str.endswith("Z"):
                ts_ms = int(datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp() * 1000)
            else:
                ts_ms = int(ts_str)
            existing.append({"ts": ts_ms, "open": row["open"], "high": row["high"],
                           "low": row["low"], "close": row["close"], "volume": row["volume"],
                           "vol_ccy": row["vol_ccy"], "vol_ccy_quote": row["vol_ccy_quote"],
                           "confirm": row["confirm"]})

    all_candles = candles + existing
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(COLUMNS)
        for c in all_candles:
            w.writerow([ts_to_iso(c["ts"]), c["open"], c["high"], c["low"],
                       c["close"], c["volume"], c["vol_ccy"], c["vol_ccy_quote"], c["confirm"]])


def main():
    if len(sys.argv) != 4:
        print("Usage: fetch_okx_5m_bulk.py <instId> <target_start_iso> <out_csv>")
        print("Example: fetch_okx_5m_bulk.py XRP-USDT-SWAP 2023-01-01T00:00:00Z dev/data/raw/XRP/5m/candles.csv")
        sys.exit(1)

    inst_id = sys.argv[1]
    target_dt = datetime.fromisoformat(sys.argv[2].replace("Z", "+00:00")).astimezone(UTC)
    target_ms = int(target_dt.timestamp() * 1000)
    out_path = Path(sys.argv[3])

    # Accumulate new pages in memory, flush to disk periodically
    new_pages: list[dict] = []  # oldest-first, to be prepended
    req_count = 0
    last_save = time.time()
    total_new = 0

    if out_path.exists():
        earliest_ms, existing_rows = load_earliest_ts(out_path)
        print(f"Existing: {existing_rows:,} candles, earliest: {ts_to_iso(earliest_ms)}")
    else:
        print("No existing file. Fetching seed page...")
        page = fetch_page(inst_id)
        if not page:
            print("ERROR: Could not fetch seed page", file=sys.stderr)
            sys.exit(1)
        append_csv(out_path, page)
        earliest_ms = page[0]["ts"]
        total_new = len(page)
        req_count = 1
        print(f"  Seed: {len(page)} candles, {ts_to_iso(earliest_ms)} → {ts_to_iso(page[-1]['ts'])}")

    # Extend backward
    while earliest_ms > target_ms:
        after = earliest_ms - 1
        page = fetch_page(inst_id, after=after)
        req_count += 1

        if not page:
            print(f"  No more data at {ts_to_iso(after)} — stopping")
            break

        # Page should be entirely before our earliest. Just collect.
        if page[-1]["ts"] >= earliest_ms:
            # Overlap possible — filter out duplicates
            page = [c for c in page if c["ts"] < earliest_ms]
            if not page:
                print(f"  All candles already present at {ts_to_iso(after)} — stopping")
                break

        new_pages.extend(page)
        total_new += len(page)
        earliest_ms = page[0]["ts"]  # page is chronological, oldest first

        # Save checkpoint every 30s
        now = time.time()
        if now - last_save >= 30:
            if new_pages:
                append_csv(out_path, new_pages)
                new_pages.clear()
            pct = min((page[-1]["ts"] - earliest_ms) / max(page[-1]["ts"] - target_ms, 1) * 100, 100) if req_count > 5 else 0
            print(f"  [{req_count} reqs] +{total_new:,} new, earliest now: {ts_to_iso(earliest_ms)}")
            last_save = now

        # Rate limit
        if req_count % BURST == 0:
            time.sleep(2.1)

    # Final flush
    if new_pages:
        append_csv(out_path, new_pages)

    _, final_rows = load_earliest_ts(out_path)
    print(f"\nDone! {final_rows:,} total candles, {ts_to_iso(earliest_ms)} → ...")
    print(f"Requests: {req_count}")
    print(f"Output: {out_path}")


if __name__ == "__main__":
    main()
