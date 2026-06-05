#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path


WORKSPACE_ROOT = Path(__file__).resolve().parents[5]
SRC = WORKSPACE_ROOT / "artifacts" / "signal_engine" / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.workspace import dev_data_root, find_workspace_root


WORKSPACE_ROOT = find_workspace_root(WORKSPACE_ROOT)
DEFAULT_DATA_ROOT = dev_data_root(WORKSPACE_ROOT)

CANONICAL_COLUMNS = [
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "vol_ccy",
    "vol_ccy_quote",
    "confirm",
]

TIMEFRAME_SPECS = OrderedDict(
    [
        ("5m", timedelta(minutes=5)),
        ("2h", timedelta(hours=2)),
        ("4h", timedelta(hours=4)),
        ("8h", timedelta(hours=8)),
        ("12h", timedelta(hours=12)),
        ("1d", timedelta(days=1)),
    ]
)


@dataclass
class Candle:
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vol_ccy: Decimal
    vol_ccy_quote: Decimal
    confirm: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build canonical dev/data from an existing raw 5m source.")
    parser.add_argument("--asset", required=True, help="Canonical asset name, e.g. BTC or ETH")
    parser.add_argument("--source", required=True, help="Logical source label, e.g. okx or binance")
    parser.add_argument("--source-file", required=True, help="Path to the source 5m CSV file")
    parser.add_argument("--source-ts-format", choices=["iso", "ms"], default="iso", help="Timestamp format in the source CSV")
    parser.add_argument("--data-root", default=str(DEFAULT_DATA_ROOT), help="Canonical dev data root.")
    return parser.parse_args()


def parse_ts(value: str, ts_format: str) -> datetime:
    if ts_format == "ms":
        return datetime.fromtimestamp(int(value) / 1000, UTC)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def format_ts(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_decimal(value: str) -> Decimal:
    return Decimal(value)


def format_decimal(value: Decimal) -> str:
    rendered = format(value.normalize(), "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def floor_ts(ts: datetime, delta: timedelta) -> datetime:
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    seconds = int((ts - epoch).total_seconds())
    bucket = int(delta.total_seconds())
    floored = seconds - (seconds % bucket)
    return epoch + timedelta(seconds=floored)


def load_source_candles(path: Path, ts_format: str) -> list[Candle]:
    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            confirm = int(row["confirm"])
            if confirm != 1:
                continue
            candles.append(
                Candle(
                    ts=parse_ts(row["ts"], ts_format),
                    open=parse_decimal(row["open"]),
                    high=parse_decimal(row["high"]),
                    low=parse_decimal(row["low"]),
                    close=parse_decimal(row["close"]),
                    volume=parse_decimal(row["volume"]),
                    vol_ccy=parse_decimal(row["vol_ccy"] if "vol_ccy" in row else row["volCcy"]),
                    vol_ccy_quote=parse_decimal(row["vol_ccy_quote"] if "vol_ccy_quote" in row else row["volCcyQuote"]),
                    confirm=confirm,
                )
            )

    candles.sort(key=lambda candle: candle.ts)

    deduped: list[Candle] = []
    last_ts: datetime | None = None
    for candle in candles:
        if candle.ts == last_ts:
            deduped[-1] = candle
            continue
        deduped.append(candle)
        last_ts = candle.ts
    return deduped


def aggregate_timeframe(base_candles: list[Candle], timeframe: str) -> list[Candle]:
    if timeframe == "5m":
        return base_candles

    delta = TIMEFRAME_SPECS[timeframe]
    base_delta = TIMEFRAME_SPECS["5m"]
    expected_bars = int(delta / base_delta)

    buckets: OrderedDict[datetime, list[Candle]] = OrderedDict()
    for candle in base_candles:
        bucket_start = floor_ts(candle.ts, delta)
        buckets.setdefault(bucket_start, []).append(candle)

    aggregated: list[Candle] = []
    for bucket_start, candles in buckets.items():
        if len(candles) != expected_bars:
            continue

        expected_ts = bucket_start
        complete = True
        for candle in candles:
            if candle.ts != expected_ts:
                complete = False
                break
            expected_ts += base_delta

        if not complete:
            continue

        aggregated.append(
            Candle(
                ts=bucket_start,
                open=candles[0].open,
                high=max(candle.high for candle in candles),
                low=min(candle.low for candle in candles),
                close=candles[-1].close,
                volume=sum((candle.volume for candle in candles), start=Decimal("0")),
                vol_ccy=sum((candle.vol_ccy for candle in candles), start=Decimal("0")),
                vol_ccy_quote=sum((candle.vol_ccy_quote for candle in candles), start=Decimal("0")),
                confirm=1,
            )
        )

    return aggregated


def write_candles(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(CANONICAL_COLUMNS)
        for candle in candles:
            writer.writerow(
                [
                    format_ts(candle.ts),
                    format_decimal(candle.open),
                    format_decimal(candle.high),
                    format_decimal(candle.low),
                    format_decimal(candle.close),
                    format_decimal(candle.volume),
                    format_decimal(candle.vol_ccy),
                    format_decimal(candle.vol_ccy_quote),
                    candle.confirm,
                ]
            )


def summary(candles: list[Candle]) -> dict[str, object]:
    return {
        "rows": len(candles),
        "start": format_ts(candles[0].ts) if candles else None,
        "end": format_ts(candles[-1].ts) if candles else None,
    }


def load_metadata(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"assets": {}}
    metadata = json.loads(path.read_text())
    assets = metadata.get("assets", {})
    return {"assets": assets if isinstance(assets, dict) else {}}


def main() -> int:
    args = parse_args()
    asset = args.asset.upper()
    source_file = Path(args.source_file).resolve()
    data_root = Path(args.data_root)
    base_candles = load_source_candles(source_file, args.source_ts_format)

    raw_root = data_root / "raw" / asset / "5m"
    derived_root = data_root / "derived" / asset

    raw_output = raw_root / "candles.csv"
    write_candles(raw_output, base_candles)

    derived_summary: dict[str, dict[str, object]] = {}
    for timeframe in TIMEFRAME_SPECS:
        timeframe_candles = aggregate_timeframe(base_candles, timeframe)
        output_path = derived_root / timeframe / "candles.csv"
        write_candles(output_path, timeframe_candles)
        derived_summary[timeframe] = summary(timeframe_candles)

    data_root.mkdir(parents=True, exist_ok=True)
    metadata_path = data_root / "metadata.json"
    metadata = load_metadata(metadata_path)
    metadata.setdefault("assets", {})
    metadata["assets"][asset] = {
        "source": args.source,
        "canonical_source": {
            "seed_file": str(source_file),
            "timeframe": "5m",
            "schema": CANONICAL_COLUMNS,
            "closed_candles_only": True,
            "timestamp_timezone": "UTC",
        },
        "raw": {
            "5m": {
                "path": str(raw_output),
                **summary(base_candles),
            }
        },
        "derived": {
            timeframe: {
                "path": str(derived_root / timeframe / "candles.csv"),
                "rule": "aggregated from canonical raw 5m on UTC bucket boundaries",
                **derived_summary[timeframe],
            }
            for timeframe in TIMEFRAME_SPECS
        },
    }
    metadata["generated_at_utc"] = format_ts(datetime.now(UTC))
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")

    print(json.dumps(metadata["assets"][asset], indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
