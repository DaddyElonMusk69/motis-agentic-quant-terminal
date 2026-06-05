from __future__ import annotations

import csv
import json
import shutil
import tempfile
from collections import OrderedDict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from vegas.schemas import Candle
from vegas.timeframes import TIMEFRAME_DELTAS, floor_timestamp


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

DERIVED_TIMEFRAMES = ("5m", "2h", "4h", "8h", "12h", "1d")


def parse_ts(value: str) -> datetime:
    value = value.strip()
    if value.isdigit():
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


def asset_to_okx_swap(asset: str) -> str:
    return f"{asset.upper()}-USDT-SWAP"


def raw_5m_path(root: str | Path, asset: str) -> Path:
    return Path(root) / "raw" / asset.upper() / "5m" / "candles.csv"


def derived_path(root: str | Path, asset: str, timeframe: str) -> Path:
    return Path(root) / "derived" / asset.upper() / timeframe / "candles.csv"


def load_candles(path: Path, confirmed_only: bool = False) -> list[Candle]:
    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            confirm = int(row["confirm"])
            if confirmed_only and confirm != 1:
                continue
            candles.append(
                Candle(
                    ts=parse_ts(row["ts"]),
                    open=parse_decimal(row["open"]),
                    high=parse_decimal(row["high"]),
                    low=parse_decimal(row["low"]),
                    close=parse_decimal(row["close"]),
                    volume=parse_decimal(row["volume"]),
                    vol_ccy=parse_decimal(row.get("vol_ccy", row.get("volCcy", "0"))),
                    vol_ccy_quote=parse_decimal(
                        row.get("vol_ccy_quote", row.get("volCcyQuote", "0"))
                    ),
                    confirm=confirm,
                )
            )
    return dedupe_sort_candles(candles)


def write_candles(path: Path, candles: list[Candle]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = ""
    with tempfile.NamedTemporaryFile("w", newline="", dir=path.parent, delete=False) as handle:
        tmp_name = handle.name
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
    Path(tmp_name).replace(path)


def dedupe_sort_candles(candles: list[Candle]) -> list[Candle]:
    by_ts: dict[datetime, Candle] = {}
    for candle in candles:
        by_ts[candle.ts] = candle
    return [by_ts[ts] for ts in sorted(by_ts)]


def aggregate_timeframe(base_candles: list[Candle], timeframe: str) -> list[Candle]:
    if timeframe == "5m":
        return base_candles

    delta = TIMEFRAME_DELTAS[timeframe]
    base_delta = TIMEFRAME_DELTAS["5m"]
    expected_bars = int(delta / base_delta)

    buckets: OrderedDict[datetime, list[Candle]] = OrderedDict()
    for candle in base_candles:
        bucket_start = floor_timestamp(candle.ts, timeframe)
        buckets.setdefault(bucket_start, []).append(candle)

    aggregated: list[Candle] = []
    for bucket_start, bucket_candles in buckets.items():
        bucket_candles = sorted(bucket_candles, key=lambda candle: candle.ts)
        if len(bucket_candles) != expected_bars:
            continue

        expected_ts = bucket_start
        complete = True
        for candle in bucket_candles:
            if candle.ts != expected_ts:
                complete = False
                break
            expected_ts += base_delta

        if not complete:
            continue

        aggregated.append(
            Candle(
                ts=bucket_start,
                open=bucket_candles[0].open,
                high=max(candle.high for candle in bucket_candles),
                low=min(candle.low for candle in bucket_candles),
                close=bucket_candles[-1].close,
                volume=sum((candle.volume for candle in bucket_candles), start=Decimal("0")),
                vol_ccy=sum((candle.vol_ccy for candle in bucket_candles), start=Decimal("0")),
                vol_ccy_quote=sum(
                    (candle.vol_ccy_quote for candle in bucket_candles),
                    start=Decimal("0"),
                ),
                confirm=1,
            )
        )
    return aggregated


def rebuild_derived(root: str | Path, asset: str) -> dict[str, dict[str, object]]:
    asset = asset.upper()
    base_candles = load_candles(raw_5m_path(root, asset), confirmed_only=True)
    summaries: dict[str, dict[str, object]] = {}
    for timeframe in DERIVED_TIMEFRAMES:
        candles = aggregate_timeframe(base_candles, timeframe)
        out_path = derived_path(root, asset, timeframe)
        write_candles(out_path, candles)
        summaries[timeframe] = candle_summary(candles, out_path)
    return summaries


def candle_summary(candles: list[Candle], path: Path | None = None) -> dict[str, object]:
    result: dict[str, object] = {
        "rows": len(candles),
        "start": format_ts(candles[0].ts) if candles else None,
        "end": format_ts(candles[-1].ts) if candles else None,
    }
    if path is not None:
        result["path"] = str(path)
    return result


def seed_live_from_training(asset: str, live_root: str | Path, training_root: str | Path) -> bool:
    asset = asset.upper()
    source = raw_5m_path(training_root, asset)
    target = raw_5m_path(live_root, asset)
    if not source.exists() or target.exists():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return True


def write_metadata(
    root: str | Path,
    asset: str,
    inst_id: str,
    source: str,
    derived_summary: dict[str, dict[str, object]],
) -> None:
    root = Path(root)
    metadata_path = root / "metadata.json"
    metadata = {"assets": {}}
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text())
        metadata.setdefault("assets", {})

    raw_candles = load_candles(raw_5m_path(root, asset), confirmed_only=True)
    metadata["assets"][asset.upper()] = {
        "source": source,
        "instrument": inst_id,
        "raw": {"5m": candle_summary(raw_candles, raw_5m_path(root, asset))},
        "derived": derived_summary,
        "timestamp_timezone": "UTC",
        "updated_at_utc": format_ts(datetime.now(UTC)),
    }
    root.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
