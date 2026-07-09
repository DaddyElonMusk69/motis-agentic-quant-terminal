#!/usr/bin/env python3
"""Audit and optionally repair canonical OKX raw 5m parquet candles.

The script treats OKX public 5m candles as the reference source and only touches
raw 5m parquet shards when --repair is explicitly supplied.
"""
from __future__ import annotations

import argparse
import json
import shutil
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import requests


HISTORY_URL = "https://www.okx.com/api/v5/market/history-candles"
CANDLES_URL = "https://www.okx.com/api/v5/market/candles"
PAGE_LIMIT = 300
FETCH_BURST = 18
FETCH_SLEEP_SECONDS = 2.1
STEP = timedelta(minutes=5)
DATA_COLUMNS = ["timestamp", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
COMPARE_COLUMNS = ["open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"]
PRICE_COLUMNS = {"open", "high", "low", "close"}
VOLUME_COLUMNS = {"volume", "vol_ccy", "vol_ccy_quote", "confirm"}


@dataclass
class CompareResult:
    expected_slots: int
    local_rows: int
    okx_rows: int
    missing_timestamps: list[str] = field(default_factory=list)
    extra_timestamps: list[str] = field(default_factory=list)
    okx_missing_expected: list[str] = field(default_factory=list)
    local_missing_expected: list[str] = field(default_factory=list)
    field_mismatches: int = 0
    price_mismatches: int = 0
    volume_only_mismatches: int = 0
    confirm_mismatches: int = 0
    mismatch_samples: list[dict[str, Any]] = field(default_factory=list)
    zero_quote_spans: list[dict[str, Any]] = field(default_factory=list)
    invalid_ohlc_samples: list[dict[str, Any]] = field(default_factory=list)

    @property
    def broken(self) -> bool:
        return any(
            [
                self.missing_timestamps,
                self.extra_timestamps,
                self.okx_missing_expected,
                self.local_missing_expected,
                self.field_mismatches,
                self.zero_quote_spans,
                self.invalid_ohlc_samples,
            ]
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit/repair raw OKX 5m parquet candles against OKX.")
    parser.add_argument("--assets", required=True, help="Comma-separated symbols, e.g. BTC,ETH,BNB,SOL")
    parser.add_argument("--start", required=True, help="Inclusive UTC start, e.g. 2026-05-01T00:00:00Z")
    parser.add_argument("--end", required=True, help="Inclusive UTC end, e.g. 2026-07-05T23:55:00Z")
    parser.add_argument("--workspace-root", default=".", help="Repository/workspace root.")
    parser.add_argument("--report-dir", default=None, help="Directory for JSON/Markdown reports.")
    parser.add_argument("--chunk-days", type=int, default=7, help="OKX fetch/audit chunk size.")
    parser.add_argument("--repair", action="store_true", help="Rewrite bad raw 5m parquet rows from OKX.")
    parser.add_argument("--backup-dir", default=None, help="Backup root used with --repair.")
    return parser.parse_args()


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def month_key(ts: str) -> tuple[int, int]:
    dt = parse_iso(ts)
    return dt.year, dt.month


def storage_uri(workspace_root: Path, asset: str) -> Path:
    return (
        workspace_root
        / ".data"
        / "market-data"
        / "origin=raw"
        / "source=okx"
        / "type=candles"
        / f"asset={asset.upper()}"
        / "timeframe=5m"
    )


def partition_path(root: Path, asset: str, year: int, month: int) -> Path:
    return storage_uri(root, asset) / f"year={year}" / f"month={month:02d}" / "data.parquet"


def month_range(start: str, end: str) -> list[tuple[int, int]]:
    current = parse_iso(start).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    stop = parse_iso(end).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    months: list[tuple[int, int]] = []
    while current <= stop:
        months.append((current.year, current.month))
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return months


def expected_timestamps(start: str, end: str) -> list[str]:
    current = parse_iso(start)
    stop = parse_iso(end)
    out: list[str] = []
    while current <= stop:
        out.append(iso_z(current))
        current += STEP
    return out


def read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return pq.ParquetFile(path).read().to_pylist()


def normalize_row(row: dict[str, Any]) -> dict[str, str]:
    return {
        "timestamp": str(row["timestamp"]),
        "open": str(row["open"]),
        "high": str(row["high"]),
        "low": str(row["low"]),
        "close": str(row["close"]),
        "volume": str(row["volume"]),
        "vol_ccy": str(row["vol_ccy"]),
        "vol_ccy_quote": str(row["vol_ccy_quote"]),
        "confirm": str(row["confirm"]),
    }


def load_local_rows(workspace_root: Path, asset: str, start: str, end: str) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
    rows_by_ts: dict[str, dict[str, str]] = {}
    duplicate_timestamps: list[str] = []
    unreadable_partitions: list[dict[str, str]] = []
    partition_rows: dict[str, int] = {}

    for year, month in month_range(start, end):
        path = partition_path(workspace_root, asset, year, month)
        try:
            rows = read_parquet_rows(path)
        except Exception as exc:  # pragma: no cover - exercised through real audits.
            unreadable_partitions.append({"path": str(path), "error": str(exc)})
            continue
        partition_rows[str(path)] = len(rows)
        for row in rows:
            normalized = normalize_row(row)
            ts = normalized["timestamp"]
            if start <= ts <= end:
                if ts in rows_by_ts:
                    duplicate_timestamps.append(ts)
                rows_by_ts[ts] = normalized

    meta = {
        "duplicate_timestamps": sorted(set(duplicate_timestamps)),
        "duplicate_timestamp_count": len(duplicate_timestamps),
        "unreadable_partitions": unreadable_partitions,
        "partition_rows": partition_rows,
    }
    return rows_by_ts, meta


def parse_okx_rows(rows: list[list[Any]]) -> list[dict[str, str]]:
    parsed = []
    for row in rows:
        parsed.append(
            {
                "timestamp": iso_z(datetime.fromtimestamp(int(row[0]) / 1000, UTC)),
                "open": str(row[1]),
                "high": str(row[2]),
                "low": str(row[3]),
                "close": str(row[4]),
                "volume": str(row[5]),
                "vol_ccy": str(row[6]),
                "vol_ccy_quote": str(row[7]),
                "confirm": str(row[8]),
            }
        )
    return sorted(parsed, key=lambda item: item["timestamp"])


def fetch_page(inst_id: str, *, after_ms: int, retries: int = 4) -> list[dict[str, str]]:
    params = {"instId": inst_id, "bar": "5m", "limit": str(PAGE_LIMIT), "after": str(after_ms)}
    for attempt in range(retries):
        for url in (HISTORY_URL, CANDLES_URL):
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
            if rows:
                return parse_okx_rows(rows)
        if attempt < retries - 1:
            time.sleep(min(2**attempt, 8))
    return []


def fetch_okx_window(inst_id: str, start: str, end: str) -> dict[str, dict[str, str]]:
    start_dt = parse_iso(start)
    end_dt = parse_iso(end)
    cursor_ms = int((end_dt + STEP).timestamp() * 1000)
    rows_by_ts: dict[str, dict[str, str]] = {}
    requests_made = 0

    while True:
        page = fetch_page(inst_id, after_ms=cursor_ms)
        requests_made += 1
        if not page:
            break

        for row in page:
            row_dt = parse_iso(row["timestamp"])
            if start_dt <= row_dt <= end_dt:
                rows_by_ts[row["timestamp"]] = row

        oldest_dt = parse_iso(page[0]["timestamp"])
        if oldest_dt < start_dt:
            break
        cursor_ms = int(oldest_dt.timestamp() * 1000) - 1

        if requests_made % FETCH_BURST == 0:
            time.sleep(FETCH_SLEEP_SECONDS)

    return dict(sorted(rows_by_ts.items()))


def compare_candles(
    *,
    local: dict[str, dict[str, str]],
    okx: dict[str, dict[str, str]],
    start: str,
    end: str,
) -> CompareResult:
    expected = expected_timestamps(start, end)
    expected_set = set(expected)
    local_set = {ts for ts in local if start <= ts <= end}
    okx_set = {ts for ts in okx if start <= ts <= end}
    result = CompareResult(
        expected_slots=len(expected),
        local_rows=len(local_set),
        okx_rows=len(okx_set),
        missing_timestamps=sorted(okx_set - local_set),
        extra_timestamps=sorted(local_set - okx_set),
        okx_missing_expected=sorted(expected_set - okx_set),
        local_missing_expected=sorted(expected_set - local_set),
        zero_quote_spans=zero_quote_spans(local, start, end),
        invalid_ohlc_samples=invalid_ohlc_samples(local, start, end),
    )

    for ts in sorted(local_set & okx_set):
        mismatched = [column for column in COMPARE_COLUMNS if to_decimal(local[ts][column]) != to_decimal(okx[ts][column])]
        if not mismatched:
            continue
        result.field_mismatches += 1
        if any(column in PRICE_COLUMNS for column in mismatched):
            result.price_mismatches += 1
        elif any(column in VOLUME_COLUMNS for column in mismatched):
            result.volume_only_mismatches += 1
        if "confirm" in mismatched:
            result.confirm_mismatches += 1
        if len(result.mismatch_samples) < 20:
            result.mismatch_samples.append(
                {
                    "timestamp": ts,
                    "fields": mismatched,
                    "local": {column: local[ts][column] for column in COMPARE_COLUMNS},
                    "okx": {column: okx[ts][column] for column in COMPARE_COLUMNS},
                }
            )

    return result


def to_decimal(value: Any) -> Decimal:
    return Decimal(str(value))


def zero_quote_spans(rows: dict[str, dict[str, str]], start: str, end: str) -> list[dict[str, Any]]:
    spans: list[dict[str, Any]] = []
    current_start: str | None = None
    current_end: str | None = None
    count = 0
    for ts in expected_timestamps(start, end):
        row = rows.get(ts)
        is_zero = bool(row) and to_decimal(row["vol_ccy"]) == 0 and to_decimal(row["vol_ccy_quote"]) == 0
        if is_zero:
            current_start = current_start or ts
            current_end = ts
            count += 1
        elif current_start is not None:
            spans.append({"start": current_start, "end": current_end, "rows": count})
            current_start = None
            current_end = None
            count = 0
    if current_start is not None:
        spans.append({"start": current_start, "end": current_end, "rows": count})
    return spans


def invalid_ohlc_samples(rows: dict[str, dict[str, str]], start: str, end: str) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for ts in expected_timestamps(start, end):
        row = rows.get(ts)
        if not row:
            continue
        open_ = to_decimal(row["open"])
        high = to_decimal(row["high"])
        low = to_decimal(row["low"])
        close = to_decimal(row["close"])
        if high < max(open_, close) or low > min(open_, close) or high < low:
            samples.append({"timestamp": ts, "row": row})
            if len(samples) >= 20:
                break
    return samples


def build_repaired_rows(
    *,
    existing_rows: list[dict[str, Any]],
    okx_rows: dict[str, dict[str, str]],
    start: str,
    end: str,
) -> list[dict[str, str]]:
    repaired: dict[str, dict[str, str]] = {}
    for row in existing_rows:
        normalized = normalize_row(row)
        ts = normalized["timestamp"]
        if not (start <= ts <= end):
            repaired[ts] = normalized
    for ts, row in okx_rows.items():
        if start <= ts <= end:
            repaired[ts] = normalize_row(row)
    return [repaired[ts] for ts in sorted(repaired)]


def write_parquet_rows(path: Path, rows: list[dict[str, str]], schema: pa.Schema | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    typed_rows = [coerce_row_to_schema(row, schema) for row in rows]
    table = pa.Table.from_pylist(typed_rows, schema=schema)
    tmp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        pq.write_table(table, tmp_path)
        pq.ParquetFile(tmp_path)
        tmp_path.replace(path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()


def coerce_row_to_schema(row: dict[str, str], schema: pa.Schema | None) -> dict[str, Any]:
    if schema is None:
        return {
            "timestamp": row["timestamp"],
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": float(row["volume"]),
            "vol_ccy": float(row["vol_ccy"]),
            "vol_ccy_quote": float(row["vol_ccy_quote"]),
            "confirm": int(Decimal(str(row["confirm"]))),
        }
    coerced: dict[str, Any] = {}
    for field_item in schema:
        name = field_item.name
        if name not in row:
            continue
        value = row[name]
        if pa.types.is_integer(field_item.type):
            coerced[name] = int(Decimal(str(value)))
        elif pa.types.is_floating(field_item.type):
            coerced[name] = float(value)
        else:
            coerced[name] = str(value)
    return coerced


def repair_asset(
    *,
    workspace_root: Path,
    asset: str,
    start: str,
    end: str,
    okx_rows: dict[str, dict[str, str]],
    backup_root: Path,
) -> list[dict[str, str]]:
    repaired_partitions: list[dict[str, str]] = []
    for year, month in month_range(start, end):
        path = partition_path(workspace_root, asset, year, month)
        month_start = max(start, f"{year:04d}-{month:02d}-01T00:00:00Z")
        if month == 12:
            next_month = datetime(year + 1, 1, 1, tzinfo=UTC)
        else:
            next_month = datetime(year, month + 1, 1, tzinfo=UTC)
        month_end = min(end, iso_z(next_month - STEP))
        existing_rows = read_parquet_rows(path)
        existing_map = {
            normalized["timestamp"]: normalized
            for row in existing_rows
            for normalized in [normalize_row(row)]
            if month_start <= normalized["timestamp"] <= month_end
        }
        month_result = compare_candles(local=existing_map, okx=okx_rows, start=month_start, end=month_end)
        if month_result.okx_missing_expected:
            raise RuntimeError(
                f"Cannot repair {asset} {year}-{month:02d}; OKX reference is missing "
                f"{len(month_result.okx_missing_expected)} expected rows."
            )
        if not month_result.broken:
            continue
        schema = pq.ParquetFile(path).schema_arrow if path.exists() else None
        repaired_rows = build_repaired_rows(
            existing_rows=existing_rows,
            okx_rows=okx_rows,
            start=month_start,
            end=month_end,
        )
        if path.exists():
            backup_path = backup_root / path.relative_to(workspace_root)
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, backup_path)
        write_parquet_rows(path, repaired_rows, schema)
        repaired_partitions.append({"path": str(path), "start": month_start, "end": month_end})
    return repaired_partitions


def chunk_ranges(start: str, end: str, chunk_days: int) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    current = parse_iso(start)
    stop = parse_iso(end)
    while current <= stop:
        chunk_end = min(current + timedelta(days=chunk_days) - STEP, stop)
        out.append((iso_z(current), iso_z(chunk_end)))
        current = chunk_end + STEP
    return out


def audit_asset(args: argparse.Namespace, asset: str, report_root: Path) -> dict[str, Any]:
    workspace_root = Path(args.workspace_root).resolve()
    asset = asset.upper()
    inst_id = f"{asset}-USDT-SWAP"
    local, local_meta = load_local_rows(workspace_root, asset, args.start, args.end)
    okx_all: dict[str, dict[str, str]] = {}
    chunk_reports = []

    for chunk_start, chunk_end in chunk_ranges(args.start, args.end, args.chunk_days):
        okx = fetch_okx_window(inst_id, chunk_start, chunk_end)
        okx_all.update(okx)
        result = compare_candles(local=local, okx=okx, start=chunk_start, end=chunk_end)
        chunk_reports.append({"start": chunk_start, "end": chunk_end, **asdict(result), "broken": result.broken})
        print(
            f"{asset} {chunk_start} -> {chunk_end}: "
            f"missing={len(result.missing_timestamps)} extra={len(result.extra_timestamps)} "
            f"okx_missing={len(result.okx_missing_expected)} local_missing={len(result.local_missing_expected)} "
            f"mismatches={result.field_mismatches} zero_spans={len(result.zero_quote_spans)}",
            flush=True,
        )

    full_result = compare_candles(local=local, okx=okx_all, start=args.start, end=args.end)
    repaired_partitions: list[dict[str, str]] = []
    post_repair: dict[str, Any] | None = None
    if args.repair and full_result.broken:
        backup_root = Path(args.backup_dir).resolve() if args.backup_dir else report_root / "backups"
        repaired_partitions = repair_asset(
            workspace_root=workspace_root,
            asset=asset,
            start=args.start,
            end=args.end,
            okx_rows=okx_all,
            backup_root=backup_root,
        )
        repaired_local, repaired_meta = load_local_rows(workspace_root, asset, args.start, args.end)
        repaired_result = compare_candles(local=repaired_local, okx=okx_all, start=args.start, end=args.end)
        post_repair = {"local_meta": repaired_meta, **asdict(repaired_result), "broken": repaired_result.broken}

    report = {
        "asset": asset,
        "instrument": inst_id,
        "start": args.start,
        "end": args.end,
        "mode": "repair" if args.repair else "audit",
        "raw_5m_storage_uri": str(storage_uri(workspace_root, asset)),
        "local_meta": local_meta,
        "chunks": chunk_reports,
        "summary": {**asdict(full_result), "broken": full_result.broken},
        "repaired_partitions": repaired_partitions,
        "post_repair": post_repair,
    }
    write_reports(report_root, report)
    return report


def write_reports(report_root: Path, report: dict[str, Any]) -> None:
    asset = report["asset"]
    report_root.mkdir(parents=True, exist_ok=True)
    json_path = report_root / f"{asset.lower()}_raw_5m_integrity.json"
    md_path = report_root / f"{asset.lower()}_raw_5m_integrity.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    summary = report["summary"]
    lines = [
        f"# {asset} Raw 5m Integrity",
        "",
        f"- Instrument: `{report['instrument']}`",
        f"- Range: `{report['start']}` -> `{report['end']}`",
        f"- Mode: `{report['mode']}`",
        f"- Expected slots: `{summary['expected_slots']}`",
        f"- Local rows: `{summary['local_rows']}`",
        f"- OKX rows: `{summary['okx_rows']}`",
        f"- Missing vs OKX: `{len(summary['missing_timestamps'])}`",
        f"- Extra vs OKX: `{len(summary['extra_timestamps'])}`",
        f"- OKX missing expected slots: `{len(summary['okx_missing_expected'])}`",
        f"- Local missing expected slots: `{len(summary['local_missing_expected'])}`",
        f"- Field mismatch rows: `{summary['field_mismatches']}`",
        f"- Price mismatch rows: `{summary['price_mismatches']}`",
        f"- Volume-only mismatch rows: `{summary['volume_only_mismatches']}`",
        f"- Zero quote-volume spans: `{len(summary['zero_quote_spans'])}`",
        f"- Duplicate local timestamps: `{report['local_meta']['duplicate_timestamp_count']}`",
        f"- Broken: `{summary['broken']}`",
    ]
    if summary["zero_quote_spans"]:
        lines.extend(["", "## Zero Quote-Volume Spans"])
        for span in summary["zero_quote_spans"][:20]:
            lines.append(f"- `{span['start']}` -> `{span['end']}` rows=`{span['rows']}`")
    if summary["mismatch_samples"]:
        lines.extend(["", "## Mismatch Samples"])
        for sample in summary["mismatch_samples"][:10]:
            lines.append(f"- `{sample['timestamp']}` fields=`{','.join(sample['fields'])}`")
    if report["repaired_partitions"]:
        lines.extend(["", "## Repaired Partitions"])
        for item in report["repaired_partitions"]:
            lines.append(f"- `{item['path']}`")
    if report["post_repair"]:
        post = report["post_repair"]
        lines.extend(
            [
                "",
                "## Post-Repair Verification",
                f"- Broken: `{post['broken']}`",
                f"- Missing vs OKX: `{len(post['missing_timestamps'])}`",
                f"- Extra vs OKX: `{len(post['extra_timestamps'])}`",
                f"- Field mismatch rows: `{post['field_mismatches']}`",
                f"- Zero quote-volume spans: `{len(post['zero_quote_spans'])}`",
            ]
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    args = parse_args()
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    report_root = (
        Path(args.report_dir).resolve()
        if args.report_dir
        else Path(args.workspace_root).resolve() / "dev" / "data_integrity_reports" / f"raw_5m_okx_{run_id}"
    )
    reports = []
    for asset in [item.strip().upper() for item in args.assets.split(",") if item.strip()]:
        reports.append(audit_asset(args, asset, report_root))
    combined_path = report_root / "summary.json"
    combined_path.write_text(json.dumps(reports, indent=2, sort_keys=True), encoding="utf-8")
    broken_assets = [
        report["asset"]
        for report in reports
        if (report["post_repair"] or report["summary"])["broken"]
    ]
    print(f"report_dir={report_root}")
    if broken_assets:
        print(f"broken_assets={','.join(broken_assets)}")
        return 2
    print("all_assets_clean=true")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
