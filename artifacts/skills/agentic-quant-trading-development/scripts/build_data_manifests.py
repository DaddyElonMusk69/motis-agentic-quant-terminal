#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path


EXPECTED_RAW_COLUMNS = [
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

EXPECTED_DERIVED_TIMEFRAMES = ["5m", "2h", "4h", "8h", "12h", "1d"]


def rel(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def read_csv_stats(path: Path) -> dict:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        columns = reader.fieldnames or []
        rows = 0
        start_ts = None
        end_ts = None
        confirmed_only = True
        prev_ts = None
        sorted_ascending = True
        duplicate_timestamps = False
        for row in reader:
            rows += 1
            ts = row.get("ts", "")
            if rows == 1:
                start_ts = ts
            end_ts = ts
            if prev_ts is not None:
                if ts < prev_ts:
                    sorted_ascending = False
                if ts == prev_ts:
                    duplicate_timestamps = True
            prev_ts = ts
            if "confirm" in row and str(row.get("confirm", "")).strip() not in {"1", "1.0", "true", "True"}:
                confirmed_only = False
    return {
        "path": path,
        "columns": columns,
        "rows": rows,
        "start_ts": start_ts,
        "end_ts": end_ts,
        "confirmed_only": confirmed_only,
        "sorted_ascending": sorted_ascending,
        "duplicate_timestamps": duplicate_timestamps,
    }


def validate_stats(stats: dict, timeframe: str, raw: bool) -> list[str]:
    warnings: list[str] = []
    if stats["rows"] == 0:
        warnings.append(f"{timeframe}: empty candle file")
    if raw and stats["columns"] != EXPECTED_RAW_COLUMNS:
        warnings.append(f"{timeframe}: unexpected raw columns {stats['columns']}")
    if not stats["sorted_ascending"]:
        warnings.append(f"{timeframe}: timestamps are not sorted ascending")
    if stats["duplicate_timestamps"]:
        warnings.append(f"{timeframe}: duplicate timestamps found")
    if raw and not stats["confirmed_only"]:
        warnings.append(f"{timeframe}: raw file contains unconfirmed candles")
    return warnings


def build_asset_manifest(root: Path, asset: str) -> dict:
    data_root = root / "dev" / "data"
    raw_path = data_root / "raw" / asset / "5m" / "candles.csv"
    warnings: list[str] = []

    raw = read_csv_stats(raw_path)
    warnings.extend(validate_stats(raw, "raw/5m", raw=True))

    derived: dict[str, dict] = {}
    missing_timeframes: list[str] = []
    for tf in EXPECTED_DERIVED_TIMEFRAMES:
        path = data_root / "derived" / asset / tf / "candles.csv"
        if not path.exists():
            missing_timeframes.append(tf)
            warnings.append(f"derived/{tf}: missing candle file")
            continue
        stats = read_csv_stats(path)
        warnings.extend(validate_stats(stats, f"derived/{tf}", raw=False))
        derived[tf] = {
            "path": rel(path, root),
            "rows": stats["rows"],
            "start_ts": stats["start_ts"],
            "end_ts": stats["end_ts"],
            "columns": stats["columns"],
            "rule": "aggregated from canonical raw 5m on UTC bucket boundaries",
            "sorted_ascending": stats["sorted_ascending"],
            "duplicate_timestamps": stats["duplicate_timestamps"],
        }

    validation_status = "valid" if not warnings else "warning"
    if missing_timeframes or raw["rows"] == 0:
        validation_status = "invalid"

    return {
        "schema_version": "0.1",
        "asset": asset,
        "source": {
            "exchange": "OKX",
            "instrument_type": "USDT-margined perpetual swap",
            "canonical_timeframe": "5m",
            "closed_candles_only": raw["confirmed_only"],
            "timestamp_timezone": "UTC",
        },
        "raw": {
            "5m": {
                "path": rel(raw_path, root),
                "rows": raw["rows"],
                "start_ts": raw["start_ts"],
                "end_ts": raw["end_ts"],
                "columns": raw["columns"],
                "sorted_ascending": raw["sorted_ascending"],
                "duplicate_timestamps": raw["duplicate_timestamps"],
            }
        },
        "derived": derived,
        "derived_timeframes": [tf for tf in EXPECTED_DERIVED_TIMEFRAMES if tf in derived],
        "coverage": {
            "start_ts": raw["start_ts"],
            "end_ts": raw["end_ts"],
        },
        "gaps": [],
        "warnings": warnings,
        "validation_status": validation_status,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
    }


def main() -> int:
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    data_root = root / "dev" / "data"
    manifests_dir = data_root / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    assets = sorted(path.name for path in (data_root / "raw").iterdir() if (path / "5m" / "candles.csv").exists())
    asset_manifests = []
    for asset in assets:
        manifest = build_asset_manifest(root, asset)
        (manifests_dir / f"{asset}.json").write_text(json.dumps(manifest, indent=2) + "\n")
        asset_manifests.append(
            {
                "asset": asset,
                "manifest_path": rel(manifests_dir / f"{asset}.json", root),
                "validation_status": manifest["validation_status"],
                "raw_5m_rows": manifest["raw"]["5m"]["rows"],
                "start_ts": manifest["coverage"]["start_ts"],
                "end_ts": manifest["coverage"]["end_ts"],
                "derived_timeframes": manifest["derived_timeframes"],
                "warning_count": len(manifest["warnings"]),
            }
        )

    index = {
        "schema_version": "0.1",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "assets": asset_manifests,
    }
    (data_root / "metadata.json").write_text(json.dumps(index, indent=2) + "\n")
    print(json.dumps({"assets": len(asset_manifests), "metadata": rel(data_root / "metadata.json", root)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
