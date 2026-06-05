#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SIGNAL_SET_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<asset>[A-Z0-9]+)-(?P<dedupe>\d+[mhd])-dedupe-vote(?P<votes>\d+)(?:-[a-z0-9]+)?$"
)
PACKET_RE = re.compile(r"^\d{8}T\d{6}Z\.json$")


def compact_timestamp(value: str) -> str:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    return dt.strftime("%Y%m%dT%H%M%SZ")


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError("expected JSON object")
    return payload


def validate_signal_set(path: Path, root: Path) -> list[str]:
    errors: list[str] = []
    rel = path.relative_to(root)
    parts = rel.parts
    if len(parts) != 3:
        return errors

    signal_engine_id, asset, signal_set_id = parts
    match = SIGNAL_SET_RE.match(signal_set_id)
    if not match:
        errors.append(f"{rel}: signal_set_id must match YYYY-ASSET-DEDUPEWINDOW-dedupe-voteN")
    else:
        if match.group("asset") != asset:
            errors.append(f"{rel}: asset folder does not match signal_set_id asset")

    manifest_path = path / "manifest.json"
    packets_dir = path / "packets"
    if not manifest_path.exists():
        errors.append(f"{rel}: missing manifest.json")
        return errors
    if not packets_dir.is_dir():
        errors.append(f"{rel}: missing packets/ directory")
        return errors

    try:
        manifest = load_json(manifest_path)
    except (json.JSONDecodeError, ValueError) as exc:
        errors.append(f"{rel}: invalid manifest.json: {exc}")
        return errors

    expected = {
        "asset": asset,
        "signal_set_id": signal_set_id,
        "packets_path": "packets/",
        "packet_filename_format": "YYYYMMDDTHHMMSSZ.json",
    }
    for key, value in expected.items():
        if manifest.get(key) != value:
            errors.append(f"{rel}: manifest {key!r} must be {value!r}")
    manifest_engine_id = manifest.get("signal_engine_id")
    manifest_family = manifest.get("signal_family")
    if manifest_engine_id in (None, ""):
        if manifest_family in (None, ""):
            errors.append(f"{rel}: manifest must include signal_engine_id or legacy signal_family")
        elif manifest_family != signal_engine_id:
            errors.append(f"{rel}: legacy signal_family must match engine directory {signal_engine_id!r}")
    elif manifest_engine_id != signal_engine_id:
        errors.append(f"{rel}: manifest 'signal_engine_id' must be {signal_engine_id!r}")

    packet_files = sorted(packets_dir.glob("*.json"))
    if manifest.get("packet_count") != len(packet_files):
        errors.append(f"{rel}: manifest packet_count does not match packets/*.json count")

    for packet_path in packet_files:
        if not PACKET_RE.match(packet_path.name):
            errors.append(f"{packet_path.relative_to(root)}: packet filename must be YYYYMMDDTHHMMSSZ.json")
            continue
        try:
            packet = load_json(packet_path)
            packet_ts = packet.get("timestamp")
            if not isinstance(packet_ts, str):
                errors.append(f"{packet_path.relative_to(root)}: packet missing string timestamp")
                continue
            expected_name = f"{compact_timestamp(packet_ts)}.json"
            if packet_path.name != expected_name:
                errors.append(
                    f"{packet_path.relative_to(root)}: filename does not match packet timestamp {packet_ts}"
                )
        except (json.JSONDecodeError, ValueError) as exc:
            errors.append(f"{packet_path.relative_to(root)}: invalid packet JSON/timestamp: {exc}")

    return errors


def main() -> int:
    workspace = Path(sys.argv[1]) if len(sys.argv) > 1 else Path.cwd()
    signals_root = workspace / "dev" / "signals"
    errors: list[str] = []
    checked = 0

    if not signals_root.is_dir():
        print(json.dumps({"valid": False, "errors": [f"missing {signals_root}"]}, indent=2))
        return 1

    for family_dir in sorted(path for path in signals_root.iterdir() if path.is_dir()):
        for asset_dir in sorted(path for path in family_dir.iterdir() if path.is_dir()):
            for set_dir in sorted(path for path in asset_dir.iterdir() if path.is_dir()):
                checked += 1
                errors.extend(validate_signal_set(set_dir, signals_root))

    result = {
        "signals_root": str(signals_root.resolve()),
        "checked_signal_sets": checked,
        "valid": not errors,
        "errors": errors,
    }
    print(json.dumps(result, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
