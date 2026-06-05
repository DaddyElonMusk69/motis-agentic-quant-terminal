from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "signals" / "fill_signal_set_tail.py"
SPEC = importlib.util.spec_from_file_location("fill_signal_set_tail", SCRIPT_PATH)
assert SPEC is not None
filler = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = filler
SPEC.loader.exec_module(filler)


WORKSPACE_MANIFEST = {
    "workspace_name": "test-workspace",
    "schema_version": "0.1",
    "purpose": "test",
    "directories": {
        "dev": "Historical data",
        "live": "Live data",
        "artifacts": "Artifacts",
    },
}


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_packet(path: Path, timestamp: str, asset: str = "BTC") -> None:
    write_json(
        path,
        {
            "schema_version": "signal_packet.v2",
            "asset": asset,
            "timestamp": timestamp,
            "active_timeframes": ["2h"],
            "interactions": [],
            "charts": {},
        },
    )


def make_workspace(tmp_path: Path, *, plan_timestamps: list[str]) -> Path:
    root = tmp_path / "workspace"
    write_json(root / "workspace_manifest.json", WORKSPACE_MANIFEST)
    for rel in ("dev", "live", "artifacts"):
        (root / rel).mkdir(parents=True, exist_ok=True)

    generator_path = root / "artifacts" / "signal_engine" / "scripts" / "signals" / "fake_generator.py"
    write_json(generator_path.with_name("plan.json"), {"BTC": plan_timestamps})
    generator_path.parent.mkdir(parents=True, exist_ok=True)
    generator_path.write_text(
        """#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


parser = argparse.ArgumentParser()
parser.add_argument("--asset", required=True)
parser.add_argument("--start", required=True)
parser.add_argument("--end", required=True)
parser.add_argument("--out-dir", required=True)
args, _ = parser.parse_known_args()

plan = json.loads(Path(__file__).with_name("plan.json").read_text())
timestamps = plan.get(args.asset.upper(), [])
start = parse_ts(args.start)
end = parse_ts(args.end)
out_dir = Path(args.out_dir)
out_dir.mkdir(parents=True, exist_ok=True)

emitted = []
for timestamp in timestamps:
    current = parse_ts(timestamp)
    if current < start or current > end:
        continue
    path = out_dir / current.strftime("%Y%m%dT%H%M%SZ.json")
    path.write_text(
        json.dumps(
            {
                "schema_version": "signal_packet.v2",
                "asset": args.asset.upper(),
                "timestamp": timestamp,
                "active_timeframes": ["2h"],
                "interactions": [],
                "charts": {},
            },
            indent=2,
        )
        + "\\n"
    )
    emitted.append(timestamp)

print(
    json.dumps(
        {
            "asset": args.asset.upper(),
            "start": args.start,
            "end": args.end,
            "generated_timestamps": emitted,
            "out_dir": str(out_dir),
        }
    )
)
""",
    )

    write_json(
        root / "artifacts" / "signal_engine" / "engine_registry.json",
        {
            "fake_engine": {
                "signal_engine_id": "fake_engine",
                "replay_generator_path": "artifacts/signal_engine/scripts/signals/fake_generator.py",
                "live_scanner_path": "artifacts/signal_engine/scripts/signals/fake_scanner.py",
                "dev_signals_root": "dev/signals/fake_engine",
                "live_signals_root": "live/signals/fake_engine",
                "signal_family": "fake_engine",
            }
        },
    )
    return root


def write_signal_set(
    root: Path,
    *,
    manifest_end: str,
    packets: list[str],
    signal_engine_id: str = "fake_engine",
    use_legacy_timestamps: bool = False,
    parameters: dict[str, object] | None = None,
) -> Path:
    signal_set_dir = root / "dev" / "signals" / "fake_engine" / "BTC" / "2026-BTC-2h-dedupe-vote2"
    packets_dir = signal_set_dir / "packets"
    for timestamp in packets:
        compact = timestamp.replace("-", "").replace(":", "")
        compact = compact.replace("T", "T").replace("Z", "Z")
        write_packet(packets_dir / f"{compact}.json", timestamp)
    manifest = {
        "schema_version": "0.1",
        "signal_set_id": "2026-BTC-2h-dedupe-vote2",
        "asset": "BTC",
        "signal_engine_version": "0.1",
        "data_manifest": "dev/data/manifests/BTC.json",
        "parameters": parameters if parameters is not None else {
            "vote_threshold": 2,
            "window_minutes": 120,
            "timeframes": ["2h"],
        },
        "packet_count": len(packets),
        "packets_path": "packets/",
        "packet_filename_format": "YYYYMMDDTHHMMSSZ.json",
        "signal_family": "fake_engine",
    }
    if use_legacy_timestamps:
        manifest["timestamp_start"] = "2026-03-01T00:00:00Z"
        manifest["timestamp_end"] = manifest_end
    else:
        manifest["start_ts"] = "2026-03-01T00:00:00Z"
        manifest["end_ts"] = manifest_end
    if signal_engine_id:
        manifest["signal_engine_id"] = signal_engine_id
    write_json(signal_set_dir / "manifest.json", manifest)
    return signal_set_dir / "manifest.json"


def test_fill_signal_set_tail_appends_new_packets_and_updates_manifest(tmp_path: Path) -> None:
    root = make_workspace(
        tmp_path,
        plan_timestamps=[
            "2026-05-20T00:00:00Z",
            "2026-05-29T00:00:00Z",
            "2026-05-31T12:00:00Z",
        ],
    )
    manifest_path = write_signal_set(
        root,
        manifest_end="2026-05-26T00:00:00Z",
        packets=["2026-05-20T00:00:00Z"],
    )

    result = filler.fill_signal_set_tail(
        workspace_root=root,
        signal_set_manifest=manifest_path,
        target_end=filler.parse_timestamp("2026-06-01T00:00:00Z"),
    )

    assert result["noop"] is False
    assert result["generator_summary"]["start"] == "2026-05-20T00:00:00Z"
    assert result["generator_summary"]["generated_timestamps"] == [
        "2026-05-20T00:00:00Z",
        "2026-05-29T00:00:00Z",
        "2026-05-31T12:00:00Z",
    ]
    assert result["existing_packet_count"] == 1
    assert result["generated_packet_count"] == 3
    assert result["appended_packet_count"] == 2

    packets = sorted((manifest_path.parent / "packets").glob("*.json"))
    assert [path.name for path in packets] == [
        "20260520T000000Z.json",
        "20260529T000000Z.json",
        "20260531T120000Z.json",
    ]

    manifest = json.loads(manifest_path.read_text())
    assert manifest["signal_engine_id"] == "fake_engine"
    assert manifest["packet_count"] == 3
    assert manifest["end_ts"] == "2026-06-01T00:00:00Z"


def test_fill_signal_set_tail_noops_when_manifest_already_reaches_target(tmp_path: Path) -> None:
    root = make_workspace(tmp_path, plan_timestamps=["2026-05-31T12:00:00Z"])
    manifest_path = write_signal_set(
        root,
        manifest_end="2026-06-01T00:00:00Z",
        packets=["2026-05-20T00:00:00Z"],
    )

    result = filler.fill_signal_set_tail(
        workspace_root=root,
        signal_set_manifest=manifest_path,
        target_end=filler.parse_timestamp("2026-06-01T00:00:00Z"),
    )

    assert result["noop"] is True
    assert result["reason"] == "already_at_or_beyond_target_end"
    assert result["existing_packet_count"] == 1
    assert result["generated_packet_count"] == 0


def test_fill_signal_set_tail_backfills_signal_engine_id_from_legacy_family(tmp_path: Path) -> None:
    root = make_workspace(tmp_path, plan_timestamps=["2026-05-31T12:00:00Z"])
    manifest_path = write_signal_set(
        root,
        manifest_end="2026-05-26T00:00:00Z",
        packets=[],
        signal_engine_id="",
    )

    result = filler.fill_signal_set_tail(
        workspace_root=root,
        signal_set_manifest=manifest_path,
        target_end=filler.parse_timestamp("2026-06-01T00:00:00Z"),
    )

    assert result["noop"] is False
    assert result["generator_summary"]["start"] == "2026-03-01T00:00:00Z"
    manifest = json.loads(manifest_path.read_text())
    assert manifest["signal_engine_id"] == "fake_engine"
    assert manifest["packet_count"] == 1


def test_fill_signal_set_tail_supports_legacy_timestamp_fields(tmp_path: Path) -> None:
    root = make_workspace(tmp_path, plan_timestamps=["2026-05-31T12:00:00Z"])
    manifest_path = write_signal_set(
        root,
        manifest_end="2026-05-26T00:00:00Z",
        packets=[],
        use_legacy_timestamps=True,
    )

    result = filler.fill_signal_set_tail(
        workspace_root=root,
        signal_set_manifest=manifest_path,
        target_end=filler.parse_timestamp("2026-06-01T00:00:00Z"),
    )

    assert result["noop"] is False
    manifest = json.loads(manifest_path.read_text())
    assert manifest["end_ts"] == "2026-06-01T00:00:00Z"


def test_fill_signal_set_tail_skips_manifest_flags_missing_from_generator_cli(tmp_path: Path) -> None:
    root = make_workspace(tmp_path, plan_timestamps=["2026-05-31T12:00:00Z"])
    manifest_path = write_signal_set(
        root,
        manifest_end="2026-05-26T00:00:00Z",
        packets=[],
        parameters={
            "vote_threshold": 2,
            "window_minutes": 120,
            "timeframes": ["2h", "4h"],
        },
    )

    generator_path = (
        root / "artifacts" / "signal_engine" / "scripts" / "signals" / "fake_generator.py"
    )
    flags = filler.supported_generator_flags(generator_path)
    assert "--timeframes" not in flags

    command = filler.build_generator_command(
        generator_path,
        context=filler.load_signal_set_context(manifest_path),
        start=filler.parse_timestamp("2026-05-01T00:00:00Z"),
        end=filler.parse_timestamp("2026-06-01T00:00:00Z"),
        out_dir=root / "tmp",
    )

    assert "--timeframes" not in command
