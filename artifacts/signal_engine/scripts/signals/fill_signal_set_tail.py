#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.engine_registry import infer_signal_engine_id, load_engine_registry
from vegas.workspace import find_workspace_root


_SUPPORTED_FLAGS_CACHE: dict[Path, set[str]] = {}


@dataclass(frozen=True)
class SignalSetContext:
    manifest_path: Path
    packets_dir: Path
    manifest: dict[str, Any]
    signal_engine_id: str
    asset: str
    signal_set_id: str
    start_ts: datetime
    end_ts: datetime
    parameters: dict[str, Any]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Tail-fill an existing replay signal set by rerunning the engine generator from "
            "the last emitted packet through a newer horizon, then merging deduplicated packets."
        )
    )
    parser.add_argument("--workspace-root", help="Workspace root. Defaults to auto-detect.")
    parser.add_argument(
        "--signal-set-manifest",
        help="Signal set manifest path. May be absolute or relative to the workspace root.",
    )
    parser.add_argument("--signal-engine-id", help="Canonical engine id, e.g. vegas_ema")
    parser.add_argument("--asset", help="Canonical asset, e.g. BTC")
    parser.add_argument("--signal-set-id", help="Signal set id, e.g. 2026-BTC-2h-dedupe-vote2")
    parser.add_argument("--target-end", required=True, help="UTC ISO target horizon end timestamp")
    parser.add_argument(
        "--engine-registry",
        help="Engine registry path. Defaults to artifacts/signal_engine/engine_registry.json",
    )
    parser.add_argument(
        "--keep-temp",
        action="store_true",
        help="Keep the temporary generator output directory for debugging.",
    )
    args = parser.parse_args()

    if not args.signal_set_manifest:
        missing = [
            name
            for name in ("signal_engine_id", "asset", "signal_set_id")
            if not getattr(args, name)
        ]
        if missing:
            parser.error(
                "--signal-set-manifest or the tuple "
                "(--signal-engine-id, --asset, --signal-set-id) is required"
            )
    return args


def parse_timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def resolve_path(root: Path, path: str | Path) -> Path:
    candidate = Path(path)
    if candidate.is_absolute():
        return candidate
    return root / candidate


def resolve_signal_set_manifest(root: Path, args: argparse.Namespace) -> Path:
    if args.signal_set_manifest:
        return resolve_path(root, args.signal_set_manifest)
    return (
        root
        / "dev"
        / "signals"
        / str(args.signal_engine_id)
        / str(args.asset).upper()
        / str(args.signal_set_id)
        / "manifest.json"
    )


def resolve_packets_dir(manifest_path: Path, manifest: dict[str, Any]) -> Path:
    packets_path = manifest.get("packets_path") or "packets/"
    packets_dir = Path(str(packets_path))
    if packets_dir.is_absolute():
        return packets_dir
    return manifest_path.parent / packets_dir


def packet_timestamp(path: Path) -> datetime:
    try:
        return datetime.strptime(path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)
    except ValueError:
        payload = load_json(path)
        raw = payload.get("timestamp")
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"packet missing timestamp and filename is noncanonical: {path}")
        return parse_timestamp(raw)


def list_packet_files(packets_dir: Path) -> list[Path]:
    if not packets_dir.exists():
        return []
    return sorted(packets_dir.glob("*.json"), key=packet_timestamp)


def load_signal_set_context(manifest_path: Path) -> SignalSetContext:
    manifest = load_json(manifest_path)
    signal_engine_id = infer_signal_engine_id(
        manifest.get("signal_engine_id"),
        manifest.get("signal_family"),
        signals_root=manifest_path.parent.parent.parent,
    )
    if not signal_engine_id:
        raise ValueError(f"signal set manifest missing signal_engine_id and signal_family: {manifest_path}")

    asset = str(manifest.get("asset") or manifest_path.parent.parent.name).upper()
    signal_set_id = str(manifest.get("signal_set_id") or manifest_path.parent.name)
    raw_parameters = manifest.get("parameters")
    parameters = raw_parameters if isinstance(raw_parameters, dict) else {}

    return SignalSetContext(
        manifest_path=manifest_path,
        packets_dir=resolve_packets_dir(manifest_path, manifest),
        manifest=manifest,
        signal_engine_id=signal_engine_id,
        asset=asset,
        signal_set_id=signal_set_id,
        start_ts=parse_timestamp(str(manifest.get("start_ts") or manifest["timestamp_start"])),
        end_ts=parse_timestamp(str(manifest.get("end_ts") or manifest["timestamp_end"])),
        parameters=parameters,
    )


def parameter_flag(key: str) -> str:
    if key == "dedupe_window_minutes":
        return "--window-minutes"
    return "--" + key.replace("_", "-")


def supported_generator_flags(generator_path: Path) -> set[str]:
    cached = _SUPPORTED_FLAGS_CACHE.get(generator_path)
    if cached is not None:
        return cached
    result = subprocess.run(
        [sys.executable, str(generator_path), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )
    flags = set(re.findall(r"(?m)(--[a-z0-9-]+)", result.stdout))
    _SUPPORTED_FLAGS_CACHE[generator_path] = flags
    return flags


def build_generator_command(
    generator_path: Path,
    *,
    context: SignalSetContext,
    start: datetime,
    end: datetime,
    out_dir: Path,
) -> list[str]:
    supported_flags = supported_generator_flags(generator_path)
    cmd = [
        sys.executable,
        str(generator_path),
        "--asset",
        context.asset,
        "--start",
        iso_z(start),
        "--end",
        iso_z(end),
        "--out-dir",
        str(out_dir),
    ]
    for key in sorted(context.parameters):
        value = context.parameters[key]
        if value is None:
            continue
        flag = parameter_flag(key)
        if flag not in supported_flags:
            continue
        if isinstance(value, bool):
            if value:
                cmd.append(flag)
            continue
        if isinstance(value, list):
            if not value:
                continue
            cmd.append(flag)
            cmd.extend(str(item) for item in value)
            continue
        if isinstance(value, dict):
            raise ValueError(f"unsupported nested manifest parameter for generator passthrough: {key}")
        cmd.extend([flag, str(value)])
    return cmd


def parse_generator_stdout(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return {"stdout": stdout}
    return payload if isinstance(payload, dict) else {"stdout": stdout}


def merge_generated_packets(packets_dir: Path, generated_dir: Path) -> tuple[int, int, list[Path]]:
    packets_dir.mkdir(parents=True, exist_ok=True)
    before_names = {path.name for path in list_packet_files(packets_dir)}
    generated_files = sorted(generated_dir.glob("*.json"), key=packet_timestamp)
    for path in generated_files:
        shutil.copy2(path, packets_dir / path.name)
    merged_files = list_packet_files(packets_dir)
    appended_count = len({path.name for path in merged_files} - before_names)
    return appended_count, len(generated_files), merged_files


def fill_signal_set_tail(
    *,
    workspace_root: str | Path,
    signal_set_manifest: str | Path,
    target_end: datetime,
    engine_registry_path: str | Path | None = None,
    keep_temp: bool = False,
) -> dict[str, Any]:
    root = Path(workspace_root).resolve()
    manifest_path = resolve_path(root, signal_set_manifest)
    context = load_signal_set_context(manifest_path)
    registry_path = (
        resolve_path(root, engine_registry_path)
        if engine_registry_path is not None
        else root / "artifacts" / "signal_engine" / "engine_registry.json"
    )
    registry = load_engine_registry(registry_path)
    entry = registry.get(context.signal_engine_id)
    if entry is None:
        raise ValueError(f"unknown signal_engine_id in engine registry: {context.signal_engine_id}")

    existing_packet_files = list_packet_files(context.packets_dir)
    summary: dict[str, Any] = {
        "asset": context.asset,
        "signal_set_id": context.signal_set_id,
        "signal_engine_id": context.signal_engine_id,
        "manifest_path": str(manifest_path),
        "existing_packet_count": len(existing_packet_files),
        "previous_end_ts": iso_z(context.end_ts),
        "target_end_ts": iso_z(target_end),
        "noop": False,
    }

    if target_end <= context.end_ts:
        summary.update(
            {
                "noop": True,
                "reason": "already_at_or_beyond_target_end",
                "generated_packet_count": 0,
                "appended_packet_count": 0,
            }
        )
        return summary

    overlap_start = packet_timestamp(existing_packet_files[-1]) if existing_packet_files else context.start_ts
    generator_path = resolve_path(root, str(entry["replay_generator_path"]))
    if not generator_path.exists():
        raise ValueError(f"replay generator missing: {generator_path}")

    temp_dir = Path(tempfile.mkdtemp(prefix="fill_signal_set_tail_"))
    generated_dir = temp_dir / "generated_packets"
    cmd = build_generator_command(
        generator_path,
        context=context,
        start=overlap_start,
        end=target_end,
        out_dir=generated_dir,
    )

    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        generator_summary = parse_generator_stdout(result.stdout)
        appended_count, generated_count, merged_files = merge_generated_packets(context.packets_dir, generated_dir)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.strip() if exc.stderr else ""
        stdout = exc.stdout.strip() if exc.stdout else ""
        details = stderr or stdout or f"exit code {exc.returncode}"
        raise RuntimeError(f"generator failed for {context.signal_engine_id}/{context.asset}: {details}") from exc
    finally:
        if keep_temp:
            summary["temp_dir"] = str(temp_dir)
        else:
            shutil.rmtree(temp_dir, ignore_errors=True)

    manifest = dict(context.manifest)
    manifest["signal_engine_id"] = context.signal_engine_id
    manifest["packet_count"] = len(merged_files)
    manifest["end_ts"] = iso_z(target_end)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")

    summary.update(
        {
            "overlap_start_ts": iso_z(overlap_start),
            "command": cmd,
            "generator_summary": generator_summary,
            "generated_packet_count": generated_count,
            "appended_packet_count": appended_count,
            "final_packet_count": len(merged_files),
        }
    )
    return summary


def main() -> int:
    args = parse_args()
    workspace_root = Path(args.workspace_root).resolve() if args.workspace_root else find_workspace_root()
    summary = fill_signal_set_tail(
        workspace_root=workspace_root,
        signal_set_manifest=resolve_signal_set_manifest(workspace_root, args),
        target_end=parse_timestamp(args.target_end),
        engine_registry_path=args.engine_registry,
        keep_temp=args.keep_temp,
    )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
