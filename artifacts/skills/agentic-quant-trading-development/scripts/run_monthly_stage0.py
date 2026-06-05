#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = SCRIPT_DIR.parents[3]
ENGINE_REGISTRY_PATH = WORKSPACE_ROOT / "artifacts" / "signal_engine" / "engine_registry.json"
SCHEMA_VERSION = "monthly_stage0_run.v0.1"


class MonthlyStage0Error(ValueError):
    pass


@dataclass(frozen=True)
class Windows:
    train_start: date
    train_end: date
    validation_start: date
    validation_end: date
    locked_oos_start: date
    locked_oos_end: date

    @property
    def cycle_start(self) -> datetime:
        return datetime.combine(self.train_start, datetime.min.time(), tzinfo=UTC)

    @property
    def signal_end(self) -> datetime:
        return datetime.combine(self.locked_oos_end + timedelta(days=1), datetime.min.time(), tzinfo=UTC)

    def as_manifest_windows(self) -> dict[str, dict[str, str]]:
        return {
            "train_window": {"start": self.train_start.isoformat(), "end": self.train_end.isoformat()},
            "validation_window": {
                "start": self.validation_start.isoformat(),
                "end": self.validation_end.isoformat(),
            },
            "locked_oos_window": {
                "start": self.locked_oos_start.isoformat(),
                "end": self.locked_oos_end.isoformat(),
            },
        }


@dataclass(frozen=True)
class Candidate:
    asset: str
    strategy_id: str
    signal_engine_id: str
    vote_threshold: int = 2
    window_minutes: int = 120
    forward_hours: int = 36
    threshold_range: tuple[float, float, float] = (0.2, 2.0, 0.1)
    scanner_args: dict[str, Any] | None = None

    @property
    def signal_set_id(self) -> str:
        if self.window_minutes % 1440 == 0:
            dedupe = f"{self.window_minutes // 1440}d"
        elif self.window_minutes % 60 == 0:
            dedupe = f"{self.window_minutes // 60}h"
        else:
            dedupe = f"{self.window_minutes}m"
        return f"{self.year}-{self.asset}-{dedupe}-dedupe-vote{self.vote_threshold}"

    # Assigned after construction by using the Stage 0 cycle start year.
    year: int = 0

    def with_year(self, year: int) -> "Candidate":
        return Candidate(
            asset=self.asset,
            strategy_id=self.strategy_id,
            signal_engine_id=self.signal_engine_id,
            vote_threshold=self.vote_threshold,
            window_minutes=self.window_minutes,
            forward_hours=self.forward_hours,
            threshold_range=self.threshold_range,
            scanner_args=self.scanner_args,
            year=year,
        )


def parse_month(value: str) -> tuple[int, int]:
    try:
        year_text, month_text = value.split("-", 1)
        year = int(year_text)
        month = int(month_text)
    except ValueError as exc:
        raise MonthlyStage0Error("--walk-forward-month must use YYYY-MM") from exc
    if month < 1 or month > 12:
        raise MonthlyStage0Error("--walk-forward-month month must be 01-12")
    return year, month


def month_delta(year: int, month: int, delta: int) -> tuple[int, int]:
    zero_based = year * 12 + (month - 1) + delta
    return zero_based // 12, zero_based % 12 + 1


def month_bounds(year: int, month: int) -> tuple[date, date]:
    return date(year, month, 1), date(year, month, calendar.monthrange(year, month)[1])


def default_windows(walk_forward_month: str) -> Windows:
    year, month = parse_month(walk_forward_month)
    validation_year, validation_month = month_delta(year, month, -1)
    train_start_year, train_start_month = month_delta(year, month, -3)
    train_end_year, train_end_month = month_delta(year, month, -2)

    train_start, _ = month_bounds(train_start_year, train_start_month)
    _, train_end = month_bounds(train_end_year, train_end_month)
    validation_start, validation_month_end = month_bounds(validation_year, validation_month)

    locked_oos_start = validation_month_end - timedelta(days=6)
    validation_end = locked_oos_start - timedelta(days=1)
    return Windows(
        train_start=train_start,
        train_end=train_end,
        validation_start=validation_start,
        validation_end=validation_end,
        locked_oos_start=locked_oos_start,
        locked_oos_end=validation_month_end,
    )


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def iso_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def compact_z(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def scoreable_outcome_end(windows: Windows) -> datetime:
    # Month-end policy: June cycle Stage 0 must not rely on June candles.
    return windows.signal_end - timedelta(minutes=5)


def scoreable_signal_end(windows: Windows, *, forward_hours: int) -> datetime:
    return scoreable_outcome_end(windows) - timedelta(hours=forward_hours)


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise MonthlyStage0Error(f"missing JSON file: {path}")
    payload = json.loads(path.read_text())
    if not isinstance(payload, dict):
        raise MonthlyStage0Error(f"expected JSON object: {path}")
    return payload


def load_registry(root: Path) -> dict[str, dict[str, Any]]:
    registry = load_json(root / "artifacts" / "signal_engine" / "engine_registry.json")
    for engine_id, entry in registry.items():
        if not isinstance(entry, dict):
            raise MonthlyStage0Error(f"engine registry entry is not an object: {engine_id}")
        if not entry.get("replay_generator_path"):
            raise MonthlyStage0Error(f"engine registry entry missing replay_generator_path: {engine_id}")
    return registry


def load_candidates(path: Path, windows: Windows) -> list[Candidate]:
    payload = load_json(path)
    raw_candidates = payload.get("candidates")
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise MonthlyStage0Error("candidate config must contain a non-empty candidates array")

    candidates: list[Candidate] = []
    for index, raw in enumerate(raw_candidates, start=1):
        if not isinstance(raw, dict):
            raise MonthlyStage0Error(f"candidate {index} must be an object")
        try:
            asset = str(raw["asset"]).upper()
            strategy_id = str(raw["strategy_id"])
            signal_engine_id = str(raw["signal_engine_id"])
        except KeyError as exc:
            raise MonthlyStage0Error(f"candidate {index} missing required field: {exc.args[0]}") from exc
        threshold_range = raw.get("threshold_range", [0.2, 2.0, 0.1])
        if not (isinstance(threshold_range, list) and len(threshold_range) == 3):
            raise MonthlyStage0Error(f"candidate {index} threshold_range must be [start, end, step]")
        candidates.append(
            Candidate(
                asset=asset,
                strategy_id=strategy_id,
                signal_engine_id=signal_engine_id,
                vote_threshold=int(raw.get("vote_threshold", 2)),
                window_minutes=int(raw.get("window_minutes", 120)),
                forward_hours=int(raw.get("forward_hours", 36)),
                threshold_range=(float(threshold_range[0]), float(threshold_range[1]), float(threshold_range[2])),
                scanner_args=dict(raw.get("scanner_args", {})) if isinstance(raw.get("scanner_args", {}), dict) else {},
            ).with_year(windows.cycle_start.year)
        )
    return candidates


def signal_manifest_path(root: Path, candidate: Candidate) -> Path:
    return root / "dev" / "signals" / candidate.signal_engine_id / candidate.asset / candidate.signal_set_id / "manifest.json"


def signal_packets_dir(root: Path, candidate: Candidate) -> Path:
    return signal_manifest_path(root, candidate).parent / "packets"


def stage0_dir(root: Path, candidate: Candidate) -> Path:
    return root / "dev" / "training_sessions" / candidate.strategy_id / "stage0" / candidate.signal_set_id


def data_manifest_path(root: Path, candidate: Candidate) -> Path:
    return root / "dev" / "data" / "manifests" / f"{candidate.asset}.json"


def raw_candles_path(root: Path, candidate: Candidate) -> Path:
    return root / "dev" / "data" / "raw" / candidate.asset / "5m" / "candles.csv"


def data_manifest_end_ts(manifest: dict[str, Any]) -> str:
    coverage = manifest.get("coverage")
    if isinstance(coverage, dict) and coverage.get("end_ts"):
        return str(coverage["end_ts"])

    raw = manifest.get("raw")
    if isinstance(raw, dict):
        raw_5m = raw.get("5m")
        if isinstance(raw_5m, dict) and raw_5m.get("end_ts"):
            return str(raw_5m["end_ts"])

    derived = manifest.get("derived")
    if isinstance(derived, dict):
        derived_5m = derived.get("5m")
        if isinstance(derived_5m, dict) and derived_5m.get("end_ts"):
            return str(derived_5m["end_ts"])

    raise MonthlyStage0Error("data manifest missing coverage.end_ts or raw.5m.end_ts")


def validate_data_coverage(root: Path, candidate: Candidate, windows: Windows) -> None:
    manifest = load_json(data_manifest_path(root, candidate))
    required_end = scoreable_outcome_end(windows)
    try:
        actual_end = parse_ts(data_manifest_end_ts(manifest))
    except MonthlyStage0Error as exc:
        raise MonthlyStage0Error(f"{exc} for {candidate.asset}") from exc
    if actual_end < required_end:
        raise MonthlyStage0Error(
            f"{candidate.asset} data ends at {iso_z(actual_end)}, but {candidate.strategy_id} needs "
            f"{iso_z(required_end)} to score the {candidate.forward_hours}h forward window"
        )


def validate_signal_manifest(root: Path, candidate: Candidate, windows: Windows) -> None:
    manifest = load_json(signal_manifest_path(root, candidate))
    start_raw = manifest.get("start_ts") or manifest.get("timestamp_start")
    end_raw = manifest.get("end_ts") or manifest.get("timestamp_end")
    if not start_raw or not end_raw:
        raise MonthlyStage0Error(f"signal manifest missing start/end timestamps: {signal_manifest_path(root, candidate)}")
    start = parse_ts(str(start_raw))
    end = parse_ts(str(end_raw))
    if start != windows.cycle_start:
        raise MonthlyStage0Error(
            f"{candidate.strategy_id} signal set starts at {iso_z(start)}, expected {iso_z(windows.cycle_start)}"
        )
    if end != windows.signal_end:
        raise MonthlyStage0Error(
            f"{candidate.strategy_id} signal set ends at {iso_z(end)}, expected {iso_z(windows.signal_end)}"
        )


def chosen_threshold(stage0: Path) -> float:
    payload = load_json(stage0 / "scores" / "threshold_calibration.json")
    value = payload.get("chosen_threshold_pct")
    if value is None:
        raise MonthlyStage0Error(f"threshold_calibration.json missing chosen_threshold_pct: {stage0}")
    return float(value)


def command_to_text(cmd: list[str]) -> str:
    return " ".join(cmd)


def scoreable_signal_subset_dir(stage0: Path) -> Path:
    return stage0 / "scores" / "_scoreable_signal_subset" / "packets"


def parse_packet_timestamp(packet_path: Path) -> datetime:
    return datetime.strptime(packet_path.stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def prepare_scoreable_signal_subset(
    signal_dir: Path,
    subset_dir: Path,
    *,
    max_signal_ts: datetime,
    dry_run: bool,
) -> Path:
    if dry_run:
        return subset_dir

    subset_dir.mkdir(parents=True, exist_ok=True)
    for stale in subset_dir.glob("*.json"):
        stale.unlink()

    selected = 0
    for packet_path in sorted(signal_dir.glob("*.json")):
        if packet_path.name in {"index.json", "summary.json"}:
            continue
        try:
            packet_ts = parse_packet_timestamp(packet_path)
        except ValueError:
            continue
        if packet_ts <= max_signal_ts:
            target = subset_dir / packet_path.name
            target.write_text(packet_path.read_text())
            selected += 1

    if selected == 0:
        raise MonthlyStage0Error(
            f"scoreable signal subset is empty for {signal_dir} at cutoff {iso_z(max_signal_ts)}"
        )
    return subset_dir


def run_command(cmd: list[str], root: Path, dry_run: bool, commands: list[list[str]]) -> None:
    commands.append(cmd)
    if dry_run:
        return
    completed = subprocess.run(cmd, cwd=root, check=False)
    if completed.returncode != 0:
        raise MonthlyStage0Error(f"command failed ({completed.returncode}): {command_to_text(cmd)}")


def generator_command(root: Path, registry: dict[str, dict[str, Any]], candidate: Candidate, windows: Windows) -> list[str]:
    entry = registry.get(candidate.signal_engine_id)
    if entry is None:
        raise MonthlyStage0Error(f"unknown signal_engine_id in registry: {candidate.signal_engine_id}")
    out_dir = signal_packets_dir(root, candidate)
    cmd = [
        sys.executable,
        str(root / str(entry["replay_generator_path"])),
        "--asset",
        candidate.asset,
        "--start",
        iso_z(windows.cycle_start),
        "--end",
        iso_z(windows.signal_end),
        "--vote-threshold",
        str(candidate.vote_threshold),
        "--window-minutes",
        str(candidate.window_minutes),
        "--out-dir",
        str(out_dir),
    ]
    for key, value in sorted((candidate.scanner_args or {}).items()):
        flag = f"--{key.replace('_', '-')}"
        if isinstance(value, list):
            cmd.append(flag)
            cmd.extend(str(item) for item in value)
        elif isinstance(value, bool):
            if value:
                cmd.append(flag)
        else:
            cmd.extend([flag, str(value)])
    return cmd


def build_candidate(
    root: Path,
    registry: dict[str, dict[str, Any]],
    candidate: Candidate,
    windows: Windows,
    dry_run: bool,
    commands: list[list[str]],
) -> Path:
    validate_data_coverage(root, candidate, windows)

    stage0 = stage0_dir(root, candidate)
    scores = stage0 / "scores"
    ground_truth = scores / "ground_truth"
    if not dry_run:
        scores.mkdir(parents=True, exist_ok=True)
        ground_truth.mkdir(parents=True, exist_ok=True)

    run_command(generator_command(root, registry, candidate, windows), root, dry_run, commands)
    if not dry_run:
        validate_signal_manifest(root, candidate, windows)

    signal_dir = signal_packets_dir(root, candidate)
    candles = raw_candles_path(root, candidate)
    subset_dir = prepare_scoreable_signal_subset(
        signal_dir,
        scoreable_signal_subset_dir(stage0),
        max_signal_ts=scoreable_signal_end(windows, forward_hours=candidate.forward_hours),
        dry_run=dry_run,
    )

    run_command(
        [
            sys.executable,
            str(root / "artifacts/skills/agentic-quant-trading-development/scripts/optimization/max_travel_distribution.py"),
            str(subset_dir),
            "--candles",
            str(candles),
            "--forward-hours",
            str(candidate.forward_hours),
            "--asset",
            candidate.asset,
            "--vote-threshold",
            str(candidate.vote_threshold),
            "--out",
            str(scores / "travel_distribution.json"),
        ],
        root,
        dry_run,
        commands,
    )
    run_command(
        [
            sys.executable,
            str(root / "artifacts/skills/agentic-quant-trading-development/scripts/optimization/significance_threshold_calibration.py"),
            str(subset_dir),
            "--candles",
            str(candles),
            "--forward-hours",
            str(candidate.forward_hours),
            "--threshold-range",
            str(candidate.threshold_range[0]),
            str(candidate.threshold_range[1]),
            str(candidate.threshold_range[2]),
            "--asset",
            candidate.asset,
            "--vote-threshold",
            str(candidate.vote_threshold),
            "--out",
            str(scores / "threshold_calibration.json"),
        ],
        root,
        dry_run,
        commands,
    )

    threshold = 0.0 if dry_run else chosen_threshold(stage0)
    run_command(
        [
            sys.executable,
            str(root / "artifacts/skills/agentic-quant-trading-development/scripts/optimization/signal_ground_truth.py"),
            str(subset_dir),
            "--candles",
            str(candles),
            "--forward-hours",
            str(candidate.forward_hours),
            "--significance-threshold",
            str(threshold),
            "--asset",
            candidate.asset,
            "--vote-threshold",
            str(candidate.vote_threshold),
            "--out",
            str(ground_truth),
        ],
        root,
        dry_run,
        commands,
    )

    signal_family = str(registry[candidate.signal_engine_id].get("signal_family") or candidate.signal_engine_id)
    run_command(
        [
            sys.executable,
            str(root / "artifacts/skills/agentic-quant-trading-development/scripts/build_stage0_manifest.py"),
            str(root),
            "--asset",
            candidate.asset,
            "--strategy-id",
            candidate.strategy_id,
            "--signal-engine-id",
            candidate.signal_engine_id,
            "--signal-family",
            signal_family,
            "--signal-set-id",
            candidate.signal_set_id,
            "--forward-hours",
            str(candidate.forward_hours),
            "--threshold-pct",
            str(threshold),
            "--scoreable-signal-end",
            iso_z(scoreable_signal_end(windows, forward_hours=candidate.forward_hours)),
            "--scoreable-outcome-end",
            iso_z(scoreable_outcome_end(windows)),
        ],
        root,
        dry_run,
        commands,
    )
    return stage0 / "manifest.json"


def validate_stage0_manifest(root: Path, manifest_path: Path, windows: Windows) -> dict[str, Any]:
    manifest = load_json(manifest_path)
    signal_manifest_rel = manifest.get("signal_set_manifest")
    if not signal_manifest_rel:
        raise MonthlyStage0Error(f"Stage 0 manifest missing signal_set_manifest: {manifest_path}")
    signal_manifest = root / str(signal_manifest_rel)
    signal_payload = load_json(signal_manifest)
    start_raw = signal_payload.get("start_ts") or signal_payload.get("timestamp_start")
    end_raw = signal_payload.get("end_ts") or signal_payload.get("timestamp_end")
    if not start_raw or not end_raw:
        raise MonthlyStage0Error(f"signal manifest missing start/end timestamps: {signal_manifest}")
    start = parse_ts(str(start_raw))
    end = parse_ts(str(end_raw))
    errors: list[str] = []
    if start != windows.cycle_start:
        errors.append(f"start {iso_z(start)} != expected {iso_z(windows.cycle_start)}")
    if end != windows.signal_end:
        errors.append(f"end {iso_z(end)} != expected {iso_z(windows.signal_end)}")
    return {
        "stage0_manifest": rel(manifest_path, root),
        "strategy_id": manifest.get("strategy_id"),
        "asset": manifest.get("asset"),
        "signal_engine_id": manifest.get("signal_engine_id"),
        "signal_set_id": manifest.get("signal_set_id"),
        "start_ts": iso_z(start),
        "end_ts": iso_z(end),
        "valid": not errors,
        "errors": errors,
    }


def build_monthly_stage0(
    root: Path,
    walk_forward_month: str,
    as_of_date: str,
    candidate_config: Path,
    out_dir: Path,
    dry_run: bool,
    path_a_threshold_pct: float,
) -> dict[str, Any]:
    windows = default_windows(walk_forward_month)
    registry = load_registry(root)
    if not candidate_config.is_absolute():
        candidate_config = root / candidate_config
    candidates = load_candidates(candidate_config, windows)
    commands: list[list[str]] = []
    stage0_manifests: list[Path] = []
    for candidate in candidates:
        stage0_manifests.append(build_candidate(root, registry, candidate, windows, dry_run, commands))

    universe_cmd = [
        sys.executable,
        str(root / "artifacts/skills/agentic-quant-trading-development/scripts/build_walk_forward_universe.py"),
        str(root),
        "--walk-forward-month",
        walk_forward_month,
        "--as-of-date",
        as_of_date,
        "--out-dir",
        str(out_dir),
        "--path-a-threshold-pct",
        str(path_a_threshold_pct),
    ]
    for path in stage0_manifests:
        universe_cmd.extend(["--stage0-manifest", str(path)])
    run_command(universe_cmd, root, dry_run, commands)
    run_command(
        [
            sys.executable,
            str(root / "artifacts/skills/agentic-quant-trading-development/scripts/validate_walk_forward_universe.py"),
            str(out_dir),
        ],
        root,
        dry_run,
        commands,
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "walk_forward_month": walk_forward_month,
        "as_of_date": as_of_date,
        "windows": windows.as_manifest_windows(),
        "cycle_signal_start": iso_z(windows.cycle_start),
        "cycle_signal_end": iso_z(windows.signal_end),
        "candidate_count": len(candidates),
        "stage0_manifests": [rel(path, root) for path in stage0_manifests],
        "out_dir": rel(out_dir, root),
        "dry_run": dry_run,
        "commands": [command_to_text(cmd) for cmd in commands],
    }


def validate_existing_stage0(root: Path, walk_forward_month: str, manifest_paths: list[Path]) -> dict[str, Any]:
    windows = default_windows(walk_forward_month)
    records = [validate_stage0_manifest(root, path if path.is_absolute() else root / path, windows) for path in manifest_paths]
    return {
        "schema_version": SCHEMA_VERSION,
        "walk_forward_month": walk_forward_month,
        "cycle_signal_start": iso_z(windows.cycle_start),
        "cycle_signal_end": iso_z(windows.signal_end),
        "valid": all(record["valid"] for record in records),
        "records": records,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run or validate the deterministic monthly Stage 0 workflow.")
    parser.add_argument("root", help="Workspace root")
    parser.add_argument("--walk-forward-month", required=True, help="Month being prepared, YYYY-MM")
    parser.add_argument("--as-of-date", required=True, help="Artifact date, YYYY-MM-DD")
    parser.add_argument("--candidate-config", help="JSON config with candidates[]")
    parser.add_argument("--out-dir", help="Output directory, usually dev/walk_forward/<YYYY-MM>")
    parser.add_argument("--path-a-threshold-pct", type=float, default=80.0)
    parser.add_argument("--dry-run", action="store_true", help="Print commands without executing them")
    parser.add_argument("--validate-only", action="store_true", help="Validate existing Stage 0 manifests against month policy")
    parser.add_argument("--stage0-manifest", action="append", default=[], help="Stage 0 manifest for --validate-only")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root).resolve()
    try:
        date.fromisoformat(args.as_of_date)
        if args.validate_only:
            if not args.stage0_manifest:
                raise MonthlyStage0Error("--validate-only requires at least one --stage0-manifest")
            result = validate_existing_stage0(root, args.walk_forward_month, [Path(path) for path in args.stage0_manifest])
            print(json.dumps(result, indent=2) + "\n")
            return 0 if result["valid"] else 1

        if not args.candidate_config:
            raise MonthlyStage0Error("--candidate-config is required unless --validate-only is used")
        out_dir = Path(args.out_dir) if args.out_dir else root / "dev" / "walk_forward" / args.walk_forward_month
        if not out_dir.is_absolute():
            out_dir = root / out_dir
        result = build_monthly_stage0(
            root=root,
            walk_forward_month=args.walk_forward_month,
            as_of_date=args.as_of_date,
            candidate_config=Path(args.candidate_config),
            out_dir=out_dir,
            dry_run=args.dry_run,
            path_a_threshold_pct=args.path_a_threshold_pct,
        )
    except (MonthlyStage0Error, json.JSONDecodeError, OSError) as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, indent=2) + "\n")
        return 1
    print(json.dumps(result, indent=2) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
