#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
import re
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any


SCHEMA_VERSION = "walk_forward_universe.v0.1"


def rel(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ValueError(f"Stage 0 manifest missing: {path}")
    try:
        payload = json.loads(path.read_text())
    except json.JSONDecodeError as exc:
        raise ValueError(f"Stage 0 manifest is not valid JSON: {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Stage 0 manifest must be a JSON object: {path}")
    return payload


def month_delta(year: int, month: int, delta: int) -> tuple[int, int]:
    zero_based = year * 12 + (month - 1) + delta
    return zero_based // 12, zero_based % 12 + 1


def month_bounds(year: int, month: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-01", f"{year:04d}-{month:02d}-{last_day:02d}"


def default_windows(walk_forward_month: str) -> dict[str, dict[str, str]]:
    year, month = parse_month(walk_forward_month)
    validation_year, validation_month = month_delta(year, month, -1)
    train_start_year, train_start_month = month_delta(year, month, -3)
    train_end_year, train_end_month = month_delta(year, month, -2)

    train_start, _ = month_bounds(train_start_year, train_start_month)
    _, train_end = month_bounds(train_end_year, train_end_month)
    validation_start, validation_month_end = month_bounds(validation_year, validation_month)

    last_day = int(validation_month_end[-2:])
    oos_start_day = max(1, last_day - 6)
    oos_start = f"{validation_year:04d}-{validation_month:02d}-{oos_start_day:02d}"
    validation_end_day = max(1, oos_start_day - 1)
    validation_end = f"{validation_year:04d}-{validation_month:02d}-{validation_end_day:02d}"

    return {
        "train_window": {"start": train_start, "end": train_end},
        "validation_window": {"start": validation_start, "end": validation_end},
        "locked_oos_window": {"start": oos_start, "end": validation_month_end},
    }


def parse_month(value: str) -> tuple[int, int]:
    match = re.fullmatch(r"(\d{4})-(\d{2})", value)
    if not match:
        raise ValueError("--walk-forward-month must use YYYY-MM")
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        raise ValueError("--walk-forward-month month must be 01-12")
    return year, month


def parse_date(value: str) -> str:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("--as-of-date must use YYYY-MM-DD") from exc
    return value


def parse_window(value: str | None, fallback: dict[str, str]) -> dict[str, Any]:
    if not value:
        return fallback
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return {"label": value}
    if not isinstance(parsed, dict):
        raise ValueError("window override must be a JSON object or plain label")
    return parsed


def infer_signal_engine_id(signal_engine_id: object, signal_family: object) -> str:
    for value in (signal_engine_id, signal_family):
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def strategy_skill_version(skill_path: Path) -> str:
    if not skill_path.exists():
        return ""
    text = skill_path.read_text(errors="ignore")
    match = re.search(r"(?m)^version:\s*([^\s]+)\s*$", text)
    return match.group(1) if match else ""


def session_sort_key(manifest_path: Path, manifest: dict[str, Any]) -> tuple[str, str]:
    created_at = str(manifest.get("created_at") or "")
    session_id = str(manifest.get("session_id") or manifest_path.parent.name)
    return created_at, session_id


def has_monthly_working_setup(session_manifest_path: Path) -> bool:
    return (session_manifest_path.parent / "promotion" / "final_report.md").exists()


def scan_strategy_freshness(
    root: Path,
    strategy_id: str,
    walk_forward_month: str,
    signal_set_id: str,
) -> dict[str, Any]:
    skill_dir = root / "artifacts" / "skills" / "strategies" / strategy_id
    skill_path = skill_dir / "SKILL.md"
    if not skill_path.exists():
        return {
            "strategy_training_status": "missing_strategy",
            "strategy_version": "",
            "latest_training_session": "",
            "latest_training_date": "",
            "_selection_rank": 0,
            "notes": "Strategy skill is missing under artifacts/skills/strategies.",
        }

    training_dir = root / "dev" / "training_sessions" / strategy_id
    matching_month_sessions: list[tuple[tuple[str, str], Path, dict[str, Any]]] = []
    current_month_sessions: list[tuple[tuple[str, str], Path, dict[str, Any]]] = []
    current_month_same_signal_set_sessions: list[tuple[tuple[str, str], Path, dict[str, Any]]] = []
    all_sessions: list[tuple[tuple[str, str], Path, dict[str, Any]]] = []
    compact_month = walk_forward_month.replace("-", "")

    if training_dir.exists():
        for manifest_path in training_dir.glob("*/manifest.json"):
            try:
                manifest = json.loads(manifest_path.read_text())
            except json.JSONDecodeError:
                continue
            if not isinstance(manifest, dict):
                continue
            key = session_sort_key(manifest_path, manifest)
            all_sessions.append((key, manifest_path, manifest))
            session_id = str(manifest.get("session_id") or manifest_path.parent.name)
            in_month = (
                manifest.get("walk_forward_month") == walk_forward_month
                or session_id.startswith(compact_month)
                or str(manifest.get("created_at") or "").startswith(walk_forward_month)
            )
            if in_month:
                current_month_sessions.append((key, manifest_path, manifest))
            manifest_signal_set_id = manifest.get("signal_set_id")
            if in_month and (
                manifest_signal_set_id in (None, "")
                or manifest_signal_set_id == signal_set_id
            ):
                current_month_same_signal_set_sessions.append((key, manifest_path, manifest))
                if has_monthly_working_setup(manifest_path):
                    matching_month_sessions.append((key, manifest_path, manifest))

    if matching_month_sessions:
        _, latest_path, latest_manifest = sorted(matching_month_sessions, key=lambda item: item[0])[-1]
        return {
            "strategy_training_status": "retrained_for_month",
            "strategy_version": str(latest_manifest.get("strategy_version") or strategy_skill_version(skill_path)),
            "latest_training_session": rel(latest_path.parent, root),
            "latest_training_date": str(latest_manifest.get("created_at") or ""),
            "_selection_rank": 3,
            "notes": "Training session is tied to this walk-forward month and has a promotion-grade Stage 4 expectancy report.",
        }

    if current_month_same_signal_set_sessions:
        _, latest_path, latest_manifest = sorted(
            current_month_same_signal_set_sessions, key=lambda item: item[0]
        )[-1]
        return {
            "strategy_training_status": "stale",
            "strategy_version": str(latest_manifest.get("strategy_version") or strategy_skill_version(skill_path)),
            "latest_training_session": rel(latest_path.parent, root),
            "latest_training_date": str(latest_manifest.get("created_at") or ""),
            "_selection_rank": 2,
            "notes": "Current-month session matches this signal set, but no promotion-grade Stage 4 expectancy report exists yet.",
        }

    if current_month_sessions:
        _, latest_path, latest_manifest = sorted(current_month_sessions, key=lambda item: item[0])[-1]
        return {
            "strategy_training_status": "stale",
            "strategy_version": str(latest_manifest.get("strategy_version") or strategy_skill_version(skill_path)),
            "latest_training_session": rel(latest_path.parent, root),
            "latest_training_date": str(latest_manifest.get("created_at") or ""),
            "_selection_rank": 1,
            "notes": "Strategy skill exists, but no session is tied to this signal set for the walk-forward month.",
        }

    latest_version = strategy_skill_version(skill_path)
    latest_session = ""
    latest_manifest: dict[str, Any] = {}
    if all_sessions:
        _, latest_path, latest_manifest = sorted(all_sessions, key=lambda item: item[0])[-1]
        latest_session = rel(latest_path.parent, root)
        latest_version = latest_version or str(latest_manifest.get("strategy_version") or "")

    return {
        "strategy_training_status": "stale",
        "strategy_version": latest_version,
        "latest_training_session": latest_session,
        "latest_training_date": str(latest_manifest.get("created_at") or "") if all_sessions else "",
        "_selection_rank": 0,
        "notes": "Strategy skill exists, but no session is tied to this walk-forward month.",
    }


def stage0_decision(root: Path, manifest_path: Path, threshold_pct: float) -> dict[str, Any]:
    manifest = load_json(manifest_path)
    metrics = manifest.get("metrics")
    if not isinstance(metrics, dict):
        raise ValueError(f"Stage 0 manifest missing metrics object: {manifest_path}")

    required = ["asset", "strategy_id", "signal_set_id", "forward_hours", "threshold_pct"]
    missing = [field for field in required if manifest.get(field) in (None, "")]
    if missing:
        raise ValueError(f"Stage 0 manifest missing required fields {missing}: {manifest_path}")

    total_valid = metrics.get("total_valid_signals", metrics.get("total_records"))
    triggered = metrics.get("triggered_signals", metrics.get("triggered_records"))
    trigger_rate = metrics.get("trigger_rate_pct")
    if total_valid is None or triggered is None or trigger_rate is None:
        raise ValueError(f"Stage 0 manifest missing trigger metrics: {manifest_path}")

    try:
        trigger_rate_float = float(trigger_rate)
        total_valid_int = int(total_valid)
        triggered_int = int(triggered)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Stage 0 trigger metrics are malformed: {manifest_path}") from exc

    branch_path = "path_a" if trigger_rate_float >= threshold_pct else "path_b"
    branch_decision = (
        "monthly_tradable_candidate"
        if branch_path == "path_a"
        else "monthly_research_watchlist"
    )

    return {
        "created_at": str(manifest.get("created_at") or ""),
        "asset": str(manifest["asset"]).upper(),
        "strategy_id": str(manifest["strategy_id"]),
        "signal_engine_id": infer_signal_engine_id(
            manifest.get("signal_engine_id"),
            manifest.get("signal_family"),
        ),
        "signal_family": str(manifest.get("signal_family") or ""),
        "signal_set_id": str(manifest["signal_set_id"]),
        "stage0_manifest_path": rel(manifest_path, root),
        "total_valid_signals": total_valid_int,
        "triggered_signals": triggered_int,
        "trigger_rate_pct": round(trigger_rate_float, 2),
        "branch_path": branch_path,
        "branch_decision": branch_decision,
        "threshold_pct": float(manifest["threshold_pct"]),
        "path_a_threshold_pct": threshold_pct,
        "forward_hours": int(manifest["forward_hours"]),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_summary(path: Path, manifest: dict[str, Any], tradable: list[dict[str, Any]], watchlist: list[dict[str, Any]]) -> None:
    lines = [
        f"# Monthly Universe {manifest['walk_forward_month']}",
        "",
        f"- As of date: {manifest['as_of_date']}",
        f"- Signal engines: {', '.join(manifest['signal_engine_ids']) if manifest['signal_engine_ids'] else 'none'}",
        f"- Path A threshold: {manifest['path_a_threshold_pct']}%",
        f"- Train window: {manifest['train_window']}",
        f"- Validation window: {manifest['validation_window']}",
        f"- Locked OOS window: {manifest['locked_oos_window']}",
        f"- Tradable candidates: {len(tradable)}",
        f"- Watchlist/research assets: {len(watchlist)}",
        "",
        "## Tradable Universe",
        "",
    ]
    if tradable:
        for item in tradable:
            lines.append(
                f"- {item['strategy_id']}: {item['trigger_rate_pct']}% trigger rate, "
                f"{item['signal_engine_id']}, {item['strategy_training_status']}, {item.get('strategy_version') or 'no version'}"
            )
    else:
        lines.append("- None")
    lines.extend(["", "## Watchlist Universe", ""])
    if watchlist:
        for item in watchlist:
            lines.append(
                f"- {item['strategy_id']}: {item['trigger_rate_pct']}% trigger rate, "
                f"{item['signal_engine_id']}, {item['exclusion_reason']}, {item['strategy_training_status']}"
            )
    else:
        lines.append("- None")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def build_universe(
    root: Path,
    walk_forward_month: str,
    as_of_date: str,
    stage0_manifest_paths: list[Path],
    out_dir: Path,
    path_a_threshold_pct: float = 80.0,
    train_window: str | None = None,
    validation_window: str | None = None,
    locked_oos_window: str | None = None,
) -> dict[str, Any]:
    parse_month(walk_forward_month)
    parse_date(as_of_date)
    if not stage0_manifest_paths:
        raise ValueError("At least one --stage0-manifest is required")

    windows = default_windows(walk_forward_month)
    train = parse_window(train_window, windows["train_window"])
    validation = parse_window(validation_window, windows["validation_window"])
    locked_oos = parse_window(locked_oos_window, windows["locked_oos_window"])

    decisions = [
        stage0_decision(root, path if path.is_absolute() else root / path, path_a_threshold_pct)
        for path in stage0_manifest_paths
    ]
    inferred_engines = sorted({item["signal_engine_id"] for item in decisions if item["signal_engine_id"]})

    created_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    files = {
        "stage0_branch_decisions": "stage0_branch_decisions.json",
        "tradable_universe": "tradable_universe.json",
        "watchlist_universe": "watchlist_universe.json",
        "monthly_summary": "summaries/monthly_universe.md",
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at,
        "walk_forward_month": walk_forward_month,
        "as_of_date": as_of_date,
        "signal_engine_ids": inferred_engines,
        "train_window": train,
        "validation_window": validation,
        "locked_oos_window": locked_oos,
        "path_a_threshold_pct": path_a_threshold_pct,
        "files": files,
        "stage0_manifests": [item["stage0_manifest_path"] for item in decisions],
    }

    branch_payload = {
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at,
        "walk_forward_month": walk_forward_month,
        "as_of_date": as_of_date,
        "path_a_threshold_pct": path_a_threshold_pct,
        "decisions": decisions,
    }

    strategy_records: dict[str, dict[str, Any]] = {}
    for decision in decisions:
        freshness = scan_strategy_freshness(
            root,
            decision["strategy_id"],
            walk_forward_month,
            decision["signal_set_id"],
        )
        base_record = {**decision, **freshness}
        if decision["branch_path"] != "path_a":
            base_record = {
                **base_record,
                "exclusion_reason": "path_b_sparse_pool",
                "tradability": "research_only",
            }

        current = strategy_records.get(decision["strategy_id"])
        if current is None:
            strategy_records[decision["strategy_id"]] = base_record
            continue

        current_key = (
            int(current.get("_selection_rank") or 0),
            str(current.get("latest_training_date") or ""),
            str(current.get("created_at") or ""),
        )
        candidate_key = (
            int(base_record.get("_selection_rank") or 0),
            str(base_record.get("latest_training_date") or ""),
            str(base_record.get("created_at") or ""),
        )
        if candidate_key >= current_key:
            strategy_records[decision["strategy_id"]] = base_record

    selected_records = sorted(strategy_records.values(), key=lambda item: (item["strategy_id"], item["asset"]))
    tradable_assets = [
        {key: value for key, value in item.items() if key != "_selection_rank"}
        for item in selected_records
        if item["branch_path"] == "path_a"
    ]
    watchlist_assets = [
        {key: value for key, value in item.items() if key != "_selection_rank"}
        for item in selected_records
        if item["branch_path"] != "path_a"
    ]

    universe_meta = {
        "schema_version": SCHEMA_VERSION,
        "created_at": created_at,
        "walk_forward_month": walk_forward_month,
        "as_of_date": as_of_date,
        "path_a_threshold_pct": path_a_threshold_pct,
        "source_branch_decisions": files["stage0_branch_decisions"],
    }
    tradable_payload = {**universe_meta, "assets": tradable_assets}
    watchlist_payload = {**universe_meta, "assets": watchlist_assets}

    write_json(out_dir / "manifest.json", manifest)
    write_json(out_dir / "stage0_branch_decisions.json", branch_payload)
    write_json(out_dir / "tradable_universe.json", tradable_payload)
    write_json(out_dir / "watchlist_universe.json", watchlist_payload)
    write_summary(out_dir / "summaries" / "monthly_universe.md", manifest, tradable_assets, watchlist_assets)

    return {
        "walk_forward_month": walk_forward_month,
        "out_dir": rel(out_dir, root),
        "tradable_count": len(tradable_assets),
        "watchlist_count": len(watchlist_assets),
        "valid": True,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a monthly walk-forward tradability universe.")
    parser.add_argument("root", help="Workspace root")
    parser.add_argument("--walk-forward-month", required=True, help="Month being prepared, YYYY-MM")
    parser.add_argument("--as-of-date", required=True, help="Artifact date, YYYY-MM-DD")
    parser.add_argument("--stage0-manifest", action="append", required=True, help="Stage 0 manifest path")
    parser.add_argument("--out-dir", required=True, help="Output directory, usually dev/walk_forward/<YYYY-MM>")
    parser.add_argument("--path-a-threshold-pct", type=float, default=80.0)
    parser.add_argument("--train-window", default=None)
    parser.add_argument("--validation-window", default=None)
    parser.add_argument("--locked-oos-window", default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = root / out_dir
    stage0_paths = [Path(path) for path in args.stage0_manifest]
    try:
        result = build_universe(
            root=root,
            walk_forward_month=args.walk_forward_month,
            as_of_date=args.as_of_date,
            stage0_manifest_paths=stage0_paths,
            out_dir=out_dir,
            path_a_threshold_pct=args.path_a_threshold_pct,
            train_window=args.train_window,
            validation_window=args.validation_window,
            locked_oos_window=args.locked_oos_window,
        )
    except ValueError as exc:
        print(json.dumps({"valid": False, "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
