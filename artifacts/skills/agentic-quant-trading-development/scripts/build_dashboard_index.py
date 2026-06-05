#!/usr/bin/env python3
"""Build dashboard index JSON files from repository artifacts.

Usage:
    python3 build_dashboard_index.py <workspace_root>

Outputs to <workspace_root>/dev/dashboard/index/:
    overview.json
    universe_<YYYY-MM>.json
    strategies.json
    sessions.json
    live_status.json
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_json(path: Path):
    """Load JSON file, return None on error."""
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


def write_json(path: Path, data) -> None:
    """Write data as formatted JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def rel_path(path: Path, root: Path) -> str:
    """Return path relative to root as posix string."""
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except ValueError:
        return str(path)


def find_current_month(root: Path) -> str | None:
    """Find latest YYYY-MM directory under dev/walk_forward/."""
    wf_dir = root / "dev" / "walk_forward"
    if not wf_dir.exists():
        return None
    months = sorted(
        d.name for d in wf_dir.iterdir()
        if d.is_dir() and not d.name.startswith(".")
    )
    return months[-1] if months else None


def has_promotion_report(session_dir: Path) -> bool:
    """Check if session has promotion/final_report.md."""
    return (session_dir / "promotion" / "final_report.md").exists()


# ---------------------------------------------------------------------------
# Score headline extraction
# ---------------------------------------------------------------------------

def extract_score_headline(iteration_dir: Path, stage: str) -> dict | None:
    """Extract score headline metrics from iteration score files."""
    scores_dir = iteration_dir / "scores"
    if not scores_dir.exists():
        return None

    stage_lower = stage.lower() if stage else ""

    # Stage 1A
    if "stage1a" in stage_lower or "1a" in stage_lower:
        data = load_json(scores_dir / "stage1a_directional_scores.json")
        if data and isinstance(data, dict):
            m = data.get("metrics", {})
            return {
                "type": "stage1a",
                "match": m.get("match"),
                "scoreable": m.get("scoreable"),
                "directional_agreement_pct": m.get("directional_agreement_pct"),
                "passed_stage_gate": m.get("passed_stage_gate"),
            }

    # Stage 1B
    if "stage1b" in stage_lower or "1b" in stage_lower:
        data = load_json(scores_dir / "stage1b_screening_scores.json")
        if data and isinstance(data, dict):
            m = data.get("metrics", {})
            return {"type": "stage1b", **m}

    # Stage 2 — capture curve
    data = load_json(scores_dir / "stage2_capture_curve.json")
    if data and isinstance(data, dict):
        results = data.get("results", {})
        headline: dict = {
            "type": "stage2",
            "total_signals": data.get("total_signals"),
        }
        for level in ["3.5", "5.0"]:
            if level in results:
                headline[f"rate_{level.replace('.', '_')}"] = results[level].get("rate")
        return headline

    # Stage 3 — check for presence of stage3 artefacts
    stage3_items = sorted(scores_dir.glob("stage3_*"))
    if stage3_items:
        return {"type": "stage3", "items": [item.name for item in stage3_items]}

    return None


# ---------------------------------------------------------------------------
# Iteration / session scanning
# ---------------------------------------------------------------------------

def scan_iteration(iteration_dir: Path, root: Path) -> dict:
    """Scan a single iteration directory."""
    manifest = load_json(iteration_dir / "manifest.json") or {}
    stage = str(manifest.get("stage", ""))

    decisions_dir = iteration_dir / "decisions"
    scores_dir = iteration_dir / "scores"

    def _has_json(d: Path) -> bool:
        if not d.exists():
            return False
        return any(f.suffix == ".json" for f in d.iterdir() if f.name != ".gitkeep")

    return {
        "iteration_id": manifest.get("iteration_id", iteration_dir.name),
        "created_at": manifest.get("created_at", ""),
        "stage": stage,
        "sample_method": manifest.get("sample_method", ""),
        "sample_size": manifest.get("sample_size", 0),
        "decisions_present": _has_json(decisions_dir),
        "scores_present": _has_json(scores_dir),
        "audit_present": (iteration_dir / "audits" / "failure_audit.md").exists(),
        "summary_present": (iteration_dir / "summaries" / "iteration_summary.md").exists(),
        "score_headline": extract_score_headline(iteration_dir, stage),
    }


def scan_session(session_dir: Path, root: Path) -> dict | None:
    """Scan a training session directory."""
    manifest = load_json(session_dir / "manifest.json")
    if not manifest or not isinstance(manifest, dict):
        return None

    iterations_dir = session_dir / "iterations"
    iterations = []
    if iterations_dir.exists():
        for iter_dir in sorted(iterations_dir.iterdir()):
            if iter_dir.is_dir() and not iter_dir.name.startswith("."):
                iterations.append(scan_iteration(iter_dir, root))

    return {
        "session_id": manifest.get("session_id", session_dir.name),
        "created_at": manifest.get("created_at", ""),
        "asset": manifest.get("asset", ""),
        "strategy_id": manifest.get("strategy_id", ""),
        "strategy_version": manifest.get("strategy_version", ""),
        "signal_engine_id": manifest.get("signal_engine_id", ""),
        "signal_family": manifest.get("signal_family", ""),
        "signal_set_id": manifest.get("signal_set_id", ""),
        "stage": manifest.get("stage", ""),
        "walk_forward_month": manifest.get("walk_forward_month", ""),
        "branch_path": manifest.get("branch_path", ""),
        "train_window": manifest.get("train_window"),
        "validation_window": manifest.get("validation_window"),
        "locked_oos_window": manifest.get("locked_oos_window"),
        "promotion_report_present": has_promotion_report(session_dir),
        "iteration_count": len(iterations),
        "iterations": iterations,
        "session_path": rel_path(session_dir, root),
    }


def scan_stage0(strategy_dir: Path) -> dict | None:
    """Scan stage0 directory for a strategy."""
    stage0_dir = strategy_dir / "stage0"
    if not stage0_dir.exists():
        return None

    results = []
    for signal_set_dir in sorted(stage0_dir.iterdir()):
        if not signal_set_dir.is_dir() or signal_set_dir.name.startswith("."):
            continue
        manifest = load_json(signal_set_dir / "manifest.json")
        if not manifest or not isinstance(manifest, dict):
            continue
        metrics = manifest.get("metrics", {})
        results.append({
            "signal_set_id": manifest.get("signal_set_id", signal_set_dir.name),
            "asset": manifest.get("asset", ""),
            "total_valid_signals": metrics.get(
                "total_valid_signals", metrics.get("total_records")
            ),
            "triggered_signals": metrics.get(
                "triggered_signals", metrics.get("triggered_records")
            ),
            "trigger_rate_pct": metrics.get("trigger_rate_pct"),
            "threshold_pct": manifest.get("threshold_pct"),
            "forward_hours": manifest.get("forward_hours"),
        })

    return results[-1] if results else None


# ---------------------------------------------------------------------------
# Top-level index builders
# ---------------------------------------------------------------------------

def build_overview(
    month: str,
    manifest_data: dict,
    tradable_rows: list,
    watchlist_rows: list,
    live_timestamps: dict,
) -> dict:
    """Build overview.json content."""
    all_rows = tradable_rows + watchlist_rows
    retrained = sum(
        1 for r in all_rows
        if r.get("strategy_training_status") == "retrained_for_month"
    )
    stale = sum(
        1 for r in all_rows if r.get("strategy_training_status") == "stale"
    )
    missing = sum(
        1 for r in all_rows
        if r.get("strategy_training_status") == "missing_strategy"
    )
    ready = sum(
        1 for r in all_rows
        if r.get("branch_path") == "path_a"
        and r.get("strategy_training_status") == "retrained_for_month"
    )
    assets = sorted({r.get("asset", "") for r in all_rows if r.get("asset")})

    return {
        "current_month": month,
        "total_tradable_rows": len(tradable_rows),
        "total_watchlist_rows": len(watchlist_rows),
        "retrained_count": retrained,
        "stale_count": stale,
        "missing_count": missing,
        "ready_for_live_count": ready,
        "signal_engines": manifest_data.get("signal_engine_ids", []),
        "assets": assets,
        "train_window": manifest_data.get("train_window", {}),
        "validation_window": manifest_data.get("validation_window", {}),
        "locked_oos_window": manifest_data.get("locked_oos_window", {}),
        "live_scan_timestamps": live_timestamps,
    }


def build_universe(
    root: Path, month: str, tradable_data: dict, watchlist_data: dict
) -> dict:
    """Build universe_YYYY-MM.json content."""
    rows = []

    for item in tradable_data.get("assets", []):
        session_path = item.get("latest_training_session", "")
        row = {
            **item,
            "list": "tradable",
            "promotion_report_present": (
                has_promotion_report(root / session_path) if session_path else False
            ),
            "live_candidate": item.get("branch_path") == "path_a",
            "ready_for_live_month": (
                item.get("branch_path") == "path_a"
                and item.get("strategy_training_status") == "retrained_for_month"
            ),
        }
        rows.append(row)

    for item in watchlist_data.get("assets", []):
        session_path = item.get("latest_training_session", "")
        row = {
            **item,
            "list": "watchlist",
            "promotion_report_present": (
                has_promotion_report(root / session_path) if session_path else False
            ),
            "live_candidate": False,
            "ready_for_live_month": False,
        }
        rows.append(row)

    return {"walk_forward_month": month, "rows": rows}


def build_strategies(root: Path, universe_rows: list) -> list:
    """Build strategies.json content."""
    training_dir = root / "dev" / "training_sessions"
    if not training_dir.exists():
        return []

    # Universe lookup by strategy_id
    universe_by_strategy: dict[str, dict] = {}
    for row in universe_rows:
        sid = row.get("strategy_id")
        if sid:
            universe_by_strategy[sid] = row

    strategies = []
    for strategy_dir in sorted(training_dir.iterdir()):
        if not strategy_dir.is_dir() or strategy_dir.name.startswith("."):
            continue

        strategy_id = strategy_dir.name
        stage0 = scan_stage0(strategy_dir)

        # Collect all sessions
        sessions = []
        for session_dir in sorted(strategy_dir.iterdir()):
            if (
                not session_dir.is_dir()
                or session_dir.name.startswith(".")
                or session_dir.name == "stage0"
            ):
                continue
            session = scan_session(session_dir, root)
            if session:
                sessions.append(session)

        sessions.sort(key=lambda s: s.get("created_at", ""))

        # Find latest promoted session
        latest_promoted = None
        for s in reversed(sessions):
            if s["promotion_report_present"]:
                latest_promoted = s["session_id"]
                break

        # Find latest score headline and audit path
        latest_score = None
        latest_audit = None
        for s in reversed(sessions):
            for it in reversed(s.get("iterations", [])):
                if it.get("score_headline") and latest_score is None:
                    latest_score = it["score_headline"]
                if it.get("audit_present") and latest_audit is None:
                    latest_audit = (
                        f"dev/training_sessions/{strategy_id}/"
                        f"{s['session_id']}/iterations/"
                        f"{it['iteration_id']}/audits/failure_audit.md"
                    )
                if latest_score is not None and latest_audit is not None:
                    break
            if latest_score is not None and latest_audit is not None:
                break

        strategies.append({
            "strategy_id": strategy_id,
            "universe_row": universe_by_strategy.get(strategy_id),
            "stage0": stage0,
            "session_count": len(sessions),
            "sessions": [
                {
                    "session_id": s["session_id"],
                    "created_at": s["created_at"],
                    "stage": s["stage"],
                    "strategy_version": s["strategy_version"],
                    "walk_forward_month": s["walk_forward_month"],
                    "promotion_report_present": s["promotion_report_present"],
                    "iteration_count": s["iteration_count"],
                }
                for s in sessions
            ],
            "latest_promoted_session": latest_promoted,
            "latest_score_headline": latest_score,
            "latest_audit_path": latest_audit,
        })

    return strategies


def build_sessions_index(root: Path) -> list:
    """Build sessions.json — flat list of all sessions with iterations."""
    training_dir = root / "dev" / "training_sessions"
    if not training_dir.exists():
        return []

    all_sessions = []
    for strategy_dir in sorted(training_dir.iterdir()):
        if not strategy_dir.is_dir() or strategy_dir.name.startswith("."):
            continue
        for session_dir in sorted(strategy_dir.iterdir()):
            if (
                not session_dir.is_dir()
                or session_dir.name.startswith(".")
                or session_dir.name == "stage0"
            ):
                continue
            session = scan_session(session_dir, root)
            if session:
                all_sessions.append(session)

    all_sessions.sort(key=lambda s: s.get("created_at", ""))
    return all_sessions


# ---------------------------------------------------------------------------
# Live state
# ---------------------------------------------------------------------------

def scan_live_state(root: Path) -> dict:
    """Scan live/data/state/ for scanner state files."""
    state_dir = root / "live" / "data" / "state"
    if not state_dir.exists():
        return {"scanned_assets": [], "scan_timestamps": {}}

    scanned: list[dict] = []
    timestamps: dict[str, str] = {}

    # Root-level state files
    for f in sorted(state_dir.glob("*.json")):
        data = load_json(f)
        if not data or not isinstance(data, dict):
            continue
        asset = data.get("asset", f.stem)
        scanned.append({
            "asset": asset,
            "inst_id": data.get("inst_id", ""),
            "last_scanned_at": data.get("last_scanned_at", ""),
            "last_emitted_at": None,
            "last_packet_path": None,
            "signal_engine": None,
            "source": "root",
        })
        timestamps[asset] = data.get("last_scanned_at", "")

    # Engine-specific subdirectories
    for engine_dir in sorted(state_dir.iterdir()):
        if not engine_dir.is_dir() or engine_dir.name.startswith("."):
            continue
        engine = engine_dir.name
        for f in sorted(engine_dir.glob("*.json")):
            data = load_json(f)
            if not data or not isinstance(data, dict):
                continue
            asset = data.get("asset", f.stem)
            scanned.append({
                "asset": asset,
                "inst_id": data.get("inst_id", ""),
                "last_scanned_at": data.get("last_scanned_at", ""),
                "last_emitted_at": data.get("last_emitted_at"),
                "last_packet_path": data.get("last_packet_path"),
                "signal_engine": engine,
                "source": f"engine/{engine}",
            })
            existing = timestamps.get(asset, "")
            new_ts = data.get("last_scanned_at", "")
            if new_ts and new_ts > existing:
                timestamps[asset] = new_ts

    return {"scanned_assets": scanned, "scan_timestamps": timestamps}


def build_live_status(root: Path, universe_rows: list, live_state: dict) -> dict:
    """Build live_status.json content."""
    # Group universe rows by asset
    universe_by_asset: dict[str, list[dict]] = {}
    for row in universe_rows:
        asset = row.get("asset")
        if asset:
            universe_by_asset.setdefault(asset, []).append(row)

    scanned_asset_names = {e["asset"] for e in live_state["scanned_assets"]}

    # Annotate each scanned entry
    entries = []
    for entry in live_state["scanned_assets"]:
        asset = entry["asset"]
        rows_for_asset = universe_by_asset.get(asset, [])
        has_universe = len(rows_for_asset) > 0
        has_retrained = any(
            r.get("strategy_training_status") == "retrained_for_month"
            for r in rows_for_asset
        )

        flags: list[str] = []
        if not has_universe:
            flags.append("scanned_but_not_in_universe")
        elif not has_retrained:
            flags.append("scanned_but_not_retrained")

        entries.append({
            **entry,
            "has_matching_universe_row": has_universe,
            "has_retrained_strategy": has_retrained,
            "mismatch_flags": flags,
            "universe_statuses": [
                {
                    "strategy_id": r.get("strategy_id"),
                    "strategy_training_status": r.get("strategy_training_status"),
                }
                for r in rows_for_asset
            ],
        })

    # Inverse: retrained strategies with no live scan
    retrained_not_scanned = []
    for row in universe_rows:
        if row.get("strategy_training_status") == "retrained_for_month":
            asset = row.get("asset")
            if asset and asset not in scanned_asset_names:
                retrained_not_scanned.append({
                    "asset": asset,
                    "strategy_id": row.get("strategy_id"),
                    "mismatch_flag": "retrained_but_not_scanned",
                })

    return {
        "scanned_entries": entries,
        "retrained_not_scanned": retrained_not_scanned,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build dashboard index JSON files from repo artifacts."
    )
    parser.add_argument("root", help="Workspace root directory")
    args = parser.parse_args()

    root = Path(args.root).resolve()
    out_dir = root / "dev" / "dashboard" / "index"

    # 1. Find current month
    month = find_current_month(root)
    if not month:
        print(
            "ERROR: No walk-forward month found under dev/walk_forward/",
            file=sys.stderr,
        )
        return 1
    print(f"Indexing month: {month}")

    # 2. Load universe data
    wf_dir = root / "dev" / "walk_forward" / month
    manifest_data = load_json(wf_dir / "manifest.json") or {}
    tradable_data = load_json(wf_dir / "tradable_universe.json") or {"assets": []}
    watchlist_data = load_json(wf_dir / "watchlist_universe.json") or {"assets": []}

    tradable_rows = tradable_data.get("assets", [])
    watchlist_rows = watchlist_data.get("assets", [])
    all_universe_rows = tradable_rows + watchlist_rows

    # 3. Scan live state
    live_state = scan_live_state(root)

    # 4. Build overview
    overview = build_overview(
        month, manifest_data, tradable_rows, watchlist_rows,
        live_state["scan_timestamps"],
    )
    write_json(out_dir / "overview.json", overview)
    print(
        f"  overview.json: {overview['total_tradable_rows']} tradable, "
        f"{overview['retrained_count']} retrained, {overview['stale_count']} stale"
    )

    # 5. Build universe index
    universe = build_universe(root, month, tradable_data, watchlist_data)
    write_json(out_dir / f"universe_{month}.json", universe)
    print(f"  universe_{month}.json: {len(universe['rows'])} rows")

    # 6. Build strategies
    strategies = build_strategies(root, all_universe_rows)
    write_json(out_dir / "strategies.json", strategies)
    print(f"  strategies.json: {len(strategies)} strategies")

    # 7. Build sessions
    sessions = build_sessions_index(root)
    write_json(out_dir / "sessions.json", sessions)
    print(f"  sessions.json: {len(sessions)} sessions")

    # 8. Build live status
    live_status = build_live_status(root, all_universe_rows, live_state)
    write_json(out_dir / "live_status.json", live_status)
    print(
        f"  live_status.json: {len(live_status['scanned_entries'])} scanned, "
        f"{len(live_status['retrained_not_scanned'])} retrained-not-scanned"
    )

    print(f"\nDone. Output written to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
