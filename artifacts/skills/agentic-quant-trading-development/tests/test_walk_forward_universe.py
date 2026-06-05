from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parents[1] / "scripts"
BUILDER_PATH = SCRIPT_DIR / "build_walk_forward_universe.py"
VALIDATOR_PATH = SCRIPT_DIR / "validate_walk_forward_universe.py"

SPEC = importlib.util.spec_from_file_location("build_walk_forward_universe", BUILDER_PATH)
assert SPEC is not None
builder = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(builder)


def write_stage0(
    root: Path,
    asset: str,
    strategy_id: str,
    trigger_rate_pct: float,
    *,
    signal_engine_id: str | None = "vegas_ema",
    signal_family: str | None = "vegas_ema",
    signal_set_id: str | None = None,
    created_at: str = "2026-05-31T00:00:00Z",
) -> Path:
    signal_set_id = signal_set_id or f"2026-{asset}-2h-dedupe-vote2"
    path = root / "dev" / "training_sessions" / strategy_id / "stage0" / signal_set_id / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    total = 10000
    triggered = int(round(total * trigger_rate_pct / 100))
    payload = {
        "created_at": created_at,
        "asset": asset,
        "strategy_id": strategy_id,
        "signal_set_id": signal_set_id,
        "forward_hours": 36,
        "threshold_pct": 3.5,
        "metrics": {
            "total_records": total,
            "triggered_records": triggered,
            "trigger_rate_pct": trigger_rate_pct,
        },
    }
    if signal_engine_id is not None:
        payload["signal_engine_id"] = signal_engine_id
    if signal_family is not None:
        payload["signal_family"] = signal_family
    path.write_text(json.dumps(payload) + "\n")
    return path


def write_strategy(root: Path, strategy_id: str, version: str = "v0.1") -> None:
    path = root / "artifacts" / "skills" / "strategies" / strategy_id / "SKILL.md"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"---\nname: {strategy_id}\nversion: {version}\n---\n\n# {strategy_id}\n")


def write_session(
    root: Path,
    strategy_id: str,
    session_id: str,
    month: str,
    version: str = "v0.2",
    *,
    signal_set_id: str = "",
    promoted: bool = False,
) -> None:
    path = root / "dev" / "training_sessions" / strategy_id / session_id / "manifest.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "session_id": session_id,
                "created_at": f"{month}-02T00:00:00Z",
                "strategy_id": strategy_id,
                "strategy_version": version,
                "signal_engine_id": "vegas_ema",
                "walk_forward_month": month,
                "signal_set_id": signal_set_id,
            }
        )
        + "\n"
    )
    if promoted:
        promotion_dir = path.parent / "promotion"
        promotion_dir.mkdir(parents=True, exist_ok=True)
        (promotion_dir / "final_report.md").write_text("# Promotion Report\n")


def test_branch_aggregation_and_validation(tmp_path: Path) -> None:
    root = tmp_path
    path_a = write_stage0(root, "AAA", "aaa-strategy", 80.0)
    path_b = write_stage0(root, "BBB", "bbb-strategy", 79.99)
    write_strategy(root, "aaa-strategy")
    write_strategy(root, "bbb-strategy")

    result = builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[path_a, path_b],
        out_dir=root / "dev" / "walk_forward" / "2026-06",
    )

    assert result["tradable_count"] == 1
    assert result["watchlist_count"] == 1

    tradable = json.loads((root / "dev" / "walk_forward" / "2026-06" / "tradable_universe.json").read_text())
    watchlist = json.loads((root / "dev" / "walk_forward" / "2026-06" / "watchlist_universe.json").read_text())

    assert tradable["assets"][0]["asset"] == "AAA"
    assert tradable["assets"][0]["signal_engine_id"] == "vegas_ema"
    assert tradable["assets"][0]["branch_path"] == "path_a"
    assert watchlist["assets"][0]["asset"] == "BBB"
    assert watchlist["assets"][0]["signal_engine_id"] == "vegas_ema"
    assert watchlist["assets"][0]["branch_path"] == "path_b"
    assert watchlist["assets"][0]["tradability"] == "research_only"
    manifest = json.loads((root / "dev" / "walk_forward" / "2026-06" / "manifest.json").read_text())
    assert manifest["signal_engine_ids"] == ["vegas_ema"]

    completed = subprocess.run(
        [sys.executable, str(VALIDATOR_PATH), str(root / "dev" / "walk_forward" / "2026-06")],
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


def test_strategy_freshness_detection(tmp_path: Path) -> None:
    root = tmp_path
    current = write_stage0(root, "AAA", "aaa-strategy", 90.0)
    stale = write_stage0(root, "BBB", "bbb-strategy", 90.0)
    missing = write_stage0(root, "CCC", "ccc-strategy", 90.0)
    write_strategy(root, "aaa-strategy", "v0.1")
    write_strategy(root, "bbb-strategy", "v0.4")
    write_session(
        root,
        "aaa-strategy",
        "20260602_aaa_v02_stage3",
        "2026-06",
        "v0.2",
        signal_set_id="2026-AAA-2h-dedupe-vote2",
        promoted=True,
    )
    write_session(root, "bbb-strategy", "20260502_bbb_v03_stage1a", "2026-05", "v0.3")

    builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[current, stale, missing],
        out_dir=root / "dev" / "walk_forward" / "2026-06",
    )

    tradable = json.loads((root / "dev" / "walk_forward" / "2026-06" / "tradable_universe.json").read_text())
    statuses = {asset["asset"]: asset for asset in tradable["assets"]}

    assert statuses["AAA"]["strategy_training_status"] == "retrained_for_month"
    assert statuses["AAA"]["strategy_version"] == "v0.2"
    assert statuses["AAA"]["latest_training_date"] == "2026-06-02T00:00:00Z"
    assert statuses["BBB"]["strategy_training_status"] == "stale"
    assert statuses["BBB"]["strategy_version"] == "v0.4"
    assert statuses["CCC"]["strategy_training_status"] == "missing_strategy"
    assert statuses["CCC"]["strategy_version"] == ""


def test_current_month_unpromoted_session_stays_stale(tmp_path: Path) -> None:
    root = tmp_path
    stage0 = write_stage0(root, "AAA", "aaa-strategy", 90.0)
    write_strategy(root, "aaa-strategy", "v0.7")
    write_session(
        root,
        "aaa-strategy",
        "20260602_aaa_v07_stage1a",
        "2026-06",
        "v0.7",
        signal_set_id="2026-AAA-2h-dedupe-vote2",
        promoted=False,
    )

    builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[stage0],
        out_dir=root / "dev" / "walk_forward" / "2026-06",
    )

    tradable = json.loads((root / "dev" / "walk_forward" / "2026-06" / "tradable_universe.json").read_text())
    asset = tradable["assets"][0]
    assert asset["strategy_training_status"] == "stale"
    assert asset["strategy_version"] == "v0.7"
    assert asset["latest_training_session"] == "dev/training_sessions/aaa-strategy/20260602_aaa_v07_stage1a"
    assert "no promotion-grade Stage 4 expectancy report exists yet" in asset["notes"]


def test_malformed_stage0_manifest_fails(tmp_path: Path) -> None:
    root = tmp_path
    bad_path = root / "dev" / "training_sessions" / "bad-strategy" / "stage0" / "bad-set" / "manifest.json"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_text(json.dumps({"asset": "BAD"}) + "\n")

    try:
        builder.build_universe(
            root=root,
            walk_forward_month="2026-06",
            as_of_date="2026-06-01",
            stage0_manifest_paths=[bad_path],
            out_dir=root / "dev" / "walk_forward" / "2026-06",
        )
    except ValueError as exc:
        assert "missing metrics" in str(exc)
    else:
        raise AssertionError("malformed Stage 0 manifest should fail")


def test_validator_rejects_non_path_a_tradable_asset(tmp_path: Path) -> None:
    root = tmp_path
    stage0 = write_stage0(root, "AAA", "aaa-strategy", 85.0)
    write_strategy(root, "aaa-strategy")
    out_dir = root / "dev" / "walk_forward" / "2026-06"
    builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[stage0],
        out_dir=out_dir,
    )

    tradable_path = out_dir / "tradable_universe.json"
    tradable = json.loads(tradable_path.read_text())
    tradable["assets"][0]["branch_path"] = "path_b"
    tradable_path.write_text(json.dumps(tradable, indent=2) + "\n")

    completed = subprocess.run(
        [sys.executable, str(VALIDATOR_PATH), str(out_dir)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 1
    assert "expected path_a" in completed.stdout


def test_build_universe_prefers_latest_training_date_for_duplicate_strategy_id(tmp_path: Path) -> None:
    root = tmp_path
    older = write_stage0(
        root,
        "AAA",
        "shared-strategy",
        81.0,
        signal_set_id="2026-AAA-2h-dedupe-vote2",
        created_at="2026-05-10T00:00:00Z",
    )
    newer = write_stage0(
        root,
        "AAA",
        "shared-strategy",
        92.0,
        signal_set_id="2026-AAA-4h-dedupe-vote2",
        created_at="2026-05-20T00:00:00Z",
    )
    write_strategy(root, "shared-strategy", "v0.9")
    session_path = root / "dev" / "training_sessions" / "shared-strategy" / "20260603_shared_strategy_stage1a" / "manifest.json"
    session_path.parent.mkdir(parents=True, exist_ok=True)
    session_path.write_text(
        json.dumps(
            {
                "session_id": "20260603_shared_strategy_stage1a",
                "created_at": "2026-06-03T12:00:00Z",
                "strategy_id": "shared-strategy",
                "strategy_version": "v1.0",
                "signal_engine_id": "vegas_ema",
                "signal_set_id": "2026-AAA-4h-dedupe-vote2",
                "walk_forward_month": "2026-06",
            }
        )
        + "\n"
    )
    promotion_dir = session_path.parent / "promotion"
    promotion_dir.mkdir(parents=True, exist_ok=True)
    (promotion_dir / "final_report.md").write_text("# Promotion Report\n")

    result = builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[older, newer],
        out_dir=root / "dev" / "walk_forward" / "2026-06",
    )

    assert result["tradable_count"] == 1
    tradable = json.loads((root / "dev" / "walk_forward" / "2026-06" / "tradable_universe.json").read_text())
    asset = tradable["assets"][0]
    assert asset["strategy_id"] == "shared-strategy"
    assert asset["signal_set_id"] == "2026-AAA-4h-dedupe-vote2"
    assert asset["trigger_rate_pct"] == 92.0
    assert asset["latest_training_date"] == "2026-06-03T12:00:00Z"


def test_build_universe_falls_back_to_signal_family_when_signal_engine_id_missing(tmp_path: Path) -> None:
    root = tmp_path
    stage0 = write_stage0(
        root,
        "AAA",
        "aaa-strategy",
        85.0,
        signal_engine_id=None,
        signal_family="bollinger",
    )
    write_strategy(root, "aaa-strategy")

    builder.build_universe(
        root=root,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        stage0_manifest_paths=[stage0],
        out_dir=root / "dev" / "walk_forward" / "2026-06",
    )

    tradable = json.loads((root / "dev" / "walk_forward" / "2026-06" / "tradable_universe.json").read_text())
    assert tradable["assets"][0]["signal_engine_id"] == "bollinger"
