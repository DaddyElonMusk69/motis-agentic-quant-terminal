from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "run_monthly_stage0.py"
SPEC = importlib.util.spec_from_file_location("run_monthly_stage0", SCRIPT_PATH)
assert SPEC is not None
runner = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_registry(root: Path) -> None:
    write_json(
        root / "artifacts" / "signal_engine" / "engine_registry.json",
        {
            "vegas_ema": {
                "signal_engine_id": "vegas_ema",
                "replay_generator_path": "artifacts/signal_engine/scripts/signals/generate_training_session.py",
                "live_scanner_path": "artifacts/signal_engine/scripts/signals/scan_okx_live_signals.py",
                "dev_signals_root": "dev/signals/vegas_ema",
                "live_signals_root": "live/signals/vegas_ema",
                "signal_family": "vegas_ema",
            },
            "bollinger": {
                "signal_engine_id": "bollinger",
                "replay_generator_path": "artifacts/signal_engine/scripts/signals/generate_bollinger_training_session.py",
                "live_scanner_path": "artifacts/signal_engine/scripts/signals/scan_okx_live_bollinger_signals.py",
                "dev_signals_root": "dev/signals/bollinger",
                "live_signals_root": "live/signals/bollinger",
                "signal_family": "bollinger",
            },
        },
    )


def write_data_manifest(root: Path, asset: str, end_ts: str) -> None:
    write_json(
        root / "dev" / "data" / "manifests" / f"{asset}.json",
        {
            "asset": asset,
            "raw": {
                "5m": {
                    "path": f"dev/data/raw/{asset}/5m/candles.csv",
                    "start_ts": "2026-01-01T00:00:00Z",
                    "end_ts": end_ts,
                }
            },
        },
    )


def test_default_windows_for_june_2026_policy() -> None:
    windows = runner.default_windows("2026-06")

    assert windows.as_manifest_windows() == {
        "train_window": {"start": "2026-03-01", "end": "2026-04-30"},
        "validation_window": {"start": "2026-05-01", "end": "2026-05-24"},
        "locked_oos_window": {"start": "2026-05-25", "end": "2026-05-31"},
    }
    assert runner.iso_z(windows.cycle_start) == "2026-03-01T00:00:00Z"
    assert runner.iso_z(windows.signal_end) == "2026-06-01T00:00:00Z"
    assert runner.iso_z(runner.scoreable_outcome_end(windows)) == "2026-05-31T23:55:00Z"
    assert runner.iso_z(runner.scoreable_signal_end(windows, forward_hours=36)) == "2026-05-30T11:55:00Z"


def test_candidate_config_derives_canonical_vote2_signal_set_id(tmp_path: Path) -> None:
    config_path = tmp_path / "candidates.json"
    write_json(
        config_path,
        {
            "candidates": [
                {
                    "asset": "aaa",
                    "strategy_id": "aaa-bollinger-band-v01",
                    "signal_engine_id": "bollinger",
                }
            ]
        },
    )

    candidates = runner.load_candidates(config_path, runner.default_windows("2026-06"))

    assert len(candidates) == 1
    assert candidates[0].asset == "AAA"
    assert candidates[0].vote_threshold == 2
    assert candidates[0].signal_set_id == "2026-AAA-2h-dedupe-vote2"


def test_data_coverage_accepts_repo_manifest_raw_5m_shape(tmp_path: Path) -> None:
    candidate = runner.Candidate(
        asset="AAA",
        strategy_id="aaa-vegas-tunnel-v00",
        signal_engine_id="vegas_ema",
    ).with_year(2026)
    windows = runner.default_windows("2026-06")
    write_data_manifest(tmp_path, "AAA", "2026-05-31T23:55:00Z")

    runner.validate_data_coverage(tmp_path, candidate, windows)


def test_data_coverage_rejects_missing_forward_window(tmp_path: Path) -> None:
    candidate = runner.Candidate(
        asset="AAA",
        strategy_id="aaa-vegas-tunnel-v00",
        signal_engine_id="vegas_ema",
    ).with_year(2026)
    windows = runner.default_windows("2026-06")
    write_data_manifest(tmp_path, "AAA", "2026-05-31T23:50:00Z")

    try:
        runner.validate_data_coverage(tmp_path, candidate, windows)
    except runner.MonthlyStage0Error as exc:
        assert "needs 2026-05-31T23:55:00Z" in str(exc)
    else:
        raise AssertionError("data coverage should reject unscoreable newest packets")


def test_validate_existing_stage0_rejects_stale_signal_horizon(tmp_path: Path) -> None:
    signal_manifest = tmp_path / "dev" / "signals" / "vegas_ema" / "ZEC" / "2026-ZEC-2h-dedupe-vote2" / "manifest.json"
    write_json(
        signal_manifest,
        {
            "signal_engine_id": "vegas_ema",
            "asset": "ZEC",
            "signal_set_id": "2026-ZEC-2h-dedupe-vote2",
            "start_ts": "2026-01-01T00:00:00Z",
            "end_ts": "2026-05-26T00:00:00Z",
        },
    )
    stage0_manifest = tmp_path / "dev" / "training_sessions" / "zec-vegas-tunnel-v00" / "stage0" / "2026-ZEC-2h-dedupe-vote2" / "manifest.json"
    write_json(
        stage0_manifest,
        {
            "asset": "ZEC",
            "strategy_id": "zec-vegas-tunnel-v00",
            "signal_engine_id": "vegas_ema",
            "signal_set_id": "2026-ZEC-2h-dedupe-vote2",
            "signal_set_manifest": "dev/signals/vegas_ema/ZEC/2026-ZEC-2h-dedupe-vote2/manifest.json",
        },
    )

    result = runner.validate_existing_stage0(tmp_path, "2026-06", [stage0_manifest])

    assert result["valid"] is False
    assert result["records"][0]["errors"] == [
        "start 2026-01-01T00:00:00Z != expected 2026-03-01T00:00:00Z",
        "end 2026-05-26T00:00:00Z != expected 2026-06-01T00:00:00Z",
    ]


def test_dry_run_builds_engine_qualified_commands_without_signal_outputs(tmp_path: Path) -> None:
    write_registry(tmp_path)
    write_data_manifest(tmp_path, "AAA", "2026-06-02T12:00:00Z")
    config_path = tmp_path / "candidates.json"
    write_json(
        config_path,
        {
            "candidates": [
                {
                    "asset": "AAA",
                    "strategy_id": "aaa-bollinger-band-v01",
                    "signal_engine_id": "bollinger",
                    "scanner_args": {"bb_period": 20, "watched_bands": ["upper", "lower"]},
                }
            ]
        },
    )

    result = runner.build_monthly_stage0(
        root=tmp_path,
        walk_forward_month="2026-06",
        as_of_date="2026-06-01",
        candidate_config=config_path,
        out_dir=tmp_path / "dev" / "walk_forward" / "2026-06",
        dry_run=True,
        path_a_threshold_pct=80.0,
    )

    assert result["dry_run"] is True
    assert result["stage0_manifests"] == [
        "dev/training_sessions/aaa-bollinger-band-v01/stage0/2026-AAA-2h-dedupe-vote2/manifest.json"
    ]
    assert "generate_bollinger_training_session.py" in result["commands"][0]
    assert "--start 2026-03-01T00:00:00Z" in result["commands"][0]
    assert "--end 2026-06-01T00:00:00Z" in result["commands"][0]
    assert "--vote-threshold 2" in result["commands"][0]
    assert "--watched-bands upper lower" in result["commands"][0]
    assert "scores/_scoreable_signal_subset/packets" in result["commands"][1]
    assert "--scoreable-signal-end 2026-05-30T11:55:00Z" in result["commands"][-3]
    assert "--scoreable-outcome-end 2026-05-31T23:55:00Z" in result["commands"][-3]
    assert "build_walk_forward_universe.py" in result["commands"][-2]
    assert "validate_walk_forward_universe.py" in result["commands"][-1]
