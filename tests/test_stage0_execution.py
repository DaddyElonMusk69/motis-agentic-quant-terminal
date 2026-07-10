from __future__ import annotations

import json
from pathlib import Path

from quant_terminal_worker.stage0.execution import execute_stage0_candidate, execute_stage0_information_gate
from quant_terminal_worker.stage0.workspace import ensure_stage0_legacy_workspace_manifest


def test_ensure_stage0_legacy_workspace_manifest_creates_required_scaffold(tmp_path: Path):
    ensure_stage0_legacy_workspace_manifest(tmp_path)

    manifest = json.loads((tmp_path / "workspace_manifest.json").read_text())

    assert manifest["directories"] == {
        "dev": "dev",
        "live": "live",
        "artifacts": "artifacts",
    }
    assert (tmp_path / "dev").is_dir()
    assert (tmp_path / "live").is_dir()
    assert (tmp_path / "artifacts").is_dir()


def test_execute_stage0_candidate_runs_skill_steps_and_returns_updated_candidate(tmp_path: Path):
    executed: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        executed.append(command)
        if command[1].endswith("significance_threshold_calibration.py"):
            out_path = Path(command[command.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({"chosen_threshold_pct": 0.8}))
        if command[1].endswith("signal_ground_truth.py"):
            out_dir = Path(command[command.index("--out") + 1])
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir.parent / "ground_truth_summary.json").write_text(
                json.dumps(
                    {
                        "metrics": {
                            "total_records": 10,
                            "status_counts": {"triggered": 9, "no_trigger": 1},
                            "trigger_rate_pct": 90,
                            "branch_path": "path_a",
                            "branch_decision": "rich_pool_go_to_stage1a",
                        }
                    }
                )
            )

    result = execute_stage0_candidate(
        workspace_root=tmp_path,
        universe_run={
            "universe_run_id": "universe-1",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-05-30T23:59:59Z",
            "forward_hours": 36,
            "trigger_rate_threshold_pct": 85,
        },
        candidate={
            "candidate_id": "universe-1:vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
        },
        signal_set={
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "manifest": {"parameters": {"vote_threshold": 2}},
        },
        signals=[
            {
                "signal_id": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2:20260301T000000Z",
                "timestamp": "2026-03-01T00:00:00Z",
                "payload": {
                    "schema_version": "signal_packet.v2",
                    "timestamp": "2026-03-01T00:00:00Z",
                    "interactions": [{"market_price": "100"}],
                },
            }
        ],
        candle_rows=[
            {
                "timestamp": "2026-03-01T00:05:00Z",
                "open": 100,
                "high": 101,
                "low": 99,
                "close": 100,
                "volume": 1,
            }
        ],
        runner=fake_runner,
    )

    assert [Path(command[1]).name for command in executed] == [
        "max_travel_distribution.py",
        "significance_threshold_calibration.py",
        "signal_ground_truth.py",
    ]
    signal_packet_dirs = {Path(command[2]) for command in executed}
    expected_subset_dir = (
        tmp_path
        / "dev/stage0/universe-1/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2/scores/_scoreable_signal_subset/packets"
    )
    assert signal_packet_dirs == {expected_subset_dir}
    assert [path.name for path in sorted(expected_subset_dir.glob("*.json"))] == ["20260301T000000Z.json"]
    assert result["candidate"]["acceptance_status"] == "accepted"
    assert result["candidate"]["branch_path"] == "path_a"
    assert result["candidate"]["trigger_rate_pct"] == 90
    assert result["candidate"]["metrics"]["significance_threshold_pct"] == 0.8
    assert result["artifact_root"] == str(
        tmp_path / "dev/stage0/universe-1/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2"
    )


def test_execute_stage0_candidate_preserves_existing_information_without_running_gate(tmp_path: Path):
    executed: list[list[str]] = []

    def fake_runner(command: list[str]) -> None:
        executed.append(command)
        if command[1].endswith("significance_threshold_calibration.py"):
            out_path = Path(command[command.index("--out") + 1])
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({"chosen_threshold_pct": 0.8}))
        if command[1].endswith("signal_ground_truth.py"):
            out_dir = Path(command[command.index("--out") + 1])
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir.parent / "ground_truth_summary.json").write_text(
                json.dumps({"metrics": {"total_records": 120, "trigger_rate_pct": 90}})
            )

    result = execute_stage0_candidate(
        workspace_root=tmp_path,
        universe_run={
            "universe_run_id": "universe-2",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-05-30T23:59:59Z",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-30",
            "forward_hours": 36,
            "trigger_rate_threshold_pct": 85,
        },
        candidate={
            "candidate_id": "universe-2:vegas_ema:BTC:canonical",
            "signal_set_key": "vegas_ema:BTC:canonical",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "signal_set_id": "canonical",
            "metrics": {
                "stage0_information": {
                    "schema_version": "stage0_information_gate.v1",
                    "status": "pass",
                    "decision_reason": "existing_info",
                }
            },
        },
        signal_set={
            "signal_set_key": "vegas_ema:BTC:canonical",
            "signal_set_id": "canonical",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "manifest": {"parameters": {"vote_threshold": 2}},
        },
        signals=[
            {
                "signal_id": "vegas_ema:BTC:canonical:20260301T000000Z",
                "timestamp": "2026-03-01T00:00:00Z",
                "payload": {"timestamp": "2026-03-01T00:00:00Z", "interactions": [{"market_price": "100"}]},
            }
        ],
        candle_rows=[
            {"timestamp": "2026-03-01T00:05:00Z", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}
        ],
        runner=fake_runner,
    )

    stage0_root = tmp_path / "dev/stage0/universe-2/vegas_ema/BTC/canonical"
    assert not (stage0_root / "scores/event_information.json").exists()
    assert result["candidate"]["metrics"]["stage0_information"]["status"] == "pass"
    assert result["candidate"]["metrics"]["stage0_information"]["decision_reason"] == "existing_info"
    assert [Path(command[1]).name for command in executed] == [
        "max_travel_distribution.py",
        "significance_threshold_calibration.py",
        "signal_ground_truth.py",
    ]


def test_execute_stage0_information_gate_writes_artifacts_without_mutating_stage0_labeling(tmp_path: Path):
    def fake_information_gate(**_: object) -> dict:
        return {
            "schema_version": "stage0_information_gate.v1",
            "status": "fail",
            "decision_reason": "train_information_not_significant",
            "random_baseline": {"mode": "broad_split_random", "random_replicates": 100},
            "splits": {"train": {"score": {"empirical_p_value": 0.42, "median_lift_pct": 1, "event_count": 120}}},
            "monthly_stability": {"eligible_months": 2, "positive_lift_months": 1, "passed": False},
            "summary_metrics": {
                "train_event_count": 120,
                "walk_forward_event_count": 40,
                "train_median_lift_pct": 1,
                "walk_forward_median_lift_pct": -2,
                "train_empirical_p_value": 0.42,
            },
        }

    stage0_root = tmp_path / "dev/stage0/universe-3/vegas_ema/BTC/canonical"
    stale_ground_truth = stage0_root / "scores" / "ground_truth" / "stale.json"
    stale_ground_truth.parent.mkdir(parents=True, exist_ok=True)
    stale_ground_truth.write_text(json.dumps({"signal_id": "stale", "natural_direction": "LONG"}))
    (stage0_root / "scores" / "ground_truth_summary.json").write_text(json.dumps({"metrics": {"total_records": 1}}))
    (stage0_root / "scores" / "travel_distribution.json").write_text(json.dumps({"distribution": {}}))
    (stage0_root / "scores" / "threshold_calibration.json").write_text(json.dumps({"chosen_threshold_pct": 1.0}))

    result = execute_stage0_information_gate(
        workspace_root=tmp_path,
        universe_run={
            "universe_run_id": "universe-3",
            "window_start": "2026-03-01T00:00:00Z",
            "window_end": "2026-05-30T23:59:59Z",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-01",
            "walk_forward_end": "2026-05-30",
            "forward_hours": 36,
            "trigger_rate_threshold_pct": 85,
        },
        candidate={
            "candidate_id": "universe-3:vegas_ema:BTC:canonical",
            "signal_set_key": "vegas_ema:BTC:canonical",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "signal_set_id": "canonical",
            "trigger_rate_pct": None,
            "branch_path": "pending",
            "acceptance_status": "pending_stage0",
            "metrics": {},
        },
        signal_set={
            "signal_set_key": "vegas_ema:BTC:canonical",
            "signal_set_id": "canonical",
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "manifest": {"parameters": {"vote_threshold": 2}},
        },
        signals=[
            {
                "signal_id": "vegas_ema:BTC:canonical:20260301T000000Z",
                "timestamp": "2026-03-01T00:00:00Z",
                "payload": {"timestamp": "2026-03-01T00:00:00Z", "interactions": [{"market_price": "100"}]},
            }
        ],
        candle_rows=[
            {"timestamp": "2026-03-01T00:05:00Z", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1}
        ],
        information_gate_evaluator=fake_information_gate,
    )

    assert result["candidate"]["acceptance_status"] == "pending_stage0"
    assert result["candidate"]["branch_path"] == "pending"
    assert result["candidate"]["metrics"]["stage0_information"]["status"] == "fail"
    assert (stage0_root / "scores/event_information.json").is_file()
    assert (stage0_root / "scores/random_baseline.json").is_file()
    assert (stage0_root / "scores/information_summary.json").is_file()
    assert (stage0_root / "scores/ground_truth_summary.json").exists()
    assert (stage0_root / "scores/travel_distribution.json").exists()
    assert (stage0_root / "scores/threshold_calibration.json").exists()
    assert (stage0_root / "scores/ground_truth").exists()
