from __future__ import annotations

import json
from pathlib import Path

from quant_terminal_worker.stage0.execution import execute_stage0_candidate
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
