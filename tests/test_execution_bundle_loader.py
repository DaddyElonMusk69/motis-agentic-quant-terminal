from __future__ import annotations

import json
from pathlib import Path

from quant_terminal_worker.execution.bundle_loader import load_execution_bundle


def test_load_execution_bundle_backfills_stage4_sizing_for_legacy_setup(tmp_path: Path):
    stage4_path = tmp_path / "dev" / "session" / "promotion" / "stage4_realized_expectancy.json"
    stage4_path.parent.mkdir(parents=True)
    stage4_path.write_text(
        json.dumps(
            {
                "run_id": "stage4-legacy",
                "best_candidate_id": "candidate-legacy",
                "simulation_inputs": {
                    "initial_capital_usdt": 10000,
                    "margin_allocation_pct": 30,
                    "leverage": 10,
                },
                "slice_windows": [
                    {"name": "walk_forward", "start": "2026-05-01T00:00:00Z", "end": "2026-06-30T23:59:59Z"},
                ],
            }
        )
    )
    bundle_root = tmp_path / "artifacts" / "execution_bundles" / "eth-bundle"
    bundle_root.mkdir(parents=True)
    strategy_path = bundle_root / "strategy.py"
    strategy_path.write_text("def decide(context):\n    return {'trade_action': 'SKIP'}\n")
    (bundle_root / "execution_setup.json").write_text(
        json.dumps(
            {
                "setup": {
                    "tp_pct": 2.3,
                    "sl_pct": 2.0,
                    "leverage": 10,
                }
            }
        )
    )

    runtime = load_execution_bundle(
        {
            "bundle_uri": str(bundle_root.relative_to(tmp_path)),
            "strategy_module_ref": str(strategy_path.relative_to(tmp_path)),
            "source_stage4_result_path": str(stage4_path.relative_to(tmp_path)),
        },
        workspace_root=tmp_path,
    )

    assert runtime["execution_setup"]["sizing"] == {
        "source": "stage4_realized_expectancy",
        "initial_capital_usdt": 10000,
        "margin_allocation_pct": 30.0,
        "leverage": 10.0,
    }
    cadence = runtime["execution_setup"]["decision_cadence"]
    assert cadence["cursor_seed_timestamp"] == "2026-06-30T23:59:59Z"
    assert cadence["source_stage4_run_id"] == "stage4-legacy"
    assert cadence["source_stage4_candidate_id"] == "candidate-legacy"
