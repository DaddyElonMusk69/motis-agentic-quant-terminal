import json
from pathlib import Path

from quant_terminal_worker.stage1.workspace import build_stage1_gate_summary
from quant_terminal_worker.stage4.realized_expectancy import delete_stage4_realized_expectancy_run
from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy
from quant_terminal_worker.stage4.timing import generate_stage4b_timing_prompt
from quant_terminal_worker.stage4.timing import run_stage4b_timing_replay


def test_run_stage4_realized_expectancy_scores_full_decision_set(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {
                        "signal_id": "sig-enter",
                        "agent_direction": "LONG",
                        "decision_direction": "LONG",
                        "agreement": "MATCH",
                        "sample_role": "training",
                    },
                    {
                        "signal_id": "sig-skip",
                        "agent_direction": "FLAT",
                        "decision_direction": "FLAT",
                        "agreement": "NEUTRAL",
                        "sample_role": "walk_forward_test",
                    },
                ]
            }
        )
    )
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "market_tp_1p0_sl_1p0",
                        "setup": {
                            "entry_model": "market",
                            "tp_pct": 1.0,
                            "sl_pct": 1.0,
                            "timeout_policy": "close_at_cutoff",
                        },
                    }
                ]
            }
        )
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }
    signals = [
        {
            "signal_id": "sig-enter",
            "timestamp": "2026-05-01T00:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T00:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 100}],
                "active_timeframes": ["2h"],
            },
        },
        {
            "signal_id": "sig-skip",
            "timestamp": "2026-05-01T01:00:00Z",
            "payload": {
                "timestamp": "2026-05-01T01:00:00Z",
                "interactions": [{"timeframe": "2h", "market_price": 200}],
                "active_timeframes": ["2h"],
            },
        },
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 200, "high": 201, "low": 199, "close": 200},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    best = result["best_candidate"]
    assert best["candidate_id"] == "market_tp_1p0_sl_1p0"
    assert best["total_decisions"] == 2
    assert best["executed_trades"] == 1
    assert best["skipped_decisions"] == 1
    assert best["tp_hits"] == 1
    assert best["net_expectancy_pct"] == 0.75
    assert result["ledger"]["candidates"][0]["trades"][1]["entry_status"] == "SKIPPED"
    assert (promotion_root / "stage4_realized_expectancy.json").exists()
    assert (promotion_root / "stage4_trade_ledger.json").exists()
    assert (promotion_root / "stage4_optimal.json").exists()
    assert "Stage 4 Realized Expectancy" in (promotion_root / "stage4_summary.md").read_text()


def test_stage4_backtest_skips_signals_while_position_is_open(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[
            _record("sig-1", "LONG"),
            _record("sig-2", "LONG"),
            _record("sig-3", "LONG"),
        ],
        setup={"tp_pct": 10.0, "sl_pct": 10.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    signals = [
        _signal("sig-1", "2026-05-01T00:00:00Z", 100),
        _signal("sig-2", "2026-05-01T00:10:00Z", 101),
        _signal("sig-3", "2026-05-01T01:10:00Z", 110),
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101, "low": 99, "close": 100.5},
        {"timestamp": "2026-05-01T00:55:00Z", "open": 100.5, "high": 101, "low": 100, "close": 100.5},
        {"timestamp": "2026-05-01T01:00:00Z", "open": 100.5, "high": 110.5, "low": 100, "close": 110},
        {"timestamp": "2026-05-01T01:15:00Z", "open": 110, "high": 121.5, "low": 109, "close": 121},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    best = result["best_candidate"]
    assert best["total_decisions"] == 3
    assert best["executed_trades"] == 2
    assert best["skipped_decisions"] == 1
    assert best["skipped_position_open"] == 1
    first_skipped = result["ledger"]["candidates"][0]["trades"][1]
    assert first_skipped["signal_id"] == "sig-2"
    assert first_skipped["skip_reason"] == "position_open"


def test_stage4_backtest_accounts_for_okx_taker_fees_and_equity(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = {
        **_session(artifact_root),
        "train_start": "2026-05-01",
        "train_end": "2026-05-01",
        "walk_forward_start": "2026-05-02",
        "walk_forward_end": "2026-05-02",
    }
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101}]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=5,
        slippage_bps_per_side=0,
    )

    account = result["best_candidate"]["account"]
    assert account["initial_capital_usdt"] == 1000
    assert account["total_entry_fees_usdt"] == 0.75
    assert account["total_exit_fees_usdt"] == 0.7575
    assert account["total_fees_usdt"] == 1.5075
    assert account["gross_pnl_usdt"] == 15.0
    assert account["net_pnl_usdt"] == 13.4925
    assert account["ending_equity_usdt"] == 1013.4925


def test_stage4_backtest_sizes_pyramid_legs_from_full_position_margin(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={
            "tp_pct": 5.0,
            "sl_pct": 10.0,
            "max_hold_hours": 1,
            "pyramid_step_pct": 1.0,
            "max_legs": 3,
            "sl_breakeven": False,
        },
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.8, "close": 101},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101, "high": 102.3, "low": 100.8, "close": 102},
        {"timestamp": "2026-05-01T00:15:00Z", "open": 102, "high": 108, "low": 101.5, "close": 107},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    trade = result["ledger"]["candidates"][0]["trades"][0]
    assert trade["filled_legs"] == 3
    assert [leg["margin_usdt"] for leg in trade["leg_details"]] == [100.0, 100.0, 100.0]
    assert [leg["entry_notional_usdt"] for leg in trade["leg_details"]] == [500.0, 500.0, 500.0]
    assert trade["gross_pnl_usdt"] == 75.0


def test_stage4_backtest_hard_exits_at_max_hold_gate(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 10.0, "sl_pct": 10.0, "max_hold_hours": 0.25},
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101, "low": 99, "close": 100.5},
        {"timestamp": "2026-05-01T00:15:00Z", "open": 100.5, "high": 101, "low": 100, "close": 100.8},
        {"timestamp": "2026-05-01T00:20:00Z", "open": 100.8, "high": 110.5, "low": 100.5, "close": 110},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    trade = result["ledger"]["candidates"][0]["trades"][0]
    assert trade["exit_status"] == "HARD_EXIT"
    assert trade["exit_ts"] == "2026-05-01T00:15:00Z"
    assert trade["exit_price"] == 100.8


def test_stage4_backtest_fixed_sl_candidate_does_not_move_stop(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={
            "tp_pct": 3.0,
            "sl_pct": 1.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": False,
            "protect_trigger_pct": 1.0,
            "trail_sl_pct": 0.5,
            "max_hold_hours": 1,
        },
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 100.8, "close": 101.0},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.0, "high": 101.1, "low": 99.0, "close": 99.5},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    best = result["best_candidate"]
    trade = result["ledger"]["candidates"][0]["trades"][0]
    assert best["sl_hits"] == 1
    assert best["protected_sl_hits"] == 0
    assert trade["protection_enabled"] is False
    assert trade["protection_activated"] is False
    assert trade["exit_status"] == "INITIAL_SL"
    assert trade["exit_price"] == 99.0


def test_stage4_backtest_protected_candidate_moves_stop_after_trigger(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={
            "tp_pct": 3.0,
            "sl_pct": 1.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": True,
            "protect_trigger_pct": 1.0,
            "trail_sl_pct": 0.5,
            "max_hold_hours": 1,
        },
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 100.8, "close": 101.0},
        {"timestamp": "2026-05-01T00:10:00Z", "open": 101.0, "high": 101.1, "low": 100.4, "close": 100.6},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    best = result["best_candidate"]
    trade = result["ledger"]["candidates"][0]["trades"][0]
    assert best["protected_sl_hits"] == 1
    assert best["initial_sl_hits"] == 0
    assert trade["protection_enabled"] is True
    assert trade["protection_activated"] is True
    assert trade["exit_status"] == "PROTECTED_SL"
    assert trade["exit_price"] == 100.5
    assert trade["leg_details"][0]["exit_status"] == "PROTECTED_SL"


def test_stage4_backtest_uses_side_specific_candidate_policy_by_direction(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-long", "LONG"), _record("sig-short", "SHORT")],
        setup={
            "policy_mode": "side_specific",
            "tp_pct": 1.0,
            "sl_pct": 1.0,
            "final_tp_pct": 1.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": False,
            "max_hold_hours": 1,
            "side_policies": {
                "LONG": {
                    "protection_enabled": False,
                    "final_tp_pct": 2.0,
                    "lock_profit_pct": 2.0,
                    "initial_sl_pct": 0.5,
                    "protect_trigger_pct": None,
                    "trail_sl_pct": None,
                    "hard_exit_hours": 1,
                },
                "SHORT": {
                    "protection_enabled": False,
                    "final_tp_pct": 0.5,
                    "lock_profit_pct": 0.5,
                    "initial_sl_pct": 1.0,
                    "protect_trigger_pct": None,
                    "trail_sl_pct": None,
                    "hard_exit_hours": 1,
                },
            },
        },
    )
    session = _session(artifact_root)
    signals = [
        _signal("sig-long", "2026-05-01T00:00:00Z", 100),
        _signal("sig-short", "2026-05-01T02:00:00Z", 200),
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.2, "low": 99.4, "close": 100.9},
        {"timestamp": "2026-05-01T02:05:00Z", "open": 200, "high": 200.2, "low": 198.8, "close": 199.0},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    long_trade, short_trade = result["ledger"]["candidates"][0]["trades"]
    assert long_trade["decision_direction"] == "LONG"
    assert long_trade["initial_sl_pct"] == 0.5
    assert long_trade["exit_status"] == "INITIAL_SL"
    assert long_trade["exit_price"] == 99.5
    assert short_trade["decision_direction"] == "SHORT"
    assert short_trade["initial_sl_pct"] == 1.0
    assert short_trade["exit_status"] == "TP"
    assert short_trade["exit_price"] == 199.0
    assert result["best_candidate"]["setup"]["policy_mode"] == "side_specific"


def test_stage4_backtest_keeps_run_history_and_updates_latest_compatibility_files(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101}]

    first = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    second = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=2000,
        margin_allocation_pct=20,
        leverage=3,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    assert first["run_id"] != second["run_id"]
    first_run_path = promotion_root = artifact_root / "promotion" / "stage4_runs" / first["run_id"]
    second_run_path = artifact_root / "promotion" / "stage4_runs" / second["run_id"]
    assert (first_run_path / "stage4_realized_expectancy.json").exists()
    assert (second_run_path / "stage4_realized_expectancy.json").exists()

    index = json.loads((artifact_root / "promotion" / "stage4_runs" / "index.json").read_text())
    assert [run["run_id"] for run in index["runs"]] == [first["run_id"], second["run_id"]]
    assert index["latest_run_id"] == second["run_id"]
    assert index["runs"][-1]["simulation_inputs"] == {
        "initial_capital_usdt": 2000,
        "margin_allocation_pct": 20,
        "leverage": 3,
    }

    latest = json.loads((artifact_root / "promotion" / "stage4_realized_expectancy.json").read_text())
    optimal = json.loads((artifact_root / "promotion" / "stage4_optimal.json").read_text())
    assert latest["run_id"] == second["run_id"]
    assert latest["source_run_path"].endswith(f"stage4_runs/{second['run_id']}/stage4_realized_expectancy.json")
    assert optimal["run_id"] == second["run_id"]


def test_stage4_delete_run_restores_previous_latest_and_clears_when_empty(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101}]

    first = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    second = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=2000,
        margin_allocation_pct=20,
        leverage=3,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    result = delete_stage4_realized_expectancy_run(workspace_root=tmp_path, session=session, run_id=second["run_id"])

    assert result["deleted_run_id"] == second["run_id"]
    assert result["latest_run_id"] == first["run_id"]
    assert result["remaining_run_count"] == 1
    assert not (artifact_root / "promotion" / "stage4_runs" / second["run_id"]).exists()
    latest = json.loads((artifact_root / "promotion" / "stage4_realized_expectancy.json").read_text())
    assert latest["run_id"] == first["run_id"]

    result = delete_stage4_realized_expectancy_run(workspace_root=tmp_path, session=session, run_id=first["run_id"])

    assert result["latest_run_id"] is None
    assert result["remaining_run_count"] == 0
    assert not (artifact_root / "promotion" / "stage4_realized_expectancy.json").exists()
    assert not (artifact_root / "promotion" / "stage4_trade_ledger.json").exists()
    assert not (artifact_root / "promotion" / "stage4_optimal.json").exists()
    assert not (artifact_root / "promotion" / "stage4_summary.md").exists()


def test_stage4_best_candidate_prefers_protected_positive_oos_candidate(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {**_record("sig-train", "LONG"), "sample_role": "training"},
                    {**_record("sig-wf", "LONG"), "sample_role": "walk_forward_test"},
                ]
            }
        )
    )
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "unprotected_high_oos",
                        "setup": {
                            "entry_model": "market",
                            "timeout_policy": "close_at_cutoff",
                            "tp_pct": 5.0,
                            "sl_pct": 5.0,
                            "max_hold_hours": 0.1,
                            "protection_enabled": False,
                        },
                    },
                    {
                        "candidate_id": "protected_lower_oos",
                        "setup": {
                            "entry_model": "market",
                            "timeout_policy": "close_at_cutoff",
                            "tp_pct": 1.0,
                            "sl_pct": 5.0,
                            "max_hold_hours": 0.1,
                            "protection_enabled": True,
                            "protect_trigger_pct": 0.5,
                            "trail_sl_pct": 0.2,
                        },
                    },
                ]
            }
        )
    )
    session = {
        **_session(artifact_root),
        "train_start": "2026-05-01",
        "train_end": "2026-05-01",
        "walk_forward_start": "2026-05-02",
        "walk_forward_end": "2026-05-02",
    }
    signals = [
        _signal("sig-train", "2026-05-01T00:00:00Z", 100),
        _signal("sig-wf", "2026-05-02T00:00:00Z", 100),
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 104, "low": 99.5, "close": 104},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 104, "low": 99.5, "close": 104},
    ]

    result = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    assert result["best_candidate"]["candidate_id"] == "protected_lower_oos"
    assert result["best_candidate"]["selection_mode"] == "protected_walk_forward_net_pnl_pct"


def test_stage4_gate_displays_protected_best_for_existing_old_selection_artifact(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    unprotected = {
        "candidate_id": "unprotected_old_best",
        "setup": {"protection_enabled": False},
        "account": {"net_pnl_usdt": 2000},
        "slices": {"walk_forward_test": {"net_pnl_pct": 40, "profit_factor": 3}},
    }
    protected = {
        "candidate_id": "protected_display_best",
        "setup": {"protection_enabled": True},
        "account": {"net_pnl_usdt": 1000},
        "slices": {"walk_forward_test": {"net_pnl_pct": 20, "profit_factor": 2}},
    }
    realized_payload = {
        "run_id": "stage4-old-run",
        "best_candidate_id": unprotected["candidate_id"],
        "best_candidate": unprotected,
        "candidates": [unprotected, protected],
        "simulation_inputs": {"initial_capital_usdt": 1000, "margin_allocation_pct": 30, "leverage": 5},
    }
    (promotion_root / "stage4_realized_expectancy.json").write_text(json.dumps(realized_payload))
    (promotion_root / "stage4_trade_ledger.json").write_text(json.dumps({"candidates": []}))
    (promotion_root / "stage4_optimal.json").write_text(json.dumps({"run_id": "stage4-old-run", "best": unprotected}))
    (promotion_root / "stage4_summary.md").write_text("# Stage 4\n")
    run_root = promotion_root / "stage4_runs" / "stage4-old-run"
    run_root.mkdir(parents=True)
    run_realized_path = run_root / "stage4_realized_expectancy.json"
    run_realized_path.write_text(json.dumps(realized_payload))
    (promotion_root / "stage4_runs" / "index.json").write_text(
        json.dumps(
            {
                "latest_run_id": "stage4-old-run",
                "runs": [
                    {
                        "run_id": "stage4-old-run",
                        "created_at": "2026-06-01T00:00:00Z",
                        "best_candidate_id": unprotected["candidate_id"],
                        "best_candidate": unprotected,
                        "account": unprotected["account"],
                        "realized_expectancy_path": str(run_realized_path),
                    }
                ],
            }
        )
    )

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=_session(artifact_root))

    stage4 = gate["stage4_realized_expectancy"]
    assert stage4["best_candidate_id"] == "protected_display_best"
    assert stage4["best_candidate"]["selection_mode"] == "protected_walk_forward_net_pnl_pct"
    assert stage4["stage4_runs"][0]["best_candidate_id"] == "protected_display_best"


def test_stage4b_timing_replay_requires_stage4a_baseline(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)

    try:
        run_stage4b_timing_replay(
            workspace_root=tmp_path,
            session=session,
            signal_rows=[_signal("sig-1", "2026-05-01T00:00:00Z", 100)],
            candles=[{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101, "low": 99, "close": 100}],
        )
    except ValueError as exc:
        assert "Stage 4B requires completed Stage 4A" in str(exc)
    else:
        raise AssertionError("Stage 4B replay should require Stage 4A baseline artifacts")


def test_stage4b_timing_overlay_skips_matching_utc_hour_and_preserves_stage4a(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[
            _record("sig-1", "LONG"),
            _record("sig-2", "LONG"),
        ],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    signals = [
        _signal("sig-1", "2026-05-01T00:00:00Z", 100),
        _signal("sig-2", "2026-05-01T01:00:00Z", 100),
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101},
    ]
    stage4a = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        initial_capital_usdt=1000,
        margin_allocation_pct=30,
        leverage=5,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    promotion_root = artifact_root / "promotion"
    timing_root = promotion_root / "stage4b_timing"
    timing_root.mkdir(parents=True)
    (timing_root / "timing_overlay.json").write_text(
        json.dumps(
            {
                "schema_version": "stage4b_timing_overlay.v1",
                "source_stage4_run_id": stage4a["run_id"],
                "exclude_utc_hours": [1],
                "applies_to": "all",
                "rationale": "Skip the weak 01 UTC window.",
            }
        )
    )

    result = run_stage4b_timing_replay(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
    )

    assert result["artifact_role"] == "stage4b_timing_replay"
    assert result["baseline"]["run_id"] == stage4a["run_id"]
    assert result["best_candidate"]["executed_trades"] == 1
    assert result["best_candidate"]["skipped_timing_filter"] == 1
    trades = result["ledger"]["candidates"][0]["trades"]
    assert trades[0]["entry_status"] == "FILLED"
    assert trades[1]["entry_status"] == "SKIPPED"
    assert trades[1]["skip_reason"] == "timing_filter"
    assert (timing_root / "timing_replay.json").exists()
    assert (timing_root / "timing_trade_ledger.json").exists()
    assert (timing_root / "timing_summary.md").exists()
    assert json.loads((promotion_root / "stage4_realized_expectancy.json").read_text())["run_id"] == stage4a["run_id"]


def test_stage4b_timing_overlay_rejects_exact_signal_filters(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    signals = [_signal("sig-1", "2026-05-01T00:00:00Z", 100)]
    candles = [{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101}]
    stage4a = run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signals,
        candles=candles,
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )
    timing_root = artifact_root / "promotion" / "stage4b_timing"
    timing_root.mkdir(parents=True)
    (timing_root / "timing_overlay.json").write_text(
        json.dumps(
            {
                "schema_version": "stage4b_timing_overlay.v1",
                "source_stage4_run_id": stage4a["run_id"],
                "exclude_utc_hours": [0],
                "exclude_signal_ids": ["sig-1"],
                "rationale": "This should be rejected because it targets exact signals.",
            }
        )
    )

    try:
        run_stage4b_timing_replay(
            workspace_root=tmp_path,
            session=session,
            signal_rows=signals,
            candles=candles,
        )
    except ValueError as exc:
        assert "exact signal" in str(exc)
    else:
        raise AssertionError("Stage 4B overlay should reject exact signal filters")


def test_generate_stage4b_timing_prompt_writes_context_after_stage4a(tmp_path: Path):
    artifact_root = _write_stage4_fixture(
        tmp_path,
        records=[_record("sig-1", "LONG")],
        setup={"tp_pct": 1.0, "sl_pct": 5.0, "max_hold_hours": 1},
    )
    session = _session(artifact_root)
    run_stage4_realized_expectancy(
        workspace_root=tmp_path,
        session=session,
        signal_rows=[_signal("sig-1", "2026-05-01T00:00:00Z", 100)],
        candles=[{"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.5, "low": 99.5, "close": 101}],
        fees_bps_per_side=0,
        slippage_bps_per_side=0,
    )

    prompt = generate_stage4b_timing_prompt(workspace_root=tmp_path, session=session)

    assert prompt["prompt_type"] == "stage4b_timing_optimizer"
    assert "$stage4b-timing-optimizer" in prompt["prompt"]
    assert "timing_overlay.json" in prompt["prompt"]
    assert Path(prompt["prompt_path"]).exists()
    assert Path(prompt["context_path"]).exists()


def _write_stage4_fixture(tmp_path: Path, *, records: list[dict], setup: dict) -> Path:
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(json.dumps({"records": records}))
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps(
            {
                "candidates": [
                    {
                        "candidate_id": "market_tp_sl",
                        "setup": {
                            "entry_model": "market",
                            "timeout_policy": "close_at_cutoff",
                            **setup,
                        },
                    }
                ]
            }
        )
    )
    return artifact_root


def _session(artifact_root: Path) -> dict:
    return {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }


def _record(signal_id: str, direction: str) -> dict:
    return {
        "signal_id": signal_id,
        "agent_direction": direction,
        "decision_direction": direction,
        "agreement": "MATCH" if direction in {"LONG", "SHORT"} else "NEUTRAL",
        "sample_role": "training",
    }


def _signal(signal_id: str, timestamp: str, price: float) -> dict:
    return {
        "signal_id": signal_id,
        "timestamp": timestamp,
        "payload": {
            "timestamp": timestamp,
            "interactions": [{"timeframe": "2h", "market_price": price}],
            "active_timeframes": ["2h"],
        },
    }
