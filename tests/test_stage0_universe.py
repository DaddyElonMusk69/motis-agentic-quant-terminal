from __future__ import annotations

from quant_terminal_worker.stage0.universe import build_stage0_universe, build_stage0_universe_config_hash


def test_build_stage0_universe_selects_signal_sets_for_window_and_marks_duplicates():
    signal_sets = [
        {
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 340,
        },
        {
            "signal_set_key": "vegas_ema:ETH:2026-ETH-2h-dedupe-vote2",
            "signal_set_id": "2026-ETH-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "ETH",
            "start_ts": "2026-01-01T00:00:00Z",
            "end_ts": "2026-02-01T00:00:00Z",
            "packet_count": 150,
        },
    ]
    metrics_by_signal_set = {
        "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": {
            "trigger_rate_pct": 86.2,
            "total_valid_signals": 210,
            "triggered_signals": 181,
        }
    }
    existing_rnd_by_signal_set = {
        "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": {
            "strategy_id": "btc-vegas-tunnel-v01",
            "status": "created",
        }
    }

    result = build_stage0_universe(
        universe_run_id="stage0-universe-2026-03-2026-05",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T11:55:00Z",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=signal_sets,
        metrics_by_signal_set=metrics_by_signal_set,
        existing_rnd_by_signal_set=existing_rnd_by_signal_set,
        signal_counts_by_signal_set={
            "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": 42,
        },
        engine_ids=["vegas_ema"],
    )

    assert result["run"]["config_hash"]
    assert result["run"]["status"] == "created"
    assert len(result["candidates"]) == 1
    candidate = result["candidates"][0]
    assert candidate["signal_set_key"] == "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2"
    assert candidate["packet_count"] == 42
    assert candidate["acceptance_status"] == "accepted"
    assert candidate["branch_path"] == "path_a"
    assert candidate["duplicate_status"] == "existing_rnd"
    assert candidate["existing_strategy_id"] == "btc-vegas-tunnel-v01"


def test_build_stage0_universe_marks_unscored_candidates_pending():
    result = build_stage0_universe(
        universe_run_id="stage0-universe-pending",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T11:55:00Z",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=[
            {
                "signal_set_key": "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2",
                "signal_set_id": "2026-SOL-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "SOL",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-06-01T00:00:00Z",
                "packet_count": 99,
            }
        ],
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={
            "vegas_ema:SOL:2026-SOL-2h-dedupe-vote2": 12,
        },
        engine_ids=None,
    )

    candidate = result["candidates"][0]
    assert candidate["acceptance_status"] == "pending_stage0"
    assert candidate["branch_path"] == "pending"
    assert candidate["duplicate_status"] == "new"


def test_build_stage0_universe_excludes_signal_sets_without_packets_in_window():
    result = build_stage0_universe(
        universe_run_id="stage0-universe-empty-window",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T11:55:00Z",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=[
            {
                "signal_set_key": "vegas_ema:OLD:2025-OLD-2h-dedupe-vote2",
                "signal_set_id": "2025-OLD-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "OLD",
                "start_ts": None,
                "end_ts": None,
                "packet_count": 99,
            }
        ],
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={},
        engine_ids=["vegas_ema"],
    )

    assert result["candidates"] == []
    assert result["run"]["summary"]["total_candidates"] == 0


def test_build_stage0_universe_uses_scanned_coverage_for_window_eligibility():
    result = build_stage0_universe(
        universe_run_id="stage0-universe-scanned-coverage",
        window_start="2026-05-16T00:00:00Z",
        window_end="2026-05-30T23:59:59Z",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=[
            {
                "signal_set_key": "vegas_ema:AAVE:canonical",
                "signal_set_id": "canonical",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "AAVE",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-05-15T11:10:00Z",
                "coverage_start_ts": "2026-03-01T00:00:00Z",
                "coverage_end_ts": "2026-06-01T11:55:00Z",
                "packet_count": 1199,
            }
        ],
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={"vegas_ema:AAVE:canonical": 1},
        engine_ids=["vegas_ema"],
    )

    assert len(result["candidates"]) == 1
    assert result["candidates"][0]["signal_set_key"] == "vegas_ema:AAVE:canonical"


def test_build_stage0_universe_keeps_one_signal_pool_per_engine_asset():
    signal_sets = [
        {
            "signal_set_key": "vegas_ema:AAVE:legacy-2026-AAVE",
            "signal_set_id": "legacy-2026-AAVE",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-05-15T11:10:00Z",
            "packet_count": 305,
        },
        {
            "signal_set_key": "vegas_ema:AAVE:canonical",
            "signal_set_id": "canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 420,
        },
    ]

    result = build_stage0_universe(
        universe_run_id="stage0-universe-canonical-pool",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T23:59:59Z",
        train_start="2026-03-01",
        train_end="2026-04-30",
        walk_forward_start="2026-05-01",
        walk_forward_end="2026-05-30",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=signal_sets,
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={
            "vegas_ema:AAVE:legacy-2026-AAVE": 305,
            "vegas_ema:AAVE:canonical": 420,
        },
        split_signal_counts_by_signal_set={
            "vegas_ema:AAVE:legacy-2026-AAVE": {
                "train": 200,
                "walk_forward": 68,
            },
            "vegas_ema:AAVE:canonical": {
                "train": 260,
                "walk_forward": 160,
            },
        },
        engine_ids=["vegas_ema"],
    )

    assert [candidate["signal_set_key"] for candidate in result["candidates"]] == ["vegas_ema:AAVE:canonical"]


def test_build_stage0_universe_requires_packets_in_each_configured_split():
    result = build_stage0_universe(
        universe_run_id="stage0-universe-split-coverage",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T23:59:59Z",
        train_start="2026-03-01",
        train_end="2026-04-30",
        walk_forward_start="2026-05-01",
        walk_forward_end="2026-05-30",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=[
            {
                "signal_set_key": "vegas_ema:AAVE:canonical",
                "signal_set_id": "canonical",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "AAVE",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-06-01T00:00:00Z",
                "packet_count": 305,
            }
        ],
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={"vegas_ema:AAVE:canonical": 305},
        split_signal_counts_by_signal_set={
            "vegas_ema:AAVE:canonical": {
                "train": 200,
                "walk_forward": 0,
            }
        },
        engine_ids=["vegas_ema"],
    )

    assert result["candidates"] == []
    assert result["run"]["summary"]["total_candidates"] == 0


def test_build_stage0_universe_rejects_empty_walk_forward_split_even_when_scanned_coverage_exists():
    result = build_stage0_universe(
        universe_run_id="stage0-universe-scanned-empty-oos",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T23:59:59Z",
        train_start="2026-03-01",
        train_end="2026-04-30",
        walk_forward_start="2026-05-01",
        walk_forward_end="2026-05-30",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=[
            {
                "signal_set_key": "vegas_ema:AAVE:canonical",
                "signal_set_id": "canonical",
                "signal_engine_id": "vegas_ema",
                "signal_engine_version": "0.1",
                "asset": "AAVE",
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-05-15T11:10:00Z",
                "coverage_start_ts": "2026-03-01T00:00:00Z",
                "coverage_end_ts": "2026-06-01T11:55:00Z",
                "packet_count": 305,
            }
        ],
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={"vegas_ema:AAVE:canonical": 305},
        split_signal_counts_by_signal_set={
            "vegas_ema:AAVE:canonical": {
                "train": 200,
                "walk_forward": 0,
            }
        },
        engine_ids=["vegas_ema"],
    )

    assert result["candidates"] == []
    assert result["run"]["summary"]["total_candidates"] == 0


def test_build_stage0_universe_filters_by_selected_assets():
    signal_sets = [
        {
            "signal_set_key": "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2",
            "signal_set_id": "2026-BTC-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 99,
        },
        {
            "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
            "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packet_count": 99,
        },
    ]

    result = build_stage0_universe(
        universe_run_id="stage0-universe-filtered",
        window_start="2026-03-01T00:00:00Z",
        window_end="2026-05-30T11:55:00Z",
        train_start="2026-03-01",
        train_end="2026-04-30",
        walk_forward_start="2026-05-01",
        walk_forward_end="2026-05-30",
        forward_hours=36,
        trigger_rate_threshold_pct=85,
        signal_sets=signal_sets,
        metrics_by_signal_set={},
        existing_rnd_by_signal_set={},
        signal_counts_by_signal_set={
            "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": 12,
            "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2": 12,
        },
        split_signal_counts_by_signal_set={
            "vegas_ema:BTC:2026-BTC-2h-dedupe-vote2": {
                "train": 6,
                "walk_forward": 6,
            },
            "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2": {
                "train": 6,
                "walk_forward": 6,
            },
        },
        engine_ids=["vegas_ema"],
        asset_symbols=["AAVE"],
    )

    assert [candidate["asset"] for candidate in result["candidates"]] == ["AAVE"]


def test_stage0_universe_config_hash_includes_batch_split_windows():
    base = {
        "window_start": "2026-03-01T00:00:00Z",
        "window_end": "2026-05-31T23:59:59Z",
        "forward_hours": 36,
        "trigger_rate_threshold_pct": 85,
        "train_start": "2026-03-01",
        "train_end": "2026-04-30",
        "walk_forward_start": "2026-05-01",
        "walk_forward_end": "2026-05-31",
        "engine_ids": ["vegas_ema"],
        "asset_symbols": ["BTC"],
    }

    original_hash = build_stage0_universe_config_hash(**base)
    changed_split_hash = build_stage0_universe_config_hash(
        **{
            **base,
            "train_end": "2026-04-20",
            "walk_forward_start": "2026-04-21",
        }
    )

    assert changed_split_hash != original_hash
