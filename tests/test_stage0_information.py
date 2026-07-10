from __future__ import annotations

from datetime import datetime, timedelta, timezone

from quant_terminal_worker.stage0.information import (
    apply_information_q_values_to_candidates,
    benjamini_hochberg_q_values,
    compute_forward_excursion,
    generate_broad_split_random_timestamps,
    run_stage0_information_gate,
    score_split_information,
)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _candles(start: str, count: int) -> list[dict]:
    start_ts = _ts(start)
    rows = []
    for index in range(count):
        ts = start_ts + timedelta(minutes=5 * index)
        rows.append(
            {
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "open": 100,
                "high": 100.5,
                "low": 99.5,
                "close": 100,
                "confirm": 1,
            }
        )
    return rows


def test_compute_forward_excursion_uses_future_candles_only():
    candles = _candles("2026-01-05T09:00:00Z", 6)
    candles[0]["high"] = 150
    candles[1]["high"] = 103
    candles[2]["low"] = 98

    result = compute_forward_excursion(
        candles=candles,
        signal_ts=_ts("2026-01-05T09:00:00Z"),
        reference_price=100,
        forward_hours=1,
        thresholds_pct=[1.0, 2.0],
    )

    assert result["up_mfe_pct"] == 3.0
    assert result["down_mfe_pct"] == 2.0
    assert result["max_abs_mfe_pct"] == 3.0
    assert result["opposite_excursion_pct"] == 2.0
    assert result["excursion_asymmetry_pct"] == 1.0
    assert result["threshold_hits"]["1.0"] is True
    assert result["threshold_hits"]["2.0"] is True


def test_generate_broad_split_random_timestamps_uses_whole_split_and_future_lookahead():
    candles = _candles("2026-01-05T09:00:00Z", 12 * 24 * 14)
    event_timestamps = [
        _ts("2026-01-05T09:00:00Z"),
        _ts("2026-01-05T09:05:00Z"),
        _ts("2026-01-12T09:10:00Z"),
    ]

    sampled = generate_broad_split_random_timestamps(
        event_timestamps=event_timestamps,
        candles=candles,
        split_start=_ts("2026-01-05T00:00:00Z"),
        split_end=_ts("2026-01-18T23:59:59Z"),
        forward_hours=24,
        seed="stable-seed",
    )

    assert len(sampled) == len(event_timestamps)
    assert any((item.weekday(), item.hour) != (0, 9) for item in sampled)
    assert len(set(sampled)) == len(sampled)
    assert all(item + timedelta(hours=24) <= _ts(candles[-1]["timestamp"]) for item in sampled)
    assert sampled == generate_broad_split_random_timestamps(
        event_timestamps=event_timestamps,
        candles=candles,
        split_start=_ts("2026-01-05T00:00:00Z"),
        split_end=_ts("2026-01-18T23:59:59Z"),
        forward_hours=24,
        seed="stable-seed",
    )


def test_score_split_information_passes_materially_better_event_distribution():
    event_values = [2.5, 2.6, 2.8, 3.0, 3.2] * 30
    random_replicates = [[1.0, 1.1, 1.2, 1.3, 1.4] * 30 for _ in range(100)]

    score = score_split_information(
        event_values=event_values,
        random_replicates=random_replicates,
        min_event_count=100,
        p_value_threshold=0.05,
        material_lift_pct=15.0,
        probability_superiority_floor=0.55,
        bootstrap_seed="score-pass",
    )

    assert score["status"] == "pass"
    assert score["event_count"] == 150
    assert score["empirical_p_value"] <= 0.05
    assert score["median_lift_pct"] >= 15
    assert score["probability_superiority"] >= 0.55


def test_score_split_information_fails_random_like_distribution():
    event_values = [1.0, 1.1, 1.2, 1.3, 1.4] * 30
    random_replicates = [[1.0, 1.1, 1.2, 1.3, 1.4] * 30 for _ in range(100)]

    score = score_split_information(
        event_values=event_values,
        random_replicates=random_replicates,
        min_event_count=100,
        p_value_threshold=0.05,
        material_lift_pct=15.0,
        probability_superiority_floor=0.55,
        bootstrap_seed="score-fail",
    )

    assert score["status"] == "fail"
    assert score["empirical_p_value"] > 0.05


def test_score_split_information_can_require_non_degraded_validation_only():
    event_values = [1.4, 1.5, 1.6] * 10
    random_replicates = [[1.0, 1.1, 1.2] * 10 for _ in range(100)]

    score = score_split_information(
        event_values=event_values,
        random_replicates=random_replicates,
        min_event_count=30,
        p_value_threshold=0.10,
        material_lift_pct=0.0,
        probability_superiority_floor=0.50,
        bootstrap_seed="wf-small",
        require_statistical_significance=False,
    )

    assert score["status"] == "pass"
    assert score["event_count"] == 30


def test_benjamini_hochberg_q_values_are_monotonic_by_rank():
    q_values = benjamini_hochberg_q_values(
        {
            "a": 0.001,
            "b": 0.02,
            "c": 0.03,
            "d": 0.50,
        }
    )

    assert q_values["a"] <= q_values["b"] <= q_values["c"] <= q_values["d"]
    assert q_values["a"] == 0.004


def test_apply_information_q_values_downgrades_failed_multiple_comparison_candidate():
    candidates = []
    for index in range(11):
        p_value = 0.001 if index < 10 else 0.20
        candidates.append(
            {
                "candidate_id": f"candidate-{index}",
                "acceptance_status": "accepted",
                "branch_path": "path_a",
                "metrics": {
                    "stage0_information": {
                        "status": "pass",
                        "train_empirical_p_value": p_value,
                    }
                },
            }
        )

    adjusted = apply_information_q_values_to_candidates(candidates)

    weak = next(candidate for candidate in adjusted if candidate["candidate_id"] == "candidate-10")
    strong = next(candidate for candidate in adjusted if candidate["candidate_id"] == "candidate-0")
    assert weak["metrics"]["stage0_information"]["train_q_value"] > 0.10
    assert weak["metrics"]["stage0_information"]["status"] == "fail"
    assert weak["metrics"]["stage0_information"]["decision_reason"] == "fdr_q_value_above_threshold"
    assert weak["acceptance_status"] == "watchlist"
    assert weak["branch_path"] == "information_fail"
    assert strong["acceptance_status"] == "accepted"


def test_run_stage0_information_gate_reports_insufficient_split_sample():
    candles = _candles("2026-01-01T00:00:00Z", 12 * 24 * 45)
    signals = []
    for index in range(10):
        ts = _ts("2026-01-05T00:00:00Z") + timedelta(hours=index)
        signals.append(
            {
                "signal_id": f"sig-{index}",
                "timestamp": ts.isoformat().replace("+00:00", "Z"),
                "payload": {
                    "timestamp": ts.isoformat().replace("+00:00", "Z"),
                    "interactions": [{"market_price": "100"}],
                },
            }
        )

    result = run_stage0_information_gate(
        universe_run={
            "universe_run_id": "universe-small",
            "train_start": "2026-01-01",
            "train_end": "2026-01-20",
            "walk_forward_start": "2026-01-21",
            "walk_forward_end": "2026-02-10",
            "forward_hours": 24,
        },
        candidate={"signal_set_key": "engine:BTC:canonical"},
        signals=signals,
        candle_rows=candles,
        random_replicates=3,
    )

    assert result["status"] == "insufficient_sample"
    assert result["splits"]["train"]["score"]["event_count"] == 10
