from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_worker.signal_discovery.brackets import (
    approve_training_brackets,
    build_bracket_preview,
    read_approved_bracket_contract,
)


def test_default_policy_reproduces_contiguous_directional_brackets() -> None:
    rows = [
        _row(0, "LONG"),
        _row(5, "LONG"),
        _row(10, "NEUTRAL"),
        _row(15, "LONG"),
        _row(20, "SHORT"),
    ]

    result = build_bracket_preview(
        labels=rows,
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(),
    )

    assert [(row["direction"], row["timestamp_count"]) for row in result["brackets"]] == [
        ("LONG", 2),
        ("LONG", 1),
        ("SHORT", 1),
    ]
    assert result["diagnostics"]["raw_total_brackets"] == 3
    assert result["diagnostics"]["preview_total_brackets"] == 3


def test_stability_filters_require_neighbor_r_and_all_delay_agreement() -> None:
    rows = [
        _row(0, "LONG"),
        _row(5, "LONG"),
        _row(0, "SHORT", risk=0.9),
        _row(5, "LONG", risk=0.9),
        _row(0, "LONG", delay=10),
        _row(5, "SHORT", delay=10),
    ]

    r_stable = build_bracket_preview(
        labels=rows,
        risk_values=[0.9, 1.0],
        entry_delays=[5, 10],
        policy=_policy(require_r_stability=True),
    )
    delay_stable = build_bracket_preview(
        labels=rows,
        risk_values=[0.9, 1.0],
        entry_delays=[5, 10],
        policy=_policy(require_delay_stability=True),
    )

    assert [row["start_ts"] for row in r_stable["brackets"]] == [_ts(5)]
    assert [row["start_ts"] for row in delay_stable["brackets"]] == [_ts(0)]
    assert r_stable["diagnostics"]["stability_rejected_timestamp_count"] == 1
    assert delay_stable["diagnostics"]["stability_rejected_timestamp_count"] == 1


def test_neutral_gap_bridging_is_continuous_but_never_crosses_ambiguous() -> None:
    rows = [
        _row(0, "LONG"),
        _row(5, "NEUTRAL"),
        _row(10, "LONG"),
        _row(15, "AMBIGUOUS"),
        _row(20, "LONG"),
    ]

    result = build_bracket_preview(
        labels=rows,
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(bridge_neutral_gap_intervals=1),
    )

    assert [(row["start_ts"], row["end_ts"]) for row in result["brackets"]] == [
        (_ts(0), _ts(10)),
        (_ts(20), _ts(20)),
    ]
    assert result["brackets"][0]["timestamp_count"] == 3
    assert result["brackets"][0]["inherited_timestamp_count"] == 1
    assert result["diagnostics"]["merged_gap_count"] == 1


def test_minimum_persistence_removes_short_brackets_after_bridging() -> None:
    rows = [_row(0, "LONG"), _row(5, "NEUTRAL"), _row(10, "LONG"), _row(20, "SHORT")]

    result = build_bracket_preview(
        labels=rows,
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(
            bridge_neutral_gap_intervals=1,
            minimum_persistence_timestamps=3,
        ),
    )

    assert [(row["direction"], row["timestamp_count"]) for row in result["brackets"]] == [
        ("LONG", 3)
    ]
    assert result["diagnostics"]["persistence_removed_count"] == 1


def test_one_active_opportunity_suppresses_brackets_until_anchor_resolution() -> None:
    rows = [
        _row(0, "LONG", first_touch_minutes=30),
        _row(5, "NEUTRAL"),
        _row(10, "SHORT", first_touch_minutes=20),
        _row(15, "NEUTRAL"),
        _row(35, "LONG", first_touch_minutes=45),
    ]

    result = build_bracket_preview(
        labels=rows,
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(one_active_opportunity=True),
    )

    assert [row["start_ts"] for row in result["brackets"]] == [_ts(0), _ts(35)]
    assert result["diagnostics"]["overlap_suppressed_count"] == 1


def test_approval_writes_revisioned_fingerprint_bound_artifacts(tmp_path) -> None:
    artifact_root = tmp_path / "discovery"
    labels_path = artifact_root / "atlas" / "training_timestamp_labels.parquet"
    labels_path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([_row(0, "LONG"), _row(5, "LONG")]), labels_path)

    first = approve_training_brackets(
        artifact_root=artifact_root,
        session_id="discovery-brackets",
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(),
    )
    second = approve_training_brackets(
        artifact_root=artifact_root,
        session_id="discovery-brackets",
        risk_values=[1.0],
        entry_delays=[5],
        policy=_policy(minimum_persistence_timestamps=2),
    )

    assert first["approval"]["revision"] == 1
    assert second["approval"]["revision"] == 2
    assert read_approved_bracket_contract(artifact_root=artifact_root)["revision"] == 2
    brackets_path = artifact_root / second["approval"]["training_brackets_path"]
    assert pq.read_table(brackets_path).num_rows == 1

    brackets_path.write_bytes(b"drift")
    with pytest.raises(ValueError, match="fingerprint changed"):
        read_approved_bracket_contract(artifact_root=artifact_root)


def _policy(**overrides: object) -> dict[str, object]:
    return {
        "risk_pct": 1.0,
        "entry_delay_minutes": 5,
        "horizon_hours": 36,
        "require_r_stability": False,
        "require_delay_stability": False,
        "bridge_neutral_gap_intervals": 0,
        "minimum_persistence_timestamps": 1,
        "one_active_opportunity": False,
        **overrides,
    }


def _row(
    minutes: int,
    label: str,
    *,
    risk: float = 1.0,
    delay: int = 5,
    first_touch_minutes: int | None = None,
) -> dict[str, object]:
    decision_ts = _ts(minutes)
    first_touch = _ts(first_touch_minutes) if first_touch_minutes is not None else None
    horizon_end = decision_ts + timedelta(hours=36, minutes=delay)
    return {
        "decision_ts": decision_ts,
        "entry_ts": decision_ts + timedelta(minutes=delay),
        "horizon_end_ts": horizon_end,
        "label": label,
        "risk_pct": risk,
        "scenario_entry_delay_minutes": delay,
        "scenario_horizon_hours": 36.0,
        "long": {"first_touch_ts": first_touch if label == "LONG" else None},
        "short": {"first_touch_ts": first_touch if label == "SHORT" else None},
    }


def _ts(minutes: int) -> datetime:
    return datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=minutes)
