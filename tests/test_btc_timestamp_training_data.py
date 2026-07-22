from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from quant_terminal_worker.signal_discovery import supervised_training_data as data


def _timeline(
    *,
    values: np.ndarray,
    steps: int = 3,
    name: str = "test",
) -> data.FeatureTimeline:
    timestamps = pd.date_range("2025-01-01", periods=len(values), freq="5min", tz="UTC")
    return data.FeatureTimeline(
        name=name,
        available_ns=timestamps.astype("int64").to_numpy(),
        values=np.asarray(values, dtype=np.float32),
        channel_names=tuple(f"feature_{index}" for index in range(values.shape[1])),
        spec=data.BranchSpec(name, None, 5, steps),
    )


def test_raw_labels_are_exact_directionless_timestamp_truth() -> None:
    workspace_root = Path(__file__).resolve().parents[1]
    artifact_root = (
        workspace_root
        / "dev"
        / "signal_discovery_sessions"
        / "discovery-btc-2024-04-01-2026-07-10-mrlrv8tg"
    )
    module = data.TimestampTrainingDataModule.from_session(
        workspace_root=workspace_root,
        artifact_root=artifact_root,
    )

    labels = module.load_labels()

    assert len(labels) == 218_303
    assert labels["raw_label"].value_counts().to_dict() == {
        "NEUTRAL": 81_298,
        "LONG": 68_620,
        "SHORT": 68_385,
    }
    assert int(labels["target"].sum()) == 137_005
    assert int((labels["target"] == 0).sum()) == 81_298
    assert not any("episode" in column for column in labels.columns)
    assert labels.loc[labels["raw_label"] == "LONG", "target"].eq(1).all()
    assert labels.loc[labels["raw_label"] == "SHORT", "target"].eq(1).all()
    assert labels.loc[labels["raw_label"] == "NEUTRAL", "target"].eq(0).all()
    assert labels["horizon_end_ts"].max() < module.config.walk_forward_start
    expected_ns = pd.DatetimeIndex(labels["decision_ts"]).as_unit("ns").asi8
    np.testing.assert_array_equal(labels["decision_ns"], expected_ns)


def test_dense_branch_specs_do_not_reuse_the_old_32_bin_shape() -> None:
    specs = {spec.name: spec for spec in data.BRANCH_SPECS}

    assert specs["5m_micro"].steps == 2016
    assert specs["5m_micro"].lookback_days == pytest.approx(7.0)
    assert specs["15m_short"].steps == 2880
    assert specs["15m_short"].lookback_days == pytest.approx(30.0)
    assert specs["1h_medium"].steps == 2160
    assert specs["4h_long"].steps == 2190
    assert specs["1d_regime"].steps == 384
    assert specs["funding_events"].steps == 1095
    assert all(spec.steps != 32 for spec in data.BRANCH_SPECS)


def test_preprocessor_uses_only_training_rows_and_adds_missing_masks() -> None:
    timeline = _timeline(
        values=np.asarray(
            [
                [1.0, np.nan],
                [2.0, 10.0],
                [3.0, 20.0],
                [1_000_000.0, 30.0],
            ]
        )
    )
    train_end = pd.Timestamp(timeline.available_ns[2], unit="ns", tz="UTC")

    state = data.fit_preprocessor(
        {timeline.name: timeline},
        train_end=train_end,
        clip_value=8.0,
    )
    transformed = data.transform_timelines({timeline.name: timeline}, state)[timeline.name]

    assert state.branches[timeline.name].center[0] == pytest.approx(2.0)
    assert state.branches[timeline.name].scale[0] == pytest.approx(1.0)
    assert transformed.channel_names == (
        "value:feature_0",
        "value:feature_1",
        "present:feature_0",
        "present:feature_1",
    )
    assert transformed.values[0, 1] == 0.0
    assert transformed.values[0, 3] == 0.0
    assert transformed.values[1, 3] == 1.0
    assert transformed.values[-1, 0] == 8.0


def test_dense_sequence_preserves_oldest_to_newest_order_and_excludes_future() -> None:
    timeline = _timeline(
        values=np.asarray([[1.0], [2.0], [3.0], [4.0], [5.0]]),
        steps=3,
    )
    state = data.fit_preprocessor(
        {timeline.name: timeline},
        train_end=pd.Timestamp(timeline.available_ns[-1], unit="ns", tz="UTC"),
        clip_value=100.0,
    )
    baseline = data.transform_timelines({timeline.name: timeline}, state)[timeline.name]
    decision_ns = int(timeline.available_ns[3])

    changed_values = timeline.values.copy()
    changed_values[4] = 999_999.0
    changed = data.FeatureTimeline(
        name=timeline.name,
        available_ns=timeline.available_ns,
        values=changed_values,
        channel_names=timeline.channel_names,
        spec=timeline.spec,
    )
    changed_transformed = data.transform_timelines(
        {changed.name: changed},
        state,
    )[changed.name]

    baseline_sequence = baseline.sequence_at(decision_ns)
    changed_sequence = changed_transformed.sequence_at(decision_ns)
    np.testing.assert_array_equal(baseline_sequence, changed_sequence)
    assert baseline_sequence.shape == (2, 3)
    assert baseline_sequence[0].tolist() == sorted(baseline_sequence[0].tolist())


def test_sample_index_and_dataset_contain_no_episode_metadata() -> None:
    timeline = _timeline(
        values=np.asarray([[1.0], [2.0], [3.0], [4.0]]),
        steps=3,
    )
    state = data.fit_preprocessor(
        {timeline.name: timeline},
        train_end=pd.Timestamp(timeline.available_ns[-1], unit="ns", tz="UTC"),
        clip_value=8.0,
    )
    transformed = data.transform_timelines({timeline.name: timeline}, state)
    decision_ts = pd.to_datetime(timeline.available_ns, unit="ns", utc=True)
    labels = pd.DataFrame(
        {
            "decision_ts": decision_ts,
            "decision_ns": timeline.available_ns,
            "horizon_end_ts": decision_ts + pd.Timedelta(hours=48),
            "raw_label": ["NEUTRAL", "LONG", "SHORT", "NEUTRAL"],
            "target": np.asarray([0, 1, 1, 0], dtype=np.int8),
        }
    )

    sample_index = data.build_sample_index(labels, transformed)
    dataset = data.TimestampDataset(sample_index=sample_index, timelines=transformed)
    sample = dataset[0]

    assert len(sample_index) == 2
    assert not any("episode" in column for column in sample_index.columns)
    assert set(sample) == {"decision_ns", "target", "sample_weight", "branches"}
    assert sample["sample_weight"].item() == 1.0
    assert sample["branches"][timeline.name].shape == (2, 3)


def test_audit_reports_raw_labels_and_dense_tensor_shapes() -> None:
    timeline = _timeline(values=np.asarray([[1.0], [2.0], [3.0]]), steps=3)
    decision_ts = pd.to_datetime(timeline.available_ns, unit="ns", utc=True)
    labels = pd.DataFrame(
        {
            "decision_ts": decision_ts,
            "decision_ns": timeline.available_ns,
            "horizon_end_ts": decision_ts + pd.Timedelta(hours=48),
            "raw_label": ["LONG", "SHORT", "NEUTRAL"],
            "target": np.asarray([1, 1, 0], dtype=np.int8),
        }
    )

    audit = data.build_data_audit(labels=labels, timelines={timeline.name: timeline})

    assert audit["labels"]["positive_rows"] == 2
    assert audit["labels"]["episode_fields_present"] is False
    assert audit["branches"][timeline.name]["sequence_steps"] == 3
    assert audit["branches"][timeline.name]["tensor_channels_with_masks"] == 2
