from __future__ import annotations

from pathlib import Path
import sys

import numpy as np
import pandas as pd
import pytest


pytest.skip(
    "legacy session-local compressed TCN was discarded; repo-level dense sequence tests replace it",
    allow_module_level=True,
)


PROMPT_DIR = (
    Path(__file__).resolve().parents[1]
    / "dev"
    / "signal_discovery_sessions"
    / "discovery-btc-2024-03-01-2026-05-30-mrlj3rgr"
    / "prompt"
)
sys.path.insert(0, str(PROMPT_DIR))

import timestamp_tcn_model as model  # noqa: E402
import train_timestamp_tcn as training  # noqa: E402


def test_sequence_store_excludes_values_unavailable_at_decision_time() -> None:
    rows = model.SEQUENCE_BINS + 3
    available = np.arange(1, rows + 1, dtype=np.int64) * 300_000_000_000
    values = np.repeat(np.arange(rows, dtype=np.float32)[:, None], 4, axis=1)
    store = model.SequenceStore(
        available_ns=available,
        binned_values=values,
        bin_bars=1,
    )
    decision_ns = int(available[model.SEQUENCE_BINS - 1])

    baseline = store.tensor_at(decision_ns)
    mutated = values.copy()
    mutated[model.SEQUENCE_BINS :] = 999_999.0
    changed_store = model.SequenceStore(
        available_ns=available,
        binned_values=mutated,
        bin_bars=1,
    )

    assert baseline is not None
    np.testing.assert_array_equal(baseline, changed_store.tensor_at(decision_ns))
    batch = store.tensor_batch_at(np.asarray([decision_ns, decision_ns], dtype=np.int64))
    np.testing.assert_array_equal(batch[0], baseline)
    np.testing.assert_array_equal(batch[1], baseline)


def test_training_labels_include_all_ordinary_negatives() -> None:
    labels, _ = training.build_timestamp_labels()

    assert len(labels) == 218_591
    assert int(labels["target"].sum()) == 107_125
    assert int((labels["target"] == 0).sum()) == 111_466
    assert int(labels["hard_negative"].sum()) == 32_085
    assert int((~labels["hard_negative"] & (labels["target"] == 0)).sum()) == 79_381


def test_purge_removes_positive_episode_that_crosses_training_boundary() -> None:
    validation_start = pd.Timestamp("2025-03-01T00:00:00Z")
    cutoff = validation_start - training.PURGE
    labels = pd.DataFrame(
        {
            "decision_ts": [cutoff - pd.Timedelta(days=1)] * 3,
            "target": [0, 1, 1],
            "episode_end": [pd.NaT, cutoff - pd.Timedelta(minutes=5), cutoff],
        }
    )
    labels["episode_end"] = pd.to_datetime(labels["episode_end"], utc=True)

    mask = training.training_mask(labels, validation_start)

    assert mask.tolist() == [True, True, False]


def test_primary_metrics_are_raw_timestamp_precision_and_recall() -> None:
    target = np.asarray([1, 1, 1, 0, 0], dtype=int)
    scores = np.asarray([0.9, 0.8, 0.1, 0.7, 0.2], dtype=float)

    metrics = training.timestamp_metrics(target, scores, threshold=0.5)

    assert metrics.true_positive == 2
    assert metrics.false_positive == 1
    assert metrics.false_negative == 1
    assert metrics.precision == pytest.approx(2 / 3, abs=1e-6)
    assert metrics.recall == pytest.approx(2 / 3, abs=1e-6)


def test_sequence_branches_cover_the_frozen_logical_history() -> None:
    minimum_days = {"5m": 7, "15m": 30, "1h": 90, "4h": 360, "1d": 365}

    for name, expected in minimum_days.items():
        assert model.MARKET_BRANCH_CONFIG[name]["logical_days"] >= expected
    assert model.FUNDING_BRANCH_CONFIG["logical_days"] >= 360
