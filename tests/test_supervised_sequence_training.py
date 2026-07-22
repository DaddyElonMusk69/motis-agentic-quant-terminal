from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest
import torch

from quant_terminal_worker.signal_discovery.supervised_sequence_model import (
    MultiResolutionCausalTCN,
    build_sequence_chunk,
    partition_indices,
)
from quant_terminal_worker.signal_discovery.supervised_sequence_training import (
    _contiguous_mixed_control_indices,
    _write_rationale,
    always_enter_comparison,
    build_expanding_folds,
    monthly_metrics,
    timestamp_metrics,
)
from quant_terminal_worker.signal_discovery.supervised_training_data import (
    BranchSpec,
    TransformedTimeline,
)


def _timeline(*, name: str = "5m_micro", steps: int = 8) -> TransformedTimeline:
    rows = 40
    return TransformedTimeline(
        name=name,
        available_ns=np.arange(1, rows + 1, dtype=np.int64) * 10,
        values=np.arange(rows * 4, dtype=np.float32).reshape(rows, 4),
        channel_names=("a", "b", "present:a", "present:b"),
        spec=BranchSpec(name=name, rule=None, interval_minutes=5, steps=steps),
    )


def test_sequence_chunk_reuses_history_and_gathers_each_decision() -> None:
    timeline = _timeline(steps=8)
    decisions = np.asarray([100, 110, 120], dtype=np.int64)

    chunk = build_sequence_chunk(
        decision_ns=decisions,
        targets=np.asarray([0, 1, 0], dtype=np.float32),
        timelines={timeline.name: timeline},
    )

    assert chunk.branch_values[timeline.name].shape == (4, 10)
    assert chunk.gather_indices[timeline.name].tolist() == [7, 8, 9]
    assert chunk.size == 3


def test_causal_model_output_is_invariant_to_future_mutation() -> None:
    torch.manual_seed(7)
    model = MultiResolutionCausalTCN(
        branch_input_channels={"micro": 4},
        branch_steps={"micro": 8},
        hidden_channels=8,
        fusion_channels=16,
        dropout=0.0,
    ).eval()
    values = torch.randn(1, 4, 12)
    gather = {"micro": torch.tensor([7])}

    baseline = model({"micro": values}, gather)
    changed = values.clone()
    changed[:, :, 8:] = 1_000_000.0

    torch.testing.assert_close(baseline, model({"micro": changed}, gather))
    assert model.branches["micro"].receptive_field == 8


def test_shared_causal_model_supports_three_class_directional_output() -> None:
    model = MultiResolutionCausalTCN(
        branch_input_channels={"micro": 4},
        branch_steps={"micro": 8},
        hidden_channels=8,
        fusion_channels=16,
        dropout=0.0,
        output_classes=3,
    )

    logits = model(
        {"micro": torch.randn(1, 4, 10)},
        {"micro": torch.tensor([7, 8, 9])},
    )

    assert logits.shape == (3, 3)


def test_partition_indices_preserves_every_row_with_nearly_equal_chunks() -> None:
    parts = partition_indices(np.arange(10), chunk_size=4)

    np.testing.assert_array_equal(np.concatenate(parts), np.arange(10))
    assert max(map(len, parts)) - min(map(len, parts)) <= 1


def test_expanding_folds_purge_overlapping_outcome_horizons() -> None:
    decision = pd.date_range("2024-01-01", "2025-06-30 23:55", freq="5min", tz="UTC")
    labels = pd.DataFrame(
        {
            "decision_ts": decision,
            "horizon_end_ts": decision + pd.Timedelta(hours=48, minutes=5),
            "target": np.arange(len(decision)) % 2,
        }
    )

    folds = build_expanding_folds(
        labels,
        research_start=pd.Timestamp("2024-01-01T00:00:00Z"),
        research_end=pd.Timestamp("2025-06-30T23:55:00Z"),
    )

    assert len(folds) == 4
    first = folds[0]
    assert labels.iloc[first.train_indices]["horizon_end_ts"].max() < first.validation_start
    assert labels.iloc[first.validation_indices]["decision_ts"].min() == first.validation_start
    assert labels.iloc[first.validation_indices]["decision_ts"].max() == first.validation_end


def test_timestamp_metrics_use_raw_timestamp_precision_and_recall() -> None:
    target = np.asarray([1, 1, 1, 0, 0], dtype=np.int8)
    scores = np.asarray([0.9, 0.8, 0.1, 0.7, 0.2], dtype=np.float64)

    metrics = timestamp_metrics(target, scores, threshold=0.5)

    assert metrics.true_positive == 2
    assert metrics.false_positive == 1
    assert metrics.false_negative == 1
    assert metrics.precision == pytest.approx(2 / 3)
    assert metrics.recall == pytest.approx(2 / 3)


def test_always_enter_comparison_rejects_the_trivial_operating_policy() -> None:
    target = np.asarray([1, 0, 1, 0], dtype=np.int8)

    trivial = timestamp_metrics(target, np.full(4, 0.6), threshold=0.5)
    selective = timestamp_metrics(
        target,
        np.asarray([0.9, 0.2, 0.8, 0.1]),
        threshold=0.5,
    )

    assert always_enter_comparison(trivial)["passed"] is False
    assert always_enter_comparison(selective)["passed"] is True


def test_overfit_control_selects_a_contiguous_mixed_class_window() -> None:
    target = np.asarray(([1] * 20) + ([0, 1] * 20) + ([0] * 20), dtype=np.int8)
    indices = np.arange(len(target), dtype=np.int64)

    selected = _contiguous_mixed_control_indices(indices, target, rows=20)

    np.testing.assert_array_equal(selected, np.arange(selected[0], selected[0] + 20))
    prevalence = target[selected].mean()
    assert 0.2 <= prevalence <= 0.8


def test_monthly_stability_uses_one_frozen_pooled_threshold() -> None:
    predictions = pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(
                ["2025-01-01", "2025-01-02", "2025-02-01", "2025-02-02"],
                utc=True,
            ),
            "target": [1, 0, 1, 0],
            "score": [0.9, 0.8, 0.4, 0.3],
        }
    )

    rows = monthly_metrics(predictions, threshold=0.75)

    assert [row["threshold"] for row in rows] == [0.75, 0.75]


def test_training_rationale_records_controls_folds_and_artifact_hashes(tmp_path) -> None:
    output_root = tmp_path / "training" / "supervised_tcn_v1"
    output_root.mkdir(parents=True)
    manifest_path = tmp_path / "training" / "supervised_input" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text("{}")
    (output_root / "preflight.json").write_text(
        json.dumps(
            {
                "manifest_path": str(manifest_path),
                "labels": {
                    "first_decision_ts": "2024-01-01T00:00:00+00:00",
                    "last_decision_ts": "2025-01-01T00:00:00+00:00",
                    "last_horizon_end_ts": "2025-01-03T00:00:00+00:00",
                },
                "branches": {
                    "5m_micro": {
                        "rows": 100,
                        "raw_channels": 2,
                        "tensor_channels_with_masks": 4,
                        "steps": 8,
                        "lookback_days": 7.0,
                    }
                },
                "folds": [
                    {
                        "fold_id": "fold_01",
                        "train_rows": 60,
                        "train_end": "2024-06-28T23:55:00+00:00",
                        "validation_rows": 20,
                        "validation_start": "2024-07-01T00:00:00+00:00",
                        "validation_end": "2024-09-30T23:55:00+00:00",
                    }
                ],
            }
        )
    )
    (output_root / "controls.json").write_text(
        json.dumps(
            {
                "status": "passed",
                "overfit": {"passed": True, "metrics": {"pr_auc": 1.0, "f1": 1.0}},
                "shuffled_label": {
                    "passed": True,
                    "metrics": {"pr_auc": 0.49, "prevalence": 0.5},
                },
            }
        )
    )
    for name in (
        "oof_predictions.parquet",
        "oof_ensemble_predictions.parquet",
        "training_report.json",
    ):
        (output_root / name).write_bytes(name.encode("ascii"))

    manifest = {
        "artifact_root": str(tmp_path),
        "manifest_hash": "manifest-hash",
        "target_config_hash": "target-hash",
        "labels": {
            "eligible_rows": 80,
            "positive_rows": 40,
            "negative_rows": 40,
            "positive_prevalence": 0.5,
        },
        "dataset_ids": {"candles": "btc-candles"},
    }
    report = {
        "status": "accepted",
        "model_config_hash": "model-hash",
        "training_config": {
            "hidden_channels": 8,
            "fusion_channels": 16,
            "kernel_size": 2,
            "dropout": 0.1,
            "learning_rate": 0.001,
            "weight_decay": 0.0001,
            "max_epochs": 5,
            "patience": 2,
            "seeds": [17],
        },
        "causal_logistic_baselines": [{"fold_id": "fold_01", "pr_auc": 0.55}],
        "fold_results": [
            {
                "fold_id": "fold_01",
                "seed": 17,
                "best_epoch": 3,
                "pr_auc": 0.6,
                "prevalence": 0.5,
                "precision": 0.58,
                "recall": 0.62,
            }
        ],
        "pooled_oof_metrics": {
            "pr_auc": 0.6,
            "roc_auc": 0.61,
            "precision": 0.58,
            "recall": 0.62,
            "f1": 0.6,
            "brier_score": 0.24,
            "calibration_error": 0.05,
            "threshold": 0.52,
        },
        "acceptance": {
            "accepted": True,
            "rule": "beat baselines",
            "median_sequence_pr_auc": 0.6,
            "median_logistic_pr_auc": 0.55,
            "median_prevalence": 0.5,
            "prevalence_win_fraction": 1.0,
            "causal_logistic_win_fraction": 1.0,
            "always_enter_comparison": {"passed": True},
        },
        "final_refit": {"artifacts": []},
    }

    _write_rationale(output_root=output_root, manifest=manifest, report=report)

    rationale = (tmp_path / "prompt" / "supervised_training_rationale.md").read_text()
    assert "## Data Audit" in rationale
    assert "## Controls" in rationale
    assert "## Fold Results" in rationale
    assert "## Artifact Hashes" in rationale
    assert "SHA-256" in rationale
