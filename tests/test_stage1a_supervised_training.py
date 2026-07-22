from __future__ import annotations

import importlib.util
from pathlib import Path

import numpy as np
import pytest

from quant_terminal_strategies.stage1a_supervised_runtime import score_two_head_artifact


SCRIPT_PATH = Path("dev/experiments/stage1/train_stage1a_supervised.py")
SPEC = importlib.util.spec_from_file_location("train_stage1a_supervised", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
TRAINER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(TRAINER)


def test_exported_logistic_head_matches_sklearn_probability() -> None:
    matrix = np.asarray([[-2.0], [-1.0], [1.0], [2.0]], dtype=float)
    labels = np.asarray([0, 0, 1, 1], dtype=int)

    head, fitted = TRAINER.fit_logistic_head(matrix, labels)
    artifact = _artifact(head)

    for value in (-1.5, 0.0, 1.5):
        standardized = (np.asarray([[value]]) - fitted["means"]) / fitted["scales"]
        expected = fitted["model"].predict_proba(standardized)[0, 1]
        actual = score_two_head_artifact({"feature": value}, artifact)["p_enter"]
        assert actual == pytest.approx(expected, abs=1e-12)


def test_training_matrix_uses_training_median_for_missing_values() -> None:
    matrix = np.asarray([[1.0, np.nan], [3.0, 5.0], [7.0, 9.0]], dtype=float)

    prepared = TRAINER.prepare_training_matrix(matrix)

    assert prepared["imputation_values"].tolist() == [3.0, 7.0]
    assert prepared["imputed_matrix"][0].tolist() == [1.0, 7.0]


def _artifact(head: dict[str, object]) -> dict[str, object]:
    return {
        "schema_version": "stage1a_supervised_model.v1",
        "model_id": "parity-test",
        "model_version": "parity-v1",
        "feature_spec_version": "vegas_5m_cluster_v6.stage1a_features.v1",
        "model_family": "two_head_logistic",
        "active_feature_names": ["feature"],
        "observed_feature_names": ["feature"],
        "heads": {"enter": head, "direction": head},
        "thresholds": {"enter_threshold": 0.3, "max_missing_fraction": 0.2},
    }
