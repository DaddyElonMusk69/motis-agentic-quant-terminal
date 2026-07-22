from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.metrics import log_loss

from quant_terminal_worker.signal_discovery.supervised_lightgbm_training import (
    LightGBMConfig,
    _lightgbm_macro_ap,
    _matrix_rows,
    _new_classifier,
    _predict_probabilities,
    _require_lightgbm,
    apply_temperature,
    block_bootstrap_action_precision,
    build_nested_monthly_folds,
    fit_temperature,
    monthly_action_metrics,
    random_entry_precision_baseline,
)
from quant_terminal_worker.signal_discovery.supervised_tabular_features import (
    LagSpec,
    build_tabular_feature_matrix,
    ordered_branch_names,
    tabular_feature_names,
)
from quant_terminal_worker.signal_discovery.supervised_training_data import (
    BranchSpec,
    FeatureTimeline,
)


def _timeline(
    *,
    name: str = "5m_micro",
    values: np.ndarray | None = None,
) -> FeatureTimeline:
    selected = (
        np.asarray(values, dtype=np.float32)
        if values is not None
        else np.arange(40, dtype=np.float32).reshape(20, 2)
    )
    return FeatureTimeline(
        name=name,
        available_ns=np.arange(1, len(selected) + 1, dtype=np.int64) * 10,
        values=selected,
        channel_names=("a", "b"),
        spec=BranchSpec(name=name, rule=None, interval_minutes=5, steps=4),
    )


def test_tabular_features_include_current_causal_deltas_and_age() -> None:
    timeline = _timeline()
    lags = {timeline.name: (LagSpec("two_steps", 2),)}
    decisions = np.asarray([55, 65], dtype=np.int64)

    matrix = build_tabular_feature_matrix(
        decision_ns=decisions,
        timelines={timeline.name: timeline},
        branch_lags=lags,
    )

    assert tabular_feature_names({timeline.name: timeline}, branch_lags=lags) == (
        "5m_micro__a__current",
        "5m_micro__b__current",
        "5m_micro__a__delta_two_steps",
        "5m_micro__b__delta_two_steps",
        "5m_micro__age_minutes",
    )
    np.testing.assert_allclose(matrix[:, :4], [[8, 9, 4, 4], [10, 11, 4, 4]])
    np.testing.assert_allclose(matrix[:, 4], np.asarray([5, 5]) / (60 * 1_000_000_000))


def test_tabular_features_are_invariant_to_future_mutation() -> None:
    values = np.arange(40, dtype=np.float32).reshape(20, 2)
    baseline_timeline = _timeline(values=values)
    changed = values.copy()
    changed[5:] = 1_000_000
    changed_timeline = _timeline(values=changed)
    lags = {"5m_micro": (LagSpec("two_steps", 2),)}
    decision = np.asarray([50], dtype=np.int64)

    baseline = build_tabular_feature_matrix(
        decision_ns=decision,
        timelines={"5m_micro": baseline_timeline},
        branch_lags=lags,
    )
    mutated = build_tabular_feature_matrix(
        decision_ns=decision,
        timelines={"5m_micro": changed_timeline},
        branch_lags=lags,
    )

    np.testing.assert_array_equal(baseline, mutated)


def test_tabular_branch_order_is_stable_and_not_manifest_order() -> None:
    timelines = {
        "funding_events": _timeline(name="funding_events"),
        "5m_micro": _timeline(name="5m_micro"),
        "1h_medium": _timeline(name="1h_medium"),
    }

    assert ordered_branch_names(timelines) == ("5m_micro", "1h_medium", "funding_events")


def test_nested_monthly_folds_use_mature_history_and_purge_outcomes() -> None:
    decisions = pd.date_range("2024-03-01", "2026-05-31", freq="1D", tz="UTC")
    labels = pd.DataFrame(
        {
            "decision_ts": decisions,
            "horizon_end_ts": decisions + pd.Timedelta(hours=48, minutes=5),
        }
    )

    folds = build_nested_monthly_folds(
        labels,
        research_start=pd.Timestamp("2024-03-01T00:00:00Z"),
        research_end=pd.Timestamp("2026-05-31T23:55:00Z"),
    )

    assert len(folds) == 7
    assert folds[0].outer_validation_start == pd.Timestamp("2024-09-01T00:00:00Z")
    assert folds[-1].outer_validation_start == pd.Timestamp("2026-03-01T00:00:00Z")
    assert folds[-1].outer_validation_end == pd.Timestamp("2026-05-31T00:00:00Z")
    for fold in folds:
        assert (
            labels.iloc[fold.inner_train_indices]["horizon_end_ts"].max()
            < fold.inner_validation_start
        )
        assert (
            labels.iloc[fold.outer_train_indices]["horizon_end_ts"].max()
            < fold.outer_validation_start
        )


def test_lightgbm_config_hash_covers_split_and_model_behavior() -> None:
    baseline = LightGBMConfig()

    assert baseline.config_hash != LightGBMConfig(num_leaves=63).config_hash
    assert baseline.config_hash != LightGBMConfig(outer_validation_months=2).config_hash
    assert baseline.config_hash != LightGBMConfig(minimum_action_recall=0.20).config_hash
    assert baseline.config_hash != LightGBMConfig(seeds=(17,)).config_hash


def test_temperature_calibration_is_fit_on_multiclass_log_loss() -> None:
    target = np.asarray([0, 1, 2] * 40, dtype=np.int64)
    probabilities = np.full((len(target), 3), 0.025, dtype=np.float64)
    probabilities[np.arange(len(target)), target] = 0.95
    probabilities[::5] = np.roll(probabilities[::5], shift=1, axis=1)

    temperature = fit_temperature(target, probabilities)
    calibrated = apply_temperature(probabilities, temperature)

    assert temperature > 1.0
    assert log_loss(target, calibrated, labels=[0, 1, 2]) < log_loss(
        target, probabilities, labels=[0, 1, 2]
    )
    np.testing.assert_allclose(calibrated.sum(axis=1), 1.0)


def test_monthly_precision_lift_uses_side_mix_and_exact_direction() -> None:
    predictions = pd.DataFrame(
        {
            "decision_ts": pd.to_datetime(
                ["2025-01-01", "2025-01-02", "2025-02-01", "2025-02-02"],
                utc=True,
            ),
            "class_target": [0, 2, 1, 2],
            "class_prediction": [0, 2, 0, 2],
        }
    )

    rows = monthly_action_metrics(predictions)

    assert random_entry_precision_baseline(
        np.asarray([0, 2]), np.asarray([0, 2])
    ) == 0.5
    assert rows[0]["action_precision"] == 1.0
    assert rows[0]["positive_precision_lift"] is True
    assert rows[1]["action_precision"] == 0.0
    assert rows[1]["positive_precision_lift"] is False


def test_block_bootstrap_precision_is_deterministic_and_bounded() -> None:
    predictions = pd.DataFrame(
        {
            "decision_ts": pd.date_range("2025-01-01", periods=120, freq="12h", tz="UTC"),
            "class_target": np.asarray([0, 1, 2] * 40),
            "class_prediction": np.asarray([0, 1, 2] * 40),
        }
    )

    first = block_bootstrap_action_precision(predictions, iterations=100, seed=17)
    second = block_bootstrap_action_precision(predictions, iterations=100, seed=17)

    assert first == second
    assert first == {"lower": 1.0, "median": 1.0, "upper": 1.0}


def test_contiguous_fold_rows_remain_memory_mapped_views() -> None:
    matrix = np.arange(60, dtype=np.float32).reshape(20, 3)

    contiguous = _matrix_rows(matrix, np.arange(5, 10, dtype=np.int64))
    scattered = _matrix_rows(matrix, np.asarray([5, 7, 9], dtype=np.int64))

    assert np.shares_memory(matrix, contiguous)
    assert not np.shares_memory(matrix, scattered)


def test_lightgbm_macro_ap_metric_uses_all_three_classes() -> None:
    target = np.asarray([0, 1, 2, 0, 1, 2], dtype=np.int64)
    probabilities = np.eye(3, dtype=np.float64)[target] * 0.9 + 0.1 / 3

    name, value, higher_is_better = _lightgbm_macro_ap(target, probabilities)

    assert name == "macro_ap"
    assert value == 1.0
    assert higher_is_better is True


def test_lightgbm_custom_macro_ap_drives_early_stopping_callback() -> None:
    rng = np.random.default_rng(17)
    matrix = rng.normal(size=(300, 8)).astype(np.float32)
    target = np.arange(len(matrix), dtype=np.int64) % 3
    matrix[:, 0] += target * 0.5
    model = _new_classifier(
        config=LightGBMConfig(min_child_samples=10),
        seed=17,
        n_estimators=20,
    )
    lightgbm = _require_lightgbm()

    model.fit(
        matrix[:240],
        target[:240],
        eval_set=[(matrix[240:], target[240:])],
        eval_metric=_lightgbm_macro_ap,
        callbacks=[
            lightgbm.early_stopping(5, first_metric_only=True, verbose=False),
            lightgbm.log_evaluation(period=0),
        ],
    )

    assert 1 <= model.best_iteration_ <= 20
    assert _predict_probabilities(model, matrix[240:]).shape == (60, 3)
