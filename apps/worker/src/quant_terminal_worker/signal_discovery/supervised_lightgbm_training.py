from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, log_loss

from quant_terminal_worker.signal_discovery.supervised_directional_training import (
    CLASS_NAMES,
    DirectionalPolicy,
    action_metrics,
    apply_policy,
    attach_directional_target,
    contiguous_three_class_window,
    probabilities_frame,
    probability_matrix,
    policy_meets_requirements,
    ranking_metrics,
    select_operating_policy,
    trivial_baselines,
)
from quant_terminal_worker.signal_discovery.supervised_sequence_training import (
    _atomic_write_json,
    _atomic_write_parquet,
    _duration,
    _sha256,
)
from quant_terminal_worker.signal_discovery.supervised_tabular_features import (
    load_lightgbm_training_input,
)


RUN_SCHEMA_VERSION = "motis_supervised_directional_lightgbm_run.v2"
CONTROL_SCHEMA_VERSION = "motis_supervised_directional_lightgbm_controls.v2"
MODEL_SCHEMA_VERSION = "motis_supervised_directional_lightgbm_model.v2"


@dataclass(frozen=True)
class LightGBMConfig:
    learning_rate: float = 0.03
    num_leaves: int = 31
    min_child_samples: int = 1000
    feature_fraction: float = 0.80
    bagging_fraction: float = 0.80
    bagging_freq: int = 1
    reg_alpha: float = 0.10
    reg_lambda: float = 1.0
    max_estimators: int = 1500
    early_stopping_rounds: int = 75
    minimum_train_months: int = 6
    inner_validation_months: int = 1
    outer_validation_months: int = 3
    outer_step_months: int = 3
    seeds: tuple[int, ...] = (17, 29, 43)
    minimum_action_precision: float = 0.55
    minimum_action_recall: float = 0.15
    minimum_entry_coverage: float = 0.0
    minimum_side_coverage: float = 0.0
    minimum_long_precision: float = 0.50
    minimum_short_precision: float = 0.50
    minimum_long_recall: float = 0.05
    minimum_short_recall: float = 0.05
    maximum_neutral_false_positive_rate: float = 0.10
    minimum_macro_pr_auc: float = 0.40
    minimum_macro_pr_auc_lift: float = 0.02
    minimum_fold_pass_fraction: float = 0.70
    minimum_worst_fold_precision: float = 0.45
    minimum_monthly_lift_fraction: float = 0.70
    minimum_bootstrap_precision_lower: float = 0.50

    @property
    def config_hash(self) -> str:
        return _hash_mapping({**asdict(self), "seeds": list(self.seeds)})


@dataclass(frozen=True)
class NestedMonthlyFold:
    fold_id: str
    inner_train_indices: np.ndarray
    inner_validation_indices: np.ndarray
    outer_train_indices: np.ndarray
    outer_validation_indices: np.ndarray
    inner_validation_start: pd.Timestamp
    inner_validation_end: pd.Timestamp
    outer_validation_start: pd.Timestamp
    outer_validation_end: pd.Timestamp
    outer_train_end: pd.Timestamp


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def apply_temperature(probabilities: np.ndarray, temperature: float) -> np.ndarray:
    scores = np.asarray(probabilities, dtype=np.float64)
    if scores.ndim != 2 or scores.shape[1] != len(CLASS_NAMES):
        raise ValueError("temperature calibration requires three-class probabilities")
    if temperature <= 0:
        raise ValueError("temperature must be positive")
    logits = np.log(np.clip(scores, 1e-12, 1.0)) / float(temperature)
    logits -= logits.max(axis=1, keepdims=True)
    calibrated = np.exp(logits)
    calibrated /= calibrated.sum(axis=1, keepdims=True)
    return calibrated.astype(np.float32)


def fit_temperature(target: np.ndarray, probabilities: np.ndarray) -> float:
    truth = np.asarray(target, dtype=np.int64)
    candidates = np.geomspace(0.50, 3.0, 51)
    return float(
        min(
            candidates,
            key=lambda value: log_loss(
                truth,
                apply_temperature(probabilities, float(value)),
                labels=[0, 1, 2],
            ),
        )
    )


def random_entry_precision_baseline(target: np.ndarray, prediction: np.ndarray) -> float:
    truth = np.asarray(target, dtype=np.int64)
    predicted = np.asarray(prediction, dtype=np.int64)
    entry = predicted != 2
    if not entry.any():
        return 0.0
    long_share = float((predicted[entry] == 0).mean())
    short_share = 1.0 - long_share
    return float(long_share * (truth == 0).mean() + short_share * (truth == 1).mean())


def monthly_action_metrics(predictions: pd.DataFrame) -> list[dict[str, Any]]:
    frame = predictions.copy()
    frame["month"] = pd.to_datetime(frame["decision_ts"], utc=True).dt.strftime("%Y-%m")
    rows: list[dict[str, Any]] = []
    for month, group in frame.groupby("month", sort=True):
        target = group["class_target"].to_numpy(dtype=np.int64)
        prediction = group["class_prediction"].to_numpy(dtype=np.int64)
        metrics = action_metrics(target, prediction)
        baseline = random_entry_precision_baseline(target, prediction)
        rows.append(
            {
                "month": str(month),
                **asdict(metrics),
                "random_entry_precision_baseline": baseline,
                "positive_precision_lift": bool(
                    metrics.entries > 0 and metrics.action_precision > baseline + 1e-6
                ),
            }
        )
    return rows


def block_bootstrap_action_precision(
    predictions: pd.DataFrame,
    *,
    iterations: int = 500,
    block_days: int = 7,
    seed: int = 20260716,
) -> dict[str, float]:
    frame = predictions.sort_values("decision_ts").reset_index(drop=True)
    timestamps = pd.to_datetime(frame["decision_ts"], utc=True)
    origin = timestamps.min().floor("D")
    block_ids = ((timestamps - origin) // pd.Timedelta(days=block_days)).to_numpy()
    groups = [np.flatnonzero(block_ids == value) for value in np.unique(block_ids)]
    target = frame["class_target"].to_numpy(dtype=np.int64)
    prediction = frame["class_prediction"].to_numpy(dtype=np.int64)
    rng = np.random.default_rng(seed)
    values: list[float] = []
    for _ in range(iterations):
        selected = rng.integers(0, len(groups), size=len(groups))
        sample = np.concatenate([groups[index] for index in selected])
        values.append(action_metrics(target[sample], prediction[sample]).action_precision)
    return {
        "lower": float(np.quantile(values, 0.025)),
        "median": float(np.quantile(values, 0.50)),
        "upper": float(np.quantile(values, 0.975)),
    }


def median_policy(policies: Sequence[DirectionalPolicy]) -> DirectionalPolicy:
    if not policies:
        raise ValueError("cannot select a final policy without passing inner policies")
    return DirectionalPolicy(
        probability_threshold=float(
            np.median([policy.probability_threshold for policy in policies])
        ),
        margin_threshold=float(np.median([policy.margin_threshold for policy in policies])),
    )


def evaluated_predictions_frame(
    labels: pd.DataFrame,
    probabilities: np.ndarray,
    *,
    fold_id: str,
    temperature: float,
    policy: DirectionalPolicy | None,
) -> pd.DataFrame:
    frame = probabilities_frame(labels, probabilities, fold_id=fold_id, seed=-1)
    frame["class_prediction"] = (
        apply_policy(probabilities, policy)
        if policy is not None
        else np.full(len(frame), 2, dtype=np.int64)
    )
    frame["temperature"] = float(temperature)
    frame["probability_threshold"] = (
        float(policy.probability_threshold) if policy is not None else np.nan
    )
    frame["margin_threshold"] = (
        float(policy.margin_threshold) if policy is not None else np.nan
    )
    return frame


def _acceptance_thresholds(config: LightGBMConfig) -> dict[str, Any]:
    return {
        "minimum_action_precision": config.minimum_action_precision,
        "minimum_action_recall": config.minimum_action_recall,
        "minimum_long_precision": config.minimum_long_precision,
        "minimum_short_precision": config.minimum_short_precision,
        "minimum_long_recall": config.minimum_long_recall,
        "minimum_short_recall": config.minimum_short_recall,
        "maximum_neutral_false_positive_rate": (
            config.maximum_neutral_false_positive_rate
        ),
        "minimum_macro_pr_auc": config.minimum_macro_pr_auc,
        "requires_each_class_ap_above_prevalence": True,
        "minimum_macro_pr_auc_lift_over_shallow_tree": (
            config.minimum_macro_pr_auc_lift
        ),
        "minimum_fold_pass_fraction": config.minimum_fold_pass_fraction,
        "minimum_worst_fold_precision": config.minimum_worst_fold_precision,
        "minimum_monthly_positive_lift_fraction": config.minimum_monthly_lift_fraction,
        "minimum_bootstrap_precision_lower": config.minimum_bootstrap_precision_lower,
        "bootstrap_confidence": 0.95,
        "bootstrap_block_days": 7,
    }


def build_nested_monthly_folds(
    labels: pd.DataFrame,
    *,
    research_start: pd.Timestamp,
    research_end: pd.Timestamp,
    minimum_train_months: int = 6,
    inner_validation_months: int = 1,
    outer_validation_months: int = 3,
    step_months: int = 3,
) -> list[NestedMonthlyFold]:
    if min(
        minimum_train_months,
        inner_validation_months,
        outer_validation_months,
        step_months,
    ) <= 0:
        raise ValueError("nested fold month spans must be positive")
    decisions = pd.to_datetime(labels["decision_ts"], utc=True)
    horizon_end = pd.to_datetime(labels["horizon_end_ts"], utc=True)
    outer_start = pd.Timestamp(research_start) + pd.DateOffset(months=minimum_train_months)
    folds: list[NestedMonthlyFold] = []
    while True:
        outer_stop = outer_start + pd.DateOffset(months=outer_validation_months)
        outer_end = outer_stop - pd.Timedelta(minutes=5)
        if outer_end > pd.Timestamp(research_end):
            break
        inner_start = outer_start - pd.DateOffset(months=inner_validation_months)
        inner_train_mask = (decisions < inner_start) & (horizon_end < inner_start)
        inner_validation_mask = (decisions >= inner_start) & (decisions < outer_start)
        outer_train_mask = (decisions < outer_start) & (horizon_end < outer_start)
        outer_validation_mask = (decisions >= outer_start) & (decisions < outer_stop)
        inner_train = np.flatnonzero(inner_train_mask.to_numpy())
        inner_validation = np.flatnonzero(inner_validation_mask.to_numpy())
        outer_train = np.flatnonzero(outer_train_mask.to_numpy())
        outer_validation = np.flatnonzero(outer_validation_mask.to_numpy())
        if not all(map(len, (inner_train, inner_validation, outer_train, outer_validation))):
            raise ValueError(f"nested monthly fold at {outer_start} is empty")
        folds.append(
            NestedMonthlyFold(
                fold_id=f"fold_{len(folds) + 1:02d}",
                inner_train_indices=inner_train,
                inner_validation_indices=inner_validation,
                outer_train_indices=outer_train,
                outer_validation_indices=outer_validation,
                inner_validation_start=pd.Timestamp(inner_start),
                inner_validation_end=pd.Timestamp(decisions.iloc[inner_validation[-1]]),
                outer_validation_start=pd.Timestamp(outer_start),
                outer_validation_end=pd.Timestamp(decisions.iloc[outer_validation[-1]]),
                outer_train_end=pd.Timestamp(decisions.iloc[outer_train[-1]]),
            )
        )
        outer_start += pd.DateOffset(months=step_months)
    if len(folds) < 3:
        raise ValueError(f"nested chronological training requires at least three folds, found {len(folds)}")
    return folds


def run_preflight(
    *,
    manifest_path: Path,
    output_root: Path,
    config: LightGBMConfig,
) -> dict[str, Any]:
    labels, matrix, manifest = load_lightgbm_training_input(manifest_path)
    labels = attach_directional_target(labels)
    folds = build_nested_monthly_folds(
        labels,
        research_start=pd.Timestamp(manifest["splits"]["research_start"]),
        research_end=pd.Timestamp(manifest["splits"]["research_end"]),
        minimum_train_months=config.minimum_train_months,
        inner_validation_months=config.inner_validation_months,
        outer_validation_months=config.outer_validation_months,
        step_months=config.outer_step_months,
    )
    failures: list[str] = []
    if len(labels) != matrix.shape[0]:
        failures.append("feature and label row counts differ")
    if len(manifest["feature_schema"]["feature_names"]) != matrix.shape[1]:
        failures.append("feature names and matrix columns differ")
    if _matrix_contains_infinity(matrix):
        failures.append("tabular feature matrix contains infinity")
    counts = labels["raw_label"].value_counts().to_dict()
    for name in CLASS_NAMES:
        if int(counts.get(name, 0)) == 0:
            failures.append(f"LightGBM training requires {name} labels")
    for fold in folds:
        inner_train = labels.iloc[fold.inner_train_indices]
        outer_train = labels.iloc[fold.outer_train_indices]
        if inner_train["horizon_end_ts"].max() >= fold.inner_validation_start:
            failures.append(f"{fold.fold_id} inner outcome horizon crosses validation")
        if outer_train["horizon_end_ts"].max() >= fold.outer_validation_start:
            failures.append(f"{fold.fold_id} outer outcome horizon crosses validation")
    report = {
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "passed" if not failures else "failed",
        "created_at": utc_now(),
        "objective": "selective_exact_long_short_neutral_v2",
        "manifest_hash": manifest["manifest_hash"],
        "source_manifest_hash": manifest["source_manifest_hash"],
        "feature_schema_hash": manifest["feature_schema"]["feature_schema_hash"],
        "config_hash": config.config_hash,
        "config": _config_mapping(config),
        "rows": int(len(labels)),
        "features": int(matrix.shape[1]),
        "class_counts": {name: int(counts.get(name, 0)) for name in CLASS_NAMES},
        "folds": [fold_summary(fold) for fold in folds],
        "failures": failures,
        "walk_forward_inspected": False,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(output_root / "preflight.json", report)
    if failures:
        raise ValueError("LightGBM preflight failed: " + "; ".join(failures))
    print(
        f"[lightgbm-preflight] status=passed rows={len(labels)} features={matrix.shape[1]} "
        f"folds={len(folds)}",
        flush=True,
    )
    return report


def run_controls(
    *,
    manifest_path: Path,
    output_root: Path,
    config: LightGBMConfig,
) -> dict[str, Any]:
    preflight = run_preflight(
        manifest_path=manifest_path,
        output_root=output_root,
        config=config,
    )
    labels, matrix, manifest = load_lightgbm_training_input(manifest_path)
    labels = attach_directional_target(labels)
    folds = build_nested_monthly_folds(
        labels,
        research_start=pd.Timestamp(manifest["splits"]["research_start"]),
        research_end=pd.Timestamp(manifest["splits"]["research_end"]),
        minimum_train_months=config.minimum_train_months,
        inner_validation_months=config.inner_validation_months,
        outer_validation_months=config.outer_validation_months,
        step_months=config.outer_step_months,
    )
    first = folds[0]
    target = labels["class_target"].to_numpy(dtype=np.int64)
    overfit_indices = contiguous_three_class_window(first.inner_train_indices, target, rows=384)
    overfit_model = _new_classifier(
        config=config,
        seed=config.seeds[0],
        n_estimators=600,
        overrides={
            "learning_rate": 0.10,
            "num_leaves": 127,
            "min_child_samples": 1,
            "feature_fraction": 1.0,
            "bagging_fraction": 1.0,
            "bagging_freq": 0,
            "reg_alpha": 0.0,
            "reg_lambda": 0.0,
        },
    )
    overfit_model.fit(_matrix_rows(matrix, overfit_indices), target[overfit_indices])
    overfit_probabilities = _predict_probabilities(
        overfit_model, _matrix_rows(matrix, overfit_indices)
    )
    overfit_ranking = ranking_metrics(target[overfit_indices], overfit_probabilities)
    overfit_prediction = np.asarray(overfit_probabilities).argmax(axis=1)
    overfit_action = action_metrics(target[overfit_indices], overfit_prediction)
    overfit_passed = bool(
        overfit_ranking.macro_pr_auc >= 0.99
        and overfit_ranking.argmax_accuracy >= 0.98
        and len(set(overfit_prediction.tolist())) == len(CLASS_NAMES)
    )

    shuffle_train = first.inner_train_indices[-min(4096, len(first.inner_train_indices)) :]
    shuffle_validation = first.inner_validation_indices[:2048]
    shuffled_target = target[shuffle_train].copy()
    np.random.default_rng(config.seeds[0] + 10_000).shuffle(shuffled_target)
    shuffle_model = _new_classifier(
        config=config,
        seed=config.seeds[0] + 10_000,
        n_estimators=100,
        overrides={"min_child_samples": 20, "feature_fraction": 1.0},
    )
    shuffle_model.fit(_matrix_rows(matrix, shuffle_train), shuffled_target)
    shuffle_probabilities = _predict_probabilities(
        shuffle_model, _matrix_rows(matrix, shuffle_validation)
    )
    shuffle_ranking = ranking_metrics(target[shuffle_validation], shuffle_probabilities)
    shuffle_passed = bool(shuffle_ranking.macro_pr_auc <= (1.0 / 3.0) + 0.06)
    passed = bool(preflight["status"] == "passed" and overfit_passed and shuffle_passed)
    report = {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "created_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "feature_schema_hash": manifest["feature_schema"]["feature_schema_hash"],
        "config_hash": config.config_hash,
        "overfit": {
            "passed": overfit_passed,
            "rows": int(len(overfit_indices)),
            "class_counts": {
                name: int((target[overfit_indices] == index).sum())
                for index, name in enumerate(CLASS_NAMES)
            },
            "ranking": asdict(overfit_ranking),
            "action": asdict(overfit_action),
        },
        "shuffled_label": {
            "passed": shuffle_passed,
            "train_rows": int(len(shuffle_train)),
            "validation_rows": int(len(shuffle_validation)),
            "ranking": asdict(shuffle_ranking),
            "maximum_macro_pr_auc": (1.0 / 3.0) + 0.06,
        },
        "walk_forward_inspected": False,
    }
    _atomic_write_json(output_root / "controls.json", report)
    print(f"[lightgbm-controls] status={report['status']}", flush=True)
    if not passed:
        raise ValueError("LightGBM controls failed; full training is blocked")
    return report


def run_full_training(
    *,
    manifest_path: Path,
    output_root: Path,
    config: LightGBMConfig,
) -> dict[str, Any]:
    labels, matrix, manifest = load_lightgbm_training_input(manifest_path)
    labels = attach_directional_target(labels)
    require_controls(output_root, manifest=manifest, config=config)
    folds = build_nested_monthly_folds(
        labels,
        research_start=pd.Timestamp(manifest["splits"]["research_start"]),
        research_end=pd.Timestamp(manifest["splits"]["research_end"]),
        minimum_train_months=config.minimum_train_months,
        inner_validation_months=config.inner_validation_months,
        outer_validation_months=config.outer_validation_months,
        step_months=config.outer_step_months,
    )
    status = {
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "running",
        "started_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "config_hash": config.config_hash,
        "fold_count": len(folds),
        "seeds": list(config.seeds),
    }
    _atomic_write_json(output_root / "status.json", status)
    feature_names = tuple(manifest["feature_schema"]["feature_names"])
    target = labels["class_target"].to_numpy(dtype=np.int64)
    seed_results: list[dict[str, Any]] = []
    fold_evaluations: list[dict[str, Any]] = []
    raw_prediction_frames: list[pd.DataFrame] = []
    inner_evaluation_frames: list[pd.DataFrame] = []
    outer_evaluation_frames: list[pd.DataFrame] = []
    baseline_evaluation_frames: list[pd.DataFrame] = []
    selected_policies: list[DirectionalPolicy] = []
    selected_temperatures: list[float] = []
    total_runs = len(folds) * len(config.seeds)
    completed_runs = 0
    started = time.monotonic()
    try:
        for fold_number, fold in enumerate(folds, start=1):
            inner_seed_probabilities: list[np.ndarray] = []
            outer_seed_probabilities: list[np.ndarray] = []
            inner_validation = labels.iloc[fold.inner_validation_indices]
            outer_validation = labels.iloc[fold.outer_validation_indices]
            for seed in config.seeds:
                run_root = output_root / fold.fold_id / f"seed_{seed}"
                result, inner_probabilities, outer_probabilities = fit_outer_run(
                    matrix=matrix,
                    labels=labels,
                    target=target,
                    feature_names=feature_names,
                    fold=fold,
                    seed=seed,
                    config=config,
                    run_root=run_root,
                    fold_number=fold_number,
                    fold_count=len(folds),
                )
                seed_results.append({"fold_id": fold.fold_id, "seed": seed, **result})
                inner_seed_probabilities.append(inner_probabilities)
                outer_seed_probabilities.append(outer_probabilities)
                raw_prediction_frames.append(
                    probabilities_frame(
                        outer_validation,
                        outer_probabilities,
                        fold_id=fold.fold_id,
                        seed=seed,
                    )
                )
                completed_runs += 1
                elapsed = time.monotonic() - started
                eta = elapsed / completed_runs * (total_runs - completed_runs)
                status.update(
                    {
                        "completed_runs": completed_runs,
                        "total_runs": total_runs,
                        "updated_at": utc_now(),
                        "eta_seconds": round(eta, 1),
                    }
                )
                _atomic_write_json(output_root / "status.json", status)
                print(
                    f"[lightgbm-progress] {completed_runs}/{total_runs} "
                    f"elapsed={_duration(elapsed)} eta={_duration(eta)}",
                    flush=True,
                )

            inner_ensemble = np.mean(inner_seed_probabilities, axis=0)
            outer_ensemble = np.mean(outer_seed_probabilities, axis=0)
            temperature = fit_temperature(
                target[fold.inner_validation_indices], inner_ensemble
            )
            calibrated_inner = apply_temperature(inner_ensemble, temperature)
            calibrated_outer = apply_temperature(outer_ensemble, temperature)
            policy, frontier = select_operating_policy(
                target[fold.inner_validation_indices],
                calibrated_inner,
                config=config,
            )
            inner_frame = evaluated_predictions_frame(
                inner_validation,
                calibrated_inner,
                fold_id=fold.fold_id,
                temperature=temperature,
                policy=policy,
            )
            outer_frame = evaluated_predictions_frame(
                outer_validation,
                calibrated_outer,
                fold_id=fold.fold_id,
                temperature=temperature,
                policy=policy,
            )
            inner_evaluation_frames.append(inner_frame)
            outer_evaluation_frames.append(outer_frame)
            inner_action = action_metrics(
                target[fold.inner_validation_indices],
                inner_frame["class_prediction"].to_numpy(dtype=np.int64),
            )
            outer_action = action_metrics(
                target[fold.outer_validation_indices],
                outer_frame["class_prediction"].to_numpy(dtype=np.int64),
            )
            outer_ranking = ranking_metrics(
                target[fold.outer_validation_indices], calibrated_outer
            )
            fold_passed = bool(
                policy is not None and policy_meets_requirements(outer_action, config)
            )
            fold_root = output_root / fold.fold_id
            _atomic_write_parquet(
                fold_root / "inner_ensemble_predictions.parquet", inner_frame
            )
            _atomic_write_parquet(
                fold_root / "outer_evaluation_predictions.parquet", outer_frame
            )
            _atomic_write_json(fold_root / "policy_frontier.json", frontier)

            baseline_inner, baseline_outer = fit_shallow_baseline(
                matrix=matrix,
                target=target,
                fold=fold,
                config=config,
                seed=config.seeds[0],
            )
            baseline_temperature = fit_temperature(
                target[fold.inner_validation_indices], baseline_inner
            )
            calibrated_baseline_inner = apply_temperature(
                baseline_inner, baseline_temperature
            )
            calibrated_baseline_outer = apply_temperature(
                baseline_outer, baseline_temperature
            )
            baseline_policy, baseline_frontier = select_operating_policy(
                target[fold.inner_validation_indices],
                calibrated_baseline_inner,
                config=config,
            )
            baseline_frame = evaluated_predictions_frame(
                outer_validation,
                calibrated_baseline_outer,
                fold_id=fold.fold_id,
                temperature=baseline_temperature,
                policy=baseline_policy,
            )
            baseline_evaluation_frames.append(baseline_frame)
            baseline_action = action_metrics(
                target[fold.outer_validation_indices],
                baseline_frame["class_prediction"].to_numpy(dtype=np.int64),
            )
            baseline_ranking = ranking_metrics(
                target[fold.outer_validation_indices], calibrated_baseline_outer
            )
            _atomic_write_parquet(
                fold_root / "shallow_tree_outer_predictions.parquet", baseline_frame
            )
            _atomic_write_json(
                fold_root / "shallow_tree_policy_frontier.json", baseline_frontier
            )
            fold_evaluations.append(
                {
                    "fold_id": fold.fold_id,
                    "temperature": temperature,
                    "selected_policy": asdict(policy) if policy is not None else None,
                    "inner_action_metrics": asdict(inner_action),
                    "outer_ranking": asdict(outer_ranking),
                    "outer_action_metrics": asdict(outer_action),
                    "passed": fold_passed,
                    "policy_frontier_path": str(fold_root / "policy_frontier.json"),
                    "shallow_tree": {
                        "temperature": baseline_temperature,
                        "selected_policy": (
                            asdict(baseline_policy)
                            if baseline_policy is not None
                            else None
                        ),
                        "outer_ranking": asdict(baseline_ranking),
                        "outer_action_metrics": asdict(baseline_action),
                        "policy_frontier_path": str(
                            fold_root / "shallow_tree_policy_frontier.json"
                        ),
                    },
                }
            )
            if policy is not None:
                selected_policies.append(policy)
                selected_temperatures.append(temperature)
            print(
                f"[lightgbm-fold-policy] fold={fold.fold_id} "
                f"policy={'selected' if policy is not None else 'none'} "
                f"outer_precision={outer_action.action_precision:.4f} "
                f"outer_recall={outer_action.action_recall:.4f} passed={fold_passed}",
                flush=True,
            )

        raw_predictions = pd.concat(raw_prediction_frames, ignore_index=True)
        _atomic_write_parquet(output_root / "oof_predictions.parquet", raw_predictions)
        inner_evaluations = pd.concat(inner_evaluation_frames, ignore_index=True)
        _atomic_write_parquet(
            output_root / "inner_ensemble_predictions.parquet", inner_evaluations
        )
        ensemble = pd.concat(outer_evaluation_frames, ignore_index=True)
        _atomic_write_parquet(
            output_root / "oof_ensemble_predictions.parquet", ensemble
        )
        baseline = pd.concat(baseline_evaluation_frames, ignore_index=True)
        _atomic_write_parquet(
            output_root / "oof_shallow_tree_predictions.parquet", baseline
        )
        ensemble_target = ensemble["class_target"].to_numpy(dtype=np.int64)
        pooled_ranking = ranking_metrics(ensemble_target, probability_matrix(ensemble))
        selected_action = action_metrics(
            ensemble_target,
            ensemble["class_prediction"].to_numpy(dtype=np.int64),
        )
        baseline_target = baseline["class_target"].to_numpy(dtype=np.int64)
        baseline_ranking = ranking_metrics(
            baseline_target, probability_matrix(baseline)
        )
        baseline_action = action_metrics(
            baseline_target,
            baseline["class_prediction"].to_numpy(dtype=np.int64),
        )
        class_prevalence = {
            name: float((ensemble_target == index).mean())
            for index, name in enumerate(CLASS_NAMES)
        }
        class_ap_above_prevalence = {
            name: bool(pooled_ranking.per_class_pr_auc[name] > class_prevalence[name])
            for name in CLASS_NAMES
        }
        macro_pr_auc_lift = float(
            pooled_ranking.macro_pr_auc - baseline_ranking.macro_pr_auc
        )
        fold_pass_fraction = float(
            np.mean([row["passed"] for row in fold_evaluations])
        )
        worst_fold_precision = float(
            min(
                row["outer_action_metrics"]["action_precision"]
                for row in fold_evaluations
            )
        )
        monthly_metrics = monthly_action_metrics(ensemble)
        monthly_lift_fraction = float(
            np.mean([row["positive_precision_lift"] for row in monthly_metrics])
        )
        bootstrap_precision = block_bootstrap_action_precision(ensemble)
        acceptance_checks = {
            "overall_action_policy": policy_meets_requirements(selected_action, config),
            "macro_pr_auc": pooled_ranking.macro_pr_auc >= config.minimum_macro_pr_auc,
            "each_class_ap_above_prevalence": all(
                class_ap_above_prevalence.values()
            ),
            "macro_pr_auc_lift_over_shallow_tree": (
                macro_pr_auc_lift >= config.minimum_macro_pr_auc_lift
            ),
            "outer_fold_pass_fraction": (
                fold_pass_fraction >= config.minimum_fold_pass_fraction
            ),
            "worst_outer_fold_precision": (
                worst_fold_precision >= config.minimum_worst_fold_precision
            ),
            "monthly_positive_random_baseline_lift": (
                monthly_lift_fraction >= config.minimum_monthly_lift_fraction
            ),
            "bootstrap_precision_lower_bound": (
                bootstrap_precision["lower"]
                >= config.minimum_bootstrap_precision_lower
            ),
        }
        final_policy = median_policy(selected_policies) if selected_policies else None
        final_temperature = (
            float(np.median(selected_temperatures))
            if selected_temperatures
            else None
        )
        accepted = bool(
            final_policy is not None
            and final_temperature is not None
            and all(acceptance_checks.values())
        )
        final_refit = (
            final_refit_ensemble(
                matrix=matrix,
                target=target,
                feature_names=feature_names,
                manifest=manifest,
                fold_results=seed_results,
                config=config,
                output_root=output_root,
                policy=final_policy,
                temperature=final_temperature,
            )
            if accepted and final_policy is not None and final_temperature is not None
            else {"status": "skipped_rejected_oof"}
        )
        report = {
            "schema_version": RUN_SCHEMA_VERSION,
            "status": "accepted" if accepted else "rejected",
            "completed_at": utc_now(),
            "objective": "selective_exact_long_short_neutral_v2",
            "class_order": list(CLASS_NAMES),
            "manifest_hash": manifest["manifest_hash"],
            "source_manifest_hash": manifest["source_manifest_hash"],
            "feature_schema_hash": manifest["feature_schema"]["feature_schema_hash"],
            "config_hash": config.config_hash,
            "config": _config_mapping(config),
            "folds": [fold_summary(fold) for fold in folds],
            "seed_results": seed_results,
            "fold_evaluations": fold_evaluations,
            "pooled_ranking": asdict(pooled_ranking),
            "class_prevalence": class_prevalence,
            "class_ap_above_prevalence": class_ap_above_prevalence,
            "selected_action_metrics": asdict(selected_action),
            "final_deployment_policy": (
                asdict(final_policy) if final_policy is not None else None
            ),
            "final_deployment_temperature": final_temperature,
            "shallow_tree_ranking": asdict(baseline_ranking),
            "shallow_tree_action_metrics": asdict(baseline_action),
            "macro_pr_auc_lift_over_shallow_tree": macro_pr_auc_lift,
            "outer_fold_pass_fraction": fold_pass_fraction,
            "worst_outer_fold_precision": worst_fold_precision,
            "monthly_positive_lift_fraction": monthly_lift_fraction,
            "monthly_metrics": monthly_metrics,
            "block_bootstrap_action_precision": bootstrap_precision,
            "trivial_baselines": trivial_baselines(ensemble_target),
            "acceptance": {
                "accepted": accepted,
                "thresholds": _acceptance_thresholds(config),
                "checks": acceptance_checks,
            },
            "final_refit": final_refit,
            "walk_forward_inspected": False,
        }
        _atomic_write_json(output_root / "training_report.json", report)
        status.update(
            {
                "status": "completed",
                "result": report["status"],
                "completed_at": utc_now(),
                "report_path": str(output_root / "training_report.json"),
            }
        )
        _atomic_write_json(output_root / "status.json", status)
        print(
            f"[lightgbm-complete] result={report['status']} "
            f"macro_ap={pooled_ranking.macro_pr_auc:.6f} "
            f"precision={selected_action.action_precision:.6f} "
            f"recall={selected_action.action_recall:.6f} "
            f"report={output_root / 'training_report.json'}",
            flush=True,
        )
        return report
    except KeyboardInterrupt:
        status.update({"status": "interrupted", "interrupted_at": utc_now()})
        _atomic_write_json(output_root / "status.json", status)
        print("\n[lightgbm-interrupted] completed fold/seed artifacts preserved", flush=True)
        raise
    except Exception:
        status.update({"status": "failed", "failed_at": utc_now()})
        _atomic_write_json(output_root / "status.json", status)
        raise


def fit_outer_run(
    *,
    matrix: np.ndarray,
    labels: pd.DataFrame,
    target: np.ndarray,
    feature_names: Sequence[str],
    fold: NestedMonthlyFold,
    seed: int,
    config: LightGBMConfig,
    run_root: Path,
    fold_number: int,
    fold_count: int,
) -> tuple[dict[str, Any], np.ndarray, np.ndarray]:
    run_root.mkdir(parents=True, exist_ok=True)
    result_path = run_root / "result.json"
    predictions_path = run_root / "predictions.parquet"
    inner_predictions_path = run_root / "inner_predictions.parquet"
    if (
        result_path.is_file()
        and predictions_path.is_file()
        and inner_predictions_path.is_file()
    ):
        result = json.loads(result_path.read_text())
        if result.get("config_hash") != config.config_hash:
            raise ValueError(f"stale LightGBM fold artifact: {result_path}")
        prediction_frame = pd.read_parquet(predictions_path)
        probabilities = probability_matrix(prediction_frame)
        inner_probabilities = probability_matrix(pd.read_parquet(inner_predictions_path))
        if len(probabilities) != len(fold.outer_validation_indices):
            raise ValueError(f"stale LightGBM prediction rows: {predictions_path}")
        if len(inner_probabilities) != len(fold.inner_validation_indices):
            raise ValueError(
                f"stale LightGBM inner prediction rows: {inner_predictions_path}"
            )
        print(f"[lightgbm-resume] fold={fold.fold_id} seed={seed}", flush=True)
        return result["run"], inner_probabilities, probabilities

    started = time.monotonic()
    inner_model = _new_classifier(
        config=config,
        seed=seed,
        n_estimators=config.max_estimators,
    )
    lightgbm = _require_lightgbm()
    run_label = f"fold={fold.fold_id} seed={seed}"
    print(
        f"[lightgbm-inner] {run_label} train_rows={len(fold.inner_train_indices)} "
        f"validation_rows={len(fold.inner_validation_indices)}",
        flush=True,
    )
    inner_model.fit(
        _matrix_rows(matrix, fold.inner_train_indices),
        target[fold.inner_train_indices],
        eval_set=[
            (
                _matrix_rows(matrix, fold.inner_validation_indices),
                target[fold.inner_validation_indices],
            )
        ],
        eval_metric=_lightgbm_macro_ap,
        callbacks=[
            lightgbm.early_stopping(
                config.early_stopping_rounds,
                first_metric_only=True,
                verbose=False,
            ),
            lightgbm.log_evaluation(period=0),
            _training_progress_callback(f"inner {run_label}"),
        ],
    )
    best_iteration = int(inner_model.best_iteration_ or config.max_estimators)
    inner_probabilities = _predict_probabilities(
        inner_model,
        _matrix_rows(matrix, fold.inner_validation_indices),
        num_iteration=best_iteration,
    )
    inner_ranking = ranking_metrics(
        target[fold.inner_validation_indices], inner_probabilities
    )
    refit_model = _new_classifier(
        config=config,
        seed=seed,
        n_estimators=best_iteration,
    )
    print(
        f"[lightgbm-refit] {run_label} rows={len(fold.outer_train_indices)} "
        f"trees={best_iteration}",
        flush=True,
    )
    refit_model.fit(
        _matrix_rows(matrix, fold.outer_train_indices),
        target[fold.outer_train_indices],
        callbacks=[_training_progress_callback(f"refit {run_label}")],
    )
    probabilities = _predict_probabilities(
        refit_model, _matrix_rows(matrix, fold.outer_validation_indices)
    )
    outer_ranking = ranking_metrics(target[fold.outer_validation_indices], probabilities)
    model_path = run_root / "model.txt"
    temporary_model = model_path.with_suffix(model_path.suffix + ".tmp")
    refit_model.booster_.save_model(str(temporary_model))
    temporary_model.replace(model_path)
    importance = _top_feature_importance(refit_model, feature_names)
    elapsed = time.monotonic() - started
    run = {
        "best_iteration": best_iteration,
        "inner_ranking": asdict(inner_ranking),
        "outer_ranking": asdict(outer_ranking),
        "seconds": round(elapsed, 3),
        "model_path": str(model_path),
        "model_sha256": _sha256(model_path),
        "top_feature_importance": importance,
    }
    prediction_frame = probabilities_frame(
        labels.iloc[fold.outer_validation_indices],
        probabilities,
        fold_id=fold.fold_id,
        seed=seed,
    )
    _atomic_write_parquet(predictions_path, prediction_frame)
    _atomic_write_parquet(
        inner_predictions_path,
        probabilities_frame(
            labels.iloc[fold.inner_validation_indices],
            inner_probabilities,
            fold_id=fold.fold_id,
            seed=seed,
        ),
    )
    _atomic_write_json(
        result_path,
        {
            "schema_version": RUN_SCHEMA_VERSION,
            "config_hash": config.config_hash,
            "fold": fold_summary(fold),
            "seed": seed,
            "run": run,
        },
    )
    print(
        f"[lightgbm fold {fold_number}/{fold_count} seed={seed}] "
        f"trees={best_iteration} inner_map={inner_ranking.macro_pr_auc:.6f} "
        f"outer_map={outer_ranking.macro_pr_auc:.6f} "
        f"accuracy={outer_ranking.argmax_accuracy:.4f} time={_duration(elapsed)}",
        flush=True,
    )
    return (
        run,
        np.asarray(inner_probabilities, dtype=np.float32),
        np.asarray(probabilities, dtype=np.float32),
    )


def fit_shallow_baseline(
    *,
    matrix: np.ndarray,
    target: np.ndarray,
    fold: NestedMonthlyFold,
    config: LightGBMConfig,
    seed: int,
) -> tuple[np.ndarray, np.ndarray]:
    inner_model = _new_classifier(
        config=config,
        seed=seed,
        n_estimators=200,
        overrides={
            "learning_rate": 0.05,
            "num_leaves": 7,
            "max_depth": 3,
            "min_child_samples": 2000,
            "feature_fraction": 1.0,
            "bagging_fraction": 1.0,
            "bagging_freq": 0,
            "reg_alpha": 0.0,
            "reg_lambda": 1.0,
        },
    )
    inner_model.fit(
        _matrix_rows(matrix, fold.inner_train_indices),
        target[fold.inner_train_indices],
    )
    inner_probabilities = _predict_probabilities(
        inner_model,
        _matrix_rows(matrix, fold.inner_validation_indices),
    )
    outer_model = _new_classifier(
        config=config,
        seed=seed,
        n_estimators=200,
        overrides={
            "learning_rate": 0.05,
            "num_leaves": 7,
            "max_depth": 3,
            "min_child_samples": 2000,
            "feature_fraction": 1.0,
            "bagging_fraction": 1.0,
            "bagging_freq": 0,
            "reg_alpha": 0.0,
            "reg_lambda": 1.0,
        },
    )
    outer_model.fit(
        _matrix_rows(matrix, fold.outer_train_indices),
        target[fold.outer_train_indices],
    )
    outer_probabilities = _predict_probabilities(
        outer_model,
        _matrix_rows(matrix, fold.outer_validation_indices),
    )
    return inner_probabilities, outer_probabilities


def final_refit_ensemble(
    *,
    matrix: np.ndarray,
    target: np.ndarray,
    feature_names: Sequence[str],
    manifest: Mapping[str, Any],
    fold_results: Sequence[Mapping[str, Any]],
    config: LightGBMConfig,
    output_root: Path,
    policy: DirectionalPolicy,
    temperature: float,
) -> dict[str, Any]:
    estimators = max(
        1,
        int(round(float(np.median([row["best_iteration"] for row in fold_results])))),
    )
    artifacts: list[dict[str, Any]] = []
    for seed in config.seeds:
        seed_root = output_root / "final_ensemble" / f"seed_{seed}"
        seed_root.mkdir(parents=True, exist_ok=True)
        model_path = seed_root / "model.txt"
        metadata_path = seed_root / "metadata.json"
        if model_path.is_file() and metadata_path.is_file():
            metadata = json.loads(metadata_path.read_text())
            if metadata.get("config_hash") != config.config_hash:
                raise ValueError(f"stale LightGBM final artifact: {model_path}")
            artifacts.append(
                {"seed": seed, "path": str(model_path), "sha256": _sha256(model_path)}
            )
            continue
        model = _new_classifier(
            config=config,
            seed=seed,
            n_estimators=estimators,
        )
        model.fit(
            matrix,
            target,
            callbacks=[_training_progress_callback(f"final seed={seed}")],
        )
        temporary_model = model_path.with_suffix(model_path.suffix + ".tmp")
        model.booster_.save_model(str(temporary_model))
        temporary_model.replace(model_path)
        _atomic_write_json(
            metadata_path,
            {
                "schema_version": MODEL_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "source_manifest_hash": manifest["source_manifest_hash"],
                "feature_schema_hash": manifest["feature_schema"]["feature_schema_hash"],
                "config_hash": config.config_hash,
                "class_order": list(CLASS_NAMES),
                "temperature": float(temperature),
                "policy": asdict(policy),
                "seed": seed,
                "estimators": estimators,
                "walk_forward_inspected": False,
            },
        )
        artifacts.append(
            {"seed": seed, "path": str(model_path), "sha256": _sha256(model_path)}
        )
    return {
        "status": "completed",
        "selected_estimators": estimators,
        "temperature": float(temperature),
        "policy": asdict(policy),
        "artifacts": artifacts,
    }


def require_controls(
    output_root: Path,
    *,
    manifest: Mapping[str, Any],
    config: LightGBMConfig,
) -> None:
    path = output_root / "controls.json"
    if not path.is_file():
        raise ValueError(f"LightGBM training requires controls at {path}")
    controls = json.loads(path.read_text())
    if controls.get("status") != "passed":
        raise ValueError("LightGBM controls did not pass")
    if controls.get("manifest_hash") != manifest["manifest_hash"]:
        raise ValueError("LightGBM input changed after controls")
    if controls.get("feature_schema_hash") != manifest["feature_schema"]["feature_schema_hash"]:
        raise ValueError("LightGBM feature schema changed after controls")
    if controls.get("config_hash") != config.config_hash:
        raise ValueError("LightGBM configuration changed after controls")


def fold_summary(fold: NestedMonthlyFold) -> dict[str, Any]:
    return {
        "fold_id": fold.fold_id,
        "inner_train_rows": int(len(fold.inner_train_indices)),
        "inner_validation_rows": int(len(fold.inner_validation_indices)),
        "inner_validation_start": fold.inner_validation_start.isoformat(),
        "inner_validation_end": fold.inner_validation_end.isoformat(),
        "outer_train_rows": int(len(fold.outer_train_indices)),
        "outer_train_end": fold.outer_train_end.isoformat(),
        "outer_validation_rows": int(len(fold.outer_validation_indices)),
        "outer_validation_start": fold.outer_validation_start.isoformat(),
        "outer_validation_end": fold.outer_validation_end.isoformat(),
    }


def _new_classifier(
    *,
    config: LightGBMConfig,
    seed: int,
    n_estimators: int,
    overrides: Mapping[str, Any] | None = None,
) -> Any:
    lightgbm = _require_lightgbm()
    parameters: dict[str, Any] = {
        "objective": "multiclass",
        "num_class": len(CLASS_NAMES),
        "metric": "None",
        "learning_rate": config.learning_rate,
        "num_leaves": config.num_leaves,
        "min_child_samples": config.min_child_samples,
        "feature_fraction": config.feature_fraction,
        "bagging_fraction": config.bagging_fraction,
        "bagging_freq": config.bagging_freq,
        "reg_alpha": config.reg_alpha,
        "reg_lambda": config.reg_lambda,
        "n_estimators": int(n_estimators),
        "random_state": int(seed),
        "feature_fraction_seed": int(seed),
        "bagging_seed": int(seed),
        "data_random_seed": int(seed),
        "deterministic": True,
        "force_col_wise": True,
        "n_jobs": -1,
        "verbosity": -1,
    }
    parameters.update(dict(overrides or {}))
    return lightgbm.LGBMClassifier(**parameters)


def _require_lightgbm() -> Any:
    matplotlib_cache = Path(tempfile.gettempdir()) / "motis-matplotlib"
    matplotlib_cache.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault("MPLCONFIGDIR", str(matplotlib_cache))
    try:
        import lightgbm
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "LightGBM is required for this runner; install the project dependencies first"
        ) from exc
    return lightgbm


def _lightgbm_macro_ap(target: np.ndarray, probabilities: np.ndarray) -> tuple[str, float, bool]:
    truth = np.asarray(target, dtype=np.int64)
    scores = np.asarray(probabilities, dtype=np.float64)
    if scores.ndim == 1:
        scores = scores.reshape(len(CLASS_NAMES), len(truth)).T
    one_hot = np.eye(len(CLASS_NAMES), dtype=np.int8)[truth]
    value = float(
        np.mean(
            [
                average_precision_score(one_hot[:, index], scores[:, index])
                for index in range(len(CLASS_NAMES))
            ]
        )
    )
    return "macro_ap", value, True


def _top_feature_importance(model: Any, feature_names: Sequence[str]) -> list[dict[str, Any]]:
    gain = np.asarray(model.booster_.feature_importance(importance_type="gain"), dtype=float)
    order = np.argsort(-gain)[:50]
    return [
        {"feature": str(feature_names[index]), "gain": float(gain[index])}
        for index in order
        if gain[index] > 0
    ]


def _predict_probabilities(
    model: Any,
    matrix: np.ndarray,
    *,
    num_iteration: int | None = None,
) -> np.ndarray:
    return np.asarray(
        model.booster_.predict(matrix, num_iteration=num_iteration),
        dtype=np.float32,
    )


def _matrix_rows(matrix: np.ndarray, indices: np.ndarray) -> np.ndarray:
    selected = np.asarray(indices, dtype=np.int64)
    if len(selected) and (len(selected) == 1 or np.all(np.diff(selected) == 1)):
        return matrix[int(selected[0]) : int(selected[-1]) + 1]
    return matrix[selected]


def _training_progress_callback(label: str, *, period: int = 50) -> Any:
    def callback(environment: Any) -> None:
        iteration = int(environment.iteration) + 1
        if iteration != 1 and iteration % period != 0:
            return
        metric = ""
        if environment.evaluation_result_list:
            name, metric_name, value, _ = environment.evaluation_result_list[0][:4]
            metric = f" {name}_{metric_name}={float(value):.6f}"
        print(
            f"[lightgbm-trees] {label} iteration={iteration}/{environment.end_iteration}{metric}",
            flush=True,
        )

    callback.order = 25
    callback.before_iteration = False
    return callback


def _matrix_contains_infinity(matrix: np.ndarray, *, chunk_rows: int = 8192) -> bool:
    for start in range(0, len(matrix), chunk_rows):
        if np.isinf(matrix[start : start + chunk_rows]).any():
            return True
    return False


def _config_mapping(config: LightGBMConfig) -> dict[str, Any]:
    value = asdict(config)
    value["seeds"] = list(config.seeds)
    return value


def _hash_mapping(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train exact LONG/SHORT/NEUTRAL LightGBM.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--mode", choices=("preflight", "controls", "train"), required=True)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--num-leaves", type=int, default=31)
    parser.add_argument("--min-child-samples", type=int, default=1000)
    parser.add_argument("--feature-fraction", type=float, default=0.80)
    parser.add_argument("--bagging-fraction", type=float, default=0.80)
    parser.add_argument("--bagging-freq", type=int, default=1)
    parser.add_argument("--reg-alpha", type=float, default=0.10)
    parser.add_argument("--reg-lambda", type=float, default=1.0)
    parser.add_argument("--max-estimators", type=int, default=1500)
    parser.add_argument("--early-stopping-rounds", type=int, default=75)
    parser.add_argument("--minimum-train-months", type=int, default=6)
    parser.add_argument("--inner-validation-months", type=int, default=1)
    parser.add_argument("--outer-validation-months", type=int, default=3)
    parser.add_argument("--outer-step-months", type=int, default=3)
    parser.add_argument("--seeds", type=int, nargs="+", default=[17, 29, 43])
    parser.add_argument("--minimum-action-precision", type=float, default=0.55)
    parser.add_argument("--minimum-action-recall", type=float, default=0.15)
    parser.add_argument("--minimum-entry-coverage", type=float, default=0.0)
    parser.add_argument("--minimum-side-coverage", type=float, default=0.0)
    parser.add_argument("--minimum-long-precision", type=float, default=0.50)
    parser.add_argument("--minimum-short-precision", type=float, default=0.50)
    parser.add_argument("--minimum-long-recall", type=float, default=0.05)
    parser.add_argument("--minimum-short-recall", type=float, default=0.05)
    parser.add_argument(
        "--maximum-neutral-false-positive-rate", type=float, default=0.10
    )
    parser.add_argument("--minimum-macro-pr-auc", type=float, default=0.40)
    parser.add_argument("--minimum-macro-pr-auc-lift", type=float, default=0.02)
    parser.add_argument("--minimum-fold-pass-fraction", type=float, default=0.70)
    parser.add_argument("--minimum-worst-fold-precision", type=float, default=0.45)
    parser.add_argument("--minimum-monthly-lift-fraction", type=float, default=0.70)
    parser.add_argument(
        "--minimum-bootstrap-precision-lower", type=float, default=0.50
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    output_root = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else Path(manifest["artifact_root"])
        / "training"
        / "supervised_directional_lightgbm_v2"
    )
    config = LightGBMConfig(
        learning_rate=args.learning_rate,
        num_leaves=args.num_leaves,
        min_child_samples=args.min_child_samples,
        feature_fraction=args.feature_fraction,
        bagging_fraction=args.bagging_fraction,
        bagging_freq=args.bagging_freq,
        reg_alpha=args.reg_alpha,
        reg_lambda=args.reg_lambda,
        max_estimators=args.max_estimators,
        early_stopping_rounds=args.early_stopping_rounds,
        minimum_train_months=args.minimum_train_months,
        inner_validation_months=args.inner_validation_months,
        outer_validation_months=args.outer_validation_months,
        outer_step_months=args.outer_step_months,
        seeds=tuple(args.seeds),
        minimum_action_precision=args.minimum_action_precision,
        minimum_action_recall=args.minimum_action_recall,
        minimum_entry_coverage=args.minimum_entry_coverage,
        minimum_side_coverage=args.minimum_side_coverage,
        minimum_long_precision=args.minimum_long_precision,
        minimum_short_precision=args.minimum_short_precision,
        minimum_long_recall=args.minimum_long_recall,
        minimum_short_recall=args.minimum_short_recall,
        maximum_neutral_false_positive_rate=(
            args.maximum_neutral_false_positive_rate
        ),
        minimum_macro_pr_auc=args.minimum_macro_pr_auc,
        minimum_macro_pr_auc_lift=args.minimum_macro_pr_auc_lift,
        minimum_fold_pass_fraction=args.minimum_fold_pass_fraction,
        minimum_worst_fold_precision=args.minimum_worst_fold_precision,
        minimum_monthly_lift_fraction=args.minimum_monthly_lift_fraction,
        minimum_bootstrap_precision_lower=args.minimum_bootstrap_precision_lower,
    )
    try:
        if args.mode == "preflight":
            report = run_preflight(
                manifest_path=manifest_path,
                output_root=output_root,
                config=config,
            )
            print(json.dumps(report, indent=2, sort_keys=True), flush=True)
            return 0
        if args.mode == "controls":
            run_controls(
                manifest_path=manifest_path,
                output_root=output_root,
                config=config,
            )
            return 0
        run_full_training(
            manifest_path=manifest_path,
            output_root=output_root,
            config=config,
        )
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
