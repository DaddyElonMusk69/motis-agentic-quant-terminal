from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import random
import sys
import time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    precision_recall_curve,
    roc_auc_score,
)
import torch
from torch import nn

from quant_terminal_worker.signal_discovery.supervised_sequence_model import (
    MODEL_SCHEMA_VERSION,
    MultiResolutionCausalTCN,
    build_sequence_chunk,
    partition_indices,
)
from quant_terminal_worker.signal_discovery.supervised_training_data import (
    BranchPreprocessorState,
    PreprocessorState,
    TransformedTimeline,
    fit_preprocessor,
    load_prepared_supervised_training_data,
    transform_timelines,
)


RUN_SCHEMA_VERSION = "motis_supervised_training_run.v1"
CONTROL_SCHEMA_VERSION = "motis_supervised_training_controls.v1"


@dataclass(frozen=True)
class TrainingConfig:
    hidden_channels: int = 24
    fusion_channels: int = 96
    kernel_size: int = 2
    dropout: float = 0.10
    learning_rate: float = 3e-4
    weight_decay: float = 1e-4
    gradient_clip: float = 1.0
    chunk_size: int = 1024
    max_epochs: int = 50
    patience: int = 8
    minimum_train_months: int = 6
    validation_months: int = 3
    fold_step_months: int = 3
    seeds: tuple[int, ...] = (17, 29, 43)

    @property
    def model_hash(self) -> str:
        model_fields = {
            "hidden_channels": self.hidden_channels,
            "fusion_channels": self.fusion_channels,
            "kernel_size": self.kernel_size,
            "dropout": self.dropout,
        }
        return _mapping_hash(model_fields)


@dataclass(frozen=True)
class ChronologicalFold:
    fold_id: str
    train_indices: np.ndarray
    validation_indices: np.ndarray
    train_end: pd.Timestamp
    validation_start: pd.Timestamp
    validation_end: pd.Timestamp


@dataclass(frozen=True)
class TimestampMetrics:
    eligible_rows: int
    positive_rows: int
    negative_rows: int
    prevalence: float
    threshold: float
    true_positive: int
    false_positive: int
    true_negative: int
    false_negative: int
    precision: float
    recall: float
    f1: float
    pr_auc: float
    roc_auc: float
    brier_score: float
    calibration_error: float


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def build_expanding_folds(
    labels: pd.DataFrame,
    *,
    research_start: pd.Timestamp,
    research_end: pd.Timestamp,
    minimum_train_months: int = 6,
    validation_months: int = 3,
    step_months: int = 3,
) -> list[ChronologicalFold]:
    decision_ts = pd.to_datetime(labels["decision_ts"], utc=True)
    horizon_end = pd.to_datetime(labels["horizon_end_ts"], utc=True)
    validation_start = research_start + pd.DateOffset(months=minimum_train_months)
    folds: list[ChronologicalFold] = []
    while True:
        validation_stop = validation_start + pd.DateOffset(months=validation_months)
        validation_end = validation_stop - pd.Timedelta(minutes=5)
        if validation_end > research_end:
            break
        train_mask = (decision_ts < validation_start) & (horizon_end < validation_start)
        validation_mask = (decision_ts >= validation_start) & (
            decision_ts < validation_stop
        )
        train_indices = np.flatnonzero(train_mask.to_numpy())
        validation_indices = np.flatnonzero(validation_mask.to_numpy())
        if len(train_indices) and len(validation_indices):
            folds.append(
                ChronologicalFold(
                    fold_id=f"fold_{len(folds) + 1:02d}",
                    train_indices=train_indices,
                    validation_indices=validation_indices,
                    train_end=pd.Timestamp(decision_ts.iloc[train_indices[-1]]),
                    validation_start=pd.Timestamp(validation_start),
                    validation_end=pd.Timestamp(validation_end),
                )
            )
        validation_start += pd.DateOffset(months=step_months)
    if len(folds) < 3:
        raise ValueError(f"chronological training requires at least three folds, found {len(folds)}")
    return folds


def timestamp_metrics(
    target: np.ndarray,
    scores: np.ndarray,
    *,
    threshold: float | None = None,
) -> TimestampMetrics:
    truth = np.asarray(target, dtype=np.int8)
    probability = np.asarray(scores, dtype=np.float64)
    if truth.shape != probability.shape or truth.ndim != 1 or not len(truth):
        raise ValueError("metrics require aligned non-empty target and score vectors")
    selected_threshold = (
        best_f1_threshold(truth, probability) if threshold is None else float(threshold)
    )
    predicted = probability >= selected_threshold
    positive = truth == 1
    tp = int(np.sum(predicted & positive))
    fp = int(np.sum(predicted & ~positive))
    tn = int(np.sum(~predicted & ~positive))
    fn = int(np.sum(~predicted & positive))
    precision = tp / max(tp + fp, 1)
    recall = tp / max(tp + fn, 1)
    f1 = 2 * precision * recall / max(precision + recall, 1e-12)
    return TimestampMetrics(
        eligible_rows=int(len(truth)),
        positive_rows=int(positive.sum()),
        negative_rows=int((~positive).sum()),
        prevalence=float(positive.mean()),
        threshold=selected_threshold,
        true_positive=tp,
        false_positive=fp,
        true_negative=tn,
        false_negative=fn,
        precision=float(precision),
        recall=float(recall),
        f1=float(f1),
        pr_auc=float(average_precision_score(truth, probability)),
        roc_auc=(
            float(roc_auc_score(truth, probability))
            if len(np.unique(truth)) > 1
            else float("nan")
        ),
        brier_score=float(brier_score_loss(truth, probability)),
        calibration_error=expected_calibration_error(truth, probability),
    )


def always_enter_comparison(metrics: TimestampMetrics) -> dict[str, float | bool]:
    always_enter_f1 = 2 * metrics.prevalence / max(1 + metrics.prevalence, 1e-12)
    predicted_positive_rows = metrics.true_positive + metrics.false_positive
    selection_rate = predicted_positive_rows / max(metrics.eligible_rows, 1)
    passed = (
        metrics.precision > metrics.prevalence + 1e-6
        and metrics.f1 > always_enter_f1 + 1e-6
        and metrics.true_negative > 0
        and predicted_positive_rows < metrics.eligible_rows
    )
    return {
        "passed": bool(passed),
        "always_enter_precision": float(metrics.prevalence),
        "always_enter_recall": 1.0,
        "always_enter_f1": float(always_enter_f1),
        "selected_precision": float(metrics.precision),
        "selected_recall": float(metrics.recall),
        "selected_f1": float(metrics.f1),
        "selected_timestamp_rate": float(selection_rate),
    }


def best_f1_threshold(target: np.ndarray, scores: np.ndarray) -> float:
    precision, recall, thresholds = precision_recall_curve(target, scores)
    if not len(thresholds):
        return 0.5
    f1 = 2 * precision[:-1] * recall[:-1] / np.maximum(
        precision[:-1] + recall[:-1], 1e-12
    )
    return float(thresholds[int(np.nanargmax(f1))])


def expected_calibration_error(
    target: np.ndarray,
    scores: np.ndarray,
    *,
    bins: int = 10,
) -> float:
    truth = np.asarray(target, dtype=np.float64)
    probability = np.asarray(scores, dtype=np.float64)
    edges = np.linspace(0.0, 1.0, bins + 1)
    bucket = np.minimum(np.digitize(probability, edges[1:-1]), bins - 1)
    error = 0.0
    for index in range(bins):
        selected = bucket == index
        if selected.any():
            error += selected.mean() * abs(
                float(probability[selected].mean() - truth[selected].mean())
            )
    return float(error)


def threshold_frontier(
    target: np.ndarray,
    scores: np.ndarray,
    *,
    points: int = 201,
) -> list[dict[str, float | int]]:
    quantiles = np.linspace(0.0, 1.0, min(points, len(scores)))
    thresholds = np.unique(np.quantile(scores, quantiles))[::-1]
    return [asdict(timestamp_metrics(target, scores, threshold=float(value))) for value in thresholds]


def resolve_device(requested: str) -> torch.device:
    if requested == "auto":
        if torch.backends.mps.is_available():
            return torch.device("mps")
        if torch.cuda.is_available():
            return torch.device("cuda")
        return torch.device("cpu")
    if requested == "mps" and not torch.backends.mps.is_available():
        raise ValueError("MPS was requested but torch.backends.mps.is_available() is false")
    if requested == "cuda" and not torch.cuda.is_available():
        raise ValueError("CUDA was requested but torch.cuda.is_available() is false")
    return torch.device(requested)


def set_deterministic_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def create_model(
    timelines: Mapping[str, TransformedTimeline],
    config: TrainingConfig,
) -> MultiResolutionCausalTCN:
    return MultiResolutionCausalTCN(
        branch_input_channels={name: value.values.shape[1] for name, value in timelines.items()},
        branch_steps={name: value.spec.steps for name, value in timelines.items()},
        hidden_channels=config.hidden_channels,
        fusion_channels=config.fusion_channels,
        kernel_size=config.kernel_size,
        dropout=config.dropout,
    )


def run_preflight(
    *,
    manifest_path: Path,
    output_root: Path,
    config: TrainingConfig,
) -> dict[str, Any]:
    labels, timelines, manifest = load_prepared_supervised_training_data(manifest_path)
    splits = manifest["splits"]
    folds = build_expanding_folds(
        labels,
        research_start=pd.Timestamp(splits["research_start"]),
        research_end=pd.Timestamp(splits["research_end"]),
        minimum_train_months=config.minimum_train_months,
        validation_months=config.validation_months,
        step_months=config.fold_step_months,
    )
    failures: list[str] = []
    if manifest["target"].get("episodes_used") is not False:
        failures.append("prepared target must declare episodes_used=false")
    if manifest["labels"].get("episode_fields_present") is not False:
        failures.append("prepared labels contain episode fields")
    if float(manifest["target"].get("base_sample_weight", 0)) != 1.0:
        failures.append("base timestamp sample weight must equal one")
    if timelines["5m_micro"].spec.lookback_days < 7.0:
        failures.append("5m_micro must preserve at least seven days")
    branch_audit: dict[str, Any] = {}
    for name, timeline in timelines.items():
        decisions = labels["decision_ns"].to_numpy(dtype=np.int64)
        latest = np.searchsorted(timeline.available_ns, decisions, side="right") - 1
        ready = latest >= timeline.spec.steps - 1
        causal = timeline.available_ns[np.maximum(latest, 0)] <= decisions
        if not ready.all():
            failures.append(f"{name} lacks complete history for prepared labels")
        if not causal.all():
            failures.append(f"{name} exposes a row before available_at")
        branch_audit[name] = {
            "rows": int(len(timeline.values)),
            "raw_channels": int(timeline.values.shape[1]),
            "tensor_channels_with_masks": int(timeline.values.shape[1] * 2),
            "steps": int(timeline.spec.steps),
            "lookback_days": float(timeline.spec.lookback_days),
            "first_available_ns": int(timeline.available_ns[0]),
            "last_available_ns": int(timeline.available_ns[-1]),
        }
    report = {
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "passed" if not failures else "failed",
        "created_at": utc_now(),
        "manifest_path": str(manifest_path.resolve()),
        "manifest_hash": manifest["manifest_hash"],
        "target_config_hash": manifest["target_config_hash"],
        "model_config_hash": config.model_hash,
        "training_config": _config_mapping(config),
        "labels": {
            "eligible_rows": int(len(labels)),
            "positive_rows": int(labels["target"].sum()),
            "negative_rows": int((labels["target"] == 0).sum()),
            "prevalence": float(labels["target"].mean()),
            "first_decision_ts": labels["decision_ts"].min().isoformat(),
            "last_decision_ts": labels["decision_ts"].max().isoformat(),
            "last_horizon_end_ts": labels["horizon_end_ts"].max().isoformat(),
        },
        "folds": [fold_summary(fold) for fold in folds],
        "branches": branch_audit,
        "failures": failures,
    }
    output_root.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(output_root / "preflight.json", report)
    if failures:
        raise ValueError("supervised training preflight failed: " + "; ".join(failures))
    return report


def run_controls(
    *,
    manifest_path: Path,
    output_root: Path,
    config: TrainingConfig,
    device: torch.device,
    overfit_epochs: int = 80,
    shuffle_epochs: int = 4,
    overfit_rows: int = 256,
    shuffle_train_rows: int = 4096,
    shuffle_validation_rows: int = 2048,
) -> dict[str, Any]:
    preflight = run_preflight(
        manifest_path=manifest_path,
        output_root=output_root,
        config=config,
    )
    labels, timelines, manifest = load_prepared_supervised_training_data(manifest_path)
    folds = _folds_from_manifest(labels, manifest, config)
    fold = folds[0]
    print(
        f"[controls] device={device} manifest={manifest['manifest_hash']} "
        f"labels={len(labels)}",
        flush=True,
    )

    overfit_indices = _contiguous_mixed_control_indices(
        fold.train_indices,
        labels["target"].to_numpy(dtype=np.int8),
        overfit_rows,
    )
    overfit_end = pd.Timestamp(labels.iloc[overfit_indices[-1]]["decision_ts"])
    preprocessor = fit_preprocessor(
        timelines,
        train_end=overfit_end,
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    transformed = transform_timelines(timelines, preprocessor)
    overfit_scores, overfit_history = _fit_control_model(
        labels=labels,
        timelines=transformed,
        train_indices=overfit_indices,
        evaluation_indices=overfit_indices,
        config=config,
        device=device,
        epochs=overfit_epochs,
        seed=config.seeds[0],
        shuffle_targets=False,
        label="overfit",
    )
    overfit_metrics = timestamp_metrics(
        labels.iloc[overfit_indices]["target"].to_numpy(dtype=np.int8),
        overfit_scores,
    )
    overfit_passed = (
        overfit_metrics.positive_rows > 0
        and overfit_metrics.negative_rows > 0
        and overfit_metrics.f1 >= 0.95
        and overfit_metrics.pr_auc >= 0.98
    )

    shuffle_train = fold.train_indices[-min(shuffle_train_rows, len(fold.train_indices)) :]
    shuffle_validation = fold.validation_indices[:shuffle_validation_rows]
    shuffle_end = pd.Timestamp(labels.iloc[shuffle_train[-1]]["decision_ts"])
    shuffle_preprocessor = fit_preprocessor(
        timelines,
        train_end=shuffle_end,
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    shuffle_timelines = transform_timelines(timelines, shuffle_preprocessor)
    shuffle_scores, shuffle_history = _fit_control_model(
        labels=labels,
        timelines=shuffle_timelines,
        train_indices=shuffle_train,
        evaluation_indices=shuffle_validation,
        config=config,
        device=device,
        epochs=shuffle_epochs,
        seed=config.seeds[0] + 10_000,
        shuffle_targets=True,
        label="shuffled-label",
    )
    shuffle_target = labels.iloc[shuffle_validation]["target"].to_numpy(dtype=np.int8)
    shuffle_metrics = timestamp_metrics(shuffle_target, shuffle_scores)
    shuffle_tolerance = 0.05
    shuffle_passed = shuffle_metrics.pr_auc <= shuffle_metrics.prevalence + shuffle_tolerance
    passed = bool(preflight["status"] == "passed" and overfit_passed and shuffle_passed)
    report = {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "created_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "target_config_hash": manifest["target_config_hash"],
        "model_config_hash": config.model_hash,
        "device": str(device),
        "overfit": {
            "passed": bool(overfit_passed),
            "rows": int(len(overfit_indices)),
            "epochs": int(overfit_epochs),
            "metrics": asdict(overfit_metrics),
            "history": overfit_history,
            "acceptance": {"minimum_f1": 0.95, "minimum_pr_auc": 0.98},
        },
        "shuffled_label": {
            "passed": bool(shuffle_passed),
            "train_rows": int(len(shuffle_train)),
            "validation_rows": int(len(shuffle_validation)),
            "epochs": int(shuffle_epochs),
            "metrics": asdict(shuffle_metrics),
            "history": shuffle_history,
            "acceptance": {"maximum_pr_auc_lift_over_prevalence": shuffle_tolerance},
        },
    }
    _atomic_write_json(output_root / "controls.json", report)
    print(f"[controls] status={report['status']}", flush=True)
    if not passed:
        raise ValueError("training controls failed; full chronological training is blocked")
    return report


def run_full_training(
    *,
    manifest_path: Path,
    output_root: Path,
    config: TrainingConfig,
    device: torch.device,
) -> dict[str, Any]:
    labels, raw_timelines, manifest = load_prepared_supervised_training_data(manifest_path)
    _require_current_controls(output_root, manifest=manifest, config=config)
    folds = _folds_from_manifest(labels, manifest, config)
    output_root.mkdir(parents=True, exist_ok=True)
    status = {
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "running",
        "started_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "target_config_hash": manifest["target_config_hash"],
        "model_config_hash": config.model_hash,
        "device": str(device),
        "fold_count": len(folds),
        "seeds": list(config.seeds),
    }
    _atomic_write_json(output_root / "status.json", status)
    result_rows: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    baseline_rows: list[dict[str, Any]] = []
    total_runs = len(folds) * len(config.seeds)
    completed_runs = 0
    started = time.monotonic()

    try:
        for fold_number, fold in enumerate(folds, start=1):
            preprocessor = fit_preprocessor(
                raw_timelines,
                train_end=fold.train_end,
                clip_value=float(manifest["preprocessing"]["clip_value"]),
            )
            timelines = transform_timelines(raw_timelines, preprocessor)
            baseline = run_causal_logistic_baseline(
                labels=labels,
                timelines=timelines,
                fold=fold,
                chunk_size=config.chunk_size,
                seed=config.seeds[0],
            )
            baseline_rows.append({"fold_id": fold.fold_id, **asdict(baseline)})
            print(
                f"[{fold.fold_id}] logistic_baseline pr_auc={baseline.pr_auc:.6f} "
                f"prevalence={baseline.prevalence:.6f}",
                flush=True,
            )
            for seed in config.seeds:
                run_root = output_root / fold.fold_id / f"seed_{seed}"
                run_root.mkdir(parents=True, exist_ok=True)
                run_result = train_fold(
                    labels=labels,
                    timelines=timelines,
                    preprocessor=preprocessor,
                    fold=fold,
                    seed=seed,
                    config=config,
                    device=device,
                    run_root=run_root,
                    fold_number=fold_number,
                    fold_count=len(folds),
                )
                scores = np.asarray(run_result.pop("validation_scores"), dtype=np.float64)
                validation = labels.iloc[fold.validation_indices]
                metrics = timestamp_metrics(
                    validation["target"].to_numpy(dtype=np.int8),
                    scores,
                )
                result_rows.append(
                    {
                        "fold_id": fold.fold_id,
                        "seed": seed,
                        **run_result,
                        **asdict(metrics),
                    }
                )
                prediction_frames.append(
                    pd.DataFrame(
                        {
                            "decision_ts": validation["decision_ts"].to_numpy(),
                            "target": validation["target"].to_numpy(dtype=np.int8),
                            "score": scores.astype(np.float32),
                            "fold_id": fold.fold_id,
                            "seed": seed,
                        }
                    )
                )
                completed_runs += 1
                elapsed = time.monotonic() - started
                eta = elapsed / completed_runs * max(total_runs - completed_runs, 0)
                print(
                    f"[progress] completed={completed_runs}/{total_runs} "
                    f"elapsed={_duration(elapsed)} eta={_duration(eta)}",
                    flush=True,
                )
                status.update(
                    {
                        "completed_runs": completed_runs,
                        "total_runs": total_runs,
                        "updated_at": utc_now(),
                        "eta_seconds": round(eta, 1),
                    }
                )
                _atomic_write_json(output_root / "status.json", status)
            del timelines
            _empty_device_cache(device)

        predictions = pd.concat(prediction_frames, ignore_index=True)
        predictions_path = output_root / "oof_predictions.parquet"
        _atomic_write_parquet(predictions_path, predictions)
        ensemble_predictions = (
            predictions.groupby("decision_ts", as_index=False)
            .agg(
                target=("target", "first"),
                score=("score", "mean"),
                fold_id=("fold_id", "first"),
            )
            .sort_values("decision_ts")
            .reset_index(drop=True)
        )
        _atomic_write_parquet(
            output_root / "oof_ensemble_predictions.parquet",
            ensemble_predictions,
        )
        pooled = timestamp_metrics(
            ensemble_predictions["target"].to_numpy(dtype=np.int8),
            ensemble_predictions["score"].to_numpy(dtype=np.float64),
        )
        median_sequence_ap = float(np.median([row["pr_auc"] for row in result_rows]))
        median_baseline_ap = float(np.median([row["pr_auc"] for row in baseline_rows]))
        median_prevalence = float(np.median([row["prevalence"] for row in result_rows]))
        baseline_by_fold = {row["fold_id"]: row["pr_auc"] for row in baseline_rows}
        prevalence_win_fraction = float(
            np.mean([row["pr_auc"] > row["prevalence"] for row in result_rows])
        )
        baseline_win_fraction = float(
            np.mean(
                [
                    row["pr_auc"] > baseline_by_fold[row["fold_id"]]
                    for row in result_rows
                ]
            )
        )
        required_win_fraction = 2.0 / 3.0
        always_enter = always_enter_comparison(pooled)
        accepted = (
            median_sequence_ap > max(median_baseline_ap, median_prevalence)
            and prevalence_win_fraction >= required_win_fraction
            and baseline_win_fraction >= required_win_fraction
            and always_enter["passed"]
        )
        final_refit = (
            refit_final_ensemble(
                labels=labels,
                raw_timelines=raw_timelines,
                manifest=manifest,
                result_rows=result_rows,
                config=config,
                device=device,
                output_root=output_root,
            )
            if accepted
            else {"status": "skipped_rejected_oof"}
        )
        report = {
            "schema_version": RUN_SCHEMA_VERSION,
            "status": "accepted" if accepted else "rejected",
            "completed_at": utc_now(),
            "manifest_hash": manifest["manifest_hash"],
            "target_config_hash": manifest["target_config_hash"],
            "model_config_hash": config.model_hash,
            "device": str(device),
            "training_config": _config_mapping(config),
            "fold_results": result_rows,
            "causal_logistic_baselines": baseline_rows,
            "pooled_oof_metrics": asdict(pooled),
            "monthly_metrics": monthly_metrics(
                ensemble_predictions,
                threshold=pooled.threshold,
            ),
            "calibration_curve": reliability_curve(
                ensemble_predictions["target"].to_numpy(dtype=np.int8),
                ensemble_predictions["score"].to_numpy(dtype=np.float64),
            ),
            "block_bootstrap_95pct": block_bootstrap_confidence_intervals(
                ensemble_predictions,
                threshold=pooled.threshold,
            ),
            "threshold_frontier": threshold_frontier(
                ensemble_predictions["target"].to_numpy(dtype=np.int8),
                ensemble_predictions["score"].to_numpy(dtype=np.float64),
            ),
            "acceptance": {
                "accepted": bool(accepted),
                "median_sequence_pr_auc": median_sequence_ap,
                "median_logistic_pr_auc": median_baseline_ap,
                "median_prevalence": median_prevalence,
                "prevalence_win_fraction": prevalence_win_fraction,
                "causal_logistic_win_fraction": baseline_win_fraction,
                "required_win_fraction": required_win_fraction,
                "always_enter_comparison": always_enter,
                "rule": (
                    "median sequence PR-AUC must beat prevalence and causal logistic, "
                    "with both lifts present in at least two thirds of fold-seed runs; "
                    "the pooled operating policy must beat always-enter precision and F1 "
                    "and reject at least one negative timestamp"
                ),
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
        _write_rationale(output_root=output_root, manifest=manifest, report=report)
        print(
            f"[complete] result={report['status']} pooled_pr_auc={pooled.pr_auc:.6f} "
            f"report={output_root / 'training_report.json'}",
            flush=True,
        )
        return report
    except KeyboardInterrupt:
        status.update({"status": "interrupted", "interrupted_at": utc_now()})
        _atomic_write_json(output_root / "status.json", status)
        print(
            f"\n[interrupted] checkpoint preserved; rerun the same command to resume from {output_root}",
            flush=True,
        )
        raise
    except Exception as exc:
        status.update(
            {
                "status": "failed",
                "failed_at": utc_now(),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        _atomic_write_json(output_root / "status.json", status)
        raise


def train_fold(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    preprocessor: PreprocessorState,
    fold: ChronologicalFold,
    seed: int,
    config: TrainingConfig,
    device: torch.device,
    run_root: Path,
    fold_number: int,
    fold_count: int,
) -> dict[str, Any]:
    set_deterministic_seed(seed)
    model = create_model(timelines, config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=config.learning_rate,
        weight_decay=config.weight_decay,
    )
    checkpoint_path = run_root / "last.pt"
    best_path = run_root / "best.pt"
    start_epoch = 1
    best_ap = -math.inf
    best_epoch = 0
    stale_epochs = 0
    history: list[dict[str, Any]] = []
    if checkpoint_path.is_file():
        checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=False)
        _validate_checkpoint(checkpoint, fold=fold, seed=seed, config=config)
        model.load_state_dict(checkpoint["model_state"])
        optimizer.load_state_dict(checkpoint["optimizer_state"])
        start_epoch = int(checkpoint["epoch"]) + 1
        best_ap = float(checkpoint["best_pr_auc"])
        best_epoch = int(checkpoint["best_epoch"])
        stale_epochs = int(checkpoint["stale_epochs"])
        history = list(checkpoint.get("history") or [])
        print(
            f"[{fold.fold_id} seed={seed}] resuming at epoch {start_epoch}",
            flush=True,
        )

    train_chunks = partition_indices(fold.train_indices, config.chunk_size)
    epoch_times: list[float] = []
    for epoch in range(start_epoch, config.max_epochs + 1):
        epoch_started = time.monotonic()
        train_loss = train_one_epoch(
            model=model,
            optimizer=optimizer,
            labels=labels,
            timelines=timelines,
            chunks=train_chunks,
            device=device,
            gradient_clip=config.gradient_clip,
            seed=seed + epoch,
        )
        scores = score_indices(
            model=model,
            labels=labels,
            timelines=timelines,
            indices=fold.validation_indices,
            chunk_size=config.chunk_size,
            device=device,
        )
        target = labels.iloc[fold.validation_indices]["target"].to_numpy(dtype=np.int8)
        metrics = timestamp_metrics(target, scores)
        improved = metrics.pr_auc > best_ap + 1e-6
        if improved:
            best_ap = metrics.pr_auc
            best_epoch = epoch
            stale_epochs = 0
            _atomic_torch_save(
                best_path,
                _model_artifact(
                    model=model,
                    preprocessor=preprocessor,
                    fold=fold,
                    seed=seed,
                    epoch=epoch,
                    config=config,
                    metrics=metrics,
                ),
            )
        else:
            stale_epochs += 1
        epoch_seconds = time.monotonic() - epoch_started
        epoch_times.append(epoch_seconds)
        history.append(
            {
                "epoch": epoch,
                "train_loss": train_loss,
                "validation_pr_auc": metrics.pr_auc,
                "validation_precision": metrics.precision,
                "validation_recall": metrics.recall,
                "validation_f1": metrics.f1,
                "best_pr_auc": best_ap,
                "best_epoch": best_epoch,
                "seconds": round(epoch_seconds, 3),
            }
        )
        _atomic_torch_save(
            checkpoint_path,
            {
                "schema_version": RUN_SCHEMA_VERSION,
                "fold_id": fold.fold_id,
                "seed": seed,
                "model_config_hash": config.model_hash,
                "epoch": epoch,
                "best_pr_auc": best_ap,
                "best_epoch": best_epoch,
                "stale_epochs": stale_epochs,
                "model_state": model.state_dict(),
                "optimizer_state": optimizer.state_dict(),
                "history": history,
            },
        )
        remaining = min(
            max(config.max_epochs - epoch, 0),
            max(config.patience - stale_epochs, 0),
        )
        eta = float(np.mean(epoch_times[-3:])) * remaining
        print(
            f"[fold {fold_number}/{fold_count} seed={seed} epoch={epoch}/{config.max_epochs}] "
            f"loss={train_loss:.6f} val_ap={metrics.pr_auc:.6f} "
            f"precision={metrics.precision:.4f} recall={metrics.recall:.4f} "
            f"best={best_ap:.6f}@{best_epoch} stale={stale_epochs}/{config.patience} "
            f"epoch_time={_duration(epoch_seconds)} eta_run={_duration(eta)}",
            flush=True,
        )
        if stale_epochs >= config.patience:
            break
    best = torch.load(best_path, map_location=device, weights_only=False)
    model.load_state_dict(best["model_state"])
    scores = score_indices(
        model=model,
        labels=labels,
        timelines=timelines,
        indices=fold.validation_indices,
        chunk_size=config.chunk_size,
        device=device,
    )
    return {
        "best_epoch": int(best_epoch),
        "best_pr_auc": float(best_ap),
        "epochs_completed": int(history[-1]["epoch"]),
        "checkpoint_path": str(best_path),
        "validation_scores": scores,
    }


def refit_final_ensemble(
    *,
    labels: pd.DataFrame,
    raw_timelines: Mapping[str, Any],
    manifest: Mapping[str, Any],
    result_rows: Sequence[Mapping[str, Any]],
    config: TrainingConfig,
    device: torch.device,
    output_root: Path,
) -> dict[str, Any]:
    selected_epochs = max(
        1,
        int(round(float(np.median([row["best_epoch"] for row in result_rows])))),
    )
    train_end = pd.Timestamp(labels["decision_ts"].max())
    preprocessor = fit_preprocessor(
        raw_timelines,
        train_end=train_end,
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    timelines = transform_timelines(raw_timelines, preprocessor)
    indices = np.arange(len(labels), dtype=np.int64)
    chunks = partition_indices(indices, config.chunk_size)
    artifacts: list[dict[str, Any]] = []
    print(
        f"[final-refit] accepted OOF candidate; epochs={selected_epochs} "
        f"seeds={list(config.seeds)} rows={len(labels)}",
        flush=True,
    )
    for seed_number, seed in enumerate(config.seeds, start=1):
        seed_root = output_root / "final_ensemble" / f"seed_{seed}"
        seed_root.mkdir(parents=True, exist_ok=True)
        artifact_path = seed_root / "model.pt"
        if artifact_path.is_file():
            existing = torch.load(artifact_path, map_location="cpu", weights_only=False)
            if (
                existing.get("manifest_hash") == manifest["manifest_hash"]
                and existing.get("model_config_hash") == config.model_hash
                and int(existing.get("epochs")) == selected_epochs
            ):
                artifacts.append(
                    {
                        "seed": seed,
                        "path": str(artifact_path),
                        "sha256": _sha256(artifact_path),
                    }
                )
                print(f"[final-refit seed={seed}] existing artifact verified", flush=True)
                continue
            raise ValueError(f"stale final artifact conflicts with current run: {artifact_path}")

        set_deterministic_seed(seed)
        model = create_model(timelines, config).to(device)
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.learning_rate,
            weight_decay=config.weight_decay,
        )
        last_path = seed_root / "last.pt"
        start_epoch = 1
        history: list[dict[str, Any]] = []
        if last_path.is_file():
            checkpoint = torch.load(last_path, map_location=device, weights_only=False)
            if (
                checkpoint.get("kind") != "final_refit"
                or checkpoint.get("manifest_hash") != manifest["manifest_hash"]
                or checkpoint.get("model_config_hash") != config.model_hash
                or int(checkpoint.get("seed")) != seed
                or int(checkpoint.get("selected_epochs")) != selected_epochs
            ):
                raise ValueError(f"stale final-refit checkpoint: {last_path}")
            model.load_state_dict(checkpoint["model_state"])
            optimizer.load_state_dict(checkpoint["optimizer_state"])
            start_epoch = int(checkpoint["epoch"]) + 1
            history = list(checkpoint.get("history") or [])
        epoch_times: list[float] = []
        for epoch in range(start_epoch, selected_epochs + 1):
            epoch_started = time.monotonic()
            loss = train_one_epoch(
                model=model,
                optimizer=optimizer,
                labels=labels,
                timelines=timelines,
                chunks=chunks,
                device=device,
                gradient_clip=config.gradient_clip,
                seed=seed + epoch,
            )
            elapsed = time.monotonic() - epoch_started
            epoch_times.append(elapsed)
            history.append({"epoch": epoch, "train_loss": loss, "seconds": round(elapsed, 3)})
            _atomic_torch_save(
                last_path,
                {
                    "schema_version": RUN_SCHEMA_VERSION,
                    "kind": "final_refit",
                    "manifest_hash": manifest["manifest_hash"],
                    "model_config_hash": config.model_hash,
                    "seed": seed,
                    "selected_epochs": selected_epochs,
                    "epoch": epoch,
                    "model_state": model.state_dict(),
                    "optimizer_state": optimizer.state_dict(),
                    "history": history,
                },
            )
            eta = float(np.mean(epoch_times[-3:])) * (selected_epochs - epoch)
            print(
                f"[final-refit {seed_number}/{len(config.seeds)} seed={seed} "
                f"epoch={epoch}/{selected_epochs}] loss={loss:.6f} "
                f"epoch_time={_duration(elapsed)} eta_seed={_duration(eta)}",
                flush=True,
            )
        state_dict = {name: value.detach().cpu() for name, value in model.state_dict().items()}
        _atomic_torch_save(
            artifact_path,
            {
                "schema_version": MODEL_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "target_config_hash": manifest["target_config_hash"],
                "model_config_hash": config.model_hash,
                "model_config": _config_mapping(config),
                "branch_schema": {
                    name: {
                        "input_channels": int(timeline.values.shape[1]),
                        "channel_names": list(timeline.channel_names),
                        "steps": int(timeline.spec.steps),
                    }
                    for name, timeline in timelines.items()
                },
                "preprocessor": _preprocessor_mapping(preprocessor),
                "seed": seed,
                "epochs": selected_epochs,
                "state_dict": state_dict,
                "walk_forward_inspected": False,
            },
        )
        artifacts.append(
            {
                "seed": seed,
                "path": str(artifact_path),
                "sha256": _sha256(artifact_path),
            }
        )
    return {
        "status": "completed",
        "selected_epochs": selected_epochs,
        "epoch_selection": "median best epoch across chronological fold-seed runs",
        "artifacts": artifacts,
    }


def train_one_epoch(
    *,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    chunks: Sequence[np.ndarray],
    device: torch.device,
    gradient_clip: float,
    seed: int,
) -> float:
    model.train()
    order = np.random.default_rng(seed).permutation(len(chunks))
    total_loss = 0.0
    total_rows = 0
    for position in order:
        indices = chunks[int(position)]
        chunk = build_sequence_chunk(
            decision_ns=labels.iloc[indices]["decision_ns"].to_numpy(dtype=np.int64),
            targets=labels.iloc[indices]["target"].to_numpy(dtype=np.float32),
            timelines=timelines,
        )
        values, gather, target = _chunk_tensors(chunk, device)
        optimizer.zero_grad(set_to_none=True)
        logits = model(values, gather)
        loss = nn.functional.binary_cross_entropy_with_logits(logits, target)
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), gradient_clip)
        optimizer.step()
        total_loss += float(loss.detach().cpu()) * chunk.size
        total_rows += chunk.size
    return total_loss / max(total_rows, 1)


def score_indices(
    *,
    model: nn.Module,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    indices: np.ndarray,
    chunk_size: int,
    device: torch.device,
) -> np.ndarray:
    model.eval()
    output = np.empty(len(indices), dtype=np.float32)
    cursor = 0
    with torch.inference_mode():
        for selected in partition_indices(indices, chunk_size):
            chunk = build_sequence_chunk(
                decision_ns=labels.iloc[selected]["decision_ns"].to_numpy(dtype=np.int64),
                targets=labels.iloc[selected]["target"].to_numpy(dtype=np.float32),
                timelines=timelines,
            )
            values, gather, _ = _chunk_tensors(chunk, device)
            scores = torch.sigmoid(model(values, gather)).detach().cpu().numpy()
            output[cursor : cursor + len(scores)] = scores
            cursor += len(scores)
    return output


def run_causal_logistic_baseline(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    fold: ChronologicalFold,
    chunk_size: int,
    seed: int,
) -> TimestampMetrics:
    classifier = SGDClassifier(
        loss="log_loss",
        penalty="l2",
        alpha=1e-4,
        average=True,
        random_state=seed,
    )
    train_parts = partition_indices(fold.train_indices, chunk_size)
    for epoch in range(3):
        order = np.random.default_rng(seed + epoch).permutation(len(train_parts))
        for position in order:
            selected = train_parts[int(position)]
            classifier.partial_fit(
                latest_feature_matrix(
                    timelines,
                    labels.iloc[selected]["decision_ns"].to_numpy(dtype=np.int64),
                ),
                labels.iloc[selected]["target"].to_numpy(dtype=np.int8),
                classes=np.asarray([0, 1], dtype=np.int8),
            )
    scores: list[np.ndarray] = []
    for selected in partition_indices(fold.validation_indices, chunk_size):
        features = latest_feature_matrix(
            timelines,
            labels.iloc[selected]["decision_ns"].to_numpy(dtype=np.int64),
        )
        scores.append(classifier.predict_proba(features)[:, 1])
    target = labels.iloc[fold.validation_indices]["target"].to_numpy(dtype=np.int8)
    return timestamp_metrics(target, np.concatenate(scores))


def latest_feature_matrix(
    timelines: Mapping[str, TransformedTimeline],
    decision_ns: np.ndarray,
) -> np.ndarray:
    values: list[np.ndarray] = []
    for timeline in timelines.values():
        latest = np.searchsorted(timeline.available_ns, decision_ns, side="right") - 1
        if np.any(latest < 0):
            raise ValueError(f"{timeline.name} is unavailable for baseline rows")
        values.append(np.asarray(timeline.values[latest], dtype=np.float32))
    return np.ascontiguousarray(np.concatenate(values, axis=1), dtype=np.float32)


def monthly_metrics(
    predictions: pd.DataFrame,
    *,
    threshold: float,
) -> list[dict[str, Any]]:
    frame = predictions.copy()
    frame["month"] = pd.to_datetime(frame["decision_ts"], utc=True).dt.strftime("%Y-%m")
    rows: list[dict[str, Any]] = []
    for month, group in frame.groupby("month", sort=True):
        metrics = timestamp_metrics(
            group["target"].to_numpy(dtype=np.int8),
            group["score"].to_numpy(dtype=np.float64),
            threshold=threshold,
        )
        rows.append({"month": str(month), **asdict(metrics)})
    return rows


def reliability_curve(
    target: np.ndarray,
    scores: np.ndarray,
    *,
    bins: int = 10,
) -> list[dict[str, float | int]]:
    truth = np.asarray(target, dtype=np.float64)
    probability = np.asarray(scores, dtype=np.float64)
    edges = np.linspace(0.0, 1.0, bins + 1)
    bucket = np.minimum(np.digitize(probability, edges[1:-1]), bins - 1)
    rows: list[dict[str, float | int]] = []
    for index in range(bins):
        selected = bucket == index
        rows.append(
            {
                "bin": index,
                "lower": float(edges[index]),
                "upper": float(edges[index + 1]),
                "rows": int(selected.sum()),
                "mean_score": (
                    float(probability[selected].mean()) if selected.any() else float("nan")
                ),
                "positive_rate": (
                    float(truth[selected].mean()) if selected.any() else float("nan")
                ),
            }
        )
    return rows


def block_bootstrap_confidence_intervals(
    predictions: pd.DataFrame,
    *,
    threshold: float,
    iterations: int = 500,
    block_days: int = 7,
    seed: int = 20260715,
) -> dict[str, dict[str, float]]:
    pooled = (
        predictions.groupby("decision_ts", as_index=False)
        .agg(target=("target", "first"), score=("score", "mean"))
        .sort_values("decision_ts")
    )
    timestamps = pd.to_datetime(pooled["decision_ts"], utc=True)
    origin = timestamps.min().floor("D")
    block_ids = ((timestamps - origin) // pd.Timedelta(days=block_days)).to_numpy()
    unique_blocks = np.unique(block_ids)
    grouped = [np.flatnonzero(block_ids == value) for value in unique_blocks]
    rng = np.random.default_rng(seed)
    values = {"precision": [], "recall": [], "f1": [], "pr_auc": []}
    target = pooled["target"].to_numpy(dtype=np.int8)
    scores = pooled["score"].to_numpy(dtype=np.float64)
    for _ in range(iterations):
        chosen = rng.integers(0, len(grouped), size=len(grouped))
        sample = np.concatenate([grouped[index] for index in chosen])
        metrics = timestamp_metrics(target[sample], scores[sample], threshold=threshold)
        for name in values:
            values[name].append(float(getattr(metrics, name)))
    return {
        name: {
            "lower": float(np.quantile(metric_values, 0.025)),
            "median": float(np.quantile(metric_values, 0.5)),
            "upper": float(np.quantile(metric_values, 0.975)),
        }
        for name, metric_values in values.items()
    }


def fold_summary(fold: ChronologicalFold) -> dict[str, Any]:
    return {
        "fold_id": fold.fold_id,
        "train_rows": int(len(fold.train_indices)),
        "validation_rows": int(len(fold.validation_indices)),
        "train_end": fold.train_end.isoformat(),
        "validation_start": fold.validation_start.isoformat(),
        "validation_end": fold.validation_end.isoformat(),
    }


def _folds_from_manifest(
    labels: pd.DataFrame,
    manifest: Mapping[str, Any],
    config: TrainingConfig,
) -> list[ChronologicalFold]:
    return build_expanding_folds(
        labels,
        research_start=pd.Timestamp(manifest["splits"]["research_start"]),
        research_end=pd.Timestamp(manifest["splits"]["research_end"]),
        minimum_train_months=config.minimum_train_months,
        validation_months=config.validation_months,
        step_months=config.fold_step_months,
    )


def _fit_control_model(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    train_indices: np.ndarray,
    evaluation_indices: np.ndarray,
    config: TrainingConfig,
    device: torch.device,
    epochs: int,
    seed: int,
    shuffle_targets: bool,
    label: str,
) -> tuple[np.ndarray, list[dict[str, float | int]]]:
    set_deterministic_seed(seed)
    model = create_model(timelines, config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=max(config.learning_rate, 1e-3),
        weight_decay=0.0 if not shuffle_targets else config.weight_decay,
    )
    control_labels = labels.copy()
    if shuffle_targets:
        shuffled = control_labels.iloc[train_indices]["target"].to_numpy(copy=True)
        np.random.default_rng(seed).shuffle(shuffled)
        control_labels.loc[control_labels.index[train_indices], "target"] = shuffled
    chunks = partition_indices(train_indices, min(config.chunk_size, len(train_indices)))
    history: list[dict[str, float | int]] = []
    for epoch in range(1, epochs + 1):
        loss = train_one_epoch(
            model=model,
            optimizer=optimizer,
            labels=control_labels,
            timelines=timelines,
            chunks=chunks,
            device=device,
            gradient_clip=config.gradient_clip,
            seed=seed + epoch,
        )
        if epoch == 1 or epoch % 5 == 0 or epoch == epochs:
            scores = score_indices(
                model=model,
                labels=labels,
                timelines=timelines,
                indices=evaluation_indices,
                chunk_size=config.chunk_size,
                device=device,
            )
            target = labels.iloc[evaluation_indices]["target"].to_numpy(dtype=np.int8)
            metrics = timestamp_metrics(target, scores)
            history.append(
                {
                    "epoch": epoch,
                    "loss": float(loss),
                    "pr_auc": metrics.pr_auc,
                    "f1": metrics.f1,
                }
            )
            print(
                f"[control:{label} epoch={epoch}/{epochs}] loss={loss:.6f} "
                f"ap={metrics.pr_auc:.6f} f1={metrics.f1:.4f}",
                flush=True,
            )
    return (
        score_indices(
            model=model,
            labels=labels,
            timelines=timelines,
            indices=evaluation_indices,
            chunk_size=config.chunk_size,
            device=device,
        ),
        history,
    )


def _contiguous_mixed_control_indices(
    indices: np.ndarray,
    target: np.ndarray,
    rows: int,
    *,
    minimum_class_fraction: float = 0.20,
) -> np.ndarray:
    if len(indices) < rows:
        raise ValueError(f"control requested {rows} rows from only {len(indices)}")
    best: tuple[float, np.ndarray] | None = None
    stride = max(1, rows // 8)
    final_start = len(indices) - rows
    starts = list(range(0, final_start + 1, stride))
    if starts[-1] != final_start:
        starts.append(final_start)
    for start in starts:
        selected = np.asarray(indices[start : start + rows], dtype=np.int64)
        prevalence = float(np.asarray(target)[selected].mean())
        class_fraction = min(prevalence, 1.0 - prevalence)
        if class_fraction < minimum_class_fraction:
            continue
        distance = abs(prevalence - 0.5)
        if best is None or distance < best[0]:
            best = (distance, selected)
    if best is None:
        raise ValueError(
            "could not find a contiguous mixed-class overfit window with "
            f"minimum class fraction {minimum_class_fraction}"
        )
    return best[1]


def _chunk_tensors(
    chunk: Any,
    device: torch.device,
) -> tuple[dict[str, torch.Tensor], dict[str, torch.Tensor], torch.Tensor]:
    values = {
        name: torch.from_numpy(branch).unsqueeze(0).to(device)
        for name, branch in chunk.branch_values.items()
    }
    gather = {
        name: torch.from_numpy(indices).to(device)
        for name, indices in chunk.gather_indices.items()
    }
    target = torch.from_numpy(chunk.targets).to(device)
    return values, gather, target


def _model_artifact(
    *,
    model: nn.Module,
    preprocessor: PreprocessorState,
    fold: ChronologicalFold,
    seed: int,
    epoch: int,
    config: TrainingConfig,
    metrics: TimestampMetrics,
) -> dict[str, Any]:
    return {
        "schema_version": MODEL_SCHEMA_VERSION,
        "model_state": model.state_dict(),
        "model_config": _config_mapping(config),
        "model_config_hash": config.model_hash,
        "preprocessor": _preprocessor_mapping(preprocessor),
        "fold": fold_summary(fold),
        "seed": seed,
        "epoch": epoch,
        "validation_metrics": asdict(metrics),
    }


def _preprocessor_mapping(preprocessor: PreprocessorState) -> dict[str, Any]:
    return {
        "schema_version": preprocessor.schema_version,
        "branches": {
            name: {
                "channel_names": list(state.channel_names),
                "center": state.center,
                "scale": state.scale,
                "clip_value": state.clip_value,
                "fitted_through_ns": state.fitted_through_ns,
            }
            for name, state in preprocessor.branches.items()
        },
    }


def preprocessor_from_mapping(value: Mapping[str, Any]) -> PreprocessorState:
    return PreprocessorState(
        schema_version=str(value["schema_version"]),
        branches={
            name: BranchPreprocessorState(
                channel_names=tuple(state["channel_names"]),
                center=np.asarray(state["center"], dtype=np.float32),
                scale=np.asarray(state["scale"], dtype=np.float32),
                clip_value=float(state["clip_value"]),
                fitted_through_ns=int(state["fitted_through_ns"]),
            )
            for name, state in value["branches"].items()
        },
    )


def _validate_checkpoint(
    checkpoint: Mapping[str, Any],
    *,
    fold: ChronologicalFold,
    seed: int,
    config: TrainingConfig,
) -> None:
    if checkpoint.get("schema_version") != RUN_SCHEMA_VERSION:
        raise ValueError("resume checkpoint schema changed")
    if checkpoint.get("fold_id") != fold.fold_id or int(checkpoint.get("seed")) != seed:
        raise ValueError("resume checkpoint belongs to another fold or seed")
    if checkpoint.get("model_config_hash") != config.model_hash:
        raise ValueError("resume checkpoint model configuration changed")


def _require_current_controls(
    output_root: Path,
    *,
    manifest: Mapping[str, Any],
    config: TrainingConfig,
) -> None:
    path = output_root / "controls.json"
    if not path.is_file():
        raise ValueError(f"full training requires passed controls at {path}")
    controls = json.loads(path.read_text())
    if controls.get("status") != "passed":
        raise ValueError("full training is blocked because controls did not pass")
    if controls.get("manifest_hash") != manifest.get("manifest_hash"):
        raise ValueError("prepared manifest changed after controls")
    if controls.get("model_config_hash") != config.model_hash:
        raise ValueError("model configuration changed after controls")


def _write_rationale(
    *,
    output_root: Path,
    manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> None:
    rationale_path = Path(manifest["artifact_root"]) / "prompt" / "supervised_training_rationale.md"
    acceptance = report["acceptance"]
    pooled = report["pooled_oof_metrics"]
    preflight_path = output_root / "preflight.json"
    controls_path = output_root / "controls.json"
    preflight = json.loads(preflight_path.read_text())
    controls = json.loads(controls_path.read_text())
    training_config = report["training_config"]
    baseline_by_fold = {
        row["fold_id"]: row for row in report["causal_logistic_baselines"]
    }

    lines = [
        "# Supervised Timestamp Training Rationale",
        "",
        f"Result: `{report['status']}`",
        f"Prepared manifest hash: `{manifest['manifest_hash']}`",
        f"Target config hash: `{manifest['target_config_hash']}`",
        f"Model config hash: `{report['model_config_hash']}`",
        "Walk-forward inspected: `false`",
        "",
        "## Data Audit",
        "",
        f"Eligible timestamps: `{manifest['labels']['eligible_rows']}`",
        f"Positive timestamps: `{manifest['labels']['positive_rows']}`",
        f"Negative timestamps: `{manifest['labels']['negative_rows']}`",
        f"Positive prevalence: `{manifest['labels']['positive_prevalence']}`",
        f"First eligible decision: `{preflight['labels']['first_decision_ts']}`",
        f"Last eligible decision: `{preflight['labels']['last_decision_ts']}`",
        f"Last outcome horizon end: `{preflight['labels']['last_horizon_end_ts']}`",
        f"Selected datasets: `{json.dumps(manifest['dataset_ids'], sort_keys=True)}`",
        "",
        "| Branch | Rows | Raw channels | Tensor channels | Steps | Lookback days |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, branch in preflight["branches"].items():
        lines.append(
            f"| {name} | {branch['rows']} | {branch['raw_channels']} | "
            f"{branch['tensor_channels_with_masks']} | {branch['steps']} | "
            f"{branch['lookback_days']} |"
        )

    lines.extend(
        [
            "",
            "## Architecture",
            "",
            "The primary model is a multiresolution causal TCN with one neutral binary "
            "opportunity logit. Each prepared branch has an independent causal temporal "
            "encoder; the gathered branch states are fused by a feed-forward head.",
            "",
            f"- Hidden channels: `{training_config['hidden_channels']}`",
            f"- Fusion channels: `{training_config['fusion_channels']}`",
            f"- Kernel size: `{training_config['kernel_size']}`",
            f"- Dropout: `{training_config['dropout']}`",
            f"- Learning rate: `{training_config['learning_rate']}`",
            f"- Weight decay: `{training_config['weight_decay']}`",
            f"- Maximum epochs: `{training_config['max_epochs']}`",
            f"- Early-stopping patience: `{training_config['patience']}`",
            f"- Seeds: `{training_config['seeds']}`",
            "- Loss: `binary_cross_entropy_with_logits` with unit timestamp weights",
            "- Normalization: fold-fitted per-branch/channel median and IQR",
            "",
            "## Controls",
            "",
            f"Controls status: `{controls['status']}`",
            f"Overfit status: `{controls['overfit']['passed']}`; "
            f"PR-AUC `{controls['overfit']['metrics']['pr_auc']}`; "
            f"F1 `{controls['overfit']['metrics']['f1']}`.",
            f"Shuffled-label status: `{controls['shuffled_label']['passed']}`; "
            f"PR-AUC `{controls['shuffled_label']['metrics']['pr_auc']}`; "
            f"validation prevalence `{controls['shuffled_label']['metrics']['prevalence']}`.",
            "",
            "## Chronological Folds",
            "",
            "Every fold is expanding-window, and training rows whose outcome horizon "
            "overlaps validation are purged.",
            "",
            "| Fold | Train rows | Train end | Validation rows | Validation start | "
            "Validation end |",
            "|---|---:|---|---:|---|---|",
        ]
    )
    for fold in preflight["folds"]:
        lines.append(
            f"| {fold['fold_id']} | {fold['train_rows']} | {fold['train_end']} | "
            f"{fold['validation_rows']} | {fold['validation_start']} | "
            f"{fold['validation_end']} |"
        )

    lines.extend(
        [
            "",
            "## Fold Results",
            "",
            "| Fold | Seed | Best epoch | Sequence PR-AUC | Logistic PR-AUC | "
            "Prevalence | Precision | Recall |",
            "|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in report["fold_results"]:
        baseline = baseline_by_fold[row["fold_id"]]
        lines.append(
            f"| {row['fold_id']} | {row['seed']} | {row['best_epoch']} | "
            f"{row['pr_auc']:.6f} | {baseline['pr_auc']:.6f} | "
            f"{row['prevalence']:.6f} | {row['precision']:.6f} | "
            f"{row['recall']:.6f} |"
        )

    lines.extend(
        [
            "",
            "## Pooled Out-Of-Fold Result",
            "",
            f"PR-AUC: `{pooled['pr_auc']}`",
            f"ROC-AUC: `{pooled['roc_auc']}`",
            f"Precision: `{pooled['precision']}`",
            f"Recall: `{pooled['recall']}`",
            f"F1: `{pooled['f1']}`",
            f"Brier score: `{pooled['brier_score']}`",
            f"Calibration error: `{pooled['calibration_error']}`",
            f"Selected threshold: `{pooled['threshold']}`",
            "",
            "## Acceptance",
            "",
            f"Decision: `{'accepted' if acceptance['accepted'] else 'rejected'}`",
            f"Rule: {acceptance['rule']}",
            f"Median sequence PR-AUC: `{acceptance['median_sequence_pr_auc']}`",
            f"Median causal logistic PR-AUC: `{acceptance['median_logistic_pr_auc']}`",
            f"Median prevalence: `{acceptance['median_prevalence']}`",
            f"Prevalence win fraction: `{acceptance['prevalence_win_fraction']}`",
            f"Causal logistic win fraction: `{acceptance['causal_logistic_win_fraction']}`",
            "Always-enter comparison: "
            f"`{json.dumps(acceptance['always_enter_comparison'], sort_keys=True)}`",
            "",
            "## Artifact Hashes",
            "",
        ]
    )
    artifact_paths = [
        ("Prepared manifest", Path(preflight["manifest_path"])),
        ("Preflight", preflight_path),
        ("Controls", controls_path),
        ("Raw OOF predictions", output_root / "oof_predictions.parquet"),
        ("Ensemble OOF predictions", output_root / "oof_ensemble_predictions.parquet"),
        ("Training report", output_root / "training_report.json"),
    ]
    for label, path in artifact_paths:
        if path.is_file():
            lines.append(f"- {label}: `{path}`; SHA-256 `{_sha256(path)}`")
    for artifact in report.get("final_refit", {}).get("artifacts", []):
        lines.append(
            f"- Final model seed {artifact['seed']}: `{artifact['path']}`; "
            f"SHA-256 `{artifact['sha256']}`"
        )
    lines.extend(["", f"Complete machine-readable report: `{output_root / 'training_report.json'}`", ""])
    text = "\n".join(lines)
    rationale_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text(rationale_path, text)


def _config_mapping(config: TrainingConfig) -> dict[str, Any]:
    value = asdict(config)
    value["seeds"] = list(config.seeds)
    return value


def _mapping_hash(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _atomic_write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(value)
    temporary.replace(path)


def _atomic_write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_parquet(temporary, index=False)
    temporary.replace(path)


def _atomic_torch_save(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    torch.save(dict(value), temporary)
    temporary.replace(path)


def _empty_device_cache(device: torch.device) -> None:
    if device.type == "mps":
        torch.mps.empty_cache()
    elif device.type == "cuda":
        torch.cuda.empty_cache()


def _duration(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    hours, remainder = divmod(total, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Train the Motis supervised timestamp multiresolution TCN.",
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--mode", choices=("preflight", "controls", "train"), required=True)
    parser.add_argument("--device", choices=("auto", "cpu", "mps", "cuda"), default="auto")
    parser.add_argument("--hidden-channels", type=int, default=24)
    parser.add_argument("--fusion-channels", type=int, default=96)
    parser.add_argument("--kernel-size", type=int, default=2)
    parser.add_argument("--dropout", type=float, default=0.10)
    parser.add_argument("--learning-rate", type=float, default=3e-4)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--gradient-clip", type=float, default=1.0)
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--epochs", type=int, default=50)
    parser.add_argument("--patience", type=int, default=8)
    parser.add_argument("--seeds", type=int, nargs="+", default=[17, 29, 43])
    parser.add_argument("--control-overfit-epochs", type=int, default=80)
    parser.add_argument("--control-shuffle-epochs", type=int, default=4)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    artifact_root = Path(manifest["artifact_root"])
    output_root = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else artifact_root / "training" / "supervised_tcn_v1"
    )
    config = TrainingConfig(
        hidden_channels=args.hidden_channels,
        fusion_channels=args.fusion_channels,
        kernel_size=args.kernel_size,
        dropout=args.dropout,
        learning_rate=args.learning_rate,
        weight_decay=args.weight_decay,
        gradient_clip=args.gradient_clip,
        chunk_size=args.chunk_size,
        max_epochs=args.epochs,
        patience=args.patience,
        seeds=tuple(args.seeds),
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
        device = resolve_device(args.device)
        if args.mode == "controls":
            run_controls(
                manifest_path=manifest_path,
                output_root=output_root,
                config=config,
                device=device,
                overfit_epochs=args.control_overfit_epochs,
                shuffle_epochs=args.control_shuffle_epochs,
            )
            return 0
        run_full_training(
            manifest_path=manifest_path,
            output_root=output_root,
            config=config,
            device=device,
        )
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
