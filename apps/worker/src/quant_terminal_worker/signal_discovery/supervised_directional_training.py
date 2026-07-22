from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
import hashlib
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import SGDClassifier
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    confusion_matrix,
    f1_score,
    log_loss,
)
import torch
from torch import nn

from quant_terminal_worker.signal_discovery.supervised_sequence_model import (
    MultiResolutionCausalTCN,
    partition_indices,
)
from quant_terminal_worker.signal_discovery.supervised_sequence_training import (
    TrainingConfig,
    _atomic_torch_save,
    _atomic_write_json,
    _atomic_write_parquet,
    _duration,
    _empty_device_cache,
    _preprocessor_mapping,
    _sha256,
    build_expanding_folds,
    fold_summary,
    latest_feature_matrix,
    resolve_device,
    run_preflight as run_binary_preflight,
    set_deterministic_seed,
)
from quant_terminal_worker.signal_discovery.supervised_training_data import (
    PreprocessorState,
    TransformedTimeline,
    fit_preprocessor,
    load_prepared_supervised_training_data,
    transform_timelines,
)


CLASS_NAMES = ("LONG", "SHORT", "NEUTRAL")
CLASS_TO_INDEX = {name: index for index, name in enumerate(CLASS_NAMES)}
RUN_SCHEMA_VERSION = "motis_supervised_directional_training_run.v1"
MODEL_SCHEMA_VERSION = "motis_supervised_directional_tcn.v1"
CONTROL_SCHEMA_VERSION = "motis_supervised_directional_controls.v1"


@dataclass(frozen=True)
class DirectionalConfig:
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
    seeds: tuple[int, ...] = (17, 29, 43)
    minimum_action_precision: float = 0.55
    minimum_action_recall: float = 0.15
    minimum_entry_coverage: float = 0.05
    minimum_side_coverage: float = 0.01
    minimum_long_precision: float = 0.50
    minimum_short_precision: float = 0.50
    minimum_long_recall: float = 0.05
    minimum_short_recall: float = 0.05
    maximum_neutral_false_positive_rate: float = 0.10

    @property
    def model_hash(self) -> str:
        return _hash_mapping(
            {
                "objective": "exact_long_short_neutral_v1",
                "classes": list(CLASS_NAMES),
                "hidden_channels": self.hidden_channels,
                "fusion_channels": self.fusion_channels,
                "kernel_size": self.kernel_size,
                "dropout": self.dropout,
            }
        )


@dataclass(frozen=True)
class RankingMetrics:
    rows: int
    macro_pr_auc: float
    multiclass_log_loss: float
    argmax_accuracy: float
    argmax_macro_f1: float
    per_class_pr_auc: dict[str, float]


@dataclass(frozen=True)
class ActionMetrics:
    rows: int
    entries: int
    entry_coverage: float
    correct_entries: int
    action_precision: float
    action_recall: float
    wrong_side_entries: int
    wrong_side_rate: float
    neutral_false_entries: int
    neutral_false_entry_rate: float
    neutral_false_positive_rate: float
    long_entries: int
    short_entries: int
    long_coverage: float
    short_coverage: float
    long_precision: float
    short_precision: float
    long_recall: float
    short_recall: float
    neutral_recall: float
    exact_accuracy: float
    macro_f1: float
    confusion_matrix: list[list[int]]


@dataclass(frozen=True)
class DirectionalPolicy:
    probability_threshold: float
    margin_threshold: float


@dataclass(frozen=True)
class PreparedSequenceChunk:
    size: int
    branch_ranges: dict[str, tuple[int, int]]
    gather_indices: dict[str, torch.Tensor]
    targets: torch.Tensor | None = None


@dataclass(frozen=True)
class ResidentTimelines:
    values: dict[str, torch.Tensor]
    device: torch.device
    allocated_bytes: int

    @classmethod
    def from_timelines(
        cls,
        timelines: Mapping[str, TransformedTimeline],
        device: torch.device,
    ) -> ResidentTimelines:
        values: dict[str, torch.Tensor] = {}
        allocated_bytes = 0
        for name, timeline in timelines.items():
            branch = np.ascontiguousarray(timeline.values.T, dtype=np.float32)
            allocated_bytes += int(branch.nbytes)
            values[name] = torch.from_numpy(branch).to(device)
        print(
            f"[directional-input-cache] device={device} branches={len(values)} "
            f"size_mib={allocated_bytes / (1024 ** 2):.1f}",
            flush=True,
        )
        return cls(values=values, device=device, allocated_bytes=allocated_bytes)

    def inputs(
        self,
        chunk: PreparedSequenceChunk,
    ) -> tuple[dict[str, torch.Tensor], dict[str, torch.Tensor]]:
        return (
            {
                name: self.values[name][:, first:stop].contiguous().unsqueeze(0)
                for name, (first, stop) in chunk.branch_ranges.items()
            },
            chunk.gather_indices,
        )


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def attach_directional_target(labels: pd.DataFrame) -> pd.DataFrame:
    frame = labels.copy()
    unknown = sorted(set(frame["raw_label"].unique()) - set(CLASS_NAMES))
    if unknown:
        raise ValueError(f"unsupported directional labels: {unknown}")
    frame["class_target"] = frame["raw_label"].map(CLASS_TO_INDEX).astype(np.int64)
    return frame


def ranking_metrics(target: np.ndarray, probabilities: np.ndarray) -> RankingMetrics:
    truth = np.asarray(target, dtype=np.int64)
    scores = np.asarray(probabilities, dtype=np.float64)
    if scores.shape != (len(truth), len(CLASS_NAMES)):
        raise ValueError("directional probabilities must be rows by three classes")
    scores = scores / np.clip(scores.sum(axis=1, keepdims=True), 1e-12, None)
    one_hot = np.eye(len(CLASS_NAMES), dtype=np.int8)[truth]
    per_class = {
        name: float(average_precision_score(one_hot[:, index], scores[:, index]))
        for index, name in enumerate(CLASS_NAMES)
    }
    predicted = scores.argmax(axis=1)
    return RankingMetrics(
        rows=int(len(truth)),
        macro_pr_auc=float(np.mean(list(per_class.values()))),
        multiclass_log_loss=float(log_loss(truth, scores, labels=[0, 1, 2])),
        argmax_accuracy=float(accuracy_score(truth, predicted)),
        argmax_macro_f1=float(f1_score(truth, predicted, average="macro", zero_division=0)),
        per_class_pr_auc=per_class,
    )


def apply_policy(probabilities: np.ndarray, policy: DirectionalPolicy) -> np.ndarray:
    scores = np.asarray(probabilities, dtype=np.float64)
    directional = scores[:, :2]
    selected_side = directional.argmax(axis=1)
    selected_probability = directional[np.arange(len(scores)), selected_side]
    other_side = directional[np.arange(len(scores)), 1 - selected_side]
    competitor = np.maximum(other_side, scores[:, CLASS_TO_INDEX["NEUTRAL"]])
    enter = (selected_probability >= policy.probability_threshold) & (
        selected_probability - competitor >= policy.margin_threshold
    )
    prediction = np.full(len(scores), CLASS_TO_INDEX["NEUTRAL"], dtype=np.int64)
    prediction[enter] = selected_side[enter]
    return prediction


def action_metrics(target: np.ndarray, prediction: np.ndarray) -> ActionMetrics:
    truth = np.asarray(target, dtype=np.int64)
    predicted = np.asarray(prediction, dtype=np.int64)
    neutral = CLASS_TO_INDEX["NEUTRAL"]
    entry = predicted != neutral
    true_opportunity = truth != neutral
    correct = entry & (predicted == truth)
    wrong_side = entry & true_opportunity & (predicted != truth)
    false_entry = entry & (truth == neutral)
    entries = int(entry.sum())
    correct_entries = int(correct.sum())
    long_entry = predicted == CLASS_TO_INDEX["LONG"]
    short_entry = predicted == CLASS_TO_INDEX["SHORT"]
    long_entries = int(long_entry.sum())
    short_entries = int(short_entry.sum())
    true_long = truth == CLASS_TO_INDEX["LONG"]
    true_short = truth == CLASS_TO_INDEX["SHORT"]
    true_neutral = truth == neutral
    matrix = confusion_matrix(truth, predicted, labels=[0, 1, 2])
    return ActionMetrics(
        rows=int(len(truth)),
        entries=entries,
        entry_coverage=float(entries / max(len(truth), 1)),
        correct_entries=correct_entries,
        action_precision=float(correct_entries / max(entries, 1)),
        action_recall=float(correct_entries / max(int(true_opportunity.sum()), 1)),
        wrong_side_entries=int(wrong_side.sum()),
        wrong_side_rate=float(wrong_side.sum() / max(entries, 1)),
        neutral_false_entries=int(false_entry.sum()),
        neutral_false_entry_rate=float(false_entry.sum() / max(entries, 1)),
        neutral_false_positive_rate=float(
            false_entry.sum() / max(int(true_neutral.sum()), 1)
        ),
        long_entries=long_entries,
        short_entries=short_entries,
        long_coverage=float(long_entries / max(len(truth), 1)),
        short_coverage=float(short_entries / max(len(truth), 1)),
        long_precision=float(((truth == 0) & long_entry).sum() / max(long_entries, 1)),
        short_precision=float(((truth == 1) & short_entry).sum() / max(short_entries, 1)),
        long_recall=float((true_long & long_entry).sum() / max(int(true_long.sum()), 1)),
        short_recall=float((true_short & short_entry).sum() / max(int(true_short.sum()), 1)),
        neutral_recall=float(((truth == neutral) & (predicted == neutral)).sum() / max(int((truth == neutral).sum()), 1)),
        exact_accuracy=float(accuracy_score(truth, predicted)),
        macro_f1=float(f1_score(truth, predicted, average="macro", zero_division=0)),
        confusion_matrix=matrix.astype(int).tolist(),
    )


def policy_meets_requirements(metrics: ActionMetrics, config: Any) -> bool:
    return bool(
        metrics.action_precision >= config.minimum_action_precision
        and metrics.action_recall >= config.minimum_action_recall
        and metrics.entry_coverage >= config.minimum_entry_coverage
        and metrics.long_coverage >= config.minimum_side_coverage
        and metrics.short_coverage >= config.minimum_side_coverage
        and metrics.long_precision >= config.minimum_long_precision
        and metrics.short_precision >= config.minimum_short_precision
        and metrics.long_recall >= config.minimum_long_recall
        and metrics.short_recall >= config.minimum_short_recall
        and metrics.neutral_false_positive_rate
        <= config.maximum_neutral_false_positive_rate
    )


def select_operating_policy(
    target: np.ndarray,
    probabilities: np.ndarray,
    *,
    config: DirectionalConfig,
) -> tuple[DirectionalPolicy | None, list[dict[str, Any]]]:
    frontier: list[dict[str, Any]] = []
    valid: list[tuple[DirectionalPolicy, ActionMetrics]] = []
    for probability_threshold in np.linspace(0.34, 0.90, 29):
        for margin_threshold in np.linspace(0.0, 0.40, 21):
            policy = DirectionalPolicy(
                probability_threshold=round(float(probability_threshold), 6),
                margin_threshold=round(float(margin_threshold), 6),
            )
            metrics = action_metrics(target, apply_policy(probabilities, policy))
            row = {"policy": asdict(policy), "metrics": asdict(metrics)}
            frontier.append(row)
            if policy_meets_requirements(metrics, config):
                valid.append((policy, metrics))
    if not valid:
        return None, frontier
    selected, _ = max(
        valid,
        key=lambda item: (
            item[1].action_recall,
            item[1].entry_coverage,
            item[1].action_precision,
        ),
    )
    return selected, frontier


def create_model(
    timelines: Mapping[str, TransformedTimeline],
    config: DirectionalConfig,
) -> MultiResolutionCausalTCN:
    return MultiResolutionCausalTCN(
        branch_input_channels={name: value.values.shape[1] for name, value in timelines.items()},
        branch_steps={name: value.spec.steps for name, value in timelines.items()},
        hidden_channels=config.hidden_channels,
        fusion_channels=config.fusion_channels,
        kernel_size=config.kernel_size,
        dropout=config.dropout,
        output_classes=len(CLASS_NAMES),
    )


def prepare_sequence_chunks(
    *,
    decision_ns: np.ndarray,
    timelines: Mapping[str, TransformedTimeline],
    indices: np.ndarray,
    chunk_size: int,
    device: torch.device,
    targets: np.ndarray | None = None,
) -> list[PreparedSequenceChunk]:
    decisions = np.asarray(decision_ns, dtype=np.int64)
    target_values = None if targets is None else np.asarray(targets, dtype=np.int64)
    if target_values is not None and len(target_values) != len(decisions):
        raise ValueError("decision timestamps and targets must have equal length")
    prepared: list[PreparedSequenceChunk] = []
    for selected in partition_indices(indices, chunk_size):
        selected_decisions = decisions[selected]
        if np.any(np.diff(selected_decisions) < 0):
            raise ValueError("sequence chunk decisions must be chronological")
        branch_ranges: dict[str, tuple[int, int]] = {}
        gather_indices: dict[str, torch.Tensor] = {}
        for name, timeline in timelines.items():
            latest = np.searchsorted(
                timeline.available_ns, selected_decisions, side="right"
            ) - 1
            first = int(latest[0] - timeline.spec.steps + 1)
            stop = int(latest[-1] + 1)
            if first < 0:
                raise ValueError(f"{name} lacks complete history for chunk")
            branch_ranges[name] = (first, stop)
            gather_indices[name] = torch.from_numpy(
                np.ascontiguousarray(latest - first, dtype=np.int64)
            ).to(device)
        chunk_targets = None
        if target_values is not None:
            chunk_targets = torch.from_numpy(
                np.ascontiguousarray(target_values[selected], dtype=np.int64)
            ).to(device)
        prepared.append(
            PreparedSequenceChunk(
                size=int(len(selected)),
                branch_ranges=branch_ranges,
                gather_indices=gather_indices,
                targets=chunk_targets,
            )
        )
    return prepared


def run_preflight(
    *,
    manifest_path: Path,
    output_root: Path,
    config: DirectionalConfig,
) -> dict[str, Any]:
    base = run_binary_preflight(
        manifest_path=manifest_path,
        output_root=output_root,
        config=_binary_config(config),
    )
    labels, _, manifest = load_prepared_supervised_training_data(manifest_path)
    labels = attach_directional_target(labels)
    counts = labels["raw_label"].value_counts().to_dict()
    failures = list(base.get("failures") or [])
    for name in CLASS_NAMES:
        if int(counts.get(name, 0)) == 0:
            failures.append(f"directional training requires {name} labels")
    report = {
        **base,
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "passed" if not failures else "failed",
        "objective": "exact_long_short_neutral_v1",
        "class_order": list(CLASS_NAMES),
        "class_counts": {name: int(counts.get(name, 0)) for name in CLASS_NAMES},
        "model_config_hash": config.model_hash,
        "directional_config": _config_mapping(config),
        "failures": failures,
        "source_manifest_hash": manifest["manifest_hash"],
    }
    _atomic_write_json(output_root / "preflight.json", report)
    if failures:
        raise ValueError("directional preflight failed: " + "; ".join(failures))
    return report


def run_controls(
    *,
    manifest_path: Path,
    output_root: Path,
    config: DirectionalConfig,
    device: torch.device,
    overfit_epochs: int = 100,
    shuffle_epochs: int = 4,
) -> dict[str, Any]:
    preflight = run_preflight(
        manifest_path=manifest_path,
        output_root=output_root,
        config=config,
    )
    labels, raw_timelines, manifest = load_prepared_supervised_training_data(manifest_path)
    labels = attach_directional_target(labels)
    folds = _folds(labels, manifest, config)
    fold = folds[0]
    overfit_indices = contiguous_three_class_window(
        fold.train_indices,
        labels["class_target"].to_numpy(dtype=np.int64),
        rows=384,
    )
    train_end = pd.Timestamp(labels.iloc[overfit_indices[-1]]["decision_ts"])
    preprocessor = fit_preprocessor(
        raw_timelines,
        train_end=train_end,
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    timelines = transform_timelines(raw_timelines, preprocessor)
    overfit_probabilities, overfit_history = fit_control_model(
        labels=labels,
        timelines=timelines,
        train_indices=overfit_indices,
        evaluation_indices=overfit_indices,
        config=config,
        device=device,
        epochs=overfit_epochs,
        seed=config.seeds[0],
        shuffle_targets=False,
        label="overfit-3class",
    )
    overfit_ranking = ranking_metrics(
        labels.iloc[overfit_indices]["class_target"].to_numpy(dtype=np.int64),
        overfit_probabilities,
    )
    overfit_prediction = overfit_probabilities.argmax(axis=1)
    overfit_action = action_metrics(
        labels.iloc[overfit_indices]["class_target"].to_numpy(dtype=np.int64),
        overfit_prediction,
    )
    overfit_passed = (
        overfit_ranking.macro_pr_auc >= 0.99
        and overfit_ranking.argmax_accuracy >= 0.98
        and all(overfit_prediction.tolist().count(index) > 0 for index in range(3))
    )

    shuffle_train = fold.train_indices[-min(4096, len(fold.train_indices)) :]
    shuffle_validation = fold.validation_indices[:2048]
    shuffle_end = pd.Timestamp(labels.iloc[shuffle_train[-1]]["decision_ts"])
    shuffle_preprocessor = fit_preprocessor(
        raw_timelines,
        train_end=shuffle_end,
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    shuffle_timelines = transform_timelines(raw_timelines, shuffle_preprocessor)
    shuffle_probabilities, shuffle_history = fit_control_model(
        labels=labels,
        timelines=shuffle_timelines,
        train_indices=shuffle_train,
        evaluation_indices=shuffle_validation,
        config=config,
        device=device,
        epochs=shuffle_epochs,
        seed=config.seeds[0] + 10_000,
        shuffle_targets=True,
        label="shuffled-3class",
    )
    shuffle_ranking = ranking_metrics(
        labels.iloc[shuffle_validation]["class_target"].to_numpy(dtype=np.int64),
        shuffle_probabilities,
    )
    shuffle_passed = shuffle_ranking.macro_pr_auc <= (1.0 / 3.0) + 0.06
    passed = bool(preflight["status"] == "passed" and overfit_passed and shuffle_passed)
    report = {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "status": "passed" if passed else "failed",
        "created_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "target_config_hash": manifest["target_config_hash"],
        "model_config_hash": config.model_hash,
        "objective": "exact_long_short_neutral_v1",
        "device": str(device),
        "overfit": {
            "passed": bool(overfit_passed),
            "rows": int(len(overfit_indices)),
            "class_counts": {
                name: int((labels.iloc[overfit_indices]["class_target"] == index).sum())
                for index, name in enumerate(CLASS_NAMES)
            },
            "epochs": overfit_epochs,
            "ranking": asdict(overfit_ranking),
            "action": asdict(overfit_action),
            "history": overfit_history,
        },
        "shuffled_label": {
            "passed": bool(shuffle_passed),
            "train_rows": int(len(shuffle_train)),
            "validation_rows": int(len(shuffle_validation)),
            "epochs": shuffle_epochs,
            "ranking": asdict(shuffle_ranking),
            "history": shuffle_history,
            "maximum_macro_pr_auc": (1.0 / 3.0) + 0.06,
        },
    }
    _atomic_write_json(output_root / "controls.json", report)
    print(f"[directional-controls] status={report['status']}", flush=True)
    if not passed:
        raise ValueError("directional controls failed; full training is blocked")
    return report


def run_full_training(
    *,
    manifest_path: Path,
    output_root: Path,
    config: DirectionalConfig,
    device: torch.device,
) -> dict[str, Any]:
    labels, raw_timelines, manifest = load_prepared_supervised_training_data(manifest_path)
    labels = attach_directional_target(labels)
    require_controls(output_root, manifest=manifest, config=config)
    folds = _folds(labels, manifest, config)
    status = {
        "schema_version": RUN_SCHEMA_VERSION,
        "status": "running",
        "started_at": utc_now(),
        "manifest_hash": manifest["manifest_hash"],
        "model_config_hash": config.model_hash,
        "device": str(device),
        "fold_count": len(folds),
        "seeds": list(config.seeds),
    }
    _atomic_write_json(output_root / "status.json", status)
    fold_results: list[dict[str, Any]] = []
    prediction_frames: list[pd.DataFrame] = []
    baseline_frames: list[pd.DataFrame] = []
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
            baseline_probabilities = causal_logistic_probabilities(
                labels=labels,
                timelines=timelines,
                fold=fold,
                chunk_size=config.chunk_size,
                seed=config.seeds[0],
            )
            validation = labels.iloc[fold.validation_indices]
            baseline_frames.append(
                probabilities_frame(
                    validation,
                    baseline_probabilities,
                    fold_id=fold.fold_id,
                    seed=-1,
                )
            )
            resident_timelines = ResidentTimelines.from_timelines(timelines, device)
            target_values = labels["class_target"].to_numpy(dtype=np.int64)
            decision_ns = labels["decision_ns"].to_numpy(dtype=np.int64)
            train_chunks = prepare_sequence_chunks(
                decision_ns=decision_ns,
                timelines=timelines,
                indices=fold.train_indices,
                chunk_size=config.chunk_size,
                device=device,
                targets=target_values,
            )
            validation_chunks = prepare_sequence_chunks(
                decision_ns=decision_ns,
                timelines=timelines,
                indices=fold.validation_indices,
                chunk_size=config.chunk_size,
                device=device,
            )
            for seed in config.seeds:
                run_root = output_root / fold.fold_id / f"seed_{seed}"
                result = train_fold(
                    labels=labels,
                    timelines=timelines,
                    resident_timelines=resident_timelines,
                    train_chunks=train_chunks,
                    validation_chunks=validation_chunks,
                    preprocessor=preprocessor,
                    fold=fold,
                    seed=seed,
                    config=config,
                    device=device,
                    run_root=run_root,
                    fold_number=fold_number,
                    fold_count=len(folds),
                )
                probabilities = result.pop("validation_probabilities")
                metrics = ranking_metrics(
                    validation["class_target"].to_numpy(dtype=np.int64),
                    probabilities,
                )
                fold_results.append(
                    {"fold_id": fold.fold_id, "seed": seed, **result, **asdict(metrics)}
                )
                prediction_frames.append(
                    probabilities_frame(
                        validation,
                        probabilities,
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
                    f"[directional-progress] {completed_runs}/{total_runs} "
                    f"elapsed={_duration(elapsed)} eta={_duration(eta)}",
                    flush=True,
                )
            del validation_chunks, train_chunks, resident_timelines, timelines
            _empty_device_cache(device)

        raw_predictions = pd.concat(prediction_frames, ignore_index=True)
        _atomic_write_parquet(output_root / "oof_predictions.parquet", raw_predictions)
        ensemble = ensemble_probabilities(raw_predictions)
        _atomic_write_parquet(output_root / "oof_ensemble_predictions.parquet", ensemble)
        baseline = pd.concat(baseline_frames, ignore_index=True)
        _atomic_write_parquet(output_root / "oof_logistic_predictions.parquet", baseline)
        target = ensemble["class_target"].to_numpy(dtype=np.int64)
        probabilities = probability_matrix(ensemble)
        pooled_ranking = ranking_metrics(target, probabilities)
        policy, frontier = select_operating_policy(target, probabilities, config=config)
        baseline_target = baseline["class_target"].to_numpy(dtype=np.int64)
        baseline_policy, baseline_frontier = select_operating_policy(
            baseline_target,
            probability_matrix(baseline),
            config=config,
        )
        selected_action = (
            action_metrics(target, apply_policy(probabilities, policy))
            if policy is not None
            else None
        )
        baseline_action = (
            action_metrics(
                baseline_target,
                apply_policy(probability_matrix(baseline), baseline_policy),
            )
            if baseline_policy is not None
            else None
        )
        recurrence = policy_recurrence(
            raw_predictions,
            policy=policy,
            config=config,
        ) if policy is not None else {"passing_fraction": 0.0, "rows": []}
        accepted = bool(
            policy is not None
            and selected_action is not None
            and recurrence["passing_fraction"] >= 2.0 / 3.0
            and (
                baseline_action is None
                or selected_action.action_recall > baseline_action.action_recall
            )
        )
        final_refit = (
            final_refit_ensemble(
                labels=labels,
                raw_timelines=raw_timelines,
                manifest=manifest,
                fold_results=fold_results,
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
            "objective": "exact_long_short_neutral_v1",
            "class_order": list(CLASS_NAMES),
            "manifest_hash": manifest["manifest_hash"],
            "target_config_hash": manifest["target_config_hash"],
            "model_config_hash": config.model_hash,
            "directional_config": _config_mapping(config),
            "fold_results": fold_results,
            "pooled_ranking": asdict(pooled_ranking),
            "selected_policy": asdict(policy) if policy is not None else None,
            "selected_action_metrics": asdict(selected_action) if selected_action else None,
            "policy_frontier": frontier,
            "causal_logistic_policy": asdict(baseline_policy) if baseline_policy else None,
            "causal_logistic_action_metrics": asdict(baseline_action) if baseline_action else None,
            "causal_logistic_frontier": baseline_frontier,
            "recurrence": recurrence,
            "monthly_metrics": directional_monthly_metrics(ensemble, policy=policy),
            "trivial_baselines": trivial_baselines(target),
            "acceptance": {
                "accepted": accepted,
                "requires_policy": True,
                "minimum_action_precision": config.minimum_action_precision,
                "minimum_entry_coverage": config.minimum_entry_coverage,
                "minimum_side_coverage": config.minimum_side_coverage,
                "minimum_fold_seed_passing_fraction": 2.0 / 3.0,
                "requires_action_recall_above_causal_logistic": True,
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
            f"[directional-complete] result={report['status']} "
            f"macro_ap={pooled_ranking.macro_pr_auc:.6f} "
            f"report={output_root / 'training_report.json'}",
            flush=True,
        )
        return report
    except KeyboardInterrupt:
        status.update({"status": "interrupted", "interrupted_at": utc_now()})
        _atomic_write_json(output_root / "status.json", status)
        print("\n[directional-interrupted] checkpoints preserved; rerun the same command", flush=True)
        raise


def train_fold(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    resident_timelines: ResidentTimelines,
    train_chunks: Sequence[PreparedSequenceChunk],
    validation_chunks: Sequence[PreparedSequenceChunk],
    preprocessor: PreprocessorState,
    fold: Any,
    seed: int,
    config: DirectionalConfig,
    device: torch.device,
    run_root: Path,
    fold_number: int,
    fold_count: int,
) -> dict[str, Any]:
    run_root.mkdir(parents=True, exist_ok=True)
    set_deterministic_seed(seed)
    model = create_model(timelines, config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay
    )
    last_path = run_root / "last.pt"
    best_path = run_root / "best.pt"
    start_epoch = 1
    best_ap = -math.inf
    best_epoch = 0
    stale = 0
    history: list[dict[str, Any]] = []
    if last_path.is_file():
        checkpoint = torch.load(last_path, map_location=device, weights_only=False)
        validate_checkpoint(checkpoint, fold_id=fold.fold_id, seed=seed, config=config)
        model.load_state_dict(checkpoint["model_state"])
        optimizer.load_state_dict(checkpoint["optimizer_state"])
        start_epoch = int(checkpoint["epoch"]) + 1
        best_ap = float(checkpoint["best_macro_pr_auc"])
        best_epoch = int(checkpoint["best_epoch"])
        stale = int(checkpoint["stale_epochs"])
        history = list(checkpoint.get("history") or [])
    epoch_times: list[float] = []
    for epoch in range(start_epoch, config.max_epochs + 1):
        started = time.monotonic()
        loss = train_epoch(
            model=model,
            optimizer=optimizer,
            resident_timelines=resident_timelines,
            chunks=train_chunks,
            gradient_clip=config.gradient_clip,
            seed=seed + epoch,
        )
        probabilities = score_chunks(
            model=model,
            resident_timelines=resident_timelines,
            chunks=validation_chunks,
        )
        metrics = ranking_metrics(
            labels.iloc[fold.validation_indices]["class_target"].to_numpy(dtype=np.int64),
            probabilities,
        )
        if metrics.macro_pr_auc > best_ap + 1e-6:
            best_ap = metrics.macro_pr_auc
            best_epoch = epoch
            stale = 0
            _atomic_torch_save(
                best_path,
                model_artifact(
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
            stale += 1
        elapsed = time.monotonic() - started
        epoch_times.append(elapsed)
        history.append(
            {
                "epoch": epoch,
                "train_loss": loss,
                "validation_macro_pr_auc": metrics.macro_pr_auc,
                "validation_accuracy": metrics.argmax_accuracy,
                "best_macro_pr_auc": best_ap,
                "best_epoch": best_epoch,
                "seconds": round(elapsed, 3),
            }
        )
        _atomic_torch_save(
            last_path,
            {
                "schema_version": RUN_SCHEMA_VERSION,
                "fold_id": fold.fold_id,
                "seed": seed,
                "model_config_hash": config.model_hash,
                "epoch": epoch,
                "best_macro_pr_auc": best_ap,
                "best_epoch": best_epoch,
                "stale_epochs": stale,
                "model_state": model.state_dict(),
                "optimizer_state": optimizer.state_dict(),
                "history": history,
            },
        )
        eta = float(np.mean(epoch_times[-3:])) * min(
            config.max_epochs - epoch, max(config.patience - stale, 0)
        )
        print(
            f"[directional fold {fold_number}/{fold_count} seed={seed} "
            f"epoch={epoch}/{config.max_epochs}] loss={loss:.6f} "
            f"val_map={metrics.macro_pr_auc:.6f} accuracy={metrics.argmax_accuracy:.4f} "
            f"best={best_ap:.6f}@{best_epoch} stale={stale}/{config.patience} "
            f"epoch_time={_duration(elapsed)} eta_run={_duration(eta)}",
            flush=True,
        )
        if stale >= config.patience:
            break
    best = torch.load(best_path, map_location=device, weights_only=False)
    model.load_state_dict(best["model_state"])
    probabilities = score_chunks(
        model=model,
        resident_timelines=resident_timelines,
        chunks=validation_chunks,
    )
    return {
        "best_epoch": best_epoch,
        "best_macro_pr_auc": best_ap,
        "epochs_completed": int(history[-1]["epoch"]),
        "checkpoint_path": str(best_path),
        "validation_probabilities": probabilities,
    }


def train_epoch(
    *,
    model: nn.Module,
    optimizer: torch.optim.Optimizer,
    resident_timelines: ResidentTimelines,
    chunks: Sequence[PreparedSequenceChunk],
    gradient_clip: float,
    seed: int,
) -> float:
    model.train()
    order = np.random.default_rng(seed).permutation(len(chunks))
    weighted_losses: list[torch.Tensor] = []
    total_rows = 0
    for position in order:
        chunk = chunks[int(position)]
        if chunk.targets is None:
            raise ValueError("training chunks require resident targets")
        values, gather = resident_timelines.inputs(chunk)
        optimizer.zero_grad(set_to_none=True)
        logits = model(values, gather)
        loss = nn.functional.cross_entropy(logits, chunk.targets)
        loss.backward()
        nn.utils.clip_grad_norm_(model.parameters(), gradient_clip)
        optimizer.step()
        weighted_losses.append(loss.detach() * chunk.size)
        total_rows += chunk.size
    if not weighted_losses:
        return 0.0
    total_loss = torch.stack(weighted_losses).sum()
    return float(total_loss.cpu()) / max(total_rows, 1)


def score_chunks(
    *,
    model: nn.Module,
    resident_timelines: ResidentTimelines,
    chunks: Sequence[PreparedSequenceChunk],
) -> np.ndarray:
    model.eval()
    outputs: list[torch.Tensor] = []
    with torch.inference_mode():
        for chunk in chunks:
            values, gather = resident_timelines.inputs(chunk)
            outputs.append(torch.softmax(model(values, gather), dim=1))
    if not outputs:
        return np.empty((0, len(CLASS_NAMES)), dtype=np.float32)
    return torch.cat(outputs, dim=0).cpu().numpy()


def fit_control_model(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    train_indices: np.ndarray,
    evaluation_indices: np.ndarray,
    config: DirectionalConfig,
    device: torch.device,
    epochs: int,
    seed: int,
    shuffle_targets: bool,
    label: str,
) -> tuple[np.ndarray, list[dict[str, Any]]]:
    set_deterministic_seed(seed)
    model = create_model(timelines, config).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=max(config.learning_rate, 1e-3), weight_decay=0.0
    )
    control_labels = labels.copy()
    if shuffle_targets:
        shuffled = control_labels.iloc[train_indices]["class_target"].to_numpy(copy=True)
        np.random.default_rng(seed).shuffle(shuffled)
        control_labels.loc[control_labels.index[train_indices], "class_target"] = shuffled
    decision_ns = labels["decision_ns"].to_numpy(dtype=np.int64)
    resident_timelines = ResidentTimelines.from_timelines(timelines, device)
    train_chunks = prepare_sequence_chunks(
        decision_ns=decision_ns,
        timelines=timelines,
        indices=train_indices,
        chunk_size=min(config.chunk_size, len(train_indices)),
        device=device,
        targets=control_labels["class_target"].to_numpy(dtype=np.int64),
    )
    evaluation_chunks = prepare_sequence_chunks(
        decision_ns=decision_ns,
        timelines=timelines,
        indices=evaluation_indices,
        chunk_size=config.chunk_size,
        device=device,
    )
    history: list[dict[str, Any]] = []
    for epoch in range(1, epochs + 1):
        loss = train_epoch(
            model=model,
            optimizer=optimizer,
            resident_timelines=resident_timelines,
            chunks=train_chunks,
            gradient_clip=config.gradient_clip,
            seed=seed + epoch,
        )
        if epoch == 1 or epoch % 5 == 0 or epoch == epochs:
            probabilities = score_chunks(
                model=model,
                resident_timelines=resident_timelines,
                chunks=evaluation_chunks,
            )
            metrics = ranking_metrics(
                labels.iloc[evaluation_indices]["class_target"].to_numpy(dtype=np.int64),
                probabilities,
            )
            history.append({"epoch": epoch, "loss": loss, **asdict(metrics)})
            print(
                f"[control:{label} epoch={epoch}/{epochs}] loss={loss:.6f} "
                f"macro_ap={metrics.macro_pr_auc:.6f} accuracy={metrics.argmax_accuracy:.4f}",
                flush=True,
            )
    return score_chunks(
        model=model,
        resident_timelines=resident_timelines,
        chunks=evaluation_chunks,
    ), history


def causal_logistic_probabilities(
    *,
    labels: pd.DataFrame,
    timelines: Mapping[str, TransformedTimeline],
    fold: Any,
    chunk_size: int,
    seed: int,
) -> np.ndarray:
    classifier = SGDClassifier(
        loss="log_loss", penalty="l2", alpha=1e-4, average=True, random_state=seed
    )
    chunks = partition_indices(fold.train_indices, chunk_size)
    for epoch in range(3):
        for position in np.random.default_rng(seed + epoch).permutation(len(chunks)):
            selected = chunks[int(position)]
            classifier.partial_fit(
                latest_feature_matrix(
                    timelines,
                    labels.iloc[selected]["decision_ns"].to_numpy(dtype=np.int64),
                ),
                labels.iloc[selected]["class_target"].to_numpy(dtype=np.int64),
                classes=np.asarray([0, 1, 2], dtype=np.int64),
            )
    outputs: list[np.ndarray] = []
    for selected in partition_indices(fold.validation_indices, chunk_size):
        outputs.append(
            classifier.predict_proba(
                latest_feature_matrix(
                    timelines,
                    labels.iloc[selected]["decision_ns"].to_numpy(dtype=np.int64),
                )
            )
        )
    return np.concatenate(outputs).astype(np.float32)


def final_refit_ensemble(
    *,
    labels: pd.DataFrame,
    raw_timelines: Mapping[str, Any],
    manifest: Mapping[str, Any],
    fold_results: Sequence[Mapping[str, Any]],
    config: DirectionalConfig,
    device: torch.device,
    output_root: Path,
) -> dict[str, Any]:
    epochs = max(1, int(round(float(np.median([row["best_epoch"] for row in fold_results])))))
    preprocessor = fit_preprocessor(
        raw_timelines,
        train_end=pd.Timestamp(labels["decision_ts"].max()),
        clip_value=float(manifest["preprocessing"]["clip_value"]),
    )
    timelines = transform_timelines(raw_timelines, preprocessor)
    resident_timelines = ResidentTimelines.from_timelines(timelines, device)
    chunks = prepare_sequence_chunks(
        decision_ns=labels["decision_ns"].to_numpy(dtype=np.int64),
        timelines=timelines,
        indices=np.arange(len(labels), dtype=np.int64),
        chunk_size=config.chunk_size,
        device=device,
        targets=labels["class_target"].to_numpy(dtype=np.int64),
    )
    artifacts: list[dict[str, Any]] = []
    for seed in config.seeds:
        seed_root = output_root / "final_ensemble" / f"seed_{seed}"
        model_path = seed_root / "model.pt"
        if model_path.is_file():
            existing = torch.load(model_path, map_location="cpu", weights_only=False)
            if existing.get("model_config_hash") != config.model_hash:
                raise ValueError(f"stale directional final artifact: {model_path}")
            artifacts.append({"seed": seed, "path": str(model_path), "sha256": _sha256(model_path)})
            continue
        set_deterministic_seed(seed)
        model = create_model(timelines, config).to(device)
        optimizer = torch.optim.AdamW(
            model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay
        )
        last_path = seed_root / "last.pt"
        start_epoch = 1
        if last_path.is_file():
            checkpoint = torch.load(last_path, map_location=device, weights_only=False)
            model.load_state_dict(checkpoint["model_state"])
            optimizer.load_state_dict(checkpoint["optimizer_state"])
            start_epoch = int(checkpoint["epoch"]) + 1
        for epoch in range(start_epoch, epochs + 1):
            loss = train_epoch(
                model=model,
                optimizer=optimizer,
                resident_timelines=resident_timelines,
                chunks=chunks,
                gradient_clip=config.gradient_clip,
                seed=seed + epoch,
            )
            _atomic_torch_save(
                last_path,
                {
                    "schema_version": RUN_SCHEMA_VERSION,
                    "model_config_hash": config.model_hash,
                    "epoch": epoch,
                    "model_state": model.state_dict(),
                    "optimizer_state": optimizer.state_dict(),
                },
            )
            print(
                f"[directional-final seed={seed} epoch={epoch}/{epochs}] loss={loss:.6f}",
                flush=True,
            )
        _atomic_torch_save(
            model_path,
            {
                "schema_version": MODEL_SCHEMA_VERSION,
                "manifest_hash": manifest["manifest_hash"],
                "target_config_hash": manifest["target_config_hash"],
                "model_config_hash": config.model_hash,
                "directional_config": _config_mapping(config),
                "class_order": list(CLASS_NAMES),
                "preprocessor": _preprocessor_mapping(preprocessor),
                "seed": seed,
                "epochs": epochs,
                "state_dict": {name: value.detach().cpu() for name, value in model.state_dict().items()},
                "walk_forward_inspected": False,
            },
        )
        artifacts.append({"seed": seed, "path": str(model_path), "sha256": _sha256(model_path)})
    return {"status": "completed", "selected_epochs": epochs, "artifacts": artifacts}


def contiguous_three_class_window(
    indices: np.ndarray,
    target: np.ndarray,
    *,
    rows: int,
    minimum_class_fraction: float = 0.10,
) -> np.ndarray:
    if len(indices) < rows:
        raise ValueError("not enough rows for three-class control")
    best: tuple[float, np.ndarray] | None = None
    stride = max(1, rows // 8)
    for start in range(0, len(indices) - rows + 1, stride):
        selected = np.asarray(indices[start : start + rows], dtype=np.int64)
        fractions = np.bincount(target[selected], minlength=3) / rows
        if fractions.min() < minimum_class_fraction:
            continue
        distance = float(np.abs(fractions - 1.0 / 3.0).sum())
        if best is None or distance < best[0]:
            best = (distance, selected)
    if best is None:
        raise ValueError("no contiguous overfit window contains all three classes")
    return best[1]


def probabilities_frame(
    labels: pd.DataFrame,
    probabilities: np.ndarray,
    *,
    fold_id: str,
    seed: int,
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "decision_ts": labels["decision_ts"].to_numpy(),
            "class_target": labels["class_target"].to_numpy(dtype=np.int64),
            "raw_label": labels["raw_label"].to_numpy(),
            "p_long": probabilities[:, 0],
            "p_short": probabilities[:, 1],
            "p_neutral": probabilities[:, 2],
            "fold_id": fold_id,
            "seed": seed,
        }
    )


def ensemble_probabilities(frame: pd.DataFrame) -> pd.DataFrame:
    return (
        frame.groupby("decision_ts", as_index=False)
        .agg(
            class_target=("class_target", "first"),
            raw_label=("raw_label", "first"),
            p_long=("p_long", "mean"),
            p_short=("p_short", "mean"),
            p_neutral=("p_neutral", "mean"),
            fold_id=("fold_id", "first"),
        )
        .sort_values("decision_ts")
        .reset_index(drop=True)
    )


def probability_matrix(frame: pd.DataFrame) -> np.ndarray:
    return frame[["p_long", "p_short", "p_neutral"]].to_numpy(dtype=np.float64)


def policy_recurrence(
    predictions: pd.DataFrame,
    *,
    policy: DirectionalPolicy,
    config: DirectionalConfig,
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for (fold_id, seed), group in predictions.groupby(["fold_id", "seed"], sort=True):
        metrics = action_metrics(
            group["class_target"].to_numpy(dtype=np.int64),
            apply_policy(probability_matrix(group), policy),
        )
        passed = policy_meets_requirements(metrics, config)
        rows.append({"fold_id": str(fold_id), "seed": int(seed), "passed": passed, **asdict(metrics)})
    return {
        "passing_fraction": float(np.mean([row["passed"] for row in rows])),
        "rows": rows,
    }


def directional_monthly_metrics(
    predictions: pd.DataFrame,
    *,
    policy: DirectionalPolicy | None,
) -> list[dict[str, Any]]:
    if policy is None:
        return []
    frame = predictions.copy()
    frame["month"] = pd.to_datetime(frame["decision_ts"], utc=True).dt.strftime("%Y-%m")
    rows: list[dict[str, Any]] = []
    for month, group in frame.groupby("month", sort=True):
        metrics = action_metrics(
            group["class_target"].to_numpy(dtype=np.int64),
            apply_policy(probability_matrix(group), policy),
        )
        rows.append({"month": str(month), **asdict(metrics)})
    return rows


def trivial_baselines(target: np.ndarray) -> dict[str, Any]:
    truth = np.asarray(target, dtype=np.int64)
    return {
        "always_long": asdict(action_metrics(truth, np.zeros(len(truth), dtype=np.int64))),
        "always_short": asdict(action_metrics(truth, np.ones(len(truth), dtype=np.int64))),
        "always_neutral": asdict(action_metrics(truth, np.full(len(truth), 2, dtype=np.int64))),
    }


def model_artifact(
    *,
    model: nn.Module,
    preprocessor: PreprocessorState,
    fold: Any,
    seed: int,
    epoch: int,
    config: DirectionalConfig,
    metrics: RankingMetrics,
) -> dict[str, Any]:
    return {
        "schema_version": MODEL_SCHEMA_VERSION,
        "model_state": model.state_dict(),
        "directional_config": _config_mapping(config),
        "model_config_hash": config.model_hash,
        "preprocessor": _preprocessor_mapping(preprocessor),
        "fold": fold_summary(fold),
        "seed": seed,
        "epoch": epoch,
        "validation_ranking": asdict(metrics),
    }


def validate_checkpoint(
    checkpoint: Mapping[str, Any],
    *,
    fold_id: str,
    seed: int,
    config: DirectionalConfig,
) -> None:
    if checkpoint.get("schema_version") != RUN_SCHEMA_VERSION:
        raise ValueError("directional checkpoint schema changed")
    if checkpoint.get("fold_id") != fold_id or int(checkpoint.get("seed")) != seed:
        raise ValueError("directional checkpoint belongs to another fold or seed")
    if checkpoint.get("model_config_hash") != config.model_hash:
        raise ValueError("directional model configuration changed")


def require_controls(
    output_root: Path,
    *,
    manifest: Mapping[str, Any],
    config: DirectionalConfig,
) -> None:
    path = output_root / "controls.json"
    if not path.is_file():
        raise ValueError(f"directional training requires controls at {path}")
    controls = json.loads(path.read_text())
    if controls.get("status") != "passed":
        raise ValueError("directional controls did not pass")
    if controls.get("manifest_hash") != manifest["manifest_hash"]:
        raise ValueError("prepared manifest changed after directional controls")
    if controls.get("model_config_hash") != config.model_hash:
        raise ValueError("directional model configuration changed after controls")


def _folds(labels: pd.DataFrame, manifest: Mapping[str, Any], config: DirectionalConfig) -> list[Any]:
    del config
    return build_expanding_folds(
        labels,
        research_start=pd.Timestamp(manifest["splits"]["research_start"]),
        research_end=pd.Timestamp(manifest["splits"]["research_end"]),
    )


def _binary_config(config: DirectionalConfig) -> TrainingConfig:
    return TrainingConfig(
        hidden_channels=config.hidden_channels,
        fusion_channels=config.fusion_channels,
        kernel_size=config.kernel_size,
        dropout=config.dropout,
        learning_rate=config.learning_rate,
        weight_decay=config.weight_decay,
        gradient_clip=config.gradient_clip,
        chunk_size=config.chunk_size,
        max_epochs=config.max_epochs,
        patience=config.patience,
        seeds=config.seeds,
    )


def _config_mapping(config: DirectionalConfig) -> dict[str, Any]:
    value = asdict(config)
    value["seeds"] = list(config.seeds)
    return value


def _hash_mapping(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train exact LONG/SHORT/NEUTRAL causal TCN.")
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
    parser.add_argument("--minimum-action-precision", type=float, default=0.55)
    parser.add_argument("--minimum-entry-coverage", type=float, default=0.05)
    parser.add_argument("--minimum-side-coverage", type=float, default=0.01)
    parser.add_argument("--control-overfit-epochs", type=int, default=100)
    parser.add_argument("--control-shuffle-epochs", type=int, default=4)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    manifest_path = args.manifest.resolve()
    manifest = json.loads(manifest_path.read_text())
    output_root = (
        args.output_dir.resolve()
        if args.output_dir is not None
        else Path(manifest["artifact_root"]) / "training" / "supervised_directional_tcn_v1"
    )
    config = DirectionalConfig(
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
        minimum_action_precision=args.minimum_action_precision,
        minimum_entry_coverage=args.minimum_entry_coverage,
        minimum_side_coverage=args.minimum_side_coverage,
    )
    try:
        if args.mode == "preflight":
            report = run_preflight(
                manifest_path=manifest_path, output_root=output_root, config=config
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
