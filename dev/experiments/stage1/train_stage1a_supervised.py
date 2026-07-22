from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.linear_model import LogisticRegression

from quant_terminal_strategies.stage1a_supervised_features import (
    BOLLINGER_FEATURE_NAMES,
    DEFAULT_ACTIVE_FEATURE_NAMES,
    FEATURE_SPEC_VERSION,
    OI_FEATURE_NAMES,
    extract_packet_features,
    observed_feature_names,
)
from quant_terminal_strategies.stage1a_supervised_runtime import score_two_head_artifact


MODEL_SCHEMA_VERSION = "stage1a_supervised_model.v1"
MODEL_ID = "vegas_5m_cluster_v6_stage1a_supervised"


def prepare_training_matrix(matrix: np.ndarray) -> dict[str, np.ndarray]:
    values = np.asarray(matrix, dtype=float)
    if values.ndim != 2 or values.shape[1] == 0:
        raise ValueError("training matrix must be two-dimensional with at least one feature")
    imputation_values = np.asarray(
        [
            float(np.median(column[~np.isnan(column)])) if np.any(~np.isnan(column)) else 0.0
            for column in values.T
        ],
        dtype=float,
    )
    imputed = np.where(np.isnan(values), imputation_values, values)
    means = np.mean(imputed, axis=0)
    scales = np.std(imputed, axis=0)
    scales = np.where(scales == 0, 1.0, scales)
    return {
        "imputation_values": imputation_values,
        "imputed_matrix": imputed,
        "means": means,
        "scales": scales,
        "standardized_matrix": (imputed - means) / scales,
    }


def fit_logistic_head(
    matrix: np.ndarray, labels: np.ndarray
) -> tuple[dict[str, Any], dict[str, Any]]:
    prepared = prepare_training_matrix(matrix)
    y = np.asarray(labels, dtype=int)
    if len(np.unique(y)) != 2:
        raise ValueError("logistic head requires both negative and positive training labels")
    model = LogisticRegression(
        class_weight="balanced",
        max_iter=2000,
        random_state=42,
        solver="lbfgs",
    )
    model.fit(prepared["standardized_matrix"], y)
    head = {
        "intercept": float(model.intercept_[0]),
        "coefficients": [float(value) for value in model.coef_[0]],
        "imputation_values": [float(value) for value in prepared["imputation_values"]],
        "means": [float(value) for value in prepared["means"]],
        "scales": [float(value) for value in prepared["scales"]],
    }
    return head, {
        "model": model,
        "means": prepared["means"],
        "scales": prepared["scales"],
        "imputation_values": prepared["imputation_values"],
    }


def load_training_rows(iteration_root: Path) -> list[dict[str, Any]]:
    sample_path = iteration_root / "builder_training_sample.json"
    sample = json.loads(sample_path.read_text())
    if sample.get("sample_method") != "training" or sample.get("ground_truth_visible") is not True:
        raise ValueError("builder_training_sample.json must be a visible training-only sample")
    rows: list[dict[str, Any]] = []
    for item in sample.get("signals", []):
        packet = item.get("packet") if isinstance(item.get("packet"), dict) else {}
        truth = (
            item.get("ground_truth", {}).get("natural_direction")
            if isinstance(item.get("ground_truth"), dict)
            else None
        )
        timestamp = str(item.get("timestamp") or packet.get("timestamp") or "")
        rows.append(
            {
                "signal_id": str(item.get("signal_id") or ""),
                "timestamp": timestamp,
                "month": timestamp[:7],
                "truth_direction": truth if truth in {"LONG", "SHORT"} else None,
                "features": extract_packet_features(packet),
            }
        )
    if not rows:
        raise ValueError("builder training sample contains no signals")
    return rows


def fit_artifact(
    rows: list[dict[str, Any]],
    *,
    active_feature_names: tuple[str, ...] = DEFAULT_ACTIVE_FEATURE_NAMES,
    model_version: str,
    enter_threshold: float,
    max_missing_fraction: float = 0.2,
) -> dict[str, Any]:
    matrix = _matrix(rows, active_feature_names)
    enter_labels = np.asarray(
        [int(row["truth_direction"] in {"LONG", "SHORT"}) for row in rows], dtype=int
    )
    direction_indices = [
        index for index, row in enumerate(rows) if row["truth_direction"] in {"LONG", "SHORT"}
    ]
    direction_labels = np.asarray(
        [int(rows[index]["truth_direction"] == "LONG") for index in direction_indices],
        dtype=int,
    )
    enter_head, _ = fit_logistic_head(matrix, enter_labels)
    direction_head, _ = fit_logistic_head(matrix[direction_indices], direction_labels)
    label_counts = Counter(row["truth_direction"] or "NONE" for row in rows)
    return {
        "schema_version": MODEL_SCHEMA_VERSION,
        "model_id": MODEL_ID,
        "model_version": model_version,
        "feature_spec_version": FEATURE_SPEC_VERSION,
        "model_family": "two_head_logistic",
        "active_feature_names": list(active_feature_names),
        "observed_feature_names": list(observed_feature_names()),
        "diagnostic_feature_names": list(BOLLINGER_FEATURE_NAMES + OI_FEATURE_NAMES),
        "heads": {
            "enter": enter_head,
            "direction": direction_head,
        },
        "thresholds": {
            "enter_threshold": float(enter_threshold),
            "max_missing_fraction": float(max_missing_fraction),
        },
        "training": {
            "sample_role": "training",
            "signal_count": len(rows),
            "start_timestamp": min(str(row["timestamp"]) for row in rows),
            "end_timestamp": max(str(row["timestamp"]) for row in rows),
            "label_counts": dict(sorted(label_counts.items())),
            "random_state": 42,
            "class_weight": "balanced",
        },
    }


def evaluate_artifact(rows: list[dict[str, Any]], artifact: dict[str, Any]) -> dict[str, Any]:
    counts = Counter()
    side_counts: dict[str, Counter[str]] = defaultdict(Counter)
    for row in rows:
        scores = score_two_head_artifact(row["features"], artifact)
        prediction = scores["direction"] if scores["enter"] else "FLAT"
        truth = row["truth_direction"]
        if prediction == "FLAT":
            agreement = "NEUTRAL"
        elif truth in {"LONG", "SHORT"} and prediction == truth:
            agreement = "MATCH"
        else:
            agreement = "MISMATCH"
        counts[agreement] += 1
        side_counts[str(truth or "NONE")][agreement] += 1
    scoreable = counts["MATCH"] + counts["MISMATCH"]
    return {
        "total": len(rows),
        "matches": counts["MATCH"],
        "mismatches": counts["MISMATCH"],
        "neutral": counts["NEUTRAL"],
        "scoreable": scoreable,
        "directional_agreement": counts["MATCH"] / scoreable if scoreable else 0.0,
        "side_counts": {side: dict(counts_) for side, counts_ in sorted(side_counts.items())},
    }


def expanding_month_evaluation(
    rows: list[dict[str, Any]],
    *,
    model_version: str,
    enter_threshold: float,
    min_train_months: int = 4,
) -> dict[str, Any]:
    months = sorted({str(row["month"]) for row in rows})
    folds = []
    aggregate = Counter()
    for index in range(min_train_months, len(months)):
        train_months = set(months[:index])
        test_month = months[index]
        train_rows = [row for row in rows if row["month"] in train_months]
        test_rows = [row for row in rows if row["month"] == test_month]
        artifact = fit_artifact(
            train_rows,
            model_version=f"{model_version}-fold-{test_month}",
            enter_threshold=enter_threshold,
        )
        metrics = evaluate_artifact(test_rows, artifact)
        aggregate.update(
            {
                "total": metrics["total"],
                "matches": metrics["matches"],
                "mismatches": metrics["mismatches"],
                "neutral": metrics["neutral"],
            }
        )
        folds.append(
            {
                "train_months": sorted(train_months),
                "test_month": test_month,
                "train_count": len(train_rows),
                "test_count": len(test_rows),
                "metrics": metrics,
            }
        )
    scoreable = aggregate["matches"] + aggregate["mismatches"]
    return {
        "min_train_months": min_train_months,
        "folds": folds,
        "aggregate": {
            "total": aggregate["total"],
            "matches": aggregate["matches"],
            "mismatches": aggregate["mismatches"],
            "neutral": aggregate["neutral"],
            "scoreable": scoreable,
            "directional_agreement": aggregate["matches"] / scoreable if scoreable else 0.0,
        },
    }


def _matrix(rows: list[dict[str, Any]], feature_names: tuple[str, ...]) -> np.ndarray:
    return np.asarray(
        [
            [
                np.nan if row["features"].get(name) is None else float(row["features"][name])
                for name in feature_names
            ]
            for row in rows
        ],
        dtype=float,
    )


def _render_report(report: dict[str, Any]) -> str:
    full = report["full_training_metrics"]
    forward = report["expanding_month_evaluation"]["aggregate"]
    lines = [
        "# Stage 1A Supervised Training",
        "",
        f"- Model: `{report['model_id']}@{report['model_version']}`",
        f"- Active features: {report['active_feature_count']}",
        f"- OI active: {report['oi_active']}",
        f"- Full training agreement: {full['directional_agreement']:.4f}",
        f"- Expanding-month agreement: {forward['directional_agreement']:.4f}",
        f"- Expanding-month scoreable: {forward['scoreable']} / {forward['total']}",
        "",
        "## Forward Months",
        "",
        "| Month | Match | Mismatch | Neutral | Agreement |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for fold in report["expanding_month_evaluation"]["folds"]:
        metrics = fold["metrics"]
        lines.append(
            f"| {fold['test_month']} | {metrics['matches']} | {metrics['mismatches']} | "
            f"{metrics['neutral']} | {metrics['directional_agreement']:.4f} |"
        )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fit and export a portable two-head logistic Stage 1A policy."
    )
    parser.add_argument("--iteration-root", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--model-version", default="btc-vegas-v6-stage1a-logistic-v1")
    parser.add_argument("--enter-threshold", type=float, default=0.3)
    parser.add_argument("--min-train-months", type=int, default=4)
    args = parser.parse_args()

    rows = load_training_rows(args.iteration_root)
    artifact = fit_artifact(
        rows,
        model_version=args.model_version,
        enter_threshold=args.enter_threshold,
    )
    forward = expanding_month_evaluation(
        rows,
        model_version=args.model_version,
        enter_threshold=args.enter_threshold,
        min_train_months=args.min_train_months,
    )
    report = {
        "model_id": artifact["model_id"],
        "model_version": artifact["model_version"],
        "active_feature_count": len(artifact["active_feature_names"]),
        "oi_active": any(name.startswith("oi_") for name in artifact["active_feature_names"]),
        "full_training_metrics": evaluate_artifact(rows, artifact),
        "expanding_month_evaluation": forward,
    }
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    report_json_path = args.output_path.with_suffix(".training_report.json")
    report_md_path = args.output_path.with_suffix(".training_report.md")
    report_json_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    report_md_path.write_text(_render_report(report))
    print(
        json.dumps(
            {
                "artifact_path": str(args.output_path),
                "report_json_path": str(report_json_path),
                "report_md_path": str(report_md_path),
                "full_training_metrics": report["full_training_metrics"],
                "forward_metrics": forward["aggregate"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
