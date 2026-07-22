from __future__ import annotations

import math
from typing import Any

from quant_terminal_strategies.stage1a_supervised_features import (
    BOLLINGER_FEATURE_NAMES,
    FEATURE_SPEC_VERSION,
    OI_FEATURE_NAMES,
    extract_signal_features,
)


MODEL_SCHEMA_VERSION = "stage1a_supervised_model.v1"


def score_two_head_artifact(
    features: dict[str, float | None], artifact: dict[str, Any]
) -> dict[str, Any]:
    _validate_artifact(artifact)
    active_names = [str(name) for name in artifact["active_feature_names"]]
    missing = [name for name in active_names if features.get(name) is None]
    missing_fraction = len(missing) / len(active_names) if active_names else 1.0
    thresholds = artifact["thresholds"]
    max_missing_fraction = float(thresholds.get("max_missing_fraction", 0.2))

    p_enter = _head_probability(features, active_names, artifact["heads"]["enter"])
    p_long = _head_probability(features, active_names, artifact["heads"]["direction"])
    p_short = 1.0 - p_long
    score_long = p_enter * p_long
    score_short = p_enter * p_short
    score_skip = 1.0 - p_enter
    direction = "LONG" if score_long >= score_short else "SHORT"
    best_enter_score = max(score_long, score_short)
    enter_threshold = float(thresholds["enter_threshold"])
    enter = best_enter_score >= enter_threshold and missing_fraction <= max_missing_fraction
    return {
        "p_enter": p_enter,
        "p_long_given_enter": p_long,
        "p_short_given_enter": p_short,
        "score_long": score_long,
        "score_short": score_short,
        "score_skip": score_skip,
        "direction": direction,
        "enter": enter,
        "enter_threshold": enter_threshold,
        "direction_margin": abs(score_long - score_short),
        "missing_active_features": missing,
        "missing_active_fraction": missing_fraction,
        "max_missing_fraction": max_missing_fraction,
    }


def decide_with_artifact(
    context: dict[str, Any],
    *,
    artifact: dict[str, Any],
    strategy_id: str,
    strategy_version: str,
) -> dict[str, Any]:
    signal = context.get("signal") if isinstance(context.get("signal"), dict) else {}
    signal_id = str(signal.get("signal_id") or "unknown")
    features = extract_signal_features(signal)
    scores = score_two_head_artifact(features, artifact)
    active_names = [str(name) for name in artifact["active_feature_names"]]
    diagnostic_names = artifact.get("diagnostic_feature_names")
    if not isinstance(diagnostic_names, list):
        diagnostic_names = artifact.get("observed_feature_names", features)
    observed_names = [str(name) for name in diagnostic_names]
    observed_not_used = [
        name for name in observed_names if name not in active_names and features.get(name) is not None
    ]

    if scores["missing_active_fraction"] > scores["max_missing_fraction"]:
        action = "SKIP"
        direction = "FLAT"
        confidence = scores["score_skip"]
        reason_code = "supervised_active_features_missing"
    elif not scores["enter"]:
        action = "SKIP"
        direction = "FLAT"
        confidence = scores["score_skip"]
        reason_code = "supervised_enter_score_below_threshold"
    else:
        action = "ENTER"
        direction = scores["direction"]
        confidence = max(scores["score_long"], scores["score_short"])
        reason_code = f"supervised_two_head_enter_{direction.lower()}"

    diagnostics = {
        "model_id": artifact["model_id"],
        "model_version": artifact["model_version"],
        "feature_spec_version": artifact["feature_spec_version"],
        "model_family": artifact["model_family"],
        "runtime_mode": context.get("runtime_mode", "backtest"),
        "p_enter": scores["p_enter"],
        "p_long_given_enter": scores["p_long_given_enter"],
        "p_short_given_enter": scores["p_short_given_enter"],
        "score_long": scores["score_long"],
        "score_short": scores["score_short"],
        "score_skip": scores["score_skip"],
        "enter_threshold": scores["enter_threshold"],
        "direction_margin": scores["direction_margin"],
        "features_used_by_model": active_names,
        "features_observed_not_used": observed_not_used,
        "missing_active_features": scores["missing_active_features"],
        "missing_active_fraction": scores["missing_active_fraction"],
        "oi_features_snapshot": {name: features.get(name) for name in OI_FEATURE_NAMES},
        "bollinger_snapshot": {
            name: features.get(name) for name in BOLLINGER_FEATURE_NAMES
        },
    }
    return {
        "decision_id": f"{strategy_id}-{strategy_version}-{signal_id}",
        "strategy_id": strategy_id,
        "strategy_version": strategy_version,
        "signal_id": signal_id,
        "trade_action": action,
        "action": action,
        "direction": direction,
        "confidence": _clamp_probability(confidence),
        "reason_code": reason_code,
        "execution_profile": {},
        "diagnostics": diagnostics,
    }


def _head_probability(
    features: dict[str, float | None], active_names: list[str], head: dict[str, Any]
) -> float:
    coefficients = [float(value) for value in head["coefficients"]]
    imputation_values = [float(value) for value in head["imputation_values"]]
    means = [float(value) for value in head["means"]]
    scales = [float(value) for value in head["scales"]]
    logit = float(head["intercept"])
    for index, name in enumerate(active_names):
        value = features.get(name)
        numeric = imputation_values[index] if value is None else float(value)
        scale = scales[index] if scales[index] != 0 else 1.0
        logit += coefficients[index] * ((numeric - means[index]) / scale)
    return _sigmoid(logit)


def _sigmoid(value: float) -> float:
    if value >= 0:
        exponent = math.exp(-value)
        return 1.0 / (1.0 + exponent)
    exponent = math.exp(value)
    return exponent / (1.0 + exponent)


def _clamp_probability(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


def _validate_artifact(artifact: dict[str, Any]) -> None:
    if artifact.get("schema_version") != MODEL_SCHEMA_VERSION:
        raise ValueError(f"unsupported supervised model schema: {artifact.get('schema_version')}")
    if artifact.get("feature_spec_version") != FEATURE_SPEC_VERSION:
        raise ValueError(
            f"feature spec mismatch: {artifact.get('feature_spec_version')} != {FEATURE_SPEC_VERSION}"
        )
    if artifact.get("model_family") != "two_head_logistic":
        raise ValueError(f"unsupported model family: {artifact.get('model_family')}")
    active_names = artifact.get("active_feature_names")
    if not isinstance(active_names, list) or not active_names:
        raise ValueError("active_feature_names must be a non-empty list")
    heads = artifact.get("heads") if isinstance(artifact.get("heads"), dict) else {}
    for head_name in ("enter", "direction"):
        head = heads.get(head_name) if isinstance(heads.get(head_name), dict) else {}
        for field in ("coefficients", "imputation_values", "means", "scales"):
            values = head.get(field)
            if not isinstance(values, list) or len(values) != len(active_names):
                raise ValueError(f"{head_name}.{field} must align with active_feature_names")
        if head.get("intercept") is None:
            raise ValueError(f"{head_name}.intercept is required")
    thresholds = (
        artifact.get("thresholds") if isinstance(artifact.get("thresholds"), dict) else {}
    )
    if thresholds.get("enter_threshold") is None:
        raise ValueError("thresholds.enter_threshold is required")
