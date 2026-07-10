from __future__ import annotations

from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
import hashlib
import json
import random
import statistics
from typing import Any


DEFAULT_THRESHOLDS_PCT = [0.5, 1.0, 2.0, 3.0]
DEFAULT_RANDOM_REPLICATES = 100
DEFAULT_BOOTSTRAP_REPLICATES = 300
DEFAULT_MAX_AUDIT_EVENTS_PER_SPLIT = 5000


def compute_forward_excursion(
    *,
    candles: list[dict[str, Any]],
    signal_ts: datetime,
    reference_price: float,
    forward_hours: int,
    thresholds_pct: list[float] | None = None,
) -> dict[str, Any]:
    normalized = _normalize_candles(candles)
    return _compute_forward_excursion_normalized(
        candles=normalized,
        candle_timestamps=[row["timestamp"] for row in normalized],
        signal_ts=signal_ts,
        reference_price=reference_price,
        forward_hours=forward_hours,
        thresholds_pct=thresholds_pct or DEFAULT_THRESHOLDS_PCT,
    )


def _compute_forward_excursion_normalized(
    *,
    candles: list[dict[str, Any]],
    candle_timestamps: list[datetime],
    signal_ts: datetime,
    reference_price: float,
    forward_hours: int,
    thresholds_pct: list[float],
) -> dict[str, Any]:
    if not candles or reference_price <= 0:
        return _empty_excursion(thresholds_pct or DEFAULT_THRESHOLDS_PCT)
    first_idx = bisect_right(candle_timestamps, _ensure_utc(signal_ts))
    if first_idx >= len(candles):
        return _empty_excursion(thresholds_pct or DEFAULT_THRESHOLDS_PCT)
    cutoff = _ensure_utc(signal_ts) + timedelta(hours=forward_hours)
    max_high = reference_price
    min_low = reference_price
    for row in candles[first_idx:]:
        if row["timestamp"] > cutoff:
            break
        max_high = max(max_high, float(row["high"]))
        min_low = min(min_low, float(row["low"]))
    up_mfe_pct = max(0.0, (max_high - reference_price) / reference_price * 100)
    down_mfe_pct = max(0.0, (reference_price - min_low) / reference_price * 100)
    max_abs_mfe_pct = max(up_mfe_pct, down_mfe_pct)
    opposite_excursion_pct = min(up_mfe_pct, down_mfe_pct)
    threshold_hits = {_threshold_key(threshold): max_abs_mfe_pct >= threshold for threshold in thresholds_pct}
    return {
        "up_mfe_pct": round(up_mfe_pct, 6),
        "down_mfe_pct": round(down_mfe_pct, 6),
        "max_abs_mfe_pct": round(max_abs_mfe_pct, 6),
        "opposite_excursion_pct": round(opposite_excursion_pct, 6),
        "excursion_asymmetry_pct": round(abs(up_mfe_pct - down_mfe_pct), 6),
        "threshold_hits": threshold_hits,
    }


def generate_broad_split_random_timestamps(
    *,
    event_timestamps: list[datetime],
    candles: list[dict[str, Any]],
    split_start: datetime,
    split_end: datetime,
    forward_hours: int,
    seed: str,
) -> list[datetime]:
    normalized = _normalize_candles(candles)
    split_start = _ensure_utc(split_start)
    split_end = _ensure_utc(split_end)
    latest_allowed = normalized[-1]["timestamp"] - timedelta(hours=forward_hours) if normalized else split_end
    all_candidates = [
        row["timestamp"]
        for row in normalized
        if split_start <= row["timestamp"] <= split_end and row["timestamp"] <= latest_allowed
    ]
    if not all_candidates:
        return []
    rng = random.Random(_stable_int_seed(seed))
    if len(event_timestamps) <= len(all_candidates):
        return rng.sample(all_candidates, len(event_timestamps))
    return [rng.choice(all_candidates) for _ in event_timestamps]


def score_split_information(
    *,
    event_values: list[float],
    random_replicates: list[list[float]],
    min_event_count: int,
    p_value_threshold: float,
    material_lift_pct: float,
    probability_superiority_floor: float,
    bootstrap_seed: str,
    hit_threshold_pct: float = 1.0,
    hit_rate_lift_floor_pct_points: float = 10.0,
    require_statistical_significance: bool = True,
    ci_floor_lift_pct: float | None = None,
) -> dict[str, Any]:
    clean_events = [float(value) for value in event_values if value is not None]
    clean_replicates = [[float(value) for value in replicate if value is not None] for replicate in random_replicates]
    clean_replicates = [replicate for replicate in clean_replicates if replicate]
    if len(clean_events) < min_event_count or not clean_replicates:
        return {
            "status": "insufficient_sample",
            "event_count": len(clean_events),
            "min_event_count": min_event_count,
            "empirical_p_value": None,
            "median_lift_pct": None,
            "probability_superiority": None,
        }
    event_median = statistics.median(clean_events)
    replicate_medians = [statistics.median(replicate) for replicate in clean_replicates]
    random_median = statistics.median(replicate_medians)
    empirical_p_value = (1 + sum(1 for value in replicate_medians if value >= event_median)) / (len(replicate_medians) + 1)
    median_lift_abs = event_median - random_median
    median_lift_pct = _relative_lift_pct(event_median, random_median)
    pooled_random = [value for replicate in clean_replicates for value in replicate]
    probability_superiority = _probability_superiority(clean_events, pooled_random)
    event_hit_rate = _hit_rate(clean_events, hit_threshold_pct)
    random_hit_rate = statistics.median([_hit_rate(replicate, hit_threshold_pct) for replicate in clean_replicates])
    hit_rate_lift_pct_points = (event_hit_rate - random_hit_rate) * 100
    ci = _bootstrap_median_lift_ci(
        clean_events,
        pooled_random,
        seed=bootstrap_seed,
        replicates=DEFAULT_BOOTSTRAP_REPLICATES,
    )
    materiality_pass = median_lift_pct >= material_lift_pct or hit_rate_lift_pct_points >= hit_rate_lift_floor_pct_points
    ci_lower_lift_pct = _relative_lift_pct(random_median + ci["lower_abs"], random_median)
    if ci_floor_lift_pct is not None:
        significance_pass = empirical_p_value <= p_value_threshold or ci_lower_lift_pct >= ci_floor_lift_pct
    elif require_statistical_significance:
        significance_pass = empirical_p_value <= p_value_threshold and ci["lower_abs"] > 0
    else:
        significance_pass = True
    status = "pass" if (
        significance_pass
        and materiality_pass
        and probability_superiority >= probability_superiority_floor
    ) else "fail"
    return {
        "status": status,
        "event_count": len(clean_events),
        "random_replicates": len(clean_replicates),
        "event_median": round(event_median, 6),
        "random_median": round(random_median, 6),
        "median_lift_abs": round(median_lift_abs, 6),
        "median_lift_pct": round(median_lift_pct, 6),
        "empirical_p_value": round(empirical_p_value, 6),
        "bootstrap_median_lift_ci_abs": {
            "lower": round(ci["lower_abs"], 6),
            "upper": round(ci["upper_abs"], 6),
        },
        "bootstrap_median_lift_ci_pct": {
            "lower": round(ci_lower_lift_pct, 6),
            "upper": round(_relative_lift_pct(random_median + ci["upper_abs"], random_median), 6),
        },
        "probability_superiority": round(probability_superiority, 6),
        "hit_threshold_pct": hit_threshold_pct,
        "event_hit_rate": round(event_hit_rate, 6),
        "random_hit_rate": round(random_hit_rate, 6),
        "hit_rate_lift_pct_points": round(hit_rate_lift_pct_points, 6),
        "materiality_pass": materiality_pass,
        "significance_pass": significance_pass,
    }


def benjamini_hochberg_q_values(p_values_by_id: dict[str, float]) -> dict[str, float]:
    if not p_values_by_id:
        return {}
    ranked = sorted((key, float(value)) for key, value in p_values_by_id.items())
    ranked.sort(key=lambda item: item[1])
    total = len(ranked)
    adjusted: dict[str, float] = {}
    running_min = 1.0
    for rank_from_end, (key, p_value) in enumerate(reversed(ranked), start=1):
        rank = total - rank_from_end + 1
        q_value = min(running_min, p_value * total / rank)
        running_min = q_value
        adjusted[key] = round(min(q_value, 1.0), 6)
    return adjusted


def apply_information_q_values_to_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    p_values: dict[str, float] = {}
    for candidate in candidates:
        info = _candidate_information(candidate)
        p_value = info.get("train_empirical_p_value")
        if isinstance(p_value, (int, float)):
            p_values[str(candidate["candidate_id"])] = float(p_value)
    if not p_values:
        return candidates
    q_values = benjamini_hochberg_q_values(p_values)
    enforce_fdr = len(p_values) > 10
    adjusted = []
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        q_value = q_values.get(candidate_id)
        if q_value is None:
            adjusted.append(candidate)
            continue
        metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
        info = dict(metrics.get("stage0_information") if isinstance(metrics.get("stage0_information"), dict) else {})
        info["train_q_value"] = q_value
        next_candidate = {
            **candidate,
            "metrics": {
                **metrics,
                "stage0_information": info,
            },
        }
        if (
            enforce_fdr
            and q_value > 0.10
            and info.get("status") == "pass"
            and candidate.get("acceptance_status") == "accepted"
        ):
            info["status"] = "fail"
            info["decision_reason"] = "fdr_q_value_above_threshold"
            next_candidate = {
                **next_candidate,
                "acceptance_status": "watchlist",
                "branch_path": "information_fail",
            }
        adjusted.append(next_candidate)
    return adjusted


def run_stage0_information_gate(
    *,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    signals: list[dict[str, Any]],
    candle_rows: list[dict[str, Any]],
    random_replicates: int = DEFAULT_RANDOM_REPLICATES,
    thresholds_pct: list[float] | None = None,
    max_audit_events_per_split: int = DEFAULT_MAX_AUDIT_EVENTS_PER_SPLIT,
) -> dict[str, Any]:
    thresholds = thresholds_pct or DEFAULT_THRESHOLDS_PCT
    candles = _normalize_candles(candle_rows)
    candle_timestamps = [row["timestamp"] for row in candles]
    if not _has_configured_train_walk_forward(universe_run):
        return _compatibility_pass_summary(universe_run=universe_run, signals=signals)
    split_windows = _split_windows(universe_run)
    event_records = _event_records(
        signals=signals,
        candles=candles,
        forward_hours=int(universe_run["forward_hours"]),
        thresholds_pct=thresholds,
    )
    split_results: dict[str, Any] = {}
    for split_name, window in split_windows.items():
        split_events = [
            record for record in event_records
            if window["start"] <= record["timestamp"] <= window["end"]
        ]
        if len(split_events) > max_audit_events_per_split:
            split_events = _deterministic_sample_records(
                split_events,
                size=max_audit_events_per_split,
                seed=f"{universe_run['universe_run_id']}:{candidate['signal_set_key']}:{split_name}:events",
            )
        random_values_by_replicate: list[list[float]] = []
        for replicate in range(random_replicates):
            random_timestamps = generate_broad_split_random_timestamps(
                event_timestamps=[record["timestamp"] for record in split_events],
                candles=candles,
                split_start=window["start"],
                split_end=window["end"],
                forward_hours=int(universe_run["forward_hours"]),
                seed=f"{universe_run['universe_run_id']}:{candidate['signal_set_key']}:{split_name}:{replicate}",
            )
            random_values_by_replicate.append(
                [
                    _compute_forward_excursion_normalized(
                        candles=candles,
                        candle_timestamps=candle_timestamps,
                        signal_ts=timestamp,
                        reference_price=_reference_price_at(candles, candle_timestamps, timestamp),
                        forward_hours=int(universe_run["forward_hours"]),
                        thresholds_pct=thresholds,
                    )["max_abs_mfe_pct"]
                    for timestamp in random_timestamps
                ]
            )
        event_values = [record["excursion"]["max_abs_mfe_pct"] for record in split_events]
        wf_large_sample = split_name == "walk_forward" and len(event_values) >= 100
        split_results[split_name] = {
            "event_distribution": _distribution([record["excursion"]["max_abs_mfe_pct"] for record in split_events]),
            "random_distribution": _replicate_distribution(random_values_by_replicate),
            "score": score_split_information(
                event_values=event_values,
                random_replicates=random_values_by_replicate,
                min_event_count=100 if split_name == "train" else 30,
                p_value_threshold=0.05 if split_name == "train" else 0.10,
                material_lift_pct=15.0 if split_name == "train" else 0.0,
                probability_superiority_floor=0.55 if split_name == "train" else 0.50,
                bootstrap_seed=f"{universe_run['universe_run_id']}:{candidate['signal_set_key']}:{split_name}:bootstrap",
                require_statistical_significance=split_name == "train" or wf_large_sample,
                ci_floor_lift_pct=-5.0 if wf_large_sample else None,
            ),
        }
    monthly = _monthly_stability(
        event_records=event_records,
        candles=candles,
        candle_timestamps=candle_timestamps,
        universe_run=universe_run,
        candidate=candidate,
        thresholds_pct=thresholds,
    )
    decision = _information_decision(split_results=split_results, monthly=monthly)
    return {
        "schema_version": "stage0_information_gate.v1",
        "status": decision["status"],
        "decision_reason": decision["reason"],
        "random_baseline": {
            "mode": "broad_split_random",
            "random_replicates": random_replicates,
            "seed_material": f"{universe_run['universe_run_id']}:{candidate['signal_set_key']}",
            "matching": ["asset", "split_window", "sample_count", "5m_grid"],
            "excluded_matching": ["utc_hour", "weekday"],
        },
        "thresholds_pct": thresholds,
        "splits": split_results,
        "monthly_stability": monthly,
        "summary_metrics": _summary_metrics(split_results=split_results, monthly=monthly),
    }


def _candidate_information(candidate: dict[str, Any]) -> dict[str, Any]:
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    info = metrics.get("stage0_information") if isinstance(metrics.get("stage0_information"), dict) else {}
    return info


def _event_records(
    *,
    signals: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    forward_hours: int,
    thresholds_pct: list[float],
) -> list[dict[str, Any]]:
    latest_allowed = candles[-1]["timestamp"] - timedelta(hours=forward_hours) if candles else None
    candle_timestamps = [row["timestamp"] for row in candles]
    records = []
    for signal in signals:
        timestamp = _signal_timestamp(signal)
        if timestamp is None or latest_allowed is None or timestamp > latest_allowed:
            continue
        reference = _signal_reference_price(signal)
        if reference["price"] is None:
            continue
        records.append(
            {
                "signal_id": signal.get("signal_id"),
                "timestamp": timestamp,
                "reference_price": reference["price"],
                "reference_source": reference["source"],
                "excursion": _compute_forward_excursion_normalized(
                    candles=candles,
                    candle_timestamps=candle_timestamps,
                    signal_ts=timestamp,
                    reference_price=reference["price"],
                    forward_hours=forward_hours,
                    thresholds_pct=thresholds_pct,
                ),
            }
        )
    return records


def _information_decision(*, split_results: dict[str, Any], monthly: dict[str, Any]) -> dict[str, str]:
    train_status = split_results.get("train", {}).get("score", {}).get("status")
    wf_score = split_results.get("walk_forward", {}).get("score", {})
    wf_status = wf_score.get("status")
    if train_status == "insufficient_sample" or wf_status == "insufficient_sample":
        return {"status": "insufficient_sample", "reason": "insufficient_train_or_walk_forward_sample"}
    if train_status != "pass":
        return {"status": "fail", "reason": "train_information_not_significant"}
    if wf_status != "pass":
        return {"status": "fail", "reason": "walk_forward_information_degraded"}
    if not monthly.get("passed", False):
        return {"status": "fail", "reason": "monthly_stability_failed"}
    return {"status": "pass", "reason": "event_distribution_beats_matched_random"}


def _monthly_stability(
    *,
    event_records: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    candle_timestamps: list[datetime],
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    thresholds_pct: list[float],
) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in event_records:
        grouped[record["timestamp"].strftime("%Y-%m")].append(record)
    eligible = []
    for month_key, rows in sorted(grouped.items()):
        if len(rows) < 20:
            continue
        start = datetime.combine(date.fromisoformat(f"{month_key}-01"), time.min, tzinfo=timezone.utc)
        if start.month == 12:
            end = datetime(start.year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
        else:
            end = datetime(start.year, start.month + 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)
        random_timestamps = generate_broad_split_random_timestamps(
            event_timestamps=[row["timestamp"] for row in rows],
            candles=candles,
            split_start=start,
            split_end=end,
            forward_hours=int(universe_run["forward_hours"]),
            seed=f"{universe_run['universe_run_id']}:{candidate['signal_set_key']}:{month_key}:monthly",
        )
        random_values = [
            _compute_forward_excursion_normalized(
                candles=candles,
                candle_timestamps=candle_timestamps,
                signal_ts=timestamp,
                reference_price=_reference_price_at(candles, candle_timestamps, timestamp),
                forward_hours=int(universe_run["forward_hours"]),
                thresholds_pct=thresholds_pct,
            )["max_abs_mfe_pct"]
            for timestamp in random_timestamps
        ]
        event_median = statistics.median([row["excursion"]["max_abs_mfe_pct"] for row in rows])
        random_median = statistics.median(random_values) if random_values else 0.0
        eligible.append(
            {
                "month": month_key,
                "event_count": len(rows),
                "event_median": round(event_median, 6),
                "random_median": round(random_median, 6),
                "median_lift_pct": round(_relative_lift_pct(event_median, random_median), 6),
                "positive_lift": event_median >= random_median,
            }
        )
    pass_count = sum(1 for row in eligible if row["positive_lift"])
    pass_rate = pass_count / len(eligible) if eligible else 1.0
    return {
        "eligible_months": len(eligible),
        "positive_lift_months": pass_count,
        "pass_rate": round(pass_rate, 6),
        "passed": pass_rate >= 0.60,
        "months": eligible,
    }


def _summary_metrics(*, split_results: dict[str, Any], monthly: dict[str, Any]) -> dict[str, Any]:
    train = split_results.get("train", {}).get("score", {})
    wf = split_results.get("walk_forward", {}).get("score", {})
    return {
        "train_event_count": train.get("event_count"),
        "walk_forward_event_count": wf.get("event_count"),
        "train_median_lift_pct": train.get("median_lift_pct"),
        "walk_forward_median_lift_pct": wf.get("median_lift_pct"),
        "train_empirical_p_value": train.get("empirical_p_value"),
        "walk_forward_empirical_p_value": wf.get("empirical_p_value"),
        "train_probability_superiority": train.get("probability_superiority"),
        "walk_forward_probability_superiority": wf.get("probability_superiority"),
        "monthly_positive_lift_months": monthly.get("positive_lift_months"),
        "monthly_eligible_months": monthly.get("eligible_months"),
    }


def _distribution(values: list[float]) -> dict[str, Any]:
    clean = sorted(float(value) for value in values if value is not None)
    if not clean:
        return {"count": 0}
    return {
        "count": len(clean),
        "mean": round(sum(clean) / len(clean), 6),
        "median": round(statistics.median(clean), 6),
        "p25": round(_percentile(clean, 25), 6),
        "p75": round(_percentile(clean, 75), 6),
        "p90": round(_percentile(clean, 90), 6),
        "p95": round(_percentile(clean, 95), 6),
    }


def _replicate_distribution(replicates: list[list[float]]) -> dict[str, Any]:
    medians = [statistics.median(replicate) for replicate in replicates if replicate]
    return {
        "replicate_count": len(medians),
        "median_of_medians": round(statistics.median(medians), 6) if medians else None,
        "median_distribution": _distribution(medians),
    }


def _compatibility_pass_summary(*, universe_run: dict[str, Any], signals: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "stage0_information_gate.v1",
        "status": "pass",
        "decision_reason": "legacy_no_train_walk_forward_split_configured",
        "random_baseline": {"mode": "skipped_legacy_no_split", "random_replicates": 0},
        "thresholds_pct": DEFAULT_THRESHOLDS_PCT,
        "splits": {},
        "monthly_stability": {"eligible_months": 0, "positive_lift_months": 0, "pass_rate": 1.0, "passed": True, "months": []},
        "summary_metrics": {
            "train_event_count": len(signals),
            "walk_forward_event_count": None,
            "train_median_lift_pct": None,
            "walk_forward_median_lift_pct": None,
            "train_empirical_p_value": None,
            "walk_forward_empirical_p_value": None,
            "monthly_positive_lift_months": 0,
            "monthly_eligible_months": 0,
        },
        "legacy_window": {
            "window_start": universe_run.get("window_start"),
            "window_end": universe_run.get("window_end"),
        },
    }


def _has_configured_train_walk_forward(universe_run: dict[str, Any]) -> bool:
    return all(
        universe_run.get(key)
        for key in ("train_start", "train_end", "walk_forward_start", "walk_forward_end")
    )


def _split_windows(universe_run: dict[str, Any]) -> dict[str, dict[str, datetime]]:
    return {
        "train": {
            "start": _parse_window_start(str(universe_run["train_start"])),
            "end": _parse_window_end(str(universe_run["train_end"])),
        },
        "walk_forward": {
            "start": _parse_window_start(str(universe_run["walk_forward_start"])),
            "end": _parse_window_end(str(universe_run["walk_forward_end"])),
        },
    }


def _normalize_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for row in candles:
        if str(row.get("confirm", 1)) in {"0", "false", "False"}:
            continue
        timestamp = _parse_timestamp(row.get("timestamp", row.get("ts")))
        if timestamp is None:
            continue
        normalized.append(
            {
                "timestamp": timestamp,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
            }
        )
    return sorted(normalized, key=lambda item: item["timestamp"])


def _signal_timestamp(signal: dict[str, Any]) -> datetime | None:
    timestamp = signal.get("timestamp")
    if timestamp is None and isinstance(signal.get("payload"), dict):
        timestamp = signal["payload"].get("timestamp")
    return _parse_timestamp(timestamp)


def _signal_reference_price(signal: dict[str, Any]) -> dict[str, Any]:
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else signal
    evidence = payload.get("evidence", {}) if isinstance(payload, dict) else {}
    if isinstance(evidence, dict):
        for key in ("trigger_candle_close", "trigger_price", "reference_price"):
            value = evidence.get(key)
            if value is not None:
                return {"price": float(value), "source": f"evidence.{key}"}
    interactions = payload.get("interactions", {}) if isinstance(payload, dict) else {}
    if isinstance(interactions, dict):
        for entries in interactions.values():
            if entries:
                mp = entries[0].get("market_price")
                if mp:
                    return {"price": float(mp), "source": "interactions.market_price"}
    elif isinstance(interactions, list):
        for entry in interactions:
            if isinstance(entry, dict) and entry.get("market_price"):
                return {"price": float(entry["market_price"]), "source": "interactions.market_price"}
    charts = payload.get("charts", {}) if isinstance(payload, dict) else {}
    for tf in ["2h", "4h", "8h", "12h", "1d"]:
        lfc = charts.get(tf, {}).get("latest_forming_candle", {}) if isinstance(charts.get(tf), dict) else {}
        if lfc and lfc.get("close"):
            return {"price": float(lfc["close"]), "source": f"charts.{tf}.latest_forming_candle.close"}
    return {"price": None, "source": None}


def _reference_price_at(candles: list[dict[str, Any]], candle_timestamps: list[datetime], timestamp: datetime) -> float:
    idx = bisect_right(candle_timestamps, _ensure_utc(timestamp)) - 1
    if idx < 0:
        idx = 0
    return float(candles[idx]["close"])


def _deterministic_sample_records(records: list[dict[str, Any]], *, size: int, seed: str) -> list[dict[str, Any]]:
    rng = random.Random(_stable_int_seed(seed))
    indices = sorted(rng.sample(range(len(records)), size))
    return [records[index] for index in indices]


def _bootstrap_median_lift_ci(
    event_values: list[float],
    random_values: list[float],
    *,
    seed: str,
    replicates: int,
) -> dict[str, float]:
    rng = random.Random(_stable_int_seed(seed))
    lifts = []
    for _ in range(replicates):
        event_sample = [rng.choice(event_values) for _ in event_values]
        random_sample = [rng.choice(random_values) for _ in event_values]
        lifts.append(statistics.median(event_sample) - statistics.median(random_sample))
    lifts.sort()
    return {
        "lower_abs": _percentile(lifts, 2.5),
        "upper_abs": _percentile(lifts, 97.5),
    }


def _probability_superiority(event_values: list[float], random_values: list[float]) -> float:
    if not event_values or not random_values:
        return 0.0
    random_sorted = sorted(random_values)
    total = 0.0
    for value in event_values:
        lower = _count_less(random_sorted, value)
        equal = _count_equal(random_sorted, value)
        total += lower + equal * 0.5
    return total / (len(event_values) * len(random_sorted))


def _count_less(values: list[float], target: float) -> int:
    from bisect import bisect_left
    return bisect_left(values, target)


def _count_equal(values: list[float], target: float) -> int:
    from bisect import bisect_left, bisect_right
    return bisect_right(values, target) - bisect_left(values, target)


def _hit_rate(values: list[float], threshold: float) -> float:
    if not values:
        return 0.0
    return sum(1 for value in values if value >= threshold) / len(values)


def _relative_lift_pct(event_value: float, random_value: float) -> float:
    if random_value == 0:
        return 0.0 if event_value == 0 else 100.0
    return (event_value - random_value) / abs(random_value) * 100


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(values)
    rank = (len(ordered) - 1) * pct / 100
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    weight = rank - low
    return ordered[low] * (1 - weight) + ordered[high] * weight


def _empty_excursion(thresholds: list[float]) -> dict[str, Any]:
    return {
        "up_mfe_pct": 0.0,
        "down_mfe_pct": 0.0,
        "max_abs_mfe_pct": 0.0,
        "opposite_excursion_pct": 0.0,
        "excursion_asymmetry_pct": 0.0,
        "threshold_hits": {_threshold_key(threshold): False for threshold in thresholds},
    }


def _threshold_key(value: float) -> str:
    return f"{value:.1f}"


def _stable_int_seed(seed: str) -> int:
    digest = hashlib.sha256(seed.encode()).hexdigest()
    return int(digest[:16], 16)


def _parse_window_start(value: str) -> datetime:
    if "T" in value:
        return _parse_timestamp(value) or datetime.min.replace(tzinfo=timezone.utc)
    return datetime.combine(date.fromisoformat(value), time.min, tzinfo=timezone.utc)


def _parse_window_end(value: str) -> datetime:
    if "T" in value:
        return _parse_timestamp(value) or datetime.max.replace(tzinfo=timezone.utc)
    return datetime.combine(date.fromisoformat(value), time.max, tzinfo=timezone.utc)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return datetime.fromtimestamp(int(text) / 1000, tz=timezone.utc)
    return _ensure_utc(datetime.fromisoformat(text.replace("Z", "+00:00")))


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
