from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_TP_LEVELS = [round(index / 10, 1) for index in range(1, 51)]
DEFAULT_FORWARD_HOURS = 36
DEFAULT_STAGE3_MIN_MATCH_CAPTURE_PCT = 40.0
DEFAULT_STAGE3_MIN_ALL_TRADE_CAPTURE_PCT = 20.0
DEFAULT_STAGE3_FALLBACK_TP_MAX_PCT = 1.0
DEFAULT_STAGE2_TRAINING_MIN_MATCH_CAPTURE_PCT = 40.0
DEFAULT_STAGE2_WF_MIN_CAPTURE_PCT = 40.0
DEFAULT_STAGE2_WF_MAX_TRAINING_GAP_PCT = 35.0
DEFAULT_STAGE2_FULL_CYCLE_MIN_CAPTURE_PCT = 20.0


def run_stage2_capture_curve(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    tp_levels: list[float] | None = None,
    forward_hours: int = DEFAULT_FORWARD_HOURS,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    canonical_scores_path = promotion_root / "stage1a_canonical_full_cycle_scores.json"
    canonical_scores = _read_json(canonical_scores_path)
    trade_decisions = _trade_decisions(canonical_scores)
    if not trade_decisions:
        raise ValueError("Stage 2 requires a non-empty canonical Stage 1A executable trade decision set.")
    match_decisions = [decision for decision in trade_decisions if decision.get("agreement") == "MATCH"]
    if not match_decisions:
        raise ValueError("Stage 2 requires at least one MATCH Stage 1A trade decision.")

    levels = tp_levels or DEFAULT_TP_LEVELS
    signals_by_id = _index_signals(signal_rows)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])

    stage3_trade_inputs = []
    for decision in trade_decisions:
        signal = _find_signal(signals_by_id, str(decision["signal_id"]))
        if signal is None:
            raise ValueError(f"Canonical Stage 1A trade decision signal not found in signal rows: {decision['signal_id']}")
        stage3_trade_inputs.append(_stage3_trade_input(decision=decision, signal=signal))

    per_signal = []
    for decision in match_decisions:
        signal = _find_signal(signals_by_id, str(decision["signal_id"]))
        if signal is None:
            raise ValueError(f"Canonical Stage 1A trade decision signal not found in signal rows: {decision['signal_id']}")
        packet = _packet_from_signal(signal)
        capture = _walk_signal_capture(
            signal_id=str(decision["signal_id"]),
            sample_role=str(decision.get("sample_role") or "full_cycle"),
            direction=str(decision["decision_direction"]).upper(),
            agreement=str(decision.get("agreement") or "unknown").upper(),
            packet=packet,
            signal_timestamp=_coerce_datetime(packet.get("timestamp") or signal["timestamp"]),
            candles=candle_rows,
            tp_levels=levels,
            forward_hours=forward_hours,
        )
        per_signal.append(capture)

    result = _build_result(
        workspace_root=workspace_root,
        session=session,
        canonical_scores_path=canonical_scores_path,
        per_signal=per_signal,
        trade_decisions=trade_decisions,
        tp_levels=levels,
        forward_hours=forward_hours,
    )
    promotion_root.mkdir(parents=True, exist_ok=True)
    _clear_stage2_downstream_artifacts(promotion_root)
    capture_path = promotion_root / "stage2_capture_curve.json"
    per_signal_path = promotion_root / "stage2_capture_per_signal.json"
    stage3_inputs_path = promotion_root / "stage3_trade_inputs.json"
    summary_path = promotion_root / "stage2_summary.md"
    capture_path.write_text(json.dumps(result, indent=2) + "\n")
    per_signal_path.write_text(json.dumps(per_signal, indent=2) + "\n")
    stage3_inputs_path.write_text(json.dumps(stage3_trade_inputs, indent=2) + "\n")
    summary_path.write_text(_render_summary(result))
    return {
        **result,
        "capture_curve_path": str(capture_path),
        "per_signal_path": str(per_signal_path),
        "stage3_trade_inputs_path": str(stage3_inputs_path),
        "summary_path": str(summary_path),
    }


def get_reference_price(packet: dict[str, Any]) -> float:
    evidence = packet.get("evidence", {})
    if isinstance(evidence, dict):
        for key in ("trigger_candle_close", "trigger_price", "reference_price"):
            value = evidence.get(key)
            if value is not None:
                return float(value)

    interactions = packet.get("interactions", {})
    if isinstance(interactions, list):
        for timeframe in packet.get("active_timeframes", []):
            for entry in interactions:
                if entry.get("timeframe") == timeframe and entry.get("market_price") is not None:
                    return float(entry["market_price"])
        for entry in interactions:
            if entry.get("market_price") is not None:
                return float(entry["market_price"])
    elif isinstance(interactions, dict):
        for timeframe in packet.get("active_timeframes", []):
            entries = interactions.get(timeframe, [])
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])
        for entries in interactions.values():
            if entries and entries[0].get("market_price") is not None:
                return float(entries[0]["market_price"])

    for timeframe in packet.get("active_timeframes", []):
        chart = packet.get("charts", {}).get(timeframe, {})
        forming = chart.get("latest_forming_candle")
        if forming:
            if isinstance(forming, dict) and forming.get("close") is not None:
                return float(forming["close"])
            columns = chart.get("columns", [])
            if isinstance(forming, list) and "close" in columns:
                return float(forming[columns.index("close")])
    for chart in packet.get("charts", {}).values():
        forming = chart.get("latest_forming_candle")
        if isinstance(forming, dict) and forming.get("close") is not None:
            return float(forming["close"])
        columns = chart.get("columns", [])
        if isinstance(forming, list) and "close" in columns:
            return float(forming[columns.index("close")])
    raise ValueError("Signal packet has no reference price.")


def _walk_signal_capture(
    *,
    signal_id: str,
    sample_role: str,
    direction: str,
    agreement: str,
    packet: dict[str, Any],
    signal_timestamp: datetime,
    candles: list[dict[str, Any]],
    tp_levels: list[float],
    forward_hours: int,
) -> dict[str, Any]:
    reference_price = get_reference_price(packet)
    cutoff = signal_timestamp + timedelta(hours=forward_hours)
    reached = {level: False for level in tp_levels}
    first_tp_reached: float | None = None
    first_tp_timestamp: datetime | None = None
    max_favorable = 0.0
    max_adverse = 0.0

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_timestamp:
            continue
        if timestamp > cutoff:
            break
        favorable, adverse = _excursions(candle, reference_price=reference_price, direction=direction)
        max_favorable = max(max_favorable, favorable)
        max_adverse = max(max_adverse, adverse)
        for level in tp_levels:
            if reached[level]:
                continue
            target = reference_price * (1 + level / 100) if direction == "LONG" else reference_price * (1 - level / 100)
            hit = candle["high"] >= target if direction == "LONG" else candle["low"] <= target
            if hit:
                reached[level] = True
                if first_tp_reached is None:
                    first_tp_reached = level
                    first_tp_timestamp = timestamp

    return {
        "signal_id": signal_id,
        "sample_role": sample_role,
        "direction": direction,
        "decision_direction": direction,
        "agreement": agreement,
        "signal_ts": signal_timestamp.isoformat().replace("+00:00", "Z"),
        "reference_price": reference_price,
        "first_tp_reached": first_tp_reached,
        "time_to_first_tp_minutes": round((first_tp_timestamp - signal_timestamp).total_seconds() / 60, 4) if first_tp_timestamp else None,
        "max_favorable_excursion_pct": round(max_favorable, 4),
        "max_adverse_excursion_pct": round(max_adverse, 4),
        "tp_reached": {f"{level:.1f}": reached[level] for level in tp_levels},
    }


def _build_result(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    canonical_scores_path: Path,
    per_signal: list[dict[str, Any]],
    trade_decisions: list[dict[str, Any]],
    tp_levels: list[float],
    forward_hours: int,
) -> dict[str, Any]:
    results: dict[str, dict[str, dict[str, float | int]]] = {}
    roles = sorted({str(item["sample_role"]) for item in per_signal})
    for level in tp_levels:
        level_key = f"{level:.1f}"
        results[level_key] = {}
        for role in [*roles, "full_cycle"]:
            cohort = per_signal if role == "full_cycle" else [item for item in per_signal if item["sample_role"] == role]
            reached = sum(1 for item in cohort if item["tp_reached"][level_key])
            total = len(cohort)
            results[level_key][role] = {
                "reached": reached,
                "total": total,
                "rate": round(reached / total * 100, 1) if total else 0.0,
            }
    cohorts = {
        cohort: _capture_rates(
            [item for item in per_signal if cohort == "full_cycle" or item.get("agreement") == cohort],
            tp_levels=tp_levels,
        )
        for cohort in ("MATCH", "MISMATCH", "full_cycle")
    }
    recommended_tp_max = _recommended_tp_max_from_training(tp_levels=tp_levels, results=results)
    tp_guardrail = _walk_forward_guardrail(tp_levels=tp_levels, results=results)
    stage0_threshold = _load_stage0_meaningful_move_threshold(workspace_root=workspace_root, session=session)
    sl_levels = _sl_levels_from_threshold(stage0_threshold)
    sl_results = _sl_hit_rates(per_signal=per_signal, sl_levels=sl_levels)
    side_splits = _side_splits(per_signal=per_signal, tp_levels=tp_levels, sl_levels=sl_levels)

    return {
        "schema_version": "0.1",
        "stage": "stage2_travel_capture_curve",
        "artifact_role": "stage2_capture_curve",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "canonical_stage1_scores_path": str(canonical_scores_path),
        "forward_hours": forward_hours,
        "tp_levels": tp_levels,
        "sl_levels": sl_levels,
        "metrics": {
            "total_match_signals": len(per_signal),
            "total_trade_decisions": len(trade_decisions),
            "match_count": sum(1 for item in trade_decisions if item.get("agreement") == "MATCH"),
            "mismatch_count": sum(1 for item in trade_decisions if item.get("agreement") == "MISMATCH"),
            "stage2_profiled_match_count": len(per_signal),
            "slice_counts": {role: sum(1 for item in per_signal if item["sample_role"] == role) for role in roles},
        },
        "results": results,
        "sl_results": sl_results,
        "side_splits": side_splits,
        "cohorts": cohorts,
        "per_signal": per_signal,
        "stage3_input": {
            "role": "tp_range_evidence",
            "description": "Use training MATCH travel to propose TP/SL bands, with walk-forward and full-cycle capture retained as guardrails.",
            "tp_range_source": "stage2_training_match_profile_with_walk_forward_guardrail",
            "recommended_tp_min_pct": 0.1,
            "recommended_tp_max_pct": recommended_tp_max,
            "sl_range_source": "stage2_matched_adverse_profile",
            "recommended_sl_min_pct": min(sl_levels) if sl_levels else None,
            "recommended_sl_max_pct": max(sl_levels) if sl_levels else None,
            "min_match_capture_pct": DEFAULT_STAGE3_MIN_MATCH_CAPTURE_PCT,
            "walk_forward_guardrail": tp_guardrail,
            "selection_notes": {
                "primary_source": "training",
                "validation_source": "walk_forward_test",
                "robustness_source": "full_cycle",
                "walk_forward_policy": "guardrail_not_primary_optimizer",
            },
        },
    }


def _render_summary(result: dict[str, Any]) -> str:
    lines = [
        "# Stage 2 Travel Capture",
        "",
        f"Session: `{result['session_id']}`",
        f"Forward hours: {result['forward_hours']}",
        f"MATCH signals: {result['metrics']['total_match_signals']}",
        "",
        "| TP | Training | Walk-forward | Full cycle |",
        "| --- | ---: | ---: | ---: |",
    ]
    for level in result["tp_levels"]:
        key = f"{level:.1f}"
        rows = result["results"][key]
        lines.append(
            f"| {key}% | {_rate(rows, 'training')} | {_rate(rows, 'walk_forward_test')} | {_rate(rows, 'full_cycle')} |"
        )
    lines.append("")
    if result.get("sl_levels"):
        lines.extend(
            [
                "## Matched Adverse Profile",
                "",
                "| SL | Training hit | Walk-forward hit | Full cycle hit |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        for level in result["sl_levels"]:
            key = f"{level:.1f}"
            rows = result["sl_results"][key]
            lines.append(
                f"| {key}% | {_sl_rate(rows, 'training')} | {_sl_rate(rows, 'walk_forward_test')} | {_sl_rate(rows, 'full_cycle')} |"
            )
        lines.append("")
    if result.get("side_splits"):
        lines.extend(
            [
                "## Direction Split",
                "",
                "| Side | Count | TP full cycle @ recommended max | SL full cycle @ max band |",
                "| --- | ---: | ---: | ---: |",
            ]
        )
        tp_key = f"{result['stage3_input']['recommended_tp_max_pct']:.1f}"
        sl_key = f"{result['stage3_input']['recommended_sl_max_pct']:.1f}" if result["stage3_input"].get("recommended_sl_max_pct") else None
        for side in ("LONG", "SHORT"):
            split = result["side_splits"].get(side, {})
            tp_rows = (split.get("results") or {}).get(tp_key, {})
            sl_rows = (split.get("sl_results") or {}).get(sl_key, {}) if sl_key else {}
            lines.append(
                f"| {side} | {split.get('count', 0)} | {_rate(tp_rows, 'full_cycle')} | {_sl_rate(sl_rows, 'full_cycle')} |"
            )
        lines.append("")
    return "\n".join(lines)


def _rate(rows: dict[str, dict[str, Any]], role: str) -> str:
    row = rows.get(role, {"rate": 0.0, "reached": 0, "total": 0})
    return f"{row['rate']:.1f}% ({row['reached']}/{row['total']})"


def _sl_rate(rows: dict[str, dict[str, Any]], role: str) -> str:
    row = rows.get(role, {"rate": 0.0, "hit": 0, "total": 0})
    return f"{row['rate']:.1f}% ({row['hit']}/{row['total']})"


def _clear_stage2_downstream_artifacts(promotion_root: Path) -> None:
    for artifact in [
        "stage3_grid_results.json",
        "stage3_optimal.json",
        "stage3_summary.md",
        "stage3_pyramid_results.json",
        "stage3_pyramid_optimal.json",
        "stage3_pyramid_summary.md",
        "stage4_candidates.json",
        "stage4_realized_expectancy.json",
        "stage4_trade_ledger.json",
        "stage4_optimal.json",
        "stage4_summary.md",
    ]:
        (promotion_root / artifact).unlink(missing_ok=True)
    shutil.rmtree(promotion_root / "stage4_runs", ignore_errors=True)


def _trade_decisions(canonical_scores: dict[str, Any]) -> list[dict[str, Any]]:
    source = canonical_scores.get("records")
    if not isinstance(source, list) or not source:
        source = canonical_scores.get("match_set", [])
    decisions = []
    for item in source:
        if not isinstance(item, dict) or not item.get("signal_id"):
            continue
        direction = str(item.get("decision_direction") or item.get("ground_truth_direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            continue
        decisions.append(
            {
                **item,
                "decision_direction": direction,
                "agreement": _agreement(item),
            }
        )
    return decisions


def _stage3_trade_input(*, decision: dict[str, Any], signal: dict[str, Any]) -> dict[str, Any]:
    packet = _packet_from_signal(signal)
    timestamp = _coerce_datetime(packet.get("timestamp") or signal["timestamp"])
    direction = str(decision["decision_direction"]).upper()
    return {
        "signal_id": str(decision["signal_id"]),
        "sample_role": str(decision.get("sample_role") or "full_cycle"),
        "direction": direction,
        "decision_direction": direction,
        "agreement": str(decision.get("agreement") or "unknown").upper(),
        "signal_ts": timestamp.isoformat().replace("+00:00", "Z"),
        "reference_price": get_reference_price(packet),
    }


def _agreement(item: dict[str, Any]) -> str:
    agreement = str(item.get("agreement") or "").upper()
    if agreement:
        return agreement
    decision = str(item.get("decision_direction") or "").upper()
    truth = str(item.get("ground_truth_direction") or "").upper()
    if decision in {"LONG", "SHORT"} and truth in {"LONG", "SHORT"}:
        return "MATCH" if decision == truth else "MISMATCH"
    return "MATCH"


def _capture_rates(rows: list[dict[str, Any]], *, tp_levels: list[float]) -> dict[str, dict[str, float | int]]:
    rates = {}
    for level in tp_levels:
        key = f"{level:.1f}"
        reached = sum(1 for item in rows if item["tp_reached"][key])
        total = len(rows)
        rates[key] = {
            "reached": reached,
            "total": total,
            "rate": round(reached / total * 100, 1) if total else 0.0,
        }
    return rates


def _recommended_tp_max(*, tp_levels: list[float], cohorts: dict[str, dict[str, dict[str, float | int]]]) -> float:
    recommended: float | None = None
    match_rates = cohorts.get("MATCH", {})
    full_rates = cohorts.get("full_cycle", {})
    for level in tp_levels:
        key = f"{level:.1f}"
        if (
            float(match_rates.get(key, {}).get("rate", 0.0)) >= DEFAULT_STAGE3_MIN_MATCH_CAPTURE_PCT
            and float(full_rates.get(key, {}).get("rate", 0.0)) >= DEFAULT_STAGE3_MIN_ALL_TRADE_CAPTURE_PCT
        ):
            recommended = level
    return round(recommended if recommended is not None else DEFAULT_STAGE3_FALLBACK_TP_MAX_PCT, 1)


def _recommended_tp_max_from_training(*, tp_levels: list[float], results: dict[str, dict[str, dict[str, float | int]]]) -> float:
    recommended: float | None = None
    for level in tp_levels:
        key = f"{level:.1f}"
        training_rate = float(results.get(key, {}).get("training", {}).get("rate", 0.0))
        if training_rate >= DEFAULT_STAGE2_TRAINING_MIN_MATCH_CAPTURE_PCT:
            recommended = level
    return round(recommended if recommended is not None else DEFAULT_STAGE3_FALLBACK_TP_MAX_PCT, 1)


def _walk_forward_guardrail(*, tp_levels: list[float], results: dict[str, dict[str, dict[str, float | int]]]) -> dict[str, dict[str, Any]]:
    guardrail: dict[str, dict[str, Any]] = {}
    for level in tp_levels:
        key = f"{level:.1f}"
        rows = results.get(key, {})
        training = rows.get("training", {})
        walk_forward = rows.get("walk_forward_test", {})
        full_cycle = rows.get("full_cycle", {})
        training_rate = float(training.get("rate", 0.0))
        wf_total = int(walk_forward.get("total", 0))
        wf_rate = float(walk_forward.get("rate", 0.0))
        full_cycle_rate = float(full_cycle.get("rate", 0.0))
        status = "pass"
        reason = "walk_forward_capture_stable"
        if wf_total == 0:
            status = "insufficient_sample"
            reason = "missing_walk_forward_match_sample"
        elif wf_rate < DEFAULT_STAGE2_WF_MIN_CAPTURE_PCT:
            status = "fail"
            reason = "walk_forward_capture_below_guardrail"
        elif training_rate - wf_rate > DEFAULT_STAGE2_WF_MAX_TRAINING_GAP_PCT:
            status = "fail"
            reason = "walk_forward_capture_collapsed_vs_training"
        elif full_cycle_rate < DEFAULT_STAGE2_FULL_CYCLE_MIN_CAPTURE_PCT:
            status = "fail"
            reason = "full_cycle_capture_below_guardrail"
        guardrail[key] = {
            "status": status,
            "reason": reason,
            "training_rate": training_rate,
            "walk_forward_rate": wf_rate,
            "walk_forward_total": wf_total,
            "full_cycle_rate": full_cycle_rate,
        }
    return guardrail


def _sl_levels_from_threshold(threshold_pct: float) -> list[float]:
    ceiling = max(0.1, round(float(threshold_pct), 1))
    count = int(round(ceiling * 10))
    return [round(index / 10, 1) for index in range(1, count + 1)]


def _sl_hit_rates(*, per_signal: list[dict[str, Any]], sl_levels: list[float]) -> dict[str, dict[str, dict[str, float | int]]]:
    roles = sorted({str(item["sample_role"]) for item in per_signal})
    results: dict[str, dict[str, dict[str, float | int]]] = {}
    for level in sl_levels:
        level_key = f"{level:.1f}"
        results[level_key] = {}
        for role in [*roles, "full_cycle"]:
            cohort = per_signal if role == "full_cycle" else [item for item in per_signal if item["sample_role"] == role]
            hit = sum(1 for item in cohort if float(item.get("max_adverse_excursion_pct", 0.0)) >= level)
            total = len(cohort)
            results[level_key][role] = {
                "hit": hit,
                "total": total,
                "rate": round(hit / total * 100, 1) if total else 0.0,
            }
    return results


def _side_splits(
    *,
    per_signal: list[dict[str, Any]],
    tp_levels: list[float],
    sl_levels: list[float],
) -> dict[str, dict[str, Any]]:
    splits: dict[str, dict[str, Any]] = {}
    for direction in ("LONG", "SHORT"):
        rows = [item for item in per_signal if str(item.get("direction") or item.get("decision_direction")).upper() == direction]
        splits[direction] = {
            "count": len(rows),
            "results": _tp_hit_rates(per_signal=rows, tp_levels=tp_levels),
            "sl_results": _sl_hit_rates(per_signal=rows, sl_levels=sl_levels),
        }
    return splits


def _tp_hit_rates(*, per_signal: list[dict[str, Any]], tp_levels: list[float]) -> dict[str, dict[str, dict[str, float | int]]]:
    roles = sorted({str(item["sample_role"]) for item in per_signal})
    results: dict[str, dict[str, dict[str, float | int]]] = {}
    for level in tp_levels:
        level_key = f"{level:.1f}"
        results[level_key] = {}
        for role in [*roles, "full_cycle"]:
            cohort = per_signal if role == "full_cycle" else [item for item in per_signal if item["sample_role"] == role]
            reached = sum(1 for item in cohort if item["tp_reached"][level_key])
            total = len(cohort)
            results[level_key][role] = {
                "reached": reached,
                "total": total,
                "rate": round(reached / total * 100, 1) if total else 0.0,
            }
    return results


def _load_stage0_meaningful_move_threshold(*, workspace_root: Path, session: dict[str, Any]) -> float:
    root_value = session.get("stage0_artifact_root") or (session.get("manifest") or {}).get("stage0_artifact_root")
    if not root_value:
        return 1.0
    stage0_root = Path(str(root_value))
    if not stage0_root.is_absolute():
        stage0_root = workspace_root / stage0_root
    summary_path = stage0_root / "scores" / "ground_truth_summary.json"
    if not summary_path.is_file():
        return 1.0
    summary = json.loads(summary_path.read_text())
    metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else summary
    value = metrics.get("meaningful_move_threshold_pct", metrics.get("significance_threshold_pct"))
    return round(float(value), 1) if value is not None else 1.0


def _excursions(candle: dict[str, Any], *, reference_price: float, direction: str) -> tuple[float, float]:
    if direction == "LONG":
        favorable = max(0.0, (candle["high"] - reference_price) / reference_price * 100)
        adverse = max(0.0, (reference_price - candle["low"]) / reference_price * 100)
        return favorable, adverse
    favorable = max(0.0, (reference_price - candle["low"]) / reference_price * 100)
    adverse = max(0.0, (candle["high"] - reference_price) / reference_price * 100)
    return favorable, adverse


def _session_artifact_root(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"Required Stage 1 canonical score artifact not found: {path}")
    return json.loads(path.read_text())


def _index_signals(signal_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed: dict[str, dict[str, Any]] = {}
    for signal in signal_rows:
        signal_id = str(signal["signal_id"])
        indexed[signal_id] = signal
        indexed.setdefault(signal_id.split(":")[-1], signal)
    return indexed


def _find_signal(signals_by_id: dict[str, dict[str, Any]], signal_id: str) -> dict[str, Any] | None:
    return signals_by_id.get(signal_id) or signals_by_id.get(signal_id.split(":")[-1])


def _packet_from_signal(signal: dict[str, Any]) -> dict[str, Any]:
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    return {
        **payload,
        "signal_id": signal["signal_id"],
        "timestamp": payload.get("timestamp") or signal["timestamp"],
    }


def _match_direction(match: dict[str, Any]) -> str:
    direction = match.get("decision_direction") or match.get("ground_truth_direction")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Stage 2 requires LONG/SHORT MATCH directions, got {direction!r}.")
    return str(direction)


def _coerce_candle(candle: Any) -> dict[str, Any]:
    if isinstance(candle, dict):
        return {
            "timestamp": _coerce_datetime(candle.get("timestamp") or candle.get("ts")),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
        }
    return {
        "timestamp": _coerce_datetime(candle.timestamp),
        "high": float(candle.high if not isinstance(candle.high, Decimal) else str(candle.high)),
        "low": float(candle.low if not isinstance(candle.low, Decimal) else str(candle.low)),
    }


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("missing timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
