from __future__ import annotations

import bisect
import itertools
import json
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_worker.execution.atr_source import ATR_DATA_TYPE, ATRSourceError, DEFAULT_LOOKUP, DEFAULT_VALUE_FIELD, resolve_atr_policy


DEFAULT_FORWARD_HOURS = 36
DEFAULT_FEES_BPS_PER_SIDE = 5.0
DEFAULT_LEVERAGE = 5
DEFAULT_SL_MULTIPLIERS = [0.75, 1.0, 1.25]
DEFAULT_ATR_TIMEFRAMES = ("1h", "2h", "4h")
DEFAULT_ATR_TP_MULTIPLIERS = (1.5, 2.0, 2.5, 3.0)
DEFAULT_ATR_SL_MULTIPLIERS = (0.75, 1.0, 1.25, 1.5)
DEFAULT_STAGE3_MIN_FULL_CYCLE_NET_PNL_PCT = 0.0
DEFAULT_STAGE3_MIN_FULL_CYCLE_PROFIT_FACTOR = 1.0


def run_stage3_grid_search(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None = None,
    sl_values: list[float] | None = None,
    forward_hours: int | None = None,
    leverage: int = DEFAULT_LEVERAGE,
    shortlist_size: int = 5,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    run_stage3_fixed_sl_baseline(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    run_stage3_exact_protection(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    return run_stage3_local_variants(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        shortlist_size=shortlist_size,
        fees_bps_per_side=fees_bps_per_side,
        market_data_repository=market_data_repository,
    )


def run_stage3_fixed_sl_baseline(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None = None,
    forward_hours: int | None = None,
    leverage: int = DEFAULT_LEVERAGE,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
) -> dict[str, Any]:
    context = _prepare_stage3_context(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    fixed_result = _score_policy_config(
        config=_fixed_sl_config(context),
        trades=context["trade_inputs"],
        candles=context["candle_rows"],
        trade_candle_windows=context["trade_candle_windows"],
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    _clear_stage3_completion_artifacts(context["promotion_root"])
    artifact = _base_stage3_artifact(context)
    artifact.update(
        {
            "fixed_sl_complete": True,
            "exact_protection_complete": False,
            "local_variants_complete": False,
            "fixed_sl_baseline_result": fixed_result,
            "results": [fixed_result],
        }
    )
    return _write_stage3_artifact(context=context, artifact=artifact, write_optimal=False, write_candidates=False)


def run_stage3_exact_protection(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None = None,
    forward_hours: int | None = None,
    leverage: int = DEFAULT_LEVERAGE,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
) -> dict[str, Any]:
    context = _prepare_stage3_context(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    existing = _load_stage3_grid_artifact(context["promotion_root"])
    fixed_result = existing.get("fixed_sl_baseline_result") if existing else None
    if not fixed_result:
        raise ValueError("Stage 3B exact protection requires completed Stage 3A fixed SL baseline.")
    exact_result = _score_policy_config(
        config=_exact_protection_config(context),
        trades=context["trade_inputs"],
        candles=context["candle_rows"],
        trade_candle_windows=context["trade_candle_windows"],
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    _clear_stage3_completion_artifacts(context["promotion_root"])
    artifact = _base_stage3_artifact(context)
    artifact.update(
        {
            "fixed_sl_complete": True,
            "exact_protection_complete": True,
            "local_variants_complete": False,
            "fixed_sl_baseline_result": fixed_result,
            "exact_protection_result": exact_result,
            "exact_policy_result": exact_result,
            "results": [fixed_result, exact_result],
        }
    )
    return _write_stage3_artifact(context=context, artifact=artifact, write_optimal=False, write_candidates=False)


def run_stage3_local_variants(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None = None,
    forward_hours: int | None = None,
    leverage: int = DEFAULT_LEVERAGE,
    shortlist_size: int = 5,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    context = _prepare_stage3_context(
        workspace_root=workspace_root,
        session=session,
        candles=candles,
        tp_values=tp_values,
        forward_hours=forward_hours,
        leverage=leverage,
        fees_bps_per_side=fees_bps_per_side,
    )
    existing = _load_stage3_grid_artifact(context["promotion_root"])
    fixed_result = existing.get("fixed_sl_baseline_result") if existing else None
    exact_result = (existing.get("exact_protection_result") or existing.get("exact_policy_result")) if existing else None
    if not exact_result:
        raise ValueError("Stage 3C local variants requires completed Stage 3B exact protection.")
    if context["fixed_target"] is not None:
        local_configs = _fixed_target_protection_configs(
            fixed_target=context["fixed_target"],
            hard_exit_hours=context["hard_exit_hours"],
        )
    else:
        local_configs = _local_variant_configs(
            stage2_policy=context["stage2_policy"],
            base_initial_sl_pct=context["risk_policy"]["initial_sl_pct"],
            tp_levels=context["tp_levels"],
            hard_exit_hours=context["hard_exit_hours"],
        )
    atr_configs = _atr_variant_configs(
        asset=str(session.get("asset") or "").upper(),
        hard_exit_hours=context["hard_exit_hours"],
        market_data_repository=market_data_repository,
    )
    local_configs = [*local_configs, *atr_configs]
    local_results = [
        _score_policy_config(
            config=config,
            trades=context["trade_inputs"],
            candles=context["candle_rows"],
            trade_candle_windows=context["trade_candle_windows"],
            workspace_root=workspace_root,
            market_data_repository=market_data_repository,
            asset=str(session.get("asset") or "").upper(),
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
        )
        for config in local_configs
    ]
    rankable = [_with_stage3_ranking_diagnostics(row) for row in [fixed_result, exact_result, *local_results] if row]
    ranked = sorted(rankable, key=_walk_forward_ranking_key, reverse=True)
    top = ranked[:shortlist_size]
    stage4_candidates = _build_stage4_candidates(
        session=session,
        records=top,
        stage2_policy=context["stage2_policy"],
        stage0_risk_policy=context["risk_policy"],
    )
    _clear_stage3_downstream_artifacts(context["promotion_root"])
    artifact = _base_stage3_artifact(context)
    artifact.update(
        {
            "fixed_sl_complete": True,
            "exact_protection_complete": True,
            "local_variants_complete": True,
            "fixed_sl_baseline_result": fixed_result,
            "exact_protection_result": exact_result,
            "exact_policy_result": exact_result,
            "local_variant_results": local_results,
            "atr_variant_results": [row for row in local_results if row.get("exit_policy_type") == "atr_dynamic"],
            "stage3c_atr_combinations_tested": len(atr_configs),
            "stage3c_total_combinations_tested": len(local_results),
            "stage3c_value_ranges": _stage3c_value_ranges(local_results),
            "stage3c_shortlist": top,
            "results": ranked,
            "optimal": {
                "criterion": "walk_forward_net_pnl_with_full_cycle_guardrails",
                "best": ranked[0] if ranked else {},
                "top_5": top,
            },
            "stage4_candidates": stage4_candidates,
        }
    )
    return _write_stage3_artifact(context=context, artifact=artifact, write_optimal=True, write_candidates=True)


def _prepare_stage3_context(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None,
    forward_hours: int | None,
    leverage: int,
    fees_bps_per_side: float,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    trade_inputs = _load_trade_inputs(promotion_root / "stage3_trade_inputs.json")
    if not trade_inputs:
        raise ValueError("Stage 3 requires a non-empty all-trade input artifact from Stage 2.")

    stage2_policy = _load_stage2_exit_policy(promotion_root / "stage2_exit_policy.json")
    stage0_risk = _load_stage0_risk_policy(workspace_root=workspace_root, session=session)
    fixed_target = (
        _validate_fixed_target_policy(stage2_policy=stage2_policy, stage0_risk=stage0_risk)
        if stage2_policy.get("policy_source") == "signal_discovery_fixed_target"
        else None
    )
    hard_exit_hours = int(
        stage0_risk["hard_exit_hours"]
        if fixed_target is not None
        else forward_hours or stage0_risk["hard_exit_hours"]
    )
    policy_values = stage2_policy["policy"]
    risk_policy = {
        "initial_sl_pct": float(policy_values["initial_sl_pct"]),
        "stage0_meaningful_move_threshold_pct": float(stage0_risk["initial_sl_pct"]),
        "hard_exit_hours": hard_exit_hours,
    }
    tp_levels = (
        [float(fixed_target["target_pct"])]
        if fixed_target is not None
        else tp_values or _load_stage2_tp_levels(promotion_root / "stage2_capture_curve.json")
    )
    if not tp_levels:
        raise ValueError("Stage 3 requires Stage 2 TP levels from promotion/stage2_capture_curve.json.")

    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    trade_candle_windows = _build_trade_candle_windows(
        trades=trade_inputs,
        candles=candle_rows,
        hard_exit_hours=hard_exit_hours,
    )
    return {
        "workspace_root": workspace_root,
        "artifact_root": artifact_root,
        "promotion_root": promotion_root,
        "session": session,
        "trade_inputs": trade_inputs,
        "stage2_policy": stage2_policy,
        "risk_policy": risk_policy,
        "fixed_target": fixed_target,
        "hard_exit_hours": hard_exit_hours,
        "tp_levels": tp_levels,
        "candle_rows": candle_rows,
        "trade_candle_windows": trade_candle_windows,
        "leverage": leverage,
        "fees_bps_per_side": fees_bps_per_side,
    }


def _fixed_sl_config(context: dict[str, Any]) -> dict[str, Any]:
    side_policies = _stage2_config_side_policies(
        context["stage2_policy"],
        protection_enabled=False,
        hard_exit_hours=context["hard_exit_hours"],
    )
    return _policy_config(
        config_id="fixed_sl_baseline",
        stage3_step="fixed_sl_baseline",
        protection_enabled=False,
        final_tp_pct=side_policies["LONG"]["final_tp_pct"],
        protect_trigger_pct=None,
        trail_sl_pct=None,
        initial_sl_pct=side_policies["LONG"]["initial_sl_pct"],
        initial_sl_multiplier=1.0,
        hard_exit_hours=context["hard_exit_hours"],
        policy_mode=context["stage2_policy"].get("policy_mode", "shared"),
        side_policies=side_policies,
    )


def _exact_protection_config(context: dict[str, Any]) -> dict[str, Any]:
    side_policies = _stage2_config_side_policies(
        context["stage2_policy"],
        protection_enabled=True,
        hard_exit_hours=context["hard_exit_hours"],
    )
    return _policy_config(
        config_id="exact_protection_policy",
        stage3_step="exact_protection_policy",
        protection_enabled=True,
        final_tp_pct=side_policies["LONG"]["final_tp_pct"],
        protect_trigger_pct=side_policies["LONG"]["protect_trigger_pct"],
        trail_sl_pct=side_policies["LONG"]["trail_sl_pct"],
        initial_sl_pct=side_policies["LONG"]["initial_sl_pct"],
        initial_sl_multiplier=1.0,
        hard_exit_hours=context["hard_exit_hours"],
        policy_mode=context["stage2_policy"].get("policy_mode", "shared"),
        side_policies=side_policies,
    )


def _base_stage3_artifact(context: dict[str, Any]) -> dict[str, Any]:
    session = context["session"]
    risk_policy = context["risk_policy"]
    return {
        "schema_version": "0.2",
        "stage": "stage3_conditional_execution_setup",
        "artifact_role": "stage3_grid_results",
        "stage3_mode": "numerical_exit_policy",
        "policy_mode": context["stage2_policy"].get("policy_mode", "shared"),
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "total_signals": len(context["trade_inputs"]),
        "total_executable_decisions": len(context["trade_inputs"]),
        "forward_hours": context["hard_exit_hours"],
        "leverage": context["leverage"],
        "fees_bps_per_side": context["fees_bps_per_side"],
        "policy_source": (
            "signal_discovery_fixed_target"
            if context["fixed_target"] is not None
            else "stage2_exit_policy"
        ),
        "fixed_target": context["fixed_target"],
        "tp_range_source": (
            "signal_discovery_fixed_target"
            if context["fixed_target"] is not None
            else "stage2_exit_policy_adjacent_levels"
        ),
        "tp_values": context["tp_levels"],
        "sl_values": (
            [float(context["fixed_target"]["stop_pct"])]
            if context["fixed_target"] is not None
            else sorted(
                {
                    round(risk_policy["initial_sl_pct"] * multiplier, 4)
                    for multiplier in DEFAULT_SL_MULTIPLIERS
                }
            )
        ),
        "stage2_exit_policy": context["stage2_policy"],
        "stage0_risk_policy": risk_policy,
        "fixed_sl_complete": False,
        "exact_protection_complete": False,
        "local_variants_complete": False,
        "fixed_sl_baseline_result": {},
        "exact_protection_result": {},
        "exact_policy_result": {},
        "local_variant_results": [],
        "stage3c_total_combinations_tested": 0,
        "stage3c_value_ranges": {},
        "stage3c_shortlist": [],
        "results": [],
        "optimal": {"criterion": "walk_forward_net_pnl_with_full_cycle_guardrails", "best": {}, "top_5": []},
        "stage4_candidates": {"candidates": []},
    }


def _write_stage3_artifact(
    *,
    context: dict[str, Any],
    artifact: dict[str, Any],
    write_optimal: bool,
    write_candidates: bool,
) -> dict[str, Any]:
    promotion_root = context["promotion_root"]
    promotion_root.mkdir(parents=True, exist_ok=True)
    results_path = promotion_root / "stage3_grid_results.json"
    optimal_path = promotion_root / "stage3_optimal.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    summary_path = promotion_root / "stage3_summary.md"
    results_path.write_text(json.dumps(artifact, indent=2) + "\n")
    if write_optimal:
        optimal_path.write_text(json.dumps(artifact["optimal"], indent=2) + "\n")
        summary_path.write_text(_render_summary(artifact))
    if write_candidates:
        candidates_path.write_text(json.dumps(artifact["stage4_candidates"], indent=2) + "\n")
    return {
        **artifact,
        "grid_results_path": str(results_path),
        "optimal_path": str(optimal_path) if optimal_path.exists() else None,
        "stage4_candidates_path": str(candidates_path) if candidates_path.exists() else None,
        "summary_path": str(summary_path) if summary_path.exists() else None,
    }


def _score_policy_config(
    *,
    config: dict[str, Any],
    trades: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    trade_candle_windows: list[list[dict[str, Any]]] | None = None,
    workspace_root: Path | None = None,
    market_data_repository: Any | None = None,
    asset: str | None = None,
    leverage: int,
    fees_bps_per_side: float,
) -> dict[str, Any]:
    if trade_candle_windows is not None and len(trade_candle_windows) != len(trades):
        raise ValueError("Stage 3 trade candle windows must align with trade inputs.")
    outcomes = []
    for index, trade in enumerate(trades):
        trade_policy = _resolve_trade_policy(
            _policy_for_trade(config, trade),
            trade=trade,
            workspace_root=workspace_root,
            market_data_repository=market_data_repository,
            asset=asset,
        )
        trade_candles = trade_candle_windows[index] if trade_candle_windows is not None else candles
        outcomes.append(
            _simulate_policy_trade(
                trade=trade,
                candles=trade_candles,
                protection_enabled=bool(trade_policy["protection_enabled"]),
                final_tp_pct=trade_policy["final_tp_pct"],
                initial_sl_pct=trade_policy["initial_sl_pct"],
                protect_trigger_pct=trade_policy["protect_trigger_pct"],
                trail_sl_pct=trade_policy["trail_sl_pct"],
                hard_exit_hours=trade_policy["hard_exit_hours"],
                fees_bps_per_side=fees_bps_per_side,
            )
        )
    metrics = _aggregate_outcomes(outcomes)
    atr_source = config.get("atr_source") if isinstance(config.get("atr_source"), dict) else None
    return {
        **config,
        **metrics,
        "stage3_mode": "numerical_exit_policy",
        "exit_policy_type": config.get("exit_policy_type", "numerical_stage2_policy"),
        "policy_mode": config.get("policy_mode", "shared"),
        "side_policies": config.get("side_policies", {}),
        "protection_enabled": bool(config["protection_enabled"]),
        "tp": config["final_tp_pct"],
        "sl": config["initial_sl_pct"],
        "entry_model": "market",
        "rr_ratio": (
            round(float(atr_source["tp_multiplier"]) / float(atr_source["sl_multiplier"]), 4)
            if atr_source
            else round(config["final_tp_pct"] / config["initial_sl_pct"], 4) if config["initial_sl_pct"] else 0.0
        ),
        "leverage": leverage,
        "fees_bps_per_side": fees_bps_per_side,
        "outcomes": outcomes,
    }


def _build_trade_candle_windows(
    *,
    trades: list[dict[str, Any]],
    candles: list[Any],
    hard_exit_hours: int,
) -> list[list[dict[str, Any]]]:
    candle_rows = [
        candle if _is_coerced_stage3_candle(candle) else _coerce_candle(candle)
        for candle in candles
    ]
    candle_rows.sort(key=lambda row: row["timestamp"])
    timestamps = [row["timestamp"] for row in candle_rows]
    windows = []
    for trade in trades:
        signal_ts = _coerce_datetime(trade["signal_ts"])
        cutoff = signal_ts + timedelta(hours=hard_exit_hours)
        start_index = bisect.bisect_right(timestamps, signal_ts)
        end_index = bisect.bisect_right(timestamps, cutoff)
        windows.append(candle_rows[start_index:end_index])
    return windows


def _is_coerced_stage3_candle(candle: Any) -> bool:
    return isinstance(candle, dict) and isinstance(candle.get("timestamp"), datetime)


def _policy_for_trade(config: dict[str, Any], trade: dict[str, Any]) -> dict[str, Any]:
    if config.get("policy_mode") == "side_specific":
        direction = str(trade["direction"]).upper()
        return config["side_policies"][direction]
    return config


def _resolve_trade_policy(
    policy: dict[str, Any],
    *,
    trade: dict[str, Any],
    workspace_root: Path | None,
    market_data_repository: Any | None,
    asset: str | None,
) -> dict[str, Any]:
    if not isinstance(policy.get("atr_source"), dict):
        return policy
    if workspace_root is None or market_data_repository is None:
        raise ValueError("ATR Stage 3 variants require workspace_root and market_data_repository")
    try:
        resolved = resolve_atr_policy(
            policy,
            signal_timestamp=trade["signal_ts"],
            market_data_repository=market_data_repository,
            workspace_root=workspace_root,
            asset=asset,
        )
    except ATRSourceError as exc:
        raise ValueError(f"Stage 3 ATR policy resolution failed for {trade['signal_id']}: {exc}") from exc
    return resolved.policy


def _simulate_policy_trade(
    *,
    trade: dict[str, Any],
    candles: list[dict[str, Any]],
    protection_enabled: bool,
    final_tp_pct: float,
    initial_sl_pct: float,
    protect_trigger_pct: float | None,
    trail_sl_pct: float | None,
    hard_exit_hours: int,
    fees_bps_per_side: float,
) -> dict[str, Any]:
    direction = str(trade["direction"]).upper()
    reference_price = float(trade["reference_price"])
    signal_ts = _coerce_datetime(trade["signal_ts"])
    cutoff = signal_ts + timedelta(hours=hard_exit_hours)
    target_price = _target_price(reference_price, pct=final_tp_pct, direction=direction)
    active_sl = _stop_price(reference_price, pct=initial_sl_pct, direction=direction)
    protected_sl = _protected_stop_price(reference_price, pct=float(trail_sl_pct), direction=direction) if protection_enabled and trail_sl_pct is not None else None
    protect_trigger = _target_price(reference_price, pct=float(protect_trigger_pct), direction=direction) if protection_enabled and protect_trigger_pct is not None else None
    sl_kind = "initial"
    protection_active = False
    last_close = reference_price
    last_timestamp = signal_ts
    exit_price = reference_price
    exit_timestamp = signal_ts
    outcome = "TIME_EXIT"

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_ts:
            continue
        if timestamp > cutoff:
            break
        last_close = float(candle["close"])
        last_timestamp = timestamp
        tp_hit, sl_hit = _tp_sl_hit(candle, tp=target_price, sl=active_sl, direction=direction)
        if tp_hit or sl_hit:
            if tp_hit and sl_hit:
                body = candle["close"] - candle["open"]
                tp_first = _body_favors_direction(body, direction=direction)
            else:
                tp_first = tp_hit
            if tp_first:
                outcome = "TP"
                exit_price = target_price
            else:
                outcome = "PROTECTED_SL" if sl_kind == "protected" else "INITIAL_SL"
                exit_price = active_sl
            exit_timestamp = timestamp
            break
        if protection_enabled and not protection_active and protect_trigger is not None and protected_sl is not None and _price_hit(candle, protect_trigger, direction=direction):
            protection_active = True
            active_sl = protected_sl
            sl_kind = "protected"
    else:
        exit_price = last_close
        exit_timestamp = last_timestamp

    if outcome == "TIME_EXIT":
        exit_price = last_close
        exit_timestamp = last_timestamp

    gross = _pnl_pct(reference_price, exit_price, direction=direction)
    fees = fees_bps_per_side * 2 / 100
    net = gross - fees
    return {
        "signal_id": trade["signal_id"],
        "sample_role": trade.get("sample_role", "full_cycle"),
        "direction": direction,
        "agreement": trade.get("agreement", "UNKNOWN"),
        "outcome": outcome,
        "entry_price": reference_price,
        "exit_price": round(exit_price, 10),
        "signal_ts": signal_ts.isoformat().replace("+00:00", "Z"),
        "exit_ts": exit_timestamp.isoformat().replace("+00:00", "Z"),
        "gross_pnl_pct": round(gross, 4),
        "fees_pct": round(fees, 4),
        "net_pnl_pct": round(net, 4),
        "protection_enabled": protection_enabled,
        "protection_activated": protection_active,
    }


def _aggregate_outcomes(outcomes: list[dict[str, Any]]) -> dict[str, Any]:
    base = _empty_split()
    for outcome in outcomes:
        _add_outcome(base, outcome)
    slice_split: dict[str, dict[str, Any]] = {}
    side_split: dict[str, dict[str, Any]] = {}
    agreement_split: dict[str, dict[str, Any]] = {}
    for outcome in outcomes:
        _add_outcome(slice_split.setdefault(str(outcome["sample_role"]), _empty_split()), outcome)
        _add_outcome(side_split.setdefault(str(outcome["direction"]), _empty_split()), outcome)
        _add_outcome(agreement_split.setdefault(str(outcome["agreement"]), _empty_split()), outcome)
    return {
        **_finalize_split(base),
        "slice_split": {key: _finalize_split(value) for key, value in slice_split.items()},
        "side_split": {key: _finalize_split(value) for key, value in side_split.items()},
        "agreement_split": {key: _finalize_split(value) for key, value in agreement_split.items()},
        "mismatch_split": {key: _finalize_split(value) for key, value in agreement_split.items()},
    }


def _empty_split() -> dict[str, Any]:
    return {
        "total": 0,
        "tp_count": 0,
        "initial_sl_count": 0,
        "protected_sl_count": 0,
        "time_exit_count": 0,
        "gross_pnl_pct": 0.0,
        "net_pnl_pct": 0.0,
        "fees_pct": 0.0,
        "gross_profit_pct": 0.0,
        "gross_loss_pct": 0.0,
        "wins": 0,
    }


def _add_outcome(target: dict[str, Any], outcome: dict[str, Any]) -> None:
    net = float(outcome["net_pnl_pct"])
    target["total"] += 1
    if outcome["outcome"] == "TP":
        target["tp_count"] += 1
    elif outcome["outcome"] == "INITIAL_SL":
        target["initial_sl_count"] += 1
    elif outcome["outcome"] == "PROTECTED_SL":
        target["protected_sl_count"] += 1
    else:
        target["time_exit_count"] += 1
    target["gross_pnl_pct"] += float(outcome["gross_pnl_pct"])
    target["net_pnl_pct"] += net
    target["fees_pct"] += float(outcome["fees_pct"])
    if net > 0:
        target["gross_profit_pct"] += net
        target["wins"] += 1
    elif net < 0:
        target["gross_loss_pct"] += abs(net)


def _finalize_split(split: dict[str, Any]) -> dict[str, Any]:
    total = int(split["total"])
    sl_count = int(split["initial_sl_count"]) + int(split["protected_sl_count"])
    net = float(split["net_pnl_pct"])
    gross = float(split["gross_pnl_pct"])
    loss = float(split["gross_loss_pct"])
    profit_factor = 999.0 if loss == 0 and split["gross_profit_pct"] > 0 else (split["gross_profit_pct"] / loss if loss else 0.0)
    return {
        "total": total,
        "tp_count": int(split["tp_count"]),
        "initial_sl_count": int(split["initial_sl_count"]),
        "protected_sl_count": int(split["protected_sl_count"]),
        "time_exit_count": int(split["time_exit_count"]),
        "sl_count": sl_count,
        "neither": int(split["time_exit_count"]),
        "wr": round(split["wins"] / total * 100, 4) if total else 0.0,
        "expectancy": round(net / total, 4) if total else 0.0,
        "gross_pnl_pct": round(gross, 4),
        "net_pnl_pct": round(net, 4),
        "pnl_pct": round(net, 4),
        "fees_pct": round(float(split["fees_pct"]), 4),
        "profit_factor": round(profit_factor, 4),
    }


def _local_variant_configs(
    *,
    stage2_policy: dict[str, Any],
    base_initial_sl_pct: float,
    tp_levels: list[float],
    hard_exit_hours: int,
) -> list[dict[str, Any]]:
    if stage2_policy.get("policy_mode") == "side_specific":
        return _paired_side_specific_variant_configs(
            stage2_policy=stage2_policy,
            tp_levels=tp_levels,
            hard_exit_hours=hard_exit_hours,
        )
    policy = stage2_policy["policy"]
    lock_values = _adjacent_values(tp_levels, float(policy["lock_profit_pct"]))
    protect_values = _adjacent_values(tp_levels, float(policy["protect_trigger_pct"]))
    trail_values = _adjacent_values(tp_levels, float(policy["trail_sl_pct"]))
    exact_protected_key = (
        round(float(policy["lock_profit_pct"]), 10),
        round(float(policy["protect_trigger_pct"]), 10),
        round(float(policy["trail_sl_pct"]), 10),
        1.0,
    )
    exact_fixed_key = (round(float(policy["lock_profit_pct"]), 10), 1.0)
    configs = []
    seen = set()
    for final_tp, multiplier in itertools.product(lock_values, DEFAULT_SL_MULTIPLIERS):
        key = ("fixed", round(final_tp, 10), multiplier)
        if (round(final_tp, 10), multiplier) == exact_fixed_key or key in seen:
            continue
        seen.add(key)
        initial_sl = round(base_initial_sl_pct * multiplier, 4)
        configs.append(
            _policy_config(
                config_id=f"fixed_variant_tp_{_id_pct(final_tp)}_sl_{_id_pct(initial_sl)}",
                stage3_step="fixed_sl_variant",
                protection_enabled=False,
                final_tp_pct=final_tp,
                protect_trigger_pct=None,
                trail_sl_pct=None,
                initial_sl_pct=initial_sl,
                initial_sl_multiplier=multiplier,
                hard_exit_hours=hard_exit_hours,
            )
        )
    for final_tp, protect, trail, multiplier in itertools.product(lock_values, protect_values, trail_values, DEFAULT_SL_MULTIPLIERS):
        if trail > protect:
            continue
        key = ("protected", round(final_tp, 10), round(protect, 10), round(trail, 10), multiplier)
        if key[1:] == exact_protected_key or key in seen:
            continue
        seen.add(key)
        initial_sl = round(base_initial_sl_pct * multiplier, 4)
        configs.append(
            _policy_config(
                config_id=f"variant_tp_{_id_pct(final_tp)}_sl_{_id_pct(initial_sl)}_protect_{_id_pct(protect)}_trail_{_id_pct(trail)}",
                stage3_step="local_variant",
                protection_enabled=True,
                final_tp_pct=final_tp,
                protect_trigger_pct=protect,
                trail_sl_pct=trail,
                initial_sl_pct=initial_sl,
                initial_sl_multiplier=multiplier,
                hard_exit_hours=hard_exit_hours,
            )
        )
    return configs


def _fixed_target_protection_configs(
    *,
    fixed_target: dict[str, Any],
    hard_exit_hours: int,
) -> list[dict[str, Any]]:
    target_pct = float(fixed_target["target_pct"])
    stop_pct = float(fixed_target["stop_pct"])
    protect_values = sorted(
        {
            round(target_pct * 0.25, 4),
            round(target_pct * 0.5, 4),
            round(target_pct * 0.75, 4),
        }
    )
    trail_values = sorted({0.0, round(stop_pct * 0.5, 4), round(stop_pct, 4)})
    exact_key = (round(stop_pct, 4), round(stop_pct, 4))
    configs = []
    for protect, trail in itertools.product(protect_values, trail_values):
        if trail > protect or (protect, trail) == exact_key:
            continue
        configs.append(
            _policy_config(
                config_id=(
                    f"fixed_target_protection_trigger_{_id_pct(protect)}_"
                    f"trail_{_id_pct(trail)}"
                ),
                stage3_step="fixed_target_protection_variant",
                protection_enabled=True,
                final_tp_pct=target_pct,
                protect_trigger_pct=protect,
                trail_sl_pct=trail,
                initial_sl_pct=stop_pct,
                initial_sl_multiplier=1.0,
                hard_exit_hours=hard_exit_hours,
            )
        )
    return configs


def _atr_variant_configs(
    *,
    asset: str,
    hard_exit_hours: int,
    market_data_repository: Any | None,
) -> list[dict[str, Any]]:
    if market_data_repository is None or not asset:
        return []
    refs = _available_atr_refs(asset=asset, market_data_repository=market_data_repository)
    configs: list[dict[str, Any]] = []
    for timeframe, ref in refs.items():
        descriptor = ref.get("schema_descriptor") if isinstance(ref.get("schema_descriptor"), dict) else {}
        period = descriptor.get("period", 14)
        for tp_multiplier, sl_multiplier in itertools.product(DEFAULT_ATR_TP_MULTIPLIERS, DEFAULT_ATR_SL_MULTIPLIERS):
            config = _policy_config(
                config_id=f"atr_{timeframe}_tp_{_id_pct(tp_multiplier)}x_sl_{_id_pct(sl_multiplier)}x",
                stage3_step="atr_dynamic_variant",
                protection_enabled=False,
                final_tp_pct=float(tp_multiplier),
                protect_trigger_pct=None,
                trail_sl_pct=None,
                initial_sl_pct=float(sl_multiplier),
                initial_sl_multiplier=1.0,
                hard_exit_hours=hard_exit_hours,
            )
            config["exit_policy_type"] = "atr_dynamic"
            config["atr_source"] = {
                "enabled": True,
                "dataset_id": ref.get("dataset_id"),
                "data_type": ATR_DATA_TYPE,
                "origin": ref.get("data_origin") or "derived",
                "timeframe": timeframe,
                "period": period,
                "value_field": DEFAULT_VALUE_FIELD,
                "lookup": DEFAULT_LOOKUP,
                "tp_multiplier": float(tp_multiplier),
                "sl_multiplier": float(sl_multiplier),
            }
            config["atr_variant"] = {
                "timeframe": timeframe,
                "period": period,
                "tp_multiplier": float(tp_multiplier),
                "sl_multiplier": float(sl_multiplier),
                "dataset_id": ref.get("dataset_id"),
            }
            configs.append(config)
    return configs


def _available_atr_refs(*, asset: str, market_data_repository: Any) -> dict[str, dict[str, Any]]:
    getter = getattr(market_data_repository, "get_data_ref", None)
    if not callable(getter):
        return {}
    refs: dict[str, dict[str, Any]] = {}
    for timeframe in DEFAULT_ATR_TIMEFRAMES:
        ref = getter(asset=asset, timeframe=timeframe, origin="derived", data_type=ATR_DATA_TYPE)
        if isinstance(ref, dict):
            refs[timeframe] = ref
    return refs


def _paired_side_specific_variant_configs(
    *,
    stage2_policy: dict[str, Any],
    tp_levels: list[float],
    hard_exit_hours: int,
) -> list[dict[str, Any]]:
    base = _stage2_config_side_policies(stage2_policy, protection_enabled=True, hard_exit_hours=hard_exit_hours)
    offsets = [-1, 0, 1]
    protected_offset_profiles = [
        (-1, 0, 0),
        (0, -1, 0),
        (0, 0, -1),
        (0, 0, 1),
        (0, 1, 0),
        (1, 0, 0),
        (-1, -1, -1),
        (1, 1, 1),
        (-1, 1, 0),
        (1, -1, 0),
    ]
    configs = []
    seen = set()
    for tp_offset, multiplier in itertools.product(offsets, DEFAULT_SL_MULTIPLIERS):
        side_policies = {
            side: _side_variant_policy(
                policy=base[side],
                tp_levels=tp_levels,
                hard_exit_hours=hard_exit_hours,
                protection_enabled=False,
                tp_offset=tp_offset,
                protect_offset=0,
                trail_offset=0,
                sl_multiplier=multiplier,
            )
            for side in ("LONG", "SHORT")
        }
        key = _side_policy_key("fixed", side_policies)
        exact = tp_offset == 0 and multiplier == 1.0
        if exact or key in seen:
            continue
        seen.add(key)
        configs.append(_side_specific_config(config_id=f"fixed_side_variant_tp_offset_{tp_offset}_slx_{_id_pct(multiplier)}", stage3_step="fixed_sl_variant", side_policies=side_policies, hard_exit_hours=hard_exit_hours))
    for tp_offset, protect_offset, trail_offset in protected_offset_profiles:
        for multiplier in DEFAULT_SL_MULTIPLIERS:
            side_policies = {
                side: _side_variant_policy(
                    policy=base[side],
                    tp_levels=tp_levels,
                    hard_exit_hours=hard_exit_hours,
                    protection_enabled=True,
                    tp_offset=tp_offset,
                    protect_offset=protect_offset,
                    trail_offset=trail_offset,
                    sl_multiplier=multiplier,
                )
                for side in ("LONG", "SHORT")
            }
            if any(policy["trail_sl_pct"] > policy["protect_trigger_pct"] for policy in side_policies.values()):
                continue
            key = _side_policy_key("protected", side_policies)
            exact = tp_offset == 0 and protect_offset == 0 and trail_offset == 0 and multiplier == 1.0
            if exact or key in seen:
                continue
            seen.add(key)
            configs.append(
                _side_specific_config(
                    config_id=f"side_variant_tp_{tp_offset}_protect_{protect_offset}_trail_{trail_offset}_slx_{_id_pct(multiplier)}",
                    stage3_step="local_variant",
                    side_policies=side_policies,
                    hard_exit_hours=hard_exit_hours,
                )
            )
    return configs


def _stage2_config_side_policies(
    stage2_policy: dict[str, Any],
    *,
    protection_enabled: bool,
    hard_exit_hours: int,
) -> dict[str, dict[str, Any]]:
    policy_mode = stage2_policy.get("policy_mode", "shared")
    if policy_mode == "side_specific":
        source = stage2_policy.get("side_policies")
        if not isinstance(source, dict):
            raise ValueError("Stage 2 side-specific exit policy is missing side_policies.")
    else:
        policy = stage2_policy.get("policy")
        if not isinstance(policy, dict):
            raise ValueError("Stage 2 exit policy artifact is missing policy values.")
        source = {"LONG": policy, "SHORT": policy}

    side_policies: dict[str, dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        policy = source.get(side)
        if not isinstance(policy, dict):
            raise ValueError(f"Stage 2 exit policy is missing {side} side policy.")
        side_policies[side] = _stage2_config_side_policy(
            side=side,
            policy=policy,
            protection_enabled=protection_enabled,
            hard_exit_hours=hard_exit_hours,
        )
    return side_policies


def _stage2_config_side_policy(
    *,
    side: str,
    policy: dict[str, Any],
    protection_enabled: bool,
    hard_exit_hours: int,
) -> dict[str, Any]:
    for key in ("lock_profit_pct", "initial_sl_pct"):
        if policy.get(key) is None:
            raise ValueError(f"Stage 2 {side} exit policy is missing {key}.")
    if protection_enabled:
        for key in ("protect_trigger_pct", "trail_sl_pct"):
            if policy.get(key) is None:
                raise ValueError(f"Stage 2 {side} exit policy is missing {key}.")
    final_tp = float(policy["lock_profit_pct"])
    return {
        "protection_enabled": bool(protection_enabled),
        "final_tp_pct": final_tp,
        "lock_profit_pct": final_tp,
        "protect_trigger_pct": float(policy["protect_trigger_pct"]) if protection_enabled else None,
        "trail_sl_pct": float(policy["trail_sl_pct"]) if protection_enabled else None,
        "initial_sl_pct": float(policy["initial_sl_pct"]),
        "initial_sl_multiplier": 1.0,
        "hard_exit_hours": hard_exit_hours,
    }


def _round_side_policies(side_policies: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    rounded: dict[str, dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        policy = side_policies[side]
        rounded[side] = {
            "protection_enabled": bool(policy["protection_enabled"]),
            "final_tp_pct": round(float(policy["final_tp_pct"]), 4),
            "lock_profit_pct": round(float(policy["lock_profit_pct"]), 4),
            "protect_trigger_pct": round(float(policy["protect_trigger_pct"]), 4) if policy.get("protect_trigger_pct") is not None else None,
            "trail_sl_pct": round(float(policy["trail_sl_pct"]), 4) if policy.get("trail_sl_pct") is not None else None,
            "initial_sl_pct": round(float(policy["initial_sl_pct"]), 4),
            "initial_sl_multiplier": float(policy.get("initial_sl_multiplier", 1.0)),
            "hard_exit_hours": int(policy["hard_exit_hours"]),
        }
    return rounded


def _side_variant_policy(
    *,
    policy: dict[str, Any],
    tp_levels: list[float],
    hard_exit_hours: int,
    protection_enabled: bool,
    tp_offset: int,
    protect_offset: int,
    trail_offset: int,
    sl_multiplier: float,
) -> dict[str, Any]:
    final_tp = _offset_value(tp_levels, float(policy["final_tp_pct"]), tp_offset)
    protect = _offset_value(tp_levels, float(policy["protect_trigger_pct"] or final_tp), protect_offset) if protection_enabled else None
    trail = _offset_value(tp_levels, float(policy["trail_sl_pct"] or protect or final_tp), trail_offset) if protection_enabled else None
    initial_sl = round(float(policy["initial_sl_pct"]) * sl_multiplier, 4)
    return {
        "protection_enabled": protection_enabled,
        "final_tp_pct": final_tp,
        "lock_profit_pct": final_tp,
        "protect_trigger_pct": protect,
        "trail_sl_pct": trail,
        "initial_sl_pct": initial_sl,
        "initial_sl_multiplier": sl_multiplier,
        "hard_exit_hours": hard_exit_hours,
    }


def _side_specific_config(*, config_id: str, stage3_step: str, side_policies: dict[str, dict[str, Any]], hard_exit_hours: int) -> dict[str, Any]:
    long_policy = side_policies["LONG"]
    protection_enabled = any(bool(policy["protection_enabled"]) for policy in side_policies.values())
    return _policy_config(
        config_id=config_id,
        stage3_step=stage3_step,
        protection_enabled=protection_enabled,
        final_tp_pct=long_policy["final_tp_pct"],
        protect_trigger_pct=long_policy["protect_trigger_pct"],
        trail_sl_pct=long_policy["trail_sl_pct"],
        initial_sl_pct=long_policy["initial_sl_pct"],
        initial_sl_multiplier=long_policy["initial_sl_multiplier"],
        hard_exit_hours=hard_exit_hours,
        policy_mode="side_specific",
        side_policies=side_policies,
    )


def _offset_value(levels: list[float], selected: float, offset: int) -> float:
    ordered = sorted({round(float(level), 4) for level in levels})
    selected = round(float(selected), 4)
    if selected not in ordered:
        ordered.append(selected)
        ordered.sort()
    index = max(0, min(len(ordered) - 1, ordered.index(selected) + offset))
    return ordered[index]


def _side_policy_key(mode: str, side_policies: dict[str, dict[str, Any]]) -> tuple[Any, ...]:
    return (
        mode,
        *(
            (
                side,
                round(float(side_policies[side]["final_tp_pct"]), 10),
                round(float(side_policies[side]["initial_sl_pct"]), 10),
                side_policies[side]["protect_trigger_pct"],
                side_policies[side]["trail_sl_pct"],
                side_policies[side]["initial_sl_multiplier"],
            )
            for side in ("LONG", "SHORT")
        ),
    )


def _policy_config(
    *,
    config_id: str,
    stage3_step: str,
    protection_enabled: bool,
    final_tp_pct: float,
    protect_trigger_pct: float | None,
    trail_sl_pct: float | None,
    initial_sl_pct: float,
    initial_sl_multiplier: float,
    hard_exit_hours: int,
    policy_mode: str = "shared",
    side_policies: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    config = {
        "config_id": config_id,
        "stage3_step": stage3_step,
        "policy_mode": policy_mode,
        "protection_enabled": protection_enabled,
        "final_tp_pct": round(final_tp_pct, 4),
        "lock_profit_pct": round(final_tp_pct, 4),
        "protect_trigger_pct": round(protect_trigger_pct, 4) if protect_trigger_pct is not None else None,
        "trail_sl_pct": round(trail_sl_pct, 4) if trail_sl_pct is not None else None,
        "initial_sl_pct": round(initial_sl_pct, 4),
        "initial_sl_multiplier": initial_sl_multiplier,
        "hard_exit_hours": hard_exit_hours,
    }
    if side_policies is not None:
        config["side_policies"] = _round_side_policies(side_policies)
    return config


def _load_trade_inputs(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"Stage 3 requires all-trade input artifact: {path}")
    payload = json.loads(path.read_text())
    if not isinstance(payload, list):
        raise ValueError("Stage 3 all-trade input artifact must be a JSON list.")
    trades = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        direction = str(item.get("decision_direction") or "").upper()
        if direction not in {"LONG", "SHORT"}:
            raise ValueError("Stage 3 requires an all-trade input artifact with decision_direction for every row.")
        if item.get("reference_price") is None or item.get("signal_ts") is None or item.get("signal_id") is None:
            raise ValueError("Stage 3 all-trade input rows require signal_id, signal_ts, and reference_price.")
        trades.append(
            {
                **item,
                "direction": direction,
                "decision_direction": direction,
                "agreement": str(item.get("agreement") or "UNKNOWN").upper(),
            }
        )
    return trades


def _load_stage2_exit_policy(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError("Stage 3 requires a promoted Stage 2 exit policy at promotion/stage2_exit_policy.json.")
    payload = json.loads(path.read_text())
    policy = payload.get("policy") if isinstance(payload, dict) else None
    if not isinstance(policy, dict):
        raise ValueError("Stage 2 exit policy artifact is missing policy values.")
    for key in ("lock_profit_pct", "initial_sl_pct", "protect_trigger_pct", "trail_sl_pct"):
        if policy.get(key) is None:
            raise ValueError(f"Stage 2 exit policy is missing {key}.")
    return payload


def _validate_fixed_target_policy(
    *,
    stage2_policy: dict[str, Any],
    stage0_risk: dict[str, Any],
) -> dict[str, Any]:
    if stage0_risk.get("policy_source") != "signal_discovery_fixed_target":
        raise ValueError("Stage 2 fixed-target policy requires a Stage 0 frozen target contract.")
    fixed_target = stage2_policy.get("fixed_target")
    if not isinstance(fixed_target, dict):
        raise ValueError("Stage 2 fixed-target policy is missing fixed_target metadata.")
    missing = sorted(
        {"target_pct", "stop_pct", "forward_hours"} - fixed_target.keys()
    )
    if missing:
        raise ValueError(
            f"Stage 2 fixed-target policy is missing fields: {', '.join(missing)}"
        )
    policy = stage2_policy["policy"]
    expected_target = float(stage0_risk["target_pct"])
    expected_stop = float(stage0_risk["initial_sl_pct"])
    expected_hours = int(stage0_risk["hard_exit_hours"])
    if float(policy["lock_profit_pct"]) != expected_target:
        raise ValueError("Stage 2 fixed-target TP does not match the frozen Stage 0 contract.")
    if float(policy["initial_sl_pct"]) != expected_stop:
        raise ValueError("Stage 2 fixed-target SL does not match the frozen Stage 0 contract.")
    if float(fixed_target.get("target_pct")) != expected_target:
        raise ValueError("Stage 2 fixed_target target_pct does not match Stage 0.")
    if float(fixed_target.get("stop_pct")) != expected_stop:
        raise ValueError("Stage 2 fixed_target stop_pct does not match Stage 0.")
    if int(fixed_target.get("forward_hours")) != expected_hours:
        raise ValueError("Stage 2 fixed_target horizon does not match Stage 0.")
    return {
        "config_hash": str(stage0_risk.get("config_hash") or ""),
        "target_pct": expected_target,
        "stop_pct": expected_stop,
        "forward_hours": expected_hours,
    }


def _load_stage0_risk_policy(*, workspace_root: Path, session: dict[str, Any]) -> dict[str, Any]:
    root_value = session.get("stage0_artifact_root") or (session.get("manifest") or {}).get("stage0_artifact_root")
    if not root_value:
        raise ValueError("Stage 3 requires Stage 0 artifact root to read risk policy.")
    stage0_root = Path(str(root_value))
    if not stage0_root.is_absolute():
        stage0_root = workspace_root / stage0_root
    summary_path = stage0_root / "scores" / "ground_truth_summary.json"
    if not summary_path.is_file():
        raise ValueError(f"Stage 0 ground truth summary not found: {summary_path}")
    summary = json.loads(summary_path.read_text())
    if summary.get("label_contract") == "fixed_r_first_touch.v1":
        contract_path_value = summary.get("fixed_target_contract_path")
        contract_path = (
            Path(str(contract_path_value))
            if contract_path_value
            else stage0_root / "scores" / "fixed_target_contract.json"
        )
        if not contract_path.is_absolute():
            contract_path = stage0_root / contract_path
        if not contract_path.is_file():
            raise ValueError(f"Fixed target contract not found: {contract_path}")
        contract = json.loads(contract_path.read_text())
        selected = contract.get("selected_target") if isinstance(contract, dict) else None
        if not isinstance(selected, dict):
            raise ValueError("Fixed target contract is missing selected_target.")
        risk_pct = float(selected["selected_risk_pct"])
        return {
            "initial_sl_pct": risk_pct * float(selected["stop_multiple"]),
            "target_pct": risk_pct * float(selected["reward_multiple"]),
            "hard_exit_hours": int(float(selected["horizon_hours"])),
            "policy_source": "signal_discovery_fixed_target",
            "config_hash": str(contract.get("config_hash") or ""),
            "contract_path": str(contract_path),
        }
    metrics = summary.get("metrics") if isinstance(summary.get("metrics"), dict) else summary
    threshold = metrics.get("significance_threshold_pct")
    forward_hours = metrics.get("forward_hours")
    if threshold is None or forward_hours is None:
        raise ValueError("Stage 0 ground truth summary must include significance_threshold_pct and forward_hours.")
    return {
        "initial_sl_pct": float(threshold),
        "hard_exit_hours": int(forward_hours),
    }


def _load_stage2_tp_levels(path: Path) -> list[float]:
    if not path.is_file():
        raise ValueError(f"Stage 2 capture curve artifact not found: {path}")
    payload = json.loads(path.read_text())
    values = payload.get("tp_levels")
    if not isinstance(values, list) or not values:
        values = list((payload.get("results") or {}).keys())
    levels = sorted({round(float(value), 4) for value in values})
    return levels


def _adjacent_values(levels: list[float], selected: float) -> list[float]:
    ordered = sorted({round(float(level), 4) for level in levels})
    selected = round(float(selected), 4)
    if selected not in ordered:
        ordered.append(selected)
        ordered.sort()
    index = ordered.index(selected)
    values = [selected]
    if index > 0:
        values.append(ordered[index - 1])
    if index < len(ordered) - 1:
        values.append(ordered[index + 1])
    return sorted(values)


def _stage3c_value_ranges(local_results: list[dict[str, Any]]) -> dict[str, Any]:
    atr_rows = [row for row in local_results if isinstance(row.get("atr_source"), dict)]
    return {
        "final_tp_pct": sorted({row["final_tp_pct"] for row in local_results}),
        "protect_trigger_pct": sorted({row["protect_trigger_pct"] for row in local_results if row.get("protect_trigger_pct") is not None}),
        "trail_sl_pct": sorted({row["trail_sl_pct"] for row in local_results if row.get("trail_sl_pct") is not None}),
        "initial_sl_pct": sorted({row["initial_sl_pct"] for row in local_results}),
        "initial_sl_multipliers": sorted({row["initial_sl_multiplier"] for row in local_results}),
        "atr_timeframes": sorted({str(row["atr_source"].get("timeframe")) for row in atr_rows}),
        "atr_tp_multipliers": sorted({float(row["atr_source"].get("tp_multiplier")) for row in atr_rows}),
        "atr_sl_multipliers": sorted({float(row["atr_source"].get("sl_multiplier")) for row in atr_rows}),
    }


def _build_stage4_candidates(
    *,
    session: dict[str, Any],
    records: list[dict[str, Any]],
    stage2_policy: dict[str, Any],
    stage0_risk_policy: dict[str, Any],
) -> dict[str, Any]:
    candidates = []
    for row in records:
        candidate_id = _candidate_id(row)
        setup = {
            "entry_model": "market",
            "tp_pct": row["final_tp_pct"],
            "sl_pct": row["initial_sl_pct"],
            "final_tp_pct": row["final_tp_pct"],
            "lock_profit_pct": row["lock_profit_pct"],
            "initial_sl_pct": row["initial_sl_pct"],
            "initial_sl_multiplier": row["initial_sl_multiplier"],
            "protection_enabled": bool(row["protection_enabled"]),
            "hard_exit_hours": row["hard_exit_hours"],
            "max_hold_hours": row["hard_exit_hours"],
            "timeout_policy": "close_at_cutoff",
            "exit_policy_type": "numerical_stage2_policy",
        }
        if isinstance(row.get("atr_source"), dict):
            setup["atr_source"] = row["atr_source"]
            setup["exit_policy_type"] = "atr_dynamic"
            if isinstance(row.get("atr_variant"), dict):
                setup["atr_variant"] = row["atr_variant"]
        if row["protection_enabled"]:
            setup["protect_trigger_pct"] = row["protect_trigger_pct"]
            setup["trail_sl_pct"] = row["trail_sl_pct"]
        if row.get("policy_mode") == "side_specific":
            setup["policy_mode"] = "side_specific"
            setup["side_policies"] = row.get("side_policies", {})
        candidates.append(
            {
                "candidate_id": candidate_id,
                "setup": setup,
                "stage3_metrics": {
                    "stage3_mode": row["stage3_mode"],
                    "stage3_step": row["stage3_step"],
                    "wr": row["wr"],
                    "profit_factor": row["profit_factor"],
                    "gross_pnl_pct": row["gross_pnl_pct"],
                    "net_pnl_pct": row["net_pnl_pct"],
                    "fees_pct": row["fees_pct"],
                    "protection_enabled": bool(row["protection_enabled"]),
                    "tp_count": row["tp_count"],
                    "initial_sl_count": row["initial_sl_count"],
                    "protected_sl_count": row["protected_sl_count"],
                    "time_exit_count": row["time_exit_count"],
                },
                "selection_diagnostics": {
                    "stage3_ranking": {
                        "criterion": "walk_forward_net_pnl_with_full_cycle_guardrails",
                        **(row.get("ranking_diagnostics") or {}),
                    }
                },
            }
        )
    return {
        "schema_version": "0.2",
        "artifact_role": "stage4_candidates",
        "source_stage": "stage3_conditional_execution_setup",
        "stage3_mode": "numerical_exit_policy",
        "session_id": session["session_id"],
        "strategy_id": session.get("strategy_id"),
        "asset": session.get("asset"),
        "stage2_exit_policy": stage2_policy,
        "stage0_risk_policy": stage0_risk_policy,
        "candidates": candidates,
    }


def _clear_stage3_downstream_artifacts(promotion_root: Path) -> None:
    for artifact in [
        "stage3_pyramid_results.json",
        "stage3_pyramid_optimal.json",
        "stage3_pyramid_summary.md",
        "stage4_realized_expectancy.json",
        "stage4_trade_ledger.json",
        "stage4_optimal.json",
        "stage4_summary.md",
    ]:
        (promotion_root / artifact).unlink(missing_ok=True)
    shutil.rmtree(promotion_root / "stage4_runs", ignore_errors=True)


def _clear_stage3_completion_artifacts(promotion_root: Path) -> None:
    for artifact in [
        "stage3_optimal.json",
        "stage4_candidates.json",
        "stage3_summary.md",
    ]:
        (promotion_root / artifact).unlink(missing_ok=True)
    _clear_stage3_downstream_artifacts(promotion_root)


def _load_stage3_grid_artifact(promotion_root: Path) -> dict[str, Any]:
    path = promotion_root / "stage3_grid_results.json"
    if not path.is_file():
        return {}
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else {}


def _render_summary(artifact: dict[str, Any]) -> str:
    best = artifact["optimal"]["best"]
    fixed = artifact["fixed_sl_baseline_result"]
    exact = artifact["exact_protection_result"]
    lines = [
        "# Stage 3 Numerical Exit Policy",
        "",
        f"Session: `{artifact['session_id']}`",
        f"Executable decisions: {artifact['total_executable_decisions']}",
        f"Initial SL from Stage 2: {artifact['stage0_risk_policy']['initial_sl_pct']:.2f}%",
        f"Stage 0 meaningful move: {artifact['stage0_risk_policy'].get('stage0_meaningful_move_threshold_pct', artifact['stage0_risk_policy']['initial_sl_pct']):.2f}%",
        f"Hard exit: {artifact['stage0_risk_policy']['hard_exit_hours']}h",
        "",
        "## 3A Fixed SL Baseline",
        "",
        f"TP / initial SL: {fixed['final_tp_pct']:.2f}% / {fixed['initial_sl_pct']:.2f}%",
        f"TP / initial SL / time: {fixed['tp_count']} / {fixed['initial_sl_count']} / {fixed['time_exit_count']}",
        f"Net PnL: {fixed['net_pnl_pct']:.4f}%",
        "",
        "## 3B Exact Protection Policy",
        "",
        f"TP / initial SL / protected SL: {exact['final_tp_pct']:.2f}% / {exact['initial_sl_pct']:.2f}% / {exact['trail_sl_pct']:.2f}%",
        f"TP / initial SL / protected SL / time: {exact['tp_count']} / {exact['initial_sl_count']} / {exact['protected_sl_count']} / {exact['time_exit_count']}",
        f"Net PnL: {exact['net_pnl_pct']:.4f}%",
        "",
        "## 3C Local Variants",
        "",
        f"Combinations tested: {artifact['stage3c_total_combinations_tested']}",
        f"ATR dynamic combinations tested: {artifact.get('stage3c_atr_combinations_tested', 0)}",
        f"Best config: `{best.get('config_id')}`",
        f"Best net PnL: {best.get('net_pnl_pct', 0):.4f}%",
        "",
        "| Config | Mode | TP | Initial SL | Protect | Trail SL | Net PnL | WR | PF |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in artifact["optimal"]["top_5"]:
        atr = row.get("atr_source") if isinstance(row.get("atr_source"), dict) else None
        mode = f"ATR {atr.get('timeframe')}" if atr else ("protected" if row.get("protection_enabled") else "fixed")
        tp = f"{float(atr['tp_multiplier']):.2f}x ATR" if atr else f"{row['final_tp_pct']:.2f}%"
        initial_sl = f"{float(atr['sl_multiplier']):.2f}x ATR" if atr else f"{row['initial_sl_pct']:.2f}%"
        protect = f"{row['protect_trigger_pct']:.2f}%" if row.get("protect_trigger_pct") is not None else "off"
        trail = f"{row['trail_sl_pct']:.2f}%" if row.get("trail_sl_pct") is not None else "off"
        lines.append(
            f"| `{row['config_id']}` | {mode} | {tp} | {initial_sl} | "
            f"{protect} | {trail} | {row['net_pnl_pct']:.4f}% | "
            f"{row['wr']:.2f}% | {row['profit_factor']:.2f} |"
        )
    lines.append("")
    return "\n".join(lines)


def _ranking_key(row: dict[str, Any]) -> tuple[float, float, float, int]:
    return (
        float(row.get("net_pnl_pct", 0.0)),
        float(row.get("profit_factor", 0.0)),
        float(row.get("wr", 0.0)),
        -int(row.get("initial_sl_count", 0)),
    )


def _stage3_ranking_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    slices = row.get("slice_split") or {}
    wf = slices.get("walk_forward_test") or {}
    full_cycle_net = float(row.get("net_pnl_pct", 0.0))
    full_cycle_pf = float(row.get("profit_factor", 0.0))
    wf_total = int(wf.get("total", 0))
    wf_net = float(wf.get("net_pnl_pct", 0.0))
    wf_pf = float(wf.get("profit_factor", 0.0))
    return {
        "walk_forward_total": wf_total,
        "walk_forward_net_pnl_pct": wf_net,
        "walk_forward_profit_factor": wf_pf,
        "walk_forward_viable": wf_total > 0 and wf_net > 0 and wf_pf >= 1.0,
        "full_cycle_net_pnl_pct": full_cycle_net,
        "full_cycle_profit_factor": full_cycle_pf,
        "full_cycle_viable": (
            full_cycle_net >= DEFAULT_STAGE3_MIN_FULL_CYCLE_NET_PNL_PCT
            and full_cycle_pf >= DEFAULT_STAGE3_MIN_FULL_CYCLE_PROFIT_FACTOR
        ),
        "protection_enabled": bool(row.get("protection_enabled")),
    }


def _with_stage3_ranking_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "ranking_diagnostics": _stage3_ranking_diagnostics(row)}


def _walk_forward_ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    diagnostics = row.get("ranking_diagnostics") or _stage3_ranking_diagnostics(row)
    wf_total = int(diagnostics["walk_forward_total"])
    if wf_total <= 0:
        return _ranking_key(row)
    return (
        bool(diagnostics["full_cycle_viable"]),
        bool(diagnostics["walk_forward_viable"]),
        float(diagnostics["walk_forward_net_pnl_pct"]),
        float(diagnostics["walk_forward_profit_factor"]),
        float(diagnostics["full_cycle_net_pnl_pct"]),
        float(diagnostics["full_cycle_profit_factor"]),
        bool(row.get("protection_enabled")),
        float(row.get("wr", 0.0)),
        -float(row.get("initial_sl_pct", 0.0)),
    )


def _target_price(entry: float, *, pct: float, direction: str) -> float:
    return entry * (1 + pct / 100) if direction == "LONG" else entry * (1 - pct / 100)


def _stop_price(entry: float, *, pct: float, direction: str) -> float:
    return entry * (1 - pct / 100) if direction == "LONG" else entry * (1 + pct / 100)


def _protected_stop_price(entry: float, *, pct: float, direction: str) -> float:
    return entry * (1 + pct / 100) if direction == "LONG" else entry * (1 - pct / 100)


def _tp_sl_hit(candle: dict[str, Any], *, tp: float, sl: float, direction: str) -> tuple[bool, bool]:
    if direction == "LONG":
        return candle["high"] >= tp, candle["low"] <= sl
    return candle["low"] <= tp, candle["high"] >= sl


def _price_hit(candle: dict[str, Any], price: float, *, direction: str) -> bool:
    return candle["high"] >= price if direction == "LONG" else candle["low"] <= price


def _body_favors_direction(body: float, *, direction: str) -> bool:
    return body >= 0 if direction == "LONG" else body <= 0


def _pnl_pct(entry: float, exit_price: float, *, direction: str) -> float:
    if direction == "LONG":
        return (exit_price - entry) / entry * 100
    return (entry - exit_price) / entry * 100


def _id_pct(value: float) -> str:
    return f"{value:.4f}".rstrip("0").rstrip(".").replace(".", "p")


def _candidate_id(row: dict[str, Any]) -> str:
    if row.get("policy_mode") == "side_specific":
        return f"numeric_{row['config_id']}"
    base = f"numeric_{row['stage3_step']}_tp_{_id_pct(row['final_tp_pct'])}_sl_{_id_pct(row['initial_sl_pct'])}"
    if not row.get("protection_enabled"):
        return f"{base}_fixed"
    return f"{base}_protect_{_id_pct(row['protect_trigger_pct'])}_trail_{_id_pct(row['trail_sl_pct'])}"


def _session_artifact_root(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _coerce_candle(candle: Any) -> dict[str, Any]:
    if isinstance(candle, dict):
        return {
            "timestamp": _coerce_datetime(candle.get("timestamp") or candle.get("ts")),
            "open": float(candle["open"]),
            "high": float(candle["high"]),
            "low": float(candle["low"]),
            "close": float(candle["close"]),
        }
    return {
        "timestamp": _coerce_datetime(candle.timestamp),
        "open": _coerce_number(candle.open),
        "high": _coerce_number(candle.high),
        "low": _coerce_number(candle.low),
        "close": _coerce_number(candle.close),
    }


def _coerce_number(value: Any) -> float:
    return float(str(value)) if isinstance(value, Decimal) else float(value)


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("missing timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
