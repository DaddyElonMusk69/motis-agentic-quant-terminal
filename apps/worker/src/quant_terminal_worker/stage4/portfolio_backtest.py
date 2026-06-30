from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.stage2.capture_curve import get_reference_price
from quant_terminal_worker.stage4.realized_expectancy import (
    _candidate_policy_for_direction,
    _coerce_candle,
    _coerce_datetime,
    _direction_move_pct,
    _entry_hit,
    _next_entry,
    _normalize_candidates,
    _normalize_side_policies,
    _resolve_dual_hit,
    _target_price,
    _tp_sl_hit,
)


PROMOTION_SOURCE_STAGE4A = "stage4_realized_expectancy"
PROMOTION_SOURCE_STAGE4B = "stage4b_timing"


def run_portfolio_backtest(
    *,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidates: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    initial_capital_usdt: float = 10_000.0,
    margin_allocations_pct: dict[str, float] | None = None,
    repository: Any = None,
) -> dict[str, Any]:
    if initial_capital_usdt <= 0:
        raise ValueError("initial_capital_usdt must be greater than zero")
    if repository is None:
        raise ValueError("Portfolio backtest requires a market data repository")
    universe_run_id = str(universe_run["universe_run_id"])
    margin_allocations_pct = {asset.upper(): float(value) for asset, value in (margin_allocations_pct or {}).items()}

    asset_contexts = _load_asset_contexts(
        workspace_root=workspace_root,
        universe_run_id=universe_run_id,
        candidates=candidates,
        sessions=sessions,
        margin_allocations_pct=margin_allocations_pct,
        repository=repository,
    )
    if not asset_contexts:
        raise ValueError("Portfolio backtest requires at least one Stage 4-complete asset")

    result = _simulate(asset_contexts=asset_contexts, initial_capital_usdt=initial_capital_usdt)

    created_at_dt = datetime.now(UTC)
    created_at = created_at_dt.isoformat().replace("+00:00", "Z")
    run_id = _run_id(created_at_dt, _portfolio_root(workspace_root, universe_run_id))

    eligible_summary = [
        {
            "asset": ctx["asset"],
            "session_id": ctx["session_id"],
            "stage4_candidate_id": ctx["stage4_candidate_id"],
            "promotion_source": ctx.get("promotion_source"),
            "promotion_source_label": ctx.get("promotion_source_label"),
            "promotion_selection_criterion": ctx.get("promotion_selection_criterion"),
            "promotion_warning": ctx.get("promotion_warning"),
            "walk_forward_net_pnl_pct": ctx.get("walk_forward_net_pnl_pct"),
            "walk_forward_profit_factor": ctx.get("walk_forward_profit_factor"),
            "overall_net_pnl_usdt": ctx.get("overall_net_pnl_usdt"),
            "timing_skips": ctx.get("timing_skips"),
            "margin_allocation_pct": ctx["margin_allocation_pct"],
            "leverage": ctx["leverage"],
            "signal_count": len(ctx["signal_inputs"]),
            "candle_count": len(ctx["candles"]),
        }
        for ctx in asset_contexts
    ]

    payload = {
        "schema_version": "0.3",
        "artifact_role": "portfolio_backtest",
        "created_at": created_at,
        "run_id": run_id,
        "universe_run_id": universe_run_id,
        "simulation_inputs": {
            "initial_capital_usdt": initial_capital_usdt,
            "margin_allocations_pct": margin_allocations_pct,
            "margin_basis": "current_equity",
            "simulation_mode": "candle_by_candle",
        },
        "eligible_assets": eligible_summary,
        "summary": result["summary"],
        "account": result["account"],
        "asset_breakdown": result["asset_breakdown"],
        "equity_curve": result["equity_curve"],
        "trade_ledger": result["trade_ledger"],
        "skipped_signals": result["skipped_signals"],
    }
    return _write_artifacts(workspace_root=workspace_root, universe_run_id=universe_run_id, run_id=run_id, payload=payload)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_asset_contexts(
    *,
    workspace_root: Path,
    universe_run_id: str,
    candidates: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    margin_allocations_pct: dict[str, float],
    repository: Any,
) -> list[dict[str, Any]]:
    accepted_candidate_ids = {
        candidate["candidate_id"]
        for candidate in candidates
        if candidate.get("universe_run_id") == universe_run_id and candidate.get("acceptance_status") == "accepted"
    }
    contexts: list[dict[str, Any]] = []
    for session in sessions:
        if session.get("source_universe_run_id") != universe_run_id:
            continue
        if session.get("source_candidate_id") not in accepted_candidate_ids:
            continue
        artifact_root = _resolve_path(workspace_root, session["artifact_root"])
        promotion_root = artifact_root / "promotion"
        realized_path = promotion_root / "stage4_realized_expectancy.json"
        scores_path = artifact_root / "promotion" / "stage1a_canonical_full_cycle_scores.json"
        if not realized_path.is_file() or not scores_path.is_file():
            continue
        realized = json.loads(realized_path.read_text())
        scores = json.loads(scores_path.read_text())
        asset = str(session.get("asset") or realized.get("asset") or "").upper()
        if not asset:
            continue
        margin_pct = float(margin_allocations_pct.get(asset, 30.0))
        if margin_pct <= 0:
            continue

        selection = _resolve_portfolio_promotion_candidate(promotion_root=promotion_root, scores=scores)
        if selection is None:
            continue
        best_candidate_id = selection["candidate_id"]
        best_candidate = selection["candidate"]
        selected_scores = selection["scores"]

        # Leverage and cost assumptions live in the realized expectancy file,
        # not in stage4_candidates.json. Override the normalized default.
        selected_best = selection.get("best") or {}
        selected_setup = selected_best.get("setup") or {}
        if selected_setup.get("leverage") is not None:
            best_candidate["leverage"] = float(selected_setup["leverage"])

        cost = realized.get("cost_assumptions", {})
        fees_bps = float(cost.get("fees_bps_per_side", 5.0))
        slippage_bps = float(cost.get("slippage_bps_per_side", 0.0))

        # Load signals
        signal_inputs = _load_signal_inputs(
            session=session,
            scores=selected_scores,
            timing_overlay=selection.get("overlay"),
            repository=repository,
        )

        # Load candles
        candles = _load_candles(
            session=session,
            repository=repository,
            workspace_root=workspace_root,
        )

        if not signal_inputs or not candles:
            continue

        leverage = float(best_candidate.get("leverage", 1.0))
        max_notional = _max_position_notional(asset, leverage)

        contexts.append({
            "asset": asset,
            "session_id": session["session_id"],
            "source_candidate_id": session.get("source_candidate_id"),
            "stage4_run_id": realized.get("run_id"),
            "stage4_candidate_id": best_candidate_id,
            "promotion_source": selection["source"],
            "promotion_source_label": selection["label"],
            "promotion_selection_criterion": selection["criterion"],
            "promotion_warning": selection.get("warning"),
            "walk_forward_net_pnl_pct": _walk_forward_net_pnl_pct(selected_best),
            "walk_forward_profit_factor": _walk_forward_profit_factor(selected_best),
            "overall_net_pnl_usdt": _overall_net_pnl_usdt(selected_best),
            "timing_skips": selected_best.get("skipped_timing_filter"),
            "candidate": best_candidate,
            "leverage": leverage,
            "margin_allocation_pct": margin_pct,
            "max_position_notional_usdt": max_notional,
            "fees_bps_per_side": fees_bps,
            "slippage_bps_per_side": slippage_bps,
            "signal_inputs": signal_inputs,
            "candles": candles,
        })
    return sorted(contexts, key=lambda item: item["asset"])


def _resolve_portfolio_promotion_candidate(*, promotion_root: Path, scores: dict[str, Any]) -> dict[str, Any] | None:
    stage4a_candidates = _stage4a_portfolio_candidates(promotion_root=promotion_root, scores=scores)
    if not stage4a_candidates:
        return None
    stage4b_candidates = _stage4b_portfolio_candidates(
        promotion_root=promotion_root,
        scores=scores,
        latest_stage4a_run_id=str(stage4a_candidates[0]["result"].get("run_id") or ""),
    )
    candidates = [*stage4a_candidates, *stage4b_candidates]
    protected_eligible = [candidate for candidate in candidates if _candidate_has_protected_sl(candidate["best"]) and _walk_forward_net_pnl_pct(candidate["best"]) > 0]
    if protected_eligible:
        selected = max(protected_eligible, key=_promotion_rank_key)
        return {**selected, "criterion": "protected_walk_forward_net_pnl_pct"}
    eligible = [candidate for candidate in candidates if _walk_forward_net_pnl_pct(candidate["best"]) > 0]
    if eligible:
        return max(eligible, key=_promotion_rank_key)
    best = max(candidates, key=lambda candidate: (_overall_net_pnl_usdt(candidate["best"]), candidate["source"] == PROMOTION_SOURCE_STAGE4A))
    return {**best, "criterion": "overall_net_pnl_fallback", "warning": "weak_walk_forward_fallback"}


def _stage4a_portfolio_candidates(*, promotion_root: Path, scores: dict[str, Any]) -> list[dict[str, Any]]:
    realized_path = promotion_root / "stage4_realized_expectancy.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    if not realized_path.is_file():
        return []
    realized = json.loads(realized_path.read_text())
    rows = [row for row in realized.get("candidates", []) if isinstance(row, dict)]
    if not rows and isinstance(realized.get("best_candidate"), dict):
        rows = [realized["best_candidate"]]
    return [
        {
            "source": PROMOTION_SOURCE_STAGE4A,
            "label": "Stage 4A",
            "criterion": "walk_forward_net_pnl_pct",
            "result": realized,
            "best": row,
            "candidate_id": str(row["candidate_id"]),
            "candidate": candidate,
            "scores": scores,
            "overlay": None,
        }
        for row in rows
        if row.get("candidate_id")
        for candidate in [_candidate_setup_for_id(candidates_path=candidates_path, candidate_id=str(row["candidate_id"]))]
        if candidate is not None
    ]


def _stage4b_portfolio_candidates(*, promotion_root: Path, scores: dict[str, Any], latest_stage4a_run_id: str) -> list[dict[str, Any]]:
    timing_root = promotion_root / "stage4b_timing"
    replay_path = timing_root / "timing_replay.json"
    overlay_path = timing_root / "timing_overlay.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    if not replay_path.is_file() or not overlay_path.is_file():
        return []
    replay = json.loads(replay_path.read_text())
    overlay = json.loads(overlay_path.read_text())
    if str(overlay.get("source_stage4_run_id") or "") != latest_stage4a_run_id:
        return []
    rows = [row for row in replay.get("candidates", []) if isinstance(row, dict)]
    if not rows and isinstance(replay.get("best_candidate"), dict):
        rows = [replay["best_candidate"]]
    return [
        {
            "source": PROMOTION_SOURCE_STAGE4B,
            "label": "Stage 4B Timing",
            "criterion": "walk_forward_net_pnl_pct",
            "result": replay,
            "best": row,
            "candidate_id": str(row["candidate_id"]),
            "candidate": candidate,
            "scores": scores,
            "overlay": overlay,
        }
        for row in rows
        if row.get("candidate_id")
        for candidate in [_candidate_setup_for_id(candidates_path=candidates_path, candidate_id=str(row["candidate_id"]))]
        if candidate is not None
    ]


def _candidate_setup_for_id(*, candidates_path: Path, candidate_id: str) -> dict[str, Any] | None:
    candidates_payload = json.loads(candidates_path.read_text()) if candidates_path.is_file() else {"candidates": []}
    normalized = _normalize_candidates(candidates_payload)
    return next((candidate for candidate in normalized if candidate["candidate_id"] == candidate_id), None)


def _candidate_has_protected_sl(candidate: dict[str, Any]) -> bool:
    setup = candidate.get("setup") if isinstance(candidate.get("setup"), dict) else candidate
    if bool(setup.get("protection_enabled")):
        return True
    side_policies = setup.get("side_policies") if isinstance(setup.get("side_policies"), dict) else {}
    return any(isinstance(policy, dict) and bool(policy.get("protection_enabled")) for policy in side_policies.values())


def _promotion_rank_key(candidate: dict[str, Any]) -> tuple[float, float, float, bool]:
    best = candidate["best"]
    return (
        _walk_forward_net_pnl_pct(best),
        _walk_forward_profit_factor(best),
        _overall_net_pnl_usdt(best),
        candidate["source"] == PROMOTION_SOURCE_STAGE4A,
    )


def _walk_forward_net_pnl_pct(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("net_pnl_pct"), 0.0)


def _walk_forward_profit_factor(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("profit_factor"), 0.0)


def _overall_net_pnl_usdt(best: dict[str, Any]) -> float:
    account = best.get("account") or {}
    return _float_or_default(account.get("net_pnl_usdt"), 0.0)


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_signal_inputs(
    *,
    session: dict[str, Any],
    scores: dict[str, Any],
    timing_overlay: dict[str, Any] | None = None,
    repository: Any,
) -> list[dict[str, Any]]:
    records = scores.get("records", [])
    if not records:
        return []
    # Load signal packets from repository
    signals_by_role: dict[str, list[dict[str, Any]]] = {}
    for sample_role in ("training", "walk_forward_test"):
        window_start, window_end = _sample_window(session, sample_role)
        signals_by_role[sample_role] = repository.list_signals_for_signal_set_window(
            signal_set_key=session["signal_set_key"],
            window_start=f"{window_start}T00:00:00Z",
            window_end=f"{window_end}T23:59:59Z",
        )
    signals_by_id: dict[str, dict[str, Any]] = {}
    for signals in signals_by_role.values():
        for signal in signals:
            signals_by_id[str(signal["signal_id"])] = signal

    inputs: list[dict[str, Any]] = []
    for record in records:
        signal_id = str(record["signal_id"])
        signal = signals_by_id.get(signal_id) or signals_by_id.get(signal_id.split(":")[-1])
        if signal is None:
            continue
        payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
        packet = {**payload, "signal_id": signal["signal_id"], "timestamp": payload.get("timestamp") or signal["timestamp"]}
        signal_ts = _coerce_datetime(packet.get("timestamp") or signal["timestamp"])
        reference_price = get_reference_price(packet)
        direction = str(record.get("decision_direction") or record.get("agent_direction") or "").upper()
        skip_reason = record.get("stage4b_skip_reason")
        if _timing_overlay_matches(signal_ts=signal_ts, direction=direction, overlay=timing_overlay):
            direction = "SKIP"
            skip_reason = "timing_filter"
        inputs.append({
            "signal_id": signal_id,
            "signal_ts": signal_ts,
            "reference_price": float(reference_price),
            "direction": direction,
            "agreement": record.get("agreement"),
            "skip_reason": skip_reason,
        })
    inputs.sort(key=lambda item: (item["signal_ts"], item["signal_id"]))
    return inputs


def _timing_overlay_matches(*, signal_ts: datetime, direction: str, overlay: dict[str, Any] | None) -> bool:
    if not overlay or direction not in {"LONG", "SHORT"}:
        return False
    hours = set(int(hour) for hour in overlay.get("exclude_utc_hours") or [])
    weekdays = set(int(day) for day in overlay.get("exclude_utc_weekdays") or [])
    applies_to = str(overlay.get("applies_to") or "all").upper()
    skip_for_time = signal_ts.hour in hours and (not weekdays or signal_ts.weekday() in weekdays)
    skip_for_side = applies_to in {"ALL", "all"} or applies_to == direction
    return skip_for_time and skip_for_side


def _load_candles(
    *,
    session: dict[str, Any],
    repository: Any,
    workspace_root: Path,
) -> list[dict[str, Any]]:
    train_start = _date_string(session["train_start"])
    walk_forward_end = _date_string(session["walk_forward_end"])
    start = f"{train_start}T00:00:00Z"
    end = _add_hours(f"{walk_forward_end}T23:59:59Z", 36)
    reader = MarketDataReader(repository=repository, workspace_root=workspace_root)
    raw_candles = reader.get_candles(
        asset=session["asset"],
        timeframe="5m",
        origin="raw",
        start=start,
        end=end,
    )
    candle_rows = [_coerce_candle(candle) for candle in raw_candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    return candle_rows


# ---------------------------------------------------------------------------
# Simulation engine
# ---------------------------------------------------------------------------

def _simulate(
    *,
    asset_contexts: list[dict[str, Any]],
    initial_capital_usdt: float,
) -> dict[str, Any]:
    available_cash = float(initial_capital_usdt)
    open_positions: dict[str, dict[str, Any]] = {}
    trade_ledger: list[dict[str, Any]] = []
    skipped_signals: list[dict[str, Any]] = []
    data_gap_candles = 0
    continuous_5m_steps = 0
    equity_curve: list[dict[str, Any]] = [
        _curve_point(None, account_equity=available_cash, isolated_margin=0.0, available_cash=available_cash)
    ]

    # Build per-asset candle index and signal cursor
    candles_by_asset: dict[str, dict[datetime, dict[str, Any]]] = {}
    candle_spans_by_asset: dict[str, tuple[datetime, datetime]] = {}
    signal_cursors: dict[str, int] = {}
    all_timestamps: set[datetime] = set()

    for ctx in asset_contexts:
        asset = ctx["asset"]
        candle_index: dict[datetime, dict[str, Any]] = {}
        for candle in ctx["candles"]:
            candle_index[candle["timestamp"]] = candle
            all_timestamps.add(candle["timestamp"])
        candles_by_asset[asset] = candle_index
        if candle_index:
            ordered = sorted(candle_index)
            candle_spans_by_asset[asset] = (ordered[0], ordered[-1])
        signal_cursors[asset] = 0

    unified_timeline = _continuous_5m_timeline(sorted(all_timestamps))

    for ts in unified_timeline:
        continuous_5m_steps += 1
        for asset, span in candle_spans_by_asset.items():
            if span[0] <= ts <= span[1] and ts not in candles_by_asset.get(asset, {}):
                data_gap_candles += 1

        # Compute account equity (realized + unrealized) for margin checks
        unrealized = _unrealized_pnl(open_positions, candles_by_asset, ts)
        isolated_margin = _used_margin(open_positions)
        account_equity = available_cash + isolated_margin + unrealized

        # Phase 1: Manage existing positions
        closed_assets: list[str] = []
        for asset, position in list(open_positions.items()):
            candle = candles_by_asset[asset].get(ts)
            if candle is None:
                continue
            managed = _manage_position(
                position=position,
                candle=candle,
                ts=ts,
                available_cash=available_cash,
            )
            available_cash -= managed["margin_consumed"]
            skipped_signals.extend(managed["pyramid_skips"])
            closed = managed["closed"]
            if closed:
                net_pnl = closed["net_pnl"]
                position_margin = closed["position_margin"]
                available_cash += position_margin + net_pnl
                trade_ledger.append(closed["trade"])
                closed_assets.append(asset)
                isolated_after_close = _used_margin(open_positions, exclude=asset)
                account_after_close = available_cash + isolated_after_close + _unrealized_pnl(open_positions, candles_by_asset, ts, exclude=asset)
                equity_curve.append(_curve_point(ts, account_equity=account_after_close, isolated_margin=isolated_after_close, available_cash=available_cash))
        for asset in closed_assets:
            open_positions.pop(asset, None)

        # Recompute account equity after phase 1 closes
        unrealized = _unrealized_pnl(open_positions, candles_by_asset, ts)
        isolated_margin = _used_margin(open_positions)
        account_equity = available_cash + isolated_margin + unrealized

        # Phase 2: Process new signals
        for ctx in asset_contexts:
            asset = ctx["asset"]
            if asset in open_positions:
                # Track signals that fire while a position is open for this asset
                cursor = signal_cursors[asset]
                inputs = ctx["signal_inputs"]
                while cursor < len(inputs) and inputs[cursor]["signal_ts"] <= ts:
                    skipped_signals.append(_skip_record(
                        asset=asset, signal=inputs[cursor], reason="asset_position_open",
                        account_equity=account_equity, isolated_margin=isolated_margin, available_cash=available_cash,
                    ))
                    cursor += 1
                signal_cursors[asset] = cursor
                continue

            candle = candles_by_asset[asset].get(ts)
            if candle is None:
                continue

            cursor = signal_cursors[asset]
            inputs = ctx["signal_inputs"]
            if cursor >= len(inputs):
                continue
            signal = inputs[cursor]
            if signal["signal_ts"] > ts:
                continue
            # Advance cursor past this signal
            signal_cursors[asset] = cursor + 1

            if signal["direction"] not in {"LONG", "SHORT"}:
                skipped_signals.append(_skip_record(
                    asset=asset, signal=signal, reason=str(signal.get("skip_reason") or "no_trade_decision"),
                    account_equity=account_equity, isolated_margin=isolated_margin, available_cash=available_cash,
                ))
                continue

            margin_needed = account_equity * ctx["margin_allocation_pct"] / 100

            # Apply max position notional cap (exchange tier + liquidity constraint)
            max_notional = ctx.get("max_position_notional_usdt", 500_000.0)
            max_margin_for_notional = max_notional / ctx["leverage"] if ctx["leverage"] > 0 else margin_needed
            if margin_needed > max_margin_for_notional:
                margin_needed = max_margin_for_notional

            initial_leg_margin = _initial_leg_margin(ctx=ctx, signal=signal, margin_budget=margin_needed)
            if initial_leg_margin > available_cash:
                skipped_signals.append(_skip_record(
                    asset=asset, signal=signal, reason="insufficient_free_margin",
                    account_equity=account_equity, isolated_margin=isolated_margin, available_cash=available_cash,
                    requested_margin=initial_leg_margin,
                ))
                continue

            # Open position
            position = _open_position(
                asset=asset,
                signal=signal,
                candle=candle,
                ctx=ctx,
                equity=account_equity,
                margin_budget=margin_needed,
            )
            open_positions[asset] = position
            available_cash -= initial_leg_margin
            isolated_margin = _used_margin(open_positions)
            account_equity = available_cash + isolated_margin + _unrealized_pnl(open_positions, candles_by_asset, ts)
            equity_curve.append(_curve_point(ts, account_equity=account_equity, isolated_margin=isolated_margin, available_cash=available_cash))

    # Close any remaining open positions at the last available candle
    for asset, position in list(open_positions.items()):
        ctx = next(c for c in asset_contexts if c["asset"] == asset)
        if ctx["candles"]:
            last_candle = ctx["candles"][-1]
            closed = _force_close_position(
                position=position,
                candle=last_candle,
            )
            if closed:
                available_cash += closed["position_margin"] + closed["net_pnl"]
                trade_ledger.append(closed["trade"])
                open_positions.pop(asset, None)
                isolated_margin = _used_margin(open_positions)
                equity_curve.append(_curve_point(last_candle["timestamp"], account_equity=available_cash + isolated_margin, isolated_margin=isolated_margin, available_cash=available_cash))

    account = _account_summary(
        initial_capital_usdt=initial_capital_usdt,
        ending_equity_usdt=available_cash,
        trades=trade_ledger,
        equity_curve=equity_curve,
    )
    final_isolated_margin = _used_margin(open_positions)
    summary = {
        "eligible_asset_count": len(asset_contexts),
        "total_signals": sum(len(ctx["signal_inputs"]) for ctx in asset_contexts),
        "executed_positions": len(trade_ledger),
        "skipped_signals": len(skipped_signals),
        "skipped_insufficient_margin": sum(1 for item in skipped_signals if item["skip_reason"] == "insufficient_free_margin"),
        "skipped_pyramid_margin": sum(1 for item in skipped_signals if item["skip_reason"] == "insufficient_free_margin_pyramid"),
        "skipped_asset_open": sum(1 for item in skipped_signals if item["skip_reason"] == "asset_position_open"),
        "skipped_no_trade": sum(1 for item in skipped_signals if item["skip_reason"] == "no_trade_decision"),
        "skipped_timing_filter": sum(1 for item in skipped_signals if item["skip_reason"] == "timing_filter"),
        "available_cash_usdt": _round_money(available_cash),
        "isolated_margin_usdt": _round_money(final_isolated_margin),
        "margin_deficit_candles": 0,
        "continuous_5m_steps": continuous_5m_steps,
        "data_gap_candles": data_gap_candles,
    }
    asset_breakdown = _asset_breakdown(
        asset_contexts=asset_contexts,
        trades=trade_ledger,
        skipped_signals=skipped_signals,
        portfolio_net_pnl_usdt=float(account.get("net_pnl_usdt") or 0.0),
        initial_capital_usdt=initial_capital_usdt,
    )
    return {
        "account": account,
        "summary": summary,
        "asset_breakdown": asset_breakdown,
        "equity_curve": equity_curve,
        "trade_ledger": trade_ledger,
        "skipped_signals": skipped_signals,
    }


def _asset_breakdown(
    *,
    asset_contexts: list[dict[str, Any]],
    trades: list[dict[str, Any]],
    skipped_signals: list[dict[str, Any]],
    portfolio_net_pnl_usdt: float,
    initial_capital_usdt: float,
) -> list[dict[str, Any]]:
    rows = []
    for ctx in asset_contexts:
        asset = ctx["asset"]
        asset_trades = [trade for trade in trades if trade.get("asset") == asset]
        asset_skips = [skip for skip in skipped_signals if skip.get("asset") == asset]
        winning = [trade for trade in asset_trades if float(trade.get("net_pnl_usdt") or 0) > 0]
        losing = [trade for trade in asset_trades if float(trade.get("net_pnl_usdt") or 0) < 0]
        gross_pnl = sum(float(trade.get("gross_pnl_usdt") or 0) for trade in asset_trades)
        net_pnl = sum(float(trade.get("net_pnl_usdt") or 0) for trade in asset_trades)
        fees = sum(float(trade.get("total_fees_usdt") or 0) for trade in asset_trades)
        slippage = sum(float(trade.get("total_slippage_usdt") or 0) for trade in asset_trades)
        contribution_pct = net_pnl / portfolio_net_pnl_usdt * 100 if portfolio_net_pnl_usdt else 0.0
        return_on_initial_capital_pct = net_pnl / initial_capital_usdt * 100 if initial_capital_usdt else 0.0
        rows.append(
            {
                "asset": asset,
                "session_id": ctx["session_id"],
                "stage4_candidate_id": ctx["stage4_candidate_id"],
                "promotion_source": ctx.get("promotion_source"),
                "promotion_source_label": ctx.get("promotion_source_label"),
                "margin_allocation_pct": ctx["margin_allocation_pct"],
                "signal_count": len(ctx["signal_inputs"]),
                "executed_positions": len(asset_trades),
                "winning_positions": len(winning),
                "losing_positions": len(losing),
                "win_rate_pct": round(len(winning) / len(asset_trades) * 100, 8) if asset_trades else 0.0,
                "gross_pnl_usdt": _round_money(gross_pnl),
                "net_pnl_usdt": _round_money(net_pnl),
                "portfolio_net_pnl_contribution_pct": round(contribution_pct, 8),
                "return_on_initial_capital_pct": round(return_on_initial_capital_pct, 8),
                "total_fees_usdt": _round_money(fees),
                "total_slippage_usdt": _round_money(slippage),
                "skipped_signals": len(asset_skips),
                "skipped_insufficient_margin": sum(1 for skip in asset_skips if skip.get("skip_reason") == "insufficient_free_margin"),
                "skipped_pyramid_margin": sum(1 for skip in asset_skips if skip.get("skip_reason") == "insufficient_free_margin_pyramid"),
                "skipped_asset_open": sum(1 for skip in asset_skips if skip.get("skip_reason") == "asset_position_open"),
                "skipped_timing_filter": sum(1 for skip in asset_skips if skip.get("skip_reason") == "timing_filter"),
            }
        )
    return sorted(rows, key=lambda item: (item["net_pnl_usdt"], item["asset"]), reverse=True)


def _manage_position(
    *,
    position: dict[str, Any],
    candle: dict[str, Any],
    ts: datetime,
    available_cash: float,
) -> dict[str, Any]:
    ctx = position["ctx"]
    candidate = ctx["candidate"]
    direction = position["direction"]
    policy = _candidate_policy_for_direction(candidate, direction)
    fee_rate = ctx["fees_bps_per_side"] / 10_000
    slippage_rate = ctx["slippage_bps_per_side"] / 10_000
    result: dict[str, Any] = {"closed": None, "margin_consumed": 0.0, "pyramid_skips": []}

    # Check hard exit
    cutoff = position["signal_ts"] + timedelta(hours=policy["max_hold_hours"])
    if ts >= cutoff:
        result["closed"] = _close_position(position=position, exit_price=candle["close"], exit_ts=ts, exit_status="HARD_EXIT")
        return result

    # Check pyramid entry
    max_legs = int((policy.get("pyramid") or {}).get("max_legs", 1))
    max_legs = max(1, max_legs)
    pyramid = policy.get("pyramid") or {}
    step_pct = float(pyramid.get("step_pct", 999))
    sl_breakeven = bool(pyramid.get("sl_breakeven", False))

    if len(position["legs"]) < max_legs:
        last_entry = position["legs"][-1]["entry_price"]
        next_entry = _next_entry(last_entry, step_pct=step_pct, direction=direction)
        if _entry_hit(candle, next_entry, direction=direction):
            per_leg_margin = position["margin_budget"] / max_legs
            remaining_cash = available_cash - result["margin_consumed"]
            if per_leg_margin > remaining_cash:
                result["pyramid_skips"].append(_skip_record(
                    asset=position["asset"],
                    signal=position["signal"],
                    reason="insufficient_free_margin_pyramid",
                    account_equity=remaining_cash + _position_margin(position),
                    isolated_margin=_position_margin(position),
                    available_cash=remaining_cash,
                    requested_margin=per_leg_margin,
                ))
            else:
                leg = _open_leg(
                    leg_number=len(position["legs"]) + 1,
                    entry_price=next_entry,
                    entry_ts=ts,
                    tp_pct=policy["tp_pct"],
                    direction=direction,
                    margin_usdt=per_leg_margin,
                    leverage=ctx["leverage"],
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                )
                position["legs"].append(leg)
                result["margin_consumed"] += per_leg_margin
                if sl_breakeven:
                    position["sl_price"] = sum(leg["entry_price"] for leg in position["legs"]) / len(position["legs"])
                    position["active_sl_kind"] = "breakeven"

    # Check protection trigger
    protection_enabled = bool(policy.get("protection_enabled", False))
    protect_trigger_pct = policy.get("protect_trigger_pct")
    trail_sl_pct = policy.get("trail_sl_pct")
    if (protection_enabled and not position["protection_activated"]
            and protect_trigger_pct is not None and trail_sl_pct is not None):
        protect_trigger_price = _target_price(position["entry_price"], tp_pct=float(protect_trigger_pct), direction=direction)
        if _entry_hit(candle, protect_trigger_price, direction=direction):
            position["protection_activated"] = True
            position["sl_price"] = _protected_stop_price(position["entry_price"], pct=float(trail_sl_pct), direction=direction)
            position["active_sl_kind"] = "protected"

    # Check TP/SL for each active leg
    sl_price = position["sl_price"]
    tp_pct = policy["tp_pct"]
    closed_legs: list[tuple[dict[str, Any], str, float, datetime]] = []
    for leg in position["legs"]:
        if leg.get("exit_status"):
            continue
        tp_hit, sl_hit = _tp_sl_hit(candle, tp=leg["tp_price"], sl=sl_price, direction=direction)
        if not tp_hit and not sl_hit:
            continue
        if tp_hit and sl_hit:
            exit_status = _resolve_dual_hit(candle, direction=direction)
        elif tp_hit:
            exit_status = "TP"
        else:
            exit_status = "SL"
        if exit_status == "SL":
            exit_status = "PROTECTED_SL" if position["active_sl_kind"] == "protected" else "INITIAL_SL"
        exit_price = leg["tp_price"] if exit_status == "TP" else sl_price
        closed_legs.append((leg, exit_status, exit_price, ts))

    for leg, exit_status, exit_price, exit_ts in closed_legs:
        _close_leg(leg, exit_status=exit_status, exit_price=exit_price, exit_ts=exit_ts, direction=direction, fee_rate=fee_rate, slippage_rate=slippage_rate)

    active_legs = [leg for leg in position["legs"] if not leg.get("exit_status")]
    if not active_legs:
        # All legs closed — close the position
        result["closed"] = _close_position(position=position, exit_price=closed_legs[-1][2], exit_ts=ts, exit_status=_resolve_exit_status(position["legs"]))
        return result

    return result


def _open_position(
    *,
    asset: str,
    signal: dict[str, Any],
    candle: dict[str, Any],
    ctx: dict[str, Any],
    equity: float,
    margin_budget: float,
) -> dict[str, Any]:
    candidate = ctx["candidate"]
    direction = signal["direction"]
    policy = _candidate_policy_for_direction(candidate, direction)
    entry_price = float(signal["reference_price"])
    max_legs = int((policy.get("pyramid") or {}).get("max_legs", 1))
    max_legs = max(1, max_legs)
    per_leg_margin = margin_budget / max_legs
    fee_rate = ctx["fees_bps_per_side"] / 10_000
    slippage_rate = ctx["slippage_bps_per_side"] / 10_000
    sl_pct = policy["sl_pct"]
    sl_price = entry_price * (1 - sl_pct / 100) if direction == "LONG" else entry_price * (1 + sl_pct / 100)

    protection_enabled = bool(policy.get("protection_enabled", False))
    protect_trigger_pct = policy.get("protect_trigger_pct")
    trail_sl_pct = policy.get("trail_sl_pct")
    protected_sl_price = _protected_stop_price(entry_price, pct=float(trail_sl_pct), direction=direction) if protection_enabled and trail_sl_pct is not None else None

    leg = _open_leg(
        leg_number=1,
        entry_price=entry_price,
        entry_ts=signal["signal_ts"],
        tp_pct=policy["tp_pct"],
        direction=direction,
        margin_usdt=per_leg_margin,
        leverage=ctx["leverage"],
        fee_rate=fee_rate,
        slippage_rate=slippage_rate,
    )

    position_id = f"{asset}:{signal['signal_id']}"
    return {
        "asset": asset,
        "ctx": ctx,
        "signal": signal,
        "direction": direction,
        "entry_price": entry_price,
        "signal_ts": signal["signal_ts"],
        "position_id": position_id,
        "margin_budget": margin_budget,
        "entry_account_equity": equity,
        "leverage": ctx["leverage"],
        "legs": [leg],
        "sl_price": sl_price,
        "active_sl_kind": "initial",
        "protection_enabled": protection_enabled,
        "protection_activated": False,
        "protect_trigger_pct": protect_trigger_pct,
        "trail_sl_pct": trail_sl_pct,
        "protected_sl_price": protected_sl_price,
        "policy": policy,
    }


def _close_position(
    *,
    position: dict[str, Any],
    exit_price: float,
    exit_ts: datetime,
    exit_status: str,
) -> dict[str, Any]:
    ctx = position["ctx"]
    direction = position["direction"]
    fee_rate = ctx["fees_bps_per_side"] / 10_000
    slippage_rate = ctx["slippage_bps_per_side"] / 10_000

    # Close any still-open legs at this price
    for leg in position["legs"]:
        if not leg.get("exit_status"):
            _close_leg(leg, exit_status=exit_status, exit_price=exit_price, exit_ts=exit_ts, direction=direction, fee_rate=fee_rate, slippage_rate=slippage_rate)

    legs = position["legs"]
    gross_pnl = sum(float(leg.get("gross_pnl_usdt") or 0) for leg in legs)
    net_pnl = sum(float(leg.get("net_pnl_usdt") or 0) for leg in legs)
    entry_fees = sum(float(leg.get("entry_fee_usdt") or 0) for leg in legs)
    exit_fees = sum(float(leg.get("exit_fee_usdt") or 0) for leg in legs)
    slippage = sum(float(leg.get("entry_slippage_usdt") or 0) + float(leg.get("exit_slippage_usdt") or 0) for leg in legs)
    position_margin = sum(float(leg.get("margin_usdt") or 0) for leg in legs)
    position_notional = sum(float(leg.get("entry_notional_usdt") or 0) for leg in legs)
    equity = float(position.get("entry_account_equity") or position_margin)
    equity_after = equity + net_pnl
    signal = position["signal"]
    policy = position["policy"]
    entry_ts_iso = legs[0]["entry_ts"] if legs else _to_iso(signal["signal_ts"])
    entry_dt = _coerce_datetime(entry_ts_iso) if entry_ts_iso else signal["signal_ts"]
    open_duration_hours = round((exit_ts - entry_dt).total_seconds() / 3600, 4) if entry_dt else None

    trade = {
        "asset": position["asset"],
        "source_session_id": ctx["session_id"],
        "candidate_id": ctx["stage4_candidate_id"],
        "signal_id": signal["signal_id"],
        "signal_ts": _to_iso(signal["signal_ts"]),
        "entry_ts": entry_ts_iso,
        "exit_ts": _to_iso(exit_ts),
        "open_duration_hours": open_duration_hours,
        "agreement": signal.get("agreement"),
        "decision_direction": direction,
        "reference_price": round(position["entry_price"], 8),
        "position_id": position["position_id"],
        "entry_status": "FILLED",
        "exit_status": _resolve_exit_status(legs) if exit_status == "HARD_EXIT" else exit_status,
        "entry_price": round(position["entry_price"], 8),
        "exit_price": round(float(legs[-1]["exit_price"]), 8),
        "filled_legs": len(legs),
        "leverage": ctx["leverage"],
        "position_margin_usdt": _round_money(position_margin),
        "position_notional_usdt": _round_money(position_notional),
        "protection_enabled": position["protection_enabled"],
        "protection_activated": position["protection_activated"],
        "active_sl_kind": position["active_sl_kind"],
        "initial_sl_pct": policy["sl_pct"],
        "gross_pnl_usdt": _round_money(gross_pnl),
        "net_pnl_usdt": _round_money(net_pnl),
        "total_fees_usdt": _round_money(entry_fees + exit_fees),
        "total_entry_fees_usdt": _round_money(entry_fees),
        "total_exit_fees_usdt": _round_money(exit_fees),
        "total_slippage_usdt": _round_money(slippage),
        "equity_before": _round_money(equity),
        "equity_after": _round_money(equity_after),
        "net_pnl_pct": round(net_pnl / equity * 100, 8) if equity else 0.0,
        "gross_pnl_pct": round(gross_pnl / equity * 100, 8) if equity else 0.0,
        "roe_pct": round(net_pnl / position_margin * 100, 8) if position_margin > 0 else 0.0,
        "leg_details": [_format_leg(leg) for leg in legs],
        "portfolio_entry_equity_usdt": _round_money(equity),
        "portfolio_margin_allocation_pct": ctx["margin_allocation_pct"],
    }
    return {"trade": trade, "net_pnl": net_pnl, "position_margin": position_margin}


def _force_close_position(
    *,
    position: dict[str, Any],
    candle: dict[str, Any],
) -> dict[str, Any] | None:
    open_legs = [leg for leg in position["legs"] if not leg.get("exit_status")]
    if not open_legs:
        return None
    return _close_position(
        position=position,
        exit_price=candle["close"],
        exit_ts=candle["timestamp"],
        exit_status="HARD_EXIT",
    )


# ---------------------------------------------------------------------------
# Leg management (adapted from realized_expectancy.py)
# ---------------------------------------------------------------------------

def _open_leg(
    *,
    leg_number: int,
    entry_price: float,
    entry_ts: datetime,
    tp_pct: float,
    direction: str,
    margin_usdt: float,
    leverage: float,
    fee_rate: float,
    slippage_rate: float,
) -> dict[str, Any]:
    entry_notional = margin_usdt * leverage
    quantity = entry_notional / entry_price
    return {
        "leg": leg_number,
        "entry_price": entry_price,
        "raw_entry_price": entry_price,
        "entry_ts": _to_iso(entry_ts),
        "tp_price": _target_price(entry_price, tp_pct=tp_pct, direction=direction),
        "margin_usdt": margin_usdt,
        "entry_notional_usdt": entry_notional,
        "quantity": quantity,
        "entry_fee_usdt": entry_notional * fee_rate,
        "entry_slippage_usdt": entry_notional * slippage_rate,
    }


def _close_leg(
    leg: dict[str, Any],
    *,
    exit_status: str,
    exit_price: float,
    exit_ts: datetime,
    direction: str,
    fee_rate: float,
    slippage_rate: float,
) -> None:
    exit_notional = leg["quantity"] * exit_price
    gross_pnl = (exit_price - leg["entry_price"]) * leg["quantity"] if direction == "LONG" else (leg["entry_price"] - exit_price) * leg["quantity"]
    leg.update({
        "exit_status": exit_status,
        "exit_price": exit_price,
        "raw_exit_price": exit_price,
        "exit_ts": _to_iso(exit_ts),
        "exit_notional_usdt": exit_notional,
        "exit_fee_usdt": exit_notional * fee_rate,
        "exit_slippage_usdt": exit_notional * slippage_rate,
        "gross_pnl_usdt": gross_pnl,
        "net_pnl_usdt": gross_pnl - leg["entry_fee_usdt"] - exit_notional * fee_rate - leg["entry_slippage_usdt"] - exit_notional * slippage_rate,
        "move_pct": _direction_move_pct(direction, leg["entry_price"], exit_price),
    })


def _protected_stop_price(entry: float, *, pct: float, direction: str) -> float:
    return entry * (1 + pct / 100) if direction == "LONG" else entry * (1 - pct / 100)


def _resolve_exit_status(legs: list[dict[str, Any]]) -> str:
    statuses = {leg.get("exit_status") for leg in legs}
    return next(iter(statuses)) if len(statuses) == 1 else "MIXED"


def _format_leg(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg": leg["leg"],
        "entry_ts": leg["entry_ts"],
        "exit_ts": leg.get("exit_ts"),
        "entry_price": round(leg["entry_price"], 8),
        "raw_entry_price": round(leg.get("raw_entry_price", leg["entry_price"]), 8),
        "exit_price": round(leg.get("exit_price", 0), 8),
        "raw_exit_price": round(leg.get("raw_exit_price", leg.get("exit_price", 0)), 8),
        "tp_price": round(leg["tp_price"], 8),
        "exit_status": leg.get("exit_status"),
        "margin_usdt": _round_money(leg["margin_usdt"]),
        "entry_notional_usdt": _round_money(leg["entry_notional_usdt"]),
        "exit_notional_usdt": _round_money(leg.get("exit_notional_usdt", 0)),
        "quantity": round(leg["quantity"], 10),
        "entry_fee_usdt": _round_money(leg["entry_fee_usdt"]),
        "exit_fee_usdt": _round_money(leg.get("exit_fee_usdt", 0)),
        "gross_pnl_usdt": _round_money(leg.get("gross_pnl_usdt", 0)),
        "net_pnl_usdt": _round_money(leg.get("net_pnl_usdt", 0)),
        "move_pct": round(leg.get("move_pct", 0), 8),
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _max_position_notional(asset: str, leverage: float) -> float:
    """Max notional per position based on OKX position tiers and order book liquidity.

    OKX altcoin perpetuals cap at 10x leverage from Tier 1 with 5% MMR.
    Beyond exchange limits, order book depth is the real constraint — a
    large market order moves the price, eating into TP/SL.

    These caps represent the practical maximum notional for a single
    position before market impact becomes significant (>0.1% slippage
    on a single market order).
    """
    asset_upper = asset.upper()
    # Major assets: deep order books, OKX allows higher tiers
    if asset_upper in {"BTC", "ETH"}:
        return 2_000_000.0
    # Mid-cap alts: moderate liquidity
    if asset_upper in {"LINK", "AVAX", "SOL", "DOGE", "XRP", "ADA"}:
        return 500_000.0
    # Smaller alts: thinner order books
    if asset_upper in {"AAVE", "ARB", "INJ", "OP", "SUI", "APT", "TIA"}:
        return 300_000.0
    # Default for unknown small-caps
    return 200_000.0


def _used_margin(open_positions: dict[str, dict[str, Any]], *, exclude: str | None = None) -> float:
    """Total isolated margin reserved by currently filled open legs."""
    total = 0.0
    for asset, position in open_positions.items():
        if asset == exclude:
            continue
        total += _position_margin(position)
    return total


def _position_margin(position: dict[str, Any]) -> float:
    return sum(float(leg.get("margin_usdt") or 0) for leg in position.get("legs", []) if not leg.get("exit_status"))


def _initial_leg_margin(*, ctx: dict[str, Any], signal: dict[str, Any], margin_budget: float) -> float:
    policy = _candidate_policy_for_direction(ctx["candidate"], signal["direction"])
    max_legs = int((policy.get("pyramid") or {}).get("max_legs", 1))
    max_legs = max(1, max_legs)
    return margin_budget / max_legs


def _continuous_5m_timeline(timestamps: list[datetime]) -> list[datetime]:
    if not timestamps:
        return []
    start = timestamps[0]
    end = timestamps[-1]
    timeline: list[datetime] = []
    current = start
    while current <= end:
        timeline.append(current)
        current += timedelta(minutes=5)
    return timeline


def _unrealized_pnl(
    open_positions: dict[str, dict[str, Any]],
    candles_by_asset: dict[str, dict[datetime, dict[str, Any]]],
    ts: datetime,
    *,
    exclude: str | None = None,
) -> float:
    """Sum of unrealized PnL across all open positions at the given timestamp."""
    total = 0.0
    for asset, position in open_positions.items():
        if asset == exclude:
            continue
        candle = candles_by_asset.get(asset, {}).get(ts)
        if candle is None:
            continue
        mark = float(candle["close"])
        direction = position["direction"]
        for leg in position["legs"]:
            if leg.get("exit_status"):
                continue
            entry = float(leg["entry_price"])
            qty = float(leg["quantity"])
            if direction == "LONG":
                total += (mark - entry) * qty
            else:
                total += (entry - mark) * qty
    return total


def _skip_record(
    *,
    asset: str,
    signal: dict[str, Any],
    reason: str,
    account_equity: float,
    isolated_margin: float,
    available_cash: float,
    requested_margin: float | None = None,
) -> dict[str, Any]:
    return {
        "asset": asset,
        "signal_id": signal["signal_id"],
        "signal_ts": _to_iso(signal["signal_ts"]),
        "skip_reason": reason,
        "requested_margin_usdt": _round_money(requested_margin) if requested_margin is not None else None,
        "used_margin_usdt": _round_money(isolated_margin),
        "isolated_margin_usdt": _round_money(isolated_margin),
        "available_cash_usdt": _round_money(available_cash),
        "free_margin_usdt": _round_money(available_cash),
        "equity_usdt": _round_money(account_equity),
    }


def _account_summary(
    *,
    initial_capital_usdt: float,
    ending_equity_usdt: float,
    trades: list[dict[str, Any]],
    equity_curve: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    gross_pnl = sum(float(trade.get("gross_pnl_usdt") or 0) for trade in trades)
    net_pnl = ending_equity_usdt - initial_capital_usdt
    fees = sum(float(trade.get("total_fees_usdt") or 0) for trade in trades)

    # Per-trade returns for risk metrics
    pnls = [float(t.get("net_pnl_usdt") or 0) for t in trades]
    winning = [p for p in pnls if p > 0]
    losing = [p for p in pnls if p < 0]
    win_rate = len(winning) / len(pnls) * 100 if pnls else 0.0
    avg_win = sum(winning) / len(winning) if winning else 0.0
    avg_loss = abs(sum(losing) / len(losing)) if losing else 0.0
    profit_factor = sum(winning) / abs(sum(losing)) if losing and sum(losing) != 0 else float("inf") if winning else 0.0
    expectancy = (sum(pnls) / len(pnls)) if pnls else 0.0
    largest_win = max(winning) if winning else 0.0
    largest_loss = min(losing) if losing else 0.0

    # Drawdown from equity curve
    max_dd_pct = 0.0
    max_dd_usdt = 0.0
    if equity_curve:
        peak = float(equity_curve[0].get("equity_usdt") or initial_capital_usdt)
        for point in equity_curve:
            eq = float(point.get("equity_usdt") or 0)
            if eq > peak:
                peak = eq
            dd = peak - eq
            dd_pct = dd / peak * 100 if peak > 0 else 0.0
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct
                max_dd_usdt = dd

    # Sharpe ratio (annualized, assuming 5m candles — 105,120 per year)
    # Uses per-trade returns, not per-candle, so we annualize by trade frequency
    sharpe_ratio = 0.0
    sortino_ratio = 0.0
    if len(pnls) > 1:
        import statistics
        mean_ret = statistics.mean(pnls)
        stdev_ret = statistics.stdev(pnls)
        # Estimate trades per year from equity curve time span
        trades_per_year = len(pnls)
        if equity_curve and len(equity_curve) > 1:
            first_ts = equity_curve[0].get("timestamp")
            last_ts = equity_curve[-1].get("timestamp")
            if first_ts and last_ts:
                from datetime import datetime as _dt
                try:
                    start_dt = _coerce_datetime(first_ts) if first_ts else None
                    end_dt = _coerce_datetime(last_ts) if last_ts else None
                    if start_dt and end_dt:
                        span_days = max((end_dt - start_dt).total_seconds() / 86400, 1.0)
                        trades_per_year = len(pnls) / span_days * 365
                except Exception:
                    pass
        if stdev_ret > 0:
            sharpe_ratio = (mean_ret / stdev_ret) * (trades_per_year ** 0.5)
        downside_dev = statistics.stdev([p for p in pnls if p < 0]) if len([p for p in pnls if p < 0]) > 1 else 0.0
        if downside_dev > 0:
            sortino_ratio = (mean_ret / downside_dev) * (trades_per_year ** 0.5)

    return {
        "initial_capital_usdt": _round_money(initial_capital_usdt),
        "ending_equity_usdt": _round_money(ending_equity_usdt),
        "gross_pnl_usdt": _round_money(gross_pnl),
        "net_pnl_usdt": _round_money(net_pnl),
        "total_fees_usdt": _round_money(fees),
        "return_pct": round(net_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
        "gross_return_pct": round(gross_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
        "win_rate_pct": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "expectancy_usdt": _round_money(expectancy),
        "avg_win_usdt": _round_money(avg_win),
        "avg_loss_usdt": _round_money(avg_loss),
        "largest_win_usdt": _round_money(largest_win),
        "largest_loss_usdt": _round_money(largest_loss),
        "max_drawdown_pct": round(max_dd_pct, 4),
        "max_drawdown_usdt": _round_money(max_dd_usdt),
        "sharpe_ratio": round(sharpe_ratio, 4),
        "sortino_ratio": round(sortino_ratio, 4),
    }


def _curve_point(timestamp: datetime | None, *, account_equity: float, isolated_margin: float, available_cash: float) -> dict[str, Any]:
    return {
        "timestamp": _to_iso(timestamp) if timestamp else None,
        "equity_usdt": _round_money(account_equity),
        "used_margin_usdt": _round_money(isolated_margin),
        "isolated_margin_usdt": _round_money(isolated_margin),
        "available_cash_usdt": _round_money(available_cash),
        "free_margin_usdt": _round_money(available_cash),
    }


# ---------------------------------------------------------------------------
# Artifact management (unchanged from original)
# ---------------------------------------------------------------------------

def _write_artifacts(*, workspace_root: Path, universe_run_id: str, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    root = _portfolio_root(workspace_root, universe_run_id)
    run_root = root / "runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    latest_path = root / "portfolio_backtest.json"
    latest_ledger_path = root / "portfolio_trade_ledger.json"
    latest_skipped_path = root / "portfolio_skipped_signals.json"
    run_path = run_root / "portfolio_backtest.json"
    run_ledger_path = run_root / "portfolio_trade_ledger.json"
    run_skipped_path = run_root / "portfolio_skipped_signals.json"
    summary_path = run_root / "portfolio_summary.md"
    ledger = {"schema_version": "0.1", "run_id": run_id, "trades": payload["trade_ledger"]}
    skipped = {"schema_version": "0.1", "run_id": run_id, "skipped_signals": payload["skipped_signals"]}
    summary = _render_summary(payload)
    for path, value in ((run_path, payload), (latest_path, payload), (run_ledger_path, ledger), (latest_ledger_path, ledger), (run_skipped_path, skipped), (latest_skipped_path, skipped)):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2) + "\n")
    summary_path.write_text(summary)
    index_path = root / "runs" / "index.json"
    index = json.loads(index_path.read_text()) if index_path.is_file() else {"schema_version": "0.1", "artifact_role": "portfolio_backtest_run_index", "runs": []}
    index["runs"] = [item for item in index.get("runs", []) if item.get("run_id") != run_id] + [
        {"run_id": run_id, "created_at": payload["created_at"], "summary": payload["summary"], "account": payload["account"], "portfolio_backtest_path": str(run_path)}
    ]
    index["latest_run_id"] = run_id
    index_path.write_text(json.dumps(index, indent=2) + "\n")
    return {
        **payload,
        "run_root": str(run_root),
        "portfolio_backtest_path": str(latest_path),
        "trade_ledger_path": str(latest_ledger_path),
        "skipped_signals_path": str(latest_skipped_path),
        "run_portfolio_backtest_path": str(run_path),
        "run_trade_ledger_path": str(run_ledger_path),
        "run_skipped_signals_path": str(run_skipped_path),
        "summary_path": str(summary_path),
        "run_index_path": str(index_path),
    }


def _portfolio_root(workspace_root: Path, universe_run_id: str) -> Path:
    return workspace_root / "dev" / "portfolio_backtests" / universe_run_id


def list_portfolio_backtest_runs(*, workspace_root: Path, universe_run_id: str) -> dict[str, Any]:
    root = _portfolio_root(workspace_root, universe_run_id)
    index_path = root / "runs" / "index.json"
    if not index_path.is_file():
        return {
            "schema_version": "0.1",
            "artifact_role": "portfolio_backtest_run_index",
            "universe_run_id": universe_run_id,
            "latest_run_id": None,
            "runs": [],
        }
    index = json.loads(index_path.read_text())
    runs = sorted(index.get("runs", []), key=lambda item: str(item.get("created_at") or ""), reverse=True)
    return {
        **index,
        "universe_run_id": universe_run_id,
        "runs": runs,
    }


def read_portfolio_backtest_run(*, workspace_root: Path, universe_run_id: str, run_id: str) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    path = _portfolio_root(workspace_root, universe_run_id) / "runs" / run_id / "portfolio_backtest.json"
    if not path.is_file():
        raise FileNotFoundError(f"portfolio backtest run not found: {run_id}")
    return json.loads(path.read_text())


def delete_portfolio_backtest_run(*, workspace_root: Path, universe_run_id: str, run_id: str) -> dict[str, Any]:
    run_id = _validate_run_id(run_id)
    root = _portfolio_root(workspace_root, universe_run_id)
    runs_root = root / "runs"
    index_path = runs_root / "index.json"
    run_root = runs_root / run_id
    if not run_root.is_dir():
        raise FileNotFoundError(f"portfolio backtest run not found: {run_id}")
    index = json.loads(index_path.read_text()) if index_path.is_file() else {"schema_version": "0.1", "artifact_role": "portfolio_backtest_run_index", "runs": []}
    previous_latest = index.get("latest_run_id")
    shutil.rmtree(run_root)
    remaining_runs = [item for item in index.get("runs", []) if item.get("run_id") != run_id]
    remaining_runs = sorted(remaining_runs, key=lambda item: str(item.get("created_at") or ""))
    next_latest = remaining_runs[-1]["run_id"] if remaining_runs else None
    index["runs"] = remaining_runs
    index["latest_run_id"] = next_latest
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(json.dumps(index, indent=2) + "\n")
    if previous_latest == run_id:
        _sync_latest_files_after_delete(root=root, next_latest=next_latest)
    return {
        "schema_version": "0.1",
        "artifact_role": "portfolio_backtest_delete_result",
        "universe_run_id": universe_run_id,
        "deleted_run_id": run_id,
        "latest_run_id": next_latest,
        "remaining_run_count": len(remaining_runs),
        "runs": list(reversed(remaining_runs)),
    }


def _validate_run_id(run_id: str) -> str:
    value = str(run_id).strip()
    if not value or Path(value).name != value or value in {".", ".."}:
        raise ValueError(f"invalid portfolio backtest run id: {run_id}")
    return value


def _sync_latest_files_after_delete(*, root: Path, next_latest: str | None) -> None:
    latest_paths = {
        "portfolio_backtest.json": root / "portfolio_backtest.json",
        "portfolio_trade_ledger.json": root / "portfolio_trade_ledger.json",
        "portfolio_skipped_signals.json": root / "portfolio_skipped_signals.json",
    }
    if next_latest is None:
        for path in latest_paths.values():
            path.unlink(missing_ok=True)
        return
    run_root = root / "runs" / next_latest
    for filename, latest_path in latest_paths.items():
        source = run_root / filename
        if source.is_file():
            shutil.copyfile(source, latest_path)


def _render_summary(payload: dict[str, Any]) -> str:
    account = payload["account"]
    summary = payload["summary"]
    return "\n".join(
        [
            "# Portfolio Backtest",
            "",
            f"Pool: `{payload['universe_run_id']}`",
            f"Mode: `candle_by_candle`",
            f"Ending equity: `${account['ending_equity_usdt']:.4f}`",
            f"Net PnL: `${account['net_pnl_usdt']:.4f}`",
            f"Available cash: `${summary.get('available_cash_usdt', 0):.4f}`",
            f"Isolated margin: `${summary.get('isolated_margin_usdt', 0):.4f}`",
            f"Continuous 5m steps: `{summary.get('continuous_5m_steps', 0)}`",
            f"Data gap candles: `{summary.get('data_gap_candles', 0)}`",
            f"Executed positions: `{summary['executed_positions']}`",
            f"Skipped signals: `{summary['skipped_signals']}`",
            f"Insufficient margin skips: `{summary['skipped_insufficient_margin']}`",
            f"Pyramid margin skips: `{summary.get('skipped_pyramid_margin', 0)}`",
            f"Same-asset skips: `{summary['skipped_asset_open']}`",
            "",
        ]
    )


def _run_id(created_at: datetime, root: Path) -> str:
    base = f"portfolio-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    run_id = base
    suffix = 2
    while (root / "runs" / run_id).exists():
        run_id = f"{base}-{suffix}"
        suffix += 1
    return run_id


def _resolve_path(workspace_root: Path, value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else workspace_root / path


def _to_iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _round_money(value: float | None) -> float:
    return round(float(value or 0.0), 4)


def _date_string(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def _sample_window(session: dict[str, Any], sample_method: str) -> tuple[str, str]:
    if sample_method == "training":
        return _date_string(session["train_start"]), _date_string(session["train_end"])
    if sample_method == "walk_forward_test":
        return _date_string(session["walk_forward_start"]), _date_string(session["walk_forward_end"])
    raise ValueError(f"Unsupported sample method: {sample_method}")


def _add_hours(value: str, hours: int) -> str:
    cleaned = value.replace("Z", "+00:00")
    return (datetime.fromisoformat(cleaned) + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")
