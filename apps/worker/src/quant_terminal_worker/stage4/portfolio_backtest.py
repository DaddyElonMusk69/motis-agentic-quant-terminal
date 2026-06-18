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
            "margin_allocation_pct": ctx["margin_allocation_pct"],
            "leverage": ctx["leverage"],
            "signal_count": len(ctx["signal_inputs"]),
            "candle_count": len(ctx["candles"]),
        }
        for ctx in asset_contexts
    ]

    payload = {
        "schema_version": "0.2",
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
        realized_path = artifact_root / "promotion" / "stage4_realized_expectancy.json"
        candidates_path = artifact_root / "promotion" / "stage4_candidates.json"
        scores_path = artifact_root / "promotion" / "stage1a_canonical_full_cycle_scores.json"
        if not realized_path.is_file() or not scores_path.is_file():
            continue
        realized = json.loads(realized_path.read_text())
        scores = json.loads(scores_path.read_text())
        best_candidate_id = str(realized.get("best_candidate_id") or (realized.get("best_candidate") or {}).get("candidate_id"))
        asset = str(session.get("asset") or realized.get("asset") or "").upper()
        if not asset:
            continue
        margin_pct = float(margin_allocations_pct.get(asset, 30.0))

        # Load Stage 4 setup
        candidates_payload = json.loads(candidates_path.read_text()) if candidates_path.is_file() else {"candidates": []}
        normalized = _normalize_candidates(candidates_payload)
        best_candidate = next((c for c in normalized if c["candidate_id"] == best_candidate_id), None)
        if best_candidate is None:
            continue

        cost = realized.get("cost_assumptions", {})
        fees_bps = float(cost.get("fees_bps_per_side", 5.0))
        slippage_bps = float(cost.get("slippage_bps_per_side", 0.0))

        # Load signals
        signal_inputs = _load_signal_inputs(
            session=session,
            scores=scores,
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

        contexts.append({
            "asset": asset,
            "session_id": session["session_id"],
            "source_candidate_id": session.get("source_candidate_id"),
            "stage4_run_id": realized.get("run_id"),
            "stage4_candidate_id": best_candidate_id,
            "candidate": best_candidate,
            "leverage": float(best_candidate.get("leverage", 1.0)),
            "margin_allocation_pct": margin_pct,
            "fees_bps_per_side": fees_bps,
            "slippage_bps_per_side": slippage_bps,
            "signal_inputs": signal_inputs,
            "candles": candles,
        })
    return sorted(contexts, key=lambda item: item["asset"])


def _load_signal_inputs(
    *,
    session: dict[str, Any],
    scores: dict[str, Any],
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
        inputs.append({
            "signal_id": signal_id,
            "signal_ts": signal_ts,
            "reference_price": float(reference_price),
            "direction": direction,
            "agreement": record.get("agreement"),
        })
    inputs.sort(key=lambda item: (item["signal_ts"], item["signal_id"]))
    return inputs


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
    equity = float(initial_capital_usdt)
    open_positions: dict[str, dict[str, Any]] = {}
    trade_ledger: list[dict[str, Any]] = []
    skipped_signals: list[dict[str, Any]] = []
    equity_curve: list[dict[str, Any]] = [
        {"timestamp": None, "equity_usdt": _round_money(equity), "used_margin_usdt": 0.0, "free_margin_usdt": _round_money(equity)}
    ]

    # Build per-asset candle index and signal cursor
    candles_by_asset: dict[str, dict[datetime, dict[str, Any]]] = {}
    signal_cursors: dict[str, int] = {}
    all_timestamps: set[datetime] = set()

    for ctx in asset_contexts:
        asset = ctx["asset"]
        candle_index: dict[datetime, dict[str, Any]] = {}
        for candle in ctx["candles"]:
            candle_index[candle["timestamp"]] = candle
            all_timestamps.add(candle["timestamp"])
        candles_by_asset[asset] = candle_index
        signal_cursors[asset] = 0

    unified_timeline = sorted(all_timestamps)

    for ts in unified_timeline:
        # Phase 1: Manage existing positions
        closed_assets: list[str] = []
        for asset, position in list(open_positions.items()):
            candle = candles_by_asset[asset].get(ts)
            if candle is None:
                continue
            closed = _manage_position(
                position=position,
                candle=candle,
                ts=ts,
                equity=equity,
            )
            if closed:
                net_pnl = closed["net_pnl"]
                equity += net_pnl
                trade_ledger.append(closed["trade"])
                closed_assets.append(asset)
                equity_curve.append(_curve_point(ts, equity=equity, used_margin=_used_margin(open_positions, exclude=asset)))
        for asset in closed_assets:
            open_positions.pop(asset, None)

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
                        equity=equity, used_margin=_used_margin(open_positions),
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
                    asset=asset, signal=signal, reason="no_trade_decision",
                    equity=equity, used_margin=_used_margin(open_positions),
                ))
                continue

            # Check margin
            margin_needed = equity * ctx["margin_allocation_pct"] / 100
            free_margin = equity - _used_margin(open_positions)
            if margin_needed > free_margin:
                skipped_signals.append(_skip_record(
                    asset=asset, signal=signal, reason="insufficient_free_margin",
                    equity=equity, used_margin=_used_margin(open_positions),
                    requested_margin=margin_needed,
                ))
                continue

            # Open position
            position = _open_position(
                asset=asset,
                signal=signal,
                candle=candle,
                ctx=ctx,
                equity=equity,
                margin_budget=margin_needed,
            )
            open_positions[asset] = position
            equity_curve.append(_curve_point(ts, equity=equity, used_margin=_used_margin(open_positions)))

    # Close any remaining open positions at the last available candle
    for asset, position in list(open_positions.items()):
        ctx = next(c for c in asset_contexts if c["asset"] == asset)
        if ctx["candles"]:
            last_candle = ctx["candles"][-1]
            closed = _force_close_position(
                position=position,
                candle=last_candle,
                equity=equity,
            )
            if closed:
                equity += closed["net_pnl"]
                trade_ledger.append(closed["trade"])
                equity_curve.append(_curve_point(last_candle["timestamp"], equity=equity, used_margin=0.0))
                open_positions.pop(asset, None)

    account = _account_summary(
        initial_capital_usdt=initial_capital_usdt,
        ending_equity_usdt=equity,
        trades=trade_ledger,
    )
    summary = {
        "eligible_asset_count": len(asset_contexts),
        "total_signals": sum(len(ctx["signal_inputs"]) for ctx in asset_contexts),
        "executed_positions": len(trade_ledger),
        "skipped_signals": len(skipped_signals),
        "skipped_insufficient_margin": sum(1 for item in skipped_signals if item["skip_reason"] == "insufficient_free_margin"),
        "skipped_asset_open": sum(1 for item in skipped_signals if item["skip_reason"] == "asset_position_open"),
        "skipped_no_trade": sum(1 for item in skipped_signals if item["skip_reason"] == "no_trade_decision"),
    }
    return {
        "account": account,
        "summary": summary,
        "equity_curve": equity_curve,
        "trade_ledger": trade_ledger,
        "skipped_signals": skipped_signals,
    }


def _manage_position(
    *,
    position: dict[str, Any],
    candle: dict[str, Any],
    ts: datetime,
    equity: float,
) -> dict[str, Any] | None:
    ctx = position["ctx"]
    candidate = ctx["candidate"]
    direction = position["direction"]
    policy = _candidate_policy_for_direction(candidate, direction)
    fee_rate = ctx["fees_bps_per_side"] / 10_000
    slippage_rate = ctx["slippage_bps_per_side"] / 10_000

    # Check hard exit
    cutoff = position["signal_ts"] + timedelta(hours=policy["max_hold_hours"])
    if ts >= cutoff:
        return _close_position(position=position, exit_price=candle["close"], exit_ts=ts, equity=equity, exit_status="HARD_EXIT")

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
        exit_status = _resolve_dual_hit(candle, direction=direction) if tp_hit and sl_hit else "TP" if tp_hit else "SL"
        if exit_status == "SL":
            exit_status = "PROTECTED_SL" if position["active_sl_kind"] == "protected" else "INITIAL_SL"
        exit_price = leg["tp_price"] if exit_status == "TP" else sl_price
        closed_legs.append((leg, exit_status, exit_price, ts))

    for leg, exit_status, exit_price, exit_ts in closed_legs:
        _close_leg(leg, exit_status=exit_status, exit_price=exit_price, exit_ts=exit_ts, direction=direction, fee_rate=fee_rate, slippage_rate=slippage_rate)

    active_legs = [leg for leg in position["legs"] if not leg.get("exit_status")]
    if not active_legs:
        # All legs closed — close the position
        return _close_position(position=position, exit_price=closed_legs[-1][2], exit_ts=ts, equity=equity, exit_status=_resolve_exit_status(position["legs"]))

    return None


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
    equity: float,
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
    entry_slippage = sum(float(leg.get("entry_slippage_usdt") or 0) for leg in legs)
    exit_slippage = sum(float(leg.get("exit_slippage_usdt") or 0) for leg in legs)
    position_margin = sum(float(leg.get("margin_usdt") or 0) for leg in legs)
    position_notional = sum(float(leg.get("entry_notional_usdt") or 0) for leg in legs)
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
        "total_slippage_usdt": _round_money(entry_slippage + exit_slippage),
        "equity_before": _round_money(equity),
        "equity_after": _round_money(equity_after),
        "net_pnl_pct": round(net_pnl / equity * 100, 8) if equity else 0.0,
        "gross_pnl_pct": round(gross_pnl / equity * 100, 8) if equity else 0.0,
        "roe_pct": round(net_pnl / position_margin * 100, 8) if position_margin > 0 else 0.0,
        "leg_details": [_format_leg(leg) for leg in legs],
        "portfolio_entry_equity_usdt": _round_money(equity),
        "portfolio_margin_allocation_pct": ctx["margin_allocation_pct"],
    }
    return {"trade": trade, "net_pnl": net_pnl}


def _force_close_position(
    *,
    position: dict[str, Any],
    candle: dict[str, Any],
    equity: float,
) -> dict[str, Any] | None:
    open_legs = [leg for leg in position["legs"] if not leg.get("exit_status")]
    if not open_legs:
        return None
    return _close_position(
        position=position,
        exit_price=candle["close"],
        exit_ts=candle["timestamp"],
        equity=equity,
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
        "exit_price": round(leg.get("exit_price", 0), 8),
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

def _used_margin(open_positions: dict[str, dict[str, Any]], *, exclude: str | None = None) -> float:
    total = 0.0
    for asset, position in open_positions.items():
        if asset == exclude:
            continue
        for leg in position["legs"]:
            if not leg.get("exit_status"):
                total += float(leg.get("margin_usdt") or 0)
    return total


def _skip_record(
    *,
    asset: str,
    signal: dict[str, Any],
    reason: str,
    equity: float,
    used_margin: float,
    requested_margin: float | None = None,
) -> dict[str, Any]:
    return {
        "asset": asset,
        "signal_id": signal["signal_id"],
        "signal_ts": _to_iso(signal["signal_ts"]),
        "skip_reason": reason,
        "requested_margin_usdt": _round_money(requested_margin) if requested_margin is not None else None,
        "used_margin_usdt": _round_money(used_margin),
        "free_margin_usdt": _round_money(equity - used_margin),
        "equity_usdt": _round_money(equity),
    }


def _account_summary(*, initial_capital_usdt: float, ending_equity_usdt: float, trades: list[dict[str, Any]]) -> dict[str, Any]:
    gross_pnl = sum(float(trade.get("gross_pnl_usdt") or 0) for trade in trades)
    net_pnl = ending_equity_usdt - initial_capital_usdt
    fees = sum(float(trade.get("total_fees_usdt") or 0) for trade in trades)
    return {
        "initial_capital_usdt": _round_money(initial_capital_usdt),
        "ending_equity_usdt": _round_money(ending_equity_usdt),
        "gross_pnl_usdt": _round_money(gross_pnl),
        "net_pnl_usdt": _round_money(net_pnl),
        "total_fees_usdt": _round_money(fees),
        "return_pct": round(net_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
        "gross_return_pct": round(gross_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
    }


def _curve_point(timestamp: datetime | None, *, equity: float, used_margin: float) -> dict[str, Any]:
    return {
        "timestamp": _to_iso(timestamp) if timestamp else None,
        "equity_usdt": _round_money(equity),
        "used_margin_usdt": _round_money(used_margin),
        "free_margin_usdt": _round_money(equity - used_margin),
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
            f"Executed positions: `{summary['executed_positions']}`",
            f"Skipped signals: `{summary['skipped_signals']}`",
            f"Insufficient margin skips: `{summary['skipped_insufficient_margin']}`",
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
