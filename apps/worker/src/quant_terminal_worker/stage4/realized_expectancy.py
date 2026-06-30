from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage2.capture_curve import get_reference_price
from quant_terminal_worker.stage3.grid_search import DEFAULT_FORWARD_HOURS, DEFAULT_LEVERAGE


DEFAULT_FEES_BPS_PER_SIDE = 5.0
DEFAULT_SLIPPAGE_BPS_PER_SIDE = 0.0


def run_stage4_realized_expectancy(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    initial_capital_usdt: float = 10_000.0,
    margin_allocation_pct: float = 30.0,
    leverage: float = DEFAULT_LEVERAGE,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
    slippage_bps_per_side: float = DEFAULT_SLIPPAGE_BPS_PER_SIDE,
) -> dict[str, Any]:
    if initial_capital_usdt <= 0:
        raise ValueError("initial_capital_usdt must be greater than zero")
    if margin_allocation_pct <= 0 or margin_allocation_pct > 100:
        raise ValueError("margin_allocation_pct must be between 0 and 100")
    if leverage <= 0:
        raise ValueError("leverage must be greater than zero")
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    stage1_scores_path = promotion_root / "stage1a_canonical_full_cycle_scores.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    stage1_scores = _read_json(stage1_scores_path)
    records = stage1_scores.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("Stage 4 requires non-empty canonical Stage 1 score records.")
    candidates_payload = _read_json(candidates_path)
    candidates = _normalize_candidates(candidates_payload)
    if not candidates:
        raise ValueError("Stage 4 requires at least one Stage 4 candidate.")

    signals_by_id = _index_signals(signal_rows)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    slice_windows = _slice_windows(session)
    results = []
    ledger_candidates = []
    for candidate in candidates:
        result, trades = _score_candidate(
            candidate=candidate,
            records=records,
            signals_by_id=signals_by_id,
            candles=candle_rows,
            initial_capital_usdt=initial_capital_usdt,
            margin_allocation_pct=margin_allocation_pct,
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
            slippage_bps_per_side=slippage_bps_per_side,
            slice_windows=slice_windows,
        )
        results.append(result)
        ledger_candidates.append(
            {
                "candidate_id": candidate["candidate_id"],
                "setup": candidate,
                "trades": trades,
            }
        )
    best = _choose_best_candidate(results)
    created_at_dt = datetime.now(UTC)
    created_at = created_at_dt.isoformat().replace("+00:00", "Z")
    run_id = _stage4_run_id(created_at_dt, promotion_root)
    ledger = {
        "schema_version": "0.1",
        "stage": "stage4_trade_ledger",
        "created_at": created_at,
        "run_id": run_id,
        "session_id": session["session_id"],
        "candidates": ledger_candidates,
    }
    payload = {
        "schema_version": "0.1",
        "stage": "stage4_realized_expectancy",
        "artifact_role": "stage4_realized_expectancy",
        "created_at": created_at,
        "run_id": run_id,
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "stage1_scores_path": str(stage1_scores_path),
        "candidates_path": str(candidates_path),
        "cost_assumptions": {
            "fee_source": "okx_usdt_margin_swap_level_1_default",
            "fee_side": "taker",
            "fees_bps_per_side": fees_bps_per_side,
            "slippage_bps_per_side": slippage_bps_per_side,
        },
        "simulation_inputs": {
            "initial_capital_usdt": initial_capital_usdt,
            "margin_allocation_pct": margin_allocation_pct,
            "leverage": leverage,
        },
        "slice_windows": [
            {"name": name, "start": start.isoformat().replace("+00:00", "Z"), "end": end.isoformat().replace("+00:00", "Z")}
            for name, start, end in slice_windows
        ],
        "best_candidate_id": best["candidate_id"],
        "best_candidate": best,
        "candidates": results,
        "ledger": ledger,
    }
    promotion_root.mkdir(parents=True, exist_ok=True)
    run_root = promotion_root / "stage4_runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    run_realized_path = run_root / "stage4_realized_expectancy.json"
    run_ledger_path = run_root / "stage4_trade_ledger.json"
    run_optimal_path = run_root / "stage4_optimal.json"
    run_summary_path = run_root / "stage4_summary.md"
    payload["source_run_path"] = str(run_realized_path)
    optimal_payload = {"criterion": "max_net_expectancy", "run_id": run_id, "best": best}
    summary = _render_summary(payload)
    run_realized_path.write_text(json.dumps(payload, indent=2) + "\n")
    run_ledger_path.write_text(json.dumps(ledger, indent=2) + "\n")
    run_optimal_path.write_text(json.dumps(optimal_payload, indent=2) + "\n")
    run_summary_path.write_text(summary)

    realized_path = promotion_root / "stage4_realized_expectancy.json"
    ledger_path = promotion_root / "stage4_trade_ledger.json"
    optimal_path = promotion_root / "stage4_optimal.json"
    summary_path = promotion_root / "stage4_summary.md"
    realized_path.write_text(json.dumps(payload, indent=2) + "\n")
    ledger_path.write_text(json.dumps(ledger, indent=2) + "\n")
    optimal_path.write_text(json.dumps(optimal_payload, indent=2) + "\n")
    summary_path.write_text(summary)
    _update_stage4_runs_index(
        promotion_root=promotion_root,
        run={
            "run_id": run_id,
            "created_at": created_at,
            "simulation_inputs": payload["simulation_inputs"],
            "best_candidate_id": payload["best_candidate_id"],
            "best_candidate": best,
            "account": best.get("account", {}),
            "realized_expectancy_path": str(run_realized_path),
            "trade_ledger_path": str(run_ledger_path),
            "optimal_path": str(run_optimal_path),
            "summary_path": str(run_summary_path),
        },
    )
    return {
        **payload,
        "run_root": str(run_root),
        "run_realized_expectancy_path": str(run_realized_path),
        "run_trade_ledger_path": str(run_ledger_path),
        "run_optimal_path": str(run_optimal_path),
        "run_summary_path": str(run_summary_path),
        "realized_expectancy_path": str(realized_path),
        "trade_ledger_path": str(ledger_path),
        "optimal_path": str(optimal_path),
        "summary_path": str(summary_path),
    }


def delete_stage4_realized_expectancy_run(*, workspace_root: Path, session: dict[str, Any], run_id: str) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    runs_root = promotion_root / "stage4_runs"
    run_root = runs_root / run_id
    index_path = runs_root / "index.json"
    if not run_root.exists():
        raise FileNotFoundError(f"Stage 4 run not found: {run_id}")
    index = _read_json_if_exists(index_path) or {"schema_version": "0.1", "artifact_role": "stage4_run_index", "runs": []}
    remaining_runs = [item for item in index.get("runs", []) if item.get("run_id") != run_id]
    shutil.rmtree(run_root)
    latest = remaining_runs[-1] if remaining_runs else None
    if latest:
        _restore_stage4_latest_from_run(
            promotion_root=promotion_root,
            realized_path=Path(latest["realized_expectancy_path"]),
            ledger_path=Path(latest["trade_ledger_path"]),
            optimal_path=Path(latest["optimal_path"]),
            summary_path=Path(latest["summary_path"]),
        )
    else:
        _clear_stage4_latest_artifacts(promotion_root)
    index_path.parent.mkdir(parents=True, exist_ok=True)
    index_path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "artifact_role": "stage4_run_index",
                "latest_run_id": latest.get("run_id") if latest else None,
                "runs": remaining_runs,
            },
            indent=2,
        )
        + "\n"
    )
    return {
        "session_id": session["session_id"],
        "deleted_run_id": run_id,
        "latest_run_id": latest.get("run_id") if latest else None,
        "remaining_run_count": len(remaining_runs),
        "stage4_runs_index_path": str(index_path),
    }


def _score_candidate(
    *,
    candidate: dict[str, Any],
    records: list[dict[str, Any]],
    signals_by_id: dict[str, dict[str, Any]],
    candles: list[dict[str, Any]],
    initial_capital_usdt: float,
    margin_allocation_pct: float,
    leverage: float,
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    slice_windows: list[tuple[str, datetime, datetime]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    candidate = {**candidate, "leverage": float(leverage)}
    inputs = []
    for record in records:
        signal_id = str(record["signal_id"])
        signal = _find_signal(signals_by_id, signal_id)
        if signal is None:
            raise ValueError(f"Stage 4 signal row not found for canonical decision: {signal_id}")
        packet = _packet_from_signal(signal)
        signal_ts = _coerce_datetime(packet.get("timestamp") or signal["timestamp"])
        reference_price = get_reference_price(packet)
        inputs.append(
            {
                "record": record,
                "signal_id": signal_id,
                "signal_ts": signal_ts,
                "reference_price": reference_price,
                "slice_name": _slice_name(signal_ts, slice_windows),
                "direction": str(record.get("decision_direction") or record.get("agent_direction") or "").upper(),
            }
        )
    inputs.sort(key=lambda item: (item["signal_ts"], item["signal_id"]))

    trades = []
    equity = float(initial_capital_usdt)
    index = 0
    while index < len(inputs):
        item = inputs[index]
        if item["direction"] not in {"LONG", "SHORT"}:
            trades.append(_skipped_decision(item=item, candidate=candidate, reason=str(item["record"].get("stage4b_skip_reason") or "no_trade_decision")))
            index += 1
            continue

        trade = _simulate_account_position(
            item=item,
            candidate=candidate,
            candles=candles,
            equity_before=equity,
            margin_allocation_pct=margin_allocation_pct,
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
            slippage_bps_per_side=slippage_bps_per_side,
        )
        trades.append(trade)
        equity = trade["equity_after"]

        exit_ts = _coerce_datetime(trade["exit_ts"])
        index += 1
        while index < len(inputs) and inputs[index]["signal_ts"] <= exit_ts:
            trades.append(
                _skipped_decision(
                    item=inputs[index],
                    candidate=candidate,
                    reason="position_open",
                    active_position_id=trade["position_id"],
                )
            )
            index += 1

    summary = _summarize_trades(trades, denominator=len(records))
    account = _summarize_account(
        initial_capital_usdt=initial_capital_usdt,
        ending_equity_usdt=equity,
        trades=trades,
    )
    if records:
        summary["gross_pnl_pct"] = account["gross_return_pct"]
        summary["net_pnl_pct"] = account["return_pct"]
        summary["gross_expectancy_pct"] = round(account["gross_return_pct"] / len(records), 8)
        summary["net_expectancy_pct"] = round(account["return_pct"] / len(records), 8)
    by_side = {
        side: _summarize_trades([trade for trade in trades if trade["decision_direction"] == side], denominator=len([trade for trade in trades if trade["decision_direction"] == side]))
        for side in ("LONG", "SHORT")
        if any(trade["decision_direction"] == side for trade in trades)
    }
    slices = {
        name: _summarize_trades([trade for trade in trades if trade["slice_name"] == name], denominator=len([trade for trade in trades if trade["slice_name"] == name]))
        for name, _, _ in slice_windows
        if any(trade["slice_name"] == name for trade in trades)
    }
    mismatch_trades = [trade for trade in trades if trade.get("agreement") == "MISMATCH"]
    return (
        {
            "candidate_id": candidate["candidate_id"],
            "entry_model": candidate["entry_model"],
            "tp_pct": candidate["tp_pct"],
            "sl_pct": candidate["sl_pct"],
            "leverage": leverage,
            "margin_allocation_pct": margin_allocation_pct,
            "setup": candidate,
            "account": account,
            **summary,
            "mismatch_cohort": _summarize_trades(mismatch_trades, denominator=len(mismatch_trades)),
            "by_side": by_side,
            "slices": slices,
        },
        trades,
    )


def _stage4_run_id(created_at: datetime, promotion_root: Path) -> str:
    base = f"stage4-{created_at.strftime('%Y%m%dT%H%M%S%fZ')}"
    run_id = base
    suffix = 2
    while (promotion_root / "stage4_runs" / run_id).exists():
        run_id = f"{base}-{suffix}"
        suffix += 1
    return run_id


def _update_stage4_runs_index(*, promotion_root: Path, run: dict[str, Any]) -> None:
    index_path = promotion_root / "stage4_runs" / "index.json"
    existing = _read_json_if_exists(index_path) or {"schema_version": "0.1", "artifact_role": "stage4_run_index", "runs": []}
    runs = [item for item in existing.get("runs", []) if item.get("run_id") != run["run_id"]]
    runs.append(run)
    index_path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "artifact_role": "stage4_run_index",
                "latest_run_id": run["run_id"],
                "runs": runs,
            },
            indent=2,
        )
        + "\n"
    )


def _restore_stage4_latest_from_run(
    *,
    promotion_root: Path,
    realized_path: Path,
    ledger_path: Path,
    optimal_path: Path,
    summary_path: Path,
) -> None:
    targets = [
        (realized_path, promotion_root / "stage4_realized_expectancy.json"),
        (ledger_path, promotion_root / "stage4_trade_ledger.json"),
        (optimal_path, promotion_root / "stage4_optimal.json"),
        (summary_path, promotion_root / "stage4_summary.md"),
    ]
    for source, target in targets:
        if not source.is_file():
            raise FileNotFoundError(f"Stage 4 run artifact not found: {source}")
        target.write_text(source.read_text())


def _clear_stage4_latest_artifacts(promotion_root: Path) -> None:
    for path in [
        promotion_root / "stage4_realized_expectancy.json",
        promotion_root / "stage4_trade_ledger.json",
        promotion_root / "stage4_optimal.json",
        promotion_root / "stage4_summary.md",
    ]:
        if path.exists():
            path.unlink()


def _simulate_account_position(
    *,
    item: dict[str, Any],
    candidate: dict[str, Any],
    candles: list[dict[str, Any]],
    equity_before: float,
    margin_allocation_pct: float,
    leverage: float,
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
) -> dict[str, Any]:
    direction = item["direction"]
    policy = _candidate_policy_for_direction(candidate, direction)
    signal_ts = item["signal_ts"]
    entry_price = float(item["reference_price"])
    max_legs = int((policy.get("pyramid") or {}).get("max_legs", 1))
    max_legs = max(1, max_legs)
    position_margin_budget = equity_before * margin_allocation_pct / 100
    per_leg_margin = position_margin_budget / max_legs
    fee_rate = fees_bps_per_side / 10_000
    slippage_rate = slippage_bps_per_side / 10_000
    cutoff = signal_ts + timedelta(hours=policy["max_hold_hours"])
    tp_pct = policy["tp_pct"]
    sl_pct = policy["sl_pct"]
    protection_enabled = bool(policy.get("protection_enabled", False))
    protect_trigger_pct = policy.get("protect_trigger_pct")
    trail_sl_pct = policy.get("trail_sl_pct")
    pyramid = policy.get("pyramid") or {}
    step_pct = float(pyramid.get("step_pct", 999))
    sl_breakeven = bool(pyramid.get("sl_breakeven", False))
    sl_price = entry_price * (1 - sl_pct / 100) if direction == "LONG" else entry_price * (1 + sl_pct / 100)
    protected_sl_price = _protected_stop_price(entry_price, pct=float(trail_sl_pct), direction=direction) if protection_enabled and trail_sl_pct is not None else None
    protect_trigger_price = _target_price(entry_price, tp_pct=float(protect_trigger_pct), direction=direction) if protection_enabled and protect_trigger_pct is not None else None
    protection_activated = False
    active_sl_kind = "initial"
    position_id = f"{candidate['candidate_id']}:{item['signal_id']}"
    legs = [
        _open_leg(
            leg_number=1,
            entry_price=entry_price,
            entry_ts=signal_ts,
            tp_pct=tp_pct,
            direction=direction,
            margin_usdt=per_leg_margin,
            leverage=leverage,
            fee_rate=fee_rate,
            slippage_rate=slippage_rate,
        )
    ]
    active = legs.copy()
    last_candle = None

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_ts:
            continue
        if timestamp > cutoff:
            break
        last_candle = candle

        if len(legs) < max_legs:
            next_entry = _next_entry(legs[-1]["entry_price"], step_pct=step_pct, direction=direction)
            if _entry_hit(candle, next_entry, direction=direction):
                leg = _open_leg(
                    leg_number=len(legs) + 1,
                    entry_price=next_entry,
                    entry_ts=timestamp,
                    tp_pct=tp_pct,
                    direction=direction,
                    margin_usdt=per_leg_margin,
                    leverage=leverage,
                    fee_rate=fee_rate,
                    slippage_rate=slippage_rate,
                )
                legs.append(leg)
                active.append(leg)
                if sl_breakeven:
                    sl_price = sum(leg_row["entry_price"] for leg_row in legs) / len(legs)
                    active_sl_kind = "breakeven"

        closed = []
        for leg in active:
            tp_hit, sl_hit = _tp_sl_hit(candle, tp=leg["tp_price"], sl=sl_price, direction=direction)
            if not tp_hit and not sl_hit:
                continue
            exit_status = _resolve_dual_hit(candle, direction=direction) if tp_hit and sl_hit else "TP" if tp_hit else "SL"
            if exit_status == "SL":
                exit_status = "PROTECTED_SL" if active_sl_kind == "protected" else "INITIAL_SL"
            exit_price = leg["tp_price"] if exit_status == "TP" else sl_price
            closed.append((leg, exit_status, exit_price, timestamp))

        for leg, exit_status, exit_price, exit_ts in closed:
            _close_leg(
                leg,
                exit_status=exit_status,
                exit_price=exit_price,
                exit_ts=exit_ts,
                direction=direction,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )
        closed_leg_numbers = {leg["leg"] for leg, _, _, _ in closed}
        active = [leg for leg in active if leg["leg"] not in closed_leg_numbers]
        if not active:
            break

        if (
            protection_enabled
            and not protection_activated
            and protect_trigger_price is not None
            and protected_sl_price is not None
            and _entry_hit(candle, protect_trigger_price, direction=direction)
        ):
            protection_activated = True
            sl_price = protected_sl_price
            active_sl_kind = "protected"

    if active:
        exit_ts = cutoff if last_candle is None else last_candle["timestamp"]
        exit_price = entry_price if policy["timeout_policy"] == "zero" or last_candle is None else last_candle["close"]
        for leg in active:
            _close_leg(
                leg,
                exit_status="HARD_EXIT",
                exit_price=exit_price,
                exit_ts=exit_ts,
                direction=direction,
                fee_rate=fee_rate,
                slippage_rate=slippage_rate,
            )

    total_entry_fees = sum(leg["entry_fee_usdt"] for leg in legs)
    total_exit_fees = sum(leg["exit_fee_usdt"] for leg in legs)
    total_slippage = sum(leg["entry_slippage_usdt"] + leg["exit_slippage_usdt"] for leg in legs)
    gross_pnl = sum(leg["gross_pnl_usdt"] for leg in legs)
    total_cost = total_entry_fees + total_exit_fees + total_slippage
    net_pnl = gross_pnl - total_cost
    statuses = {leg["exit_status"] for leg in legs}
    exit_status = next(iter(statuses)) if len(statuses) == 1 else "MIXED"
    last_exit = max(legs, key=lambda leg: _coerce_datetime(leg["exit_ts"]))
    base = _trade_base(item=item, candidate=policy)
    return {
        **base,
        "position_id": position_id,
        "entry_status": "FILLED",
        "exit_status": exit_status,
        "entry_price": round(entry_price, 8),
        "exit_price": round(last_exit["exit_price"], 8),
        "exit_ts": last_exit["exit_ts"],
        "filled_legs": len(legs),
        "protection_enabled": protection_enabled,
        "protection_activated": protection_activated,
        "active_sl_kind": active_sl_kind,
        "initial_sl_pct": sl_pct,
        "protect_trigger_pct": protect_trigger_pct,
        "trail_sl_pct": trail_sl_pct,
        "gross_pnl_usdt": _round_money(gross_pnl),
        "net_pnl_usdt": _round_money(net_pnl),
        "total_fees_usdt": _round_money(total_entry_fees + total_exit_fees),
        "total_entry_fees_usdt": _round_money(total_entry_fees),
        "total_exit_fees_usdt": _round_money(total_exit_fees),
        "total_slippage_usdt": _round_money(total_slippage),
        "equity_before": _round_money(equity_before),
        "equity_after": _round_money(equity_before + net_pnl),
        "gross_pnl_pct": round(gross_pnl / equity_before * 100, 8) if equity_before else 0.0,
        "net_pnl_pct": round(net_pnl / equity_before * 100, 8) if equity_before else 0.0,
        "cost_pct": round(total_cost / equity_before * 100, 8) if equity_before else 0.0,
        "leg_details": [_format_account_leg(leg) for leg in legs],
    }


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
        "entry_ts": entry_ts.isoformat().replace("+00:00", "Z"),
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
    leg.update(
        {
            "exit_status": exit_status,
            "exit_price": exit_price,
            "exit_ts": exit_ts.isoformat().replace("+00:00", "Z"),
            "exit_notional_usdt": exit_notional,
            "exit_fee_usdt": exit_notional * fee_rate,
            "exit_slippage_usdt": exit_notional * slippage_rate,
            "gross_pnl_usdt": gross_pnl,
            "net_pnl_usdt": gross_pnl - leg["entry_fee_usdt"] - exit_notional * fee_rate - leg["entry_slippage_usdt"] - exit_notional * slippage_rate,
            "move_pct": _direction_move_pct(direction, leg["entry_price"], exit_price),
        }
    )


def _trade_base(*, item: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
    return {
        "candidate_id": candidate["candidate_id"],
        "signal_id": item["signal_id"],
        "signal_ts": item["signal_ts"].isoformat().replace("+00:00", "Z"),
        "slice_name": item["slice_name"],
        "agreement": item["record"].get("agreement"),
        "decision_direction": item["direction"],
        "reference_price": round(float(item["reference_price"]), 8),
    }


def _skipped_decision(
    *,
    item: dict[str, Any],
    candidate: dict[str, Any],
    reason: str,
    active_position_id: str | None = None,
) -> dict[str, Any]:
    return {
        **_trade_base(item=item, candidate=candidate),
        "position_id": active_position_id,
        "entry_status": "SKIPPED",
        "exit_status": "SKIPPED",
        "skip_reason": reason,
        "entry_price": None,
        "exit_price": None,
        "exit_ts": item["signal_ts"].isoformat().replace("+00:00", "Z"),
        "filled_legs": 0,
        "gross_pnl_usdt": 0.0,
        "net_pnl_usdt": 0.0,
        "total_fees_usdt": 0.0,
        "total_entry_fees_usdt": 0.0,
        "total_exit_fees_usdt": 0.0,
        "total_slippage_usdt": 0.0,
        "gross_pnl_pct": 0.0,
        "net_pnl_pct": 0.0,
        "cost_pct": 0.0,
        "leg_details": [],
    }


def _summarize_account(*, initial_capital_usdt: float, ending_equity_usdt: float, trades: list[dict[str, Any]]) -> dict[str, Any]:
    gross_pnl = sum(trade.get("gross_pnl_usdt", 0.0) for trade in trades)
    total_entry_fees = sum(trade.get("total_entry_fees_usdt", 0.0) for trade in trades)
    total_exit_fees = sum(trade.get("total_exit_fees_usdt", 0.0) for trade in trades)
    total_slippage = sum(trade.get("total_slippage_usdt", 0.0) for trade in trades)
    total_fees = total_entry_fees + total_exit_fees
    net_pnl = ending_equity_usdt - initial_capital_usdt
    return {
        "initial_capital_usdt": _round_money(initial_capital_usdt),
        "ending_equity_usdt": _round_money(ending_equity_usdt),
        "gross_pnl_usdt": _round_money(gross_pnl),
        "net_pnl_usdt": _round_money(net_pnl),
        "total_fees_usdt": _round_money(total_fees),
        "total_entry_fees_usdt": _round_money(total_entry_fees),
        "total_exit_fees_usdt": _round_money(total_exit_fees),
        "total_slippage_usdt": _round_money(total_slippage),
        "return_pct": round(net_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
        "gross_return_pct": round(gross_pnl / initial_capital_usdt * 100, 8) if initial_capital_usdt else 0.0,
    }


def _simulate_candidate_trade(
    *,
    record: dict[str, Any],
    candidate: dict[str, Any],
    signal_ts: datetime,
    reference_price: float,
    candles: list[dict[str, Any]],
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    slice_name: str | None,
) -> dict[str, Any]:
    direction = str(record.get("decision_direction") or record.get("agent_direction") or "").upper()
    base = {
        "candidate_id": candidate["candidate_id"],
        "signal_id": record["signal_id"],
        "signal_ts": signal_ts.isoformat().replace("+00:00", "Z"),
        "slice_name": slice_name,
        "agreement": record.get("agreement"),
        "decision_direction": direction,
        "reference_price": round(reference_price, 8),
    }
    if direction not in {"LONG", "SHORT"}:
        return {
            **base,
            "entry_status": "SKIPPED",
            "exit_status": "SKIPPED",
            "entry_price": None,
            "exit_price": None,
            "exit_ts": None,
            "filled_legs": 0,
            "gross_pnl_pct": 0.0,
            "net_pnl_pct": 0.0,
            "cost_pct": 0.0,
            "leg_details": [],
        }

    outcome = (
        _pyramid_outcome(direction=direction, signal_ts=signal_ts, entry_price=reference_price, candidate=candidate, candles=candles)
        if candidate.get("pyramid")
        else _single_leg_outcome(direction=direction, signal_ts=signal_ts, entry_price=reference_price, candidate=candidate, candles=candles)
    )
    gross_pnl_pct = outcome["gross_pnl_pct_unlevered"] * candidate["leverage"]
    cost_pct = _trade_cost_pct(
        leverage=candidate["leverage"],
        legs=outcome["filled_legs"],
        fees_bps_per_side=fees_bps_per_side,
        slippage_bps_per_side=slippage_bps_per_side,
    )
    return {
        **base,
        "entry_status": "FILLED",
        "exit_status": outcome["exit_status"],
        "entry_price": round(outcome["entry_price"], 8),
        "exit_price": round(outcome["exit_price"], 8) if outcome["exit_price"] is not None else None,
        "exit_ts": outcome["exit_ts"].isoformat().replace("+00:00", "Z") if isinstance(outcome["exit_ts"], datetime) else outcome["exit_ts"],
        "filled_legs": outcome["filled_legs"],
        "gross_pnl_pct": round(gross_pnl_pct, 8),
        "net_pnl_pct": round(gross_pnl_pct - cost_pct, 8),
        "cost_pct": round(cost_pct, 8),
        "leg_details": outcome["leg_details"],
    }


def _single_leg_outcome(
    *,
    direction: str,
    signal_ts: datetime,
    entry_price: float,
    candidate: dict[str, Any],
    candles: list[dict[str, Any]],
) -> dict[str, Any]:
    tp_pct = candidate["tp_pct"]
    sl_pct = candidate["sl_pct"]
    cutoff = signal_ts + timedelta(hours=candidate["max_hold_hours"])
    last_candle = None
    tp_price = entry_price * (1 + tp_pct / 100) if direction == "LONG" else entry_price * (1 - tp_pct / 100)
    sl_price = entry_price * (1 - sl_pct / 100) if direction == "LONG" else entry_price * (1 + sl_pct / 100)
    for candle in candles:
        if candle["timestamp"] <= signal_ts:
            continue
        if candle["timestamp"] > cutoff:
            break
        last_candle = candle
        tp_hit, sl_hit = _tp_sl_hit(candle, tp=tp_price, sl=sl_price, direction=direction)
        if not tp_hit and not sl_hit:
            continue
        exit_status = _resolve_dual_hit(candle, direction=direction) if tp_hit and sl_hit else "TP" if tp_hit else "SL"
        exit_price = tp_price if exit_status == "TP" else sl_price
        move = tp_pct if exit_status == "TP" else -sl_pct
        return _outcome(entry_price, exit_price, candle["timestamp"], 1, move, exit_status)
    if candidate["timeout_policy"] == "zero" or last_candle is None:
        return _outcome(entry_price, entry_price, cutoff, 1, 0.0, "TIMEOUT")
    move = _direction_move_pct(direction, entry_price, last_candle["close"])
    return _outcome(entry_price, last_candle["close"], last_candle["timestamp"], 1, move, "TIMEOUT")


def _pyramid_outcome(
    *,
    direction: str,
    signal_ts: datetime,
    entry_price: float,
    candidate: dict[str, Any],
    candles: list[dict[str, Any]],
) -> dict[str, Any]:
    tp_pct = candidate["tp_pct"]
    sl_pct = candidate["sl_pct"]
    pyramid = candidate["pyramid"]
    step_pct = pyramid["step_pct"]
    max_legs = pyramid["max_legs"]
    sl_breakeven = pyramid["sl_breakeven"]
    cutoff = signal_ts + timedelta(hours=candidate["max_hold_hours"])
    sl_price = entry_price * (1 - sl_pct / 100) if direction == "LONG" else entry_price * (1 + sl_pct / 100)
    active = [{"leg": 1, "entry": entry_price, "tp": _target_price(entry_price, tp_pct=tp_pct, direction=direction)}]
    entries = [entry_price]
    last_candle = None
    leg_details = []
    for candle in candles:
        if candle["timestamp"] <= signal_ts:
            continue
        if candle["timestamp"] > cutoff:
            break
        last_candle = candle
        if len(entries) < max_legs:
            next_entry = _next_entry(entries[-1], step_pct=step_pct, direction=direction)
            if _entry_hit(candle, next_entry, direction=direction):
                entries.append(next_entry)
                active.append({"leg": len(entries), "entry": next_entry, "tp": _target_price(next_entry, tp_pct=tp_pct, direction=direction)})
                if sl_breakeven:
                    sl_price = sum(entries) / len(entries)
        closed = []
        for leg in active:
            tp_hit, sl_hit = _tp_sl_hit(candle, tp=leg["tp"], sl=sl_price, direction=direction)
            if not tp_hit and not sl_hit:
                continue
            exit_status = _resolve_dual_hit(candle, direction=direction) if tp_hit and sl_hit else "TP" if tp_hit else "SL"
            exit_price = leg["tp"] if exit_status == "TP" else sl_price
            move = tp_pct if exit_status == "TP" else -abs(leg["entry"] - sl_price) / leg["entry"] * 100
            closed.append({**leg, "exit_status": exit_status, "exit_price": exit_price, "move_pct": move, "exit_ts": candle["timestamp"]})
        if closed:
            closed_ids = {leg["leg"] for leg in closed}
            active = [leg for leg in active if leg["leg"] not in closed_ids]
            leg_details.extend(closed)
            if not active:
                break
    if active:
        timeout_price = entry_price if candidate["timeout_policy"] == "zero" or last_candle is None else last_candle["close"]
        timeout_ts = cutoff if last_candle is None else last_candle["timestamp"]
        for leg in active:
            move = 0.0 if candidate["timeout_policy"] == "zero" or last_candle is None else _direction_move_pct(direction, leg["entry"], timeout_price)
            leg_details.append({**leg, "exit_status": "TIMEOUT", "exit_price": timeout_price, "move_pct": move, "exit_ts": timeout_ts})
    gross = sum(leg["move_pct"] for leg in leg_details)
    statuses = {leg["exit_status"] for leg in leg_details}
    exit_status = next(iter(statuses)) if len(statuses) == 1 else "MIXED"
    last_exit = max(leg_details, key=lambda leg: leg["exit_ts"])
    return {
        "entry_price": entry_price,
        "exit_price": last_exit["exit_price"],
        "exit_ts": last_exit["exit_ts"],
        "filled_legs": len(entries),
        "gross_pnl_pct_unlevered": gross,
        "exit_status": exit_status,
        "leg_details": [_format_leg_detail(leg) for leg in leg_details],
    }


def _outcome(entry: float, exit_price: float, exit_ts: datetime, filled_legs: int, move: float, status: str) -> dict[str, Any]:
    return {
        "entry_price": entry,
        "exit_price": exit_price,
        "exit_ts": exit_ts,
        "filled_legs": filled_legs,
        "gross_pnl_pct_unlevered": move,
        "exit_status": status,
        "leg_details": [{"leg": 1, "entry_price": round(entry, 8), "exit_price": round(exit_price, 8), "exit_status": status, "move_pct": round(move, 8)}],
    }


def _summarize_trades(trades: list[dict[str, Any]], *, denominator: int) -> dict[str, Any]:
    executed = [trade for trade in trades if trade["entry_status"] == "FILLED"]
    positive = [trade for trade in executed if trade["net_pnl_pct"] > 0]
    negative = [trade for trade in executed if trade["net_pnl_pct"] < 0]
    gross_total = sum(trade["gross_pnl_pct"] for trade in trades)
    net_total = sum(trade["net_pnl_pct"] for trade in trades)
    gross_profit = sum(trade["net_pnl_pct"] for trade in positive)
    gross_loss = abs(sum(trade["net_pnl_pct"] for trade in negative))
    profit_factor = 999.0 if gross_loss == 0 and gross_profit > 0 else 0.0 if gross_loss == 0 else gross_profit / gross_loss
    return {
        "total_decisions": denominator,
        "executed_trades": len(executed),
        "tp_hits": sum(1 for trade in trades if trade["exit_status"] == "TP"),
        "sl_hits": sum(1 for trade in trades if trade["exit_status"] in {"SL", "INITIAL_SL", "PROTECTED_SL"}),
        "initial_sl_hits": sum(1 for trade in trades if trade["exit_status"] in {"SL", "INITIAL_SL"}),
        "protected_sl_hits": sum(1 for trade in trades if trade["exit_status"] == "PROTECTED_SL"),
        "no_hit": sum(1 for trade in trades if trade["exit_status"] == "TIMEOUT"),
        "hard_exits": sum(1 for trade in trades if trade["exit_status"] == "HARD_EXIT"),
        "mixed_exit": sum(1 for trade in trades if trade["exit_status"] == "MIXED"),
        "unfilled": sum(1 for trade in trades if trade["entry_status"] == "UNFILLED"),
        "skipped_decisions": sum(1 for trade in trades if trade["entry_status"] == "SKIPPED"),
        "skipped_position_open": sum(1 for trade in trades if trade.get("skip_reason") == "position_open"),
        "profitable_trades": len(positive),
        "losing_trades": len(negative),
        "flat_trades": sum(1 for trade in executed if trade["net_pnl_pct"] == 0),
        "gross_pnl_pct": round(gross_total, 8),
        "net_pnl_pct": round(net_total, 8),
        "gross_expectancy_pct": round(gross_total / denominator, 8) if denominator else 0.0,
        "net_expectancy_pct": round(net_total / denominator, 8) if denominator else 0.0,
        "win_rate_pct": round(len(positive) / len(executed) * 100, 8) if executed else 0.0,
        "profit_factor": round(profit_factor, 8),
    }


def _normalize_candidates(payload: dict[str, Any]) -> list[dict[str, Any]]:
    normalized = []
    for row in payload.get("candidates", []):
        setup = row.get("setup", {})
        candidate = {
            "candidate_id": row["candidate_id"],
            "entry_model": setup.get("entry_model", setup.get("entry_type", "market")),
            "tp_pct": float(setup.get("final_tp_pct", setup["tp_pct"])),
            "sl_pct": float(setup.get("initial_sl_pct", setup["sl_pct"])),
            "final_tp_pct": float(setup.get("final_tp_pct", setup["tp_pct"])),
            "initial_sl_pct": float(setup.get("initial_sl_pct", setup["sl_pct"])),
            "protection_enabled": bool(setup.get("protection_enabled", False)),
            "timeout_policy": setup.get("timeout_policy", "close_at_cutoff"),
            "max_hold_hours": float(setup.get("max_hold_hours", DEFAULT_FORWARD_HOURS)),
            "leverage": float(setup.get("leverage", DEFAULT_LEVERAGE)),
        }
        if setup.get("policy_mode") == "side_specific":
            candidate["policy_mode"] = "side_specific"
            candidate["side_policies"] = _normalize_side_policies(setup, fallback=candidate)
        if candidate["protection_enabled"]:
            if setup.get("protect_trigger_pct") is None or setup.get("trail_sl_pct") is None:
                raise ValueError("Protected Stage 4 candidates require protect_trigger_pct and trail_sl_pct.")
            candidate["protect_trigger_pct"] = float(setup["protect_trigger_pct"])
            candidate["trail_sl_pct"] = float(setup["trail_sl_pct"])
        if setup.get("pyramid_step_pct") is not None:
            candidate["pyramid"] = {
                "step_pct": float(setup["pyramid_step_pct"]),
                "max_legs": int(setup.get("max_legs", 3)),
                "sl_breakeven": bool(setup.get("sl_breakeven", False)),
            }
        normalized.append(candidate)
    return normalized


def _normalize_side_policies(setup: dict[str, Any], *, fallback: dict[str, Any]) -> dict[str, dict[str, Any]]:
    source = setup.get("side_policies")
    if not isinstance(source, dict):
        raise ValueError("Side-specific Stage 4 candidates require side_policies.")
    policies: dict[str, dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        raw = source.get(side)
        if not isinstance(raw, dict):
            raise ValueError(f"Side-specific Stage 4 candidate is missing {side} policy.")
        final_tp = raw.get("final_tp_pct", raw.get("lock_profit_pct", raw.get("tp_pct")))
        initial_sl = raw.get("initial_sl_pct", raw.get("sl_pct"))
        if final_tp is None or initial_sl is None:
            raise ValueError(f"Side-specific Stage 4 {side} policy requires final_tp_pct and initial_sl_pct.")
        protection_enabled = bool(raw.get("protection_enabled", fallback.get("protection_enabled", False)))
        policy = {
            "protection_enabled": protection_enabled,
            "tp_pct": float(final_tp),
            "sl_pct": float(initial_sl),
            "final_tp_pct": float(final_tp),
            "lock_profit_pct": float(final_tp),
            "initial_sl_pct": float(initial_sl),
            "timeout_policy": fallback["timeout_policy"],
            "max_hold_hours": float(raw.get("hard_exit_hours", fallback["max_hold_hours"])),
            "leverage": fallback["leverage"],
        }
        if protection_enabled:
            if raw.get("protect_trigger_pct") is None or raw.get("trail_sl_pct") is None:
                raise ValueError(f"Protected Stage 4 {side} policy requires protect_trigger_pct and trail_sl_pct.")
            policy["protect_trigger_pct"] = float(raw["protect_trigger_pct"])
            policy["trail_sl_pct"] = float(raw["trail_sl_pct"])
        policies[side] = policy
    return policies


def _candidate_policy_for_direction(candidate: dict[str, Any], direction: str) -> dict[str, Any]:
    if candidate.get("policy_mode") != "side_specific":
        return candidate
    side_policy = candidate["side_policies"][str(direction).upper()]
    policy = {
        **candidate,
        **side_policy,
        "candidate_id": candidate["candidate_id"],
        "entry_model": candidate["entry_model"],
        "policy_mode": "side_specific",
        "side_policies": candidate["side_policies"],
    }
    if candidate.get("pyramid"):
        policy["pyramid"] = candidate["pyramid"]
    return policy


def _choose_best_candidate(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Select the best candidate using walk-forward OOS performance.

    Selection priority:
    1. Protected-SL candidates with positive OOS return
    2. Candidates with OOS expectancy >= 30% of training expectancy (option 2)
    3. If none pass the ratio gate, fall back to combined metric (option 3)
       and flag the winner with oos_warning.
    """
    OOS_RATIO_THRESHOLD = 0.30

    def _oos_metrics(item: dict[str, Any]) -> tuple[float, float]:
        slices = item.get("slices") or {}
        wf = slices.get("walk_forward_test") or {}
        return (
            float(wf.get("net_expectancy_pct", 0)),
            float(wf.get("profit_factor", 0)),
        )

    def _training_metrics(item: dict[str, Any]) -> tuple[float, float]:
        slices = item.get("slices") or {}
        tr = slices.get("training") or {}
        return (
            float(tr.get("net_expectancy_pct", 0)),
            float(tr.get("profit_factor", 0)),
        )

    protected_eligible = [item for item in results if _candidate_has_protected_sl(item) and _walk_forward_net_pnl_pct(item) > 0]
    if protected_eligible:
        best = max(protected_eligible, key=lambda item: (
            _walk_forward_net_pnl_pct(item),
            _oos_metrics(item)[1],
            _overall_net_pnl_usdt(item),
            -item["unfilled"],
        ))
        return {
            **best,
            "selection_mode": "protected_walk_forward_net_pnl_pct",
            "oos_selection_mode": "protected_walk_forward_net_pnl_pct",
            "oos_warning": False,
        }

    # Option 2: candidates where OOS expectancy >= 30% of training expectancy
    viable = []
    for item in results:
        wf_exp, _ = _oos_metrics(item)
        tr_exp, _ = _training_metrics(item)
        if tr_exp > 0 and wf_exp >= tr_exp * OOS_RATIO_THRESHOLD:
            viable.append(item)
        elif tr_exp <= 0 and wf_exp > 0:
            # Training was negative but OOS is positive — accept
            viable.append(item)

    if viable:
        # Among viable candidates, pick best OOS expectancy, then OOS PF
        best = max(viable, key=lambda item: (
            _oos_metrics(item)[0],
            _oos_metrics(item)[1],
            -item["unfilled"],
        ))
        return {**best, "selection_mode": "oos_ratio_gate", "oos_selection_mode": "oos_ratio_gate", "oos_warning": False}

    # Option 3: fallback to combined metric, flag with warning
    best = max(results, key=lambda item: (item["net_expectancy_pct"], item["profit_factor"], -item["unfilled"]))
    return {**best, "selection_mode": "fallback_combined", "oos_selection_mode": "fallback_combined", "oos_warning": True}


def _candidate_has_protected_sl(candidate: dict[str, Any]) -> bool:
    setup = candidate.get("setup") if isinstance(candidate.get("setup"), dict) else candidate
    if bool(setup.get("protection_enabled")):
        return True
    side_policies = setup.get("side_policies") if isinstance(setup.get("side_policies"), dict) else {}
    return any(isinstance(policy, dict) and bool(policy.get("protection_enabled")) for policy in side_policies.values())


def _walk_forward_net_pnl_pct(candidate: dict[str, Any]) -> float:
    wf = (candidate.get("slices") or {}).get("walk_forward_test") or {}
    return float(wf.get("net_pnl_pct", 0) or 0)


def _overall_net_pnl_usdt(candidate: dict[str, Any]) -> float:
    account = candidate.get("account") or {}
    return float(account.get("net_pnl_usdt", 0) or 0)


def _render_summary(payload: dict[str, Any]) -> str:
    best = payload["best_candidate"]
    account = best.get("account") or {}
    return "\n".join(
        [
            "# Stage 4 Realized Expectancy",
            "",
            "Mode: `sequential_account_backtest`",
            f"Best candidate: `{best['candidate_id']}`",
            f"Net expectancy: `{best['net_expectancy_pct']:.4f}%` per decision",
            f"Gross expectancy: `{best['gross_expectancy_pct']:.4f}%` per decision",
            f"Ending equity: `${account.get('ending_equity_usdt', 0):.4f}`",
            f"Net PnL: `${account.get('net_pnl_usdt', 0):.4f}`",
            f"Total OKX taker fees: `${account.get('total_fees_usdt', 0):.4f}`",
            f"Executed trades: `{best['executed_trades']}` / `{best['total_decisions']}` decisions",
            f"TP / SL / HARD_EXIT / UNFILLED / SKIPPED: `{best['tp_hits']}` / `{best['sl_hits']}` / `{best.get('hard_exits', 0)}` / `{best['unfilled']}` / `{best['skipped_decisions']}`",
            f"Profit factor: `{best['profit_factor']:.4f}`",
            "",
        ]
    )


def _slice_windows(session: dict[str, Any]) -> list[tuple[str, datetime, datetime]]:
    windows = []
    if session.get("train_start") and session.get("train_end"):
        windows.append(("training", _date_start(session["train_start"]), _date_end(session["train_end"])))
    if session.get("walk_forward_start") and session.get("walk_forward_end"):
        windows.append(("walk_forward_test", _date_start(session["walk_forward_start"]), _date_end(session["walk_forward_end"])))
    return windows


def _date_start(value: str) -> datetime:
    return datetime.fromisoformat(str(value)).replace(tzinfo=UTC)


def _date_end(value: str) -> datetime:
    return datetime.fromisoformat(str(value)).replace(tzinfo=UTC) + timedelta(days=1)


def _packet_from_signal(signal: dict[str, Any]) -> dict[str, Any]:
    payload = signal.get("payload") if isinstance(signal.get("payload"), dict) else {}
    return {**payload, "signal_id": signal["signal_id"], "timestamp": payload.get("timestamp") or signal["timestamp"]}


def _index_signals(signal_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    indexed = {}
    for signal in signal_rows:
        signal_id = str(signal["signal_id"])
        indexed[signal_id] = signal
        indexed.setdefault(signal_id.split(":")[-1], signal)
    return indexed


def _find_signal(signals_by_id: dict[str, dict[str, Any]], signal_id: str) -> dict[str, Any] | None:
    return signals_by_id.get(signal_id) or signals_by_id.get(signal_id.split(":")[-1])


def _slice_name(timestamp: datetime, windows: list[tuple[str, datetime, datetime]]) -> str | None:
    for name, start, end in windows:
        if start <= timestamp < end:
            return name
    return None


def _session_artifact_root(*, workspace_root: Path, session: dict[str, Any]) -> Path:
    artifact_root = Path(session["artifact_root"])
    return artifact_root if artifact_root.is_absolute() else workspace_root / artifact_root


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ValueError(f"Required Stage 4 artifact not found: {path}")
    return json.loads(path.read_text())


def _read_json_if_exists(path: Path) -> dict[str, Any] | None:
    return json.loads(path.read_text()) if path.is_file() else None


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
        "open": float(candle.open if not isinstance(candle.open, Decimal) else str(candle.open)),
        "high": float(candle.high if not isinstance(candle.high, Decimal) else str(candle.high)),
        "low": float(candle.low if not isinstance(candle.low, Decimal) else str(candle.low)),
        "close": float(candle.close if not isinstance(candle.close, Decimal) else str(candle.close)),
    }


def _coerce_datetime(value: str | datetime | None) -> datetime:
    if value is None:
        raise ValueError("missing timestamp")
    if isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _target_price(entry: float, *, tp_pct: float, direction: str) -> float:
    return entry * (1 + tp_pct / 100) if direction == "LONG" else entry * (1 - tp_pct / 100)


def _protected_stop_price(entry: float, *, pct: float, direction: str) -> float:
    return entry * (1 + pct / 100) if direction == "LONG" else entry * (1 - pct / 100)


def _next_entry(entry: float, *, step_pct: float, direction: str) -> float:
    return entry * (1 + step_pct / 100) if direction == "LONG" else entry * (1 - step_pct / 100)


def _entry_hit(candle: dict[str, Any], entry: float, *, direction: str) -> bool:
    return candle["high"] >= entry if direction == "LONG" else candle["low"] <= entry


def _tp_sl_hit(candle: dict[str, Any], *, tp: float, sl: float, direction: str) -> tuple[bool, bool]:
    if direction == "LONG":
        return candle["high"] >= tp, candle["low"] <= sl
    return candle["low"] <= tp, candle["high"] >= sl


def _resolve_dual_hit(candle: dict[str, Any], *, direction: str) -> str:
    body = candle["close"] - candle["open"]
    return "TP" if (body >= 0 if direction == "LONG" else body <= 0) else "SL"


def _direction_move_pct(direction: str, entry: float, exit_price: float) -> float:
    return (exit_price - entry) / entry * 100 if direction == "LONG" else (entry - exit_price) / entry * 100


def _trade_cost_pct(*, leverage: float, legs: int, fees_bps_per_side: float, slippage_bps_per_side: float) -> float:
    return (fees_bps_per_side + slippage_bps_per_side) * 2 / 100 * leverage * legs


def _format_leg_detail(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg": leg["leg"],
        "entry_price": round(leg["entry"], 8),
        "exit_price": round(leg["exit_price"], 8),
        "exit_status": leg["exit_status"],
        "move_pct": round(leg["move_pct"], 8),
    }


def _format_account_leg(leg: dict[str, Any]) -> dict[str, Any]:
    return {
        "leg": leg["leg"],
        "entry_ts": leg["entry_ts"],
        "exit_ts": leg["exit_ts"],
        "entry_price": round(leg["entry_price"], 8),
        "exit_price": round(leg["exit_price"], 8),
        "tp_price": round(leg["tp_price"], 8),
        "exit_status": leg["exit_status"],
        "margin_usdt": _round_money(leg["margin_usdt"]),
        "entry_notional_usdt": _round_money(leg["entry_notional_usdt"]),
        "exit_notional_usdt": _round_money(leg["exit_notional_usdt"]),
        "quantity": round(leg["quantity"], 10),
        "entry_fee_usdt": _round_money(leg["entry_fee_usdt"]),
        "exit_fee_usdt": _round_money(leg["exit_fee_usdt"]),
        "gross_pnl_usdt": _round_money(leg["gross_pnl_usdt"]),
        "net_pnl_usdt": _round_money(leg["net_pnl_usdt"]),
        "move_pct": round(leg["move_pct"], 8),
    }


def _round_money(value: float) -> float:
    return round(float(value), 4)
