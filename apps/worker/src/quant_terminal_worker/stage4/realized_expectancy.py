from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage2.capture_curve import get_reference_price
from quant_terminal_worker.stage3.grid_search import DEFAULT_FORWARD_HOURS, DEFAULT_LEVERAGE


DEFAULT_FEES_BPS_PER_SIDE = 5.0
DEFAULT_SLIPPAGE_BPS_PER_SIDE = 3.0


def run_stage4_realized_expectancy(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
    slippage_bps_per_side: float = DEFAULT_SLIPPAGE_BPS_PER_SIDE,
) -> dict[str, Any]:
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
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    ledger = {
        "schema_version": "0.1",
        "stage": "stage4_trade_ledger",
        "created_at": created_at,
        "session_id": session["session_id"],
        "candidates": ledger_candidates,
    }
    payload = {
        "schema_version": "0.1",
        "stage": "stage4_realized_expectancy",
        "artifact_role": "stage4_realized_expectancy",
        "created_at": created_at,
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "stage1_scores_path": str(stage1_scores_path),
        "candidates_path": str(candidates_path),
        "cost_assumptions": {
            "fees_bps_per_side": fees_bps_per_side,
            "slippage_bps_per_side": slippage_bps_per_side,
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
    realized_path = promotion_root / "stage4_realized_expectancy.json"
    ledger_path = promotion_root / "stage4_trade_ledger.json"
    optimal_path = promotion_root / "stage4_optimal.json"
    summary_path = promotion_root / "stage4_summary.md"
    realized_path.write_text(json.dumps(payload, indent=2) + "\n")
    ledger_path.write_text(json.dumps(ledger, indent=2) + "\n")
    optimal_path.write_text(json.dumps({"criterion": "max_net_expectancy", "best": best}, indent=2) + "\n")
    summary_path.write_text(_render_summary(payload))
    return {
        **payload,
        "realized_expectancy_path": str(realized_path),
        "trade_ledger_path": str(ledger_path),
        "optimal_path": str(optimal_path),
        "summary_path": str(summary_path),
    }


def _score_candidate(
    *,
    candidate: dict[str, Any],
    records: list[dict[str, Any]],
    signals_by_id: dict[str, dict[str, Any]],
    candles: list[dict[str, Any]],
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    slice_windows: list[tuple[str, datetime, datetime]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    trades = []
    for record in records:
        signal_id = str(record["signal_id"])
        signal = _find_signal(signals_by_id, signal_id)
        if signal is None:
            raise ValueError(f"Stage 4 signal row not found for canonical decision: {signal_id}")
        packet = _packet_from_signal(signal)
        signal_ts = _coerce_datetime(packet.get("timestamp") or signal["timestamp"])
        reference_price = get_reference_price(packet)
        trades.append(
            _simulate_candidate_trade(
                record=record,
                candidate=candidate,
                signal_ts=signal_ts,
                reference_price=reference_price,
                candles=candles,
                fees_bps_per_side=fees_bps_per_side,
                slippage_bps_per_side=slippage_bps_per_side,
                slice_name=_slice_name(signal_ts, slice_windows),
            )
        )
    summary = _summarize_trades(trades, denominator=len(records))
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
            "leverage": candidate["leverage"],
            "setup": candidate,
            **summary,
            "mismatch_cohort": _summarize_trades(mismatch_trades, denominator=len(mismatch_trades)),
            "by_side": by_side,
            "slices": slices,
        },
        trades,
    )


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
        "sl_hits": sum(1 for trade in trades if trade["exit_status"] == "SL"),
        "no_hit": sum(1 for trade in trades if trade["exit_status"] == "TIMEOUT"),
        "mixed_exit": sum(1 for trade in trades if trade["exit_status"] == "MIXED"),
        "unfilled": sum(1 for trade in trades if trade["entry_status"] == "UNFILLED"),
        "skipped_decisions": sum(1 for trade in trades if trade["entry_status"] == "SKIPPED"),
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
            "tp_pct": float(setup["tp_pct"]),
            "sl_pct": float(setup["sl_pct"]),
            "timeout_policy": setup.get("timeout_policy", "close_at_cutoff"),
            "max_hold_hours": float(setup.get("max_hold_hours", DEFAULT_FORWARD_HOURS)),
            "leverage": float(setup.get("leverage", DEFAULT_LEVERAGE)),
        }
        if setup.get("pyramid_step_pct") is not None:
            candidate["pyramid"] = {
                "step_pct": float(setup["pyramid_step_pct"]),
                "max_legs": int(setup.get("max_legs", 3)),
                "sl_breakeven": bool(setup.get("sl_breakeven", False)),
            }
        normalized.append(candidate)
    return normalized


def _choose_best_candidate(results: list[dict[str, Any]]) -> dict[str, Any]:
    return max(results, key=lambda item: (item["net_expectancy_pct"], item["profit_factor"], -item["unfilled"]))


def _render_summary(payload: dict[str, Any]) -> str:
    best = payload["best_candidate"]
    return "\n".join(
        [
            "# Stage 4 Realized Expectancy",
            "",
            f"Best candidate: `{best['candidate_id']}`",
            f"Net expectancy: `{best['net_expectancy_pct']:.4f}%` per decision",
            f"Gross expectancy: `{best['gross_expectancy_pct']:.4f}%` per decision",
            f"Executed trades: `{best['executed_trades']}` / `{best['total_decisions']}` decisions",
            f"TP / SL / TIMEOUT / UNFILLED / SKIPPED: `{best['tp_hits']}` / `{best['sl_hits']}` / `{best['no_hit']}` / `{best['unfilled']}` / `{best['skipped_decisions']}`",
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
