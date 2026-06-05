#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, NamedTuple


def parse_ts(ts_str: str) -> datetime:
    ts_str = ts_str.strip()
    if ts_str.isdigit():
        return datetime.fromtimestamp(int(ts_str) / 1000, tz=timezone.utc)
    if ts_str.endswith("Z") and len(ts_str) == 16:
        return datetime.strptime(ts_str, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    return datetime.fromisoformat(ts_str.replace("Z", "+00:00"))


class SliceWindow(NamedTuple):
    name: str
    start: datetime
    end: datetime


def isoformat_z(ts: datetime | None) -> str | None:
    if ts is None:
        return None
    return ts.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def resolve_path(base: Path, raw_path: str | None) -> Path | None:
    if not raw_path:
        return None
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base / path).resolve()


def load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def load_candles(csv_path: Path, start: datetime, end: datetime) -> list[dict[str, Any]]:
    candles: list[dict[str, Any]] = []
    with csv_path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            ts = parse_ts(row["ts"])
            if ts < start:
                continue
            if ts > end:
                break
            candles.append(
                {
                    "ts": ts,
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                }
            )
    return candles


def direction_move_pct(direction: str, entry_price: float, exit_price: float) -> float:
    if direction == "LONG":
        return (exit_price - entry_price) / entry_price * 100.0
    return (entry_price - exit_price) / entry_price * 100.0


def trade_cost_pct(leverage: float, legs: int, fees_bps_per_side: float, slippage_bps_per_side: float) -> float:
    round_trip_cost_pct = (fees_bps_per_side + slippage_bps_per_side) * 2 / 100.0
    return round_trip_cost_pct * leverage * legs


def choose_slice_name(signal_ts: datetime, slice_windows: list[SliceWindow]) -> str | None:
    for slice_window in slice_windows:
        if slice_window.start <= signal_ts < slice_window.end:
            return slice_window.name
    return None


def single_leg_outcome(
    *,
    direction: str,
    signal_ts: datetime,
    entry_price: float,
    tp_pct: float,
    sl_pct: float,
    max_hold_hours: float,
    candles: list[dict[str, Any]],
    timeout_exit_policy: str,
) -> dict[str, Any]:
    cutoff = signal_ts + timedelta(hours=max_hold_hours)
    last_candle: dict[str, Any] | None = None
    tp_price = entry_price * (1 + tp_pct / 100.0) if direction == "LONG" else entry_price * (1 - tp_pct / 100.0)
    sl_price = entry_price * (1 - sl_pct / 100.0) if direction == "LONG" else entry_price * (1 + sl_pct / 100.0)

    for candle in candles:
        if candle["ts"] <= signal_ts:
            continue
        if candle["ts"] > cutoff:
            break
        last_candle = candle

        if direction == "LONG":
            tp_hit = candle["high"] >= tp_price
            sl_hit = candle["low"] <= sl_price
        else:
            tp_hit = candle["low"] <= tp_price
            sl_hit = candle["high"] >= sl_price

        if tp_hit and sl_hit:
            body = candle["close"] - candle["open"]
            exit_status = "TP" if (direction == "LONG" and body >= 0) or (direction == "SHORT" and body <= 0) else "SL"
        elif tp_hit:
            exit_status = "TP"
        elif sl_hit:
            exit_status = "SL"
        else:
            continue

        if exit_status == "TP":
            exit_price = tp_price
            gross_pnl_pct = tp_pct
        else:
            exit_price = sl_price
            gross_pnl_pct = -sl_pct

        return {
            "entry_status": "FILLED",
            "exit_status": exit_status,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "exit_ts": candle["ts"],
            "filled_legs": 1,
            "gross_pnl_pct_unlevered": gross_pnl_pct,
            "leg_details": [
                {
                    "leg": 1,
                    "entry_price": round(entry_price, 8),
                    "exit_price": round(exit_price, 8),
                    "exit_status": exit_status,
                    "move_pct": round(gross_pnl_pct, 8),
                }
            ],
        }

    if timeout_exit_policy == "zero" or last_candle is None:
        return {
            "entry_status": "FILLED",
            "exit_status": "TIMEOUT",
            "entry_price": entry_price,
            "exit_price": entry_price,
            "exit_ts": isoformat_z(cutoff) if isinstance(cutoff, datetime) else None,
            "filled_legs": 1,
            "gross_pnl_pct_unlevered": 0.0,
            "leg_details": [
                {
                    "leg": 1,
                    "entry_price": round(entry_price, 8),
                    "exit_price": round(entry_price, 8),
                    "exit_status": "TIMEOUT",
                    "move_pct": 0.0,
                }
            ],
        }

    realized_move_pct = direction_move_pct(direction, entry_price, last_candle["close"])
    return {
        "entry_status": "FILLED",
        "exit_status": "TIMEOUT",
        "entry_price": entry_price,
        "exit_price": last_candle["close"],
        "exit_ts": last_candle["ts"],
        "filled_legs": 1,
        "gross_pnl_pct_unlevered": realized_move_pct,
        "leg_details": [
            {
                "leg": 1,
                "entry_price": round(entry_price, 8),
                "exit_price": round(last_candle["close"], 8),
                "exit_status": "TIMEOUT",
                "move_pct": round(realized_move_pct, 8),
            }
        ],
    }


def pyramid_outcome(
    *,
    direction: str,
    signal_ts: datetime,
    entry_price: float,
    tp_pct: float,
    sl_pct: float,
    max_hold_hours: float,
    candles: list[dict[str, Any]],
    timeout_exit_policy: str,
    step_pct: float,
    max_legs: int,
    sl_breakeven: bool,
) -> dict[str, Any]:
    cutoff = signal_ts + timedelta(hours=max_hold_hours)
    if direction == "LONG":
        sl_price = entry_price * (1 - sl_pct / 100.0)
    else:
        sl_price = entry_price * (1 + sl_pct / 100.0)

    active = [
        {
            "leg": 1,
            "entry_price": entry_price,
            "tp_price": entry_price * (1 + tp_pct / 100.0)
            if direction == "LONG"
            else entry_price * (1 - tp_pct / 100.0),
        }
    ]
    entries = [entry_price]
    legs_filled = 1
    last_candle: dict[str, Any] | None = None
    leg_details: list[dict[str, Any]] = []

    for candle in candles:
        if candle["ts"] <= signal_ts:
            continue
        if candle["ts"] > cutoff:
            break
        last_candle = candle

        if legs_filled < max_legs:
            if direction == "LONG":
                next_entry = entries[-1] * (1 + step_pct / 100.0)
                new_leg_hit = candle["high"] >= next_entry
            else:
                next_entry = entries[-1] * (1 - step_pct / 100.0)
                new_leg_hit = candle["low"] <= next_entry
            if new_leg_hit:
                legs_filled += 1
                entries.append(next_entry)
                active.append(
                    {
                        "leg": legs_filled,
                        "entry_price": next_entry,
                        "tp_price": next_entry * (1 + tp_pct / 100.0)
                        if direction == "LONG"
                        else next_entry * (1 - tp_pct / 100.0),
                    }
                )
                if sl_breakeven:
                    sl_price = sum(entries) / len(entries)

        closed_legs: list[dict[str, Any]] = []
        for leg in active:
            if direction == "LONG":
                tp_hit = candle["high"] >= leg["tp_price"]
                sl_hit = candle["low"] <= sl_price
            else:
                tp_hit = candle["low"] <= leg["tp_price"]
                sl_hit = candle["high"] >= sl_price

            if tp_hit and sl_hit:
                body = candle["close"] - candle["open"]
                exit_status = "TP" if (direction == "LONG" and body >= 0) or (direction == "SHORT" and body <= 0) else "SL"
            elif tp_hit:
                exit_status = "TP"
            elif sl_hit:
                exit_status = "SL"
            else:
                continue

            if exit_status == "TP":
                exit_price = leg["tp_price"]
                move_pct = tp_pct
            else:
                exit_price = sl_price
                move_pct = abs(leg["entry_price"] - sl_price) / leg["entry_price"] * 100.0
                move_pct = -move_pct

            closed_legs.append(
                {
                    "leg": leg["leg"],
                    "entry_price": leg["entry_price"],
                    "exit_price": exit_price,
                    "exit_status": exit_status,
                    "move_pct": move_pct,
                    "exit_ts": candle["ts"],
                }
            )

        if closed_legs:
            closed_ids = {item["leg"] for item in closed_legs}
            active = [leg for leg in active if leg["leg"] not in closed_ids]
            leg_details.extend(closed_legs)
            if not active:
                break

    if active:
        timeout_price = entry_price if timeout_exit_policy == "zero" or last_candle is None else last_candle["close"]
        timeout_ts = cutoff if last_candle is None else last_candle["ts"]
        for leg in active:
            move_pct = 0.0 if timeout_exit_policy == "zero" or last_candle is None else direction_move_pct(direction, leg["entry_price"], timeout_price)
            leg_details.append(
                {
                    "leg": leg["leg"],
                    "entry_price": leg["entry_price"],
                    "exit_price": timeout_price,
                    "exit_status": "TIMEOUT",
                    "move_pct": move_pct,
                    "exit_ts": timeout_ts,
                }
            )

    gross_unlevered = sum(item["move_pct"] for item in leg_details)
    statuses = {item["exit_status"] for item in leg_details}
    if statuses == {"TP"}:
        exit_status = "TP"
    elif statuses == {"SL"}:
        exit_status = "SL"
    elif statuses == {"TIMEOUT"}:
        exit_status = "TIMEOUT"
    else:
        exit_status = "MIXED"

    last_exit = max(leg_details, key=lambda item: item["exit_ts"]) if leg_details else None
    return {
        "entry_status": "FILLED",
        "exit_status": exit_status,
        "entry_price": entry_price,
        "exit_price": last_exit["exit_price"] if last_exit else entry_price,
        "exit_ts": last_exit["exit_ts"] if last_exit else signal_ts,
        "filled_legs": legs_filled,
        "gross_pnl_pct_unlevered": gross_unlevered,
        "leg_details": [
            {
                "leg": item["leg"],
                "entry_price": round(item["entry_price"], 8),
                "exit_price": round(item["exit_price"], 8),
                "exit_status": item["exit_status"],
                "move_pct": round(item["move_pct"], 8),
            }
            for item in leg_details
        ],
    }


def simulate_candidate_trade(
    *,
    record: dict[str, Any],
    signal_ts: datetime,
    reference_price: float,
    candidate: dict[str, Any],
    candles: list[dict[str, Any]],
    leverage: float,
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    slice_name: str | None,
) -> dict[str, Any]:
    direction = (record.get("agent_direction") or record.get("direction") or "").upper()
    agreement = record.get("agreement", "")
    signal_id = record["signal_id"]
    trade: dict[str, Any] = {
        "candidate_id": candidate["candidate_id"],
        "signal_id": signal_id,
        "signal_ts": isoformat_z(signal_ts),
        "slice_name": slice_name,
        "agreement": agreement,
        "decision_direction": direction,
        "reference_price": round(reference_price, 8),
        "entry_type": candidate["entry_type"],
    }

    if direction not in {"LONG", "SHORT"}:
        trade.update(
            {
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
        )
        return trade

    tp_pct = float(candidate["tp_pct"])
    sl_pct = float(candidate["sl_pct"])
    max_hold_hours = float(candidate.get("max_hold_hours", 36))
    timeout_exit_policy = candidate.get("timeout_exit_policy", "close_at_cutoff")

    if candidate["entry_type"] == "limit":
        limit_offset_pct = float(candidate.get("limit_offset_pct", 0.0))
        if direction == "LONG":
            entry_price = reference_price * (1 - limit_offset_pct / 100.0)
        else:
            entry_price = reference_price * (1 + limit_offset_pct / 100.0)

        cutoff = signal_ts + timedelta(hours=max_hold_hours)
        fill_candle: dict[str, Any] | None = None
        for candle in candles:
            if candle["ts"] <= signal_ts:
                continue
            if candle["ts"] > cutoff:
                break
            if direction == "LONG" and candle["low"] <= entry_price:
                fill_candle = candle
                break
            if direction == "SHORT" and candle["high"] >= entry_price:
                fill_candle = candle
                break
        if fill_candle is None:
            trade.update(
                {
                    "entry_status": "UNFILLED",
                    "exit_status": "UNFILLED",
                    "entry_price": round(entry_price, 8),
                    "exit_price": None,
                    "exit_ts": None,
                    "filled_legs": 0,
                    "gross_pnl_pct": 0.0,
                    "net_pnl_pct": 0.0,
                    "cost_pct": 0.0,
                    "leg_details": [],
                }
            )
            return trade
        base = single_leg_outcome(
            direction=direction,
            signal_ts=signal_ts,
            entry_price=entry_price,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            max_hold_hours=max_hold_hours,
            candles=candles[candles.index(fill_candle):],
            timeout_exit_policy=timeout_exit_policy,
        )
    elif candidate.get("pyramid"):
        pyramid = candidate["pyramid"]
        base = pyramid_outcome(
            direction=direction,
            signal_ts=signal_ts,
            entry_price=reference_price,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            max_hold_hours=max_hold_hours,
            candles=candles,
            timeout_exit_policy=timeout_exit_policy,
            step_pct=float(pyramid["step_pct"]),
            max_legs=int(pyramid["max_legs"]),
            sl_breakeven=bool(pyramid.get("sl_breakeven", False)),
        )
    else:
        base = single_leg_outcome(
            direction=direction,
            signal_ts=signal_ts,
            entry_price=reference_price,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            max_hold_hours=max_hold_hours,
            candles=candles,
            timeout_exit_policy=timeout_exit_policy,
        )

    gross_pnl_pct = base["gross_pnl_pct_unlevered"] * leverage
    cost_pct = trade_cost_pct(leverage, base["filled_legs"], fees_bps_per_side, slippage_bps_per_side)
    net_pnl_pct = gross_pnl_pct - cost_pct

    trade.update(
        {
            "entry_status": base["entry_status"],
            "exit_status": base["exit_status"],
            "entry_price": round(base["entry_price"], 8),
            "exit_price": None if base["exit_price"] is None else round(base["exit_price"], 8),
            "exit_ts": isoformat_z(base["exit_ts"]) if isinstance(base["exit_ts"], datetime) else base["exit_ts"],
            "filled_legs": base["filled_legs"],
            "gross_pnl_pct": round(gross_pnl_pct, 8),
            "net_pnl_pct": round(net_pnl_pct, 8),
            "cost_pct": round(cost_pct, 8),
            "leg_details": base["leg_details"],
        }
    )
    return trade


def summarize_group(trades: list[dict[str, Any]], *, denominator: int | None = None) -> dict[str, Any]:
    if denominator is None:
        denominator = len(trades)
    executed = [trade for trade in trades if trade["entry_status"] == "FILLED"]
    positive = [trade for trade in executed if trade["net_pnl_pct"] > 0]
    negative = [trade for trade in executed if trade["net_pnl_pct"] < 0]
    gross_total = sum(trade["gross_pnl_pct"] for trade in trades)
    net_total = sum(trade["net_pnl_pct"] for trade in trades)
    gross_profit = sum(trade["net_pnl_pct"] for trade in positive)
    gross_loss = abs(sum(trade["net_pnl_pct"] for trade in negative))
    profit_factor = 999.0 if gross_loss == 0 and gross_profit > 0 else (0.0 if gross_loss == 0 else gross_profit / gross_loss)
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
        "win_rate_pct": round(len(positive) / len(executed) * 100.0, 8) if executed else 0.0,
        "profit_factor": round(profit_factor, 8),
    }


def score_candidate(
    *,
    candidate: dict[str, Any],
    records: list[dict[str, Any]],
    ground_truth_dir: Path,
    candles: list[dict[str, Any]],
    leverage: float,
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    slice_windows: list[SliceWindow],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    trades: list[dict[str, Any]] = []
    for record in records:
        signal_id = record["signal_id"]
        ground_truth = load_json(ground_truth_dir / f"{signal_id}.json")
        signal_ts = parse_ts(signal_id)
        slice_name = choose_slice_name(signal_ts, slice_windows)
        trades.append(
            simulate_candidate_trade(
                record=record,
                signal_ts=signal_ts,
                reference_price=float(ground_truth["reference_price"]),
                candidate=candidate,
                candles=candles,
                leverage=leverage,
                fees_bps_per_side=fees_bps_per_side,
                slippage_bps_per_side=slippage_bps_per_side,
                slice_name=slice_name,
            )
        )

    summary = summarize_group(trades, denominator=len(records))
    by_side: dict[str, dict[str, Any]] = {}
    for side in ("LONG", "SHORT"):
        side_trades = [trade for trade in trades if trade["decision_direction"] == side]
        if side_trades:
            by_side[side] = summarize_group(side_trades, denominator=len(side_trades))

    slices: dict[str, dict[str, Any]] = {}
    for slice_window in slice_windows:
        slice_trades = [trade for trade in trades if trade["slice_name"] == slice_window.name]
        if slice_trades:
            slices[slice_window.name] = summarize_group(slice_trades, denominator=len(slice_trades))

    mismatch_trades = [trade for trade in trades if trade["agreement"] == "MISMATCH"]
    result = {
        "candidate_id": candidate["candidate_id"],
        "entry_type": candidate["entry_type"],
        "tp_pct": float(candidate["tp_pct"]),
        "sl_pct": float(candidate["sl_pct"]),
        "leverage": leverage,
        "cost_assumptions": {
            "fees_bps_per_side": fees_bps_per_side,
            "slippage_bps_per_side": slippage_bps_per_side,
        },
        **summary,
        "mismatch_cohort": summarize_group(mismatch_trades, denominator=len(mismatch_trades))
        if mismatch_trades
        else summarize_group([], denominator=0),
        "by_side": by_side,
        "slices": slices,
    }

    if "limit_offset_pct" in candidate:
        result["limit_offset_pct"] = float(candidate["limit_offset_pct"])
    if candidate.get("pyramid"):
        result["pyramid"] = candidate["pyramid"]
    if candidate.get("source_stage"):
        result["source_stage"] = candidate["source_stage"]
    if candidate.get("source_path"):
        result["source_path"] = candidate["source_path"]
    return result, trades


def choose_best_candidate(results: list[dict[str, Any]]) -> dict[str, Any]:
    def key_fn(item: dict[str, Any]) -> tuple[float, float, float]:
        return (
            float(item["net_expectancy_pct"]),
            float(item["profit_factor"]),
            -float(item["unfilled"]),
        )

    return max(results, key=key_fn)


def write_summary(path: Path, payload: dict[str, Any]) -> None:
    best = payload["best_candidate"]
    lines = [
        "# Stage 4 Realized Expectancy",
        "",
        f"- Best candidate: `{best['candidate_id']}`",
        f"- Net expectancy: `{best['net_expectancy_pct']:.4f}%` per decision",
        f"- Gross expectancy: `{best['gross_expectancy_pct']:.4f}%` per decision",
        f"- Executed trades: `{best['executed_trades']}` / `{best['total_decisions']}` decisions",
        f"- TP / SL / TIMEOUT / UNFILLED: `{best['tp_hits']}` / `{best['sl_hits']}` / `{best['no_hit']}` / `{best['unfilled']}`",
        f"- Profit factor: `{best['profit_factor']:.4f}`",
    ]
    if best.get("mismatch_cohort", {}).get("total_decisions"):
        lines.append(
            f"- Mismatch cohort net expectancy: `{best['mismatch_cohort']['net_expectancy_pct']:.4f}%` over `{best['mismatch_cohort']['total_decisions']}` decisions"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")


def run_stage4(
    *,
    stage1_scores_path: Path,
    candidates_path: Path,
    candles_path: Path,
    out_dir: Path,
    fees_bps_per_side: float,
    slippage_bps_per_side: float,
    ground_truth_dir: Path | None = None,
    slice_windows: list[SliceWindow] | None = None,
    summary_out: Path | None = None,
) -> dict[str, Any]:
    stage1_scores_path = stage1_scores_path.resolve()
    candidates_path = candidates_path.resolve()
    candles_path = candles_path.resolve()
    out_dir = out_dir.resolve()
    slice_windows = slice_windows or []

    stage1 = load_json(stage1_scores_path)
    records = stage1.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("stage1 score file must contain non-empty records")

    if ground_truth_dir is None:
        raw_ground_truth_dir = stage1.get("inputs", {}).get("ground_truth_dir")
        ground_truth_dir = resolve_path(stage1_scores_path.parent, raw_ground_truth_dir)
    if ground_truth_dir is None or not ground_truth_dir.exists():
        raise ValueError("ground truth directory is required for Stage 4 scoring")

    candidate_manifest = load_json(candidates_path)
    defaults = candidate_manifest.get("defaults", {})
    candidates = candidate_manifest.get("candidates", [])
    if not isinstance(candidates, list) or not candidates:
        raise ValueError("candidate manifest must contain at least one candidate")

    signal_times = [parse_ts(record["signal_id"]) for record in records]
    max_hold_hours = max(float(candidate.get("max_hold_hours", defaults.get("max_hold_hours", 36))) for candidate in candidates)
    candles = load_candles(
        candles_path,
        min(signal_times),
        max(signal_times) + timedelta(hours=max_hold_hours),
    )

    scored_results: list[dict[str, Any]] = []
    ledger_candidates: list[dict[str, Any]] = []
    for candidate in candidates:
        merged_candidate = dict(defaults)
        merged_candidate.update(candidate)
        if "candidate_id" not in merged_candidate or "entry_type" not in merged_candidate:
            raise ValueError("each candidate requires candidate_id and entry_type")
        leverage = float(merged_candidate.get("leverage", 1))
        result, trades = score_candidate(
            candidate=merged_candidate,
            records=records,
            ground_truth_dir=ground_truth_dir,
            candles=candles,
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
            slippage_bps_per_side=slippage_bps_per_side,
            slice_windows=slice_windows,
        )
        scored_results.append(result)
        ledger_candidates.append(
            {
                "candidate_id": merged_candidate["candidate_id"],
                "entry_type": merged_candidate["entry_type"],
                "tp_pct": float(merged_candidate["tp_pct"]),
                "sl_pct": float(merged_candidate["sl_pct"]),
                "trades": trades,
            }
        )

    best = choose_best_candidate(scored_results)
    payload = {
        "schema_version": "0.1",
        "stage": "stage4_realized_expectancy",
        "created_at": isoformat_z(datetime.now(timezone.utc)),
        "asset": stage1.get("asset"),
        "strategy_id": stage1.get("strategy_id"),
        "strategy_version": stage1.get("strategy_version"),
        "signal_engine_id": stage1.get("signal_engine_id") or stage1.get("signal_family"),
        "signal_set_id": stage1.get("signal_set_id"),
        "stage1_scores_path": str(stage1_scores_path),
        "ground_truth_dir": str(ground_truth_dir),
        "candles_path": str(candles_path),
        "candidates_path": str(candidates_path),
        "cost_assumptions": {
            "fees_bps_per_side": fees_bps_per_side,
            "slippage_bps_per_side": slippage_bps_per_side,
        },
        "slice_windows": [
            {
                "name": slice_window.name,
                "start": isoformat_z(slice_window.start),
                "end": isoformat_z(slice_window.end),
            }
            for slice_window in slice_windows
        ],
        "best_candidate_id": best["candidate_id"],
        "best_candidate": best,
        "candidates": scored_results,
    }
    ledger_payload = {
        "schema_version": "0.1",
        "stage": "stage4_trade_ledger",
        "created_at": payload["created_at"],
        "strategy_id": payload["strategy_id"],
        "stage1_scores_path": payload["stage1_scores_path"],
        "candidates": ledger_candidates,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "stage4_realized_expectancy.json").write_text(json.dumps(payload, indent=2) + "\n")
    (out_dir / "stage4_trade_ledger.json").write_text(json.dumps(ledger_payload, indent=2) + "\n")

    if summary_out is None:
        if out_dir.name == "scores":
            summary_out = out_dir.parent / "summaries" / "iteration_summary.md"
        else:
            summary_out = out_dir / "stage4_summary.md"
    write_summary(summary_out, payload)
    return payload


def parse_slice_window(raw_value: str) -> SliceWindow:
    if ":" not in raw_value:
        raise argparse.ArgumentTypeError("slice must be NAME:START:END")

    name, rest = raw_value.split(":", 1)
    for index, char in enumerate(rest):
        if char != ":":
            continue
        start_raw = rest[:index]
        end_raw = rest[index + 1 :]
        try:
            start = parse_ts(start_raw)
            end = parse_ts(end_raw)
        except Exception:
            continue
        return SliceWindow(name=name, start=start, end=end)

    raise argparse.ArgumentTypeError("slice must be NAME:START:END")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage1-scores", required=True)
    parser.add_argument("--candidates", required=True)
    parser.add_argument("--candles", required=True)
    parser.add_argument("--out-dir", required=True)
    parser.add_argument("--ground-truth-dir")
    parser.add_argument("--fees-bps-per-side", type=float, default=5.0)
    parser.add_argument("--slippage-bps-per-side", type=float, default=3.0)
    parser.add_argument("--slice", action="append", default=[], type=parse_slice_window)
    parser.add_argument("--summary-out")
    args = parser.parse_args()

    run_stage4(
        stage1_scores_path=Path(args.stage1_scores),
        candidates_path=Path(args.candidates),
        candles_path=Path(args.candles),
        out_dir=Path(args.out_dir),
        ground_truth_dir=Path(args.ground_truth_dir) if args.ground_truth_dir else None,
        fees_bps_per_side=args.fees_bps_per_side,
        slippage_bps_per_side=args.slippage_bps_per_side,
        slice_windows=args.slice,
        summary_out=Path(args.summary_out) if args.summary_out else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
