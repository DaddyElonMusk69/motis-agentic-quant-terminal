from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage3.grid_search import (
    DEFAULT_FORWARD_HOURS,
    DEFAULT_LEVERAGE,
    _coerce_candle,
    _coerce_datetime,
    _load_trade_inputs,
    _session_artifact_root,
)


DEFAULT_PYRAMID_STEPS = [0.3, 0.5, 0.7, 0.9, 1.1, 1.3, 1.5]
DEFAULT_MAX_LEGS = 3


def run_stage3_pyramid(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_pct: float | None = None,
    sl_pct: float | None = None,
    steps: list[float] | None = None,
    max_legs: int = DEFAULT_MAX_LEGS,
    sl_breakeven: bool = False,
    forward_hours: int = DEFAULT_FORWARD_HOURS,
    leverage: int = DEFAULT_LEVERAGE,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    trade_inputs = _load_trade_inputs(promotion_root / "stage2_capture_per_signal.json")
    if not trade_inputs:
        raise ValueError("Stage 3 pyramid requires non-empty Stage 2 MATCH signal inputs.")

    setup = _resolve_grid_setup(promotion_root, tp_pct=tp_pct, sl_pct=sl_pct)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    step_values = steps or DEFAULT_PYRAMID_STEPS

    baseline = _score_pyramid_setup(
        trades=trade_inputs,
        candles=candle_rows,
        tp_pct=setup["tp_pct"],
        sl_pct=setup["sl_pct"],
        step_pct=999,
        max_legs=1,
        sl_breakeven=False,
        forward_hours=forward_hours,
        leverage=leverage,
    )
    records = []
    for step in step_values:
        record = _score_pyramid_setup(
            trades=trade_inputs,
            candles=candle_rows,
            tp_pct=setup["tp_pct"],
            sl_pct=setup["sl_pct"],
            step_pct=step,
            max_legs=max_legs,
            sl_breakeven=sl_breakeven,
            forward_hours=forward_hours,
            leverage=leverage,
        )
        delta = record["pnl_pct"] - baseline["pnl_pct"]
        records.append(
            {
                **record,
                "delta_vs_baseline_pct": round(delta, 4),
                "comparison": "BETTER" if delta > 0 else "worse" if delta < 0 else "same",
            }
        )
    best = max(records, key=lambda row: row["pnl_pct"]) if records else None
    stage4_candidates = _build_stage4_candidates(
        promotion_root=promotion_root,
        session=session,
        setup=setup,
        best=best,
        sl_breakeven=sl_breakeven,
        max_legs=max_legs,
    )
    artifact = {
        "schema_version": "0.1",
        "stage": "stage3_conditional_execution_setup",
        "artifact_role": "stage3_pyramid_results",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "total_signals": len(trade_inputs),
        "tp_pct": setup["tp_pct"],
        "sl_pct": setup["sl_pct"],
        "max_legs": max_legs,
        "sl_breakeven": sl_breakeven,
        "forward_hours": forward_hours,
        "leverage": leverage,
        "baseline": baseline,
        "results": records,
        "optimal": {
            "criterion": "max_pnl_pct",
            "best": best,
        },
        "stage4_candidates": stage4_candidates,
    }
    promotion_root.mkdir(parents=True, exist_ok=True)
    results_path = promotion_root / "stage3_pyramid_results.json"
    optimal_path = promotion_root / "stage3_pyramid_optimal.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    summary_path = promotion_root / "stage3_pyramid_summary.md"
    results_path.write_text(json.dumps(artifact, indent=2) + "\n")
    optimal_path.write_text(json.dumps(artifact["optimal"], indent=2) + "\n")
    candidates_path.write_text(json.dumps(stage4_candidates, indent=2) + "\n")
    summary_path.write_text(_render_summary(artifact))
    return {
        **artifact,
        "results_path": str(results_path),
        "optimal_path": str(optimal_path),
        "stage4_candidates_path": str(candidates_path),
        "summary_path": str(summary_path),
    }


def simulate_pyramid_trade(
    *,
    trade: dict[str, Any],
    candles: list[dict[str, Any]],
    tp_pct: float,
    sl_pct: float,
    step_pct: float,
    max_legs: int,
    sl_breakeven: bool,
    forward_hours: int,
    leverage: int,
) -> dict[str, Any]:
    direction = trade["direction"]
    reference_price = float(trade["reference_price"])
    signal_ts = _coerce_datetime(trade["signal_ts"])
    cutoff = signal_ts + timedelta(hours=forward_hours)
    sl_price = reference_price * (1 - sl_pct / 100) if direction == "LONG" else reference_price * (1 + sl_pct / 100)
    active = [
        {
            "leg": 1,
            "entry": reference_price,
            "tp": _target_price(reference_price, tp_pct=tp_pct, direction=direction),
        }
    ]
    entries = [reference_price]
    legs_filled = 1
    wins = 0
    losses = 0
    pnl = 0.0

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_ts:
            continue
        if timestamp > cutoff:
            break

        if legs_filled < max_legs:
            next_entry = _next_entry(entries[-1], step_pct=step_pct, direction=direction)
            if _entry_hit(candle, next_entry, direction=direction):
                legs_filled += 1
                entries.append(next_entry)
                active.append(
                    {
                        "leg": legs_filled,
                        "entry": next_entry,
                        "tp": _target_price(next_entry, tp_pct=tp_pct, direction=direction),
                    }
                )
                if sl_breakeven:
                    sl_price = sum(entries) / len(entries)

        closed: list[tuple[int, str]] = []
        for leg in active:
            tp_hit, sl_hit = _tp_sl_hit(candle, tp=leg["tp"], sl=sl_price, direction=direction)
            if tp_hit and sl_hit:
                body = candle["close"] - candle["open"]
                closed.append((leg["leg"], "TP" if _body_favors_direction(body, direction=direction) else "SL"))
            elif tp_hit:
                closed.append((leg["leg"], "TP"))
            elif sl_hit:
                closed.append((leg["leg"], "SL"))

        for leg_number, outcome in closed:
            if outcome == "TP":
                pnl += tp_pct * leverage
                wins += 1
            else:
                entry = entries[leg_number - 1]
                loss_pct = abs(entry - sl_price) / entry * 100
                pnl -= loss_pct * leverage
                losses += 1

        closed_leg_numbers = {leg_number for leg_number, _ in closed}
        active = [leg for leg in active if leg["leg"] not in closed_leg_numbers]
        if not active:
            break

    return {
        "pnl_pct": pnl,
        "legs_filled": legs_filled,
        "wins": wins,
        "losses": losses,
    }


def _score_pyramid_setup(
    *,
    trades: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    tp_pct: float,
    sl_pct: float,
    step_pct: float,
    max_legs: int,
    sl_breakeven: bool,
    forward_hours: int,
    leverage: int,
) -> dict[str, Any]:
    pnl = 0.0
    legs = 0
    wins = 0
    losses = 0
    for trade in trades:
        outcome = simulate_pyramid_trade(
            trade=trade,
            candles=candles,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            step_pct=step_pct,
            max_legs=max_legs,
            sl_breakeven=sl_breakeven,
            forward_hours=forward_hours,
            leverage=leverage,
        )
        pnl += outcome["pnl_pct"]
        legs += outcome["legs_filled"]
        wins += outcome["wins"]
        losses += outcome["losses"]
    return {
        "step_pct": round(step_pct, 1) if step_pct < 100 else None,
        "pnl_pct": round(pnl, 4),
        "avg_legs_per_signal": round(legs / len(trades), 4) if trades else 0,
        "wins": wins,
        "losses": losses,
    }


def _resolve_grid_setup(promotion_root: Path, *, tp_pct: float | None, sl_pct: float | None) -> dict[str, float]:
    if tp_pct is not None and sl_pct is not None:
        return {"tp_pct": tp_pct, "sl_pct": sl_pct}
    optimal_path = promotion_root / "stage3_optimal.json"
    if not optimal_path.is_file():
        raise ValueError("Stage 3 pyramid requires Stage 3 grid optimal artifact.")
    optimal = json.loads(optimal_path.read_text())
    best = optimal.get("best") or {}
    if "tp" not in best or "sl" not in best:
        raise ValueError("Stage 3 pyramid requires TP/SL from Stage 3 grid optimal artifact.")
    return {"tp_pct": float(best["tp"]), "sl_pct": float(best["sl"])}


def _build_stage4_candidates(
    *,
    promotion_root: Path,
    session: dict[str, Any],
    setup: dict[str, float],
    best: dict[str, Any] | None,
    sl_breakeven: bool,
    max_legs: int,
) -> dict[str, Any]:
    existing = _read_json(promotion_root / "stage4_candidates.json") or {}
    candidates = list(existing.get("candidates") or [])
    if best:
        candidates.append(
            {
                "candidate_id": f"pyramid_tp_{setup['tp_pct']:.1f}_sl_{setup['sl_pct']:.1f}_step_{best['step_pct']:.1f}".replace(".", "p"),
                "setup": {
                    "entry_model": "market",
                    "tp_pct": setup["tp_pct"],
                    "sl_pct": setup["sl_pct"],
                    "pyramid_step_pct": best["step_pct"],
                    "max_legs": max_legs,
                    "sl_breakeven": sl_breakeven,
                    "timeout_policy": "close_at_cutoff",
                },
                "stage3_metrics": {
                    "pnl_pct": best["pnl_pct"],
                    "delta_vs_baseline_pct": best["delta_vs_baseline_pct"],
                    "avg_legs_per_signal": best["avg_legs_per_signal"],
                    "wins": best["wins"],
                    "losses": best["losses"],
                    "comparison": best["comparison"],
                },
            }
        )
    return {
        "schema_version": "0.1",
        "artifact_role": "stage4_candidates",
        "source_stage": "stage3_conditional_execution_setup",
        "session_id": session["session_id"],
        "strategy_id": session.get("strategy_id"),
        "asset": session.get("asset"),
        "candidates": candidates,
    }


def _render_summary(artifact: dict[str, Any]) -> str:
    baseline = artifact["baseline"]
    lines = [
        "# Stage 3 Pyramid",
        "",
        f"Session: `{artifact['session_id']}`",
        f"Signals: {artifact['total_signals']}",
        f"Base setup: TP {artifact['tp_pct']:.1f}% / SL {artifact['sl_pct']:.1f}%",
        f"Baseline PnL: {baseline['pnl_pct']:.1f}%",
        "",
        "| Step | PnL | Delta | Avg Legs | Wins | Losses | Comparison |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    for row in artifact["results"]:
        lines.append(
            f"| {row['step_pct']:.1f}% | {row['pnl_pct']:.1f}% | {row['delta_vs_baseline_pct']:.1f}% | "
            f"{row['avg_legs_per_signal']:.2f} | {row['wins']} | {row['losses']} | {row['comparison']} |"
        )
    lines.append("")
    lines.append("Stage 4 must test shortlisted execution setups on the full frozen Stage 1 decision set.")
    lines.append("")
    return "\n".join(lines)


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


def _body_favors_direction(body: float, *, direction: str) -> bool:
    return body >= 0 if direction == "LONG" else body <= 0


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    payload = json.loads(path.read_text())
    return payload if isinstance(payload, dict) else None
