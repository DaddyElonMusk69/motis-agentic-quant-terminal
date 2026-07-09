from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage3.grid_search import (
    DEFAULT_FORWARD_HOURS,
    DEFAULT_FEES_BPS_PER_SIDE,
    DEFAULT_LEVERAGE,
    _build_trade_candle_windows,
    _coerce_candle,
    _coerce_datetime,
    _load_trade_inputs,
    _session_artifact_root,
)


DEFAULT_PYRAMID_STEPS = [0.1, 0.2, 0.3, 0.4, 0.5]
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
    shortlist_size: int = 5,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    trade_inputs = _load_trade_inputs(promotion_root / "stage3_trade_inputs.json")
    if not trade_inputs:
        raise ValueError("Stage 3 pyramid requires non-empty all-trade Stage 3 inputs.")

    setups = _resolve_stage4_setups(promotion_root, tp_pct=tp_pct, sl_pct=sl_pct)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    trade_candle_windows = _build_trade_candle_windows(
        trades=trade_inputs,
        candles=candle_rows,
        hard_exit_hours=forward_hours,
    )

    baseline = None
    records = []
    for setup in setups:
        setup_baseline = _score_pyramid_setup(
            trades=trade_inputs,
            candles=candle_rows,
            trade_candle_windows=trade_candle_windows,
            tp_pct=setup["tp_pct"],
            sl_pct=setup["sl_pct"],
            side_policies=setup.get("side_policies"),
            step_pct=999,
            max_legs=1,
            sl_breakeven=False,
            forward_hours=forward_hours,
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
        )
        setup_baseline = {
            **setup_baseline,
            "source_candidate_id": setup["candidate_id"],
            "source_setup": setup["setup"],
            "tp_pct": setup["tp_pct"],
            "sl_pct": setup["sl_pct"],
            "protect_trigger_pct": setup.get("protect_trigger_pct"),
            "baseline_pnl_pct": setup_baseline["pnl_pct"],
            "delta_vs_baseline_pct": 0.0,
            "comparison": "same",
        }
        if baseline is None:
            baseline = setup_baseline
        records.append(setup_baseline)
        step_values = steps or _pyramid_steps_for_setup(promotion_root, setup)
        for leg_count in range(2, max_legs + 1):
            for step in step_values:
                record = _score_pyramid_setup(
                    trades=trade_inputs,
                    candles=candle_rows,
                    trade_candle_windows=trade_candle_windows,
                    tp_pct=setup["tp_pct"],
                    sl_pct=setup["sl_pct"],
                    side_policies=setup.get("side_policies"),
                    step_pct=step,
                    max_legs=leg_count,
                    sl_breakeven=sl_breakeven,
                    forward_hours=forward_hours,
                    leverage=leverage,
                    fees_bps_per_side=fees_bps_per_side,
                )
                delta = record["pnl_pct"] - setup_baseline["pnl_pct"]
                records.append(
                    {
                        **record,
                        "source_candidate_id": setup["candidate_id"],
                        "source_setup": setup["setup"],
                        "tp_pct": setup["tp_pct"],
                        "sl_pct": setup["sl_pct"],
                        "protect_trigger_pct": setup.get("protect_trigger_pct"),
                        "baseline_pnl_pct": setup_baseline["pnl_pct"],
                        "delta_vs_baseline_pct": round(delta, 4),
                        "comparison": "BETTER" if delta > 0 else "worse" if delta < 0 else "same",
                    }
                )
    pyramid_records = [row for row in records if row.get("max_legs", 1) > 1]
    best = max(pyramid_records or records, key=lambda row: row["pnl_pct"]) if records else None
    _clear_stage3_pyramid_downstream_artifacts(promotion_root)
    stage4_candidates = _build_stage4_candidates(
        promotion_root=promotion_root,
        session=session,
        best_records=sorted(pyramid_records, key=lambda row: row["pnl_pct"], reverse=True)[:shortlist_size],
        sl_breakeven=sl_breakeven,
    )
    artifact = {
        "schema_version": "0.1",
        "stage": "stage3_conditional_execution_setup",
        "artifact_role": "stage3_pyramid_results",
        "stage3_mode": "numerical_exit_policy_pyramid",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "total_signals": len(trade_inputs),
        "tp_pct": best.get("tp_pct") if best else None,
        "sl_pct": best.get("sl_pct") if best else None,
        "max_legs": best.get("max_legs") if best else max_legs,
        "sl_breakeven": sl_breakeven,
        "forward_hours": forward_hours,
        "leverage": leverage,
        "fees_bps_per_side": fees_bps_per_side,
        "baseline": baseline or {},
        "results": records,
        "optimal": {
            "criterion": "max_pyramid_pnl_pct",
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
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
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
    round_trip_fee_pct = fees_bps_per_side * 2 / 100

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
                pnl += tp_pct - round_trip_fee_pct
                wins += 1
            else:
                entry = entries[leg_number - 1]
                loss_pct = abs(entry - sl_price) / entry * 100
                pnl -= loss_pct + round_trip_fee_pct
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
    trade_candle_windows: list[list[dict[str, Any]]] | None = None,
    tp_pct: float,
    sl_pct: float,
    side_policies: dict[str, Any] | None = None,
    step_pct: float,
    max_legs: int,
    sl_breakeven: bool,
    forward_hours: int,
    leverage: int,
    fees_bps_per_side: float = DEFAULT_FEES_BPS_PER_SIDE,
) -> dict[str, Any]:
    if trade_candle_windows is not None and len(trade_candle_windows) != len(trades):
        raise ValueError("Stage 3 pyramid trade candle windows must align with trade inputs.")
    pnl = 0.0
    legs = 0
    wins = 0
    losses = 0
    for index, trade in enumerate(trades):
        trade_tp_pct, trade_sl_pct = _pyramid_trade_policy(
            trade=trade,
            default_tp_pct=tp_pct,
            default_sl_pct=sl_pct,
            side_policies=side_policies,
        )
        trade_candles = trade_candle_windows[index] if trade_candle_windows is not None else candles
        outcome = simulate_pyramid_trade(
            trade=trade,
            candles=trade_candles,
            tp_pct=trade_tp_pct,
            sl_pct=trade_sl_pct,
            step_pct=step_pct,
            max_legs=max_legs,
            sl_breakeven=sl_breakeven,
            forward_hours=forward_hours,
            leverage=leverage,
            fees_bps_per_side=fees_bps_per_side,
        )
        pnl += outcome["pnl_pct"]
        legs += outcome["legs_filled"]
        wins += outcome["wins"]
        losses += outcome["losses"]
    return {
        "step_pct": round(step_pct, 1) if step_pct < 100 else None,
        "max_legs": max_legs,
        "pnl_pct": round(pnl, 4),
        "avg_legs_per_signal": round(legs / len(trades), 4) if trades else 0,
        "wins": wins,
        "losses": losses,
    }


def _pyramid_trade_policy(
    *,
    trade: dict[str, Any],
    default_tp_pct: float,
    default_sl_pct: float,
    side_policies: dict[str, Any] | None,
) -> tuple[float, float]:
    if not isinstance(side_policies, dict):
        return default_tp_pct, default_sl_pct
    side = str(trade["direction"]).upper()
    policy = side_policies.get(side)
    if not isinstance(policy, dict):
        return default_tp_pct, default_sl_pct
    tp = policy.get("final_tp_pct", policy.get("lock_profit_pct", policy.get("tp_pct", default_tp_pct)))
    sl = policy.get("initial_sl_pct", policy.get("sl_pct", default_sl_pct))
    return float(tp), float(sl)


def _resolve_stage4_setups(promotion_root: Path, *, tp_pct: float | None, sl_pct: float | None) -> list[dict[str, Any]]:
    if tp_pct is not None and sl_pct is not None:
        return [
            {
                "candidate_id": f"manual_tp_{tp_pct}_sl_{sl_pct}".replace(".", "p"),
                "setup": {"entry_model": "market", "tp_pct": tp_pct, "sl_pct": sl_pct},
                "tp_pct": float(tp_pct),
                "sl_pct": float(sl_pct),
                "protect_trigger_pct": None,
            }
        ]
    candidates_path = promotion_root / "stage4_candidates.json"
    candidates_payload = _read_json(candidates_path)
    if not candidates_payload:
        raise ValueError("Stage 3 pyramid requires a Stage 4 candidate shortlist from Stage 3 policy testing.")
    setups = []
    for candidate in candidates_payload.get("candidates", []):
        candidate_id = str(candidate.get("candidate_id") or "")
        if candidate_id.startswith("pyramid_"):
            continue
        setup = candidate.get("setup") if isinstance(candidate.get("setup"), dict) else {}
        if setup.get("tp_pct") is None or setup.get("sl_pct") is None:
            continue
        setups.append(
            {
                "candidate_id": candidate_id,
                "setup": setup,
                "tp_pct": float(setup["tp_pct"]),
                "sl_pct": float(setup["sl_pct"]),
                "protect_trigger_pct": float(setup["protect_trigger_pct"]) if setup.get("protect_trigger_pct") is not None else None,
                "side_policies": setup.get("side_policies") if setup.get("policy_mode") == "side_specific" else None,
            }
        )
    if setups:
        return setups
    raise ValueError("Stage 3 pyramid requires at least one non-pyramid setup in the Stage 4 candidate shortlist.")


def _pyramid_steps_for_setup(promotion_root: Path, setup: dict[str, Any]) -> list[float]:
    return DEFAULT_PYRAMID_STEPS


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


def _build_stage4_candidates(
    *,
    promotion_root: Path,
    session: dict[str, Any],
    best_records: list[dict[str, Any]],
    sl_breakeven: bool,
) -> dict[str, Any]:
    existing = _read_json(promotion_root / "stage4_candidates.json") or {}
    candidates = [
        candidate
        for candidate in list(existing.get("candidates") or [])
        if not str(candidate.get("candidate_id", "")).startswith("pyramid_")
    ]
    for best in best_records:
        setup = dict(best.get("source_setup") or {})
        candidates.append(
            {
                "candidate_id": (
                    f"pyramid_{best['source_candidate_id']}_legs_{best['max_legs']}_step_{best['step_pct']:.1f}".replace(".", "p")
                ),
                "source_candidate_id": best["source_candidate_id"],
                "setup": {
                    **setup,
                    "entry_model": "market",
                    "tp_pct": best["tp_pct"],
                    "sl_pct": best["sl_pct"],
                    "pyramid_step_pct": best["step_pct"],
                    "max_legs": best["max_legs"],
                    "sl_breakeven": sl_breakeven,
                    "timeout_policy": "close_at_cutoff",
                },
                "stage3_metrics": {
                    "stage3_mode": "numerical_exit_policy_pyramid",
                    "pnl_pct": best["pnl_pct"],
                    "baseline_pnl_pct": best["baseline_pnl_pct"],
                    "delta_vs_baseline_pct": best["delta_vs_baseline_pct"],
                    "avg_legs_per_signal": best["avg_legs_per_signal"],
                    "max_legs": best["max_legs"],
                    "step_pct": best["step_pct"],
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
        "stage3_mode": "numerical_exit_policy_pyramid",
        "session_id": session["session_id"],
        "strategy_id": session.get("strategy_id"),
        "asset": session.get("asset"),
        "candidates": candidates,
    }


def _clear_stage3_pyramid_downstream_artifacts(promotion_root: Path) -> None:
    for artifact in [
        "stage4_realized_expectancy.json",
        "stage4_trade_ledger.json",
        "stage4_optimal.json",
        "stage4_summary.md",
    ]:
        (promotion_root / artifact).unlink(missing_ok=True)
    shutil.rmtree(promotion_root / "stage4_runs", ignore_errors=True)


def _render_summary(artifact: dict[str, Any]) -> str:
    baseline = artifact["baseline"]
    lines = [
        "# Stage 3 Pyramiding",
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
        step = "baseline" if row.get("step_pct") is None else f"{row['step_pct']:.1f}%"
        lines.append(
            f"| {step} | {row['pnl_pct']:.1f}% | {row['delta_vs_baseline_pct']:.1f}% | "
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
