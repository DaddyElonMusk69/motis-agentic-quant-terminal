from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any


DEFAULT_TP_VALUES = [round(1.0 + (0.3 * index), 1) for index in range(9)]
DEFAULT_SL_VALUES = [round(0.3 + (0.1 * index), 1) for index in range(13)]
DEFAULT_FORWARD_HOURS = 36
DEFAULT_LEVERAGE = 5


def run_stage3_grid_search(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    candles: list[Any],
    tp_values: list[float] | None = None,
    sl_values: list[float] | None = None,
    forward_hours: int = DEFAULT_FORWARD_HOURS,
    leverage: int = DEFAULT_LEVERAGE,
    shortlist_size: int = 5,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    trade_inputs = _load_trade_inputs(promotion_root / "stage2_capture_per_signal.json")
    if not trade_inputs:
        raise ValueError("Stage 3 requires non-empty Stage 2 MATCH signal inputs.")

    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    tps = tp_values or DEFAULT_TP_VALUES
    sls = sl_values or DEFAULT_SL_VALUES
    results = [
        _score_setup(
            trades=trade_inputs,
            candles=candle_rows,
            tp_pct=tp,
            sl_pct=sl,
            forward_hours=forward_hours,
            leverage=leverage,
        )
        for tp in tps
        for sl in sls
    ]
    sorted_results = sorted(
        results,
        key=lambda row: (row["pnl_pct"], row["expectancy"], row["profit_factor"], row["wr"]),
        reverse=True,
    )
    top = sorted_results[:shortlist_size]
    stage4_candidates = _build_stage4_candidates(session=session, candidates=top)
    artifact = {
        "schema_version": "0.1",
        "stage": "stage3_conditional_execution_setup",
        "artifact_role": "stage3_grid_results",
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "total_signals": len(trade_inputs),
        "forward_hours": forward_hours,
        "leverage": leverage,
        "entry_model": "market",
        "tp_values": tps,
        "sl_values": sls,
        "tiebreaker": "candle_body_direction",
        "results": results,
        "optimal": {
            "criterion": "max_pnl_pct",
            "top_5": top,
            "best": top[0] if top else None,
        },
        "stage4_candidates": stage4_candidates,
    }
    promotion_root.mkdir(parents=True, exist_ok=True)
    grid_path = promotion_root / "stage3_grid_results.json"
    optimal_path = promotion_root / "stage3_optimal.json"
    candidates_path = promotion_root / "stage4_candidates.json"
    summary_path = promotion_root / "stage3_summary.md"
    grid_path.write_text(json.dumps(artifact, indent=2) + "\n")
    optimal_path.write_text(json.dumps(artifact["optimal"], indent=2) + "\n")
    candidates_path.write_text(json.dumps(stage4_candidates, indent=2) + "\n")
    summary_path.write_text(_render_summary(artifact))
    return {
        **artifact,
        "grid_results_path": str(grid_path),
        "optimal_path": str(optimal_path),
        "stage4_candidates_path": str(candidates_path),
        "summary_path": str(summary_path),
    }


def simulate_trade(
    *,
    trade: dict[str, Any],
    candles: list[dict[str, Any]],
    tp_pct: float,
    sl_pct: float,
    forward_hours: int,
) -> str:
    entry = float(trade["reference_price"])
    direction = trade["direction"]
    signal_ts = _coerce_datetime(trade["signal_ts"])
    cutoff = signal_ts + timedelta(hours=forward_hours)

    for candle in candles:
        timestamp = candle["timestamp"]
        if timestamp <= signal_ts:
            continue
        if timestamp > cutoff:
            break
        if direction == "LONG":
            tp_price = entry * (1 + tp_pct / 100)
            sl_price = entry * (1 - sl_pct / 100)
            tp_hit = candle["high"] >= tp_price
            sl_hit = candle["low"] <= sl_price
        else:
            tp_price = entry * (1 - tp_pct / 100)
            sl_price = entry * (1 + sl_pct / 100)
            tp_hit = candle["low"] <= tp_price
            sl_hit = candle["high"] >= sl_price

        if tp_hit and sl_hit:
            body = candle["close"] - candle["open"]
            if direction == "LONG":
                return "TP" if body >= 0 else "SL"
            return "TP" if body <= 0 else "SL"
        if tp_hit:
            return "TP"
        if sl_hit:
            return "SL"
    return "NEITHER"


def _score_setup(
    *,
    trades: list[dict[str, Any]],
    candles: list[dict[str, Any]],
    tp_pct: float,
    sl_pct: float,
    forward_hours: int,
    leverage: int,
) -> dict[str, Any]:
    outcomes = []
    for trade in trades:
        outcome = simulate_trade(
            trade=trade,
            candles=candles,
            tp_pct=tp_pct,
            sl_pct=sl_pct,
            forward_hours=forward_hours,
        )
        outcomes.append({**trade, "outcome": outcome})
    return _summarize_outcomes(
        outcomes=outcomes,
        tp_pct=tp_pct,
        sl_pct=sl_pct,
        leverage=leverage,
    )


def _summarize_outcomes(
    *,
    outcomes: list[dict[str, Any]],
    tp_pct: float,
    sl_pct: float,
    leverage: int,
) -> dict[str, Any]:
    total = len(outcomes)
    tp_count = sum(1 for row in outcomes if row["outcome"] == "TP")
    sl_count = sum(1 for row in outcomes if row["outcome"] == "SL")
    neither = sum(1 for row in outcomes if row["outcome"] == "NEITHER")
    gross_tp = tp_count * tp_pct
    gross_sl = sl_count * sl_pct
    return {
        "tp": round(tp_pct, 1),
        "sl": round(sl_pct, 1),
        "entry_model": "market",
        "tp_count": tp_count,
        "sl_count": sl_count,
        "neither": neither,
        "total": total,
        "wr": round(tp_count / total * 100, 1) if total else 0.0,
        "expectancy": round((gross_tp - gross_sl) / total, 2) if total else 0.0,
        "profit_factor": round(gross_tp / gross_sl, 2) if gross_sl > 0 else (999 if gross_tp > 0 else 0),
        "pnl_pct": round((gross_tp - gross_sl) * leverage, 1),
        "rr_ratio": round(tp_pct / sl_pct, 1) if sl_pct > 0 else 0,
        "slice_split": _split_counts(outcomes, "sample_role"),
        "side_split": _split_counts(outcomes, "direction"),
    }


def _split_counts(outcomes: list[dict[str, Any]], key: str) -> dict[str, dict[str, int]]:
    split: dict[str, dict[str, int]] = {}
    for row in outcomes:
        bucket = str(row.get(key) or "unknown")
        current = split.setdefault(bucket, {"tp_count": 0, "sl_count": 0, "neither": 0, "total": 0})
        current["total"] += 1
        if row["outcome"] == "TP":
            current["tp_count"] += 1
        elif row["outcome"] == "SL":
            current["sl_count"] += 1
        else:
            current["neither"] += 1
    return split


def _build_stage4_candidates(*, session: dict[str, Any], candidates: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "0.1",
        "artifact_role": "stage4_candidates",
        "source_stage": "stage3_conditional_execution_setup",
        "session_id": session["session_id"],
        "strategy_id": session.get("strategy_id"),
        "asset": session.get("asset"),
        "candidates": [
            {
                "candidate_id": f"market_tp_{row['tp']:.1f}_sl_{row['sl']:.1f}".replace(".", "p"),
                "setup": {
                    "entry_model": "market",
                    "tp_pct": row["tp"],
                    "sl_pct": row["sl"],
                    "timeout_policy": "close_at_cutoff",
                },
                "stage3_metrics": {
                    "wr": row["wr"],
                    "expectancy": row["expectancy"],
                    "profit_factor": row["profit_factor"],
                    "pnl_pct": row["pnl_pct"],
                    "tp_count": row["tp_count"],
                    "sl_count": row["sl_count"],
                    "neither": row["neither"],
                },
            }
            for row in candidates
        ],
    }


def _render_summary(artifact: dict[str, Any]) -> str:
    lines = [
        "# Stage 3 Grid Search",
        "",
        f"Session: `{artifact['session_id']}`",
        f"Signals: {artifact['total_signals']}",
        f"Leverage: {artifact['leverage']}x",
        f"Tiebreaker: {artifact['tiebreaker']}",
        "",
        "| TP | SL | WR | TP | SL | Neither | Expectancy | PF | PnL |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for row in artifact["optimal"]["top_5"]:
        lines.append(
            f"| {row['tp']:.1f}% | {row['sl']:.1f}% | {row['wr']:.1f}% | {row['tp_count']} | "
            f"{row['sl_count']} | {row['neither']} | {row['expectancy']:.2f}% | "
            f"{row['profit_factor']:.2f} | {row['pnl_pct']:.1f}% |"
        )
    lines.append("")
    lines.append("Stage 4 must test these shortlisted setups on the full frozen Stage 1 decision set.")
    lines.append("")
    return "\n".join(lines)


def _load_trade_inputs(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise ValueError(f"Stage 3 requires Stage 2 per-signal artifact: {path}")
    rows = json.loads(path.read_text())
    return [
        {
            "signal_id": str(row["signal_id"]),
            "sample_role": str(row.get("sample_role") or "full_cycle"),
            "direction": _direction(row),
            "signal_ts": row["signal_ts"],
            "reference_price": float(row["reference_price"]),
        }
        for row in rows
    ]


def _direction(row: dict[str, Any]) -> str:
    direction = row.get("direction") or row.get("natural_direction")
    if direction not in {"LONG", "SHORT"}:
        raise ValueError(f"Stage 3 requires LONG/SHORT directions, got {direction!r}.")
    return str(direction)


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
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)
