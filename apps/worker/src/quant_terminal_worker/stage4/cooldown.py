from __future__ import annotations

import json
import statistics
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from quant_terminal_worker.stage4.realized_expectancy import DEFAULT_FEES_BPS_PER_SIDE
from quant_terminal_worker.stage4.realized_expectancy import DEFAULT_SLIPPAGE_BPS_PER_SIDE
from quant_terminal_worker.stage4.realized_expectancy import _coerce_candle
from quant_terminal_worker.stage4.realized_expectancy import _index_signals
from quant_terminal_worker.stage4.realized_expectancy import _normalize_candidates
from quant_terminal_worker.stage4.realized_expectancy import _read_json
from quant_terminal_worker.stage4.realized_expectancy import _score_candidate
from quant_terminal_worker.stage4.realized_expectancy import _session_artifact_root
from quant_terminal_worker.stage4.realized_expectancy import _slice_windows


SCHEMA_VERSION = "stage4_loss_cooldown_experiment.v1"


def run_stage4_loss_cooldown_experiment(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    consecutive_loss_options: Iterable[int] = (2, 3),
    cooldown_hour_options: Iterable[int] = range(4, 13),
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    realized = _read_json(promotion_root / "stage4_realized_expectancy.json")
    source_candidate_id = str(realized.get("best_candidate_id") or "")
    if not source_candidate_id:
        raise ValueError("Stage 4 loss cooldown requires a selected Stage 4 candidate.")

    candidates = _normalize_candidates(_read_json(promotion_root / "stage4_candidates.json"))
    candidate = next((row for row in candidates if row["candidate_id"] == source_candidate_id), None)
    if candidate is None:
        raise ValueError(f"Selected Stage 4 candidate not found: {source_candidate_id}")

    stage1_scores = _read_json(promotion_root / "stage1a_canonical_full_cycle_scores.json")
    records = stage1_scores.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("Stage 4 loss cooldown requires non-empty canonical Stage 1 score records.")

    signals_by_id = _index_signals(signal_rows)
    candle_rows = sorted((_coerce_candle(candle) for candle in candles), key=lambda row: row["timestamp"])
    inputs = realized.get("simulation_inputs") or {}
    costs = realized.get("cost_assumptions") or {}
    score_kwargs = {
        "candidate": candidate,
        "records": records,
        "signals_by_id": signals_by_id,
        "candles": candle_rows,
        "initial_capital_usdt": float(inputs.get("initial_capital_usdt", 10_000.0)),
        "margin_allocation_pct": float(inputs.get("margin_allocation_pct", 30.0)),
        "leverage": float(inputs.get("leverage", candidate.get("leverage", 5.0))),
        "fees_bps_per_side": float(costs.get("fees_bps_per_side", DEFAULT_FEES_BPS_PER_SIDE)),
        "slippage_bps_per_side": float(costs.get("slippage_bps_per_side", DEFAULT_SLIPPAGE_BPS_PER_SIDE)),
        "slice_windows": _slice_windows(session),
    }

    baseline, baseline_trades = _score_candidate(**score_kwargs)
    _validate_baseline_replay(baseline, realized.get("best_candidate") or {})

    results: list[dict[str, Any]] = []
    ledgers: dict[tuple[int, int], list[dict[str, Any]]] = {}
    for consecutive_losses in _normalized_options(consecutive_loss_options, "consecutive losses"):
        for cooldown_hours in _normalized_options(cooldown_hour_options, "cooldown hours"):
            result, trades = _score_candidate(
                **score_kwargs,
                loss_cooldown={
                    "consecutive_losses": consecutive_losses,
                    "cooldown_hours": cooldown_hours,
                },
            )
            result["policy_id"] = f"losses_{consecutive_losses}_cooldown_{cooldown_hours}h"
            results.append(result)
            ledgers[(consecutive_losses, cooldown_hours)] = trades

    selected = max(results, key=_training_selection_key)
    selected_policy = selected["loss_cooldown"]
    selected_ledger = ledgers[(selected_policy["consecutive_losses"], selected_policy["cooldown_hours"])]
    walk_forward_oracle = max(results, key=_walk_forward_diagnostic_key)
    created_at = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_role": "stage4_loss_cooldown_experiment",
        "created_at": created_at,
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "source_stage4_run_id": realized.get("run_id"),
        "source_stage4_candidate_id": source_candidate_id,
        "selection_rule": {
            "data_used": "training_only",
            "primary": "training compounded account return percent",
            "tie_breakers": ["training profit factor", "training executed trades", "shorter cooldown", "higher loss threshold"],
            "walk_forward_role": "held-out evaluation only",
        },
        "cooldown_semantics": {
            "loss_definition": "executed trade net PnL after fees is below zero",
            "activation_time": "exit timestamp of the threshold loss",
            "entry_rule": "signals before cooldown_until are skipped; a signal exactly at cooldown_until is eligible",
            "counter_reset": "on a profitable/flat trade or when a cooldown triggers",
        },
        "baseline": baseline,
        "selected_policy_id": selected["policy_id"],
        "selected_policy": selected,
        "walk_forward_oracle_diagnostic": {
            "warning": "Diagnostic only; not eligible for selection because it uses held-out outcomes.",
            "policy_id": walk_forward_oracle["policy_id"],
            "loss_cooldown": walk_forward_oracle["loss_cooldown"],
            "walk_forward_test": (walk_forward_oracle.get("slices") or {}).get("walk_forward_test", {}),
        },
        "grid_results": results,
        "monthly_stability": {
            "baseline": _monthly_stability(baseline_trades),
            "selected": _monthly_stability(selected_ledger),
        },
    }
    ledger = {
        "schema_version": SCHEMA_VERSION,
        "artifact_role": "stage4_loss_cooldown_selected_trade_ledger",
        "created_at": created_at,
        "session_id": session["session_id"],
        "source_stage4_candidate_id": source_candidate_id,
        "selected_policy_id": selected["policy_id"],
        "trades": selected_ledger,
    }
    output_root = promotion_root / "stage4_loss_cooldown"
    output_root.mkdir(parents=True, exist_ok=True)
    experiment_path = output_root / "cooldown_experiment.json"
    ledger_path = output_root / "cooldown_trade_ledger.json"
    summary_path = output_root / "cooldown_summary.md"
    experiment_path.write_text(json.dumps(payload, indent=2) + "\n")
    ledger_path.write_text(json.dumps(ledger, indent=2) + "\n")
    summary_path.write_text(_render_summary(payload))
    return {
        **payload,
        "experiment_path": str(experiment_path),
        "trade_ledger_path": str(ledger_path),
        "summary_path": str(summary_path),
    }


def _normalized_options(values: Iterable[int], label: str) -> list[int]:
    normalized = sorted({int(value) for value in values})
    if not normalized or normalized[0] < 1:
        raise ValueError(f"{label} options must contain positive integers")
    return normalized


def _training_selection_key(result: dict[str, Any]) -> tuple[float, float, int, int, int]:
    training = (result.get("slices") or {}).get("training") or {}
    training_account = (result.get("slice_accounts") or {}).get("training") or {}
    policy = result["loss_cooldown"]
    return (
        float(training_account.get("return_pct") or 0.0),
        float(training.get("profit_factor") or 0.0),
        int(training.get("executed_trades") or 0),
        -int(policy["cooldown_hours"]),
        int(policy["consecutive_losses"]),
    )


def _walk_forward_diagnostic_key(result: dict[str, Any]) -> tuple[float, float]:
    walk_forward = (result.get("slices") or {}).get("walk_forward_test") or {}
    walk_forward_account = (result.get("slice_accounts") or {}).get("walk_forward_test") or {}
    return (
        float(walk_forward_account.get("return_pct") or 0.0),
        float(walk_forward.get("profit_factor") or 0.0),
    )


def _monthly_stability(trades: list[dict[str, Any]]) -> dict[str, Any]:
    by_month: dict[str, list[dict[str, Any]]] = {}
    for trade in trades:
        if trade.get("entry_status") != "FILLED":
            continue
        month = str(trade.get("exit_ts") or trade.get("signal_ts"))[:7]
        by_month.setdefault(month, []).append(trade)
    months = []
    for month, rows in sorted(by_month.items()):
        start_equity = float(rows[0]["equity_before"])
        end_equity = float(rows[-1]["equity_after"])
        return_pct = (end_equity / start_equity - 1) * 100 if start_equity else 0.0
        months.append(
            {
                "month": month,
                "start_equity_usdt": round(start_equity, 4),
                "end_equity_usdt": round(end_equity, 4),
                "return_pct": round(return_pct, 8),
                "executed_trades": len(rows),
            }
        )
    returns = [row["return_pct"] for row in months]
    return {
        "months": months,
        "month_count": len(months),
        "positive_months": sum(1 for value in returns if value > 0),
        "negative_months": sum(1 for value in returns if value < 0),
        "flat_months": sum(1 for value in returns if value == 0),
        "mean_monthly_return_pct": round(statistics.mean(returns), 8) if returns else 0.0,
        "monthly_return_stdev_pct": round(statistics.pstdev(returns), 8) if len(returns) > 1 else 0.0,
        "best_month_return_pct": round(max(returns), 8) if returns else 0.0,
        "worst_month_return_pct": round(min(returns), 8) if returns else 0.0,
    }


def _validate_baseline_replay(replayed: dict[str, Any], persisted: dict[str, Any]) -> None:
    checks = {
        "candidate_id": (str(replayed.get("candidate_id")), str(persisted.get("candidate_id"))),
        "executed_trades": (int(replayed.get("executed_trades") or 0), int(persisted.get("executed_trades") or 0)),
        "ending_equity_usdt": (
            round(float((replayed.get("account") or {}).get("ending_equity_usdt") or 0.0), 4),
            round(float((persisted.get("account") or {}).get("ending_equity_usdt") or 0.0), 4),
        ),
    }
    mismatches = [f"{name}: replay={actual}, persisted={expected}" for name, (actual, expected) in checks.items() if actual != expected]
    if mismatches:
        raise ValueError("Cooldown baseline replay does not match persisted Stage 4: " + "; ".join(mismatches))


def _render_summary(payload: dict[str, Any]) -> str:
    baseline = payload["baseline"]
    selected = payload["selected_policy"]
    baseline_wf = (baseline.get("slices") or {}).get("walk_forward_test") or {}
    selected_wf = (selected.get("slices") or {}).get("walk_forward_test") or {}
    selected_training = (selected.get("slices") or {}).get("training") or {}
    baseline_wf_account = (baseline.get("slice_accounts") or {}).get("walk_forward_test") or {}
    selected_wf_account = (selected.get("slice_accounts") or {}).get("walk_forward_test") or {}
    selected_training_account = (selected.get("slice_accounts") or {}).get("training") or {}
    policy = selected["loss_cooldown"]
    return "\n".join(
        [
            "# Stage 4 Loss Cooldown Experiment",
            "",
            f"Source Stage 4 run: `{payload.get('source_stage4_run_id')}`",
            f"Fixed candidate: `{payload.get('source_stage4_candidate_id')}`",
            f"Selected on training only: `{selected['policy_id']}`",
            f"Policy: pause `{policy['cooldown_hours']}` hours after `{policy['consecutive_losses']}` consecutive net losing trades.",
            f"Training compounded return: `{selected_training_account.get('return_pct', 0):.4f}%`",
            f"Training summed trade returns: `{selected_training.get('net_pnl_pct', 0):.4f}%`",
            f"Training profit factor: `{selected_training.get('profit_factor', 0):.4f}`",
            f"Walk-forward compounded return: `{baseline_wf_account.get('return_pct', 0):.4f}%` -> `{selected_wf_account.get('return_pct', 0):.4f}%`",
            f"Walk-forward summed trade returns: `{baseline_wf.get('net_pnl_pct', 0):.4f}%` -> `{selected_wf.get('net_pnl_pct', 0):.4f}%`",
            f"Walk-forward profit factor: `{baseline_wf.get('profit_factor', 0):.4f}` -> `{selected_wf.get('profit_factor', 0):.4f}`",
            f"Overall ending equity: `${(baseline.get('account') or {}).get('ending_equity_usdt', 0):.4f}` -> `${(selected.get('account') or {}).get('ending_equity_usdt', 0):.4f}`",
            f"Cooldown triggers / skipped signals: `{selected.get('loss_cooldown_triggers', 0)}` / `{selected.get('skipped_loss_cooldown', 0)}`",
            "",
        ]
    )
