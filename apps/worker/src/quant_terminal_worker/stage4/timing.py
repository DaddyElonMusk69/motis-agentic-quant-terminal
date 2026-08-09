from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_terminal_worker.stage4.realized_expectancy import DEFAULT_FEES_BPS_PER_SIDE
from quant_terminal_worker.stage4.realized_expectancy import DEFAULT_SLIPPAGE_BPS_PER_SIDE
from quant_terminal_worker.stage4.realized_expectancy import _choose_best_candidate
from quant_terminal_worker.stage4.realized_expectancy import _coerce_candle
from quant_terminal_worker.stage4.realized_expectancy import _coerce_datetime
from quant_terminal_worker.stage4.realized_expectancy import _index_signals
from quant_terminal_worker.stage4.realized_expectancy import _normalize_candidates
from quant_terminal_worker.stage4.realized_expectancy import _packet_from_signal
from quant_terminal_worker.stage4.realized_expectancy import _read_json
from quant_terminal_worker.stage4.realized_expectancy import _read_json_if_exists
from quant_terminal_worker.stage4.realized_expectancy import _score_candidate
from quant_terminal_worker.stage4.realized_expectancy import _session_artifact_root
from quant_terminal_worker.stage4.realized_expectancy import _slice_windows
from quant_terminal_worker.stage4.realized_expectancy import _stage4_run_id


OVERLAY_SCHEMA_VERSION = "stage4b_timing_overlay.v1"
REPLAY_SCHEMA_VERSION = "0.1"


def generate_stage4b_timing_prompt(*, workspace_root: Path, session: dict[str, Any]) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    realized = _require_stage4a(promotion_root)
    best = realized.get("best_candidate") or {}
    timing_root = promotion_root / "stage4b_timing"
    timing_root.mkdir(parents=True, exist_ok=True)

    context_path = timing_root / "timing_context.json"
    prompt_path = timing_root / "timing_optimizer_prompt.md"
    overlay_path = timing_root / "timing_overlay.json"
    context = {
        "schema_version": "0.1",
        "artifact_role": "stage4b_timing_context",
        "created_at": _utc_now(),
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "signal_engine_id": session.get("signal_engine_id"),
        "source_stage4_run_id": realized.get("run_id"),
        "source_stage4_candidate_id": realized.get("best_candidate_id") or best.get("candidate_id"),
        "stage1_scores_path": str(promotion_root / "stage1a_canonical_full_cycle_scores.json"),
        "stage4_candidates_path": str(promotion_root / "stage4_candidates.json"),
        "stage4_realized_expectancy_path": str(promotion_root / "stage4_realized_expectancy.json"),
        "stage4_trade_ledger_path": str(promotion_root / "stage4_trade_ledger.json"),
        "timing_overlay_path": str(overlay_path),
        "simulation_inputs": realized.get("simulation_inputs", {}),
        "required_skill": "$stage4b-timing-optimizer",
    }
    prompt = _render_prompt(context)
    context_path.write_text(json.dumps(context, indent=2) + "\n")
    prompt_path.write_text(prompt)
    return {
        "prompt_type": "stage4b_timing_optimizer",
        "session_id": session["session_id"],
        "prompt": prompt,
        "prompt_path": str(prompt_path),
        "context_path": str(context_path),
        "overlay_path": str(overlay_path),
    }


def run_stage4b_timing_replay(
    *,
    workspace_root: Path,
    session: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    candles: list[Any],
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    artifact_root = _session_artifact_root(workspace_root=workspace_root, session=session)
    promotion_root = artifact_root / "promotion"
    realized = _require_stage4a(promotion_root)
    timing_root = promotion_root / "stage4b_timing"
    overlay_path = timing_root / "timing_overlay.json"
    overlay = _validate_overlay(_read_json(overlay_path), source_stage4_run_id=str(realized.get("run_id") or ""))

    stage1_scores = _read_json(promotion_root / "stage1a_canonical_full_cycle_scores.json")
    records = stage1_scores.get("records", [])
    if not isinstance(records, list) or not records:
        raise ValueError("Stage 4B requires non-empty canonical Stage 1 score records.")
    candidates = _normalize_candidates(_read_json(promotion_root / "stage4_candidates.json"))
    if not candidates:
        raise ValueError("Stage 4B requires at least one Stage 4 candidate.")

    signals_by_id = _index_signals(signal_rows)
    candle_rows = [_coerce_candle(candle) for candle in candles]
    candle_rows.sort(key=lambda row: row["timestamp"])
    filtered_records = _apply_overlay(records=records, signals_by_id=signals_by_id, overlay=overlay)
    inputs = realized.get("simulation_inputs") or {}
    costs = realized.get("cost_assumptions") or {}
    slice_windows = _slice_windows(session)
    realized_candidates_by_id = {
        str(candidate.get("candidate_id")): candidate
        for candidate in realized.get("candidates", [])
        if isinstance(candidate, dict) and candidate.get("candidate_id")
    }
    results = []
    ledger_candidates = []
    for candidate in candidates:
        realized_candidate = realized_candidates_by_id.get(str(candidate["candidate_id"])) or {}
        pause_rules = realized_candidate.get("pause_rules") or None
        pause_rule = None if pause_rules else realized_candidate.get("pause_rule") or None
        loss_cooldown = None if pause_rules or pause_rule else realized_candidate.get("loss_cooldown") or None
        result, trades = _score_candidate(
            candidate=candidate,
            records=filtered_records,
            signals_by_id=signals_by_id,
            candles=candle_rows,
            asset=str(session.get("asset") or "").upper() or None,
            market_data_repository=market_data_repository,
            workspace_root=workspace_root,
            initial_capital_usdt=float(inputs.get("initial_capital_usdt", 10_000.0)),
            margin_allocation_pct=float(inputs.get("margin_allocation_pct", 30.0)),
            leverage=float(inputs.get("leverage", candidate.get("leverage", 5.0))),
            fees_bps_per_side=float(costs.get("fees_bps_per_side", DEFAULT_FEES_BPS_PER_SIDE)),
            slippage_bps_per_side=float(costs.get("slippage_bps_per_side", DEFAULT_SLIPPAGE_BPS_PER_SIDE)),
            slice_windows=slice_windows,
            pause_rules=pause_rules,
            pause_rule=pause_rule,
            loss_cooldown=loss_cooldown,
        )
        result["skipped_timing_filter"] = sum(1 for trade in trades if trade.get("skip_reason") == "timing_filter")
        results.append(result)
        ledger_candidates.append({"candidate_id": candidate["candidate_id"], "setup": candidate, "trades": trades})

    best = _choose_best_candidate(results)
    created_at_dt = datetime.now(UTC)
    created_at = created_at_dt.isoformat().replace("+00:00", "Z")
    run_id = _stage4b_run_id(created_at_dt, timing_root)
    ledger = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "stage": "stage4b_timing_trade_ledger",
        "artifact_role": "stage4b_timing_trade_ledger",
        "created_at": created_at,
        "run_id": run_id,
        "session_id": session["session_id"],
        "candidates": ledger_candidates,
    }
    payload = {
        "schema_version": REPLAY_SCHEMA_VERSION,
        "stage": "stage4b_timing_replay",
        "artifact_role": "stage4b_timing_replay",
        "created_at": created_at,
        "run_id": run_id,
        "session_id": session["session_id"],
        "asset": session.get("asset"),
        "strategy_id": session.get("strategy_id"),
        "strategy_version": session.get("strategy_version"),
        "signal_engine_id": session.get("signal_engine_id"),
        "signal_set_id": session.get("signal_set_id"),
        "baseline": {
            "source": "stage4_realized_expectancy",
            "run_id": realized.get("run_id"),
            "best_candidate_id": realized.get("best_candidate_id"),
            "best_candidate": realized.get("best_candidate"),
        },
        "overlay": overlay,
        "simulation_inputs": inputs,
        "cost_assumptions": costs,
        "decision_cadence": realized.get("decision_cadence"),
        "slice_windows": realized.get("slice_windows", []),
        "best_candidate_id": best["candidate_id"],
        "best_candidate": best,
        "candidates": results,
        "ledger": ledger,
    }
    _write_replay_artifacts(timing_root=timing_root, run_id=run_id, payload=payload, ledger=ledger)
    return {
        **payload,
        "timing_replay_path": str(timing_root / "timing_replay.json"),
        "timing_trade_ledger_path": str(timing_root / "timing_trade_ledger.json"),
        "timing_summary_path": str(timing_root / "timing_summary.md"),
    }


def _require_stage4a(promotion_root: Path) -> dict[str, Any]:
    realized_path = promotion_root / "stage4_realized_expectancy.json"
    ledger_path = promotion_root / "stage4_trade_ledger.json"
    optimal_path = promotion_root / "stage4_optimal.json"
    summary_path = promotion_root / "stage4_summary.md"
    if not (realized_path.is_file() and ledger_path.is_file() and optimal_path.is_file() and summary_path.is_file()):
        raise ValueError("Stage 4B requires completed Stage 4A evidence")
    return _read_json(realized_path)


def _validate_overlay(overlay: dict[str, Any], *, source_stage4_run_id: str) -> dict[str, Any]:
    if not isinstance(overlay, dict):
        raise ValueError("Stage 4B timing overlay must be a JSON object.")
    if overlay.get("schema_version") != OVERLAY_SCHEMA_VERSION:
        raise ValueError(f"Stage 4B timing overlay schema_version must be {OVERLAY_SCHEMA_VERSION}.")
    if str(overlay.get("source_stage4_run_id") or "") != source_stage4_run_id:
        raise ValueError("Stage 4B timing overlay source_stage4_run_id must match the latest Stage 4A run.")
    forbidden = {
        "signal_id",
        "signal_ids",
        "exclude_signal_id",
        "exclude_signal_ids",
        "include_signal_id",
        "include_signal_ids",
        "exact_timestamps",
        "exclude_dates",
        "include_dates",
        "direction",
        "tp_pct",
        "sl_pct",
        "leverage",
        "margin_allocation_pct",
        "sizing",
        "pyramid",
    }
    present_forbidden = sorted(forbidden.intersection(overlay))
    if present_forbidden:
        if any("signal" in key for key in present_forbidden):
            raise ValueError("Stage 4B timing overlay cannot target exact signal filters.")
        raise ValueError(f"Stage 4B timing overlay contains forbidden fields: {', '.join(present_forbidden)}")
    hours = overlay.get("exclude_utc_hours")
    if not isinstance(hours, list) or not hours:
        raise ValueError("Stage 4B timing overlay requires non-empty exclude_utc_hours.")
    normalized_hours = []
    for hour in hours:
        if not isinstance(hour, int) or hour < 0 or hour > 23:
            raise ValueError("Stage 4B exclude_utc_hours values must be integers from 0 through 23.")
        normalized_hours.append(hour)
    weekdays = overlay.get("exclude_utc_weekdays", [])
    if weekdays is None:
        weekdays = []
    if not isinstance(weekdays, list):
        raise ValueError("Stage 4B exclude_utc_weekdays must be a list when provided.")
    normalized_weekdays = []
    for weekday in weekdays:
        if not isinstance(weekday, int) or weekday < 0 or weekday > 6:
            raise ValueError("Stage 4B exclude_utc_weekdays values must be integers from 0 through 6.")
        normalized_weekdays.append(weekday)
    applies_to = str(overlay.get("applies_to") or "all").upper()
    if applies_to not in {"ALL", "LONG", "SHORT"}:
        raise ValueError("Stage 4B applies_to must be all, LONG, or SHORT.")
    rationale = str(overlay.get("rationale") or "").strip()
    if not rationale:
        raise ValueError("Stage 4B timing overlay requires rationale.")
    return {
        **overlay,
        "exclude_utc_hours": sorted(set(normalized_hours)),
        "exclude_utc_weekdays": sorted(set(normalized_weekdays)),
        "applies_to": applies_to.lower() if applies_to == "ALL" else applies_to,
        "rationale": rationale,
    }


def _apply_overlay(*, records: list[dict[str, Any]], signals_by_id: dict[str, dict[str, Any]], overlay: dict[str, Any]) -> list[dict[str, Any]]:
    hours = set(overlay["exclude_utc_hours"])
    weekdays = set(overlay.get("exclude_utc_weekdays") or [])
    applies_to = str(overlay.get("applies_to") or "all").upper()
    output = []
    for record in records:
        signal = signals_by_id.get(str(record["signal_id"])) or signals_by_id.get(str(record["signal_id"]).split(":")[-1])
        if signal is None:
            raise ValueError(f"Stage 4B signal row not found for canonical decision: {record['signal_id']}")
        packet = _packet_from_signal(signal)
        signal_ts = _coerce_datetime(packet.get("timestamp") or signal["timestamp"])
        direction = str(record.get("decision_direction") or record.get("agent_direction") or "").upper()
        skip_for_time = signal_ts.hour in hours and (not weekdays or signal_ts.weekday() in weekdays)
        skip_for_side = applies_to in {"ALL", "all"} or applies_to == direction
        if skip_for_time and skip_for_side:
            updated = {**record, "decision_direction": "SKIP", "agent_direction": "SKIP", "stage4b_skip_reason": "timing_filter"}
        else:
            updated = dict(record)
        output.append(updated)
    return output


def _write_replay_artifacts(*, timing_root: Path, run_id: str, payload: dict[str, Any], ledger: dict[str, Any]) -> None:
    timing_root.mkdir(parents=True, exist_ok=True)
    run_root = timing_root / "stage4b_runs" / run_id
    run_root.mkdir(parents=True, exist_ok=True)
    summary = _render_summary(payload)
    for root in (timing_root, run_root):
        (root / "timing_replay.json").write_text(json.dumps(payload, indent=2) + "\n")
        (root / "timing_trade_ledger.json").write_text(json.dumps(ledger, indent=2) + "\n")
        (root / "timing_summary.md").write_text(summary)
    _update_run_index(
        timing_root=timing_root,
        run={
            "run_id": run_id,
            "created_at": payload["created_at"],
            "best_candidate_id": payload["best_candidate_id"],
            "best_candidate": payload["best_candidate"],
            "account": (payload["best_candidate"] or {}).get("account", {}),
            "overlay": payload["overlay"],
            "timing_replay_path": str(run_root / "timing_replay.json"),
            "timing_trade_ledger_path": str(run_root / "timing_trade_ledger.json"),
            "timing_summary_path": str(run_root / "timing_summary.md"),
        },
    )


def _update_run_index(*, timing_root: Path, run: dict[str, Any]) -> None:
    index_path = timing_root / "stage4b_runs" / "index.json"
    existing = _read_json_if_exists(index_path) or {"schema_version": REPLAY_SCHEMA_VERSION, "artifact_role": "stage4b_timing_run_index", "runs": []}
    runs = [item for item in existing.get("runs", []) if item.get("run_id") != run["run_id"]]
    runs.append(run)
    index_path.write_text(
        json.dumps(
            {
                "schema_version": REPLAY_SCHEMA_VERSION,
                "artifact_role": "stage4b_timing_run_index",
                "latest_run_id": run["run_id"],
                "runs": runs,
            },
            indent=2,
        )
        + "\n"
    )


def _stage4b_run_id(created_at: datetime, timing_root: Path) -> str:
    return _stage4_run_id(created_at, timing_root)


def _render_prompt(context: dict[str, Any]) -> str:
    return "\n".join(
        [
            "# Stage 4B Timing Optimization Prompt",
            "",
            "Use `$stage4b-timing-optimizer`.",
            "",
            f"Session: `{context['session_id']}`",
            f"Asset: `{context.get('asset')}`",
            f"Source Stage 4A run: `{context.get('source_stage4_run_id')}`",
            f"Source Stage 4A candidate: `{context.get('source_stage4_candidate_id')}`",
            "",
            "Write the timing overlay to:",
            f"`{context['timing_overlay_path']}`",
            "",
            "Rules:",
            "- Use UTC packet/signal timestamps only.",
            "- Build timing buckets from the full canonical Stage 1 decision set, including signals that Stage 4A skipped because a position was open.",
            "- Use training buckets to propose weak windows; use walk-forward buckets only to confirm or reject them.",
            "- Use broad recurring timing windows only.",
            "- Test simple UTC hours and contiguous hour blocks before choosing the smallest coherent filter.",
            "- Do not use exact dates or exact signal IDs.",
            "- Do not flip direction or change TP/SL, sizing, leverage, pyramids, or execution fields.",
            "- The overlay may only convert matching decisions into SKIP.",
            "- After writing the overlay, run Stage 4B timing replay and compare against Stage 4A using realized metrics, especially walk-forward return and profit factor.",
            "",
            "Required overlay schema:",
            "```json",
            json.dumps(
                {
                    "schema_version": OVERLAY_SCHEMA_VERSION,
                    "source_stage4_run_id": context.get("source_stage4_run_id"),
                    "source_stage4_candidate_id": context.get("source_stage4_candidate_id"),
                    "exclude_utc_hours": [0],
                    "exclude_utc_weekdays": [],
                    "applies_to": "all",
                    "rationale": "Replace with the training-supported timing weakness.",
                },
                indent=2,
            ),
            "```",
            "",
        ]
    )


def _render_summary(payload: dict[str, Any]) -> str:
    best = payload["best_candidate"]
    account = best.get("account") or {}
    baseline = payload.get("baseline") or {}
    return "\n".join(
        [
            "# Stage 4B Timing Replay",
            "",
            f"Baseline run: `{baseline.get('run_id')}`",
            f"Timing run: `{payload['run_id']}`",
            f"Best candidate: `{best['candidate_id']}`",
            f"Timing skips: `{best.get('skipped_timing_filter', 0)}`",
            f"Ending equity: `${account.get('ending_equity_usdt', 0):.4f}`",
            f"Net PnL: `${account.get('net_pnl_usdt', 0):.4f}`",
            f"Profit factor: `{best.get('profit_factor', 0):.4f}`",
            "",
        ]
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")
