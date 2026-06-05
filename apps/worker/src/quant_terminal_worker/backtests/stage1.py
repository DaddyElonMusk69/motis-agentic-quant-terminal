from __future__ import annotations

from typing import Any

from quant_terminal_worker.subprocess_runner import run_entrypoint_subprocess


def run_stage1_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    engine = payload["signal_engine"]
    strategy = payload["strategy"]
    engine_result = run_entrypoint_subprocess(
        entrypoint=engine["runtime_entrypoint"],
        payload={
            "asset": payload["asset"],
            "instrument": payload["instrument"],
            "dataset_refs": payload.get("dataset_refs", []),
            "rows": payload.get("rows", []),
            "parameters": engine.get("parameters", {}),
        },
        timeout_seconds=payload.get("timeout_seconds", 30),
    )
    signals = sorted(engine_result.get("signals", []), key=lambda signal: signal["timestamp"])
    decisions = [
        run_entrypoint_subprocess(
            entrypoint=strategy["runtime_entrypoint"],
            payload={
                "signal": signal,
                "runtime_mode": "backtest",
                "parameters": strategy.get("parameters", {}),
                "raw_data": {"dataset_refs": payload.get("dataset_refs", [])},
            },
            timeout_seconds=payload.get("timeout_seconds", 30),
        )
        for signal in signals
    ]

    return {
        "run_id": payload["run_id"],
        "stage": "stage1a",
        "signal_engine_id": engine["signal_engine_id"],
        "signal_engine_version": engine["version"],
        "strategy_id": strategy["strategy_id"],
        "strategy_version": strategy["version"],
        "dataset_refs": payload.get("dataset_refs", []),
        "signals": signals,
        "decisions": decisions,
        "score_summary": _score_stage1a(decisions, payload.get("ground_truth", {})),
    }


def _score_stage1a(
    decisions: list[dict[str, Any]],
    ground_truth: dict[str, str],
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    matched = 0
    mismatched = 0
    skipped = 0

    for decision in decisions:
        signal_id = decision["signal_id"]
        truth = ground_truth.get(signal_id)
        direction = decision["direction"]
        if decision["action"] == "SKIP" or direction == "FLAT":
            agreement = "SKIP"
            status = "SKIPPED"
            skipped += 1
        elif truth and direction == truth:
            agreement = "MATCH"
            status = "CORRECT"
            matched += 1
        else:
            agreement = "MISMATCH"
            status = "INCORRECT"
            mismatched += 1
        records.append(
            {
                "signal_id": signal_id,
                "ground_truth_direction": truth,
                "decision_direction": direction,
                "agreement": agreement,
                "status": status,
            }
        )

    total = len(records)
    return {
        "scoring_method": "stage1a_directional_agreement",
        "metrics": {
            "total": total,
            "matched": matched,
            "mismatched": mismatched,
            "skipped": skipped,
            "agreement_rate": round(matched / total, 6) if total else 0.0,
        },
        "records": records,
    }
