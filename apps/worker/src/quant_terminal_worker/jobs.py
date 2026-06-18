from __future__ import annotations

import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.adapters.okx import OKXAdapter
from quant_terminal_worker.ingestion.ema_enrichment import enrich_derived_ema_datasets
from quant_terminal_worker.ingestion.feature_enrichment import enrich_feature_family_datasets
from quant_terminal_worker.ingestion.raw_candle_fill import fill_raw_candle_dataset
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles
from quant_terminal_worker.stage0.execution import execute_stage0_candidate
from quant_terminal_worker.stage0.workspace import read_parquet_candles_for_stage0
from quant_terminal_worker.stage1.scoring import run_stage1a_canonical_full_cycle
from quant_terminal_worker.stage1.scoring import run_stage1a_score
from quant_terminal_worker.stage2.capture_curve import run_stage2_capture_curve
from quant_terminal_worker.stage3.grid_search import run_stage3_exact_protection
from quant_terminal_worker.stage3.grid_search import run_stage3_fixed_sl_baseline
from quant_terminal_worker.stage3.grid_search import run_stage3_grid_search
from quant_terminal_worker.stage3.grid_search import run_stage3_local_variants
from quant_terminal_worker.stage3.pyramid import run_stage3_pyramid
from quant_terminal_worker.stage4.portfolio_backtest import run_portfolio_backtest
from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy


def execute_job(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    handlers = {
        "market_data_refresh": _execute_market_data_refresh,
        "market_data_ema_refresh": _execute_market_data_ema_refresh,
        "market_data_feature_refresh": _execute_market_data_feature_refresh,
        "signal_pool_extend": _execute_signal_pool_extend,
        "stage0_candidate": _execute_stage0_candidate_job,
        "stage0_candidate_batch": _execute_stage0_candidate_batch,
        "stage1_canonical": _execute_stage1_canonical,
        "stage1_score": _execute_stage1_score,
        "stage2_capture_curve": _execute_stage2_capture_curve,
        "stage3_policy_step": _execute_stage3_policy_step,
        "stage3_pyramid": _execute_stage3_pyramid,
        "stage4_realized_expectancy": _execute_stage4_realized_expectancy,
        "portfolio_backtest": _execute_portfolio_backtest,
    }
    handler = handlers.get(job["job_type"])
    if handler is None:
        raise ValueError(f"Unsupported job type: {job['job_type']}")
    return handler(
        repository=repository,
        job=job,
        workspace_root=workspace_root,
        market_data_repository=market_data_repository,
    )


def run_claimed_job(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any] | None:
    with _job_heartbeat(repository=repository, job_id=job["job_id"]):
        try:
            result = execute_job(
                repository=repository,
                job=job,
                workspace_root=workspace_root,
                market_data_repository=market_data_repository,
            )
        except Exception as exc:
            return repository.fail_job(
                job["job_id"],
                error={
                    "message": str(exc),
                    "type": exc.__class__.__name__,
                },
            )
        return repository.complete_job(job["job_id"], result=result)


class _job_heartbeat:
    def __init__(self, *, repository: Any, job_id: str, interval_seconds: float = 10.0) -> None:
        self.repository = repository
        self.job_id = job_id
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name=f"job-heartbeat-{job_id}", daemon=True)

    def __enter__(self) -> "_job_heartbeat":
        self._thread.start()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.wait(self.interval_seconds):
            try:
                self.repository.heartbeat_job(self.job_id)
            except Exception:
                pass


def _execute_market_data_refresh(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del repository, workspace_root
    if market_data_repository is None:
        raise ValueError("market data repository is required for market_data_refresh jobs")
    payload = job.get("payload") or {}
    dataset_id = str(payload["dataset_id"])
    registration = market_data_repository.get_ref(dataset_id)
    if registration is None:
        raise ValueError(f"dataset not found: {dataset_id}")
    return fill_raw_candle_dataset(
        registration=registration,
        repository=market_data_repository,
        adapter=OKXAdapter(
            {
                "backend": "okx_cli",
                "mode": payload.get("okx_mode", "demo"),
                "market_mode": payload.get("market_mode", "live"),
            }
        ),
    )


def _execute_market_data_ema_refresh(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del repository, workspace_root
    if market_data_repository is None:
        raise ValueError("market data repository is required for market_data_ema_refresh jobs")
    payload = job.get("payload") or {}
    asset = str(payload["asset"]).upper() if payload.get("asset") else None
    return enrich_derived_ema_datasets(repository=market_data_repository, asset=asset)


def _execute_market_data_feature_refresh(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del repository
    if market_data_repository is None:
        raise ValueError("market data repository is required for market_data_feature_refresh jobs")
    payload = job.get("payload") or {}
    asset = str(payload["asset"]).upper()
    family = str(payload["family"])
    return enrich_feature_family_datasets(
        repository=market_data_repository,
        asset=asset,
        family=family,
        target_root=workspace_root / ".data" / "market-data",
    )


def _execute_signal_pool_extend(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    repository.heartbeat_job(job["job_id"], current_step="signal_pool_extend")

    def report_progress(step: str) -> None:
        repository.heartbeat_job(job["job_id"], current_step=step)

    return extend_signal_pool_from_local_candles(
        workspace_root=workspace_root,
        repository=repository,
        signal_engine_id=str(payload["signal_engine_id"]),
        asset=str(payload["asset"]),
        target_end=payload.get("target_end"),
        progress_callback=report_progress,
    )


def _execute_stage0_candidate_job(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    if market_data_repository is None:
        raise ValueError("market data repository is required for stage0_candidate jobs")
    payload = job.get("payload") or {}
    universe_run = repository.get_stage0_universe_run(str(payload["universe_run_id"]))
    if universe_run is None:
        raise ValueError(f"stage0 universe run not found: {payload['universe_run_id']}")
    candidate = repository.get_stage0_universe_candidate(str(payload["candidate_id"]))
    if candidate is None:
        raise ValueError(f"stage0 universe candidate not found: {payload['candidate_id']}")
    repository.heartbeat_job(job["job_id"], current_step=f"stage0_{candidate['asset']}")
    result = _run_stage0_candidate(
        repository=repository,
        market_data_repository=market_data_repository,
        workspace_root=workspace_root,
        universe_run=universe_run,
        candidate=candidate,
    )
    repository.update_stage0_universe_candidate(result["candidate"])
    repository.refresh_stage0_universe_summary(universe_run["universe_run_id"])
    return result


def _execute_stage0_candidate_batch(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    if market_data_repository is None:
        raise ValueError("market data repository is required for stage0_candidate_batch jobs")
    payload = job.get("payload") or {}
    universe_run_id = str(payload["universe_run_id"])
    universe_run = repository.get_stage0_universe_run(universe_run_id)
    if universe_run is None:
        raise ValueError(f"stage0 universe run not found: {universe_run_id}")
    all_candidates = repository.list_stage0_universe_candidates(universe_run_id)
    pending_candidates = [candidate for candidate in all_candidates if candidate["acceptance_status"] == "pending_stage0"]
    selected_candidates = pending_candidates[: int(payload.get("limit") or 500)]
    results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    for candidate in selected_candidates:
        repository.heartbeat_job(job["job_id"], current_step=f"stage0_{candidate['asset']}")
        try:
            result = _run_stage0_candidate(
                repository=repository,
                market_data_repository=market_data_repository,
                workspace_root=workspace_root,
                universe_run=universe_run,
                candidate=candidate,
            )
            repository.update_stage0_universe_candidate(result["candidate"])
            results.append(result)
        except Exception as exc:
            errors.append({"candidate_id": candidate["candidate_id"], "asset": candidate["asset"], "detail": str(exc)})
            repository.mark_stage0_universe_candidate_error(
                candidate["candidate_id"],
                {"detail": str(exc), "type": exc.__class__.__name__},
            )
    repository.refresh_stage0_universe_summary(universe_run_id)
    refreshed_run = repository.get_stage0_universe_run(universe_run_id) or universe_run
    refreshed_candidates = repository.list_stage0_universe_candidates(universe_run_id)
    return {
        "run": refreshed_run,
        "candidates": refreshed_candidates,
        "results": results,
        "errors": errors,
        "summary": {
            "requested": len(selected_candidates),
            "succeeded": len(results),
            "failed": len(errors),
            "skipped": len(all_candidates) - len(selected_candidates),
            "remaining_pending": sum(1 for candidate in refreshed_candidates if candidate["acceptance_status"] == "pending_stage0"),
        },
    }


def _execute_stage1_score(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session_id = str(payload["session_id"])
    iteration_id = str(payload["iteration_id"])
    sample_role = str(payload["sample_role"])
    repository.heartbeat_job(job["job_id"], current_step="scoring")
    session = repository.get_stage1_research_session(session_id)
    if session is None:
        raise ValueError(f"Stage 1 session not found: {session_id}")
    artifact_root = Path(session["artifact_root"])
    if not artifact_root.is_absolute():
        artifact_root = workspace_root / artifact_root
    iteration_root = artifact_root / "iterations" / iteration_id
    if not iteration_root.is_dir():
        raise ValueError(f"Stage 1 iteration not found: {iteration_id}")
    score = run_stage1a_score(iteration_root=iteration_root, sample_role=sample_role)
    return {
        "score": score,
        "session_id": session_id,
        "iteration_id": iteration_id,
        "sample_role": sample_role,
    }


def _execute_stage1_canonical(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session_id = str(payload["session_id"])
    repository.heartbeat_job(job["job_id"], current_step="canonical_stage1a")
    session = _stage1_session(repository, session_id)
    result = run_stage1a_canonical_full_cycle(
        workspace_root=workspace_root,
        session=session,
        signals_by_role=_stage1_full_cycle_signals(repository, session),
    )
    frozen_manifest = {
        **(session.get("manifest") or {}),
        "status": "stage1a_frozen",
        "stage1a_canonical_readout": result,
    }
    updater = getattr(repository, "update_stage1_research_session_state", None)
    if callable(updater):
        updater(session_id=session_id, status="stage1a_frozen", manifest=frozen_manifest)
    return {"canonical_readout": result, "session_id": session_id}


def _execute_stage2_capture_curve(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _stage1_session(repository, str(payload["session_id"]))
    repository.heartbeat_job(job["job_id"], current_step="stage2_capture_curve")
    result = run_stage2_capture_curve(
        workspace_root=workspace_root,
        session=session,
        signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(repository, session)),
        candles=_stage2_raw_candles(repository, session, workspace_root=workspace_root),
    )
    return {"stage2_capture": result, "session_id": session["session_id"]}


def _execute_stage3_policy_step(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _stage1_session(repository, str(payload["session_id"]))
    step = str(payload["step"])
    repository.heartbeat_job(job["job_id"], current_step=f"stage3_{step}")
    runner = {
        "grid_search": run_stage3_grid_search,
        "fixed_sl": run_stage3_fixed_sl_baseline,
        "exact_protection": run_stage3_exact_protection,
        "local_variants": run_stage3_local_variants,
    }[step]
    result = runner(
        workspace_root=workspace_root,
        session=session,
        candles=_stage2_raw_candles(repository, session, workspace_root=workspace_root),
    )
    return {"stage3_grid": result, "session_id": session["session_id"], "step": step}


def _execute_stage3_pyramid(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _stage1_session(repository, str(payload["session_id"]))
    repository.heartbeat_job(job["job_id"], current_step="stage3_pyramid")
    result = run_stage3_pyramid(
        workspace_root=workspace_root,
        session=session,
        candles=_stage2_raw_candles(repository, session, workspace_root=workspace_root),
    )
    return {"stage3_pyramid": result, "session_id": session["session_id"]}


def _execute_stage4_realized_expectancy(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _stage1_session(repository, str(payload["session_id"]))
    repository.heartbeat_job(job["job_id"], current_step="stage4_realized_expectancy")
    result = run_stage4_realized_expectancy(
        workspace_root=workspace_root,
        session=session,
        signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(repository, session)),
        candles=_stage2_raw_candles(repository, session, workspace_root=workspace_root),
        initial_capital_usdt=float(payload["initial_capital_usdt"]),
        margin_allocation_pct=float(payload["margin_allocation_pct"]),
        leverage=float(payload["leverage"]),
    )
    return {"stage4_realized_expectancy": result, "session_id": session["session_id"]}


def _execute_portfolio_backtest(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    universe_run_id = str(payload["universe_run_id"])
    universe_run = repository.get_stage0_universe_run(universe_run_id)
    if universe_run is None:
        raise ValueError(f"stage0 universe run not found: {universe_run_id}")
    repository.heartbeat_job(job["job_id"], current_step="portfolio_backtest")
    result = run_portfolio_backtest(
        workspace_root=workspace_root,
        universe_run=universe_run,
        candidates=repository.list_stage0_universe_candidates(universe_run_id),
        sessions=repository.list_stage1_research_sessions(),
        initial_capital_usdt=float(payload.get("initial_capital_usdt") or 10_000.0),
        margin_allocations_pct={str(key): float(value) for key, value in (payload.get("margin_allocations_pct") or {}).items()},
        repository=repository,
    )
    return {"portfolio_backtest": result, "universe_run_id": universe_run_id}


def _stage1_session(repository: Any, session_id: str) -> dict[str, Any]:
    session = repository.get_stage1_research_session(session_id)
    if session is None:
        raise ValueError(f"Stage 1 session not found: {session_id}")
    return session


def _run_stage0_candidate(
    *,
    repository: Any,
    market_data_repository: Any,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
) -> dict[str, Any]:
    signal_set = repository.get_signal_set(candidate["signal_set_key"])
    if signal_set is None:
        raise ValueError("signal set not found")
    candle_ref = market_data_repository.get_raw_candle_ref(candidate["asset"], "5m")
    if candle_ref is None:
        raise ValueError("raw 5m candle data not found")
    window_start = _iso_datetime(universe_run["window_start"])
    window_end = _iso_datetime(universe_run["window_end"])
    signals = repository.list_signals_for_signal_set_window(
        signal_set_key=candidate["signal_set_key"],
        window_start=window_start,
        window_end=window_end,
    )
    candle_rows = read_parquet_candles_for_stage0(
        storage_uri=Path(candle_ref["storage_uri"]),
        window_start=window_start,
        window_end=window_end,
        forward_hours=universe_run["forward_hours"],
    )
    if not signals:
        raise ValueError("candidate has no signal packets in window")
    if not candle_rows:
        raise ValueError("candidate has no candle rows for window")
    return execute_stage0_candidate(
        workspace_root=workspace_root,
        universe_run={**universe_run, "window_start": window_start, "window_end": window_end},
        candidate=candidate,
        signal_set=signal_set,
        signals=signals,
        candle_rows=candle_rows,
    )


def _stage1_full_cycle_signals(repository: Any, session: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    signals_by_role = {}
    for sample_role in ("training", "walk_forward_test"):
        window_start, window_end = _stage1_sample_window(session, sample_role)
        signals_by_role[sample_role] = repository.list_signals_for_signal_set_window(
            signal_set_key=session["signal_set_key"],
            window_start=f"{window_start}T00:00:00Z",
            window_end=f"{window_end}T23:59:59Z",
        )
    return signals_by_role


def _stage1_sample_window(session: dict[str, Any], sample_method: str) -> tuple[str, str]:
    if sample_method == "training":
        return _date_string(session["train_start"]), _date_string(session["train_end"])
    if sample_method == "walk_forward_test":
        return _date_string(session["walk_forward_start"]), _date_string(session["walk_forward_end"])
    raise ValueError(f"Unsupported Stage 1 sample method: {sample_method}")


def _flatten_signal_roles(signals_by_role: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    signals_by_id: dict[str, dict[str, Any]] = {}
    for signals in signals_by_role.values():
        for signal in signals:
            signals_by_id[str(signal["signal_id"])] = signal
    return list(signals_by_id.values())


def _stage2_raw_candles(repository: Any, session: dict[str, Any], *, workspace_root: Path) -> list[Any]:
    start = f"{_date_string(session['train_start'])}T00:00:00Z"
    end = _add_hours(f"{_date_string(session['walk_forward_end'])}T23:59:59Z", 36)
    reader = MarketDataReader(repository=repository, workspace_root=workspace_root)
    return reader.get_candles(
        asset=session["asset"],
        timeframe="5m",
        origin="raw",
        start=start,
        end=end,
    )


def _date_string(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    return str(value)[:10]


def _iso_datetime(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)


def _add_hours(value: str, hours: int) -> str:
    cleaned = value.replace("Z", "+00:00")
    return (datetime.fromisoformat(cleaned) + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")
