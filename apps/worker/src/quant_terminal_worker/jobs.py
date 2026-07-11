from __future__ import annotations

import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from quant_terminal_sdk.market_data_reader import (
    MarketDataReader,
    read_candles_from_ref,
    read_rows_from_ref,
)
from quant_terminal_worker.adapters.okx import OKXAdapter
from quant_terminal_worker.adapters.binance import BinanceCLIAdapter
from quant_terminal_worker.ingestion.ema_enrichment import enrich_derived_ema_datasets
from quant_terminal_worker.ingestion.feature_enrichment import enrich_feature_family_datasets
from quant_terminal_worker.ingestion.raw_candle_fill import fill_raw_candle_dataset
from quant_terminal_worker.ingestion.binance_open_interest import fill_raw_open_interest_dataset
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles
from quant_terminal_worker.signal_discovery.atlas import (
    DiscoveryConfig,
    run_fixed_target_window,
    run_training_atlas,
)
from quant_terminal_worker.signal_discovery.features import (
    build_causal_feature_rows,
    select_hard_negatives,
)
from quant_terminal_worker.signal_discovery.evaluation import evaluate_registered_engine
from quant_terminal_worker.signal_discovery.handoff import handoff_accepted_candidate
from quant_terminal_worker.signal_discovery.workspace import (
    materialize_training_atlas,
    materialize_walk_forward_atlas,
    write_session_manifest,
)
from quant_terminal_worker.stage0.execution import execute_stage0_candidate, execute_stage0_information_gate
from quant_terminal_worker.stage0.information import apply_information_q_values_to_candidates
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
from quant_terminal_worker.stage4.timing import run_stage4b_timing_replay


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
        "stage0_information_candidate": _execute_stage0_information_candidate_job,
        "stage0_candidate_batch": _execute_stage0_candidate_batch,
        "stage1_canonical": _execute_stage1_canonical,
        "stage1_score": _execute_stage1_score,
        "stage2_capture_curve": _execute_stage2_capture_curve,
        "stage3_policy_step": _execute_stage3_policy_step,
        "stage3_pyramid": _execute_stage3_pyramid,
        "stage4_realized_expectancy": _execute_stage4_realized_expectancy,
        "stage4b_timing_replay": _execute_stage4b_timing_replay,
        "portfolio_backtest": _execute_portfolio_backtest,
        "signal_discovery_atlas": _execute_signal_discovery_atlas,
        "signal_discovery_walk_forward": _execute_signal_discovery_walk_forward,
        "signal_discovery_engine_evaluation": _execute_signal_discovery_engine_evaluation,
        "signal_discovery_handoff": _execute_signal_discovery_handoff,
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
    if registration.get("data_type") == "open_interest":
        return fill_raw_open_interest_dataset(
            registration=registration,
            repository=market_data_repository,
            adapter=BinanceCLIAdapter(
                {
                    "cli_path": payload.get("binance_cli_path"),
                    "profile": payload.get("binance_profile"),
                }
            ),
        )
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


def _execute_signal_discovery_atlas(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    if market_data_repository is None:
        raise ValueError("market data repository is required for signal discovery atlas jobs")
    payload = job.get("payload") or {}
    session = _signal_discovery_session(repository, str(payload["session_id"]))
    repository.heartbeat_job(job["job_id"], current_step="signal_discovery_atlas")
    session = repository.update_signal_discovery_session(
        session["session_id"],
        status="atlas_running",
    )
    try:
        ref = _signal_discovery_data_ref(market_data_repository, session)
        config_value = session.get("config") or {}
        horizons = tuple(float(value) for value in config_value.get("horizon_hours", (48,)))
        delays = tuple(
            int(value) for value in config_value.get("entry_delays_minutes", (5,))
        )
        config = DiscoveryConfig(
            risk_values=tuple(float(value) for value in config_value["risk_values"]),
            research_start=session["research_start"],
            research_end=session["research_end"],
            walk_forward_start=session["walk_forward_start"],
            reward_multiple=float(config_value.get("reward_multiple") or 2.0),
            stop_multiple=float(config_value.get("stop_multiple") or 1.0),
            horizon_hours=horizons,
            entry_delays_minutes=delays,
            fee_bps_per_side=float(config_value.get("fee_bps_per_side") or 0.0),
            slippage_bps_per_side=float(config_value.get("slippage_bps_per_side") or 0.0),
        )
        read_start = session["research_start"] - timedelta(days=7)
        outcome_end = session["research_end"] + timedelta(
            hours=max(horizons),
            minutes=max(delays),
        )
        read_end = min(
            outcome_end,
            session["walk_forward_start"] - timedelta(microseconds=1),
        )
        candles = read_candles_from_ref(
            ref,
            workspace_root=workspace_root,
            start=read_start,
            end=read_end,
        )
        atlas = run_training_atlas(candles=candles, config=config)
        primary_delay = min(config.entry_delays_minutes)
        primary_horizon = min(config.horizon_hours)
        unique_decisions = sorted(
            {row["decision_ts"] for row in atlas["timestamp_labels"]}
        )
        oi_rows = _signal_discovery_oi_rows(
            market_data_repository=market_data_repository,
            workspace_root=workspace_root,
            config=config_value,
            start=read_start,
            end=session["research_end"],
        )
        features_with_labels = build_causal_feature_rows(
            candles=candles,
            decision_rows=[
                {"decision_ts": timestamp, "label": "UNLABELED"}
                for timestamp in unique_decisions
            ],
            walk_forward_start=session["walk_forward_start"],
            oi_rows=oi_rows,
        )
        features_by_timestamp = {
            row["decision_ts"]: {key: value for key, value in row.items() if key != "label"}
            for row in features_with_labels
        }
        hard_negatives: list[dict[str, Any]] = []
        for risk_pct in config.risk_values:
            labels = [
                row
                for row in atlas["timestamp_labels"]
                if row["risk_pct"] == risk_pct
                and row["scenario_entry_delay_minutes"] == primary_delay
                and row["scenario_horizon_hours"] == primary_horizon
            ]
            episodes = [
                row
                for row in atlas["episodes"]
                if row["risk_pct"] == risk_pct
                and row["entry_delay_minutes"] == primary_delay
                and row["horizon_hours"] == primary_horizon
            ]
            scenario_features = [
                {
                    **features_by_timestamp[row["decision_ts"]],
                    "label": row["label"],
                }
                for row in labels
                if row["decision_ts"] in features_by_timestamp
            ]
            for negative in select_hard_negatives(
                feature_rows=scenario_features,
                episodes=episodes,
            ):
                hard_negatives.append(
                    {
                        **negative,
                        "risk_pct": risk_pct,
                        "entry_delay_minutes": primary_delay,
                        "horizon_hours": primary_horizon,
                    }
                )

        feasibility = {
            "schema_version": "signal_discovery_r_feasibility.v1",
            "r_summaries": atlas["r_summaries"],
            "neighboring_r_diagnostics": atlas["neighboring_r_diagnostics"],
            "purged_decision_count": atlas["purged_decision_count"],
        }
        artifact_root = _signal_discovery_artifact_root(session, workspace_root=workspace_root)
        artifact_paths = materialize_training_atlas(
            artifact_root=artifact_root,
            timestamp_labels=atlas["timestamp_labels"],
            episodes=atlas["episodes"],
            features=list(features_by_timestamp.values()),
            hard_negatives=hard_negatives,
            r_feasibility=feasibility,
        )
        summary = {
            **feasibility,
            "training_timestamp_label_count": len(atlas["timestamp_labels"]),
            "training_episode_count": len(atlas["episodes"]),
            "training_feature_count": len(features_by_timestamp),
            "training_hard_negative_count": len(hard_negatives),
            "artifacts": {key: str(path) for key, path in artifact_paths.items()},
        }
        write_session_manifest(
            artifact_root=artifact_root,
            manifest={
                "session_id": session["session_id"],
                "status": "atlas_ready",
                "dataset_id": session["dataset_id"],
                "training_artifacts": summary["artifacts"],
            },
        )
        updated = repository.update_signal_discovery_session(
            session["session_id"],
            status="atlas_ready",
            summary=summary,
        )
        return {"session": updated, "summary": summary}
    except Exception as exc:
        _fail_signal_discovery_session(repository, session=session, exc=exc)
        raise


def _execute_signal_discovery_walk_forward(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    if market_data_repository is None:
        raise ValueError("market data repository is required for signal discovery WF jobs")
    payload = job.get("payload") or {}
    session = _signal_discovery_session(repository, str(payload["session_id"]))
    if not session.get("frozen_target") or session.get("target_version") is None:
        raise ValueError("signal discovery target must be frozen before WF evaluation")
    repository.heartbeat_job(job["job_id"], current_step="signal_discovery_walk_forward")
    session = repository.update_signal_discovery_session(
        session["session_id"],
        status="walk_forward_running",
    )
    try:
        ref = _signal_discovery_data_ref(market_data_repository, session)
        contract = session["frozen_target"]
        if contract.get("source_data", {}).get("dataset_id") != session["dataset_id"]:
            raise ValueError("frozen target dataset does not match the discovery session")
        selected_target = contract["selected_target"]
        read_end = session["walk_forward_end"] + timedelta(
            hours=float(selected_target["horizon_hours"]),
            minutes=int(selected_target["entry_delay_minutes"]),
        )
        candles = read_candles_from_ref(
            ref,
            workspace_root=workspace_root,
            start=session["walk_forward_start"],
            end=read_end,
        )
        result = run_fixed_target_window(
            candles=candles,
            window_start=session["walk_forward_start"],
            window_end=session["walk_forward_end"],
            selected_target=selected_target,
        )
        artifact_root = _signal_discovery_artifact_root(session, workspace_root=workspace_root)
        artifact_paths = materialize_walk_forward_atlas(
            artifact_root=artifact_root,
            timestamp_labels=result["timestamp_labels"],
            episodes=result["episodes"],
            summary=result["summary"],
        )
        summary = {
            **(session.get("summary") or {}),
            "walk_forward": result["summary"],
            "walk_forward_artifacts": {
                key: str(path) for key, path in artifact_paths.items()
            },
        }
        write_session_manifest(
            artifact_root=artifact_root,
            manifest={
                "session_id": session["session_id"],
                "status": "walk_forward_ready",
                "dataset_id": session["dataset_id"],
                "training_artifacts": summary.get("artifacts", {}),
                "walk_forward_artifacts": summary["walk_forward_artifacts"],
                "target_config_hash": contract["config_hash"],
            },
        )
        updated = repository.update_signal_discovery_session(
            session["session_id"],
            status="walk_forward_ready",
            summary=summary,
        )
        return {"session": updated, "walk_forward": result["summary"]}
    except Exception as exc:
        _fail_signal_discovery_session(repository, session=session, exc=exc)
        raise


def _execute_signal_discovery_engine_evaluation(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _signal_discovery_session(repository, str(payload["session_id"]))
    if not session.get("candidate_engine_id") or not session.get("candidate_signal_set_key"):
        raise ValueError("attach a signal discovery engine candidate before evaluation")
    repository.heartbeat_job(job["job_id"], current_step="signal_discovery_engine_evaluation")
    session = repository.update_signal_discovery_session(
        session["session_id"],
        status="evaluation_running",
    )
    try:
        evaluation = evaluate_registered_engine(
            workspace_root=workspace_root,
            repository=repository,
            session=session,
        )
        repository.update_signal_discovery_session(
            session["session_id"],
            status="evaluated",
            evaluation=evaluation,
        )
        if evaluation.get("accepted"):
            updated = repository.update_signal_discovery_session(
                session["session_id"],
                status="accepted",
            )
        else:
            updated = repository.get_signal_discovery_session(session["session_id"])
        return {"session": updated, "evaluation": evaluation}
    except Exception as exc:
        _fail_signal_discovery_session(repository, session=session, exc=exc)
        raise


def _execute_signal_discovery_handoff(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _signal_discovery_session(repository, str(payload["session_id"]))
    if not (session.get("evaluation") or {}).get("accepted"):
        raise ValueError("signal discovery handoff requires an accepted evaluation")
    repository.heartbeat_job(job["job_id"], current_step="signal_discovery_handoff")
    session = repository.update_signal_discovery_session(
        session["session_id"],
        status="handoff_running",
    )
    try:
        handoff = handoff_accepted_candidate(
            workspace_root=workspace_root,
            repository=repository,
            session=session,
        )
        updated = repository.update_signal_discovery_session(
            session["session_id"],
            status="handed_off",
            handoff=handoff,
        )
        return {"session": updated, "handoff": handoff}
    except Exception as exc:
        _fail_signal_discovery_session(repository, session=session, exc=exc)
        raise


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
        label_mode=str(payload.get("label_mode") or "threshold_first_hit"),
    )
    repository.update_stage0_universe_candidate(result["candidate"])
    _refresh_stage0_information_q_values(repository, universe_run["universe_run_id"])
    repository.refresh_stage0_universe_summary(universe_run["universe_run_id"])
    return result


def _execute_stage0_information_candidate_job(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    if market_data_repository is None:
        raise ValueError("market data repository is required for stage0_information_candidate jobs")
    payload = job.get("payload") or {}
    universe_run = repository.get_stage0_universe_run(str(payload["universe_run_id"]))
    if universe_run is None:
        raise ValueError(f"stage0 universe run not found: {payload['universe_run_id']}")
    candidate = repository.get_stage0_universe_candidate(str(payload["candidate_id"]))
    if candidate is None:
        raise ValueError(f"stage0 universe candidate not found: {payload['candidate_id']}")
    repository.heartbeat_job(job["job_id"], current_step=f"stage0_information_{candidate['asset']}")
    result = _run_stage0_information_candidate(
        repository=repository,
        market_data_repository=market_data_repository,
        workspace_root=workspace_root,
        universe_run=universe_run,
        candidate=candidate,
    )
    repository.update_stage0_universe_candidate(result["candidate"])
    _refresh_stage0_information_q_values(repository, universe_run["universe_run_id"])
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
                label_mode=str(payload.get("label_mode") or "threshold_first_hit"),
            )
            repository.update_stage0_universe_candidate(result["candidate"])
            results.append(result)
        except Exception as exc:
            errors.append({"candidate_id": candidate["candidate_id"], "asset": candidate["asset"], "detail": str(exc)})
            repository.mark_stage0_universe_candidate_error(
                candidate["candidate_id"],
                {"detail": str(exc), "type": exc.__class__.__name__},
            )
    _refresh_stage0_information_q_values(repository, universe_run_id)
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


def _execute_stage4b_timing_replay(
    *,
    repository: Any,
    job: dict[str, Any],
    workspace_root: Path,
    market_data_repository: Any | None = None,
) -> dict[str, Any]:
    del market_data_repository
    payload = job.get("payload") or {}
    session = _stage1_session(repository, str(payload["session_id"]))
    repository.heartbeat_job(job["job_id"], current_step="stage4b_timing_replay")
    result = run_stage4b_timing_replay(
        workspace_root=workspace_root,
        session=session,
        signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(repository, session)),
        candles=_stage2_raw_candles(repository, session, workspace_root=workspace_root),
    )
    return {"stage4b_timing": result, "session_id": session["session_id"]}


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
        signal_offset_count=int(payload.get("signal_offset_count") or 0),
        entry_fill_model=str(payload.get("entry_fill_model") or "reference_price"),
        exit_fill_model=str(payload.get("exit_fill_model") or "level_price"),
        repository=repository,
    )
    return {"portfolio_backtest": result, "universe_run_id": universe_run_id}


def _stage1_session(repository: Any, session_id: str) -> dict[str, Any]:
    session = repository.get_stage1_research_session(session_id)
    if session is None:
        raise ValueError(f"Stage 1 session not found: {session_id}")
    return session


def _signal_discovery_session(repository: Any, session_id: str) -> dict[str, Any]:
    session = repository.get_signal_discovery_session(session_id)
    if session is None:
        raise ValueError(f"Signal discovery session not found: {session_id}")
    return session


def _signal_discovery_data_ref(
    market_data_repository: Any,
    session: dict[str, Any],
) -> dict[str, Any]:
    getter = getattr(market_data_repository, "get_ref", None)
    ref = getter(session["dataset_id"]) if callable(getter) else None
    if ref is None:
        raise ValueError(f"signal discovery dataset not found: {session['dataset_id']}")
    if ref.get("storage_backend") != "parquet":
        raise ValueError("signal discovery requires canonical Parquet data")
    if str(ref.get("asset") or "").upper() != str(session["asset"]).upper():
        raise ValueError("signal discovery dataset asset does not match the session")
    if ref.get("data_type") != "candles" or ref.get("timeframe") != "5m":
        raise ValueError("signal discovery requires canonical 5m candles")
    return ref


def _signal_discovery_oi_rows(
    *,
    market_data_repository: Any,
    workspace_root: Path,
    config: dict[str, Any],
    start: datetime,
    end: datetime,
) -> list[dict[str, Any]]:
    dataset_id = config.get("oi_dataset_id")
    if not dataset_id:
        return []
    getter = getattr(market_data_repository, "get_ref", None)
    ref = getter(str(dataset_id)) if callable(getter) else None
    if ref is None:
        raise ValueError(f"configured OI dataset not found: {dataset_id}")
    return read_rows_from_ref(
        ref,
        workspace_root=workspace_root,
        start=start,
        end=end,
    )


def _signal_discovery_artifact_root(
    session: dict[str, Any],
    *,
    workspace_root: Path,
) -> Path:
    root = Path(session["artifact_root"])
    return root if root.is_absolute() else workspace_root / root


def _fail_signal_discovery_session(
    repository: Any,
    *,
    session: dict[str, Any],
    exc: Exception,
) -> None:
    try:
        repository.update_signal_discovery_session(
            session["session_id"],
            status="failed",
            summary={
                **(session.get("summary") or {}),
                "last_error": {
                    "message": str(exc),
                    "type": exc.__class__.__name__,
                },
            },
        )
    except Exception:
        pass


def _run_stage0_candidate(
    *,
    repository: Any,
    market_data_repository: Any,
    workspace_root: Path,
    universe_run: dict[str, Any],
    candidate: dict[str, Any],
    label_mode: str = "threshold_first_hit",
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
        label_mode=label_mode,
    )


def _run_stage0_information_candidate(
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
    return execute_stage0_information_gate(
        workspace_root=workspace_root,
        universe_run={**universe_run, "window_start": window_start, "window_end": window_end},
        candidate=candidate,
        signal_set=signal_set,
        signals=signals,
        candle_rows=candle_rows,
    )


def _refresh_stage0_information_q_values(repository: Any, universe_run_id: str) -> None:
    candidates = repository.list_stage0_universe_candidates(universe_run_id)
    adjusted = apply_information_q_values_to_candidates(candidates)
    for before, after in zip(candidates, adjusted, strict=False):
        if before != after:
            repository.update_stage0_universe_candidate(after)


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
    get_source_run = getattr(repository, "get_stage0_universe_run", None)
    source_run_id = session.get("source_universe_run_id")
    source_run = (
        get_source_run(source_run_id)
        if callable(get_source_run) and source_run_id
        else None
    )
    forward_hours = int(source_run["forward_hours"]) if source_run is not None else 36
    end = _add_hours(
        f"{_date_string(session['walk_forward_end'])}T23:59:59Z",
        forward_hours,
    )
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
