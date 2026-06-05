from __future__ import annotations

import os
import shutil
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_api.services.market_data_catalog import (
    build_catalog,
    build_refresh_plan,
    read_parquet_candles,
)
from quant_terminal_sdk.agent_tasks import AgentTaskBundle
from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.adapters.okx import OKXAdapter, OKXCLIError
from quant_terminal_worker.backtests.stage1 import run_stage1_backtest
from quant_terminal_worker.ingestion.raw_candle_fill import fill_raw_candle_dataset
from quant_terminal_worker.ingestion.legacy_signals import import_legacy_signal_sets
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles
from quant_terminal_worker.stage0.workspace import build_stage0_commands
from quant_terminal_worker.stage0.workspace import read_parquet_candles_for_stage0
from quant_terminal_worker.stage0.execution import execute_stage0_candidate
from quant_terminal_worker.stage0.universe import (
    build_stage0_universe,
    build_stage0_universe_config_hash,
)
from quant_terminal_worker.stage1.workspace import materialize_stage1_session_workspace
from quant_terminal_worker.stage1.workspace import create_stage1_iteration_workspace
from quant_terminal_worker.stage1.workspace import build_stage1_gate_summary
from quant_terminal_worker.stage1.workspace import list_stage1_iterations
from quant_terminal_worker.stage1.scoring import run_stage1a_training_score
from quant_terminal_worker.stage1.scoring import run_stage1a_score
from quant_terminal_worker.stage1.scoring import run_stage1a_canonical_full_cycle
from quant_terminal_worker.stage1.scoring import generate_stage1a_failure_audit
from quant_terminal_worker.stage2.capture_curve import run_stage2_capture_curve
from quant_terminal_worker.stage3.grid_search import run_stage3_grid_search
from quant_terminal_worker.stage3.pyramid import run_stage3_pyramid
from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy


STAGE1_ROLE_ACTIONS = {
    "training": ("create_training_bundle", "Create Training Bundle"),
    "walk_forward_test": ("create_walk_forward_bundle", "Create Walk-Forward Test Bundle"),
}


class AgentTaskPreviewRequest(BaseModel):
    task_id: str
    cycle_id: str
    stage: str
    strategy_id: str
    strategy_version: str
    allowed_context_paths: list[str] = Field(default_factory=list)
    forbidden_context_paths: list[str] = Field(default_factory=list)


class SignalEngineRegistrationRequest(BaseModel):
    signal_engine_id: str
    name: str
    version: str
    runtime_entrypoint: str
    description: str = ""
    code_ref: dict[str, Any] = Field(default_factory=dict)
    supported_input_data_types: list[str] = Field(default_factory=lambda: ["candles"])
    output_envelope_version: str = "signal_envelope.v1"
    live_scanner_entrypoint: str | None = None
    configuration_schema: dict[str, Any] = Field(default_factory=dict)


class StrategyRegistrationRequest(BaseModel):
    strategy_id: str
    name: str
    version: str
    runtime_entrypoint: str
    description: str = ""
    code_ref: dict[str, Any] = Field(default_factory=dict)
    supported_signal_engine_ids: list[str] = Field(default_factory=list)
    parameter_schema: dict[str, Any] = Field(default_factory=dict)
    decision_schema: dict[str, Any] = Field(default_factory=dict)
    execution_profile_schema: dict[str, Any] = Field(default_factory=dict)
    test_suite_status: str = "unknown"


class Stage1BacktestRequest(BaseModel):
    run_id: str
    asset: str
    instrument: str
    dataset_refs: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]]
    signal_engine: dict[str, Any]
    strategy: dict[str, Any]
    ground_truth: dict[str, str] = Field(default_factory=dict)
    template_id: str = "ad_hoc"
    parameters_hash: str = "unhashed"


class Stage1SessionCreateRequest(BaseModel):
    source_candidate_id: str
    strategy_id: str
    strategy_version: str
    train_start: str | None = None
    train_end: str | None = None
    walk_forward_start: str | None = None
    walk_forward_end: str | None = None


class Stage1IterationCreateRequest(BaseModel):
    sample_method: str = "training"
    bundle_role: str = "strategy_builder"


class LegacySignalImportRequest(BaseModel):
    root: str = "dev/signals/vegas_ema"
    limit: int | None = None


class SignalPoolExtendRequest(BaseModel):
    target_end: str | None = None


class Stage0RunRequest(BaseModel):
    run_id: str
    strategy_id: str
    strategy_version: str
    signal_set_key: str
    forward_hours: int = 36
    significance_threshold_pct: float = 0.9


class Stage0UniverseRunRequest(BaseModel):
    universe_run_id: str
    train_start: str
    train_end: str
    walk_forward_start: str
    walk_forward_end: str
    forward_hours: int = 36
    trigger_rate_threshold_pct: float = 85
    engine_ids: list[str] = Field(default_factory=list)
    assets: list[str] = Field(default_factory=list)


class ExecuteStage0CandidateRequest(BaseModel):
    candidate_id: str


class ExecuteStage0CandidateBatchRequest(BaseModel):
    limit: int = Field(default=500, ge=1, le=1000)


DEFAULT_WALK_FORWARD_TEMPLATES: list[dict[str, Any]] = [
    {
        "template_id": "rolling_90d_14d_14d_weekly",
        "anchor": "rolling",
        "retrain_cadence": "7d",
        "train_range": "90d",
        "walk_forward_range": "14d",
        "embargo": "0d",
    }
]


def create_app(
    market_data_repository: Any | None = None,
    market_data_fill_service: Any | None = None,
    runtime_repository: Any | None = None,
    stage0_executor: Any | None = None,
    signal_pool_extension_service: Any | None = None,
) -> FastAPI:
    app = FastAPI(title="Motis Deterministic Quant Terminal", version="0.1.0")
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r"^http://(127\.0\.0\.1|localhost):51[0-9]{2}$",
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    repository = market_data_repository
    fill_service = market_data_fill_service
    runtime_repo = runtime_repository
    injected_stage0_executor = stage0_executor
    signal_pool_extender = signal_pool_extension_service

    def get_market_data_repository() -> Any:
        nonlocal repository
        if repository is None:
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
            repository = PostgresMarketDataRepository(database_url)
        return repository

    def get_runtime_repository() -> Any:
        nonlocal runtime_repo
        if runtime_repo is None:
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise HTTPException(status_code=503, detail="DATABASE_URL is not configured")
            runtime_repo = RuntimeRepository(database_url)
        return runtime_repo

    def run_stage0_candidate(universe_run: dict[str, Any], candidate: dict[str, Any]) -> dict[str, Any]:
        if injected_stage0_executor is not None:
            return injected_stage0_executor(universe_run, candidate)
        signal_set = get_runtime_repository().get_signal_set(candidate["signal_set_key"])
        if signal_set is None:
            raise HTTPException(status_code=404, detail="signal set not found")
        candle_ref = get_market_data_repository().get_raw_candle_ref(candidate["asset"], "5m")
        if candle_ref is None:
            raise HTTPException(status_code=404, detail="raw 5m candle data not found")
        signals = get_runtime_repository().list_signals_for_signal_set_window(
            signal_set_key=candidate["signal_set_key"],
            window_start=_iso_datetime(universe_run["window_start"]),
            window_end=_iso_datetime(universe_run["window_end"]),
        )
        candle_rows = read_parquet_candles_for_stage0(
            storage_uri=Path(candle_ref["storage_uri"]),
            window_start=_iso_datetime(universe_run["window_start"]),
            window_end=_iso_datetime(universe_run["window_end"]),
            forward_hours=universe_run["forward_hours"],
        )
        if not signals:
            raise HTTPException(status_code=400, detail="candidate has no signal packets in window")
        if not candle_rows:
            raise HTTPException(status_code=400, detail="candidate has no candle rows for window")
        return execute_stage0_candidate(
            workspace_root=Path.cwd(),
            universe_run={
                **universe_run,
                "window_start": _iso_datetime(universe_run["window_start"]),
                "window_end": _iso_datetime(universe_run["window_end"]),
            },
            candidate=candidate,
            signal_set=signal_set,
            signals=signals,
            candle_rows=candle_rows,
        )

    @app.get("/api/v1/health")
    def health() -> dict[str, Any]:
        return {
            "status": "ok",
            "services": {
                "api": "ready",
                "database": "configured",
                "worker": "configured",
            },
        }

    @app.get("/api/v1/walk-forward/templates")
    def list_walk_forward_templates() -> dict[str, Any]:
        return {"templates": DEFAULT_WALK_FORWARD_TEMPLATES}

    @app.post("/api/v1/agent-tasks/preview")
    def preview_agent_task(request: AgentTaskPreviewRequest) -> dict[str, str]:
        bundle = AgentTaskBundle(
            task_id=request.task_id,
            cycle_id=request.cycle_id,
            stage=request.stage,
            strategy_id=request.strategy_id,
            strategy_version=request.strategy_version,
            allowed_context_paths=request.allowed_context_paths,
            forbidden_context_paths=request.forbidden_context_paths,
        )
        return {"prompt": bundle.render_prompt(repo_root=Path.cwd())}

    @app.post("/api/v1/signal-engines/register")
    def register_signal_engine(request: SignalEngineRegistrationRequest) -> dict[str, str]:
        get_runtime_repository().register_signal_engine(request.model_dump())
        return {"status": "registered", "signal_engine_id": request.signal_engine_id}

    @app.get("/api/v1/signal-engines")
    def list_signal_engines() -> dict[str, Any]:
        return {"engines": get_runtime_repository().list_signal_engines()}

    @app.get("/api/v1/signal-engines/{signal_engine_id}/signal-sets")
    def list_signal_sets(signal_engine_id: str) -> dict[str, Any]:
        return {"signal_sets": get_runtime_repository().list_signal_sets(signal_engine_id)}

    @app.get("/api/v1/signals")
    def list_signals(
        signal_set_key: str | None = None,
        signal_engine_id: str | None = None,
        asset: str | None = None,
        limit: int = 25,
    ) -> dict[str, Any]:
        return {
            "signals": get_runtime_repository().list_signals(
                signal_set_key=signal_set_key,
                signal_engine_id=signal_engine_id,
                asset=asset,
                limit=min(limit, 200),
            )
        }

    @app.post("/api/v1/signal-engines/{signal_engine_id}/signal-sets/{asset}/extend-local")
    def extend_signal_set_from_local_candles(
        signal_engine_id: str,
        asset: str,
        request: SignalPoolExtendRequest,
    ) -> dict[str, Any]:
        service = signal_pool_extender or extend_signal_pool_from_local_candles
        try:
            return service(
                workspace_root=Path.cwd(),
                repository=get_runtime_repository(),
                signal_engine_id=signal_engine_id,
                asset=asset,
                target_end=request.target_end,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @app.post("/api/v1/research/stage0-runs")
    def create_stage0_run(request: Stage0RunRequest) -> dict[str, Any]:
        signal_set = get_runtime_repository().get_signal_set(request.signal_set_key)
        if signal_set is None:
            raise HTTPException(status_code=404, detail="signal set not found")
        workspace_root = Path.cwd()
        strategy_id = request.strategy_id
        signal_set_id = signal_set["signal_set_id"]
        stage0_dir = workspace_root / "dev" / "training_sessions" / strategy_id / "stage0" / signal_set_id
        vote_threshold = int(signal_set.get("manifest", {}).get("parameters", {}).get("vote_threshold", 0))
        commands = build_stage0_commands(
            workspace_root=workspace_root,
            strategy_id=strategy_id,
            asset=signal_set["asset"],
            signal_engine_id=signal_set["signal_engine_id"],
            signal_set_id=signal_set_id,
            signal_packets_dir=str(
                workspace_root
                / "dev"
                / "signals"
                / signal_set["signal_engine_id"]
                / signal_set["asset"]
                / signal_set_id
                / "packets"
            ),
            candles_csv=str(workspace_root / "dev" / "data" / "raw" / signal_set["asset"] / "5m" / "candles.csv"),
            forward_hours=request.forward_hours,
            vote_threshold=vote_threshold,
            significance_threshold_pct=request.significance_threshold_pct,
        )
        run = {
            "run_id": request.run_id,
            "stage": "stage0",
            "strategy_id": strategy_id,
            "strategy_version": request.strategy_version,
            "signal_engine_id": signal_set["signal_engine_id"],
            "signal_engine_version": signal_set["signal_engine_version"],
            "signal_set_key": request.signal_set_key,
            "asset": signal_set["asset"],
            "forward_hours": request.forward_hours,
            "significance_threshold_pct": request.significance_threshold_pct,
            "artifact_root": str(stage0_dir),
            "commands": commands,
            "status": "created",
            "metrics": {},
        }
        get_runtime_repository().create_strategy_development_run(run)
        return run

    @app.get("/api/v1/research/runs")
    def list_research_runs() -> dict[str, Any]:
        return {"runs": get_runtime_repository().list_strategy_development_runs()}

    @app.get("/api/v1/research/stage1-sessions")
    def list_stage1_research_sessions() -> dict[str, Any]:
        return {"sessions": get_runtime_repository().list_stage1_research_sessions()}

    @app.post("/api/v1/research/stage1-sessions")
    def create_stage1_research_session(request: Stage1SessionCreateRequest) -> dict[str, Any]:
        candidate = get_runtime_repository().get_stage0_universe_candidate(request.source_candidate_id)
        if candidate is None:
            raise HTTPException(status_code=404, detail="Stage 0 candidate not found")
        if candidate["acceptance_status"] != "accepted":
            raise HTTPException(
                status_code=400,
                detail="Stage 1 sessions require an accepted Stage 0 candidate",
            )

        source_universe_run = get_runtime_repository().get_stage0_universe_run(candidate["universe_run_id"])
        stage1_windows = _stage1_windows_for_batch(source_universe_run, request)
        session_id = _stage1_session_id(
            strategy_id=request.strategy_id,
            asset=candidate["asset"],
            train_start=stage1_windows["train_start"],
            walk_forward_end=stage1_windows["walk_forward_end"],
            source_candidate_id=candidate["candidate_id"],
        )
        artifact_root = Path.cwd() / "dev" / "training_sessions" / request.strategy_id / session_id
        seed_strategy = _resolve_stage1_seed_strategy(
            repository=get_runtime_repository(),
            candidate=candidate,
            strategy_id=request.strategy_id,
        )
        manifest = {
            "schema_version": "0.1",
            "session_id": session_id,
            "asset": candidate["asset"],
            "strategy_id": request.strategy_id,
            "strategy_version": request.strategy_version,
            "signal_engine_id": candidate["signal_engine_id"],
            "signal_family": candidate["signal_engine_id"],
            "signal_set_id": candidate["signal_set_id"],
            "signal_set_key": candidate["signal_set_key"],
            "stage": "stage1a_directional_agreement",
            "status": "draft",
            "stage0_universe_run_id": candidate["universe_run_id"],
            "stage0_candidate_id": candidate["candidate_id"],
            "stage0_artifact_root": candidate.get("metrics", {}).get("artifact_root"),
            "train_window": {"start": stage1_windows["train_start"], "end": stage1_windows["train_end"]},
            "walk_forward_window": {"start": stage1_windows["walk_forward_start"], "end": stage1_windows["walk_forward_end"]},
            "inputs": {},
            "outputs": {},
            "scoring": {},
            "seed_strategy": seed_strategy,
        }
        session = {
            "session_id": session_id,
            "source_universe_run_id": candidate["universe_run_id"],
            "source_candidate_id": candidate["candidate_id"],
            "signal_set_key": candidate["signal_set_key"],
            "signal_engine_id": candidate["signal_engine_id"],
            "signal_engine_version": candidate["signal_engine_version"],
            "asset": candidate["asset"],
            "signal_set_id": candidate["signal_set_id"],
            "strategy_id": request.strategy_id,
            "strategy_version": request.strategy_version,
            "train_start": stage1_windows["train_start"],
            "train_end": stage1_windows["train_end"],
            "walk_forward_start": stage1_windows["walk_forward_start"],
            "walk_forward_end": stage1_windows["walk_forward_end"],
            "artifact_root": str(artifact_root),
            "status": "draft",
            "manifest": manifest,
            "seed_strategy_source_type": seed_strategy["source_type"],
            "seed_strategy_source_path": seed_strategy.get("source_path"),
            "seed_strategy_source_version": seed_strategy.get("source_version"),
            "seed_strategy_source_session_id": seed_strategy.get("source_session_id"),
        }
        session["manifest"]["strategy_entrypoint"] = "strategy_module.strategy:decide"
        session["manifest"]["strategy_path"] = str(artifact_root / "strategy_module" / "strategy.py")
        session["manifest"]["artifact_root"] = str(artifact_root)
        materialize_stage1_session_workspace(workspace_root=Path.cwd(), session=session)
        get_runtime_repository().create_stage1_research_session(session)
        return {"session": session}

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations")
    def create_stage1_research_iteration(
        session_id: str,
        request: Stage1IterationCreateRequest,
    ) -> dict[str, Any]:
        if request.bundle_role not in {"strategy_builder", "evaluator"}:
            raise HTTPException(status_code=400, detail="bundle_role must be strategy_builder or evaluator")
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_mutable(session)
        window_start, window_end = _stage1_sample_window(session, request.sample_method)
        signals = get_runtime_repository().list_signals_for_signal_set_window(
            signal_set_key=session["signal_set_key"],
            window_start=f"{window_start}T00:00:00Z",
            window_end=f"{window_end}T23:59:59Z",
        )
        if not signals:
            sample_label = {
                "training": "training",
                "walk_forward_test": "walk-forward test",
            }.get(request.sample_method, request.sample_method.replace("_", " "))
            raise HTTPException(
                status_code=400,
                detail=f"No {sample_label} signals found for Stage 1 session between {window_start} and {window_end}",
            )
        try:
            iteration = create_stage1_iteration_workspace(
                workspace_root=Path.cwd(),
                session={
                    **session,
                    "train_start": _date_string(session["train_start"]),
                    "train_end": _date_string(session["train_end"]),
                    "walk_forward_start": _date_string(session["walk_forward_start"]),
                    "walk_forward_end": _date_string(session["walk_forward_end"]),
                },
                signals=signals,
                sample_method=request.sample_method,
                bundle_role=request.bundle_role,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "iteration": _relative_iteration_paths(Path.cwd(), iteration),
        }

    @app.get("/api/v1/research/stage1-sessions/{session_id}/iterations")
    def list_stage1_research_iterations(session_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        iterations = list_stage1_iterations(workspace_root=Path.cwd(), session=session)
        return {"iterations": [_relative_nested_paths(Path.cwd(), iteration) for iteration in iterations]}

    @app.delete("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}")
    def delete_stage1_research_iteration(session_id: str, iteration_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_mutable(session)
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        iterations_root = (artifact_root / "iterations").resolve()
        iteration_root = (iterations_root / iteration_id).resolve()
        if iteration_root.parent != iterations_root or not iteration_root.name.startswith("iter_"):
            raise HTTPException(status_code=400, detail="Invalid Stage 1 iteration id")
        if not iteration_root.is_dir():
            raise HTTPException(status_code=404, detail="Stage 1 iteration not found")
        shutil.rmtree(iteration_root)
        return {"status": "deleted", "session_id": session_id, "iteration_id": iteration_id}

    @app.get("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/agent-prompt")
    def get_stage1_research_iteration_agent_prompt(session_id: str, iteration_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_mutable(session)
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        iterations_root = (artifact_root / "iterations").resolve()
        iteration_root = (iterations_root / iteration_id).resolve()
        if iteration_root.parent != iterations_root or not iteration_root.name.startswith("iter_"):
            raise HTTPException(status_code=400, detail="Invalid Stage 1 iteration id")
        if not iteration_root.is_dir():
            raise HTTPException(status_code=404, detail="Stage 1 iteration not found")
        prompt_candidates = [
            ("failure_audit", iteration_root / "agent_failure_audit_prompt.md"),
            ("strategy_builder", iteration_root / "strategy_builder_prompt.md"),
            ("iteration_handoff", iteration_root / "agent_prompt.md"),
        ]
        for prompt_type, prompt_path in prompt_candidates:
            if prompt_path.is_file():
                return {
                    "session_id": session_id,
                    "iteration_id": iteration_id,
                    "prompt_type": prompt_type,
                    "prompt_path": str(prompt_path.relative_to(Path.cwd())),
                    "prompt": prompt_path.read_text(),
                }
        raise HTTPException(status_code=404, detail="Stage 1 agent prompt not found")

    @app.get("/api/v1/research/stage1-sessions/{session_id}/gate")
    def get_stage1_gate(session_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        return {"gate": _relative_nested_paths(Path.cwd(), gate)}

    def _stage1_full_cycle_signals(session: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        signals_by_role = {}
        for sample_role in ("training", "walk_forward_test"):
            window_start, window_end = _stage1_sample_window(session, sample_role)
            signals_by_role[sample_role] = get_runtime_repository().list_signals_for_signal_set_window(
                signal_set_key=session["signal_set_key"],
                window_start=f"{window_start}T00:00:00Z",
                window_end=f"{window_end}T23:59:59Z",
            )
        return signals_by_role

    @app.post("/api/v1/research/stage1-sessions/{session_id}/canonical-stage1a")
    def run_stage1_canonical_readout(session_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not gate["ready_to_freeze"]:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Stage 1A canonical readout requires passing training and walk-forward test scores.",
                    "blockers": gate["blockers"],
                },
            )
        try:
            result = run_stage1a_canonical_full_cycle(
                workspace_root=Path.cwd(),
                session=session,
                signals_by_role=_stage1_full_cycle_signals(session),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        frozen_manifest = {
            **(session.get("manifest") or {}),
            "status": "stage1a_frozen",
            "stage1a_canonical_readout": _relative_nested_paths(Path.cwd(), result),
        }
        updater = getattr(get_runtime_repository(), "update_stage1_research_session_state", None)
        if callable(updater):
            updater(session_id=session_id, status="stage1a_frozen", manifest=frozen_manifest)
        return {
            "canonical_readout": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session={**session, "status": "stage1a_frozen"}),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage2/capture-curve")
    def run_stage2_capture_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if session.get("status") != "stage1a_frozen" or not (gate.get("canonical_readout") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 2 requires a frozen canonical Stage 1A readout")
        try:
            result = run_stage2_capture_curve(
                workspace_root=Path.cwd(),
                session=session,
                signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(session)),
                candles=_stage2_raw_candles(session, repository=repository),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage2_capture": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/grid-search")
    def run_stage3_grid_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage2_capture") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 3 requires completed Stage 2 travel capture")
        try:
            result = run_stage3_grid_search(
                workspace_root=Path.cwd(),
                session=session,
                candles=_stage2_raw_candles(session, repository=repository),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage3_grid": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/pyramid")
    def run_stage3_pyramid_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage3_grid") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 3 pyramid requires completed Stage 3 grid search")
        try:
            result = run_stage3_pyramid(
                workspace_root=Path.cwd(),
                session=session,
                candles=_stage2_raw_candles(session, repository=repository),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage3_pyramid": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage4/realized-expectancy")
    def run_stage4_realized_expectancy_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage3_pyramid") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 4 requires completed Stage 3 pyramid")
        try:
            result = run_stage4_realized_expectancy(
                workspace_root=Path.cwd(),
                session=session,
                signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(session)),
                candles=_stage2_raw_candles(session, repository=repository),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage4_realized_expectancy": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/score-training")
    def score_stage1_training_iteration(session_id: str, iteration_id: str) -> dict[str, Any]:
        return _score_stage1_iteration(session_id=session_id, iteration_id=iteration_id, sample_role="training")

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/score-walk-forward")
    def score_stage1_walk_forward_iteration(session_id: str, iteration_id: str) -> dict[str, Any]:
        return _score_stage1_iteration(session_id=session_id, iteration_id=iteration_id, sample_role="walk_forward_test")

    def _score_stage1_iteration(*, session_id: str, iteration_id: str, sample_role: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_mutable(session)
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        iteration_root = artifact_root / "iterations" / iteration_id
        if not iteration_root.is_dir():
            raise HTTPException(status_code=404, detail="Stage 1 iteration not found")
        try:
            score = run_stage1a_score(iteration_root=iteration_root, sample_role=sample_role)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"score": _relative_score_paths(Path.cwd(), score)}

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/generate-failure-audit")
    def generate_stage1_failure_audit(
        session_id: str,
        iteration_id: str,
        sample_role: str = "training",
    ) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        iteration_root = artifact_root / "iterations" / iteration_id
        if not iteration_root.is_dir():
            raise HTTPException(status_code=404, detail="Stage 1 iteration not found")
        try:
            audit = generate_stage1a_failure_audit(iteration_root=iteration_root, sample_role=sample_role)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"audit": _relative_audit_paths(Path.cwd(), audit)}

    @app.post("/api/v1/research/stage0-universe-runs")
    def create_stage0_universe_run(request: Stage0UniverseRunRequest) -> dict[str, Any]:
        if get_runtime_repository().get_stage0_universe_run(request.universe_run_id):
            raise HTTPException(status_code=409, detail="stage0 universe run id already exists")
        _validate_two_window_dates(
            train_start=request.train_start,
            train_end=request.train_end,
            walk_forward_start=request.walk_forward_start,
            walk_forward_end=request.walk_forward_end,
        )
        window_start = f"{request.train_start}T00:00:00Z"
        window_end = f"{request.walk_forward_end}T23:59:59Z"
        config_hash = build_stage0_universe_config_hash(
            window_start=window_start,
            window_end=window_end,
            forward_hours=request.forward_hours,
            trigger_rate_threshold_pct=request.trigger_rate_threshold_pct,
            train_start=request.train_start,
            train_end=request.train_end,
            walk_forward_start=request.walk_forward_start,
            walk_forward_end=request.walk_forward_end,
            engine_ids=request.engine_ids,
            asset_symbols=request.assets,
        )
        signal_sets_for_engines: list[dict[str, Any]] = []
        if request.engine_ids:
            for engine_id in request.engine_ids:
                signal_sets_for_engines.extend(get_runtime_repository().list_signal_sets(engine_id))
        else:
            signal_sets_for_engines = get_runtime_repository().list_signal_sets()
        universe = build_stage0_universe(
            universe_run_id=request.universe_run_id,
            window_start=window_start,
            window_end=window_end,
            forward_hours=request.forward_hours,
            trigger_rate_threshold_pct=request.trigger_rate_threshold_pct,
            train_start=request.train_start,
            train_end=request.train_end,
            walk_forward_start=request.walk_forward_start,
            walk_forward_end=request.walk_forward_end,
            signal_sets=signal_sets_for_engines,
            asset_symbols=request.assets,
            metrics_by_signal_set=get_runtime_repository().stage0_metrics_by_signal_set(),
            existing_rnd_by_signal_set=get_runtime_repository().existing_rnd_by_signal_set(),
            signal_counts_by_signal_set=get_runtime_repository().signal_counts_by_signal_set_window(
                window_start=window_start,
                window_end=window_end,
                engine_ids=request.engine_ids,
            ),
            split_signal_counts_by_signal_set=get_runtime_repository().split_signal_counts_by_signal_set(
                train_start=request.train_start,
                train_end=request.train_end,
                walk_forward_start=request.walk_forward_start,
                walk_forward_end=request.walk_forward_end,
                engine_ids=request.engine_ids,
            ),
            engine_ids=request.engine_ids,
        )
        get_runtime_repository().create_stage0_universe(
            universe["run"],
            universe["candidates"],
        )
        return universe

    @app.get("/api/v1/research/stage0-universe-runs")
    def list_stage0_universe_runs() -> dict[str, Any]:
        return {"runs": get_runtime_repository().list_stage0_universe_runs()}

    @app.get("/api/v1/research/stage0-universe-runs/{universe_run_id}/candidates")
    def list_stage0_universe_candidates(universe_run_id: str) -> dict[str, Any]:
        return {"candidates": get_runtime_repository().list_stage0_universe_candidates(universe_run_id)}

    @app.get("/api/v1/research/cycles/{universe_run_id}/development-queue")
    def get_development_queue(universe_run_id: str) -> dict[str, Any]:
        universe_run = get_runtime_repository().get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        candidates = get_runtime_repository().list_stage0_universe_candidates(universe_run_id)
        sessions = [
            session
            for session in get_runtime_repository().list_stage1_research_sessions()
            if session.get("source_universe_run_id") == universe_run_id
        ]
        queue = _build_development_queue(
            workspace_root=Path.cwd(),
            universe_run_id=universe_run_id,
            candidates=candidates,
            stage1_sessions=sessions,
        )
        return {"universe_run": universe_run, "queue": _relative_nested_paths(Path.cwd(), queue)}

    @app.post("/api/v1/research/stage0-universe-runs/{universe_run_id}/candidates/execute")
    def execute_stage0_universe_candidate(
        universe_run_id: str,
        request: ExecuteStage0CandidateRequest,
    ) -> dict[str, Any]:
        universe_run = get_runtime_repository().get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        candidate = get_runtime_repository().get_stage0_universe_candidate(request.candidate_id)
        if candidate is None:
            raise HTTPException(status_code=404, detail="stage0 universe candidate not found")

        result = run_stage0_candidate(universe_run, candidate)
        get_runtime_repository().update_stage0_universe_candidate(result["candidate"])
        get_runtime_repository().refresh_stage0_universe_summary(universe_run_id)
        return result

    @app.post("/api/v1/research/stage0-universe-runs/{universe_run_id}/candidates/execute-batch")
    def execute_stage0_universe_candidate_batch(
        universe_run_id: str,
        request: ExecuteStage0CandidateBatchRequest,
    ) -> dict[str, Any]:
        universe_run = get_runtime_repository().get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")

        all_candidates = get_runtime_repository().list_stage0_universe_candidates(universe_run_id)
        pending_candidates = [
            candidate for candidate in all_candidates if candidate["acceptance_status"] == "pending_stage0"
        ]
        selected_candidates = pending_candidates[: request.limit]
        results: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        for candidate in selected_candidates:
            try:
                result = run_stage0_candidate(universe_run, candidate)
                get_runtime_repository().update_stage0_universe_candidate(result["candidate"])
                results.append(result)
            except HTTPException as exc:
                errors.append(
                    {
                        "candidate_id": candidate["candidate_id"],
                        "asset": candidate["asset"],
                        "detail": exc.detail,
                    }
                )
                get_runtime_repository().mark_stage0_universe_candidate_error(
                    candidate["candidate_id"],
                    {
                        "detail": exc.detail,
                        "type": "http_error",
                    },
                )
            except Exception as exc:  # pragma: no cover - defensive API boundary
                errors.append(
                    {
                        "candidate_id": candidate["candidate_id"],
                        "asset": candidate["asset"],
                        "detail": str(exc),
                    }
                )
                get_runtime_repository().mark_stage0_universe_candidate_error(
                    candidate["candidate_id"],
                    {
                        "detail": str(exc),
                        "type": exc.__class__.__name__,
                    },
                )

        get_runtime_repository().refresh_stage0_universe_summary(universe_run_id)
        refreshed_run = get_runtime_repository().get_stage0_universe_run(universe_run_id) or universe_run
        refreshed_candidates = get_runtime_repository().list_stage0_universe_candidates(universe_run_id)
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
                "remaining_pending": sum(
                    1
                    for candidate in refreshed_candidates
                    if candidate["acceptance_status"] == "pending_stage0"
                ),
            },
        }

    @app.post("/api/v1/research/stage0-universe-runs/{universe_run_id}/supersede")
    def supersede_stage0_universe_run(universe_run_id: str) -> dict[str, Any]:
        universe_run = get_runtime_repository().get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        get_runtime_repository().supersede_stage0_universe_run(universe_run_id)
        return {"run": get_runtime_repository().get_stage0_universe_run(universe_run_id)}

    @app.delete("/api/v1/research/stage0-universe-runs/{universe_run_id}")
    def delete_stage0_universe_run(universe_run_id: str) -> dict[str, Any]:
        universe_run = get_runtime_repository().get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        linked_sessions = [
            session
            for session in get_runtime_repository().list_stage1_research_sessions()
            if session.get("source_universe_run_id") == universe_run_id
        ]
        linked_session_ids = [session["session_id"] for session in linked_sessions]
        get_runtime_repository().delete_stage0_universe_run(universe_run_id)
        return {
            "status": "deleted",
            "universe_run_id": universe_run_id,
            "deleted_stage1_session_count": len(linked_session_ids),
            "deleted_stage1_session_ids": linked_session_ids,
        }

    @app.post("/api/v1/signals/import/legacy")
    def import_legacy_signals(request: LegacySignalImportRequest) -> dict[str, Any]:
        root = Path(request.root)
        if not root.exists():
            raise HTTPException(status_code=404, detail="legacy signal root not found")
        return import_legacy_signal_sets(
            root=root,
            repository=get_runtime_repository(),
            limit=request.limit,
        )

    @app.post("/api/v1/strategies/register")
    def register_strategy(request: StrategyRegistrationRequest) -> dict[str, str]:
        get_runtime_repository().register_strategy(request.model_dump())
        return {"status": "registered", "strategy_id": request.strategy_id}

    @app.post("/api/v1/backtests/stage1")
    def launch_stage1_backtest(request: Stage1BacktestRequest) -> dict[str, Any]:
        result = run_stage1_backtest(request.model_dump())
        result["asset"] = request.asset
        result["instrument"] = request.instrument
        result["template_id"] = request.template_id
        result["parameters_hash"] = request.parameters_hash
        get_runtime_repository().persist_stage1_backtest(result)
        return result

    @app.get("/api/v1/backtests/{run_id}")
    def get_backtest(run_id: str) -> dict[str, Any]:
        result = get_runtime_repository().get_backtest_run(run_id)
        if result is None:
            raise HTTPException(status_code=404, detail="backtest run not found")
        return result

    @app.get("/api/v1/market-data/catalog")
    def market_data_catalog() -> dict[str, Any]:
        return build_catalog(get_market_data_repository().list_refs())

    @app.get("/api/v1/market-data/{dataset_id}/candles")
    def read_market_data_candles(dataset_id: str, limit: int = 200) -> dict[str, Any]:
        registration = get_market_data_repository().get_ref(dataset_id)
        if registration is None:
            raise HTTPException(status_code=404, detail="dataset not found")
        if registration["data_type"] != "candles":
            raise HTTPException(status_code=400, detail="dataset is not candles")
        return {
            "dataset_id": dataset_id,
            "rows": read_parquet_candles(Path(registration["storage_uri"]), limit=limit),
        }

    @app.post("/api/v1/market-data/{dataset_id}/refresh")
    def refresh_market_data(dataset_id: str) -> dict[str, Any]:
        registration = get_market_data_repository().get_ref(dataset_id)
        if registration is None:
            raise HTTPException(status_code=404, detail="dataset not found")
        plan = build_refresh_plan(registration)
        if plan["status"] == "blocked":
            return plan
        service = fill_service or fill_raw_candle_dataset
        try:
            return service(
                registration=registration,
                repository=get_market_data_repository(),
                adapter=OKXAdapter({"backend": "okx_cli", "mode": os.environ.get("OKX_MODE", "demo")}),
            )
        except OKXCLIError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    return app


app = create_app()


def _iso_datetime(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat().replace("+00:00", "Z")
    return str(value)


def _stage1_session_id(
    *,
    strategy_id: str,
    asset: str,
    train_start: str,
    walk_forward_end: str,
    source_candidate_id: str,
) -> str:
    return f"stage1-{strategy_id}-{asset.lower()}-{train_start}-{walk_forward_end}-{_stage1_candidate_slug(source_candidate_id)}"


def _stage1_candidate_slug(source_candidate_id: str) -> str:
    tail = source_candidate_id.split(":")[0]
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", tail).strip("-").lower()
    return slug[-48:] or "candidate"


def _stage1_windows_for_batch(
    universe_run: dict[str, Any] | None,
    request: Stage1SessionCreateRequest,
) -> dict[str, str]:
    split_keys = (
        "train_start",
        "train_end",
        "walk_forward_start",
        "walk_forward_end",
    )
    if universe_run and all(universe_run.get(key) is not None for key in split_keys):
        return {key: _date_string(universe_run[key]) for key in split_keys}

    request_values = {key: getattr(request, key) for key in split_keys}
    missing = [key for key, value in request_values.items() if value is None]
    if missing:
        raise HTTPException(
            status_code=400,
            detail={
                "message": "Stage 1 requires training and walk-forward windows. New sessions should define them on the Stage 0 batch.",
                "missing": missing,
            },
        )
    windows = {key: str(value) for key, value in request_values.items()}
    _validate_two_window_dates(**windows)
    return windows


def _validate_two_window_dates(
    *,
    train_start: str,
    train_end: str,
    walk_forward_start: str,
    walk_forward_end: str,
) -> None:
    parsed = {
        "train_start": date.fromisoformat(train_start),
        "train_end": date.fromisoformat(train_end),
        "walk_forward_start": date.fromisoformat(walk_forward_start),
        "walk_forward_end": date.fromisoformat(walk_forward_end),
    }
    if parsed["train_start"] > parsed["train_end"]:
        raise HTTPException(status_code=400, detail="Training start must be on or before training end")
    if parsed["walk_forward_start"] > parsed["walk_forward_end"]:
        raise HTTPException(status_code=400, detail="Walk-forward start must be on or before walk-forward end")
    if parsed["train_end"] >= parsed["walk_forward_start"]:
        raise HTTPException(status_code=400, detail="Training window must end before walk-forward window starts")


def _date_string(value: Any) -> str:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def _resolve_stage1_seed_strategy(
    *,
    repository: Any,
    candidate: dict[str, Any],
    strategy_id: str,
) -> dict[str, Any]:
    latest_seed = _latest_pair_seed(
        repository=repository,
        asset=candidate["asset"],
        signal_engine_id=candidate["signal_engine_id"],
        strategy_id=strategy_id,
    )
    if latest_seed:
        return latest_seed
    engine_seed = _engine_base_seed(
        repository=repository,
        signal_engine_id=candidate["signal_engine_id"],
    )
    if engine_seed:
        return engine_seed
    return {
        "source_type": "system_starter",
        "source_path": None,
        "source_version": None,
        "source_session_id": None,
    }


def _latest_pair_seed(
    *,
    repository: Any,
    asset: str,
    signal_engine_id: str,
    strategy_id: str,
) -> dict[str, Any] | None:
    resolver = getattr(repository, "latest_stage1_strategy_seed", None)
    if callable(resolver):
        seed = resolver(asset=asset, signal_engine_id=signal_engine_id, strategy_id=strategy_id)
        if seed:
            return seed
    sessions = [
        session
        for session in repository.list_stage1_research_sessions()
        if session.get("asset") == asset
        and session.get("signal_engine_id") == signal_engine_id
        and session.get("strategy_id") == strategy_id
    ]
    for session in sorted(sessions, key=lambda item: str(item.get("created_at", "")), reverse=True):
        artifact_root = Path(session["artifact_root"])
        frozen_path = artifact_root / "promotion" / "frozen_stage1a_strategy_module" / "strategy.py"
        if frozen_path.is_file():
            return {
                "source_type": "latest_pair_frozen",
                "source_path": str(frozen_path),
                "source_version": session.get("strategy_version"),
                "source_session_id": session.get("session_id"),
            }
        draft_path = artifact_root / "strategy_module" / "strategy.py"
        if draft_path.is_file():
            return {
                "source_type": "latest_pair_draft",
                "source_path": str(draft_path),
                "source_version": session.get("strategy_version"),
                "source_session_id": session.get("session_id"),
            }
    return None


def _engine_base_seed(*, repository: Any, signal_engine_id: str) -> dict[str, Any] | None:
    engines = [
        engine
        for engine in repository.list_signal_engines()
        if engine.get("signal_engine_id") == signal_engine_id
    ]
    if not engines:
        return None
    engine = engines[0]
    code_ref = engine.get("code_ref") or {}
    base_path = code_ref.get("base_strategy_path") or code_ref.get("base_strategy")
    if not base_path:
        return None
    return {
        "source_type": "engine_base",
        "source_path": str(base_path),
        "source_version": engine.get("version"),
        "source_session_id": None,
    }


def _stage1_sample_window(session: dict[str, Any], sample_method: str) -> tuple[str, str]:
    if sample_method == "training":
        return _date_string(session["train_start"]), _date_string(session["train_end"])
    if sample_method == "walk_forward_test":
        return _date_string(session["walk_forward_start"]), _date_string(session["walk_forward_end"])
    raise HTTPException(status_code=400, detail=f"Unsupported Stage 1 sample method: {sample_method}")


def _flatten_signal_roles(signals_by_role: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    signals_by_id: dict[str, dict[str, Any]] = {}
    for signals in signals_by_role.values():
        for signal in signals:
            signals_by_id[str(signal["signal_id"])] = signal
    return list(signals_by_id.values())


def _stage2_raw_candles(session: dict[str, Any], *, repository: Any) -> list[Any]:
    start = f"{_date_string(session['train_start'])}T00:00:00Z"
    end = _add_hours(f"{_date_string(session['walk_forward_end'])}T23:59:59Z", 36)
    reader = MarketDataReader(repository=repository, workspace_root=Path.cwd())
    return reader.get_candles(
        asset=session["asset"],
        timeframe="5m",
        origin="raw",
        start=start,
        end=end,
    )


def _add_hours(value: str, hours: int) -> str:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return (parsed + timedelta(hours=hours)).isoformat().replace("+00:00", "Z")


def _build_development_queue(
    *,
    workspace_root: Path,
    universe_run_id: str,
    candidates: list[dict[str, Any]],
    stage1_sessions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    sessions_by_candidate = {
        session["source_candidate_id"]: session
        for session in sorted(stage1_sessions, key=lambda item: str(item.get("created_at", "")))
        if session.get("source_universe_run_id") == universe_run_id
    }
    return [
        _development_queue_row(
            workspace_root=workspace_root,
            candidate=candidate,
            session=sessions_by_candidate.get(candidate["candidate_id"]),
        )
        for candidate in candidates
    ]


def _development_queue_row(
    *,
    workspace_root: Path,
    candidate: dict[str, Any],
    session: dict[str, Any] | None,
) -> dict[str, Any]:
    gate = build_stage1_gate_summary(workspace_root=workspace_root, session=session) if session else None
    current_stage, development_status, next_action = _development_queue_state(candidate, session, gate)
    return {
        "candidate_id": candidate["candidate_id"],
        "universe_run_id": candidate["universe_run_id"],
        "asset": candidate["asset"],
        "signal_engine_id": candidate["signal_engine_id"],
        "signal_set_id": candidate["signal_set_id"],
        "signal_set_key": candidate["signal_set_key"],
        "strategy_id": session.get("strategy_id") if session else candidate.get("existing_strategy_id"),
        "stage0_status": candidate["acceptance_status"],
        "packet_count": candidate.get("packet_count"),
        "stage0_evaluated_signal_count": _stage0_evaluated_signal_count(candidate),
        "trigger_rate_pct": candidate.get("trigger_rate_pct"),
        "branch_path": candidate.get("branch_path"),
        "stage1_session_id": session.get("session_id") if session else None,
        "stage1_status": session.get("status") if session else None,
        "stage1_gate": gate,
        "current_stage": current_stage,
        "development_status": development_status,
        "next_action": next_action,
    }


def _stage0_evaluated_signal_count(candidate: dict[str, Any]) -> int | None:
    metrics = candidate.get("metrics") if isinstance(candidate.get("metrics"), dict) else {}
    total_records = metrics.get("total_records")
    if isinstance(total_records, int):
        return total_records
    if isinstance(total_records, float) and total_records.is_integer():
        return int(total_records)
    status_counts = metrics.get("status_counts")
    if isinstance(status_counts, dict):
        triggered = status_counts.get("triggered")
        no_trigger = status_counts.get("no_trigger")
        if isinstance(triggered, (int, float)) and isinstance(no_trigger, (int, float)):
            return int(triggered + no_trigger)
    packet_count = candidate.get("packet_count")
    if isinstance(packet_count, int):
        return packet_count
    return None


def _development_queue_state(
    candidate: dict[str, Any],
    session: dict[str, Any] | None,
    gate: dict[str, Any] | None,
) -> tuple[str, str, dict[str, Any]]:
    stage0_status = candidate["acceptance_status"]
    if stage0_status == "pending_stage0":
        return (
            "stage0_pending",
            "stage0_pending",
            _next_action("stage0_pending", "Wait for Stage 0 Result", disabled=True, target_stage="stage0"),
        )
    if candidate.get("last_error"):
        return (
            "stage0_failed",
            "stage0_failed",
            _next_action("review_stage0_failure", "Review Stage 0 Failure", disabled=True, target_stage="stage0"),
        )
    if stage0_status != "accepted":
        label = "Below Stage 0 Gate" if stage0_status == "watchlist" else "Not Startable"
        return (
            f"stage0_{stage0_status}",
            f"{stage0_status}_not_startable",
            _next_action("stage0_not_startable", label, disabled=True, target_stage="stage0"),
        )
    if session is None:
        return (
            "stage1_not_started",
            "stage1_not_started",
            _next_action("start_stage1", "Start Stage 1", target_stage="stage1"),
        )
    if gate and (gate.get("stage4_realized_expectancy") or {}).get("exists"):
        return (
            "promotion_review_ready",
            "stage4_complete",
            _next_action("review_promotion", "Review Promotion", disabled=True, target_stage="stage4"),
        )
    if gate and (gate.get("stage3_pyramid") or {}).get("exists"):
        return (
            "stage4_ready",
            "stage3_complete",
            _next_action("run_stage4_realized_expectancy", "Run Realized Expectancy", target_stage="stage4"),
        )
    if gate and (gate.get("stage3_grid") or {}).get("exists"):
        return (
            "stage3_pyramid_ready",
            "stage3_grid_complete",
            _next_action("run_stage3_pyramid", "Run Pyramid", target_stage="stage3"),
        )
    if gate and (gate.get("stage2_capture") or {}).get("exists"):
        return (
            "stage3_ready",
            "stage2_complete",
            _next_action("run_stage3_grid_search", "Run Stage 3 Grid", target_stage="stage3"),
        )
    if gate and (gate.get("canonical_readout") or {}).get("exists"):
        return (
            "stage2_ready",
            "stage1_frozen",
            _next_action("run_stage2_capture_curve", "Run Travel Capture", target_stage="stage2"),
        )
    if session.get("status") == "stage1a_frozen":
        return (
            "stage2_ready",
            "stage1_frozen",
            _next_action("run_stage2_capture_curve", "Run Travel Capture", target_stage="stage2"),
        )
    if gate and gate.get("ready_to_freeze"):
        return (
            "stage1_ready_to_freeze",
            "stage1_ready_to_freeze",
            _next_action("run_canonical_stage1a", "Run Canonical Stage 1A", target_stage="stage1"),
        )
    role_action = _stage1_role_next_action(gate)
    return (
        "stage1_in_progress",
        "stage1_in_progress",
        role_action,
    )


def _stage1_role_next_action(gate: dict[str, Any] | None) -> dict[str, Any]:
    roles = (gate or {}).get("roles") or {}
    if (roles.get("walk_forward_test") or {}).get("status") == "fail":
        return _next_action("walk_forward_failed_new_cycle", "Walk-Forward Failed", disabled=True, target_stage="stage1")
    for role in ("training", "walk_forward_test"):
        state = roles.get(role) or {}
        status = state.get("status", "missing")
        action_type, label = STAGE1_ROLE_ACTIONS[role]
        if status == "missing":
            return _next_action(action_type, label, target_stage="stage1")
        if status == "fail" and role == "training":
            return _next_action("audit_and_revise_training", "Audit Training Failures", target_stage="stage1")
        if status == "fail":
            return _next_action("walk_forward_failed_new_cycle", "Walk-Forward Failed", disabled=True, target_stage="stage1")
    return _next_action("create_training_bundle", "Create Training Bundle", target_stage="stage1")


def _ensure_stage1_session_mutable(session: dict[str, Any]) -> None:
    if session.get("status") == "stage1a_frozen":
        raise HTTPException(status_code=409, detail="Stage 1 session is frozen")
    gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
    if (gate.get("canonical_readout") or {}).get("exists"):
        raise HTTPException(status_code=409, detail="Stage 1 session is frozen")


def _next_action(
    action_type: str,
    label: str,
    *,
    disabled: bool = False,
    target_stage: str,
) -> dict[str, Any]:
    return {
        "type": action_type,
        "label": label,
        "disabled": disabled,
        "target_stage": target_stage,
    }


def _relative_iteration_paths(workspace_root: Path, iteration: dict[str, str]) -> dict[str, str]:
    result = dict(iteration)
    for key in (
        "manifest_path",
        "handoff_path",
        "signal_sample_path",
        "agent_prompt_path",
        "builder_prompt_path",
        "builder_training_sample_path",
    ):
        if key not in result:
            continue
        result[key] = str(Path(result[key]).relative_to(workspace_root))
    return result


def _relative_score_paths(workspace_root: Path, score: dict[str, Any]) -> dict[str, Any]:
    result = dict(score)
    for key in ("decisions_path", "scores_path", "summary_path"):
        if key not in result:
            continue
        result[key] = str(Path(result[key]).relative_to(workspace_root))
    return result


def _relative_audit_paths(workspace_root: Path, audit: dict[str, Any]) -> dict[str, Any]:
    result = dict(audit)
    for key in ("audit_json_path", "audit_md_path", "agent_prompt_path"):
        if key not in result:
            continue
        result[key] = str(Path(result[key]).relative_to(workspace_root))
    return result


def _relative_nested_paths(workspace_root: Path, payload: Any) -> Any:
    if isinstance(payload, list):
        return [_relative_nested_paths(workspace_root, item) for item in payload]
    if isinstance(payload, dict):
        return {key: _relative_nested_paths(workspace_root, value) for key, value in payload.items()}
    if isinstance(payload, str):
        try:
            path = Path(payload)
            if path.is_absolute():
                return str(path.relative_to(workspace_root))
        except ValueError:
            return payload
    return payload
