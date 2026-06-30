from __future__ import annotations

import hashlib
import json
import os
import shutil
import re
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from quant_terminal_api.job_dispatch import dispatch_runtime_job
from quant_terminal_api.repositories.market_data import PostgresMarketDataRepository
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_api.services.market_data_catalog import (
    build_catalog,
    build_refresh_plan,
    read_parquet_candles,
)
from quant_terminal_sdk.agent_tasks import AgentTaskBundle
from quant_terminal_sdk.engine_contracts import (
    ContractValidationError,
    SignalEngineSpec,
    validate_execution_bundle_contract,
    validate_strategy_module,
)
from quant_terminal_sdk.market_data_reader import MarketDataReader
from quant_terminal_worker.adapters.exchange import ExchangeAdapterError, build_exchange_adapter
from quant_terminal_worker.adapters.okx import OKXAdapter
from quant_terminal_worker.backtests.stage1 import run_stage1_backtest
from quant_terminal_worker.ingestion.ema_enrichment import enrich_derived_ema_datasets
from quant_terminal_worker.ingestion.feature_enrichment import enrich_feature_family_datasets
from quant_terminal_worker.ingestion.raw_candle_fill import fill_raw_candle_dataset
from quant_terminal_worker.ingestion.legacy_signals import import_legacy_signal_sets
from quant_terminal_worker.ingestion.signal_pool_extension import extend_signal_pool_from_local_candles
from quant_terminal_worker.execution.lifecycle import run_route_lifecycle_cycle
from quant_terminal_worker.execution.order_submission import submit_wake_order_intents
from quant_terminal_worker.execution.scheduler import RouteLifecycleScheduler
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
from quant_terminal_worker.stage1.workspace import read_stage1_iteration_detail
from quant_terminal_worker.stage1.workspace import read_stage4_candidate_detail
from quant_terminal_worker.stage1.scoring import run_stage1a_training_score
from quant_terminal_worker.stage1.scoring import run_stage1a_score
from quant_terminal_worker.stage1.scoring import run_stage1a_canonical_full_cycle
from quant_terminal_worker.stage1.scoring import generate_stage1a_failure_audit
from quant_terminal_worker.stage2.capture_curve import run_stage2_capture_curve
from quant_terminal_worker.stage3.grid_search import run_stage3_exact_protection
from quant_terminal_worker.stage3.grid_search import run_stage3_fixed_sl_baseline
from quant_terminal_worker.stage3.grid_search import run_stage3_grid_search
from quant_terminal_worker.stage3.grid_search import run_stage3_local_variants
from quant_terminal_worker.stage3.pyramid import run_stage3_pyramid
from quant_terminal_worker.stage4.portfolio_backtest import delete_portfolio_backtest_run
from quant_terminal_worker.stage4.portfolio_backtest import list_portfolio_backtest_runs
from quant_terminal_worker.stage4.portfolio_backtest import read_portfolio_backtest_run
from quant_terminal_worker.stage4.portfolio_backtest import run_portfolio_backtest
from quant_terminal_worker.stage4.realized_expectancy import delete_stage4_realized_expectancy_run
from quant_terminal_worker.stage4.realized_expectancy import run_stage4_realized_expectancy
from quant_terminal_worker.stage4.timing import generate_stage4b_timing_prompt
from quant_terminal_worker.stage4.timing import run_stage4b_timing_replay


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
    required_data: list[dict[str, Any]] = Field(default_factory=list)
    output_envelope_version: str = "signal_packet.v2"
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
    seed_strategy_preference: str = "auto"


class Stage1IterationCreateRequest(BaseModel):
    sample_method: str = "training"
    bundle_role: str = "strategy_builder"


class Stage1CanonicalRequest(BaseModel):
    force: bool = False


class Stage2ExitPolicyValues(BaseModel):
    lock_profit_pct: float = Field(ge=0)
    initial_sl_pct: float = Field(ge=0)
    protect_trigger_pct: float = Field(gt=0)
    trail_sl_pct: float = Field(gt=0)


class Stage2ExitPolicyRequest(BaseModel):
    lock_profit_pct: float | None = Field(default=None, ge=0)
    initial_sl_pct: float | None = Field(default=None, ge=0)
    protect_trigger_pct: float | None = Field(default=None, gt=0)
    trail_sl_pct: float | None = Field(default=None, gt=0)
    side_policies: dict[str, Stage2ExitPolicyValues] | None = None


class Stage4RealizedExpectancyRequest(BaseModel):
    initial_capital_usdt: float = Field(default=10_000.0, gt=0)
    margin_allocation_pct: float = Field(default=30.0, gt=0, le=100)
    leverage: float = Field(default=5.0, ge=1, le=125)


class PortfolioBacktestRequest(BaseModel):
    initial_capital_usdt: float = Field(default=10_000.0, gt=0)
    margin_allocations_pct: dict[str, float] = Field(default_factory=dict)


class LegacySignalImportRequest(BaseModel):
    root: str = "dev/signals/vegas_ema"
    limit: int | None = None


class SignalPoolExtendRequest(BaseModel):
    target_end: str | None = None


class SignalEngineUpdateRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1)


class SignalPoolCreateRequest(BaseModel):
    asset: str


class Stage0RunRequest(BaseModel):
    run_id: str
    strategy_id: str
    strategy_version: str
    signal_set_key: str
    forward_hours: int = 36
    significance_threshold_pct: float = 0.9


class Stage0UniverseRunRequest(BaseModel):
    universe_run_id: str
    name: str | None = None
    train_start: str
    train_end: str
    walk_forward_start: str
    walk_forward_end: str
    forward_hours: int = 36
    trigger_rate_threshold_pct: float = 85
    engine_ids: list[str] = Field(default_factory=list)
    assets: list[str] = Field(default_factory=list)


class Stage0UniverseAppendAssetsRequest(BaseModel):
    assets: list[str] = Field(default_factory=list)


class ExecuteStage0CandidateRequest(BaseModel):
    candidate_id: str


class ExecuteStage0CandidateBatchRequest(BaseModel):
    limit: int = Field(default=500, ge=1, le=1000)


class ExecutionBundlePromotionRequest(BaseModel):
    account_mode: str = "live"
    execution_adapter: str = "okx"
    risk_limits: dict[str, Any] = Field(
        default_factory=lambda: {
            "max_notional_usd": 1000,
            "max_daily_loss_usd": 250,
        }
    )


class OrderSubmissionRequest(BaseModel):
    confirm_live: bool = False
    quantity: str | None = None
    notional_usd: float | None = None


class DeploymentRouteSettingsRequest(BaseModel):
    cron_interval_minutes: int = Field(ge=1, le=1440)
    execution_adapter: str = "okx"
    exchange_account: str = "default"
    margin_allocation_pct: float = Field(default=10.0, ge=0.1, le=100.0)
    leverage: float = Field(default=1.0, ge=1.0, le=125.0)
    manual_sizing_enabled: bool = False
    auto_submit_enabled: bool = True


class DeploymentRouteStartRequest(BaseModel):
    confirm_live: bool = False
    auto_submit_enabled: bool = True


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
    live_signal_scan_service: Any | None = None,
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
    live_signal_scanner = live_signal_scan_service

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

    def enqueue_runtime_job(
        repository: Any,
        *,
        job_type: str,
        scope_key: str,
        payload: dict[str, Any],
        current_step: str,
    ) -> dict[str, Any] | None:
        enqueuer = getattr(repository, "enqueue_job", None)
        if not callable(enqueuer):
            return None
        job = enqueuer(
            job_type=job_type,
            scope_key=scope_key,
            payload=payload,
            current_step=current_step,
        )
        try:
            dispatch = dispatch_runtime_job(job)
        except RuntimeError as exc:
            raise HTTPException(
                status_code=503,
                detail={"message": str(exc), "job_id": job.get("job_id")},
            ) from exc
        return {"accepted": True, "job": _relative_nested_paths(Path.cwd(), job), "dispatch": dispatch}

    def build_execution_adapter(route: dict[str, Any]) -> Any:
        try:
            return build_exchange_adapter(route)
        except ExchangeAdapterError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    def run_lifecycle_cycle_for_route(route_id: str) -> dict[str, Any]:
        route = get_runtime_repository().get_deployment_route(route_id)
        if route is None:
            raise ValueError(f"deployment route not found: {route_id}")
        non_data_blockers = [blocker for blocker in route.get("blockers", []) if blocker != "data_not_warmed"]
        return run_route_lifecycle_cycle(
            route_id=route_id,
            runtime_repository=get_runtime_repository(),
            market_data_repository=None if non_data_blockers else get_market_data_repository(),
            fill_service=fill_service or fill_raw_candle_dataset,
            signal_pool_extender=signal_pool_extender,
            live_signal_scanner=live_signal_scanner,
            adapter=build_execution_adapter(route),
            workspace_root=Path.cwd(),
        )

    scheduler = RouteLifecycleScheduler(
        load_route=lambda route_id: get_runtime_repository().get_deployment_route(route_id),
        list_routes=lambda: get_runtime_repository().list_deployment_routes(),
        update_route=lambda route_id, updates: get_runtime_repository().update_deployment_route_gate(route_id, **updates),
        run_cycle=run_lifecycle_cycle_for_route,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            scheduler.resume_running()
        except Exception:
            pass
        yield

    app.router.lifespan_context = lifespan

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
        return {"engines": _signal_engine_catalog(get_runtime_repository(), workspace_root=Path.cwd())}

    @app.patch("/api/v1/signal-engines/{signal_engine_id}")
    def update_signal_engine(signal_engine_id: str, request: SignalEngineUpdateRequest) -> dict[str, Any]:
        repository = get_runtime_repository()
        engine = _materialize_signal_engine(repository, workspace_root=Path.cwd(), signal_engine_id=signal_engine_id)
        if engine is None:
            raise HTTPException(status_code=404, detail="Signal engine not found")
        if request.name is not None:
            updater = getattr(repository, "update_signal_engine", None)
            if not callable(updater):
                raise HTTPException(status_code=501, detail="Signal engine update is not supported by this repository")
            engine = updater(signal_engine_id, name=request.name)
        catalog_engine = next(
            (item for item in _signal_engine_catalog(repository, workspace_root=Path.cwd()) if item["signal_engine_id"] == signal_engine_id),
            engine,
        )
        return {"engine": catalog_engine}

    @app.get("/api/v1/signal-engines/{signal_engine_id}/signal-sets")
    def list_signal_sets(signal_engine_id: str) -> dict[str, Any]:
        return {"signal_sets": get_runtime_repository().list_signal_sets(signal_engine_id)}

    @app.get("/api/v1/signal-engines/{signal_engine_id}/assets/{asset}/live-observations")
    def list_live_signal_observations(signal_engine_id: str, asset: str, limit: int = 100, offset: int = 0) -> dict[str, Any]:
        repository = get_runtime_repository()
        lister = getattr(repository, "list_live_signal_observations", None)
        if not callable(lister):
            raise HTTPException(status_code=503, detail="live signal observations are not configured")
        return _relative_nested_paths(
            Path.cwd(),
            lister(signal_engine_id=signal_engine_id, asset=asset, limit=limit, offset=offset),
        )

    @app.post("/api/v1/signal-engines/{signal_engine_id}/signal-sets")
    def create_signal_set(signal_engine_id: str, request: SignalPoolCreateRequest) -> dict[str, Any]:
        repository = get_runtime_repository()
        engine = _materialize_signal_engine(repository, workspace_root=Path.cwd(), signal_engine_id=signal_engine_id)
        if engine is None:
            raise HTTPException(status_code=404, detail="Signal engine not found")
        try:
            signal_set = _create_canonical_signal_set(repository=repository, engine=engine, asset=request.asset)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"signal_set": signal_set}

    @app.get("/api/v1/signals")
    def list_signals(
        signal_set_key: str | None = None,
        signal_engine_id: str | None = None,
        asset: str | None = None,
        limit: int = 25,
        descending: bool = False,
    ) -> dict[str, Any]:
        return {
            "signals": get_runtime_repository().list_signals(
                signal_set_key=signal_set_key,
                signal_engine_id=signal_engine_id,
                asset=asset,
                limit=min(limit, 200),
                descending=descending,
            )
        }

    @app.get("/api/v1/jobs/runtime")
    def get_job_runtime_status() -> dict[str, Any]:
        repository = get_runtime_repository()
        status_reader = getattr(repository, "get_worker_runtime_status", None)
        if not callable(status_reader):
            raise HTTPException(status_code=503, detail="job runtime is not configured")
        return {"worker_runtime": _relative_nested_paths(Path.cwd(), status_reader())}

    @app.get("/api/v1/jobs")
    def list_jobs(scope_key: str | None = None, limit: int = 50) -> dict[str, Any]:
        repository = get_runtime_repository()
        lister = getattr(repository, "list_jobs", None)
        if not callable(lister):
            raise HTTPException(status_code=503, detail="job runtime is not configured")
        return {
            "jobs": _relative_nested_paths(
                Path.cwd(),
                lister(scope_key=scope_key, limit=min(limit, 200)),
            )
        }

    @app.get("/api/v1/jobs/{job_id}")
    def get_job(job_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        getter = getattr(repository, "get_job", None)
        if not callable(getter):
            raise HTTPException(status_code=503, detail="job runtime is not configured")
        job = getter(job_id)
        if job is None:
            raise HTTPException(status_code=404, detail="job not found")
        return {"job": _relative_nested_paths(Path.cwd(), job)}

    @app.post("/api/v1/jobs/{job_id}/cancel")
    def cancel_job(job_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        canceller = getattr(repository, "cancel_job", None)
        if not callable(canceller):
            raise HTTPException(status_code=503, detail="job runtime is not configured")
        job = canceller(job_id)
        if job is None:
            raise HTTPException(status_code=409, detail="job cannot be cancelled")
        return {"job": _relative_nested_paths(Path.cwd(), job)}

    @app.post("/api/v1/signal-engines/{signal_engine_id}/signal-sets/{asset}/extend-local")
    def extend_signal_set_from_local_candles(
        signal_engine_id: str,
        asset: str,
        request: SignalPoolExtendRequest,
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        if signal_pool_extender is None:
            queued = enqueue_runtime_job(
                repository,
                job_type="signal_pool_extend",
                scope_key=f"signal_set:{signal_engine_id}:{asset.upper()}",
                payload={
                    "signal_engine_id": signal_engine_id,
                    "asset": asset,
                    "target_end": request.target_end,
                },
                current_step="queued",
            )
            if queued:
                return queued
        service = signal_pool_extender or extend_signal_pool_from_local_candles
        try:
            return service(
                workspace_root=Path.cwd(),
                repository=repository,
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
        if request.seed_strategy_preference not in {"auto", "engine_base", "latest_pair"}:
            raise HTTPException(status_code=400, detail="seed_strategy_preference must be auto, engine_base, or latest_pair")
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
            preference=request.seed_strategy_preference,
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

    @app.delete("/api/v1/research/stage1-sessions/{session_id}")
    def reset_stage1_research_session(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_resettable(repository=repository, session=session)
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        if artifact_root.exists():
            shutil.rmtree(artifact_root)
        deleter = getattr(get_runtime_repository(), "delete_stage1_research_session", None)
        if not callable(deleter):
            raise HTTPException(status_code=501, detail="Stage 1 session deletion is not supported by this repository")
        deleter(session_id)
        return {
            "status": "deleted",
            "session_id": session_id,
            "source_candidate_id": session["source_candidate_id"],
        }

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

    @app.get("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/details")
    def get_stage1_research_iteration_detail(session_id: str, iteration_id: str) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        try:
            detail = read_stage1_iteration_detail(
                workspace_root=Path.cwd(),
                session=session,
                iteration_id=iteration_id,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"detail": _relative_nested_paths(Path.cwd(), detail)}

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
    def run_stage1_canonical_readout(session_id: str, request: Stage1CanonicalRequest = Stage1CanonicalRequest()) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not gate["ready_to_freeze"] and not request.force:
            raise HTTPException(
                status_code=400,
                detail={
                    "message": "Stage 1A canonical readout requires passing training and walk-forward test scores.",
                    "blockers": gate["blockers"],
                },
            )
        queued = enqueue_runtime_job(
            repository,
            job_type="stage1_canonical",
            scope_key=f"stage1_session:{session_id}",
            payload={"session_id": session_id, "force": request.force},
            current_step="queued",
        )
        if queued:
            return queued
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
            "forced": bool(request.force and not gate["ready_to_freeze"]),
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
        queued = enqueue_runtime_job(
            repository,
            job_type="stage2_capture_curve",
            scope_key=f"stage1_session:{session_id}",
            payload={"session_id": session_id},
            current_step="queued",
        )
        if queued:
            return queued
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

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage2/exit-policy")
    def promote_stage2_exit_policy(session_id: str, request: Stage2ExitPolicyRequest) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage2_capture") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 2 exit policy requires completed Stage 2 travel capture")

        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        promotion_root = artifact_root / "promotion"
        capture_path = promotion_root / "stage2_capture_curve.json"
        capture = json.loads(capture_path.read_text())
        allowed_tp = _stage2_policy_allowed_values(capture, key="tp_levels", fallback_key="results")
        allowed_sl = _stage2_policy_allowed_values(capture, key="sl_levels", fallback_key="sl_results")
        if not allowed_sl:
            raise HTTPException(
                status_code=400,
                detail="Stage 2 exit policy requires a matched adverse SL band. Rerun Stage 2 capture to rebuild the SL curve.",
            )
        try:
            policy_mode, side_policies = _normalize_stage2_side_policies(request)
            for side, side_policy in side_policies.items():
                _validate_stage2_policy_values(side_policy, allowed_tp=allowed_tp, allowed_sl=allowed_sl, label=side)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        selected = side_policies["LONG"]

        policy = {
            "schema_version": "0.1",
            "stage": "stage2_exit_policy_handoff",
            "artifact_role": "stage2_exit_policy",
            "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "session_id": session_id,
            "asset": session.get("asset"),
            "strategy_id": session.get("strategy_id"),
            "policy": selected,
            "policy_mode": policy_mode,
            "side_policies": side_policies,
            "source": {
                "capture_curve_path": str(capture_path),
                "selection_source": "stage2_capture_curve_tp_and_sl_bands",
            },
        }
        policy_path = promotion_root / "stage2_exit_policy.json"
        policy_path.write_text(json.dumps(policy, indent=2) + "\n")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        return {
            "stage2_exit_policy": _relative_nested_paths(Path.cwd(), gate["stage2_exit_policy"]),
            "gate": _relative_nested_paths(Path.cwd(), gate),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/grid-search")
    def run_stage3_grid_readout(session_id: str) -> dict[str, Any]:
        return _run_stage3_grid_step(session_id, step="grid_search")

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/fixed-sl")
    def run_stage3_fixed_sl_readout(session_id: str) -> dict[str, Any]:
        return _run_stage3_grid_step(session_id, step="fixed_sl")

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/exact-protection")
    def run_stage3_exact_protection_readout(session_id: str) -> dict[str, Any]:
        return _run_stage3_grid_step(session_id, step="exact_protection")

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage3/local-variants")
    def run_stage3_local_variants_readout(session_id: str) -> dict[str, Any]:
        return _run_stage3_grid_step(session_id, step="local_variants")

    def _run_stage3_grid_step(session_id: str, *, step: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage2_capture") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 3 requires completed Stage 2 travel capture")
        if not (gate.get("stage2_exit_policy") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 3 requires promoted Stage 2 exit policy")
        queued = enqueue_runtime_job(
            repository,
            job_type="stage3_policy_step",
            scope_key=f"stage1_session:{session_id}",
            payload={"session_id": session_id, "step": step},
            current_step="queued",
        )
        if queued:
            return queued
        try:
            runner = {
                "grid_search": run_stage3_grid_search,
                "fixed_sl": run_stage3_fixed_sl_baseline,
                "exact_protection": run_stage3_exact_protection,
                "local_variants": run_stage3_local_variants,
            }[step]
            result = runner(
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
            raise HTTPException(status_code=400, detail="Stage 3 pyramid requires completed Stage 3 policy test")
        queued = enqueue_runtime_job(
            repository,
            job_type="stage3_pyramid",
            scope_key=f"stage1_session:{session_id}",
            payload={"session_id": session_id},
            current_step="queued",
        )
        if queued:
            return queued
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
    def run_stage4_realized_expectancy_readout(
        session_id: str,
        request: Stage4RealizedExpectancyRequest = Stage4RealizedExpectancyRequest(),
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage3_pyramid") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Stage 4 requires completed Stage 3 pyramid")
        queued = enqueue_runtime_job(
            repository,
            job_type="stage4_realized_expectancy",
            scope_key=f"stage1_session:{session_id}",
            payload={
                "session_id": session_id,
                "initial_capital_usdt": request.initial_capital_usdt,
                "margin_allocation_pct": request.margin_allocation_pct,
                "leverage": request.leverage,
            },
            current_step="queued",
        )
        if queued:
            return queued
        try:
            result = run_stage4_realized_expectancy(
                workspace_root=Path.cwd(),
                session=session,
                signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(session)),
                candles=_stage2_raw_candles(session, repository=repository),
                initial_capital_usdt=request.initial_capital_usdt,
                margin_allocation_pct=request.margin_allocation_pct,
                leverage=request.leverage,
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

    @app.delete("/api/v1/research/stage1-sessions/{session_id}/stage4/runs/{run_id}")
    def delete_stage4_realized_expectancy_run_history(session_id: str, run_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        try:
            result = delete_stage4_realized_expectancy_run(
                workspace_root=Path.cwd(),
                session=session,
                run_id=run_id,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage4_run_delete": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage4/timing-prompt")
    def generate_stage4b_timing_prompt_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        try:
            result = generate_stage4b_timing_prompt(workspace_root=Path.cwd(), session=session)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage4b_timing_prompt": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/stage4/timing-replay")
    def run_stage4b_timing_replay_readout(session_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        queued = enqueue_runtime_job(
            repository,
            job_type="stage4b_timing_replay",
            scope_key=f"stage1_session:{session_id}",
            payload={"session_id": session_id},
            current_step="queued",
        )
        if queued:
            return queued
        try:
            result = run_stage4b_timing_replay(
                workspace_root=Path.cwd(),
                session=session,
                signal_rows=_flatten_signal_roles(_stage1_full_cycle_signals(session)),
                candles=_stage2_raw_candles(session, repository=repository),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "stage4b_timing": _relative_nested_paths(Path.cwd(), result),
            "gate": _relative_nested_paths(
                Path.cwd(),
                build_stage1_gate_summary(workspace_root=Path.cwd(), session=session),
            ),
        }

    @app.post("/api/v1/research/stage0-universe-runs/{universe_run_id}/portfolio-backtest")
    def run_portfolio_backtest_readout(
        universe_run_id: str,
        request: PortfolioBacktestRequest = PortfolioBacktestRequest(),
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        universe_run = repository.get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        candidates = repository.list_stage0_universe_candidates(universe_run_id)
        sessions = [session for session in repository.list_stage1_research_sessions() if session.get("source_universe_run_id") == universe_run_id]
        if not _portfolio_stage4_complete_sessions(Path.cwd(), sessions=sessions, candidates=candidates):
            raise HTTPException(status_code=400, detail="Portfolio backtest requires at least one Stage 4-complete asset")
        queued = enqueue_runtime_job(
            repository,
            job_type="portfolio_backtest",
            scope_key=f"stage0:{universe_run_id}",
            payload={
                "universe_run_id": universe_run_id,
                "initial_capital_usdt": request.initial_capital_usdt,
                "margin_allocations_pct": request.margin_allocations_pct,
            },
            current_step="queued",
        )
        if queued:
            return queued
        try:
            result = run_portfolio_backtest(
                workspace_root=Path.cwd(),
                universe_run=universe_run,
                candidates=candidates,
                sessions=sessions,
                initial_capital_usdt=request.initial_capital_usdt,
                margin_allocations_pct=request.margin_allocations_pct,
                repository=get_runtime_repository(),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "portfolio_backtest": _relative_nested_paths(Path.cwd(), result),
        }

    @app.get("/api/v1/research/stage0-universe-runs/{universe_run_id}/portfolio-backtest/runs")
    def list_portfolio_backtest_run_history(universe_run_id: str) -> dict[str, Any]:
        if get_runtime_repository().get_stage0_universe_run(universe_run_id) is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        return {
            "portfolio_backtest_runs": _relative_nested_paths(
                Path.cwd(),
                list_portfolio_backtest_runs(workspace_root=Path.cwd(), universe_run_id=universe_run_id),
            )
        }

    @app.get("/api/v1/research/stage0-universe-runs/{universe_run_id}/portfolio-backtest/runs/{run_id}")
    def get_portfolio_backtest_run(universe_run_id: str, run_id: str) -> dict[str, Any]:
        if get_runtime_repository().get_stage0_universe_run(universe_run_id) is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        try:
            run = read_portfolio_backtest_run(workspace_root=Path.cwd(), universe_run_id=universe_run_id, run_id=run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"portfolio_backtest": _relative_nested_paths(Path.cwd(), run)}

    @app.delete("/api/v1/research/stage0-universe-runs/{universe_run_id}/portfolio-backtest/runs/{run_id}")
    def delete_portfolio_backtest_run_history(universe_run_id: str, run_id: str) -> dict[str, Any]:
        if get_runtime_repository().get_stage0_universe_run(universe_run_id) is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        try:
            result = delete_portfolio_backtest_run(workspace_root=Path.cwd(), universe_run_id=universe_run_id, run_id=run_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"portfolio_backtest_delete": _relative_nested_paths(Path.cwd(), result)}

    @app.get("/api/v1/research/stage1-sessions/{session_id}/stage4/candidates/{candidate_id}/details")
    def get_stage4_candidate_detail(
        session_id: str,
        candidate_id: str,
        source: str = "stage4_realized_expectancy",
    ) -> dict[str, Any]:
        session = get_runtime_repository().get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        try:
            detail = read_stage4_candidate_detail(
                workspace_root=Path.cwd(),
                session=session,
                candidate_id=candidate_id,
                source=source,
            )
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"detail": _relative_nested_paths(Path.cwd(), detail)}

    @app.post("/api/v1/research/stage1-sessions/{session_id}/promote-execution-bundle")
    def promote_execution_bundle(
        session_id: str,
        request: ExecutionBundlePromotionRequest = ExecutionBundlePromotionRequest(),
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
        if not (gate.get("stage4_realized_expectancy") or {}).get("exists"):
            raise HTTPException(status_code=400, detail="Execution bundle promotion requires completed Stage 4 evidence")
        try:
            bundle = _materialize_execution_bundle(
                workspace_root=Path.cwd(),
                repository=repository,
                session=session,
                gate=gate,
                account_mode=request.account_mode,
                execution_adapter=request.execution_adapter,
                risk_limits=request.risk_limits,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        stored_bundle = repository.create_execution_bundle(bundle)
        route = repository.upsert_deployment_route_for_bundle(
            bundle=stored_bundle,
            account_mode=request.account_mode,
            execution_adapter=request.execution_adapter,
        )
        return {
            "bundle": _relative_nested_paths(Path.cwd(), stored_bundle),
            "route": _relative_nested_paths(Path.cwd(), route),
        }

    @app.get("/api/v1/trading/routes")
    def list_trading_routes() -> dict[str, Any]:
        return {"routes": _relative_nested_paths(Path.cwd(), get_runtime_repository().list_deployment_routes())}

    @app.get("/api/v1/trading/routes/archived")
    def list_archived_trading_routes() -> dict[str, Any]:
        archived_routes = [
            route
            for route in get_runtime_repository().list_deployment_routes(include_archived=True)
            if route.get("archived")
        ]
        return {"routes": _relative_nested_paths(Path.cwd(), archived_routes)}

    @app.get("/api/v1/trading/routes/{route_id}")
    def get_trading_route(route_id: str) -> dict[str, Any]:
        route = get_runtime_repository().get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        return {"route": _relative_nested_paths(Path.cwd(), route)}

    @app.get("/api/v1/trading/routes/{route_id}/wakes")
    def list_trading_route_wakes(route_id: str, limit: int = 25, offset: int = 0) -> dict[str, Any]:
        route = get_runtime_repository().get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        page = get_runtime_repository().list_wake_run_page(route_id, limit=limit, offset=offset)
        return {
            "wakes": _relative_nested_paths(Path.cwd(), page["wakes"]),
            "total": page["total"],
            "limit": page["limit"],
            "offset": page["offset"],
        }

    @app.get("/api/v1/trading/routes/{route_id}/exchange-health")
    def get_trading_route_exchange_health(route_id: str) -> dict[str, Any]:
        route = get_runtime_repository().get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        try:
            adapter = build_execution_adapter(route)
        except HTTPException:
            raise
        cli_path_getter = getattr(adapter, "_cli_path", None)
        cli_path = cli_path_getter() if callable(cli_path_getter) else None
        base = {
            "route_id": route_id,
            "adapter": route.get("execution_adapter"),
            "account_mode": route.get("account_mode"),
            "exchange_account": route.get("exchange_account"),
            "instrument": route.get("instrument"),
            "cli_path": cli_path,
            "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        readiness_blockers = list(adapter.readiness_blockers()) if hasattr(adapter, "readiness_blockers") else []
        if readiness_blockers:
            return {
                **base,
                "status": "blocked",
                "connected": False,
                "readiness_blockers": readiness_blockers,
                "snapshot": {},
                "error": ", ".join(readiness_blockers),
            }
        try:
            snapshot = adapter.snapshot(route["instrument"])
        except ExchangeAdapterError as exc:
            return {
                **base,
                "status": "disconnected",
                "connected": False,
                "readiness_blockers": [],
                "snapshot": {},
                "error": _sanitize_exchange_error(str(exc)),
            }
        return {
            **base,
            "status": "connected",
            "connected": True,
            "readiness_blockers": [],
            "snapshot": {
                "position_count": len(snapshot.get("positions") or []),
                "open_order_count": len(snapshot.get("open_orders") or []),
                "protection_order_count": len(snapshot.get("protection_orders") or []),
                "recent_fill_count": len(snapshot.get("recent_fills") or []),
                "has_balance": bool(snapshot.get("balance")),
            },
            "error": None,
        }

    @app.post("/api/v1/trading/routes/{route_id}/enable")
    def enable_trading_route(route_id: str) -> dict[str, Any]:
        return _update_trading_route_gate(route_id, enabled=True)

    @app.post("/api/v1/trading/routes/{route_id}/disable")
    def disable_trading_route(route_id: str) -> dict[str, Any]:
        return _update_trading_route_gate(route_id, enabled=False)

    @app.post("/api/v1/trading/routes/{route_id}/archive")
    def archive_trading_route(route_id: str) -> dict[str, Any]:
        try:
            scheduler.stop(route_id)
        except ValueError:
            pass
        route = get_runtime_repository().archive_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        return {"route": _relative_nested_paths(Path.cwd(), route)}

    @app.delete("/api/v1/trading/routes/{route_id}/archived-strategy")
    def delete_archived_trading_route(route_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        route = repository.get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        if not route.get("archived"):
            raise HTTPException(status_code=409, detail="Only archived strategies can be deleted")
        if not route.get("active_bundle_id"):
            raise HTTPException(status_code=409, detail="Archived strategy has no active bundle")
        try:
            adapter = build_execution_adapter(route)
            readiness_blockers = list(adapter.readiness_blockers()) if hasattr(adapter, "readiness_blockers") else []
        except HTTPException as exc:
            raise HTTPException(status_code=409, detail="Unable to verify archived strategy exchange exposure") from exc
        if readiness_blockers:
            raise HTTPException(status_code=409, detail="Unable to verify archived strategy exchange exposure")
        try:
            snapshot = adapter.snapshot(route["instrument"])
        except ExchangeAdapterError as exc:
            raise HTTPException(
                status_code=409,
                detail=f"Unable to verify archived strategy exchange exposure: {_sanitize_exchange_error(str(exc))}",
            ) from exc
        if any(snapshot.get(key) for key in ("positions", "open_orders", "protection_orders")):
            raise HTTPException(status_code=409, detail="Archived strategy still has live exchange exposure")
        try:
            deleted = repository.delete_archived_strategy_route(route_id)
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        if deleted is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        artifact_deleted = _delete_filesystem_path(deleted.get("bundle_uri"))
        return {
            "status": "deleted",
            "route_id": route_id,
            "bundle_id": deleted["bundle_id"],
            "deleted_wake_count": deleted["deleted_wake_count"],
            "deleted_owner_state_count": deleted["deleted_owner_state_count"],
            "artifact_deleted": artifact_deleted,
        }

    @app.post("/api/v1/trading/routes/{route_id}/arm")
    def arm_trading_route(route_id: str) -> dict[str, Any]:
        return _update_trading_route_gate(route_id, manually_armed=True)

    @app.post("/api/v1/trading/routes/{route_id}/disarm")
    def disarm_trading_route(route_id: str) -> dict[str, Any]:
        return _update_trading_route_gate(route_id, manually_armed=False)

    @app.post("/api/v1/trading/routes/{route_id}/mark-data-warmed")
    def mark_trading_route_data_warmed(route_id: str) -> dict[str, Any]:
        return _update_trading_route_gate(route_id, data_warmed=True)

    @app.patch("/api/v1/trading/routes/{route_id}/settings")
    def update_trading_route_settings(route_id: str, request: DeploymentRouteSettingsRequest) -> dict[str, Any]:
        route = get_runtime_repository().update_deployment_route_gate(
            route_id,
            cron_interval_minutes=request.cron_interval_minutes,
            execution_adapter=request.execution_adapter,
            exchange_account=request.exchange_account,
            margin_allocation_pct=request.margin_allocation_pct,
            leverage=request.leverage,
            manual_sizing_enabled=request.manual_sizing_enabled,
            auto_submit_enabled=request.auto_submit_enabled,
        )
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        return {"route": _relative_nested_paths(Path.cwd(), route)}

    @app.post("/api/v1/trading/routes/{route_id}/start")
    def start_trading_route(route_id: str, request: DeploymentRouteStartRequest) -> dict[str, Any]:
        repository = get_runtime_repository()
        route = repository.get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        if route.get("account_mode") == "live" and not request.confirm_live:
            raise HTTPException(status_code=400, detail="live route start requires confirm_live")
        route = repository.update_deployment_route_gate(
            route_id,
            enabled=True,
            manually_armed=True if route.get("account_mode") == "live" else route.get("manually_armed", False),
            scheduler_status="running",
            auto_submit_enabled=request.auto_submit_enabled,
            next_wake_at=datetime.now(UTC),
            last_lifecycle_error={},
        )
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        try:
            cycle = run_lifecycle_cycle_for_route(route_id)
            scheduled_route = scheduler.start(route_id, run_immediately=False)
        except ExchangeAdapterError as exc:
            repository.update_deployment_route_gate(
                route_id,
                scheduler_status="stopped",
                last_lifecycle_error={"message": str(exc), "stage": "start"},
            )
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {
            "cycle": _relative_nested_paths(Path.cwd(), cycle),
            "route": _relative_nested_paths(Path.cwd(), scheduled_route),
        }

    @app.post("/api/v1/trading/routes/{route_id}/stop")
    def stop_trading_route(route_id: str) -> dict[str, Any]:
        try:
            route = scheduler.stop(route_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        route = get_runtime_repository().update_deployment_route_gate(route_id, enabled=False, next_wake_at=None) or route
        return {"route": _relative_nested_paths(Path.cwd(), route)}

    def _update_trading_route_gate(route_id: str, **updates: Any) -> dict[str, Any]:
        route = get_runtime_repository().update_deployment_route_gate(route_id, **updates)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        return {"route": _relative_nested_paths(Path.cwd(), route)}

    @app.post("/api/v1/trading/routes/{route_id}/wake")
    def run_trading_route_wake(route_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        route = repository.get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        try:
            cycle = run_lifecycle_cycle_for_route(route_id)
        except ExchangeAdapterError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {
            "warmup": _relative_nested_paths(Path.cwd(), cycle["warmup"]),
            "signal_update": _relative_nested_paths(Path.cwd(), cycle["signal_update"]),
            "wake": _relative_nested_paths(Path.cwd(), cycle["wake"]),
            "submission": _relative_nested_paths(Path.cwd(), cycle["submission"]),
            "route": _relative_nested_paths(Path.cwd(), cycle["route"] or repository.get_deployment_route(route_id) or route),
        }

    @app.post("/api/v1/trading/routes/{route_id}/wakes/{wake_id}/submit-orders")
    def submit_trading_route_wake_orders(
        route_id: str,
        wake_id: str,
        request: OrderSubmissionRequest,
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        route = repository.get_deployment_route(route_id)
        if route is None:
            raise HTTPException(status_code=404, detail="deployment route not found")
        if repository.get_wake_run(wake_id) is None:
            raise HTTPException(status_code=404, detail="wake run not found")
        adapter = build_execution_adapter(route)
        try:
            result = submit_wake_order_intents(
                route_id=route_id,
                wake_id=wake_id,
                repository=repository,
                adapter=adapter,
                confirm_live=request.confirm_live,
                quantity_override=request.quantity,
                notional_usd_override=request.notional_usd,
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ExchangeAdapterError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc
        return {
            **_relative_nested_paths(Path.cwd(), result),
            "route": _relative_nested_paths(Path.cwd(), repository.get_deployment_route(route_id) or route),
        }

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/score-training")
    def score_stage1_training_iteration(session_id: str, iteration_id: str) -> dict[str, Any]:
        return _score_stage1_iteration(session_id=session_id, iteration_id=iteration_id, sample_role="training")

    @app.post("/api/v1/research/stage1-sessions/{session_id}/iterations/{iteration_id}/score-walk-forward")
    def score_stage1_walk_forward_iteration(session_id: str, iteration_id: str) -> dict[str, Any]:
        return _score_stage1_iteration(session_id=session_id, iteration_id=iteration_id, sample_role="walk_forward_test")

    def _score_stage1_iteration(*, session_id: str, iteration_id: str, sample_role: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        session = repository.get_stage1_research_session(session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="Stage 1 session not found")
        _ensure_stage1_session_mutable(session)
        artifact_root = Path(session["artifact_root"])
        if not artifact_root.is_absolute():
            artifact_root = Path.cwd() / artifact_root
        iteration_root = artifact_root / "iterations" / iteration_id
        if not iteration_root.is_dir():
            raise HTTPException(status_code=404, detail="Stage 1 iteration not found")
        queued = enqueue_runtime_job(
            repository,
            job_type="stage1_score",
            scope_key=f"stage1_session:{session_id}",
            payload={
                "session_id": session_id,
                "iteration_id": iteration_id,
                "sample_role": sample_role,
            },
            current_step="queued",
        )
        if queued:
            return queued
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
        name = request.name.strip() if request.name else None
        universe["run"]["name"] = name or None
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

    @app.get("/api/v1/research/stage0-universe-runs/{universe_run_id}/appendable-assets")
    def list_stage0_universe_appendable_assets(universe_run_id: str) -> dict[str, Any]:
        repository = get_runtime_repository()
        universe_run = repository.get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        candidates = _stage0_universe_append_candidates(repository=repository, universe_run=universe_run, asset_symbols=None)
        existing_keys = {candidate["signal_set_key"] for candidate in repository.list_stage0_universe_candidates(universe_run_id)}
        assets = sorted({candidate["asset"] for candidate in candidates if candidate["signal_set_key"] not in existing_keys})
        return {"assets": assets}

    @app.post("/api/v1/research/stage0-universe-runs/{universe_run_id}/append-assets")
    def append_stage0_universe_assets(
        universe_run_id: str,
        request: Stage0UniverseAppendAssetsRequest,
    ) -> dict[str, Any]:
        repository = get_runtime_repository()
        universe_run = repository.get_stage0_universe_run(universe_run_id)
        if universe_run is None:
            raise HTTPException(status_code=404, detail="stage0 universe run not found")
        normalized_assets = sorted({asset.strip().upper() for asset in request.assets if asset.strip()})
        if not normalized_assets:
            raise HTTPException(status_code=400, detail="No assets requested")
        candidates = _stage0_universe_append_candidates(
            repository=repository,
            universe_run=universe_run,
            asset_symbols=normalized_assets,
        )
        existing_keys = {candidate["signal_set_key"] for candidate in repository.list_stage0_universe_candidates(universe_run_id)}
        added_candidates = [candidate for candidate in candidates if candidate["signal_set_key"] not in existing_keys]
        appendable_assets = {candidate["asset"] for candidate in added_candidates}
        invalid_assets = [asset for asset in normalized_assets if asset not in appendable_assets]
        if invalid_assets:
            raise HTTPException(
                status_code=400,
                detail=f"No generated signals available for selected pool engine/window: {', '.join(invalid_assets)}",
            )
        appender = getattr(repository, "append_stage0_universe_candidates", None)
        if not callable(appender):
            raise HTTPException(status_code=501, detail="Stage 0 universe append is not supported by this repository")
        appender(universe_run_id, added_candidates)
        repository.refresh_stage0_universe_summary(universe_run_id)
        refreshed_run = repository.get_stage0_universe_run(universe_run_id) or universe_run
        refreshed_candidates = repository.list_stage0_universe_candidates(universe_run_id)
        return {
            "run": refreshed_run,
            "candidates": refreshed_candidates,
            "added_candidates": added_candidates,
            "added_candidate_count": len(added_candidates),
        }

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
        queued = None
        if injected_stage0_executor is None:
            queued = enqueue_runtime_job(
                get_runtime_repository(),
                job_type="stage0_candidate",
                scope_key=f"stage0:{universe_run_id}",
                payload={"universe_run_id": universe_run_id, "candidate_id": request.candidate_id},
                current_step="queued",
            )
        if queued:
            return queued

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
        queued = None
        if injected_stage0_executor is None:
            queued = enqueue_runtime_job(
                get_runtime_repository(),
                job_type="stage0_candidate_batch",
                scope_key=f"stage0:{universe_run_id}",
                payload={"universe_run_id": universe_run_id, "limit": request.limit},
                current_step="queued",
            )
        if queued:
            return queued

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
        repository = get_runtime_repository()
        linked_sessions = [
            session
            for session in repository.list_stage1_research_sessions()
            if session.get("source_universe_run_id") == universe_run_id
        ]
        _ensure_stage0_universe_run_deletable(repository=repository, linked_sessions=linked_sessions)
        linked_session_ids = [session["session_id"] for session in linked_sessions]
        repository.delete_stage0_universe_run(universe_run_id)
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

    @app.get("/api/v1/market-data/{dataset_id}/rows")
    def read_market_data_rows(dataset_id: str, limit: int = 200) -> dict[str, Any]:
        registration = get_market_data_repository().get_ref(dataset_id)
        if registration is None:
            raise HTTPException(status_code=404, detail="dataset not found")
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
        if fill_service is None:
            queued = enqueue_runtime_job(
                get_runtime_repository(),
                job_type="market_data_refresh",
                scope_key=f"dataset:{dataset_id}",
                payload={
                    "dataset_id": dataset_id,
                    "okx_mode": os.environ.get("OKX_MODE", "demo"),
                    "market_mode": "live",
                },
                current_step="queued",
            )
            if queued:
                return queued
        service = fill_service or fill_raw_candle_dataset
        try:
            return service(
                registration=registration,
                repository=get_market_data_repository(),
                adapter=OKXAdapter({"backend": "okx_cli", "mode": os.environ.get("OKX_MODE", "demo"), "market_mode": "live"}),
            )
        except ExchangeAdapterError as exc:
            raise HTTPException(status_code=502, detail=str(exc)) from exc

    @app.post("/api/v1/market-data/assets/{asset}/ema/refresh")
    def refresh_asset_ema_data(asset: str) -> dict[str, Any]:
        asset = asset.upper()
        queued = enqueue_runtime_job(
            get_runtime_repository(),
            job_type="market_data_ema_refresh",
            scope_key=f"asset:{asset}:ema",
            payload={"asset": asset},
            current_step="queued",
        )
        if queued:
            return queued
        return enrich_derived_ema_datasets(repository=get_market_data_repository(), asset=asset)

    @app.post("/api/v1/market-data/assets/{asset}/features/{family}/refresh")
    def refresh_asset_feature_family_data(asset: str, family: str) -> dict[str, Any]:
        asset = asset.upper()
        queued = enqueue_runtime_job(
            get_runtime_repository(),
            job_type="market_data_feature_refresh",
            scope_key=f"asset:{asset}:feature:{family}",
            payload={"asset": asset, "family": family},
            current_step="queued",
        )
        if queued:
            return queued
        return enrich_feature_family_datasets(
            repository=get_market_data_repository(),
            asset=asset,
            family=family,
            target_root=Path(".data/market-data"),
        )

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


def _stage0_universe_append_candidates(
    *,
    repository: Any,
    universe_run: dict[str, Any],
    asset_symbols: list[str] | None,
) -> list[dict[str, Any]]:
    engine_ids = list(universe_run.get("engine_filter") or [])
    signal_sets_for_engines: list[dict[str, Any]] = []
    if engine_ids:
        for engine_id in engine_ids:
            signal_sets_for_engines.extend(repository.list_signal_sets(engine_id))
    else:
        signal_sets_for_engines = repository.list_signal_sets()
    window_start = _date_string(universe_run["window_start"])
    window_end = _date_string(universe_run["window_end"])
    universe = build_stage0_universe(
        universe_run_id=str(universe_run["universe_run_id"]),
        window_start=window_start,
        window_end=window_end,
        forward_hours=int(universe_run["forward_hours"]),
        trigger_rate_threshold_pct=float(universe_run["trigger_rate_threshold_pct"]),
        train_start=_date_string(universe_run.get("train_start")) if universe_run.get("train_start") else None,
        train_end=_date_string(universe_run.get("train_end")) if universe_run.get("train_end") else None,
        walk_forward_start=_date_string(universe_run.get("walk_forward_start")) if universe_run.get("walk_forward_start") else None,
        walk_forward_end=_date_string(universe_run.get("walk_forward_end")) if universe_run.get("walk_forward_end") else None,
        signal_sets=signal_sets_for_engines,
        asset_symbols=asset_symbols,
        metrics_by_signal_set=repository.stage0_metrics_by_signal_set(),
        existing_rnd_by_signal_set=repository.existing_rnd_by_signal_set(),
        signal_counts_by_signal_set=repository.signal_counts_by_signal_set_window(
            window_start=window_start,
            window_end=window_end,
            engine_ids=engine_ids,
        ),
        split_signal_counts_by_signal_set=repository.split_signal_counts_by_signal_set(
            train_start=_date_string(universe_run.get("train_start")) if universe_run.get("train_start") else None,
            train_end=_date_string(universe_run.get("train_end")) if universe_run.get("train_end") else None,
            walk_forward_start=_date_string(universe_run.get("walk_forward_start")) if universe_run.get("walk_forward_start") else None,
            walk_forward_end=_date_string(universe_run.get("walk_forward_end")) if universe_run.get("walk_forward_end") else None,
            engine_ids=engine_ids,
        ),
        engine_ids=engine_ids,
    )
    return universe["candidates"]


def _resolve_stage1_seed_strategy(
    *,
    repository: Any,
    candidate: dict[str, Any],
    strategy_id: str,
    preference: str = "auto",
) -> dict[str, Any]:
    latest_seed = lambda: _latest_pair_seed(
        repository=repository,
        asset=candidate["asset"],
        signal_engine_id=candidate["signal_engine_id"],
        strategy_id=strategy_id,
    )
    engine_seed = lambda: _engine_base_seed(
        repository=repository,
        signal_engine_id=candidate["signal_engine_id"],
    )
    if preference == "latest_pair":
        chosen = latest_seed()
        if chosen:
            return chosen
        raise HTTPException(status_code=400, detail="No latest developed strategy is available for this pair yet")
    if preference == "engine_base":
        chosen = engine_seed()
        if chosen:
            return chosen
        raise HTTPException(status_code=400, detail="No engine base strategy template is configured for this signal engine")
    chosen = latest_seed()
    if chosen:
        return chosen
    chosen = engine_seed()
    if chosen:
        return chosen
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


def _materialize_execution_bundle(
    *,
    workspace_root: Path,
    repository: Any,
    session: dict[str, Any],
    gate: dict[str, Any],
    account_mode: str,
    execution_adapter: str,
    risk_limits: dict[str, Any],
) -> dict[str, Any]:
    if account_mode not in {"demo", "live", "paper"}:
        raise ValueError("account_mode must be demo, live, or paper")
    artifact_root = Path(session["artifact_root"])
    if not artifact_root.is_absolute():
        artifact_root = workspace_root / artifact_root
    promotion_root = artifact_root / "promotion"
    strategy_path = _stage1_strategy_path(artifact_root=artifact_root, promotion_root=promotion_root)
    selection = _resolve_stage4_promotion_candidate(promotion_root=promotion_root)
    selected_source = selection["source"]
    selected_result = selection["result"]
    selected_result_path = selection["result_path"]
    selected_summary_path = selection.get("summary_path")
    best = selection["best"]
    if selected_source == "stage4b_timing":
        strategy_path = _materialize_stage4b_timing_strategy_module(
            promotion_root=promotion_root,
            base_strategy_path=strategy_path,
            overlay=selection["overlay"],
        )

    simulation_inputs = selected_result.get("simulation_inputs") if isinstance(selected_result.get("simulation_inputs"), dict) else {}
    source_universe_run = repository.get_stage0_universe_run(session["source_universe_run_id"])
    if source_universe_run is None:
        raise ValueError("Source Stage 0 universe run is missing")
    forward_hours = int(source_universe_run["forward_hours"])
    evidence_refs = {
        "stage1_session_id": session["session_id"],
        "stage1_canonical_scores": str(promotion_root / "stage1a_canonical_full_cycle_scores.json"),
        "stage2_capture_curve": str(promotion_root / "stage2_capture_curve.json"),
        "stage3_optimal": str(promotion_root / "stage3_optimal.json"),
        "stage3_pyramid_optimal": str(promotion_root / "stage3_pyramid_optimal.json"),
        "stage4_realized_expectancy": str(promotion_root / "stage4_realized_expectancy.json"),
        "stage4_optimal": str(promotion_root / "stage4_optimal.json"),
        "stage4_summary": str(promotion_root / "stage4_summary.md"),
    }
    if selected_source == "stage4b_timing":
        evidence_refs.update(
            {
                "stage4b_timing_replay": str(selected_result_path),
                "stage4b_timing_overlay": str(promotion_root / "stage4b_timing" / "timing_overlay.json"),
                "stage4b_timing_summary": str(selected_summary_path or promotion_root / "stage4b_timing" / "timing_summary.md"),
                "stage4b_timing_strategy": str(strategy_path),
            }
        )
    execution_setup = {
        "schema_version": "0.1",
        "source": selected_source,
        "account_mode": account_mode,
        "execution_adapter": execution_adapter,
        "forward_hours": forward_hours,
        "hard_exit_after_hours": forward_hours,
        "stage4_candidate_id": best.get("candidate_id") or selected_result.get("best_candidate_id"),
        "setup": best.get("setup") or best,
        "sizing": {
            "source": selected_source,
            "initial_capital_usdt": simulation_inputs.get("initial_capital_usdt"),
            "margin_allocation_pct": simulation_inputs.get("margin_allocation_pct"),
            "leverage": simulation_inputs.get("leverage"),
        },
        "cost_assumptions": selected_result.get("cost_assumptions", {}),
        "slice_windows": selected_result.get("slice_windows", []),
        "training_window": {
            "start": _date_string(session["train_start"]),
            "end": _date_string(session["train_end"]),
        },
        "walk_forward_window": {
            "start": _date_string(session["walk_forward_start"]),
            "end": _date_string(session["walk_forward_end"]),
        },
        "promotion_selection": {
            "source": selected_source,
            "criterion": selection["criterion"],
            "warning": selection.get("warning"),
        },
    }

    engine_spec = _resolve_engine_spec_for_promotion(
        repository=repository,
        signal_engine_id=session["signal_engine_id"],
        version=session.get("signal_engine_version"),
    )
    try:
        validate_strategy_module(strategy_path)
        validate_execution_bundle_contract({"execution_setup": execution_setup})
    except ContractValidationError as exc:
        raise ValueError(str(exc)) from exc
    bundle_seed = {
        "asset": session["asset"],
        "signal_engine_id": session["signal_engine_id"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "session_id": session["session_id"],
        "stage4_candidate_id": execution_setup["stage4_candidate_id"],
        "promotion_source": selected_source,
        "content": {
            "strategy": strategy_path.read_text(),
            "execution_setup": execution_setup,
            "risk_limits": risk_limits,
            "evidence_refs": evidence_refs,
        },
    }
    content_hash = _stable_hash(bundle_seed)
    bundle_id = f"{session['asset'].lower()}-{session['signal_engine_id']}-{session['strategy_id']}-{content_hash[:12]}"
    bundle_root = workspace_root / "artifacts" / "execution_bundles" / bundle_id
    bundle_root.mkdir(parents=True, exist_ok=True)

    strategy_copy = bundle_root / "strategy.py"
    strategy_copy.write_text(strategy_path.read_text())
    stage4b_base_copy = None
    if selected_source == "stage4b_timing":
        base_strategy_sidecar = strategy_path.with_name("stage1a_base_strategy.py")
        if not base_strategy_sidecar.is_file():
            raise ValueError("Stage 4B timing base strategy sidecar is missing")
        stage4b_base_copy = bundle_root / "stage1a_base_strategy.py"
        stage4b_base_copy.write_text(base_strategy_sidecar.read_text())
    (bundle_root / "execution_setup.json").write_text(json.dumps(execution_setup, indent=2, sort_keys=True) + "\n")
    (bundle_root / "evidence_refs.json").write_text(json.dumps(evidence_refs, indent=2, sort_keys=True) + "\n")
    manifest = {
        "schema_version": "0.1",
        "bundle_id": bundle_id,
        "asset": session["asset"],
        "instrument": f"{session['asset']}-USDT-SWAP",
        "signal_engine_id": session["signal_engine_id"],
        "signal_engine_version": session["signal_engine_version"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "source_stage1_session_id": session["session_id"],
        "account_mode": account_mode,
        "execution_adapter": execution_adapter,
        "promotion_source": selected_source,
        "promotion_selection": execution_setup["promotion_selection"],
        "content_hash": content_hash,
        "contract_version": "engine_strategy_contract.v1",
        "validated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "signal_engine_spec": engine_spec.to_mapping(),
        "created_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "gate": _relative_nested_paths(workspace_root, gate),
    }
    (bundle_root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    checksums = {
        "strategy.py": _file_sha256(strategy_copy),
        "execution_setup.json": _file_sha256(bundle_root / "execution_setup.json"),
        "evidence_refs.json": _file_sha256(bundle_root / "evidence_refs.json"),
        "manifest.json": _file_sha256(bundle_root / "manifest.json"),
    }
    if stage4b_base_copy is not None:
        checksums["stage1a_base_strategy.py"] = _file_sha256(stage4b_base_copy)
    (bundle_root / "checksums.json").write_text(json.dumps(checksums, indent=2, sort_keys=True) + "\n")
    (bundle_root / "bundle.json").write_text(
        json.dumps(
            {
                **manifest,
                "execution_setup": execution_setup,
                "risk_limits": risk_limits,
                "evidence_refs": evidence_refs,
                "checksums": checksums,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    )
    return {
        "bundle_id": bundle_id,
        "asset": session["asset"],
        "instrument": f"{session['asset']}-USDT-SWAP",
        "signal_engine_id": session["signal_engine_id"],
        "signal_engine_version": session["signal_engine_version"],
        "strategy_id": session["strategy_id"],
        "strategy_version": session["strategy_version"],
        "source_stage1_session_id": session["session_id"],
        "source_stage4_result_path": str(selected_result_path),
        "bundle_uri": str(bundle_root),
        "strategy_module_ref": str(strategy_copy),
        "execution_setup": execution_setup,
        "risk_limits": risk_limits,
        "evidence_refs": evidence_refs,
        "content_hash": content_hash,
        "status": "promoted",
    }


def _stage1_strategy_path(*, artifact_root: Path, promotion_root: Path) -> Path:
    strategy_path = promotion_root / "frozen_stage1a_strategy_module" / "strategy.py"
    if not strategy_path.is_file():
        fallback_strategy_path = artifact_root / "strategy_module" / "strategy.py"
        if fallback_strategy_path.is_file():
            strategy_path = fallback_strategy_path
        else:
            raise ValueError("Frozen strategy module is missing")
    return strategy_path


def _resolve_stage4_promotion_candidate(*, promotion_root: Path) -> dict[str, Any]:
    optimal_path = promotion_root / "stage4_optimal.json"
    realized_path = promotion_root / "stage4_realized_expectancy.json"
    summary_path = promotion_root / "stage4_summary.md"
    if not optimal_path.is_file():
        raise ValueError("Stage 4 optimal artifact is missing")
    if not realized_path.is_file():
        raise ValueError("Stage 4 realized expectancy artifact is missing")
    optimal = _read_json_file(optimal_path)
    realized = _read_json_file(realized_path)
    stage4a_best = optimal.get("best") or realized.get("best_candidate") or {}
    if not stage4a_best:
        raise ValueError("Stage 4 optimal artifact does not include a best candidate")
    candidates = _stage4a_promotion_candidates(
        realized=realized,
        fallback_best=stage4a_best,
        realized_path=realized_path,
        optimal_path=optimal_path,
        summary_path=summary_path,
    )
    candidates.extend(_stage4b_promotion_candidates(promotion_root=promotion_root, latest_stage4a_run_id=str(realized.get("run_id") or "")))
    protected_eligible = [candidate for candidate in candidates if _candidate_has_protected_sl(candidate["best"]) and _walk_forward_net_pnl_pct(candidate["best"]) > 0]
    if protected_eligible:
        selected = max(protected_eligible, key=_promotion_rank_key)
        return {**selected, "criterion": "protected_walk_forward_net_pnl_pct"}
    eligible = [candidate for candidate in candidates if _walk_forward_net_pnl_pct(candidate["best"]) > 0]
    if eligible:
        return max(eligible, key=_promotion_rank_key)
    best = max(candidates, key=lambda candidate: (_overall_net_pnl_usdt(candidate["best"]), candidate["source"] == "stage4_realized_expectancy"))
    return {**best, "criterion": "overall_net_pnl_fallback", "warning": "weak_walk_forward_fallback"}


def _stage4a_promotion_candidates(
    *,
    realized: dict[str, Any],
    fallback_best: dict[str, Any],
    realized_path: Path,
    optimal_path: Path,
    summary_path: Path,
) -> list[dict[str, Any]]:
    rows = [row for row in realized.get("candidates", []) if isinstance(row, dict)]
    if not rows:
        rows = [fallback_best]
    return [
        {
            "source": "stage4_realized_expectancy",
            "result": realized,
            "result_path": realized_path,
            "optimal_path": optimal_path,
            "summary_path": summary_path,
            "best": row,
            "criterion": "walk_forward_net_pnl_pct",
            "overlay": None,
        }
        for row in rows
        if row.get("candidate_id")
    ]


def _stage4b_promotion_candidates(*, promotion_root: Path, latest_stage4a_run_id: str) -> list[dict[str, Any]]:
    timing_root = promotion_root / "stage4b_timing"
    replay_path = timing_root / "timing_replay.json"
    overlay_path = timing_root / "timing_overlay.json"
    ledger_path = timing_root / "timing_trade_ledger.json"
    summary_path = timing_root / "timing_summary.md"
    if not (replay_path.is_file() and overlay_path.is_file() and ledger_path.is_file() and summary_path.is_file()):
        return []
    replay = _read_json_file(replay_path)
    overlay = _read_json_file(overlay_path)
    if str(overlay.get("source_stage4_run_id") or "") != latest_stage4a_run_id:
        return []
    rows = [row for row in replay.get("candidates", []) if isinstance(row, dict)]
    if not rows and isinstance(replay.get("best_candidate"), dict):
        rows = [replay["best_candidate"]]
    return [
        {
            "source": "stage4b_timing",
            "result": replay,
            "result_path": replay_path,
            "summary_path": summary_path,
            "best": row,
            "criterion": "walk_forward_net_pnl_pct",
            "overlay": overlay,
        }
        for row in rows
        if row.get("candidate_id")
    ]


def _promotion_rank_key(candidate: dict[str, Any]) -> tuple[float, float, float, bool]:
    best = candidate["best"]
    return (
        _walk_forward_net_pnl_pct(best),
        _walk_forward_profit_factor(best),
        _overall_net_pnl_usdt(best),
        candidate["source"] == "stage4_realized_expectancy",
    )


def _candidate_has_protected_sl(candidate: dict[str, Any]) -> bool:
    setup = candidate.get("setup") if isinstance(candidate.get("setup"), dict) else candidate
    if bool(setup.get("protection_enabled")):
        return True
    side_policies = setup.get("side_policies") if isinstance(setup.get("side_policies"), dict) else {}
    return any(isinstance(policy, dict) and bool(policy.get("protection_enabled")) for policy in side_policies.values())


def _walk_forward_net_pnl_pct(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("net_pnl_pct"), 0.0)


def _walk_forward_profit_factor(best: dict[str, Any]) -> float:
    wf = (best.get("slices") or {}).get("walk_forward_test") or {}
    return _float_or_default(wf.get("profit_factor"), 0.0)


def _overall_net_pnl_usdt(best: dict[str, Any]) -> float:
    account = best.get("account") or {}
    return _float_or_default(account.get("net_pnl_usdt"), 0.0)


def _float_or_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _materialize_stage4b_timing_strategy_module(*, promotion_root: Path, base_strategy_path: Path, overlay: dict[str, Any]) -> Path:
    wrapper_root = promotion_root / "frozen_stage4b_timing_strategy_module"
    wrapper_root.mkdir(parents=True, exist_ok=True)
    base_copy = wrapper_root / "stage1a_base_strategy.py"
    base_copy.write_text(base_strategy_path.read_text())
    wrapper_path = wrapper_root / "strategy.py"
    wrapper_path.write_text(_render_stage4b_timing_strategy_wrapper(overlay=overlay))
    return wrapper_path


def _render_stage4b_timing_strategy_wrapper(*, overlay: dict[str, Any]) -> str:
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            "from datetime import UTC, datetime",
            "import importlib.util",
            "import json",
            "from pathlib import Path",
            "from typing import Any",
            "",
            f"TIMING_OVERLAY = json.loads({json.dumps(json.dumps(overlay, sort_keys=True))})",
            "_BASE_MODULE = None",
            "",
            "def _load_base_module():",
            "    global _BASE_MODULE",
            "    if _BASE_MODULE is not None:",
            "        return _BASE_MODULE",
            "    path = Path(__file__).with_name('stage1a_base_strategy.py')",
            "    spec = importlib.util.spec_from_file_location('stage4b_stage1a_base_strategy', path)",
            "    if spec is None or spec.loader is None:",
            "        raise ImportError(f'cannot load base strategy: {path}')",
            "    module = importlib.util.module_from_spec(spec)",
            "    spec.loader.exec_module(module)",
            "    _BASE_MODULE = module",
            "    return module",
            "",
            "def decide(context: dict[str, Any]) -> dict[str, Any]:",
            "    base = dict(_load_base_module().decide(context))",
            "    if _timing_filter_matches(context, base):",
            "        return _timing_skip_decision(base)",
            "    return base",
            "",
            "def manage_position(context: dict[str, Any]) -> dict[str, Any]:",
            "    module = _load_base_module()",
            "    if hasattr(module, 'manage_position'):",
            "        return module.manage_position(context)",
            "    return {'action': 'HOLD'}",
            "",
            "def _timing_filter_matches(context: dict[str, Any], decision: dict[str, Any]) -> bool:",
            "    action = str(decision.get('action') or decision.get('trade_action') or '').upper()",
            "    direction = str(decision.get('direction') or '').upper()",
            "    if action != 'ENTER' or direction not in {'LONG', 'SHORT'}:",
            "        return False",
            "    applies_to = str(TIMING_OVERLAY.get('applies_to') or 'all').upper()",
            "    if applies_to in {'LONG', 'SHORT'} and direction != applies_to:",
            "        return False",
            "    timestamp = _packet_timestamp(context)",
            "    if timestamp is None:",
            "        return False",
            "    hours = set(TIMING_OVERLAY.get('exclude_utc_hours') or [])",
            "    weekdays = set(TIMING_OVERLAY.get('exclude_utc_weekdays') or [])",
            "    if hours and timestamp.hour not in hours:",
            "        return False",
            "    if weekdays and timestamp.weekday() not in weekdays:",
            "        return False",
            "    return True",
            "",
            "def _packet_timestamp(context: dict[str, Any]) -> datetime | None:",
            "    candidates = [context.get('timestamp'), context.get('signal_ts')]",
            "    packet = context.get('packet') if isinstance(context.get('packet'), dict) else context",
            "    candidates.extend([packet.get('timestamp'), packet.get('signal_ts')])",
            "    signal = context.get('signal') if isinstance(context.get('signal'), dict) else {}",
            "    payload = signal.get('payload') if isinstance(signal.get('payload'), dict) else {}",
            "    candidates.extend([signal.get('timestamp'), signal.get('signal_ts'), payload.get('timestamp'), payload.get('signal_ts')])",
            "    for value in candidates:",
            "        if not value:",
            "            continue",
            "        if isinstance(value, datetime):",
            "            return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)",
            "        if isinstance(value, str):",
            "            try:",
            "                return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(UTC)",
            "            except ValueError:",
            "                continue",
            "    return None",
            "",
            "def _timing_skip_decision(base: dict[str, Any]) -> dict[str, Any]:",
            "    diagnostics = dict(base.get('diagnostics') or {})",
            "    diagnostics['stage4b_timing_overlay'] = {",
            "        'exclude_utc_hours': TIMING_OVERLAY.get('exclude_utc_hours', []),",
            "        'exclude_utc_weekdays': TIMING_OVERLAY.get('exclude_utc_weekdays', []),",
            "        'applies_to': TIMING_OVERLAY.get('applies_to', 'all'),",
            "    }",
            "    return {",
            "        **base,",
            "        'action': 'SKIP',",
            "        'trade_action': 'SKIP',",
            "        'direction': 'FLAT',",
            "        'reason_code': 'timing_filter_utc_window',",
            "        'diagnostics': diagnostics,",
            "    }",
            "",
        ]
    )


def _resolve_engine_spec_for_promotion(
    *,
    repository: Any,
    signal_engine_id: str,
    version: str | None,
) -> SignalEngineSpec:
    for engine in repository.list_signal_engines():
        if engine.get("signal_engine_id") != signal_engine_id:
            continue
        if version is not None and engine.get("version") != version:
            continue
        return SignalEngineSpec.from_mapping({**engine, "output_envelope_version": engine.get("output_envelope_version") or "signal_packet.v2"})
    registry_path = Path.cwd() / "artifacts" / "signal_engine" / "engine_registry.json"
    if registry_path.is_file():
        registry = json.loads(registry_path.read_text())
        entry = registry.get(signal_engine_id) if isinstance(registry, dict) else None
        if isinstance(entry, dict):
            return SignalEngineSpec.from_mapping({**entry, "output_envelope_version": entry.get("output_envelope_version") or "signal_packet.v2"})
    raise ValueError(f"Signal engine spec not found for {signal_engine_id}")


def _signal_engine_catalog(repository: Any, *, workspace_root: Path) -> list[dict[str, Any]]:
    engines_by_id = {engine["signal_engine_id"]: dict(engine) for engine in repository.list_signal_engines()}
    registry_path = workspace_root / "artifacts" / "signal_engine" / "engine_registry.json"
    if registry_path.is_file():
        registry = json.loads(registry_path.read_text())
        if isinstance(registry, dict):
            for signal_engine_id, entry in registry.items():
                if not isinstance(entry, dict) or signal_engine_id in engines_by_id:
                    continue
                engines_by_id[signal_engine_id] = {
                    **entry,
                    "signal_set_count": 0,
                    "packet_count": 0,
                }
    return sorted(engines_by_id.values(), key=_signal_engine_catalog_sort_key)


def _signal_engine_catalog_sort_key(engine: dict[str, Any]) -> tuple[bool, float, str]:
    created_at = engine.get("created_at")
    created_at_ts = _signal_engine_created_at_timestamp(created_at)
    return (created_at_ts is None, -(created_at_ts or 0.0), str(engine.get("signal_engine_id") or ""))


def _signal_engine_created_at_timestamp(value: Any) -> float | None:
    if isinstance(value, datetime):
        return value.timestamp()
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed.timestamp()
    return None


def _materialize_signal_engine(repository: Any, *, workspace_root: Path, signal_engine_id: str) -> dict[str, Any] | None:
    existing = next((engine for engine in repository.list_signal_engines() if engine["signal_engine_id"] == signal_engine_id), None)
    if existing is not None:
        return existing
    entry = _registry_signal_engine(workspace_root=workspace_root, signal_engine_id=signal_engine_id)
    if entry is None:
        return None
    registration = {
        "signal_engine_id": entry["signal_engine_id"],
        "name": entry.get("name") or entry["signal_engine_id"],
        "description": entry.get("description", ""),
        "version": entry.get("version") or "0.1",
        "code_ref": entry.get("code_ref") if isinstance(entry.get("code_ref"), dict) else {},
        "supported_input_data_types": entry.get("supported_input_data_types") or ["candles"],
        "required_data": entry.get("required_data") or [],
        "output_envelope_version": entry.get("output_envelope_version") or "signal_packet.v2",
        "runtime_entrypoint": entry.get("runtime_entrypoint") or "",
        "live_scanner_entrypoint": entry.get("live_scanner_entrypoint"),
        "configuration_schema": entry.get("configuration_schema") if isinstance(entry.get("configuration_schema"), dict) else {},
    }
    repository.register_signal_engine(registration)
    return next((engine for engine in repository.list_signal_engines() if engine["signal_engine_id"] == signal_engine_id), registration)


def _registry_signal_engine(*, workspace_root: Path, signal_engine_id: str) -> dict[str, Any] | None:
    registry_path = workspace_root / "artifacts" / "signal_engine" / "engine_registry.json"
    if not registry_path.is_file():
        return None
    registry = json.loads(registry_path.read_text())
    entry = registry.get(signal_engine_id) if isinstance(registry, dict) else None
    return entry if isinstance(entry, dict) else None


def _create_canonical_signal_set(*, repository: Any, engine: dict[str, Any], asset: str) -> dict[str, Any]:
    asset = asset.upper()
    required_refs, missing = _required_data_refs(repository=repository, engine=engine, asset=asset)
    if missing:
        raise ValueError(f"Missing required local data for {asset}: {', '.join(missing)}")
    signal_engine_id = engine["signal_engine_id"]
    signal_set_id = f"{asset}-{signal_engine_id}-canonical"
    signal_set_key = f"{signal_engine_id}:{asset}:{signal_set_id}"
    existing = repository.get_signal_set(signal_set_key)
    if existing is not None:
        return _relative_nested_paths(Path.cwd(), existing)
    instrument = _instrument_for_refs(asset=asset, refs=required_refs)
    configuration_schema = engine.get("configuration_schema") if isinstance(engine.get("configuration_schema"), dict) else {}
    parameters = configuration_schema.get("default_parameters") if isinstance(configuration_schema.get("default_parameters"), dict) else {}
    signal_set = {
        "signal_set_key": signal_set_key,
        "signal_set_id": signal_set_id,
        "signal_engine_id": signal_engine_id,
        "signal_engine_version": engine.get("version") or "0.1",
        "asset": asset,
        "instrument": instrument,
        "start_ts": None,
        "end_ts": None,
        "packet_count": 0,
        "payload_schema": engine.get("output_envelope_version") or "signal_packet.v2",
        "source_path": "canonicalized:signals",
        "manifest": {
            "schema_version": "0.1",
            "signal_set_id": signal_set_id,
            "signal_engine_id": signal_engine_id,
            "asset": asset,
            "instrument": instrument,
            "parameters": dict(parameters),
            "data_refs": [ref["dataset_id"] for ref in required_refs if ref.get("dataset_id")],
            "scan_coverage": {"source": "parquet_market_data", "start_ts": None, "end_ts": None},
        },
    }
    repository.upsert_signal_set(signal_set)
    return _relative_nested_paths(Path.cwd(), repository.get_signal_set(signal_set_key) or signal_set)


def _required_data_refs(*, repository: Any, engine: dict[str, Any], asset: str) -> tuple[list[dict[str, Any]], list[str]]:
    refs: list[dict[str, Any]] = []
    missing: list[str] = []
    for requirement in engine.get("required_data") or []:
        data_type = str(requirement.get("data_type") or "").strip()
        origin = requirement.get("origin") or requirement.get("data_origin") or "raw"
        timeframe = requirement.get("timeframe") or "5m"
        ref = repository.get_candle_ref(asset=asset, data_type=data_type or "candles", origin=origin, timeframe=timeframe)
        if ref is None:
            missing.append(f"{origin} {data_type or 'candles'} {timeframe}")
            continue
        refs.append(ref)
    return refs, missing


def _instrument_for_refs(*, asset: str, refs: list[dict[str, Any]]) -> str:
    for ref in refs:
        if ref.get("instrument"):
            return str(ref["instrument"])
    return f"{asset}-USDT-SWAP"


def _read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def _sanitize_exchange_error(message: str) -> str:
    lines = [line.strip() for line in str(message).splitlines() if line.strip()]
    safe_lines: list[str] = []
    blocked_terms = ("secret", "api_key", "apikey", "passphrase", "password", "token")
    for line in lines:
        lowered = line.lower()
        if any(term in lowered for term in blocked_terms):
            continue
        safe_lines.append(line)
    return safe_lines[0] if safe_lines else "Exchange adapter could not connect"


def _stable_hash(payload: Any) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


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


def _portfolio_stage4_complete_sessions(
    workspace_root: Path,
    *,
    sessions: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    accepted_candidate_ids = {
        candidate["candidate_id"]
        for candidate in candidates
        if candidate.get("acceptance_status") == "accepted"
    }
    complete = []
    for session in sessions:
        if session.get("source_candidate_id") not in accepted_candidate_ids:
            continue
        artifact_root = Path(str(session.get("artifact_root") or ""))
        if not artifact_root.is_absolute():
            artifact_root = workspace_root / artifact_root
        promotion_root = artifact_root / "promotion"
        if (promotion_root / "stage4_realized_expectancy.json").is_file() and (promotion_root / "stage1a_canonical_full_cycle_scores.json").is_file():
            complete.append(session)
    return complete


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
    if gate and (gate.get("stage3_grid") or {}).get("exact_protection_complete"):
        return (
            "stage3_local_variants_ready",
            "stage3_exact_protection_complete",
            _next_action("run_stage3_local_variants", "Run Local Variants", target_stage="stage3"),
        )
    if gate and (gate.get("stage3_grid") or {}).get("fixed_sl_complete"):
        return (
            "stage3_exact_protection_ready",
            "stage3_fixed_sl_complete",
            _next_action("run_stage3_exact_protection", "Run Exact Protection", target_stage="stage3"),
        )
    if gate and (gate.get("stage2_exit_policy") or {}).get("exists"):
        return (
            "stage3_ready",
            "stage2_policy_promoted",
            _next_action("run_stage3_fixed_sl", "Run Fixed SL", target_stage="stage3"),
        )
    if gate and (gate.get("stage2_capture") or {}).get("exists"):
        return (
            "stage2_policy_ready",
            "stage2_complete",
            _next_action("promote_stage2_exit_policy", "Promote Exit Policy", target_stage="stage2"),
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


def _stage2_policy_allowed_values(capture: dict[str, Any], *, key: str, fallback_key: str) -> set[float]:
    values = capture.get(key)
    if not isinstance(values, list) or not values:
        values = list((capture.get(fallback_key) or {}).keys())
    allowed: set[float] = set()
    for value in values:
        try:
            allowed.add(round(float(value), 10))
        except (TypeError, ValueError):
            continue
    return allowed


def _normalize_stage2_side_policies(request: Stage2ExitPolicyRequest) -> tuple[str, dict[str, dict[str, float]]]:
    if request.side_policies:
        missing = [side for side in ("LONG", "SHORT") if side not in request.side_policies]
        if missing:
            raise ValueError(f"Stage 2 side-specific policy requires both LONG and SHORT: missing {', '.join(missing)}")
        return (
            "side_specific",
            {
                side: _stage2_policy_values_to_dict(request.side_policies[side])
                for side in ("LONG", "SHORT")
            },
        )
    required = ("lock_profit_pct", "initial_sl_pct", "protect_trigger_pct", "trail_sl_pct")
    missing = [key for key in required if getattr(request, key) is None]
    if missing:
        raise ValueError(f"Stage 2 shared policy is missing values: {', '.join(missing)}")
    shared = {
        "lock_profit_pct": float(request.lock_profit_pct),
        "initial_sl_pct": float(request.initial_sl_pct),
        "protect_trigger_pct": float(request.protect_trigger_pct),
        "trail_sl_pct": float(request.trail_sl_pct),
    }
    return "shared", {"LONG": dict(shared), "SHORT": dict(shared)}


def _stage2_policy_values_to_dict(values: Stage2ExitPolicyValues) -> dict[str, float]:
    return {
        "lock_profit_pct": float(values.lock_profit_pct),
        "initial_sl_pct": float(values.initial_sl_pct),
        "protect_trigger_pct": float(values.protect_trigger_pct),
        "trail_sl_pct": float(values.trail_sl_pct),
    }


def _validate_stage2_policy_values(
    policy: dict[str, float],
    *,
    allowed_tp: set[float],
    allowed_sl: set[float],
    label: str,
) -> None:
    invalid_tp = [
        key
        for key in ("lock_profit_pct", "protect_trigger_pct", "trail_sl_pct")
        if round(float(policy[key]), 10) not in allowed_tp
    ]
    if invalid_tp:
        raise ValueError(f"{label} Stage 2 policy values must be selected from the capture curve TP band: {', '.join(invalid_tp)}")
    if round(float(policy["initial_sl_pct"]), 10) not in allowed_sl:
        raise ValueError(f"{label} initial_sl_pct must be selected from the Stage 2 matched adverse SL band")
    if policy["trail_sl_pct"] > policy["protect_trigger_pct"]:
        raise ValueError(f"{label} trail_sl_pct cannot be greater than protect_trigger_pct")
    if policy["protect_trigger_pct"] > policy["lock_profit_pct"]:
        raise ValueError(f"{label} protect_trigger_pct cannot be greater than lock_profit_pct")


def _ensure_stage1_session_mutable(session: dict[str, Any]) -> None:
    if session.get("status") == "stage1a_frozen":
        raise HTTPException(status_code=409, detail="Stage 1 session is frozen")
    gate = build_stage1_gate_summary(workspace_root=Path.cwd(), session=session)
    if (gate.get("canonical_readout") or {}).get("exists"):
        raise HTTPException(status_code=409, detail="Stage 1 session is frozen")


def _ensure_stage1_session_resettable(*, repository: Any, session: dict[str, Any]) -> None:
    finder = getattr(repository, "list_execution_bundles_for_stage1_session", None)
    if callable(finder):
        bundles = finder(session["session_id"])
    else:
        bundles = [
            bundle
            for bundle in repository.list_execution_bundles()
            if bundle.get("source_stage1_session_id") == session["session_id"]
        ]
    if bundles:
        raise HTTPException(status_code=409, detail="Stage 1 session has a promoted execution bundle")


def _ensure_stage0_universe_run_deletable(*, repository: Any, linked_sessions: list[dict[str, Any]]) -> None:
    finder = getattr(repository, "list_execution_bundles_for_stage1_session", None)
    for session in linked_sessions:
        if callable(finder):
            bundles = finder(session["session_id"])
        else:
            bundles = [
                bundle
                for bundle in repository.list_execution_bundles()
                if bundle.get("source_stage1_session_id") == session["session_id"]
            ]
        if bundles:
            raise HTTPException(status_code=409, detail="Training pool has linked promoted execution bundles")


def _delete_filesystem_path(path_value: Any) -> bool:
    if path_value in (None, ""):
        return False
    path = Path(str(path_value))
    if not path.is_absolute():
        path = Path.cwd() / path
    if not path.exists():
        return False
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()
    return True


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
