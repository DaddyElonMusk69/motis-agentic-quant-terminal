import React from "react";
import ReactDOM from "react-dom/client";
import { QueryClient, QueryClientProvider, useMutation, useQuery } from "@tanstack/react-query";
import { Activity, ChevronRight, Database, FlaskConical, Lock, Play, RefreshCw, Radar, Shield, Terminal, Trash2, UploadCloud } from "lucide-react";
import "./styles.css";

const queryClient = new QueryClient();
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";
type ActiveView = "dashboard" | "data" | "engines" | "research" | "trading";
type ResearchSubView = "stage0-batches" | "development";
type ResearchStageId = "stage0" | "stage1" | "stage2" | "stage3" | "stage4";
type Stage1SampleRole = "training" | "walk_forward_test";
type Stage1SampleMethod = Stage1SampleRole;

const researchStages: Array<{
  id: ResearchStageId;
  label: string;
  title: string;
  output: string;
}> = [
  { id: "stage0", label: "Stage 0", title: "Universe", output: "Tradable pairs" },
  { id: "stage1", label: "Stage 1", title: "Direction", output: "Match set" },
  { id: "stage2", label: "Stage 2", title: "Travel Capture", output: "Capture curve" },
  { id: "stage3", label: "Stage 3", title: "Execution Setup", output: "TP/SL setup" },
  { id: "stage4", label: "Stage 4", title: "Expectancy", output: "Promotion evidence" }
];

type Dataset = {
  dataset_id: string;
  asset: string;
  instrument: string;
  data_type: string;
  timeframe: string | null;
  data_origin: string;
  start_ts: string | null;
  end_ts: string | null;
  row_count: number | null;
  storage_backend: string;
  storage_uri: string;
  quality_status: string;
  ingestion_version: string;
};

type CatalogAsset = {
  asset: string;
  datasets: Dataset[];
};

type CatalogResponse = {
  summary: {
    assets: number;
    datasets: number;
    data_types: string[];
  };
  assets: CatalogAsset[];
};

type RefreshPlan = {
  dataset_id: string;
  status: string;
  from_ts?: string;
  to_ts?: string;
  start_ts?: string;
  end_ts?: string;
  rows_added?: number;
  row_count?: number;
  derived_rebuilt?: Array<{ dataset_id: string; timeframe: string; row_count: number }>;
  reason?: string;
};

type SignalEngine = {
  signal_engine_id: string;
  name: string;
  description: string;
  version: string | null;
  code_ref: Record<string, unknown> | null;
  runtime_entrypoint: string | null;
  live_scanner_entrypoint: string | null;
  signal_set_count: number;
  packet_count: number;
};

type SignalSet = {
  signal_set_key: string;
  signal_set_id: string;
  signal_engine_id: string;
  signal_engine_version: string;
  asset: string;
  instrument: string;
  start_ts: string | null;
  end_ts: string | null;
  packet_start_ts?: string | null;
  packet_end_ts?: string | null;
  coverage_start_ts?: string | null;
  coverage_end_ts?: string | null;
  packet_count: number;
  payload_schema: string;
  source_path: string;
  manifest: Record<string, unknown>;
};

type SignalPoolExtendResult = {
  status: string;
  signal_engine_id: string;
  asset: string;
  signal_set_key: string;
  target_end_ts: string;
  raw_candle_end_ts: string;
  previous_signal_end_ts?: string | null;
  scan_coverage_end_ts?: string | null;
  final_signal_end_ts?: string | null;
  coverage_end_ts?: string | null;
  previous_end_ts?: string | null;
  final_end_ts?: string | null;
  generated_packet_count: number;
  appended_packet_count: number;
  final_packet_count?: number | null;
  local_only: boolean;
};

type SignalRecord = {
  signal_id: string;
  signal_set_key: string | null;
  signal_engine_id: string;
  signal_engine_version: string;
  asset: string;
  instrument: string;
  timestamp: string;
  data_refs: string[];
  payload_schema: string;
  payload: Record<string, unknown>;
};

type Stage0UniverseRun = {
  universe_run_id: string;
  config_hash: string;
  window_start: string;
  window_end: string;
  train_start?: string | null;
  train_end?: string | null;
  walk_forward_start?: string | null;
  walk_forward_end?: string | null;
  forward_hours: number;
  trigger_rate_threshold_pct: number;
  engine_filter: string[];
  status: string;
  summary: {
    total_candidates?: number;
    accepted?: number;
    watchlist?: number;
    pending_stage0?: number;
    failed?: number;
  };
};

type Stage0UniverseCandidate = {
  candidate_id: string;
  universe_run_id: string;
  signal_set_key: string;
  signal_engine_id: string;
  signal_engine_version: string;
  asset: string;
  signal_set_id: string;
  packet_count: number;
  trigger_rate_pct: number | null;
  branch_path: string;
  acceptance_status: string;
  duplicate_status: string;
  existing_strategy_id: string | null;
  last_error?: Record<string, unknown>;
  metrics: Record<string, unknown>;
};

type Stage0UniverseResponse = {
  run: Stage0UniverseRun;
  candidates: Stage0UniverseCandidate[];
};

type Stage0ExecutionResponse = {
  candidate: Stage0UniverseCandidate;
  commands: Record<string, string[]>;
  artifact_root: string;
};

type Stage0BatchExecutionResponse = {
  run: Stage0UniverseRun;
  candidates: Stage0UniverseCandidate[];
  results: Stage0ExecutionResponse[];
  errors: Array<{ candidate_id: string; asset: string; detail: string }>;
  summary: {
    requested: number;
    succeeded: number;
    failed: number;
    skipped: number;
    remaining_pending: number;
  };
};

type Stage1ResearchSession = {
  session_id: string;
  source_universe_run_id: string;
  source_candidate_id: string;
  signal_set_key: string;
  signal_engine_id: string;
  signal_engine_version: string;
  asset: string;
  signal_set_id: string;
  strategy_id: string;
  strategy_version: string;
  train_start: string;
  train_end: string;
  walk_forward_start: string;
  walk_forward_end: string;
  artifact_root: string;
  status: string;
  seed_strategy_source_type?: string | null;
  seed_strategy_source_path?: string | null;
  seed_strategy_source_version?: string | null;
  seed_strategy_source_session_id?: string | null;
  manifest: Record<string, unknown>;
};

type Stage1IterationBundle = {
  iteration_id: string;
  iteration_root: string;
  manifest_path: string;
  handoff_path: string;
  signal_sample_path: string;
  agent_prompt_path: string;
  builder_prompt_path?: string;
  builder_training_sample_path?: string;
  strategy_snapshot_path: string;
  bundle_role?: string;
  sample_method?: string;
};

type Stage1IterationSummary = Stage1IterationBundle & {
  signal_count?: number;
  status?: string;
  scores?: Record<string, Stage1TrainingScore>;
  has_training_score: boolean;
  training_score?: Stage1TrainingScore | null;
  has_failure_audit: boolean;
  failure_audit?: Stage1FailureAudit | null;
};

type Stage1TrainingScore = {
  decisions_path: string;
  scores_path: string;
  summary_path: string;
  metrics: {
    total: number;
    matches: number;
    mismatches: number;
    neutral: number;
    scoreable: number;
    directional_agreement: number;
    promotion_threshold_pct: number;
    passes_threshold: boolean;
  };
};

type Stage1FailureAudit = {
  audit_json_path: string;
  audit_md_path: string;
  agent_prompt_path: string;
  sample_role?: Stage1SampleRole;
  agent_handoff_policy?: string;
  metrics: {
    total: number;
    failure_count: number;
    mismatch_count: number;
    neutral_count: number;
    protected_count: number;
  };
};

type Stage1AgentPrompt = {
  session_id: string;
  iteration_id: string;
  prompt_type: string;
  prompt_path: string;
  prompt: string;
};

type Stage1GateSummary = {
  session_id: string;
  status: string;
  ready_to_freeze: boolean;
  blockers: string[];
  roles: Record<string, {
    role: string;
    label: string;
    status: "pass" | "fail" | "missing";
    blocker?: string | null;
    score?: (Stage1TrainingScore & {
      iteration_id?: string;
      sample_method?: string;
    }) | null;
  }>;
  canonical_readout: {
    exists: boolean;
    scores_path?: string | null;
    decisions_path?: string | null;
    summary_path?: string | null;
    frozen_strategy_path?: string | null;
    metrics: Partial<Stage1TrainingScore["metrics"]>;
    slice_metrics: Record<string, Partial<Stage1TrainingScore["metrics"]>>;
    match_count: number;
  };
  stage2_capture: Stage2CaptureState;
  stage3_grid: Stage3GridState;
  stage3_pyramid: Stage3PyramidState;
  stage4_realized_expectancy: Stage4RealizedExpectancyState;
  downstream_contract: {
    stage2_stage3: string;
    stage4: string;
  };
};

type Stage2CaptureRate = {
  reached: number;
  total: number;
  rate: number;
};

type Stage2CaptureState = {
  exists: boolean;
  capture_curve_path?: string | null;
  per_signal_path?: string | null;
  summary_path?: string | null;
  metrics: {
    total_match_signals?: number;
    slice_counts?: Record<string, number>;
  };
  results: Record<string, Record<string, Stage2CaptureRate>>;
};

type Stage3GridSetup = {
  tp: number;
  sl: number;
  entry_model?: string;
  tp_count: number;
  sl_count: number;
  neither: number;
  total: number;
  wr: number;
  expectancy: number;
  profit_factor: number;
  pnl_pct: number;
  rr_ratio: number;
  slice_split?: Record<string, {
    tp_count: number;
    sl_count: number;
    neither: number;
    total: number;
  }>;
  side_split?: Record<string, {
    tp_count: number;
    sl_count: number;
    neither: number;
    total: number;
  }>;
};

type Stage3GridState = {
  exists: boolean;
  grid_results_path?: string | null;
  optimal_path?: string | null;
  stage4_candidates_path?: string | null;
  summary_path?: string | null;
  total_signals: number;
  forward_hours?: number | null;
  leverage?: number | null;
  best: Partial<Stage3GridSetup>;
  top_5: Stage3GridSetup[];
};

type Stage3PyramidRecord = {
  step_pct: number | null;
  pnl_pct: number;
  delta_vs_baseline_pct?: number;
  avg_legs_per_signal: number;
  wins: number;
  losses: number;
  comparison?: string;
};

type Stage3PyramidState = {
  exists: boolean;
  results_path?: string | null;
  optimal_path?: string | null;
  stage4_candidates_path?: string | null;
  summary_path?: string | null;
  total_signals: number;
  tp_pct?: number | null;
  sl_pct?: number | null;
  max_legs?: number | null;
  sl_breakeven?: boolean | null;
  baseline: Partial<Stage3PyramidRecord>;
  best: Partial<Stage3PyramidRecord>;
  results: Stage3PyramidRecord[];
};

type Stage4CandidateResult = {
  candidate_id: string;
  net_expectancy_pct?: number;
  gross_expectancy_pct?: number;
  total_decisions?: number;
  executed_trades?: number;
  skipped_decisions?: number;
  tp_hits?: number;
  sl_hits?: number;
  no_hit?: number;
  mixed_exit?: number;
  unfilled?: number;
  profit_factor?: number;
  win_rate_pct?: number;
  net_pnl_pct?: number;
};

type Stage4RealizedExpectancyState = {
  exists: boolean;
  realized_expectancy_path?: string | null;
  trade_ledger_path?: string | null;
  optimal_path?: string | null;
  summary_path?: string | null;
  best_candidate_id?: string | null;
  best_candidate: Partial<Stage4CandidateResult>;
  candidates: Stage4CandidateResult[];
};

type DevelopmentNextAction = {
  type: string;
  label: string;
  disabled: boolean;
  target_stage: ResearchStageId | string;
};

type DevelopmentQueueRow = {
  candidate_id: string;
  universe_run_id: string;
  asset: string;
  signal_engine_id: string;
  signal_set_id: string;
  signal_set_key: string;
  strategy_id: string | null;
  stage0_status: string;
  packet_count: number | null;
  stage0_evaluated_signal_count: number | null;
  trigger_rate_pct: number | null;
  branch_path: string;
  stage1_session_id: string | null;
  stage1_status: string | null;
  stage1_gate: Stage1GateSummary | null;
  current_stage: string;
  development_status: string;
  next_action: DevelopmentNextAction;
};

async function fetchCatalog(): Promise<CatalogResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/market-data/catalog`);
  if (!response.ok) {
    throw new Error("Failed to load market data catalog");
  }
  return response.json();
}

async function fetchSignalEngines(): Promise<{ engines: SignalEngine[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/signal-engines`);
  if (!response.ok) {
    throw new Error("Failed to load signal engines");
  }
  return response.json();
}

async function fetchSignalSets(signalEngineId: string): Promise<{ signal_sets: SignalSet[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/signal-engines/${signalEngineId}/signal-sets`);
  if (!response.ok) {
    throw new Error("Failed to load signal sets");
  }
  return response.json();
}

async function extendSignalPoolFromLocalCandles(request: {
  signal_engine_id: string;
  asset: string;
}): Promise<SignalPoolExtendResult> {
  const response = await fetch(
    `${API_BASE_URL}/api/v1/signal-engines/${request.signal_engine_id}/signal-sets/${request.asset}/extend-local`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({})
    }
  );
  const payload = await response.json().catch(() => null);
  if (!response.ok) {
    throw new Error(payload?.detail ?? "Failed to update signal pool from local candles");
  }
  return payload;
}

async function fetchSignals(signalSetKey: string): Promise<{ signals: SignalRecord[] }> {
  const params = new URLSearchParams({ signal_set_key: signalSetKey, limit: "5" });
  const response = await fetch(`${API_BASE_URL}/api/v1/signals?${params}`);
  if (!response.ok) {
    throw new Error("Failed to load signal packets");
  }
  return response.json();
}

async function fetchStage0UniverseRuns(): Promise<{ runs: Stage0UniverseRun[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs`);
  if (!response.ok) {
    throw new Error("Failed to load Stage 0 universe runs");
  }
  return response.json();
}

async function fetchStage0UniverseCandidates(universeRunId: string): Promise<{ candidates: Stage0UniverseCandidate[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs/${universeRunId}/candidates`);
  if (!response.ok) {
    throw new Error("Failed to load Stage 0 candidates");
  }
  return response.json();
}

async function fetchDevelopmentQueue(universeRunId: string): Promise<{ universe_run: Stage0UniverseRun; queue: DevelopmentQueueRow[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/cycles/${universeRunId}/development-queue`);
  if (!response.ok) {
    throw new Error("Failed to load development queue");
  }
  return response.json();
}

async function createStage0UniverseRun(request: {
  train_start_date?: string;
  train_end_date?: string;
  walk_forward_start_date?: string;
  walk_forward_end_date?: string;
  forward_hours: number;
  trigger_rate_threshold_pct: number;
  engine_ids: string[];
  assets?: string[];
}): Promise<Stage0UniverseResponse> {
  const trainStart = request.train_start_date ?? "";
  const walkForwardEnd = request.walk_forward_end_date ?? "";
  const runId = `stage0-universe-${trainStart}-${walkForwardEnd}-${Date.now()}`;
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      universe_run_id: runId,
      train_start: trainStart,
      train_end: request.train_end_date,
      walk_forward_start: request.walk_forward_start_date,
      walk_forward_end: walkForwardEnd,
      forward_hours: request.forward_hours,
      trigger_rate_threshold_pct: request.trigger_rate_threshold_pct,
      engine_ids: request.engine_ids,
      assets: request.assets ?? []
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to create Stage 0 universe");
  }
  return response.json();
}

async function executeStage0Candidate(request: {
  universe_run_id: string;
  candidate_id: string;
}): Promise<Stage0ExecutionResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs/${request.universe_run_id}/candidates/execute`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ candidate_id: request.candidate_id })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to execute Stage 0 candidate");
  }
  return response.json();
}

async function executeStage0CandidateBatch(request: {
  universe_run_id: string;
  limit: number;
  confirm_large_run: boolean;
}): Promise<Stage0BatchExecutionResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs/${request.universe_run_id}/candidates/execute-batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ limit: request.limit })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to batch execute Stage 0 candidates");
  }
  return response.json();
}

async function supersedeStage0UniverseRun(universeRunId: string): Promise<{ run: Stage0UniverseRun }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs/${universeRunId}/supersede`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to supersede Stage 0 run");
  }
  return response.json();
}

type Stage0UniverseDeleteResponse = {
  status: string;
  universe_run_id: string;
  deleted_stage1_session_count: number;
  deleted_stage1_session_ids: string[];
};

async function deleteStage0UniverseRun(universeRunId: string): Promise<Stage0UniverseDeleteResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage0-universe-runs/${universeRunId}`, {
    method: "DELETE"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to delete Stage 0 batch");
  }
  return response.json();
}

async function fetchStage1ResearchSessions(): Promise<{ sessions: Stage1ResearchSession[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions`);
  if (!response.ok) {
    throw new Error("Failed to load Stage 1 sessions");
  }
  return response.json();
}

async function createStage1ResearchSession(request: {
  source_candidate_id: string;
  strategy_id: string;
  strategy_version: string;
  train_start: string;
  train_end: string;
  walk_forward_start: string;
  walk_forward_end: string;
}): Promise<{ session: Stage1ResearchSession }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request)
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to start Stage 1 session");
  }
  return response.json();
}

async function createStage1Iteration(request: {
  session_id: string;
  sample_method: string;
  bundle_role: string;
}): Promise<{ iteration: Stage1IterationBundle }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/iterations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sample_method: request.sample_method,
      bundle_role: request.bundle_role
    })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to create Stage 1 iteration");
  }
  return response.json();
}

async function fetchStage1Iterations(sessionId: string): Promise<{ iterations: Stage1IterationSummary[] }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${sessionId}/iterations`);
  if (!response.ok) {
    throw new Error("Failed to load Stage 1 iterations");
  }
  return response.json();
}

async function fetchStage1Gate(sessionId: string): Promise<{ gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${sessionId}/gate`);
  if (!response.ok) {
    throw new Error("Failed to load Stage 1 gate");
  }
  return response.json();
}

async function fetchStage1AgentPrompt(request: {
  session_id: string;
  iteration_id: string;
}): Promise<Stage1AgentPrompt> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/agent-prompt`);
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to load Stage 1 agent prompt");
  }
  return response.json();
}

async function deleteStage1Iteration(request: {
  session_id: string;
  iteration_id: string;
}): Promise<{ status: string; session_id: string; iteration_id: string }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}`, {
    method: "DELETE"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to delete Stage 1 iteration");
  }
  return response.json();
}

async function runStage1CanonicalReadout(request: {
  session_id: string;
}): Promise<{ canonical_readout: Stage1TrainingScore & {
  frozen_strategy_path: string;
  slice_metrics: Record<string, Stage1TrainingScore["metrics"]>;
  match_count: number;
}; gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/canonical-stage1a`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to run canonical Stage 1A readout");
  }
  return response.json();
}

async function runStage2CaptureCurve(request: {
  session_id: string;
}): Promise<{ stage2_capture: Stage2CaptureState; gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/stage2/capture-curve`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to run Stage 2 travel capture");
  }
  return response.json();
}

async function runStage3GridSearch(request: {
  session_id: string;
}): Promise<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/stage3/grid-search`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to run Stage 3 grid search");
  }
  return response.json();
}

async function runStage3Pyramid(request: {
  session_id: string;
}): Promise<{ stage3_pyramid: Stage3PyramidState; gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/stage3/pyramid`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to run Stage 3 pyramid");
  }
  return response.json();
}

async function runStage4RealizedExpectancy(request: {
  session_id: string;
}): Promise<{ stage4_realized_expectancy: Stage4RealizedExpectancyState; gate: Stage1GateSummary }> {
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/stage4/realized-expectancy`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    const detail = typeof payload?.detail === "string" ? payload.detail : payload?.detail?.message;
    throw new Error(detail ?? "Failed to run Stage 4 realized expectancy");
  }
  return response.json();
}

async function scoreStage1TrainingIteration(request: {
  session_id: string;
  iteration_id: string;
  sample_role?: Stage1SampleRole;
}): Promise<{ score: Stage1TrainingScore }> {
  const endpointByRole = {
    training: "score-training",
    walk_forward_test: "score-walk-forward"
  };
  const endpoint = endpointByRole[request.sample_role ?? "training"];
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/${endpoint}`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to run Stage 1 training score");
  }
  return response.json();
}

async function generateStage1FailureAudit(request: {
  session_id: string;
  iteration_id: string;
  sample_role?: Stage1SampleRole;
}): Promise<{ audit: Stage1FailureAudit }> {
  const params = new URLSearchParams();
  if (request.sample_role) {
    params.set("sample_role", request.sample_role);
  }
  const query = params.toString() ? `?${params.toString()}` : "";
  const response = await fetch(`${API_BASE_URL}/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/generate-failure-audit${query}`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to generate Stage 1 failure audit");
  }
  return response.json();
}

async function refreshDataset(datasetId: string): Promise<RefreshPlan> {
  const response = await fetch(`${API_BASE_URL}/api/v1/market-data/${datasetId}/refresh`, {
    method: "POST"
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? "Failed to fill raw candle data");
  }
  return response.json();
}

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <TerminalApp />
    </QueryClientProvider>
  );
}

function TerminalApp() {
  const [activeView, setActiveView] = React.useState<ActiveView>("dashboard");
  const [researchSubView, setResearchSubView] = React.useState<ResearchSubView>("stage0-batches");
  const [focusedDevelopment, setFocusedDevelopment] = React.useState<{
    universeRunId: string;
    candidateId: string;
  } | null>(null);
  const catalogQuery = useQuery({ queryKey: ["market-data-catalog"], queryFn: fetchCatalog });
  const signalEnginesQuery = useQuery({ queryKey: ["signal-engines"], queryFn: fetchSignalEngines });
  const stage0UniverseRunsQuery = useQuery({ queryKey: ["stage0-universe-runs"], queryFn: fetchStage0UniverseRuns });
  const stage1SessionsQuery = useQuery({ queryKey: ["stage1-sessions"], queryFn: fetchStage1ResearchSessions });
  const refreshMutation = useMutation({
    mutationFn: refreshDataset,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["market-data-catalog"] });
    }
  });
  const createStage0UniverseMutation = useMutation({
    mutationFn: createStage0UniverseRun,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const executeStage0CandidateMutation = useMutation({
    mutationFn: executeStage0Candidate,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const executeStage0CandidateBatchMutation = useMutation({
    mutationFn: executeStage0CandidateBatch,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const supersedeStage0UniverseRunMutation = useMutation({
    mutationFn: supersedeStage0UniverseRun,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const deleteStage0UniverseRunMutation = useMutation({
    mutationFn: deleteStage0UniverseRun,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-sessions"] });
    }
  });
  const createStage1SessionMutation = useMutation({
    mutationFn: createStage1ResearchSession,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-sessions"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const createStage1IterationMutation = useMutation({
    mutationFn: createStage1Iteration,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-iterations", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const deleteStage1IterationMutation = useMutation({
    mutationFn: deleteStage1Iteration,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-iterations", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const fetchStage1AgentPromptMutation = useMutation({
    mutationFn: fetchStage1AgentPrompt,
  });
  const scoreStage1TrainingMutation = useMutation({
    mutationFn: scoreStage1TrainingIteration,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-iterations", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const generateStage1FailureAuditMutation = useMutation({
    mutationFn: generateStage1FailureAudit,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-iterations", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const runStage1CanonicalMutation = useMutation({
    mutationFn: runStage1CanonicalReadout,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage1-sessions"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const runStage2CaptureMutation = useMutation({
    mutationFn: runStage2CaptureCurve,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const runStage3GridMutation = useMutation({
    mutationFn: runStage3GridSearch,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const runStage3PyramidMutation = useMutation({
    mutationFn: runStage3Pyramid,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const runStage4RealizedExpectancyMutation = useMutation({
    mutationFn: runStage4RealizedExpectancy,
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({ queryKey: ["stage1-gate", variables.session_id] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue"] });
    }
  });
  const catalog = catalogQuery.data;
  const dynamicMetrics = [
    {
      label: "Assets",
      value: catalog ? String(catalog.summary.assets) : catalogQuery.isLoading ? "..." : "n/a",
      detail: catalogQuery.error ? "catalog unavailable" : "cataloged",
    },
    {
      label: "Datasets",
      value: catalog ? String(catalog.summary.datasets) : catalogQuery.isLoading ? "..." : "n/a",
      detail: catalogQuery.error ? "catalog unavailable" : "registered",
    },
    {
      label: "Engines",
      value: signalEnginesQuery.data ? String(signalEnginesQuery.data.engines.length) : signalEnginesQuery.isLoading ? "..." : "n/a",
      detail: signalEnginesQuery.error ? "engine catalog unavailable" : "registered",
    },
    {
      label: "Stage 0",
      value: stage0UniverseRunsQuery.data ? String(stage0UniverseRunsQuery.data.runs.length) : stage0UniverseRunsQuery.isLoading ? "..." : "n/a",
      detail: stage0UniverseRunsQuery.error ? "runs unavailable" : "batches",
    },
  ];
  const dashboardCycles = buildDashboardCycles(stage0UniverseRunsQuery.data?.runs ?? [], stage1SessionsQuery.data?.sessions ?? []);
  const showResearchSubNav = activeView === "research";
  const openResearchSubView = React.useCallback((subView: ResearchSubView) => {
    setActiveView("research");
    setResearchSubView(subView);
  }, []);

  return (
    <div className="app-shell">
      <aside className="sidebar" aria-label="Primary navigation">
        <div className="brand">
          <Terminal size={22} />
          <span>Motis</span>
        </div>
        <nav>
          <button className={activeView === "dashboard" ? "active" : ""} type="button" onClick={() => setActiveView("dashboard")}><Activity size={18} />Dashboard</button>
          <button className={activeView === "data" ? "active" : ""} type="button" onClick={() => setActiveView("data")}><Database size={18} />Data</button>
          <button className={activeView === "engines" ? "active" : ""} type="button" onClick={() => setActiveView("engines")}><Radar size={18} />Engines</button>
          <button className={activeView === "research" ? "active" : ""} type="button" onClick={() => openResearchSubView(researchSubView)}><FlaskConical size={18} />R&amp;D</button>
          {showResearchSubNav && (
            <div className="nav-subgroup" aria-label="R&D navigation">
              <button
                className={researchSubView === "stage0-batches" ? "active nav-child" : "nav-child"}
                type="button"
                onClick={() => openResearchSubView("stage0-batches")}
              >
                Stage 0 Batches
              </button>
              <button
                className={researchSubView === "development" ? "active nav-child" : "nav-child"}
                type="button"
                onClick={() => openResearchSubView("development")}
              >
                Development
              </button>
            </div>
          )}
          <button className={activeView === "trading" ? "active" : ""} type="button" onClick={() => setActiveView("trading")}><Shield size={18} />Trading</button>
        </nav>
      </aside>

      <main className="workspace">
        <section className="topbar" id="dashboard">
          <div>
            <h1>Deterministic Quant Terminal</h1>
            <p>Local research, walk-forward scoring, agent iteration, and gated execution.</p>
          </div>
          <div className="topbar-actions">
            <button type="button" onClick={() => catalogQuery.refetch()}><RefreshCw size={16} />Sync Data</button>
            <button type="button" className="primary"><Play size={16} />Run Cycle</button>
          </div>
        </section>

        {activeView !== "research" && (
          <section className="metric-grid" aria-label="System metrics">
            {dynamicMetrics.map((metric) => (
              <article className="metric" key={metric.label}>
                <span>{metric.label}</span>
                <strong>{metric.value}</strong>
                <small>{metric.detail}</small>
              </article>
            ))}
          </section>
        )}

        {activeView === "data" && (
          <section className="content-grid">
            <DataCatalog
              catalog={catalog}
              loading={catalogQuery.isLoading}
              error={catalogQuery.error}
              refreshMutation={refreshMutation}
            />
          </section>
        )}

        {activeView === "engines" && (
          <section className="content-grid">
            <SignalEnginesPanel
              engines={signalEnginesQuery.data?.engines}
              loading={signalEnginesQuery.isLoading}
              error={signalEnginesQuery.error}
            />
          </section>
        )}

        {activeView === "dashboard" && (
          <section className="content-grid">
            <article className="panel large">
              <div className="panel-header">
                <h2>Research Cycles</h2>
                <span className="pill">{formatNumber(dashboardCycles.length)} backend rows</span>
              </div>
              {stage0UniverseRunsQuery.isLoading && <p className="panel-copy">Loading Stage 0 batches...</p>}
              {stage0UniverseRunsQuery.error && <p className="panel-copy error-text">{stage0UniverseRunsQuery.error.message}</p>}
              {stage1SessionsQuery.error && <p className="panel-copy error-text">{stage1SessionsQuery.error.message}</p>}
              <div className="table">
                <div className="row header">
                  <span>Cycle</span>
                  <span>Stage</span>
                  <span>Train</span>
                  <span>Walk-Forward</span>
                  <span>Status</span>
                </div>
                {dashboardCycles.map((cycle) => (
                  <div className="row" key={cycle.id}>
                    <span>{cycle.label}</span>
                    <span>{cycle.stage}</span>
                    <span>{cycle.train}</span>
                    <span>{cycle.walkForward}</span>
                    <span className="status">{cycle.status}</span>
                  </div>
                ))}
              </div>
              {!stage0UniverseRunsQuery.isLoading && dashboardCycles.length === 0 && (
                <p className="panel-copy">No backend Stage 0 batches or Stage 1 sessions exist yet.</p>
              )}
            </article>
          </section>
        )}

        {activeView === "research" && (
          <section className="content-grid">
            {researchSubView === "stage0-batches" ? (
              <Stage0BatchesPanel
                signalEngines={signalEnginesQuery.data?.engines}
                universeRuns={stage0UniverseRunsQuery.data?.runs}
                createStage0UniverseMutation={createStage0UniverseMutation}
                executeStage0CandidateBatchMutation={executeStage0CandidateBatchMutation}
                deleteStage0UniverseRunMutation={deleteStage0UniverseRunMutation}
                onOpenDevelopment={(universeRunId, candidateId) => {
                  setFocusedDevelopment({ universeRunId, candidateId });
                  setResearchSubView("development");
                }}
              />
            ) : (
              <DevelopmentPanel
                universeRuns={stage0UniverseRunsQuery.data?.runs}
                focusedRunId={focusedDevelopment?.universeRunId}
                focusedCandidateId={focusedDevelopment?.candidateId}
                stage1Sessions={stage1SessionsQuery.data?.sessions}
                stage1SessionsLoading={stage1SessionsQuery.isLoading}
                stage1SessionsError={stage1SessionsQuery.error}
                createStage1SessionMutation={createStage1SessionMutation}
                createStage1IterationMutation={createStage1IterationMutation}
                deleteStage1IterationMutation={deleteStage1IterationMutation}
                fetchStage1AgentPromptMutation={fetchStage1AgentPromptMutation}
                scoreStage1TrainingMutation={scoreStage1TrainingMutation}
                generateStage1FailureAuditMutation={generateStage1FailureAuditMutation}
                runStage1CanonicalMutation={runStage1CanonicalMutation}
                runStage2CaptureMutation={runStage2CaptureMutation}
                runStage3GridMutation={runStage3GridMutation}
                runStage3PyramidMutation={runStage3PyramidMutation}
                runStage4RealizedExpectancyMutation={runStage4RealizedExpectancyMutation}
              />
            )}
          </section>
        )}

        {activeView === "trading" && (
          <section className="content-grid">
            <article className="panel" id="trading">
              <div className="panel-header">
                <h2>Live Executing Strategies</h2>
                <span className="pill red">not wired</span>
              </div>
              <p className="panel-copy">
                No deployment-route API is exposed yet, so this tab is intentionally empty instead of showing placeholder routes.
              </p>
            </article>
          </section>
        )}
      </main>
    </div>
  );
}

function Stage0BatchesPanel({
  signalEngines,
  universeRuns,
  createStage0UniverseMutation,
  executeStage0CandidateBatchMutation,
  deleteStage0UniverseRunMutation,
  onOpenDevelopment,
}: {
  signalEngines?: SignalEngine[];
  universeRuns?: Stage0UniverseRun[];
  createStage0UniverseMutation: ReturnType<typeof useMutation<Stage0UniverseResponse, Error, {
    train_start_date?: string;
    train_end_date?: string;
    walk_forward_start_date?: string;
    walk_forward_end_date?: string;
    forward_hours: number;
    trigger_rate_threshold_pct: number;
    engine_ids: string[];
    assets?: string[];
  }>>;
  executeStage0CandidateBatchMutation: ReturnType<typeof useMutation<Stage0BatchExecutionResponse, Error, {
    universe_run_id: string;
    limit: number;
    confirm_large_run: boolean;
  }>>;
  deleteStage0UniverseRunMutation: ReturnType<typeof useMutation<Stage0UniverseDeleteResponse, Error, string>>;
  onOpenDevelopment: (universeRunId: string, candidateId: string) => void;
}) {
  const [selectedRunId, setSelectedRunId] = React.useState<string | null>(null);
  const [batchLabel, setBatchLabel] = React.useState("");
  const [selectedEngineId, setSelectedEngineId] = React.useState<string | null>(null);
  const [tickerInput, setTickerInput] = React.useState("");
  const [selectedTickers, setSelectedTickers] = React.useState<string[]>(["BTC", "ETH", "AAVE", "SOL", "WIF"]);
  const [trainStartDate, setTrainStartDate] = React.useState("2026-03-01");
  const [trainEndDate, setTrainEndDate] = React.useState("2026-04-30");
  const [walkForwardStartDate, setWalkForwardStartDate] = React.useState("2026-05-01");
  const [walkForwardEndDate, setWalkForwardEndDate] = React.useState("2026-05-30");
  const [forwardHours, setForwardHours] = React.useState(36);
  const [triggerRateThresholdPct, setTriggerRateThresholdPct] = React.useState(85);
  const [autoRunCreatedBatchId, setAutoRunCreatedBatchId] = React.useState<string | null>(null);
  const effectiveRun = React.useMemo(
    () => universeRuns?.find((run) => run.universe_run_id === selectedRunId) ?? universeRuns?.[0] ?? null,
    [selectedRunId, universeRuns]
  );
  const effectiveEngineId = selectedEngineId ?? signalEngines?.[0]?.signal_engine_id ?? null;
  const queueQuery = useQuery({
    queryKey: ["development-queue", effectiveRun?.universe_run_id],
    queryFn: () => fetchDevelopmentQueue(effectiveRun?.universe_run_id as string),
    enabled: Boolean(effectiveRun?.universe_run_id)
  });
  const candidatesQuery = useQuery({
    queryKey: ["stage0-universe-candidates", effectiveRun?.universe_run_id],
    queryFn: () => fetchStage0UniverseCandidates(effectiveRun?.universe_run_id as string),
    enabled: Boolean(effectiveRun?.universe_run_id)
  });
  const signalSetsQuery = useQuery({
    queryKey: ["signal-sets", effectiveEngineId],
    queryFn: () => fetchSignalSets(effectiveEngineId as string),
    enabled: Boolean(effectiveEngineId)
  });
  const assetOptions = React.useMemo(() => {
    const assets = new Set((signalSetsQuery.data?.signal_sets ?? []).map((set) => set.asset));
    return Array.from(assets).sort();
  }, [signalSetsQuery.data?.signal_sets]);
  React.useEffect(() => {
    if (!selectedRunId && universeRuns?.[0]?.universe_run_id) {
      setSelectedRunId(universeRuns[0].universe_run_id);
    }
  }, [selectedRunId, universeRuns]);
  React.useEffect(() => {
    const createdRunId = createStage0UniverseMutation.data?.run.universe_run_id;
    if (createdRunId) {
      setSelectedRunId(createdRunId);
      setAutoRunCreatedBatchId(createdRunId);
    }
  }, [createStage0UniverseMutation.data?.run.universe_run_id]);
  React.useEffect(() => {
    const deletedRunId = deleteStage0UniverseRunMutation.data?.universe_run_id;
    if (deletedRunId && selectedRunId === deletedRunId) {
      const nextRun = universeRuns?.find((run) => run.universe_run_id !== deletedRunId) ?? null;
      setSelectedRunId(nextRun?.universe_run_id ?? null);
    }
  }, [deleteStage0UniverseRunMutation.data?.universe_run_id, selectedRunId, universeRuns]);
  const queueRows = queueQuery.data?.queue ?? [];
  const candidateById = React.useMemo(
    () => new Map((candidatesQuery.data?.candidates ?? []).map((candidate) => [candidate.candidate_id, candidate])),
    [candidatesQuery.data?.candidates]
  );
  const acceptedRows = queueRows.filter((row) => row.stage0_status === "accepted");
  const stage0Progress = React.useMemo(() => buildStage0Progress(effectiveRun, queueRows), [effectiveRun, queueRows]);
  const runAllPendingStage0 = React.useCallback(() => {
    if (!effectiveRun || stage0Progress.pending <= 0) {
      return;
    }
    executeStage0CandidateBatchMutation.mutate({
      universe_run_id: effectiveRun.universe_run_id,
      limit: stage0Progress.pending,
      confirm_large_run: true,
    });
  }, [effectiveRun, executeStage0CandidateBatchMutation, stage0Progress.pending]);
  React.useEffect(() => {
    if (
      autoRunCreatedBatchId
      && effectiveRun?.universe_run_id === autoRunCreatedBatchId
      && !queueQuery.isLoading
      && stage0Progress.pending > 0
      && !executeStage0CandidateBatchMutation.isPending
    ) {
      setAutoRunCreatedBatchId(null);
      runAllPendingStage0();
    }
  }, [
    autoRunCreatedBatchId,
    effectiveRun?.universe_run_id,
    executeStage0CandidateBatchMutation.isPending,
    queueQuery.isLoading,
    runAllPendingStage0,
    stage0Progress.pending,
  ]);
  const dataCoveragePct = selectedTickers.length ? 100 : 0;
  const addTicker = React.useCallback(() => {
    const symbol = tickerInput.trim().toUpperCase();
    if (!symbol || selectedTickers.includes(symbol)) {
      setTickerInput("");
      return;
    }
    setSelectedTickers([...selectedTickers, symbol]);
    setTickerInput("");
  }, [selectedTickers, tickerInput]);
  return (
    <article className="panel large stage0-batches-page">
      <div className="stage0-batches-grid">
        <section className="stage0-batch-left">
          <div className="panel-header">
            <h2>Past Stage 0 Batches</h2>
            <div className="header-actions">
              <span className="pill">{formatNumber(universeRuns?.length ?? 0)} batches</span>
              <button
                type="button"
                className="danger-button"
                disabled={!effectiveRun || deleteStage0UniverseRunMutation.isPending}
                onClick={() => {
                  if (!effectiveRun) {
                    return;
                  }
                  const confirmed = window.confirm(`Delete Stage 0 batch ${effectiveRun.universe_run_id}? Linked Development sessions for this batch will be deleted too.`);
                  if (confirmed) {
                    deleteStage0UniverseRunMutation.mutate(effectiveRun.universe_run_id);
                  }
                }}
              >
                <Trash2 size={15} />Delete Batch
              </button>
            </div>
          </div>
          {deleteStage0UniverseRunMutation.error && <p className="panel-copy error-text">{deleteStage0UniverseRunMutation.error.message}</p>}
          <div className="stage0-batch-table">
            <div className="stage0-batch-row header">
              <span>Batch ID</span>
              <span>Batch Windows</span>
              <span>Engine</span>
              <span>Tickers</span>
              <span>Accepted</span>
              <span>Watchlist</span>
              <span>Pending</span>
              <span>Status</span>
            </div>
            {(universeRuns ?? []).map((run) => (
              <button
                type="button"
                className={effectiveRun?.universe_run_id === run.universe_run_id ? "stage0-batch-row selected" : "stage0-batch-row"}
                key={run.universe_run_id}
                onClick={() => setSelectedRunId(run.universe_run_id)}
              >
                <strong>{shortBatchId(run.universe_run_id)}</strong>
                <span className="split-window-stack">{formatStage0SplitWindows(run)}</span>
                <span>{run.engine_filter.join(", ") || "all"}</span>
                <span>{formatNumber(run.summary.total_candidates ?? 0)}</span>
                <span>{formatNumber(run.summary.accepted ?? 0)}</span>
                <span>{formatNumber(run.summary.watchlist ?? 0)}</span>
                <span>{formatNumber(run.summary.pending_stage0 ?? 0)}</span>
                <span className={run.status === "completed" ? "status-badge pass" : "status-badge"}>{run.status}</span>
              </button>
            ))}
            {universeRuns && universeRuns.length === 0 && <p className="panel-copy">No Stage 0 batch sessions yet.</p>}
          </div>
          <section className="selected-batch-candidates">
            <div className="summary-line">
              <strong>Selected Batch Candidates</strong>
              <span>
                {effectiveRun ? `Batch ${shortBatchId(effectiveRun.universe_run_id)} · ${formatNumber(acceptedRows.length)} accepted` : "No batch selected"}
              </span>
            </div>
            {effectiveRun && (
              <div className="selected-batch-window-readout">
                <span>{formatStage0SplitWindows(effectiveRun)}</span>
              </div>
            )}
            <div className="stage0-progress-card">
              <div className="stage0-progress-head">
                <div>
                  <span>Stage 0 Scoring Progress</span>
                  <strong>{stage0Progress.scored} / {stage0Progress.total} candidates scored</strong>
                </div>
                <span className={stage0Progress.pending > 0 ? "status-badge warn" : "status-badge pass"}>
                  {executeStage0CandidateBatchMutation.isPending ? "running" : stage0Progress.pending > 0 ? "pending" : "complete"}
                </span>
              </div>
              <div className="stage0-progress-track" aria-label="Stage 0 scoring progress">
                <div style={{ width: `${stage0Progress.percent}%` }} />
              </div>
              <div className="stage0-progress-metrics">
                <span>{formatNumber(stage0Progress.accepted)} accepted</span>
                <span>{formatNumber(stage0Progress.watchlist)} watchlist</span>
                <span>{formatNumber(stage0Progress.pending)} pending</span>
                <span>{formatNumber(stage0Progress.failed)} failed</span>
              </div>
              {executeStage0CandidateBatchMutation.data?.summary && (
                <small>
                  Last run: {formatNumber(executeStage0CandidateBatchMutation.data.summary.succeeded)} succeeded,
                  {" "}{formatNumber(executeStage0CandidateBatchMutation.data.summary.failed)} failed,
                  {" "}{formatNumber(executeStage0CandidateBatchMutation.data.summary.remaining_pending)} remaining.
                </small>
              )}
              {executeStage0CandidateBatchMutation.error && <p className="panel-copy error-text">{executeStage0CandidateBatchMutation.error.message}</p>}
            </div>
            {(queueQuery.isLoading || candidatesQuery.isLoading) && <p className="panel-copy">Loading selected batch candidates...</p>}
            {queueQuery.error && <p className="panel-copy error-text">{queueQuery.error.message}</p>}
            {candidatesQuery.error && <p className="panel-copy error-text">{candidatesQuery.error.message}</p>}
            <div className="stage0-candidate-table">
              <div className="stage0-candidate-row header">
                <span>Asset</span>
                <span>Engine</span>
                <span>Evaluated</span>
                <span>Trigger %</span>
                <span>Branch</span>
                <span>Stage 0</span>
                <span>Development</span>
                <span>Action</span>
              </div>
              {queueRows.map((row) => {
                const candidate = candidateById.get(row.candidate_id);
                const evaluatedSignalCount = row.stage0_evaluated_signal_count
                  ?? (candidate ? stage0EvaluatedSignalCount(candidate) : row.packet_count)
                  ?? null;
                return (
                  <div className="stage0-candidate-row" key={row.candidate_id}>
                    <strong>{row.asset}</strong>
                    <span>{row.signal_engine_id}</span>
                    <span>{formatNumber(evaluatedSignalCount)}</span>
                    <span>{row.trigger_rate_pct === null ? "pending" : `${row.trigger_rate_pct}%`}</span>
                    <span>{row.branch_path}</span>
                    <span className={row.stage0_status === "accepted" ? "status-badge pass" : row.stage0_status === "pending_stage0" ? "status-badge muted" : "status-badge warn"}>{row.stage0_status}</span>
                    <span className={developmentStatusClass(row)}>{row.development_status.replaceAll("_", " ")}</span>
                    <button
                      type="button"
                      disabled={row.stage0_status !== "accepted"}
                      onClick={() => onOpenDevelopment(row.universe_run_id, row.candidate_id)}
                    >
                      {row.stage0_status === "accepted" ? "Open Development" : "Wait"}
                    </button>
                  </div>
                );
              })}
            </div>
          </section>
        </section>
        <section className="stage0-batch-right">
          <div className="panel-header">
            <h2>Start New Stage 0 Batch</h2>
            <span className="pill">setup</span>
          </div>
          <div className="stage0-create-card">
            <label>
              <span>Batch Label</span>
              <input value={batchLabel} placeholder="e.g. Vegas EMA March-May 2026" onChange={(event) => setBatchLabel(event.target.value)} />
            </label>
            <label>
              <span>Signal Engine</span>
              <select value={effectiveEngineId ?? ""} onChange={(event) => setSelectedEngineId(event.target.value)}>
                {(signalEngines ?? []).map((engine) => <option value={engine.signal_engine_id} key={engine.signal_engine_id}>{engine.signal_engine_id}</option>)}
              </select>
            </label>
            <div className="ticker-picker">
              <span>Tickers</span>
              <div className="ticker-chip-row">
                {selectedTickers.map((ticker) => (
                  <button type="button" className="ticker-chip" key={ticker} onClick={() => setSelectedTickers(selectedTickers.filter((item) => item !== ticker))}>
                    {ticker} x
                  </button>
                ))}
              </div>
              <div className="ticker-input-row">
                <input
                  list="stage0-assets"
                  value={tickerInput}
                  placeholder="Add ticker"
                  onChange={(event) => setTickerInput(event.target.value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter") {
                      event.preventDefault();
                      addTicker();
                    }
                  }}
                />
                <datalist id="stage0-assets">
                  {assetOptions.map((asset) => <option value={asset} key={asset} />)}
                </datalist>
                <button type="button" onClick={addTicker}>Add</button>
              </div>
            </div>
            <div className="form-grid two-col">
              <label>
                <span>Train Start</span>
                <input type="date" value={trainStartDate} onChange={(event) => setTrainStartDate(event.target.value)} />
              </label>
              <label>
                <span>Train End</span>
                <input type="date" value={trainEndDate} onChange={(event) => setTrainEndDate(event.target.value)} />
              </label>
              <label>
                <span>Walk-Forward Start</span>
                <input type="date" value={walkForwardStartDate} onChange={(event) => setWalkForwardStartDate(event.target.value)} />
              </label>
              <label>
                <span>Walk-Forward End</span>
                <input type="date" value={walkForwardEndDate} onChange={(event) => setWalkForwardEndDate(event.target.value)} />
              </label>
              <label>
                <span>Forward Hours</span>
                <input type="number" min={1} value={forwardHours} onChange={(event) => setForwardHours(Number(event.target.value))} />
              </label>
              <label>
                <span>Trigger Rate Threshold</span>
                <input type="number" min={0} max={100} value={triggerRateThresholdPct} onChange={(event) => setTriggerRateThresholdPct(Number(event.target.value))} />
              </label>
            </div>
            <details className="advanced-row">
              <summary>Advanced Parameters</summary>
              <small>Stage 0 scoring window is derived as {trainStartDate} through {walkForwardEndDate}. Additional Stage 0 knobs will appear here.</small>
            </details>
            <div className="stage0-create-actions">
              <button
                type="button"
                className="primary"
                disabled={!effectiveEngineId || selectedTickers.length === 0 || createStage0UniverseMutation.isPending}
                onClick={() => createStage0UniverseMutation.mutate({
                  train_start_date: trainStartDate,
                  train_end_date: trainEndDate,
                  walk_forward_start_date: walkForwardStartDate,
                  walk_forward_end_date: walkForwardEndDate,
                  forward_hours: forwardHours,
                  trigger_rate_threshold_pct: triggerRateThresholdPct,
                  engine_ids: effectiveEngineId ? [effectiveEngineId] : [],
                  assets: selectedTickers
                })}
              >
                <Play size={16} />Create Stage 0 Batch
              </button>
              <button type="button" disabled>Save Draft</button>
            </div>
            {createStage0UniverseMutation.error && <p className="panel-copy error-text">{createStage0UniverseMutation.error.message}</p>}
          </div>
          <div className="stage0-readout-card">
            <div>
              <span>Estimated Candidates</span>
              <strong>{formatNumber(selectedTickers.length)}</strong>
              <small>Based on selected tickers and engine.</small>
            </div>
            <div>
              <span>Data Coverage</span>
              <strong>{dataCoveragePct}%</strong>
              <small>{selectedTickers.length ? "Using registered signal sets." : "Select tickers to estimate."}</small>
            </div>
          </div>
        </section>
      </div>
    </article>
  );
}

function DevelopmentPanel({
  universeRuns,
  focusedRunId,
  focusedCandidateId,
  stage1Sessions,
  stage1SessionsLoading,
  stage1SessionsError,
  createStage1SessionMutation,
  createStage1IterationMutation,
  deleteStage1IterationMutation,
  fetchStage1AgentPromptMutation,
  scoreStage1TrainingMutation,
  generateStage1FailureAuditMutation,
  runStage1CanonicalMutation,
  runStage2CaptureMutation,
  runStage3GridMutation,
  runStage3PyramidMutation,
  runStage4RealizedExpectancyMutation,
}: {
  universeRuns?: Stage0UniverseRun[];
  focusedRunId?: string;
  focusedCandidateId?: string;
  stage1Sessions?: Stage1ResearchSession[];
  stage1SessionsLoading: boolean;
  stage1SessionsError: Error | null;
  createStage1SessionMutation: ReturnType<typeof useMutation<{ session: Stage1ResearchSession }, Error, {
    source_candidate_id: string;
    strategy_id: string;
    strategy_version: string;
    train_start: string;
    train_end: string;
    walk_forward_start: string;
    walk_forward_end: string;
  }>>;
  createStage1IterationMutation: ReturnType<typeof useMutation<{ iteration: Stage1IterationBundle }, Error, {
    session_id: string;
    sample_method: string;
    bundle_role: string;
  }>>;
  deleteStage1IterationMutation: ReturnType<typeof useMutation<{ status: string; session_id: string; iteration_id: string }, Error, {
    session_id: string;
    iteration_id: string;
  }>>;
  fetchStage1AgentPromptMutation: ReturnType<typeof useMutation<Stage1AgentPrompt, Error, {
    session_id: string;
    iteration_id: string;
  }>>;
  scoreStage1TrainingMutation: ReturnType<typeof useMutation<{ score: Stage1TrainingScore }, Error, {
    session_id: string;
    iteration_id: string;
    sample_role?: Stage1SampleRole;
  }>>;
  generateStage1FailureAuditMutation: ReturnType<typeof useMutation<{ audit: Stage1FailureAudit }, Error, {
    session_id: string;
    iteration_id: string;
    sample_role?: Stage1SampleRole;
  }>>;
  runStage1CanonicalMutation: ReturnType<typeof useMutation<{ canonical_readout: Stage1TrainingScore & {
    frozen_strategy_path: string;
    slice_metrics: Record<string, Stage1TrainingScore["metrics"]>;
    match_count: number;
  }; gate: Stage1GateSummary }, Error, {
    session_id: string;
  }>>;
  runStage2CaptureMutation: ReturnType<typeof useMutation<{ stage2_capture: Stage2CaptureState; gate: Stage1GateSummary }, Error, {
    session_id: string;
  }>>;
  runStage3GridMutation: ReturnType<typeof useMutation<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary }, Error, {
    session_id: string;
  }>>;
  runStage3PyramidMutation: ReturnType<typeof useMutation<{ stage3_pyramid: Stage3PyramidState; gate: Stage1GateSummary }, Error, {
    session_id: string;
  }>>;
  runStage4RealizedExpectancyMutation: ReturnType<typeof useMutation<{ stage4_realized_expectancy: Stage4RealizedExpectancyState; gate: Stage1GateSummary }, Error, {
    session_id: string;
  }>>;
}) {
  const [selectedRunId, setSelectedRunId] = React.useState<string | null>(focusedRunId ?? null);
  const [selectedCandidateId, setSelectedCandidateId] = React.useState<string | null>(focusedCandidateId ?? null);
  const [candidateFilter, setCandidateFilter] = React.useState<"all" | "needs_action" | "in_progress" | "frozen">("all");
  const [activeStage, setActiveStage] = React.useState<ResearchStageId>("stage1");
  const [agentPrompt, setAgentPrompt] = React.useState<Stage1AgentPrompt | null>(null);
  const effectiveRun = React.useMemo(
    () => universeRuns?.find((run) => run.universe_run_id === selectedRunId) ?? universeRuns?.[0] ?? null,
    [selectedRunId, universeRuns]
  );
  const queueQuery = useQuery({
    queryKey: ["development-queue", effectiveRun?.universe_run_id],
    queryFn: () => fetchDevelopmentQueue(effectiveRun?.universe_run_id as string),
    enabled: Boolean(effectiveRun?.universe_run_id)
  });
  const acceptedRows = React.useMemo(
    () => (queueQuery.data?.queue ?? []).filter((row) => row.stage0_status === "accepted"),
    [queueQuery.data?.queue]
  );
  const filteredRows = React.useMemo(() => acceptedRows.filter((row) => {
    if (candidateFilter === "needs_action") {
      return !row.next_action.disabled && row.development_status !== "stage3_complete";
    }
    if (candidateFilter === "in_progress") {
      return row.development_status === "stage1_in_progress";
    }
    if (candidateFilter === "frozen") {
      return row.development_status === "stage1_frozen" || row.development_status === "stage2_complete" || row.development_status === "stage3_complete" || row.development_status === "stage4_complete";
    }
    return true;
  }), [acceptedRows, candidateFilter]);
  const selectedRow = acceptedRows.find((row) => row.candidate_id === selectedCandidateId) ?? acceptedRows[0] ?? null;
  const selectedSession = stage1Sessions?.find((session) => session.session_id === selectedRow?.stage1_session_id)
    ?? stage1Sessions?.find((session) => session.source_candidate_id === selectedRow?.candidate_id)
    ?? null;
  const iterationsQuery = useQuery({
    queryKey: ["stage1-iterations", selectedSession?.session_id],
    queryFn: () => fetchStage1Iterations(selectedSession?.session_id ?? ""),
    enabled: Boolean(selectedSession?.session_id)
  });
  const gateQuery = useQuery({
    queryKey: ["stage1-gate", selectedSession?.session_id],
    queryFn: () => fetchStage1Gate(selectedSession?.session_id ?? ""),
    enabled: Boolean(selectedSession?.session_id)
  });
  const gate = gateQuery.data?.gate ?? selectedRow?.stage1_gate ?? null;
  const iterations = iterationsQuery.data?.iterations ?? [];
  const roleIterations = React.useMemo(() => buildStage1RoleIterations(iterations), [iterations]);
  const evidenceMode = React.useMemo(
    () => buildStage1EvidenceMode(gate, selectedSession),
    [gate, selectedSession]
  );
  const defaultWindows = React.useMemo(() => stage1DefaultWindows(effectiveRun), [effectiveRun]);

  React.useEffect(() => {
    if (focusedRunId) {
      setSelectedRunId(focusedRunId);
    }
  }, [focusedRunId]);
  React.useEffect(() => {
    if (focusedCandidateId) {
      setSelectedCandidateId(focusedCandidateId);
    }
  }, [focusedCandidateId]);
  React.useEffect(() => {
    if (!selectedCandidateId && acceptedRows[0]?.candidate_id) {
      setSelectedCandidateId(acceptedRows[0].candidate_id);
    }
    if (selectedCandidateId && acceptedRows.length && !acceptedRows.some((row) => row.candidate_id === selectedCandidateId)) {
      setSelectedCandidateId(acceptedRows[0].candidate_id);
    }
  }, [acceptedRows, selectedCandidateId]);
  React.useEffect(() => {
    if (selectedRow?.current_stage) {
      setActiveStage(normalizeResearchStage(selectedRow.current_stage));
    }
  }, [selectedRow?.candidate_id, selectedRow?.current_stage]);

  const createStage1Session = React.useCallback(() => {
    if (!selectedRow) {
      return;
    }
    createStage1SessionMutation.mutate({
      source_candidate_id: selectedRow.candidate_id,
      strategy_id: selectedRow.strategy_id ?? `${selectedRow.asset.toLowerCase()}-${selectedRow.signal_engine_id}-strategy-v01`,
      strategy_version: "v0.1",
      train_start: defaultWindows.trainStart,
      train_end: defaultWindows.trainEnd,
      walk_forward_start: defaultWindows.walkForwardStart,
      walk_forward_end: defaultWindows.walkForwardEnd,
    });
  }, [createStage1SessionMutation, defaultWindows, selectedRow]);

  const createBundle = React.useCallback((role: Stage1SampleMethod) => {
    if (!selectedSession) {
      return;
    }
    createStage1IterationMutation.mutate({
      session_id: selectedSession.session_id,
      sample_method: role,
      bundle_role: stage1BundleRoleForMethod(role),
    });
  }, [createStage1IterationMutation, selectedSession]);

  const openAgentPrompt = React.useCallback((iteration: Stage1IterationSummary) => {
    if (!selectedSession) {
      return;
    }
    fetchStage1AgentPromptMutation.mutate(
      {
        session_id: selectedSession.session_id,
        iteration_id: iteration.iteration_id,
      },
      {
        onSuccess: (prompt) => setAgentPrompt(prompt),
      }
    );
  }, [fetchStage1AgentPromptMutation, selectedSession]);

  const runNextAction = React.useCallback(() => {
    if (!selectedRow) {
      return;
    }
    if (selectedRow.next_action.type === "start_stage1") {
      createStage1Session();
      return;
    }
    if (!selectedSession) {
      return;
    }
    if (selectedRow.next_action.type === "create_training_bundle") {
      createBundle("training");
    }
    if (selectedRow.next_action.type === "create_walk_forward_bundle") {
      createBundle("walk_forward_test");
    }
    if (selectedRow.next_action.type === "run_canonical_stage1a") {
      runStage1CanonicalMutation.mutate({ session_id: selectedSession.session_id });
    }
    if (selectedRow.next_action.type === "run_stage2_capture_curve") {
      runStage2CaptureMutation.mutate({ session_id: selectedSession.session_id });
    }
    if (selectedRow.next_action.type === "run_stage3_grid_search") {
      runStage3GridMutation.mutate({ session_id: selectedSession.session_id });
    }
    if (selectedRow.next_action.type === "run_stage3_pyramid") {
      runStage3PyramidMutation.mutate({ session_id: selectedSession.session_id });
    }
    if (selectedRow.next_action.type === "run_stage4_realized_expectancy") {
      runStage4RealizedExpectancyMutation.mutate({ session_id: selectedSession.session_id });
    }
  }, [createBundle, createStage1Session, runStage1CanonicalMutation, runStage2CaptureMutation, runStage3GridMutation, runStage3PyramidMutation, runStage4RealizedExpectancyMutation, selectedRow, selectedSession]);

  const filterCounts = {
    all: acceptedRows.length,
    needs_action: acceptedRows.filter((row) => !row.next_action.disabled && row.development_status !== "stage3_complete").length,
    in_progress: acceptedRows.filter((row) => row.development_status === "stage1_in_progress").length,
    frozen: acceptedRows.filter((row) => row.development_status === "stage1_frozen" || row.development_status === "stage2_complete" || row.development_status === "stage3_complete" || row.development_status === "stage4_complete").length,
  };

  return (
    <article className="panel large development-page">
      <section className="development-context-bar">
        <div>
          <span>Selected Stage 0 batch</span>
          <strong>{effectiveRun ? shortBatchId(effectiveRun.universe_run_id) : "No batch selected"}</strong>
        </div>
        <div>
          <span>Selected asset</span>
          <strong>{selectedRow?.asset ?? "None"}</strong>
        </div>
        <div>
          <span>Engine</span>
          <strong>{selectedRow?.signal_engine_id ?? "n/a"}</strong>
        </div>
        <div>
          <span>Batch Windows</span>
          <strong className="split-window-stack">{effectiveRun ? formatStage0SplitWindows(effectiveRun) : "n/a"}</strong>
        </div>
        <div>
          <span>Current state</span>
          <strong className="status-badge info">{selectedRow ? developmentStateLabel(selectedRow) : "none"}</strong>
        </div>
        <div>
          <span>Next action</span>
          <button
            type="button"
            className="link-action"
            disabled={!selectedRow || selectedRow.next_action.disabled || runStage3GridMutation.isPending || runStage3PyramidMutation.isPending}
            onClick={runNextAction}
          >
            {selectedRow?.next_action.label ?? "Select candidate"}
          </button>
        </div>
      </section>
      <div className="development-grid">
        <section className="dev-candidate-list">
          <div className="panel-header">
            <h2>Batch Candidates</h2>
            <button type="button" onClick={() => queueQuery.refetch()}><RefreshCw size={15} /></button>
          </div>
          <label className="dev-run-selector">
            <span>Batch</span>
            <select value={effectiveRun?.universe_run_id ?? ""} onChange={(event) => setSelectedRunId(event.target.value)}>
              {(universeRuns ?? []).map((run) => <option value={run.universe_run_id} key={run.universe_run_id}>{shortBatchId(run.universe_run_id)}</option>)}
            </select>
          </label>
          <div className="dev-filter-row">
            <button type="button" className={candidateFilter === "all" ? "active" : ""} onClick={() => setCandidateFilter("all")}>All <span>{filterCounts.all}</span></button>
            <button type="button" className={candidateFilter === "needs_action" ? "active" : ""} onClick={() => setCandidateFilter("needs_action")}>Needs Action <span>{filterCounts.needs_action}</span></button>
            <button type="button" className={candidateFilter === "in_progress" ? "active" : ""} onClick={() => setCandidateFilter("in_progress")}>In Progress <span>{filterCounts.in_progress}</span></button>
            <button type="button" className={candidateFilter === "frozen" ? "active" : ""} onClick={() => setCandidateFilter("frozen")}>Frozen <span>{filterCounts.frozen}</span></button>
          </div>
          {queueQuery.isLoading && <p className="panel-copy">Loading accepted candidates...</p>}
          {queueQuery.error && <p className="panel-copy error-text">{queueQuery.error.message}</p>}
          <div className="dev-candidate-table">
            <div className="dev-candidate-row header">
              <span>Asset</span>
              <span>Engine</span>
              <span>Trigger Rate</span>
              <span>Current Stage</span>
              <span>Next Action</span>
            </div>
            {filteredRows.map((row) => (
              <button
                type="button"
                className={row.candidate_id === selectedRow?.candidate_id ? "dev-candidate-row selected" : "dev-candidate-row"}
                key={row.candidate_id}
                onClick={() => setSelectedCandidateId(row.candidate_id)}
              >
                <strong>{row.asset}</strong>
                <span>{row.signal_engine_id}</span>
                <span className={row.trigger_rate_pct && row.trigger_rate_pct >= 85 ? "pass-text" : ""}>{row.trigger_rate_pct === null ? "n/a" : `${row.trigger_rate_pct}%`}</span>
                <span className="status-badge info">{developmentStageLabel(row)}</span>
                <span>{row.next_action.label}</span>
              </button>
            ))}
          </div>
        </section>
        <section className="dev-workspace">
          {stage1SessionsLoading && <p className="panel-copy">Loading Stage 1 sessions...</p>}
          {stage1SessionsError && <p className="panel-copy error-text">{stage1SessionsError.message}</p>}
          {createStage1SessionMutation.error && <p className="panel-copy error-text">{createStage1SessionMutation.error.message}</p>}
          {createStage1IterationMutation.error && <p className="panel-copy error-text">{createStage1IterationMutation.error.message}</p>}
          {scoreStage1TrainingMutation.error && <p className="panel-copy error-text">{scoreStage1TrainingMutation.error.message}</p>}
          {generateStage1FailureAuditMutation.error && <p className="panel-copy error-text">{generateStage1FailureAuditMutation.error.message}</p>}
          {fetchStage1AgentPromptMutation.error && <p className="panel-copy error-text">{fetchStage1AgentPromptMutation.error.message}</p>}
          {runStage1CanonicalMutation.error && <p className="panel-copy error-text">{runStage1CanonicalMutation.error.message}</p>}
          {runStage2CaptureMutation.error && <p className="panel-copy error-text">{runStage2CaptureMutation.error.message}</p>}
          {runStage3GridMutation.error && <p className="panel-copy error-text">{runStage3GridMutation.error.message}</p>}
          {runStage3PyramidMutation.error && <p className="panel-copy error-text">{runStage3PyramidMutation.error.message}</p>}
          {runStage4RealizedExpectancyMutation.error && <p className="panel-copy error-text">{runStage4RealizedExpectancyMutation.error.message}</p>}
          <div className="dev-workspace-header">
            <div>
              <h2>{selectedRow ? `${selectedRow.asset} / ${selectedRow.signal_engine_id}` : "Select a candidate"}</h2>
              <span>{selectedSession ? `${selectedSession.strategy_id} @ ${selectedSession.strategy_version}` : selectedRow?.strategy_id ?? "Stage 1 not started"}</span>
            </div>
            <div>
              <span>Inherited Windows</span>
              <strong className="split-window-stack">{effectiveRun ? formatStage0SplitWindows(effectiveRun) : "n/a"}</strong>
            </div>
            <div>
              <span>Current blocker</span>
              <strong className={gate?.blockers.length ? "error-text" : "pass-text"}>{gate?.blockers[0] ?? (selectedSession ? "No blocker" : "Stage 1 session not started")}</strong>
            </div>
            <button
              type="button"
              className="primary"
              disabled={!selectedRow || selectedRow.next_action.disabled || runStage3GridMutation.isPending || runStage3PyramidMutation.isPending || runStage4RealizedExpectancyMutation.isPending}
              onClick={runNextAction}
            >
              <Play size={16} />{selectedRow?.next_action.label ?? "Select Candidate"}
            </button>
          </div>
          <DevelopmentLifecycle row={selectedRow} gate={gate} activeStage={activeStage} onStageChange={setActiveStage} />
          {activeStage === "stage0" && (
            <DevelopmentStage0Panel row={selectedRow} run={effectiveRun} />
          )}
          {activeStage === "stage1" && (
          <section className="dev-stage1-panel">
            <div className="stage-heading compact">
              <div>
                <h2>Stage 1: Direction Strategy Development</h2>
                <p className="panel-copy">Build deterministic strategy scripts on the training window, then freeze only if the walk-forward test passes.</p>
              </div>
            </div>
            {!selectedSession && selectedRow && (
              <div className="dev-start-stage1">
                <strong>Stage 1 has not been started for {selectedRow.asset}.</strong>
                <small>Creates a deterministic strategy workspace using the selected Stage 0 batch windows: {formatStage0SplitWindows(effectiveRun)}.</small>
                <button type="button" className="primary" disabled={createStage1SessionMutation.isPending} onClick={createStage1Session}>
                  <Play size={16} />Start Stage 1
                </button>
              </div>
            )}
            <Stage1EvidenceModeBanner mode={evidenceMode} />
            <DevelopmentGateSummary gate={gate} />
            {selectedSession && (
              <>
                <DevelopmentStage1Lanes
                  session={selectedSession}
                  gate={gate}
                  roleIterations={roleIterations}
                  creatingIteration={createStage1IterationMutation.isPending}
                  runningCanonical={runStage1CanonicalMutation.isPending}
                  onCreateBundle={createBundle}
                  onRunCanonical={() => runStage1CanonicalMutation.mutate({ session_id: selectedSession.session_id })}
                />
                <div className="dev-lower-grid">
                  <DevelopmentIterationHistory
                    sessionId={selectedSession.session_id}
                    frozen={Boolean(gate?.canonical_readout.exists)}
                    iterations={iterations}
                    loading={iterationsQuery.isLoading}
                    error={iterationsQuery.error}
                    promptLoadingIterationId={fetchStage1AgentPromptMutation.isPending ? fetchStage1AgentPromptMutation.variables?.iteration_id ?? null : null}
                    scoringIterationId={scoreStage1TrainingMutation.isPending ? scoreStage1TrainingMutation.variables?.iteration_id ?? null : null}
                    auditingIterationId={generateStage1FailureAuditMutation.isPending ? generateStage1FailureAuditMutation.variables?.iteration_id ?? null : null}
                    deletingIterationId={deleteStage1IterationMutation.variables?.iteration_id ?? null}
                    promptLoading={fetchStage1AgentPromptMutation.isPending}
                    scoring={scoreStage1TrainingMutation.isPending}
                    auditing={generateStage1FailureAuditMutation.isPending}
                    deleting={deleteStage1IterationMutation.isPending}
                    deleteError={deleteStage1IterationMutation.error}
                    onOpenPrompt={openAgentPrompt}
                    onScore={(iteration) => scoreStage1TrainingMutation.mutate({
                      session_id: selectedSession.session_id,
                      iteration_id: iteration.iteration_id,
                      sample_role: stage1ScoreRoleForIteration(iteration),
                    })}
                    onAudit={(iteration) => generateStage1FailureAuditMutation.mutate({
                      session_id: selectedSession.session_id,
                      iteration_id: iteration.iteration_id,
                      sample_role: stage1ScoreRoleForIteration(iteration),
                    })}
                    onDelete={(iterationId) => deleteStage1IterationMutation.mutate({
                      session_id: selectedSession.session_id,
                      iteration_id: iterationId,
                    })}
                  />
                </div>
                <details className="dev-artifacts">
                  <summary>Artifacts</summary>
                  <small>Stage 2/3 scores: {gate?.canonical_readout.scores_path ?? "promotion/stage1a_canonical_full_cycle_scores.json"}</small>
                  <small>Stage 4 decisions: {gate?.canonical_readout.decisions_path ?? "promotion/stage1a_canonical_full_cycle_decisions.json"}</small>
                </details>
              </>
            )}
          </section>
          )}
          {activeStage === "stage2" && selectedSession && (
            <DevelopmentStage2Panel
              gate={gate}
              running={runStage2CaptureMutation.isPending}
              onRun={() => runStage2CaptureMutation.mutate({ session_id: selectedSession.session_id })}
            />
          )}
          {activeStage === "stage3" && selectedSession && (
            <DevelopmentStage3Panel
              gate={gate}
              gridRunning={runStage3GridMutation.isPending}
              pyramidRunning={runStage3PyramidMutation.isPending}
              onRunGrid={() => runStage3GridMutation.mutate({ session_id: selectedSession.session_id })}
              onRunPyramid={() => runStage3PyramidMutation.mutate({ session_id: selectedSession.session_id })}
            />
          )}
          {activeStage === "stage4" && (
            <DevelopmentStage4Panel
              gate={gate}
              running={runStage4RealizedExpectancyMutation.isPending}
              onRun={() => selectedSession && runStage4RealizedExpectancyMutation.mutate({ session_id: selectedSession.session_id })}
            />
          )}
        </section>
      </div>
      <Stage1AgentPromptModal prompt={agentPrompt} onClose={() => setAgentPrompt(null)} />
    </article>
  );
}

function DevelopmentLifecycle({
  row,
  gate,
  activeStage,
  onStageChange,
}: {
  row: DevelopmentQueueRow | null;
  gate: Stage1GateSummary | null;
  activeStage: ResearchStageId;
  onStageChange: (stage: ResearchStageId) => void;
}) {
  const stage2Complete = Boolean(gate?.stage2_capture.exists);
  const stage2Ready = Boolean(gate?.canonical_readout.exists);
  const stage3GridComplete = Boolean(gate?.stage3_grid.exists);
  const stage3Complete = Boolean(gate?.stage3_pyramid.exists);
  const stage4Complete = Boolean(gate?.stage4_realized_expectancy.exists);
  const stage3Ready = stage2Complete;
  const lifecycle: Array<{ id: ResearchStageId; label: string; state: string; status: string }> = [
    { id: "stage0", label: "Stage 0", state: row?.stage0_status === "accepted" ? "Passed" : "Blocked", status: row?.stage0_status === "accepted" ? "pass" : "locked" },
    { id: "stage1", label: "Stage 1", state: gate?.canonical_readout.exists ? "Frozen" : row?.stage1_session_id ? "In Progress" : "Not Started", status: gate?.canonical_readout.exists ? "pass" : row?.stage1_session_id ? "active" : "locked" },
    { id: "stage2", label: "Stage 2", state: stage2Complete ? "Complete" : stage2Ready ? "Ready" : "Locked", status: stage2Complete ? "pass" : stage2Ready ? "active" : "locked" },
    { id: "stage3", label: "Stage 3", state: stage3Complete ? "Pyramid Complete" : stage3GridComplete ? "Pyramid Ready" : stage3Ready ? "Grid Ready" : "Locked", status: stage3Complete ? "pass" : stage3Ready ? "active" : "locked" },
    { id: "stage4", label: "Stage 4", state: stage4Complete ? "Complete" : stage3Complete ? "Ready" : "Locked", status: stage4Complete ? "pass" : stage3Complete ? "active" : "locked" },
  ];
  return (
    <section className="dev-lifecycle" aria-label="Candidate stage lifecycle">
      {lifecycle.map((item, index) => (
        <React.Fragment key={item.label}>
          <button type="button" className={`dev-lifecycle-step ${item.status} ${activeStage === item.id ? "selected" : ""}`} onClick={() => onStageChange(item.id)}>
            <strong>{item.label}</strong>
            <span>{item.state}</span>
          </button>
          {index < lifecycle.length - 1 && <ChevronRight size={18} className="stage-arrow" />}
        </React.Fragment>
      ))}
    </section>
  );
}

function Stage1EvidenceModeBanner({ mode }: { mode: Stage1EvidenceMode }) {
  return (
    <section className={`stage1-mode-banner ${mode.status}`} aria-label="Stage 1 evidence mode">
      <div className="stage1-mode-title">
        <span>Current Evidence Mode</span>
        <strong>{mode.title}</strong>
      </div>
      <div className="stage1-mode-grid">
        <div>
          <span>Allowed Evidence</span>
          <strong>{mode.allowedEvidence}</strong>
        </div>
        <div>
          <span>Agent Use</span>
          <strong>{mode.agentUse}</strong>
        </div>
        <div>
          <span>Next Action</span>
          <strong>{mode.nextAction}</strong>
        </div>
        <div>
          <span>Return Path</span>
          <strong>{mode.returnPath}</strong>
        </div>
      </div>
    </section>
  );
}

function DevelopmentStage0Panel({
  row,
  run,
}: {
  row: DevelopmentQueueRow | null;
  run: Stage0UniverseRun | null;
}) {
  return (
    <section className="dev-stage0-panel">
      <div className="stage-heading compact">
        <div>
          <h2>Stage 0: Candidate Gate</h2>
          <p className="panel-copy">Stage 0 evaluates the selected batch horizon and decides whether this engine-asset pair has enough travel to develop.</p>
        </div>
      </div>
      <div className="stage2-summary-strip">
        <div>
          <span>Status</span>
          <strong>{row?.stage0_status ?? "n/a"}</strong>
        </div>
        <div>
          <span>Trigger Rate</span>
          <strong>{row?.trigger_rate_pct === null || row?.trigger_rate_pct === undefined ? "n/a" : `${row.trigger_rate_pct}%`}</strong>
        </div>
        <div>
          <span>Evaluated Signals</span>
          <strong>{formatNumber(row?.stage0_evaluated_signal_count ?? 0)}</strong>
        </div>
      </div>
      <p className="panel-copy">{run ? formatStage0SplitWindows(run) : "No Stage 0 batch selected."}</p>
    </section>
  );
}

function DevelopmentGateSummary({ gate }: { gate: Stage1GateSummary | null }) {
  const rows: Array<{ label: string; value: string; status: string }> = [
    { label: "Training", value: gateSummaryValue(gate, "training"), status: gate?.roles.training?.status ?? "missing" },
    { label: "Walk-Forward", value: gateSummaryValue(gate, "walk_forward_test"), status: gate?.roles.walk_forward_test?.status ?? "missing" },
    { label: "Freeze", value: gate?.canonical_readout.exists ? "complete" : gate?.ready_to_freeze ? "ready" : "blocked", status: gate?.canonical_readout.exists ? "pass" : gate?.ready_to_freeze ? "pass" : "fail" },
  ];
  return (
    <section className="dev-gate-summary">
      {rows.map((row) => (
        <div key={row.label}>
          <span>{row.label}</span>
          <strong className={row.status === "pass" ? "pass-text" : row.status === "fail" ? "error-text" : "warn-text"}>{row.status}</strong>
          <small>{row.value}</small>
        </div>
      ))}
    </section>
  );
}

function DevelopmentStage1Lanes({
  session,
  gate,
  roleIterations,
  creatingIteration,
  runningCanonical,
  onCreateBundle,
  onRunCanonical,
}: {
  session: Stage1ResearchSession;
  gate: Stage1GateSummary | null;
  roleIterations: Record<Stage1SampleRole, Stage1IterationSummary[]>;
  creatingIteration: boolean;
  runningCanonical: boolean;
  onCreateBundle: (role: Stage1SampleRole) => void;
  onRunCanonical: () => void;
}) {
  const isFrozen = Boolean(gate?.canonical_readout.exists);
  return (
    <section className="dev-stage1-lanes">
      {stage1Roles.map((role, index) => {
        const latest = roleIterations[role][roleIterations[role].length - 1] ?? null;
        const score = latest ? stage1ScoreForRole(latest, role) : null;
        const roleStatus = gate?.roles[role]?.status ?? "missing";
        return (
          <div className="dev-stage1-lane" key={role}>
            <div className="stage1-lane-head">
              <span>{index + 1}. {stage1RoleLabel(role)}</span>
              <strong className={roleStatus === "pass" ? "pass-text" : roleStatus === "fail" ? "error-text" : "warn-text"}>{roleStatus}</strong>
            </div>
            <small>Latest iteration</small>
            <strong>{latest?.iteration_id ?? "-"}</strong>
            <small>Score</small>
            <strong className={score?.metrics.passes_threshold ? "pass-text" : score ? "warn-text" : ""}>{score ? stage1Agreement(score.metrics.directional_agreement) : "No score yet"}</strong>
            <div className="lane-actions">
              <button
                type="button"
                disabled={isFrozen || creatingIteration || (role === "walk_forward_test" && gate?.roles.training?.status !== "pass")}
                onClick={() => onCreateBundle(role)}
              >
                <Play size={16} />Create {role === "training" ? "Training" : "Walk-Forward"} Bundle
              </button>
            </div>
          </div>
        );
      })}
      <div className="dev-stage1-lane freeze">
        <div className="stage1-lane-head">
          <span>3. Freeze</span>
          <strong className={gate?.ready_to_freeze ? "pass-text" : "error-text"}>{gate?.canonical_readout.exists ? "complete" : gate?.ready_to_freeze ? "ready" : "blocked"}</strong>
        </div>
        <small>Waits for walk-forward pass</small>
        <strong>{gate?.canonical_readout.exists ? `${gate.canonical_readout.match_count} matches` : "-"}</strong>
        <small>Score</small>
        <strong>-</strong>
        <div className="lane-actions">
          <button type="button" disabled={isFrozen || !gate?.ready_to_freeze || runningCanonical} onClick={onRunCanonical}>
            <Play size={16} />{isFrozen ? "Canonical Readout Complete" : "Run Canonical Readout"}
          </button>
        </div>
      </div>
      <span hidden>{session.session_id}</span>
    </section>
  );
}

function DevelopmentStage2Panel({
  gate,
  running,
  onRun,
}: {
  gate: Stage1GateSummary | null;
  running: boolean;
  onRun: () => void;
}) {
  const canonicalReady = Boolean(gate?.canonical_readout.exists);
  const stage2 = gate?.stage2_capture;
  const complete = Boolean(stage2?.exists);
  return (
    <section className="dev-stage2-panel">
      <div className="stage-heading compact">
        <div>
          <h2>Stage 2: Travel Capture</h2>
          <p className="panel-copy">Measure TP hit rates on the frozen Stage 1 MATCH set. This narrows the Stage 3 execution grid.</p>
        </div>
        <button type="button" className="primary" disabled={!canonicalReady || complete || running} onClick={onRun}>
          <Play size={16} />{complete ? "Travel Capture Complete" : running ? "Running..." : "Run Travel Capture"}
        </button>
      </div>
      {!canonicalReady && <p className="panel-copy">Stage 2 unlocks after the canonical Stage 1 readout is frozen.</p>}
      {canonicalReady && !complete && <p className="panel-copy">Ready to run on {formatNumber(gate?.canonical_readout.match_count ?? 0)} matched Stage 1 decisions.</p>}
      {complete && stage2 && (
        <>
          <div className="stage2-summary-strip">
            <div>
              <span>MATCH signals</span>
              <strong>{formatNumber(stage2.metrics.total_match_signals ?? 0)}</strong>
            </div>
            <div>
              <span>Training</span>
              <strong>{formatNumber(stage2.metrics.slice_counts?.training ?? 0)}</strong>
            </div>
            <div>
              <span>Walk-forward</span>
              <strong>{formatNumber(stage2.metrics.slice_counts?.walk_forward_test ?? 0)}</strong>
            </div>
          </div>
          <div className="table stage2-capture-table">
            <div className="row header">
              <span>TP</span>
              <span>Training</span>
              <span>Walk-forward</span>
              <span>Full Cycle</span>
            </div>
            {Object.entries(stage2.results).map(([level, rows]) => (
              <div className="row" key={level}>
                <span>{level}%</span>
                <span>{formatCaptureRate(rows.training)}</span>
                <span>{formatCaptureRate(rows.walk_forward_test)}</span>
                <span>{formatCaptureRate(rows.full_cycle)}</span>
              </div>
            ))}
          </div>
          <details className="dev-artifacts">
            <summary>Stage 2 Artifacts</summary>
            <small>Capture curve: {stage2.capture_curve_path}</small>
            <small>Per signal: {stage2.per_signal_path}</small>
            <small>Summary: {stage2.summary_path}</small>
          </details>
        </>
      )}
    </section>
  );
}

function DevelopmentIterationHistory({
  sessionId,
  frozen,
  iterations,
  loading,
  error,
  promptLoadingIterationId,
  scoringIterationId,
  auditingIterationId,
  deletingIterationId,
  promptLoading,
  scoring,
  auditing,
  deleting,
  deleteError,
  onOpenPrompt,
  onScore,
  onAudit,
  onDelete,
}: {
  sessionId: string;
  frozen: boolean;
  iterations: Stage1IterationSummary[];
  loading: boolean;
  error: Error | null;
  promptLoadingIterationId: string | null;
  scoringIterationId: string | null;
  auditingIterationId: string | null;
  deletingIterationId: string | null;
  promptLoading: boolean;
  scoring: boolean;
  auditing: boolean;
  deleting: boolean;
  deleteError: Error | null;
  onOpenPrompt: (iteration: Stage1IterationSummary) => void;
  onScore: (iteration: Stage1IterationSummary) => void;
  onAudit: (iteration: Stage1IterationSummary) => void;
  onDelete: (iterationId: string) => void;
}) {
  return (
    <section className="dev-iteration-history">
      <div className="summary-line">
        <strong>Iteration History</strong>
        <span>{formatNumber(iterations.length)} runs</span>
      </div>
      {loading && <p className="panel-copy">Loading iterations...</p>}
      {error && <p className="panel-copy error-text">{error.message}</p>}
      <div className="dev-iteration-table">
        <div className="dev-iteration-row header">
          <span>Iteration</span>
          <span>Slice</span>
          <span>Bundle Type</span>
          <span>Score</span>
          <span>Audit</span>
          <span>Agent Use</span>
          <span>Action</span>
        </div>
        {iterations.slice().reverse().map((iteration) => {
          const role = stage1ScoreRoleForIteration(iteration);
          const score = stage1ScoreForRole(iteration, role);
          const isPromptLoading = promptLoading && promptLoadingIterationId === iteration.iteration_id;
          const isScoring = scoring && scoringIterationId === iteration.iteration_id;
          const isAuditing = auditing && auditingIterationId === iteration.iteration_id;
          const isDeleting = deleting && deletingIterationId === iteration.iteration_id;
          const canAudit = Boolean(score);
          return (
            <div className="dev-iteration-row" key={iteration.iteration_id}>
              <strong>{iteration.iteration_id}</strong>
              <span>{stage1IterationPhaseLabel(iteration)}</span>
              <span>{stage1BundleLabel(iteration)}</span>
              <span className={score?.metrics.passes_threshold ? "pass-text" : score ? "warn-text" : ""}>{score ? stage1Agreement(score.metrics.directional_agreement) : "-"}</span>
              <span>{iteration.has_failure_audit ? "yes" : "-"}</span>
              <span className={`agent-use-badge ${role}`}>{stage1AgentUseLabel(role)}</span>
              <div className="iteration-actions">
                <button type="button" disabled={isPromptLoading} onClick={() => onOpenPrompt(iteration)}>
                  Prompt
                </button>
                <button type="button" disabled={frozen || isScoring} onClick={() => onScore(iteration)}>
                  Score
                </button>
                <button type="button" disabled={frozen || !canAudit || isAuditing} onClick={() => onAudit(iteration)}>
                  Audit
                </button>
                <button
                  type="button"
                  className="icon-danger"
                  disabled={frozen || isDeleting}
                  title={`Delete ${iteration.iteration_id}`}
                  onClick={() => {
                    const confirmed = window.confirm(`Delete ${iteration.iteration_id} from ${sessionId}? This removes the iteration folder and its decisions, scores, audits, and prompts.`);
                    if (confirmed) {
                      onDelete(iteration.iteration_id);
                    }
                  }}
                >
                  <Trash2 size={15} />
                </button>
              </div>
            </div>
          );
        })}
      </div>
      {deleteError && <p className="panel-copy error-text">{deleteError.message}</p>}
    </section>
  );
}

function DevelopmentStage3Panel({
  gate,
  gridRunning,
  pyramidRunning,
  onRunGrid,
  onRunPyramid,
}: {
  gate: Stage1GateSummary | null;
  gridRunning: boolean;
  pyramidRunning: boolean;
  onRunGrid: () => void;
  onRunPyramid: () => void;
}) {
  const stage2Complete = Boolean(gate?.stage2_capture.exists);
  const stage3 = gate?.stage3_grid;
  const pyramid = gate?.stage3_pyramid;
  const gridComplete = Boolean(stage3?.exists);
  const pyramidComplete = Boolean(pyramid?.exists);
  const best = stage3?.best ?? {};
  const pyramidBest = pyramid?.best ?? {};
  return (
    <section className="dev-stage3-panel">
      <div className="stage-heading compact">
        <div>
          <h2>Stage 3: Execution Setup</h2>
          <p className="panel-copy">Run execution setup substeps on Stage 2 MATCH signals: first TP/SL grid, then pyramid behavior.</p>
        </div>
      </div>
      {!stage2Complete && <p className="panel-copy">Stage 3 unlocks after Stage 2 travel capture writes the per-signal MATCH artifact.</p>}
      <section className="stage3-substep">
        <div className="stage-heading compact">
          <div>
            <h3>TP/SL Grid</h3>
            <p className="panel-copy">Find the best one-leg market-entry TP/SL setup using the skill grid semantics.</p>
          </div>
          <button type="button" className="primary" disabled={!stage2Complete || gridComplete || gridRunning} onClick={onRunGrid}>
            <Play size={16} />{gridComplete ? "Grid Complete" : gridRunning ? "Running..." : "Run Grid"}
          </button>
        </div>
        {stage2Complete && !gridComplete && (
          <p className="panel-copy">
            Ready to test market-entry TP/SL combinations against {formatNumber(gate?.stage2_capture.metrics.total_match_signals ?? 0)} Stage 2 MATCH signals.
          </p>
        )}
      {gridComplete && stage3 && (
        <>
          <div className="stage3-summary-strip">
            <div>
              <span>Best TP / SL</span>
              <strong>{formatStage3Pct(best.tp)} / {formatStage3Pct(best.sl)}</strong>
            </div>
            <div>
              <span>Win Rate</span>
              <strong>{formatStage3Pct(best.wr)}</strong>
            </div>
            <div>
              <span>Expectancy</span>
              <strong>{formatStage3Pct(best.expectancy)}</strong>
            </div>
            <div>
              <span>PnL @ {stage3.leverage ?? "-"}x</span>
              <strong>{formatStage3Pct(best.pnl_pct)}</strong>
            </div>
          </div>
          <div className="table stage3-grid-table">
            <div className="row header">
              <span>Setup</span>
              <span>WR</span>
              <span>TP / SL / Neither</span>
              <span>Expectancy</span>
              <span>PF</span>
              <span>PnL</span>
            </div>
            {stage3.top_5.map((row) => (
              <div className="row" key={`${row.tp}-${row.sl}`}>
                <span>{formatStage3Pct(row.tp)} TP / {formatStage3Pct(row.sl)} SL</span>
                <span>{formatStage3Pct(row.wr)}</span>
                <span>{formatNumber(row.tp_count)} / {formatNumber(row.sl_count)} / {formatNumber(row.neither)}</span>
                <span>{formatStage3Pct(row.expectancy)}</span>
                <span>{row.profit_factor === 999 ? "inf" : row.profit_factor.toFixed(2)}</span>
                <span>{formatStage3Pct(row.pnl_pct)}</span>
              </div>
            ))}
          </div>
          <details className="dev-artifacts">
            <summary>Stage 3 Artifacts</summary>
            <small>Grid results: {stage3.grid_results_path}</small>
            <small>Optimal setup: {stage3.optimal_path}</small>
            <small>Stage 4 candidates: {stage3.stage4_candidates_path}</small>
            <small>Summary: {stage3.summary_path}</small>
          </details>
        </>
      )}
      </section>
      <section className="stage3-substep">
        <div className="stage-heading compact">
          <div>
            <h3>Pyramid</h3>
            <p className="panel-copy">Use the grid’s best TP/SL and test add-leg spacing against the one-leg baseline.</p>
          </div>
          <button type="button" className="primary" disabled={!gridComplete || pyramidComplete || pyramidRunning} onClick={onRunPyramid}>
            <Play size={16} />{pyramidComplete ? "Pyramid Complete" : pyramidRunning ? "Running..." : "Run Pyramid"}
          </button>
        </div>
        {!gridComplete && <p className="panel-copy">Pyramid unlocks after the TP/SL grid writes `promotion/stage3_optimal.json`.</p>}
        {gridComplete && !pyramidComplete && (
          <p className="panel-copy">Ready to compare pyramid step sizes against the best one-leg grid setup.</p>
        )}
        {pyramidComplete && pyramid && (
          <>
            <div className="stage3-summary-strip">
              <div>
                <span>Grid TP / SL</span>
                <strong>{formatStage3Pct(pyramid.tp_pct ?? undefined)} / {formatStage3Pct(pyramid.sl_pct ?? undefined)}</strong>
              </div>
              <div>
                <span>Best Step</span>
                <strong>{pyramidBest.step_pct === null || pyramidBest.step_pct === undefined ? "-" : formatStage3Pct(pyramidBest.step_pct)}</strong>
              </div>
              <div>
                <span>Delta vs Baseline</span>
                <strong>{formatStage3Pct(pyramidBest.delta_vs_baseline_pct)}</strong>
              </div>
              <div>
                <span>PnL</span>
                <strong>{formatStage3Pct(pyramidBest.pnl_pct)}</strong>
              </div>
            </div>
            <div className="table stage3-pyramid-table">
              <div className="row header">
                <span>Step</span>
                <span>PnL</span>
                <span>Delta</span>
                <span>Avg Legs</span>
                <span>Wins / Losses</span>
                <span>Result</span>
              </div>
              {pyramid.results.map((row) => (
                <div className="row" key={`${row.step_pct}`}>
                  <span>{row.step_pct === null ? "baseline" : formatStage3Pct(row.step_pct)}</span>
                  <span>{formatStage3Pct(row.pnl_pct)}</span>
                  <span>{formatStage3Pct(row.delta_vs_baseline_pct)}</span>
                  <span>{row.avg_legs_per_signal.toFixed(2)}</span>
                  <span>{formatNumber(row.wins)} / {formatNumber(row.losses)}</span>
                  <span>{row.comparison ?? "-"}</span>
                </div>
              ))}
            </div>
            <details className="dev-artifacts">
              <summary>Stage 3 Pyramid Artifacts</summary>
              <small>Results: {pyramid.results_path}</small>
              <small>Optimal setup: {pyramid.optimal_path}</small>
              <small>Stage 4 candidates: {pyramid.stage4_candidates_path}</small>
              <small>Summary: {pyramid.summary_path}</small>
            </details>
          </>
        )}
      </section>
      {pyramidComplete && <p className="panel-copy">Stage 4 is ready. It will consume the Stage 4 candidate setups and the full frozen Stage 1 decision set.</p>}
    </section>
  );
}

function DevelopmentStage4Panel({
  gate,
  running,
  onRun,
}: {
  gate: Stage1GateSummary | null;
  running: boolean;
  onRun: () => void;
}) {
  const pyramidComplete = Boolean(gate?.stage3_pyramid.exists);
  const stage4 = gate?.stage4_realized_expectancy;
  const complete = Boolean(stage4?.exists);
  const best = stage4?.best_candidate ?? {};
  return (
    <section className="dev-stage4-panel">
      <div className="stage-heading compact">
        <div>
          <h2>Stage 4: Realized Expectancy</h2>
          <p className="panel-copy">Test shortlisted execution setups on every frozen Stage 1 decision, including skipped/flat decisions and costs.</p>
        </div>
        <button type="button" className="primary" disabled={!pyramidComplete || complete || running} onClick={onRun}>
          <Play size={16} />{complete ? "Stage 4 Complete" : running ? "Running..." : "Run Realized Expectancy"}
        </button>
      </div>
      {!pyramidComplete && <p className="panel-copy">Locked until Stage 3 grid and pyramid artifacts are complete.</p>}
      {pyramidComplete && !complete && (
        <p className="panel-copy">Ready to score `promotion/stage4_candidates.json` against the full canonical Stage 1 decision set.</p>
      )}
      {complete && stage4 && (
        <>
          <div className="stage3-summary-strip">
            <div>
              <span>Best Candidate</span>
              <strong>{stage4.best_candidate_id ?? best.candidate_id ?? "-"}</strong>
            </div>
            <div>
              <span>Net Expectancy</span>
              <strong>{formatStage3Pct(best.net_expectancy_pct)}</strong>
            </div>
            <div>
              <span>Executed / Decisions</span>
              <strong>{formatNumber(best.executed_trades ?? 0)} / {formatNumber(best.total_decisions ?? 0)}</strong>
            </div>
            <div>
              <span>Profit Factor</span>
              <strong>{best.profit_factor === 999 ? "inf" : typeof best.profit_factor === "number" ? best.profit_factor.toFixed(2) : "-"}</strong>
            </div>
          </div>
          <div className="table stage4-results-table">
            <div className="row header">
              <span>Candidate</span>
              <span>Net Exp</span>
              <span>TP / SL / TO</span>
              <span>Skipped</span>
              <span>Win Rate</span>
              <span>Net PnL</span>
            </div>
            {stage4.candidates.map((candidate) => (
              <div className="row" key={candidate.candidate_id}>
                <span>{candidate.candidate_id}</span>
                <span>{formatStage3Pct(candidate.net_expectancy_pct)}</span>
                <span>{formatNumber(candidate.tp_hits ?? 0)} / {formatNumber(candidate.sl_hits ?? 0)} / {formatNumber(candidate.no_hit ?? 0)}</span>
                <span>{formatNumber(candidate.skipped_decisions ?? 0)}</span>
                <span>{formatStage3Pct(candidate.win_rate_pct)}</span>
                <span>{formatStage3Pct(candidate.net_pnl_pct)}</span>
              </div>
            ))}
          </div>
          <details className="dev-artifacts">
            <summary>Stage 4 Artifacts</summary>
            <small>Realized expectancy: {stage4.realized_expectancy_path}</small>
            <small>Trade ledger: {stage4.trade_ledger_path}</small>
            <small>Optimal setup: {stage4.optimal_path}</small>
            <small>Summary: {stage4.summary_path}</small>
          </details>
        </>
      )}
    </section>
  );
}

function Stage1AgentPromptModal({
  prompt,
  onClose,
}: {
  prompt: Stage1AgentPrompt | null;
  onClose: () => void;
}) {
  const [copied, setCopied] = React.useState(false);
  React.useEffect(() => {
    setCopied(false);
  }, [prompt?.iteration_id, prompt?.prompt_type]);
  if (!prompt) {
    return null;
  }
  const copyPrompt = () => {
    void navigator.clipboard.writeText(prompt.prompt).then(() => setCopied(true));
  };
  return (
    <div className="modal-backdrop" role="presentation" onMouseDown={onClose}>
      <section className="prompt-modal" role="dialog" aria-modal="true" aria-labelledby="stage1-agent-prompt-title" onMouseDown={(event) => event.stopPropagation()}>
        <div className="prompt-modal-header">
          <div>
            <span className="stage-kicker">Agent Handoff</span>
            <h2 id="stage1-agent-prompt-title">{prompt.iteration_id}</h2>
            <small>{prompt.prompt_type.replaceAll("_", " ")} · {prompt.prompt_path}</small>
          </div>
          <button type="button" onClick={onClose}>Close</button>
        </div>
        <textarea readOnly value={prompt.prompt} />
        <div className="prompt-modal-actions">
          <button type="button" className="primary" onClick={copyPrompt}>
            {copied ? "Copied" : "Copy Prompt"}
          </button>
        </div>
      </section>
    </div>
  );
}

function SignalEnginesPanel({
  engines,
  loading,
  error
}: {
  engines?: SignalEngine[];
  loading: boolean;
  error: Error | null;
}) {
  const [selectedEngineId, setSelectedEngineId] = React.useState<string | null>(null);
  const [selectedSignalSetKey, setSelectedSignalSetKey] = React.useState<string | null>(null);
  const [signalUpdateResult, setSignalUpdateResult] = React.useState<SignalPoolExtendResult | null>(null);
  const effectiveEngineId = selectedEngineId ?? engines?.[0]?.signal_engine_id ?? null;
  const signalSetsQuery = useQuery({
    queryKey: ["signal-sets", effectiveEngineId],
    queryFn: () => fetchSignalSets(effectiveEngineId as string),
    enabled: Boolean(effectiveEngineId)
  });
  const signalSets = signalSetsQuery.data?.signal_sets ?? [];
  const effectiveSignalSetKey = selectedSignalSetKey ?? signalSets[0]?.signal_set_key ?? null;
  const signalsQuery = useQuery({
    queryKey: ["signals", effectiveSignalSetKey],
    queryFn: () => fetchSignals(effectiveSignalSetKey as string),
    enabled: Boolean(effectiveSignalSetKey)
  });
  const signalUpdateMutation = useMutation({
    mutationFn: extendSignalPoolFromLocalCandles,
    onSuccess: (result) => {
      setSignalUpdateResult(result);
      setSelectedSignalSetKey(result.signal_set_key);
      queryClient.invalidateQueries({ queryKey: ["signal-engines"] });
      queryClient.invalidateQueries({ queryKey: ["signal-sets", result.signal_engine_id] });
      queryClient.invalidateQueries({ queryKey: ["signals", result.signal_set_key] });
    }
  });

  React.useEffect(() => {
    setSelectedSignalSetKey(null);
    setSignalUpdateResult(null);
  }, [effectiveEngineId]);

  return (
    <article className="panel large" id="signals">
      <div className="panel-header">
        <h2>Signal Engines</h2>
        <span className="pill">neutral packets</span>
      </div>
      {loading && <p className="panel-copy">Loading signal engine catalog...</p>}
      {error && <p className="panel-copy error-text">{error.message}</p>}
      {engines && engines.length === 0 && <p className="panel-copy">No signal engines registered yet.</p>}
      {engines && engines.length > 0 && (
        <div className="signal-layout">
          <div className="engine-list">
            {engines.map((engine) => (
              <button
                type="button"
                className={engine.signal_engine_id === effectiveEngineId ? "engine-card selected" : "engine-card"}
                key={engine.signal_engine_id}
                onClick={() => setSelectedEngineId(engine.signal_engine_id)}
              >
                <strong>{engine.name}</strong>
                <span>{engine.signal_engine_id}@{engine.version ?? "n/a"}</span>
                <small>{formatNumber(engine.signal_set_count)} sets · {formatNumber(engine.packet_count)} packets</small>
              </button>
            ))}
          </div>
          <div className="signal-detail">
            {signalSetsQuery.isLoading && <p className="panel-copy">Loading signal sets...</p>}
            {signalSetsQuery.error && <p className="panel-copy error-text">{signalSetsQuery.error.message}</p>}
            {signalSets.length > 0 && (
              <>
                <div className="table signal-set-table">
                  <div className="row header">
                    <span>Set</span>
                    <span>Asset</span>
                    <span>Scanned Coverage</span>
                    <span>Packets</span>
                    <span>Schema</span>
                    <span>Local Update</span>
                  </div>
                  {signalSets.map((set) => (
                    <div
                      className={set.signal_set_key === effectiveSignalSetKey ? "row signal-set-row selected" : "row signal-set-row"}
                      key={set.signal_set_key}
                      onClick={() => setSelectedSignalSetKey(set.signal_set_key)}
                      role="button"
                      tabIndex={0}
                      onKeyDown={(event) => {
                        if (event.key === "Enter" || event.key === " ") {
                          setSelectedSignalSetKey(set.signal_set_key);
                        }
                      }}
                    >
                      <span>{set.signal_set_id}</span>
                      <span>{set.asset}</span>
                      <span>{formatTimestamp(set.coverage_start_ts ?? set.start_ts)} - {formatTimestamp(set.coverage_end_ts ?? set.end_ts)}</span>
                      <span>{formatNumber(set.packet_count)} · last {formatTimestamp(set.packet_end_ts ?? set.end_ts)}</span>
                      <span>{set.payload_schema}</span>
                      <span>
                        <button
                          className="compact-action"
                          type="button"
                          disabled={signalUpdateMutation.isPending}
                          onClick={(event) => {
                            event.stopPropagation();
                            setSelectedSignalSetKey(set.signal_set_key);
                            setSignalUpdateResult(null);
                            signalUpdateMutation.mutate({
                              signal_engine_id: set.signal_engine_id,
                              asset: set.asset
                            });
                          }}
                        >
                          <RefreshCw size={14} />
                          {signalUpdateMutation.isPending && signalUpdateMutation.variables?.asset === set.asset ? "Updating" : "Update Signals"}
                        </button>
                      </span>
                    </div>
                  ))}
                </div>
                {(signalUpdateMutation.error || signalUpdateResult) && (
                  <div className={signalUpdateMutation.error ? "inline-status error" : "inline-status"}>
                    {signalUpdateMutation.error
                      ? signalUpdateMutation.error.message
                      : formatSignalUpdateResult(signalUpdateResult)}
                  </div>
                )}
                <SignalPacketPreview signals={signalsQuery.data?.signals} loading={signalsQuery.isLoading} error={signalsQuery.error} />
              </>
            )}
          </div>
        </div>
      )}
    </article>
  );
}

function SignalPacketPreview({
  signals,
  loading,
  error
}: {
  signals?: SignalRecord[];
  loading: boolean;
  error: Error | null;
}) {
  const sample = signals?.[0];
  return (
    <div className="packet-preview">
      <div className="panel-header">
        <h2>Packet Sample</h2>
        <span className="pill">{sample?.payload_schema ?? "sample"}</span>
      </div>
      {loading && <p className="panel-copy">Loading packet sample...</p>}
      {error && <p className="panel-copy error-text">{error.message}</p>}
      {sample && (
        <>
          <div className="summary-line">
            <strong>{sample.signal_id}</strong>
            <span>{formatTimestamp(sample.timestamp)}</span>
          </div>
          <pre>{JSON.stringify(sample.payload, null, 2)}</pre>
        </>
      )}
    </div>
  );
}

type Stage1NextAction = {
  title: string;
  detail: string;
  label: string;
  disabled: boolean;
  onClick: () => void;
};

type Stage1EvidenceMode = {
  key: "not_started" | "training" | "walk_forward_ready" | "walk_forward" | "walk_forward_postmortem" | "frozen" | "stage2_complete" | "stage3_complete" | "stage4_complete";
  title: string;
  status: "active" | "warn" | "pass" | "locked";
  allowedEvidence: string;
  agentUse: string;
  nextAction: string;
  returnPath: string;
};

const stage1Roles: Stage1SampleRole[] = ["training", "walk_forward_test"];

function buildStage1RoleIterations(iterations: Stage1IterationSummary[]): Record<Stage1SampleRole, Stage1IterationSummary[]> {
  return {
    training: iterations.filter((iteration) => stage1IterationLane(iteration) === "training"),
    walk_forward_test: iterations.filter((iteration) => stage1IterationLane(iteration) === "walk_forward_test"),
  };
}

function buildStage1EvidenceMode(
  gate: Stage1GateSummary | null,
  selectedSession: Stage1ResearchSession | null
): Stage1EvidenceMode {
  if (!selectedSession) {
    return {
      key: "not_started",
      title: "Not Started",
      status: "locked",
      allowedEvidence: "None yet",
      agentUse: "Start Stage 1 first",
      nextAction: "Create a candidate strategy workspace",
      returnPath: "Stage 0 accepted candidate",
    };
  }
  const trainStatus = gate?.roles.training?.status ?? "missing";
  const walkForwardStatus = gate?.roles.walk_forward_test?.status ?? "missing";
  if (gate?.stage4_realized_expectancy.exists) {
    return {
      key: "stage4_complete",
      title: "Stage 4 Complete",
      status: "pass",
      allowedEvidence: "Full realized expectancy artifacts",
      agentUse: "Promotion review only",
      nextAction: "Review promotion readiness",
      returnPath: "Promote manually if accepted",
    };
  }
  if (gate?.stage3_pyramid.exists) {
    return {
      key: "stage3_complete",
      title: "Stage 3 Complete",
      status: "pass",
      allowedEvidence: "Stage 3 grid and pyramid artifacts",
      agentUse: "No judgment edits",
      nextAction: "Stage 4 realized expectancy is next",
      returnPath: "Use Stage 4 candidates with full frozen decisions",
    };
  }
  if (gate?.stage2_capture.exists) {
    return {
      key: "stage2_complete",
      title: "Stage 2 Capture Complete",
      status: "pass",
      allowedEvidence: "Frozen Stage 1 MATCH set",
      agentUse: "No judgment edits",
      nextAction: "Stage 3 execution setup is next",
      returnPath: "Use Stage 2 to narrow TP/SL grid",
    };
  }
  if (gate?.canonical_readout.exists) {
    return {
      key: "frozen",
      title: "Frozen Canonical Readout",
      status: "pass",
      allowedEvidence: "Full canonical Stage 1 decision set",
      agentUse: "No further same-cycle edits",
      nextAction: "Run Stage 2 travel capture",
      returnPath: "Promote or start a new Stage 0 batch",
    };
  }
  if (walkForwardStatus === "fail") {
    return {
      key: "walk_forward_postmortem",
      title: "Walk-Forward Postmortem",
      status: "warn",
      allowedEvidence: "Walk-forward failure summary only",
      agentUse: "Postmortem only",
      nextAction: "Record why promotion failed",
      returnPath: "Start a new cycle; do not tune on walk-forward",
    };
  }
  if (trainStatus === "pass" && walkForwardStatus !== "missing") {
    return {
      key: "walk_forward",
      title: "Walk-Forward Promotion Gate",
      status: walkForwardStatus === "pass" ? "pass" : "active",
      allowedEvidence: "Walk-forward score only",
      agentUse: "Evaluate only",
      nextAction: gate?.ready_to_freeze ? "Run canonical readout" : "Score walk-forward test",
      returnPath: "Freeze on pass; new cycle on fail",
    };
  }
  if (trainStatus === "pass") {
    return {
      key: "walk_forward_ready",
      title: "Walk-Forward Ready",
      status: "active",
      allowedEvidence: "Training evidence only; walk-forward remains evaluator-only",
      agentUse: "Evaluate only",
      nextAction: "Create or score the walk-forward test bundle",
      returnPath: "Freeze on pass; new cycle on fail",
    };
  }
  return {
    key: "training",
    title: "Develop on Training A",
    status: trainStatus === "fail" ? "warn" : "active",
    allowedEvidence: "Training A labels and packets",
    agentUse: "Can edit deterministic strategy",
    nextAction: trainStatus === "fail" ? "Audit failures and iterate" : "Create or score a training bundle",
    returnPath: "Walk-forward test after training passes",
  };
}

function buildStage1NextAction({
  gate,
  roleIterations,
  selectedSession,
  onCreateIteration,
  onScoreTraining,
  onGenerateAudit,
  onRunCanonical,
}: {
  gate: Stage1GateSummary | null;
  roleIterations: Record<Stage1SampleRole, Stage1IterationSummary[]>;
  selectedSession: Stage1ResearchSession | null;
  onCreateIteration: (session: Stage1ResearchSession, sampleRole?: Stage1SampleMethod) => void;
  onScoreTraining: (session: Stage1ResearchSession, iteration: Stage1IterationBundle, sampleRole?: Stage1SampleRole) => void;
  onGenerateAudit: (session: Stage1ResearchSession, iteration: Stage1IterationBundle) => void;
  onRunCanonical: (session: Stage1ResearchSession) => void;
}): Stage1NextAction {
  if (!selectedSession) {
    return {
      title: "Start a Stage 1 session",
      detail: "Pick an accepted Stage 0 pair and create the strategy workspace before generating bundles.",
      label: "Start Stage 1 Draft",
      disabled: true,
      onClick: () => undefined,
    };
  }
  if (gate?.canonical_readout.exists) {
    return {
      title: "Stage 1A frozen",
      detail: "The canonical readout exists. Stage 2/3 can use the MATCH set, and Stage 4 can use the full decision set.",
      label: "Frozen",
      disabled: true,
      onClick: () => undefined,
    };
  }
  if (gate?.ready_to_freeze) {
    return {
      title: "Freeze the Stage 1A strategy",
      detail: "Training and walk-forward have passed. Run the canonical readout before Stage 2.",
      label: "Run Canonical Readout",
      disabled: false,
      onClick: () => onRunCanonical(selectedSession),
    };
  }
  const trainStatus = gate?.roles.training?.status ?? "missing";
  const walkForwardStatus = gate?.roles.walk_forward_test?.status ?? "missing";
  if (walkForwardStatus === "fail") {
    const latestWalkForward = roleIterations.walk_forward_test[roleIterations.walk_forward_test.length - 1] ?? null;
    return {
      title: "Walk-forward failed",
      detail: "This cycle failed the promotion gate. Generate the postmortem if needed, then start a new Stage 0 cycle instead of reopening training.",
      label: latestWalkForward && !latestWalkForward.has_failure_audit ? "Generate Walk-Forward Postmortem" : "Start New Cycle",
      disabled: !latestWalkForward || Boolean(latestWalkForward?.has_failure_audit),
      onClick: () => {
        if (latestWalkForward && !latestWalkForward.has_failure_audit) {
          onGenerateAudit(selectedSession, latestWalkForward);
        }
      },
    };
  }
  for (const role of stage1Roles) {
    const latest = roleIterations[role][roleIterations[role].length - 1] ?? null;
    const score = latest ? stage1ScoreForRole(latest, role) : null;
    const roleStatus = gate?.roles[role]?.status;
    if (!latest) {
      return {
        title: `${stage1RoleLabel(role)} bundle needed`,
        detail: stage1RolePurpose(role),
        label: `Create ${stage1BundleKind(role)}`,
        disabled: false,
        onClick: () => onCreateIteration(selectedSession, role),
      };
    }
    if (!score) {
      return {
        title: `${stage1RoleLabel(role)} score needed`,
        detail: "A bundle exists for this slice, but the deterministic strategy has not been scored against Stage 0 truth yet.",
        label: `Score ${stage1RoleLabel(role)}`,
        disabled: false,
        onClick: () => onScoreTraining(selectedSession, latest, role),
      };
    }
    if (roleStatus === "fail" && role === "training") {
      return {
        title: "Training failed",
        detail: "Generate a failure audit, update the strategy script from that training-only evidence, then create another training bundle.",
        label: latest.has_failure_audit ? "Create Training Bundle" : "Generate Failure Audit",
        disabled: false,
        onClick: () => latest.has_failure_audit
          ? onCreateIteration(selectedSession, "training")
          : onGenerateAudit(selectedSession, latest),
      };
    }
    if (roleStatus === "fail") {
      return {
        title: `${stage1RoleLabel(role)} failed`,
        detail: "Generate a walk-forward postmortem. Walk-forward is a promotion gate, not an optimization set.",
        label: latest.has_failure_audit ? "Start New Cycle" : "Generate Walk-Forward Postmortem",
        disabled: latest.has_failure_audit,
        onClick: () => latest.has_failure_audit
          ? undefined
          : onGenerateAudit(selectedSession, latest),
      };
    }
  }
  return {
    title: "Stage 1 evidence is loading",
    detail: "The gate state is still being resolved from saved iterations.",
    label: "Refresh Evidence",
    disabled: true,
    onClick: () => undefined,
  };
}

function stage1RoleForIteration(iteration: Pick<Stage1IterationBundle, "sample_method">): Stage1SampleRole {
  if (iteration.sample_method === "walk_forward_test") {
    return "walk_forward_test";
  }
  return "training";
}

function stage1IterationLane(iteration: Pick<Stage1IterationBundle, "sample_method">): Stage1SampleRole | null {
  if (iteration.sample_method === "walk_forward_test") {
    return "walk_forward_test";
  }
  return "training";
}

function stage1ScoreRoleForIteration(iteration: Pick<Stage1IterationBundle, "sample_method">): Stage1SampleRole {
  if (iteration.sample_method === "walk_forward_test") {
    return "walk_forward_test";
  }
  return "training";
}

function stage1ScoreForRole(iteration: Stage1IterationSummary, role: Stage1SampleRole): Stage1TrainingScore | null {
  if (role === "training") {
    return iteration.scores?.training ?? iteration.training_score ?? null;
  }
  return iteration.scores?.[role] ?? null;
}

function stage1RoleLabel(role: Stage1SampleRole): string {
  if (role === "walk_forward_test") {
    return "Walk-Forward Test";
  }
  return "Training";
}

function stage1IterationPhaseLabel(iteration: Pick<Stage1IterationBundle, "sample_method">): string {
  return stage1RoleLabel(stage1ScoreRoleForIteration(iteration));
}

function stage1RolePurpose(role: Stage1SampleRole): string {
  if (role === "walk_forward_test") {
    return "Evaluator-only window that checks the trained script on later data; failure ends this cycle.";
  }
  return "Builder slice where the agent may inspect training labels, update deterministic rules, score, and audit failures.";
}

function stage1AgentUseLabel(role: Stage1SampleRole): string {
  if (role === "walk_forward_test") {
    return "Postmortem only";
  }
  return "Can edit";
}

function stage1BundleKind(role: Stage1SampleRole): string {
  return role === "training" ? "Builder Bundle" : "Evaluator Bundle";
}

function stage1BundleRoleForMethod(method: Stage1SampleMethod): "strategy_builder" | "evaluator" {
  return method === "training" ? "strategy_builder" : "evaluator";
}

function stage1BundleLabel(iteration: Stage1IterationSummary): string {
  return iteration.bundle_role === "strategy_builder" ? "builder bundle" : "evaluator bundle";
}

function stage1ScoreLine(score: Stage1TrainingScore): string {
  return `${stage1Agreement(score.metrics.directional_agreement)} · ${score.metrics.matches} match / ${score.metrics.mismatches} mismatch / ${score.metrics.neutral} neutral`;
}

function stage1Agreement(value: number | undefined): string {
  return `${((value ?? 0) * 100).toFixed(2)}%`;
}

function formatCaptureRate(value: Stage2CaptureRate | undefined): string {
  if (!value) {
    return "-";
  }
  return `${value.rate.toFixed(1)}% (${formatNumber(value.reached)}/${formatNumber(value.total)})`;
}

function formatStage3Pct(value: number | undefined): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return `${value.toFixed(1)}%`;
}

function developmentStateLabel(row: DevelopmentQueueRow): string {
  if (row.development_status === "stage4_complete") {
    return "Promotion review";
  }
  if (row.development_status === "stage3_complete") {
    return "Stage 4 ready";
  }
  if (row.development_status === "stage3_grid_complete") {
    return "Pyramid ready";
  }
  if (row.development_status === "stage2_complete") {
    return "Grid ready";
  }
  if (row.development_status === "stage1_frozen") {
    return "Stage 2 ready";
  }
  if (row.development_status === "stage1_in_progress") {
    return "Stage 1 in progress";
  }
  if (row.development_status === "stage1_not_started") {
    return "Not started";
  }
  if (row.development_status === "stage1_ready_to_freeze") {
    return "Ready to freeze";
  }
  return row.development_status.replaceAll("_", " ");
}

function developmentStageLabel(row: DevelopmentQueueRow): string {
  if (row.development_status === "stage4_complete") {
    return "Promotion review";
  }
  if (row.development_status === "stage3_complete") {
    return "Stage 4 ready";
  }
  if (row.development_status === "stage3_grid_complete") {
    return "Pyramid ready";
  }
  if (row.development_status === "stage2_complete") {
    return "Grid ready";
  }
  if (row.development_status === "stage1_frozen") {
    return "Stage 2 ready";
  }
  if (row.stage1_session_id) {
    return "Stage 1";
  }
  return "Not Started";
}

function developmentStatusClass(row: DevelopmentQueueRow): string {
  if (row.development_status === "stage4_complete" || row.development_status === "stage3_complete" || row.development_status === "stage3_grid_complete" || row.development_status === "stage2_complete" || row.development_status === "stage1_frozen") {
    return "status-badge pass";
  }
  if (row.development_status === "stage1_in_progress" || row.development_status === "stage1_ready_to_freeze") {
    return "status-badge info";
  }
  return "status-badge muted";
}

function gateSummaryValue(gate: Stage1GateSummary | null, role: Stage1SampleRole): string {
  const score = gate?.roles[role]?.score;
  if (!score) {
    return "-";
  }
  return stage1Agreement(score.metrics.directional_agreement);
}

function buildStage0Progress(run: Stage0UniverseRun | null, rows: DevelopmentQueueRow[]): {
  total: number;
  accepted: number;
  watchlist: number;
  pending: number;
  failed: number;
  scored: number;
  percent: number;
} {
  const total = rows.length || run?.summary.total_candidates || 0;
  const accepted = rows.length
    ? rows.filter((row) => row.stage0_status === "accepted").length
    : run?.summary.accepted ?? 0;
  const watchlist = rows.length
    ? rows.filter((row) => row.stage0_status === "watchlist").length
    : run?.summary.watchlist ?? 0;
  const pending = rows.length
    ? rows.filter((row) => row.stage0_status === "pending_stage0").length
    : run?.summary.pending_stage0 ?? 0;
  const failed = run?.summary.failed ?? rows.filter((row) => row.stage0_status === "failed").length;
  const scored = Math.max(0, total - pending);
  const percent = total > 0 ? Math.round((scored / total) * 100) : 0;
  return { total, accepted, watchlist, pending, failed, scored, percent };
}

function stage0EvaluatedSignalCount(candidate: Stage0UniverseCandidate): number | null {
  const metrics = candidate.metrics ?? {};
  const totalRecords = metrics.total_records;
  if (typeof totalRecords === "number") {
    return totalRecords;
  }
  const statusCounts = metrics.status_counts && typeof metrics.status_counts === "object"
    ? metrics.status_counts as Record<string, unknown>
    : {};
  const triggered = statusCounts.triggered;
  const noTrigger = statusCounts.no_trigger;
  if (typeof triggered === "number" && typeof noTrigger === "number") {
    return triggered + noTrigger;
  }
  return candidate.packet_count;
}

function buildDashboardCycles(
  runs: Stage0UniverseRun[],
  sessions: Stage1ResearchSession[],
): Array<{
  id: string;
  label: string;
  stage: string;
  train: string;
  walkForward: string;
  status: string;
}> {
  const stage1ByRun = new Map<string, number>();
  for (const session of sessions) {
    if (!session.source_universe_run_id) {
      continue;
    }
    stage1ByRun.set(
      session.source_universe_run_id,
      (stage1ByRun.get(session.source_universe_run_id) ?? 0) + 1,
    );
  }
  return runs.map((run) => {
    const stage1Count = stage1ByRun.get(run.universe_run_id) ?? 0;
    return {
      id: run.universe_run_id,
      label: shortBatchId(run.universe_run_id),
      stage: stage1Count > 0 ? `Stage 1 (${formatNumber(stage1Count)})` : "Stage 0",
      train: `${formatDateOnly(run.train_start ?? null)} - ${formatDateOnly(run.train_end ?? null)}`,
      walkForward: `${formatDateOnly(run.walk_forward_start ?? null)} - ${formatDateOnly(run.walk_forward_end ?? null)}`,
      status: run.status,
    };
  });
}

function formatStage0SplitWindows(run: Stage0UniverseRun | null): string {
  const windows = stage1DefaultWindows(run);
  return `Train ${windows.trainStart} - ${windows.trainEnd} · Walk-forward ${windows.walkForwardStart} - ${windows.walkForwardEnd}`;
}

function stage1DefaultWindows(run: Stage0UniverseRun | null): {
  trainStart: string;
  trainEnd: string;
  walkForwardStart: string;
  walkForwardEnd: string;
} {
  if (!run) {
    return {
      trainStart: "2026-03-01",
      trainEnd: "2026-04-30",
      walkForwardStart: "2026-05-01",
      walkForwardEnd: "2026-05-31",
    };
  }
  if (
    run.train_start
    && run.train_end
    && run.walk_forward_start
    && run.walk_forward_end
  ) {
    return {
      trainStart: formatDateOnly(run.train_start),
      trainEnd: formatDateOnly(run.train_end),
      walkForwardStart: formatDateOnly(run.walk_forward_start),
      walkForwardEnd: formatDateOnly(run.walk_forward_end),
    };
  }
  const windowStart = formatDateOnly(run.window_start);
  const windowEnd = formatDateOnly(run.window_end);
  return {
    trainStart: windowStart,
    trainEnd: "2026-04-30",
    walkForwardStart: "2026-05-01",
    walkForwardEnd: windowEnd,
  };
}

function normalizeResearchStage(value: string | undefined | null): ResearchStageId {
  if (value === "stage1" || value?.startsWith("stage1")) {
    return "stage1";
  }
  if (value === "stage2" || value?.startsWith("stage2")) {
    return "stage2";
  }
  if (value === "stage3" || value?.startsWith("stage3")) {
    return "stage3";
  }
  if (value === "stage4" || value?.startsWith("stage4")) {
    return "stage4";
  }
  return "stage0";
}

function StageGateStack({
  selectedCandidate,
  acceptedCount,
  selectedStage1Session
}: {
  selectedCandidate: Stage0UniverseCandidate | null;
  acceptedCount: number;
  selectedStage1Session: Stage1ResearchSession | null;
}) {
  const gates = [
    { label: "Stage 1", state: selectedStage1Session ? "draft session created" : acceptedCount > 0 ? "ready for draft" : "blocked by Stage 0" },
    { label: "Stage 2", state: selectedStage1Session ? "blocked by Stage 1 pass" : "blocked by Stage 1 draft" },
    { label: "Stage 3", state: "blocked by Stage 2 capture evidence" },
    { label: "Stage 4", state: "blocked by Stage 3 setup" }
  ];
  return (
    <div className="gate-stack">
      <h2>Next Gates</h2>
      {gates.map((gate) => (
        <div className="gate-row" key={gate.label}>
          <span>{gate.label}</span>
          <small>{gate.state}</small>
        </div>
      ))}
      {selectedCandidate?.acceptance_status === "accepted" && (
        <small className="refresh-note">Selected pair is eligible for the future Stage 1 connection.</small>
      )}
    </div>
  );
}

function Stage0CandidateDetail({
  candidate,
  execution,
  batchErrors,
  executing,
  onExecute
}: {
  candidate: Stage0UniverseCandidate | null;
  execution?: Stage0ExecutionResponse;
  batchErrors: Array<{ candidate_id: string; asset: string; detail: string }>;
  executing: boolean;
  onExecute: (candidate: Stage0UniverseCandidate) => void;
}) {
  if (!candidate) {
    return (
      <div className="candidate-detail">
        <p className="panel-copy">Select a candidate to inspect Stage 0 evidence.</p>
      </div>
    );
  }
  const metrics = candidate.metrics ?? {};
  const artifactRoot = String(metrics.artifact_root ?? (execution?.candidate.candidate_id === candidate.candidate_id ? execution.artifact_root : ""));
  const batchError = batchErrors.find((error) => error.candidate_id === candidate.candidate_id);
  const persistedError = candidate.last_error?.detail ? String(candidate.last_error.detail) : "";
  const travelDistribution = metrics.travel_distribution && typeof metrics.travel_distribution === "object"
    ? metrics.travel_distribution as Record<string, unknown>
    : {};
  const evaluatedSignalCount = stage0EvaluatedSignalCount(candidate);
  const statusCounts = metrics.status_counts && typeof metrics.status_counts === "object"
    ? metrics.status_counts as Record<string, unknown>
    : {};
  return (
    <div className="candidate-detail">
      <div className="panel-header">
        <h2>{candidate.asset} Evidence</h2>
        <span className={candidate.acceptance_status === "accepted" ? "pill" : candidate.acceptance_status === "watchlist" ? "pill amber" : "pill red"}>
          {candidate.acceptance_status}
        </span>
      </div>
      <div className="evidence-grid">
        <div>
          <span>Trigger Rate</span>
          <strong>{candidate.trigger_rate_pct === null ? "pending" : `${candidate.trigger_rate_pct}%`}</strong>
        </div>
        <div>
          <span>Threshold</span>
          <strong>{formatMetric(metrics.significance_threshold_pct, "%")}</strong>
        </div>
        <div>
          <span>Reversal</span>
          <strong>{formatMetric(metrics.reversal_rate_pct, "%")}</strong>
        </div>
        <div>
          <span>Evaluated</span>
          <strong>{formatNumber(evaluatedSignalCount)}</strong>
        </div>
        <div>
          <span>Source Packets</span>
          <strong>{formatNumber(candidate.packet_count)}</strong>
        </div>
      </div>
      <div className="evidence-list">
        <span>Branch: {candidate.branch_path}</span>
        <span>Decision: {String(metrics.branch_decision ?? "pending")}</span>
        <span>Triggered / No Trigger: {formatMetric(statusCounts.triggered)} / {formatMetric(statusCounts.no_trigger)}</span>
        <span>Direction: {formatDirectionCounts(metrics.direction_counts)}</span>
        <span>Travel: P25 {formatMetric(travelDistribution.p25, "%")} / P50 {formatMetric(travelDistribution.p50, "%")} / P75 {formatMetric(travelDistribution.p75, "%")}</span>
        <span>Stable threshold range: {formatUnknownList(metrics.stable_threshold_range)}</span>
        <span>Duplicate: {candidate.duplicate_status}</span>
      </div>
      {artifactRoot && <small className="refresh-note">Artifact root: {artifactRoot}</small>}
      {batchError && <small className="refresh-note blocked">Last batch error: {batchError.detail}</small>}
      {persistedError && <small className="refresh-note blocked">Persisted error: {persistedError}</small>}
      <button
        type="button"
        disabled={candidate.acceptance_status !== "pending_stage0" || executing}
        onClick={() => onExecute(candidate)}
      >
        <Play size={16} />Execute Candidate
      </button>
    </div>
  );
}

function DataCatalog({
  catalog,
  loading,
  error,
  refreshMutation
}: {
  catalog?: CatalogResponse;
  loading: boolean;
  error: Error | null;
  refreshMutation: ReturnType<typeof useMutation<RefreshPlan, Error, string>>;
}) {
  return (
    <article className="panel large" id="data">
      <div className="panel-header">
        <h2>Data Catalog</h2>
        <span className="pill">multi-type</span>
      </div>
      {loading && <p className="panel-copy">Loading local market data coverage...</p>}
      {error && <p className="panel-copy error-text">{error.message}</p>}
      {catalog && (
        <div className="asset-list">
          {catalog.assets.map((asset) => (
            <section className="asset-section" key={asset.asset}>
              <div className="asset-title">
                <strong>{asset.asset}</strong>
                <span>{asset.datasets.length} datasets</span>
              </div>
              <div className="dataset-grid">
                {asset.datasets.map((dataset) => (
                  <DatasetRow
                    dataset={dataset}
                    key={dataset.dataset_id}
                    refreshMutation={refreshMutation}
                  />
                ))}
              </div>
            </section>
          ))}
        </div>
      )}
    </article>
  );
}

function DatasetRow({
  dataset,
  refreshMutation
}: {
  dataset: Dataset;
  refreshMutation: ReturnType<typeof useMutation<RefreshPlan, Error, string>>;
}) {
  const canRefresh = dataset.data_type === "candles" && dataset.data_origin === "raw";
  const lastPlan = refreshMutation.data?.dataset_id === dataset.dataset_id ? refreshMutation.data : undefined;
  const lastError = refreshMutation.variables === dataset.dataset_id ? refreshMutation.error : undefined;

  return (
    <div className="dataset-row">
      <div>
        <strong>{dataset.data_type}</strong>
        <span>{dataset.timeframe ?? "event"} · {dataset.data_origin}</span>
      </div>
      <span>{formatTimestamp(dataset.start_ts)} - {formatTimestamp(dataset.end_ts)}</span>
      <span>{formatNumber(dataset.row_count)} rows</span>
      <span className="status">{dataset.quality_status}</span>
      <button
        type="button"
        disabled={!canRefresh || refreshMutation.isPending}
        onClick={() => refreshMutation.mutate(dataset.dataset_id)}
        title={canRefresh ? "Fill raw candle data to current time" : "Refresh is currently supported for raw candle datasets"}
      >
        <UploadCloud size={16} />Fill
      </button>
      {lastPlan && (
        <small className={lastPlan.status === "filled" || lastPlan.status === "current" ? "refresh-note" : "refresh-note blocked"}>
          {lastPlan.status === "filled"
            ? `Added ${formatNumber(lastPlan.rows_added ?? 0)} rows, rebuilt ${formatNumber(lastPlan.derived_rebuilt?.length ?? 0)} derived`
            : lastPlan.status === "current"
            ? `Current through ${formatTimestamp(lastPlan.end_ts ?? null)}`
            : lastPlan.status === "no_new_rows"
            ? `No new rows from ${formatTimestamp(lastPlan.from_ts ?? null)} to ${formatTimestamp(lastPlan.to_ts ?? null)}`
            : lastPlan.reason}
        </small>
      )}
      {lastError && <small className="refresh-note blocked">{lastError.message}</small>}
    </div>
  );
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) {
    return "n/a";
  }
  return value.replace("T", " ").replace("Z", " UTC");
}

function formatSignalUpdateResult(result: SignalPoolExtendResult | null): string {
  if (!result) {
    return "";
  }
  const rawCoverage = formatTimestamp(result.raw_candle_end_ts);
  const scannedCoverage = formatTimestamp(
    result.scan_coverage_end_ts ?? result.coverage_end_ts ?? result.target_end_ts
  );
  const packetCoverage = formatTimestamp(result.final_signal_end_ts ?? result.final_end_ts ?? null);
  const appended = formatNumber(result.appended_packet_count);
  if (result.status === "no_new_signals") {
    return `${result.asset} Parquet update complete: scanned coverage advanced through ${scannedCoverage}, but the engine emitted no new packets. Last packet remains ${packetCoverage} with ${formatNumber(result.final_packet_count ?? null)} packets.`;
  }
  if (result.status === "noop") {
    return `${result.asset} already scanned through ${scannedCoverage}. Raw candles cover through ${rawCoverage}; last packet remains ${packetCoverage}.`;
  }
  return `${result.asset} Parquet update complete: appended ${appended} packets. Scanned coverage is through ${scannedCoverage}; last packet is ${packetCoverage}.`;
}

function formatDateOnly(value: string | null): string {
  if (!value) {
    return "n/a";
  }
  return value.slice(0, 10);
}

function shortBatchId(value: string): string {
  return value
    .replace("stage0-universe-", "")
    .replace("vegas-2026-mar-may-v2", "Vegas Mar-May v2")
    .replace("vegas-2026-mar-may", "Vegas Mar-May");
}

function formatNumber(value: number | null | undefined): string {
  return typeof value === "number" ? value.toLocaleString() : "n/a";
}

function formatMetric(value: unknown, suffix = ""): string {
  if (typeof value !== "number") {
    return "n/a";
  }
  return `${value}${suffix}`;
}

function formatDirectionCounts(value: unknown): string {
  if (!value || typeof value !== "object") {
    return "n/a";
  }
  const counts = value as Record<string, unknown>;
  return Object.entries(counts)
    .map(([direction, count]) => `${direction} ${String(count)}`)
    .join(" / ");
}

function formatUnknownList(value: unknown): string {
  if (!Array.isArray(value) || value.length === 0) {
    return "n/a";
  }
  return value.map((item) => String(item)).join(" - ");
}

ReactDOM.createRoot(document.getElementById("root") as HTMLElement).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
