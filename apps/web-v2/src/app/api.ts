export const API_BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";

export type Dataset = {
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
  schema_descriptor?: Record<string, unknown>;
  quality_status: string;
  ingestion_version: string;
};

export type CatalogAsset = {
  asset: string;
  datasets: Dataset[];
};

export type CatalogResponse = {
  summary: {
    assets: number;
    datasets: number;
    data_types: string[];
  };
  assets: CatalogAsset[];
};

export type RefreshPlan = {
  dataset_id: string;
  status: string;
  asset?: string;
  family?: string;
  instrument?: string;
  data_type?: string;
  timeframe?: string | null;
  from_ts?: string;
  to_ts?: string;
  start_ts?: string;
  end_ts?: string;
  rows_added?: number;
  row_count?: number;
  derived_rebuilt?: Array<{ dataset_id: string; timeframe: string; row_count: number }>;
  enriched?: Array<{ dataset_id: string; timeframe: string; row_count: number; ema_columns?: string[] }>;
  features?: Array<{ dataset_id: string; timeframe: string; row_count: number; columns?: string[] }>;
  feature_count?: number;
  enriched_count?: number;
  skipped_count?: number;
  reason?: string;
};

export type RuntimeJob = {
  job_id: string;
  job_type: string;
  scope_key: string;
  status: "queued" | "running" | "completed" | "failed" | "cancelled" | string;
  payload: Record<string, unknown>;
  result: Record<string, unknown>;
  error: Record<string, unknown>;
  current_step?: string | null;
  created_at?: string;
  started_at?: string | null;
  finished_at?: string | null;
};

export type WorkerRuntimeStatus = {
  status: "online" | "stale" | "offline" | string;
  online: boolean;
  active_worker_count: number;
  stale_worker_count: number;
  queued_job_count: number;
  running_job_count: number;
  stale_after_seconds: number;
  checked_at: string;
  workers: Array<{
    worker_id: string;
    status: string;
    current_job_id?: string | null;
    current_step?: string | null;
    started_at: string;
    last_seen_at: string;
  }>;
};

export type AsyncJobResponse = {
  accepted: true;
  job: RuntimeJob;
  dispatch?: Record<string, unknown>;
};

export function isJobResponse(value: unknown): value is AsyncJobResponse {
  return Boolean(value && typeof value === "object" && (value as AsyncJobResponse).accepted === true && (value as AsyncJobResponse).job);
}

export type CandlePreviewResponse = {
  dataset_id: string;
  rows: Array<Record<string, unknown>>;
};

export type SignalEngine = {
  signal_engine_id: string;
  name: string;
  description: string;
  version: string | null;
  created_at?: string | null;
  code_ref: Record<string, unknown> | null;
  required_data?: Array<Record<string, unknown>>;
  output_envelope_version?: string | null;
  configuration_schema?: Record<string, unknown>;
  runtime_entrypoint: string | null;
  live_scanner_entrypoint: string | null;
  signal_set_count: number;
  packet_count: number;
};

export type SignalSet = {
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

export type SignalPoolExtendResult = {
  status: string;
  signal_engine_id: string;
  asset: string;
  signal_set_key: string;
  target_end_ts?: string | null;
  raw_candle_end_ts?: string | null;
  previous_signal_end_ts?: string | null;
  scan_coverage_end_ts?: string | null;
  final_signal_end_ts?: string | null;
  coverage_end_ts?: string | null;
  previous_end_ts?: string | null;
  final_end_ts?: string | null;
  generated_packet_count?: number;
  appended_packet_count: number;
  final_packet_count?: number | null;
  local_only?: boolean;
};

export type SignalRecord = {
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

export type Stage0UniverseRun = {
  universe_run_id: string;
  name?: string | null;
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

export type Stage0UniverseCandidate = {
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

export type Stage0UniverseResponse = {
  run: Stage0UniverseRun;
  candidates: Stage0UniverseCandidate[];
};

export type Stage0UniverseAppendableAssetsResponse = {
  assets: string[];
};

export type Stage0UniverseAppendAssetsResponse = {
  run: Stage0UniverseRun;
  candidates: Stage0UniverseCandidate[];
  added_candidates: Stage0UniverseCandidate[];
  added_candidate_count: number;
};

export type Stage0ExecutionResponse = {
  candidate: Stage0UniverseCandidate;
  commands: Record<string, string[]>;
  artifact_root: string;
};

export type Stage0BatchExecutionResponse = {
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

export type Stage0UniverseDeleteResponse = {
  status: string;
  universe_run_id: string;
  deleted_stage1_session_count: number;
  deleted_stage1_session_ids: string[];
};

export type DevelopmentQueueRow = {
  candidate_id: string;
  universe_run_id: string;
  asset: string;
  signal_engine_id: string;
  signal_set_id: string;
  signal_set_key: string;
  strategy_id?: string | null;
  stage0_status: string;
  trigger_rate_pct: number | null;
  branch_path: string;
  stage1_session_id?: string | null;
  stage1_status?: string | null;
  stage1_gate?: Stage1GateSummary | null;
  current_stage: string;
  development_status: string;
  next_action: {
    label: string;
    action_type: string;
    disabled?: boolean;
  };
  packet_count?: number;
  stage0_evaluated_signal_count?: number | null;
};

export type Stage1SampleRole = "training" | "walk_forward_test";
export type Stage1SampleMethod = Stage1SampleRole;
export type ResearchStageId = "stage1" | "stage2" | "stage3" | "stage4";
export type Stage1SeedStrategyPreference = "auto" | "engine_base" | "latest_pair";

export type Stage1ResearchSession = {
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

export type Stage1IterationBundle = {
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

export type Stage1TrainingScore = {
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

export type Stage1FailureAudit = {
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

export type Stage1IterationSummary = Stage1IterationBundle & {
  signal_count?: number;
  status?: string;
  scores?: Record<string, Stage1TrainingScore>;
  has_training_score: boolean;
  training_score?: Stage1TrainingScore | null;
  has_failure_audit: boolean;
  failure_audit?: Stage1FailureAudit | null;
};

export type Stage1AgentPrompt = {
  session_id: string;
  iteration_id: string;
  prompt_type: string;
  prompt_path: string;
  prompt: string;
};

export type Stage1IterationDetailRecord = {
  signal_id: string;
  timestamp?: string | null;
  packet_path?: string | null;
  ground_truth_direction?: string | null;
  decision_direction?: string | null;
  agreement: "MATCH" | "MISMATCH" | "NEUTRAL";
  status: string;
  confidence?: number | null;
  reason_code?: string | null;
};

export type Stage1IterationDetailMonth = {
  month: string;
  metrics: Stage1TrainingScore["metrics"];
};

export type Stage1IterationDetail = {
  iteration_id: string;
  sample_role: Stage1SampleRole;
  bundle_role?: string | null;
  signal_count: number;
  metrics: Stage1TrainingScore["metrics"];
  records: Stage1IterationDetailRecord[];
  monthly: Stage1IterationDetailMonth[];
  score_path?: string | null;
  signal_sample_path?: string | null;
};

export type Stage2CaptureRate = {
  reached?: number;
  hit?: number;
  total: number;
  rate: number;
};

export type Stage2CaptureState = {
  exists: boolean;
  capture_curve_path?: string | null;
  per_signal_path?: string | null;
  stage3_trade_inputs_path?: string | null;
  summary_path?: string | null;
  metrics: {
    total_match_signals?: number;
    total_trade_decisions?: number;
    match_count?: number;
    mismatch_count?: number;
    stage2_profiled_match_count?: number;
    slice_counts?: Record<string, number>;
  };
  results: Record<string, Record<string, Stage2CaptureRate>>;
  cohorts?: Record<string, Record<string, Stage2CaptureRate>>;
  sl_results?: Record<string, Record<string, Stage2CaptureRate>>;
  side_splits?: Record<"LONG" | "SHORT", {
    count: number;
    results: Record<string, Record<string, Stage2CaptureRate>>;
    sl_results: Record<string, Record<string, Stage2CaptureRate>>;
  }>;
  stage3_input?: {
    tp_range_source?: string;
    recommended_tp_min_pct?: number;
    recommended_tp_max_pct?: number;
    sl_range_source?: string;
    recommended_sl_min_pct?: number;
    recommended_sl_max_pct?: number;
  };
  tp_levels?: number[];
  sl_levels?: number[];
  total_trade_decisions?: number;
  match_count?: number;
  mismatch_count?: number;
  recommended_tp_min_pct?: number | null;
  recommended_tp_max_pct?: number | null;
  recommended_sl_min_pct?: number | null;
  recommended_sl_max_pct?: number | null;
};

export type Stage2PolicyValues = {
  lock_profit_pct?: number;
  initial_sl_pct?: number;
  protect_trigger_pct?: number;
  trail_sl_pct?: number;
};

export type Stage2ExitPolicyState = {
  exists: boolean;
  policy_path?: string | null;
  created_at?: string | null;
  policy_mode?: "shared" | "side_specific" | string | null;
  policy: Stage2PolicyValues;
  side_policies?: Record<"LONG" | "SHORT", Stage2PolicyValues>;
};

export type Stage3GridSetup = {
  config_id?: string;
  policy_mode?: "shared" | "side_specific" | string | null;
  tp: number;
  sl: number;
  final_tp_pct?: number;
  lock_profit_pct?: number;
  protect_trigger_pct?: number;
  trail_sl_pct?: number;
  initial_sl_pct?: number;
  initial_sl_multiplier?: number;
  protection_enabled?: boolean;
  stage3_step?: string;
  stage3_mode?: string;
  entry_model?: string;
  tp_count: number;
  initial_sl_count?: number;
  protected_sl_count?: number;
  time_exit_count?: number;
  sl_count: number;
  neither: number;
  total: number;
  wr: number;
  expectancy: number;
  profit_factor: number;
  pnl_pct: number;
  gross_pnl_pct?: number;
  net_pnl_pct?: number;
  fees_pct?: number;
  rr_ratio: number;
  side_policies?: Record<"LONG" | "SHORT", Stage2PolicyValues & {
    final_tp_pct?: number;
    initial_sl_multiplier?: number;
    protection_enabled?: boolean;
    hard_exit_hours?: number;
  }>;
  agreement_split?: Record<string, { tp_count: number; sl_count: number; neither: number; total: number }>;
  mismatch_split?: Record<string, { tp_count: number; sl_count: number; neither: number; total: number }>;
};

export type Stage3GridState = {
  exists: boolean;
  fixed_sl_complete?: boolean;
  exact_protection_complete?: boolean;
  local_variants_complete?: boolean;
  grid_results_path?: string | null;
  optimal_path?: string | null;
  stage4_candidates_path?: string | null;
  summary_path?: string | null;
  total_signals: number;
  total_executable_decisions?: number;
  forward_hours?: number | null;
  leverage?: number | null;
  tp_range_source?: string | null;
  tp_values?: number[];
  sl_values?: number[];
  fees_bps_per_side?: number | null;
  policy_mode?: "shared" | "side_specific" | string | null;
  stage0_risk_policy?: {
    initial_sl_pct?: number;
    stage0_meaningful_move_threshold_pct?: number;
    hard_exit_hours?: number;
  };
  stage2_exit_policy?: Stage2ExitPolicyState | Record<string, unknown>;
  fixed_sl_baseline_result?: Partial<Stage3GridSetup>;
  exact_protection_result?: Partial<Stage3GridSetup>;
  exact_policy_result?: Partial<Stage3GridSetup>;
  stage3c_total_combinations_tested?: number;
  stage3c_value_ranges?: {
    final_tp_pct?: number[];
    protect_trigger_pct?: number[];
    trail_sl_pct?: number[];
    initial_sl_pct?: number[];
    initial_sl_multipliers?: number[];
  };
  stage3c_shortlist?: Stage3GridSetup[];
  best: Partial<Stage3GridSetup>;
  top_5: Stage3GridSetup[];
};

export type Stage3PyramidRecord = {
  step_pct: number | null;
  max_legs?: number;
  tp_pct?: number;
  sl_pct?: number;
  source_candidate_id?: string;
  source_setup?: {
    policy_mode?: "shared" | "side_specific" | string | null;
    protection_enabled?: boolean;
    protect_trigger_pct?: number;
    trail_sl_pct?: number;
    final_tp_pct?: number;
    initial_sl_pct?: number;
    tp_pct?: number;
    sl_pct?: number;
    side_policies?: Record<"LONG" | "SHORT", Stage2PolicyValues & {
      final_tp_pct?: number;
      protection_enabled?: boolean;
      hard_exit_hours?: number;
    }>;
  };
  baseline_pnl_pct?: number;
  pnl_pct: number;
  delta_vs_baseline_pct?: number;
  avg_legs_per_signal: number;
  wins: number;
  losses: number;
  comparison?: string;
};

export type Stage3PyramidState = {
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

export type Stage4CandidateResult = {
  candidate_id: string;
  net_expectancy_pct?: number;
  gross_expectancy_pct?: number;
  total_decisions?: number;
  executed_trades?: number;
  skipped_decisions?: number;
  tp_hits?: number;
  sl_hits?: number;
  initial_sl_hits?: number;
  protected_sl_hits?: number;
  no_hit?: number;
  hard_exits?: number;
  mixed_exit?: number;
  unfilled?: number;
  profit_factor?: number;
  win_rate_pct?: number;
  net_pnl_pct?: number;
  skipped_position_open?: number;
  margin_allocation_pct?: number;
  leverage?: number;
  setup?: {
    policy_mode?: "shared" | "side_specific" | string | null;
    protection_enabled?: boolean;
    tp_pct?: number;
    sl_pct?: number;
    final_tp_pct?: number;
    lock_profit_pct?: number;
    initial_sl_pct?: number;
    protect_trigger_pct?: number;
    trail_sl_pct?: number;
    max_hold_hours?: number;
    hard_exit_hours?: number;
    side_policies?: Record<"LONG" | "SHORT", Stage2PolicyValues & {
      final_tp_pct?: number;
      lock_profit_pct?: number;
      initial_sl_pct?: number;
      protection_enabled?: boolean;
      hard_exit_hours?: number;
      max_hold_hours?: number;
    }>;
    pyramid?: {
      step_pct?: number;
      max_legs?: number;
      sl_breakeven?: boolean;
    };
  };
  account?: {
    initial_capital_usdt?: number;
    ending_equity_usdt?: number;
    gross_pnl_usdt?: number;
    net_pnl_usdt?: number;
    total_fees_usdt?: number;
    total_entry_fees_usdt?: number;
    total_exit_fees_usdt?: number;
    total_slippage_usdt?: number;
    return_pct?: number;
    gross_return_pct?: number;
  };
};

export type Stage4TradeLedgerRow = {
  candidate_id: string;
  signal_id: string;
  signal_ts?: string;
  entry_ts?: string;
  exit_ts?: string;
  open_duration_hours?: number;
  slice_name?: string;
  agreement?: string;
  decision_direction?: "LONG" | "SHORT" | "FLAT" | string;
  reference_price?: number;
  position_id?: string;
  entry_status?: string;
  exit_status?: string;
  entry_price?: number;
  exit_price?: number;
  filled_legs?: number;
  leverage?: number;
  position_margin_usdt?: number;
  position_notional_usdt?: number;
  protection_enabled?: boolean;
  protection_activated?: boolean;
  active_sl_kind?: string;
  initial_sl_pct?: number;
  protect_trigger_pct?: number;
  trail_sl_pct?: number;
  gross_pnl_usdt?: number;
  net_pnl_usdt?: number;
  total_fees_usdt?: number;
  total_entry_fees_usdt?: number;
  total_exit_fees_usdt?: number;
  total_slippage_usdt?: number;
  equity_before?: number;
  equity_after?: number;
  gross_pnl_pct?: number;
  net_pnl_pct?: number;
  roe_pct?: number;
  cost_pct?: number;
  leg_details?: Array<{
    leg?: number;
    entry_ts?: string;
    exit_ts?: string;
    entry_price?: number;
    exit_price?: number;
    tp_price?: number;
    exit_status?: string;
    margin_usdt?: number;
    entry_notional_usdt?: number;
    exit_notional_usdt?: number;
    quantity?: number;
    entry_fee_usdt?: number;
    exit_fee_usdt?: number;
    gross_pnl_usdt?: number;
    net_pnl_usdt?: number;
    move_pct?: number;
  }>;
};

export type Stage4CandidateDetail = {
  session_id: string;
  run_id?: string | null;
  created_at?: string | null;
  trade_count: number;
  candidate: Stage4CandidateResult;
  trades: Stage4TradeLedgerRow[];
};

export type PortfolioBacktestResult = {
  schema_version: string;
  artifact_role: "portfolio_backtest" | string;
  created_at: string;
  run_id: string;
  universe_run_id: string;
  simulation_inputs: {
    initial_capital_usdt: number;
    margin_allocations_pct: Record<string, number>;
    margin_basis?: string;
    pyramid_margin_mode?: string;
  };
  eligible_assets: Array<{
    asset: string;
    session_id: string;
    stage4_candidate_id: string;
    margin_allocation_pct: number;
  }>;
  summary: {
    eligible_asset_count: number;
    total_signals: number;
    executed_positions: number;
    skipped_signals: number;
    skipped_insufficient_margin: number;
    skipped_asset_open: number;
    blocked_pyramid_legs: number;
  };
  account: {
    initial_capital_usdt: number;
    ending_equity_usdt: number;
    gross_pnl_usdt: number;
    net_pnl_usdt: number;
    total_fees_usdt: number;
    return_pct: number;
    gross_return_pct: number;
  };
  equity_curve: Array<{
    timestamp: string | null;
    equity_usdt: number;
    used_margin_usdt: number;
    free_margin_usdt: number;
  }>;
  trade_ledger: Stage4TradeLedgerRow[];
  skipped_signals: Array<{
    asset: string;
    source_session_id?: string;
    candidate_id?: string;
    signal_id?: string;
    signal_ts?: string;
    skip_reason: string;
    requested_margin_usdt?: number | null;
    used_margin_usdt: number;
    free_margin_usdt: number;
    equity_usdt: number;
  }>;
  portfolio_backtest_path?: string;
  trade_ledger_path?: string;
  skipped_signals_path?: string;
};

export type PortfolioBacktestRunIndex = {
  schema_version: string;
  artifact_role: "portfolio_backtest_run_index" | string;
  universe_run_id: string;
  latest_run_id?: string | null;
  runs: Array<{
    run_id: string;
    created_at: string;
    summary: PortfolioBacktestResult["summary"];
    account: PortfolioBacktestResult["account"];
    portfolio_backtest_path?: string;
  }>;
};

export type PortfolioBacktestDeleteResult = {
  schema_version: string;
  artifact_role: "portfolio_backtest_delete_result" | string;
  universe_run_id: string;
  deleted_run_id: string;
  latest_run_id?: string | null;
  remaining_run_count: number;
  runs: PortfolioBacktestRunIndex["runs"];
};

export type Stage4RealizedExpectancyState = {
  exists: boolean;
  realized_expectancy_path?: string | null;
  trade_ledger_path?: string | null;
  optimal_path?: string | null;
  summary_path?: string | null;
  latest_run_id?: string | null;
  latest_simulation_inputs?: {
    initial_capital_usdt?: number;
    margin_allocation_pct?: number;
    leverage?: number;
  };
  latest_account?: Stage4CandidateResult["account"];
  stage4_runs?: Array<{
    run_id: string;
    created_at: string;
    simulation_inputs: {
      initial_capital_usdt?: number;
      margin_allocation_pct?: number;
      leverage?: number;
    };
    best_candidate_id?: string;
    account?: Stage4CandidateResult["account"];
  }>;
  best_candidate_id?: string | null;
  best_candidate: Partial<Stage4CandidateResult>;
  candidates: Stage4CandidateResult[];
};

export type Stage1GateSummary = {
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
  stage2_exit_policy: Stage2ExitPolicyState;
  stage3_grid: Stage3GridState;
  stage3_pyramid: Stage3PyramidState;
  stage4_realized_expectancy: Stage4RealizedExpectancyState;
  downstream_contract: {
    stage2_stage3: string;
    stage4: string;
  };
};

export type ExecutionBundle = {
  bundle_id: string;
  asset: string;
  instrument: string;
  signal_engine_id: string;
  signal_engine_version: string;
  strategy_id: string;
  strategy_version: string;
  source_stage1_session_id: string;
  source_stage4_result_path: string;
  bundle_uri: string;
  strategy_module_ref: string;
  execution_setup: Record<string, unknown>;
  risk_limits: Record<string, unknown>;
  evidence_refs: Record<string, unknown>;
  content_hash: string;
  status: string;
  created_at?: string;
};

export type DeploymentRoute = {
  route_id: string;
  active_bundle_id?: string | null;
  asset: string;
  instrument: string;
  signal_engine_id: string;
  signal_engine_version?: string;
  strategy_id: string;
  strategy_version?: string;
  bundle_id: string;
  active_bundle?: ExecutionBundle | null;
  status: string;
  execution_adapter: string;
  exchange_account?: string;
  account_mode: string;
  cron_interval_minutes?: number;
  margin_allocation_pct?: number;
  leverage?: number;
  manual_sizing_enabled?: boolean;
  scheduler_status?: string;
  auto_submit_enabled?: boolean;
  last_wake_at?: string | null;
  last_wake_id?: string | null;
  next_wake_at?: string | null;
  last_lifecycle_error?: Record<string, unknown>;
  risk_limits?: Record<string, unknown>;
  promoted?: boolean;
  data_warmed?: boolean;
  manually_armed?: boolean;
  enabled?: boolean;
  archived?: boolean;
  archived_at?: string | null;
  blockers?: string[];
  created_at?: string;
};

export type ExchangeHealth = {
  route_id: string;
  adapter: string;
  account_mode: string;
  exchange_account: string;
  instrument: string;
  cli_path: string | null;
  checked_at: string;
  status: "connected" | "disconnected" | "blocked" | string;
  connected: boolean;
  readiness_blockers: string[];
  snapshot: {
    position_count?: number;
    open_order_count?: number;
    protection_order_count?: number;
    recent_fill_count?: number;
    has_balance?: boolean;
  };
  error: string | null;
};

export type OrderIntent = {
  intent_id?: string;
  status?: string;
  action?: string;
  side?: string;
  quantity?: string;
  notional_usd?: number;
  reduce_only?: boolean;
  client_order_id?: string;
  signal_id?: string;
};

export type WakeRun = {
  wake_id: string;
  route_id: string;
  bundle_id?: string | null;
  status: string;
  branch: string;
  blockers: string[];
  signal_scan_result: Record<string, unknown>;
  exchange_snapshot?: Record<string, unknown>;
  strategy_decision?: Record<string, unknown>;
  order_intents: OrderIntent[];
  adapter_results: unknown[];
  error: Record<string, unknown>;
  started_at?: string;
  completed_at?: string | null;
};

export type WarmupRequirement = {
  data_type?: string;
  origin?: string;
  timeframe?: string;
  status: string;
  reason?: string;
  dataset_id?: string;
  fill_result?: {
    status?: string;
    rows_added?: number;
    derived_rebuilt?: unknown[];
    end_ts?: string;
  };
};

export type DataWarmupReport = {
  status: "warmed" | "blocked" | "skipped";
  route_id: string;
  asset?: string;
  signal_engine_id?: string;
  reason?: string;
  requirements?: WarmupRequirement[];
};

export type SubmitWakeOrdersResult = {
  status: "submitted" | "blocked";
  submitted_count: number;
  blockers: string[];
  wake: WakeRun;
  route: DeploymentRoute;
  adapter_results: unknown[];
};

export type RouteLifecycleResult = {
  cycle?: {
    warmup?: DataWarmupReport;
    signal_update?: Record<string, unknown>;
    wake?: WakeRun | null;
    submission?: Record<string, unknown>;
  };
  route: DeploymentRoute;
};

export type HealthSnapshot = {
  api: "ready" | "unknown";
  database: "configured" | "unknown";
  okxCli: "connected" | "unchecked";
  utc: string;
};

export function getStaticHealthSnapshot(): HealthSnapshot {
  return {
    api: "unknown",
    database: "unknown",
    okxCli: "unchecked",
    utc: new Date().toISOString()
  };
}

async function requestJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE_URL}${path}`, init);
  if (!response.ok) {
    const payload = await response.json().catch(() => undefined);
    throw new Error(payload?.detail ?? `Request failed: ${response.status}`);
  }
  return response.json();
}

export function fetchMarketDataCatalog(): Promise<CatalogResponse> {
  return requestJson<CatalogResponse>("/api/v1/market-data/catalog");
}

export function refreshMarketDataDataset(datasetId: string): Promise<RefreshPlan | AsyncJobResponse> {
  return requestJson<RefreshPlan | AsyncJobResponse>(`/api/v1/market-data/${datasetId}/refresh`, {
    method: "POST"
  });
}

export function refreshMarketDataEma(asset: string): Promise<RefreshPlan | AsyncJobResponse> {
  return requestJson<RefreshPlan | AsyncJobResponse>(`/api/v1/market-data/assets/${asset}/ema/refresh`, {
    method: "POST"
  });
}

export function refreshMarketDataFeatureFamily(asset: string, family: string): Promise<RefreshPlan | AsyncJobResponse> {
  return requestJson<RefreshPlan | AsyncJobResponse>(`/api/v1/market-data/assets/${asset}/features/${family}/refresh`, {
    method: "POST"
  });
}

export function fetchJob(jobId: string): Promise<{ job: RuntimeJob }> {
  return requestJson<{ job: RuntimeJob }>(`/api/v1/jobs/${jobId}`);
}

export function fetchJobs(scopeKey: string, limit = 10): Promise<{ jobs: RuntimeJob[] }> {
  const params = new URLSearchParams({ scope_key: scopeKey, limit: String(limit) });
  return requestJson<{ jobs: RuntimeJob[] }>(`/api/v1/jobs?${params.toString()}`);
}

export function fetchWorkerRuntimeStatus(): Promise<{ worker_runtime: WorkerRuntimeStatus }> {
  return requestJson<{ worker_runtime: WorkerRuntimeStatus }>("/api/v1/jobs/runtime");
}

export function fetchDatasetCandles(datasetId: string, limit = 25): Promise<CandlePreviewResponse> {
  return requestJson<CandlePreviewResponse>(`/api/v1/market-data/${datasetId}/candles?limit=${limit}`);
}

export function fetchDatasetRows(datasetId: string, limit = 25): Promise<CandlePreviewResponse> {
  return requestJson<CandlePreviewResponse>(`/api/v1/market-data/${datasetId}/rows?limit=${limit}`);
}

export function fetchSignalEngines(): Promise<{ engines: SignalEngine[] }> {
  return requestJson<{ engines: SignalEngine[] }>("/api/v1/signal-engines");
}

export function fetchSignalSets(signalEngineId: string): Promise<{ signal_sets: SignalSet[] }> {
  return requestJson<{ signal_sets: SignalSet[] }>(`/api/v1/signal-engines/${signalEngineId}/signal-sets`);
}

export function updateSignalEngine(signalEngineId: string, request: { name: string }): Promise<{ engine: SignalEngine }> {
  return requestJson<{ engine: SignalEngine }>(`/api/v1/signal-engines/${signalEngineId}`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request)
  });
}

export function createSignalSet(request: { signal_engine_id: string; asset: string }): Promise<{ signal_set: SignalSet }> {
  return requestJson<{ signal_set: SignalSet }>(`/api/v1/signal-engines/${request.signal_engine_id}/signal-sets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ asset: request.asset })
  });
}

export function fetchSignals(signalSetKey: string, limit = 5, descending = false): Promise<{ signals: SignalRecord[] }> {
  const params = new URLSearchParams({ signal_set_key: signalSetKey, limit: String(limit) });
  if (descending) {
    params.set("descending", "true");
  }
  return requestJson<{ signals: SignalRecord[] }>(`/api/v1/signals?${params.toString()}`);
}

export function extendSignalPoolFromLocalCandles(request: { signal_engine_id: string; asset: string }): Promise<SignalPoolExtendResult | AsyncJobResponse> {
  return requestJson<SignalPoolExtendResult | AsyncJobResponse>(`/api/v1/signal-engines/${request.signal_engine_id}/signal-sets/${request.asset}/extend-local`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({})
  });
}

export function fetchStage0UniverseRuns(): Promise<{ runs: Stage0UniverseRun[] }> {
  return requestJson<{ runs: Stage0UniverseRun[] }>("/api/v1/research/stage0-universe-runs");
}

export function fetchStage0UniverseCandidates(universeRunId: string): Promise<{ candidates: Stage0UniverseCandidate[] }> {
  return requestJson<{ candidates: Stage0UniverseCandidate[] }>(`/api/v1/research/stage0-universe-runs/${universeRunId}/candidates`);
}

export function fetchStage0UniverseAppendableAssets(universeRunId: string): Promise<Stage0UniverseAppendableAssetsResponse> {
  return requestJson<Stage0UniverseAppendableAssetsResponse>(
    `/api/v1/research/stage0-universe-runs/${universeRunId}/appendable-assets`
  );
}

export function fetchDevelopmentQueue(universeRunId: string): Promise<{ universe_run: Stage0UniverseRun; queue: DevelopmentQueueRow[] }> {
  return requestJson<{ universe_run: Stage0UniverseRun; queue: DevelopmentQueueRow[] }>(`/api/v1/research/cycles/${universeRunId}/development-queue`);
}

export function runPortfolioBacktest(request: {
  universe_run_id: string;
  initial_capital_usdt: number;
  margin_allocations_pct: Record<string, number>;
}): Promise<{ portfolio_backtest: PortfolioBacktestResult } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage0-universe-runs/${request.universe_run_id}/portfolio-backtest`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      initial_capital_usdt: request.initial_capital_usdt,
      margin_allocations_pct: request.margin_allocations_pct
    })
  });
}

export function fetchPortfolioBacktestRuns(universeRunId: string): Promise<{ portfolio_backtest_runs: PortfolioBacktestRunIndex }> {
  return requestJson<{ portfolio_backtest_runs: PortfolioBacktestRunIndex }>(`/api/v1/research/stage0-universe-runs/${universeRunId}/portfolio-backtest/runs`);
}

export function fetchPortfolioBacktestRun(request: { universe_run_id: string; run_id: string }): Promise<{ portfolio_backtest: PortfolioBacktestResult }> {
  return requestJson<{ portfolio_backtest: PortfolioBacktestResult }>(`/api/v1/research/stage0-universe-runs/${request.universe_run_id}/portfolio-backtest/runs/${request.run_id}`);
}

export function deletePortfolioBacktestRun(request: { universe_run_id: string; run_id: string }): Promise<{ portfolio_backtest_delete: PortfolioBacktestDeleteResult }> {
  return requestJson<{ portfolio_backtest_delete: PortfolioBacktestDeleteResult }>(`/api/v1/research/stage0-universe-runs/${request.universe_run_id}/portfolio-backtest/runs/${request.run_id}`, { method: "DELETE" });
}

export function createStage0UniverseRun(request: {
  name?: string;
  train_start: string;
  train_end: string;
  walk_forward_start: string;
  walk_forward_end: string;
  forward_hours: number;
  trigger_rate_threshold_pct: number;
  engine_ids: string[];
  assets: string[];
}): Promise<Stage0UniverseResponse> {
  const slug = [
    "training-pool",
    request.engine_ids.join("-") || "all-engines",
    request.train_start,
    request.walk_forward_end,
    Date.now().toString(36)
  ]
    .join("-")
    .replace(/[^a-zA-Z0-9_-]+/g, "-")
    .toLowerCase();
  return requestJson<Stage0UniverseResponse>("/api/v1/research/stage0-universe-runs", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      universe_run_id: slug,
      ...request
    })
  });
}

export function executeStage0CandidateBatch(request: { universe_run_id: string; limit: number }): Promise<Stage0BatchExecutionResponse | AsyncJobResponse> {
  return requestJson<Stage0BatchExecutionResponse | AsyncJobResponse>(`/api/v1/research/stage0-universe-runs/${request.universe_run_id}/candidates/execute-batch`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ limit: request.limit })
  });
}

export function deleteStage0UniverseRun(universeRunId: string): Promise<Stage0UniverseDeleteResponse> {
  return requestJson<Stage0UniverseDeleteResponse>(`/api/v1/research/stage0-universe-runs/${universeRunId}`, {
    method: "DELETE"
  });
}

export function appendStage0UniverseAssets(request: {
  universe_run_id: string;
  assets: string[];
}): Promise<Stage0UniverseAppendAssetsResponse> {
  return requestJson<Stage0UniverseAppendAssetsResponse>(
    `/api/v1/research/stage0-universe-runs/${request.universe_run_id}/append-assets`,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ assets: request.assets })
    }
  );
}

export function fetchStage1ResearchSessions(): Promise<{ sessions: Stage1ResearchSession[] }> {
  return requestJson<{ sessions: Stage1ResearchSession[] }>("/api/v1/research/stage1-sessions");
}

export function createStage1ResearchSession(request: {
  source_candidate_id: string;
  strategy_id: string;
  strategy_version: string;
  train_start: string;
  train_end: string;
  walk_forward_start: string;
  walk_forward_end: string;
  seed_strategy_preference?: Stage1SeedStrategyPreference;
}): Promise<{ session: Stage1ResearchSession }> {
  return requestJson<{ session: Stage1ResearchSession }>("/api/v1/research/stage1-sessions", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(request)
  });
}

export function deleteStage1ResearchSession(sessionId: string): Promise<{ status: string; session_id: string; source_candidate_id: string }> {
  return requestJson<{ status: string; session_id: string; source_candidate_id: string }>(
    `/api/v1/research/stage1-sessions/${sessionId}`,
    { method: "DELETE" }
  );
}

export function createStage1Iteration(request: {
  session_id: string;
  sample_method: Stage1SampleMethod;
  bundle_role: "strategy_builder" | "evaluator";
}): Promise<{ iteration: Stage1IterationBundle }> {
  return requestJson<{ iteration: Stage1IterationBundle }>(`/api/v1/research/stage1-sessions/${request.session_id}/iterations`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      sample_method: request.sample_method,
      bundle_role: request.bundle_role
    })
  });
}

export function fetchStage1Iterations(sessionId: string): Promise<{ iterations: Stage1IterationSummary[] }> {
  return requestJson<{ iterations: Stage1IterationSummary[] }>(`/api/v1/research/stage1-sessions/${sessionId}/iterations`);
}

export function fetchStage1Gate(sessionId: string): Promise<{ gate: Stage1GateSummary }> {
  return requestJson<{ gate: Stage1GateSummary }>(`/api/v1/research/stage1-sessions/${sessionId}/gate`);
}

export function fetchStage1AgentPrompt(request: {
  session_id: string;
  iteration_id: string;
}): Promise<Stage1AgentPrompt> {
  return requestJson<Stage1AgentPrompt>(`/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/agent-prompt`);
}

export function fetchStage1IterationDetail(request: {
  session_id: string;
  iteration_id: string;
}): Promise<{ detail: Stage1IterationDetail }> {
  return requestJson<{ detail: Stage1IterationDetail }>(
    `/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/details`
  );
}

export function deleteStage1Iteration(request: {
  session_id: string;
  iteration_id: string;
}): Promise<{ status: string; session_id: string; iteration_id: string }> {
  return requestJson<{ status: string; session_id: string; iteration_id: string }>(
    `/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}`,
    { method: "DELETE" }
  );
}

export function scoreStage1Iteration(request: {
  session_id: string;
  iteration_id: string;
  sample_role?: Stage1SampleRole;
}): Promise<{ score: Stage1TrainingScore } | AsyncJobResponse> {
  const endpoint = request.sample_role === "walk_forward_test" ? "score-walk-forward" : "score-training";
  return requestJson<{ score: Stage1TrainingScore } | AsyncJobResponse>(
    `/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/${endpoint}`,
    { method: "POST" }
  );
}

export function generateStage1FailureAudit(request: {
  session_id: string;
  iteration_id: string;
  sample_role?: Stage1SampleRole;
}): Promise<{ audit: Stage1FailureAudit }> {
  const params = request.sample_role ? `?sample_role=${encodeURIComponent(request.sample_role)}` : "";
  return requestJson<{ audit: Stage1FailureAudit }>(
    `/api/v1/research/stage1-sessions/${request.session_id}/iterations/${request.iteration_id}/generate-failure-audit${params}`,
    { method: "POST" }
  );
}

export function runStage1CanonicalReadout(request: {
  session_id: string;
  force?: boolean;
}): Promise<{ forced?: boolean; canonical_readout: Stage1TrainingScore & {
  frozen_strategy_path: string;
  slice_metrics: Record<string, Stage1TrainingScore["metrics"]>;
  match_count: number;
}; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${request.session_id}/canonical-stage1a`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ force: Boolean(request.force) })
  });
}

export function runStage2CaptureCurve(sessionId: string): Promise<{ stage2_capture: Stage2CaptureState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage2/capture-curve`, { method: "POST" });
}

export function promoteStage2ExitPolicy(request: {
  session_id: string;
  side_policies: Record<"LONG" | "SHORT", {
    lock_profit_pct: number;
    initial_sl_pct: number;
    protect_trigger_pct: number;
    trail_sl_pct: number;
  }>;
}): Promise<{ stage2_exit_policy: Stage2ExitPolicyState; gate: Stage1GateSummary }> {
  return requestJson(`/api/v1/research/stage1-sessions/${request.session_id}/stage2/exit-policy`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      side_policies: request.side_policies
    })
  });
}

export function runStage3GridSearch(sessionId: string): Promise<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage3/grid-search`, { method: "POST" });
}

export function runStage3FixedSl(sessionId: string): Promise<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage3/fixed-sl`, { method: "POST" });
}

export function runStage3ExactProtection(sessionId: string): Promise<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage3/exact-protection`, { method: "POST" });
}

export function runStage3LocalVariants(sessionId: string): Promise<{ stage3_grid: Stage3GridState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage3/local-variants`, { method: "POST" });
}

export function runStage3Pyramid(sessionId: string): Promise<{ stage3_pyramid: Stage3PyramidState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${sessionId}/stage3/pyramid`, { method: "POST" });
}

export type Stage4RealizedExpectancyRequest = {
  session_id: string;
  initial_capital_usdt: number;
  margin_allocation_pct: number;
  leverage: number;
};

export function runStage4RealizedExpectancy(request: Stage4RealizedExpectancyRequest): Promise<{ stage4_realized_expectancy: Stage4RealizedExpectancyState; gate: Stage1GateSummary } | AsyncJobResponse> {
  return requestJson(`/api/v1/research/stage1-sessions/${request.session_id}/stage4/realized-expectancy`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      initial_capital_usdt: request.initial_capital_usdt,
      margin_allocation_pct: request.margin_allocation_pct,
      leverage: request.leverage
    })
  });
}

export function fetchStage4CandidateDetail(request: { session_id: string; candidate_id: string }): Promise<{ detail: Stage4CandidateDetail }> {
  return requestJson(`/api/v1/research/stage1-sessions/${request.session_id}/stage4/candidates/${encodeURIComponent(request.candidate_id)}/details`);
}

export function promoteExecutionBundle(sessionId: string): Promise<{ bundle: ExecutionBundle; route: DeploymentRoute }> {
  return requestJson<{ bundle: ExecutionBundle; route: DeploymentRoute }>(`/api/v1/research/stage1-sessions/${sessionId}/promote-execution-bundle`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ account_mode: "live", execution_adapter: "okx" })
  });
}

export function fetchTradingRoutes(): Promise<{ routes: DeploymentRoute[] }> {
  return requestJson<{ routes: DeploymentRoute[] }>("/api/v1/trading/routes");
}

export function fetchArchivedTradingRoutes(): Promise<{ routes: DeploymentRoute[] }> {
  return requestJson<{ routes: DeploymentRoute[] }>("/api/v1/trading/routes/archived");
}

export function archiveTradingRoute(routeId: string): Promise<{ route: DeploymentRoute }> {
  return requestJson<{ route: DeploymentRoute }>(`/api/v1/trading/routes/${routeId}/archive`, {
    method: "POST"
  });
}

export type ArchivedStrategyDeleteResponse = {
  status: "deleted";
  route_id: string;
  bundle_id: string;
  deleted_wake_count: number;
  deleted_owner_state_count: number;
  artifact_deleted: boolean;
};

export function deleteArchivedTradingRoute(routeId: string): Promise<ArchivedStrategyDeleteResponse> {
  return requestJson<ArchivedStrategyDeleteResponse>(`/api/v1/trading/routes/${routeId}/archived-strategy`, {
    method: "DELETE"
  });
}

export type WakeRunsPage = {
  wakes: WakeRun[];
  total: number;
  limit: number;
  offset: number;
};

export function fetchRouteWakes(routeId: string, options: { limit?: number; offset?: number } = {}): Promise<WakeRunsPage> {
  const params = new URLSearchParams();
  if (typeof options.limit === "number") {
    params.set("limit", String(options.limit));
  }
  if (typeof options.offset === "number") {
    params.set("offset", String(options.offset));
  }
  const query = params.toString();
  return requestJson<WakeRunsPage>(`/api/v1/trading/routes/${routeId}/wakes${query ? `?${query}` : ""}`);
}

export function fetchRouteExchangeHealth(routeId: string): Promise<ExchangeHealth> {
  return requestJson<ExchangeHealth>(`/api/v1/trading/routes/${routeId}/exchange-health`);
}

export function updateRouteSettings(request: {
  route_id: string;
  cron_interval_minutes: number;
  execution_adapter: string;
  exchange_account: string;
  margin_allocation_pct: number;
  leverage: number;
  manual_sizing_enabled: boolean;
  auto_submit_enabled: boolean;
}): Promise<{ route: DeploymentRoute }> {
  return requestJson<{ route: DeploymentRoute }>(`/api/v1/trading/routes/${request.route_id}/settings`, {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      cron_interval_minutes: request.cron_interval_minutes,
      execution_adapter: request.execution_adapter,
      exchange_account: request.exchange_account,
      margin_allocation_pct: request.margin_allocation_pct,
      leverage: request.leverage,
      manual_sizing_enabled: request.manual_sizing_enabled,
      auto_submit_enabled: request.auto_submit_enabled
    })
  });
}

export function startRouteLifecycle(request: {
  route_id: string;
  confirm_live: boolean;
  auto_submit_enabled: boolean;
}): Promise<RouteLifecycleResult> {
  return requestJson<RouteLifecycleResult>(`/api/v1/trading/routes/${request.route_id}/start`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      confirm_live: request.confirm_live,
      auto_submit_enabled: request.auto_submit_enabled
    })
  });
}

export function stopRouteLifecycle(routeId: string): Promise<{ route: DeploymentRoute }> {
  return requestJson<{ route: DeploymentRoute }>(`/api/v1/trading/routes/${routeId}/stop`, {
    method: "POST"
  });
}

export function runRouteWake(routeId: string): Promise<{ warmup: DataWarmupReport; wake: WakeRun; route: DeploymentRoute }> {
  return requestJson<{ warmup: DataWarmupReport; wake: WakeRun; route: DeploymentRoute }>(`/api/v1/trading/routes/${routeId}/wake`, {
    method: "POST"
  });
}

export function submitWakeOrders(request: {
  route_id: string;
  wake_id: string;
  confirm_live: boolean;
  quantity?: string;
  notional_usd?: number;
}): Promise<SubmitWakeOrdersResult> {
  return requestJson<SubmitWakeOrdersResult>(`/api/v1/trading/routes/${request.route_id}/wakes/${request.wake_id}/submit-orders`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      confirm_live: request.confirm_live,
      quantity: request.quantity,
      notional_usd: request.notional_usd
    })
  });
}
