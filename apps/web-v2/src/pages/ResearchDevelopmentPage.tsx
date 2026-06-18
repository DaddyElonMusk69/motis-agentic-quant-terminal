import { useEffect, useMemo, useState, type Dispatch, type SetStateAction } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { BarChart3, Clipboard, Play, RefreshCw, RotateCcw, Trash2, UploadCloud, X } from "lucide-react";
import {
  createStage1Iteration,
  createStage1ResearchSession,
  deletePortfolioBacktestRun,
  deleteStage1Iteration,
  deleteStage1ResearchSession,
  fetchPortfolioBacktestRun,
  fetchPortfolioBacktestRuns,
  fetchDevelopmentQueue,
  fetchJob,
  fetchJobs,
  fetchStage4CandidateDetail,
  fetchStage0UniverseRuns,
  fetchStage1AgentPrompt,
  fetchStage1Gate,
  fetchStage1IterationDetail,
  fetchStage1Iterations,
  fetchStage1ResearchSessions,
  generateStage1FailureAudit,
  isJobResponse,
  promoteExecutionBundle,
  promoteStage2ExitPolicy,
  runPortfolioBacktest,
  runStage1CanonicalReadout,
  runStage2CaptureCurve,
  runStage3ExactProtection,
  runStage3FixedSl,
  runStage3LocalVariants,
  runStage3Pyramid,
  runStage4RealizedExpectancy,
  scoreStage1Iteration,
  type DevelopmentQueueRow,
  type PortfolioBacktestResult,
  type PortfolioBacktestRunIndex,
  type ResearchStageId,
  type Stage0UniverseRun,
  type Stage1AgentPrompt,
  type Stage1GateSummary,
  type Stage1IterationDetail,
  type Stage1IterationSummary,
  type Stage1ResearchSession,
  type Stage1SampleMethod,
  type Stage1SampleRole,
  type Stage1SeedStrategyPreference,
  type Stage1TrainingScore,
  type Stage2CaptureRate,
  type Stage2PolicyValues,
  type Stage4CandidateDetail,
  type Stage4CandidateResult,
  type Stage4TradeLedgerRow,
  type Stage3GridSetup,
  type RuntimeJob
} from "../app/api";
import { formatNumber } from "../app/format";
import { queryClient } from "../app/queryClient";
import { useAppRouter } from "../app/router";
import { buildStage1Consistency, type Stage1ConsistencyMonth, type Stage1ConsistencySide } from "../app/stage1Consistency";
import { DataTable } from "../components/DataTable";
import { FieldRow } from "../components/FieldRow";
import { SplitPane } from "../components/SplitPane";
import { StatusBadge } from "../components/StatusBadge";
import { TerminalPanel } from "../components/TerminalPanel";
import { WorkerRuntimeNotice } from "../components/WorkerRuntimeNotice";

type Stage1EvidenceMode = {
  title: string;
  tone: "pass" | "warn" | "info" | "idle";
  allowedEvidence: string;
  agentUse: string;
  nextAction: string;
};

type Stage1OverrideAction =
  | { kind: "create_walk_forward_bundle"; title: string; body: string; confirmLabel: string }
  | { kind: "run_canonical_stage1a"; title: string; body: string; confirmLabel: string };

type Stage1StartChoice = {
  strategyId: string;
  latestAvailable: boolean;
  latestLabel: string;
};

type ActiveDevelopmentJob = {
  action: string;
  jobId: string;
  label: string;
  sessionId?: string;
};

type PortfolioBacktestModalState = {
  initialCapital: number;
  allocations: Record<string, number>;
  result: PortfolioBacktestResult | null;
};

type ExitSide = "LONG" | "SHORT";

type DisplaySidePolicy = Stage2PolicyValues & {
  final_tp_pct?: number;
  lock_profit_pct?: number;
  initial_sl_pct?: number;
  protection_enabled?: boolean;
  hard_exit_hours?: number;
  max_hold_hours?: number;
};

type DisplayExecutionSetup = {
  policy_mode?: "shared" | "side_specific" | string | null;
  protection_enabled?: boolean;
  tp?: number;
  sl?: number;
  tp_pct?: number;
  sl_pct?: number;
  final_tp_pct?: number;
  lock_profit_pct?: number;
  initial_sl_pct?: number;
  protect_trigger_pct?: number;
  trail_sl_pct?: number;
  pyramid?: {
    step_pct?: number;
    max_legs?: number;
    sl_breakeven?: boolean;
  };
  side_policies?: Record<ExitSide, DisplaySidePolicy>;
};

function restoreDevelopmentJob(job: RuntimeJob): ActiveDevelopmentJob | null {
  const payload = job.payload ?? {};
  const sessionId = typeof payload.session_id === "string" ? payload.session_id : undefined;
  if (job.job_type === "stage1_score") {
    return { action: "score", jobId: job.job_id, label: "Scoring iteration", sessionId };
  }
  if (job.job_type === "stage1_canonical") {
    return { action: "canonical", jobId: job.job_id, label: "Freezing Stage 1", sessionId };
  }
  if (job.job_type === "stage2_capture_curve") {
    return { action: "stage2", jobId: job.job_id, label: "Running Stage 2 capture", sessionId };
  }
  if (job.job_type === "stage3_policy_step") {
    const step = typeof payload.step === "string" ? payload.step : "";
    if (step === "fixed_sl") {
      return { action: "stage3_fixed", jobId: job.job_id, label: "Testing fixed SL", sessionId };
    }
    if (step === "exact_protection") {
      return { action: "stage3_exact", jobId: job.job_id, label: "Testing protection", sessionId };
    }
    if (step === "local_variants" || step === "grid_search") {
      return { action: "stage3_local", jobId: job.job_id, label: "Testing local variants", sessionId };
    }
  }
  if (job.job_type === "stage3_pyramid") {
    return { action: "stage3_pyramid", jobId: job.job_id, label: "Testing pyramiding", sessionId };
  }
  if (job.job_type === "stage4_realized_expectancy") {
    return { action: "stage4", jobId: job.job_id, label: "Running Stage 4 backtest", sessionId };
  }
  if (job.job_type === "portfolio_backtest") {
    return { action: "portfolio", jobId: job.job_id, label: "Running portfolio backtest" };
  }
  return null;
}

const stage1Roles: Stage1SampleRole[] = ["training", "walk_forward_test"];

function updateDevelopmentUrl(next: { pool?: string; candidate?: string; stage?: ResearchStageId }) {
  const params = new URLSearchParams(window.location.search);
  if (next.pool !== undefined) {
    params.set("pool", next.pool);
  }
  if (next.candidate !== undefined) {
    params.set("candidate", next.candidate);
  }
  if (next.stage !== undefined) {
    params.set("stage", next.stage);
  }
  const query = params.toString();
  const nextUrl = `/research/development${query ? `?${query}` : ""}`;
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function selectedPool(runs: Stage0UniverseRun[] | undefined, searchParams: URLSearchParams): Stage0UniverseRun | undefined {
  const requested = searchParams.get("pool");
  return runs?.find((run) => run.universe_run_id === requested) ?? runs?.[0];
}

function selectedCandidate(rows: DevelopmentQueueRow[], searchParams: URLSearchParams): DevelopmentQueueRow | undefined {
  const requested = searchParams.get("candidate");
  return rows.find((row) => row.candidate_id === requested) ?? rows[0];
}

function shortPoolId(value: string): string {
  return value.replace("stage0-universe-", "").replace("training-pool-", "");
}

function dateOnly(value: string | null | undefined): string {
  return value ? value.slice(0, 10) : "n/a";
}

function stageWindows(run: Stage0UniverseRun | undefined): {
  trainStart: string;
  trainEnd: string;
  walkForwardStart: string;
  walkForwardEnd: string;
} {
  return {
    trainStart: dateOnly(run?.train_start ?? run?.window_start),
    trainEnd: dateOnly(run?.train_end),
    walkForwardStart: dateOnly(run?.walk_forward_start),
    walkForwardEnd: dateOnly(run?.walk_forward_end ?? run?.window_end)
  };
}

function splitWindowLine(run: Stage0UniverseRun | undefined): string {
  const windows = stageWindows(run);
  return `Train ${windows.trainStart} - ${windows.trainEnd} · Walk-forward ${windows.walkForwardStart} - ${windows.walkForwardEnd}`;
}

function actionType(row: DevelopmentQueueRow | undefined): string {
  const nextAction = row?.next_action as { action_type?: string; type?: string } | undefined;
  return nextAction?.action_type ?? nextAction?.type ?? "";
}

function stageTone(row: DevelopmentQueueRow): "pass" | "warn" | "info" | "idle" {
  if (["stage4_complete", "stage3_complete", "stage3_grid_complete", "stage2_policy_promoted", "stage2_complete", "stage1_frozen"].includes(row.development_status)) {
    return "pass";
  }
  if (["stage1_in_progress", "stage1_ready_to_freeze"].includes(row.development_status)) {
    return "info";
  }
  if (row.stage0_status !== "accepted") {
    return "warn";
  }
  return "idle";
}

function developmentLabel(row: DevelopmentQueueRow | undefined): string {
  if (!row) {
    return "No candidate";
  }
  const labels: Record<string, string> = {
    stage1_not_started: "Ready for Stage 1",
    stage1_in_progress: "Stage 1 in progress",
    stage1_ready_to_freeze: "Ready to freeze",
    stage1_frozen: "Stage 2 ready",
    stage2_complete: "Exit policy ready",
    stage2_policy_promoted: "Stage 3 ready",
    stage3_grid_complete: "Pyramid ready",
    stage3_complete: "Stage 4 ready",
    stage4_complete: "Promotion review"
  };
  return labels[row.development_status] ?? row.development_status.replaceAll("_", " ");
}

function normalizeResearchStage(value: string | null | undefined): ResearchStageId {
  if (value?.startsWith("stage2")) {
    return "stage2";
  }
  if (value?.startsWith("stage3")) {
    return "stage3";
  }
  if (value?.startsWith("stage4")) {
    return "stage4";
  }
  return "stage1";
}

function developmentVisualStage(row: DevelopmentQueueRow | undefined): ResearchStageId {
  if (!row) {
    return "stage1";
  }
  if (row.development_status === "stage4_complete") {
    return "stage4";
  }
  if (["stage3_grid_complete", "stage3_complete"].includes(row.development_status)) {
    return "stage3";
  }
  if (["stage1_frozen", "stage2_complete", "stage2_policy_promoted"].includes(row.development_status)) {
    return "stage2";
  }
  return normalizeResearchStage(row.current_stage);
}

function developmentStageClass(prefix: string, stage: ResearchStageId): string {
  return `${prefix}--${stage}`;
}

function stage1RoleForIteration(iteration: Pick<Stage1IterationSummary, "sample_method">): Stage1SampleRole {
  return iteration.sample_method === "walk_forward_test" ? "walk_forward_test" : "training";
}

function stage1BundleRoleForMethod(method: Stage1SampleMethod): "strategy_builder" | "evaluator" {
  return method === "training" ? "strategy_builder" : "evaluator";
}

function stage1RoleLabel(role: Stage1SampleRole): string {
  return role === "walk_forward_test" ? "Walk-forward" : "Training";
}

function stage1BundleLabel(iteration: Stage1IterationSummary): string {
  return iteration.bundle_role === "strategy_builder" ? "Builder" : "Evaluator";
}

function stage1ScoreForRole(iteration: Stage1IterationSummary, role: Stage1SampleRole): Stage1TrainingScore | null {
  if (role === "training") {
    return iteration.scores?.training ?? iteration.training_score ?? null;
  }
  return iteration.scores?.[role] ?? null;
}

function stage1Agreement(value: number | undefined): string {
  return `${((value ?? 0) * 100).toFixed(2)}%`;
}

function formatUtcTimestamp(value: string | null | undefined): string {
  if (!value) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toLocaleString("en-US", {
    timeZone: "UTC",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).replace(",", "") + " UTC";
}

function formatCompactUtcTimestamp(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "-";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return String(value);
  }
  return date.toLocaleString("en-US", {
    timeZone: "UTC",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).replace(",", "") + " UTC";
}

function roleIterations(iterations: Stage1IterationSummary[]): Record<Stage1SampleRole, Stage1IterationSummary[]> {
  return {
    training: iterations.filter((iteration) => stage1RoleForIteration(iteration) === "training"),
    walk_forward_test: iterations.filter((iteration) => stage1RoleForIteration(iteration) === "walk_forward_test")
  };
}

function gateScore(gate: Stage1GateSummary | null, role: Stage1SampleRole): string {
  const score = gate?.roles[role]?.score;
  return score ? stage1Agreement(score.metrics.directional_agreement) : "n/a";
}

function evidenceMode(gate: Stage1GateSummary | null, session: Stage1ResearchSession | null): Stage1EvidenceMode {
  if (!session) {
    return {
      title: "Not started",
      tone: "idle",
      allowedEvidence: "None yet",
      agentUse: "Start Stage 1",
      nextAction: "Create candidate workspace"
    };
  }
  if (gate?.stage4_realized_expectancy.exists) {
    return {
      title: "Promotion evidence ready",
      tone: "pass",
      allowedEvidence: "Frozen Stage 1, Stage 2-4 artifacts",
      agentUse: "Review only",
      nextAction: "Promote or rerun a new pool"
    };
  }
  if (gate?.canonical_readout.exists) {
    return {
      title: "Stage 1 frozen",
      tone: "pass",
      allowedEvidence: "Canonical decision set",
      agentUse: "No same-cycle edits",
      nextAction: "Run downstream stages"
    };
  }
  const trainingStatus = gate?.roles.training?.status ?? "missing";
  const walkForwardStatus = gate?.roles.walk_forward_test?.status ?? "missing";
  if (walkForwardStatus === "fail") {
    return {
      title: "Walk-forward failed",
      tone: "warn",
      allowedEvidence: "Walk-forward postmortem only",
      agentUse: "No tuning on test data",
      nextAction: "Audit, then start a new pool"
    };
  }
  if (trainingStatus === "pass") {
    return {
      title: "Walk-forward gate",
      tone: "info",
      allowedEvidence: "Training is fixed; test is evaluator-only",
      agentUse: "Score or postmortem",
      nextAction: gate?.ready_to_freeze ? "Freeze Stage 1" : "Create/score walk-forward"
    };
  }
  return {
    title: "Training iteration",
    tone: trainingStatus === "fail" ? "warn" : "info",
    allowedEvidence: "Training labels and packets",
    agentUse: "Can edit strategy script",
    nextAction: trainingStatus === "fail" ? "Audit and iterate" : "Create/score training"
  };
}

function formatCaptureRate(value: Stage2CaptureRate | undefined): string {
  if (!value) {
    return "-";
  }
  const count = value.reached ?? value.hit ?? 0;
  return `${value.rate.toFixed(1)}% (${formatNumber(count)}/${formatNumber(value.total)})`;
}

function formatPct(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return `${value.toFixed(1)}%`;
}

function formatRate(value: number | undefined | null, digits = 1): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function formatSignedPp(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  const points = value * 100;
  return `${points >= 0 ? "+" : ""}${points.toFixed(1)}pp`;
}

function consistencyAgreementTone(value: number | null): "pass" | "warn" | "risk" | "idle" {
  if (value === null) {
    return "idle";
  }
  if (value >= 0.55) {
    return "pass";
  }
  if (value >= 0.5) {
    return "warn";
  }
  return "risk";
}

function consistencyDeviationTone(value: number | null): "pass" | "warn" | "risk" | "idle" {
  if (value === null) {
    return "idle";
  }
  const magnitude = Math.abs(value);
  if (magnitude > 0.25) {
    return "risk";
  }
  if (magnitude >= 0.15) {
    return "warn";
  }
  return "pass";
}

function consistencyImbalanceTone(value: number | null): "pass" | "warn" | "risk" | "idle" {
  if (value === null) {
    return "idle";
  }
  if (value >= 0.5) {
    return "risk";
  }
  if (value >= 0.25) {
    return "warn";
  }
  return "pass";
}

function formatDecimal(value: number | undefined | null, digits = 2): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return value.toFixed(digits);
}

function shortId(value: string | undefined | null): string {
  if (!value) {
    return "-";
  }
  return value.length > 26 ? `${value.slice(0, 23)}...` : value;
}

function formatRangeList(values: number[] | undefined): string {
  if (!values?.length) {
    return "-";
  }
  if (values.length <= 4) {
    return values.map((value) => formatPct(value)).join(", ");
  }
  return `${formatPct(values[0])} - ${formatPct(values[values.length - 1])} (${formatNumber(values.length)})`;
}

function formatStage3Policy(item: DisplayExecutionSetup | undefined): string {
  if (item?.policy_mode === "side_specific" && item.side_policies) {
    const long = item.side_policies.LONG;
    const short = item.side_policies.SHORT;
    return `L ${formatPct(long.final_tp_pct ?? long.lock_profit_pct)} / ${formatPct(long.initial_sl_pct)} | S ${formatPct(short.final_tp_pct ?? short.lock_profit_pct)} / ${formatPct(short.initial_sl_pct)}`;
  }
  return `${formatPct(item?.final_tp_pct ?? item?.tp)} TP / ${formatPct(item?.initial_sl_pct ?? item?.sl)} SL`;
}

function formatStage3Protection(item: DisplayExecutionSetup | undefined): string {
  if (item?.policy_mode === "side_specific" && item.side_policies) {
    const long = item.side_policies.LONG;
    const short = item.side_policies.SHORT;
    if (!long.protection_enabled && !short.protection_enabled) {
      return "Fixed SL";
    }
    return `L ${formatPct(long.protect_trigger_pct)} / ${formatPct(long.trail_sl_pct)} | S ${formatPct(short.protect_trigger_pct)} / ${formatPct(short.trail_sl_pct)}`;
  }
  if (item?.protection_enabled === false) {
    return "Fixed SL";
  }
  return `${formatPct(item?.protect_trigger_pct)} / ${formatPct(item?.trail_sl_pct)}`;
}

function formatPyramidPolicy(item: DisplayExecutionSetup | undefined): string {
  const pyramid = item?.pyramid;
  if (!pyramid) {
    return "off";
  }
  const legs = pyramid.max_legs ? `${formatNumber(pyramid.max_legs)} legs` : "pyramid";
  const step = pyramid.step_pct == null ? "step -" : `step ${formatPct(pyramid.step_pct)}`;
  return pyramid.sl_breakeven ? `${legs} / ${step} / SL to BE` : `${legs} / ${step}`;
}

function formatStage3SidePolicy(
  item: DisplayExecutionSetup | undefined,
  side: ExitSide
): string {
  const sidePolicy = item?.side_policies?.[side];
  if (sidePolicy) {
    const finalTp = sidePolicy.final_tp_pct ?? sidePolicy.lock_profit_pct;
    return `${formatPct(finalTp)} TP / ${formatPct(sidePolicy.initial_sl_pct)} SL`;
  }
  return `${formatPct(item?.final_tp_pct ?? item?.tp)} TP / ${formatPct(item?.initial_sl_pct ?? item?.sl)} SL`;
}

function formatStage3SideProtection(
  item: DisplayExecutionSetup | undefined,
  side: ExitSide
): string {
  const sidePolicy = item?.side_policies?.[side];
  if (sidePolicy) {
    if (!sidePolicy.protection_enabled) {
      return "Fixed SL";
    }
    return `${formatPct(sidePolicy.protect_trigger_pct)} trigger / ${formatPct(sidePolicy.trail_sl_pct)} protected SL`;
  }
  if (item?.protection_enabled === false) {
    return "Fixed SL";
  }
  return `${formatPct(item?.protect_trigger_pct)} trigger / ${formatPct(item?.trail_sl_pct)} protected SL`;
}

function formatStage4ExitMode(setup: DisplayExecutionSetup | undefined): string {
  if (setup?.policy_mode === "side_specific" && setup.side_policies) {
    const sides = [setup.side_policies.LONG, setup.side_policies.SHORT];
    const protectedCount = sides.filter((policy) => Boolean(policy.protection_enabled)).length;
    if (protectedCount === 0) {
      return "Fixed SL";
    }
    if (protectedCount === sides.length) {
      return "Protected SL";
    }
    return "Split Protection";
  }
  return setup?.protection_enabled ? "Protected SL" : "Fixed SL";
}

function formatUsd(value: number | undefined | null): string {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "-";
  }
  return `$${value.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
}

function stage4FilledTrades(trades: Stage4TradeLedgerRow[]): Stage4TradeLedgerRow[] {
  return trades.filter((trade) => trade.entry_status === "FILLED");
}

function stage4WinningTrades(trades: Stage4TradeLedgerRow[]): Stage4TradeLedgerRow[] {
  return trades.filter((trade) => (trade.net_pnl_usdt ?? 0) > 0);
}

function stage4LosingTrades(trades: Stage4TradeLedgerRow[]): Stage4TradeLedgerRow[] {
  return trades.filter((trade) => (trade.net_pnl_usdt ?? 0) < 0);
}

function stage4MaxDrawdownPct(trades: Stage4TradeLedgerRow[]): number | null {
  if (!trades.length) {
    return null;
  }
  let peak = trades[0].equity_before ?? trades[0].equity_after ?? 0;
  let maxDrawdown = 0;
  for (const trade of trades) {
    const equity = trade.equity_after ?? trade.equity_before;
    if (typeof equity !== "number" || Number.isNaN(equity)) {
      continue;
    }
    if (equity > peak) {
      peak = equity;
      continue;
    }
    if (peak > 0) {
      maxDrawdown = Math.max(maxDrawdown, ((peak - equity) / peak) * 100);
    }
  }
  return maxDrawdown;
}

function stage4TradeMarginUsed(trade: Stage4TradeLedgerRow): number | null {
  const legs = trade.leg_details ?? [];
  const margin = legs.reduce((sum, leg) => sum + (leg.margin_usdt ?? 0), 0);
  return margin > 0 ? margin : null;
}

function stage4TradeNotional(trade: Stage4TradeLedgerRow): number | null {
  const legs = trade.leg_details ?? [];
  const notional = legs.reduce((sum, leg) => sum + (leg.entry_notional_usdt ?? 0), 0);
  return notional > 0 ? notional : null;
}

function stage4TradeRoePct(trade: Stage4TradeLedgerRow): number | null {
  const margin = stage4TradeMarginUsed(trade);
  if (!margin) {
    return null;
  }
  return ((trade.net_pnl_usdt ?? 0) / margin) * 100;
}

function stage4TradeRowClassName(trade: Stage4TradeLedgerRow): string {
  const pnl = trade.net_pnl_usdt ?? 0;
  if (pnl > 0) {
    return "stage4-trade-row stage4-trade-row--win";
  }
  if (pnl < 0) {
    return "stage4-trade-row stage4-trade-row--loss";
  }
  return "stage4-trade-row stage4-trade-row--flat";
}

function portfolioBacktestFromJob(job: RuntimeJob | null): PortfolioBacktestResult | null {
  const result = job?.result?.portfolio_backtest;
  return result && typeof result === "object" ? result as PortfolioBacktestResult : null;
}

function scoreExists(gate: Stage1GateSummary | null, role: Stage1SampleRole): boolean {
  return Boolean(gate?.roles[role]?.score);
}

function invalidateDevelopment(sessionId?: string, poolId?: string) {
  void queryClient.invalidateQueries({ queryKey: ["stage1-sessions"] });
  if (sessionId) {
    void queryClient.invalidateQueries({ queryKey: ["stage1-iterations", sessionId] });
    void queryClient.invalidateQueries({ queryKey: ["stage1-gate", sessionId] });
    void queryClient.invalidateQueries({ queryKey: ["stage4-candidate-detail", sessionId] });
  }
  if (poolId) {
    void queryClient.invalidateQueries({ queryKey: ["development-queue", poolId] });
  }
}

export function ResearchDevelopmentPage() {
  const { searchParams } = useAppRouter();
  const [prompt, setPrompt] = useState<Stage1AgentPrompt | null>(null);
  const [copiedPrompt, setCopiedPrompt] = useState(false);
  const [overrideAction, setOverrideAction] = useState<Stage1OverrideAction | null>(null);
  const [selectedIteration, setSelectedIteration] = useState<Stage1IterationSummary | null>(null);
  const [selectedStage4Candidate, setSelectedStage4Candidate] = useState<Stage4CandidateResult | null>(null);
  const [startChoice, setStartChoice] = useState<Stage1StartChoice | null>(null);
  const [activeJob, setActiveJob] = useState<ActiveDevelopmentJob | null>(null);
  const [portfolioBacktestModal, setPortfolioBacktestModal] = useState<PortfolioBacktestModalState | null>(null);
  const [stage4Inputs, setStage4Inputs] = useState({
    initial_capital_usdt: 10000,
    margin_allocation_pct: 30,
    leverage: 5
  });

  const poolQuery = useQuery({ queryKey: ["stage0-universe-runs"], queryFn: fetchStage0UniverseRuns });
  const pool = selectedPool(poolQuery.data?.runs, searchParams);
  const queueQuery = useQuery({
    enabled: Boolean(pool?.universe_run_id),
    queryKey: ["development-queue", pool?.universe_run_id],
    queryFn: () => fetchDevelopmentQueue(pool!.universe_run_id)
  });
  const sessionsQuery = useQuery({ queryKey: ["stage1-sessions"], queryFn: fetchStage1ResearchSessions });

  const acceptedRows = useMemo(
    () => (queueQuery.data?.queue ?? []).filter((row) => row.stage0_status === "accepted"),
    [queueQuery.data?.queue]
  );
  const row = selectedCandidate(acceptedRows, searchParams);
  const session = useMemo(() => {
    return sessionsQuery.data?.sessions.find((item) => item.session_id === row?.stage1_session_id)
      ?? sessionsQuery.data?.sessions.find((item) => item.source_candidate_id === row?.candidate_id)
      ?? null;
  }, [row?.candidate_id, row?.stage1_session_id, sessionsQuery.data?.sessions]);
  const activeStage = normalizeResearchStage(searchParams.get("stage") ?? row?.current_stage);

  const iterationsQuery = useQuery({
    enabled: Boolean(session?.session_id),
    queryKey: ["stage1-iterations", session?.session_id],
    queryFn: () => fetchStage1Iterations(session!.session_id)
  });
  const gateQuery = useQuery({
    enabled: Boolean(session?.session_id),
    queryKey: ["stage1-gate", session?.session_id],
    queryFn: () => fetchStage1Gate(session!.session_id)
  });
  const iterationDetailQuery = useQuery({
    enabled: Boolean(session?.session_id && selectedIteration?.iteration_id),
    queryKey: ["stage1-iteration-detail", session?.session_id, selectedIteration?.iteration_id],
    queryFn: () => fetchStage1IterationDetail({ session_id: session!.session_id, iteration_id: selectedIteration!.iteration_id })
  });
  const stage4CandidateDetailQuery = useQuery({
    enabled: Boolean(session?.session_id && selectedStage4Candidate?.candidate_id),
    queryKey: ["stage4-candidate-detail", session?.session_id, selectedStage4Candidate?.candidate_id],
    queryFn: () => fetchStage4CandidateDetail({ session_id: session!.session_id, candidate_id: selectedStage4Candidate!.candidate_id })
  });
  const gate = gateQuery.data?.gate ?? row?.stage1_gate ?? null;
  const completedStage4Assets = useMemo(
    () => acceptedRows.filter((candidate) => Boolean(candidate.stage1_gate?.stage4_realized_expectancy.exists)),
    [acceptedRows]
  );
  const activeJobQuery = useQuery({
    enabled: Boolean(activeJob?.jobId),
    queryKey: ["runtime-job", activeJob?.jobId],
    queryFn: () => fetchJob(activeJob!.jobId),
    refetchInterval: (query) => {
      const job = query.state.data?.job;
      return !job || ["queued", "running"].includes(job.status) ? 1500 : false;
    }
  });
  const activeSessionScopeKey = session?.session_id ? `stage1_session:${session.session_id}` : null;
  const activePoolScopeKey = pool?.universe_run_id ? `stage0:${pool.universe_run_id}` : null;
  const latestSessionScopeJobsQuery = useQuery({
    enabled: Boolean(activeSessionScopeKey) && !activeJob?.jobId,
    queryKey: ["runtime-jobs", activeSessionScopeKey],
    queryFn: () => fetchJobs(activeSessionScopeKey!, 10)
  });
  const latestPoolScopeJobsQuery = useQuery({
    enabled: Boolean(activePoolScopeKey) && !activeJob?.jobId,
    queryKey: ["runtime-jobs", activePoolScopeKey],
    queryFn: () => fetchJobs(activePoolScopeKey!, 10)
  });
  const portfolioRunsQuery = useQuery({
    enabled: Boolean(pool?.universe_run_id && portfolioBacktestModal),
    queryKey: ["portfolio-backtest-runs", pool?.universe_run_id],
    queryFn: () => fetchPortfolioBacktestRuns(pool!.universe_run_id)
  });

  const iterations = iterationsQuery.data?.iterations ?? [];
  const groupedIterations = useMemo(() => roleIterations(iterations), [iterations]);
  const mode = evidenceMode(gate, session);
  const canCreateWalkForward = scoreExists(gate, "training");
  const canForceWalkForward = canCreateWalkForward && gate?.roles.training?.status === "fail";
  const canForceFreeze = scoreExists(gate, "training") && scoreExists(gate, "walk_forward_test") && !gate?.ready_to_freeze;
  const plannedStrategyId = row ? (row.strategy_id ?? `${row.asset.toLowerCase()}-${row.signal_engine_id}-strategy-v01`) : "";
  const latestSeedSession = useMemo(() => {
    if (!row) {
      return null;
    }
    return (sessionsQuery.data?.sessions ?? []).find(
      (item) =>
        item.asset === row.asset &&
        item.signal_engine_id === row.signal_engine_id &&
        item.strategy_id === plannedStrategyId &&
        item.session_id !== session?.session_id
    ) ?? null;
  }, [plannedStrategyId, row, session?.session_id, sessionsQuery.data?.sessions]);
  const trackedJob = activeJobQuery.data?.job ?? null;
  const trackedJobRunning = Boolean(trackedJob && ["queued", "running"].includes(trackedJob.status));
  const sessionJobRunning = trackedJobRunning && activeJob?.action !== "portfolio" && (!activeJob?.sessionId || activeJob.sessionId === session?.session_id);
  const portfolioJobRunning = trackedJobRunning && activeJob?.action === "portfolio";
  const isJobRunning = (action: string) => activeJob?.action === action && (action === "portfolio" ? portfolioJobRunning : sessionJobRunning);
  const trackAsyncJob = (result: unknown, action: string, label: string, sessionId?: string): boolean => {
    if (!isJobResponse(result)) {
      return false;
    }
    setActiveJob({ action, jobId: result.job.job_id, label, sessionId });
    return true;
  };

  const createSessionMutation = useMutation({
    mutationFn: createStage1ResearchSession,
    onSuccess: (result) => {
      invalidateDevelopment(result.session.session_id, pool?.universe_run_id);
    }
  });
  const createIterationMutation = useMutation({
    mutationFn: createStage1Iteration,
    onSuccess: (_result, variables) => invalidateDevelopment(variables.session_id, pool?.universe_run_id)
  });
  const deleteIterationMutation = useMutation({
    mutationFn: deleteStage1Iteration,
    onSuccess: (_result, variables) => invalidateDevelopment(variables.session_id, pool?.universe_run_id)
  });
  const deleteSessionMutation = useMutation({
    mutationFn: deleteStage1ResearchSession,
    onSuccess: (_result, sessionId) => {
      setPrompt(null);
      setSelectedIteration(null);
      invalidateDevelopment(sessionId, pool?.universe_run_id);
    }
  });
  const promptMutation = useMutation({
    mutationFn: fetchStage1AgentPrompt,
    onSuccess: (result) => {
      setCopiedPrompt(false);
      setPrompt(result);
    }
  });
  const scoreMutation = useMutation({
    mutationFn: scoreStage1Iteration,
    onSuccess: (result, variables) => {
      if (!trackAsyncJob(result, "score", "Scoring iteration", variables.session_id)) {
        invalidateDevelopment(variables.session_id, pool?.universe_run_id);
      }
    }
  });
  const auditMutation = useMutation({
    mutationFn: generateStage1FailureAudit,
    onSuccess: (_result, variables) => invalidateDevelopment(variables.session_id, pool?.universe_run_id)
  });
  const canonicalMutation = useMutation({
    mutationFn: runStage1CanonicalReadout,
    onSuccess: (result, variables) => {
      if (!trackAsyncJob(result, "canonical", "Freezing Stage 1", variables.session_id)) {
        invalidateDevelopment(variables.session_id, pool?.universe_run_id);
      }
    }
  });
  const stage2Mutation = useMutation({
    mutationFn: runStage2CaptureCurve,
    onSuccess: (result, sessionId) => {
      if (!trackAsyncJob(result, "stage2", "Running Stage 2 capture", sessionId)) {
        invalidateDevelopment(sessionId, pool?.universe_run_id);
      }
    }
  });
  const stage2ExitPolicyMutation = useMutation({
    mutationFn: promoteStage2ExitPolicy,
    onSuccess: (_result, variables) => invalidateDevelopment(variables.session_id, pool?.universe_run_id)
  });
  const stage3FixedSlMutation = useMutation({
    mutationFn: runStage3FixedSl,
    onSuccess: (result, sessionId) => {
      if (!trackAsyncJob(result, "stage3_fixed", "Testing fixed SL", sessionId)) {
        invalidateDevelopment(sessionId, pool?.universe_run_id);
      }
    }
  });
  const stage3ExactProtectionMutation = useMutation({
    mutationFn: runStage3ExactProtection,
    onSuccess: (result, sessionId) => {
      if (!trackAsyncJob(result, "stage3_exact", "Testing protection", sessionId)) {
        invalidateDevelopment(sessionId, pool?.universe_run_id);
      }
    }
  });
  const stage3LocalVariantsMutation = useMutation({
    mutationFn: runStage3LocalVariants,
    onSuccess: (result, sessionId) => {
      if (!trackAsyncJob(result, "stage3_local", "Testing local variants", sessionId)) {
        invalidateDevelopment(sessionId, pool?.universe_run_id);
      }
    }
  });
  const stage3PyramidMutation = useMutation({
    mutationFn: runStage3Pyramid,
    onSuccess: (result, sessionId) => {
      if (!trackAsyncJob(result, "stage3_pyramid", "Testing pyramiding", sessionId)) {
        invalidateDevelopment(sessionId, pool?.universe_run_id);
      }
    }
  });
  const stage4Mutation = useMutation({
    mutationFn: runStage4RealizedExpectancy,
    onSuccess: (result, variables) => {
      if (!trackAsyncJob(result, "stage4", "Running Stage 4 backtest", variables.session_id)) {
        invalidateDevelopment(variables.session_id, pool?.universe_run_id);
      }
    }
  });
  const portfolioBacktestMutation = useMutation({
    mutationFn: runPortfolioBacktest,
    onSuccess: (result, variables) => {
      if (!trackAsyncJob(result, "portfolio", "Running portfolio backtest")) {
        invalidateDevelopment(undefined, variables.universe_run_id);
        void queryClient.invalidateQueries({ queryKey: ["portfolio-backtest-runs", variables.universe_run_id] });
        if ("portfolio_backtest" in result) {
          setPortfolioBacktestModal((current) => (current ? { ...current, result: result.portfolio_backtest } : current));
        }
      }
    }
  });
  const loadPortfolioRunMutation = useMutation({
    mutationFn: fetchPortfolioBacktestRun,
    onSuccess: (result) => {
      setPortfolioBacktestModal((current) => (current ? { ...current, result: result.portfolio_backtest } : current));
    }
  });
  const deletePortfolioRunMutation = useMutation({
    mutationFn: deletePortfolioBacktestRun,
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ["portfolio-backtest-runs", result.portfolio_backtest_delete.universe_run_id] });
      setPortfolioBacktestModal((current) => (
        current?.result?.run_id === result.portfolio_backtest_delete.deleted_run_id
          ? { ...current, result: null }
          : current
      ));
    }
  });
  const promoteMutation = useMutation({
    mutationFn: promoteExecutionBundle,
    onSuccess: (_result, sessionId) => invalidateDevelopment(sessionId, pool?.universe_run_id)
  });

  useEffect(() => {
    if (activeJob?.jobId) {
      return;
    }
    const jobs = [
      ...(latestSessionScopeJobsQuery.data?.jobs ?? []),
      ...(latestPoolScopeJobsQuery.data?.jobs ?? [])
    ];
    const job = jobs.find((item) => ["queued", "running"].includes(item.status));
    if (!job) {
      return;
    }
    const restored = restoreDevelopmentJob(job);
    if (restored) {
      setActiveJob(restored);
    }
  }, [activeJob?.jobId, latestPoolScopeJobsQuery.data?.jobs, latestSessionScopeJobsQuery.data?.jobs]);

  useEffect(() => {
    if (!searchParams.get("pool") && pool?.universe_run_id) {
      updateDevelopmentUrl({ pool: pool.universe_run_id });
    }
  }, [pool?.universe_run_id, searchParams]);

  useEffect(() => {
    if (!searchParams.get("candidate") && row?.candidate_id) {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: activeStage });
    }
  }, [activeStage, row?.candidate_id, row?.universe_run_id, searchParams]);

  useEffect(() => {
    setSelectedIteration(null);
    setSelectedStage4Candidate(null);
  }, [session?.session_id]);

  useEffect(() => {
    if (!trackedJob || !activeJob || ["queued", "running"].includes(trackedJob.status)) {
      return;
    }
    if (activeJob.action === "portfolio" && trackedJob.status === "completed") {
      const portfolioResult = portfolioBacktestFromJob(trackedJob);
      if (portfolioResult) {
        setPortfolioBacktestModal((current) => (current ? { ...current, result: portfolioResult } : current));
        void queryClient.invalidateQueries({ queryKey: ["portfolio-backtest-runs", portfolioResult.universe_run_id] });
      }
    }
    invalidateDevelopment(activeJob.sessionId, pool?.universe_run_id);
    const timeout = window.setTimeout(() => setActiveJob(null), trackedJob.status === "completed" ? 2500 : 5000);
    return () => window.clearTimeout(timeout);
  }, [activeJob?.action, activeJob?.jobId, activeJob?.sessionId, pool?.universe_run_id, trackedJob, trackedJob?.status]);

  useEffect(() => {
    const latestInputs = gate?.stage4_realized_expectancy.latest_simulation_inputs;
    if (!session?.session_id || !gate?.stage4_realized_expectancy.exists || !latestInputs) {
      return;
    }
    const nextInputs = {
      initial_capital_usdt: Number(latestInputs.initial_capital_usdt ?? 10000),
      margin_allocation_pct: Number(latestInputs.margin_allocation_pct ?? 30),
      leverage: Number(latestInputs.leverage ?? 5)
    };
    setStage4Inputs((current) => (
      current.initial_capital_usdt === nextInputs.initial_capital_usdt
      && current.margin_allocation_pct === nextInputs.margin_allocation_pct
      && current.leverage === nextInputs.leverage
        ? current
        : nextInputs
    ));
  }, [
    gate?.stage4_realized_expectancy.exists,
    gate?.stage4_realized_expectancy.latest_run_id,
    gate?.stage4_realized_expectancy.latest_simulation_inputs?.initial_capital_usdt,
    gate?.stage4_realized_expectancy.latest_simulation_inputs?.margin_allocation_pct,
    gate?.stage4_realized_expectancy.latest_simulation_inputs?.leverage,
    session?.session_id
  ]);

  const startStage1 = (seedPreference: Stage1SeedStrategyPreference) => {
    if (!row || !pool) {
      return;
    }
    const windows = stageWindows(pool);
    createSessionMutation.mutate({
      source_candidate_id: row.candidate_id,
      strategy_id: plannedStrategyId,
      strategy_version: "v0.1",
      train_start: windows.trainStart,
      train_end: windows.trainEnd,
      walk_forward_start: windows.walkForwardStart,
      walk_forward_end: windows.walkForwardEnd,
      seed_strategy_preference: seedPreference
    });
  };

  const requestStartStage1 = () => {
    if (!row) {
      return;
    }
    setStartChoice({
      strategyId: plannedStrategyId,
      latestAvailable: Boolean(latestSeedSession),
      latestLabel: latestSeedSession
        ? `${latestSeedSession.strategy_version} · ${latestSeedSession.seed_strategy_source_type ?? "latest"}`
        : "No prior developed strategy for this pair",
    });
  };

  const createBundle = (role: Stage1SampleMethod) => {
    if (!session) {
      return;
    }
    createIterationMutation.mutate({
      session_id: session.session_id,
      sample_method: role,
      bundle_role: stage1BundleRoleForMethod(role)
    });
  };

  const runStage4 = () => {
    if (!session) {
      return;
    }
    stage4Mutation.mutate({
      session_id: session.session_id,
      ...stage4Inputs
    });
  };

  const openPortfolioBacktest = () => {
    if (!pool) {
      return;
    }
    const baseAllocation = completedStage4Assets.length ? Math.min(30, Math.floor(100 / completedStage4Assets.length)) : 0;
    const allocations = Object.fromEntries(completedStage4Assets.map((candidate) => [candidate.asset, baseAllocation]));
    setPortfolioBacktestModal({ initialCapital: 10000, allocations, result: null });
  };

  const runPortfolioBacktestNow = () => {
    if (!pool || !portfolioBacktestModal) {
      return;
    }
    portfolioBacktestMutation.mutate({
      universe_run_id: pool.universe_run_id,
      initial_capital_usdt: portfolioBacktestModal.initialCapital,
      margin_allocations_pct: portfolioBacktestModal.allocations
    });
  };

  const requestCreateBundle = (role: Stage1SampleMethod) => {
    if (role === "walk_forward_test" && canForceWalkForward) {
      setOverrideAction({
        kind: "create_walk_forward_bundle",
        title: "Proceed to Walk-Forward?",
        body: "Training is below the 55% Stage 1 threshold. You can still create and score the walk-forward bundle to inspect how the pair behaves out of sample.",
        confirmLabel: "Create Walk-Forward Bundle",
      });
      return;
    }
    createBundle(role);
  };

  const requestCanonicalFreeze = (force = false) => {
    if (!session) {
      return;
    }
    if (!force && canForceFreeze) {
      setOverrideAction({
        kind: "run_canonical_stage1a",
        title: "Freeze Below Gate?",
        body: "Training and walk-forward have both been scored, but at least one slice is below the 55% threshold. You can still freeze the canonical Stage 1 set and continue into downstream stages.",
        confirmLabel: "Freeze Anyway",
      });
      return;
    }
    canonicalMutation.mutate({ session_id: session.session_id, force });
  };

  const runNextAction = () => {
    const type = actionType(row);
    if (!row) {
      return;
    }
    if (type === "start_stage1") {
      requestStartStage1();
      return;
    }
    if (!session) {
      return;
    }
    if (type === "create_training_bundle") {
      createBundle("training");
    } else if (type === "create_walk_forward_bundle") {
      requestCreateBundle("walk_forward_test");
    } else if (type === "run_canonical_stage1a") {
      requestCanonicalFreeze();
    } else if (type === "run_stage2_capture_curve") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage2" });
      stage2Mutation.mutate(session.session_id);
    } else if (type === "run_stage3_fixed_sl") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage3" });
      stage3FixedSlMutation.mutate(session.session_id);
    } else if (type === "run_stage3_exact_protection") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage3" });
      stage3ExactProtectionMutation.mutate(session.session_id);
    } else if (type === "run_stage3_local_variants" || type === "run_stage3_grid_search") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage3" });
      stage3LocalVariantsMutation.mutate(session.session_id);
    } else if (type === "run_stage3_pyramid") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage3" });
      stage3PyramidMutation.mutate(session.session_id);
    } else if (type === "run_stage4_realized_expectancy") {
      updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage: "stage4" });
      runStage4();
    }
  };

  const stage1PrimaryAction = (() => {
    if (!row) {
      return { label: "Select Candidate", disabled: true, run: () => undefined };
    }
    if (!session) {
      return {
        label: createSessionMutation.isPending ? "Starting" : "Start Stage 1",
        disabled: createSessionMutation.isPending,
        run: requestStartStage1,
      };
    }
    if (!scoreExists(gate, "training")) {
      return {
        label: createIterationMutation.isPending ? "Creating" : "Create Training Bundle",
        disabled: createIterationMutation.isPending,
        run: () => createBundle("training"),
      };
    }
    if (!scoreExists(gate, "walk_forward_test")) {
      return {
        label: createIterationMutation.isPending ? "Creating" : "Create Walk-Forward Bundle",
        disabled: createIterationMutation.isPending || !canCreateWalkForward,
        run: () => requestCreateBundle("walk_forward_test"),
      };
    }
    if (!gate?.canonical_readout.exists) {
      return {
        label: canonicalMutation.isPending ? "Freezing" : "Freeze Stage 1",
        disabled: canonicalMutation.isPending,
        run: () => requestCanonicalFreeze(),
      };
    }
    return {
      label: row.next_action.label ?? "Ready",
      disabled: Boolean(row.next_action.disabled),
      run: runNextAction,
    };
  })();

  const visibleErrors = [
    poolQuery.error,
    queueQuery.error,
    sessionsQuery.error,
    iterationsQuery.error,
    gateQuery.error,
    iterationDetailQuery.error,
    portfolioRunsQuery.error,
    activeJobQuery.error,
    createSessionMutation.error,
    createIterationMutation.error,
    deleteIterationMutation.error,
    deleteSessionMutation.error,
    promptMutation.error,
    scoreMutation.error,
    auditMutation.error,
    canonicalMutation.error,
    stage2Mutation.error,
    stage2ExitPolicyMutation.error,
    stage3FixedSlMutation.error,
    stage3ExactProtectionMutation.error,
    stage3LocalVariantsMutation.error,
    stage3PyramidMutation.error,
    stage4Mutation.error,
    portfolioBacktestMutation.error,
    loadPortfolioRunMutation.error,
    deletePortfolioRunMutation.error,
    promoteMutation.error
  ].filter(Boolean) as Error[];

  return (
    <div className="page page--workspace">
      <SplitPane
        className="split-pane--wide-list"
        workbenchClassName="development-workbench"
        left={
          <>
            <div className="list-header">
              <span>Development</span>
              <button
                className="button button--secondary button--compact portfolio-backtest-entry"
                disabled={completedStage4Assets.length === 0}
                onClick={openPortfolioBacktest}
                title={completedStage4Assets.length ? "Run a pool-level account replay across Stage 4-complete assets" : "Complete Stage 4 for at least one accepted asset first"}
                type="button"
              >
                <BarChart3 aria-hidden="true" />
                Portfolio Backtest
              </button>
            </div>
            <label className="compact-select">
              Training Pool
              <select value={pool?.universe_run_id ?? ""} onChange={(event) => updateDevelopmentUrl({ pool: event.target.value })}>
                {(poolQuery.data?.runs ?? []).map((run) => (
                  <option value={run.universe_run_id} key={run.universe_run_id}>{shortPoolId(run.universe_run_id)}</option>
                ))}
              </select>
            </label>
            <div className="state-line development-pool-summary">
              <strong>{pool ? shortPoolId(pool.universe_run_id) : "No pool"}</strong>
              <span>{splitWindowLine(pool)}</span>
            </div>
            {queueQuery.isLoading || sessionsQuery.isLoading ? <div className="state-line">Loading development queue...</div> : null}
            {acceptedRows.length === 0 && !queueQuery.isLoading ? <div className="state-line">No accepted candidates in this training pool.</div> : null}
            <div className="development-candidate-list">
              {acceptedRows.map((candidate) => {
                const candidateStage = developmentVisualStage(candidate);
                return (
                  <button
                    className={[
                      "development-candidate-card",
                      developmentStageClass("development-candidate-card", candidateStage),
                      candidate.candidate_id === row?.candidate_id ? "is-selected" : ""
                    ].filter(Boolean).join(" ")}
                    key={candidate.candidate_id}
                    onClick={() => updateDevelopmentUrl({ pool: candidate.universe_run_id, candidate: candidate.candidate_id, stage: candidateStage })}
                    type="button"
                  >
                    <div className="signal-pool-card__top">
                      <strong>{candidate.asset}</strong>
                      <StatusBadge tone={stageTone(candidate)}>{developmentLabel(candidate)}</StatusBadge>
                    </div>
                    <span>{candidate.signal_engine_id} · {candidate.strategy_id ?? "base strategy"}</span>
                    <small>Trigger {candidate.trigger_rate_pct === null ? "n/a" : `${candidate.trigger_rate_pct}%`} · {formatNumber(candidate.stage0_evaluated_signal_count ?? candidate.packet_count)} signals</small>
                    <small>{candidate.next_action.label}</small>
                  </button>
                );
              })}
            </div>
          </>
        }
        right={
          <>
            <div className="workbench-header development-header">
              <div>
                <span className="eyebrow">Candidate workbench</span>
                <h1>{session ? `${session.asset} / ${session.strategy_id}` : row ? `${row.asset} / ${row.signal_engine_id}` : "Select a candidate"}</h1>
              </div>
              <div className="header-actions">
                {session ? (
                  <button
                    className="button button--secondary button--compact"
                    disabled={deleteSessionMutation.isPending}
                    onClick={() => {
                      if (window.confirm("Reset this development session back to clean slate? This deletes the current Stage 1-4 workspace and iteration history for this candidate. Promoted execution bundles cannot be reset here.")) {
                        deleteSessionMutation.mutate(session.session_id);
                      }
                    }}
                    title="Reset this development session to the clean slate before Stage 1 started"
                    type="button"
                  >
                    <RotateCcw aria-hidden="true" />
                    {deleteSessionMutation.isPending ? "Resetting" : "Reset Session"}
                  </button>
                ) : null}
                {row ? (
                  <span className={["development-stage-tag", developmentStageClass("development-stage-tag", developmentVisualStage(row))].join(" ")}>
                    <StatusBadge tone={stageTone(row)}>{developmentLabel(row)}</StatusBadge>
                  </span>
                ) : null}
                <button
                  className="button button--primary"
                  disabled={activeStage === "stage1" ? stage1PrimaryAction.disabled || sessionJobRunning : (!row || Boolean(row.next_action.disabled) || createSessionMutation.isPending || createIterationMutation.isPending || sessionJobRunning)}
                  onClick={activeStage === "stage1" ? stage1PrimaryAction.run : runNextAction}
                  type="button"
                >
                  {sessionJobRunning ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
                  {sessionJobRunning ? activeJob?.label ?? "Running job" : activeStage === "stage1" ? stage1PrimaryAction.label : row?.next_action.label ?? "Select Candidate"}
                </button>
              </div>
            </div>

            {visibleErrors.map((error) => <div className="state-line state-line--error" key={error.message}>{error.message}</div>)}
            {activeJob && trackedJob ? (
              <div className={trackedJob.status === "failed" ? "development-job-overlay is-error" : "development-job-overlay"}>
                <div className="development-job-overlay__status">
                  <RefreshCw aria-hidden="true" className={trackedJobRunning ? "spin-icon" : undefined} />
                  <div>
                    <strong>{activeJob.label}</strong>
                    <span>{trackedJob.current_step ?? trackedJob.status}</span>
                  </div>
                </div>
                <WorkerRuntimeNotice active={trackedJobRunning} job={trackedJob} />
              </div>
            ) : null}

            <div className="development-summary-strip">
              <div>
                <span>Pool</span>
                <strong>{pool ? shortPoolId(pool.universe_run_id) : "n/a"}</strong>
              </div>
              <div>
                <span>Windows</span>
                <strong>{splitWindowLine(pool)}</strong>
              </div>
              <div>
                <span>Strategy</span>
                <strong>{session ? `${session.strategy_id} @ ${session.strategy_version}` : row?.strategy_id ?? "not started"}</strong>
              </div>
              <div>
                <span>Blocker</span>
                <strong className={gate?.blockers.length ? "tone-risk" : "tone-pass"}>{gate?.blockers[0] ?? (session ? "none" : "Stage 1 not started")}</strong>
              </div>
            </div>

            <StageTabs
              activeStage={activeStage}
              gate={gate}
              row={row}
              onStageChange={(stage) => row && updateDevelopmentUrl({ pool: row.universe_run_id, candidate: row.candidate_id, stage })}
            />

            {activeStage === "stage1" ? (
              <Stage1Panel
                createBundlePending={createIterationMutation.isPending}
                gate={gate}
                groupedIterations={groupedIterations}
                iterations={iterations}
                loadingIterations={iterationsQuery.isLoading}
                mode={mode}
                onCreateBundle={requestCreateBundle}
                onAudit={(iteration) => auditMutation.mutate({ session_id: session!.session_id, iteration_id: iteration.iteration_id, sample_role: stage1RoleForIteration(iteration) })}
                onDelete={(iteration) => deleteIterationMutation.mutate({ session_id: session!.session_id, iteration_id: iteration.iteration_id })}
                onOpenIteration={setSelectedIteration}
                onOpenPrompt={(iteration) => promptMutation.mutate({ session_id: session!.session_id, iteration_id: iteration.iteration_id })}
                onRunCanonical={() => requestCanonicalFreeze()}
                onScore={(iteration) => scoreMutation.mutate({ session_id: session!.session_id, iteration_id: iteration.iteration_id, sample_role: stage1RoleForIteration(iteration) })}
                onStartStage1={requestStartStage1}
                row={row}
                runningCanonical={canonicalMutation.isPending || isJobRunning("canonical")}
                session={session}
                startingSession={createSessionMutation.isPending}
              />
            ) : null}

            {activeStage === "stage2" ? (
              <Stage2Panel
                gate={gate}
                onPromotePolicy={(policy) => session && stage2ExitPolicyMutation.mutate({ session_id: session.session_id, side_policies: policy })}
                onRun={() => session && stage2Mutation.mutate(session.session_id)}
                promotingPolicy={stage2ExitPolicyMutation.isPending}
                running={stage2Mutation.isPending || isJobRunning("stage2")}
              />
            ) : null}

            {activeStage === "stage3" ? (
              <Stage3Panel
                gate={gate}
                onRunExactProtection={() => session && stage3ExactProtectionMutation.mutate(session.session_id)}
                onRunFixedSl={() => session && stage3FixedSlMutation.mutate(session.session_id)}
                onRunLocalVariants={() => session && stage3LocalVariantsMutation.mutate(session.session_id)}
                onRunPyramid={() => session && stage3PyramidMutation.mutate(session.session_id)}
                exactProtectionRunning={stage3ExactProtectionMutation.isPending || isJobRunning("stage3_exact")}
                fixedSlRunning={stage3FixedSlMutation.isPending || isJobRunning("stage3_fixed")}
                localVariantsRunning={stage3LocalVariantsMutation.isPending || isJobRunning("stage3_local")}
                pyramidRunning={stage3PyramidMutation.isPending || isJobRunning("stage3_pyramid")}
              />
            ) : null}

            {activeStage === "stage4" ? (
              <Stage4Panel
                gate={gate}
                onOpenCandidate={setSelectedStage4Candidate}
                onPromote={() => session && promoteMutation.mutate(session.session_id)}
                onRun={runStage4}
                inputs={stage4Inputs}
                onInputsChange={setStage4Inputs}
                promoting={promoteMutation.isPending}
                running={stage4Mutation.isPending || isJobRunning("stage4")}
              />
            ) : null}
          </>
        }
      />

      {startChoice ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal stage1-start-modal" role="dialog" aria-modal="true" aria-labelledby="stage1-start-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Start Stage 1</span>
                <h2 id="stage1-start-title">Choose Base Strategy</h2>
              </div>
              <button className="icon-button" onClick={() => setStartChoice(null)} type="button" aria-label="Close start stage 1 dialog">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <div className="stage1-start-grid">
                <button className="stage1-start-option" onClick={() => { setStartChoice(null); startStage1("engine_base"); }} type="button">
                  <strong>Base Strategy Template</strong>
                  <span>Use the signal engine’s deterministic base script.</span>
                </button>
                <button
                  className={startChoice.latestAvailable ? "stage1-start-option" : "stage1-start-option is-disabled"}
                  disabled={!startChoice.latestAvailable}
                  onClick={() => { setStartChoice(null); startStage1("latest_pair"); }}
                  type="button"
                >
                  <strong>Latest Developed Strategy</strong>
                  <span>{startChoice.latestLabel}</span>
                </button>
              </div>
            </div>
            <footer className="terminal-modal__footer">
              <span>{startChoice.strategyId}</span>
              <button className="button button--secondary" onClick={() => setStartChoice(null)} type="button">Cancel</button>
            </footer>
          </section>
        </div>
      ) : null}

      {overrideAction ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal stage1-override-modal" role="dialog" aria-modal="true" aria-labelledby="stage1-override-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Override Gate</span>
                <h2 id="stage1-override-title">{overrideAction.title}</h2>
              </div>
              <button className="icon-button" onClick={() => setOverrideAction(null)} type="button" aria-label="Close override dialog">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <p className="modal-copy">{overrideAction.body}</p>
            </div>
            <footer className="terminal-modal__footer">
              <span>The current cycle stays auditable. This only removes the UI hard stop.</span>
              <div className="table-action-row">
                <button className="button button--secondary" onClick={() => setOverrideAction(null)} type="button">Cancel</button>
                <button
                  className="button button--primary"
                  onClick={() => {
                    const action = overrideAction;
                    setOverrideAction(null);
                    if (action.kind === "create_walk_forward_bundle") {
                      createBundle("walk_forward_test");
                    } else if (action.kind === "run_canonical_stage1a") {
                      requestCanonicalFreeze(true);
                    }
                  }}
                  type="button"
                >
                  <Play aria-hidden="true" />
                  {overrideAction.confirmLabel}
                </button>
              </div>
            </footer>
          </section>
        </div>
      ) : null}

      {selectedIteration ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal iteration-detail-modal" role="dialog" aria-modal="true" aria-labelledby="iteration-detail-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Iteration Detail</span>
                <h2 id="iteration-detail-title">{selectedIteration.iteration_id}</h2>
              </div>
              <button className="icon-button" onClick={() => setSelectedIteration(null)} type="button" aria-label="Close iteration details">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              {iterationDetailQuery.isLoading ? <div className="state-line">Loading iteration detail...</div> : null}
              {iterationDetailQuery.error ? <div className="state-line state-line--error">{iterationDetailQuery.error.message}</div> : null}
              {iterationDetailQuery.data?.detail ? <IterationDetailPanel detail={iterationDetailQuery.data.detail} iteration={selectedIteration} /> : null}
            </div>
            <footer className="terminal-modal__footer">
              <span>Review the full signal ledger before auditing or spawning the next bundle.</span>
              <button className="button button--secondary" onClick={() => setSelectedIteration(null)} type="button">Close</button>
            </footer>
          </section>
        </div>
      ) : null}

      {selectedStage4Candidate ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal stage4-candidate-modal" role="dialog" aria-modal="true" aria-labelledby="stage4-candidate-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Stage 4 Candidate</span>
                <h2 id="stage4-candidate-title">{selectedStage4Candidate.candidate_id}</h2>
              </div>
              <button className="icon-button" onClick={() => setSelectedStage4Candidate(null)} type="button" aria-label="Close candidate detail">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              {stage4CandidateDetailQuery.isLoading ? <div className="state-line">Loading candidate detail...</div> : null}
              {stage4CandidateDetailQuery.error ? <div className="state-line state-line--error">{stage4CandidateDetailQuery.error.message}</div> : null}
              {stage4CandidateDetailQuery.data?.detail ? <Stage4CandidateDetailPanel detail={stage4CandidateDetailQuery.data.detail} /> : null}
            </div>
            <footer className="terminal-modal__footer">
              <span>Review realized equity and filled-trade outcomes before promoting or rerunning.</span>
              <button className="button button--secondary" onClick={() => setSelectedStage4Candidate(null)} type="button">Close</button>
            </footer>
          </section>
        </div>
      ) : null}

      {portfolioBacktestModal ? (
        <PortfolioBacktestModal
          assets={completedStage4Assets}
          state={portfolioBacktestModal}
          runHistory={portfolioRunsQuery.data?.portfolio_backtest_runs.runs ?? []}
          latestRunId={portfolioRunsQuery.data?.portfolio_backtest_runs.latest_run_id ?? null}
          running={portfolioBacktestMutation.isPending || isJobRunning("portfolio")}
          loadingRunId={loadPortfolioRunMutation.variables?.run_id}
          deletingRunId={deletePortfolioRunMutation.variables?.run_id}
          onClose={() => setPortfolioBacktestModal(null)}
          onDeleteRun={(runId) => pool && deletePortfolioRunMutation.mutate({ universe_run_id: pool.universe_run_id, run_id: runId })}
          onLoadRun={(runId) => pool && loadPortfolioRunMutation.mutate({ universe_run_id: pool.universe_run_id, run_id: runId })}
          onRun={runPortfolioBacktestNow}
          onStateChange={setPortfolioBacktestModal}
        />
      ) : null}

      {prompt ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal prompt-terminal-modal" role="dialog" aria-modal="true" aria-labelledby="agent-prompt-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">{prompt.prompt_type}</span>
                <h2 id="agent-prompt-title">{prompt.iteration_id}</h2>
              </div>
              <button className="icon-button" onClick={() => setPrompt(null)} type="button" aria-label="Close agent prompt">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <div className="field-stack">
                <FieldRow label="Prompt path" value={prompt.prompt_path} />
                <pre className="agent-prompt-box">{prompt.prompt}</pre>
              </div>
            </div>
            <footer className="terminal-modal__footer">
              <span>{copiedPrompt ? "Copied to clipboard" : "Copy this prompt into the local agent session."}</span>
              <button
                className="button button--primary"
                onClick={() => {
                  void navigator.clipboard.writeText(prompt.prompt).then(() => setCopiedPrompt(true));
                }}
                type="button"
              >
                <Clipboard aria-hidden="true" />
                Copy Prompt
              </button>
            </footer>
          </section>
        </div>
      ) : null}
    </div>
  );
}

function StageTabs({
  activeStage,
  gate,
  onStageChange,
  row
}: {
  activeStage: ResearchStageId;
  gate: Stage1GateSummary | null;
  onStageChange: (stage: ResearchStageId) => void;
  row: DevelopmentQueueRow | undefined;
}) {
  const stages: Array<{ id: ResearchStageId; label: string; state: string; tone: "pass" | "warn" | "info" | "idle" }> = [
    {
      id: "stage1",
      label: "Stage 1",
      state: gate?.canonical_readout.exists ? "Frozen" : row?.stage1_session_id ? "In progress" : "Not started",
      tone: gate?.canonical_readout.exists ? "pass" : row?.stage1_session_id ? "info" : "idle"
    },
    {
      id: "stage2",
      label: "Stage 2",
      state: gate?.stage2_capture.exists ? "Complete" : gate?.canonical_readout.exists ? "Ready" : "Locked",
      tone: gate?.stage2_capture.exists ? "pass" : gate?.canonical_readout.exists ? "info" : "idle"
    },
    {
      id: "stage3",
      label: "Stage 3",
      state: gate?.stage3_pyramid.exists ? "Complete" : gate?.stage2_capture.exists ? "Ready" : "Locked",
      tone: gate?.stage3_pyramid.exists ? "pass" : gate?.stage2_capture.exists ? "info" : "idle"
    },
    {
      id: "stage4",
      label: "Stage 4",
      state: gate?.stage4_realized_expectancy.exists ? "Complete" : gate?.stage3_pyramid.exists ? "Ready" : "Locked",
      tone: gate?.stage4_realized_expectancy.exists ? "pass" : gate?.stage3_pyramid.exists ? "info" : "idle"
    }
  ];
  return (
    <div className="development-stage-tabs" role="tablist" aria-label="Development stages">
      {stages.map((stage) => (
        <button
          className={[
            "development-stage-tab",
            developmentStageClass("development-stage-tab", stage.id),
            `development-stage-tab--${stage.tone}`,
            activeStage === stage.id ? "is-active" : ""
          ].filter(Boolean).join(" ")}
          key={stage.id}
          onClick={() => onStageChange(stage.id)}
          type="button"
        >
          <strong>{stage.label}</strong>
          <StatusBadge tone={stage.tone}>{stage.state}</StatusBadge>
        </button>
      ))}
    </div>
  );
}

function ConsistencyMetricCard({
  label,
  meta,
  tone,
  value
}: {
  label: string;
  meta: string;
  tone: "pass" | "warn" | "risk" | "info" | "idle";
  value: string;
}) {
  return (
    <div className={`consistency-metric consistency-metric--${tone}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{meta}</small>
    </div>
  );
}

function ConsistencyFlagList({ flags }: { flags: string[] }) {
  if (!flags.length) {
    return <span className="consistency-flags consistency-flags--quiet">stable</span>;
  }
  return (
    <span className="consistency-flags">
      {flags.map((flag) => (
        <span key={flag}>{flag}</span>
      ))}
    </span>
  );
}

function Stage1ConsistencyPanel({ detail, iteration }: { detail: Stage1IterationDetail; iteration: Stage1IterationSummary }) {
  const consistency = useMemo(() => buildStage1Consistency(detail), [detail]);
  const protectedCount = iteration.failure_audit?.metrics.protected_count;
  const protectedValue = typeof protectedCount === "number" ? formatNumber(protectedCount) : "not audited";
  const protectedMeta = typeof protectedCount === "number" ? "from failure audit" : "generate audit to show protected cases";

  return (
    <TerminalPanel eyebrow="stage 1a" title="Consistency Gates">
      <div className="consistency-layout">
        <div className="consistency-metric-grid">
          <ConsistencyMetricCard
            label="Agreement"
            meta={`${formatNumber(detail.metrics.matches)} match / ${formatNumber(detail.metrics.mismatches)} mismatch`}
            tone={consistencyAgreementTone(consistency.summary.directionalAgreement)}
            value={formatRate(consistency.summary.directionalAgreement, 2)}
          />
          <ConsistencyMetricCard
            label="Worst Month"
            meta={consistency.summary.worstMonth ? consistency.summary.worstMonth.month : "no monthly sample"}
            tone={consistencyAgreementTone(consistency.summary.worstMonth?.directionalAgreement ?? null)}
            value={formatRate(consistency.summary.worstMonth?.directionalAgreement ?? null, 2)}
          />
          <ConsistencyMetricCard
            label="Coverage"
            meta={`${formatNumber(consistency.summary.called)} called / ${formatNumber(consistency.summary.total)} total`}
            tone="info"
            value={formatRate(consistency.summary.calledCoverage)}
          />
          <ConsistencyMetricCard
            label="Skip Drift"
            meta={`${formatRate(consistency.summary.skipRate)} skip vs ${formatRate(consistency.summary.naturalNullRate)} null-GT`}
            tone={consistencyDeviationTone(consistency.summary.neutralDeviation)}
            value={formatSignedPp(consistency.summary.neutralDeviation)}
          />
          <ConsistencyMetricCard
            label="Side Imbalance"
            meta="abs(LONG calls - SHORT calls) / called"
            tone={consistencyImbalanceTone(consistency.summary.sideImbalance)}
            value={formatRate(consistency.summary.sideImbalance)}
          />
          <ConsistencyMetricCard
            label="Protected Cases"
            meta={protectedMeta}
            tone={typeof protectedCount === "number" ? "pass" : "idle"}
            value={protectedValue}
          />
        </div>

        <div className="consistency-split-grid">
          <div className="consistency-table-block">
            <div className="consistency-table-heading">
              <strong>LONG / SHORT Balance</strong>
              <span>Call agreement is measured only on non-neutral calls.</span>
            </div>
            <DataTable<Stage1ConsistencySide>
              columns={[
                { key: "side", header: "Side", render: (item) => <strong>{item.side}</strong> },
                { key: "truth", header: "Truth", align: "right", render: (item) => formatNumber(item.truthCount) },
                { key: "calls", header: "Calls", align: "right", render: (item) => formatNumber(item.callCount) },
                { key: "matches", header: "Match", align: "right", render: (item) => formatNumber(item.matches) },
                { key: "mismatches", header: "Mismatch", align: "right", render: (item) => formatNumber(item.mismatches) },
                { key: "agreement", header: "Agreement", align: "right", render: (item) => <span className={`tone-${consistencyAgreementTone(item.agreement)}`}>{formatRate(item.agreement, 2)}</span> },
              ]}
              getRowKey={(item) => item.side}
              rows={consistency.sides}
            />
          </div>

          <div className="consistency-table-block">
            <div className="consistency-table-heading">
              <strong>Coverage Discipline</strong>
              <span>Natural null-GT is read from returned ground-truth directions.</span>
            </div>
            <div className="consistency-discipline-grid">
              <FieldRow label="Highest skip month" value={consistency.summary.highestSkipMonth ? `${consistency.summary.highestSkipMonth.month} · ${formatRate(consistency.summary.highestSkipMonth.skipRate)}` : "n/a"} />
              <FieldRow label="Neutral count" value={formatNumber(consistency.summary.neutral)} />
              <FieldRow label="Natural null-GT" value={formatRate(consistency.summary.naturalNullRate)} />
              <FieldRow label="Neutral deviation" value={formatSignedPp(consistency.summary.neutralDeviation)} />
            </div>
          </div>
        </div>

        <div className="consistency-table-block consistency-monthly-block">
          <div className="consistency-table-heading">
            <strong>Monthly Consistency</strong>
            <span>Chronological stability, side health, and coverage warnings.</span>
          </div>
          <DataTable<Stage1ConsistencyMonth>
            columns={[
              { key: "month", header: "Month", render: (item) => <strong>{item.month}</strong> },
              { key: "scoreable", header: "Scoreable", align: "right", render: (item) => formatNumber(item.scoreable) },
              { key: "agreement", header: "Agreement", align: "right", render: (item) => <span className={`tone-${consistencyAgreementTone(item.directionalAgreement)}`}>{formatRate(item.directionalAgreement, 2)}</span> },
              { key: "coverage", header: "Coverage", align: "right", render: (item) => formatRate(item.calledCoverage) },
              { key: "skip", header: "Skip", align: "right", render: (item) => formatRate(item.skipRate) },
              { key: "null", header: "Null-GT", align: "right", render: (item) => formatRate(item.naturalNullRate) },
              { key: "drift", header: "Drift", align: "right", render: (item) => <span className={`tone-${consistencyDeviationTone(item.neutralDeviation)}`}>{formatSignedPp(item.neutralDeviation)}</span> },
              { key: "long", header: "LONG", align: "right", render: (item) => formatRate(item.longAgreement, 2) },
              { key: "short", header: "SHORT", align: "right", render: (item) => formatRate(item.shortAgreement, 2) },
              { key: "flags", header: "Flags", render: (item) => <ConsistencyFlagList flags={item.flags} /> },
            ]}
            getRowClassName={(item) => item.flags.length ? "consistency-month-row consistency-month-row--flagged" : "consistency-month-row"}
            getRowKey={(item) => item.month}
            rows={consistency.months}
          />
        </div>
      </div>
    </TerminalPanel>
  );
}

function IterationDetailPanel({ detail, iteration }: { detail: Stage1IterationDetail; iteration: Stage1IterationSummary }) {
  return (
    <div className="iteration-detail-layout">
      <div className="workbench-grid">
        <TerminalPanel eyebrow={detail.sample_role === "walk_forward_test" ? "walk-forward" : "training"} title="Score Summary">
          <div className="field-grid">
            <FieldRow label="Signals" value={formatNumber(detail.signal_count)} />
            <FieldRow label="Scoreable" value={formatNumber(detail.metrics.scoreable)} />
            <FieldRow label="Agreement" value={stage1Agreement(detail.metrics.directional_agreement)} />
            <FieldRow label="Threshold" value={`${detail.metrics.promotion_threshold_pct}%`} />
          </div>
        </TerminalPanel>
        <TerminalPanel eyebrow="artifacts" title="Bundle State">
          <div className="field-grid">
            <FieldRow label="Bundle" value={detail.bundle_role ?? "unknown"} />
            <FieldRow label="Matches" value={formatNumber(detail.metrics.matches)} />
            <FieldRow label="Mismatches" value={formatNumber(detail.metrics.mismatches)} />
            <FieldRow label="Neutral" value={formatNumber(detail.metrics.neutral)} />
          </div>
        </TerminalPanel>
      </div>

      <Stage1ConsistencyPanel detail={detail} iteration={iteration} />

      <TerminalPanel className="scroll-panel" title="Signal Breakdown">
        <DataTable
          columns={[
            { key: "timestamp", header: "Timestamp", render: (item) => formatUtcTimestamp(item.timestamp) },
            { key: "signal_id", header: "Signal", render: (item) => item.signal_id },
            { key: "truth", header: "Truth", render: (item) => item.ground_truth_direction ?? "-" },
            { key: "decision", header: "Decision", render: (item) => item.decision_direction ?? "-" },
            { key: "agreement", header: "Outcome", render: (item) => item.agreement },
            { key: "confidence", header: "Confidence", align: "right", render: (item) => typeof item.confidence === "number" ? item.confidence.toFixed(2) : "-" },
            { key: "reason", header: "Reason", render: (item) => item.reason_code ?? "-" },
          ]}
          getRowKey={(item) => `${item.signal_id}-${item.timestamp ?? "na"}`}
          rows={detail.records}
        />
      </TerminalPanel>
    </div>
  );
}

function Stage1Panel({
  createBundlePending,
  gate,
  groupedIterations,
  iterations,
  loadingIterations,
  mode,
  onAudit,
  onCreateBundle,
  onDelete,
  onOpenIteration,
  onOpenPrompt,
  onRunCanonical,
  onScore,
  onStartStage1,
  row,
  runningCanonical,
  session,
  startingSession
}: {
  createBundlePending: boolean;
  gate: Stage1GateSummary | null;
  groupedIterations: Record<Stage1SampleRole, Stage1IterationSummary[]>;
  iterations: Stage1IterationSummary[];
  loadingIterations: boolean;
  mode: Stage1EvidenceMode;
  onAudit: (iteration: Stage1IterationSummary) => void;
  onCreateBundle: (role: Stage1SampleMethod) => void;
  onDelete: (iteration: Stage1IterationSummary) => void;
  onOpenIteration: (iteration: Stage1IterationSummary) => void;
  onOpenPrompt: (iteration: Stage1IterationSummary) => void;
  onRunCanonical: () => void;
  onScore: (iteration: Stage1IterationSummary) => void;
  onStartStage1: () => void;
  row: DevelopmentQueueRow | undefined;
  runningCanonical: boolean;
  session: Stage1ResearchSession | null;
  startingSession: boolean;
}) {
  const frozen = Boolean(gate?.canonical_readout.exists);
  const canForceFreeze = scoreExists(gate, "training") && scoreExists(gate, "walk_forward_test") && !gate?.ready_to_freeze;
  return (
    <div className="development-stage-body">
      <div className="workbench-grid">
        <TerminalPanel eyebrow="stage 1" title="Evidence Mode">
          <div className="stage1-mode-card">
            <div>
              <StatusBadge tone={mode.tone}>{mode.title}</StatusBadge>
              <strong>{mode.nextAction}</strong>
            </div>
            <FieldRow label="Allowed evidence" value={mode.allowedEvidence} />
            <FieldRow label="Agent use" value={mode.agentUse} />
          </div>
        </TerminalPanel>
        <TerminalPanel eyebrow="gate" title="Current Readout">
          <div className="field-grid">
            <FieldRow label="Training" value={`${gate?.roles.training?.status ?? "missing"} · ${gateScore(gate, "training")}`} />
            <FieldRow label="Walk-forward" value={`${gate?.roles.walk_forward_test?.status ?? "missing"} · ${gateScore(gate, "walk_forward_test")}`} />
            <FieldRow label="Freeze" value={frozen ? "complete" : gate?.ready_to_freeze ? "ready" : "blocked"} />
            <FieldRow label="MATCH set" value={frozen ? formatNumber(gate?.canonical_readout.match_count) : "not frozen"} />
          </div>
        </TerminalPanel>
      </div>

      {!session ? (
        <TerminalPanel title="Start Candidate Workspace">
          <div className="action-card">
            <span>{row ? `${row.asset} passed Training Pool. Create the pair-specific strategy workspace and inherit the pool windows.` : "Select an accepted candidate first."}</span>
            <button className="button button--primary" disabled={!row || startingSession} onClick={onStartStage1} type="button">
              <Play aria-hidden="true" />
              {startingSession ? "Starting" : "Start Stage 1"}
            </button>
          </div>
        </TerminalPanel>
      ) : (
        <>
          <div className="stage1-lanes">
            {stage1Roles.map((role, index) => {
              const latest = groupedIterations[role][groupedIterations[role].length - 1];
              const score = latest ? stage1ScoreForRole(latest, role) : null;
              const status = gate?.roles[role]?.status ?? "missing";
              const walkForwardLocked = role === "walk_forward_test" && !scoreExists(gate, "training");
              return (
                <TerminalPanel eyebrow={`step ${index + 1}`} title={stage1RoleLabel(role)} key={role}>
                  <div className="field-stack">
                    <FieldRow label="Gate" value={status} />
                    <FieldRow label="Latest bundle" value={latest?.iteration_id ?? "none"} />
                    <FieldRow label="Signals" value={formatNumber(latest?.signal_count)} />
                    <FieldRow label="Score" value={score ? stage1Agreement(score.metrics.directional_agreement) : "not scored"} />
                  </div>
                  <button className="button button--secondary full-width-action" disabled={frozen || createBundlePending || walkForwardLocked} onClick={() => onCreateBundle(role)} type="button">
                    <Play aria-hidden="true" />
                    Create {role === "training" ? "Builder" : "Evaluator"} Bundle
                  </button>
                </TerminalPanel>
              );
            })}
            <TerminalPanel eyebrow="step 3" title="Freeze">
              <div className="field-stack">
                <FieldRow label="Status" value={frozen ? "complete" : gate?.ready_to_freeze ? "ready" : "blocked"} />
                <FieldRow label="Artifact" value="canonical Stage 1 decision set" />
                <FieldRow label="Downstream use" value="Stage 2-4" />
              </div>
              <button className="button button--primary full-width-action" disabled={frozen || runningCanonical || (!gate?.ready_to_freeze && !canForceFreeze)} onClick={onRunCanonical} type="button">
                <Play aria-hidden="true" />
                {frozen ? "Frozen" : runningCanonical ? "Freezing" : "Freeze Stage 1"}
              </button>
            </TerminalPanel>
          </div>

          <TerminalPanel className="iteration-ledger-panel" title="Iteration Ledger">
            {loadingIterations ? <div className="state-line">Loading iterations...</div> : null}
            <DataTable
              columns={[
                { key: "id", header: "Iteration", render: (iteration) => <strong>{iteration.iteration_id}</strong> },
                { key: "role", header: "Use", render: (iteration) => stage1RoleLabel(stage1RoleForIteration(iteration)) },
                { key: "bundle", header: "Bundle", render: (iteration) => stage1BundleLabel(iteration) },
                { key: "signals", header: "Signals", align: "right", render: (iteration) => formatNumber(iteration.signal_count) },
                { key: "score", header: "Score", align: "right", render: (iteration) => {
                  const score = stage1ScoreForRole(iteration, stage1RoleForIteration(iteration));
                  return score ? <span className={score.metrics.passes_threshold ? "tone-pass" : "tone-warn"}>{stage1Agreement(score.metrics.directional_agreement)}</span> : "not scored";
                } },
                { key: "audit", header: "Audit", render: (iteration) => iteration.has_failure_audit ? "ready" : "none" },
                { key: "actions", header: "Actions", align: "right", render: (iteration) => (
                  <div className="table-action-row">
                    <button className="button button--secondary" onClick={(event) => { event.stopPropagation(); onOpenPrompt(iteration); }} type="button">Prompt</button>
                    <button className="button button--secondary" disabled={frozen} onClick={(event) => { event.stopPropagation(); onScore(iteration); }} type="button">Score</button>
                    <button className="button button--secondary" disabled={frozen || !stage1ScoreForRole(iteration, stage1RoleForIteration(iteration))} onClick={(event) => { event.stopPropagation(); onAudit(iteration); }} type="button">Audit</button>
                    <button
                      className="icon-button"
                      disabled={frozen}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (window.confirm(`Delete ${iteration.iteration_id}?`)) {
                          onDelete(iteration);
                        }
                      }}
                      type="button"
                      aria-label={`Delete ${iteration.iteration_id}`}
                    >
                      <Trash2 aria-hidden="true" />
                    </button>
                  </div>
                ) }
              ]}
              getRowKey={(iteration) => iteration.iteration_id}
              onRowClick={onOpenIteration}
              rows={iterations.slice().reverse()}
            />
          </TerminalPanel>
        </>
      )}
    </div>
  );
}

function StageRunProgress({ detail, steps, title }: { detail: string; steps: string[]; title: string }) {
  return (
    <div className="progress-card stage-run-progress">
      <div className="progress-card__header">
        <strong>{title}</strong>
        <span>{detail}</span>
      </div>
      <div className="progress-rail" aria-label={title}>
        <span />
      </div>
      <div className="progress-steps">
        {steps.map((step, index) => <span className={`progress-step progress-step--${index + 1}`} key={step}>{step}</span>)}
      </div>
    </div>
  );
}

type Stage2ExitPolicyDraft = {
  lock_profit_pct: number;
  initial_sl_pct: number;
  protect_trigger_pct: number;
  trail_sl_pct: number;
};

type Stage2SidePolicyDraft = Record<"LONG" | "SHORT", Stage2ExitPolicyDraft>;
type Stage2PolicyPreset = "balanced" | "aggressive";

function stage2TpOptions(stage2: Stage1GateSummary["stage2_capture"] | undefined): number[] {
  const values = stage2?.tp_levels?.length
    ? stage2.tp_levels
    : Object.keys(stage2?.results ?? {}).map((value) => Number(value));
  return Array.from(new Set(values.filter((value) => Number.isFinite(value)).map((value) => Number(value.toFixed(1))))).sort((a, b) => a - b);
}

function stage2SlOptions(stage2: Stage1GateSummary["stage2_capture"] | undefined): number[] {
  const values = stage2?.sl_levels?.length
    ? stage2.sl_levels
    : Object.keys(stage2?.sl_results ?? {}).map((value) => Number(value));
  return Array.from(new Set(values.filter((value) => Number.isFinite(value)).map((value) => Number(value.toFixed(1))))).sort((a, b) => a - b);
}

function buildStage2PolicyPreset(
  stage2: Stage1GateSummary["stage2_capture"] | undefined,
  tpOptions: number[],
  slOptions: number[],
  preset: Stage2PolicyPreset
): Stage2SidePolicyDraft {
  const profile = preset === "balanced"
    ? { finalTpRate: 60, protectRate: 85, maxSlHitRate: 25, trailRatio: 0.5 }
    : { finalTpRate: 45, protectRate: 75, maxSlHitRate: 35, trailRatio: 0.35 };
  const fallbackTp = tpOptions[0] ?? 0;
  const fallbackSl = slOptions[0] ?? 0;
  return {
    LONG: buildStage2SidePolicyPreset(stage2, tpOptions, slOptions, "LONG", profile, fallbackTp, fallbackSl),
    SHORT: buildStage2SidePolicyPreset(stage2, tpOptions, slOptions, "SHORT", profile, fallbackTp, fallbackSl)
  };
}

function buildStage2SidePolicyPreset(
  stage2: Stage1GateSummary["stage2_capture"] | undefined,
  tpOptions: number[],
  slOptions: number[],
  side: "LONG" | "SHORT",
  profile: { finalTpRate: number; protectRate: number; maxSlHitRate: number; trailRatio: number },
  fallbackTp: number,
  fallbackSl: number
): Stage2ExitPolicyDraft {
  const split = stage2?.side_splits?.[side];
  const tpCurve = split?.count ? split.results : stage2?.results;
  const slCurve = split?.count ? split.sl_results : stage2?.sl_results;
  const lockProfit = highestLevelAtOrAboveRate(tpOptions, tpCurve, profile.finalTpRate) ?? fallbackTp;
  const protectOptions = tpOptions.filter((value) => value <= lockProfit);
  const protectTrigger = highestLevelAtOrAboveRate(protectOptions, tpCurve, profile.protectRate) ?? protectOptions[0] ?? lockProfit;
  const initialSl = smallestLevelAtOrBelowHitRate(slOptions, slCurve, profile.maxSlHitRate) ?? slOptions[slOptions.length - 1] ?? fallbackSl;
  const trailSl = nearestLevelAtOrBelow(tpOptions, protectTrigger * profile.trailRatio, protectTrigger) ?? protectTrigger;
  return {
    lock_profit_pct: lockProfit,
    initial_sl_pct: initialSl,
    protect_trigger_pct: protectTrigger,
    trail_sl_pct: trailSl
  };
}

function highestLevelAtOrAboveRate(
  levels: number[],
  curve: Record<string, Record<string, Stage2CaptureRate>> | undefined,
  minRate: number
): number | undefined {
  return [...levels].reverse().find((level) => captureRateAt(curve, level) >= minRate);
}

function smallestLevelAtOrBelowHitRate(
  levels: number[],
  curve: Record<string, Record<string, Stage2CaptureRate>> | undefined,
  maxRate: number
): number | undefined {
  return levels.find((level) => captureRateAt(curve, level) <= maxRate);
}

function nearestLevelAtOrBelow(levels: number[], target: number, ceiling: number): number | undefined {
  const candidates = levels.filter((level) => level <= ceiling);
  if (!candidates.length) {
    return undefined;
  }
  return candidates.reduce((best, level) => {
    const bestDistance = Math.abs(best - target);
    const distance = Math.abs(level - target);
    return distance <= bestDistance ? level : best;
  }, candidates[0]);
}

function nearestStage2Level(levels: number[], value: number | undefined, fallback: number): number {
  if (!levels.length) {
    return fallback;
  }
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  return levels.reduce((best, level) => {
    const bestDistance = Math.abs(best - value);
    const distance = Math.abs(level - value);
    return distance < bestDistance ? level : best;
  }, levels[0]);
}

function normalizeStage2SidePolicy(
  draft: Stage2ExitPolicyDraft,
  tpOptions: number[],
  slOptions: number[]
): Stage2ExitPolicyDraft {
  const fallbackTp = tpOptions[0] ?? 0;
  const fallbackSl = slOptions[0] ?? 0;
  const lockProfit = nearestStage2Level(tpOptions, draft.lock_profit_pct, fallbackTp);
  const protectCandidates = tpOptions.filter((value) => value <= lockProfit);
  const protectTrigger = nearestStage2Level(protectCandidates.length ? protectCandidates : tpOptions, draft.protect_trigger_pct, fallbackTp);
  const trailCandidates = tpOptions.filter((value) => value <= protectTrigger);
  return {
    lock_profit_pct: lockProfit,
    initial_sl_pct: nearestStage2Level(slOptions, draft.initial_sl_pct, fallbackSl),
    protect_trigger_pct: protectTrigger,
    trail_sl_pct: nearestStage2Level(trailCandidates.length ? trailCandidates : tpOptions, draft.trail_sl_pct, fallbackTp)
  };
}

function normalizeStage2PolicyDraft(
  draft: Stage2SidePolicyDraft,
  tpOptions: number[],
  slOptions: number[]
): Stage2SidePolicyDraft {
  return {
    LONG: normalizeStage2SidePolicy(draft.LONG, tpOptions, slOptions),
    SHORT: normalizeStage2SidePolicy(draft.SHORT, tpOptions, slOptions)
  };
}

function stage2PolicyDraftIsValid(draft: Stage2SidePolicyDraft, tpOptions: number[], slOptions: number[]): boolean {
  if (!tpOptions.length || !slOptions.length) {
    return false;
  }
  return (["LONG", "SHORT"] as const).every((side) => {
    const policy = draft[side];
    return (
      tpOptions.includes(policy.lock_profit_pct)
      && slOptions.includes(policy.initial_sl_pct)
      && tpOptions.includes(policy.protect_trigger_pct)
      && tpOptions.includes(policy.trail_sl_pct)
      && policy.trail_sl_pct <= policy.protect_trigger_pct
      && policy.protect_trigger_pct <= policy.lock_profit_pct
    );
  });
}

function captureRateAt(curve: Record<string, Record<string, Stage2CaptureRate>> | undefined, level: number): number {
  return curve?.[level.toFixed(1)]?.full_cycle?.rate ?? Number.NEGATIVE_INFINITY;
}

function Stage2Panel({
  gate,
  onPromotePolicy,
  onRun,
  promotingPolicy,
  running
}: {
  gate: Stage1GateSummary | null;
  onPromotePolicy: (policy: Stage2SidePolicyDraft) => void;
  onRun: () => void;
  promotingPolicy: boolean;
  running: boolean;
}) {
  const ready = Boolean(gate?.canonical_readout.exists);
  const stage2 = gate?.stage2_capture;
  const complete = Boolean(stage2?.exists);
  const policy = gate?.stage2_exit_policy;
  const longSplit = stage2?.side_splits?.LONG;
  const shortSplit = stage2?.side_splits?.SHORT;
  const tpOptions = useMemo(() => stage2TpOptions(stage2), [stage2?.results, stage2?.tp_levels]);
  const slOptions = useMemo(() => stage2SlOptions(stage2), [stage2?.sl_levels, stage2?.sl_results]);
  const [policyDraft, setPolicyDraft] = useState<Stage2SidePolicyDraft>({
    LONG: { lock_profit_pct: 0, initial_sl_pct: 0, protect_trigger_pct: 0, trail_sl_pct: 0 },
    SHORT: { lock_profit_pct: 0, initial_sl_pct: 0, protect_trigger_pct: 0, trail_sl_pct: 0 }
  });

  useEffect(() => {
    const tpFallback = tpOptions[0] ?? 0;
    const slFallback = slOptions[0] ?? 0;
    const existingPolicy = policy?.policy ?? {};
    const fallbackPolicy = {
      lock_profit_pct: existingPolicy.lock_profit_pct ?? tpFallback,
      initial_sl_pct: existingPolicy.initial_sl_pct ?? slFallback,
      protect_trigger_pct: existingPolicy.protect_trigger_pct ?? tpFallback,
      trail_sl_pct: existingPolicy.trail_sl_pct ?? tpFallback
    };
    setPolicyDraft(normalizeStage2PolicyDraft(
      {
        LONG: {
          lock_profit_pct: policy?.side_policies?.LONG?.lock_profit_pct ?? fallbackPolicy.lock_profit_pct,
          initial_sl_pct: policy?.side_policies?.LONG?.initial_sl_pct ?? fallbackPolicy.initial_sl_pct,
          protect_trigger_pct: policy?.side_policies?.LONG?.protect_trigger_pct ?? fallbackPolicy.protect_trigger_pct,
          trail_sl_pct: policy?.side_policies?.LONG?.trail_sl_pct ?? fallbackPolicy.trail_sl_pct
        },
        SHORT: {
          lock_profit_pct: policy?.side_policies?.SHORT?.lock_profit_pct ?? fallbackPolicy.lock_profit_pct,
          initial_sl_pct: policy?.side_policies?.SHORT?.initial_sl_pct ?? fallbackPolicy.initial_sl_pct,
          protect_trigger_pct: policy?.side_policies?.SHORT?.protect_trigger_pct ?? fallbackPolicy.protect_trigger_pct,
          trail_sl_pct: policy?.side_policies?.SHORT?.trail_sl_pct ?? fallbackPolicy.trail_sl_pct
        }
      },
      tpOptions,
      slOptions
    ));
  }, [policy?.policy?.initial_sl_pct, policy?.policy?.lock_profit_pct, policy?.policy?.protect_trigger_pct, policy?.policy?.trail_sl_pct, policy?.side_policies, slOptions, tpOptions]);

  const updateSidePolicy = (side: "LONG" | "SHORT", patch: Partial<Stage2ExitPolicyDraft>) => {
    setPolicyDraft((current) => ({
      ...current,
      [side]: {
        ...current[side],
        ...patch
      }
    }));
  };

  const policyReady = complete && tpOptions.length > 0 && slOptions.length > 0;
  const normalizedPolicyDraft = useMemo(() => normalizeStage2PolicyDraft(policyDraft, tpOptions, slOptions), [policyDraft, slOptions, tpOptions]);
  const policyDraftValid = stage2PolicyDraftIsValid(normalizedPolicyDraft, tpOptions, slOptions);
  return (
    <div className="development-stage-body">
      <TerminalPanel
        actions={
          <button className={running ? "button button--primary button--loading" : "button button--primary"} disabled={!ready || running} onClick={onRun} type="button">
            {running ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
            {running ? "Capturing" : complete ? "Rerun Profile" : "Run Capture"}
          </button>
        }
        eyebrow="stage 2"
        title="Travel Capture"
      >
        {running ? (
          <StageRunProgress
            detail="Reading MATCH canonical decisions, walking forward 5m candles, and writing the TP band for setup search"
            steps={["Load MATCH trades", "Scan candles", "Profile travel", "Write TP band"]}
            title="Running Stage 2 capture"
          />
        ) : null}
        <div className="field-grid">
          <FieldRow label="Input" value="Canonical Stage 1 MATCH decisions" />
          <FieldRow label="State" value={running ? "running" : !ready ? "locked" : complete ? "complete" : "ready"} />
          <FieldRow label="Profiled matches" value={formatNumber(stage2?.metrics.stage2_profiled_match_count ?? stage2?.metrics.total_match_signals)} />
          <FieldRow label="Stage 3 trade pool" value={`${formatNumber(stage2?.match_count ?? stage2?.metrics.match_count)} MATCH / ${formatNumber(stage2?.mismatch_count ?? stage2?.metrics.mismatch_count)} MISMATCH`} />
          <FieldRow label="Side split" value={`${formatNumber(longSplit?.count)} LONG / ${formatNumber(shortSplit?.count)} SHORT`} />
          <FieldRow label="TP band" value={`${formatPct(stage2?.recommended_tp_min_pct)} - ${formatPct(stage2?.recommended_tp_max_pct)}`} />
          <FieldRow label="SL band" value={`${formatPct(stage2?.recommended_sl_min_pct)} - ${formatPct(stage2?.recommended_sl_max_pct)}`} />
          <FieldRow label="Artifact" value="MATCH travel curve + all-trade setup input" />
        </div>
        {policyReady ? (
          <div className="stage2-policy-card">
            <div className="stage2-policy-card__copy">
              <strong>Exit Policy Handoff</strong>
              <span>{policy?.exists ? "Promoted side policy exists. Update it before rerunning Stage 3 if the exit setup changes." : "Select LONG and SHORT numerical profit-protection policies before Stage 3."}</span>
            </div>
            <div className="stage2-policy-presets">
              <button className="button button--secondary button--compact" onClick={() => setPolicyDraft(buildStage2PolicyPreset(stage2, tpOptions, slOptions, "balanced"))} type="button">
                Balanced
              </button>
              <button className="button button--secondary button--compact" onClick={() => setPolicyDraft(buildStage2PolicyPreset(stage2, tpOptions, slOptions, "aggressive"))} type="button">
                Aggressive
              </button>
            </div>
            {(["LONG", "SHORT"] as const).map((side) => (
              <div className="stage2-policy-grid" key={side}>
                <strong>{side}</strong>
                <label>
                  Lock Profit
                  <select
                    value={policyDraft[side].lock_profit_pct}
                    onChange={(event) => updateSidePolicy(side, { lock_profit_pct: Number(event.target.value) })}
                  >
                    {tpOptions.map((value) => <option key={value} value={value}>{formatPct(value)}</option>)}
                  </select>
                </label>
                <label>
                  Initial SL
                  <select
                    value={policyDraft[side].initial_sl_pct}
                    onChange={(event) => updateSidePolicy(side, { initial_sl_pct: Number(event.target.value) })}
                  >
                    {slOptions.map((value) => <option key={value} value={value}>{formatPct(value)}</option>)}
                  </select>
                </label>
                <label>
                  Protect Trigger
                  <select
                    value={policyDraft[side].protect_trigger_pct}
                    onChange={(event) => updateSidePolicy(side, { protect_trigger_pct: Number(event.target.value) })}
                  >
                    {tpOptions.map((value) => <option key={value} value={value}>{formatPct(value)}</option>)}
                  </select>
                </label>
                <label>
                  Trail SL To
                  <select
                    value={policyDraft[side].trail_sl_pct}
                    onChange={(event) => updateSidePolicy(side, { trail_sl_pct: Number(event.target.value) })}
                  >
                    {tpOptions.map((value) => <option key={value} value={value}>{formatPct(value)}</option>)}
                  </select>
                </label>
              </div>
            ))}
            <div className="stage2-policy-actions">
              <button className="button button--secondary" disabled={promotingPolicy || !policyDraftValid} onClick={() => onPromotePolicy(normalizedPolicyDraft)} type="button">
                <UploadCloud aria-hidden="true" />
                {promotingPolicy ? "Promoting" : policy?.exists ? "Update Policy" : "Promote Policy"}
              </button>
            </div>
          </div>
        ) : null}
      </TerminalPanel>
      {complete && stage2 ? (
        <TerminalPanel className="scroll-panel stage2-curve-panel" title="TP Capture Curve">
          <DataTable
            columns={[
              { key: "tp", header: "TP", render: (entry) => `${entry.level}%` },
              { key: "training", header: "Training", render: (entry) => formatCaptureRate(entry.rows.training) },
              { key: "walk", header: "Walk-forward", render: (entry) => formatCaptureRate(entry.rows.walk_forward_test) },
              { key: "full", header: "Full", render: (entry) => formatCaptureRate(entry.rows.full_cycle) },
              { key: "long", header: "LONG", render: (entry) => formatCaptureRate(longSplit?.results?.[entry.level]?.full_cycle) },
              { key: "short", header: "SHORT", render: (entry) => formatCaptureRate(shortSplit?.results?.[entry.level]?.full_cycle) }
            ]}
            getRowKey={(entry) => entry.level}
            rows={Object.entries(stage2.results).map(([level, rows]) => ({ level, rows }))}
          />
        </TerminalPanel>
      ) : null}
      {complete && stage2?.sl_results ? (
        <TerminalPanel className="scroll-panel stage2-curve-panel" title="Matched Adverse SL Curve">
          <DataTable
            columns={[
              { key: "sl", header: "SL", render: (entry) => `${entry.level}%` },
              { key: "training", header: "Training hit", render: (entry) => formatCaptureRate(entry.rows.training) },
              { key: "walk", header: "Walk-forward hit", render: (entry) => formatCaptureRate(entry.rows.walk_forward_test) },
              { key: "full", header: "Full hit", render: (entry) => formatCaptureRate(entry.rows.full_cycle) },
              { key: "long", header: "LONG hit", render: (entry) => formatCaptureRate(longSplit?.sl_results?.[entry.level]?.full_cycle) },
              { key: "short", header: "SHORT hit", render: (entry) => formatCaptureRate(shortSplit?.sl_results?.[entry.level]?.full_cycle) }
            ]}
            getRowKey={(entry) => entry.level}
            rows={Object.entries(stage2.sl_results).map(([level, rows]) => ({ level, rows }))}
          />
        </TerminalPanel>
      ) : null}
    </div>
  );
}

function Stage3Panel({
  gate,
  exactProtectionRunning,
  fixedSlRunning,
  localVariantsRunning,
  onRunExactProtection,
  onRunFixedSl,
  onRunLocalVariants,
  onRunPyramid,
  pyramidRunning
}: {
  gate: Stage1GateSummary | null;
  exactProtectionRunning: boolean;
  fixedSlRunning: boolean;
  localVariantsRunning: boolean;
  onRunExactProtection: () => void;
  onRunFixedSl: () => void;
  onRunLocalVariants: () => void;
  onRunPyramid: () => void;
  pyramidRunning: boolean;
}) {
  const stage2Ready = Boolean(gate?.stage2_capture.exists);
  const policyReady = Boolean(gate?.stage2_exit_policy.exists);
  const grid = gate?.stage3_grid;
  const pyramid = gate?.stage3_pyramid;
  const fixed = grid?.fixed_sl_baseline_result;
  const exact = grid?.exact_protection_result ?? grid?.exact_policy_result;
  const ranges = grid?.stage3c_value_ranges;
  const fixedComplete = Boolean(grid?.fixed_sl_complete || fixed?.config_id);
  const exactComplete = Boolean(grid?.exact_protection_complete || exact?.config_id);
  const localComplete = Boolean(grid?.local_variants_complete || grid?.exists);
  const stage2InitialSl = grid?.stage0_risk_policy?.initial_sl_pct;
  const stage0MeaningfulMove = grid?.stage0_risk_policy?.stage0_meaningful_move_threshold_pct;
  const stage0HardExit = grid?.stage0_risk_policy?.hard_exit_hours;
  const pyramidBest = pyramid?.best ?? {};
  const pyramidBaseline = pyramid?.baseline ?? {};
  const pyramidRows = [...(pyramid?.results ?? [])].sort((left, right) => {
    const leftPnl = Number(left.pnl_pct ?? Number.NEGATIVE_INFINITY);
    const rightPnl = Number(right.pnl_pct ?? Number.NEGATIVE_INFINITY);
    return rightPnl - leftPnl;
  }).slice(0, 8);
  const bestSourceSetup = pyramidBest.source_setup ?? {};
  const bestPyramidMode = bestSourceSetup.protection_enabled ? "Protected SL" : "Fixed SL";
  return (
    <div className="development-stage-body">
      <div className="workbench-grid">
        <TerminalPanel
          actions={
            <button className={fixedSlRunning ? "button button--primary button--loading" : "button button--primary"} disabled={!stage2Ready || !policyReady || fixedSlRunning} onClick={onRunFixedSl} type="button">
              {fixedSlRunning ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {fixedSlRunning ? "Testing" : fixedComplete ? "Rerun Fixed SL" : "Run Fixed SL"}
            </button>
          }
          eyebrow="stage 3a"
          title="Fixed SL Baseline"
        >
          {!policyReady ? <div className="state-line state-line--warn">Promote a Stage 2 exit policy before running Stage 3.</div> : null}
          {fixedSlRunning ? (
            <StageRunProgress
              detail="Testing the Stage 2 TP with the selected Stage 2 initial stop and no stop movement"
              steps={["Load executable decisions", "Apply fixed TP/SL", "Walk 5m candles", "Write baseline"]}
              title="Running fixed SL baseline"
            />
          ) : null}
          <div className="field-grid">
            <FieldRow label="Input" value="Stage 2 final TP + selected Stage 2 initial SL" />
            <FieldRow label="Policy" value={policyReady ? "promoted" : "missing"} />
            <FieldRow label="Initial SL" value={formatPct(stage2InitialSl)} />
            <FieldRow label="Stage 0 move" value={formatPct(stage0MeaningfulMove)} />
            <FieldRow label="Hard exit" value={stage0HardExit ? `${formatNumber(stage0HardExit)}h` : "n/a"} />
            <FieldRow label="Executable decisions" value={formatNumber(grid?.total_executable_decisions ?? grid?.total_signals)} />
            <FieldRow label="LONG TP / SL" value={formatStage3SidePolicy(fixed, "LONG")} />
            <FieldRow label="SHORT TP / SL" value={formatStage3SidePolicy(fixed, "SHORT")} />
            <FieldRow label="Hits" value={`${formatNumber(fixed?.tp_count ?? 0)} TP / ${formatNumber(fixed?.initial_sl_count ?? 0)} SL / ${formatNumber(fixed?.time_exit_count ?? 0)} time`} />
            <FieldRow label="Net PnL" value={formatPct(fixed?.net_pnl_pct ?? fixed?.pnl_pct)} />
          </div>
        </TerminalPanel>
        <TerminalPanel
          actions={
            <button className={exactProtectionRunning ? "button button--primary button--loading" : "button button--primary"} disabled={!fixedComplete || exactProtectionRunning} onClick={onRunExactProtection} type="button">
              {exactProtectionRunning ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {exactProtectionRunning ? "Testing" : exactComplete ? "Rerun Protection" : "Run Protection"}
            </button>
          }
          eyebrow="stage 3b"
          title="Exact Protection"
        >
          {exactProtectionRunning ? (
            <StageRunProgress
              detail="Testing the promoted Stage 2 protect trigger and protected stop exactly"
              steps={["Load baseline", "Activate protection after trigger", "Track protected SL", "Write exact result"]}
              title="Running exact protection test"
            />
          ) : null}
          <div className="field-grid">
            <FieldRow label="Input" value="3A baseline + Stage 2 protection policy" />
            <FieldRow label="LONG TP / SL" value={formatStage3SidePolicy(exact, "LONG")} />
            <FieldRow label="LONG protection" value={formatStage3SideProtection(exact, "LONG")} />
            <FieldRow label="SHORT TP / SL" value={formatStage3SidePolicy(exact, "SHORT")} />
            <FieldRow label="SHORT protection" value={formatStage3SideProtection(exact, "SHORT")} />
            <FieldRow label="Hits" value={`${formatNumber(exact?.tp_count ?? 0)} TP / ${formatNumber(exact?.initial_sl_count ?? 0)} init SL / ${formatNumber(exact?.protected_sl_count ?? 0)} protected`} />
            <FieldRow label="Win rate" value={formatPct(exact?.wr)} />
            <FieldRow label="Net PnL" value={formatPct(exact?.net_pnl_pct ?? exact?.pnl_pct)} />
          </div>
        </TerminalPanel>
      </div>
      <div className="workbench-grid">
        <TerminalPanel
          className="scroll-panel"
          actions={
            <button className={localVariantsRunning ? "button button--primary button--loading" : "button button--primary"} disabled={!exactComplete || localVariantsRunning} onClick={onRunLocalVariants} type="button">
              {localVariantsRunning ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {localVariantsRunning ? "Testing" : localComplete ? "Rerun Variants" : "Run Variants"}
            </button>
          }
          eyebrow="stage 3c"
          title="Local Variants"
        >
          {localVariantsRunning ? (
            <StageRunProgress
              detail="Testing all valid adjacent TP/protect/trail/SL permutations"
              steps={["Build adjacent grid", "Walk candles", "Rank every setup", "Write Stage 4 candidates"]}
              title="Running local variant test"
            />
          ) : null}
          <div className="field-grid">
            <FieldRow label="Input" value="3A + 3B results" />
            <FieldRow label="Combinations" value={formatNumber(grid?.stage3c_total_combinations_tested)} />
            <FieldRow label="TP values" value={formatRangeList(ranges?.final_tp_pct)} />
            <FieldRow label="Protect values" value={formatRangeList(ranges?.protect_trigger_pct)} />
            <FieldRow label="Trail SL values" value={formatRangeList(ranges?.trail_sl_pct)} />
          </div>
          {localComplete ? (
            <DataTable
              columns={[
                { key: "mode", header: "Mode", render: (item) => item.protection_enabled ? "Protected" : "Fixed SL" },
                { key: "setup", header: "L/S TP / SL", render: (item) => formatStage3Policy(item) },
                { key: "protect", header: "L/S Protect / Trail", render: (item) => formatStage3Protection(item) },
                { key: "wr", header: "WR", align: "right", render: (item) => formatPct(item.wr) },
                { key: "hits", header: "TP / Init SL / Prot SL / Time", render: (item) => `${formatNumber(item.tp_count)} / ${formatNumber(item.initial_sl_count ?? 0)} / ${formatNumber(item.protected_sl_count ?? 0)} / ${formatNumber(item.time_exit_count ?? item.neither)}` },
                { key: "pf", header: "PF", align: "right", render: (item) => item.profit_factor === 999 ? "inf" : item.profit_factor.toFixed(2) },
                { key: "pnl", header: "PnL", align: "right", render: (item) => formatPct(item.pnl_pct) }
              ]}
              getRowKey={(item) => item.config_id ?? `${item.stage3_step}-${item.final_tp_pct ?? item.tp}-${item.initial_sl_pct ?? item.sl}-${item.protect_trigger_pct}-${item.trail_sl_pct}`}
              rows={grid?.top_5 ?? []}
            />
          ) : null}
        </TerminalPanel>
        <TerminalPanel
          className="scroll-panel"
          actions={
            <button className={pyramidRunning ? "button button--primary button--loading" : "button button--primary"} disabled={!localComplete || pyramidRunning} onClick={onRunPyramid} type="button">
              {pyramidRunning ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {pyramidRunning ? "Searching" : pyramid?.exists ? "Rerun Pyramid" : "Run Pyramid"}
            </button>
          }
          eyebrow="stage 3d"
          title="Pyramiding"
        >
          {pyramidRunning ? (
            <StageRunProgress
              detail="Testing pyramid spacing and leg behavior from the Stage 3C shortlist"
              steps={["Load Stage 3C top 5", "Sweep max legs", "Compare baseline", "Write setup"]}
              title="Running pyramiding test"
            />
          ) : null}
          <div className="field-grid">
            <FieldRow label="Input" value="Stage 3C top 5" />
            <FieldRow label="Mode" value={pyramid?.exists ? bestPyramidMode : "not tested"} />
            <FieldRow label="Baseline PnL" value={formatPct(pyramidBaseline.pnl_pct)} />
            <FieldRow label="Best legs / step" value={`${formatNumber(pyramidBest.max_legs ?? pyramid?.max_legs)} legs / ${formatPct(pyramidBest.step_pct)}`} />
            <FieldRow label="Source TP / SL" value={formatStage3Policy(bestSourceSetup)} />
            <FieldRow label="Source protection" value={formatStage3Protection(bestSourceSetup)} />
            <FieldRow label="Avg legs" value={formatDecimal(pyramidBest.avg_legs_per_signal)} />
            <FieldRow label="Delta vs baseline" value={formatPct(pyramidBest.delta_vs_baseline_pct)} />
            <FieldRow label="Wins / losses" value={`${formatNumber(pyramidBest.wins)} / ${formatNumber(pyramidBest.losses)}`} />
            <FieldRow label="Net PnL" value={formatPct(pyramidBest.pnl_pct)} />
          </div>
          {pyramid?.exists ? (
            <DataTable
              columns={[
                { key: "source", header: "Source", render: (item) => item.source_candidate_id ? shortId(item.source_candidate_id) : "baseline" },
                { key: "setup", header: "Source TP / SL", render: (item) => item.source_setup ? formatStage3Policy(item.source_setup) : `${formatPct(item.tp_pct ?? pyramid.tp_pct)} TP / ${formatPct(item.sl_pct ?? pyramid.sl_pct)} SL` },
                { key: "protect", header: "Protection", render: (item) => item.source_setup ? formatStage3Protection(item.source_setup) : "-" },
                { key: "legs", header: "Legs / Step", render: (item) => `${formatNumber(item.max_legs)} / ${item.step_pct == null ? "base" : formatPct(item.step_pct)}` },
                { key: "avg", header: "Avg Legs", align: "right", render: (item) => formatDecimal(item.avg_legs_per_signal) },
                { key: "wl", header: "W / L", align: "right", render: (item) => `${formatNumber(item.wins)} / ${formatNumber(item.losses)}` },
                { key: "delta", header: "Delta", align: "right", render: (item) => formatPct(item.delta_vs_baseline_pct) },
                { key: "pnl", header: "PnL", align: "right", render: (item) => formatPct(item.pnl_pct) }
              ]}
              getRowKey={(item) => `${item.source_candidate_id ?? "baseline"}-${item.max_legs}-${item.step_pct ?? "base"}-${item.pnl_pct}`}
              rows={pyramidRows}
            />
          ) : null}
        </TerminalPanel>
      </div>
    </div>
  );
}

function Stage4Panel({
  gate,
  onOpenCandidate,
  onPromote,
  onRun,
  inputs,
  onInputsChange,
  promoting,
  running
}: {
  gate: Stage1GateSummary | null;
  onOpenCandidate: (candidate: Stage4CandidateResult) => void;
  onPromote: () => void;
  onRun: () => void;
  inputs: { initial_capital_usdt: number; margin_allocation_pct: number; leverage: number };
  onInputsChange: (inputs: { initial_capital_usdt: number; margin_allocation_pct: number; leverage: number }) => void;
  promoting: boolean;
  running: boolean;
}) {
  const ready = Boolean(gate?.stage3_pyramid.exists);
  const stage4 = gate?.stage4_realized_expectancy;
  const complete = Boolean(stage4?.exists);
  const best = stage4?.best_candidate ?? {};
  const account = best.account ?? {};
  const latestInputs = stage4?.latest_simulation_inputs ?? null;
  const inputsDirty = Boolean(
    complete && latestInputs && (
      Number(latestInputs.initial_capital_usdt) !== Number(inputs.initial_capital_usdt)
      || Number(latestInputs.margin_allocation_pct) !== Number(inputs.margin_allocation_pct)
      || Number(latestInputs.leverage) !== Number(inputs.leverage)
    )
  );
  const runLabel = running ? "Backtesting" : complete ? inputsDirty ? "Run Updated Test" : "Run New Test" : "Run Expectancy";
  const bestSetup: Stage4CandidateResult["setup"] = best.setup ?? {};
  const exitMode = best.candidate_id ? formatStage4ExitMode(bestSetup) : "n/a";
  const pyramid = bestSetup.pyramid;
  return (
    <div className="development-stage-body">
      <TerminalPanel
        actions={
          <>
            <button className={running ? "button button--primary button--loading" : "button button--primary"} disabled={!ready || running} onClick={onRun} type="button">
              {running ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {runLabel}
            </button>
            <button className="button button--secondary" disabled={!complete || promoting || inputsDirty} onClick={onPromote} type="button"><UploadCloud aria-hidden="true" />{promoting ? "Promoting" : "Promote"}</button>
          </>
        }
        eyebrow="stage 4"
        title="Realized Expectancy"
      >
        {inputsDirty ? <div className="state-line state-line--warn">Setup changed - rerun before promotion.</div> : null}
        {running ? (
          <StageRunProgress
            detail="Walking canonical decisions sequentially, simulating positions, fees, pyramids, and hard exits"
            steps={["Load decisions", "Replay candles", "Simulate account", "Write ledger"]}
            title="Running sequential Stage 4 backtest"
          />
        ) : null}
        <div className="stage4-setup-strip">
          <div className="stage4-setup-strip__title">
            <strong>Simulation Setup</strong>
            <span>Rerun whenever capital, margin, or leverage changes.</span>
          </div>
          <label className="stage4-number-control">
            <span>Capital</span>
            <input
              min="1"
              step="100"
              type="number"
              value={inputs.initial_capital_usdt}
              onChange={(event) => onInputsChange({ ...inputs, initial_capital_usdt: Number(event.target.value) })}
            />
            <em>USDT</em>
          </label>
          <label className="stage4-slider-control">
            <span>Margin</span>
            <input
              min="1"
              max="100"
              step="1"
              type="range"
              value={inputs.margin_allocation_pct}
              onChange={(event) => onInputsChange({ ...inputs, margin_allocation_pct: Number(event.target.value) })}
            />
            <strong>{formatPct(inputs.margin_allocation_pct)}</strong>
          </label>
          <label className="stage4-slider-control">
            <span>Leverage</span>
            <input
              min="1"
              max="20"
              step="1"
              type="range"
              value={inputs.leverage}
              onChange={(event) => onInputsChange({ ...inputs, leverage: Number(event.target.value) })}
            />
            <strong>{formatNumber(inputs.leverage)}x</strong>
          </label>
        </div>
        <div className="stage4-result-strip">
          <div>
            <span>Best Candidate</span>
            <strong>{stage4?.best_candidate_id ?? best.candidate_id ?? "n/a"}</strong>
          </div>
          <div>
            <span>Net Expectancy</span>
            <strong>{formatPct(best.net_expectancy_pct)}</strong>
          </div>
          <div>
            <span>Trades</span>
            <strong>{formatNumber(best.executed_trades)}</strong>
          </div>
          <div>
            <span>Ending Equity</span>
            <strong>{formatUsd(account.ending_equity_usdt)}</strong>
          </div>
          <div>
            <span>Net PnL</span>
            <strong>{formatUsd(account.net_pnl_usdt)}</strong>
          </div>
          <div>
            <span>Fees</span>
            <strong>{formatUsd(account.total_fees_usdt)}</strong>
          </div>
        </div>
        {best.candidate_id ? <Stage4ExitPolicyPanel setup={bestSetup} /> : null}
        <div className="stage4-footnote-grid">
          <FieldRow label="Input" value="Stage 4 candidates + full canonical decisions" />
          <FieldRow label="Simulator" value="Sequential account backtest" />
          <FieldRow label="Exit mode" value={exitMode} />
          <FieldRow label="TP / Initial SL" value={formatStage3Policy(bestSetup)} />
          <FieldRow label="Protect / Trail" value={formatStage3Protection(bestSetup)} />
          <FieldRow label="Hard exit" value={bestSetup.max_hold_hours ? `${formatNumber(bestSetup.max_hold_hours)}h` : "n/a"} />
          <FieldRow label="Pyramid" value={pyramid ? `${formatNumber(pyramid.max_legs)} legs @ ${formatPct(pyramid.step_pct)}` : "off"} />
          <FieldRow label="Fees" value="OKX USDT swap taker default, 5 bps per fill" />
          <FieldRow label="Position-open skips" value={formatNumber(best.skipped_position_open)} />
          <FieldRow label="Initial / Protected SL" value={`${formatNumber(best.initial_sl_hits)} / ${formatNumber(best.protected_sl_hits)}`} />
          <FieldRow label="Latest run" value={stage4?.latest_run_id ?? "n/a"} />
        </div>
      </TerminalPanel>
      {complete && stage4 ? (
        <TerminalPanel className="scroll-panel stage4-results-panel" title="Candidate Results">
          <DataTable
            columns={[
              { key: "id", header: "Candidate", render: (item) => item.candidate_id },
              { key: "policy", header: "Policy", render: (item) => formatStage3Policy(item.setup) },
              { key: "protect", header: "Protection", render: (item) => formatStage3Protection(item.setup) },
              { key: "pyramid", header: "Pyramid", render: (item) => formatPyramidPolicy(item.setup) },
              { key: "net", header: "Net Exp", align: "right", render: (item) => formatPct(item.net_expectancy_pct) },
              { key: "trades", header: "Trades", align: "right", render: (item) => formatNumber(item.executed_trades) },
              { key: "win", header: "Win Rate", align: "right", render: (item) => formatPct(item.win_rate_pct) },
              { key: "pnl", header: "Account PnL", align: "right", render: (item) => formatPct(item.net_pnl_pct) },
              { key: "fees", header: "Fees", align: "right", render: (item) => formatUsd(item.account?.total_fees_usdt) }
            ]}
            getRowKey={(item) => item.candidate_id}
            onRowClick={onOpenCandidate}
            rows={stage4.candidates}
          />
        </TerminalPanel>
      ) : null}
      {stage4?.stage4_runs?.length ? (
        <TerminalPanel className="scroll-panel stage4-run-history-panel" title="Stage 4 Run History">
          <DataTable
            columns={[
              { key: "time", header: "Run", render: (item) => item.created_at?.replace("T", " ").replace("Z", " UTC") ?? item.run_id },
              { key: "setup", header: "Setup", render: (item) => `${formatUsd(item.simulation_inputs.initial_capital_usdt)} · ${formatPct(item.simulation_inputs.margin_allocation_pct)} · ${formatNumber(item.simulation_inputs.leverage)}x` },
              { key: "candidate", header: "Best", render: (item) => item.best_candidate_id ?? "n/a" },
              { key: "equity", header: "Ending Equity", align: "right", render: (item) => formatUsd(item.account?.ending_equity_usdt) },
              { key: "pnl", header: "Net PnL", align: "right", render: (item) => formatUsd(item.account?.net_pnl_usdt) },
              { key: "fees", header: "Fees", align: "right", render: (item) => formatUsd(item.account?.total_fees_usdt) }
            ]}
            getRowKey={(item) => item.run_id}
            rows={stage4.stage4_runs.slice().reverse()}
          />
        </TerminalPanel>
      ) : null}
    </div>
  );
}

function PortfolioBacktestModal({
  assets,
  state,
  runHistory,
  latestRunId,
  running,
  loadingRunId,
  deletingRunId,
  onClose,
  onDeleteRun,
  onLoadRun,
  onRun,
  onStateChange
}: {
  assets: DevelopmentQueueRow[];
  state: PortfolioBacktestModalState;
  runHistory: PortfolioBacktestRunIndex["runs"];
  latestRunId: string | null;
  running: boolean;
  loadingRunId?: string;
  deletingRunId?: string;
  onClose: () => void;
  onDeleteRun: (runId: string) => void;
  onLoadRun: (runId: string) => void;
  onRun: () => void;
  onStateChange: Dispatch<SetStateAction<PortfolioBacktestModalState | null>>;
}) {
  const result = state.result;
  const [ledgerTab, setLedgerTab] = useState<"trades" | "skipped">("trades");
  const allocationTotal = Object.values(state.allocations).reduce((sum, value) => sum + Number(value || 0), 0);
  const tradeRows = stage4FilledTrades(result?.trade_ledger ?? []).map((trade, index) => {
    const asset = (trade as Stage4TradeLedgerRow & { asset?: string }).asset;
    return { ...trade, asset, row_key: `${asset ?? "asset"}-${trade.position_id ?? trade.signal_id}-${index}` };
  });
  const skippedRows = (result?.skipped_signals ?? []).map((item, index) => ({ ...item, row_key: `${item.asset}-${item.signal_id ?? item.signal_ts ?? "skip"}-${index}` }));
  const latestPoint = result?.equity_curve[result.equity_curve.length - 1];

  return (
    <div className="modal-backdrop" role="presentation">
      <section className="terminal-modal portfolio-backtest-modal" role="dialog" aria-modal="true" aria-labelledby="portfolio-backtest-title">
        <header className="terminal-modal__header">
          <div>
            <span className="eyebrow">Pool Replay</span>
            <h2 id="portfolio-backtest-title">Portfolio Backtest</h2>
          </div>
          <button className="icon-button" onClick={onClose} type="button" aria-label="Close portfolio backtest">
            <X aria-hidden="true" />
          </button>
        </header>

        <div className="terminal-modal__body portfolio-backtest-body">
          <div className="portfolio-backtest-layout">
            <div className="portfolio-backtest-controls">
            <TerminalPanel eyebrow="inputs" title="Shared Account">
              <div className="portfolio-control-stack">
                <label className="stage4-number-control portfolio-capital-control">
                  <span>Capital</span>
                  <input
                    min={100}
                    onChange={(event) => {
                      const next = Number(event.target.value);
                      onStateChange((current) => (current ? { ...current, initialCapital: Number.isFinite(next) ? next : current.initialCapital } : current));
                    }}
                    step={100}
                    type="number"
                    value={state.initialCapital}
                  />
                  <em>USDT</em>
                </label>
                <FieldRow label="Eligible Assets" value={formatNumber(assets.length)} />
                <FieldRow label="Allocation Sum" value={formatPct(allocationTotal)} tone={allocationTotal > 100 ? "warn" : "default"} />
                <FieldRow label="Margin Basis" value="current equity" />
              </div>
            </TerminalPanel>

            <TerminalPanel eyebrow="margin" title="Asset Allocation">
              <div className="portfolio-allocation-list">
                {assets.map((asset) => {
                  const value = state.allocations[asset.asset] ?? 0;
                  return (
                    <label className="portfolio-allocation-row" key={asset.candidate_id}>
                      <span>
                        <strong>{asset.asset}</strong>
                        <em>{asset.signal_engine_id}</em>
                      </span>
                      <input
                        max={100}
                        min={0}
                        onChange={(event) => {
                          const next = Number(event.target.value);
                          onStateChange((current) => current ? {
                            ...current,
                            allocations: { ...current.allocations, [asset.asset]: Number.isFinite(next) ? next : value }
                          } : current);
                        }}
                        step={1}
                        type="range"
                        value={value}
                      />
                      <strong>{formatPct(value)}</strong>
                    </label>
                  );
                })}
              </div>
            </TerminalPanel>

            <TerminalPanel eyebrow="artifacts" title="Saved Runs">
              <div className="portfolio-run-history-list">
                {runHistory.length ? runHistory.map((run) => (
                  <div className={run.run_id === result?.run_id ? "portfolio-run-history-row is-active" : "portfolio-run-history-row"} key={run.run_id}>
                    <button className="portfolio-run-history-main" onClick={() => onLoadRun(run.run_id)} type="button">
                      <strong>{run.run_id === latestRunId ? "Latest" : formatCompactUtcTimestamp(run.created_at)}</strong>
                      <span>{formatUsd(run.account?.ending_equity_usdt)} · {formatUsd(run.account?.net_pnl_usdt)} PnL</span>
                      <small>{formatNumber(run.summary?.executed_positions)} trades · {formatNumber(run.summary?.skipped_signals)} skipped</small>
                    </button>
                    <div className="portfolio-run-history-actions">
                      <button
                        className="button button--danger button--compact"
                        disabled={deletingRunId === run.run_id}
                        onClick={() => {
                          if (window.confirm(`Delete portfolio backtest run ${run.run_id}? This removes its persisted run artifact.`)) {
                            onDeleteRun(run.run_id);
                          }
                        }}
                        type="button"
                      >
                        <Trash2 aria-hidden="true" />
                        {deletingRunId === run.run_id ? "…" : ""}
                      </button>
                    </div>
                  </div>
                )) : <div className="state-line">No saved portfolio runs yet.</div>}
              </div>
            </TerminalPanel>
            </div>

            <div className="portfolio-backtest-results">
            {result ? (
              <>
                <div className="portfolio-summary-strip">
                  <div><span>Assets</span><strong>{formatNumber(result.summary.eligible_asset_count)}</strong></div>
                  <div><span>Executed</span><strong>{formatNumber(result.summary.executed_positions)}</strong></div>
                  <div><span>Win Rate</span><strong>{result.summary.executed_positions > 0 ? formatPct((stage4FilledTrades(result.trade_ledger ?? []).filter((t) => (t.net_pnl_usdt ?? 0) > 0).length / result.summary.executed_positions) * 100) : "-"}</strong></div>
                  <div><span>Net PnL</span><strong className={result.account.net_pnl_usdt >= 0 ? "tone-pass" : "tone-risk"}>{formatUsd(result.account.net_pnl_usdt)}</strong></div>
                  <div><span>Fees Paid</span><strong>{formatUsd(result.account.total_fees_usdt)}</strong></div>
                  <div><span>Ending Equity</span><strong>{formatUsd(result.account.ending_equity_usdt)}</strong></div>
                  <div><span>Margin Skips</span><strong>{formatNumber(result.summary.skipped_insufficient_margin)}</strong></div>
                  <div><span>Asset Skips</span><strong>{formatNumber(result.summary.skipped_asset_open)}</strong></div>
                </div>

                <div className="stage4-detail-chart-card portfolio-chart-card">
                  <div className="stage4-detail-chart-card__header">
                    <strong>Shared Account Equity</strong>
                    <span>{latestPoint ? `${formatUsd(latestPoint.free_margin_usdt)} free margin` : "No account points"}</span>
                  </div>
                  <PortfolioEquityCurve points={result.equity_curve} />
                </div>

                <div>
                  <div className="portfolio-ledger-tabs">
                    <button
                      className={`portfolio-ledger-tab ${ledgerTab === "trades" ? "portfolio-ledger-tab--active" : ""}`}
                      onClick={() => setLedgerTab("trades")}
                      type="button"
                    >
                      Filled Trades ({formatNumber(tradeRows.length)})
                    </button>
                    <button
                      className={`portfolio-ledger-tab ${ledgerTab === "skipped" ? "portfolio-ledger-tab--active" : ""}`}
                      onClick={() => setLedgerTab("skipped")}
                      type="button"
                    >
                      Skipped Signals ({formatNumber(skippedRows.length)})
                    </button>
                  </div>

                  {ledgerTab === "trades" ? (
                    <TerminalPanel className="portfolio-ledger-panel" title="">
                      <DataTable
                        columns={[
                          { key: "asset", header: "Asset", render: (item) => item.asset ?? "-" },
                          { key: "open", header: "Open", render: (item) => formatUtcTimestamp(item.entry_ts ?? item.signal_ts) },
                          { key: "close", header: "Close", render: (item) => formatUtcTimestamp(item.exit_ts ?? item.signal_ts) },
                          { key: "dur", header: "Dur", align: "right", render: (item) => item.open_duration_hours != null ? `${item.open_duration_hours.toFixed(1)}h` : "-" },
                          { key: "side", header: "Side", render: (item) => item.decision_direction ?? "-" },
                          { key: "exit", header: "Exit", render: (item) => item.exit_status ?? "-" },
                          { key: "lev", header: "Lev", align: "right", render: (item) => item.leverage ? `${item.leverage}x` : "-" },
                          { key: "size", header: "Size", align: "right", render: (item) => formatUsd(item.position_notional_usdt ?? stage4TradeNotional(item)) },
                          { key: "margin", header: "Margin", align: "right", render: (item) => formatUsd(item.position_margin_usdt ?? stage4TradeMarginUsed(item)) },
                          { key: "gross", header: "Gross", align: "right", render: (item) => formatUsd(item.gross_pnl_usdt) },
                          { key: "fees", header: "Fees", align: "right", render: (item) => formatUsd(item.total_fees_usdt) },
                          { key: "net", header: "Net PnL", align: "right", render: (item) => formatUsd(item.net_pnl_usdt) },
                          { key: "roe", header: "ROE", align: "right", render: (item) => formatPct(item.roe_pct ?? stage4TradeRoePct(item) ?? 0) },
                          { key: "equity", header: "Equity", align: "right", render: (item) => formatUsd(item.equity_after) },
                          { key: "legs", header: "Legs", align: "right", render: (item) => formatNumber(item.filled_legs) }
                        ]}
                        getRowKey={(item) => item.row_key}
                        getRowClassName={stage4TradeRowClassName}
                        rows={tradeRows}
                      />
                    </TerminalPanel>
                  ) : (
                    <TerminalPanel className="portfolio-ledger-panel" title="">
                      <DataTable
                        columns={[
                          { key: "asset", header: "Asset", render: (item) => item.asset },
                          { key: "time", header: "Signal", render: (item) => formatUtcTimestamp(item.signal_ts) },
                          { key: "reason", header: "Reason", render: (item) => item.skip_reason },
                          { key: "requested", header: "Requested", align: "right", render: (item) => formatUsd(item.requested_margin_usdt) },
                          { key: "free", header: "Free", align: "right", render: (item) => formatUsd(item.free_margin_usdt) }
                        ]}
                        getRowKey={(item) => item.row_key}
                        rows={skippedRows}
                      />
                    </TerminalPanel>
                  )}
                </div>
              </>
            ) : (
              <div className="portfolio-empty-state">
                <BarChart3 aria-hidden="true" />
                <strong>Configure allocations and run the shared account replay.</strong>
                <span>The result will use each asset's latest Stage 4-complete candidate and save a pool-level artifact.</span>
              </div>
            )}
            </div>
          </div>
        </div>

        <footer className="terminal-modal__footer">
          <span>{result ? `${result.run_id} · ${result.portfolio_backtest_path ?? "saved under pool artifacts"}` : "Stage 4-complete assets only"}</span>
          <div className="table-action-row">
            <button className="button button--secondary" onClick={onClose} type="button">Close</button>
            <button className="button button--primary" disabled={running || assets.length === 0} onClick={onRun} type="button">
              {running ? <RefreshCw aria-hidden="true" className="spin-icon" /> : <Play aria-hidden="true" />}
              {running ? "Running" : "Run Backtest"}
            </button>
          </div>
        </footer>
      </section>
    </div>
  );
}

function PortfolioEquityCurve({ points }: { points: PortfolioBacktestResult["equity_curve"] }) {
  const curve = points.filter((point) => typeof point.equity_usdt === "number" && !Number.isNaN(point.equity_usdt));
  if (curve.length < 2) {
    return <div className="state-line">Not enough equity points to render the chart.</div>;
  }
  const equities = curve.map((point) => point.equity_usdt);
  const min = Math.min(...equities);
  const max = Math.max(...equities);
  const span = max - min || 1;
  const paddedMin = min - span * 0.06;
  const paddedMax = max + span * 0.06;
  const paddedSpan = paddedMax - paddedMin || 1;
  const width = 1000;
  const height = 300;
  const left = 78;
  const right = 18;
  const top = 18;
  const bottom = 36;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const path = curve.map((point, index) => {
    const x = left + (index / Math.max(curve.length - 1, 1)) * plotWidth;
    const y = top + ((paddedMax - point.equity_usdt) / paddedSpan) * plotHeight;
    return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
  }).join(" ");
  const baselineY = top + ((paddedMax - curve[0].equity_usdt) / paddedSpan) * plotHeight;
  const firstTimestamp = curve.find((point) => point.timestamp)?.timestamp;
  const lastTimestamp = curve.slice().reverse().find((point) => point.timestamp)?.timestamp;

  return (
    <div className="stage4-equity-curve">
      <svg aria-label="Portfolio shared account equity curve" preserveAspectRatio="none" viewBox={`0 0 ${width} ${height}`}>
        <line className="stage4-equity-curve__axis" x1={left} x2={left} y1={top} y2={height - bottom} />
        <line className="stage4-equity-curve__axis" x1={left} x2={width - right} y1={height - bottom} y2={height - bottom} />
        <line className="stage4-equity-curve__baseline" x1={left} x2={width - right} y1={baselineY} y2={baselineY} />
        <text className="stage4-equity-curve__axis-label" x={left - 10} y={top + 4} textAnchor="end">{formatUsd(max)}</text>
        <text className="stage4-equity-curve__axis-label" x={left - 10} y={height - bottom} textAnchor="end">{formatUsd(min)}</text>
        <text className="stage4-equity-curve__axis-label" x={left} y={height - 10} textAnchor="start">{formatCompactUtcTimestamp(firstTimestamp)}</text>
        <text className="stage4-equity-curve__axis-label" x={width - right} y={height - 10} textAnchor="end">{formatCompactUtcTimestamp(lastTimestamp)}</text>
        <path className="stage4-equity-curve__path" d={path} vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="stage4-equity-curve__scale">
        <span>Shared equity</span>
        <span>Replay order</span>
      </div>
    </div>
  );
}

function Stage4CandidateDetailPanel({ detail }: { detail: Stage4CandidateDetail }) {
  const candidate = detail.candidate ?? {};
  const setup = candidate.setup ?? {};
  const allTrades = detail.trades ?? [];
  const filledTrades = stage4FilledTrades(allTrades);
  const wins = stage4WinningTrades(filledTrades);
  const losses = stage4LosingTrades(filledTrades);
  const totalNetPnl = filledTrades.reduce((sum, trade) => sum + (trade.net_pnl_usdt ?? 0), 0);
  const totalFees = filledTrades.reduce((sum, trade) => sum + (trade.total_fees_usdt ?? 0), 0);
  const totalSkipped = (candidate.skipped_decisions ?? 0) + (candidate.skipped_position_open ?? 0);
  const expectancyPerTrade = filledTrades.length ? totalNetPnl / filledTrades.length : null;
  const maxDrawdownPct = stage4MaxDrawdownPct(filledTrades);

  return (
    <div className="stage4-detail-layout">
      <div className="stage4-detail-summary">
        <FieldRow label="Run" value={detail.run_id ?? "n/a"} />
        <FieldRow label="Policy" value={formatStage3Policy(setup)} />
        <FieldRow label="Protection" value={formatStage3Protection(setup)} />
        <FieldRow label="Pyramid" value={formatPyramidPolicy(setup)} />
        <FieldRow label="Initial Capital" value={formatUsd(candidate.account?.initial_capital_usdt)} />
        <FieldRow label="Ending Equity" value={formatUsd(candidate.account?.ending_equity_usdt)} />
        <FieldRow label="Net PnL" value={formatUsd(candidate.account?.net_pnl_usdt)} />
        <FieldRow label="Return" value={formatPct(candidate.account?.return_pct)} />
        <FieldRow label="Fees" value={formatUsd(totalFees)} />
        <FieldRow label="Filled Trades" value={formatNumber(filledTrades.length)} />
        <FieldRow label="Win Rate" value={formatPct(candidate.win_rate_pct)} />
        <FieldRow label="Expectancy / Trade" value={formatUsd(expectancyPerTrade)} />
        <FieldRow label="Max Drawdown" value={formatPct(maxDrawdownPct)} />
        <FieldRow label="Skipped Signals" value={formatNumber(totalSkipped)} />
      </div>

      <div className="stage4-detail-chart-card">
        <div className="stage4-detail-chart-card__header">
          <strong>Account Growth</strong>
          <span>{filledTrades.length ? `${formatNumber(filledTrades.length)} realized trades` : "No filled trades"}</span>
        </div>
        <Stage4EquityCurve trades={filledTrades} />
      </div>

      <div className="stage4-detail-note-grid">
        <FieldRow label="Wins / Losses" value={`${formatNumber(wins.length)} / ${formatNumber(losses.length)}`} />
        <FieldRow label="Latest close" value={formatUtcTimestamp(filledTrades[filledTrades.length - 1]?.exit_ts)} />
        <FieldRow label="Exit mode" value={formatStage4ExitMode(setup)} />
        <FieldRow label="Position-open skips" value={formatNumber(candidate.skipped_position_open)} />
      </div>

      <TerminalPanel className="scroll-panel stage4-detail-trades-panel" title="Filled Trades">
        <DataTable
          columns={[
            { key: "close", header: "Close", render: (item) => formatUtcTimestamp(item.exit_ts ?? item.signal_ts) },
            { key: "side", header: "Side", render: (item) => item.decision_direction ?? "-" },
            { key: "exit", header: "Exit", render: (item) => item.exit_status ?? "-" },
            { key: "entry", header: "Entry", align: "right", render: (item) => formatDecimal(item.entry_price, 4) },
            { key: "exit_px", header: "Exit Px", align: "right", render: (item) => formatDecimal(item.exit_price, 4) },
            { key: "margin", header: "Margin", align: "right", render: (item) => formatUsd(stage4TradeMarginUsed(item)) },
            { key: "notional", header: "Notional", align: "right", render: (item) => formatUsd(stage4TradeNotional(item)) },
            { key: "roe", header: "ROE", align: "right", render: (item) => formatPct(stage4TradeRoePct(item)) },
            { key: "pnl", header: "Net PnL", align: "right", render: (item) => formatUsd(item.net_pnl_usdt) },
            { key: "fees", header: "Fees", align: "right", render: (item) => formatUsd(item.total_fees_usdt) },
            { key: "equity", header: "Equity After", align: "right", render: (item) => formatUsd(item.equity_after) },
            { key: "legs", header: "Legs", align: "right", render: (item) => formatNumber(item.filled_legs) },
            { key: "protect", header: "Protected", render: (item) => item.protection_activated ? "yes" : "no" }
          ]}
          getRowKey={(item) => item.position_id ?? item.signal_id}
          getRowClassName={stage4TradeRowClassName}
          rows={filledTrades}
        />
      </TerminalPanel>
    </div>
  );
}

function Stage4EquityCurve({ trades }: { trades: Stage4TradeLedgerRow[] }) {
  if (!trades.length) {
    return <div className="state-line">No filled trades were recorded for this candidate.</div>;
  }
  const firstTrade = trades[0];
  const points = [
    {
      time: new Date(firstTrade.signal_ts ?? firstTrade.exit_ts ?? "").getTime(),
      equity: firstTrade.equity_before ?? firstTrade.equity_after ?? 0,
      index: 0,
    },
    ...trades.map((trade, index) => ({
      time: new Date(trade.exit_ts ?? trade.signal_ts ?? "").getTime(),
      equity: trade.equity_after ?? trade.equity_before ?? 0,
      index: index + 1,
    })),
  ].filter((point) => (
    typeof point.equity === "number"
    && !Number.isNaN(point.equity)
    && typeof point.time === "number"
    && !Number.isNaN(point.time)
  )).sort((left, right) => left.time - right.time || left.index - right.index);
  if (points.length < 2) {
    return <div className="state-line">Not enough equity points to render the chart.</div>;
  }
  const equities = points.map((point) => point.equity);
  const times = points.map((point) => point.time);
  const min = Math.min(...equities);
  const max = Math.max(...equities);
  const minTime = Math.min(...times);
  const maxTime = Math.max(...times);
  const span = max - min || 1;
  const paddedMin = min - span * 0.06;
  const paddedMax = max + span * 0.06;
  const paddedSpan = paddedMax - paddedMin || 1;
  const timeSpan = maxTime - minTime;
  const width = 1000;
  const height = 300;
  const left = 78;
  const right = 18;
  const top = 18;
  const bottom = 36;
  const plotWidth = width - left - right;
  const plotHeight = height - top - bottom;
  const path = points
    .map((point, index) => {
      const xRatio = timeSpan > 0 ? (point.time - minTime) / timeSpan : index / Math.max(points.length - 1, 1);
      const x = left + (xRatio * plotWidth);
      const y = top + ((paddedMax - point.equity) / paddedSpan) * plotHeight;
      return `${index === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)}`;
    })
    .join(" ");
  const initialEquity = points[0].equity;
  const baselineY = top + ((paddedMax - initialEquity) / paddedSpan) * plotHeight;

  return (
    <div className="stage4-equity-curve">
      <svg aria-label="Stage 4 account growth curve" preserveAspectRatio="none" viewBox={`0 0 ${width} ${height}`}>
        <line className="stage4-equity-curve__axis" x1={left} x2={left} y1={top} y2={height - bottom} />
        <line className="stage4-equity-curve__axis" x1={left} x2={width - right} y1={height - bottom} y2={height - bottom} />
        <line className="stage4-equity-curve__baseline" x1={left} x2={width - right} y1={baselineY} y2={baselineY} />
        <text className="stage4-equity-curve__axis-label" x={left - 10} y={top + 4} textAnchor="end">{formatUsd(max)}</text>
        <text className="stage4-equity-curve__axis-label" x={left - 10} y={height - bottom} textAnchor="end">{formatUsd(min)}</text>
        <text className="stage4-equity-curve__axis-label" x={left} y={height - 10} textAnchor="start">{formatCompactUtcTimestamp(minTime)}</text>
        <text className="stage4-equity-curve__axis-label" x={width - right} y={height - 10} textAnchor="end">{formatCompactUtcTimestamp(maxTime)}</text>
        <path className="stage4-equity-curve__path" d={path} vectorEffect="non-scaling-stroke" />
      </svg>
      <div className="stage4-equity-curve__scale">
        <span>Account size</span>
        <span>Time</span>
      </div>
    </div>
  );
}

function Stage4ExitPolicyPanel({ setup }: { setup: Stage4CandidateResult["setup"] }) {
  const sides: ExitSide[] = ["LONG", "SHORT"];
  return (
    <div className="stage4-policy-split">
      <div className="stage4-policy-split__header">
        <strong>Selected Exit Policy</strong>
        <span>{setup?.policy_mode === "side_specific" ? "Side-specific Stage 4 setup" : "Shared Stage 4 setup"}</span>
      </div>
      {sides.map((side) => (
        <div className="stage4-policy-split__row" key={side}>
          <span className={`stage4-policy-split__side stage4-policy-split__side--${side.toLowerCase()}`}>{side}</span>
          <strong>{formatStage3SidePolicy(setup, side)}</strong>
          <em>{formatStage3SideProtection(setup, side)}</em>
        </div>
      ))}
    </div>
  );
}
