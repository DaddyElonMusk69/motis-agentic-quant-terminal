import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Play, Plus, RefreshCw, Trash2, X } from "lucide-react";
import {
  appendStage0UniverseAssets,
  createStage0UniverseRun,
  deleteStage0UniverseRun,
  executeStage0CandidateBatch,
  fetchDevelopmentQueue,
  fetchJob,
  fetchJobs,
  fetchSignalEngines,
  fetchSignalSets,
  fetchStage0UniverseAppendableAssets,
  fetchStage0UniverseCandidates,
  fetchStage0UniverseRuns,
  isJobResponse,
  type DevelopmentQueueRow,
  type Stage0UniverseCandidate,
  type Stage0UniverseRun
} from "../app/api";
import { formatNumber, formatTimestamp } from "../app/format";
import { queryClient } from "../app/queryClient";
import { useAppRouter } from "../app/router";
import { DataTable } from "../components/DataTable";
import { FieldRow } from "../components/FieldRow";
import { SplitPane } from "../components/SplitPane";
import { StatusBadge } from "../components/StatusBadge";
import { TerminalPanel } from "../components/TerminalPanel";
import { WorkerRuntimeNotice } from "../components/WorkerRuntimeNotice";

type TrainingPoolProgress = {
  total: number;
  accepted: number;
  watchlist: number;
  pending: number;
  failed: number;
  scored: number;
  percent: number;
};

function updateTrainingPoolUrl(next: { pool?: string; candidate?: string }) {
  const params = new URLSearchParams(window.location.search);
  if (next.pool !== undefined) {
    params.set("pool", next.pool);
  }
  if (next.candidate !== undefined) {
    params.set("candidate", next.candidate);
  }
  const query = params.toString();
  const nextUrl = `/research/stage0${query ? `?${query}` : ""}`;
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function selectTrainingPool(runs: Stage0UniverseRun[] | undefined, searchParams: URLSearchParams): Stage0UniverseRun | undefined {
  const requested = searchParams.get("pool");
  return runs?.find((run) => run.universe_run_id === requested) ?? runs?.[0];
}

function selectCandidate(rows: DevelopmentQueueRow[], searchParams: URLSearchParams): DevelopmentQueueRow | undefined {
  const requested = searchParams.get("candidate");
  return rows.find((row) => row.candidate_id === requested) ?? rows[0];
}

function poolProgress(run: Stage0UniverseRun | undefined, rows: DevelopmentQueueRow[]): TrainingPoolProgress {
  const total = rows.length || run?.summary.total_candidates || 0;
  const accepted = rows.length ? rows.filter((row) => row.stage0_status === "accepted").length : run?.summary.accepted ?? 0;
  const watchlist = rows.length ? rows.filter((row) => row.stage0_status === "watchlist").length : run?.summary.watchlist ?? 0;
  const pending = rows.length ? rows.filter((row) => row.stage0_status === "pending_stage0").length : run?.summary.pending_stage0 ?? 0;
  const failed = run?.summary.failed ?? rows.filter((row) => row.stage0_status === "failed").length;
  const scored = Math.max(0, total - pending);
  const percent = total > 0 ? Math.round((scored / total) * 100) : 0;
  return { total, accepted, watchlist, pending, failed, scored, percent };
}

function evaluatedSignalCount(candidate: Stage0UniverseCandidate | undefined, row: DevelopmentQueueRow): number | null {
  if (row.stage0_evaluated_signal_count !== undefined && row.stage0_evaluated_signal_count !== null) {
    return row.stage0_evaluated_signal_count;
  }
  const metrics = candidate?.metrics ?? {};
  if (typeof metrics.total_records === "number") {
    return metrics.total_records;
  }
  const statusCounts = typeof metrics.status_counts === "object" && metrics.status_counts ? metrics.status_counts as Record<string, unknown> : {};
  if (typeof statusCounts.triggered === "number" && typeof statusCounts.no_trigger === "number") {
    return statusCounts.triggered + statusCounts.no_trigger;
  }
  return candidate?.packet_count ?? row.packet_count ?? null;
}

function significanceThresholdLabel(candidate: Stage0UniverseCandidate | undefined): string {
  const value = candidate?.metrics?.significance_threshold_pct;
  if (typeof value !== "number" || Number.isNaN(value)) {
    return "n/a";
  }
  return `${value}%`;
}

function statusTone(status: string): "pass" | "warn" | "idle" | "risk" {
  if (status === "accepted" || status === "completed") {
    return "pass";
  }
  if (status === "watchlist" || status === "pending_stage0" || status === "running") {
    return "warn";
  }
  if (status === "failed") {
    return "risk";
  }
  return "idle";
}

function shortPoolId(value: string): string {
  return value.replace("stage0-universe-", "").replace("training-pool-", "");
}

function poolDisplayName(run: Stage0UniverseRun): string {
  const name = run.name?.trim();
  return name || shortPoolId(run.universe_run_id);
}

function dateOnly(value: string | null | undefined): string {
  if (!value) {
    return "n/a";
  }
  return value.slice(0, 10);
}

function splitWindowLine(run: Stage0UniverseRun | undefined): string {
  if (!run) {
    return "No training pool selected.";
  }
  return `Train ${dateOnly(run.train_start ?? run.window_start)} - ${dateOnly(run.train_end)} · Walk-forward ${dateOnly(run.walk_forward_start)} - ${dateOnly(run.walk_forward_end ?? run.window_end)}`;
}

function useAvailableAssets(engineId: string | null) {
  const signalSetsQuery = useQuery({
    enabled: Boolean(engineId),
    queryKey: ["signal-sets", engineId],
    queryFn: () => fetchSignalSets(engineId as string)
  });
  const assets = useMemo(() => {
    const set = new Set((signalSetsQuery.data?.signal_sets ?? []).map((signalSet) => signalSet.asset));
    return Array.from(set).sort();
  }, [signalSetsQuery.data?.signal_sets]);
  return { assets, loading: signalSetsQuery.isLoading };
}

export function ResearchStage0Page() {
  const { searchParams, navigate } = useAppRouter();
  const [poolName, setPoolName] = useState("");
  const [selectedEngineId, setSelectedEngineId] = useState<string>("");
  const [tickerInput, setTickerInput] = useState("");
  const [selectedTickers, setSelectedTickers] = useState<string[]>(["BTC", "ETH", "AAVE", "SOL", "WIF"]);
  const [trainStart, setTrainStart] = useState("2026-03-01");
  const [trainEnd, setTrainEnd] = useState("2026-04-30");
  const [walkForwardStart, setWalkForwardStart] = useState("2026-05-01");
  const [walkForwardEnd, setWalkForwardEnd] = useState("2026-05-30");
  const [forwardHours, setForwardHours] = useState(36);
  const [triggerRateThresholdPct, setTriggerRateThresholdPct] = useState(85);
  const [autoRunPoolId, setAutoRunPoolId] = useState<string | null>(null);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [appendModalOpen, setAppendModalOpen] = useState(false);
  const [appendSelectedTickers, setAppendSelectedTickers] = useState<string[]>([]);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);

  const enginesQuery = useQuery({ queryKey: ["signal-engines"], queryFn: fetchSignalEngines });
  const effectiveEngineId = selectedEngineId || enginesQuery.data?.engines[0]?.signal_engine_id || "";
  const { assets: assetOptions } = useAvailableAssets(effectiveEngineId || null);
  const runsQuery = useQuery({ queryKey: ["stage0-universe-runs"], queryFn: fetchStage0UniverseRuns });
  const selectedRun = selectTrainingPool(runsQuery.data?.runs, searchParams);
  const queueQuery = useQuery({
    enabled: Boolean(selectedRun?.universe_run_id),
    queryKey: ["development-queue", selectedRun?.universe_run_id],
    queryFn: () => fetchDevelopmentQueue(selectedRun!.universe_run_id)
  });
  const candidatesQuery = useQuery({
    enabled: Boolean(selectedRun?.universe_run_id),
    queryKey: ["stage0-universe-candidates", selectedRun?.universe_run_id],
    queryFn: () => fetchStage0UniverseCandidates(selectedRun!.universe_run_id)
  });
  const appendableAssetsQuery = useQuery({
    enabled: appendModalOpen && Boolean(selectedRun?.universe_run_id),
    queryKey: ["stage0-universe-appendable-assets", selectedRun?.universe_run_id],
    queryFn: () => fetchStage0UniverseAppendableAssets(selectedRun!.universe_run_id)
  });
  const activeJobQuery = useQuery({
    enabled: Boolean(activeJobId),
    queryKey: ["runtime-job", activeJobId],
    queryFn: () => fetchJob(activeJobId!),
    refetchInterval: (query) => {
      const job = query.state.data?.job;
      return !job || ["queued", "running"].includes(job.status) ? 1500 : false;
    }
  });
  const activeScopeKey = selectedRun?.universe_run_id ? `stage0:${selectedRun.universe_run_id}` : null;
  const latestScopeJobsQuery = useQuery({
    enabled: Boolean(activeScopeKey) && !activeJobId,
    queryKey: ["runtime-jobs", activeScopeKey],
    queryFn: () => fetchJobs(activeScopeKey!, 10)
  });

  const queueRows = queueQuery.data?.queue ?? [];
  const selectedRow = selectCandidate(queueRows, searchParams);
  const candidateById = useMemo(
    () => new Map((candidatesQuery.data?.candidates ?? []).map((candidate) => [candidate.candidate_id, candidate])),
    [candidatesQuery.data?.candidates]
  );
  const selectedCandidate = selectedRow ? candidateById.get(selectedRow.candidate_id) : undefined;
  const progress = poolProgress(selectedRun, queueRows);

  const createPoolMutation = useMutation({
    mutationFn: createStage0UniverseRun,
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue", result.run.universe_run_id] });
      updateTrainingPoolUrl({ pool: result.run.universe_run_id });
      setAutoRunPoolId(result.run.universe_run_id);
      setCreateModalOpen(false);
      setPoolName("");
    }
  });

  const executePoolMutation = useMutation({
    mutationFn: executeStage0CandidateBatch,
    onSuccess: (result) => {
      if (isJobResponse(result)) {
        setActiveJobId(result.job.job_id);
        return;
      }
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue", result.run.universe_run_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-candidates", result.run.universe_run_id] });
    }
  });

  const deletePoolMutation = useMutation({
    mutationFn: deleteStage0UniverseRun,
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue", result.universe_run_id] });
      window.history.pushState(null, "", "/research/stage0");
      window.dispatchEvent(new PopStateEvent("popstate"));
    }
  });

  const appendAssetsMutation = useMutation({
    mutationFn: appendStage0UniverseAssets,
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
      void queryClient.invalidateQueries({ queryKey: ["development-queue", result.run.universe_run_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-candidates", result.run.universe_run_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-appendable-assets", result.run.universe_run_id] });
      setAppendSelectedTickers([]);
      setAppendModalOpen(false);
    }
  });

  useEffect(() => {
    if (!selectedEngineId && enginesQuery.data?.engines[0]?.signal_engine_id) {
      setSelectedEngineId(enginesQuery.data.engines[0].signal_engine_id);
    }
  }, [enginesQuery.data?.engines, selectedEngineId]);

  useEffect(() => {
    if (activeJobId) {
      return;
    }
    const job = latestScopeJobsQuery.data?.jobs.find((item) => ["queued", "running"].includes(item.status));
    if (job) {
      setActiveJobId(job.job_id);
    }
  }, [activeJobId, latestScopeJobsQuery.data?.jobs]);

  useEffect(() => {
    const job = activeJobQuery.data?.job;
    if (!job || ["queued", "running"].includes(job.status)) {
      return;
    }
    void queryClient.invalidateQueries({ queryKey: ["stage0-universe-runs"] });
    if (selectedRun?.universe_run_id) {
      void queryClient.invalidateQueries({ queryKey: ["development-queue", selectedRun.universe_run_id] });
      void queryClient.invalidateQueries({ queryKey: ["stage0-universe-candidates", selectedRun.universe_run_id] });
    }
    const timeout = window.setTimeout(() => setActiveJobId(null), job.status === "completed" ? 2500 : 5000);
    return () => window.clearTimeout(timeout);
  }, [activeJobQuery.data?.job?.status, selectedRun?.universe_run_id]);

  useEffect(() => {
    setAppendSelectedTickers([]);
  }, [appendModalOpen, selectedRun?.universe_run_id]);

  useEffect(() => {
    if (
      autoRunPoolId
      && selectedRun?.universe_run_id === autoRunPoolId
      && !queueQuery.isLoading
      && progress.pending > 0
      && !executePoolMutation.isPending
    ) {
      setAutoRunPoolId(null);
      executePoolMutation.mutate({ universe_run_id: autoRunPoolId, limit: progress.pending });
    }
  }, [autoRunPoolId, executePoolMutation, progress.pending, queueQuery.isLoading, selectedRun?.universe_run_id]);

  const addTicker = () => {
    const symbol = tickerInput.trim().toUpperCase();
    if (!symbol || selectedTickers.includes(symbol)) {
      setTickerInput("");
      return;
    }
    setSelectedTickers([...selectedTickers, symbol]);
    setTickerInput("");
  };

  const canCreate = Boolean(effectiveEngineId) && selectedTickers.length > 0 && !createPoolMutation.isPending;
  const activeJob = activeJobQuery.data?.job ?? null;
  const activeJobRunning = Boolean(activeJob && ["queued", "running"].includes(activeJob.status));
  const isScoring = executePoolMutation.isPending || activeJobRunning;
  const appendableAssets = appendableAssetsQuery.data?.assets ?? [];
  const appendSelectedSet = useMemo(() => new Set(appendSelectedTickers), [appendSelectedTickers]);
  const allAppendableSelected = appendableAssets.length > 0 && appendableAssets.every((asset) => appendSelectedSet.has(asset));

  const toggleAppendTicker = (asset: string) => {
    setAppendSelectedTickers((current) => current.includes(asset) ? current.filter((item) => item !== asset) : [...current, asset]);
  };

  const toggleAllAppendableTickers = () => {
    setAppendSelectedTickers(allAppendableSelected ? [] : appendableAssets);
  };

  return (
    <div className="page page--workspace">
      <SplitPane
        className="split-pane--wide-list"
        workbenchClassName="training-pool-workbench"
        left={
          <>
            <div className="list-header">
              <span>Training Pools</span>
              <button
                className="icon-button"
                onClick={() => setCreateModalOpen(true)}
                type="button"
                aria-label="Create training pool"
              >
                <Plus aria-hidden="true" />
              </button>
            </div>
            {runsQuery.isLoading ? <div className="state-line">Loading training pools...</div> : null}
            {runsQuery.error ? <div className="state-line state-line--error">{runsQuery.error.message}</div> : null}
            {deletePoolMutation.error ? <div className="state-line state-line--error">{deletePoolMutation.error.message}</div> : null}
            {runsQuery.data?.runs.length === 0 ? <div className="state-line">No training pools yet.</div> : null}
            {runsQuery.data?.runs.map((run) => (
              <div
                className={run.universe_run_id === selectedRun?.universe_run_id ? "training-pool-card is-selected" : "training-pool-card"}
                key={run.universe_run_id}
              >
                <button className="training-pool-card__main" onClick={() => updateTrainingPoolUrl({ pool: run.universe_run_id })} type="button">
                  <div className="signal-pool-card__top">
                    <strong>{poolDisplayName(run)}</strong>
                    <StatusBadge tone={statusTone(run.status)}>{run.status}</StatusBadge>
                  </div>
                  {run.name ? <small className="mono">{shortPoolId(run.universe_run_id)}</small> : null}
                  <span>{splitWindowLine(run)}</span>
                  <small>{run.engine_filter.join(", ") || "all engines"} · {formatNumber(run.summary.total_candidates ?? 0)} candidates</small>
                  <small>{formatNumber(run.summary.accepted ?? 0)} accepted · {formatNumber(run.summary.pending_stage0 ?? 0)} pending</small>
                </button>
                <button
                  className="icon-button training-pool-card__delete"
                  disabled={deletePoolMutation.isPending}
                  onClick={() => {
                    if (window.confirm(`Delete training pool ${poolDisplayName(run)}? Linked development sessions for this pool will be deleted too.`)) {
                      deletePoolMutation.mutate(run.universe_run_id);
                    }
                  }}
                  type="button"
                  aria-label={`Delete training pool ${poolDisplayName(run)}`}
                >
                  <Trash2 aria-hidden="true" />
                </button>
              </div>
            ))}
          </>
        }
        leftLabel="Training pool list"
        right={
          <>
            <div className="workbench-header">
              <div>
                <span className="eyebrow">Training pool gate</span>
                <h1>{selectedRun ? poolDisplayName(selectedRun) : "Training Pools"}</h1>
              </div>
              <div className="header-actions">
                <StatusBadge tone={progress.pending > 0 ? "warn" : "pass"}>{progress.pending > 0 ? "Pending" : "Complete"}</StatusBadge>
                <button
                  className="button button--secondary"
                  disabled={!selectedRun}
                  onClick={() => setAppendModalOpen(true)}
                  type="button"
                >
                  <Plus aria-hidden="true" />
                  Add Tickers
                </button>
                <button
                  className="button button--secondary"
                  disabled={!selectedRun || progress.pending <= 0 || isScoring}
                  onClick={() => selectedRun && executePoolMutation.mutate({ universe_run_id: selectedRun.universe_run_id, limit: progress.pending })}
                  type="button"
                >
                  <Play aria-hidden="true" />
                  {isScoring ? "Scoring" : "Score Pending"}
                </button>
                <button className="button button--secondary" disabled={runsQuery.isFetching} onClick={() => void runsQuery.refetch()} type="button">
                  <RefreshCw aria-hidden="true" />
                  Refresh
                </button>
              </div>
            </div>

            {isScoring ? (
              <div className="progress-card">
                <div className="progress-card__header">
                  <strong>Scoring training pool candidates</strong>
                  <span>{activeJob ? `${activeJob.status} · ${activeJob.current_step ?? "waiting"}` : "Reading signal packets, scoring travel, classifying accepted/watchlist/failed"}</span>
                </div>
                <div className="progress-rail" aria-label="Training pool scoring in progress">
                  <span />
                </div>
                <div className="progress-steps">
                  <span>Load candidate signals</span>
                  <span>Evaluate forward travel</span>
                  <span>Persist pool evidence</span>
                </div>
                <WorkerRuntimeNotice active={isScoring} job={activeJob} />
              </div>
            ) : null}

            <div className="workbench-grid">
              <TerminalPanel title="Pool Progress">
                <div className="progress-readout">
                  <div className="progress-card__header">
                    <strong>{formatNumber(progress.scored)} / {formatNumber(progress.total)} scored</strong>
                    <span>{progress.percent}%</span>
                  </div>
                  <div className="progress-rail progress-rail--static" aria-label="Training pool scoring progress">
                    <span style={{ width: `${progress.percent}%` }} />
                  </div>
                </div>
                <div className="field-grid">
                  <FieldRow label="Accepted" value={formatNumber(progress.accepted)} />
                  <FieldRow label="Watchlist" value={formatNumber(progress.watchlist)} />
                  <FieldRow label="Pending" value={formatNumber(progress.pending)} />
                  <FieldRow label="Failed" value={formatNumber(progress.failed)} />
                </div>
              </TerminalPanel>
              <TerminalPanel title="Windows">
                <div className="field-stack">
                  <FieldRow label="Pool name" value={selectedRun ? poolDisplayName(selectedRun) : "n/a"} />
                  <FieldRow label="Training" value={`${dateOnly(selectedRun?.train_start ?? selectedRun?.window_start)} - ${dateOnly(selectedRun?.train_end)}`} />
                  <FieldRow label="Walk-forward" value={`${dateOnly(selectedRun?.walk_forward_start)} - ${dateOnly(selectedRun?.walk_forward_end ?? selectedRun?.window_end)}`} />
                  <FieldRow label="Forward hours" value={selectedRun ? `${selectedRun.forward_hours}h` : "n/a"} />
                  <FieldRow label="Trigger threshold" value={selectedRun ? `${selectedRun.trigger_rate_threshold_pct}%` : "n/a"} />
                </div>
              </TerminalPanel>
            </div>

            <div className="workbench-grid workbench-grid--wide-left training-pool-candidate-grid">
              <TerminalPanel className="candidate-list-panel" title="Pool Candidates">
                {queueQuery.error ? <div className="state-line state-line--error">{queueQuery.error.message}</div> : null}
                {candidatesQuery.error ? <div className="state-line state-line--error">{candidatesQuery.error.message}</div> : null}
                <DataTable
                  columns={[
                    { key: "asset", header: "Asset", render: (row) => <strong>{row.asset}</strong> },
                    { key: "engine", header: "Engine", render: (row) => row.signal_engine_id },
                    { key: "evaluated", header: "Evaluated", align: "right", render: (row) => formatNumber(evaluatedSignalCount(candidateById.get(row.candidate_id), row)) },
                    { key: "trigger", header: "Trigger", align: "right", render: (row) => row.trigger_rate_pct === null ? "pending" : `${row.trigger_rate_pct}%` },
                    { key: "branch", header: "Branch", render: (row) => row.branch_path },
                    { key: "state", header: "Pool Gate", align: "right", render: (row) => <StatusBadge tone={statusTone(row.stage0_status)}>{row.stage0_status}</StatusBadge> }
                  ]}
                  getRowClassName={(row) => row.candidate_id === selectedRow?.candidate_id ? "is-selected" : undefined}
                  getRowKey={(row) => row.candidate_id}
                  onRowClick={(row) => updateTrainingPoolUrl({ pool: row.universe_run_id, candidate: row.candidate_id })}
                  rows={queueRows}
                />
              </TerminalPanel>

              <TerminalPanel eyebrow={selectedRow?.asset ?? "candidate"} title="Candidate Evidence">
                {selectedRow ? (
                  <div className="field-stack">
                    <FieldRow label="Asset" value={selectedRow.asset} />
                    <FieldRow label="Signal engine" value={selectedRow.signal_engine_id} />
                    <FieldRow label="Evaluated signals" value={formatNumber(evaluatedSignalCount(selectedCandidate, selectedRow))} />
                    <FieldRow label="Trigger rate" value={selectedRow.trigger_rate_pct === null ? "pending" : `${selectedRow.trigger_rate_pct}%`} />
                    <FieldRow label="Significant travel threshold" value={significanceThresholdLabel(selectedCandidate)} />
                    <FieldRow label="Source packets" value={formatNumber(selectedCandidate?.packet_count ?? selectedRow.packet_count)} />
                    <FieldRow label="Development" value={selectedRow.development_status.replaceAll("_", " ")} />
                    <FieldRow label="Next action" value={selectedRow.next_action.label} />
                  </div>
                ) : (
                  <div className="state-line">Select a candidate to inspect evidence.</div>
                )}
                {selectedRow?.stage0_status === "accepted" ? (
                  <button
                    className="button button--primary full-width-action"
                    onClick={() => navigate("/research/development", `?pool=${selectedRow.universe_run_id}&candidate=${selectedRow.candidate_id}`)}
                    type="button"
                  >
                    Open Development
                  </button>
                ) : null}
              </TerminalPanel>
            </div>

            {executePoolMutation.data && !isJobResponse(executePoolMutation.data) && executePoolMutation.data.summary ? (
              <div className="state-line">
                Last scoring: {formatNumber(executePoolMutation.data.summary.succeeded)} succeeded · {formatNumber(executePoolMutation.data.summary.failed)} failed · {formatNumber(executePoolMutation.data.summary.remaining_pending)} remaining
              </div>
            ) : null}
          </>
        }
      />
      {createModalOpen ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal" role="dialog" aria-modal="true" aria-labelledby="create-training-pool-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">New training pool</span>
                <h2 id="create-training-pool-title">Create Training Pool</h2>
              </div>
              <button className="icon-button" onClick={() => setCreateModalOpen(false)} type="button" aria-label="Close create training pool">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <div className="form-grid form-grid--dense">
                <label>
                  Pool Name
                  <input
                    value={poolName}
                    placeholder="March-May AAVE Vegas pool"
                    onChange={(event) => setPoolName(event.target.value)}
                  />
                </label>
                <label>
                  Engine
                  <select value={effectiveEngineId} onChange={(event) => setSelectedEngineId(event.target.value)}>
                    {(enginesQuery.data?.engines ?? []).map((engine) => <option value={engine.signal_engine_id} key={engine.signal_engine_id}>{engine.name}</option>)}
                  </select>
                </label>
                <label>
                  Add Ticker
                  <div className="inline-input">
                    <input
                      list="stage0-assets"
                      value={tickerInput}
                      placeholder="AAVE"
                      onChange={(event) => setTickerInput(event.target.value)}
                      onKeyDown={(event) => {
                        if (event.key === "Enter") {
                          event.preventDefault();
                          addTicker();
                        }
                      }}
                    />
                    <button className="button button--secondary" onClick={addTicker} type="button">Add</button>
                  </div>
                  <datalist id="stage0-assets">
                    {assetOptions.map((asset) => <option value={asset} key={asset} />)}
                  </datalist>
                </label>
                <label>
                  Train Start
                  <input type="date" value={trainStart} onChange={(event) => setTrainStart(event.target.value)} />
                </label>
                <label>
                  Train End
                  <input type="date" value={trainEnd} onChange={(event) => setTrainEnd(event.target.value)} />
                </label>
                <label>
                  Walk-forward Start
                  <input type="date" value={walkForwardStart} onChange={(event) => setWalkForwardStart(event.target.value)} />
                </label>
                <label>
                  Walk-forward End
                  <input type="date" value={walkForwardEnd} onChange={(event) => setWalkForwardEnd(event.target.value)} />
                </label>
                <label>
                  Forward Hours
                  <input type="number" min={1} value={forwardHours} onChange={(event) => setForwardHours(Number(event.target.value))} />
                </label>
                <label>
                  Trigger Threshold %
                  <input type="number" min={0} max={100} value={triggerRateThresholdPct} onChange={(event) => setTriggerRateThresholdPct(Number(event.target.value))} />
                </label>
              </div>
              <div className="ticker-chip-row">
                {selectedTickers.map((ticker) => (
                  <button className="ticker-chip" key={ticker} onClick={() => setSelectedTickers(selectedTickers.filter((item) => item !== ticker))} type="button">
                    {ticker} x
                  </button>
                ))}
              </div>
              {createPoolMutation.error ? <div className="state-line state-line--error">{createPoolMutation.error.message}</div> : null}
            </div>
            <footer className="terminal-modal__footer">
              <span>{formatNumber(selectedTickers.length)} selected tickers</span>
              <div className="header-actions">
                <button className="button button--secondary" onClick={() => setCreateModalOpen(false)} type="button">Cancel</button>
                <button
                  className="button button--primary"
                  disabled={!canCreate}
                  onClick={() => createPoolMutation.mutate({
                    name: poolName.trim() || undefined,
                    train_start: trainStart,
                    train_end: trainEnd,
                    walk_forward_start: walkForwardStart,
                    walk_forward_end: walkForwardEnd,
                    forward_hours: forwardHours,
                    trigger_rate_threshold_pct: triggerRateThresholdPct,
                    engine_ids: effectiveEngineId ? [effectiveEngineId] : [],
                    assets: selectedTickers
                  })}
                  type="button"
                >
                  <Play aria-hidden="true" />
                  {createPoolMutation.isPending ? "Creating" : "Create and Score Pool"}
                </button>
              </div>
            </footer>
          </section>
        </div>
      ) : null}
      {appendModalOpen && selectedRun ? (
        <div className="terminal-modal-backdrop">
          <section className="terminal-modal add-ticker-modal" role="dialog" aria-modal="true" aria-labelledby="append-training-pool-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">{poolDisplayName(selectedRun)}</span>
                <h2 id="append-training-pool-title">Add Tickers</h2>
              </div>
              <button className="icon-button" onClick={() => setAppendModalOpen(false)} type="button" aria-label="Close add tickers dialog">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="add-ticker-toolbar">
              <div>
                <strong>{appendSelectedTickers.length} selected</strong>
                <span>{appendableAssets.length} tickers already aligned to this engine and signal window</span>
              </div>
              <div className="header-actions">
                <button
                  className="button button--secondary"
                  disabled={appendableAssets.length === 0 || appendAssetsMutation.isPending}
                  onClick={toggleAllAppendableTickers}
                  type="button"
                >
                  {allAppendableSelected ? "Clear" : "Select All Ready"}
                </button>
                <button
                  className="button button--primary"
                  disabled={appendSelectedTickers.length === 0 || appendAssetsMutation.isPending}
                  onClick={() => appendAssetsMutation.mutate({
                    universe_run_id: selectedRun.universe_run_id,
                    assets: appendSelectedTickers
                  })}
                  type="button"
                >
                  {appendAssetsMutation.isPending ? "Appending" : `Append ${appendSelectedTickers.length || ""}`.trim()}
                </button>
              </div>
            </div>
            <div className="add-ticker-list">
              {appendableAssetsQuery.isLoading ? <div className="state-line">Loading eligible tickers...</div> : null}
              {appendableAssetsQuery.error ? <div className="state-line state-line--error">{appendableAssetsQuery.error.message}</div> : null}
              {appendableAssets.map((asset) => {
                const selected = appendSelectedSet.has(asset);
                return (
                  <button
                    className={["ticker-option", selected ? "is-selected" : ""].filter(Boolean).join(" ")}
                    disabled={appendAssetsMutation.isPending}
                    key={asset}
                    onClick={() => toggleAppendTicker(asset)}
                    type="button"
                  >
                    <div>
                      <strong>{asset}</strong>
                      <span>Generated signal set aligned to this pool</span>
                    </div>
                    <StatusBadge tone="pass">{selected ? "Selected" : "Ready"}</StatusBadge>
                  </button>
                );
              })}
              {!appendableAssetsQuery.isLoading && !appendableAssetsQuery.error && appendableAssets.length === 0 ? (
                <div className="state-line">No additional aligned tickers are available for this pool.</div>
              ) : null}
              {appendAssetsMutation.error ? <div className="state-line state-line--error">{appendAssetsMutation.error.message}</div> : null}
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
