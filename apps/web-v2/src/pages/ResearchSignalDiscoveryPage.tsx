import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  ArrowRight,
  Clipboard,
  FileCode2,
  FlaskConical,
  Link2,
  Play,
  Plus,
  RefreshCw,
  Snowflake,
  Trash2,
  X
} from "lucide-react";
import {
  attachSignalDiscoveryCandidate,
  createSignalDiscoverySession,
  deleteSignalDiscoverySession,
  evaluateSignalDiscoveryCandidate,
  fetchJob,
  fetchMarketDataCatalog,
  fetchSignalDiscoverySession,
  fetchSignalDiscoverySessions,
  fetchSignalEngines,
  fetchSignalSets,
  freezeSignalDiscoveryTarget,
  generateSignalDiscoveryPrompt,
  handoffSignalDiscoveryCandidate,
  runSignalDiscoveryAtlas,
  runSignalDiscoveryWalkForward,
  type SignalDiscoveryPrompt,
  type SignalDiscoveryRResult,
  type SignalDiscoverySession
} from "../app/api";
import { formatNumber, formatTimestamp } from "../app/format";
import { queryClient } from "../app/queryClient";
import { useAppRouter } from "../app/router";
import { buildDiscoveryTickers, buildRiskGrid, formatRiskGrid } from "../app/signalDiscovery";
import { DataTable } from "../components/DataTable";
import { FieldRow } from "../components/FieldRow";
import { ListSkeleton } from "../components/ListSkeleton";
import { SplitPane } from "../components/SplitPane";
import { StatusBadge } from "../components/StatusBadge";
import { WorkerRuntimeNotice } from "../components/WorkerRuntimeNotice";

type CreateState = {
  name: string;
  tickerKey: string;
  researchStart: string;
  researchEnd: string;
  walkForwardStart: string;
  walkForwardEnd: string;
  riskMinimum: number;
  riskMaximum: number;
  maxHoldHours: number;
  entryDelays: string;
  feeBps: number;
  slippageBps: number;
};

const INITIAL_CREATE_STATE: CreateState = {
  name: "",
  tickerKey: "",
  researchStart: "2025-03-01",
  researchEnd: "2026-03-31",
  walkForwardStart: "2026-04-01",
  walkForwardEnd: "2026-05-30",
  riskMinimum: 0.6,
  riskMaximum: 1.4,
  maxHoldHours: 48,
  entryDelays: "5, 10",
  feeBps: 5,
  slippageBps: 5
};

const RUNNING_STATUSES = new Set([
  "atlas_running",
  "walk_forward_running",
  "evaluation_running",
  "handoff_running"
]);

function statusTone(status: string): "pass" | "warn" | "risk" | "info" | "idle" {
  if (["atlas_ready", "walk_forward_ready", "evaluated", "accepted", "handed_off"].includes(status)) {
    return "pass";
  }
  if (RUNNING_STATUSES.has(status) || ["target_frozen", "candidate_attached"].includes(status)) {
    return "warn";
  }
  if (status === "failed") {
    return "risk";
  }
  return status === "draft" ? "idle" : "info";
}

function updateSessionUrl(sessionId?: string) {
  const next = sessionId ? `/research/discovery?session=${encodeURIComponent(sessionId)}` : "/research/discovery";
  if (`${window.location.pathname}${window.location.search}` === next) {
    return;
  }
  window.history.pushState(null, "", next);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function parseNumberList(value: string): number[] {
  return Array.from(
    new Set(
      value
        .split(",")
        .map((part) => Number(part.trim()))
        .filter((number) => Number.isFinite(number) && number >= 0)
    )
  ).sort((left, right) => left - right);
}

function dateStart(value: string): string {
  return `${value}T00:00:00Z`;
}

function dateEnd(value: string): string {
  return `${value}T23:55:00Z`;
}

function dateOnly(value: string | undefined): string {
  return value ? value.slice(0, 10) : "n/a";
}

function pct(value: number | undefined): string {
  return value === undefined ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function decimal(value: number | undefined, digits = 3): string {
  return value === undefined ? "n/a" : value.toFixed(digits);
}

function selectSession(sessions: SignalDiscoverySession[] | undefined, requested: string | null) {
  return sessions?.find((session) => session.session_id === requested) ?? sessions?.[0];
}

function primaryEpisodes(row: SignalDiscoveryRResult): number {
  return row.primary?.episode_count ?? 0;
}

function directionSummary(row: SignalDiscoveryRResult, direction: "LONG" | "SHORT"): string {
  const count = row.primary?.direction_counts?.[direction] ?? 0;
  const total = row.primary?.qualifying_timestamp_count ?? 0;
  return total > 0 ? `${formatNumber(count)} · ${pct(count / total)}` : formatNumber(count);
}

function largestMonthlyConcentration(row: SignalDiscoveryRResult): string {
  const monthly = Object.entries(row.primary?.monthly_episode_counts ?? {});
  const total = row.primary?.episode_count ?? 0;
  if (monthly.length === 0 || total === 0) {
    return "n/a";
  }
  const [month, count] = monthly.reduce((largest, current) => (
    current[1] > largest[1] ? current : largest
  ));
  return `${month} · ${pct(count / total)}`;
}

export function ResearchSignalDiscoveryPage() {
  const { searchParams, navigate } = useAppRouter();
  const [createOpen, setCreateOpen] = useState(false);
  const [createState, setCreateState] = useState<CreateState>(INITIAL_CREATE_STATE);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const [prompt, setPrompt] = useState<SignalDiscoveryPrompt | null>(null);
  const [copiedPrompt, setCopiedPrompt] = useState(false);
  const [selectedRisk, setSelectedRisk] = useState(0);
  const [selectedHorizon, setSelectedHorizon] = useState(48);
  const [selectedDelay, setSelectedDelay] = useState(5);
  const [candidateEngineId, setCandidateEngineId] = useState("");
  const [candidateSignalSetKey, setCandidateSignalSetKey] = useState("");

  const sessionsQuery = useQuery({
    queryKey: ["signal-discovery-sessions"],
    queryFn: fetchSignalDiscoverySessions
  });
  const selectedFromList = selectSession(
    sessionsQuery.data?.sessions,
    searchParams.get("session")
  );
  const sessionQuery = useQuery({
    enabled: Boolean(selectedFromList?.session_id),
    queryKey: ["signal-discovery-session", selectedFromList?.session_id],
    queryFn: () => fetchSignalDiscoverySession(selectedFromList!.session_id)
  });
  const session = sessionQuery.data?.session ?? selectedFromList;
  const catalogQuery = useQuery({
    enabled: createOpen,
    queryKey: ["market-data-catalog"],
    queryFn: fetchMarketDataCatalog
  });
  const enginesQuery = useQuery({
    queryKey: ["signal-engines"],
    queryFn: fetchSignalEngines
  });
  const signalSetsQuery = useQuery({
    enabled: Boolean(candidateEngineId),
    queryKey: ["signal-sets", candidateEngineId],
    queryFn: () => fetchSignalSets(candidateEngineId)
  });
  const activeJobQuery = useQuery({
    enabled: Boolean(activeJobId),
    queryKey: ["runtime-job", activeJobId],
    queryFn: () => fetchJob(activeJobId!),
    refetchInterval: (query) => {
      const status = query.state.data?.job.status;
      return !status || ["queued", "running"].includes(status) ? 1500 : false;
    }
  });

  const discoveryTickers = useMemo(
    () => buildDiscoveryTickers(
      (catalogQuery.data?.assets ?? []).flatMap((asset) => asset.datasets)
    ),
    [catalogQuery.data?.assets]
  );
  const selectedTicker = discoveryTickers.find(
    (ticker) => ticker.key === createState.tickerKey
  );
  const candidateSignalSets = useMemo(
    () =>
      (signalSetsQuery.data?.signal_sets ?? []).filter(
        (signalSet) => !session || signalSet.asset === session.asset
      ),
    [session, signalSetsQuery.data?.signal_sets]
  );
  const configuredRiskValues = useMemo(() => {
    try {
      return buildRiskGrid(createState.riskMinimum, createState.riskMaximum);
    } catch {
      return [];
    }
  }, [createState.riskMaximum, createState.riskMinimum]);
  const job = activeJobQuery.data?.job;
  const jobRunning = Boolean(job && ["queued", "running"].includes(job.status));

  useEffect(() => {
    if (!createState.tickerKey && discoveryTickers[0]) {
      setCreateState((current) => ({ ...current, tickerKey: discoveryTickers[0].key }));
    }
  }, [createState.tickerKey, discoveryTickers]);

  useEffect(() => {
    if (!session) {
      return;
    }
    const target = "selected_target" in session.frozen_target
      ? session.frozen_target.selected_target
      : undefined;
    setSelectedRisk(target?.selected_risk_pct ?? session.config.risk_values[0] ?? 0);
    setSelectedHorizon(target?.horizon_hours ?? session.config.horizon_hours[0] ?? 48);
    setSelectedDelay(target?.entry_delay_minutes ?? session.config.entry_delays_minutes[0] ?? 5);
    setCandidateEngineId(session.candidate_engine_id ?? "");
    setCandidateSignalSetKey(session.candidate_signal_set_key ?? "");
  }, [session?.session_id, session?.updated_at]);

  useEffect(() => {
    if (!job || ["queued", "running"].includes(job.status)) {
      return;
    }
    void queryClient.invalidateQueries({ queryKey: ["signal-discovery-sessions"] });
    if (session?.session_id) {
      void queryClient.invalidateQueries({
        queryKey: ["signal-discovery-session", session.session_id]
      });
    }
    setActiveJobId(null);
  }, [job?.status, session?.session_id]);

  useEffect(() => {
    if (!candidateSignalSetKey && candidateSignalSets[0]) {
      setCandidateSignalSetKey(candidateSignalSets[0].signal_set_key);
    }
  }, [candidateSignalSetKey, candidateSignalSets]);

  const refreshSessions = (sessionId?: string) => {
    void queryClient.invalidateQueries({ queryKey: ["signal-discovery-sessions"] });
    if (sessionId) {
      void queryClient.invalidateQueries({ queryKey: ["signal-discovery-session", sessionId] });
    }
  };

  const createMutation = useMutation({
    mutationFn: createSignalDiscoverySession,
    onSuccess: ({ session: created }) => {
      refreshSessions(created.session_id);
      updateSessionUrl(created.session_id);
      setCreateOpen(false);
      setCreateState(INITIAL_CREATE_STATE);
    }
  });
  const deleteMutation = useMutation({
    mutationFn: deleteSignalDiscoverySession,
    onSuccess: () => {
      refreshSessions();
      updateSessionUrl();
    }
  });
  const atlasMutation = useMutation({
    mutationFn: runSignalDiscoveryAtlas,
    onSuccess: (result) => setActiveJobId(result.job.job_id)
  });
  const freezeMutation = useMutation({
    mutationFn: freezeSignalDiscoveryTarget,
    onSuccess: ({ session: updated }) => refreshSessions(updated.session_id)
  });
  const promptMutation = useMutation({
    mutationFn: generateSignalDiscoveryPrompt,
    onSuccess: (result) => {
      setPrompt(result);
      setCopiedPrompt(false);
    }
  });
  const walkForwardMutation = useMutation({
    mutationFn: runSignalDiscoveryWalkForward,
    onSuccess: (result) => setActiveJobId(result.job.job_id)
  });
  const attachMutation = useMutation({
    mutationFn: attachSignalDiscoveryCandidate,
    onSuccess: ({ session: updated }) => refreshSessions(updated.session_id)
  });
  const evaluateMutation = useMutation({
    mutationFn: evaluateSignalDiscoveryCandidate,
    onSuccess: (result) => setActiveJobId(result.job.job_id)
  });
  const handoffMutation = useMutation({
    mutationFn: handoffSignalDiscoveryCandidate,
    onSuccess: (result) => setActiveJobId(result.job.job_id)
  });

  const mutationError = [
    createMutation.error,
    deleteMutation.error,
    atlasMutation.error,
    freezeMutation.error,
    promptMutation.error,
    walkForwardMutation.error,
    attachMutation.error,
    evaluateMutation.error,
    handoffMutation.error
  ].find(Boolean);
  const rSummaries = session?.summary.r_summaries ?? [];
  const selectedRResult = rSummaries.find((row) => row.risk_pct === selectedRisk) ?? rSummaries[0];
  const monthlyRecurrenceRows = Object.entries(
    selectedRResult?.primary?.monthly_episode_counts ?? {}
  )
    .map(([month, episodeCount]) => ({ month, episodeCount }))
    .sort((left, right) => left.month.localeCompare(right.month));
  const scenarioRows = selectedRResult?.scenarios ?? [];
  const target = session && "selected_target" in session.frozen_target
    ? session.frozen_target
    : null;
  const trainingEvaluation = session?.evaluation.slices?.training;
  const walkForwardEvaluation = session?.evaluation.slices?.walk_forward;

  const submitCreate = () => {
    if (!selectedTicker) {
      return;
    }
    const entryDelays = parseNumberList(createState.entryDelays).map((value) => Math.round(value));
    createMutation.mutate({
      name: createState.name.trim() || `${selectedTicker.asset} Outcome-First`,
      asset: selectedTicker.asset,
      instrument: selectedTicker.instrument,
      research_start: dateStart(createState.researchStart),
      research_end: dateEnd(createState.researchEnd),
      walk_forward_start: dateStart(createState.walkForwardStart),
      walk_forward_end: dateEnd(createState.walkForwardEnd),
      risk_values: configuredRiskValues,
      reward_multiple: 2,
      stop_multiple: 1,
      horizon_hours: [Math.round(createState.maxHoldHours)],
      entry_delays_minutes: entryDelays,
      fee_bps_per_side: createState.feeBps,
      slippage_bps_per_side: createState.slippageBps
    });
  };

  return (
    <div className="page page--workspace">
      <SplitPane
        className="signal-discovery-split"
        leftLabel="Signal discovery sessions"
        workbenchClassName="signal-discovery-workbench"
        left={
          <>
            <div className="list-header">
              <span>Discovery Sessions</span>
              <button
                className="icon-button"
                onClick={() => setCreateOpen(true)}
                type="button"
                aria-label="Create signal discovery session"
                title="Create signal discovery session"
              >
                <Plus aria-hidden="true" />
              </button>
            </div>
            {sessionsQuery.isLoading ? (
              <ListSkeleton count={6} label="Loading discovery sessions" />
            ) : (sessionsQuery.data?.sessions ?? []).length ? (
              (sessionsQuery.data?.sessions ?? []).map((row) => (
                <button
                  className={row.session_id === session?.session_id ? "entity-row is-selected" : "entity-row"}
                  key={row.session_id}
                  onClick={() => updateSessionUrl(row.session_id)}
                  type="button"
                >
                  <div className="discovery-session-row__top">
                    <strong>{row.name}</strong>
                    <StatusBadge tone={statusTone(row.status)}>{row.status.replaceAll("_", " ")}</StatusBadge>
                  </div>
                  <span>{row.asset} · {dateOnly(row.research_start)} — {dateOnly(row.walk_forward_end)}</span>
                  <span>{formatRiskGrid(row.config.risk_values)} · {Math.max(...row.config.horizon_hours)}h max</span>
                </button>
              ))
            ) : (
              <div className="state-line">No discovery sessions.</div>
            )}
          </>
        }
        right={
          session ? (
            <>
              <div className="workbench-header discovery-workbench-header">
                <div>
                  <span className="eyebrow">Outcome-First · {session.asset}</span>
                  <h1>{session.name}</h1>
                  <p>{dateOnly(session.research_start)} — {dateOnly(session.walk_forward_end)} · updated {formatTimestamp(session.updated_at)}</p>
                </div>
                <div className="header-actions">
                  <StatusBadge tone={statusTone(session.status)}>{session.status.replaceAll("_", " ")}</StatusBadge>
                  <button
                    className="icon-button"
                    disabled={RUNNING_STATUSES.has(session.status) || deleteMutation.isPending}
                    onClick={() => {
                      if (window.confirm(`Delete ${session.name}?`)) {
                        deleteMutation.mutate(session.session_id);
                      }
                    }}
                    type="button"
                    aria-label="Delete discovery session"
                    title="Delete discovery session"
                  >
                    <Trash2 aria-hidden="true" />
                  </button>
                </div>
              </div>

              <WorkerRuntimeNotice active={jobRunning} job={job} />

              <div className="discovery-stage-strip" aria-label="Discovery lifecycle">
                {[
                  ["01", "Atlas", Boolean(rSummaries.length)],
                  ["02", "Frozen", Boolean(target)],
                  ["03", "Candidate", Boolean(session.candidate_engine_id)],
                  ["04", "Handoff", session.status === "handed_off"]
                ].map(([number, label, complete]) => (
                  <div className={complete ? "is-complete" : undefined} key={String(label)}>
                    <span>{number}</span>
                    <strong>{label}</strong>
                  </div>
                ))}
              </div>

              {jobRunning ? (
                <div className="state-line state-line--subtle discovery-job-line">
                  <RefreshCw className="spin-icon" aria-hidden="true" />
                  {job?.current_step?.replaceAll("_", " ") || job?.job_type.replaceAll("_", " ")}
                </div>
              ) : null}
              {job?.status === "failed" ? (
                <div className="state-line state-line--error">{String(job.error?.message ?? "Discovery job failed")}</div>
              ) : null}
              {mutationError ? <div className="state-line state-line--error">{mutationError.message}</div> : null}
              {session.summary.last_error?.message ? (
                <div className="state-line state-line--error">{session.summary.last_error.message}</div>
              ) : null}

              <section className="discovery-band">
                <header>
                  <div>
                    <span className="eyebrow">Setup</span>
                    <h2>Research Contract</h2>
                  </div>
                  <button
                    className="button button--secondary button--compact"
                    disabled={jobRunning || session.target_version !== null && session.target_version !== undefined || !["draft", "atlas_ready", "failed"].includes(session.status)}
                    onClick={() => atlasMutation.mutate(session.session_id)}
                    type="button"
                  >
                    <Play aria-hidden="true" />
                    {rSummaries.length ? "Rebuild Atlas" : "Run Atlas"}
                  </button>
                </header>
                <div className="discovery-field-grid">
                  <FieldRow label="Primary label source" value={session.dataset_id} />
                  <FieldRow label="Instrument" value={session.instrument} />
                  <FieldRow label="Training" value={`${dateOnly(session.research_start)} — ${dateOnly(session.research_end)}`} />
                  <FieldRow label="Walk-forward" value={`${dateOnly(session.walk_forward_start)} — ${dateOnly(session.walk_forward_end)}`} />
                  <FieldRow label="R range" value={formatRiskGrid(session.config.risk_values)} />
                  <FieldRow label="Max hold" value={`${Math.max(...session.config.horizon_hours)}h`} />
                  <FieldRow label="Entry delays" value={`${session.config.entry_delays_minutes.join(" / ")}m`} />
                  <FieldRow label="Round-trip costs" value={`${2 * (session.config.fee_bps_per_side + session.config.slippage_bps_per_side)} bps`} />
                  {session.summary.evidence ? <FieldRow label="Evidence sources" value={`${formatNumber(session.summary.evidence.included_dataset_count)} datasets · ${session.summary.evidence.data_types.join(" / ")}`} /> : null}
                  {session.summary.evidence ? <FieldRow label="Evidence cutoff" value={formatTimestamp(session.summary.evidence.authorized_end)} /> : null}
                  {session.summary.evidence ? <FieldRow label="Evidence timeframes" value={session.summary.evidence.timeframes.join(" / ") || "n/a"} /> : null}
                  {session.summary.evidence ? <FieldRow label="Evidence warnings" value={session.summary.evidence.warning_datasets.length ? session.summary.evidence.warning_datasets.map((row) => row.dataset_id).join(" / ") : "None"} /> : null}
                  {session.summary.evidence ? <FieldRow label="Evidence exclusions" value={session.summary.evidence.excluded_datasets.length ? session.summary.evidence.excluded_datasets.map((row) => row.dataset_id).join(" / ") : "None"} /> : null}
                  {session.summary.evidence ? <FieldRow label="Evidence hash" value={session.summary.evidence.manifest_hash.slice(0, 16)} /> : null}
                </div>
              </section>

              <section className="discovery-band">
                <header>
                  <div>
                    <span className="eyebrow">R Feasibility</span>
                    <h2>Opportunity Atlas</h2>
                  </div>
                  <span className="discovery-band__meta">{formatNumber(session.summary.training_episode_count)} episodes · {formatNumber(session.summary.training_feature_count)} feature rows</span>
                </header>
                <DataTable
                  columns={[
                    { key: "risk", header: "R", render: (row) => <strong>{row.risk_pct}%</strong> },
                    { key: "episodes", header: "Episodes", align: "right", render: (row) => formatNumber(primaryEpisodes(row)) },
                    { key: "qualifying", header: "Qualifying", align: "right", render: (row) => formatNumber(row.primary?.qualifying_timestamp_count) },
                    { key: "neutral", header: "Neutral", align: "right", render: (row) => formatNumber(row.primary?.neutral_count) },
                    { key: "scenario", header: "Scenario", align: "right", render: (row) => `${row.primary_scenario?.entry_delay_minutes ?? "n/a"}m / ${row.primary_scenario?.horizon_hours ?? "n/a"}h` },
                    { key: "cost", header: "Cost", align: "right", render: (row) => `${decimal(row.cost?.cost_in_r)}R` }
                  ]}
                  rows={rSummaries}
                  getRowKey={(row) => String(row.risk_pct)}
                  getRowClassName={(row) => row.risk_pct === selectedRResult?.risk_pct ? "is-selected" : undefined}
                  onRowClick={(row) => setSelectedRisk(row.risk_pct)}
                  emptyLabel="Run the atlas to compare fixed R candidates."
                />
                {selectedRResult ? (
                  <div className="discovery-atlas-detail">
                    <div className="discovery-atlas-detail__header">
                      <div>
                        <span className="eyebrow">Selected R</span>
                        <h3>{selectedRResult.risk_pct}% Opportunity Profile</h3>
                      </div>
                      <span>{formatNumber(scenarioRows.length)} scenarios</span>
                    </div>
                    <div className="discovery-field-grid">
                      <FieldRow label="Independent episodes" value={formatNumber(selectedRResult.primary?.episode_count)} />
                      <FieldRow label="Qualifying timestamps" value={formatNumber(selectedRResult.primary?.qualifying_timestamp_count)} />
                      <FieldRow label="LONG distribution" value={directionSummary(selectedRResult, "LONG")} />
                      <FieldRow label="SHORT distribution" value={directionSummary(selectedRResult, "SHORT")} />
                      <FieldRow label="Largest month share" value={largestMonthlyConcentration(selectedRResult)} />
                      <FieldRow label="Neutral / ambiguous" value={`${formatNumber(selectedRResult.primary?.neutral_count)} / ${formatNumber(selectedRResult.primary?.ambiguous_count)}`} />
                      <FieldRow label="Net reward" value={`${decimal(selectedRResult.cost?.net_reward_r)}R`} />
                      <FieldRow label="Net stop" value={`${decimal(selectedRResult.cost?.net_stop_r)}R`} />
                    </div>
                    <div className="discovery-atlas-tables">
                      <div className="discovery-atlas-table">
                        <h3>Delay & Horizon Sensitivity</h3>
                        <DataTable
                          columns={[
                            { key: "delay", header: "Delay", render: (row) => `${row.entry_delay_minutes}m` },
                            { key: "horizon", header: "Horizon", render: (row) => `${row.horizon_hours}h` },
                            { key: "episodes", header: "Episodes", align: "right", render: (row) => formatNumber(row.episode_count) },
                            { key: "qualifying", header: "Qualifying", align: "right", render: (row) => formatNumber(row.qualifying_timestamp_count) },
                            { key: "long", header: "Long", align: "right", render: (row) => formatNumber(row.direction_counts?.LONG) },
                            { key: "short", header: "Short", align: "right", render: (row) => formatNumber(row.direction_counts?.SHORT) }
                          ]}
                          rows={scenarioRows}
                          getRowKey={(row) => `${row.entry_delay_minutes}-${row.horizon_hours}`}
                          emptyLabel="No scenario sensitivity results."
                        />
                      </div>
                      <div className="discovery-atlas-table">
                        <h3>Monthly Episode Recurrence</h3>
                        <DataTable
                          columns={[
                            { key: "month", header: "Month", render: (row) => row.month },
                            { key: "episodes", header: "Episodes", align: "right", render: (row) => formatNumber(row.episodeCount) },
                            { key: "share", header: "Share", align: "right", render: (row) => pct(row.episodeCount / Math.max(1, selectedRResult.primary?.episode_count ?? 0)) }
                          ]}
                          rows={monthlyRecurrenceRows}
                          getRowKey={(row) => row.month}
                          emptyLabel="No recurring opportunity months."
                        />
                      </div>
                    </div>
                  </div>
                ) : null}
              </section>

              <section className="discovery-band">
                <header>
                  <div>
                    <span className="eyebrow">Frozen Target</span>
                    <h2>2R Before 1R</h2>
                  </div>
                  <div className="discovery-action-row">
                    <button
                      className="button button--secondary button--compact"
                      disabled={!target || promptMutation.isPending}
                      onClick={() => promptMutation.mutate(session.session_id)}
                      type="button"
                    >
                      <FileCode2 aria-hidden="true" />
                      Generate Engine Builder Prompt
                    </button>
                    <button
                      className="button button--primary button--compact"
                      disabled={session.status !== "target_frozen" || jobRunning}
                      onClick={() => walkForwardMutation.mutate(session.session_id)}
                      type="button"
                    >
                      <Play aria-hidden="true" />
                      Run Walk-Forward
                    </button>
                  </div>
                </header>
                {target ? (
                  <div className="discovery-field-grid">
                    <FieldRow label="Risk unit" value={`${target.selected_target.selected_risk_pct}%`} />
                    <FieldRow label="Target / stop" value={`${target.selected_target.selected_risk_pct * target.selected_target.reward_multiple}% / ${target.selected_target.selected_risk_pct * target.selected_target.stop_multiple}%`} />
                    <FieldRow label="Holding horizon" value={`${target.selected_target.horizon_hours}h`} />
                    <FieldRow label="Entry" value={`${target.selected_target.entry_semantics} + ${target.selected_target.entry_delay_minutes}m`} />
                    <FieldRow label="Target version" value={`v${target.target_version}`} />
                    <FieldRow label="Config hash" value={target.config_hash.slice(0, 16)} />
                  </div>
                ) : (
                  <div className="discovery-freeze-controls">
                    <label>
                      Risk Unit
                      <select value={selectedRisk} onChange={(event) => setSelectedRisk(Number(event.target.value))}>
                        {session.config.risk_values.map((value) => <option key={value} value={value}>{value}%</option>)}
                      </select>
                    </label>
                    <label>
                      Horizon
                      <select value={selectedHorizon} onChange={(event) => setSelectedHorizon(Number(event.target.value))}>
                        {session.config.horizon_hours.map((value) => <option key={value} value={value}>{value}h</option>)}
                      </select>
                    </label>
                    <label>
                      Entry Delay
                      <select value={selectedDelay} onChange={(event) => setSelectedDelay(Number(event.target.value))}>
                        {session.config.entry_delays_minutes.map((value) => <option key={value} value={value}>{value}m</option>)}
                      </select>
                    </label>
                    <button
                      className="button button--primary"
                      disabled={session.status !== "atlas_ready" || freezeMutation.isPending}
                      onClick={() => freezeMutation.mutate({
                        session_id: session.session_id,
                        selected_risk_pct: selectedRisk,
                        horizon_hours: selectedHorizon,
                        entry_delay_minutes: selectedDelay
                      })}
                      type="button"
                    >
                      <Snowflake aria-hidden="true" />
                      Freeze Target
                    </button>
                  </div>
                )}
              </section>

              <section className="discovery-band">
                <header>
                  <div>
                    <span className="eyebrow">Engine Candidate</span>
                    <h2>Direct Target Evaluation</h2>
                  </div>
                  {session.status === "handed_off" && session.handoff.candidate_id ? (
                    <button
                      className="button button--primary button--compact"
                      onClick={() => navigate("/research/development", `?pool=${session.handoff.universe_run_id}&candidate=${encodeURIComponent(session.handoff.candidate_id!)}`)}
                      type="button"
                    >
                      Open Development
                      <ArrowRight aria-hidden="true" />
                    </button>
                  ) : null}
                </header>

                {["walk_forward_ready", "evaluated"].includes(session.status) ? (
                  <div className="discovery-candidate-controls">
                    <label>
                      Engine
                      <select value={candidateEngineId} onChange={(event) => {
                        setCandidateEngineId(event.target.value);
                        setCandidateSignalSetKey("");
                      }}>
                        <option value="">Select engine</option>
                        {(enginesQuery.data?.engines ?? []).map((engine) => (
                          <option key={engine.signal_engine_id} value={engine.signal_engine_id}>{engine.name}</option>
                        ))}
                      </select>
                    </label>
                    <label>
                      Canonical Signal Set
                      <select value={candidateSignalSetKey} onChange={(event) => setCandidateSignalSetKey(event.target.value)}>
                        <option value="">Select signal set</option>
                        {candidateSignalSets.map((signalSet) => (
                          <option key={signalSet.signal_set_key} value={signalSet.signal_set_key}>{signalSet.signal_set_id}</option>
                        ))}
                      </select>
                    </label>
                    <button
                      className="button button--primary"
                      disabled={!candidateEngineId || !candidateSignalSetKey || attachMutation.isPending}
                      onClick={() => attachMutation.mutate({
                        session_id: session.session_id,
                        signal_engine_id: candidateEngineId,
                        signal_set_key: candidateSignalSetKey
                      })}
                      type="button"
                    >
                      <Link2 aria-hidden="true" />
                      Attach Candidate
                    </button>
                  </div>
                ) : null}

                {session.candidate_engine_id ? (
                  <div className="discovery-field-grid">
                    <FieldRow label="Engine" value={session.candidate_engine_id} />
                    <FieldRow label="Signal set" value={session.candidate_signal_set_key ?? "n/a"} />
                    <FieldRow label="Training precision" value={pct(trainingEvaluation?.opportunity_precision)} />
                    <FieldRow label="WF precision" value={pct(walkForwardEvaluation?.opportunity_precision)} />
                    <FieldRow label="Training episode recall" value={pct(trainingEvaluation?.episode_recall)} />
                    <FieldRow label="WF episode recall" value={pct(walkForwardEvaluation?.episode_recall)} />
                    <FieldRow label="Training net R" value={decimal(trainingEvaluation?.net_r_after_costs)} />
                    <FieldRow label="WF net R" value={decimal(walkForwardEvaluation?.net_r_after_costs)} />
                  </div>
                ) : (
                  <div className="state-line">No engine candidate attached.</div>
                )}

                <div className="discovery-action-row discovery-action-row--footer">
                  <button
                    className="button button--secondary"
                    disabled={session.status !== "candidate_attached" || jobRunning}
                    onClick={() => evaluateMutation.mutate(session.session_id)}
                    type="button"
                  >
                    <FlaskConical aria-hidden="true" />
                    Evaluate Candidate
                  </button>
                  <button
                    className="button button--primary"
                    disabled={session.status !== "accepted" || jobRunning}
                    onClick={() => handoffMutation.mutate(session.session_id)}
                    type="button"
                  >
                    <ArrowRight aria-hidden="true" />
                    Hand Off to Stage 1
                  </button>
                </div>
              </section>
            </>
          ) : (
            <div className="empty-workbench">
              <FlaskConical aria-hidden="true" />
              <h2>Signal Discovery</h2>
              <button className="button button--primary" onClick={() => setCreateOpen(true)} type="button">
                <Plus aria-hidden="true" />
                New Session
              </button>
            </div>
          )
        }
      />

      {createOpen ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal discovery-create-modal" role="dialog" aria-modal="true" aria-labelledby="create-discovery-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Outcome-First Research</span>
                <h2 id="create-discovery-title">New Signal Discovery Session</h2>
              </div>
              <button className="icon-button" onClick={() => setCreateOpen(false)} type="button" aria-label="Close create session">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <div className="form-grid form-grid--dense discovery-create-grid">
                <label>
                  Session Name
                  <input value={createState.name} onChange={(event) => setCreateState((current) => ({ ...current, name: event.target.value }))} placeholder="BTC fixed-R discovery" />
                </label>
                <label>
                  Ticker
                  <select value={createState.tickerKey} onChange={(event) => setCreateState((current) => ({ ...current, tickerKey: event.target.value }))}>
                    {discoveryTickers.map((ticker) => <option value={ticker.key} key={ticker.key}>{ticker.label}</option>)}
                  </select>
                </label>
                <label>
                  Minimum R (%)
                  <input min={0.1} step={0.1} type="number" value={createState.riskMinimum} onChange={(event) => setCreateState((current) => ({ ...current, riskMinimum: Number(event.target.value) }))} />
                </label>
                <label>
                  Maximum R (%)
                  <input min={0.1} step={0.1} type="number" value={createState.riskMaximum} onChange={(event) => setCreateState((current) => ({ ...current, riskMaximum: Number(event.target.value) }))} />
                </label>
                <label>
                  Research Start
                  <input type="date" value={createState.researchStart} onChange={(event) => setCreateState((current) => ({ ...current, researchStart: event.target.value }))} />
                </label>
                <label>
                  Research End
                  <input type="date" value={createState.researchEnd} onChange={(event) => setCreateState((current) => ({ ...current, researchEnd: event.target.value }))} />
                </label>
                <label>
                  Walk-Forward Start
                  <input type="date" value={createState.walkForwardStart} onChange={(event) => setCreateState((current) => ({ ...current, walkForwardStart: event.target.value }))} />
                </label>
                <label>
                  Walk-Forward End
                  <input type="date" value={createState.walkForwardEnd} onChange={(event) => setCreateState((current) => ({ ...current, walkForwardEnd: event.target.value }))} />
                </label>
                <label>
                  Max Hold (hours)
                  <input min={1} step={1} type="number" value={createState.maxHoldHours} onChange={(event) => setCreateState((current) => ({ ...current, maxHoldHours: Number(event.target.value) }))} />
                </label>
                <label>
                  Entry Delays (min)
                  <input value={createState.entryDelays} onChange={(event) => setCreateState((current) => ({ ...current, entryDelays: event.target.value }))} />
                </label>
                <label>
                  Fee / Side (bps)
                  <input min={0} step={0.5} type="number" value={createState.feeBps} onChange={(event) => setCreateState((current) => ({ ...current, feeBps: Number(event.target.value) }))} />
                </label>
                <label>
                  Slippage / Side (bps)
                  <input min={0} step={0.5} type="number" value={createState.slippageBps} onChange={(event) => setCreateState((current) => ({ ...current, slippageBps: Number(event.target.value) }))} />
                </label>
              </div>
              {createMutation.error ? <div className="state-line state-line--error">{createMutation.error.message}</div> : null}
            </div>
            <footer className="terminal-modal__footer">
              <span>{selectedTicker ? selectedTicker.label : "Select a ticker with canonical 5m candles"}</span>
              <div className="modal-actions">
                <button className="button button--secondary" onClick={() => setCreateOpen(false)} type="button">Cancel</button>
                <button
                  className="button button--primary"
                  disabled={!selectedTicker || configuredRiskValues.length === 0 || !Number.isInteger(createState.maxHoldHours) || createState.maxHoldHours <= 0 || createMutation.isPending}
                  onClick={submitCreate}
                  type="button"
                >
                  <Plus aria-hidden="true" />
                  Create Session
                </button>
              </div>
            </footer>
          </section>
        </div>
      ) : null}

      {prompt ? (
        <div className="modal-backdrop" role="presentation">
          <section className="terminal-modal prompt-terminal-modal" role="dialog" aria-modal="true" aria-labelledby="discovery-prompt-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Engine Builder Prompt</span>
                <h2 id="discovery-prompt-title">{prompt.session_id}</h2>
              </div>
              <button className="icon-button" onClick={() => setPrompt(null)} type="button" aria-label="Close engine builder prompt">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="terminal-modal__body">
              <div className="field-stack">
                <FieldRow label="Prompt path" value={prompt.prompt_path ?? prompt.path ?? "n/a"} />
                <pre className="agent-prompt-box">{prompt.prompt}</pre>
              </div>
            </div>
            <footer className="terminal-modal__footer">
              <span>{copiedPrompt ? "Copied to clipboard" : prompt.target_config_hash?.slice(0, 20)}</span>
              <button
                className="button button--primary"
                onClick={() => void navigator.clipboard.writeText(prompt.prompt).then(() => setCopiedPrompt(true))}
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
