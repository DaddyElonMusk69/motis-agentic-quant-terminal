import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { ArrowLeft, FileJson, MoreVertical, Plus, RefreshCw, SlidersHorizontal, X } from "lucide-react";
import {
  createSignalSet,
  extendSignalPoolFromLocalCandles,
  fetchJob,
  fetchJobs,
  fetchMarketDataCatalog,
  fetchSignalEngines,
  fetchSignals,
  fetchSignalSets,
  isJobResponse,
  updateSignalEngine,
  type CatalogAsset,
  type SignalEngine,
  type SignalPoolExtendResult,
  type SignalRecord,
  type SignalSet
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

function updateEngineUrl(next: { engine?: string; signalSetKey?: string; asset?: string }) {
  const params = new URLSearchParams(window.location.search);
  if (next.engine !== undefined) {
    params.set("engine", next.engine);
  }
  if (next.signalSetKey !== undefined) {
    params.set("set", next.signalSetKey);
  }
  if (next.asset !== undefined) {
    params.set("asset", next.asset);
  }
  const query = params.toString();
  const nextUrl = `/engines${query ? `?${query}` : ""}`;
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function showEngineIndex() {
  if (window.location.pathname === "/engines" && !window.location.search) {
    return;
  }
  window.history.pushState(null, "", "/engines");
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function selectEngine(engines: SignalEngine[] | undefined, searchParams: URLSearchParams): SignalEngine | undefined {
  const requested = searchParams.get("engine");
  return engines?.find((engine) => engine.signal_engine_id === requested);
}

function selectSignalSet(signalSets: SignalSet[], searchParams: URLSearchParams): SignalSet | undefined {
  const requestedSet = searchParams.get("set");
  const requestedAsset = searchParams.get("asset");
  return signalSets.find((set) => set.signal_set_key === requestedSet) ?? signalSets.find((set) => set.asset === requestedAsset) ?? signalSets[0];
}

function signalSetState(set: SignalSet): { label: string; tone: "pass" | "warn" | "idle" | "info" } {
  const coverageEnd = set.coverage_end_ts ?? set.end_ts;
  const packetEnd = set.packet_end_ts ?? set.end_ts;
  if (!set.packet_count) {
    return { label: "Empty", tone: "warn" };
  }
  if (coverageEnd && packetEnd && coverageEnd !== packetEnd) {
    return { label: "No-emission gap", tone: "warn" };
  }
  return { label: "Ready", tone: "pass" };
}

function signalUpdateResultText(result: SignalPoolExtendResult | undefined): string {
  if (!result) {
    return "No signal update has run for this pool in this session.";
  }
  const finalEnd = result.final_signal_end_ts ?? result.final_end_ts ?? null;
  const scannedEnd = result.scan_coverage_end_ts ?? result.coverage_end_ts ?? result.raw_candle_end_ts ?? null;
  return [
    `${result.status}: appended ${formatNumber(result.appended_packet_count)} packets`,
    `generated ${formatNumber(result.generated_packet_count ?? 0)}`,
    `scanned through ${formatTimestamp(scannedEnd)}`,
    `final signal ${formatTimestamp(finalEnd)}`
  ].join(" · ");
}

function engineRequiredData(engine: SignalEngine | undefined): Array<{ label: string; value: string }> {
  const codeRef = engine?.code_ref ?? {};
  const requiredData = Array.isArray(engine?.required_data) ? engine.required_data : Array.isArray(codeRef.required_data) ? codeRef.required_data : null;
  if (!requiredData?.length) {
    return [
      { label: "Canonical input", value: "Raw 5m Parquet candles" },
      { label: "Derived use", value: "Engine-defined replay state" },
      { label: "Future updates", value: "No legacy CSV source" }
    ];
  }
  return requiredData.slice(0, 4).map((item, index) => ({
    label: `Requirement ${index + 1}`,
    value: typeof item === "string" ? item : JSON.stringify(item)
  }));
}

function engineRequirements(engine: SignalEngine | undefined): Array<Record<string, unknown>> {
  if (Array.isArray(engine?.required_data)) {
    return engine.required_data;
  }
  if (engine?.code_ref && Array.isArray(engine.code_ref.required_data)) {
    return engine.code_ref.required_data as Array<Record<string, unknown>>;
  }
  return [];
}

function assetRequirementState(asset: CatalogAsset, engine: SignalEngine | undefined): { eligible: boolean; missing: string[] } {
  const requirements = engineRequirements(engine);
  const missing = requirements
    .filter((requirement) => {
      const dataType = String(requirement.data_type ?? "");
      const origin = String(requirement.origin ?? requirement.data_origin ?? "");
      const timeframe = String(requirement.timeframe ?? "");
      return !asset.datasets.some(
        (dataset) =>
          dataset.data_type === dataType &&
          dataset.data_origin === origin &&
          String(dataset.timeframe ?? "") === timeframe
      );
    })
    .map((requirement) => `${String(requirement.origin ?? requirement.data_origin ?? "raw")} ${String(requirement.data_type ?? "data")} ${String(requirement.timeframe ?? "")}`.trim());
  return { eligible: missing.length === 0, missing };
}

function PacketPreview({ error, loading, signals }: { error: Error | null; loading: boolean; signals?: SignalRecord[] }) {
  const sample = signals?.[0];
  return (
    <TerminalPanel eyebrow={sample?.payload_schema ?? "packet"} title="Packet Sample">
      {loading ? <div className="state-line">Loading packet sample...</div> : null}
      {error ? <div className="state-line state-line--error">{error.message}</div> : null}
      {!loading && !error && !sample ? <div className="state-line">No signal packet available for this pool.</div> : null}
      {sample ? (
        <>
          <div className="field-grid">
            <FieldRow label="Signal ID" value={sample.signal_id} />
            <FieldRow label="Timestamp" value={formatTimestamp(sample.timestamp)} />
            <FieldRow label="Asset" value={sample.asset} />
            <FieldRow label="Schema" value={sample.payload_schema} />
          </div>
          <pre className="packet-json">{JSON.stringify(sample.payload, null, 2)}</pre>
        </>
      ) : null}
    </TerminalPanel>
  );
}

export function EnginesPage() {
  const { searchParams } = useAppRouter();
  const [menuEngineId, setMenuEngineId] = useState<string | null>(null);
  const [renameEngine, setRenameEngine] = useState<SignalEngine | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const [addTickerOpen, setAddTickerOpen] = useState(false);
  const [selectedTickerAssets, setSelectedTickerAssets] = useState<string[]>([]);
  const [activeJobId, setActiveJobId] = useState<string | null>(null);
  const enginesQuery = useQuery({ queryKey: ["signal-engines"], queryFn: fetchSignalEngines });
  const catalogQuery = useQuery({ queryKey: ["market-data-catalog"], queryFn: fetchMarketDataCatalog });
  const selectedEngine = selectEngine(enginesQuery.data?.engines, searchParams);
  const engineId = selectedEngine?.signal_engine_id ?? "";
  const signalSetsQuery = useQuery({
    enabled: Boolean(engineId),
    queryKey: ["signal-sets", engineId],
    queryFn: () => fetchSignalSets(engineId)
  });
  const signalSets = signalSetsQuery.data?.signal_sets ?? [];
  const selectedSignalSet = selectSignalSet(signalSets, searchParams);
  const selectedState = selectedSignalSet ? signalSetState(selectedSignalSet) : undefined;
  const requiredData = useMemo(() => engineRequiredData(selectedEngine), [selectedEngine]);

  const signalsQuery = useQuery({
    enabled: Boolean(selectedSignalSet?.signal_set_key),
    queryKey: ["signals", selectedSignalSet?.signal_set_key],
    queryFn: () => fetchSignals(selectedSignalSet!.signal_set_key, 5)
  });
  const activeScopeKey = engineId && selectedSignalSet?.asset ? `signal_set:${engineId}:${selectedSignalSet.asset.toUpperCase()}` : null;
  const latestScopeJobsQuery = useQuery({
    enabled: Boolean(activeScopeKey) && !activeJobId,
    queryKey: ["runtime-jobs", activeScopeKey],
    queryFn: () => fetchJobs(activeScopeKey!, 10)
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

  const signalUpdateMutation = useMutation({
    mutationFn: extendSignalPoolFromLocalCandles,
    onSuccess: (result) => {
      if (isJobResponse(result)) {
        setActiveJobId(result.job.job_id);
        return;
      }
      updateEngineUrl({ engine: result.signal_engine_id, asset: result.asset, signalSetKey: result.signal_set_key });
      void queryClient.invalidateQueries({ queryKey: ["signal-engines"] });
      void queryClient.invalidateQueries({ queryKey: ["signal-sets", result.signal_engine_id] });
      void queryClient.invalidateQueries({ queryKey: ["signals", result.signal_set_key] });
    }
  });

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
    void queryClient.invalidateQueries({ queryKey: ["signal-engines"] });
    if (engineId) {
      void queryClient.invalidateQueries({ queryKey: ["signal-sets", engineId] });
    }
    if (selectedSignalSet?.signal_set_key) {
      void queryClient.invalidateQueries({ queryKey: ["signals", selectedSignalSet.signal_set_key] });
    }
    const timeout = window.setTimeout(() => setActiveJobId(null), job.status === "completed" ? 2500 : 5000);
    return () => window.clearTimeout(timeout);
  }, [activeJobQuery.data?.job?.status, engineId, selectedSignalSet?.signal_set_key]);

  const renameMutation = useMutation({
    mutationFn: ({ engineId, name }: { engineId: string; name: string }) => updateSignalEngine(engineId, { name }),
    onSuccess: (result) => {
      setRenameEngine(null);
      setMenuEngineId(null);
      void queryClient.invalidateQueries({ queryKey: ["signal-engines"] });
      updateEngineUrl({ engine: result.engine.signal_engine_id });
    }
  });

  const createSignalSetMutation = useMutation({
    mutationFn: async ({ signal_engine_id, assets }: { signal_engine_id: string; assets: string[] }) => {
      const created = await Promise.all(assets.map((asset) => createSignalSet({ signal_engine_id, asset })));
      return {
        signal_engine_id,
        signal_sets: created.map((result) => result.signal_set)
      };
    },
    onSuccess: (result) => {
      setAddTickerOpen(false);
      setSelectedTickerAssets([]);
      const firstSignalSet = result.signal_sets[0];
      if (firstSignalSet) {
        updateEngineUrl({ engine: firstSignalSet.signal_engine_id, asset: firstSignalSet.asset, signalSetKey: firstSignalSet.signal_set_key });
      }
      void queryClient.invalidateQueries({ queryKey: ["signal-engines"] });
      void queryClient.invalidateQueries({ queryKey: ["signal-sets", result.signal_engine_id] });
    }
  });

  const signalUpdateResult = isJobResponse(signalUpdateMutation.data) ? undefined : signalUpdateMutation.data;
  const activeSignalJob = activeJobQuery.data?.job ?? null;
  const activeSignalJobRunning = Boolean(activeSignalJob && ["queued", "running"].includes(activeSignalJob.status));
  const selectedUpdateResult = signalUpdateResult?.signal_set_key === selectedSignalSet?.signal_set_key ? signalUpdateResult : undefined;
  const selectedUpdateError = signalUpdateMutation.variables?.asset === selectedSignalSet?.asset ? signalUpdateMutation.error : undefined;
  const isUpdatingSelected = (signalUpdateMutation.isPending && signalUpdateMutation.variables?.asset === selectedSignalSet?.asset) || activeSignalJobRunning;
  const renameError = renameMutation.error;
  const createSignalSetError = createSignalSetMutation.error;
  const selectedTickerSet = useMemo(() => new Set(selectedTickerAssets), [selectedTickerAssets]);
  const availableTickerAssets = useMemo(() => {
    if (!selectedEngine || !catalogQuery.data) {
      return [];
    }
    return catalogQuery.data.assets
      .filter((asset) => assetRequirementState(asset, selectedEngine).eligible && !signalSets.some((set) => set.asset === asset.asset))
      .map((asset) => asset.asset);
  }, [catalogQuery.data, selectedEngine, signalSets]);
  const allAvailableSelected = availableTickerAssets.length > 0 && availableTickerAssets.every((asset) => selectedTickerSet.has(asset));

  function openAddTickerModal() {
    setSelectedTickerAssets([]);
    setAddTickerOpen(true);
  }

  function closeAddTickerModal() {
    setSelectedTickerAssets([]);
    setAddTickerOpen(false);
  }

  function toggleTickerAsset(asset: string) {
    setSelectedTickerAssets((current) => (current.includes(asset) ? current.filter((item) => item !== asset) : [...current, asset]));
  }

  function toggleAllAvailableTickers() {
    setSelectedTickerAssets(allAvailableSelected ? [] : availableTickerAssets);
  }

  return (
    <div className="page page--workspace">
      <SplitPane
        left={
          <>
            {!selectedEngine ? (
              <>
                <div className="list-header">
                  <span>Signal Engines</span>
                  <SlidersHorizontal aria-hidden="true" />
                </div>
                {enginesQuery.isLoading ? <div className="state-line">Loading signal engine catalog...</div> : null}
                {enginesQuery.error ? <div className="state-line state-line--error">{enginesQuery.error.message}</div> : null}
                {enginesQuery.data?.engines.length === 0 ? <div className="state-line">No signal engines registered.</div> : null}
                {enginesQuery.data?.engines.map((engine) => (
                  <div className="entity-row entity-row--with-menu" key={engine.signal_engine_id}>
                    <button className="entity-row__main" onClick={() => updateEngineUrl({ engine: engine.signal_engine_id })} type="button">
                      <strong>{engine.name}</strong>
                      <span>
                        {engine.signal_engine_id}@{engine.version ?? "n/a"} · {formatNumber(engine.signal_set_count)} sets · {formatNumber(engine.packet_count)} packets
                      </span>
                    </button>
                    <button className="icon-button icon-button--bare" onClick={() => setMenuEngineId(menuEngineId === engine.signal_engine_id ? null : engine.signal_engine_id)} type="button" aria-label={`Open ${engine.name} menu`}>
                      <MoreVertical aria-hidden="true" />
                    </button>
                    {menuEngineId === engine.signal_engine_id ? (
                      <div className="card-menu">
                        <button
                          onClick={() => {
                            setRenameEngine(engine);
                            setRenameValue(engine.name);
                            setMenuEngineId(null);
                          }}
                          type="button"
                        >
                          Rename
                        </button>
                      </div>
                    ) : null}
                  </div>
                ))}
              </>
            ) : (
              <>
                <div className="list-header list-header--stacked">
                  <button className="list-back" onClick={showEngineIndex} type="button">
                    <ArrowLeft aria-hidden="true" />
                    {selectedEngine.name}
                  </button>
                  <span>{selectedEngine.signal_engine_id}</span>
                </div>
                {signalSetsQuery.isLoading ? <div className="state-line">Loading signal pools...</div> : null}
                {signalSetsQuery.error ? <div className="state-line state-line--error">{signalSetsQuery.error.message}</div> : null}
                {!signalSetsQuery.isLoading && signalSets.length === 0 ? <div className="state-line">No signal pools registered for this engine.</div> : null}
                {signalSets.map((set) => {
                  const state = signalSetState(set);
                  return (
                    <button
                      className={set.signal_set_key === selectedSignalSet?.signal_set_key ? "signal-pool-card is-selected" : "signal-pool-card"}
                      key={set.signal_set_key}
                      onClick={() => updateEngineUrl({ engine: set.signal_engine_id, asset: set.asset, signalSetKey: set.signal_set_key })}
                      type="button"
                    >
                      <div className="signal-pool-card__top">
                        <strong>{set.asset}</strong>
                        <StatusBadge tone={state.tone}>{state.label}</StatusBadge>
                      </div>
                      <span>{set.instrument}</span>
                      <small className="mono">
                        {formatTimestamp(set.coverage_start_ts ?? set.start_ts)} - {formatTimestamp(set.coverage_end_ts ?? set.end_ts)}
                      </small>
                      <small>{formatNumber(set.packet_count)} packets · last {formatTimestamp(set.packet_end_ts ?? set.end_ts)}</small>
                    </button>
                  );
                })}
              </>
            )}
          </>
        }
        leftLabel="Signal engine list"
        right={
          !selectedEngine ? (
            <>
              <div className="workbench-header">
                <div>
                  <span className="eyebrow">Engine registry</span>
                  <h1>Signal Engines</h1>
                </div>
                <button className="button button--secondary" disabled={enginesQuery.isFetching} onClick={() => void enginesQuery.refetch()} type="button">
                  <RefreshCw aria-hidden="true" />
                  {enginesQuery.isFetching ? "Refreshing" : "Refresh"}
                </button>
              </div>

              <div className="workbench-grid">
                <TerminalPanel title="Registry Summary">
                  <div className="field-stack">
                    <FieldRow label="Registered engines" value={formatNumber(enginesQuery.data?.engines.length ?? 0)} />
                    <FieldRow label="Signal pools" value={formatNumber((enginesQuery.data?.engines ?? []).reduce((total, engine) => total + engine.signal_set_count, 0))} />
                    <FieldRow label="Signal packets" value={formatNumber((enginesQuery.data?.engines ?? []).reduce((total, engine) => total + engine.packet_count, 0))} />
                  </div>
                </TerminalPanel>
                <TerminalPanel title="Navigation">
                  <div className="state-card">
                    <FileJson aria-hidden="true" />
                    <span>Click an engine in the middle column to page into its asset signal pools. The right workbench will then focus on one selected pool.</span>
                  </div>
                </TerminalPanel>
              </div>

              <TerminalPanel title="Engines">
                <DataTable
                  columns={[
                    { key: "engine", header: "Engine", render: (row) => <strong>{row.name}</strong> },
                    { key: "id", header: "ID", render: (row) => <span className="mono">{row.signal_engine_id}</span> },
                    { key: "version", header: "Version", render: (row) => row.version ?? "n/a" },
                    { key: "sets", header: "Pools", align: "right", render: (row) => formatNumber(row.signal_set_count) },
                    { key: "packets", header: "Packets", align: "right", render: (row) => formatNumber(row.packet_count) },
                    {
                      key: "menu",
                      header: "",
                      align: "right",
                      render: (row) => (
                        <button
                          className="icon-button icon-button--bare"
                          onClick={(event) => {
                            event.stopPropagation();
                            setRenameEngine(row);
                            setRenameValue(row.name);
                          }}
                          type="button"
                          aria-label={`Rename ${row.name}`}
                        >
                          <MoreVertical aria-hidden="true" />
                        </button>
                      )
                    }
                  ]}
                  getRowKey={(row) => row.signal_engine_id}
                  onRowClick={(row) => updateEngineUrl({ engine: row.signal_engine_id })}
                  rows={enginesQuery.data?.engines ?? []}
                />
              </TerminalPanel>
            </>
          ) : (
            <>
            <div className="workbench-header">
              <div>
                <span className="eyebrow">{selectedEngine.name}</span>
                <h1>{selectedSignalSet?.instrument ?? selectedSignalSet?.asset ?? "Signal Pool"}</h1>
              </div>
              <div className="header-actions">
                {selectedState ? <StatusBadge tone={selectedState.tone}>{selectedState.label}</StatusBadge> : null}
                <button className="button button--secondary" onClick={openAddTickerModal} type="button">
                  <Plus aria-hidden="true" />
                  Add Ticker
                </button>
                <button
                  className="button button--secondary"
                  disabled={!selectedSignalSet || isUpdatingSelected}
                  onClick={() => selectedSignalSet && signalUpdateMutation.mutate({ signal_engine_id: selectedSignalSet.signal_engine_id, asset: selectedSignalSet.asset })}
                  type="button"
                >
                  <RefreshCw aria-hidden="true" />
                  {isUpdatingSelected ? "Updating Signals" : "Update Selected Pool"}
                </button>
              </div>
            </div>

            {isUpdatingSelected ? (
              <div className="progress-card">
                <div className="progress-card__header">
                  <strong>Updating {selectedSignalSet?.asset} signal pool</strong>
                  <span>{activeSignalJob ? `${activeSignalJob.status} · ${activeSignalJob.current_step ?? "waiting"}` : "Scan canonical Parquet candles, emit packets, import DB rows"}</span>
                </div>
                <div className="progress-rail" aria-label="Signal update in progress">
                  <span />
                </div>
                <div className="progress-steps">
                  <span>Read raw candles</span>
                  <span>Run engine</span>
                  <span>Persist signal rows</span>
                </div>
                <WorkerRuntimeNotice active={isUpdatingSelected} job={activeSignalJob} />
              </div>
            ) : null}

            <div className="workbench-grid">
              <TerminalPanel title="Required Data">
                <div className="field-stack">
                  {requiredData.map((item) => (
                    <FieldRow key={item.label} label={item.label} value={item.value} />
                  ))}
                </div>
              </TerminalPanel>
              <TerminalPanel title="Coverage Semantics">
                <div className="field-stack">
                  <FieldRow label="Scanned coverage" value="Canonical Parquet horizon" />
                  <FieldRow label="Packet coverage" value="DB emitted signals" />
                  <FieldRow label="No-emission gap" value="Shown as a valid engine outcome" />
                </div>
              </TerminalPanel>
            </div>

            <div className="workbench-grid workbench-grid--wide-left">
              <TerminalPanel eyebrow={selectedSignalSet?.asset ?? "pool"} title="Selected Signal Pool">
                {selectedSignalSet ? (
                  <div className="field-grid">
                    <FieldRow label="Engine" value={`${selectedSignalSet.signal_engine_id} ${selectedSignalSet.signal_engine_version}`} />
                    <FieldRow label="Instrument" value={selectedSignalSet.instrument} />
                    <FieldRow label="Packets" value={formatNumber(selectedSignalSet.packet_count)} />
                    <FieldRow label="Payload schema" value={selectedSignalSet.payload_schema} />
                    <FieldRow label="Scanned end" value={formatTimestamp(selectedSignalSet.coverage_end_ts ?? selectedSignalSet.end_ts)} />
                    <FieldRow label="Packet end" value={formatTimestamp(selectedSignalSet.packet_end_ts ?? selectedSignalSet.end_ts)} />
                    <FieldRow label="Source path" value={selectedSignalSet.source_path ? "audit artifact" : "n/a"} />
                    <FieldRow label="State" value={selectedState?.label ?? "n/a"} />
                  </div>
                ) : (
                  <div className="state-line">No signal pool selected.</div>
                )}
              </TerminalPanel>

              <TerminalPanel title="Update Result">
                <div className="state-card">
                  <FileJson aria-hidden="true" />
                  <span>{selectedUpdateError ? selectedUpdateError.message : signalUpdateResultText(selectedUpdateResult)}</span>
                </div>
              </TerminalPanel>
            </div>

            <PacketPreview error={signalsQuery.error} loading={signalsQuery.isLoading} signals={signalsQuery.data?.signals} />
          </>
          )
        }
      />
      {renameEngine ? (
        <div className="terminal-modal-backdrop">
          <section className="terminal-modal compact-modal" role="dialog" aria-modal="true" aria-labelledby="rename-engine-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">Engine display name</span>
                <h2 id="rename-engine-title">Rename Engine</h2>
              </div>
              <button className="icon-button" onClick={() => setRenameEngine(null)} type="button" aria-label="Close rename dialog">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="modal-stack">
              <label>
                Display name
                <input value={renameValue} onChange={(event) => setRenameValue(event.target.value)} />
              </label>
              {renameError ? <div className="state-line state-line--error">{renameError.message}</div> : null}
              <div className="modal-actions">
                <button className="button button--secondary" onClick={() => setRenameEngine(null)} type="button">Cancel</button>
                <button
                  className="button button--primary"
                  disabled={!renameValue.trim() || renameMutation.isPending}
                  onClick={() => renameMutation.mutate({ engineId: renameEngine.signal_engine_id, name: renameValue.trim() })}
                  type="button"
                >
                  {renameMutation.isPending ? "Renaming" : "Rename"}
                </button>
              </div>
            </div>
          </section>
        </div>
      ) : null}
      {addTickerOpen && selectedEngine ? (
        <div className="terminal-modal-backdrop">
          <section className="terminal-modal add-ticker-modal" role="dialog" aria-modal="true" aria-labelledby="add-ticker-title">
            <header className="terminal-modal__header">
              <div>
                <span className="eyebrow">{selectedEngine.name}</span>
                <h2 id="add-ticker-title">Add Ticker</h2>
              </div>
              <button className="icon-button" onClick={closeAddTickerModal} type="button" aria-label="Close add ticker dialog">
                <X aria-hidden="true" />
              </button>
            </header>
            <div className="add-ticker-toolbar">
              <div>
                <strong>{selectedTickerAssets.length} selected</strong>
                <span>{availableTickerAssets.length} ready from local data</span>
              </div>
              <div className="header-actions">
                <button className="button button--secondary" disabled={availableTickerAssets.length === 0 || createSignalSetMutation.isPending} onClick={toggleAllAvailableTickers} type="button">
                  {allAvailableSelected ? "Clear" : "Select All Ready"}
                </button>
                <button
                  className="button button--primary"
                  disabled={selectedTickerAssets.length === 0 || createSignalSetMutation.isPending}
                  onClick={() => createSignalSetMutation.mutate({ signal_engine_id: selectedEngine.signal_engine_id, assets: selectedTickerAssets })}
                  type="button"
                >
                  {createSignalSetMutation.isPending ? "Importing" : `Bulk Import ${selectedTickerAssets.length || ""}`.trim()}
                </button>
              </div>
            </div>
            <div className="add-ticker-list">
              {catalogQuery.isLoading ? <div className="state-line">Loading data catalog...</div> : null}
              {catalogQuery.error ? <div className="state-line state-line--error">{catalogQuery.error.message}</div> : null}
              {catalogQuery.data?.assets.map((asset) => {
                const state = assetRequirementState(asset, selectedEngine);
                const existing = signalSets.some((set) => set.asset === asset.asset);
                const selected = selectedTickerSet.has(asset.asset);
                return (
                  <button
                    className={[
                      "ticker-option",
                      selected ? "is-selected" : "",
                      !state.eligible || existing ? "is-disabled" : ""
                    ].filter(Boolean).join(" ")}
                    disabled={!state.eligible || existing || createSignalSetMutation.isPending}
                    key={asset.asset}
                    onClick={() => toggleTickerAsset(asset.asset)}
                    type="button"
                  >
                    <div>
                      <strong>{asset.asset}</strong>
                      <span>{asset.datasets.length} local refs</span>
                    </div>
                    <StatusBadge tone={existing ? "info" : state.eligible ? "pass" : "warn"}>{existing ? "Added" : state.eligible ? "Ready" : "Missing data"}</StatusBadge>
                    {!state.eligible ? <small>Missing {state.missing.join(", ")}</small> : null}
                  </button>
                );
              })}
              {catalogQuery.data?.assets.length === 0 ? <div className="state-line">No local data assets are available.</div> : null}
              {createSignalSetError ? <div className="state-line state-line--error">{createSignalSetError.message}</div> : null}
            </div>
          </section>
        </div>
      ) : null}
    </div>
  );
}
