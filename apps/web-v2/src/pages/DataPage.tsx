import { useEffect, useMemo, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Database, RefreshCw, Search, UploadCloud } from "lucide-react";
import {
  fetchDatasetRows,
  fetchDatasetCandles,
  fetchJob,
  fetchJobs,
  fetchMarketDataCatalog,
  isJobResponse,
  refreshMarketDataFeatureFamily,
  refreshMarketDataEma,
  refreshMarketDataDataset,
  type CatalogResponse,
  type Dataset,
  type RefreshPlan
} from "../app/api";
import { formatCompactValue, formatNumber, formatTimestamp } from "../app/format";
import { queryClient } from "../app/queryClient";
import { useAppRouter } from "../app/router";
import { DataTable } from "../components/DataTable";
import { FieldRow } from "../components/FieldRow";
import { SplitPane } from "../components/SplitPane";
import { StatusBadge } from "../components/StatusBadge";
import { TerminalPanel } from "../components/TerminalPanel";
import { WorkerRuntimeNotice } from "../components/WorkerRuntimeNotice";

type DatasetTypeOption = {
  dataType: string;
  label: string;
  count: number;
};

type RefreshRequest =
  | { kind: "candles"; datasetId: string }
  | { kind: "ema"; asset: string }
  | { kind: "feature"; asset: string; family: string };

const FEATURE_CATEGORIES = [
  { dataType: "feature_base_candle", family: "base_candle", label: "Base Candle Features" },
  { dataType: "feature_volatility_range", family: "volatility_range", label: "Volatility / Range" },
  { dataType: "feature_volume", family: "volume", label: "Volume" },
  { dataType: "feature_ema_vegas_structure", family: "ema_vegas_structure", label: "EMA / Vegas Structure" },
  { dataType: "feature_bollinger", family: "bollinger", label: "Bollinger Context" },
  { dataType: "feature_regime_momentum", family: "regime_momentum", label: "Regime / Momentum" }
] as const;

function featureCategoryForDataType(dataType: string) {
  return FEATURE_CATEGORIES.find((category) => category.dataType === dataType);
}

function getSelectedAsset(catalog: CatalogResponse | undefined, searchParams: URLSearchParams): string {
  const requested = searchParams.get("asset");
  if (requested && catalog?.assets.some((asset) => asset.asset === requested)) {
    return requested;
  }
  return catalog?.assets[0]?.asset ?? "";
}

function getSelectedDataset(catalog: CatalogResponse | undefined, selectedAsset: string, searchParams: URLSearchParams): Dataset | undefined {
  const requestedDataset = searchParams.get("dataset");
  const allDatasets = catalog?.assets.flatMap((asset) => asset.datasets) ?? [];
  const requested = allDatasets.find((dataset) => dataset.dataset_id === requestedDataset);
  if (requested) {
    return requested;
  }
  const assetDatasets = catalog?.assets.find((asset) => asset.asset === selectedAsset)?.datasets ?? [];
  return assetDatasets[0] ?? allDatasets[0];
}

function getDataTypeOptions(catalog: CatalogResponse | undefined, selectedAsset: string): DatasetTypeOption[] {
  const datasets = catalog?.assets.find((asset) => asset.asset === selectedAsset)?.datasets ?? [];
  const counts = new Map<string, number>();
  for (const dataset of datasets) {
    counts.set(dataset.data_type, (counts.get(dataset.data_type) ?? 0) + 1);
  }
  const options = Array.from(counts.entries())
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([dataType, count]) => ({
      dataType,
      label: dataType === "candles" ? "Candle data" : featureCategoryForDataType(dataType)?.label ?? titleize(dataType),
      count
    }));
  const derivedCandles = datasets.filter((dataset) => dataset.data_type === "candles" && dataset.data_origin === "derived");
  if (derivedCandles.length) {
    options.push({ dataType: "ema", label: "EMA", count: derivedCandles.length });
  }
  return options;
}

function getSelectedDataType(catalog: CatalogResponse | undefined, selectedAsset: string, searchParams: URLSearchParams): string {
  const requested = searchParams.get("data_type") ?? searchParams.get("filter");
  const options = getDataTypeOptions(catalog, selectedAsset);
  if (requested && options.some((option) => option.dataType === requested)) {
    return requested;
  }
  return options[0]?.dataType ?? "";
}

function updateDataUrl(next: { asset?: string; dataset?: string; dataType?: string }) {
  const params = new URLSearchParams(window.location.search);
  if (next.asset !== undefined) {
    params.set("asset", next.asset);
  }
  if (next.dataset !== undefined) {
    params.set("dataset", next.dataset);
  }
  if (next.dataType !== undefined) {
    params.set("data_type", next.dataType);
    params.delete("filter");
  }
  const query = params.toString();
  const nextUrl = `/data${query ? `?${query}` : ""}`;
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function datasetsForType(catalog: CatalogResponse | undefined, selectedAsset: string, dataType: string): Dataset[] {
  const datasets = catalog?.assets.find((asset) => asset.asset === selectedAsset)?.datasets ?? [];
  if (dataType === "ema") {
    return datasets.filter((dataset) => dataset.data_type === "candles" && dataset.data_origin === "derived");
  }
  return datasets.filter((dataset) => dataset.data_type === dataType);
}

function getPrimaryDatasetForType(datasets: Dataset[], dataType?: string): Dataset | undefined {
  if (dataType === "ema") {
    return datasets.find((dataset) => dataset.timeframe === "2h") ?? datasets[0];
  }
  return datasets.find((dataset) => dataset.data_origin === "raw") ?? datasets[0];
}

function getRefreshTargetForType(datasets: Dataset[], dataType: string, selectedAsset: string): RefreshRequest | undefined {
  if (dataType === "ema" && datasets.length) {
    return { kind: "ema", asset: selectedAsset };
  }
  const featureCategory = featureCategoryForDataType(dataType);
  if (featureCategory) {
    return { kind: "feature", asset: selectedAsset, family: featureCategory.family };
  }
  if (dataType !== "candles") {
    return undefined;
  }
  const dataset = datasets.find((item) => item.data_origin === "raw" && item.timeframe === "5m") ?? datasets.find((item) => item.data_origin === "raw");
  return dataset ? { kind: "candles", datasetId: dataset.dataset_id } : undefined;
}

function datasetStatusTone(dataset: Dataset): "pass" | "warn" | "info" | "idle" {
  if (dataset.quality_status === "updated" || dataset.quality_status === "ingested" || dataset.quality_status === "rebuilt" || dataset.quality_status === "ema_enriched") {
    return "pass";
  }
  if (dataset.quality_status === "blocked" || dataset.quality_status === "failed") {
    return "warn";
  }
  if (dataset.data_origin === "derived") {
    return "info";
  }
  return "idle";
}

function refreshResultText(result: RefreshPlan | undefined): string {
  if (!result) {
    return "No fill action has run for this dataset in this session.";
  }
  if (result.status === "filled") {
    return `Added ${formatNumber(result.rows_added ?? 0)} rows, rebuilt ${formatNumber(result.derived_rebuilt?.length ?? 0)} derived datasets.`;
  }
  if (result.status === "enriched") {
    if (result.feature_count !== undefined) {
      return `Enriched ${formatNumber(result.feature_count ?? result.features?.length ?? 0)} feature datasets.`;
    }
    return `Enriched ${formatNumber(result.enriched_count ?? result.enriched?.length ?? 0)} EMA datasets.`;
  }
  if (result.status === "noop" && result.feature_count !== undefined) {
    return "No feature datasets were updated.";
  }
  if (result.status === "noop" && result.enriched_count !== undefined) {
    return "No EMA datasets were updated.";
  }
  if (result.status === "current") {
    return `Current through ${formatTimestamp(result.end_ts ?? null)}.`;
  }
  if (result.status === "no_new_rows") {
    return `No new rows from ${formatTimestamp(result.from_ts ?? null)} to ${formatTimestamp(result.to_ts ?? null)}.`;
  }
  if (result.status === "planned") {
    return `Planned fill from ${formatTimestamp(result.from_ts ?? null)} to ${formatTimestamp(result.to_ts ?? null)}.`;
  }
  return result.reason ?? result.status;
}

function datasetBelongsToCategory(dataset: Dataset, dataType: string): boolean {
  if (dataType === "ema") {
    return dataset.data_type === "candles" && dataset.data_origin === "derived";
  }
  return dataset.data_type === dataType;
}

function hasEmaColumns(dataset: Dataset): boolean {
  const schema = dataset.schema_descriptor ?? {};
  const ema = schema.ema;
  return Boolean(ema && typeof ema === "object");
}

function refreshRequestKey(request: RefreshRequest | undefined): string | undefined {
  if (!request) {
    return undefined;
  }
  if (request.kind === "ema") {
    return `ema:${request.asset}`;
  }
  if (request.kind === "feature") {
    return `feature:${request.asset}:${request.family}`;
  }
  return request.datasetId;
}

function titleize(value: string): string {
  return value
    .split(/[_-]/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

export function DataPage() {
  const { searchParams } = useAppRouter();
  const [activeRefreshJobId, setActiveRefreshJobId] = useState<string | null>(null);
  const catalogQuery = useQuery({ queryKey: ["market-data-catalog"], queryFn: fetchMarketDataCatalog });
  const catalog = catalogQuery.data;
  const selectedAsset = getSelectedAsset(catalog, searchParams);
  const selectedDataType = getSelectedDataType(catalog, selectedAsset, searchParams);
  const typeOptions = useMemo(() => getDataTypeOptions(catalog, selectedAsset), [catalog, selectedAsset]);
  const visibleDatasets = useMemo(() => datasetsForType(catalog, selectedAsset, selectedDataType), [catalog, selectedAsset, selectedDataType]);
  const requestedDataset = getSelectedDataset(catalog, selectedAsset, searchParams);
  const selectedDataset = requestedDataset?.asset === selectedAsset && datasetBelongsToCategory(requestedDataset, selectedDataType) ? requestedDataset : getPrimaryDatasetForType(visibleDatasets, selectedDataType);
  const refreshTarget = getRefreshTargetForType(visibleDatasets, selectedDataType, selectedAsset);
  const refreshTargetKey = refreshRequestKey(refreshTarget);

  const rowPreviewQuery = useQuery({
    enabled: Boolean(selectedDataset),
    queryKey: ["market-data-rows", selectedDataset?.dataset_id],
    queryFn: () => selectedDataset!.data_type === "candles" ? fetchDatasetCandles(selectedDataset!.dataset_id, 25) : fetchDatasetRows(selectedDataset!.dataset_id, 25)
  });
  const refreshJobQuery = useQuery({
    enabled: Boolean(activeRefreshJobId),
    queryKey: ["runtime-job", activeRefreshJobId],
    queryFn: () => fetchJob(activeRefreshJobId!),
    refetchInterval: (query) => {
      const job = query.state.data?.job;
      return !job || ["queued", "running"].includes(job.status) ? 1500 : false;
    }
  });
  const activeScopeKey = refreshTarget
    ? (refreshTarget.kind === "ema"
      ? `asset:${refreshTarget.asset}:ema`
      : refreshTarget.kind === "feature"
        ? `asset:${refreshTarget.asset}:feature:${refreshTarget.family}`
        : `dataset:${refreshTarget.datasetId}`)
    : null;
  const latestScopeJobsQuery = useQuery({
    enabled: Boolean(activeScopeKey) && !activeRefreshJobId,
    queryKey: ["runtime-jobs", activeScopeKey],
    queryFn: () => fetchJobs(activeScopeKey!, 10)
  });

  const refreshMutation = useMutation({
    mutationFn: (request: RefreshRequest) => {
      if (request.kind === "ema") {
        return refreshMarketDataEma(request.asset);
      }
      if (request.kind === "feature") {
        return refreshMarketDataFeatureFamily(request.asset, request.family);
      }
      return refreshMarketDataDataset(request.datasetId);
    },
    onSuccess: (result) => {
      if (isJobResponse(result)) {
        setActiveRefreshJobId(result.job.job_id);
        return;
      }
      void queryClient.invalidateQueries({ queryKey: ["market-data-catalog"] });
      void queryClient.invalidateQueries({ queryKey: ["market-data-rows", result.dataset_id] });
    }
  });

  const canRefreshType = Boolean(refreshTarget);
  const refreshResult = isJobResponse(refreshMutation.data) ? undefined : refreshMutation.data;
  const selectedRefreshResult = refreshResult && (
    selectedDataType === "ema"
      ? refreshResult.asset === selectedAsset || refreshResult.status === "enriched"
      : featureCategoryForDataType(selectedDataType)
        ? refreshResult.asset === selectedAsset && refreshResult.family === featureCategoryForDataType(selectedDataType)?.family
        : refreshResult.dataset_id === refreshTargetKey
  ) ? refreshResult : undefined;
  const selectedRefreshError = refreshRequestKey(refreshMutation.variables) === refreshTargetKey ? refreshMutation.error : undefined;
  const selectedTypeLabel = typeOptions.find((option) => option.dataType === selectedDataType)?.label ?? titleize(selectedDataType || "data");
  const refreshJob = refreshJobQuery.data?.job ?? null;
  const refreshJobRunning = Boolean(refreshJob && ["queued", "running"].includes(refreshJob.status));
  const isRefreshingType = (refreshMutation.isPending && refreshRequestKey(refreshMutation.variables) === refreshTargetKey) || refreshJobRunning;

  useEffect(() => {
    if (activeRefreshJobId) {
      return;
    }
    const job = latestScopeJobsQuery.data?.jobs.find((item) => ["queued", "running"].includes(item.status));
    if (job) {
      setActiveRefreshJobId(job.job_id);
    }
  }, [activeRefreshJobId, latestScopeJobsQuery.data?.jobs]);

  useEffect(() => {
    if (!refreshJob || ["queued", "running"].includes(refreshJob.status)) {
      return;
    }
    void queryClient.invalidateQueries({ queryKey: ["market-data-catalog"] });
    if (selectedDataset?.dataset_id) {
      void queryClient.invalidateQueries({ queryKey: ["market-data-rows", selectedDataset.dataset_id] });
    }
    const timeout = window.setTimeout(() => setActiveRefreshJobId(null), refreshJob.status === "completed" ? 2500 : 5000);
    return () => window.clearTimeout(timeout);
  }, [refreshJob?.status, selectedDataset?.dataset_id]);

  return (
    <div className="page page--workspace">
      <SplitPane
        left={
          <>
            <div className="list-header">
              <span>Catalog Assets</span>
              <Search aria-hidden="true" />
            </div>
            {catalogQuery.isLoading ? <div className="state-line">Loading local market data coverage...</div> : null}
            {catalogQuery.error ? <div className="state-line state-line--error">{catalogQuery.error.message}</div> : null}
            {catalog?.assets.map((asset) => {
              const dataTypes = new Set(asset.datasets.map((dataset) => dataset.data_type));
              const candleCount = asset.datasets.filter((dataset) => dataset.data_type === "candles").length;
              return (
                <button
                  className={asset.asset === selectedAsset ? "entity-row is-selected" : "entity-row"}
                  key={asset.asset}
                  onClick={() => {
                    const firstType = getDataTypeOptions(catalog, asset.asset)[0]?.dataType ?? "";
                    const firstDataset = getPrimaryDatasetForType(datasetsForType(catalog, asset.asset, firstType), firstType);
                    updateDataUrl({ asset: asset.asset, dataType: firstType, dataset: firstDataset?.dataset_id });
                  }}
                  type="button"
                >
                  <strong>{asset.asset}</strong>
                  <span>{dataTypes.size} data types · {candleCount} candle refs</span>
                </button>
              );
            })}
          </>
        }
        leftLabel="Data catalog assets"
        right={
          <>
            <div className="workbench-header">
              <div>
                <span className="eyebrow">Market data</span>
                <h1>{selectedAsset ? `${selectedAsset} Dataset Catalog` : "Canonical Dataset Catalog"}</h1>
              </div>
              <div className="header-actions">
                <StatusBadge tone="info">{catalog ? `${formatNumber(catalog.summary.assets)} assets` : "Catalog"}</StatusBadge>
                <button className="button button--secondary" disabled={catalogQuery.isFetching} onClick={() => void catalogQuery.refetch()} type="button">
                  <RefreshCw aria-hidden="true" />
                  {catalogQuery.isFetching ? "Refreshing" : "Refresh"}
                </button>
              </div>
            </div>

            <div className="filter-strip" aria-label="Dataset type filters">
              {typeOptions.map((option) => (
                <button
                  className={selectedDataType === option.dataType ? "filter-chip is-active" : "filter-chip"}
                  key={option.dataType}
                  onClick={() => {
                    const firstDataset = getPrimaryDatasetForType(datasetsForType(catalog, selectedAsset, option.dataType), option.dataType);
                    updateDataUrl({ dataType: option.dataType, dataset: firstDataset?.dataset_id });
                  }}
                  type="button"
                >
                  {option.label}
                  <span>{option.count}</span>
                </button>
              ))}
            </div>

            <TerminalPanel
              actions={
                selectedDataset ? (
                  <button
                    className="button button--primary"
                    disabled={!canRefreshType || isRefreshingType}
                    onClick={() => refreshTarget && refreshMutation.mutate(refreshTarget)}
                    title={canRefreshType ? `Fill ${selectedTypeLabel.toLowerCase()} to current time` : "Fill is supported for candle, EMA, and feature data"}
                    type="button"
                  >
                    <UploadCloud aria-hidden="true" />
                    {isRefreshingType ? `Filling ${selectedTypeLabel}` : `Fill ${selectedTypeLabel}`}
                  </button>
                ) : null
              }
              title={`${selectedTypeLabel} Coverage`}
            >
              {isRefreshingType ? (
                <div className="progress-card">
                  <div className="progress-card__header">
                    <strong>Updating {selectedTypeLabel.toLowerCase()}</strong>
                    <span>{refreshJob ? `${refreshJob.status} · ${refreshJob.current_step ?? "waiting"}` : selectedDataType === "ema" ? "Derived candle scan + EMA enrichment" : featureCategoryForDataType(selectedDataType) ? "Derived candle scan + feature enrichment" : "OKX download + Parquet persist + derived rebuild"}</span>
                  </div>
                  <div className="progress-rail" aria-label="Data fill in progress">
                    <span />
                  </div>
                  <div className="progress-steps">
                    {selectedDataType === "ema" ? (
                      <>
                        <span>Read derived candles</span>
                        <span>Compute recursive EMA</span>
                        <span>Persist enriched Parquet</span>
                      </>
                    ) : featureCategoryForDataType(selectedDataType) ? (
                      <>
                        <span>Read derived candles</span>
                        <span>Compute feature family</span>
                        <span>Persist feature Parquet</span>
                      </>
                    ) : (
                      <>
                        <span>Fetch raw candles</span>
                        <span>Persist canonical Parquet</span>
                        <span>Rebuild derived candles</span>
                      </>
                    )}
                  </div>
                  <WorkerRuntimeNotice active={isRefreshingType} job={refreshJob} />
                </div>
              ) : null}
              <DataTable
                columns={[
                  { key: "dataset", header: "Dataset", render: (row) => <span className="mono">{row.dataset_id}</span> },
                  { key: "origin", header: "Origin", render: (row) => row.data_origin },
                  { key: "timeframe", header: "TF", render: (row) => row.timeframe ?? "event" },
                  { key: "start", header: "Start", render: (row) => <span className="mono">{formatTimestamp(row.start_ts)}</span> },
                  { key: "end", header: "End", render: (row) => <span className="mono">{formatTimestamp(row.end_ts)}</span> },
                  { key: "rows", header: "Rows", align: "right", render: (row) => formatNumber(row.row_count) },
                  ...(selectedDataType === "ema" ? [{ key: "ema", header: "EMA", align: "right" as const, render: (row: Dataset) => <StatusBadge tone={hasEmaColumns(row) ? "pass" : "warn"}>{hasEmaColumns(row) ? "Ready" : "Missing"}</StatusBadge> }] : []),
                  { key: "status", header: "Status", align: "right", render: (row) => <StatusBadge tone={datasetStatusTone(row)}>{row.quality_status}</StatusBadge> }
                ]}
                getRowClassName={(row) => (row.dataset_id === selectedDataset?.dataset_id ? "is-selected" : undefined)}
                getRowKey={(row) => row.dataset_id}
                onRowClick={(row) => updateDataUrl({ asset: row.asset, dataType: selectedDataType, dataset: row.dataset_id })}
                rows={visibleDatasets}
              />
            </TerminalPanel>

            <div className="workbench-grid workbench-grid--wide-left">
              <TerminalPanel eyebrow={selectedDataset?.storage_backend ?? "storage"} title="Selected Dataset">
                {selectedDataset ? (
                  <div className="field-grid">
                    <FieldRow label="Asset" value={selectedDataset.asset} />
                    <FieldRow label="Instrument" value={selectedDataset.instrument} />
                    <FieldRow label="Type" value={selectedDataType === "ema" ? `ema / ${selectedDataset.data_origin} candles` : `${selectedDataset.data_type} / ${selectedDataset.data_origin}`} />
                    <FieldRow label="Timeframe" value={selectedDataset.timeframe ?? "event"} />
                    <FieldRow label="Start UTC" value={formatTimestamp(selectedDataset.start_ts)} />
                    <FieldRow label="End UTC" value={formatTimestamp(selectedDataset.end_ts)} />
                    <FieldRow label="Rows" value={formatNumber(selectedDataset.row_count)} />
                    <FieldRow label="Ingestion" value={selectedDataset.ingestion_version} />
                    <FieldRow label="Source of truth" value={selectedDataset.storage_backend === "parquet" ? "Parquet refs" : selectedDataset.storage_backend} />
                    <FieldRow label="Quality" value={selectedDataset.quality_status} />
                    {selectedDataType === "ema" ? <FieldRow label="EMA columns" value={hasEmaColumns(selectedDataset) ? "Ready" : "Missing"} /> : null}
                  </div>
                ) : (
                  <div className="state-line">No dataset selected.</div>
                )}
                {selectedDataset ? <div className="storage-uri mono">{selectedDataset.storage_uri}</div> : null}
              </TerminalPanel>

              <TerminalPanel title="Fill Result">
                <div className="state-card">
                  <Database aria-hidden="true" />
                  <span>{selectedRefreshError ? selectedRefreshError.message : refreshJob ? `${refreshJob.status}: ${refreshJob.current_step ?? refreshJob.job_type}` : refreshResultText(selectedRefreshResult)}</span>
                </div>
                {selectedRefreshResult?.derived_rebuilt?.length ? (
                  <div className="derived-list">
                    {selectedRefreshResult.derived_rebuilt.map((item) => (
                      <div className="field-row" key={item.dataset_id}>
                        <span>{item.timeframe}</span>
                        <strong>{formatNumber(item.row_count)} rows</strong>
                      </div>
                    ))}
                  </div>
                ) : null}
              </TerminalPanel>
            </div>

            <TerminalPanel title={selectedDataType === "ema" ? "EMA Preview" : featureCategoryForDataType(selectedDataType) ? "Feature Preview" : "Candle Preview"}>
              {rowPreviewQuery.isLoading ? <div className="state-line">Loading row preview...</div> : null}
              {rowPreviewQuery.error ? <div className="state-line state-line--error">{rowPreviewQuery.error.message}</div> : null}
              {rowPreviewQuery.data ? (
                <DataTable
                  columns={previewColumns(selectedDataType, rowPreviewQuery.data.rows)}
                  getRowKey={(row) => String(row.timestamp ?? row.ts ?? JSON.stringify(row))}
                  rows={rowPreviewQuery.data.rows}
                />
              ) : null}
            </TerminalPanel>
          </>
        }
      />
    </div>
  );
}

function previewColumns(selectedDataType: string, rows: Array<Record<string, unknown>>) {
  if (featureCategoryForDataType(selectedDataType)) {
    const sample = rows[0] ?? {};
    const featureKeys = Object.keys(sample).filter((key) => key !== "timestamp" && key !== "ts").slice(0, 8);
    return [
      { key: "timestamp", header: "Timestamp", render: (row: Record<string, unknown>) => <span className="mono">{formatTimestamp(String(row.timestamp ?? row.ts ?? ""))}</span> },
      ...featureKeys.map((key) => ({
        key,
        header: titleize(key),
        align: "right" as const,
        render: (row: Record<string, unknown>) => formatCompactValue(row[key])
      }))
    ];
  }
  return [
    { key: "timestamp", header: "Timestamp", render: (row: Record<string, unknown>) => <span className="mono">{formatTimestamp(String(row.timestamp ?? row.ts ?? ""))}</span> },
    { key: "open", header: "Open", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.open) },
    { key: "high", header: "High", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.high) },
    { key: "low", header: "Low", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.low) },
    { key: "close", header: "Close", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.close) },
    ...(selectedDataType === "ema" ? [
      { key: "ema_36", header: "EMA 36", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.ema_36) },
      { key: "ema_144", header: "EMA 144", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.ema_144) },
      { key: "ema_676", header: "EMA 676", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.ema_676) }
    ] : []),
    { key: "volume", header: "Volume", align: "right" as const, render: (row: Record<string, unknown>) => formatCompactValue(row.volume ?? row.vol) }
  ];
}
