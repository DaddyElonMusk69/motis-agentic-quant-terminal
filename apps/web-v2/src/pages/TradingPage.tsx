import { useEffect, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Archive, ChevronLeft, ChevronRight, Play, RefreshCw, Send, Square, Trash2, X } from "lucide-react";
import {
  archiveTradingRoute,
  deleteArchivedTradingRoute,
  fetchArchivedTradingRoutes,
  fetchRouteExchangeHealth,
  fetchRouteWakes,
  fetchTradingRoutes,
  runRouteWake,
  startRouteLifecycle,
  stopRouteLifecycle,
  submitWakeOrders,
  updateRouteSettings,
  type DataFreshnessReport,
  type DataWarmupReport,
  type DeploymentRoute,
  type ExchangeHealth,
  type OrderIntent,
  type WakeRun
} from "../app/api";
import { formatNumber, formatTimestamp } from "../app/format";
import { queryClient } from "../app/queryClient";
import { useAppRouter } from "../app/router";
import { DataTable } from "../components/DataTable";
import { FieldRow } from "../components/FieldRow";
import { ListSkeleton } from "../components/ListSkeleton";
import { SplitPane } from "../components/SplitPane";
import { StatusBadge } from "../components/StatusBadge";
import { TerminalPanel } from "../components/TerminalPanel";

const STARTABLE_ROUTE_BLOCKERS = new Set(["route_disabled", "data_not_warmed", "route_not_manually_armed"]);
const WAKE_PAGE_SIZE = 25;
const LIVE_WAKE_POLL_MS = 5000;
const ROUTE_STATUS_POLL_MS = 10000;

function updateTradingUrl(routeId: string) {
  const params = new URLSearchParams(window.location.search);
  params.set("route", routeId);
  const nextUrl = `/trading?${params.toString()}`;
  if (`${window.location.pathname}${window.location.search}` === nextUrl) {
    return;
  }
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function clearTradingRouteUrl() {
  const params = new URLSearchParams(window.location.search);
  params.delete("route");
  const query = params.toString();
  const nextUrl = query ? `/trading?${query}` : "/trading";
  window.history.pushState(null, "", nextUrl);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function selectedRoute(routes: DeploymentRoute[], requested: string | null): DeploymentRoute | undefined {
  return routes.find((route) => route.route_id === requested) ?? routes[0];
}

function routeStatus(route: DeploymentRoute): { state: "running" | "blocked" | "ready" | "idle"; label: string; detail: string; tone: "live" | "warn" | "pass" | "idle" } {
  if (route.scheduler_status === "running") {
    return {
      state: "running",
      label: "Running",
      detail: route.auto_submit_enabled ? "Live order submission enabled" : "Intent-only scheduler running",
      tone: "live"
    };
  }
  const blockers = route.blockers ?? [];
  const hardBlockers = blockers.filter((blocker) => !STARTABLE_ROUTE_BLOCKERS.has(blocker));
  if (hardBlockers.length > 0) {
    return {
      state: "blocked",
      label: "Blocked",
      detail: hardBlockers.map(formatBlocker).join(", "),
      tone: "warn"
    };
  }
  if (route.enabled && route.data_warmed && (route.account_mode !== "live" || route.manually_armed)) {
    return {
      state: "ready",
      label: "Ready",
      detail: "Route gates are open",
      tone: "pass"
    };
  }
  return {
    state: "idle",
    label: "Idle",
    detail: blockers.length ? `Start will handle ${blockers.map(formatBlocker).join(", ")}` : "Ready to start",
    tone: "idle"
  };
}

function hasHardBlockers(route: DeploymentRoute): boolean {
  return (route.blockers ?? []).some((blocker) => !STARTABLE_ROUTE_BLOCKERS.has(blocker));
}

function routeActionLabel(route: DeploymentRoute): string {
  if (route.scheduler_status === "running") {
    return "Stop";
  }
  if (hasHardBlockers(route)) {
    return "Blocked";
  }
  return "Start";
}

function formatBlocker(value: string): string {
  const labels: Record<string, string> = {
    route_disabled: "route disabled",
    missing_active_bundle: "missing active bundle",
    route_not_promoted: "route not promoted",
    data_not_warmed: "data not warmed",
    route_not_manually_armed: "live route not armed"
  };
  return labels[value] ?? value.replaceAll("_", " ");
}

function readRecordValue(value: unknown, key: string): unknown {
  if (!value || typeof value !== "object") {
    return undefined;
  }
  return (value as Record<string, unknown>)[key];
}

function routeSetup(route: DeploymentRoute): Record<string, unknown> {
  const setup = route.active_bundle?.execution_setup;
  if (!setup || typeof setup !== "object") {
    return {};
  }
  const nested = readRecordValue(setup, "setup");
  return nested && typeof nested === "object" ? nested as Record<string, unknown> : setup as Record<string, unknown>;
}

function routeExecutionSetup(route: DeploymentRoute): Record<string, unknown> {
  const setup = route.active_bundle?.execution_setup;
  return setup && typeof setup === "object" ? setup as Record<string, unknown> : {};
}

function routeBundleSizing(route: DeploymentRoute): { margin_allocation_pct: number; leverage: number; source: string } {
  const executionSetup = routeExecutionSetup(route);
  const setup = routeSetup(route);
  const sizing = readRecordValue(executionSetup, "sizing");
  const sizingRecord = sizing && typeof sizing === "object" ? sizing as Record<string, unknown> : {};
  const margin = Number(
    readRecordValue(sizingRecord, "margin_allocation_pct")
    ?? readRecordValue(executionSetup, "margin_allocation_pct")
    ?? readRecordValue(setup, "margin_allocation_pct")
    ?? route.margin_allocation_pct
    ?? 0
  );
  const leverage = Number(
    readRecordValue(sizingRecord, "leverage")
    ?? readRecordValue(executionSetup, "leverage")
    ?? readRecordValue(setup, "leverage")
    ?? route.leverage
    ?? 0
  );
  return {
    margin_allocation_pct: Number.isFinite(margin) ? margin : 0,
    leverage: Number.isFinite(leverage) ? leverage : 0,
    source: String(readRecordValue(sizingRecord, "source") ?? "stage4 bundle")
  };
}

function effectiveRouteSizing(route: DeploymentRoute): { margin_allocation_pct: number; leverage: number; source: string; manual: boolean } {
  if (route.manual_sizing_enabled) {
    return {
      margin_allocation_pct: Number(route.margin_allocation_pct ?? 0),
      leverage: Number(route.leverage ?? 0),
      source: "manual override",
      manual: true
    };
  }
  return { ...routeBundleSizing(route), manual: false };
}

function pyramidMaxLegs(route: DeploymentRoute): number {
  const setup = routeSetup(route);
  const pyramid = readRecordValue(setup, "pyramid");
  const raw = pyramid && typeof pyramid === "object" ? readRecordValue(pyramid, "max_legs") : readRecordValue(setup, "max_legs");
  const legs = Number(raw ?? 1);
  return Number.isFinite(legs) && legs > 0 ? Math.max(1, Math.round(legs)) : 1;
}

function formatSetupValue(value: unknown, suffix = ""): string {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return `${formatNumber(number)}${suffix}`;
}

function formatPercent(value: unknown): string {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return "n/a";
  }
  return `${number.toFixed(number % 1 === 0 ? 0 : 1)}%`;
}

function formatAgeSeconds(value: unknown): string {
  const seconds = Number(value);
  if (!Number.isFinite(seconds)) {
    return "n/a";
  }
  if (seconds < 60) {
    return `${Math.max(0, Math.round(seconds))}s`;
  }
  return `${formatNumber(Math.round(seconds / 60))}m`;
}

type TradeSide = "LONG" | "SHORT";

function sidePolicy(setup: Record<string, unknown>, side: TradeSide): Record<string, unknown> | null {
  const sidePolicies = readRecordValue(setup, "side_policies");
  if (!sidePolicies || typeof sidePolicies !== "object") {
    return null;
  }
  const policy = readRecordValue(sidePolicies, side);
  return policy && typeof policy === "object" ? policy as Record<string, unknown> : null;
}

function formatTpSlPolicy(policy: Record<string, unknown>): string {
  const tp = readRecordValue(policy, "final_tp_pct") ?? readRecordValue(policy, "lock_profit_pct") ?? readRecordValue(policy, "tp_pct") ?? readRecordValue(policy, "tp");
  const sl = readRecordValue(policy, "initial_sl_pct") ?? readRecordValue(policy, "sl_pct") ?? readRecordValue(policy, "sl");
  return `${formatSetupValue(tp, "%")} / ${formatSetupValue(sl, "%")}`;
}

function routeTpSlRows(route: DeploymentRoute): Array<{ label: string; value: string }> {
  const setup = routeSetup(route);
  const long = sidePolicy(setup, "LONG");
  const short = sidePolicy(setup, "SHORT");
  if (long && short) {
    return [
      { label: "Long TP / SL", value: formatTpSlPolicy(long) },
      { label: "Short TP / SL", value: formatTpSlPolicy(short) }
    ];
  }
  return [{ label: "TP / SL", value: formatTpSlPolicy(setup) }];
}

function boolValue(value: unknown): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    return ["true", "1", "yes", "on"].includes(value.toLowerCase());
  }
  return Boolean(value);
}

function formatProtectionPolicy(policy: Record<string, unknown>): string {
  const enabled = boolValue(readRecordValue(policy, "protection_enabled"));
  if (!enabled) {
    return "Off";
  }
  const trigger = readRecordValue(policy, "protect_trigger_pct");
  const trail = readRecordValue(policy, "trail_sl_pct");
  return [
    "On",
    `trigger ${formatSetupValue(trigger, "%")}`,
    `trail ${formatSetupValue(trail, "%")}`
  ].filter(Boolean).join(" · ");
}

function routeProtectionRows(route: DeploymentRoute): Array<{ label: string; value: string }> {
  const setup = routeSetup(route);
  const long = sidePolicy(setup, "LONG");
  const short = sidePolicy(setup, "SHORT");
  if (long && short) {
    return [
      { label: "SL Protection", value: `Long ${formatProtectionPolicy(long)} | Short ${formatProtectionPolicy(short)}` }
    ];
  }
  return [{ label: "SL Protection", value: formatProtectionPolicy(setup) }];
}

function executionInterval(route: DeploymentRoute): string {
  if (typeof route.cron_interval_minutes === "number" && route.cron_interval_minutes > 0) {
    return `${route.cron_interval_minutes}m`;
  }
  return "15m";
}

function parseDateLike(value: unknown): Date | null {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }
  const normalized = /^\d{4}-\d{2}-\d{2}$/.test(value) ? `${value}T00:00:00Z` : value;
  const date = new Date(normalized);
  return Number.isNaN(date.getTime()) ? null : date;
}

function promotedTimestamp(route: DeploymentRoute): string | null {
  return route.active_bundle?.created_at ?? route.created_at ?? null;
}

function latestTrainingDate(route: DeploymentRoute): Date | null {
  const setup = route.active_bundle?.execution_setup ?? {};
  const directWindows = [
    readRecordValue(readRecordValue(setup, "walk_forward_window"), "end"),
    readRecordValue(readRecordValue(setup, "training_window"), "end"),
    readRecordValue(setup, "walk_forward_end"),
    readRecordValue(setup, "train_end")
  ];
  const sliceWindows = readRecordValue(setup, "slice_windows");
  const sliceEnds = Array.isArray(sliceWindows)
    ? sliceWindows.flatMap((window) => [
      readRecordValue(window, "end"),
      readRecordValue(window, "walk_forward_end"),
      readRecordValue(window, "test_end"),
      readRecordValue(window, "train_end")
    ])
    : [];
  const candidates = [...directWindows, ...sliceEnds, promotedTimestamp(route)]
    .map(parseDateLike)
    .filter((date): date is Date => Boolean(date));
  if (!candidates.length) {
    return null;
  }
  return new Date(Math.max(...candidates.map((date) => date.getTime())));
}

function daysSinceTraining(route: DeploymentRoute): number | null {
  const latest = latestTrainingDate(route);
  if (!latest) {
    return null;
  }
  return Math.max(0, Math.floor((Date.now() - latest.getTime()) / 86_400_000));
}

function trainingAgeLabel(route: DeploymentRoute): string {
  const days = daysSinceTraining(route);
  if (days === null) {
    return "age n/a";
  }
  return `${formatNumber(days)}d since training`;
}

function formatSignalScan(wake: WakeRun): string {
  const signalId = wake.signal_scan_result?.signal_id;
  const status = wake.signal_scan_result?.status;
  if (signalId) {
    return String(signalId);
  }
  return status ? String(status).replaceAll("_", " ") : "not scanned";
}

function formatDecision(wake: WakeRun): string {
  const decision = wake.strategy_decision ?? {};
  const action = decision.action ?? decision.trade_action;
  const reason = decision.reason_code;
  if (action && reason) {
    return `${String(action)} · ${String(reason)}`;
  }
  if (action) {
    return String(action);
  }
  return wake.status === "blocked" ? "Blocked" : "No decision";
}

function formatSnapshotValue(value: unknown): string {
  if (value === null || value === undefined) {
    return "n/a";
  }
  if (typeof value === "string" || typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }
  if (Array.isArray(value)) {
    return `${formatNumber(value.length)} rows`;
  }
  if (typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    return entries.length ? entries.slice(0, 2).map(([key, item]) => `${key}: ${String(item)}`).join(" · ") : "empty";
  }
  return String(value);
}

function effectiveDataFreshness(route: DeploymentRoute, warmup: DataWarmupReport | null): DataFreshnessReport | null {
  return warmup?.data_freshness ?? route.data_freshness ?? null;
}

function dataFreshnessSummary(report: DataFreshnessReport | null): string {
  if (!report) {
    return "Data freshness n/a";
  }
  const raw = report.raw_5m;
  const derived = report.derived_5m;
  const limit = formatAgeSeconds(report.max_age_seconds);
  const parts = [
    `raw ${raw?.timestamp ? formatTimestamp(raw.timestamp) : raw?.status ?? "n/a"} (${formatAgeSeconds(raw?.age_seconds)})`,
    derived ? `derived ${derived.timestamp ? formatTimestamp(derived.timestamp) : derived.status} (${formatAgeSeconds(derived.age_seconds)})` : null,
    `limit ${limit}`
  ].filter(Boolean);
  return parts.join(" · ");
}

function dataFreshnessLabel(report: DataFreshnessReport | null): string {
  if (!report) {
    return "Data unchecked";
  }
  if (report.status === "stale") {
    return `Data stale${report.reason ? `: ${String(report.reason).replaceAll("_", " ")}` : ""}`;
  }
  if (report.status === "fresh") {
    return "Data fresh";
  }
  return `Data ${report.status}`;
}

function pendingIntentForLatestWake(wake: WakeRun | null, route: DeploymentRoute | undefined): { wake: WakeRun; intent: OrderIntent } | null {
  if (!wake || route?.auto_submit_enabled) {
    return null;
  }
  const intent = wake.order_intents.find((item) => item.status === "intent_only");
  return intent ? { wake, intent } : null;
}

function failedIntentForLatestWake(wake: WakeRun | null): { wake: WakeRun; intent: OrderIntent } | null {
  if (!wake) {
    return null;
  }
  const intent = wake.order_intents.find((item) => item.status === "submission_failed");
  return intent ? { wake, intent } : null;
}

function requiresNotional(intent: OrderIntent | undefined): boolean {
  const action = String(intent?.action ?? "").toUpperCase();
  return !["EXIT", "REDUCE", "UPDATE_PROTECTION"].includes(action) && !intent?.reduce_only;
}

function invalidateTrading(routeId?: string) {
  void queryClient.invalidateQueries({ queryKey: ["trading-routes"] });
  if (routeId) {
    void queryClient.invalidateQueries({ queryKey: ["route-wakes", routeId] });
    void queryClient.invalidateQueries({ queryKey: ["route-exchange-health", routeId] });
  }
}

export function TradingPage() {
  const { searchParams } = useAppRouter();
  const [submitSizing, setSubmitSizing] = useState<Record<string, { quantity: string; notionalUsd: string }>>({});
  const [archivedOpen, setArchivedOpen] = useState(false);
  const [wakePage, setWakePage] = useState(0);
  const routesQuery = useQuery({
    queryKey: ["trading-routes"],
    queryFn: fetchTradingRoutes,
    refetchInterval: (query) => (query.state.data?.routes ?? []).some((item) => item.scheduler_status === "running") ? LIVE_WAKE_POLL_MS : ROUTE_STATUS_POLL_MS,
    refetchIntervalInBackground: true
  });
  const archivedRoutesQuery = useQuery({
    enabled: archivedOpen,
    queryKey: ["trading-routes", "archived"],
    queryFn: fetchArchivedTradingRoutes
  });
  const routes = routesQuery.data?.routes ?? [];
  const archivedRoutes = archivedRoutesQuery.data?.routes ?? [];
  const route = selectedRoute(routes, searchParams.get("route"));
  const routeRunning = route?.scheduler_status === "running";
  const wakePollingEnabled = Boolean(route?.route_id && routeRunning);

  const wakesQuery = useQuery({
    enabled: Boolean(route?.route_id),
    queryKey: ["route-wakes", route?.route_id, wakePage, WAKE_PAGE_SIZE],
    queryFn: () => fetchRouteWakes(route!.route_id, {
      limit: WAKE_PAGE_SIZE,
      offset: wakePage * WAKE_PAGE_SIZE
    }),
    refetchInterval: wakePollingEnabled ? LIVE_WAKE_POLL_MS : false,
    refetchIntervalInBackground: true
  });
  const latestWakesQuery = useQuery({
    enabled: Boolean(route?.route_id) && wakePage !== 0,
    queryKey: ["route-wakes-latest", route?.route_id],
    queryFn: () => fetchRouteWakes(route!.route_id, { limit: 1, offset: 0 }),
    refetchInterval: wakePollingEnabled ? LIVE_WAKE_POLL_MS : false,
    refetchIntervalInBackground: true
  });
  const healthQuery = useQuery({
    enabled: Boolean(route?.route_id),
    queryKey: ["route-exchange-health", route?.route_id],
    queryFn: () => fetchRouteExchangeHealth(route!.route_id)
  });

  const settingsMutation = useMutation({
    mutationFn: updateRouteSettings,
    onSuccess: (result) => invalidateTrading(result.route.route_id)
  });
  const startMutation = useMutation({
    mutationFn: startRouteLifecycle,
    onSuccess: (result) => {
      setWakePage(0);
      invalidateTrading(result.route.route_id);
    }
  });
  const stopMutation = useMutation({
    mutationFn: stopRouteLifecycle,
    onSuccess: (result) => invalidateTrading(result.route.route_id)
  });
  const wakeMutation = useMutation({
    mutationFn: runRouteWake,
    onSuccess: (result) => {
      setWakePage(0);
      invalidateTrading(result.route.route_id);
    }
  });
  const submitMutation = useMutation({
    mutationFn: submitWakeOrders,
    onSuccess: (result) => invalidateTrading(result.route.route_id)
  });
  const archiveMutation = useMutation({
    mutationFn: archiveTradingRoute,
    onSuccess: (result) => {
      invalidateTrading(result.route.route_id);
      void queryClient.invalidateQueries({ queryKey: ["trading-routes", "archived"] });
      if (searchParams.get("route") === result.route.route_id) {
        clearTradingRouteUrl();
      }
    }
  });
  const deleteArchivedMutation = useMutation({
    mutationFn: deleteArchivedTradingRoute,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: ["trading-routes"] });
      void queryClient.invalidateQueries({ queryKey: ["trading-routes", "archived"] });
    }
  });

  const wakes = wakesQuery.data?.wakes ?? [];
  const latestWake = wakePage === 0 ? wakes[0] ?? null : latestWakesQuery.data?.wakes[0] ?? null;
  const wakeTotal = wakesQuery.data?.total ?? wakes.length;
  const wakeOffset = wakesQuery.data?.offset ?? wakePage * WAKE_PAGE_SIZE;
  const wakeRangeStart = wakeTotal === 0 ? 0 : wakeOffset + 1;
  const wakeRangeEnd = Math.min(wakeOffset + wakes.length, wakeTotal);
  const hasPreviousWakes = wakeOffset > 0;
  const hasNextWakes = wakeRangeEnd < wakeTotal;
  const pending = pendingIntentForLatestWake(latestWake, route);
  const failedSubmission = failedIntentForLatestWake(latestWake);
  const startResult = startMutation.data;
  const wakeResult = wakeMutation.data;
  let warmup: DataWarmupReport | null = null;
  if (startResult && startResult.route.route_id === route?.route_id) {
    warmup = startResult.cycle?.warmup ?? null;
  } else if (wakeResult && wakeResult.route.route_id === route?.route_id) {
    warmup = wakeResult.warmup;
  }
  const startInProgress = Boolean(route && startMutation.isPending && startMutation.variables?.route_id === route.route_id);
  const stopInProgress = Boolean(route && stopMutation.isPending && stopMutation.variables === route.route_id);

  useEffect(() => {
    if (!searchParams.get("route") && route?.route_id) {
      updateTradingUrl(route.route_id);
    }
  }, [route?.route_id, searchParams]);

  useEffect(() => {
    setWakePage(0);
  }, [route?.route_id]);

  const toggleLifecycle = (targetRoute: DeploymentRoute | undefined) => {
    if (!targetRoute) {
      return;
    }
    if (targetRoute.scheduler_status === "running") {
      stopMutation.mutate(targetRoute.route_id);
      return;
    }
    const message = targetRoute.auto_submit_enabled
      ? `Start LIVE auto-submit execution for ${targetRoute.instrument}?`
      : `Start intent-only execution for ${targetRoute.instrument}?`;
    if (targetRoute.account_mode === "live" && !window.confirm(`${message} This will warm data, refresh signals, run the first wake, and start the scheduler.`)) {
      return;
    }
    startMutation.mutate({
      route_id: targetRoute.route_id,
      confirm_live: targetRoute.account_mode === "live",
      auto_submit_enabled: Boolean(targetRoute.auto_submit_enabled)
    });
  };

  const submitPending = () => {
    if (!route || !pending) {
      return;
    }
    const sizing = submitSizing[pending.wake.wake_id] ?? { quantity: "", notionalUsd: "" };
    const quantity = sizing.quantity.trim() || String(pending.intent.quantity ?? "").trim();
    const notionalUsd = Number(sizing.notionalUsd);
    if (!quantity) {
      window.alert("Enter OKX size before submitting.");
      return;
    }
    if (requiresNotional(pending.intent) && (!Number.isFinite(notionalUsd) || notionalUsd <= 0)) {
      window.alert("Enter audited USD notional before submitting.");
      return;
    }
    if (route.account_mode === "live" && !window.confirm(`Submit LIVE ${route.instrument} order intent from ${pending.wake.wake_id}?`)) {
      return;
    }
    submitMutation.mutate({
      route_id: route.route_id,
      wake_id: pending.wake.wake_id,
      confirm_live: route.account_mode === "live",
      quantity,
      notional_usd: requiresNotional(pending.intent) ? notionalUsd : undefined
    });
  };

  const errors = [
    routesQuery.error,
    wakesQuery.error,
    latestWakesQuery.error,
    healthQuery.error,
    settingsMutation.error,
    startMutation.error,
    stopMutation.error,
    wakeMutation.error,
    submitMutation.error,
    archiveMutation.error,
    archivedOpen ? archivedRoutesQuery.error : null
  ].filter(Boolean) as Error[];

  const archiveRoute = (targetRoute: DeploymentRoute) => {
    const message = targetRoute.scheduler_status === "running"
      ? `Archive ${targetRoute.instrument}? This will stop the route and hide it from active trading.`
      : `Archive ${targetRoute.instrument}? This will hide it from active trading.`;
    if (!window.confirm(message)) {
      return;
    }
    archiveMutation.mutate(targetRoute.route_id);
  };

  return (
    <div className="page page--workspace">
      <SplitPane
        className="split-pane--wide-list trading-split"
        workbenchClassName="trading-workbench"
        left={
          <>
            <div className="list-header">
              <span>Execution Routes</span>
              <div className="list-header-actions">
                {route ? <span className="selected-route-pill">{route.asset}</span> : null}
                <span className="count-pill">{formatNumber(routes.length)}</span>
                <button className="button button--secondary button--compact" onClick={() => setArchivedOpen(true)} type="button">
                  <Archive aria-hidden="true" />
                  Archived Strategies
                </button>
              </div>
            </div>
            {routesQuery.isLoading ? <ListSkeleton count={5} label="Loading promoted routes" variant="card" /> : null}
            {routes.length === 0 && !routesQuery.isLoading ? <div className="state-line">No promoted routes yet. Promote a Stage 4 bundle first.</div> : null}
            <div className="trading-route-list-v2">
              {routes.map((item) => (
                <RouteCard
                  health={item.route_id === route?.route_id ? healthQuery.data ?? null : null}
                  key={item.route_id}
                  latestWake={item.route_id === route?.route_id ? latestWake : null}
                  actionBusy={
                    (startMutation.isPending && startMutation.variables?.route_id === item.route_id)
                    || (stopMutation.isPending && stopMutation.variables === item.route_id)
                  }
                  onSelect={() => updateTradingUrl(item.route_id)}
                  onArchive={() => archiveRoute(item)}
                  route={item}
                  archiveBusy={archiveMutation.isPending && archiveMutation.variables === item.route_id}
                  selected={item.route_id === route?.route_id}
                  warmup={item.route_id === route?.route_id ? warmup : null}
                />
              ))}
            </div>
          </>
        }
        right={
          <>
            <div className="workbench-header trading-header-v2">
              <div>
                <span className="eyebrow">Live execution route</span>
                <h1>{route ? `${route.asset} / ${route.signal_engine_id}` : "Trading"}</h1>
              </div>
              <div className="header-actions">
                <ExchangeHealthPill health={healthQuery.data ?? null} loading={healthQuery.isLoading} />
                <button className="button button--secondary" disabled={!route || healthQuery.isFetching} onClick={() => void healthQuery.refetch()} type="button">
                  <RefreshCw aria-hidden="true" className={healthQuery.isFetching ? "spin-icon" : undefined} />
                  Check CLI
                </button>
              </div>
            </div>

            {errors.map((error) => <div className="state-line state-line--error" key={error.message}>{error.message}</div>)}

            {route ? (
              <>
                <ExecutionCommandCenter
                  onToggle={() => toggleLifecycle(route)}
                  route={route}
                  startInProgress={startInProgress}
                  stopInProgress={stopInProgress}
                  warmup={warmup}
                />
                {startInProgress ? (
                  <div className="progress-card trading-start-progress">
                    <div className="progress-card__header">
                      <strong>Starting {route.instrument}</strong>
                      <span>Warming data, refreshing signals, running first wake, then scheduling cron</span>
                    </div>
                    <div className="progress-rail" aria-label="Route start in progress">
                      <span />
                    </div>
                    <div className="progress-steps">
                      <span>Warm data</span>
                      <span>Refresh signals</span>
                      <span>Run wake</span>
                      <span>Start scheduler</span>
                    </div>
                  </div>
                ) : null}

                {failedSubmission ? (
                  <TerminalPanel title="Execution Error">
                    <div className="state-line state-line--error">
                      Automatic {String(failedSubmission.intent.action ?? "order").toLowerCase().replaceAll("_", " ")} failed
                      {failedSubmission.intent.submission_error?.code ? ` (OKX ${failedSubmission.intent.submission_error.code})` : ""}. The scheduler will retry from a fresh wake.
                    </div>
                  </TerminalPanel>
                ) : null}

                {pending ? (
                  <TerminalPanel title="Pending Intent">
                    <div className="pending-intent-v2">
                      <div>
                        <span className="eyebrow">Manual submission path</span>
                        <strong>{String(pending.intent.action ?? "ORDER")} · {pending.wake.wake_id}</strong>
                        <small>Only shown because this wake produced intent-only orders. Normal live execution uses the Start control with Live Orders enabled.</small>
                      </div>
                      <input
                        aria-label="OKX size"
                        placeholder="OKX size"
                        value={(submitSizing[pending.wake.wake_id] ?? { quantity: pending.intent.quantity ?? "", notionalUsd: "" }).quantity}
                        onChange={(event) => setSubmitSizing((current) => ({
                          ...current,
                          [pending.wake.wake_id]: {
                            quantity: event.target.value,
                            notionalUsd: current[pending.wake.wake_id]?.notionalUsd ?? ""
                          }
                        }))}
                      />
                      <input
                        aria-label="USD notional"
                        disabled={!requiresNotional(pending.intent)}
                        placeholder={requiresNotional(pending.intent) ? "USD notional" : "Not required"}
                        value={(submitSizing[pending.wake.wake_id] ?? { quantity: "", notionalUsd: "" }).notionalUsd}
                        onChange={(event) => setSubmitSizing((current) => ({
                          ...current,
                          [pending.wake.wake_id]: {
                            quantity: current[pending.wake.wake_id]?.quantity ?? "",
                            notionalUsd: event.target.value
                          }
                        }))}
                      />
                      <button className="button button--primary" disabled={submitMutation.isPending} onClick={submitPending} type="button">
                        <Send aria-hidden="true" />
                        Submit
                      </button>
                    </div>
                  </TerminalPanel>
                ) : null}

                <div className="execution-detail-grid-v2">
                  <section className="execution-detail-section-v2">
                    <ExecutionSettings compact route={route} saving={settingsMutation.isPending} onSave={(settings) => settingsMutation.mutate({ route_id: route.route_id, ...settings })} />
                  </section>
                  <section className="execution-detail-section-v2">
                    <div className="execution-section-heading">
                      <span>promoted bundle</span>
                      <strong>Strategy Readout</strong>
                    </div>
                    <BundleReadout route={route} />
                  </section>
                </div>

                <TerminalPanel
                  className="scroll-panel trading-wake-history-panel"
                  title={
                    <span className="panel-title-with-dot">
                      Wake History
                      {wakePollingEnabled ? <span className="live-poll-dot" aria-label="Live polling" /> : null}
                    </span>
                  }
                  actions={
                    <div className="header-actions">
                      <div className="wake-pagination-v2" aria-label="Wake history pagination">
                        <span>{formatNumber(wakeRangeStart)}-{formatNumber(wakeRangeEnd)} of {formatNumber(wakeTotal)}</span>
                        <button
                          className="icon-button"
                          disabled={!route || !hasPreviousWakes || wakesQuery.isFetching}
                          onClick={() => setWakePage((page) => Math.max(0, page - 1))}
                          type="button"
                          aria-label="Previous wake page"
                        >
                          <ChevronLeft aria-hidden="true" />
                        </button>
                        <button
                          className="icon-button"
                          disabled={!route || !hasNextWakes || wakesQuery.isFetching}
                          onClick={() => setWakePage((page) => page + 1)}
                          type="button"
                          aria-label="Next wake page"
                        >
                          <ChevronRight aria-hidden="true" />
                        </button>
                      </div>
                      <button className="button button--secondary" disabled={!route || wakeMutation.isPending} onClick={() => wakeMutation.mutate(route.route_id)} type="button">
                        <Play aria-hidden="true" />
                        {wakeMutation.isPending ? "Running wake" : "Run One Wake"}
                      </button>
                      <button className="icon-button" disabled={!route || wakesQuery.isFetching} onClick={() => void wakesQuery.refetch()} type="button" aria-label="Refresh wake history">
                        <RefreshCw aria-hidden="true" className={wakesQuery.isFetching ? "spin-icon" : undefined} />
                      </button>
                    </div>
                  }
                >
                  <DataTable
                    columns={[
                      { key: "time", header: "Time", render: (wake) => <span className="mono">{formatTimestamp(wake.completed_at ?? wake.started_at)}</span> },
                      { key: "branch", header: "Branch", render: (wake) => wake.branch },
                      { key: "decision", header: "Decision", render: (wake) => formatDecision(wake) },
                      { key: "signal", header: "Signal", render: (wake) => formatSignalScan(wake) },
                      { key: "intents", header: "Intents", align: "right", render: (wake) => formatNumber(wake.order_intents.length) },
                      { key: "status", header: "Status", align: "right", render: (wake) => <StatusBadge tone={wake.status === "completed" ? "pass" : wake.status === "blocked" ? "warn" : "idle"}>{wake.status}</StatusBadge> }
                    ]}
                    getRowKey={(wake) => wake.wake_id}
                    rows={wakes}
                    loading={wakesQuery.isLoading}
                    loadingLabel="Loading wake history"
                    loadingRowCount={8}
                    emptyLabel={route ? "No wake history for this route yet." : "Select a route to load wake history."}
                  />
                  {wakesQuery.isFetching && !wakesQuery.isLoading && wakes.length > 0 ? (
                    <div className="state-line state-line--subtle">Refreshing wake history...</div>
                  ) : null}
                </TerminalPanel>
              </>
            ) : null}
          </>
        }
      />
      {archivedOpen ? (
        <ArchivedStrategiesModal
          deleteBusyRouteId={deleteArchivedMutation.isPending ? deleteArchivedMutation.variables : null}
          loading={archivedRoutesQuery.isLoading}
          onClose={() => setArchivedOpen(false)}
          onDelete={(targetRoute) => {
            if (
              window.confirm(
                `Delete archived strategy ${targetRoute.asset} / ${targetRoute.signal_engine_id}? This permanently deletes the archived route, execution bundle, wake history, owner state history, and bundle artifacts.`
              )
            ) {
              deleteArchivedMutation.mutate(targetRoute.route_id);
            }
          }}
          routes={archivedRoutes}
        />
      ) : null}
    </div>
  );
}

function ExecutionCommandCenter({
  onToggle,
  route,
  startInProgress,
  stopInProgress,
  warmup
}: {
  onToggle: () => void;
  route: DeploymentRoute;
  startInProgress: boolean;
  stopInProgress: boolean;
  warmup: DataWarmupReport | null;
}) {
  const status = routeStatus(route);
  const freshness = effectiveDataFreshness(route, warmup);
  const actionDisabled = startInProgress || stopInProgress || (route.scheduler_status !== "running" && hasHardBlockers(route));
  return (
    <section className={`execution-command-center execution-command-center--${status.state}`}>
      <div className="execution-command-center__identity">
        <span className="eyebrow">Selected Route</span>
        <div className="execution-command-center__title">
          <strong>{route.instrument}</strong>
          <StatusBadge tone={status.tone}>{status.label}</StatusBadge>
        </div>
        <p>{status.detail}</p>
      </div>

      <div className="execution-command-center__action">
        <button
          className={route.scheduler_status === "running" ? "button button--danger execution-command-center__button" : "button button--primary execution-command-center__button"}
          disabled={actionDisabled}
          onClick={onToggle}
          type="button"
        >
          {route.scheduler_status === "running" ? <Square aria-hidden="true" /> : <Play aria-hidden="true" />}
          {startInProgress ? "Starting" : stopInProgress ? "Stopping" : routeActionLabel(route)}
        </button>
      </div>

      <div className="execution-command-center__metrics">
        <div>
          <span>Data</span>
          <strong className={freshness?.status === "stale" ? "tone-warn" : freshness?.status === "fresh" ? "tone-pass" : undefined}>{dataFreshnessLabel(freshness)}</strong>
        </div>
        <div>
          <span>Next Wake</span>
          <strong>{formatTimestamp(route.next_wake_at)}</strong>
        </div>
      </div>
    </section>
  );
}

function RouteCard({
  actionBusy,
  archiveBusy,
  health,
  latestWake,
  onArchive,
  onSelect,
  route,
  selected,
  warmup
}: {
  actionBusy: boolean;
  archiveBusy: boolean;
  health: ExchangeHealth | null;
  latestWake: WakeRun | null;
  onArchive: () => void;
  onSelect: () => void;
  route: DeploymentRoute;
  selected: boolean;
  warmup: DataWarmupReport | null;
}) {
  const status = routeStatus(route);
  const legs = pyramidMaxLegs(route);
  const sizing = effectiveRouteSizing(route);
  const margin = Number(sizing.margin_allocation_pct ?? 0);
  const perLegMarginPct = legs > 0 ? margin / legs : margin;
  const tpSlRows = routeTpSlRows(route);
  const freshness = effectiveDataFreshness(route, warmup);
  const setupLine = [
    tpSlRows.map((r) => r.value).join(" / "),
    `${formatNumber(legs)} legs`,
    `${formatPercent(perLegMarginPct)} @ ${formatNumber(sizing.leverage)}x`
  ].join(" · ");

  const cardClass = [
    "route-card-v2",
    `route-card-v2--${status.state}`,
    selected ? "is-selected" : ""
  ].filter(Boolean).join(" ");

  return (
    <article className={cardClass}>
      <button className="route-card-v2__header" onClick={onSelect} type="button" aria-current={selected ? "true" : undefined}>
        <div className="route-card-v2__top">
          <div className="route-card-v2__identity">
            <strong>{route.asset}</strong>
            <span>{route.signal_engine_id}</span>
          </div>
          <div className="route-card-v2__badges">
            <StatusBadge tone={status.tone}>{status.label}</StatusBadge>
          </div>
        </div>
        <div className="route-card-v2__sub">
          {route.instrument} · {route.account_mode} · {executionInterval(route)}
        </div>
        <div className="route-card-v2__meta">
          Promoted {formatTimestamp(promotedTimestamp(route))} · {trainingAgeLabel(route)}{health ? ` · ${health.connected ? "CLI OK" : "CLI check"}` : ""}
        </div>
      </button>

      <div className="route-card-v2__compact-grid">
        <div className={`route-card-v2__data-freshness route-card-v2__data-freshness--${freshness?.status === "stale" ? "stale" : freshness?.status === "fresh" ? "fresh" : "unknown"}`}>
          <span>{dataFreshnessLabel(freshness)}</span>
          <small>{freshness?.raw_5m?.timestamp ? formatTimestamp(freshness.raw_5m.timestamp) : "raw n/a"}</small>
        </div>
        <div className="route-card-v2__decision">
          <span>Last decision</span>
          <strong>{latestWake ? formatDecision(latestWake) : status.detail}</strong>
        </div>
      </div>
      <div className="route-card-v2__setup-line">{setupLine}</div>

      <div className="route-card-v2__footer">
        <span className={`route-card-v2__status ${status.state === "blocked" ? "tone-warn" : status.state === "running" ? "tone-pass" : ""}`}>
          {status.detail}
        </span>
        <div className="route-card-v2__actions route-card-v2__actions--quiet">
          <button
            className="route-card-v2__archive-btn"
            disabled={archiveBusy || actionBusy}
            onClick={onArchive}
            type="button"
            aria-label="Archive route"
          >
            <Archive aria-hidden="true" />
          </button>
        </div>
      </div>
    </article>
  );
}

function ArchivedStrategiesModal({ deleteBusyRouteId, loading, onClose, onDelete, routes }: {
  deleteBusyRouteId: string | null;
  loading: boolean;
  onClose: () => void;
  onDelete: (route: DeploymentRoute) => void;
  routes: DeploymentRoute[];
}) {
  return (
    <div className="modal-backdrop" role="presentation">
      <section className="terminal-modal archived-strategies-modal" role="dialog" aria-modal="true" aria-labelledby="archived-strategies-title">
        <header className="terminal-modal__header">
          <div>
            <span className="eyebrow">Trading archive</span>
            <h2 id="archived-strategies-title">Archived Strategies</h2>
          </div>
          <button className="icon-button" onClick={onClose} type="button" aria-label="Close archived strategies">
            <X aria-hidden="true" />
          </button>
        </header>
        <div className="terminal-modal__body">
          {!loading && routes.length === 0 ? <div className="state-line">No archived strategies yet.</div> : null}
          {loading || routes.length > 0 ? (
            <DataTable
              columns={[
                { key: "route", header: "Strategy", render: (item) => <strong>{item.asset} / {item.signal_engine_id}</strong> },
                { key: "instrument", header: "Instrument", render: (item) => item.instrument },
                { key: "promoted", header: "Promoted", render: (item) => <span className="mono">{formatTimestamp(promotedTimestamp(item))}</span> },
                { key: "age", header: "Age", render: (item) => trainingAgeLabel(item) },
                { key: "archived", header: "Archived", render: (item) => <span className="mono">{formatTimestamp(item.archived_at)}</span> },
                {
                  key: "actions",
                  header: "",
                  render: (item) => (
                    <button
                      className="button button--danger"
                      disabled={deleteBusyRouteId === item.route_id}
                      onClick={() => onDelete(item)}
                      type="button"
                    >
                      <Trash2 aria-hidden="true" />
                      {deleteBusyRouteId === item.route_id ? "Deleting" : "Delete"}
                    </button>
                  )
                }
              ]}
              getRowKey={(item) => item.route_id}
              rows={routes}
              loading={loading}
              loadingLabel="Loading archived strategies"
              loadingRowCount={6}
              emptyLabel="No archived strategies yet."
            />
          ) : null}
        </div>
        <footer className="terminal-modal__footer">
          <span>{formatNumber(routes.length)} archived routes</span>
          <button className="button button--secondary" onClick={onClose} type="button">Close</button>
        </footer>
      </section>
    </div>
  );
}

function ExchangeHealthPill({ health, loading }: { health: ExchangeHealth | null; loading: boolean }) {
  if (loading) {
    return <StatusBadge tone="idle">Checking OKX CLI</StatusBadge>;
  }
  if (health?.connected) {
    return <StatusBadge tone="pass">OKX CLI Connected</StatusBadge>;
  }
  if (health?.status === "blocked") {
    return <StatusBadge tone="warn">OKX CLI Blocked</StatusBadge>;
  }
  return <StatusBadge tone="idle">OKX CLI Unchecked</StatusBadge>;
}

function ExecutionSettings({ compact = false, onSave, route, saving }: {
  compact?: boolean;
  onSave: (settings: { cron_interval_minutes: number; execution_adapter: string; exchange_account: string; margin_allocation_pct: number; leverage: number; manual_sizing_enabled: boolean; auto_submit_enabled: boolean }) => void;
  route: DeploymentRoute;
  saving: boolean;
}) {
  const bundleSizing = routeBundleSizing(route);
  const effectiveSizing = effectiveRouteSizing(route);
  const [cron, setCron] = useState(String(route.cron_interval_minutes ?? 5));
  const [exchange, setExchange] = useState(route.execution_adapter ?? "okx");
  const [account, setAccount] = useState(route.exchange_account ?? "default");
  const [manualSizing, setManualSizing] = useState(Boolean(route.manual_sizing_enabled));
  const [margin, setMargin] = useState(Number(route.margin_allocation_pct ?? bundleSizing.margin_allocation_pct ?? 10));
  const [leverage, setLeverage] = useState(Number(route.leverage ?? bundleSizing.leverage ?? 1));
  const [autoSubmit, setAutoSubmit] = useState(Boolean(route.auto_submit_enabled));

  useEffect(() => {
    setCron(String(route.cron_interval_minutes ?? 5));
    setExchange(route.execution_adapter ?? "okx");
    setAccount(route.exchange_account ?? "default");
    setManualSizing(Boolean(route.manual_sizing_enabled));
    setMargin(Number(route.margin_allocation_pct ?? bundleSizing.margin_allocation_pct ?? 10));
    setLeverage(Number(route.leverage ?? bundleSizing.leverage ?? 1));
    setAutoSubmit(Boolean(route.auto_submit_enabled));
  }, [
    route.route_id,
    route.cron_interval_minutes,
    route.execution_adapter,
    route.exchange_account,
    route.margin_allocation_pct,
    route.leverage,
    route.manual_sizing_enabled,
    route.auto_submit_enabled,
    bundleSizing.margin_allocation_pct,
    bundleSizing.leverage
  ]);

  const cronMinutes = Number(cron);
  const savedManualSizing = Boolean(route.manual_sizing_enabled);
  const dirty = cronMinutes !== route.cron_interval_minutes
    || exchange !== route.execution_adapter
    || account !== route.exchange_account
    || manualSizing !== savedManualSizing
    || (manualSizing && margin !== route.margin_allocation_pct)
    || (manualSizing && leverage !== route.leverage)
    || autoSubmit !== route.auto_submit_enabled;
  const valid = Number.isInteger(cronMinutes) && cronMinutes >= 1 && cronMinutes <= 1440 && margin >= 0.1 && margin <= 100 && leverage >= 1 && leverage <= 125;
  const displayMargin = manualSizing ? margin : bundleSizing.margin_allocation_pct;
  const displayLeverage = manualSizing ? leverage : bundleSizing.leverage;
  const toggleManualSizing = (enabled: boolean) => {
    setManualSizing(enabled);
    if (enabled && !manualSizing) {
      setMargin(bundleSizing.margin_allocation_pct || Number(route.margin_allocation_pct ?? 10));
      setLeverage(bundleSizing.leverage || Number(route.leverage ?? 1));
    }
  };

  const saveButton = (
    <button
      className="button button--secondary execution-settings-save-v2"
      disabled={!dirty || !valid || saving}
      onClick={() => onSave({
        cron_interval_minutes: cronMinutes,
        execution_adapter: exchange,
        exchange_account: account,
        margin_allocation_pct: margin,
        leverage,
        manual_sizing_enabled: manualSizing,
        auto_submit_enabled: autoSubmit
      })}
      type="button"
    >
      {saving ? "Saving" : "Save Setup"}
    </button>
  );

  return (
    <div className="execution-settings-block-v2">
      {compact ? (
        <div className="execution-section-heading execution-section-heading--action">
          <span>risk and schedule</span>
          <strong>Execution Setup</strong>
          {saveButton}
        </div>
      ) : null}
      <div className={compact ? "execution-settings-v2 execution-settings-v2--compact" : "execution-settings-v2"}>
        <label className="execution-field-v2">
          <span>Cron</span>
          <input min="1" max="1440" type="number" value={cron} onChange={(event) => setCron(event.target.value)} />
        </label>
        <label className="execution-field-v2">
          <span>Exchange</span>
          <select value={exchange} onChange={(event) => setExchange(event.target.value)}>
            <option value="okx">OKX</option>
          </select>
        </label>
        <label className="slider-row-v2">
          <span>
            <span>Margin</span>
            <strong>{formatPercent(displayMargin)}</strong>
          </span>
          {manualSizing ? (
            <input min="1" max="100" step="1" type="range" value={margin} onChange={(event) => setMargin(Number(event.target.value))} />
          ) : (
            <em>{formatPercent(bundleSizing.margin_allocation_pct)} from Stage 4</em>
          )}
        </label>
        <label className="slider-row-v2">
          <span>
            <span>Leverage</span>
            <strong>{formatNumber(displayLeverage)}x</strong>
          </span>
          {manualSizing ? (
            <input min="1" max="20" step="1" type="range" value={leverage} onChange={(event) => setLeverage(Number(event.target.value))} />
          ) : (
            <em>{formatNumber(bundleSizing.leverage)}x from Stage 4</em>
          )}
        </label>
        <label className="execution-toggle-row-v2">
          <span>
            <strong>Manual Sizing</strong>
          </span>
          <input aria-label="Manual sizing override" type="checkbox" checked={manualSizing} onChange={(event) => toggleManualSizing(event.target.checked)} />
        </label>
        <label className="execution-toggle-row-v2">
          <span>
            <strong>Live Orders</strong>
          </span>
          <input aria-label="Live orders" type="checkbox" checked={autoSubmit} onChange={(event) => setAutoSubmit(event.target.checked)} />
        </label>
        {compact ? null : saveButton}
      </div>
    </div>
  );
}

function BundleReadout({ route }: { route: DeploymentRoute }) {
  const setup = routeSetup(route);
  const legs = pyramidMaxLegs(route);
  const sizing = effectiveRouteSizing(route);
  const margin = Number(sizing.margin_allocation_pct ?? 0);
  const perLegMarginPct = legs > 0 ? margin / legs : margin;
  const tpSlRows = routeTpSlRows(route);
  const protectionRows = routeProtectionRows(route);
  return (
    <div className="strategy-readout-grid">
      {tpSlRows.map((row) => (
        <div className="strategy-readout-grid__item" key={row.label}>
          <span>{row.label}</span>
          <strong>{row.value}</strong>
        </div>
      ))}
      {protectionRows.map((row) => (
        <div className="strategy-readout-grid__item strategy-readout-grid__item--wide" key={row.label}>
          <span>{row.label}</span>
          <strong>{row.value}</strong>
        </div>
      ))}
      <div className="strategy-readout-grid__item">
        <span>Pyramid</span>
        <strong>{formatNumber(legs)} legs</strong>
      </div>
      <div className="strategy-readout-grid__item">
        <span>Step</span>
        <strong>{formatSetupValue(readRecordValue(readRecordValue(setup, "pyramid"), "step_pct") ?? readRecordValue(setup, "step_pct"), "%")}</strong>
      </div>
      <div className="strategy-readout-grid__item">
        <span>Hold Gate</span>
        <strong>{formatSetupValue(setup.max_hold_hours ?? readRecordValue(route.active_bundle?.execution_setup, "hard_exit_after_hours"), "h")}</strong>
      </div>
      <div className="strategy-readout-grid__item">
        <span>Per-Leg Margin</span>
        <strong>{formatPercent(perLegMarginPct)}</strong>
      </div>
    </div>
  );
}
