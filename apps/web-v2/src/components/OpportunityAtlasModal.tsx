import { useEffect, useMemo, useRef, useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { Check, Focus, RotateCcw, SlidersHorizontal, X } from "lucide-react";
import {
  approveSignalDiscoveryBrackets,
  fetchSignalDiscoveryAtlasEpisode,
  fetchSignalDiscoveryAtlasVisualization,
  previewSignalDiscoveryBrackets,
  type SignalDiscoveryAtlasEpisode,
  type SignalDiscoveryBracketPolicy,
  type SignalDiscoveryRResult,
  type SignalDiscoverySession
} from "../app/api";
import {
  createDefaultBracketPolicy,
  replaceActiveAtlasLane
} from "../app/atlasVisualization";
import { formatNumber, formatTimestamp } from "../app/format";
import { FieldRow } from "./FieldRow";
import { OpportunityAtlasChart } from "./OpportunityAtlasChart";

type OpportunityAtlasModalProps = {
  session: SignalDiscoverySession;
  candidate: SignalDiscoveryRResult;
  selectedEntryDelayMinutes: number;
  selectedHorizonHours: number;
  onClose: () => void;
  onApproved: (session: SignalDiscoverySession) => void;
};

type AtlasWindow = { start: string; end: string } | null;

function percent(value: number | null | undefined, digits = 2): string {
  return value === null || value === undefined ? "n/a" : `${value.toFixed(digits)}%`;
}

function price(value: number | null | undefined): string {
  return value === null || value === undefined
    ? "n/a"
    : value.toLocaleString(undefined, { maximumFractionDigits: 4 });
}

function episodeWindow(
  session: SignalDiscoverySession,
  episode: SignalDiscoveryAtlasEpisode
): AtlasWindow {
  const padding = 12 * 60 * 60 * 1000;
  const start = Math.max(Date.parse(session.research_start), Date.parse(episode.start_ts) - padding);
  const end = Math.min(Date.parse(session.research_end), Date.parse(episode.end_ts) + padding);
  return { start: new Date(start).toISOString(), end: new Date(end).toISOString() };
}

export default function OpportunityAtlasModal({
  session,
  candidate,
  selectedEntryDelayMinutes,
  selectedHorizonHours,
  onClose,
  onApproved
}: OpportunityAtlasModalProps) {
  const dialogRef = useRef<HTMLElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const [window, setWindow] = useState<AtlasWindow>(null);
  const [selectedEpisode, setSelectedEpisode] = useState<SignalDiscoveryAtlasEpisode | null>(null);
  const approvedPolicy = session.summary.bracket_cleanup?.policy;
  const defaultPolicy = useMemo(
    () => createDefaultBracketPolicy(
      candidate.risk_pct,
      selectedEntryDelayMinutes,
      selectedHorizonHours
    ),
    [candidate.risk_pct, selectedEntryDelayMinutes, selectedHorizonHours]
  );
  const [policy, setPolicy] = useState<SignalDiscoveryBracketPolicy>(() => (
    approvedPolicy
    && approvedPolicy.risk_pct === candidate.risk_pct
    && approvedPolicy.entry_delay_minutes === selectedEntryDelayMinutes
    && approvedPolicy.horizon_hours === selectedHorizonHours
      ? approvedPolicy
      : defaultPolicy
  ));
  const [previewPolicy, setPreviewPolicy] = useState<SignalDiscoveryBracketPolicy>(policy);
  const visualizationQuery = useQuery({
    queryKey: [
      "signal-discovery-atlas-visualization",
      session.session_id,
      candidate.risk_pct,
      window?.start ?? "full",
      window?.end ?? "full"
    ],
    queryFn: () => fetchSignalDiscoveryAtlasVisualization({
      session_id: session.session_id,
      risk_pct: candidate.risk_pct,
      start: window?.start,
      end: window?.end,
      max_candles: window ? 5_000 : 4_000
    })
  });
  const episodeQuery = useQuery({
    enabled: Boolean(selectedEpisode && !selectedEpisode.episode_id.startsWith("bracket-")),
    queryKey: [
      "signal-discovery-atlas-episode",
      session.session_id,
      candidate.risk_pct,
      selectedEpisode?.episode_id
    ],
    queryFn: () => fetchSignalDiscoveryAtlasEpisode({
      session_id: session.session_id,
      risk_pct: candidate.risk_pct,
      episode_id: selectedEpisode!.episode_id
    })
  });
  const previewQuery = useQuery({
    enabled: session.status === "atlas_ready" || Boolean(session.summary.bracket_cleanup),
    queryKey: ["signal-discovery-bracket-preview", session.session_id, previewPolicy],
    queryFn: () => previewSignalDiscoveryBrackets({
      session_id: session.session_id,
      policy: previewPolicy
    }),
    placeholderData: (previous) => previous
  });
  const approveMutation = useMutation({
    mutationFn: approveSignalDiscoveryBrackets,
    onSuccess: (result) => onApproved(result.session)
  });
  const targetPct = candidate.risk_pct * (session.config.reward_multiple ?? 2);
  const stopPct = candidate.risk_pct * (session.config.stop_multiple ?? 1);
  const primaryScenario = {
    entry_delay_minutes: selectedEntryDelayMinutes,
    horizon_hours: selectedHorizonHours
  };
  const detail = episodeQuery.data;
  const episodeCount = candidate.primary?.episode_count ?? 0;
  const directionCounts = candidate.primary?.direction_counts ?? {};
  const diagnostics = previewQuery.data?.diagnostics;
  const zeroMonths = diagnostics
    ? Object.keys(diagnostics.raw_monthly_bracket_counts).filter(
        (month) => !diagnostics.monthly_bracket_counts[month]
      )
    : [];
  const previewBrackets = previewQuery.data?.brackets;
  const previewVisualization = useMemo(
    () => visualizationQuery.data && previewBrackets
      ? replaceActiveAtlasLane(
          visualizationQuery.data,
          previewBrackets,
          selectedEntryDelayMinutes,
          selectedHorizonHours
        )
      : visualizationQuery.data,
    [
      previewBrackets,
      selectedEntryDelayMinutes,
      selectedHorizonHours,
      visualizationQuery.data
    ]
  );
  const subtitle = useMemo(
    () => `${session.instrument} · ${formatNumber(episodeCount)} episodes`,
    [episodeCount, session.instrument]
  );

  useEffect(() => {
    const previousFocus = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
        return;
      }
      if (event.key !== "Tab" || !dialogRef.current) {
        return;
      }
      const focusable = Array.from(
        dialogRef.current.querySelectorAll<HTMLElement>(
          "button:not([disabled]), [href], input:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex='-1'])"
        )
      );
      if (focusable.length === 0) {
        event.preventDefault();
        return;
      }
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    };
    document.addEventListener("keydown", handleKeyDown);
    document.body.classList.add("has-modal-open");
    closeButtonRef.current?.focus();
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.classList.remove("has-modal-open");
      previousFocus?.focus();
    };
  }, [onClose]);

  useEffect(() => {
    setPolicy(
      approvedPolicy
      && approvedPolicy.risk_pct === candidate.risk_pct
      && approvedPolicy.entry_delay_minutes === selectedEntryDelayMinutes
      && approvedPolicy.horizon_hours === selectedHorizonHours
        ? approvedPolicy
        : defaultPolicy
    );
  }, [
    approvedPolicy,
    candidate.risk_pct,
    defaultPolicy,
    selectedEntryDelayMinutes,
    selectedHorizonHours
  ]);

  useEffect(() => {
    const timeout = globalThis.setTimeout(() => setPreviewPolicy(policy), 250);
    return () => globalThis.clearTimeout(timeout);
  }, [policy]);

  const selectEpisode = (episode: SignalDiscoveryAtlasEpisode) => {
    setSelectedEpisode(episode);
    setWindow(episodeWindow(session, episode));
  };

  const resetRange = () => {
    setWindow(null);
    setSelectedEpisode(null);
  };

  return (
    <div
      className="terminal-modal-backdrop opportunity-atlas-modal-backdrop"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
      role="presentation"
    >
      <section
        aria-labelledby="opportunity-atlas-title"
        aria-modal="true"
        className="terminal-modal opportunity-atlas-modal"
        ref={dialogRef}
        role="dialog"
      >
        <header className="terminal-modal__header opportunity-atlas-modal__header">
          <div>
            <span className="eyebrow">Opportunity Atlas · {candidate.risk_pct}% R</span>
            <h2 id="opportunity-atlas-title">{subtitle}</h2>
          </div>
          <div className="opportunity-atlas-modal__actions">
            {window ? (
              <button className="button button--secondary button--compact" onClick={resetRange} type="button">
                <RotateCcw aria-hidden="true" />
                Full range
              </button>
            ) : null}
            <button
              aria-label="Close opportunity atlas"
              className="icon-button"
              onClick={onClose}
              ref={closeButtonRef}
              type="button"
            >
              <X aria-hidden="true" />
            </button>
          </div>
        </header>
        <div className="opportunity-atlas-summary">
          <span><strong>{targetPct.toFixed(2)}%</strong> target</span>
          <span><strong>{stopPct.toFixed(2)}%</strong> stop</span>
          <span><strong>{primaryScenario?.entry_delay_minutes ?? "n/a"}m</strong> entry delay</span>
          <span><strong>{primaryScenario?.horizon_hours ?? "n/a"}h</strong> horizon</span>
          <span><strong>{formatNumber(diagnostics?.preview_total_brackets ?? candidate.primary?.episode_count)}</strong> brackets</span>
          <span><strong>{formatNumber(diagnostics?.preview_direction_counts.LONG ?? directionCounts.LONG)}</strong> long</span>
          <span><strong>{formatNumber(diagnostics?.preview_direction_counts.SHORT ?? directionCounts.SHORT)}</strong> short</span>
        </div>
        <div className="terminal-modal__body opportunity-atlas-modal__body has-inspector">
          <div className="opportunity-atlas-modal__visualization">
            {visualizationQuery.isPending ? (
              <div className="opportunity-atlas-chart-state">Loading atlas window…</div>
            ) : visualizationQuery.isError ? (
              <div className="opportunity-atlas-chart-state is-error">
                <span>{visualizationQuery.error.message}</span>
                <button className="button button--secondary button--compact" onClick={() => void visualizationQuery.refetch()} type="button">
                  Retry
                </button>
              </div>
            ) : visualizationQuery.data.candles.length === 0 ? (
              <div className="opportunity-atlas-chart-state">No candles in this research window.</div>
            ) : (
              <>
                <OpportunityAtlasChart
                  activeEntryDelayMinutes={selectedEntryDelayMinutes}
                  activeHorizonHours={selectedHorizonHours}
                  onEpisodeSelect={selectEpisode}
                  selectedEpisodeId={selectedEpisode?.episode_id ?? null}
                  visualization={previewVisualization ?? visualizationQuery.data}
                />
                <div className="opportunity-atlas-window-meta">
                  <span>{formatTimestamp(visualizationQuery.data.window_start)} – {formatTimestamp(visualizationQuery.data.window_end)}</span>
                  <span>{formatNumber(visualizationQuery.data.candles.length)} candles · {visualizationQuery.data.candle_interval_minutes}m</span>
                </div>
              </>
            )}
          </div>
          <aside className="opportunity-atlas-inspector opportunity-atlas-cleanup">
            <header>
              <div>
                <span className="eyebrow">Bracket Cleanup</span>
                <h3>{previewQuery.isFetching ? "Updating preview" : "Opportunity target"}</h3>
              </div>
              <SlidersHorizontal aria-hidden="true" />
            </header>

            <div className="opportunity-atlas-cleanup__counts">
              <div><strong>{formatNumber(diagnostics?.preview_total_brackets)}</strong><span>Total</span></div>
              <div><strong>{formatNumber(diagnostics?.preview_direction_counts.LONG)}</strong><span>Long</span></div>
              <div><strong>{formatNumber(diagnostics?.preview_direction_counts.SHORT)}</strong><span>Short</span></div>
              <div><strong>{formatNumber(diagnostics?.removed_bracket_count)}</strong><span>Removed</span></div>
              <div><strong>{formatNumber(diagnostics?.merged_gap_count)}</strong><span>Merged</span></div>
              <div><strong>{formatNumber(diagnostics?.overlap_suppressed_count)}</strong><span>Suppressed</span></div>
            </div>

            <div className="opportunity-atlas-cleanup__controls">
              <label className="toggle-row">
                <span>Require R stability</span>
                <input
                  checked={policy.require_r_stability}
                  disabled={session.config.risk_values.length < 2 || session.status !== "atlas_ready"}
                  onChange={(event) => setPolicy((current) => ({ ...current, require_r_stability: event.target.checked }))}
                  type="checkbox"
                />
              </label>
              <label className="toggle-row">
                <span>Require delay stability</span>
                <input
                  checked={policy.require_delay_stability}
                  disabled={session.config.entry_delays_minutes.length < 2 || session.status !== "atlas_ready"}
                  onChange={(event) => setPolicy((current) => ({ ...current, require_delay_stability: event.target.checked }))}
                  type="checkbox"
                />
              </label>
              <label className="toggle-row">
                <span>Bridge neutral gaps</span>
                <input
                  checked={policy.bridge_neutral_gap_intervals > 0}
                  disabled={session.status !== "atlas_ready"}
                  onChange={(event) => setPolicy((current) => ({
                    ...current,
                    bridge_neutral_gap_intervals: event.target.checked ? 1 : 0
                  }))}
                  type="checkbox"
                />
              </label>
              {policy.bridge_neutral_gap_intervals > 0 ? (
                <label className="slider-row">
                  <span>Maximum gap <strong>{policy.bridge_neutral_gap_intervals * 5}m</strong></span>
                  <input
                    max={12}
                    min={1}
                    onChange={(event) => setPolicy((current) => ({ ...current, bridge_neutral_gap_intervals: Number(event.target.value) }))}
                    type="range"
                    value={policy.bridge_neutral_gap_intervals}
                  />
                </label>
              ) : null}
              <label className="toggle-row">
                <span>Minimum persistence</span>
                <input
                  checked={policy.minimum_persistence_timestamps > 1}
                  disabled={session.status !== "atlas_ready"}
                  onChange={(event) => setPolicy((current) => ({
                    ...current,
                    minimum_persistence_timestamps: event.target.checked ? 2 : 1
                  }))}
                  type="checkbox"
                />
              </label>
              {policy.minimum_persistence_timestamps > 1 ? (
                <label className="slider-row">
                  <span>Minimum span <strong>{policy.minimum_persistence_timestamps * 5}m</strong></span>
                  <input
                    max={24}
                    min={2}
                    onChange={(event) => setPolicy((current) => ({ ...current, minimum_persistence_timestamps: Number(event.target.value) }))}
                    type="range"
                    value={policy.minimum_persistence_timestamps}
                  />
                </label>
              ) : null}
              <label className="toggle-row">
                <span>One active opportunity</span>
                <input
                  checked={policy.one_active_opportunity}
                  disabled={session.status !== "atlas_ready"}
                  onChange={(event) => setPolicy((current) => ({ ...current, one_active_opportunity: event.target.checked }))}
                  type="checkbox"
                />
              </label>
            </div>

            {previewQuery.isError ? (
              <div className="state-line state-line--risk">{previewQuery.error.message}</div>
            ) : null}
            {approveMutation.isError ? (
              <div className="state-line state-line--risk">{approveMutation.error.message}</div>
            ) : null}
            {zeroMonths.length > 0 ? (
              <div className="state-line state-line--risk">
                No opportunities in {zeroMonths.join(", ")}
              </div>
            ) : null}
            <div className="opportunity-atlas-cleanup__actions">
              <button
                className="button button--secondary button--compact"
                disabled={session.status !== "atlas_ready"}
                onClick={() => setPolicy(defaultPolicy)}
                type="button"
              >
                <RotateCcw aria-hidden="true" />
                Reset
              </button>
              <button
                className="button button--primary button--compact"
                disabled={session.status !== "atlas_ready" || !previewQuery.data?.brackets.length || approveMutation.isPending}
                onClick={() => approveMutation.mutate({ session_id: session.session_id, policy })}
                type="button"
              >
                <Check aria-hidden="true" />
                Approve brackets
              </button>
            </div>
            <div className="opportunity-atlas-cleanup__status">
              {session.summary.bracket_cleanup?.policy_hash
                ? `Approved r${session.summary.bracket_cleanup.revision ?? 1} · ${session.summary.bracket_cleanup.policy_hash.slice(0, 10)}`
                : "Draft preview"}
            </div>

            {selectedEpisode ? (
              <div className="opportunity-atlas-episode-detail">
                <header>
                  <div>
                    <span className="eyebrow">Selected Bracket</span>
                    <h3>{selectedEpisode.direction}</h3>
                  </div>
                  <Focus aria-hidden="true" />
                </header>
                <div className="opportunity-atlas-inspector__fields">
                  <FieldRow label="Start" value={formatTimestamp(selectedEpisode.start_ts)} />
                  <FieldRow label="End" value={formatTimestamp(selectedEpisode.end_ts)} />
                  <FieldRow label="Duration" value={`${selectedEpisode.duration_minutes}m`} />
                  <FieldRow label="Timestamps" value={formatNumber(selectedEpisode.timestamp_count)} />
                </div>
                {episodeQuery.isPending ? (
                  <div className="state-line">Loading bucket start…</div>
                ) : episodeQuery.isError ? (
                  <div className="state-line state-line--risk">{episodeQuery.error.message}</div>
                ) : detail ? (
                  <>
                    <h4>Bucket Start Snapshot</h4>
                    <div className="opportunity-atlas-inspector__fields">
                      <FieldRow label="Decision" value={formatTimestamp(detail.snapshot.decision_ts)} />
                      <FieldRow label="Entry" value={formatTimestamp(detail.snapshot.entry_ts ?? undefined)} />
                      <FieldRow label="Entry price" value={price(detail.snapshot.entry_price)} />
                      <FieldRow label="Outcome" value={detail.snapshot.path.outcome} />
                      <FieldRow label="Target" value={price(detail.snapshot.path.target_price)} />
                      <FieldRow label="Stop" value={price(detail.snapshot.path.stop_price)} />
                      <FieldRow label="First touch" value={formatTimestamp(detail.snapshot.path.first_touch_ts ?? undefined)} />
                      <FieldRow label="MFE" value={percent(detail.snapshot.path.mfe_pct)} />
                      <FieldRow label="MAE" value={percent(detail.snapshot.path.mae_pct)} />
                      <FieldRow label="Terminal" value={percent(detail.snapshot.path.terminal_return_pct)} />
                    </div>
                  </>
                ) : null}
              </div>
            ) : null}
          </aside>
        </div>
      </section>
    </div>
  );
}
