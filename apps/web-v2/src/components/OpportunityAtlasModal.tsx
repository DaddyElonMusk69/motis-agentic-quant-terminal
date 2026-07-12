import { useEffect, useMemo, useRef, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Focus, RotateCcw, X } from "lucide-react";
import {
  fetchSignalDiscoveryAtlasEpisode,
  fetchSignalDiscoveryAtlasVisualization,
  type SignalDiscoveryAtlasEpisode,
  type SignalDiscoveryRResult,
  type SignalDiscoverySession
} from "../app/api";
import { formatNumber, formatTimestamp } from "../app/format";
import { FieldRow } from "./FieldRow";
import { OpportunityAtlasChart } from "./OpportunityAtlasChart";

type OpportunityAtlasModalProps = {
  session: SignalDiscoverySession;
  candidate: SignalDiscoveryRResult;
  onClose: () => void;
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
  onClose
}: OpportunityAtlasModalProps) {
  const dialogRef = useRef<HTMLElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const [window, setWindow] = useState<AtlasWindow>(null);
  const [selectedEpisode, setSelectedEpisode] = useState<SignalDiscoveryAtlasEpisode | null>(null);
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
    enabled: Boolean(selectedEpisode),
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
  const targetPct = candidate.risk_pct * (session.config.reward_multiple ?? 2);
  const stopPct = candidate.risk_pct * (session.config.stop_multiple ?? 1);
  const primaryScenario = candidate.primary_scenario;
  const detail = episodeQuery.data;
  const episodeCount = candidate.primary?.episode_count ?? 0;
  const directionCounts = candidate.primary?.direction_counts ?? {};
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
          <span><strong>{formatNumber(directionCounts.LONG)}</strong> long</span>
          <span><strong>{formatNumber(directionCounts.SHORT)}</strong> short</span>
        </div>
        <div className={`terminal-modal__body opportunity-atlas-modal__body${detail ? " has-inspector" : ""}`}>
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
                  onEpisodeSelect={selectEpisode}
                  selectedEpisodeId={selectedEpisode?.episode_id ?? null}
                  visualization={visualizationQuery.data}
                />
                <div className="opportunity-atlas-window-meta">
                  <span>{formatTimestamp(visualizationQuery.data.window_start)} – {formatTimestamp(visualizationQuery.data.window_end)}</span>
                  <span>{formatNumber(visualizationQuery.data.candles.length)} candles · {visualizationQuery.data.candle_interval_minutes}m</span>
                </div>
              </>
            )}
          </div>
          {selectedEpisode ? (
            <aside className="opportunity-atlas-inspector">
              <header>
                <div>
                  <span className="eyebrow">Selected Episode</span>
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
            </aside>
          ) : null}
        </div>
      </section>
    </div>
  );
}
