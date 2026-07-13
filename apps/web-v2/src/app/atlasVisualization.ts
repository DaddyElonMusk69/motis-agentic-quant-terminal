export type AtlasVisibleRange = {
  from: number;
  to: number;
};

export const atlasChartPalette = {
  background: "#122326",
  text: "#82979b",
  grid: "rgba(55, 78, 82, 0.55)",
  border: "#374e52",
  crosshair: "#82979b",
  up: "#2f9e74",
  down: "#c95858"
} as const;

type AtlasEpisodeRange = {
  start_ts: string;
  end_ts: string;
};

export type AtlasRelativePosition = {
  left: number;
  width: number;
};

export type AtlasBracketPolicy = {
  risk_pct: number;
  entry_delay_minutes: number;
  horizon_hours: number;
  require_r_stability: boolean;
  r_stability_radius: number;
  require_delay_stability: boolean;
  delay_agreement_pct: number;
  bridge_neutral_gap_intervals: number;
  minimum_persistence_timestamps: number;
  one_active_opportunity: boolean;
};

export function createDefaultBracketPolicy(
  riskPct: number,
  entryDelayMinutes: number,
  horizonHours: number
): AtlasBracketPolicy {
  return {
    risk_pct: riskPct,
    entry_delay_minutes: entryDelayMinutes,
    horizon_hours: horizonHours,
    require_r_stability: false,
    r_stability_radius: 1,
    require_delay_stability: false,
    delay_agreement_pct: 100,
    bridge_neutral_gap_intervals: 0,
    minimum_persistence_timestamps: 1,
    one_active_opportunity: false
  };
}

export function shouldShowBracketPreviewLoading(
  hasVisualization: boolean,
  isPreviewFetching: boolean
): boolean {
  return hasVisualization && isPreviewFetching;
}

export function replaceActiveAtlasLane<
  T extends { lanes: Array<{ entry_delay_minutes: number; horizon_hours: number; episodes: unknown[] }> },
  E
>(visualization: T, episodes: E[], entryDelayMinutes: number, horizonHours: number): T {
  return {
    ...visualization,
    lanes: visualization.lanes.map((lane) => (
      lane.entry_delay_minutes === entryDelayMinutes && lane.horizon_hours === horizonHours
        ? { ...lane, episodes }
        : lane
    ))
  };
}

type AtlasLaneLike = {
  entry_delay_minutes: number;
  horizon_hours: number;
};

export function clipEpisodeToRange(
  episode: AtlasEpisodeRange,
  range: AtlasVisibleRange
): AtlasVisibleRange | null {
  const episodeStart = Date.parse(episode.start_ts) / 1000;
  const episodeEnd = Date.parse(episode.end_ts) / 1000;
  const from = Math.max(episodeStart, range.from);
  const to = Math.min(episodeEnd, range.to);
  return from <= to ? { from, to } : null;
}

function timestampToLogicalPosition(
  timestamp: number,
  candleTimes: readonly number[]
): number | null {
  if (candleTimes.length === 0 || !Number.isFinite(timestamp)) {
    return null;
  }
  if (candleTimes.length === 1) {
    return 0;
  }

  let low = 0;
  let high = candleTimes.length;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (candleTimes[middle] < timestamp) {
      low = middle + 1;
    } else {
      high = middle;
    }
  }
  if (low < candleTimes.length && candleTimes[low] === timestamp) {
    return low;
  }

  const rightIndex = Math.min(Math.max(low, 1), candleTimes.length - 1);
  const leftIndex = rightIndex - 1;
  const interval = candleTimes[rightIndex] - candleTimes[leftIndex];
  if (interval <= 0) {
    return leftIndex;
  }
  return leftIndex + (timestamp - candleTimes[leftIndex]) / interval;
}

export function positionEpisodeOnLogicalRange(
  episode: AtlasEpisodeRange,
  candleTimes: readonly number[],
  range: AtlasVisibleRange
): AtlasRelativePosition | null {
  const span = range.to - range.from;
  const start = timestampToLogicalPosition(Date.parse(episode.start_ts) / 1000, candleTimes);
  const end = timestampToLogicalPosition(Date.parse(episode.end_ts) / 1000, candleTimes);
  if (span <= 0 || start === null || end === null || end < range.from || start > range.to) {
    return null;
  }
  const clippedStart = Math.max(start, range.from);
  const clippedEnd = Math.min(end, range.to);
  return {
    left: (clippedStart - range.from) / span,
    width: Math.max(0, (clippedEnd - clippedStart) / span)
  };
}

export function episodeFill(direction: string): string {
  if (direction === "LONG") {
    return "var(--atlas-long-fill)";
  }
  if (direction === "SHORT") {
    return "var(--atlas-short-fill)";
  }
  return "transparent";
}

export function sortAtlasLanes<T extends AtlasLaneLike>(lanes: readonly T[]): T[] {
  return [...lanes].sort(
    (left, right) =>
      left.horizon_hours - right.horizon_hours ||
      left.entry_delay_minutes - right.entry_delay_minutes
  );
}
