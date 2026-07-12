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
