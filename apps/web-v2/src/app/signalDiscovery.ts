export type DiscoveryDataset = {
  asset: string;
  instrument: string;
  data_type: string;
  timeframe: string | null;
  data_origin: string;
  storage_backend: string;
};

export type DiscoveryTicker = {
  key: string;
  asset: string;
  instrument: string;
  label: string;
};

type BracketPolicyCoordinates = {
  risk_pct: number;
  entry_delay_minutes: number;
  horizon_hours: number;
};

export function formatOpportunityGap(minutes: number | undefined): string {
  if (minutes === undefined || !Number.isFinite(minutes)) {
    return "n/a";
  }
  const totalMinutes = Math.max(0, Math.round(minutes));
  const days = Math.floor(totalMinutes / 1440);
  const hours = Math.floor((totalMinutes % 1440) / 60);
  const remainder = totalMinutes % 60;
  if (days > 0) {
    return `${days}d${hours > 0 ? ` ${hours}h` : ""}`;
  }
  if (hours > 0) {
    return `${hours}h${remainder > 0 ? ` ${remainder}m` : ""}`;
  }
  return `${remainder}m`;
}

export function shouldDisableAtlasRun({
  isSubmitting,
  hasActiveJob,
  targetFrozen,
  status
}: {
  isSubmitting: boolean;
  hasActiveJob: boolean;
  targetFrozen: boolean;
  status: string;
}): boolean {
  return isSubmitting
    || hasActiveJob
    || targetFrozen
    || !["draft", "atlas_ready", "failed"].includes(status);
}

export function isApprovedBracketRisk(
  policy: BracketPolicyCoordinates | undefined,
  riskPct: number
): boolean {
  return Boolean(policy && policy.risk_pct === riskPct);
}

export function getApprovedBracketCount(
  policy: BracketPolicyCoordinates | undefined,
  riskPct: number,
  diagnostics: { preview_total_brackets?: number } | undefined
): number | undefined {
  return isApprovedBracketRisk(policy, riskPct)
    ? diagnostics?.preview_total_brackets
    : undefined;
}

export function isApprovedBracketTarget(
  policy: BracketPolicyCoordinates | undefined,
  riskPct: number,
  entryDelayMinutes: number,
  horizonHours: number
): boolean {
  return Boolean(
    policy
    && policy.risk_pct === riskPct
    && policy.entry_delay_minutes === entryDelayMinutes
    && policy.horizon_hours === horizonHours
  );
}

export function isValidRewardMultiple(value: number): boolean {
  return Number.isFinite(value) && value > 0;
}

export function buildDiscoveryTickers(datasets: DiscoveryDataset[]): DiscoveryTicker[] {
  const tickers = new Map<string, DiscoveryTicker>();
  for (const dataset of datasets) {
    if (
      dataset.storage_backend !== "parquet" ||
      dataset.data_type !== "candles" ||
      dataset.timeframe !== "5m" ||
      dataset.data_origin !== "raw"
    ) {
      continue;
    }
    const key = `${dataset.asset}|${dataset.instrument}`;
    tickers.set(key, {
      key,
      asset: dataset.asset,
      instrument: dataset.instrument,
      label: `${dataset.asset} · ${dataset.instrument}`
    });
  }
  return Array.from(tickers.values()).sort((left, right) => (
    left.asset.localeCompare(right.asset) || left.instrument.localeCompare(right.instrument)
  ));
}

export function buildRiskGrid(minimum: number, maximum: number): number[] {
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum) || minimum <= 0 || maximum <= 0) {
    throw new Error("R range values must be finite and positive");
  }
  if (minimum > maximum) {
    throw new Error("R range minimum must not exceed maximum");
  }

  const minimumTenths = Math.ceil(minimum * 10 - Number.EPSILON);
  const maximumTenths = Math.floor(maximum * 10 + Number.EPSILON);
  if (minimumTenths <= 0 || minimumTenths > maximumTenths) {
    throw new Error("R range minimum must not exceed maximum after 0.1 normalization");
  }
  return Array.from(
    { length: maximumTenths - minimumTenths + 1 },
    (_, index) => (minimumTenths + index) / 10
  );
}

export function formatRiskGrid(values: number[]): string {
  if (values.length === 0) {
    return "n/a";
  }
  if (values.length === 1) {
    return `${values[0]}% R`;
  }
  const isContiguousTenthGrid = values.every((value, index) => (
    index === 0 || Math.abs(value - values[index - 1] - 0.1) < 1e-9
  ));
  if (isContiguousTenthGrid) {
    return `${values[0]}–${values[values.length - 1]}% R · 0.1% step`;
  }
  return `${values.join(" / ")}% R`;
}
