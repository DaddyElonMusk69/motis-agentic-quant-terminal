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
