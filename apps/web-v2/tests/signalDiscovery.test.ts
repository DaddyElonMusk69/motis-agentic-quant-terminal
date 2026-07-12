import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDiscoveryTickers,
  buildRiskGrid,
  formatRiskGrid,
  isValidRewardMultiple
} from "../src/app/signalDiscovery.ts";
import {
  atlasChartPalette,
  clipEpisodeToRange,
  episodeFill,
  positionEpisodeOnLogicalRange,
  sortAtlasLanes
} from "../src/app/atlasVisualization.ts";

test("buildRiskGrid expands an inclusive range in stable 0.1 increments", () => {
  assert.deepEqual(buildRiskGrid(0.6, 1.0), [0.6, 0.7, 0.8, 0.9, 1.0]);
  assert.deepEqual(buildRiskGrid(0.75, 1.05), [0.8, 0.9, 1.0]);
});

test("buildRiskGrid rejects nonpositive and descending ranges", () => {
  assert.throws(() => buildRiskGrid(0, 1), /positive/);
  assert.throws(() => buildRiskGrid(1.2, 0.8), /minimum/);
});

test("discovery target multiple accepts only finite positive values", () => {
  assert.equal(isValidRewardMultiple(0.75), true);
  assert.equal(isValidRewardMultiple(3.25), true);
  assert.equal(isValidRewardMultiple(0), false);
  assert.equal(isValidRewardMultiple(-1), false);
  assert.equal(isValidRewardMultiple(Number.NaN), false);
  assert.equal(isValidRewardMultiple(Number.POSITIVE_INFINITY), false);
});

test("formatRiskGrid compacts ranges without mislabeling legacy grids", () => {
  assert.equal(formatRiskGrid([0.6, 0.7, 0.8, 0.9, 1.0]), "0.6–1% R · 0.1% step");
  assert.equal(formatRiskGrid([0.75, 1.0, 1.25]), "0.75 / 1 / 1.25% R");
});

test("buildDiscoveryTickers exposes unique raw 5m instruments without dataset ids", () => {
  const datasets = [
    { dataset_id: "btc-a", asset: "BTC", instrument: "BTC-USDT-SWAP", data_type: "candles", timeframe: "5m", data_origin: "raw", storage_backend: "parquet" },
    { dataset_id: "btc-b", asset: "BTC", instrument: "BTC-USDT-SWAP", data_type: "candles", timeframe: "5m", data_origin: "raw", storage_backend: "parquet" },
    { dataset_id: "btc-oi", asset: "BTC", instrument: "BTCUSDT", data_type: "open_interest", timeframe: "5m", data_origin: "raw", storage_backend: "parquet" },
    { dataset_id: "eth-derived", asset: "ETH", instrument: "ETH-USDT-SWAP", data_type: "candles", timeframe: "5m", data_origin: "derived", storage_backend: "parquet" },
    { dataset_id: "sol", asset: "SOL", instrument: "SOL-USDT-SWAP", data_type: "candles", timeframe: "5m", data_origin: "raw", storage_backend: "parquet" },
  ];

  assert.deepEqual(buildDiscoveryTickers(datasets), [
    { key: "BTC|BTC-USDT-SWAP", asset: "BTC", instrument: "BTC-USDT-SWAP", label: "BTC · BTC-USDT-SWAP" },
    { key: "SOL|SOL-USDT-SWAP", asset: "SOL", instrument: "SOL-USDT-SWAP", label: "SOL · SOL-USDT-SWAP" },
  ]);
});

test("clipEpisodeToRange clips overlapping episodes and rejects invisible ones", () => {
  assert.deepEqual(
    clipEpisodeToRange(
      { start_ts: "2026-01-01T00:00:00Z", end_ts: "2026-01-01T02:00:00Z" },
      { from: 1767229200, to: 1767236400 }
    ),
    { from: 1767229200, to: 1767232800 }
  );
  assert.equal(
    clipEpisodeToRange(
      { start_ts: "2026-01-01T00:00:00Z", end_ts: "2026-01-01T00:30:00Z" },
      { from: 1767232800, to: 1767236400 }
    ),
    null
  );
});

test("atlas episodes retain their scale when the viewport extends beyond candle data", () => {
  const position = positionEpisodeOnLogicalRange(
    { start_ts: "2026-01-01T00:00:00Z", end_ts: "2026-01-01T01:00:00Z" },
    [1767225600, 1767229200, 1767232800],
    { from: -1, to: 3 }
  );
  assert.deepEqual(position, { left: 0.25, width: 0.25 });
});

test("episodeFill only colors directional opportunities", () => {
  assert.equal(episodeFill("LONG"), "var(--atlas-long-fill)");
  assert.equal(episodeFill("SHORT"), "var(--atlas-short-fill)");
  assert.equal(episodeFill("AMBIGUOUS"), "transparent");
  assert.equal(episodeFill("NEUTRAL"), "transparent");
});

test("atlas chart palette only uses colors supported by Lightweight Charts", () => {
  for (const color of Object.values(atlasChartPalette)) {
    assert.equal(typeof color, "string");
    assert.doesNotMatch(color, /oklch|color-mix|var\(/i);
  }
});

test("sortAtlasLanes orders horizon before entry delay", () => {
  const lanes = [
    { entry_delay_minutes: 10, horizon_hours: 48, episodes: [] },
    { entry_delay_minutes: 10, horizon_hours: 36, episodes: [] },
    { entry_delay_minutes: 5, horizon_hours: 36, episodes: [] }
  ];
  assert.deepEqual(
    sortAtlasLanes(lanes).map((lane) => [lane.entry_delay_minutes, lane.horizon_hours]),
    [[5, 36], [10, 36], [10, 48]]
  );
});
