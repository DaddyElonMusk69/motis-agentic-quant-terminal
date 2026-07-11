import assert from "node:assert/strict";
import test from "node:test";

import { buildRiskGrid, formatRiskGrid } from "../src/app/signalDiscovery.ts";

test("buildRiskGrid expands an inclusive range in stable 0.1 increments", () => {
  assert.deepEqual(buildRiskGrid(0.6, 1.0), [0.6, 0.7, 0.8, 0.9, 1.0]);
  assert.deepEqual(buildRiskGrid(0.75, 1.05), [0.8, 0.9, 1.0]);
});

test("buildRiskGrid rejects nonpositive and descending ranges", () => {
  assert.throws(() => buildRiskGrid(0, 1), /positive/);
  assert.throws(() => buildRiskGrid(1.2, 0.8), /minimum/);
});

test("formatRiskGrid compacts ranges without mislabeling legacy grids", () => {
  assert.equal(formatRiskGrid([0.6, 0.7, 0.8, 0.9, 1.0]), "0.6–1% R · 0.1% step");
  assert.equal(formatRiskGrid([0.75, 1.0, 1.25]), "0.75 / 1 / 1.25% R");
});
