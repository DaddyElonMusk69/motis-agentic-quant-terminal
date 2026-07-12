# Opportunity Atlas Explorer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a selected-R candlestick and synchronized scenario-lane modal to Signal Discovery's Opportunity Atlas.

**Architecture:** A focused worker service filters atlas Parquet artifacts and aggregates canonical candles into bounded viewport responses. FastAPI exposes visualization and episode-detail routes, while React Query and Lightweight Charts render a modal that keeps candle overlays and scenario lanes synchronized.

**Tech Stack:** Python 3.13, PyArrow, FastAPI, React 19, TanStack Query, TypeScript, Lightweight Charts, pytest, Node test runner, Vite.

---

### Task 1: Atlas Visualization Service

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/visualization.py`
- Create: `tests/test_signal_discovery_visualization.py`

- [ ] Write a failing test that materializes candle, episode, and label fixtures for two R values and requests one selected R.
- [ ] Assert only the selected R's lanes are returned, overlapping intervals are retained, and LONG/SHORT directions remain unchanged.
- [ ] Write a failing OHLC aggregation test asserting first-open, maximum-high, minimum-low, and last-close semantics with `max_candles=2`.
- [ ] Implement `build_atlas_visualization(...)` with filtered PyArrow reads, deterministic scenario grouping, bounded aggregation, and UTC output.
- [ ] Write a failing episode-detail test asserting the episode and bucket-start directional path are returned together.
- [ ] Implement `read_atlas_episode_detail(...)` and verify the focused test file passes.

### Task 2: Read-Only API Contracts

**Files:**
- Modify: `apps/api/src/quant_terminal_api/main.py`
- Modify: `tests/test_api.py`

- [ ] Add failing API tests for selected-R visualization, invalid R rejection, authorization-window clipping, unavailable atlas artifacts, and episode/R mismatch.
- [ ] Add `GET .../atlas-visualization` with `risk_pct`, optional `start`/`end`, and `max_candles` validation.
- [ ] Resolve the session's canonical candle reference, read only the authorized research window, and delegate artifact processing to the visualization service.
- [ ] Add `GET .../atlas-episodes/{episode_id}` with explicit R validation.
- [ ] Run the focused API and visualization tests to green.

### Task 3: Frontend Data And Projection Helpers

**Files:**
- Modify: `apps/web-v2/package.json`
- Modify: `package-lock.json`
- Modify: `apps/web-v2/src/app/api.ts`
- Create: `apps/web-v2/src/app/atlasVisualization.ts`
- Modify: `apps/web-v2/tests/signalDiscovery.test.ts`

- [ ] Install `lightweight-charts` in the `apps/web-v2` workspace.
- [ ] Add response types and `fetchSignalDiscoveryAtlasVisualization` / `fetchSignalDiscoveryAtlasEpisode` clients.
- [ ] Write failing pure tests for visible-range clipping, direction color mapping, and scenario lane ordering.
- [ ] Implement the projection helpers without DOM dependencies and run the Node tests to green.

### Task 4: Chart And Modal

**Files:**
- Create: `apps/web-v2/src/components/OpportunityAtlasChart.tsx`
- Create: `apps/web-v2/src/components/OpportunityAtlasModal.tsx`
- Modify: `apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx`
- Modify: `apps/web-v2/src/styles/shell.css`

- [ ] Build the chart component with stable dimensions, chart cleanup, resize observation, candlestick series, visible-range callbacks, SVG episode overlays, and synchronized scenario lanes.
- [ ] Build the modal with existing terminal dialog structure, Escape/backdrop close, focus restoration, React Query loading/error/empty states, reset-range control, and bucket-start inspector.
- [ ] Change Opportunity Atlas row activation to select the R and open the modal while preserving the selected value for Freeze Target.
- [ ] Apply faint green LONG and faint red SHORT backgrounds; render no interval background for ambiguous or neutral labels.
- [ ] Add full-screen responsive behavior below 900px and ensure chart/labels do not overlap.

### Task 5: Verification

**Files:**
- Modify only if failures reveal contract errors: files listed above.

- [ ] Run `pytest tests/test_signal_discovery_visualization.py tests/test_api.py tests/test_signal_discovery_end_to_end.py -q`.
- [ ] Run `npm --workspace apps/web-v2 run test:signal-discovery`.
- [ ] Run `npm --workspace apps/web-v2 run build`.
- [ ] Run scoped Ruff checks and `git diff --check`.
- [ ] Start the existing user-managed stack only if it is not already running, then inspect the modal at desktop and mobile widths with the browser tooling.
- [ ] Verify nonblank candle rendering, synchronized lanes, selected-R filtering, episode zoom/detail, Escape/backdrop close, and no text overlap.
- [ ] Confirm `artifacts/signal_engine/engine_registry.json` remains unstaged and unmodified by this feature.
