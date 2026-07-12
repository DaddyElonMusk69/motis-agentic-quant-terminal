# Opportunity Atlas Explorer Design

## Purpose

The Opportunity Atlas currently summarizes each fixed-R candidate in tables but does not show where its opportunity episodes occur on the underlying market path. The Atlas Explorer gives a quant researcher a synchronized candle-and-episode view for one selected R candidate without loading the full timestamp-label frontier into the browser.

## Entry And Scope

Clicking an R candidate row in the Opportunity Atlas selects that R and opens a large analytical modal. The modal only presents data belonging to that candidate. Closing it leaves the R selected for the existing Freeze Target workflow.

The modal header identifies the asset, selected R, target/stop percentages, primary entry delay and horizon, episode count, and LONG/SHORT distribution. A close icon, Escape handling, backdrop dismissal, focus containment, and restored trigger focus follow the terminal's existing modal contract.

## Visualization

The upper surface is a candlestick chart. LONG episode intervals overlay the candle region in faint green and SHORT intervals in faint red. AMBIGUOUS and NEUTRAL timestamps receive no background color.

Scenario lanes sit immediately below the chart and share its visible time range. Each lane represents one entry-delay/holding-horizon scenario for the selected R. Episode segments use the same direction colors as the candle overlay. Panning, zooming, crosshair movement, and reset-to-full-range remain synchronized.

Clicking an episode segment selects it, zooms the candle chart to the episode with surrounding context, and loads a compact inspector. The inspector shows direction, start/end, duration, timestamp count, and a clearly labeled bucket-start snapshot containing entry price, target/stop prices, first-touch time, MFE, MAE, and terminal return. It must not imply that one snapshot represents every timestamp in the episode.

## Data Contract

Add a viewport-aware read-only endpoint:

`GET /api/v1/research/signal-discovery-sessions/{session_id}/atlas-visualization`

Parameters are `risk_pct`, optional UTC `start`/`end`, and bounded `max_candles`. The response contains the authorized research window, OHLC bars, effective candle interval, downsampling status, and scenario lanes with episode intervals. It never exposes artifact paths.

Add an episode-detail endpoint:

`GET /api/v1/research/signal-discovery-sessions/{session_id}/atlas-episodes/{episode_id}`

It validates that the episode belongs to the requested R candidate and returns the episode plus its bucket-start label snapshot.

The API reads `training_episodes.parquet` with risk/time filters and reads the canonical primary candle registration only inside the requested authorized research range. It downsamples OHLC server-side to a bounded response while preserving open-first, high-max, low-min, and close-last semantics. Episode intervals remain exact. A focused episode request retrieves a 5m candle window around the selected interval.

## Component Boundaries

- `signal_discovery/visualization.py`: Parquet filtering, candle aggregation, lane grouping, and episode detail.
- API routes: session/risk/window validation and response delivery.
- `atlasVisualization.ts`: frontend response types and pure time-range/episode projection helpers.
- `OpportunityAtlasModal.tsx`: modal state, queries, header, loading/error/empty states, and inspector.
- `OpportunityAtlasChart.tsx`: Lightweight Charts lifecycle, candle rendering, visible-range synchronization, and episode overlays/lanes.

## Performance

The browser never receives all 1.25 million BTC timestamp labels. The full-period modal initially receives at most 4,000 aggregated candles and only the selected R's episode intervals. Selecting an episode requests a bounded high-resolution window. Query keys include session, R, and UTC range so React Query caches revisited views.

## States

- Loading: fixed chart dimensions with an in-surface progress state.
- Ready: candles, scenario lanes, summary header, and optional episode inspector.
- Empty: valid candidate with no episodes in the requested range.
- Error: concise API error with retry command.
- Missing artifacts: modal does not open and the API returns a conflict response.
- Narrow viewport: modal becomes full-screen; inspector moves below the lanes and no controls disappear.

## Visual Direction

This remains a dense, restrained quant workbench consistent with the current terminal. The chart is the dominant surface, controls are compact, borders are structural, and direction colors are quiet enough that candles remain legible. There are no decorative cards, gradients, or explanatory onboarding panels.

## Acceptance

- Clicking an R row opens the modal and retains the selected R after close.
- Only the selected R is returned and rendered.
- LONG is faint green, SHORT is faint red, and ambiguous/neutral have no background.
- Every scenario lane shares the candle chart's visible time range.
- Episode selection loads exact 5m context and a bucket-start inspector.
- Full-year BTC data remains responsive and bounded.
- Existing freeze, prompt, walk-forward, and candidate workflows are unchanged.
