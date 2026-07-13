# Vegas 5m Cluster v6 Design

## Objective

Create a new `vegas_5m_cluster_v6` signal engine and paired base strategy without changing `vegas_5m_cluster_v5` behavior.

V6 preserves the causal 5m Vegas EMA cluster trigger, replaces the strategy's 1d EMA confirmation with 12h context, and adds completed plus forming 1d Bollinger evidence to the neutral packet. Bollinger evidence is observational in the initial base strategy: it is exposed for later Stage 1 research but cannot change the initial direction or entry decision.

## Scope

V6 includes:

- The existing 5m EMA cluster trigger: price must be within the configured proximity threshold of at least the configured number of EMA rails.
- A two-hour dedupe window by default.
- Causal `2h`, `8h`, and `12h` completed and forming candle context.
- Causal completed 1d Bollinger rows and one provisional forming 1d Bollinger row.
- Canonical signal availability and reference-price evidence.
- A paired strategy that replaces v5's 1d directional logic with equivalent 12h logic.
- Registry, runtime, packet, strategy, consumer-contract, extension-parity, and point-in-time tests.

V6 does not include:

- A Bollinger mean-reversion rule or direction override.
- New Stage 1 thresholds selected from outcomes.
- Changes to v5, its packets, its strategy, or existing deployment routes.
- A migration of existing derived candle or Bollinger datasets to native OKX boundaries.
- Strategy profitability optimization, exit changes, sizing, leverage, or order behavior.

## Data Dependencies

The registry entry declares:

- Raw confirmed 5m candles.
- Derived EMA-enriched 5m candles for the trigger.
- Derived EMA-enriched `2h`, `8h`, and `12h` candles for strategy context.
- Derived 1d candles used only to establish the existing daily bucket anchor, completed daily closes, and the forming daily bucket.
- Derived `feature_bollinger` 1d rows with `bb_mid_20`, `bb_upper_20_2`, `bb_lower_20_2`, `bb_position_pct`, `bb_bandwidth_pct`, and `bb_zscore`.

The implementation deliberately preserves the repository's existing HTF bucket shape. Training and live scans rebuild and consume the same anchored buckets from the same canonical 5m history. The anchor need not match native OKX chart boundaries to remain causal, but it must remain stable between training and live generation.

V6 is usable only for assets with all declared datasets. BTC is the initial supported asset because its 1d Bollinger dataset already exists. Missing 12h candle or 1d Bollinger data blocks generation with an explicit dependency error rather than emitting a partial packet.

## Signal And Availability Semantics

Let the triggering raw candle open at `T` and close at `T + 5m`.

- `evidence.signal_candle_open_ts = T`.
- `evidence.signal_candle_close_ts = T + 5m`.
- `evidence.signal_available_at = T + 5m`.
- Packet `timestamp = T + 5m`.
- `evidence.reference_price` and `evidence.trigger_candle_close` equal the confirmed trigger candle close.

All trigger inputs, context rows, Bollinger rows, joins, and packet fields must have `available_at <= signal_available_at`.

Completed HTF candle availability is `open_timestamp + timeframe`. A completed `12h` row opening at 04:00 is unavailable before 16:00. Forming HTF rows are aggregated only from confirmed raw 5m candles whose close time is at or before `signal_available_at`.

This corrects v5's ambiguity where the packet was stamped with the 5m candle open while containing its confirmed close.

## Engine Data Flow

Training generation and live scanning call one shared packet builder.

1. Prepare and index EMA-enriched 5m rows, confirmed raw 5m rows, completed HTF candle rows, and completed 1d Bollinger rows once per generation call.
2. Evaluate the existing EMA proximity trigger on a confirmed 5m row.
3. Apply the continuous two-hour dedupe state.
4. Select completed `2h`, `8h`, and `12h` candles by close availability.
5. Aggregate the current forming `2h`, `8h`, and `12h` candles from confirmed raw 5m rows through `signal_available_at`.
6. Select completed 1d Bollinger rows whose source daily candle close is at or before `signal_available_at`.
7. Aggregate the current forming daily candle using the same anchor as the existing derived 1d series.
8. Calculate one provisional Bollinger slice using the latest 19 completed daily closes plus the forming daily close.
9. Build and validate a neutral `signal_packet.v2` packet.

Indexes and rolling state must avoid scanning full history for every event. Full-window training generation must remain linear or near-linear in input row count.

## Bollinger Calculation

Completed Bollinger values come from the registered 1d feature dataset and are joined to their source 1d candle by timestamp. A completed output row includes the source daily close and explicit availability time.

The forming slice uses standard 20-period, two-standard-deviation Bollinger math consistent with `feature_enrichment.py`:

- Window: 19 latest completed daily closes plus the current forming daily close.
- Middle band: arithmetic mean of the 20 closes.
- Standard deviation: population standard deviation.
- Upper band: middle plus two standard deviations.
- Lower band: middle minus two standard deviations.
- Position: `(forming_close - lower) / (upper - lower) * 100` when band width is nonzero.
- Bandwidth: `(upper - lower) / middle * 100` when the middle is nonzero.
- Z-score: `(forming_close - middle) / standard_deviation` when standard deviation is nonzero.

The forming row is omitted until 19 completed daily closes are available. It is marked `complete: false`; completed feature rows are marked `complete: true`.

Changing or appending any raw 5m observation whose close is after `signal_available_at` must not change the forming candle, Bollinger values, trigger decision, dedupe admission, or packet bytes at that signal.

## Packet Shape

The packet contains candle charts named `5m`, `2h`, `8h`, and `12h`. Each chart uses one `candles` array and an explicit `candle_columns` header. Completed and forming rows share the same schema and include:

- Candle open timestamp.
- OHLCV values.
- Confirmation marker.
- `is_completed`.
- `source_candle_count`.
- Partial close or availability timestamp.
- Expected close timestamp.

The packet also contains `charts.bollinger_1d`:

```json
{
  "role": "mean_reversion_context",
  "timeframe": "1d",
  "source": "derived_completed_plus_causal_forming",
  "columns": [
    "open_ts",
    "available_at",
    "complete",
    "source_candle_count",
    "close",
    "bb_mid_20",
    "bb_upper_20_2",
    "bb_lower_20_2",
    "bb_position_pct",
    "bb_bandwidth_pct",
    "bb_zscore"
  ],
  "rows": []
}
```

The final row may be forming. Its `available_at` equals `signal_available_at`, and its `source_candle_count` records how many confirmed 5m candles contributed to the partial day.

The packet remains neutral. It contains no direction, confidence, action, sizing, leverage, TP, SL, or Bollinger-derived recommendation.

## Paired Strategy

Add `vegas_ema_5m_hft_v6_base.py` rather than modifying the v5 strategy.

The initial v6 strategy:

- Requires candle-only `5m`, `2h`, `8h`, and `12h` context.
- Uses the same cluster-vote gate as v5.
- Replaces all v5 1d statistics, forming statistics, context directions, priority branches, reversals, reason codes, and diagnostics with 12h equivalents.
- Keeps the existing numeric thresholds initially so the first comparison isolates the timeframe substitution rather than combining it with threshold optimization.
- Retains v5's 8h logic.
- Reads and reports the latest completed and forming Bollinger values in diagnostics when present.
- Does not use Bollinger values to select `LONG`, `SHORT`, `ENTER`, or `SKIP`.
- Does not fail solely because optional Bollinger diagnostics are unavailable after the engine packet has otherwise passed validation; engine generation itself remains strict about its declared dependency.

Future Stage 1 iterations may test a Bollinger mean-reversion override, but that rule is outside this build.

## Registry And Compatibility

Register `vegas_5m_cluster_v6` with:

- Version `0.1`.
- Runtime and live scanner entrypoints in the new v6 module.
- Default context timeframes `2h`, `8h`, and `12h`.
- Context mode `candles_only_integrated_forming_with_bollinger_1d`.
- The existing proximity, vote, context-bar, and dedupe defaults.
- `code_ref.base_strategy_path` pointing to the new v6 strategy.

Repository registry entries are merged without reverting unrelated existing registry changes. V5 remains independently selectable and behaviorally unchanged.

## Error Handling

Generation blocks with a specific error when:

- Raw or derived 5m data is empty.
- A required `2h`, `8h`, or `12h` context dataset is absent.
- The source 1d candle dataset is absent.
- The 1d Bollinger feature dataset is absent or lacks required columns.
- A trigger reference price is zero or invalid.

Insufficient Bollinger warmup omits only the provisional row; it does not fabricate zero bands.

## Tests

Tests are written before implementation and must cover:

1. Registry validation and runtime resolution for v6.
2. Training generation from canonical Parquet and latest-candle live scanning.
3. Trigger parity with v5 when both receive the same 5m trigger rows and dedupe state.
4. Presence of `2h`, `8h`, and `12h` charts and absence of the 1d EMA context chart.
5. Presence and schema of `bollinger_1d`.
6. Exact provisional Bollinger math on a small deterministic 20-close fixture.
7. Completed daily Bollinger rows satisfy `available_at <= signal_available_at`.
8. Forming daily values use only confirmed 5m rows closing by `signal_available_at`.
9. Future-mutation invariance for trigger, selected context, dedupe, forming Bollinger, and complete packet bytes.
10. Packet timestamp equals the trigger candle close and all standard evidence fields are present.
11. Repeated extension preserves the training dedupe window.
12. Training and live packet builders emit identical packets for identical as-of data.
13. Packet neutrality and packet-consumer audit compatibility.
14. V6 strategy requires 12h rather than 1d context.
15. V6 strategy returns the same decision when only Bollinger values are changed, proving Bollinger is diagnostic-only initially.
16. Missing required datasets produce explicit blocked errors.
17. API catalog includes the registry-only engine with zero signal and packet counts.

Focused runtime, strategy, registry, API, and packet-audit tests run before the full Python test suite.

## Acceptance Criteria

V6 is ready for research when:

- Engine, packet, and strategy validators pass.
- A representative BTC training packet and live-scan packet pass the consumer audit.
- No completed or forming source observation is used before its explicit availability time.
- Future source mutations cannot alter a previously emitted packet.
- Training extension and live scanning preserve identical packet semantics.
- The paired base strategy uses 12h in place of 1d and is invariant to Bollinger changes.
- V5 behavior and artifacts remain unchanged.

Performance evaluation, Stage 1 Bollinger overrides, and live promotion occur only after this implementation is accepted.
