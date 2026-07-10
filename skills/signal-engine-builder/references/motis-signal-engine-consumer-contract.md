# Motis Signal Engine Consumer Contract

Use this reference when a new signal engine packet reaches beyond engine-local tests. Passing `validate_signal_packet()` is necessary but not enough; the packet must also be consumable by Stage 0, Stage 1, Stage 2/3/4, portfolio backtest, and live execution without engine-specific consumer patches.

## Required Packet Evidence

Every event packet should include:

- `schema_version: "signal_packet.v2"`
- `asset`, `instrument`, and packet `timestamp`
- `active_timeframes`
- `evidence.reference_price`: numeric string or number used by Stage 0/2/3/4 as the simulation entry reference
- `evidence.trigger_candle_close`: numeric string or number for the confirmed trigger candle close
- `evidence.signal_available_at`: when all packet evidence became knowable
- `evidence.signal_candle_open_ts` and `evidence.signal_candle_close_ts` when the event is tied to a candle

For confirmed 5m event engines, `reference_price` and `trigger_candle_close` usually equal the confirmed signal candle close. If the engine intentionally uses a different reference, document the reason in evidence and tests.

Do not rely on engine-specific aliases such as `evidence.close` alone. If existing consumers cannot read a packet, first fix the engine to emit the standard fields. Add consumer compatibility only for a deliberate system-wide format migration.

## Downstream Consumers

- Stage 0 legacy scoring reads packet files, packet timestamp, and reference price. Missing reference price can result in all signals being dropped.
- Stage 0A information gate reads DB signal rows, packet timestamp, reference price, and candles. Missing reference price produces zero usable event records.
- Stage 1 invokes the paired base strategy with the canonical wrapper: `context["signal"]["payload"] == raw_packet`.
- Stage 2/3/4 and portfolio backtest reuse the same reference price extraction path to calculate TP/SL, capture, and replay.
- Live execution consumes the canonical signal pool and strategy bundle. Training and live packet builders must stay identical.

## Chart And Context Shape

- Prefer one canonical chart structure per timeframe with a `columns` header and row arrays.
- Keep column headers readable enough for an optimizer agent to reason from them. Avoid opaque abbreviations for domain-specific fields such as open-interest ratios unless a legend is present.
- Mark HTF rows with enough metadata to distinguish completed and forming context.
- Completed HTF rows must have close/availability time at or before `signal_available_at`.
- Forming HTF rows must be built only from confirmed lower-timeframe rows available at `signal_available_at`.
- Do not store duplicate completed arrays plus equivalent candle arrays unless compatibility requires it.

## Performance Contract

- Build rolling features with causal caches or bounded windows.
- Build per-packet chart/HTF context from bounded recent source rows.
- Do not scan full prior history for every emitted packet.
- Time a representative full-asset generation before declaring the engine practical.

## Required Smoke Checks

Run these before moving the engine into Stage 1:

1. Generate at least one real training packet and one live-scan packet from canonical parquet data.
2. Validate both raw packets with `validate_signal_packet`.
3. Run `scripts/audit_signal_packet_contract.py --packet <packet.json>`.
4. Wrap the real packet as `context["signal"]["payload"]` and call the paired base strategy.
5. Confirm Stage 0 scoring does not report reference-price errors.
6. Confirm Stage 0A information gate sees nonzero train/WF event counts when samples exist.
7. Inspect representative packet size and full-asset generation runtime.

If packet contract fields change, regenerate stale signal pools. Do not judge a new engine from old DB packets.
