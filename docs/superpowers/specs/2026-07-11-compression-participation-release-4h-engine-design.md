# Compression Participation Release 4h Engine Design

## Objective

Add a separately selectable `compression_participation_release_4h_v1` signal engine that uses the validated Compression Participation Release event definition while enforcing an immutable four-hour dedupe cadence. The existing `compression_participation_release_v1` engine and its eight-hour canonical signal pool remain unchanged.

## Engine Identity

- Engine id: `compression_participation_release_4h_v1`
- Display name: `Compression Participation Release 4h v1`
- Version: `0.1`
- Dedupe cadence: exactly 240 minutes
- Required data: canonical raw 5m candles and canonical raw 5m open-interest rows
- Output schema: `signal_packet.v2`
- Paired strategy: the existing Compression Participation Release base strategy, because the event evidence and directional interpretation are unchanged

The four-hour engine has its own canonical signal-set key and packet identities. It does not reuse, rewrite, or extend the eight-hour engine's pool.

## Architecture

Implement a thin four-hour adapter over the existing event scanner rather than duplicating the full quant implementation. The shared event-building functions accept an explicit evidence engine id while preserving the existing engine id as their default, so the eight-hour engine remains backward compatible.

The four-hour adapter:

1. Reads the same candle and OI datasets as the existing engine.
2. replaces every caller-provided `dedupe_window_minutes` value with `240`;
3. delegates event detection and packet construction to the existing causal implementation;
4. emits the new engine id in packet evidence; and
5. preserves identical training and live packet shapes.

## Fixed Parameter Contract

Extend engine configuration metadata with an optional `fixed_parameters` mapping. Parameter precedence is:

1. generic runtime defaults;
2. engine `default_parameters`;
3. signal-set manifest or route parameters; and
4. engine `fixed_parameters`.

The new engine declares:

```json
"fixed_parameters": {
  "dedupe_window_minutes": 240
}
```

Both the canonical pool extension service and direct live scanner parameter resolution apply this precedence. The four-hour adapter clamps the value again so direct function calls cannot bypass the contract.

## Fill Data Flow

Manual fill and live execution fill already converge on `extend_signal_pool_from_local_candles`:

- The manual API enqueues or invokes that service for the selected engine and asset.
- The live route lifecycle invokes that same service after market-data warmup.

For either caller, the service resolves the selected engine spec, merges fixed parameters, reads the latest existing signal from that engine's canonical pool, and passes it to the generator as `_dedupe_seed_timestamp`. The engine rejects candidates less than 240 minutes after the seed. The extension packet sink independently rejects any emitted packet less than 240 minutes after the last admitted canonical signal.

This provides continuous cadence across repeated fills and a second admission check at the persistence boundary.

## Boundary Semantics

- A candidate at exactly 240 minutes after the prior admitted signal is allowed.
- A candidate at 239 minutes or less is rejected.
- The cadence is chronological and applies across fill calls through the canonical-pool seed.
- Attempts to set the cadence to `0`, `120`, `480`, or any other value are ignored for this engine.
- The cadence applies per engine and asset because each combination has a separate canonical signal pool.

## Packet And Strategy Compatibility

Packets remain neutral and retain standard evidence fields including `reference_price`, `trigger_candle_close`, `signal_available_at`, and signal candle timestamps. Only the engine identity and dedupe evidence value differ from the eight-hour variant.

The existing paired base strategy continues to map upper-boundary releases to scoreable long decisions and lower-boundary releases to scoreable short decisions through the canonical `context["signal"]["payload"]` wrapper.

## Testing

Tests will establish:

- the registry entry validates and is visible as a separate engine with zero pools;
- default and fixed cadence metadata both report 240 minutes;
- engine calls ignore conflicting dedupe overrides;
- generated packet evidence identifies the four-hour engine and reports 240 minutes;
- training and live generation preserve packet parity and point-in-time evidence;
- an existing canonical signal blocks a candidate inside four hours across a subsequent extension;
- a candidate exactly four hours later is admitted;
- the extension persistence filter enforces fixed parameters even if a generator emits too-frequent packets;
- manual API fill selects the four-hour engine's canonical pool;
- live lifecycle fill selects the same engine and extension path;
- the existing eight-hour engine still defaults to 480 minutes; and
- the paired strategy remains scoreable with representative four-hour packets.

Focused tests, packet contract audit, lint, and relevant regression tests must pass before the engine is considered ready. Creating or backfilling the production BTC pool is outside this implementation unless separately requested.

## Non-Goals

- Changing event thresholds, OI logic, context charts, or validated 2h/4h information claims
- Modifying the existing eight-hour engine or its stored signals
- Adding order-book depth
- Running Stage 0A or promoting the four-hour engine
- Changing strategy direction, exits, sizing, leverage, or execution policy
