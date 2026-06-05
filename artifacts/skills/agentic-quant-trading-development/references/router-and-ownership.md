# Router And Ownership

The router decides whether to wake an execution agent.

Default precedence:

1. Check exchange position for the asset.
2. If a position exists, route position management only to the owning strategy.
3. If flat, check working non-reduce-only entry orders.
4. If a resting entry order exists and is below TTL, exit quietly.
5. If stale, cancel it and continue to scanner flow.
6. If flat and no working entry order remains, run scanner.

## Owner State

Owner state identifies which strategy owns position management and which signal engine
produced the live signal stream used by that strategy.

It is routing-only and should contain only fields like:

```json
{
  "asset": "BTC",
  "inst_id": "BTC-USDT-SWAP",
  "owner_strategy": "btc-vegas-tunnel-v01",
  "signal_engine_id": "vegas_ema",
  "signal_family": "vegas_ema"
}
```

`signal_engine_id` is required on forward writes. `signal_family` is optional legacy
metadata and is accepted only as a migration fallback when historical owner state lacks
`signal_engine_id`.

Do not use owner state to infer direction, size, entry price, entry time, fill state, or
hold duration.

## Engine Routing

Live setup should pass `--strategy-id` for the strategy skill and `--signal-engine-id` for
the scanner/signal-root pair. The router resolves `live_scanner_path` and
`live_signals_root` from `artifacts/signal_engine/engine_registry.json` unless explicit
`--scanner-path` or `--signals-root` overrides are provided.

Live setup should also treat strategy-skill install and candle warm-up as prerequisites,
not optional operator hygiene. Before the router is scheduled, the execution agent should
already have:

- installed the latest promoted `strategy_id` skill into its own skill set
- seeded the asset's `live/data/raw/<ASSET>/5m/candles.csv` from `dev/data`
- rebuilt or refreshed `live/data/derived/<ASSET>/`
