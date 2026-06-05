# Signal Engine Artifact

This is the deterministic signal-engine package for the new Motis workspace shape only.

It owns neutral signal generation, live scanning, live candle cache updates, and the wake
router. It does not own historical data fetching, training data preparation, Stage 0
scoring, strategy optimization, or strategy judgment. Those workflow tools live in
`artifacts/skills/agentic-quant-trading-development/`.

It expects to run inside a workspace containing `workspace_manifest.json`, `dev/`, `live/`,
and `artifacts/`. It does not support old root folders such as `training_data`,
`training_sessions`, `live_data`, or `live_signals`.

## Canonical Roots

- Signal engine registry: `artifacts/signal_engine/engine_registry.json`
- Replay signal packets: `dev/signals/<signal_engine_id>/<ASSET>/...`
- Live candle cache: `live/data`
- Live signal packets: `live/signals/<signal_engine_id>/<ASSET>/...`
- Live router state: `live/router`

`signal_engine_id` is the canonical producer identity. Current engine ids are the existing
root names, such as `vegas_ema` and `bollinger`. `signal_family` is legacy descriptive
metadata only and must not drive routing.

## Common Commands

Generate EMA/Vegas replay signals:

```bash
python3 artifacts/signal_engine/scripts/signals/generate_training_session.py \
  --asset BTC \
  --start 2026-01-01T00:00:00Z \
  --end 2026-01-07T00:00:00Z \
  --vote-threshold 2
```

Scan live EMA/Vegas signals:

```bash
python3 artifacts/signal_engine/scripts/signals/scan_okx_live_signals.py \
  --asset BTC
```

Route live wakes:

```bash
python3 artifacts/signal_engine/scripts/live/autonomous_wake_router.py \
  --asset BTC \
  --account-mode live \
  --strategy-id btc-vegas-tunnel-v01 \
  --signal-engine-id vegas_ema \
  --engine-registry-path artifacts/signal_engine/engine_registry.json \
  --router-state-root live/router/state/wake_router \
  --owner-state-root live/router/state/live/open_position_owner \
  --position-review-state-root live/router/state/live/position_reviews \
  --entry-order-ttl-minutes 30
```

Cron jobs should call the router, not the scanner directly. Run one strategy per asset per
cron on a 5-minute cadence. The router checks exchange position truth first, then working
entry orders, then resolves the scanner and live signal root from `signal_engine_id` via
the engine registry. Explicit `--scanner-path` and `--signals-root` overrides remain
supported for controlled migration or debugging.

## Boundary

The signal engine may normalize candles, rebuild derived timeframes, reconstruct active
HTF candles, compute neutral indicators, emit signal packets, route live wakes, and score
runtime scanner/router outcomes.

The signal engine must not decide final trade direction, entry, sizing, TP, SL, leverage,
position management, or optimization conclusions. Those decisions belong in strategy
skills and agent evaluation. Historical data fetching and staged scoring belong in the
agentic development skill.
