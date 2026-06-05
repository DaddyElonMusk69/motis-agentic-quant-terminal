# Workspace Contract

The workspace is a reproducible scaffold.

## Root

```text
README.md
workspace_manifest.json
dev/
live/
artifacts/
```

## Dev

Use `dev/` for historical research only.

```text
dev/data/raw/<ASSET>/5m/candles.csv
dev/data/derived/<ASSET>/<TF>/candles.csv
dev/data/manifests/<ASSET>.json
dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/manifest.json
dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets/*.json
dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/manifest.json
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/manifest.json
dev/walk_forward/<YYYY-MM>/manifest.json
dev/walk_forward/<YYYY-MM>/stage0_branch_decisions.json
dev/walk_forward/<YYYY-MM>/tradable_universe.json
dev/walk_forward/<YYYY-MM>/watchlist_universe.json
dev/walk_forward/<YYYY-MM>/summaries/monthly_universe.md
```

`SIGNAL_SET_ID` must be `<YEAR>-<ASSET>-<DEDUPE_WINDOW>-dedupe-vote<VOTE_THRESHOLD>`,
for example `2026-BTC-2h-dedupe-vote2`. Packet files must be named
`YYYYMMDDTHHMMSSZ.json` and match their packet `timestamp`.

`SIGNAL_ENGINE_ID` is the canonical producer identity and uses current engine root names
such as `vegas_ema` and `bollinger`. `signal_family` is legacy metadata only.

`dev/walk_forward/<YYYY-MM>/` is the monthly strategy-engine tradability record. It is
created after Stage 0 for the current walk-forward training window and before Stage 1+
training sessions. Rows are keyed by `strategy_id`; the same asset can appear more than
once if different strategy-engine candidates qualify.

## Live

Use `live/` for runtime state only.

```text
live/data/raw/<ASSET>/5m/candles.csv
live/data/derived/<ASSET>/<TF>/candles.csv
live/signals/<SIGNAL_ENGINE_ID>/<ASSET>/latest_scan.json
live/signals/<SIGNAL_ENGINE_ID>/<ASSET>/packets/*.json
live/router/state/open_position_owner/<ASSET>.json
live/router/state/position_reviews/<ASSET>.json
live/router/state/wake_router/<ASSET>.json
```

## Artifacts

Use `artifacts/` for portable reusable components.

```text
artifacts/signal_engine/
artifacts/skills/agentic-quant-trading-development/
artifacts/skills/strategy-bases/<BASE_TEMPLATE>/
artifacts/skills/strategies/<STRATEGY_SKILL>/
artifacts/docs/
```

Generated candle data, signal packets, and training results do not belong in `artifacts/`.
