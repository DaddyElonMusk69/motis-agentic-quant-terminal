# Motis Deterministic Quant Terminal

Locally runnable web app scaffold for deterministic, agent-assisted quant strategy research
and execution.

## Architecture

- `apps/web`: original React/Vite operator terminal.
- `apps/web-v2`: current terminal-style React/Vite research, data, engine, and trading UI.
- `apps/api`: FastAPI API and SQLAlchemy schema metadata.
- `apps/worker`: Python worker runtime for ingestion, research stages, signal-engine dispatch, lifecycle wakes, and exchange adapter boundaries.
- `packages/strategy_sdk`: shared signal, strategy, engine, market-data, and deployment contracts.
- `packages/strategy_modules`: paired base strategy modules used to seed Stage 1 development.
- `artifacts/signal_engine`: canonical engine registry plus legacy signal-engine source retained as implementation evidence.
- `docs/engine-strategy-contract.md`: source of truth for new signal engine / strategy pair contracts.
- `skills`: repo-local Codex skills for future agents, including signal-engine building and Stage 1A optimization.
- `ops`: Docker Compose and container packaging.

## Signal Engines And Strategy Contracts

Signal engines are contract-driven. New engines must register a canonical
`SignalEngineSpec` in `artifacts/signal_engine/engine_registry.json`, emit neutral
`signal_packet.v2` packets, read canonical Parquet market data, and provide both:

- `runtime_entrypoint` for training/research signal-pool generation.
- `live_scanner_entrypoint` for latest-candle live scans.

Signal packets must not contain direction, sizing, leverage, TP/SL, or order intent.
The paired base strategy owns `decide(context)` and optional `manage_position(context)`.
Live execution owns sizing, TP/SL price derivation, protection, pyramiding, exchange
routing, and idempotent order submission.

Current contract-ready engines:

- `vegas_ema`: multi-timeframe Vegas EMA tunnel, default 2 votes.
- `vegas_ema_vote1`: Vegas EMA variant, default 1 vote.
- `bollinger`: multi-timeframe Bollinger band proximity engine with paired
  `bollinger_base` strategy.

When building a new engine, use the repo-local skill:

```text
skills/signal-engine-builder/SKILL.md
```

The Stage 1A optimizer skill is also vendored for future agents:

```text
skills/stage1a-training-optimizer/SKILL.md
```

## Local Development

```bash
cp .env.example .env
python3 -m pytest tests -q
npm install
# Start Redis separately when using the Celery job backend, or use compose-up.
make dev-stack
```

`make dev-stack` starts the API, Celery worker, and v2 frontend in the
background. Runtime files are written under `.run/`, logs under `.run/logs/`,
and the v2 frontend is served at `http://127.0.0.1:5174`. Stop the local stack
with `make stop-stack`.

Manual service commands remain available:

```bash
make dev-api
make dev-worker
VITE_API_BASE_URL=http://127.0.0.1:8000 npm --workspace apps/web-v2 run dev -- --host 127.0.0.1 --port 5174 --strictPort
```

`make dev-worker` runs the Celery-backed concurrent job worker. Use
`CELERY_CONCURRENCY=8 make dev-worker` to increase parallel job slots, and keep
live/execution workers isolated with `CELERY_QUEUES=execution,default` when needed.
The previous single-job Postgres polling worker remains available as
`make dev-worker-legacy`.

Useful verification commands:

```bash
pytest -q
npm --workspace apps/web-v2 run build
```

Docker Compose packaging:

```bash
cp .env.example .env
make compose-up
```

If you use an existing local Postgres instead of the Compose Postgres container, create the
default app role/database before running migrations:

```bash
bash ops/scripts/bootstrap_local_postgres.sh
```

The v1 live-route path uses an OKX adapter boundary. The default local backend is the
installed OKX CLI with JSON output:

```bash
okx --profile <name> --demo --json market candles BTC-USDT-SWAP --bar 5m --limit 2
okx --profile <name> --live --json swap place --instId BTC-USDT-SWAP --side buy --ordType market --sz 1 --tdMode cross --clOrdId <id>
```

Strategies never call the CLI directly. Worker adapters own exchange access, parse JSON,
record command outputs, and enforce idempotent client order ids. Routes must still be
promoted, warmed, manually armed, and enabled before live execution is allowed by the SDK.

## OKX Candle Ingestion

The worker ingestion path fetches candles through the OKX adapter, normalizes OKX candle
arrays, writes partitioned Parquet, and returns a registration payload matching
`market_data_refs`.

Core module:

```text
apps/worker/src/quant_terminal_worker/ingestion/okx_candles.py
```

The API repository layer exposes a matching insert builder:

```text
apps/api/src/quant_terminal_api/repositories/market_data.py
```
