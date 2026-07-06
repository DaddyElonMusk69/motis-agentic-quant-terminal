# Motis Agentic Quant Terminal

Most attempts to apply agents to trading fail because trading is not a generic
automation problem. It is a probabilistic system problem that demands data
discipline, reproducibility, controlled experimentation, and tightly governed
execution.

That is the problem Motis is built to solve.

Motis is an agentic quantitative research and trading platform that gives
humans and coding agents a rigorous environment to gather market data, build
event-driven signal engines, label and score outcomes, optimize
asset-specific strategies, validate them with staged backtests and
walk-forward research, and deploy them through the same controlled execution
stack.

Motis does not ask agents to improvise trades with real money. It gives them a
scientific operating environment for quantitative research and controlled
deployment.

## The Problem

Since the rise of agents, both retail traders and institutions have tried
multiple ways to use models in financial markets:

- asking models directly what to buy or sell;
- using agents to assist discretionary trade research;
- wiring agents into tools, data feeds, exchanges, MCP servers, and external
  skills in the hope they can behave like autonomous traders.

These approaches can work well in many industries. Trading is different.
Markets require probabilistic reasoning under uncertainty, where a small amount
of hallucination, inconsistency, or undocumented decision drift can destroy
real performance. A trading system cannot rely on explanations that sound
plausible after the fact. It must produce decisions that are measurable,
repeatable, and testable against real market history.

## The Motis Thesis

Motis is built on a simple thesis: quantitative trading is a system problem,
not an agent problem.

Agents are powerful in the parts of the workflow where they are strongest:

- coding;
- structured analysis;
- signal and strategy iteration;
- mathematical reasoning;
- research acceleration.

They are much less reliable when asked to make unbounded, untestable live
trading decisions in open-ended natural language. Motis constrains agents
inside a deterministic research and execution framework so their output becomes
data-driven, backtestable, auditable, and promotable into production.

## What Motis Solves

Motis is designed to democratize quantitative trading for retail investors and
small research teams.

Historically, the barrier to building profitable quantitative systems has been
extremely high. A practitioner needed to assemble and maintain the entire
research stack:

- market data collection and continuity;
- signal generation;
- labeling and scoring;
- strategy development;
- overfit control;
- walk-forward validation;
- execution logic;
- deployment infrastructure.

Even when retail tools existed, they rarely closed the gap between "having a
strategy idea" and "running an institutional-style quantitative process."
Motis closes that gap by providing a single environment where agents can help
build and improve the full research loop without bypassing rigor.

## What The Platform Does

Motis supports the full lifecycle of quantitative strategy development and
deployment:

- canonical market data ingestion and storage;
- signal-engine creation with neutral, contract-safe packet output;
- Stage 0 labeling and scoring;
- Stage 1 strategy optimization against training samples;
- downstream staged validation and replay;
- Stage 4A realized expectancy backtesting;
- Stage 4B timing overlays for controlled timing optimization;
- portfolio-level backtesting across promoted candidates;
- promotion into execution bundles;
- live route operation through the same governed execution path.

The core design principle is continuity. The same contracts that govern
research artifacts also govern what can be promoted and executed live. That
reduces drift between research, validation, and production behavior.

## Why The Architecture Matters

Motis is not a chat wrapper around a brokerage API. It is a contract-driven
research and execution substrate.

Signal engines emit neutral `signal_packet.v2` evidence instead of live trading
instructions. Strategies decide direction and position logic within a bounded
interface. The worker runtime owns data warming, simulation, risk controls,
protection logic, pyramiding, exchange adapter boundaries, and idempotent order
submission. Promotion freezes a validated candidate into an execution bundle so
live trading uses audited artifacts rather than open-ended model behavior.

This architecture lets agents contribute where they add leverage while keeping
real-money execution inside a deterministic system.

## Agent And System Boundary

Motis is intentionally split between what the agent does and what the system
does.

The agent handles the research work that benefits from flexible reasoning:

- proposing and refining signal-engine logic;
- writing and updating strategy code;
- analyzing audits, failures, and backtest results;
- iterating on hypotheses, filters, and timing rules;
- generating structured prompts and research artifacts for each stage.

The system handles the parts that must remain deterministic, reproducible, and
operationally safe:

- ingesting and storing market data;
- generating canonical signal packets from registered engines;
- labeling and scoring Stage 0 outcomes;
- running staged backtests, replays, and walk-forward validation;
- enforcing portfolio simulation rules, risk logic, and execution math;
- freezing promoted candidates into execution bundles;
- warming data, scanning live signals, routing orders, and recording exchange
  state.

This separation is fundamental to the Motis approach. The agent can change the
research logic, but it does not get to improvise around the accounting,
simulation, promotion, or execution substrate. That boundary is what makes the
platform both agentic and governable.

## Roadmap

Motis is being built in stages.

`V1` is the proof-of-concept system now in the repository. Agents are not
embedded directly into the platform. Instead, they work through the platform's
file structure, generated prompts, staged artifacts, and specialized skills for
each part of the quantitative workflow. The purpose of V1 is to prove that this
approach can work in practice: reduce the operational pain of quantitative
research while remaining rigorous enough to survive real live trading.

`V2` brings the agent inside the platform. Instead of moving between external
tools and repository artifacts, the user experience becomes native: an agent can
inspect market data, build or extend signal engines, run backtests, perform
walk-forward training, compare research outputs, prepare promotions, and manage
live routes from within the same system boundary.

The long-term end state is simple for the user: go from idea to backtest to
live trading through natural language, while the platform continues to enforce
the quantitative discipline underneath. The user should be able to express a
research idea in plain English, let the system turn it into a structured
research workflow, inspect the evidence, approve the strongest candidate, and
deploy it without leaving the platform or giving up deterministic controls.

## Repository Architecture

- `apps/web-v2`: current terminal-style React/Vite interface for research,
  data, engines, portfolio backtests, and trading operations.
- `apps/api`: FastAPI API surface and SQLAlchemy-backed persistence layer.
- `apps/worker`: worker runtime for ingestion, staged research jobs,
  backtests, signal-engine dispatch, and live route wakes.
- `packages/strategy_sdk`: shared contracts for signals, strategies, engines,
  market data, and deployments.
- `packages/strategy_modules`: paired base strategies and public examples used
  to seed Stage 1 development.
- `artifacts/signal_engine`: canonical engine registry, shared signal-engine
  tooling, and public example implementations.
- `docs/engine-strategy-contract.md`: source of truth for engine / strategy
  contract boundaries.
- `skills`: repo-local Codex skills for future agents, including
  signal-engine building and Stage 1A optimization.
- `ops`: Docker Compose and local infrastructure packaging.

## Signal Engines And Strategy Contracts

Signal engines are contract-driven. New engines must register a canonical
`SignalEngineSpec` in `artifacts/signal_engine/engine_registry.json`, emit
neutral `signal_packet.v2` packets, read canonical Parquet market data, and
provide both:

- `runtime_entrypoint` for training and research signal-pool generation;
- `live_scanner_entrypoint` for latest-candle live scans.

Signal packets must not contain direction, sizing, leverage, TP/SL, or order
intent. The paired base strategy owns `decide(context)` and optional
`manage_position(context)`. Live execution owns sizing, TP/SL price
derivation, protection, pyramiding, exchange routing, and idempotent order
submission.

The public repository keeps the contract shape visible and may include selected
example engines and paired strategies for reference, including
`liquidity_sweep_v1`. Proprietary production engines and production strategy
logic are treated as private research assets rather than public documentation.

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
live and execution workers isolated with `CELERY_QUEUES=execution,default` when
needed. The previous single-job Postgres polling worker remains available as
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

If you use an existing local Postgres instead of the Compose Postgres
container, create the default app role and database before running migrations:

```bash
bash ops/scripts/bootstrap_local_postgres.sh
```

The live-route path uses an OKX adapter boundary. The default local backend is
the installed OKX CLI with JSON output:

```bash
okx --profile <name> --demo --json market candles BTC-USDT-SWAP --bar 5m --limit 2
okx --profile <name> --live --json swap place --instId BTC-USDT-SWAP --side buy --ordType market --sz 1 --tdMode cross --clOrdId <id>
```

Strategies never call the CLI directly. Worker adapters own exchange access,
parse JSON, record command outputs, and enforce idempotent client order ids.
Routes must still be promoted, warmed, manually armed, and enabled before live
execution is allowed by the SDK.

## Exchange Connectivity

Motis currently connects exchange accounts through local exchange CLIs rather
than storing live API credentials directly inside the platform.

This boundary is intentional. The local CLI acts as the security boundary for
account access and order submission, while Motis focuses on orchestrating the
research, routing, validation, and execution workflow around that connection.
In practice, that means a local OKX CLI can be used today, and the same adapter
shape can later be extended to other exchange CLIs that expose machine-readable
command output.

The platform does not ask strategy code or agents to talk to an exchange
directly. Worker adapters own that responsibility:

- invoking the local exchange CLI;
- parsing structured output;
- recording command results;
- applying route and order safety rules;
- preserving idempotent execution behavior.

This gives users a safer connection model in V1 while the platform proves the
research and execution workflow in live conditions. Direct API-based exchange
connectivity can be added later behind the same adapter boundary once the
security and operational model is ready.
