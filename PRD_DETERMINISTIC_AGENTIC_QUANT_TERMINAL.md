# PRD: Deterministic Agentic Quant Trading Terminal

## Purpose

Build a fresh quant trading research and execution platform that keeps the useful ideas
from the current agentic trading development workflow, but replaces prose-driven strategy
evaluation with deterministic software.

The product is an agent-assisted quant trading terminal:

- agents help design, audit, refactor, and explain strategies
- signal engines, strategy logic, backtests, walk-forward evaluation, and live execution are
  deterministic software
- the frontend is the primary control surface for managing data, engines, strategies,
  experiments, and deployments
- the database is the system of record, not a layered filesystem

This project should start as a new application. It should not be built as an incremental
extension of the current file-based workspace.

## Source Context

Use the current development skill as historical design input, not as an implementation
dependency:

`/Users/haokaiqin/.codex/skills/agentic-quant-trading-development/SKILL.md`

Key concepts to carry forward:

- deterministic signal engines
- neutral signal packets
- engine-qualified strategy universes
- monthly or scheduled walk-forward cycles
- train / validation / OOS evaluation discipline
- Stage 0 travel distribution, threshold calibration, and ground truth
- staged scoring and promotion gates
- backtest/live parity
- one live execution route per strategy-asset pair

Key concepts to replace:

- prose strategy skills as the primary decision engine
- evaluator agents reading packet samples one by one
- artifact state spread across nested folders as the primary source of truth
- fixed calendar assumptions baked into workflow code

## Problem

The current agentic trading workflow is too slow and expensive to scale.

Observed constraints:

- each agentic evaluation pass can take about 10 minutes
- a single strategy-asset pair can require 5-6 passes
- 20+ strategy-asset pairs implies tens of hours of waiting
- token cost grows with every pass
- prose strategy behavior is hard to reproduce exactly
- live execution should not depend on fuzzy interpretation

The current workflow is useful for strategy discovery, but not as the core optimization or
execution engine.

## Product Goals

1. Build a deterministic research and execution platform for quant strategies.
2. Preserve the staged walk-forward discipline from the current workflow.
3. Make signal engines and strategies fully pluggable.
4. Support arbitrary data sources, arbitrary signal packet shapes, and arbitrary strategy
   logic without the pipeline becoming the limiting layer.
5. Provide a frontend terminal where the full lifecycle can be managed without manually
   navigating filesystem artifacts.
6. Use a database as the system of record for data catalogs, runs, scores, strategy
   versions, deployment state, and audit trails.
7. Keep agents in the loop for development and analysis, not runtime decisions.

## Non-Goals

- Do not build another natural-language evaluator pipeline.
- Do not require every strategy to fit a fixed rule-table DSL.
- Do not require every signal engine to emit the same packet schema beyond a minimal
  envelope.
- Do not hard-code a 2-month train / 1-month validation / 1-week OOS policy.
- Do not make the filesystem artifact tree the primary database.
- Do not start by migrating every historical artifact from the current workspace.

## Users

Primary user:

- Quant developer/operator who wants to rapidly test, compare, promote, and deploy
  deterministic strategy modules.

Agent user:

- Coding/research agent that can inspect runs, write strategy modules, generate tests,
  produce failure audits, and propose next experiments.

Execution user:

- Live execution process that only consumes promoted deterministic modules and deployment
  configuration.

## Core Principle

The pipeline must be permissive at the boundaries and strict at the contracts.

Permissive:

- any raw data source
- any signal engine
- any packet payload shape
- any deterministic strategy implementation
- any walk-forward window template
- any execution profile shape

Strict:

- every run is versioned
- every decision is reproducible
- every backtest has immutable inputs
- every live deployment points to exact strategy and engine versions
- every promoted strategy has train, validation, and OOS evidence
- the same deterministic strategy code is used in backtest and live execution

## System Overview

The platform has six major subsystems:

1. Data platform
2. Signal engine platform
3. Strategy module platform
4. Walk-forward and backtest platform
5. Live execution platform
6. Frontend terminal

Agents operate around these subsystems by editing code, writing tests, proposing
experiments, and auditing results.

## Data Platform

The data platform stores metadata and references for any raw or derived dataset.

Supported data types should include, without requiring special-case pipeline changes:

- OHLCV candles
- order book snapshots
- trades
- funding rates
- open interest
- liquidation data
- options data
- on-chain data
- news or sentiment data
- custom user-provided features
- portfolio, position, and execution state

Data records should support:

- source id
- asset or instrument id
- timeframe or sampling model when applicable
- timestamp range
- schema descriptor
- storage backend reference
- ingestion version
- data quality status

The first implementation may store large time-series data in Postgres tables. The design
should allow later movement of large blobs or columnar data to object storage without
changing strategy contracts.

## Signal Engine Platform

A signal engine is deterministic software that transforms data into signal events.

Signal engines should be registered in the database with:

- `signal_engine_id`
- name
- version
- code package reference
- supported input data types
- output envelope version
- runtime entrypoint
- live scanner entrypoint if applicable
- configuration schema

Signal engines must not be forced into a shared strategy-specific schema.

Every emitted signal should use a minimal envelope:

```json
{
  "signal_id": "...",
  "signal_engine_id": "...",
  "signal_engine_version": "...",
  "asset": "...",
  "instrument": "...",
  "timestamp": "...",
  "data_refs": [],
  "payload_schema": "...",
  "payload": {}
}
```

The `payload` is intentionally open-ended. A Vegas engine, Bollinger engine, order-book
engine, on-chain engine, or custom composite engine may emit completely different payloads.
The platform should store and route these packets without needing to understand every
field.

## Strategy Module Platform

A strategy is deterministic code that consumes a context and returns a decision.

Strategies should be registered with:

- `strategy_id`
- strategy name
- version
- code package reference
- supported signal engine ids or packet schemas
- required data refs
- required feature extractors
- parameter schema
- output decision schema
- execution profile schema
- test suite status

The strategy interface should be broad enough for simple rules and complex systems:

```python
def decide(context: StrategyContext) -> StrategyDecision:
    ...
```

`StrategyContext` should be able to include:

- signal envelope
- arbitrary signal payload
- raw data windows
- derived features
- portfolio state when needed
- prior strategy state when needed
- runtime mode: backtest, paper, live
- immutable config and parameters

`StrategyDecision` should be structured, for example:

```json
{
  "decision_id": "...",
  "strategy_id": "...",
  "strategy_version": "...",
  "signal_id": "...",
  "action": "ENTER",
  "direction": "LONG",
  "confidence": 0.72,
  "reason_code": "forceful_reclaim",
  "execution_profile": {},
  "diagnostics": {}
}
```

The platform should not require the strategy internals to be a rule table. A strategy may
be:

- simple threshold rules
- a decision tree
- a hand-coded state machine
- a statistical model
- an ML model with fixed weights
- an ensemble
- a complex Python module

The only hard requirement is deterministic output for identical inputs, code version, and
parameters.

## Walk-Forward Platform

Walk-forward evaluation should be configurable, not fixed to one calendar pattern.

Each walk-forward template should define:

- training windows
- validation windows
- locked OOS windows
- rebalance cadence
- data sufficiency rules
- signal fitting policy
- scoring metrics
- promotion gates

Examples:

- 2-month train, 1-month validation, 1-week OOS
- 6-month train, 1-month validation, 1-month OOS
- rolling 90-day train, 30-day validation, 14-day OOS
- expanding train window with fixed validation
- event-driven retraining after regime shifts

The system should support multiple templates and allow each strategy family or experiment
to choose one explicitly.

## Backtest Platform

The backtest platform should run deterministic strategy modules against stored signal
events and data refs.

Core requirements:

- batch evaluate many strategy-asset pairs locally
- run Stage 1 directional scoring without an evaluator agent
- run execution simulations using deterministic fills and explicit assumptions
- support Stage 2/3/4 equivalents from the current pipeline
- write immutable run records
- compare strategy versions on the same sample
- report train, validation, and OOS metrics side by side

Backtest output should include:

- run id
- strategy id and version
- signal engine id and version
- dataset versions
- walk-forward template id
- parameter hash
- decision records
- score summaries
- execution assumptions
- failure clusters
- promotion status

## Live Execution Platform

Live execution should use the exact same deterministic strategy module contract as
backtesting.

Live deployment records should specify:

- strategy id and version
- signal engine id and version
- asset / instrument
- account mode
- execution adapter
- risk limits
- schedule or event trigger
- data warmup requirements
- enabled / disabled state
- owner state

Live execution should support many strategy-asset routes in parallel.

The live runtime should:

- warm required data before enabling a route
- run the registered signal engine or consume its live events
- build `StrategyContext`
- call the deterministic strategy module
- validate risk constraints
- submit orders through an exchange adapter
- record every input, decision, and order result

Agents should not be required in the live decision path.

## Frontend Terminal

The frontend is the primary operator surface.

Major views:

- Dashboard: current system health, active deployments, latest walk-forward results
- Data Catalog: datasets, coverage, gaps, quality checks
- Signal Engines: registry, versions, configs, generated signals, live scanner status
- Strategies: modules, versions, parameters, tests, linked engines
- Experiments: walk-forward templates, run queues, backtest results
- Strategy Lab: inspect failures, compare versions, launch agent-assisted edits
- Universe: monthly or scheduled eligible strategy-engine candidates
- Deployment: paper/live routes, cron/event schedules, risk limits, warmup state
- Audit Log: immutable history of code versions, runs, decisions, promotions, deployments

The frontend should make it easy to:

- register a new signal engine
- register a new strategy module
- launch walk-forward runs
- compare strategy versions
- inspect train/validation/OOS performance
- promote or reject a strategy
- enable or disable a live route
- see exactly why a route is blocked from deployment

## Database Direction

Use a relational database as the primary system of record.

Postgres is the default candidate because it supports:

- strong relational contracts
- JSONB for arbitrary packet payloads
- indexing over structured metadata
- transactional audit records
- optional time-series extensions later

Likely core tables:

- `data_sources`
- `datasets`
- `instruments`
- `signal_engines`
- `signal_engine_versions`
- `signals`
- `strategy_modules`
- `strategy_versions`
- `strategy_engine_bindings`
- `walk_forward_templates`
- `walk_forward_runs`
- `backtest_runs`
- `decisions`
- `score_summaries`
- `promotion_records`
- `deployment_routes`
- `live_events`
- `orders`
- `audit_log`

Large payloads may be stored as JSONB initially. The design should allow future offloading
to object storage with database references.

## Agent Role In The New System

Agents should help build and operate the deterministic system, but should not be required
for deterministic evaluation.

Agent responsibilities:

- write new signal engines
- write new strategy modules
- generate feature extractors
- write tests
- inspect failure clusters
- propose rule changes
- convert strategy ideas into deterministic code
- generate Pine scripts or visual inspection tools
- summarize walk-forward performance
- recommend kill / continue / promote decisions

Agent non-responsibilities:

- live trade decision inference
- manual one-by-one packet evaluation as the normal Stage 1 path
- hidden tuning outside recorded code and run artifacts

## Success Metrics

System-level:

- run Stage 1 evaluation over thousands of packets in seconds or minutes
- run 20+ strategy-asset pairs in one batch without manual evaluator prompts
- reproduce the same decisions given the same inputs and strategy version
- promote only strategies with train, validation, and OOS evidence
- execute live routes using the same strategy module tested in backtest

Product-level:

- user can register an engine and strategy from the frontend
- user can launch a walk-forward run from the frontend
- user can compare strategy versions without reading raw JSON files
- user can see why a strategy is not deployable
- user can enable or disable live routes safely

## MVP Scope

MVP should prove the new architecture with one or two strategies, not migrate everything.

MVP features:

1. New project scaffold with frontend, backend API, database, and worker process.
2. Postgres schema for engines, strategies, signals, runs, decisions, and scores.
3. Generic signal envelope storage with JSONB payloads.
4. Deterministic strategy module contract.
5. Stage 1 deterministic backtest runner.
6. Configurable walk-forward template.
7. Minimal frontend views for strategies, runs, scores, and failure inspection.
8. One migrated signal engine adapter from the current workspace.
9. One deterministic strategy module pilot.
10. Agent-facing development notes for how to add new strategy modules.

Recommended pilot:

- use a small existing signal set from the current workspace
- implement a deterministic strategy module
- run train and validation locally
- compare speed and reproducibility against the old agentic evaluator path

## Open Design Questions

- Should strategy modules run in-process Python first, or isolated worker containers from
  day one?
- Should the frontend and backend be a single full-stack app or separate services?
- Should large time-series data live in Postgres initially or in files/object storage with
  database metadata?
- Should strategy package versioning be git commit based, database artifact based, or both?
- What is the minimum sandboxing needed before user-authored strategy code can run safely?
- Should live execution be in the MVP or wait until deterministic backtesting is proven?

## Build Guidance For Future Sessions

Start fresh. Do not extend the current nested `dev/`, `live/`, and `artifacts/` filesystem
as the application architecture.

Use the current workspace as reference material only:

- copy concepts
- port useful scripts selectively
- preserve lessons from failure audits
- avoid inheriting the prose evaluator loop

The first implementation session should produce an architecture plan and project scaffold
for the new terminal, not another strategy training pass.
