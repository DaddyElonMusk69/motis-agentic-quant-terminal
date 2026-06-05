---
name: agentic-quant-trading-development
description: Use when creating, validating, optimizing, or operating an agentic quant trading workspace from scratch, including data prep, training sessions, staged scoring, strategy skill iteration, live readiness handoff, and promotion discipline.
---

# Agentic Quant Trading Development

## Purpose

Use this skill as the canonical operating procedure for agentic quant trading work.

The goal is that a fresh agent can start from an empty scaffold, install the signal engine
and strategy skills, fetch data, generate neutral signals, optimize a strategy, and prepare
live execution handoff without relying on hidden history from another workspace.

## Core Boundaries

- Signal engines are deterministic attention systems.
- Signal engines own only neutral signal generation, live scanning, live cache support,
  and wake routing.
- Signal packets are neutral evidence, not trade calls.
- Strategy skills own trading judgment and position management.
- This skill owns data prep, scoring tools, optimization process, artifact contracts,
  staged evidence discipline, and promotion rules.
- Training artifacts are evidence, not strategy rules.
- Live state is runtime state, not training evidence.

## Agent Role Boundaries

Use the repository role boundary before taking action.

Quant strategist:

- owns data fetching, data validation, neutral signal generation, Stage 0/2/3 scoring,
  training-session setup, sample selection, failure audit, strategy-skill iteration, and
  promotion recommendations
- writes evaluator handoff docs and exact `signal_sample.json` files
- must not act as the unbiased backtester for its own handoff batch
- must not execute live trades

Backtester:

- owns unbiased evaluation of the exact signals assigned by the strategist
- reads only the handoff, `signal_sample.json`, listed signal packets, and active strategy
  skill
- preserves packet order and writes the requested decisions/results
- must not choose additional signals, inspect future candles, read ground truth, inspect
  scoring outputs, alter strategy skills, tune parameters, or execute trades

Executor:

- owns live cron/router operation and live trade execution
- uses exchange truth for balances, positions, orders, fills, and protection
- follows the dedicated execution deployment skill for cron/router wiring and runtime safety
- installs the latest promoted strategy skill into its own skill set before live operation
- warms the live candle cache before enabling a cron
- writes routing owner state only as required after fresh entry order submission
- must not run research backtests, optimize strategy wording, or alter training artifacts

## Workspace Map

Expected root:

```text
dev/        historical data, replay signals, training sessions, audits
live/       live cache, live signal packets, router state, runtime logs
artifacts/  signal engine, workflow skill, strategy skills, portable docs
```

For exact folder contracts, read `references/workspace-contract.md`.

## First Action Checklist

When starting in a workspace:

1. Validate the scaffold with `scripts/validate_workspace.py`.
2. Read `workspace_manifest.json`.
3. Confirm the signal engine exists under `artifacts/signal_engine/`.
4. Confirm the target strategy skill exists under `artifacts/skills/strategies/`.
5. Identify whether the task is data prep, signal generation, optimization, skill update,
   or live operation.
6. For signal artifacts, enforce `dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets/`.
7. For strategy skills, enforce `references/strategy-skill-contract.md`.
8. Load only the reference file for that task.

`signal_engine_id` is the canonical signal producer identity. Current ids use the existing
engine root names, such as `vegas_ema` and `bollinger`; `signal_family` is legacy metadata
only.

## Progressive References

- Data setup: `references/data-fetch-and-prep.md`
- Signal engine requirements: `references/signal-engine-contract.md`
- Signal set generation: `references/signal-generation.md`
- Strategy skill structure: `references/strategy-skill-contract.md`
- Optimization stages: `references/optimization-stages.md`
- Agent evaluation prompts: `references/agent-evaluation-prompts.md`
- Scoring artifact format: `references/scoring-artifacts.md`
- Failure audits and skill updates: `references/failure-audit-and-skill-updates.md`
- Live operation: `references/live-trading-contract.md`
- Router and owner state: `references/router-and-ownership.md`
- Execution deployment SOP: `../execution/live-execution-sop/SKILL.md`
- Manifests and schemas: `references/session-manifest-schema.md`
- Promotion gate: `references/promotion-checklist.md`

## Canonical Optimization Flow

1. Stage 0: One-time deterministic foundation: data, signal pool, travel distribution,
   threshold calibration, and ground truth.
2. Monthly walk-forward universe: build the current month tradability record from Stage 0
   branch decisions before choosing Stage 1 work.
3. Stage 1A: Directional agreement against natural direction.
4. Stage 1B: Opportunity screening, only for research/watchlist sparse-pool work.
5. Stage 2: Travel capture analysis on calls that pass the judgment layer.
6. Stage 3: Conditional execution setup, TP/SL, pyramiding, hold time, and management design on correct calls.
7. Stage 4: Full-decision realized expectancy and final execution-setup promotion.
8. Stage 5: Live readiness handoff.

Stage 5 in this skill is a research-to-execution handoff, not the execution runbook itself.
Once a strategy-engine candidate is promoted and appears in the monthly tradable universe,
the execution agent should switch to the dedicated execution SOP at:

`artifacts/skills/execution/live-execution-sop/SKILL.md`

This research skill should only verify that the handoff artifacts exist and are current:

- the exact `strategy_id` row exists in the current month `tradable_universe.json`
- the row has the expected `signal_engine_id`
- the promoted strategy skill exists under `artifacts/skills/strategies/<STRATEGY_ID>/`
- the live execution contract references remain consistent with the current research artifacts

Do not duplicate detailed cron, router, launcher, skill-install, or warmup procedures in
this skill. Those procedures belong to the execution SOP so live deployment rules stay in
one place.

Stage 0 is computed once per strategy/signal-set cycle and stored under
`dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/`. Do not put Stage 0 inside
session or iteration folders. Within one retraining cycle, Stage 0 should cover the full
frozen scoreable horizon that will later be sliced into training, validation, and locked
OOS samples. The repeated loop is Stage 1 onward: create a session, sample signals,
evaluate with a strategy skill, score, audit, update the skill, and create the next
iteration.

Stage 1 and later samples must be latest-regime-first and chronological. Optimize on the
recent regime that matters for live trading, validate forward, and keep the newest slice as
untouched OOS. Older historical data is stress evidence, not the primary source for skill
wording, unless the session explicitly targets that older regime.

After each month's Stage 0 pass, build `dev/walk_forward/<YYYY-MM>/` and treat it as the
cross-strategy tradability source of truth for that retraining cycle. Universe rows are
strategy-engine candidates keyed by `strategy_id`, with `signal_engine_id` identifying the
scanner/signal root. The same asset can appear more than once if different strategies use
different signal engines. Rows with `trigger_rate >= 80%` are Path A and may appear in
`tradable_universe.json`. Rows below that threshold are Path B and belong in
`watchlist_universe.json` as research-only by default. Do not treat Path B as the normal
live-trading route unless a user explicitly opens a research exception.

For monthly freshness, do not treat "a current-month session exists" as sufficient. Mark a
strategy row as `retrained_for_month` only when the current walk-forward month has a
matching signal-set session plus a promotion-grade Stage 4 expectancy artifact under
`promotion/final_report.md`. If the strategy has current-month research or directional
sessions but no completed Stage 4 promotion pass for that month, keep it `stale`.

For crypto `2h` signals with a roughly `36h` forward window, default to training on the two
complete calendar months immediately before the validation month, validating on the latest
scoreable month excluding the locked newest slice, and reserving that newest slice as
untouched OOS. At the beginning of June 2026, that means March-April training, May
validation, and the newest May slice as OOS.

Once Stage 1A passes its locked OOS check, freeze the final directional strategy version
and run one canonical final Stage 1A scoring pass across the full frozen cycle before Stage
2 or Stage 3 begins. For the June 2026 cycle, that canonical final pass must cover:

- March-April training
- May forward validation excluding the locked newest slice
- the locked newest May slice

That canonical final pass is a readout, not another tuning pass. Do not keep editing the
strategy between the locked OOS pass and the full-cycle final readout unless you explicitly
reopen Stage 1 work. Stage 2 and Stage 3 must use the match set produced by that frozen
final Stage 1A pass rather than an older partial-slice run. Stage 4 must use the full
frozen Stage 1A decision set from that same final pass, not the match-only subset.

For Stage 4, shortlist execution candidates into the template
`templates/stage4_candidates.json`, then score them with
`scripts/optimization/stage4_realized_expectancy.py`. Use the same frozen full-cycle Stage
1A score file, the raw `5m` candles, and explicit slice windows for train, validation, and
locked OOS. Promotion-grade Stage 4 uses the default `close_at_cutoff` timeout realization
for filled trades that never touch TP or SL.

Stage 0 for that June cycle should be built once across the full frozen March-May scoring
horizon, not separately for March-April and then again for May. In other words:

- fit the signal-set definition on the training window only if fitting is required
- freeze that signal-set definition
- generate the cycle signal pool across the full frozen horizon needed for training,
  validation, and locked OOS
- compute travel distribution, threshold calibration, and ground truth once on that full
  horizon
- slice the shared Stage 0 records by timestamp for Stage 1 training, validation, and OOS

When the monthly policy says "do not do anything with June" for a June 2026 cycle, keep the
signal set frozen through `2026-06-01T00:00:00Z` but score Stage 0 only on the subset whose
full forward window stays inside May. With the default `36h` forward window and `5m`
candles, that means:

- latest allowed outcome candle: `2026-05-31T23:55:00Z`
- latest scoreable signal timestamp: `2026-05-30T11:55:00Z`

`scripts/run_monthly_stage0.py` enforces this by validating candle coverage only through the
last May candle and by scoring a deterministic subset of packets up to the scoreable-signal
cutoff. Do not extend Stage 0 candle truth into June merely to score the newest May signals.

Do not create one Stage 0 for the training slice and a second Stage 0 for the validation
slice when both belong to the same retraining cycle.

If the signal filter, vote threshold, asset shortlist, dedupe rule, or signal-generation
threshold is fitted differently, it creates a different signal set and needs its own Stage
0. Fit those choices on the training window only, then freeze them and apply them forward
to validation and OOS. Once frozen, Stage 0 should be generated once for the whole cycle
horizon that those later stages will sample from.

For normal monthly retraining, use `scripts/run_monthly_stage0.py` as the deterministic
Stage 0/universe orchestrator. It computes the calendar windows, validates data coverage,
generates each configured engine-qualified signal set, runs the Stage 0 step tools, writes
Stage 0 manifests, then builds and validates `dev/walk_forward/<YYYY-MM>/`.

When a canonical replay signal set already exists and only the tail horizon is stale, do
not blindly regenerate the whole set. Extend it with
`artifacts/signal_engine/scripts/signals/fill_signal_set_tail.py`, which replays from the
last emitted packet timestamp, preserves dedupe continuity, merges by canonical packet
timestamp filename, and rewrites the manifest with the updated `end_ts`, `packet_count`,
and required `signal_engine_id`.

Example:

```bash
python3 artifacts/signal_engine/scripts/signals/fill_signal_set_tail.py \
  --signal-engine-id bollinger \
  --asset BTC \
  --signal-set-id 2026-BTC-2h-dedupe-vote2 \
  --target-end 2026-06-01T00:00:00Z
```

Use the individual Stage 0 step tools manually only for diagnostics, repairs, or explicitly
non-monthly experiments. Manual runs must still use the same frozen cycle horizon; do not
improvise different Stage 0 horizons for training, validation, and OOS slices.

Read `references/optimization-stages.md` before running or judging any training session.
Read `references/agent-evaluation-prompts.md` before launching sub-agents or asking a
model to evaluate signal packets.

## Strategy Update Rule

Do not update a strategy skill from anecdotes. A promoted strategy update requires:

- a training session manifest
- scored results
- a written failure audit
- the exact strategy skill version being changed
- the exact strategy skill snapshot used for each iteration
- the intended behavioral change
- regression risk
- a retest plan

For every new iteration, create an immutable copy of the strategy skill inside:

`iterations/<ITERATION_ID>/source_artifacts/strategy_skill_snapshot/`

Evaluators must read that snapshot path only. Do not evaluate against the mutable
`artifacts/skills/strategies/<STRATEGY_ID>/` path after iteration creation.

For Stage 1B/path-B updates, opportunity screening must be expressed as an entry-gate
change in the strategy skill's main `SKILL.md`. The updated skill must state what packet
evidence permits `trade_action: ENTER`, what packet evidence requires `trade_action: SKIP`,
and that a directional bias alone is not permission to trade. Do not update only soft
confidence language such as "travel may be limited", "be careful", or travel-conviction
wording. Low expected travel means skip the signal, not enter with smaller size or lower
confidence.

For sparse-pool/path-B work, default to Stage 1B before Stage 1A. First make the entry gate
work: the strategy should lean `ENTER` by default and name only the specific conditions that
require `SKIP`. Do not begin by tightening direction language to force more skips. Only
after Stage 1B passes its gate should Stage 1A directional optimization become the active
focus for that strategy/version.

Because Path B is research/watchlist by default in the monthly universe, only perform this
sparse-pool loop when the user intentionally chooses to research a non-tradable candidate.

Stage 1A directional logic and Stage 1B entry logic must stay separate in strategy edits.
Stage 1A rules choose `LONG` or `SHORT`; they must not decide `ENTER` or `SKIP`. Stage 1B
rules consume the chosen direction plus packet evidence; they must not treat directional
confidence, macro bias, or a plausible side as entry permission. Computed features are a
strategist development aid for rewriting the skill, not part of what the evaluator or live
execution agent should see. When Stage 1B errors show direction evidence leaking into
`trade_action`, update the strategy skill to make the two-step contract explicit before
further tuning.

If the first scored Stage 1A iteration for an active sample finishes below its promotion
threshold, run `scripts/analysis/signal_feature_audit.py` before writing the next strategy
skill update. Do not continue Stage 1A rewrites from anecdotes alone once the first failing
score exists.

If the first scored Stage 1B iteration for an active sample finishes below either its
precision or recall threshold, run
`scripts/analysis/stage1b_entry_classifier_audit.py` before writing the next strategy skill
update. Do not continue Stage 1B entry-gate rewrites from anecdotes alone once the first
failing score exists.

## Scripts

This skill includes small workflow scripts. They are tools for the agent to call at
specific workflow steps, not an automated replacement for the agent.

- `scripts/validate_workspace.py`
- `scripts/scaffold_workspace.py`
- `scripts/build_data_manifests.py`
- `scripts/build_stage0_manifest.py`
- `scripts/build_walk_forward_universe.py`
- `scripts/validate_walk_forward_universe.py`
- `scripts/run_monthly_stage0.py`
- `scripts/data/build_training_data.py`
- `scripts/data/fetch_binance_5m_history.py`
- `scripts/data/fetch_okx_5m_bulk.py`
- `scripts/data/fetch_okx_5m_history.py`
- `scripts/data/fetch_okx_5m_history_cli.py`
- `scripts/new_training_session.py`
- `scripts/validate_training_session.py`
- `scripts/new_iteration.py`
- `scripts/validate_iteration.py`
- `scripts/validate_strategy_skill.py`
- `scripts/seed_vegas_ema_skill.py`
- `scripts/summarize_training_session.py`
- `scripts/validate_signal_sets.py`
- `scripts/optimization/max_travel_distribution.py`
- `scripts/optimization/significance_threshold_calibration.py`
- `scripts/optimization/signal_ground_truth.py`
- `scripts/optimization/stage1a_directional_score.py`
- `scripts/optimization/stage1b_screening_eval.py`
- `scripts/optimization/stage2_capture_curve.py`
- `scripts/optimization/stage3_grid_search.py`
- `scripts/optimization/stage3_limit_grid.py`
- `scripts/optimization/stage3_pyramid.py`
- `scripts/analysis/signal_feature_audit.py`
- `scripts/analysis/stage1b_entry_classifier_audit.py`
- `scripts/analysis/inspect_replay_snapshot.py`

One-off artifact-normalization scripts do not belong in this skill. Keep only files
that conform to the canonical workspace shape.

Signal generation and live routing scripts stay under `artifacts/signal_engine/scripts/`.
