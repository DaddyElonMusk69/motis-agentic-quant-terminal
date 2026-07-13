---
name: stage1a-training-optimizer
description: Use when Codex is asked to update or review a deterministic Stage 1A trade-decision strategy from training failure audits, builder_training_sample.json, signal_sample.json, packet evidence, or first-iteration strategy-builder training bundles.
---

# Stage 1A Training Optimizer

## Purpose

Optimize a Stage 1A trade-decision strategy against the current training bundle, then stop.

Stage 1A answers one signal-time question only: should `decide(...)` enter `LONG`, enter `SHORT`, or remain `FLAT`? It does not decide sizing, exits, trade management, or live execution.

The user will run Score, validation, walk-forward, and locked OOS outside this skill.

## Quant Research Stance

Act like a quant researcher looking for recurring signal-time regimes that should generalize, not like a replay optimizer trying to squeeze training accuracy from one bundle.

Primary objective:

- identify broad market states in which the signal has a stable directional edge
- express those states as a small deterministic ruleset
- reject the update when the evidence does not support a robust regime read

Secondary objective:

- improve training replay only after a regime thesis is established

Replay improvement is evidence, not the goal. A candidate that lifts aggregate training metrics but depends on narrow thresholds, one or two months, side collapse, or heavy skip conversion should be rejected.

## Hard Boundaries

- Read only the artifacts named in the user's training-iteration request.
- Use labels only from `builder_training_sample.json`.
- Do not read validation, walk-forward, locked OOS, future candles, later score files, or live state.
- Do not tune to exact timestamps, signal ids, or date clusters.
- Do not use blind threshold sweeps or brute-force tree mining as the basis for rules.
- Do not copy fitted classifiers, exact split sets, or opaque trees into `strategy.py`.
- Do not treat an arbitrary base strategy, especially `iter_001`, as directional truth.
- Do not add Stage 1B travel filters, trade management, exchange calls, randomness, or network access.
- Do not invent `SKIP` rules unless the labels support a real no-trade class or the user explicitly asks for supervised skip handling.
- Do not claim promotion readiness. Report training-only behavior and tell the user to rerun Score.
- Do not edit read-only snapshots, sample files, signal packets, audit files, or evaluator handoff files.

## Required Inputs

Before editing, read:

- `failure_audit.json`
- `failure_audit.md`
- `builder_training_sample.json`
- `signal_sample.json`
- the mutable session `strategy_module/strategy.py`
- the iteration `source_artifacts/strategy_module_snapshot` as read-only evidence of what failed

If any required artifact is missing, stop and report the blocker.

## Decision Contract And Scorer Semantics

Treat action and direction as separate fields:

- Enter long: `trade_action: ENTER`, `action: ENTER`, `direction: LONG`
- Enter short: `trade_action: ENTER`, `action: ENTER`, `direction: SHORT`
- Skip: `trade_action: SKIP`, `action: SKIP`, `direction: FLAT`

Never put `SKIP` in `direction`. Never pair `ENTER` with `FLAT` or `SKIP` with `LONG` or `SHORT`.

Before replay, inspect the actual `StrategyDecision` validation and Stage 1 scoring code. Reproduce the real scorer semantics exactly:

- `SKIP` action or `FLAT` direction -> `NEUTRAL`
- entered direction equal to a present truth direction -> `MATCH`
- every other entered `LONG` or `SHORT` -> `MISMATCH`, including entries on neutral, no-trigger, null, or missing truth
- `scoreable = MATCH + MISMATCH`
- `directional_agreement = MATCH / scoreable`, or `0` when scoreable is zero

A `FLAT/SKIP` decision is not a correct third-class match. It is neutral and excluded from the directional-agreement denominator.

## Baseline Replay

Before proposing any patch, run the current strategy through the real Stage 1 replay and scoring path when available.

Record:

- total count, scoreable count, and neutral/no-trigger/null-truth count
- LONG and SHORT truth balance
- match, mismatch, and neutral counts
- failures by `reason_code`, truth direction, and decision direction
- whether the baseline failure surface is mainly wrong-way direction, over-entry, over-skip, or mixed

For `iter_001`, use baseline replay to understand the evaluator contract and failure surface only. Do not treat the seed direction logic as a prior.

## Regime-First Workflow

Use this workflow in order.

### 1. Audit The Packet Surface

Inspect the actual packet structure in `builder_training_sample.json` and `signal_sample.json`.

Inventory:

- top-level keys and nested evidence objects
- chart and timeframe containers
- row tables, columns, and completion flags
- derived feature blocks
- missing and null rates
- any leakage or post-signal fields that must be excluded

Only signal-time-safe inputs may be used later.

### 2. Build A Factor Catalog

Discover factors from the packet rather than assuming legacy field names.

Search at least:

- `signal`, `payload`, `payload.evidence`, and nested evidence dictionaries
- chart and timeframe tables
- active timeframe and event metadata
- price, trend, EMA, tunnel, range, volume, volatility, OI, long-short ratio, taker flow, divergence, and regime fields

For each usable factor, record:

- stable path
- source family
- scalar, categorical, or table-derived type
- availability count and missing rate
- whether it is signal-time-safe

If table data contains useful information, derive simple causal factors such as latest completed return, slope sign, range location, EMA distance, or broad bucket state.

### 3. Form Regime Hypotheses Before Rule Testing

Before scoring candidate rules, state one to three market-readable hypotheses such as:

- the signal is only directional when higher-timeframe trend and local trigger agree
- the signal behaves differently in compression versus expansion regimes
- OI confirmation improves price-following signals while OI divergence supports fades

Each candidate rule tested later must trace back to one of these hypotheses.

### 4. Evaluate Hypotheses With Deterministic Code

Use deterministic counting and scoring code, not eyeballing.

For each hypothesis or broad factor family, measure:

- support count
- LONG and SHORT label split
- scorer-native match, mismatch, neutral, and directional agreement
- month-by-month behavior
- side stability
- missing behavior
- whether the effect remains after simplifying to broad zones

When useful, compare broad families such as:

- price trend by timeframe
- higher-timeframe completed returns
- EMA slope, distance, and stack state
- range location, volatility, ATR, and wick rejection
- volume expansion or contraction
- OI return, OI z-score, OI-price confirmation or divergence
- long-short ratio and taker-flow confirmation

### 5. Use Internal Robustness Checks Inside Training

Validation and OOS are out of scope, but the skill should still reject fragile patterns.

Within the training window, check:

- monthly stability
- early-versus-late consistency
- whether one side collapses
- whether the candidate only works in one calendar pocket
- whether the improvement comes mostly from converting wrong entries into neutral skips

Do not create timestamp rules or date-cluster exceptions from these diagnostics.

### 6. Extract A Small Ordered Ruleset

Only after a regime thesis survives the checks above may it become code.

Prefer:

- one broad default thesis plus one or two robust overrides
- coarse thresholds or discrete states
- clear market-readable `reason_code`s

Reject:

- deep conjunction mining
- narrow threshold tuning
- brittle exception branches
- rules whose main effect is to neutralize mistakes with unsupported skips

If no stable regime survives simplification, report that no robust Stage 1A edge was found and do not patch the strategy just to improve replay.

## Skip Handling

Natural neutral or no-trigger truth is part of the evaluator surface.

When such labels exist:

- count them explicitly
- use scorer-native aggregate metrics as the primary evaluation
- treat skip precision, skip recall, no-trigger capture, over-entry, and over-skip as secondary diagnostics
- reject skip rules that merely hide directional mistakes unless they reflect a stable no-trade regime with broad support

## Generalization Guardrails

Every accepted rule must satisfy all of the following:

- it implements a stated pre-edit regime hypothesis
- it is market-readable in plain language
- it uses only signal-time-safe inputs
- it has meaningful support overall and across multiple months when data allows
- it survives simplification to broad thresholds or state buckets
- it adds value after considering overlap with simpler rules
- it does not rely on one side carrying the whole result unless the rule is explicitly side-specific and stable

Prefer rejection over complexity. "No robust regime edge found" is an acceptable result.

## Rule Extraction Dossier

Before editing `strategy.py`, write a short dossier for accepted and rejected candidate rules.

For each candidate, include:

- regime hypothesis implemented
- market-readable rule statement
- factor families used
- broad predicate shape
- support count and missing behavior
- LONG, SHORT, and neutral label mix when applicable
- aggregate directional agreement
- monthly and side stability
- skip impact when the rule predicts `SKIP`
- expected fixed and broken counts versus baseline
- accept or reject decision and why

No dossier, no edit.

## Editing Strategy.py

Patch only the mutable session strategy file named by the user.

The edited `decide(...)` must:

- remain deterministic
- return a StrategyDecision-compatible dict or object
- preserve contract fields expected by the evaluator
- emit `ENTER` plus `LONG` or `SHORT`, or emit `SKIP` plus `FLAT`
- include `confidence`
- include stable `reason_code`s
- include diagnostics that explain the regime evidence used

Follow existing local patterns unless they conflict with the evaluator contract.

## Training Verification

After editing, run:

- Python syntax verification
- the real Stage 1 replay and scoring path on the training sample when available
- monthly stability audit
- a rule-delta audit showing what was fixed and what broke

Report:

- baseline and updated match, mismatch, and neutral counts
- baseline and updated directional agreement
- neutral/no-trigger truth rate and final strategy skip rate when applicable
- the regime hypotheses tested
- accepted and rejected candidate rules
- worst-month result and overall monthly-stability judgment
- fixed and broken counts attributable to each changed rule
- any command that could not be run

Do not use validation, walk-forward, or locked OOS evidence for the patch.

## Final Response Shape

Keep the final response concise:

- file edited
- regime hypotheses tested
- deterministic rules changed
- accepted and rejected candidate rules
- training replay result
- monthly stability result
- explicit note that validation, walk-forward, and OOS were not used
- next action: user should rerun Score on the training iteration
