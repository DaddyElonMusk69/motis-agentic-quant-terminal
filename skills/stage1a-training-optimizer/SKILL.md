---
name: stage1a-training-optimizer
description: Use when Codex is asked to update or review a deterministic Stage 1A direction-only strategy from training failure audits, builder_training_sample.json, signal_sample.json, packet evidence, or first-iteration strategy-builder training bundles.
---

# Stage 1A Training Optimizer

## Purpose

Optimize a Stage 1A direction-only strategy script against the current training bundle, then stop. The user will run Score, validation, walk-forward, or locked OOS outside this skill.

Stage 1A asks one question: for scoreable signals, should `decide(...)` return `LONG` or `SHORT`? It does not decide whether the trade is worth entering.

## Hard Boundaries

- Read only the artifacts named in the user's training-iteration request.
- Use training labels only from `builder_training_sample.json`.
- Do not read validation, walk-forward, locked OOS, future candles, score files from later gates, or live state.
- Do not tune to exact timestamps, signal ids, or date clusters.
- Do not discover rules by blind candidate sweeps. Derive candidate tests from pre-edit ground-truth attribution.
- Do not treat an arbitrary/base strategy as directional authority, especially for `iter_001`.
- Do not add Stage 1B entry gates, opportunity filters, expected-travel filters, trade management, order routing, exchange calls, randomness, or network access.
- Do not claim promotion readiness. Report only training-sample behavior and tell the user to rerun Score.
- Do not edit read-only snapshots, sample files, signal packets, audit files, or evaluator handoff files.

## Required Inputs

Before editing, read:

- `failure_audit.json`
- `failure_audit.md`
- `builder_training_sample.json`
- `signal_sample.json`
- the mutable session `strategy_module/strategy.py`
- the iteration `source_artifacts/strategy_module_snapshot` as read-only evidence of what failed

If any required training artifact is missing, stop and report the blocker.

## Baseline Replay

Before proposing rules, replay the current strategy on the training sample and report or record:

- scoreable count and LONG/SHORT label balance
- match, mismatch, and neutral counts
- failure counts by `reason_code`, truth direction, and decision direction
- whether failures are mostly neutrality, wrong-way direction, or both

This replay is the baseline. Do not patch before understanding it.

For `iter_001`, baseline replay establishes the evaluator contract and failure surface only. It does not validate the base strategy's directional thesis.

## Iteration 001 Base Strategy Handling

If the request or path indicates `iter_001`, assume the supplied strategy may be a generic/base scaffold with arbitrary direction rules unless the user explicitly says otherwise.

For `iter_001`:

- Use the strategy script to learn the decision contract, output fields, diagnostics shape, and replay baseline.
- Treat existing `reason_code`s as diagnostic buckets, not as proof that those rules are market-valid.
- Do not use baseline direction choices as a directional prior.
- Candidate rules must come from ground-truth attribution across packet evidence, not from guessing patches to existing scaffold logic.
- Prefer replacing broad seed logic when attribution supports it over stacking overlays onto arbitrary base behavior.

## Monthly Stability Audit

Before handing back any edited strategy, evaluate training performance by calendar month. Use only timestamps and labels from the training sample.

Report or record:

- scoreable signal count per month
- monthly match, mismatch, and neutral counts
- monthly directional agreement
- monthly LONG agreement and SHORT agreement when enough samples exist
- worst-month agreement
- whether the improvement is concentrated in only one or two months
- whether any month regresses sharply from the baseline
- whether any side collapses, such as LONG working while SHORT fails

Use monthly stability as a training-only robustness check, not as a source for timestamp rules.

Flag the strategy as unstable when:

- aggregate training agreement improves but one or more meaningful months collapse
- monthly variance is high enough that the aggregate score hides regime dependence
- the updated rules help dense months while damaging sparse months
- a month with enough scoreable signals falls materially below random directional agreement

When monthly stability is poor, prefer simplifying or rejecting the candidate update over maximizing aggregate training agreement. A lower aggregate score with smoother monthly behavior is preferable to a brittle high aggregate score.

## Feature Audit Discipline

Before rule discovery, inspect the current embedded packet structure in `builder_training_sample.json` and `signal_sample.json`. Signal packet schemas vary; do not assume prior field names, nesting, chart columns, completion flags, or diagnostic shapes.

Inventory:

- top-level packet keys and nested evidence-like dictionaries
- chart/timeframe objects, row containers, column headers, and completion flags
- price, range, return, wick, volume, OI, long/short ratio, divergence, and active-timeframe fields
- missing/null rates and type consistency
- whether candle rows are completed or partial before using them as regime evidence

Run or implement feature audit only as diagnostics.

Feature audit may be used to:

- find broad differences between ground-truth `LONG` and `SHORT` labels
- find broad differences between failed and matched training cases
- identify recurring failure patterns
- rank candidate packet evidence
- prepare the pre-edit attribution report

Feature audit must not be used to:

- copy a fitted classifier into `strategy.py`
- maximize training accuracy at any cost
- hard-code deep decision trees
- add many narrow threshold branches
- justify a rule only because it improves the current replay

Treat feature thresholds as clues. Convert only durable, simple, explainable market patterns into strategy rules.

## Pre-Edit Ground Truth Attribution

Before testing candidate rule changes or editing `strategy.py`, run a training-only attribution pass against labels from `builder_training_sample.json`.

The attribution pass must summarize:

- `LONG` vs `SHORT` label balance, plus any `None`/unscoreable label behavior used by the evaluator
- baseline matches/mismatches by `reason_code`, truth direction, decision direction, and month
- feature distributions for `LONG` vs `SHORT` labels using available packet evidence
- feature distributions for baseline matches vs mismatches
- independent directional agreement of broad sources when available, such as 5m, 2h, 8h, 1d, range location, volume, OI, long/short ratio, and divergence
- support counts, missing rates, and broad effect sizes for any promising feature family
- side and monthly stability for each promising attribution pattern

End attribution with 1-3 ranked hypotheses that can be stated as market-readable directional ideas. Each candidate rule tested later must trace to one of these hypotheses.

Do not:

- scan many thresholds and pick the best replay result without a prior attribution hypothesis
- copy a fitted classifier, feature tree, or exact split set into `strategy.py`
- justify a rule only because it improves aggregate replay
- use baseline scaffold rules as the source of the hypothesis for `iter_001`

## Rule Complexity Budget

Keep each iteration small.

- Prefer 1-3 new directional rules.
- Use no more than 2-3 numeric thresholds per rule.
- Round numeric thresholds to broad zones unless exact domain constants already exist.
- Avoid nested logic deeper than two levels.
- Do not add special-case exceptions for baseline matches by timestamp or id.

A rule is acceptable only if it can be explained without saying "the tree picked this split."

## Rule Justification

For each candidate rule, be able to state:

- which pre-edit attribution hypothesis it implements
- which training failure pattern it targets
- which packet evidence supports it
- support count, expected fixed/broken risk, and side/month stability from attribution
- why the evidence should be a general directional read
- why it remains Stage 1A direction-only

Reject candidate rules that do not have a clear directional interpretation.

## Editing Strategy.py

Patch only the mutable session strategy file named by the user.

The edited `decide(...)` must:

- return a deterministic StrategyDecision-compatible object or dict
- choose `LONG` or `SHORT` for scoreable signals when sufficient packet context exists
- include `confidence`
- include a stable `reason_code`
- include diagnostics explaining the packet evidence used
- preserve the existing decision contract fields used by the evaluator

Use existing local patterns in the strategy file. Add helper functions only when they reduce repeated logic or make diagnostics clearer.

## Training Verification

After editing, run:

- Python syntax verification, such as `PYTHONDONTWRITEBYTECODE=1 python3 -m py_compile <strategy.py>`
- a replay of the training sample
- the monthly stability audit
- a rule-delta attribution showing fixed and broken cases by old reason, old direction, new direction, truth direction, side, and month
- a diff against the read-only strategy snapshot or a scoped diff of the edited strategy file

Report:

- baseline match/mismatch/neutral counts
- updated match/mismatch/neutral counts
- pre-edit attribution hypothesis used
- worst-month and monthly-stability result
- fixed and broken counts attributable to each changed rule
- changed rule summary
- targeted training failure patterns
- any verification command that could not be run

Do not treat the training replay as promotion evidence.

## Walk-Forward And OOS Handling

If the user provides validation, walk-forward, or locked OOS failure evidence and asks for a patch, do not edit from that evidence unless they also provide a fresh training bundle explicitly designated for optimization.

For failed validation, walk-forward, or locked OOS requests:

- write a postmortem only when instructed
- identify general failure hypotheses
- recommend a fresh training cycle if needed
- do not create same-cycle revision rules from gate labels

The user owns running Score and walk-forward after the training patch.

## Final Response Shape

Keep the final response concise:

- file edited
- deterministic rules changed
- training replay result
- monthly stability result
- explicit note that validation/walk-forward/OOS was not used
- next action: user should rerun Score on the training iteration
