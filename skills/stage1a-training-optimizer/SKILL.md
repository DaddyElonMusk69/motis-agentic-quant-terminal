---
name: stage1a-training-optimizer
description: Training-only Stage 1A directional strategy optimization for agentic quant research. Use when Codex is asked to update a deterministic Stage 1A strategy script from a training failure audit, builder_training_sample.json, signal_sample.json, and packet evidence; improve LONG/SHORT agreement on the training sample while preserving protected training matches and avoiding validation, walk-forward, locked OOS, live execution, Stage 1B entry gates, or overfit feature-tree patches.
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
- protected training cases and whether the current strategy matches them
- whether failures are mostly neutrality, wrong-way direction, or both

This replay is the baseline. Do not patch before understanding it.

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
- protected cases pass only because of narrow exceptions while broader monthly behavior degrades

When monthly stability is poor, prefer simplifying or rejecting the candidate update over maximizing aggregate training agreement. A lower aggregate score with smoother monthly behavior is preferable to a brittle high aggregate score.

## Feature Audit Discipline

Run or implement a feature audit only as diagnostics. Useful packet evidence includes multi-timeframe returns, range positions, candle direction, active timeframes, and existing strategy diagnostics.

Feature audit may be used to:

- find broad differences between failed and protected training cases
- identify recurring failure patterns
- rank candidate packet evidence
- test whether a simple rule might help

Feature audit must not be used to:

- copy a fitted classifier into `strategy.py`
- maximize training accuracy at any cost
- hard-code deep decision trees
- add many narrow threshold branches
- justify a rule only because it improves the current replay

Treat feature thresholds as clues. Convert only durable, simple, explainable market patterns into strategy rules.

## Rule Complexity Budget

Keep each iteration small.

- Prefer 1-3 new directional rules.
- Use no more than 2-3 numeric thresholds per rule.
- Round numeric thresholds to broad zones unless exact domain constants already exist.
- Avoid nested logic deeper than two levels.
- Do not add special-case exceptions for protected cases by timestamp or id.
- Do not preserve protected cases with rules that are narrower than the failure pattern itself.

A rule is acceptable only if it can be explained without saying "the tree picked this split."

## Rule Justification

For each candidate rule, be able to state:

- which training failure pattern it targets
- which packet evidence supports it
- why the evidence should be a general directional read
- which protected training pattern it could regress
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
- a protected-case check
- the monthly stability audit
- a diff against the read-only strategy snapshot or a scoped diff of the edited strategy file

Report:

- baseline match/mismatch/neutral counts
- updated match/mismatch/neutral counts
- protected cases preserved or regressed
- worst-month and monthly-stability result
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
- protected-case result
- monthly stability result
- explicit note that validation/walk-forward/OOS was not used
- next action: user should rerun Score on the training iteration
