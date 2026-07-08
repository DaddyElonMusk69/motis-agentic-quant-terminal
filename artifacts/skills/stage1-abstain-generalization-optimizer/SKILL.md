---
name: stage1-abstain-generalization-optimizer
description: Use when optimizing or postmorteming Motis Stage 1 directional strategies where walk-forward instability, low-travel or natural_direction=None labels, abstain/SKIP policy, regime recognition, EMA packet context, or generalization matters more than maximizing same-sample training accuracy.
---

# Stage 1 Abstain Generalization Optimizer

## Purpose

Use this skill for Stage 1 research when the problem may not be "pick LONG or SHORT better" but "stop forcing a direction on low-edge signals." It separates directional classification, abstention/no-trade filtering, and regime diagnosis so an agent does not patch a brittle tree from failed walk-forward labels.

This is a research and strategy-building skill. It is less restrictive than the conservative Stage 1A training-only optimizer, but it must still protect validation integrity.

## Core Principle

Treat Stage 1 as two coupled models:

- **Direction model:** on scoreable directional labels, choose `LONG` or `SHORT`.
- **Abstain model:** identify rows whose packet context implies low travel, ambiguous regime, conflicting timeframe evidence, or `natural_direction=None`.

Do not let either model hide weakness in the other. A high directional score after skipping many rows is only useful if the skip policy is learned from training evidence and transfers across months or a fresh sample.

## Inputs To Read

Read the available artifacts before proposing changes:

- mutable `strategy_module/strategy.py`
- current iteration `builder_training_sample.json`
- current iteration `signal_sample.json`
- failure audit files when present
- source strategy snapshot when present
- walk-forward or validation score files only when the user asks for postmortem, stability diagnosis, or abstain theory testing

If the user explicitly says "do not update," run diagnostics only and do not edit strategy files.

## Integrity Rules

- Use training labels for rule construction.
- Use walk-forward, validation, locked OOS, or live results only for postmortem, hypothesis ranking, and deciding what fresh training objective to request.
- Do not patch thresholds directly from failed walk-forward labels.
- Do not tune to signal ids, exact timestamps, date clusters, or one-cycle leaf quirks.
- Do not optimize for aggregate accuracy alone. Require side balance, monthly stability, and skip-cost accounting.
- Do not create nested trees unless the branches express a durable market structure such as trend exhaustion, EMA reclaim, range compression, or conflicted timeframe regime.
- If abstain/SKIP rows remain scoreable mismatches in the evaluator, treat abstention as Stage 1B/no-trade research rather than Stage 1A promotion evidence.

## Diagnostic Workflow

1. **Replay current behavior.**
   Record total rows, matches, mismatches, neutral/none labels, LONG/SHORT truth balance, decision balance, reason-code counts, and confusion matrix.

2. **Split failure modes.**
   Separate:
   - wrong directional rows: truth is `LONG` or `SHORT`, decision is the opposite
   - no-direction rows: truth or `natural_direction` is `None`
   - missing-context skips
   - unstable protected cases, if any

3. **Run packet feature audit.**
   Extract broad packet features only:
   - multi-timeframe returns: `5m`, `2h`, `8h`, `1d`
   - EMA distances and fast/mid/slow spread
   - range size, close position, upper/lower wick share
   - forming candle body/range and source candle count
   - active timeframe and cluster vote count

4. **Test abstain ceiling.**
   Measure oracle skip outcomes separately:
   - skip only training `natural_direction=None`
   - skip low-travel rows below the significance threshold if travel fields exist
   - skip current wrong rows only as a label-leaky upper bound

   Report retained accuracy, number skipped, wrong skipped, correct skipped, and none skipped. Label oracle or walk-forward-fitted results as non-patchable.

5. **Build training-only abstain candidates.**
   Candidate abstain rules should be simple and market-readable, for example:
   - compressed daily range near flat EMA spread
   - large HTF range but close trapped near midrange
   - 2h/8h disagreement around fast EMA
   - forming candle has tiny body after extended move
   - 5m cluster fires against weak or conflicting HTF context

   Prefer one or two rules with rounded thresholds. Reject a rule if it only sounds like "the split found it."

6. **Transfer-check candidate rules.**
   Before editing, test each candidate on training months and any allowed fresh holdout:
   - skipped count by month
   - skipped correct vs wrong directional rows
   - skipped `None`/low-travel rows
   - retained directional accuracy
   - LONG and SHORT retained accuracy
   - whether one side or one month is carrying the result

7. **Decide whether to patch.**
   Patch only when the rule was learned from training evidence, has a stable market interpretation, and does not rely on same-cycle walk-forward labels. If not, write a postmortem and recommend a fresh training cycle with explicit abstain labels.

## Directional Rule Workflow

For non-abstain directional improvements:

- Compare failures against protected/correct cases using broad packet features.
- Prefer regime overlays over more tree depth when the model fails in a recognizable market phase.
- EMA context can be used for regime recognition, but require timeframe agreement or a clear transition pattern:
  - daily/8h fast EMA reclaim
  - price extended far beyond slow EMA with wick rejection
  - fast/mid/slow compression before low travel
  - 2h local reversal against daily trend
- Keep directional overlays small and auditable. One robust overlay is better than several narrow branches.

## Editing Strategy.py

Only edit when the user asks for an implementation pass or a fresh training bundle authorizes it.

The edited strategy must:

- preserve the evaluator contract and existing diagnostics shape
- return deterministic decisions
- include stable `reason_code` values for new directional or abstain logic
- expose the packet evidence used in diagnostics
- use `SKIP`/`FLAT` only if the stage/evaluator semantics make that meaningful

Do not use walk-forward labels inside code or comments as rule justification.

## Verification

After any edit, run:

- Python syntax check for `strategy.py`
- training replay
- monthly stability audit
- side-balance audit for LONG and SHORT
- skip-cost audit if abstain logic exists
- protected-case check if protected cases exist
- scoped diff of strategy changes

Report both aggregate and retained metrics. For abstain logic, always report:

- rows skipped
- correct directional rows skipped
- wrong directional rows skipped
- `None`/low-travel rows skipped
- retained directional agreement
- whether skipped rows are excluded or counted by the evaluator

## Final Response

Keep the final response focused:

- whether files were edited
- baseline vs updated training metrics, if edited
- abstain/skip-cost result, if tested
- monthly and side stability
- whether walk-forward evidence was diagnostic only
- next recommended experiment

When the result is not patchable, say so directly and name the missing evidence, usually a fresh training objective for abstain/no-trade or a holdout that was not used to discover the rule.
