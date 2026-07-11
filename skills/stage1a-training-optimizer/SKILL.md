---
name: stage1a-training-optimizer
description: Use when Codex is asked to update or review a deterministic Stage 1A trade-decision strategy from training failure audits, builder_training_sample.json, signal_sample.json, packet evidence, or first-iteration strategy-builder training bundles.
---

# Stage 1A Training Optimizer

## Purpose

Optimize a Stage 1A trade-decision strategy script against the current training bundle, then stop. The user will run Score, validation, walk-forward, or locked OOS outside this skill.

Stage 1A asks one supervised signal-time question: should `decide(...)` enter `LONG`/`SHORT` or remain `FLAT`? It does not decide sizing, exits, trade management, or live execution.

## Quant Research Operating Stance

Act like a rigorous strategy researcher optimizing a deterministic signal-time decision script, not like a code assistant making local edits.

When evidence is weak, contradictory, or the first replay does not improve, keep working the research problem:

- distrust arbitrary base-strategy direction rules, especially in `iter_001`
- treat labels, packet evidence, and signal-time feature attribution as the source of truth
- actively look for market-readable patterns across price, OI, flow, trend, volatility, regime, timeframe, and event-context features
- examine residual failures, not just aggregate accuracy
- prefer simple rules only after richer factor and conjunctive-rule searches have been tried and disclosed
- reject rules that win by overfitting, leakage, date clustering, side collapse, or unstable monthly behavior
- report "no robust Stage 1A decision edge found" when the evidence does not support a durable rule

The goal is not to make the script more complex. The goal is to find the simplest deterministic rule set that is actually supported by the training evidence.

## Hard Boundaries

- Read only the artifacts named in the user's training-iteration request.
- Use training labels only from `builder_training_sample.json`.
- Do not read validation, walk-forward, locked OOS, future candles, score files from later gates, or live state.
- Do not tune to exact timestamps, signal ids, or date clusters.
- Do not discover rules by blind candidate sweeps. Derive candidate tests from pre-edit ground-truth attribution.
- Do not rely on the language model alone to eyeball a tree or split set. Use deterministic counting/scoring code for support, label split, lift, and stability; use the model to interpret and simplify.
- Do not treat an arbitrary/base strategy as directional authority, especially for `iter_001`.
- Do not add Stage 1B expected-travel filters, trade management, order routing, exchange calls, randomness, or network access.
- Do not invent `SKIP` rules unless the training labels contain a ground-truth no-trade/neutral class or the user's current request explicitly asks for supervised skip handling.
- Do not claim promotion readiness. Report only training-sample behavior and tell the user to rerun Score.
- Do not edit read-only snapshots, sample files, signal packets, audit files, or evaluator handoff files.
- Do not invent scoring semantics. Locate and use the session's actual Stage 1 normalizer, scorer, and metrics implementation whenever available.

## Required Inputs

Before editing, read:

- `failure_audit.json`
- `failure_audit.md`
- `builder_training_sample.json`
- `signal_sample.json`
- the mutable session `strategy_module/strategy.py`
- the iteration `source_artifacts/strategy_module_snapshot` as read-only evidence of what failed

If any required training artifact is missing, stop and report the blocker.

## Decision Contract And Scorer Semantics

Treat action and direction as separate fields:

- Enter: `trade_action: ENTER`, `action: ENTER`, and `direction: LONG` or `SHORT`.
- Skip: `trade_action: SKIP`, `action: SKIP`, `direction: FLAT`.
- Never put `SKIP` in `direction`. Never pair `ENTER` with `FLAT` or `SKIP` with `LONG`/`SHORT`.

Before replay, inspect the actual `StrategyDecision` validation and Stage 1 scoring code. For the current v1 scorer, reproduce its categories exactly:

- `SKIP` action or `FLAT` direction -> `NEUTRAL`, regardless of truth.
- Entered direction equal to a present truth direction -> `MATCH`; every other entered `LONG`/`SHORT` -> `MISMATCH`, including a directional preference on a naturally neutral, no-trigger, null, or missing truth.
- `scoreable = MATCH + MISMATCH`; `directional_agreement = MATCH / scoreable`, or `0` when scoreable is zero.

A `FLAT/SKIP` decision is not a correct third-class match under this scorer. It is neutral and excluded from the directional-agreement denominator. Do not map null truth to `SKIP`, count it as a match, or silently discard entered decisions on null truth.

## Baseline Replay

Before proposing rules, run the current strategy through the real Stage 1 replay/scoring path when available and report or record:

- total count, scoreable count, LONG/SHORT label balance, and naturally neutral/no-trigger/null truth count
- match, mismatch, and neutral counts
- failure counts by `reason_code`, truth direction, and decision direction
- whether failures are mostly neutrality/skip, wrong-way direction, over-entry, over-skip, or a mix

If a diagnostic replay must reimplement the scorer, reconcile every aggregate count with the real scorer before trusting candidate metrics. A label-filtered LONG/SHORT accuracy is not the system score. Do not patch before understanding the baseline.

For `iter_001`, baseline replay establishes the evaluator contract and failure surface only. It does not validate the base strategy's directional thesis.

## Neutral Labels And Skip Analysis

Natural neutral/no-trigger/null truth is part of the evaluator surface, not an automatic exclusion. An entered `LONG`/`SHORT` on such a row is a system `MISMATCH`; ignoring these rows inflates reported performance.

When neutral/no-trigger labels exist:

- record their count separately from LONG and SHORT truth balance
- use the real scorer's `MATCH`, `MISMATCH`, `NEUTRAL`, scoreable, and directional-agreement counts as the primary metrics
- discover skip conditions from signal-time evidence only when the labels support no-trade analysis or the user explicitly requests it
- report no-trigger capture, skip precision/recall, over-skip, over-entry, and strategy skip rate only as secondary diagnostics, never as three-class system accuracy
- label-only directional accuracy may appear only as a clearly named secondary diagnostic, such as `LONG/SHORT-truth-only accuracy`; never substitute it for system directional agreement
- reject skip rules that merely turn wrong-way entries into neutral rows without evidence of a stable no-trade condition
- audit skip behavior by month and side-adjacent context so one regime is not silently discarded

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
- monthly match, mismatch, neutral, and scorer-native directional agreement, including entered `LONG`/`SHORT` on neutral/no-trigger truth as mismatches
- monthly LONG agreement and SHORT agreement when enough samples exist
- monthly skip rate, no-trigger capture, skip precision, over-skip count, and over-entry count when neutral/no-trigger labels exist
- worst-month agreement
- whether the improvement is concentrated in only one or two months
- whether any month regresses sharply from the baseline
- whether any side collapses, such as LONG working while SHORT fails
- whether skip behavior collapses, such as one month or regime consuming most skips

Use monthly stability as a training-only robustness check, not as a source for timestamp rules.

Flag the strategy as unstable when:

- aggregate training agreement improves but one or more meaningful months collapse
- monthly variance is high enough that the aggregate score hides regime dependence
- the updated rules help dense months while damaging sparse months
- a month with enough scoreable signals falls materially below random decision agreement

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

- find broad differences between ground-truth `LONG`, `SHORT`, and neutral/no-trigger labels
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

## Signal Packet Factor Discovery

The agent must discover available factors from the actual signal packets before attribution or rule induction. Do not rely on a fixed list of expected fields.

Build a factor catalog by recursively inspecting scoreable rows in `builder_training_sample.json` and the representative packet in `signal_sample.json`.

Search at least:

- `signal`, `payload`, `payload.evidence`, nested evidence dictionaries, and any `features`, `diagnostics`, `metrics`, `context`, or `state` objects
- chart/timeframe containers, row arrays, candle tables, column lists, and completed-candle flags
- active timeframe lists, event metadata, market regime fields, and derived feature blocks
- numeric, boolean, categorical, enum-like, timestamp, and list/table values

For each candidate factor, record:

- stable path, such as `payload.evidence.oi_features.oi_return_8h_zscore_2d`
- source family, such as price, OI, OI value, funding, CVD, volume, volatility, trend, EMA, range, wick, divergence, event metadata, or timeframe state
- value type and examples
- availability count, missing/null rate, and type consistency
- whether it is scalar, categorical, or table-derived
- whether table-derived values use only completed/signal-time rows
- whether it is an input factor, label/target field, evaluator result, future outcome, or post-signal leakage risk

Only input factors that are available at signal time may enter attribution, predicate generation, or `strategy.py`.

If useful information is stored in table-like packet data, derive simple signal-time factors from it with deterministic code, such as latest completed return, slope sign, distance from EMA, range location, or broad quantile bucket. Record the derived factor path and derivation in the catalog.

The factor catalog is the source of truth for later attribution. If a factor is not in the catalog, do not use it in a candidate rule.

## Pre-Edit Ground Truth Attribution

Before testing candidate rule changes or editing `strategy.py`, run a training-only attribution pass against labels from `builder_training_sample.json`.

The attribution pass must summarize:

- `LONG` vs `SHORT` vs neutral/no-trigger/null truth balance and exact evaluator behavior
- the factor catalog coverage: total candidate factors, usable input factors, missing-heavy factors, and excluded leakage/target fields
- baseline matches/mismatches by `reason_code`, truth direction, decision direction, and month
- feature distributions for `LONG`, `SHORT`, and neutral/no-trigger labels using available packet evidence
- feature distributions for baseline matches vs mismatches
- independent decision agreement of broad sources when available, such as 5m, 2h, 8h, 1d, range location, volume, OI, long/short ratio, and divergence
- skip-vs-enter attribution when neutral/no-trigger labels exist
- support counts, missing rates, and broad effect sizes for any promising feature family
- side and monthly stability for each promising attribution pattern

End attribution with 1-3 ranked hypotheses that can be stated as market-readable decision ideas. Each candidate rule extracted or tested later must trace to one of these hypotheses.

Do not:

- scan many thresholds and pick the best replay result without a prior attribution hypothesis
- copy a fitted classifier, feature tree, or exact split set into `strategy.py`
- justify a rule only because it improves aggregate replay
- use baseline scaffold rules as the source of the hypothesis for `iter_001`

## Factor Family Attribution

Evaluate feature families from the discovered factor catalog before selecting rules. Do not stop at the single best column.

When available in the packet evidence, evaluate:

- price trend by timeframe
- higher-timeframe regime and completed candle returns
- OI return and OI value return
- OI, OI value, and long/short ratio trend slopes
- funding, CVD, taker flow, volume, volatility, ATR, range, wick, and location features
- EMA slope, EMA distance, tunnel/range context, active timeframe, and divergence fields

For each family, record or compute:

- feature availability and missing rate
- standalone decision agreement or label split
- support count and broad effect size
- monthly stability and side stability
- whether it duplicates a stronger feature, such as the same price trend expressed through several columns

Treat weak standalone edge as a clue, not a rule. A factor can still be useful if it adds marginal value inside a regime where another factor is weak.

## Marginal Factor Stacking

After ranking standalone factors, test whether other factor families add independent value over the strongest simple factor.

Ask:

- When the strongest factor is wrong, does another family separate the failures?
- Does a factor act as a direction signal, skip signal, regime filter, or contradiction/override?
- Does the factor improve both sides or only flip one side into collapse?
- Does the improvement survive calendar-month and side audits?
- Is the factor orthogonal, or just a correlated restatement of the same price/OI move?

Use deterministic scoring for factor combinations. Prefer shallow, interpretable stacking such as base factor plus one override or confirmation. Reject stacks that improve aggregate training agreement while materially damaging a meaningful month or side.

## No-Improvement Deep Feature Pass

If baseline replay, standalone attribution, or the first candidate ruleset shows little or no meaningful improvement, do not immediately patch a weak rule or stop at the obvious fields.

Run a deeper signal-time feature pass before concluding that no robust Stage 1A edge exists.

Use the factor catalog and packet tables to derive broad, explainable features when available:

- completed-candle trend direction, return, slope, and acceleration by timeframe
- OI, OI value, volume, funding, CVD, taker flow, and long/short-ratio returns, z-scores, ranks, and slope signs
- price/OI divergence and confirmation states across multiple timeframes
- EMA/tunnel distance, slope, compression, expansion, and reclaim/rejection states
- ATR, volatility, range location, wick/rejection, breakout/fade, and session/context states
- residual features that separate baseline wrong-way cases from baseline matches

For every derived feature, record:

- source packet path or table columns
- derivation formula in plain language
- signal-time safety and completed-row handling
- availability and missing rate
- standalone label split and marginal lift

Then rerun factor attribution, conjunctive leaf scoring, and candidate replay using the expanded catalog.

Guardrails:

- derive broad market-readable features, not many narrow fitted transforms
- do not use future rows, label fields, evaluator outcomes, or post-signal metrics
- do not keep a derived feature only because it improves aggregate replay
- reject the strategy update if deeper features still fail support, side, monthly, or interpretability checks

## Transparent Rule Induction

Heavy ML is not required. Prefer a transparent, deterministic rule-induction pass before editing.

Build a broad predicate library from the factor catalog, using only input factors available at signal time:

- signs, broad quantile/rank buckets, and coarse threshold zones
- completed-candle returns by timeframe
- categorical states such as divergence, regime, trend, or active timeframe
- simple conjunctions from attribution hypotheses, not arbitrary deep interactions

Score predicates and shallow combinations with code, not manual eyeballing. The induction pass must explicitly attempt all of the following within the available feature set:

- single-factor leaves: `A -> LONG/SHORT/FLAT-SKIP`
- two-factor leaves: `A + B -> LONG/SHORT/FLAT-SKIP`
- three-factor leaves when support remains meaningful: `A + B + C -> LONG/SHORT/FLAT-SKIP`
- conflict leaves where one factor overrides another, such as `base factor wrong + contradiction factor -> opposite direction`
- skip leaves that separate stable no-trigger/neutral conditions from enterable `LONG`/`SHORT` cases
- fallback/default decision only after extracted leaves have been tested

For each candidate leaf, compute:

- support count
- `LONG`/`SHORT`/no-trigger label split and scorer-native decision agreement
- marginal lift over the base factor or baseline decision
- fixed/broken estimate against the baseline replay
- monthly and side stability
- skip precision, skip recall, and skip-rate impact when the leaf predicts `SKIP`

Do not stop after finding the single best column. A Stage 1A iteration should produce a candidate leaf table, even if the final strategy remains simple.

Constrain rule induction:

- max predicate/tree depth 2-3; this is factors per leaf/path, not a limit on total accepted leaves
- minimum meaningful support per leaf/rule
- no exact timestamp, signal id, or date-cluster predicates
- no narrow fitted thresholds unless they are established domain constants
- no copied boosted-tree or classifier logic

If XGBoost, LightGBM, CatBoost, or another model is used, it is discovery-only. Extracted model paths must still pass simplification, stability, and market-meaning checks before becoming candidate rules.

## Ordered Ruleset Construction

Convert surviving leaves into a deterministic ordered ruleset, not an opaque classifier.

The ruleset construction pass must:

- rank accepted leaves by stable marginal lift, support, and market meaning
- identify overlapping leaves and choose either the more general stable leaf or a clear ordered precedence
- assign each accepted leaf a stable, market-readable `reason_code`
- specify the fallback/default rule separately from learned leaves
- replay the ordered ruleset as a whole, because overlapping leaves can change fixed/broken counts

Accept multiple conditional `LONG`/`SHORT`/`SKIP` leaves when each adds stable marginal value, then apply a separately defined fallback.

Do not require multiple leaves when the evidence does not support them. The required behavior is to search, score, disclose, and simplify conjunctive leaves; not to force complexity.

There is no hard three-leaf limit. Factor count describes conditions per leaf and depth describes branching complexity. The final ordered ruleset may contain more than three accepted leaves when each adds independently supported, stable, interpretable marginal value. Prefer fewer leaves and reject redundant additions.

## Rule Extraction Dossier

Before testing a candidate patch or editing `strategy.py`, produce a short rule-extraction dossier for each candidate rule.

Each rule entry must include:

- market-readable rule statement
- source factor families
- exact predicate shape being tested
- support count and missing behavior
- `LONG`/`SHORT`/no-trigger label split when neutral/no-trigger labels exist
- standalone edge and marginal lift over the base factor or baseline
- skip-rate impact and skip precision/recall when the rule predicts `SKIP`
- expected fixed and broken counts
- monthly stability and side stability
- simplified threshold form
- why the rule should generalize as market behavior
- accept/reject decision

The dossier must include accepted and rejected high-scoring leaves. Rejected entries should state the rejection reason, such as low support, side collapse, month collapse, duplicate factor exposure, weak marginal lift, brittle threshold, or unclear market meaning.

No dossier, no edit. If no extracted rule has stable marginal value, do not patch the strategy; report that no robust Stage 1 rule was found.

## Generalize And Simplify

Every extracted rule must be simplified before it can be patched.

- Round thresholds to broad zones.
- Remove conditions that do not preserve meaningful lift.
- Merge similar leaves or paths into one readable rule.
- Prefer fewer factors over a brittle high-accuracy path.
- Preserve market meaning over exact fitted performance.
- Keep a rejected-rules note for rules that improved aggregate but failed monthly, side, support, or interpretability checks.

## Candidate Replay Before Editing

Test extracted rules in a diagnostic replay before modifying `strategy.py`.

The candidate replay must compare:

- baseline strategy
- base factor or simplest candidate
- each extracted rule or stack
- the final ordered ruleset with all accepted leaves and fallback precedence

Evaluate aggregate counts, monthly stability, side stability, skip-rate budget, skip precision/recall when applicable, and fixed/broken deltas. Only patch `strategy.py` after the candidate rule survives this training-only replay.

## Rule Complexity Budget

Keep each iteration small.

- Prefer 1-3 new decision rules per iteration as a complexity prior, not a hard cap on total ordered leaves.
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
- which extracted-rule dossier entry it came from
- support count, expected fixed/broken risk, and side/month stability from attribution
- marginal lift over the base factor or baseline
- why the evidence should be a general signal-time decision read
- why it remains Stage 1A decision logic, not Stage 1B trade management

Reject candidate rules that do not have a clear market-readable enter-long, enter-short, or flat-skip interpretation.

## Editing Strategy.py

Patch only the mutable session strategy file named by the user.

The edited `decide(...)` must:

- return a deterministic StrategyDecision-compatible object or dict
- emit `ENTER` plus `LONG`/`SHORT`, or emit `SKIP` plus `FLAT`, using both `action` and `trade_action` when the local dict contract expects both
- include `confidence`
- include a stable `reason_code`
- include diagnostics explaining the packet evidence used
- preserve the existing decision contract fields used by the evaluator

Use existing local patterns in the strategy file. Add helper functions only when they reduce repeated logic or make diagnostics clearer.

## Common Scoring Mistakes

- `direction: SKIP`: invalid. Use `direction: FLAT`, `action: SKIP`, and `trade_action: SKIP`.
- Filtering evaluation to LONG/SHORT truth: wrong for the current scorer. Entered directions on neutral/no-trigger/null truth are mismatches.
- Treating FLAT/SKIP as a correct third-class match: wrong for the current scorer. It is always `NEUTRAL`.
- Reporting a custom label-filtered or three-class metric as directional agreement: wrong. Use scorer-native counts and formula.

## Training Verification

After editing, run:

- Python syntax verification and the real Stage 1 replay/scoring path on the training sample, including decision normalization
- the monthly stability audit
- a rule-delta attribution showing fixed and broken cases by old reason, old decision, new decision, truth decision, side/skip class, and month
- a diff against the read-only strategy snapshot or a scoped diff of the edited strategy file

Report:

- baseline match/mismatch/neutral counts
- updated match/mismatch/neutral counts
- neutral/no-trigger truth rate and final strategy skip rate when applicable
- pre-edit attribution hypothesis used
- whether a no-improvement deep feature pass was triggered, and which derived factors were accepted or rejected
- accepted ordered ruleset leaves and their reason codes
- top rejected higher-scoring or more complex leaves and rejection reasons
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
- deterministic rules changed, including whether deeper trend/z-score/regime discovery was needed
- accepted/rejected candidate leaves and neutral/no-trigger truth rate vs final strategy skip rate when applicable
- training replay result
- monthly stability result
- explicit note that validation/walk-forward/OOS was not used
- next action: user should rerun Score on the training iteration
