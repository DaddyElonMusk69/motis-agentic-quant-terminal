# Failure Audit And Skill Updates

Strategy skill changes must be evidence-backed.

Use this reference when a model result suggests the active strategy skill should change.

## Required Audit

Write audits under:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/
```

Each audit should include:

- strategy skill version
- signal set
- stage
- failure cluster
- representative signal ids
- neutral packet evidence
- proposed skill change
- expected benefit
- regression risk
- retest plan

Also include:

- iteration id
- sample method and sampled signal ids
- protected cases that should not regress
- next strategy skill version if an update is proposed

Audit files should be explicit enough that another agent can understand why the strategy
changed without reading the whole conversation.

## Update Rules

- Follow `strategy-skill-contract.md` for file responsibilities.
- Update strategy skills, not signal engine logic, when judgment improves.
- Make the smallest possible skill change that addresses a repeated directional failure.
- Preserve conditions that were already working.
- Prefer directional reclassification over broader avoidance language.
- Prefer proof-of-control wording over action-forcing guard wording.
- Do not encode exact training timestamps as strategy rules.
- Retest failed, protected, and out-of-sample sets before promotion.
- Keep Stage 1A and Stage 1B fixes separate. Stage 1A edits change directional
  classification only. Stage 1B edits change entry-gate permission only, after direction is
  chosen.
- If a Stage 1B false positive has plausible direction but weak tradability, fix the entry
  gate. Do not weaken or reverse the directional rule just to avoid entering.
- For sparse-pool/path-B work, keep Stage 1B as the active priority until it passes. Use a
  default-enter gate that names only the specific skip conditions. Do not begin by writing a
  proof-heavy positive checklist.

## Failure Ledger

Before editing a skill, build a ledger with:

- all current failures
- prior wins that might regress
- current wins that must be protected
- no-trigger or low-travel cases
- representative packet features

Avoid diagnosing only from a single memorable miss.

## Feature Audit Trigger

If the first scored Stage 1A iteration on the active sample finishes below threshold, run a
neutral feature audit before writing the next skill update. This prevents the agent from
inventing broad wording from anecdotes once a failing score exists.

Use:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/analysis/signal_feature_audit.py \
  --signal-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --ground-truth-dir dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth \
  --stage1-score dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage1a_directional_scores.json \
  --primary-tf 1d \
  --anchor-tf 2h \
  --out-csv dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/signal_feature_audit.csv \
  --out-json dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/signal_feature_audit_summary.json
```

Use the audit to find repeated neutral packet features behind mismatches, such as primary
trend versus anchor conflict, mixed anchor structure, stretched range position, or forming
candle rejection. The audit is evidence for skill wording; it is not a trade-decision
model and must not be copied into the signal engine, signal packet, or evaluator handoff.

If the first scored Stage 1B iteration on the active sample finishes below its precision or
recall threshold, run the Stage 1B entry-classifier audit before writing the next skill
update.

Use:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/analysis/stage1b_entry_classifier_audit.py \
  --packet-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --ground-truth-dir dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth \
  --score dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage1b_screening_scores.json \
  --primary-tf 1d \
  --anchor-tf 2h \
  --out-csv dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/stage1b_entry_classifier_audit.csv \
  --out-json dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/stage1b_entry_classifier_audit_summary.json \
  --out-md dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/audits/stage1b_entry_classifier_audit.md
```

Use the Stage 1B audit to separate true entry blockers from merely plausible directional
stories. The next Stage 1B rewrite should cite the repeated blocker patterns that caused
false positives, false negatives, or overbroad skip behavior.

## Good Skill Update Shape

A good update says:

- what recurring condition the model misread
- what evidence should change the read
- what evidence preserves the old read
- where the rule applies
- where it does not apply
- whether the change belongs to the direction layer or the entry-gate layer

Avoid wording that tells the model to skip broadly just to avoid being wrong unless the
current stage is explicitly opportunity screening.

## Promotion

A skill update is not promoted until:

- failed set improves
- protected set does not degrade materially
- out-of-sample or recent real packet set is checked
- execution setup remains consistent with the updated behavior
