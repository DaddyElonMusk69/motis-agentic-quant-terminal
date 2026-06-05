# Agent Evaluation Prompts

Use these prompt contracts when a model evaluates signal packets during optimization.

## General Rules

- The evaluator is the backtester role. It exists only to provide unbiased performance
  evidence for the current strategy skill on the assigned signal sample.
- Give the agent only pre-decision signal packets and the iteration strategy skill snapshot.
- Do not leak ground truth, future candles, scores, or proposed fixes.
- Require JSON-only outputs for batch runs.
- Preserve input order.
- Validate every chunk before merging.
- Rerun failed chunks instead of hand-editing model decisions.

## Builder Handoff Artifact

Before invoking an evaluator agent, write:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/handoff.md
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/signal_sample.json
```

`handoff.md` states the strategy skill path/version, stage, signal set, sample size,
output requirements, and contamination rules. It must not include future outcomes,
ground truth, previous failures, proposed fixes, or score summaries.

`handoff.md` must also tell the evaluator to process packets sequentially, one at a time,
using `signal_sample.json` as the checklist. The evaluator should not load all packets at
once. It must read the full signal packet for each listed path and apply the strategy
snapshot as if the packet had arrived live. Do not permit scratch notes, abbreviated packet
summaries, or shortcut evaluations. The evaluator may keep only final decision objects
needed to assemble the required JSON after all listed packets are complete.

The handoff must include a forbidden shortcut rule: evaluators may not use scripts,
formulas, batch heuristics, neighboring signals, filenames, timestamps, prior scores, or
any other estimate to approximate packet decisions. A decision is valid only after the
specific packet has been opened, read in full, and evaluated directly against the strategy
snapshot.

`signal_sample.json` records the exact ordered packet paths and sample method so the
run is reproducible.

## Sampling Responsibility

The builder chooses the sample. The evaluator does not choose, expand, replace, or reorder
signals.

The sample may be in-sample, out-of-sample, protected wins, failed-case retest, recent-live,
or another explicitly named role. The role belongs in `sample_method`.

The evaluator receives only:

- `handoff.md`
- `signal_sample.json`
- sampled signal packets listed in `signal_sample.json`
- the iteration strategy skill snapshot under `source_artifacts/strategy_skill_snapshot/`

Do not include builder-only sampling rationale if it reveals ground truth, future movement,
prior failures, prior wins, proposed fixes, or score files.

## Stage 1A Directional Agreement

```text
You are evaluating neutral trading signal packets using this strategy skill snapshot:
{strategy_skill_path}

Task:
For each signal, choose the trade direction that the strategy skill supports: LONG or SHORT.

Inputs:
- signal packet paths in this exact order:
{signal_packet_paths}

Rules:
- Use only the signal packet evidence and the strategy skill.
- Evaluate only the listed packet paths.
- Preserve the listed order exactly.
- Do not scan the signal folder for additional packets.
- Do not use future outcomes.
- Do not optimize TP, SL, size, or hold time.
- Do not skip unless the strategy skill truly cannot choose a side.

Output only JSON:
{
  "stage": "stage1a_directional_agreement",
  "strategy_skill": "{strategy_id}",
  "strategy_version": "{strategy_version}",
  "decisions": [
    {
      "signal_id": "...",
      "direction": "LONG",
      "confidence": 0.75,
      "reasoning": "Concise packet-grounded reason"
    }
  ]
}
```

## Stage 1B Opportunity Screening

```text
You are evaluating neutral trading signal packets using this strategy skill snapshot:
{strategy_skill_path}

The calibrated travel threshold is {threshold_pct}% within {forward_hours} hours.

Task:
For each signal, decide whether the strategy skill permits a live entry for that signal.
Choose the best direction either way, but make `trade_action` the operative decision.

Inputs:
- signal packet paths in this exact order:
{signal_packet_paths}

Rules:
- Use only the signal packet evidence and the strategy skill.
- Evaluate only the listed packet paths.
- Preserve the listed order exactly.
- Do not scan the signal folder for additional packets.
- Do not use future outcomes.
- Reason in two layers: choose direction first, then apply the entry gate. Directional
  support alone is not evidence that the entry gate passes.
- trade_action = ENTER means the signal passes the entry gate and should be treated as a
  tradable opportunity.
- trade_action = SKIP means the signal fails the entry gate. Do not place a live entry
  order for that signal, even if a directional bias exists.
- expected_travel remains supporting metadata only. It must agree with the action:
  ENTER uses expected_travel = high, SKIP uses expected_travel = low.

Output only JSON:
{
  "stage": "stage1b_opportunity_screening",
  "strategy_skill": "{strategy_id}",
  "strategy_version": "{strategy_version}",
  "decisions": [
    {
      "signal_id": "...",
      "trade_action": "ENTER",
      "direction": "LONG",
      "confidence": 0.72,
      "expected_travel": "high",
      "entry_gate": "pass",
      "gate_reason_code": "accepted_reclaim_with_room",
      "reasoning": "Concise packet-grounded reason"
    }
  ]
}
```

## Chunk Validation

For each sub-agent chunk:

- JSON parses
- `stage` matches requested stage
- `decisions` length equals input signal count
- signal ids match input order
- required fields exist
- no extra commentary outside JSON

Merged decisions go under:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/decisions/
```
