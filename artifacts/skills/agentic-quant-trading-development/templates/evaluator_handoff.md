# Evaluator Handoff

Session:
Iteration:
Stage:
Asset:
Strategy skill snapshot:
Strategy version:
Signal engine id:
Signal family (legacy, optional):
Signal set:
Sample size:

## Task

Evaluate the sampled neutral signal packets using only the strategy skill snapshot for this
iteration.

You are the Backtester role: your job is unbiased evaluation of the assigned sample only,
not strategy improvement, sampling, scoring, or execution.

## Process Protocol

Process signals sequentially, one at a time. Read a single packet, evaluate it fully
against the strategy skill snapshot, record the decision, then proceed to the next packet.
Do not load all packets at once. This mirrors live execution, where signals arrive
individually.

Use `signal_sample.json` as the checklist. Preserve its order exactly. For each listed
path, open and read the full signal packet and apply the strategy skill snapshot as if the
packet had arrived live. Do not use scratch notes, abbreviated packet summaries, or
shortcut evaluations. Keep only the final decision object for each packet, then assemble
the final JSON after all listed packets are complete.

Forbidden shortcut rule: do not use scripts, formulas, batch heuristics, neighboring
signals, filenames, timestamps, prior scores, or any other estimate to approximate a
decision. If a packet has not been opened, read in full, and evaluated directly against the
strategy snapshot, no decision may be recorded for it.

## Inputs

- Strategy skill path:
- Signal sample path:
- Output path:

## Sample Boundary

- Evaluate only the packet paths listed in `signal_sample.json`.
- Preserve the listed order exactly.
- Do not scan the signal directory for more packets.
- Do not replace or expand the sample.

## Contamination Rules

- Do not use ground truth.
- Do not use future candles.
- Do not use prior iteration scores, failures, audits, or proposed fixes.
- Do not use execution setup unless the requested stage is execution setup.

## Output Contract

Return JSON only. Preserve signal order exactly.

For Stage 1B, output `trade_action: ENTER` only when the packet passes the strategy skill's
entry gate. Output `trade_action: SKIP` when the gate fails, even if a directional bias
exists. `trade_action` is the live entry decision.

Stage 1B decision shape:

```json
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
```
