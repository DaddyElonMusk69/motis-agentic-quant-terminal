# Optimization Stages

This is the canonical development workflow for the new scaffold:

```text
dev/data -> dev/signals -> dev/training_sessions/stage0 -> dev/walk_forward -> dev/training_sessions -> artifacts/skills/strategies -> live/
```

Do not mix signal generation, model judgment, skill updates, and execution setup into one
undifferentiated backtest. Each stage has its own inputs, outputs, scoring, and promotion
gate.

## Adaptive Stage Map

| Stage | Focus | Core question | Agent judgment |
| --- | --- | --- | --- |
| 0a | Blind travel distribution | What can the raw signal pool deliver? | No |
| 0b | Threshold calibration | What travel threshold gives stable direction? | No |
| 0c | Ground truth | What is each signal's natural direction? | No |
| Monthly universe | Strategy-engine tradability | Which strategy-engine candidates are Path A this month? | No |
| 1A | Directional agreement | Does the skill agree with natural direction? | Yes |
| 1B | Opportunity screening | Can the skill filter no-travel signals? | Yes |
| 2 | Travel capture | How much travel do correct calls capture? | Mostly no |
| 3 | Conditional execution setup | Which TP/SL/management setup extracts value when direction is correct? | No |
| 4 | Realized expectancy | Does the chosen execution setup still make money on the full frozen decision set? | No |
| 5 | Live readiness | Is the strategy ready for demo/live operation? | Yes |

## Core Philosophy

Each signal has a mechanical natural direction: the side that makes the first meaningful
move after the signal fires. If price drops 3% first and rallies 5% later, the natural
direction is `SHORT`; trading that signal `LONG` requires surviving the adverse move first.

The skill is optimized against this ground truth:

- Path A signal pools test direction directly and can proceed as live-tradable strategy-engine candidates
- Path B signal pools are research/watchlist by default rather than the normal live route
- execution setup is first explored conditionally after the judgment layer has a measurable edge
- promotion requires a full-decision realized-expectancy pass after conditional execution design

## Stage Lifecycle

Stage 0 is deterministic and shared at strategy/signal-set cycle level. Run Stage 0 once
for a given data manifest, frozen signal set, scoreable cycle horizon, forward window, and
threshold-calibration method, then treat its outputs as the ground-truth source for later
scoring.

Do not create a new Stage 0 artifact for every session or strategy-skill iteration. Re-run
Stage 0 only when one of its inputs changes:

- the candle data changes
- the signal set changes
- the scoreable cycle horizon changes
- the forward window changes
- the threshold-calibration method changes
- the ground-truth/scoring algorithm changes

Stage 1 onward is iterative. Each agentic evaluation pass creates a new folder under a
session's `iterations/`, using the same shared Stage 0 ground truth unless one of the inputs
above changes.

Within one retraining cycle, Stage 0 should not be recomputed just because the agent moves
from training to validation or from validation to locked OOS. Those are sample slices from
the same frozen Stage 0 record set.

## Required Resources

Before any stage, identify:

- data manifest: `dev/data/manifests/<ASSET>.json`
- signal set manifest: `dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/manifest.json`
- signal packets: `dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets/`
- strategy skill: `artifacts/skills/strategies/<STRATEGY_ID>/`
- stage0 path: `dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/`
- monthly universe path: `dev/walk_forward/<YYYY-MM>/`
- session path: `dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/`
- iteration path: `dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/`
- forward candles: `dev/data/raw/<ASSET>/5m/candles.csv`

Create sessions with `scripts/new_training_session.py` before producing outputs.
Create each agentic evaluation pass with `scripts/new_iteration.py`.

For normal monthly cycle builds, use `scripts/run_monthly_stage0.py` instead of manually
assembling per-asset Stage 0 commands. The monthly runner is the deterministic
orchestration layer for Stage 0 and the walk-forward universe; it calls the small scoring
step tools internally. Use individual Stage 0 tools directly only for diagnostics, repairs,
or explicitly non-monthly experiments.

## Walk-Forward Calendar Rule

Use calendar windows that a future agent can reconstruct from the session `as_of_date`.
For crypto `2h` signals with a roughly `36h` forward window, default to:

- training: the two complete calendar months immediately before the validation month
- forward validation: the validation month, excluding the locked newest slice
- locked OOS/live-readiness: the newest untouched slice of the validation month, normally
  the final 7 calendar days or latest 40-100 triggered signals, with the chosen rule
  recorded in `sampling_notes`

The validation month is the latest month with enough completed candles and forward outcomes
to score. At the beginning of June 2026, this means train on March-April 2026, validate on
May 2026 excluding the locked newest May slice, and reserve the newest May slice for OOS.
When June later has enough scoreable signals, roll forward to train on April-May, validate
on June excluding the newest slice, and reserve the newest June slice for OOS.

For the June 2026 cycle, the Stage 0 horizon should therefore span the full frozen
March-April-May scoring pool needed by that cycle, provided enough forward candles exist to
score the newest included packet. Stage 1 then slices that one shared Stage 0 by time:

- March-April packets for optimization/training iterations
- May packets excluding the locked newest slice for forward validation
- the locked newest May slice for untouched OOS/live-readiness

If there is not enough data for the default split, shorten the windows but keep chronological
order and write the exception in the session manifest.

## Walk-Forward Signal-Set Fitting

Distinguish sample selection from signal-set fitting and from Stage 0 horizon selection:

- Same signal set, different chronological sample: do not recompute Stage 0.
- Same frozen signal set, same frozen cycle horizon, different stage sample: do not
  recompute Stage 0.
- Different fitted signal filter, vote threshold, asset shortlist, dedupe rule, or signal
  generation threshold: this is a different signal set and requires its own Stage 0.

Any asset selection or signal-filter threshold chosen because of historical travel,
trigger rate, Path A/B status, or downstream score is fitted. Fit it only on the training
window. After the asset/filter/threshold is chosen, freeze it and apply the same settings
forward to validation and locked OOS. Do not re-fit on validation or OOS to make each window
look like a rich pool.

For every fitted signal set, run Stage 0 once on the resulting frozen cycle-horizon packets
and record the exact filter settings, cycle horizon, asset universe, and signal-set id.
After that cycle-scoped Stage 0 pass, build the monthly universe and let it decide which
strategy-engine candidates are Path A tradable candidates for the cycle. Forward validation
and OOS then test whether that fitted choice survives unseen regime data without recomputing
Stage 0 for those later samples.

Monthly candidate configuration is explicit and engine-qualified:

```json
{
  "candidates": [
    {
      "asset": "ZEC",
      "strategy_id": "zec-vegas-tunnel-v00",
      "signal_engine_id": "vegas_ema",
      "vote_threshold": 2,
      "window_minutes": 120,
      "forward_hours": 36
    },
    {
      "asset": "ZEC",
      "strategy_id": "zec-bollinger-band-v01",
      "signal_engine_id": "bollinger",
      "vote_threshold": 2,
      "window_minutes": 120,
      "forward_hours": 36
    }
  ]
}
```

The same asset may appear more than once because universe identity is `strategy_id`, not
bare asset. Engine-specific replay arguments can be placed under `scanner_args`; the runner
passes them to the registry-selected replay generator.

## Agentic Sampling Protocol

Stage 1 and later agentic evaluations should use selected subsets when the full signal pool
is too expensive to evaluate. Sampling must be deterministic, regime-aware, and latest-data
first. Do not pick a casual "representative" batch from the whole historical pool.

The builder chooses the sample before creating the iteration, then records the exact ordered
packet paths in:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/signal_sample.json
```

The evaluator agent never scans the whole signal folder, never adds packets, and never
chooses its own sample. It evaluates only the ordered packet paths in `signal_sample.json`.

Builder sampling rules:

- Treat market regime as the first sampling dimension. A strategy is being prepared for live
  trading, so recent behavior is more important than old historical elegance. Do not optimize
  on old data merely because it makes the strategy look cleaner.
- Default to a rolling recent-regime split:
  - optimization: the training months from the Walk-Forward Calendar Rule
  - forward validation: the validation month excluding the locked newest slice
  - locked OOS/live-readiness: the newest untouched validation-month slice
- These are Stage 1+ sampling slices from one shared Stage 0 cycle record set, not separate
  Stage 0 builds.
- For early Stage 1A optimization iterations, use 25-40 signals when available, sampled
  deterministically from the optimization window.
- If the signal pool is smaller than the default sample size, using all packets is valid.
- When the optimization window spans multiple months or distinct regimes, allocate samples
  across chronological buckets first, then side/label balance for Stage 1A diagnostics when
  hidden ground truth is available. Use a fixed seed for any random tie-breaks and record it
  in `sampling_notes`.
- For out-of-sample checks, choose packets from the forward validation or locked newest
  window that were not used in skill-editing iterations. Keep locked OOS untouched until a
  candidate strategy update is ready.
- For protected-set checks, reuse known working packets that should not regress.
- For failed-set checks, reuse prior mismatches to verify that a surgical skill update fixed
  the intended behavior.
- For recent-real or live-shadow checks, choose recent packets without using later outcomes
  in the evaluator prompt.
- Old data outside the recent regime can be used for stress testing and failure discovery,
  but it is not the primary optimizer unless the stated live target is that older regime.
- Record the role in `sample_method`, such as `recent_regime_train`,
  `forward_validation`, `locked_recent_oos`, `protected_wins`, `failed_cases_retest`, or
  `recent_live_shadow`.
- Keep sample identity stable for comparisons. If the sample changes, explain why in the
  iteration summary or audit.

Builder may use ground truth and prior scores to choose samples, but those facts must not
be included in `handoff.md`, evaluator prompts, or signal packets given to the evaluator.
The goal is not to discover a universal market rule across all history. The goal is to
develop a strategy that survives the current tradable regime, then prove it forward on data
that did not drive the wording.

## Stage 0a: Blind Travel Distribution

Purpose: measure raw signal movement without direction labels.

Run every signal through a forward walk and record absolute max travel. This answers whether
the signal engine produces enough movement to justify model training.

Output location:

```text
dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/travel_distribution.json
```

Record percentiles such as P10, P25, P50, P75, P90, P95. No model judgment is used.

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/max_travel_distribution.py \
  dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --forward-hours 36 \
  --asset <ASSET> \
  --vote-threshold <VOTE_THRESHOLD> \
  --out dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/travel_distribution.json
```

## Stage 0b: Threshold Calibration

Purpose: find the significance threshold where directional assignment stops being noise.

Scan thresholds, commonly `0.2%` to `2.0%` in `0.1%` steps. Measure:

| Diagnostic | Meaning | Preferred behavior |
| --- | --- | --- |
| Direction split | LONG/SHORT balance at threshold | avoid exact coin-flip unless expected |
| Reversal rate | hit one side then reverse past threshold | usually under 15% |
| Travel adequacy | P25 first move at threshold | usually at least 1% |

Pick the midpoint of the stable range. Record the chosen threshold and why.

Output:

```text
scores/threshold_calibration.json
summaries/threshold_calibration.md
```

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/significance_threshold_calibration.py \
  dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --forward-hours 36 \
  --asset <ASSET> \
  --vote-threshold <VOTE_THRESHOLD> \
  --out dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/threshold_calibration.json
```

## Stage 0c: Ground Truth

Purpose: compute per-signal natural direction and first significant move.

For each signal, output:

```json
{
  "signal_id": "20260301T072000Z",
  "reference_price": 66976,
  "significance_threshold_pct": 0.9,
  "calibration_method": "sensitivity_scan",
  "natural_direction": "LONG",
  "first_move_pct": 4.24,
  "max_travel_pct": 4.24,
  "opposite_max_pct": 0.82,
  "first_move_hours": 31.8,
  "reversed": false,
  "status": "triggered"
}
```

Field rules:

- `natural_direction`: first side to hit the calibrated threshold
- `first_move_pct`: travel in natural direction before opposite threshold reversal
- `max_travel_pct`: peak travel in natural direction over the forward window
- `opposite_max_pct`: peak opposite-side move
- `status`: `triggered`, `no_trigger`, `no_candles`, or `invalid`

Outputs:

```text
scores/ground_truth/
scores/ground_truth_summary.json
summaries/stage0_ground_truth.md
```

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/signal_ground_truth.py \
  dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --forward-hours 36 \
  --significance-threshold <THRESHOLD_PCT> \
  --asset <ASSET> \
  --vote-threshold <VOTE_THRESHOLD> \
  --out dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth
```

## Monthly Walk-Forward Universe

Compute:

```text
trigger_rate = triggered_signals / total_valid_signals
```

Branch decisions are made once per walk-forward month after Stage 0 has been run on the
current training-window signal sets. Build the monthly universe under:

```text
dev/walk_forward/<YYYY-MM>/
  manifest.json
  stage0_branch_decisions.json
  tradable_universe.json
  watchlist_universe.json
  summaries/monthly_universe.md
```

Monthly policy:

- `trigger_rate >= 80%`: Path A. Add the strategy-engine row to `tradable_universe.json`.
- `trigger_rate < 80%`: Path B. Add the strategy-engine row to `watchlist_universe.json`.
- Path B is research-only by default. Do not treat it as the normal live-trading route.
- The monthly universe is the source of truth for agents. Execution resolves the strategy
  skill by `strategy_id` and scanner/signal roots by `signal_engine_id`.
- The same asset may appear more than once under different `strategy_id`s.
- Enforce exactly one row per `strategy_id` per month. If duplicate candidates exist for
  one `strategy_id`, select the row tied to the latest usable training session timestamp,
  then latest available session timestamp, then Stage 0 manifest `created_at`.

Each universe record must include strategy freshness metadata:

- `strategy_id`
- `signal_engine_id`
- `strategy_training_status`: `retrained_for_month`, `stale`, or `missing_strategy`
- `strategy_version`
- `latest_training_session`
- `latest_training_date`
- `notes`

Use strict monthly freshness:

- `retrained_for_month` requires a current-month session for the same `signal_set_id` plus
  a promotion-grade Stage 4 expectancy artifact at `promotion/final_report.md`
- current-month Stage 1, Stage 2, or Stage 3 work without a completed monthly Stage 4 pass remains
  `stale`
- `missing_strategy` means no strategy skill exists

For normal monthly builds, run the deterministic orchestrator:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/run_monthly_stage0.py \
  . \
  --walk-forward-month <YYYY-MM> \
  --as-of-date <YYYY-MM-DD> \
  --candidate-config <CANDIDATES_JSON> \
  --out-dir dev/walk_forward/<YYYY-MM>
```

For June 2026 with the default `36h` forward window under the "no June candles" policy, the
runner still requires signal packets from `2026-03-01T00:00:00Z` through
`2026-06-01T00:00:00Z`, but Stage 0 scoring uses only the scoreable subset whose full
forward window ends inside May. That means:

- latest allowed outcome candle: `2026-05-31T23:55:00Z`
- latest scoreable signal timestamp: `2026-05-30T11:55:00Z`

If candle coverage cannot reach the last May `5m` candle, the runner fails before writing
Stage 0. It does not require June candle truth just to score the newest May packets.

Use dry-run before a large monthly rebuild:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/run_monthly_stage0.py \
  . \
  --walk-forward-month <YYYY-MM> \
  --as-of-date <YYYY-MM-DD> \
  --candidate-config <CANDIDATES_JSON> \
  --out-dir dev/walk_forward/<YYYY-MM> \
  --dry-run
```

Audit existing Stage 0 manifests against the monthly horizon before continuing training:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/run_monthly_stage0.py \
  . \
  --walk-forward-month <YYYY-MM> \
  --as-of-date <YYYY-MM-DD> \
  --validate-only \
  --stage0-manifest <STAGE0_MANIFEST_PATH>
```

Manual fallback: after Stage 0 scores are written, build the required Stage 0 manifest:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/build_stage0_manifest.py \
  . \
  --asset <ASSET> \
  --strategy-id <STRATEGY_ID> \
  --signal-engine-id <SIGNAL_ENGINE_ID> \
  --signal-family <LEGACY_SIGNAL_FAMILY> \
  --signal-set-id <SIGNAL_SET_ID> \
  --forward-hours 36 \
  --threshold-pct <THRESHOLD_PCT>
```

Manual fallback: after all candidate Stage 0 manifests for the month are available, build
and validate the monthly universe:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/build_walk_forward_universe.py \
  . \
  --walk-forward-month <YYYY-MM> \
  --as-of-date <YYYY-MM-DD> \
  --stage0-manifest <STAGE0_MANIFEST_PATH> \
  --stage0-manifest <STAGE0_MANIFEST_PATH> \
  --out-dir dev/walk_forward/<YYYY-MM>
```

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/validate_walk_forward_universe.py \
  dev/walk_forward/<YYYY-MM>
```

Create Stage 1+ training sessions only after this monthly record exists. Sessions for Path
A assets should link back to `dev/walk_forward/<YYYY-MM>/manifest.json`. Path B sessions
are allowed for research exceptions, but they should not be promoted as normal live-trading
candidates merely because Stage 1B can be attempted.

## Stage 1A: Directional Agreement

Purpose: test whether the strategy skill chooses the same side as natural direction.

Use this path directly for Path A assets from the monthly universe.

Stage 1A is direction-only. Strategy rules used for Stage 1A may choose `LONG` or `SHORT`
and explain the directional evidence, but they must not decide whether the signal is
tradable. Do not add `ENTER`, `SKIP`, expected-travel, or entry-gate language to a Stage 1A
fix unless a separate Stage 1B score and audit justify it.

After a strategy version passes the locked OOS Stage 1A check for the active cycle, freeze
that version and run one canonical final Stage 1A scoring pass across the whole frozen
cycle before Stage 2, Stage 3, or Stage 4. For the June 2026 cycle, that means one final directional
readout across:

- March-April training
- May forward validation excluding the locked newest slice
- the locked newest May slice

This final pass is a reporting pass, not a new optimization loop. Do not keep rewriting the
strategy between the accepted locked OOS pass and this final full-cycle score unless Stage 1
is intentionally reopened.

Agent output per signal:

```json
{
  "signal_id": "20260301T072000Z",
  "direction": "LONG",
  "confidence": 0.75,
  "reasoning": "Concise explanation referencing packet evidence"
}
```

Scoring:

| Outcome | Meaning |
| --- | --- |
| `MATCH` | skill direction equals natural direction |
| `MISMATCH` | skill direction differs from natural direction |
| `NEUTRAL` | no scoreable direction was provided |

Primary metric:

```text
directional_agreement = MATCH / (MATCH + MISMATCH)
```

Default promotion threshold: `>= 55%`, adjusted only with written rationale.

Policy:

- If the first scored Stage 1A iteration on the active sample finishes below threshold, run
  `scripts/analysis/signal_feature_audit.py` before writing the next strategy update.
- Do not continue Stage 1A rewrites from narrative impressions once a failing score exists;
  the next edit must cite the feature audit plus the failure ledger.

Outputs:

```text
iterations/<ITERATION_ID>/decisions/stage1a_directional_decisions.json
iterations/<ITERATION_ID>/scores/stage1a_directional_scores.json
iterations/<ITERATION_ID>/summaries/iteration_summary.md
```

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage1a_directional_score.py \
  --decisions dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/decisions/stage1a_directional_decisions.json \
  --ground-truth-dir dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth \
  --out dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage1a_directional_scores.json \
  --summary-out dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/summaries/iteration_summary.md \
  --promotion-threshold-pct 55
```

## Stage 1B: Opportunity / Entry Screening

Purpose: for an explicit Path B research exception, teach the skill to screen sparse-pool
signals. Stage 1B is not soft travel commentary. It decides whether a sparse-pool signal is
worth continued research; Path B remains outside the default live-trading route.

Stage 1B must be a second layer after direction selection:

1. Choose the best available direction from the Stage 1A/directional rules.
2. Apply the entry gate to that chosen direction using entry-specific packet evidence.

Do not collapse those steps. A plausible direction, strong macro bias, support/resistance
alignment, or high directional confidence is not enough to pass Stage 1B. `trade_action`
must come from entry-gate evidence such as visible room, accepted reclaim/rejection,
non-chase timing, or support/resistance acceptance.

For a Path B research exception, work Stage 1B first. The default skill shape should lean
`ENTER` and then name only the specific `SKIP` disqualifiers. In other words, use a
default-enter gate with explicit blockers, not a broad positive checklist that requires the
model to prove a perfect setup before every trade.

`trade_action` is the source of truth:

- `ENTER`: the signal passes the strategy skill's entry gate and may proceed to
  directional/execution evaluation.
- `SKIP`: the signal fails the entry gate. No live entry order should be placed for that
  signal, even if a directional bias exists.

`expected_travel` remains supporting metadata for continuity and migration. It should agree
with the gate decision: `ENTER` uses `high`, `SKIP` uses `low`.

Agent output per signal:

```json
{
  "signal_id": "20260301T072000Z",
  "trade_action": "ENTER",
  "direction": "LONG",
  "confidence": 0.72,
  "expected_travel": "high",
  "entry_gate": "pass",
  "gate_reason_code": "accepted_reclaim_with_room",
  "reasoning": "Concise packet-grounded reason"
}
```

Score `trade_action` against ground truth:

| Outcome | Meaning |
| --- | --- |
| `TP` | `ENTER` and signal triggered |
| `FP` | `ENTER` and signal did not trigger |
| `TN` | `SKIP` and signal did not trigger |
| `FN` | `SKIP` and signal triggered |

Default promotion thresholds:

- precision `>= 70%`
- recall `>= 50%`

Policy:

- If the first scored Stage 1B iteration on the active sample finishes below either
  threshold, run `scripts/analysis/stage1b_entry_classifier_audit.py` before writing the
  next strategy update.
- Do not continue Stage 1B entry-gate rewrites from anecdotes once a failing score exists;
  the next edit must cite the Stage 1B audit plus the failure ledger.

Directional match is recorded only as a secondary diagnostic for entered triggered signals.
It is not the Stage 1B gate.

After Stage 1B in a research exception, run Stage 1A only on the `ENTER` subset. `SKIP`
decisions are excluded from directional promotion work because they represent no-trade
decisions.

Do not start Stage 1A tuning for a sparse-pool/path-B research exception until Stage 1B has
passed on its active sample. If Stage 1B still fails, continue working the entry gate rather
than rewriting the direction layer to suppress entries.

Outputs:

```text
iterations/<ITERATION_ID>/decisions/stage1b_screening_decisions.json
iterations/<ITERATION_ID>/scores/stage1b_screening_scores.json
iterations/<ITERATION_ID>/summaries/iteration_summary.md
```

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage1b_screening_eval.py \
  dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth \
  dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/source_artifacts/strategy_skill_snapshot/SKILL.md \
  dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/decisions/stage1b \
  --sample-size 30 \
  --threshold-pct <THRESHOLD_PCT> \
  --forward-hours 36
```

## Stage 2: Travel Capture Curve

Purpose: measure how much of the available move the correct calls can capture.

Use signals where the skill matched natural direction from the canonical final Stage 1A
full-cycle scoring pass. Do not build Stage 2 from an earlier training-only, validation-
only, or partial retest run if a later final Stage 1A version has already been accepted.

Compute TP hit rates over a range such as `0.5%`, `1.0%`, `1.5%`, `2.0%`, `2.5%`, and
`3.0%`.

Run Stage 2 across the full frozen cycle owned by that final Stage 1A readout, but keep the
cycle slices visible in the evidence. At minimum, the strategist should preserve separate
readouts for:

- training matches
- forward-validation matches
- locked-OOS matches

You may also report a pooled full-cycle summary, but do not replace the slice split with a
single blended number when judging robustness.

Example output:

```text
TP=0.5% -> 92%
TP=1.0% -> 78%
TP=1.5% -> 61%
TP=2.0% -> 43%
TP=2.5% -> 28%
```

Stage 2 does not choose the final execution setup. It narrows the Stage 3 grid range. Stage
3 should inherit the same canonical final Stage 1A match universe rather than switching to a
different directional source. Stage 4 then scores shortlisted Stage 3 candidates back on
the full frozen Stage 1A decision set.

Outputs:

```text
iterations/<ITERATION_ID>/scores/stage2_capture_curve.json
iterations/<ITERATION_ID>/summaries/iteration_summary.md
```

Tool:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage2_capture_curve.py \
  <MATCH_RESULT_DIR_OR_FILE> \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --signal-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --gt-dir dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/scores/ground_truth \
  --out iterations/<ITERATION_ID>/scores/stage2_capture_curve.json
```

## Stage 3: Conditional Execution Setup

Purpose: optimize TP, SL, hold time, leverage, margin, pyramiding, and management rules on
the signals where the judgment layer read direction correctly.

Use the canonical final Stage 1A full-cycle pass as the judgment source of truth. Stage 3
must not reach back to an earlier partial-slice directional run once a later final Stage 1A
version has been accepted for the cycle.

The canonical Stage 3 optimization set is the `MATCH` subset from that frozen final Stage
1A pass. Stage 3 is therefore conditional execution research. Its question is:

- if the strategy reads direction correctly, what execution structure harvests travel best?

Do not treat Stage 3 alone as proof of live profitability. A strong Stage 3 winner may
still fail after directional mismatches, no-hit cases, side imbalance, fees, or slippage
are restored in Stage 4.

Simulate with 5m candles from signal time. For each setup, record:

- TP hits
- SL hits
- no-hit count
- win rate
- expectancy in R
- profit factor
- max drawdown if compounding is tested
- average and tail hold time
- side split, if LONG/SHORT differ materially

Within-candle TP/SL tiebreakers must be deterministic and documented. The old Vegas
pipeline used candle body direction as a tiebreaker.

Outputs:

```text
iterations/<ITERATION_ID>/scores/stage3_grid_results.json
iterations/<ITERATION_ID>/scores/stage3_pyramid_results.json
iterations/<ITERATION_ID>/summaries/iteration_summary.md
```

Optional but recommended:

- emit a small full-decision preview for shortlisted setups if it helps reject obviously
  fragile candidates early
- do not use that preview as the promotion gate; Stage 4 remains the integrated gate

The final promoted setup still belongs in the strategy skill's execution reference, not in
the signal engine, but promotion occurs only after Stage 4 passes.

### Stage 3 Output Role

After selecting Stage 3 candidate setups, record the shortlisted candidates and evidence.
Do not leave them only in raw score files.

Record them in:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/promotion/final_report.md
```

That report should state:

- the shortlisted Stage 3 candidates
- evidence paths for each candidate
- the conditional winner on the match subset
- any robustness caveats that must be tested in Stage 4

Do not change `SKILL.md` directional logic from Stage 3 setup results unless Stage 1 or
Stage 2 evidence also supports a judgment change. Stage 3 optimizes extraction from accepted
calls; it does not by itself redefine the signal read.

Common tools:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage3_grid_search.py \
  --match-signals <MATCH_SIGNALS_JSON> \
  --signal-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --out dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage3_grid
```

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage3_limit_grid.py \
  --signals <MATCH_SIGNALS_JSON> \
  --signal-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --out dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage3_limit_grid
```

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage3_pyramid.py \
  --signals <MATCH_SIGNALS_JSON> \
  --signal-dir dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/packets \
  --candles dev/data/raw/<ASSET>/5m/candles.csv \
  --tp <TP_PCT> \
  --sl <SL_PCT> \
  --out dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage3_pyramid
```

## Stage 4: Realized Expectancy

Purpose: score shortlisted Stage 3 execution setups on the full frozen Stage 1 decision set
so promotion reflects the trades the strategy would actually take, not only the subset
where direction happened to be correct.

Use the same frozen final Stage 1A full-cycle decision set that fed Stage 2 and Stage 3.
Restore all scored directional outcomes:

- `MATCH`
- `MISMATCH`
- `NEUTRAL` or `SKIP`, if the strategy contract allows them

Stage 4 is the integrated live-likeness gate. Its question is:

- does the chosen execution setup still produce acceptable expectancy after directional
  misses, no-hit cases, side asymmetry, and trading frictions are included?

Required inputs:

- frozen final Stage 1A full-cycle decision file
- one or more shortlisted Stage 3 candidate setups in
  `templates/stage4_candidates.json` shape
- raw candles
- fee and slippage assumptions
- side and slice windows from the active cycle

For each candidate setup, record at minimum:

- total decisions
- executed trades
- TP hits
- SL hits
- no-hit or timed-out trades
- unfilled decisions for limit-style entries
- win rate
- gross expectancy
- net expectancy after costs
- profit factor
- side split
- slice split
- mismatch-cohort damage contribution

The Stage 4 winner is the candidate with the best robust full-cycle realized expectancy, not
necessarily the highest conditional Stage 3 metric.

Outputs:

```text
iterations/<ITERATION_ID>/scores/stage4_realized_expectancy.json
iterations/<ITERATION_ID>/scores/stage4_trade_ledger.json
iterations/<ITERATION_ID>/summaries/iteration_summary.md
```

Canonical scorer:

```text
scripts/optimization/stage4_realized_expectancy.py
```

Candidate manifest template:

```text
templates/stage4_candidates.json
```

Example:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/optimization/stage4_realized_expectancy.py \
  --stage1-scores dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage1a_full_cycle_scores.json \
  --candidates dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores/stage4_candidates.json \
  --candles dev/data/derived/<ASSET>/5m/candles.csv \
  --out-dir dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/iterations/<ITERATION_ID>/scores \
  --fees-bps-per-side 5 \
  --slippage-bps-per-side 3 \
  --slice train:2026-03-01T00:00:00Z:2026-05-01T00:00:00Z \
  --slice validation:2026-05-01T00:00:00Z:2026-05-24T00:00:00Z \
  --slice locked_oos:2026-05-24T00:00:00Z:2026-06-01T00:00:00Z
```

Stage 4 candidate rows are execution-setup records, not strategy records. Keep them generic:

- `candidate_id`
- `entry_type`: `market` or `limit`
- `tp_pct`
- `sl_pct`
- optional `limit_offset_pct`
- optional `pyramid.step_pct`
- optional `pyramid.max_legs`
- optional `pyramid.sl_breakeven`
- optional `source_stage`
- optional `source_path`

`timeout_exit_policy` defaults to `close_at_cutoff`, which means a filled trade that does
not hit TP or SL is realized at the last candle close inside the hold window. Use `zero`
only for explicit research counterfactuals.

### Stage 4 Promotion To Strategy Skill

Only after a Stage 4 candidate passes should the active strategy skill be updated.

Write the selected setup into:

```text
artifacts/skills/strategies/<STRATEGY_ID>/references/execution-parameters.md
```

The execution reference must state the full live-tradable setup:

- evidence path for the selected Stage 4 result
- upstream Stage 3 candidate evidence path
- instrument and account mode assumptions
- leverage
- margin per leg as account margin, not exposure
- maximum legs and maximum total margin
- sizing formula, including contract-unit and lot-size caveats
- entry order type by side, including limit offset and order TTL when applicable
- TP and SL by side when LONG and SHORT differ
- max hold time or no-hit handling when the promoted setup depends on it
- pyramiding trigger, spacing, maximum adds, and whether adds are mechanical
- aggregate TP/SL behavior after pyramiding
- within-candle TP/SL tiebreaker used by the scoring script
- fee and slippage assumptions used by the passing Stage 4 pass
- any side-specific exception supported by Stage 4 evidence

Update:

```text
artifacts/skills/strategies/<STRATEGY_ID>/references/position-management.md
```

only when Stage 4 changes open-position behavior. Position management must read
`execution-parameters.md` as the setup source of truth and must not contradict the promoted
TP/SL, sizing, pyramiding, hold-time, or order-expiration rules.

Record the promoted setup and evidence path in:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/promotion/final_report.md
```

Promotion policy:

- if Stage 3 looked good but Stage 4 fails, do not promote
- reopen Stage 1 if directional misses dominate the loss
- reopen Stage 3 if conditional extraction is still weak on correct calls
- keep the strategy `stale` for the month until a Stage 4 pass exists

## Stage 5: Live Readiness

Purpose: verify that the strategy can be run by a live execution agent without using hidden
training context.

Required outputs:

```text
promotion/final_report.md
promotion/live_prompt_draft.md
```

Promotion checks:

- active strategy skill version is explicit
- execution parameters are complete
- live packet shape matches replay packet shape
- router ownership contract is documented
- account mode is explicit
- demo/live state roots are separated
- sizing rules use exchange contract units correctly

## Failure-Mode Optimization Loop

Run this loop when Stage 1A or Stage 1B stalls, regresses, or exposes repeated ambiguity.
The first below-threshold Stage 1 iteration is already enough to require the corresponding
feature audit before the next strategy rewrite.

1. Freeze exact comparison sets.
2. Join decisions to ground truth.
3. Build a failure ledger.
4. Compute neutral packet features if useful.
5. Isolate the smallest contested subset.
6. Prefer structural separators over one-off thresholds.
7. Write the smallest possible strategy skill update that preserves working cases.
8. Retest failed, protected, and out-of-sample sets.

Good separators explain both failures and protected wins. Examples:

- trend maturity plus local reclaim failure
- accepted support retest versus rejected support retest
- current control versus stale higher-timeframe memory
- range acceptance versus breakout failure

Avoid:

- reversing direction just because the last call failed
- adding broad skip language to hide directional weakness
- action-forcing guard wording that replaces proof of market control
- changing the signal engine to improve model judgment
- encoding exact historical timestamps as strategy rules

Prefer directional reclassification over broader avoidance language. Prefer proof-of-control
wording over action-forcing guard wording.

## Sub-Agent Prompt Templates

### Stage 1A

```text
You are evaluating one neutral trading signal using the strategy skill at:
{strategy_skill_path}

Read the signal packet:
{signal_packet_path}

Task: choose the trade direction, LONG or SHORT, using only the packet evidence and the
strategy skill.

Output only JSON:
{
  "signal_id": "{signal_id}",
  "direction": "LONG or SHORT",
  "confidence": 0.0,
  "reasoning": "Concise packet-grounded reason"
}
```

### Stage 1B

```text
You are evaluating one neutral trading signal using the strategy skill at:
{strategy_skill_path}

The calibrated travel threshold is {threshold_pct}% within {forward_hours} hours.
Read the signal packet:
{signal_packet_path}

Task: decide whether this signal passes the strategy skill's entry gate. Choose the best
direction either way, but make `trade_action` the live entry decision.

Process requirement: reason in two layers. First state the chosen direction from the
directional rules. Then apply the entry gate independently. Do not use "direction is
supported" as the reason that `trade_action` is `ENTER`.

Output only JSON:
{
  "signal_id": "{signal_id}",
  "trade_action": "ENTER or SKIP",
  "direction": "LONG or SHORT",
  "confidence": 0.0,
  "expected_travel": "high or low",
  "entry_gate": "pass or fail",
  "gate_reason_code": "short_snake_case_reason",
  "reasoning": "Concise packet-grounded reason for the entry gate decision"
}
```

## Default Parameters

| Parameter | Default |
| --- | --- |
| forward window | 36h unless strategy mandates otherwise |
| threshold scan | 0.2% to 2.0% by 0.1% |
| monthly Path A trigger rate | 80% |
| Stage 1A agreement gate | 55% |
| Stage 1B precision gate | 70% |
| Stage 1B recall gate | 50% |
| sample size before full run | 25-40 signals from the active recent-regime window |
| sub-agent output | JSON only |

Changing defaults requires a note in the session manifest or stage summary.

## Cross-Session Conventions

- Never mutate prior sessions.
- Every session has `manifest.json`.
- Every score file should identify asset, strategy id, strategy version, signal engine id,
  signal set id, and scoring method. `signal_family` may be retained as legacy metadata.
- Store model decisions separately from deterministic scores.
- Keep failed, protected, and out-of-sample sets identifiable.
- Promote only through `references/promotion-checklist.md`.
