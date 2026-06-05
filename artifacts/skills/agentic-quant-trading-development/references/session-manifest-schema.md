# Session Manifest Schema

Every training session must include `manifest.json`.

Session path:

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/
```

Required subfolders:

- `inputs/`
- `iterations/`
- `promotion/`

Session-level folders hold agentic evaluation setup and final promotion artifacts. Agentic
evaluation runs live under `iterations/`.

Stage 0 is shared deterministic foundation for a strategy/signal set. For normal monthly
cycle builds, create or audit it through:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/run_monthly_stage0.py
```

The runner computes the month windows, validates signal/candle coverage, runs the Stage 0
step tools, and writes the monthly walk-forward universe. Store Stage 0 outside sessions
at:

```text
dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/
```

Do not create Stage 0 inside session or iteration folders. Do not re-run or overwrite Stage
0 for each strategy version. Re-run Stage 0 only when the data, signal set, forward window,
calibration method, or ground-truth algorithm changes.

After each monthly Stage 0 pass, store the walk-forward universe at:

```text
dev/walk_forward/<YYYY-MM>/
  manifest.json
  stage0_branch_decisions.json
  tradable_universe.json
  watchlist_universe.json
  summaries/monthly_universe.md
```

This monthly layer links cross-asset Stage 0 branch decisions to later per-asset sessions.
It does not move existing sessions.

```text
dev/training_sessions/<STRATEGY_ID>/<SESSION_ID>/
  manifest.json
  inputs/
  iterations/
    iter_001_<STRATEGY_VERSION>/
      manifest.json
      handoff.md
      signal_sample.json
      decisions/
      scores/
      audits/
      summaries/
      source_artifacts/
        strategy_skill_snapshot/
        strategy_skill_snapshot_manifest.json
  promotion/
```

Minimal manifest:

```json
{
  "session_id": "20260527_btc_vegas_v016_stage2_direction",
  "created_at": "2026-05-27T00:00:00Z",
  "asset": "BTC",
  "strategy_id": "btc-vegas-tunnel-v01",
  "strategy_version": "v0.16",
  "signal_engine_id": "vegas_ema",
  "signal_family": "vegas_ema",
  "signal_set_id": "2026-BTC-2h-dedupe-vote2",
  "stage": "stage1a_directional_agreement",
  "iteration_mode": true,
  "active_iteration": "",
  "iteration_count": 0,
  "data_manifest": "dev/data/manifests/BTC.json",
  "signal_set_manifest": "dev/signals/vegas_ema/BTC/2026-BTC-2h-dedupe-vote2/manifest.json",
  "stage0_manifest": "dev/training_sessions/btc-vegas-tunnel-v01/stage0/2026-BTC-2h-dedupe-vote2/manifest.json",
  "walk_forward_month": "2026-06",
  "train_window": {
    "start": "2026-03-01",
    "end": "2026-04-30"
  },
  "validation_window": {
    "start": "2026-05-01",
    "end": "2026-05-24"
  },
  "locked_oos_window": {
    "start": "2026-05-25",
    "end": "2026-05-31"
  },
  "universe_manifest_path": "dev/walk_forward/2026-06/manifest.json",
  "inputs": {},
  "outputs": {},
  "scoring": {},
  "status": "created"
}
```

Recommended status values:

- `created`
- `running`
- `scored`
- `audited`
- `promoted`
- `rejected`

## Stage 0 Output Naming

Use stable names under `dev/training_sessions/<STRATEGY_ID>/stage0/<SIGNAL_SET_ID>/`:

- `manifest.json`
- `scores/travel_distribution.json`
- `scores/threshold_calibration.json`
- `scores/_scoreable_signal_subset/packets/`
- `scores/ground_truth/`
- `scores/ground_truth_summary.json`
- `summaries/threshold_calibration.md`
- `summaries/stage0_ground_truth.md`
- `summaries/branch_decision.md`

## Monthly Walk-Forward Universe Naming

Use stable names under `dev/walk_forward/<YYYY-MM>/`:

- `manifest.json`
- `stage0_branch_decisions.json`
- `tradable_universe.json`
- `watchlist_universe.json`
- `summaries/monthly_universe.md`

`manifest.json` records the walk-forward month, as-of date, `signal_engine_ids`, train
window, validation window, locked OOS window, Path A trigger-rate threshold, and linked
files. A singular `signal_family` is legacy metadata and should not be used for new
routing.

`stage0_branch_decisions.json` records one strategy-engine candidate per Stage 0 manifest:

- asset
- strategy id
- signal engine id
- signal set id
- Stage 0 manifest path
- total valid signals
- triggered signals
- trigger rate
- branch path: `path_a` or `path_b`
- threshold pct and forward hours

`tradable_universe.json` contains only Path A records. `watchlist_universe.json` contains
Path B records and exclusion reasons. The top-level array is named `assets` for
compatibility, but each element is keyed by `strategy_id`, not deduped asset. The same
asset may appear multiple times when different strategies or signal engines are eligible.
Exactly one row per `strategy_id` is allowed for a month. Both files include:

- `strategy_id`
- `signal_engine_id`
- `asset`
- `signal_set_id`
- `trigger_rate_pct`
- `branch_path`
- `branch_decision`
- `strategy_training_status`: `retrained_for_month`, `stale`, or `missing_strategy`
- `strategy_version`
- `latest_training_session`
- `latest_training_date`
- `notes`

Interpret freshness strictly:

- `retrained_for_month`: a current walk-forward-month session exists for the same
  `signal_set_id` and that session has a promotion-grade Stage 4 expectancy artifact at
  `promotion/final_report.md`
- `stale`: the strategy skill exists, but either no current-month session exists, or the
  current-month work has not yet produced a completed monthly Stage 4 pass
- `missing_strategy`: no strategy skill exists

## Iteration Manifest

Each agentic evaluation pass must create an iteration folder.

Iteration id:

```text
iter_<NNN>_<STRATEGY_VERSION>
```

Example:

```text
iter_001_v0.16
```

Minimal iteration manifest:

```json
{
  "schema_version": "0.2",
  "iteration_id": "iter_001_v0.16",
  "created_at": "2026-05-27T00:00:00Z",
  "session_id": "20260527_btc_vegas_v016_stage1a",
  "stage": "stage1a_directional_agreement",
  "asset": "BTC",
  "strategy_id": "btc-vegas-tunnel-v01",
  "strategy_version": "v0.16",
  "signal_engine_id": "vegas_ema",
  "signal_family": "vegas_ema",
  "signal_set_id": "2026-BTC-2h-dedupe-vote2",
  "sample_method": "recent_regime_train",
  "sample_size": 32,
  "contamination_controls": {
    "ground_truth_hidden": true,
    "future_candles_hidden": true,
    "prior_iteration_results_hidden": true,
    "proposed_fixes_hidden": true
  },
  "handoff_path": "handoff.md",
  "signal_sample_path": "signal_sample.json",
  "strategy_skill_snapshot": {
    "path": "source_artifacts/strategy_skill_snapshot",
    "manifest_path": "source_artifacts/strategy_skill_snapshot_manifest.json"
  },
  "outputs": {
    "decisions": "decisions/",
    "scores": "scores/",
    "audit": "audits/failure_audit.md",
    "summary": "summaries/iteration_summary.md"
  },
  "status": "created"
}
```

`strategy_skill_snapshot_manifest.json` should include capture time, source skill path,
and per-file hashes so the iteration can be replayed with the exact same skill content.

## Iteration Output Naming

Use stable names inside each iteration folder:

- `handoff.md`
- `signal_sample.json`
- `decisions/stage1a_directional_decisions.json`
- `scores/stage1a_directional_scores.json`
- `decisions/stage1b_screening_decisions.json`
- `scores/stage1b_screening_scores.json`
- `scores/stage2_capture_curve.json`
- `scores/stage3_grid_results.json`
- `summaries/iteration_summary.md`
- `audits/failure_audit.md`
- `source_artifacts/`

## Agentic Iteration Rules

- The builder writes `handoff.md` and `signal_sample.json`.
- The builder chooses the sample before invoking the evaluator.
- The builder records the exact ordered packet paths in `signal_sample.json`.
- The builder records the sample role in `sample_method`, for example
  `recent_regime_train`, `forward_validation`, `locked_recent_oos`, `protected_wins`,
  `failed_cases_retest`, or `recent_live_shadow`.
- The builder must choose samples chronologically from the active live-relevant regime:
  for crypto `2h` / `36h` work, train on the two complete calendar months immediately
  before the validation month, validate on the latest scoreable month excluding the locked
  newest slice, and keep that newest slice untouched for OOS/live-readiness. At the beginning
  of June 2026, this means March-April training, May validation, and the newest May slice as
  OOS. Older historical packets are stress diagnostics unless the session explicitly targets
  that older regime.
- For the June 2026 monthly Stage 0 cycle under the "no June candles" policy, keep the
  signal set frozen through `2026-06-01T00:00:00Z` but score only the subset whose full
  `36h` outcome window stays inside May. With `5m` candles, that means a latest outcome
  candle of `2026-05-31T23:55:00Z` and a latest scoreable signal timestamp of
  `2026-05-30T11:55:00Z`.
- Those train/validation/OOS slices should come from one shared Stage 0 record set for the
  frozen cycle horizon. Do not create a separate Stage 0 just because the session advances
  from training to validation.
- If a fitted signal filter, vote threshold, asset shortlist, dedupe rule, or signal
  generation threshold changes, the builder records a new signal set id and reruns Stage 0
  for that signal set. The fitted choices must come from the training window only and then
  stay frozen for validation and OOS.
- If random tie-breaks are used inside a time bucket, the builder records the seed and
  bucket policy in `signal_sample.json` `sampling_notes`.
- The evaluator agent receives only the handoff, sampled packet paths, and iteration strategy skill snapshot.
- The evaluator agent must evaluate only the sampled packet paths and must not scan the
  signal folder for extra packets.
- The evaluator agent must not receive ground truth, future candles, prior iteration failures,
  proposed fixes, or score files.
- Stage 1B decisions must use `trade_action: ENTER` or `trade_action: SKIP`; `trade_action`
  is the entry-gate source of truth.
- The builder scores returned decisions and writes summaries/audits.
- Strategy skill edits happen only after scoring and audit.
- A new strategy skill version requires a new iteration folder.
- Do not overwrite old iterations.

## Immutability

Do not overwrite old sessions. If a scoring method changes, create a new session or write a
new score file with a method/version suffix and explain it in the summary.

Use `scripts/validate_training_session.py` and `scripts/validate_iteration.py` before
asking another agent to evaluate packets or before promoting any strategy update.
