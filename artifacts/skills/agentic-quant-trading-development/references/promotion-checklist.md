# Promotion Checklist

Promote a strategy only when all relevant checks pass.

## Research

- Data manifest is valid.
- Signal set manifest is valid.
- Training session manifests are complete.
- The current monthly walk-forward universe exists and the asset appears in
  `tradable_universe.json`.
- The promoted session links to the monthly universe manifest and records the train,
  validation, and locked OOS windows.
- Train, validation, and locked OOS samples came from the same frozen Stage 0 cycle record
  set rather than recomputed Stage 0 artifacts per sample.
- After Stage 1A locked OOS passed, the final directional version was re-scored once across
  the whole frozen cycle and that canonical full-cycle readout is the source for Stage 2
  and Stage 3 evidence.
- Stage 4 used the same frozen final Stage 1A decision set rather than only the Stage 3
  match subset.
- Ground truth and scoring are deterministic.
- Failure audits exist for material skill changes.
- Regression sets were checked.

## Strategy Skill

- Active version is explicit.
- Entry, sizing, TP, SL, hold time, and management rules are in the skill.
- Passing Stage 4 evidence path is recorded in `references/execution-parameters.md`.
- Leverage, account-margin sizing, entry order type, TP/SL, order TTL, pyramiding, and
  no-hit handling are explicit when they apply.
- `position-management.md` treats `execution-parameters.md` as the setup source of truth.
- Live prompt references the active skill and execution reference.
- Old strategy variants are not treated as active.

## Live

- Live scanner packet shape matches replay packet shape.
- Router account mode is explicit.
- Owner state path is correct.
- Execution agent uses exchange truth for positions, orders, fills, and balances.
- Execution agent has checked the current monthly universe and strategy freshness status.
- Execution agent has installed the latest promoted strategy skill into its own skill set.
- Execution agent has warmed `live/data/raw/<ASSET>/5m/candles.csv` from `dev/data` and refreshed the derived live candles before enabling cron.
- Demo or dry-run has been checked before live capital.
