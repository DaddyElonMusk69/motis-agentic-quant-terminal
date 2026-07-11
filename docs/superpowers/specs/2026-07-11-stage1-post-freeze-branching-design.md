# Stage 1 Post-Freeze Branching Design

## Goal

Allow researchers to continue creating and evaluating Stage 1 builder and walk-forward branches after a canonical Stage 1 strategy has been frozen and Stage 2-4 have run. Preserve every existing Stage 1 iteration and the active frozen canonical strategy until a new training branch is explicitly promoted.

Post-freeze branch work is blocked when the Stage 1 session has an attached execution bundle.

## Current Constraint

The session-level frozen guard currently blocks iteration creation, scoring, auditing, prompt access, and deletion. The web UI mirrors that guard by disabling the corresponding controls. Training-iteration promotion is already available after freeze, but it can replace the canonical Stage 1 strategy without invalidating Stage 2-4 artifacts derived from the previous canonical strategy.

## Lifecycle

### Frozen, no execution bundle

- Keep the session status `stage1a_frozen`.
- Keep the active canonical Stage 1 files and all Stage 2-4 artifacts unchanged.
- Allow creation of additional training builder and walk-forward evaluator iterations.
- Allow those iterations to be opened, scored, audited, and deleted.
- Treat every new iteration as an experimental branch. Creating or scoring it does not change the active canonical strategy or downstream evidence.

### Frozen, execution bundle attached

- Block creation, scoring, auditing, deletion, and promotion of Stage 1 branches with HTTP 409.
- Continue to allow read-only access to iteration details and prompts.
- Treat any execution bundle linked by `source_stage1_session_id` as attached. This conservative rule keeps deployed and reproducible bundles pinned to immutable research evidence.

### Promote a post-freeze training branch

1. Verify that the session has no attached execution bundle.
2. Validate and copy the selected iteration strategy into the session strategy module.
3. Regenerate the frozen canonical Stage 1 strategy, decisions, scores, and summary.
4. Delete every Stage 2-4 artifact derived from the previous canonical strategy.
5. Delete Stage 4 and Stage 4B run histories and generated wrappers.
6. Delete portfolio backtests for the source universe because they may include the invalidated session.
7. Leave all Stage 1 iterations intact.
8. Keep the session status `stage1a_frozen`; Stage 2 becomes the next available downstream action.

Downstream cleanup happens only on promotion, not when a branch is created or scored.

## Backend Design

Replace the broad frozen-session guard on iteration actions with an action-specific permission helper:

- Draft sessions allow all existing iteration actions.
- Frozen sessions without execution bundles allow the branch workflow.
- Frozen sessions with any attached execution bundle reject mutating branch actions.
- Read-only detail and prompt endpoints do not require session mutability.

Centralize post-Stage-1 cleanup in one helper so promotion cannot miss artifacts added by later stages. The helper preserves only the regenerated Stage 1 canonical artifacts under `promotion/` and removes:

- Stage 2 capture, per-signal, summary, trade-input, and exit-policy artifacts.
- Stage 3 grid, optimal, pyramid, summary, and Stage 4 candidate artifacts.
- Stage 4 latest artifacts and `stage4_runs/`.
- `stage4b_timing/` and `frozen_stage4b_timing_strategy_module/`.
- The source universe directory under `dev/portfolio_backtests/`.

No database schema or Stage 1 revision counter is required for this minimal change because the active canonical strategy remains unchanged until explicit promotion.

## Frontend Design

- Remove the frozen-state restriction from Create Builder Bundle and Create Evaluator Bundle.
- Remove the frozen-state restriction from Score, Audit, and iteration Delete.
- Keep Freeze disabled while a canonical readout exists.
- Keep Promote available for scored training iterations.
- Surface the backend 409 error when an execution bundle blocks branch mutation.
- Do not reset or visually hide existing Stage 2-4 results while an experimental branch is being developed.

The existing iteration ledger remains the branch history. No new branch-management UI is introduced.

## Failure Handling

- Check for attached execution bundles before any mutating filesystem operation.
- Regenerate canonical Stage 1 successfully before deleting downstream artifacts.
- If canonical generation fails, preserve the previous downstream artifacts.
- If cleanup fails, return an error and do not present Stage 2 as ready. Cleanup should use the known workspace roots and idempotent missing-file handling.

## Testing

- Frozen session without execution bundle can create training and walk-forward iterations.
- Frozen session without execution bundle can score, audit, and delete iterations.
- Read-only iteration prompt access remains available when frozen.
- Any attached execution bundle blocks branch mutation and promotion.
- Creating and scoring a post-freeze branch does not change canonical or Stage 2-4 artifacts.
- Promoting a post-freeze training branch preserves all iterations, replaces canonical Stage 1, and removes Stage 2-4, Stage 4B, run-history, wrapper, and portfolio artifacts.
- Draft-session behavior remains unchanged.
- Existing Stage 1, Stage 2, Stage 3, Stage 4, API, and web build checks pass.

## Out Of Scope

- Archived downstream revisions.
- Parallel active canonical Stage 1 branches.
- Automatic live-route retirement or execution-bundle replacement.
- Stage 1 revision identifiers or database migrations.
- Changes to strategy optimization, scoring thresholds, or downstream algorithms.
