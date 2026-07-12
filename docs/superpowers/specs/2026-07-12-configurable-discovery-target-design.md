# Configurable Discovery Target Design

## Goal

Allow each Outcome-First signal-discovery session to select one positive target multiple while keeping the stop fixed at 1R. Existing sessions and the default behavior remain 2R target / 1R stop.

## Design

- Preserve the existing `reward_multiple` field across API, session config, frozen target, atlas artifacts, prompts, evaluation, handoff, and downstream training.
- Add one positive numeric `Target multiple` input to the discovery-session creation form. It defaults to `2` and accepts any finite value greater than zero.
- Rename the creation-form `R range` label to `Stop distance range` so it is not confused with the reward multiple.
- Send the selected value as `reward_multiple`; continue sending `stop_multiple: 1`.
- Replace the API's exact 2R/1R validation with positive reward validation and exact 1R stop validation.
- Replace the frozen-target workspace's exact 2R check with positive reward validation. Retain its exact 1R stop check.
- Do not add a schema version, migration, target grid, presets, or alternate target field.

## Compatibility

Existing sessions retain their stored reward multiple. Requests that omit the field continue to receive the existing 2R default. Atlas labeling, scoring, target/stop display, prompt generation, Stage 0 handoff, and downstream fixed exits already consume the stored reward multiple dynamically.

## Validation

- Frontend creation tests prove the configured target is sent and invalid nonpositive input cannot be submitted.
- API tests prove positive non-2R targets are accepted while zero, negative, and non-1R stops are rejected.
- Workspace and end-to-end discovery tests prove a non-2R target survives freezing and downstream artifact generation.
- Run focused discovery/API/worker tests, frontend signal-discovery tests, the frontend production build, scoped Ruff, and `git diff --check`.
