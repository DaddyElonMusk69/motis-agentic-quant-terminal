# Opportunity Bracket Cleanup Design

## Goal

Let a user transform noisy fixed-R timestamp labels into a smaller deterministic set of tradeable opportunity brackets. The user chooses general cleanup conditions, never individual historical timestamps or brackets. The approved training brackets become the evidence shown to the signal-engine agent and the primary target used to score the candidate on training and hidden walk-forward data.

## Principles

- Raw timestamp labels and raw episodes remain immutable audit artifacts.
- Cleanup is a deterministic ordered policy, not manual historical selection.
- Every control is reversible during preview. Resetting all controls reproduces the current raw episode set exactly.
- The same approved policy is applied without modification to hidden walk-forward labels.
- The engine-builder prompt and candidate evaluation bind to the approved policy hash and bracket-artifact hash.
- An approved policy remains reversible until target freeze. Target freeze atomically locks the selected R, entry delay, horizon, target multiple, and approved bracket policy; it is immutable afterward.

## Definitions

- **Raw label:** one fixed-R outcome at one decision timestamp and scenario.
- **Raw bracket:** the existing maximal run of exactly contiguous 5-minute labels with one LONG or SHORT direction.
- **Cleanup policy:** the ordered set of enabled controls and numeric values.
- **Approved bracket:** a continuous LONG or SHORT interval produced by the cleanup policy. It includes its member decision timestamps, inherited bridged timestamps, source bracket IDs, selected R, entry delay, horizon, direction, start/end, resolution timestamp, and policy hash.
- **Active scenario:** the selected R, entry delay, and holding horizon that will be frozen as the discovery target. Counts and chart overlays always describe this scenario.

## Cleanup Pipeline

The processing order is fixed and included in the policy schema.

### 1. Require R Stability

When enabled, a directional timestamp survives only if the selected stop distance and its immediately adjacent available stop-distance values have the same direction. At a grid edge, use the one available neighbor. Disable the control when the session has only one stop distance.

### 2. Require Delay Stability

When enabled, a directional timestamp survives only if every configured entry delay has the same direction for the selected stop distance and horizon. Disable the control when the session has only one entry delay.

Timestamps rejected by either stability rule are excluded and cannot later be restored by gap bridging.

### 3. Bridge Neutral Gaps

This toggle exposes a slider from 0 to 12 intervals, measured in 5-minute timestamps, with zero reproducing raw behavior. Merge two same-direction runs when the timestamps between them are all raw `NEUTRAL` labels and the gap does not exceed the configured size. Never bridge `AMBIGUOUS`, opposite-direction, missing-data, or stability-rejected timestamps.

Bridged neutral timestamps inherit the surrounding direction and become valid members of the continuous approved bracket. The artifact records which members were inherited rather than originally qualifying.

### 4. Minimum Persistence

This toggle exposes a minimum bracket length from 1 to 24, measured in 5-minute timestamps. One reproduces raw behavior. Remove a bracket when its inclusive timestamp count after gap bridging is below the configured minimum.

### 5. One Active Opportunity

Process remaining brackets chronologically. Keep the earliest bracket, using its earliest member as the entry anchor, then suppress every later bracket whose start precedes that anchor label's directional target first-touch timestamp. If the anchor has no first-touch timestamp, use its configured horizon end. Resume selection after resolution. This applies across both directions and models one non-overlapping position at a time.

The policy never merges opposite directions. Suppressed overlaps are counted separately from persistence removals.

## Modal Experience

The opportunity-atlas modal gains a permanent right-side cleanup panel. Episode detail can share the rail through `Cleanup` and `Episode` tabs rather than creating a second nested panel.

The cleanup panel contains:

- Require R stability toggle.
- Require delay stability toggle.
- Bridge neutral gaps toggle and gap slider.
- Minimum persistence toggle and timestamp-count slider.
- One active opportunity toggle.
- Reset button.
- Approve brackets button.
- Approved/draft state, policy revision, and abbreviated policy hash.

The count summary prominently shows raw-to-preview total brackets, LONG, SHORT, removed, merged, and overlap-suppressed counts. It also shows monthly retention and flags any month reduced to zero opportunities.

Controls edit local draft state and request a debounced server preview. Preview never writes artifacts. Reset restores the all-disabled policy and exact raw bracket counts. Approve persists or replaces the complete draft policy and all resulting brackets while the session remains `atlas_ready`; it never accepts bracket IDs selected by the user. Changing the selected R, delay, or horizon makes an earlier approval stale and requires approval again.

## Chart Behavior

The candle overlay and active scenario lane use one source at a time:

- With the default policy, render the current raw brackets.
- With any cleanup control changed, render the server-returned preview brackets.
- After approval, render the approved brackets when no newer draft exists.

The chart never renders individual timestamp bars for a tuned policy. Each approved or preview bracket is one continuous span from bracket start to bracket end. Removed brackets disappear, bridged neutral gaps display as part of the continuous bracket, and overlap-suppressed brackets disappear. LONG remains faint green, SHORT remains faint red, and no ambiguous bracket is rendered.

Other scenario lanes remain available as stability context, but the plot overlay, counts, cleanup controls, and approved artifacts always refer only to the active scenario.

## Contracts And Artifacts

Add `signal_discovery_bracket_policy.v1` with:

- Session and selected-target coordinates.
- Ordered control values.
- Source atlas manifest hash.
- Policy revision and deterministic policy hash.
- Raw and cleaned counts, direction counts, monthly counts, and transformation diagnostics.
- Training bracket artifact path and hash.

Persist approved brackets as Parquet under pre-freeze session artifacts. Target freeze requires a current approval for the selected target coordinates and copies its policy path, policy hash, bracket path, and bracket hash into the immutable frozen discovery target. An all-disabled approval is valid and preserves current behavior. Prompt generation continues to require a frozen target, so it always receives a locked bracket contract.

The revised session order is: run atlas, choose target coordinates, preview and approve brackets, freeze target, generate the engine-builder prompt, attach a candidate, and evaluate. Existing frozen sessions without a bracket policy retain raw-bracket behavior and remain usable.

The prompt authorizes approved training brackets and their matched cleaned hard negatives. Raw timestamp outcomes remain terminal audit evidence and are not the agent's primary target. Removed raw opportunities become hard negatives under the cleaned target because candidate signals there are false positives for the selected opportunity definition.

## Walk-Forward And Evaluation

The terminal applies the frozen policy to hidden walk-forward labels after the candidate is attached. The candidate is scored against approved training brackets and policy-transformed walk-forward brackets using:

- Bracket-level precision and recall.
- Directional accuracy.
- Timestamp coverage within approved brackets.
- Signals in removed or suppressed regions as false positives.
- Monthly opportunity and hit counts.
- Duplicate and non-overlap behavior.
- Net fixed-R outcomes using the frozen target and costs.

Raw-label metrics remain diagnostics and cannot determine acceptance.

## Failure Handling

- Reject preview or approval when target coordinates are missing or not present in the completed atlas.
- Reject policies whose source atlas hash has drifted.
- Reject impossible slider values or controls without the required neighboring scenarios.
- Reject approval that produces zero total brackets. A zero count for one direction is permitted but shown as a strong imbalance warning.
- If preview fails, keep the last valid chart and show the error in the cleanup panel.
- Reject policy mutation after target freeze. Before freeze, a new approval replaces the previous revision and remains fully reversible.

## Tests

- Prove every cleanup transform independently and in the fixed pipeline order.
- Prove reset exactly reproduces raw brackets and counts.
- Prove gap bridging never crosses ambiguous, opposite, missing, or stability-rejected timestamps.
- Prove one-active-opportunity suppression uses first touch and falls back to horizon end.
- Prove preview is read-only and approval writes deterministic, hash-bound policy and Parquet artifacts.
- Prove chart responses contain preview or approved brackets rather than per-timestamp tuned overlays.
- Prove counts, direction totals, monthly retention, removed, merged, and suppressed diagnostics.
- Prove prompt generation requires approval and binds the policy/artifact hashes.
- Prove the identical policy is applied to hidden walk-forward labels and primary evaluation uses cleaned brackets.
- Prove raw/default compatibility for existing sessions and the all-disabled policy.
- Run focused atlas/workspace/API/evaluation tests, discovery-to-Stage-1 integration, frontend signal-discovery tests, production build, scoped Ruff, and `git diff --check`.
