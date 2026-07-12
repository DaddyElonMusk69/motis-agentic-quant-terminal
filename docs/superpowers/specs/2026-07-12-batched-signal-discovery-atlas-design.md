# Batched Signal Discovery Atlas Design

## Problem

The training atlas currently builds every fixed-R label and episode in Python memory before writing any training artifact. A one-year 5m BTC study with 21 R values produces roughly 2.4 million nested label records. Final Arrow conversion temporarily duplicates that data, interruption loses all completed work, and the UI has no meaningful progress beyond a heartbeat.

## Goals

- Bound atlas label and episode memory to one R frontier at a time.
- Durably publish progress after every R value.
- Resume a retry without recomputing completed R values.
- Reject checkpoints created for different evidence or configuration.
- Preserve the existing final artifact names and schemas used by prompts, evaluation, and handoff.
- Mark the session ready only after every final artifact is atomically published.

## Architecture

`run_training_atlas` remains the deterministic in-memory calculation for one supplied risk grid and continues to support small unit tests. The atlas job calls it once per R value. After each call, a workspace component writes immutable label, episode, and hard-negative Parquet parts beneath `atlas/.work/`, then atomically updates `atlas/checkpoint.json`.

The checkpoint carries a deterministic run fingerprint derived from the evidence-manifest hash, primary dataset, split boundaries, and complete atlas configuration. On retry, every recorded part and fingerprint is validated before completed R values are skipped. A part written without a checkpoint is treated as uncommitted and may be replaced safely.

After all R values complete, the workspace streams the ordered parts into temporary final Parquet files and atomically renames them to the established paths:

- `atlas/training_timestamp_labels.parquet`
- `atlas/training_episodes.parquet`
- `atlas/training_hard_negatives.parquet`
- `atlas/training_features.parquet`
- `atlas/r_feasibility.json`

The worker retains only the causal feature baseline and one R result in memory. Episode identifiers are renumbered with a deterministic cumulative offset before each part is written, preserving uniqueness across R partitions.

## Progress And Failure Semantics

The job heartbeat `current_step` reports `risk_<n>_of_<total>`. A completed checkpoint remains reusable after worker termination, disk-write failure, or process restart. The session remains `atlas_running` during partial work and changes to `atlas_ready` only after final compaction and the session manifest succeed.

If the run fingerprint or a checkpointed part hash differs, the job fails explicitly rather than combining incompatible evidence. Final files are never read from partially written paths because every write uses a sibling temporary file followed by atomic replacement.

## Compatibility

Downstream readers continue to receive single Parquet files. Existing frozen sessions are untouched. The `.work` partitions and checkpoint are implementation artifacts and are not authorized as agent evidence.

## Tests

- Prove one risk partition is checkpointed atomically and reloadable.
- Prove mismatched run identity and mutated/missing parts are rejected.
- Prove final compaction preserves row order, counts, schemas, and existing artifact paths.
- Prove a retry skips an already checkpointed R value.
- Prove finalization failure never marks the session `atlas_ready`.
- Run focused atlas/workspace/worker tests and the discovery-to-Stage-1 end-to-end test.
