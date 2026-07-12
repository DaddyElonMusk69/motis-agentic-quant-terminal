# Batched Signal Discovery Atlas Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Signal Discovery atlas generation bounded-memory, checkpointed per R value, and resumable while preserving all existing final artifact contracts.

**Architecture:** Add a focused workspace persistence class that owns run identity, per-R immutable parts, checkpoint validation, and streaming final compaction. Change the atlas job to calculate and persist one R value at a time, reuse completed checkpoint entries on retry, and publish the session only after final artifacts exist.

**Tech Stack:** Python 3.13, PyArrow Parquet, JSON checkpoints, pytest, SQLAlchemy runtime fixtures.

---

### Task 1: Checkpointed Workspace

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`
- Modify: `tests/test_signal_discovery_workspace.py`

- [ ] Add a failing test that creates a `TrainingAtlasWorkspace`, writes one risk partition, reconstructs the workspace, and observes the completed risk and persisted counts.
- [ ] Run `pytest tests/test_signal_discovery_workspace.py -q` and verify the new test fails because the workspace class does not exist.
- [ ] Implement deterministic run fingerprinting, atomic part writes, SHA-256 part fingerprints, and atomic `signal_discovery_atlas_checkpoint.v1` JSON persistence.
- [ ] Add failing tests proving changed run identity and changed checkpointed part bytes are rejected.
- [ ] Implement checkpoint validation and rerun the focused tests to green.

### Task 2: Streaming Final Compaction

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`
- Modify: `tests/test_signal_discovery_workspace.py`

- [ ] Add a failing test with two risk partitions that finalizes the workspace and reads the established final Parquet artifact paths.
- [ ] Verify the test fails because finalization is absent.
- [ ] Implement ordered Parquet batch streaming through `pyarrow.parquet.ParquetWriter`, atomic final replacement, atomic feature/feasibility writes, and returned legacy artifact paths.
- [ ] Verify label and episode rows retain deterministic risk order and unique episode identifiers.
- [ ] Run `pytest tests/test_signal_discovery_workspace.py -q` to green.

### Task 3: Per-R Worker Orchestration And Resume

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/jobs.py`
- Modify: `tests/test_worker_jobs.py`

- [ ] Add a failing worker test that pre-populates one completed R partition, runs a two-R atlas job, and asserts `run_training_atlas` is called only for the missing R value.
- [ ] Verify the focused test fails under the current all-R orchestration.
- [ ] Build causal features once, iterate configured R values in deterministic order, heartbeat with risk progress, run the deterministic atlas for one R, renumber episodes, select hard negatives for that R, and checkpoint the result.
- [ ] Assemble feasibility summaries from checkpoint entries and call streaming finalization only after all R values complete.
- [ ] Update the existing write-failure test to inject finalization failure and prove the session remains failed rather than ready.
- [ ] Run `pytest tests/test_worker_jobs.py -q` to green.

### Task 4: Contract Regression

**Files:**
- Modify only if required by observed failures: `tests/test_signal_discovery_end_to_end.py`

- [ ] Run `pytest tests/test_signal_discovery_atlas.py tests/test_signal_discovery_workspace.py tests/test_worker_jobs.py -q`.
- [ ] Run `pytest tests/test_signal_discovery_end_to_end.py -q`.
- [ ] Run scoped Ruff checks over the changed worker modules and tests.
- [ ] Inspect `git diff --check` and confirm `artifacts/signal_engine/engine_registry.json` remains untouched by this work.
