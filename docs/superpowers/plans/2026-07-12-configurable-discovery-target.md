# Configurable Discovery Target Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let each signal-discovery session choose one positive reward multiple while retaining the fixed 1R stop and 2R default.

**Architecture:** Reuse the existing `reward_multiple` field end to end. Unlock its exact-value validation in the API and frozen-target workspace, then bind a positive numeric frontend input to the existing create request. No schemas or downstream calculations change.

**Tech Stack:** React/TypeScript, FastAPI/Pydantic, Python worker artifacts, Node test runner, pytest.

---

### Task 1: Unlock the backend contract

**Files:**
- Modify: `tests/test_api.py`
- Modify: `tests/test_signal_discovery_workspace.py`
- Modify: `apps/api/src/quant_terminal_api/main.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`

- [ ] **Step 1: Write failing API and workspace tests**

Create a session with `reward_multiple: 1.5`, assert its stored config contains `1.5`, and freeze a target with `reward_multiple: 1.5`. Add rejection assertions for a non-1R stop and a nonpositive frozen reward.

- [ ] **Step 2: Run tests to verify the exact 2R locks fail**

Run: `pytest -q tests/test_api.py::test_signal_discovery_api_lifecycle_and_validation tests/test_signal_discovery_workspace.py::test_frozen_target_is_versioned_hashed_idempotent_and_immutable`

Expected: failure from `Outcome-First v1 requires 2R/1R barriers` or `requires a 2R reward barrier`.

- [ ] **Step 3: Replace exact reward checks with positive checks**

In API validation, reject only `stop_multiple != 1.0`; Pydantic continues enforcing `reward_multiple > 0`. In workspace validation, use:

```python
if float(selected_target["reward_multiple"]) <= 0:
    raise ValueError("reward_multiple must be positive")
```

Keep the exact 1R stop check.

- [ ] **Step 4: Run focused backend tests**

Run the two tests from Step 2 and expect both to pass.

### Task 2: Expose the target multiple in session creation

**Files:**
- Modify: `apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx`
- Modify: `apps/web-v2/src/app/signalDiscovery.ts`
- Modify: `apps/web-v2/tests/signalDiscovery.test.ts`

- [ ] **Step 1: Write a failing frontend validation test**

Add and test a pure helper that accepts finite positive target multiples and rejects zero, negative, `NaN`, and infinity.

- [ ] **Step 2: Run the frontend test to verify failure**

Run: `npm --workspace apps/web-v2 run test:signal-discovery`

Expected: failure because the positive-target helper is not exported.

- [ ] **Step 3: Add the minimal frontend state and input**

Add `targetMultiple: 2` to creation state, bind a `Target multiple (R)` number input using `min={Number.MIN_VALUE}` and `step="any"`, send it as `reward_multiple`, and disable creation unless it is finite and positive. Rename `Minimum R (%)` and `Maximum R (%)` to `Minimum stop distance (%)` and `Maximum stop distance (%)`.

- [ ] **Step 4: Run focused frontend tests and production build**

Run `npm --workspace apps/web-v2 run test:signal-discovery` and `npm --workspace apps/web-v2 run build`.

Expected: tests and build pass.

### Task 3: Verify and commit

**Files:**
- Verify all files modified in Tasks 1 and 2.

- [ ] **Step 1: Run focused regression checks**

Run the focused pytest tests, the signal-discovery frontend tests, the frontend production build, scoped Ruff on modified Python files, and `git diff --check`.

Expected: every command exits successfully.

- [ ] **Step 2: Commit only the configurable-target implementation**

Stage the API, worker, frontend, focused tests, and this plan. Commit with `feat: configure discovery target multiple`. Do not stage `artifacts/signal_engine/engine_registry.json`.
