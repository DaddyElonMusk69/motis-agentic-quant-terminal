# Stage 1 Post-Freeze Branching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow complete Stage 1 iteration work after freeze while preserving the active canonical strategy until promotion, then remove stale Stage 2-4 and portfolio evidence.

**Architecture:** Replace the global frozen-session guard on iteration endpoints with an execution-bundle attachment guard. Add one Stage 1-owned downstream cleanup helper and invoke it only after a promoted training branch successfully regenerates canonical Stage 1. Enable the existing iteration controls in web-v2 without new state or API types.

**Tech Stack:** FastAPI, Python filesystem artifacts, pytest, React, TypeScript, TanStack Query, Vite.

---

## File Structure

- Modify `apps/api/src/quant_terminal_api/main.py`: endpoint authorization and promotion cleanup orchestration.
- Modify `apps/worker/src/quant_terminal_worker/stage1/workspace.py`: centralized Stage 1 downstream cleanup.
- Modify `apps/web-v2/src/pages/ResearchDevelopmentPage.tsx`: enable post-freeze iteration controls.
- Modify `tests/test_api.py`: API permission and promotion cleanup coverage.
- Modify `tests/test_stage1_workspace.py`: cleanup helper coverage.

### Task 1: Permit Post-Freeze Iteration Work

**Files:**
- Modify: `apps/api/src/quant_terminal_api/main.py:893-1061,1884-1942,3755-3775`
- Test: `tests/test_api.py:2507-2540,4880-4911`

- [ ] **Step 1: Write failing post-freeze permission tests**

Replace the frozen rejection tests with creation and scoring tests for a frozen session that has no execution bundle. Add a linked execution-bundle test that expects HTTP 409.

```python
response = client.post(
    "/api/v1/research/stage1-sessions/stage1-aave/iterations",
    json={"sample_method": "training", "bundle_role": "strategy_builder"},
)
assert response.status_code == 200
assert response.json()["iteration"]["iteration_id"] == "iter_001_v0.1"


repository.execution_bundles = [{
    "bundle_id": "bundle-aave",
    "source_stage1_session_id": "stage1-aave",
    "status": "promoted",
}]
response = client.post(
    "/api/v1/research/stage1-sessions/stage1-aave/iterations",
    json={"sample_method": "training", "bundle_role": "strategy_builder"},
)
assert response.status_code == 409
assert response.json()["detail"] == "Stage 1 session has an attached execution bundle"
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=packages/strategy_sdk/src:apps/api/src:apps/worker/src pytest tests/test_api.py -q -k 'allows_frozen_session_without_execution_bundle or attached_execution_bundle'
```

Expected: allowed operations fail with HTTP 409 and the bundle-specific guard is absent.

- [ ] **Step 3: Implement the action-specific guard**

Add a repository-aware helper and call it before create, delete, score, audit, and promote. Remove the frozen guard from read-only prompt access.

```python
def _ensure_stage1_branch_action_allowed(*, repository: Any, session: dict[str, Any]) -> None:
    finder = getattr(repository, "list_execution_bundles_for_stage1_session", None)
    if callable(finder):
        bundles = finder(session["session_id"])
    else:
        bundles = [
            bundle
            for bundle in repository.list_execution_bundles()
            if bundle.get("source_stage1_session_id") == session["session_id"]
        ]
    if bundles:
        raise HTTPException(status_code=409, detail="Stage 1 session has an attached execution bundle")
```

- [ ] **Step 4: Run focused API tests**

Run the Step 2 command. Expected: PASS.

### Task 2: Clean Downstream Evidence On Promotion

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage1/workspace.py`
- Modify: `apps/api/src/quant_terminal_api/main.py:982-1053`
- Test: `tests/test_stage1_workspace.py`
- Test: `tests/test_api.py:3248-3322`

- [ ] **Step 1: Write a failing cleanup-helper test**

Seed Stage 1 iterations and canonical files alongside Stage 2-4 files, Stage 4/4B directories, a wrapper, and portfolio output. Assert that cleanup preserves Stage 1 and removes downstream evidence.

```python
def test_clear_stage1_downstream_artifacts_preserves_stage1(tmp_path: Path):
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(tmp_path / "dev/training_sessions/aave/stage1-aave"),
        "source_universe_run_id": "pool-aave",
    }
    clear_stage1_downstream_artifacts(workspace_root=tmp_path, session=session)
    root = Path(session["artifact_root"])
    assert (root / "iterations/iter_001_v0.1/manifest.json").exists()
    assert (root / "promotion/stage1a_canonical_full_cycle_scores.json").exists()
    assert not (root / "promotion/stage2_capture_curve.json").exists()
    assert not (root / "promotion/stage4_runs").exists()
    assert not (root / "promotion/stage4b_timing").exists()
    assert not (tmp_path / "dev/portfolio_backtests/pool-aave").exists()
```

- [ ] **Step 2: Run test to verify it fails**

```bash
PYTHONPATH=packages/strategy_sdk/src:apps/api/src:apps/worker/src pytest tests/test_stage1_workspace.py::test_clear_stage1_downstream_artifacts_preserves_stage1 -q
```

Expected: FAIL because the helper does not exist.

- [ ] **Step 3: Implement centralized cleanup**

```python
def clear_stage1_downstream_artifacts(*, workspace_root: Path, session: dict[str, Any]) -> None:
    promotion_root = _artifact_root(workspace_root, session) / "promotion"
    for name in STAGE1_DOWNSTREAM_FILES:
        (promotion_root / name).unlink(missing_ok=True)
    for name in ("stage4_runs", "stage4b_timing", "frozen_stage4b_timing_strategy_module"):
        shutil.rmtree(promotion_root / name, ignore_errors=True)
    universe_run_id = session.get("source_universe_run_id")
    if universe_run_id:
        shutil.rmtree(
            workspace_root / "dev" / "portfolio_backtests" / str(universe_run_id),
            ignore_errors=True,
        )
```

`STAGE1_DOWNSTREAM_FILES` must enumerate Stage 2 capture/per-signal/summary/exit-policy/trade-input files, Stage 3 grid/optimal/pyramid/summary files, Stage 4 candidates, and Stage 4 latest artifacts. It must not include canonical Stage 1 files.

- [ ] **Step 4: Add promotion integration assertions**

Extend the training-iteration promotion API test with representative downstream and portfolio paths. After promotion, assert that the promoted frozen strategy and every iteration remain while downstream paths are gone.

```python
assert (artifact_root / "iterations" / iteration_id / "manifest.json").exists()
assert "promoted_training" in (artifact_root / "promotion/frozen_stage1a_strategy_module/strategy.py").read_text()
assert not (artifact_root / "promotion/stage2_capture_curve.json").exists()
assert not (artifact_root / "promotion/stage4b_timing").exists()
assert not (tmp_path / "dev/portfolio_backtests/universe-march-may-vegas").exists()
```

- [ ] **Step 5: Invoke cleanup after canonical generation succeeds**

Import the helper into `main.py`. Guard against execution bundles before copying the strategy, then call cleanup only after `run_stage1a_canonical_full_cycle` succeeds.

```python
result = run_stage1a_canonical_full_cycle(
    workspace_root=Path.cwd(),
    session=session,
    signals_by_role=_stage1_full_cycle_signals(session),
)
clear_stage1_downstream_artifacts(workspace_root=Path.cwd(), session=session)
```

- [ ] **Step 6: Run Stage 1 workspace and promotion tests**

```bash
PYTHONPATH=packages/strategy_sdk/src:apps/api/src:apps/worker/src pytest tests/test_stage1_workspace.py tests/test_api.py -q -k 'stage1 or clear_stage1_downstream'
```

Expected: PASS.

### Task 3: Enable Existing Post-Freeze Controls

**Files:**
- Modify: `apps/web-v2/src/pages/ResearchDevelopmentPage.tsx:2297-2409`

- [ ] **Step 1: Enable bundle creation**

Remove only the frozen restriction while retaining pending and walk-forward prerequisites.

```tsx
disabled={createBundlePending || walkForwardLocked}
```

- [ ] **Step 2: Enable score, audit, and delete**

```tsx
<button
  className="button button--secondary"
  onClick={(event) => { event.stopPropagation(); onScore(iteration); }}
  type="button"
>
  Score
</button>
<button
  className="button button--secondary"
  disabled={!stage1ScoreForRole(iteration, stage1RoleForIteration(iteration))}
  onClick={(event) => { event.stopPropagation(); onAudit(iteration); }}
  type="button"
>
  Audit
</button>
```

Remove `disabled={frozen}` from Delete. Keep Freeze disabled whenever `frozen` is true.

- [ ] **Step 3: Build web-v2**

```bash
npm --workspace apps/web-v2 run build
```

Expected: PASS.

### Task 4: Full Verification

**Files:**
- Verify only; preserve unrelated dirty files.

- [ ] **Step 1: Run backend regression coverage**

```bash
PYTHONPATH=packages/strategy_sdk/src:apps/api/src:apps/worker/src pytest tests/test_stage1_workspace.py tests/test_stage1_scoring.py tests/test_stage2_capture.py tests/test_api.py -q
```

Expected: PASS.

- [ ] **Step 2: Run web-v2 production build**

```bash
npm --workspace apps/web-v2 run build
```

Expected: PASS.

- [ ] **Step 3: Inspect final scope**

```bash
git diff --check
git status --short
git diff -- apps/api/src/quant_terminal_api/main.py apps/worker/src/quant_terminal_worker/stage1/workspace.py apps/web-v2/src/pages/ResearchDevelopmentPage.tsx tests/test_api.py tests/test_stage1_workspace.py
```

Confirm that the pre-existing Stage 0 `terminal_fallback` edits and `skills/stage1a-training-optimizer/SKILL.md` remain untouched.
