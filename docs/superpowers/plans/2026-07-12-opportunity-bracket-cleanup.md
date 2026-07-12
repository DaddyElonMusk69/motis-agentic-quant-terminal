# Opportunity Bracket Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add reversible, deterministic bracket-cleanup controls whose approved output becomes the frozen training and walk-forward opportunity target shown to the engine-building agent and used in candidate scoring.

**Architecture:** A new worker module owns policy normalization, timestamp transforms, diagnostics, and artifact hashing. API preview is read-only; approval writes a revisioned policy and approved Parquet brackets before target freeze. Freeze binds those hashes, walk-forward replays the same policy on hidden labels, evaluation scores signals against cleaned brackets, and the modal swaps its active-scenario overlay to preview/approved brackets.

**Tech Stack:** Python, PyArrow/Parquet, FastAPI/Pydantic, React/TypeScript, TanStack Query, Lightweight Charts, pytest, Node test runner.

---

### Task 1: Deterministic cleanup engine

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/brackets.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/__init__.py`
- Create: `tests/test_signal_discovery_brackets.py`

- [ ] Write failing unit tests for the all-disabled identity policy, adjacent-R agreement, all-delay agreement, neutral-only gap bridging, minimum persistence, and one-active-opportunity suppression.
- [ ] Run `pytest -q tests/test_signal_discovery_brackets.py` and confirm imports or assertions fail because the cleanup engine is absent.
- [ ] Implement `normalize_bracket_policy`, `build_bracket_preview`, `load_training_bracket_preview`, `approve_training_brackets`, `read_approved_bracket_contract`, and `apply_bracket_policy`.
- [ ] Ensure approved brackets contain continuous start/end spans, member timestamps, inherited timestamp count, direction, active target coordinates, first-touch resolution, and deterministic IDs.
- [ ] Write `bracket_policy.json`, `training_brackets.parquet`, and `training_hard_negatives.parquet` atomically with SHA-256 fingerprints and a deterministic policy hash.
- [ ] Run `pytest -q tests/test_signal_discovery_brackets.py` and confirm all transform and hash tests pass.

### Task 2: Approval and freeze contracts

**Files:**
- Modify: `apps/api/src/quant_terminal_api/main.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`
- Modify: `tests/test_api.py`
- Modify: `tests/test_signal_discovery_workspace.py`

- [ ] Write failing API tests for read-only preview, reversible approval replacement while `atlas_ready`, coordinate validation, zero-bracket rejection, and mutation rejection after freeze.
- [ ] Write a failing workspace test proving target freeze embeds bracket policy/artifact paths and hashes.
- [ ] Add `SignalDiscoveryBracketPolicyRequest` and POST routes for `/bracket-cleanup/preview` and `/bracket-cleanup/approve`.
- [ ] Store approval metadata in the session summary without a database migration.
- [ ] Extend `freeze_target_contract` with an optional bracket contract and make the API create an all-disabled approval when a legacy caller freezes without one.
- [ ] Validate approval coordinates and source-atlas fingerprints during freeze.
- [ ] Run focused API and workspace tests and confirm they pass.

### Task 3: Hidden walk-forward replay and cleaned scoring

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/jobs.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/evaluation.py`
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/prompt.py`
- Modify: `tests/test_worker_jobs.py`
- Modify: `tests/test_signal_discovery_evaluation.py`
- Modify: `tests/test_signal_discovery_prompt.py`

- [ ] Write failing tests proving the frozen policy produces hidden walk-forward brackets and that a signal outside cleaned brackets is a false positive even when its raw fixed-R label is directional.
- [ ] Add scenario-label expansion for neighboring R and configured delays only when the frozen policy requires them.
- [ ] Materialize `walk_forward_brackets.parquet` and cleanup diagnostics with the frozen policy hash.
- [ ] Load approved training/WF brackets during evaluation; calculate bracket precision, direction-aware bracket recall, and cleaned-target directional accuracy while retaining realized fixed-R net returns.
- [ ] Replace raw timestamp/episode prompt evidence with approved training brackets, cleaned hard negatives, baseline features, and the frozen target contract. Preserve raw behavior for legacy targets without bracket artifacts.
- [ ] Run focused worker, evaluation, and prompt tests and confirm they pass.

### Task 4: Modal preview and approval panel

**Files:**
- Modify: `apps/web-v2/src/app/api.ts`
- Modify: `apps/web-v2/src/app/atlasVisualization.ts`
- Modify: `apps/web-v2/src/components/OpportunityAtlasChart.tsx`
- Modify: `apps/web-v2/src/components/OpportunityAtlasModal.tsx`
- Modify: `apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx`
- Modify: `apps/web-v2/src/styles/shell.css`
- Modify: `apps/web-v2/tests/signalDiscovery.test.ts`

- [ ] Write failing frontend helper tests for default policy identity, policy normalization, count formatting, and active-lane bracket replacement.
- [ ] Add API types and clients for preview and approval.
- [ ] Pass selected entry delay/horizon into the modal and keep cleanup approval synchronized with page target controls.
- [ ] Add the permanent right rail with R stability, delay stability, neutral-gap, persistence, and one-active-opportunity controls; expose sliders only while their control is enabled.
- [ ] Show raw-to-preview total, LONG, SHORT, removed, merged, overlap-suppressed, monthly-zero warnings, draft/approved state, reset, and approve actions.
- [ ] Render only the active scenario's preview/approved brackets over candles; keep other scenario lanes as context. Reset must restore the exact raw active lane.
- [ ] Run `npm --workspace apps/web-v2 run test:signal-discovery` and `npm --workspace apps/web-v2 run build`.

### Task 5: End-to-end verification and commit

**Files:**
- Verify all files modified above.

- [ ] Run focused bracket, atlas, workspace, API, worker-job, prompt, evaluation, and discovery-to-Stage-1 tests.
- [ ] Run frontend signal-discovery tests and the production build.
- [ ] Run scoped Ruff on changed Python modules/tests and `git diff --check`.
- [ ] Inspect staged files and exclude `artifacts/signal_engine/engine_registry.json`.
- [ ] Commit with `feat: add opportunity bracket cleanup`.
