# Outcome-First Signal Discovery v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an end-to-end Signal Discovery workflow that creates a fixed-R opportunity atlas from canonical Parquet, freezes a leakage-safe target, generates one engine-builder prompt, evaluates the resulting engine, and hands an accepted candidate into the existing Stage pipeline without manual artifact repair.

**Architecture:** Add a new `signal_discovery` worker domain for first-touch labeling, episode construction, artifacts, prompts, evaluation, and handoff. Persist session state in a dedicated Postgres table while keeping large timestamp/episode data in versioned Parquet/JSON artifacts. Expose the lifecycle through queued API jobs and a new R&D workspace page; bridge accepted engines into the existing Stage 1 workflow by materializing compatible fixed-target Stage 0 artifacts rather than running legacy threshold calibration.

**Tech Stack:** Python 3.12, FastAPI, SQLAlchemy, PyArrow/Parquet, pytest, React 19, TypeScript, TanStack Query, Vite, Lucide icons, existing Motis runtime jobs and signal-engine contracts.

---

## File Structure

**Create:**

- `apps/worker/src/quant_terminal_worker/signal_discovery/__init__.py` - public discovery-domain exports.
- `apps/worker/src/quant_terminal_worker/signal_discovery/atlas.py` - executable first-touch labels, delayed-entry sensitivity, episode grouping, and split summaries.
- `apps/worker/src/quant_terminal_worker/signal_discovery/features.py` - causal candle/OI feature snapshots and deterministic hard-negative matching.
- `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py` - artifact-root layout, Parquet/JSON writers/readers, immutable target freeze, and manifest updates.
- `apps/worker/src/quant_terminal_worker/signal_discovery/prompt.py` - deterministic training-only engine-builder prompt.
- `apps/worker/src/quant_terminal_worker/signal_discovery/evaluation.py` - candidate packet/strategy scoring against the frozen target.
- `apps/worker/src/quant_terminal_worker/signal_discovery/handoff.py` - compatible fixed-target Stage 0 artifact and candidate materialization.
- `apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx` - create/list/review/freeze/prompt/evaluate/handoff workspace.
- `tests/test_signal_discovery_atlas.py` - barrier, ambiguity, delay, episode, and split-leakage tests.
- `tests/test_signal_discovery_features.py` - causal availability and hard-negative matching tests.
- `tests/test_signal_discovery_workspace.py` - artifact and immutable-target tests.
- `tests/test_signal_discovery_prompt.py` - prompt allowlist/forbidden-path tests.
- `tests/test_signal_discovery_evaluation.py` - engine precision, coverage, direction, and parity tests.
- `tests/test_signal_discovery_handoff.py` - fixed-target compatibility artifacts and Stage 1 bridge tests.

**Modify:**

- `apps/api/src/quant_terminal_api/db/models.py` - `signal_discovery_sessions` table.
- `apps/api/src/quant_terminal_api/repositories/runtime.py` - discovery CRUD and state transitions.
- `apps/api/src/quant_terminal_api/main.py` - request models and lifecycle endpoints.
- `apps/worker/src/quant_terminal_worker/jobs.py` - atlas, WF, evaluation, and handoff job handlers.
- `apps/worker/src/quant_terminal_worker/job_routing.py` - research-queue routing.
- `apps/worker/src/quant_terminal_worker/stage2/capture_curve.py` - honor frozen fixed-R TP/SL/horizon metadata.
- `apps/worker/src/quant_terminal_worker/stage3/grid_search.py` - preserve fixed target as the baseline execution policy.
- `apps/web-v2/src/app/api.ts` - discovery types and API calls.
- `apps/web-v2/src/app/router.tsx` - `/research/discovery` route.
- `apps/web-v2/src/shell/SidebarNav.tsx` - Signal Discovery research navigation.
- `apps/web-v2/src/shell/TerminalShell.tsx` - render the discovery workspace.
- `apps/web-v2/src/styles/shell.css` - responsive discovery workbench layout.
- `skills/signal-engine-builder/SKILL.md` - outcome-first discovery prompt contract.
- `/Users/haokaiqin/.codex/skills/signal-engine-builder/SKILL.md` - installed skill parity after workspace verification.
- `tests/test_runtime_repository.py`, `tests/test_worker_jobs.py`, `tests/test_api.py` - persistence, jobs, and endpoints.
- `tests/test_stage2_capture.py`, `tests/test_stage3_grid.py` - fixed-target downstream compatibility.

### Task 1: Implement Executable Fixed-R Labels

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/__init__.py`
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/atlas.py`
- Test: `tests/test_signal_discovery_atlas.py`

- [x] **Step 1: Write failing first-touch tests**

Cover LONG target-first, LONG stop-first despite later MFE, SHORT target-first, NEUTRAL timeout, and same-candle AMBIGUOUS. Use this public interface:

```python
result = label_fixed_r_timestamp(
    candles=candles,
    decision_ts=_ts("2026-01-01T00:00:00Z"),
    entry_delay_minutes=5,
    risk_pct=1.0,
    reward_multiple=2.0,
    stop_multiple=1.0,
    horizon_hours=36,
)
assert result["label"] == "LONG"
assert result["long"]["outcome"] == "TP"
assert result["short"]["outcome"] == "SL"
assert result["entry_semantics"] == "next_5m_open"
```

- [x] **Step 2: Run tests and confirm the missing module failure**

Run: `pytest -q tests/test_signal_discovery_atlas.py`

Expected: collection fails because `quant_terminal_worker.signal_discovery.atlas` does not exist.

- [x] **Step 3: Implement causal entry and barrier ordering**

Add frozen dataclasses `DiscoveryConfig` and `FixedRLabel`, normalize UTC timestamps, choose the first candle whose open timestamp is at or after `decision_ts + entry_delay`, enter at that candle open, and scan only subsequent candle ranges through the configured cutoff. Return direction-specific TP/SL/timeout records, first-touch timestamps, MFE, MAE, terminal return, and one aggregate label. If both a direction's TP and SL occur in one 5m candle before either was previously touched, mark that direction and the aggregate label `AMBIGUOUS`.

- [x] **Step 4: Verify focused tests**

Run: `pytest -q tests/test_signal_discovery_atlas.py`

Expected: all fixed-R labeling tests pass.

- [x] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery tests/test_signal_discovery_atlas.py
git commit -m "feat: add executable fixed-r opportunity labels"
```

### Task 2: Build Opportunity Episodes and R Feasibility Summaries

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/signal_discovery/atlas.py`
- Test: `tests/test_signal_discovery_atlas.py`

- [x] **Step 1: Add failing episode and summary tests**

Test that consecutive 5m LONG labels form one episode, a NEUTRAL row closes it, the next LONG component gets a new episode id, and SHORT never joins LONG. Test that summaries report raw timestamps, independent episodes, direction counts, monthly recurrence, delay robustness, 36h/48h sensitivity, and cost-to-R.

- [x] **Step 2: Run the focused test**

Run: `pytest -q tests/test_signal_discovery_atlas.py -k 'episode or feasibility'`

Expected: failures for missing `build_opportunity_episodes` and `summarize_r_candidate`.

- [x] **Step 3: Implement episode and feasibility functions**

Implement stable public functions named `build_opportunity_episodes`,
`summarize_r_candidate`, and `run_training_atlas`. The episode builder groups only
adjacent 5m rows with the same LONG or SHORT label. The candidate summary accepts
the complete delay/horizon result map plus `risk_pct`, `fee_bps_per_side`, and
`slippage_bps_per_side`. The atlas runner accepts confirmed `MarketDataCandle`
rows and a validated `DiscoveryConfig`, then returns timestamp labels, episodes,
and one summary for every configured R value.

Only training timestamps may enter `run_training_atlas`; reject a timestamp on or after `walk_forward_start`. Select no winning `R` in this function: return the complete frontier and contiguous neighboring feasibility diagnostics.

- [x] **Step 4: Verify domain tests**

Run: `pytest -q tests/test_signal_discovery_atlas.py`

Expected: all label, episode, summary, and split-boundary tests pass.

- [x] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/atlas.py tests/test_signal_discovery_atlas.py
git commit -m "feat: summarize fixed-r opportunity episodes"
```

### Task 3: Build Causal Feature and Hard-Negative Evidence

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/features.py`
- Test: `tests/test_signal_discovery_features.py`

- [x] **Step 1: Write failing causal-feature tests**

Build fixture candles where future rows contain extreme values. Assert an episode-onset feature row includes only prior/available returns, ranges, realized volatility, volume z-score, and trend values. Add optional OI rows and assert 1h/4h/12h changes never use a row after the decision timestamp. Test deterministic hard negatives matched by month, UTC-hour block, and prior-volatility quintile.

- [x] **Step 2: Run focused tests**

Run: `pytest -q tests/test_signal_discovery_features.py`

Expected: missing feature module failure.

- [x] **Step 3: Implement causal snapshots and matching**

Implement `build_causal_feature_rows` with bounded prior windows for 1h/4h/12h/24h returns, 4h/24h realized volatility, 4h/24h high-low range, prior 7-day volume z-score, and optional 1h/4h/12h OI changes. Implement `select_hard_negatives` that deterministically chooses NEUTRAL timestamps from the same calendar month, six-hour UTC block, and prior-volatility quintile as episode timestamps. Exclude WF rows and all post-event outcomes from both artifacts.

- [x] **Step 4: Verify feature evidence**

Run: `pytest -q tests/test_signal_discovery_features.py`

Expected: point-in-time and deterministic matching tests pass.

- [x] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/features.py tests/test_signal_discovery_features.py
git commit -m "feat: build causal discovery evidence"
```

### Task 4: Materialize Discovery Artifacts and Freeze Targets

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py`
- Test: `tests/test_signal_discovery_workspace.py`

- [x] **Step 1: Write failing workspace tests**

Assert this artifact layout:

```text
dev/signal_discovery_sessions/<session_id>/
  manifest.json
  atlas/training_timestamp_labels.parquet
  atlas/training_episodes.parquet
  atlas/training_features.parquet
  atlas/training_hard_negatives.parquet
  atlas/r_feasibility.json
  target/frozen_target.json
  prompt/engine_builder_prompt.md
  evaluation/engine_evaluation.json
  handoff/stage0/
```

Test that freezing writes schema `signal_discovery_target.v1`, includes the source dataset id and split boundaries, stores a SHA-256 config hash, and raises `ValueError` on a second freeze with different contents.

- [x] **Step 2: Verify failure**

Run: `pytest -q tests/test_signal_discovery_workspace.py`

Expected: import failure for the missing workspace module.

- [x] **Step 3: Implement artifact writers/readers**

Use PyArrow for timestamp labels and episodes; JSON for compact summaries and contracts. Write target files atomically through a sibling `.tmp` file followed by `Path.replace`. Implement `materialize_training_atlas`, `freeze_target_contract`, `read_frozen_target`, `write_session_manifest`, and `discovery_artifact_root`.

- [x] **Step 4: Verify artifacts**

Run: `pytest -q tests/test_signal_discovery_workspace.py`

Expected: all artifact and immutability tests pass.

- [x] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/workspace.py tests/test_signal_discovery_workspace.py
git commit -m "feat: persist signal discovery artifacts"
```

### Task 5: Add Discovery Session Persistence

**Files:**
- Modify: `apps/api/src/quant_terminal_api/db/models.py`
- Modify: `apps/api/src/quant_terminal_api/repositories/runtime.py`
- Create: `db/migrations/versions/0029_signal_discovery_sessions.py`
- Test: `tests/test_runtime_repository.py`

- [x] **Step 1: Add failing repository tests**

Create a SQLite test session, list it, transition `draft -> atlas_ready -> target_frozen`, and reject changes to `config` or `frozen_target` after freeze. Test deletion and lookup by id.

- [x] **Step 2: Run repository tests**

Run: `pytest -q tests/test_runtime_repository.py -k signal_discovery`

Expected: failures because the table and repository methods are absent.

- [x] **Step 3: Add the table and repository API**

Add `signal_discovery_sessions` with columns: `session_id`, `name`, `asset`, `instrument`, `dataset_id`, `research_start`, `research_end`, `walk_forward_start`, `walk_forward_end`, `artifact_root`, `status`, `config`, `summary`, `frozen_target`, `target_version`, `candidate_engine_id`, `candidate_signal_set_key`, `evaluation`, `handoff`, `created_at`, and `updated_at`. Add repository methods `create_signal_discovery_session`, `list_signal_discovery_sessions`, `get_signal_discovery_session`, `update_signal_discovery_session`, and `delete_signal_discovery_session`.

- [x] **Step 4: Verify repository behavior**

Run: `pytest -q tests/test_runtime_repository.py -k signal_discovery`

Expected: all discovery repository tests pass on SQLite.

- [x] **Step 5: Commit**

```bash
git add apps/api/src/quant_terminal_api/db/models.py apps/api/src/quant_terminal_api/repositories/runtime.py tests/test_runtime_repository.py
git commit -m "feat: persist signal discovery sessions"
```

### Task 6: Add Atlas and Walk-Forward Jobs

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/jobs.py`
- Modify: `apps/worker/src/quant_terminal_worker/job_routing.py`
- Test: `tests/test_worker_jobs.py`

- [x] **Step 1: Add failing worker-job tests**

Register a raw 5m Parquet ref, create a discovery session, execute `signal_discovery_atlas`, and assert training artifacts exist while no WF labels exist. Freeze a target, execute `signal_discovery_walk_forward`, and assert WF artifacts use only the frozen risk percentage.

- [x] **Step 2: Run focused tests**

Run: `pytest -q tests/test_worker_jobs.py -k signal_discovery`

Expected: unsupported job type failures.

- [x] **Step 3: Implement job handlers**

Add research-queue routes for `signal_discovery_atlas`, `signal_discovery_walk_forward`, `signal_discovery_engine_evaluation`, and `signal_discovery_handoff`. Atlas jobs read canonical candles through `MarketDataReader`, extend the read end by the maximum horizon only for forward outcome availability, call the domain functions, write artifacts, and update session status. The WF handler must require `target_frozen` and use exactly one frozen `risk_pct`.

The atlas job must materialize all training-only evidence in one successful lifecycle transition: `training_timestamp_labels.parquet`, `training_episodes.parquet`, `training_features.parquet`, `training_hard_negatives.parquet`, and `r_feasibility.json`. If any artifact write fails, do not mark the session `atlas_ready`.

- [x] **Step 4: Verify job lifecycle**

Run: `pytest -q tests/test_worker_jobs.py -k signal_discovery`

Expected: training/WF jobs complete and enforce sealed split ordering.

- [x] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/jobs.py apps/worker/src/quant_terminal_worker/job_routing.py tests/test_worker_jobs.py
git commit -m "feat: run signal discovery research jobs"
```

### Task 7: Expose the Discovery API

**Files:**
- Modify: `apps/api/src/quant_terminal_api/main.py`
- Test: `tests/test_api.py`

- [x] **Step 1: Write failing API tests**

Test create/list/get/delete, atlas enqueue, freeze, prompt retrieval, attach candidate engine, evaluation enqueue, and handoff enqueue. Validate ordered split windows, nonempty R values, positive multiples, 36/48-style horizons, nonnegative costs, and immutable frozen targets.

- [x] **Step 2: Run focused API tests**

Run: `pytest -q tests/test_api.py -k signal_discovery`

Expected: 404 responses for missing routes.

- [x] **Step 3: Add request models and endpoints**

Add Pydantic models `SignalDiscoverySessionCreateRequest`, `SignalDiscoveryFreezeRequest`, `SignalDiscoveryCandidateRequest`, and endpoints under `/api/v1/research/signal-discovery-sessions`. Queue long-running work with scope `signal_discovery:<session_id>` and return the existing async job envelope. Freeze must read training feasibility, validate the selected R was in the configured grid, persist `target_version=1`, and never read WF artifacts.

- [x] **Step 4: Verify endpoints**

Run: `pytest -q tests/test_api.py -k signal_discovery`

Expected: all discovery API tests pass.

- [x] **Step 5: Commit**

```bash
git add apps/api/src/quant_terminal_api/main.py tests/test_api.py
git commit -m "feat: expose signal discovery sessions api"
```

### Task 8: Generate the Single Engine-Builder Prompt and Update the Skill

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/prompt.py`
- Modify: `skills/signal-engine-builder/SKILL.md`
- Test: `tests/test_signal_discovery_prompt.py`

- [ ] **Step 1: Write failing prompt tests**

Assert the prompt names `$signal-engine-builder`, the frozen target, training label/episode/feature/hard-negative paths, the engine registry and paired strategy destinations, and required evaluation. Assert it does not contain WF label paths, exact opportunity timestamps, or embedded outcome rows.

- [ ] **Step 2: Run prompt tests**

Run: `pytest -q tests/test_signal_discovery_prompt.py`

Expected: missing prompt module failure.

- [ ] **Step 3: Implement deterministic prompt generation**

Render a prompt that authorizes one agent to research, reject or implement, persist `engine_research_rationale.md`, build a neutral engine and paired strategy, run contract/parity tests, and identify its engine id for evaluation. Add an Outcome-First Discovery section to the workspace skill requiring train-only artifacts, episode-level evidence, direct target scoring, and rejection when no causal mechanism recurs.

- [ ] **Step 4: Verify prompt and skill content**

Run: `pytest -q tests/test_signal_discovery_prompt.py`

Expected: prompt allowlist and leakage tests pass.

- [ ] **Step 5: Commit the workspace version**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/prompt.py skills/signal-engine-builder/SKILL.md tests/test_signal_discovery_prompt.py
git commit -m "feat: generate outcome-first engine builder prompt"
```

Do not copy to the installed skill until the complete workspace test suite passes in Task 13.

### Task 9: Evaluate Registered Engine Candidates

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/evaluation.py`
- Modify: `apps/worker/src/quant_terminal_worker/jobs.py`
- Test: `tests/test_signal_discovery_evaluation.py`
- Test: `tests/test_worker_jobs.py`

- [ ] **Step 1: Write failing evaluator tests**

Use a deterministic fixture engine with known packet timestamps and a paired strategy. Assert opportunity precision, episode recall, LONG/SHORT/NEUTRAL counts, directional accuracy, net R after costs, train/WF slices, packet neutrality, strategy wrapper compatibility, and training/live cadence metadata parity.

- [ ] **Step 2: Run evaluator tests**

Run: `pytest -q tests/test_signal_discovery_evaluation.py tests/test_worker_jobs.py -k signal_discovery_engine`

Expected: missing evaluator failure.

- [ ] **Step 3: Implement candidate evaluation**

Resolve the registered engine through `resolve_signal_engine`, ensure its canonical signal set exists, generate/fill packets through the shared training runtime, load the paired base strategy from `code_ref.base_strategy_path`, wrap each packet canonically, and score each emitted timestamp directly from candles with the frozen target. Episode recall uses interval membership only as a diagnostic; primary precision and net R come from each emitted timestamp's own path.

- [ ] **Step 4: Verify candidate metrics**

Run: `pytest -q tests/test_signal_discovery_evaluation.py tests/test_worker_jobs.py -k signal_discovery_engine`

Expected: evaluator and queued job tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/evaluation.py apps/worker/src/quant_terminal_worker/jobs.py tests/test_signal_discovery_evaluation.py tests/test_worker_jobs.py
git commit -m "feat: evaluate engines against frozen discovery targets"
```

### Task 10: Bridge Accepted Engines into Existing Stage 1

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_discovery/handoff.py`
- Modify: `apps/worker/src/quant_terminal_worker/jobs.py`
- Test: `tests/test_signal_discovery_handoff.py`

- [ ] **Step 1: Write failing handoff tests**

Given an accepted evaluation, assert handoff creates a versioned Stage 0 run/candidate tied to the candidate signal set, materializes `scores/ground_truth/*.json` for the candidate's train and WF signals, writes `ground_truth_summary.json` with `label_contract: fixed_r_first_touch.v1`, marks the candidate accepted, and returns a candidate id usable by the existing Stage 1 create endpoint.

- [ ] **Step 2: Run handoff tests**

Run: `pytest -q tests/test_signal_discovery_handoff.py`

Expected: missing handoff module failure.

- [ ] **Step 3: Implement compatibility artifacts and repository writes**

Materialize only the frozen target labels for actual engine signal timestamps. Store `target_pct=risk_pct*reward_multiple`, `stop_pct=risk_pct*stop_multiple`, `forward_hours`, source discovery session id, config hash, and artifact paths in candidate metrics. Do not call legacy threshold calibration or the Stage 0A max-excursion gate.

- [ ] **Step 4: Verify Stage 1 compatibility**

Run: `pytest -q tests/test_signal_discovery_handoff.py tests/test_stage1_scoring.py`

Expected: the existing Stage 1 scorer consumes the generated labels and the new handoff tests pass.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/signal_discovery/handoff.py apps/worker/src/quant_terminal_worker/jobs.py tests/test_signal_discovery_handoff.py
git commit -m "feat: hand discovery engines into stage1"
```

### Task 11: Preserve Fixed-R Semantics in Stage 2 and Stage 3

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage2/capture_curve.py`
- Modify: `apps/worker/src/quant_terminal_worker/stage3/grid_search.py`
- Test: `tests/test_stage2_capture.py`
- Test: `tests/test_stage3_grid.py`

- [ ] **Step 1: Add failing fixed-target compatibility tests**

Test that Stage 2 reads `label_contract: fixed_r_first_touch.v1`, reports the frozen target and stop percentages, and emits a fixed shared exit policy instead of selecting a different base TP/SL. Test that Stage 3 includes the frozen configuration as its baseline and does not create TP/SL candidates outside the frozen target unless explicitly running protection-only variants.

- [ ] **Step 2: Run focused downstream tests**

Run: `pytest -q tests/test_stage2_capture.py tests/test_stage3_grid.py -k fixed_target`

Expected: current travel-derived policies fail the assertions.

- [ ] **Step 3: Implement fixed-target compatibility branches**

Add `_load_fixed_target_contract` to Stage 2 and reuse its target/stop/horizon. Mark artifacts with `policy_source: signal_discovery_fixed_target`; retain capture diagnostics without using them to replace the frozen policy. In Stage 3, seed the exact fixed TP/SL baseline and constrain local variants to protection behavior while preserving base target and stop.

- [ ] **Step 4: Verify downstream compatibility and regressions**

Run: `pytest -q tests/test_stage2_capture.py tests/test_stage3_grid.py`

Expected: fixed-target and legacy Stage 0 paths both pass.

- [ ] **Step 5: Commit**

```bash
git add apps/worker/src/quant_terminal_worker/stage2/capture_curve.py apps/worker/src/quant_terminal_worker/stage3/grid_search.py tests/test_stage2_capture.py tests/test_stage3_grid.py
git commit -m "feat: preserve discovery targets downstream"
```

### Task 12: Build the Signal Discovery Frontend

**Files:**
- Create: `apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx`
- Modify: `apps/web-v2/src/app/api.ts`
- Modify: `apps/web-v2/src/app/router.tsx`
- Modify: `apps/web-v2/src/shell/SidebarNav.tsx`
- Modify: `apps/web-v2/src/shell/TerminalShell.tsx`
- Modify: `apps/web-v2/src/styles/shell.css`

- [ ] **Step 1: Add typed discovery API contracts**

Define `SignalDiscoverySession`, `SignalDiscoveryConfig`, `SignalDiscoveryRResult`, `SignalDiscoveryTarget`, and lifecycle functions for create/list/get/delete/run-atlas/freeze/get-prompt/attach-candidate/evaluate/handoff. Use the existing `requestJson` and async job response types.

- [ ] **Step 2: Add the route and workspace shell**

Register `/research/discovery`, add `Signal Discovery` under R&D, and render `ResearchSignalDiscoveryPage`. Keep existing `/research/stage0` and `/research/development` behavior unchanged.

- [ ] **Step 3: Implement the dense operational workflow**

Build a split-pane page using existing `TerminalWorkbench`, `TerminalPanel`, `DataTable`, `StatusBadge`, `FieldRow`, modal, and job polling patterns. The left pane lists sessions. The right pane presents lifecycle actions and four unframed bands: Setup, R Feasibility, Frozen Target, and Engine Candidate. Use one `Generate Engine Builder Prompt` action after freeze; do not add a proposal prompt or automatic agent orchestration.

- [ ] **Step 4: Add responsive styling and build**

Add stable grid tracks, compact metric tables, non-overlapping modal fields, and mobile stacking under existing breakpoints. Run: `npm --workspace apps/web-v2 run build`.

Expected: TypeScript and Vite build pass.

- [ ] **Step 5: Commit**

```bash
git add apps/web-v2/src/app/api.ts apps/web-v2/src/app/router.tsx apps/web-v2/src/shell/SidebarNav.tsx apps/web-v2/src/shell/TerminalShell.tsx apps/web-v2/src/pages/ResearchSignalDiscoveryPage.tsx apps/web-v2/src/styles/shell.css
git commit -m "feat: add signal discovery workspace"
```

### Task 13: Full Verification, Installed Skill Sync, and Documentation

**Files:**
- Modify: `/Users/haokaiqin/.codex/skills/signal-engine-builder/SKILL.md`
- Modify: `docs/engine-strategy-contract.md`
- Modify: `docs/superpowers/specs/2026-07-11-fixed-r-outcome-first-signal-research-design.md`

- [ ] **Step 1: Run focused backend verification**

Run:

```bash
pytest -q \
  tests/test_signal_discovery_atlas.py \
  tests/test_signal_discovery_features.py \
  tests/test_signal_discovery_workspace.py \
  tests/test_signal_discovery_prompt.py \
  tests/test_signal_discovery_evaluation.py \
  tests/test_signal_discovery_handoff.py \
  tests/test_runtime_repository.py \
  tests/test_worker_jobs.py \
  tests/test_api.py \
  tests/test_stage1_scoring.py \
  tests/test_stage2_capture.py \
  tests/test_stage3_grid.py
```

Expected: all focused tests pass.

- [ ] **Step 2: Run broad static and regression checks**

Run:

```bash
ruff check apps/api/src apps/worker/src tests
pytest -q
npm --workspace apps/web-v2 run build
```

Expected: no new lint failures, all relevant regression tests pass, and the frontend builds.

- [ ] **Step 3: Sync and verify the installed skill**

After workspace tests pass, copy `skills/signal-engine-builder/SKILL.md` to `/Users/haokaiqin/.codex/skills/signal-engine-builder/SKILL.md`, then run `cmp` to prove byte parity. This external write requires approval.

- [ ] **Step 4: Run browser and end-to-end smoke verification**

With the user-managed stack running, verify desktop and mobile views in the in-app browser: create a small fixture session, run atlas, inspect feasibility, freeze target, open the generated prompt, attach a fixture engine, run evaluation, and hand off. Capture screenshots and confirm no overlaps, blank panels, clipped controls, or console errors. Verify the resulting candidate can create a Stage 1 session through the existing UI.

- [ ] **Step 5: Update docs and commit**

Document the discovery target contract, artifact roles, leakage boundary, engine prompt contract, and fixed-target downstream semantics. Update the concept spec's status to implemented only after the end-to-end smoke test succeeds.

```bash
git add docs/engine-strategy-contract.md docs/superpowers/specs/2026-07-11-fixed-r-outcome-first-signal-research-design.md
git commit -m "docs: document outcome-first signal discovery"
```

## Completion Audit

Before marking the goal complete, collect authoritative evidence for every Definition of Done item:

- API response and DB row prove session creation and immutable freeze.
- Training artifact listing proves executable labels, episodes, neighboring-R, delay, and cost metrics.
- Absence of WF label artifacts before freeze proves the leakage boundary.
- Frontend screenshots prove R feasibility, concentration, direction, recurrence, cost, and horizon review.
- Prompt test and rendered prompt prove training-only context plus the required skill contract.
- Candidate evaluation artifact proves precision, coverage, direct R outcomes, direction, parity, and WF slices.
- Handoff artifact, accepted Stage 0 candidate, and successfully created Stage 1 session prove no manual artifact surgery.
- Stage 2/3 tests prove the frozen target is not recalibrated downstream.
- Full test, lint, frontend build, and browser smoke outputs prove regression and UX quality.
