# BTC Outcome-First Signal Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Determine whether authorized BTC training evidence supports one stable causal event engine, then implement and verify exactly one candidate or document rejection.

**Architecture:** A disposable research harness reads only frozen training artifacts and manifest-authorized rows, constructs causal features, and evaluates deduped event leaves. A surviving rule is reimplemented in one worker adapter with a shared historical/live packet builder and one paired strategy; production code never reads discovery outcomes.

**Tech Stack:** Python 3, PyArrow, pandas/numpy where already available, pytest, Motis strategy SDK and worker runtime.

---

### Task 1: Validate Evidence Semantics

**Files:**
- Read: `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/target/frozen_target.json`
- Read: `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/evidence/evidence_manifest.json`
- Create or modify: `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/prompt/engine_research_rationale.md`

- [ ] Inspect schemas, timestamp ranges, confirmation semantics, and row counts without reading held-out artifacts.
- [ ] Verify every market source is filtered at `research_end` row-by-row.
- [ ] Record dataset ids, examined columns, availability semantics, and any rejected derived sources.

### Task 2: Research Causal Hypotheses

**Files:**
- Create temporary analysis code only under `/tmp`.
- Modify: `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/prompt/engine_research_rationale.md`

- [ ] Build episode-level and matched-negative feature summaries for compression/release, OI flush/trap, and candle-only sweep families.
- [ ] Evaluate broad thresholds across chronological blocks and months using final deduped streams.
- [ ] If needed, broaden to causal resampling, trends, z-scores, accelerations, and interactions while keeping bounded complexity.
- [ ] Reject brittle leaves; document perturbation, overlap, ablation, recurrence, cadence, and drought results.
- [ ] Decide build versus rejection without inspecting walk-forward evidence.

### Task 3: Write Candidate Contract Tests

**Files:**
- Create: `tests/test_btc_causal_regime_v1.py` if the build gate passes.
- Modify: `tests/test_api.py` only if catalog coverage requires a candidate-specific assertion.

- [ ] Add a failing registry/spec test for the selected engine id and strategy path.
- [ ] Add failing tests for neutral packet fields, reference prices, causal timestamps, deterministic generation, live parity, and seeded cadence.
- [ ] Add failing canonical-wrapper and paired-strategy tests using real training and live packets.
- [ ] Add a failing Stage 1 scorer compatibility test.
- [ ] Run focused tests and confirm failures are caused by the missing candidate implementation.

### Task 4: Implement One Candidate

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_engines/btc_causal_regime_v1.py`
- Create: `packages/strategy_modules/src/quant_terminal_strategies/btc_causal_regime_v1_base.py`
- Modify: `artifacts/signal_engine/engine_registry.json`

- [ ] Implement the minimal shared causal scanner and neutral packet builder needed by the tests.
- [ ] Implement historical and latest-confirmed-candle entrypoints with identical feature and event semantics.
- [ ] Implement the directional strategy against `context["signal"]["payload"]`.
- [ ] Add the registry entry without changing existing engine behavior or reverting user edits.
- [ ] Run focused tests until green, then refactor without changing behavior.

### Task 5: Direct Training Evaluation and Contract Verification

**Files:**
- Modify: `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/prompt/engine_research_rationale.md`

- [ ] Generate the final deduped training stream from canonical Parquet.
- [ ] Run packet contract audit on a representative emitted packet.
- [ ] Score coverage and direction directly against the frozen target using training labels only.
- [ ] Report episode precision/recall, timestamp coverage, hard-negative rate, expected net R, monthly/block stability, cadence, drought, duplicates, and per-leaf diagnostics.
- [ ] Run focused engine, evaluator, API catalog, and Stage 1 scorer tests.
- [ ] Run `pytest -q` and record any unrelated pre-existing failures separately.
- [ ] Verify `GET /api/v1/signal-engines` after restarting the backend when practical.
