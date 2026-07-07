# Stage 2/3 Walk-Forward Guardrails Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Stage 2 and Stage 3 use walk-forward evidence explicitly as a stability/ranking signal without turning walk-forward into an unconstrained overfit target.

**Architecture:** Stage 2 will keep training as the primary TP/SL band proposal source, then add explicit walk-forward and full-cycle guardrail metadata. Stage 3 will preserve the existing simulation engine, but change candidate ranking from aggregate-only to walk-forward-aware ranking with full-cycle robustness gates. Stage 4 inputs remain the same shape, with extra ranking diagnostics added for visibility.

**Tech Stack:** Python worker modules, JSON artifacts under `promotion/`, FastAPI existing endpoints, pytest.

---

## Current Behavior To Preserve

- Stage 2 reads `promotion/stage1a_canonical_full_cycle_scores.json`.
- Stage 2 profiles only executable Stage 1 `MATCH` decisions for TP/SL travel capture.
- Stage 2 writes:
  - `promotion/stage2_capture_curve.json`
  - `promotion/stage2_capture_per_signal.json`
  - `promotion/stage3_trade_inputs.json`
  - `promotion/stage2_summary.md`
- Stage 3 reads:
  - `promotion/stage2_capture_curve.json`
  - `promotion/stage2_exit_policy.json`
  - `promotion/stage3_trade_inputs.json`
- Stage 3 writes:
  - `promotion/stage3_grid_results.json`
  - `promotion/stage3_optimal.json`
  - `promotion/stage4_candidates.json`
  - `promotion/stage3_summary.md`

## Target Design

- Stage 2:
  - Training MATCH capture proposes TP bands.
  - Training MATCH adverse excursion proposes SL bands, still bounded by Stage 0 risk threshold.
  - Walk-forward capture is a stability guardrail, not the primary optimizer.
  - Full-cycle capture remains reported and can be used as a secondary robustness check.
- Stage 3:
  - Simulates all executable decisions as today.
  - Ranks primarily by walk-forward net PnL where walk-forward sample exists.
  - Rejects or demotes candidates with weak full-cycle behavior.
  - Keeps protected setups preferred when performance is close.
  - Emits ranking diagnostics so frontend and future agents can explain why a candidate won.

---

### Task 1: Add Stage 2 Training-Proposal / WF-Guardrail Artifact Fields

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage2/capture_curve.py`
- Modify: `tests/test_stage2_capture.py`

- [ ] **Step 1: Write a failing Stage 2 test for training proposal and WF guardrail fields**

Add this test to `tests/test_stage2_capture.py`:

```python
def test_stage2_recommends_tp_from_training_and_reports_walk_forward_guardrails(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps(
            {
                "records": [
                    {"signal_id": "train-1", "sample_role": "training", "decision_direction": "LONG", "agreement": "MATCH"},
                    {"signal_id": "train-2", "sample_role": "training", "decision_direction": "LONG", "agreement": "MATCH"},
                    {"signal_id": "wf-1", "sample_role": "walk_forward_test", "decision_direction": "LONG", "agreement": "MATCH"},
                ]
            }
        )
    )
    session = {
        "session_id": "stage1-aave",
        "artifact_root": str(artifact_root),
        "asset": "AAVE",
        "strategy_id": "aave-vegas",
        "strategy_version": "v0.1",
        "signal_engine_id": "vegas_ema",
        "signal_set_id": "AAVE-vegas_ema-canonical",
    }
    signal_rows = [
        {"signal_id": "train-1", "timestamp": "2026-05-01T00:00:00Z", "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}}},
        {"signal_id": "train-2", "timestamp": "2026-05-01T01:00:00Z", "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}}},
        {"signal_id": "wf-1", "timestamp": "2026-05-02T00:00:00Z", "payload": {"active_timeframes": ["5m"], "interactions": {"5m": [{"market_price": 100}]}}},
    ]
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 101.1, "low": 99.8, "close": 100.8},
        {"timestamp": "2026-05-01T01:05:00Z", "open": 100, "high": 101.2, "low": 99.7, "close": 101.0},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 100.6, "low": 99.6, "close": 100.4},
    ]

    result = run_stage2_capture_curve(
        workspace_root=tmp_path,
        session=session,
        signal_rows=signal_rows,
        candles=candles,
        tp_levels=[0.5, 1.0],
        forward_hours=1,
    )

    assert result["stage3_input"]["tp_range_source"] == "stage2_training_match_profile_with_walk_forward_guardrail"
    assert result["stage3_input"]["recommended_tp_max_pct"] == 1.0
    assert result["stage3_input"]["walk_forward_guardrail"]["0.5"]["status"] == "pass"
    assert result["stage3_input"]["walk_forward_guardrail"]["1.0"]["status"] == "fail"
    assert result["stage3_input"]["walk_forward_guardrail"]["1.0"]["reason"] == "walk_forward_capture_below_guardrail"
    assert result["stage3_input"]["selection_notes"]["primary_source"] == "training"
```

- [ ] **Step 2: Run the failing Stage 2 test**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage2_capture.py::test_stage2_recommends_tp_from_training_and_reports_walk_forward_guardrails -q
```

Expected: fail because `walk_forward_guardrail` and the new `tp_range_source` do not exist.

- [ ] **Step 3: Add Stage 2 constants and helper functions**

In `apps/worker/src/quant_terminal_worker/stage2/capture_curve.py`, add constants near the existing defaults:

```python
DEFAULT_STAGE2_TRAINING_MIN_MATCH_CAPTURE_PCT = 40.0
DEFAULT_STAGE2_WF_MIN_CAPTURE_PCT = 40.0
DEFAULT_STAGE2_WF_MAX_TRAINING_GAP_PCT = 35.0
DEFAULT_STAGE2_FULL_CYCLE_MIN_CAPTURE_PCT = 20.0
```

Add these helpers below `_recommended_tp_max`:

```python
def _recommended_tp_max_from_training(*, tp_levels: list[float], results: dict[str, dict[str, dict[str, float | int]]]) -> float:
    recommended: float | None = None
    for level in tp_levels:
        key = f"{level:.1f}"
        training_rate = float(results.get(key, {}).get("training", {}).get("rate", 0.0))
        if training_rate >= DEFAULT_STAGE2_TRAINING_MIN_MATCH_CAPTURE_PCT:
            recommended = level
    return round(recommended if recommended is not None else DEFAULT_STAGE3_FALLBACK_TP_MAX_PCT, 1)


def _walk_forward_guardrail(*, tp_levels: list[float], results: dict[str, dict[str, dict[str, float | int]]]) -> dict[str, dict[str, Any]]:
    guardrail: dict[str, dict[str, Any]] = {}
    for level in tp_levels:
        key = f"{level:.1f}"
        rows = results.get(key, {})
        training = rows.get("training", {})
        walk_forward = rows.get("walk_forward_test", {})
        full_cycle = rows.get("full_cycle", {})
        training_rate = float(training.get("rate", 0.0))
        wf_total = int(walk_forward.get("total", 0))
        wf_rate = float(walk_forward.get("rate", 0.0))
        full_cycle_rate = float(full_cycle.get("rate", 0.0))
        status = "pass"
        reason = "walk_forward_capture_stable"
        if wf_total == 0:
            status = "insufficient_sample"
            reason = "missing_walk_forward_match_sample"
        elif wf_rate < DEFAULT_STAGE2_WF_MIN_CAPTURE_PCT:
            status = "fail"
            reason = "walk_forward_capture_below_guardrail"
        elif training_rate - wf_rate > DEFAULT_STAGE2_WF_MAX_TRAINING_GAP_PCT:
            status = "fail"
            reason = "walk_forward_capture_collapsed_vs_training"
        elif full_cycle_rate < DEFAULT_STAGE2_FULL_CYCLE_MIN_CAPTURE_PCT:
            status = "fail"
            reason = "full_cycle_capture_below_guardrail"
        guardrail[key] = {
            "status": status,
            "reason": reason,
            "training_rate": training_rate,
            "walk_forward_rate": wf_rate,
            "walk_forward_total": wf_total,
            "full_cycle_rate": full_cycle_rate,
        }
    return guardrail
```

- [ ] **Step 4: Wire Stage 2 `stage3_input` to training proposal and guardrail metadata**

In `_build_result`, replace:

```python
recommended_tp_max = _recommended_tp_max(tp_levels=tp_levels, cohorts=cohorts)
```

with:

```python
recommended_tp_max = _recommended_tp_max_from_training(tp_levels=tp_levels, results=results)
tp_guardrail = _walk_forward_guardrail(tp_levels=tp_levels, results=results)
```

In the returned `stage3_input`, replace:

```python
"description": "Use this MATCH-only travel profile to narrow Stage 3 TP/SL/management grids on the frozen Stage 1 decision set.",
"tp_range_source": "stage2_trade_profile",
```

with:

```python
"description": "Use training MATCH travel to propose TP/SL bands, with walk-forward and full-cycle capture retained as guardrails.",
"tp_range_source": "stage2_training_match_profile_with_walk_forward_guardrail",
```

Add these fields inside `stage3_input`:

```python
"walk_forward_guardrail": tp_guardrail,
"selection_notes": {
    "primary_source": "training",
    "validation_source": "walk_forward_test",
    "robustness_source": "full_cycle",
    "walk_forward_policy": "guardrail_not_primary_optimizer",
},
```

- [ ] **Step 5: Run Stage 2 tests**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage2_capture.py -q
```

Expected: all Stage 2 tests pass after updating assertions that expected the old `tp_range_source`.

- [ ] **Step 6: Commit Stage 2 artifact semantics**

Run:

```bash
git add apps/worker/src/quant_terminal_worker/stage2/capture_curve.py tests/test_stage2_capture.py
git commit -m "feat: add stage2 walk-forward guardrails"
```

---

### Task 2: Validate Stage 2 Exit Policy Against WF Guardrail Without Blocking Manual Overrides

**Files:**
- Modify: `apps/api/src/quant_terminal_api/main.py`
- Modify: `tests/test_api.py`

- [ ] **Step 1: Write a failing API test for guardrail warnings**

Add this test near the Stage 2 exit policy tests in `tests/test_api.py`:

```python
def test_stage2_exit_policy_reports_walk_forward_guardrail_warning(tmp_path, monkeypatch):
    session = _stage1_session(
        session_id="stage1-aave",
        artifact_root=str(tmp_path / "dev/training_sessions/aave/stage1-aave"),
        status="stage1a_frozen",
    )
    promotion_root = Path(session["artifact_root"]) / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps(
            {
                "tp_levels": [0.5, 1.0],
                "sl_levels": [0.5, 1.0],
                "results": {"0.5": {}, "1.0": {}},
                "sl_results": {"0.5": {}, "1.0": {}},
                "stage3_input": {
                    "walk_forward_guardrail": {
                        "1.0": {
                            "status": "fail",
                            "reason": "walk_forward_capture_below_guardrail",
                            "training_rate": 100.0,
                            "walk_forward_rate": 0.0,
                            "walk_forward_total": 2,
                            "full_cycle_rate": 66.7,
                        }
                    }
                },
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2 Travel Capture\n")
    repository = FakeRuntimeRepository()
    repository.stage1_sessions = {"stage1-aave": session}
    monkeypatch.setattr("quant_terminal_api.main.get_runtime_repository", lambda: repository)
    monkeypatch.chdir(tmp_path)

    response = client.post(
        "/api/v1/research/stage1-sessions/stage1-aave/stage2/exit-policy",
        json={
            "lock_profit_pct": 1.0,
            "initial_sl_pct": 0.5,
            "protect_trigger_pct": 0.5,
            "trail_sl_pct": 0.5,
        },
    )

    assert response.status_code == 200
    warning = response.json()["stage2_exit_policy"]["source"]["walk_forward_guardrail"]
    assert warning["status"] == "fail"
    assert warning["reason"] == "walk_forward_capture_below_guardrail"
```

- [ ] **Step 2: Run the failing API test**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_api.py::test_stage2_exit_policy_reports_walk_forward_guardrail_warning -q
```

Expected: fail because the Stage 2 policy source does not include `walk_forward_guardrail`.

- [ ] **Step 3: Add the guardrail source to Stage 2 exit policy**

In `apps/api/src/quant_terminal_api/main.py`, inside `promote_stage2_exit_policy`, after `selected = side_policies["LONG"]`, add:

```python
        guardrail_by_tp = ((capture.get("stage3_input") or {}).get("walk_forward_guardrail") or {})
        selected_tp_key = f"{float(selected['lock_profit_pct']):.1f}"
        selected_guardrail = guardrail_by_tp.get(selected_tp_key, {})
```

Then update the `source` object in `policy` from:

```python
"selection_source": "stage2_capture_curve_tp_and_sl_bands",
```

to:

```python
"selection_source": "stage2_capture_curve_tp_and_sl_bands",
"walk_forward_guardrail": selected_guardrail,
```

- [ ] **Step 4: Run API tests around Stage 2 policy**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_api.py -q
```

Expected: pass.

- [ ] **Step 5: Commit Stage 2 policy diagnostics**

Run:

```bash
git add apps/api/src/quant_terminal_api/main.py tests/test_api.py
git commit -m "feat: report stage2 walk-forward policy guardrails"
```

---

### Task 3: Add Stage 3 WF-Aware Ranking Helper

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage3/grid_search.py`
- Modify: `tests/test_stage3_grid.py`

- [ ] **Step 1: Write a failing Stage 3 ranking test**

Add this test to `tests/test_stage3_grid.py`:

```python
def test_stage3_ranking_prefers_walk_forward_when_full_cycle_is_viable(tmp_path: Path):
    promotion_root = _stage3_fixture_with_stage2_policy(tmp_path)
    _write_stage3_trade_inputs(
        promotion_root,
        [
            {"signal_id": "train-a", "sample_role": "training", "direction": "LONG", "agreement": "MATCH", "signal_ts": "2026-05-01T00:00:00Z", "reference_price": 100},
            {"signal_id": "wf-a", "sample_role": "walk_forward_test", "direction": "LONG", "agreement": "MATCH", "signal_ts": "2026-05-02T00:00:00Z", "reference_price": 100},
        ],
    )
    session = _stage1_session(tmp_path)
    candles = [
        {"timestamp": "2026-05-01T00:05:00Z", "open": 100, "high": 103.0, "low": 99.8, "close": 102.0},
        {"timestamp": "2026-05-02T00:05:00Z", "open": 100, "high": 101.0, "low": 99.8, "close": 100.8},
    ]

    result = run_stage3_local_variants(
        workspace_root=tmp_path,
        session=session,
        candles=candles,
        tp_values=[1.0, 3.0],
        forward_hours=1,
        leverage=1,
        fees_bps_per_side=0,
    )

    best = result["optimal"]["best"]
    assert result["optimal"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
    assert best["ranking_diagnostics"]["walk_forward_viable"] is True
    assert best["ranking_diagnostics"]["full_cycle_viable"] is True
    assert best["slice_split"]["walk_forward_test"]["net_pnl_pct"] >= 1.0
```

If existing test helper names differ, create minimal local helpers in the test file following current fixtures:

```python
def _write_stage3_trade_inputs(promotion_root: Path, rows: list[dict[str, Any]]) -> None:
    (promotion_root / "stage3_trade_inputs.json").write_text(json.dumps(rows))
```

- [ ] **Step 2: Run the failing Stage 3 ranking test**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage3_grid.py::test_stage3_ranking_prefers_walk_forward_when_full_cycle_is_viable -q
```

Expected: fail because current criterion is aggregate-only.

- [ ] **Step 3: Add ranking diagnostics helpers**

In `apps/worker/src/quant_terminal_worker/stage3/grid_search.py`, add constants near the defaults:

```python
DEFAULT_STAGE3_MIN_FULL_CYCLE_NET_PNL_PCT = 0.0
DEFAULT_STAGE3_MIN_FULL_CYCLE_PROFIT_FACTOR = 1.0
DEFAULT_STAGE3_WF_CLOSE_NET_PNL_TOLERANCE_PCT = 1.0
```

Add these helpers near `_ranking_key`:

```python
def _stage3_ranking_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    slices = row.get("slice_split") or {}
    wf = slices.get("walk_forward_test") or {}
    full_cycle_net = float(row.get("net_pnl_pct", 0.0))
    full_cycle_pf = float(row.get("profit_factor", 0.0))
    wf_total = int(wf.get("total", 0))
    wf_net = float(wf.get("net_pnl_pct", 0.0))
    wf_pf = float(wf.get("profit_factor", 0.0))
    return {
        "walk_forward_total": wf_total,
        "walk_forward_net_pnl_pct": wf_net,
        "walk_forward_profit_factor": wf_pf,
        "walk_forward_viable": wf_total > 0 and wf_net > 0 and wf_pf >= 1.0,
        "full_cycle_net_pnl_pct": full_cycle_net,
        "full_cycle_profit_factor": full_cycle_pf,
        "full_cycle_viable": full_cycle_net >= DEFAULT_STAGE3_MIN_FULL_CYCLE_NET_PNL_PCT and full_cycle_pf >= DEFAULT_STAGE3_MIN_FULL_CYCLE_PROFIT_FACTOR,
        "protection_enabled": bool(row.get("protection_enabled")),
    }


def _with_stage3_ranking_diagnostics(row: dict[str, Any]) -> dict[str, Any]:
    return {**row, "ranking_diagnostics": _stage3_ranking_diagnostics(row)}


def _walk_forward_ranking_key(row: dict[str, Any]) -> tuple[Any, ...]:
    diagnostics = row.get("ranking_diagnostics") or _stage3_ranking_diagnostics(row)
    wf_total = int(diagnostics["walk_forward_total"])
    wf_net = float(diagnostics["walk_forward_net_pnl_pct"])
    wf_pf = float(diagnostics["walk_forward_profit_factor"])
    full_cycle_net = float(diagnostics["full_cycle_net_pnl_pct"])
    full_cycle_pf = float(diagnostics["full_cycle_profit_factor"])
    if wf_total <= 0:
        return _ranking_key(row)
    return (
        bool(diagnostics["full_cycle_viable"]),
        bool(diagnostics["walk_forward_viable"]),
        wf_net,
        wf_pf,
        full_cycle_net,
        full_cycle_pf,
        bool(row.get("protection_enabled")),
        float(row.get("wr", 0.0)),
        -float(row.get("initial_sl_pct", 0.0)),
    )
```

- [ ] **Step 4: Apply diagnostics before sorting Stage 3 local variants**

In `run_stage3_local_variants`, replace:

```python
ranked = sorted([row for row in [fixed_result, exact_result, *local_results] if row], key=_ranking_key, reverse=True)
```

with:

```python
rankable = [_with_stage3_ranking_diagnostics(row) for row in [fixed_result, exact_result, *local_results] if row]
ranked = sorted(rankable, key=_walk_forward_ranking_key, reverse=True)
```

Replace the `optimal.criterion` string with:

```python
"criterion": "walk_forward_net_pnl_with_full_cycle_guardrails",
```

- [ ] **Step 5: Run Stage 3 grid tests**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage3_grid.py -q
```

Expected: pass after updating assertions that expected the old criterion.

- [ ] **Step 6: Commit Stage 3 ranking**

Run:

```bash
git add apps/worker/src/quant_terminal_worker/stage3/grid_search.py tests/test_stage3_grid.py
git commit -m "feat: rank stage3 by walk-forward with guardrails"
```

---

### Task 4: Ensure Stage 4 Candidate Handoff Carries Ranking Diagnostics

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage3/grid_search.py`
- Modify: `tests/test_stage3_grid.py`

- [ ] **Step 1: Write a failing handoff test**

Add this assertion to the Stage 3 local variants test that verifies `stage4_candidates`:

```python
candidate = result["stage4_candidates"]["candidates"][0]
assert candidate["selection_diagnostics"]["stage3_ranking"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
assert "walk_forward_net_pnl_pct" in candidate["selection_diagnostics"]["stage3_ranking"]
assert "full_cycle_net_pnl_pct" in candidate["selection_diagnostics"]["stage3_ranking"]
```

- [ ] **Step 2: Run the failing Stage 3 handoff test**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage3_grid.py -q
```

Expected: fail because candidates do not yet carry `selection_diagnostics.stage3_ranking`.

- [ ] **Step 3: Add selection diagnostics to Stage 4 candidates**

In `_build_stage4_candidates`, locate where each candidate dict is built. Add:

```python
"selection_diagnostics": {
    "stage3_ranking": {
        "criterion": "walk_forward_net_pnl_with_full_cycle_guardrails",
        **(record.get("ranking_diagnostics") or {}),
    }
},
```

Keep all existing candidate fields unchanged.

- [ ] **Step 4: Run Stage 3 and Stage 4 smoke tests**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage3_grid.py tests/test_stage4_realized_expectancy.py -q
```

Expected: pass.

- [ ] **Step 5: Commit Stage 4 candidate diagnostics**

Run:

```bash
git add apps/worker/src/quant_terminal_worker/stage3/grid_search.py tests/test_stage3_grid.py
git commit -m "feat: carry stage3 ranking diagnostics to stage4 candidates"
```

---

### Task 5: Gate Summary And Frontend Visibility

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/stage1/workspace.py`
- Modify: `apps/web-v2/src/pages/ResearchDevelopmentPage.tsx`
- Modify: `tests/test_stage1_workspace.py`

- [ ] **Step 1: Write a failing gate summary test**

Add to `tests/test_stage1_workspace.py`:

```python
def test_build_stage1_gate_summary_exposes_stage2_wf_guardrails_and_stage3_ranking(tmp_path: Path):
    artifact_root = tmp_path / "dev/training_sessions/aave/stage1-aave"
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True)
    (promotion_root / "stage2_capture_curve.json").write_text(
        json.dumps(
            {
                "metrics": {"total_match_signals": 2},
                "stage3_input": {
                    "tp_range_source": "stage2_training_match_profile_with_walk_forward_guardrail",
                    "walk_forward_guardrail": {"1.0": {"status": "pass"}},
                },
            }
        )
    )
    (promotion_root / "stage2_capture_per_signal.json").write_text("[]")
    (promotion_root / "stage3_trade_inputs.json").write_text("[]")
    (promotion_root / "stage2_summary.md").write_text("# Stage 2\n")
    (promotion_root / "stage3_grid_results.json").write_text(
        json.dumps({"optimal": {"criterion": "walk_forward_net_pnl_with_full_cycle_guardrails", "best": {"ranking_diagnostics": {"walk_forward_net_pnl_pct": 12.3}}}})
    )
    session = {"session_id": "stage1-aave", "artifact_root": str(artifact_root)}

    gate = build_stage1_gate_summary(workspace_root=tmp_path, session=session)

    assert gate["stage2_capture"]["stage3_input"]["tp_range_source"] == "stage2_training_match_profile_with_walk_forward_guardrail"
    assert gate["stage3_grid"]["optimal"]["criterion"] == "walk_forward_net_pnl_with_full_cycle_guardrails"
```

- [ ] **Step 2: Run the failing workspace test**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage1_workspace.py::test_build_stage1_gate_summary_exposes_stage2_wf_guardrails_and_stage3_ranking -q
```

Expected: fail if the gate summary does not expose these nested fields.

- [ ] **Step 3: Expose the new fields in the gate summary**

In `_stage2_capture_state` in `apps/worker/src/quant_terminal_worker/stage1/workspace.py`, include:

```python
"stage3_input": capture.get("stage3_input", {}) if capture else {},
```

If `stage3_grid` state already exposes `optimal`, keep it unchanged. If not, add:

```python
"optimal": grid.get("optimal", {}) if grid else {},
```

- [ ] **Step 4: Add compact frontend labels**

In `apps/web-v2/src/pages/ResearchDevelopmentPage.tsx`, update the Stage 2 panel to show:

```tsx
<span>TP source: {stage2.stage3_input?.tp_range_source ?? "capture curve"}</span>
```

and the Stage 3 panel to show:

```tsx
<span>Ranking: {stage3.optimal?.criterion ?? "not selected"}</span>
```

Keep this display compact; do not add new backend calls.

- [ ] **Step 5: Run frontend build and workspace tests**

Run:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage1_workspace.py -q
npm --workspace apps/web-v2 run build
```

Expected: both pass.

- [ ] **Step 6: Commit visibility update**

Run:

```bash
git add apps/worker/src/quant_terminal_worker/stage1/workspace.py apps/web-v2/src/pages/ResearchDevelopmentPage.tsx tests/test_stage1_workspace.py
git commit -m "feat: expose stage2 stage3 wf guardrails"
```

---

## Verification Plan

Run the focused backend suite:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src pytest tests/test_stage2_capture.py tests/test_stage3_grid.py tests/test_stage3_pyramid.py tests/test_stage4_realized_expectancy.py tests/test_api.py tests/test_stage1_workspace.py -q
```

Run the frontend build:

```bash
npm --workspace apps/web-v2 run build
```

Known unrelated full-suite issues from the previous run should not be solved inside this plan:

- `tests/test_dev_stack_scripts.py::test_stop_dev_stack_uses_pid_files_and_does_not_grep_processes`
- `tests/test_engine_strategy_contracts.py::test_current_aave_execution_bundle_validates_with_legacy_aliases`

---

## Self-Review

- Stage 2 uses training as proposal source and walk-forward as guardrail.
- Stage 2 exit policy remains manually selectable but reports WF guardrail status.
- Stage 3 ranking becomes WF-aware with full-cycle guardrails.
- Stage 4 candidate shape remains compatible and only gains diagnostics.
- Frontend visibility uses existing gate payloads and does not add backend endpoints.
