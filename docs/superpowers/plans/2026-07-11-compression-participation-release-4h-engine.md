# Compression Participation Release 4h Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a separately selectable Compression Participation Release engine whose 240-minute cadence cannot be overridden and is preserved by manual and live canonical-pool fills.

**Architecture:** Add a thin four-hour adapter over the existing causal event scanner, plus a generic fixed-parameter merge at runtime call sites. Keep the existing eight-hour adapter backward compatible, and enforce cadence both before generation and at the canonical persistence boundary.

**Tech Stack:** Python 3, dataclasses, Pytest, Ruff, JSON engine registry, Motis signal-engine SDK/runtime.

---

### Task 1: Fixed Engine Parameters

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/signal_engines/runtime.py`
- Modify: `apps/worker/src/quant_terminal_worker/ingestion/signal_pool_extension.py`
- Modify: `apps/worker/src/quant_terminal_worker/execution/live_signal_scan.py`
- Test: `tests/test_signal_pool_extension.py`
- Test: `tests/test_signal_engine_runtime.py`

- [ ] **Step 1: Write failing tests for fixed-parameter precedence**

Add tests that construct a fake spec with both defaults and fixed parameters, then assert a conflicting manifest or route value is replaced:

```python
class FakeSpec:
    configuration_schema = {
        "default_parameters": {"dedupe_window_minutes": 480},
        "fixed_parameters": {"dedupe_window_minutes": 240},
    }

assert apply_fixed_engine_parameters(
    FakeSpec(), {"dedupe_window_minutes": 0}
) == {"dedupe_window_minutes": 240}
```

In the extension test, make the fake generator emit packets 180 and 240 minutes after the prior canonical signal while the manifest requests `480`; assert only the 240-minute packet is admitted and the generator context receives `240`.

- [ ] **Step 2: Run the tests and verify RED**

Run:

```bash
pytest -q tests/test_signal_pool_extension.py -k fixed_parameters tests/test_signal_engine_runtime.py -k fixed_parameters
```

Expected: failure because `apply_fixed_engine_parameters` does not exist and the extension currently honors manifest cadence.

- [ ] **Step 3: Add the fixed-parameter helper and apply it at both runtime boundaries**

Add to `runtime.py`:

```python
def apply_fixed_engine_parameters(spec: SignalEngineSpec, parameters: dict[str, Any]) -> dict[str, Any]:
    merged = dict(parameters)
    schema = spec.configuration_schema if isinstance(spec.configuration_schema, dict) else {}
    fixed = schema.get("fixed_parameters")
    if isinstance(fixed, dict):
        merged.update(fixed)
    return merged
```

Call it in `signal_pool_extension.py` after merging generic, spec-default, and manifest parameters. Call it in `live_signal_scan.py` after merging spec defaults and execution-bundle engine parameters.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the command from Step 2. Expected: all selected tests pass.

### Task 2: Shared Scanner Identity Hook

**Files:**
- Modify: `apps/worker/src/quant_terminal_worker/signal_engines/compression_participation_release_v1.py`
- Test: `tests/test_compression_participation_release_engine.py`

- [ ] **Step 1: Write a failing identity-preservation test**

Generate one fixture packet with `engine_id="compression_participation_release_4h_v1"` and assert:

```python
assert packet["evidence"]["engine"] == "compression_participation_release_4h_v1"
assert packet["evidence"]["dedupe_window_minutes"] == 240
```

Also retain the existing assertion that calls without `engine_id` emit `compression_participation_release_v1`.

- [ ] **Step 2: Run the test and verify RED**

Run:

```bash
pytest -q tests/test_compression_participation_release_engine.py -k engine_identity
```

Expected: failure because the scanner does not accept an engine id.

- [ ] **Step 3: Thread an optional engine id through shared packet construction**

Add keyword-only `engine_id: str = ENGINE_ID` to `generate_compression_participation_packets`, `scan_compression_participation_latest`, and `_scan_index`. Pass it through both call paths and use it for `evidence["engine"]`. Do not change any default threshold or the existing engine's `DEFAULT_DEDUPE_WINDOW_MINUTES = 480`.

- [ ] **Step 4: Run the identity and existing engine tests**

Run:

```bash
pytest -q tests/test_compression_participation_release_engine.py
```

Expected: all tests pass, including the existing eight-hour seed test.

### Task 3: Four-Hour Engine Adapter And Registry

**Files:**
- Create: `apps/worker/src/quant_terminal_worker/signal_engines/compression_participation_release_4h_v1.py`
- Modify: `artifacts/signal_engine/engine_registry.json`
- Test: `tests/test_compression_participation_release_4h_engine.py`
- Test: `tests/test_api.py`

- [ ] **Step 1: Write failing adapter and registry tests**

Tests must assert:

```python
ENGINE_ID = "compression_participation_release_4h_v1"
validate_signal_engine_spec(ENGINE_ID)
assert resolved.spec.configuration_schema["default_parameters"]["dedupe_window_minutes"] == 240
assert resolved.spec.configuration_schema["fixed_parameters"]["dedupe_window_minutes"] == 240
assert engine._enforced_parameters({"dedupe_window_minutes": 0})["dedupe_window_minutes"] == 240
assert engine._enforced_parameters({"dedupe_window_minutes": 480})["dedupe_window_minutes"] == 240
```

Using the existing fixture data, exercise both runtime entrypoints and assert training/live packets identify the new engine, report cadence 240, validate as neutral packets, and produce scoreable base-strategy decisions through the canonical wrapper.

- [ ] **Step 2: Run tests and verify RED**

Run:

```bash
pytest -q tests/test_compression_participation_release_4h_engine.py tests/test_api.py -k compression_participation_release_4h
```

Expected: import or registry lookup failure because the new engine does not exist.

- [ ] **Step 3: Implement the thin adapter**

The module defines:

```python
ENGINE_ID = "compression_participation_release_4h_v1"
DEDUPE_WINDOW_MINUTES = 240

def _enforced_parameters(parameters: dict[str, Any] | None) -> dict[str, Any]:
    return {**dict(parameters or {}), "dedupe_window_minutes": DEDUPE_WINDOW_MINUTES}
```

Implement `generate_training_signals` and `scan_live_signal` with the same reader calls and result contracts as the existing adapter. Delegate to shared generation/scanning functions with enforced parameters and `engine_id=ENGINE_ID`.

- [ ] **Step 4: Register the engine**

Add a registry entry with the same required data, base strategy path, thresholds, and context defaults as the existing engine, but use the new adapter entrypoints and include:

```json
"default_parameters": {"dedupe_window_minutes": 240},
"fixed_parameters": {"dedupe_window_minutes": 240}
```

Keep every other threshold identical to the eight-hour engine.

- [ ] **Step 5: Run adapter, registry, and API tests and verify GREEN**

Run the command from Step 2. Expected: all selected tests pass and the catalog reports the registry-only engine with zero pool and packet counts.

### Task 4: Manual And Live Fill Contract

**Files:**
- Test: `tests/test_api.py`
- Test: `tests/test_execution_lifecycle.py`
- Test: `tests/test_signal_pool_extension.py`

- [ ] **Step 1: Add manual API selection coverage**

Invoke `/api/v1/signal-engines/compression_participation_release_4h_v1/signal-sets/BTC/extend-local` with a stub service and assert the service receives the new engine id and BTC asset.

- [ ] **Step 2: Add live lifecycle selection coverage**

Configure the fake route with `signal_engine_id="compression_participation_release_4h_v1"`, run one lifecycle cycle, and assert its signal-pool extender receives that engine id with `target_end=None`.

- [ ] **Step 3: Run manual and live fill tests**

Run:

```bash
pytest -q tests/test_api.py tests/test_execution_lifecycle.py tests/test_signal_pool_extension.py -k 'compression_participation_release_4h or fixed_parameters'
```

Expected: all selected tests pass, proving both callers converge on the fixed-parameter extension path.

### Task 5: Contract Audit And Regression Verification

**Files:**
- Verify: `apps/worker/src/quant_terminal_worker/signal_engines/compression_participation_release_4h_v1.py`
- Verify: `artifacts/signal_engine/engine_registry.json`

- [ ] **Step 1: Run focused Python tests**

```bash
pytest -q tests/test_compression_participation_release_engine.py tests/test_compression_participation_release_4h_engine.py tests/test_signal_pool_extension.py tests/test_execution_lifecycle.py
```

Expected: zero failures.

- [ ] **Step 2: Run engine/runtime and API regressions**

```bash
pytest -q tests/test_signal_engine_runtime.py tests/test_api.py
```

Expected: zero failures.

- [ ] **Step 3: Run lint on changed Python files**

```bash
ruff check apps/worker/src/quant_terminal_worker/signal_engines/runtime.py apps/worker/src/quant_terminal_worker/signal_engines/compression_participation_release_v1.py apps/worker/src/quant_terminal_worker/signal_engines/compression_participation_release_4h_v1.py apps/worker/src/quant_terminal_worker/ingestion/signal_pool_extension.py apps/worker/src/quant_terminal_worker/execution/live_signal_scan.py tests/test_compression_participation_release_4h_engine.py tests/test_compression_participation_release_engine.py tests/test_signal_pool_extension.py tests/test_execution_lifecycle.py
```

Expected: `All checks passed!`

- [ ] **Step 4: Generate and audit a representative packet**

Use the fixture-backed test helper to serialize one emitted packet to a temporary JSON file, then run:

```bash
python /Users/haokaiqin/.codex/skills/signal-engine-builder/scripts/audit_signal_packet_contract.py --packet /tmp/compression_participation_release_4h_packet.json
```

Expected: packet contract audit passes with standard reference-price and candle-availability fields.

- [ ] **Step 5: Verify the API catalog if the local backend is running**

```bash
curl -sS http://127.0.0.1:8000/api/v1/signal-engines
```

Expected: the response includes `compression_participation_release_4h_v1`. Restart the backend only if it is healthy but still serving the old registry.

No production BTC canonical pool is created or backfilled by this plan.
