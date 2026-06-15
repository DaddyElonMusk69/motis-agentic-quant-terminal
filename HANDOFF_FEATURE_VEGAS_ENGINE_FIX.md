# Handoff: Fix `vegas_ema_recursive_features` Engine Shape

## Current User Intent

The feature-backed Vegas engine is wrong and should be rebuilt in a simpler shape.

The desired engine is not a new signal-discovery design and should not delegate signal discovery to another engine as a black box. It should be a direct duplicate of the existing working 5m Vegas engine, with one extra step at signal emission time: append derived feature rows to the packet.

Target shape:

```text
vegas_ema_recursive_features
= exact copy of old 5m Vegas signal engine behavior
+ append feature rows at packet emission
```

## Root Finding

Current file:

- `apps/worker/src/quant_terminal_worker/signal_engines/vegas_ema_recursive_features.py`

It currently imports:

```python
from quant_terminal_worker.signal_engines import vegas_ema_recursive
```

and calls:

```python
vegas_ema_recursive.generate_recursive_vegas_packets(...)
vegas_ema_recursive.scan_recursive_vegas_at(...)
```

That is not the shape the user wants.

The existing old 5m engine to clone is:

- `apps/worker/src/quant_terminal_worker/signal_engines/vegas_ema_5m_cluster.py`

That file is self-contained:

- reads derived 5m EMA rows
- scans 5m rows directly
- counts EMA rail proximity votes
- uses 5m as trigger
- appends 2h and 1d context charts
- emits packets with candle/EMA context
- supports training and live scan paths with aligned behavior

## Required Rebuild

Rewrite `vegas_ema_recursive_features.py` as a near-copy of `vegas_ema_5m_cluster.py`.

Keep identical signal discovery behavior:

- same derived 5m EMA row input
- same `EMA_PERIODS`
- same `EMA_TUNNELS`
- same proximity calculation
- same vote threshold behavior
- same dedupe behavior
- same packet timing
- same `charts` / candle / EMA packet shape
- same live scan behavior

Only add feature enrichment at packet emission.

## Feature Append Behavior

At the point where the old 5m engine has built a valid packet, append derived feature rows.

Feature families:

```python
FEATURE_FAMILIES = {
    "base_candle": "feature_base_candle",
    "volatility_range": "feature_volatility_range",
    "volume": "feature_volume",
    "ema_vegas_structure": "feature_ema_vegas_structure",
    "bollinger": "feature_bollinger",
    "regime_momentum": "feature_regime_momentum",
}
```

Default feature timeframes:

```python
("5m", "2h", "1d")
```

Default feature window bars:

```python
{"5m": 24, "2h": 12, "1d": 10}
```

The feature payload should be additional context only. It must not decide whether a signal exists.

Recommended packet addition:

```json
{
  "features": {
    "5m": {
      "latest": {
        "base_candle": { "...": "..." },
        "volatility_range": { "...": "..." },
        "volume": { "...": "..." },
        "ema_vegas_structure": { "...": "..." },
        "bollinger": { "...": "..." },
        "regime_momentum": { "...": "..." }
      },
      "window": [
        {
          "timestamp": "...",
          "base_candle": { "...": "..." },
          "volatility_range": { "...": "..." }
        }
      ],
      "window_bars": 24
    }
  }
}
```

Also mirror this under `packet["evidence"]["features"]` if existing strategy/tests expect it, but do not remove the old packet fields.

## Important Failure Mode

The user inspected packets and found new feature fields showing as empty `{}`.

That is not acceptable.

If feature rows are required by the engine registry, missing rows should fail clearly or produce explicit diagnostics. Do not silently emit empty feature sections as if enrichment succeeded.

Preferred behavior:

- if an entire required feature dataset is missing for the requested asset/timeframe/family, raise a clear `ValueError`
- if a signal timestamp predates a feature family row window, include a clear `missing_feature_families` diagnostic for that timestamp
- do not emit `{}` for every feature family without explanation

## Registry Context

Registry entry:

- `artifacts/signal_engine/engine_registry.json`
- key: `vegas_ema_recursive_features`

Current entry points:

```json
"runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:generate_training_signals",
"live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema_recursive_features:scan_live_signal"
```

Keep those entrypoint names unless there is a strong reason to rename.

The registry currently declares feature required data refs. The signal-pool creation path has already been adjusted to accept feature required data types, so the issue is engine packet construction, not registry acceptance.

## Tests To Update Or Add

Primary test file:

- `tests/test_signal_engine_runtime.py`

Required tests:

1. Old 5m engine and feature engine produce the same signal timestamps on the same fixture.

   Use:

   - `vegas_ema_5m_cluster`
   - `vegas_ema_recursive_features`

   Assert:

   - same generated packet count
   - same signal timestamp
   - same 5m interactions / matched EMA periods
   - same `charts["5m"]` structure, except feature engine has additional feature payload

2. Feature engine preserves old 5m packet shape.

   Assert feature packet still has:

   - `charts`
   - `charts["5m"]`
   - `charts["2h"]`
   - `charts["1d"]`
   - `interactions`
   - old evidence fields such as vote threshold, matched periods, EMA mode/pattern as appropriate

3. Feature payload is non-empty when feature data exists.

   Assert:

   - `packet["features"]["5m"]["latest"]["base_candle"]` is non-empty
   - `packet["features"]["5m"]["latest"]["volatility_range"]` is non-empty
   - same for each declared feature family in the fixture
   - `packet["features"]["5m"]["window"]` has rows

4. Missing feature data is explicit.

   Assert either:

   - `ValueError` with missing asset/timeframe/family detail, or
   - packet has a clear `missing_feature_families` diagnostic

5. Live scan preserves training packet shape.

   Use the same fixture data and confirm live scan emits the same old 5m context plus feature rows.

## Related Existing Tests

Existing feature tests are around:

- `test_recursive_vegas_features_registry_entry_is_contract_compliant`
- `test_recursive_vegas_features_training_emits_compact_feature_windows`
- `test_recursive_vegas_features_live_scan_preserves_training_feature_shape`
- `test_recursive_vegas_features_training_can_stream_packets_in_chunks`

These probably need to be rewritten to compare against `vegas_ema_5m_cluster`, because the new engine should be a feature-augmented clone of the 5m cluster engine.

## Recent Fixes Already Made

These were already changed before this handoff:

- `apps/api/src/quant_terminal_api/main.py`
  - `_required_data_refs()` now accepts feature data types instead of rejecting non-candle refs.

- `apps/api/src/quant_terminal_api/repositories/runtime.py`
  - `get_data_ref()` alias added.
  - `enqueue_job()` now requeues expired running jobs before returning an active job.

- `apps/worker/src/quant_terminal_worker/ingestion/signal_pool_extension.py`
  - streamed packet chunks now call `repository.refresh_signal_set_coverage(signal_set_key)` so packet counts update during long-running generation.

- `ops/scripts/stop_dev_stack.sh`
  - stops stale Celery/API processes by pattern to avoid old workers consuming new jobs.

## Operational Note

There were stale Celery workers still running old SDK code. They caused jobs to fail with:

```text
unsupported required data type: feature_base_candle
```

That was an operational stale-worker issue after the contract code had already been fixed.

Before retesting:

```bash
cd "/Users/haokaiqin/Motis Agentic Quant Terminal"
ops/scripts/stop_dev_stack.sh
ops/scripts/start_dev_stack.sh
```

Then ensure there are no old Celery workers:

```bash
ps aux | rg 'celery|uvicorn quant_terminal_api'
```

## Verification Commands

Backend focused:

```bash
PYTHONPATH=packages/strategy_sdk/src:packages/engine_sdk/src:packages/strategy_modules/src:apps/api/src:apps/worker/src \
pytest -q \
  tests/test_api.py \
  tests/test_signal_engine_runtime.py \
  tests/test_runtime_repository.py \
  tests/test_job_dispatch.py \
  tests/test_execution_data_warmup.py \
  tests/test_feature_enrichment.py \
  tests/test_market_data_reader.py \
  -k 'not current_aave_execution_bundle'
```

Frontend:

```bash
npm --workspace apps/web-v2 run build
```

Signal-pool update smoke test:

```bash
curl -sS -X POST \
  http://127.0.0.1:8000/api/v1/signal-engines/vegas_ema_recursive_features/signal-sets/AAVE/extend-local \
  -H 'Content-Type: application/json' \
  -d '{}'
```

Check packets:

```bash
psql postgresql://motis:motis@127.0.0.1:5432/motis -c \
"select count(*) as signals, min(timestamp) as first_ts, max(timestamp) as last_ts
 from signals
 where signal_set_key='vegas_ema_recursive_features:AAVE:AAVE-vegas_ema_recursive_features-canonical';"
```

## Implementation Rule

Do not over-abstract this.

Do not build a generic feature engine.

Do not delegate discovery to old engines.

For this task, copy the old 5m engine shape directly, then append features at emission.

