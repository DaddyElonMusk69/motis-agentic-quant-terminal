# Canonical Engine and Strategy Contract

This document is the repo-owned source of truth for building new signal engine and strategy pairs. It defines the contracts that future builders must target. It does not refactor the current runtime dispatch layer; any Vegas-specific runtime paths that still exist are compatibility behavior to remove in a later phase.

## Ownership Boundaries

- Market data is canonical in Parquet and discovered through `market_data_refs`.
- Signal engines own market-state scanning and emit neutral evidence packets only.
- Strategies own entry direction and entry/skip judgment through `decide(context)`.
- Strategies may own discretionary position judgment through `manage_position(context)`.
- Live execution owns sizing, exchange routing, TP/SL prices, protection updates, pyramiding submissions, hard time exits, and idempotent order submission.
- Postgres remains canonical for promoted bundle metadata, routes, wake audit, and queryable research signal pools.

## Signal Engine Spec

New engines declare a `SignalEngineSpec` compatible registry entry:

```json
{
  "signal_engine_id": "example_breakout",
  "version": "0.1.0",
  "name": "Example Breakout",
  "required_data": [
    {
      "data_type": "candles",
      "origin": "raw",
      "timeframe": "5m",
      "lookback_bars": 500
    }
  ],
  "output_envelope_version": "signal_packet.v2",
  "runtime_entrypoint": "engines/example_breakout/generate_training_signals.py",
  "live_scanner_entrypoint": "engines/example_breakout/scan_live_signal.py"
}
```

Required fields:

- `signal_engine_id`: stable id used by API, research sessions, routes, and bundles.
- `version`: engine implementation version.
- `required_data`: canonical market-data needs.
- `output_envelope_version`: currently `signal_packet.v2`.
- `runtime_entrypoint`: training/research signal generation entrypoint.
- `live_scanner_entrypoint`: live latest-candle scan entrypoint.

Legacy fields such as `replay_generator_path` and `live_scanner_path` may be parsed by validators for existing metadata, but new engines should use the canonical names.

## Required Market Data

Supported v1 data declarations:

```json
{
  "data_type": "candles",
  "origin": "raw",
  "timeframe": "5m",
  "lookback_bars": 20000,
  "freshness_tolerance_seconds": 300
}
```

Derived candles may declare a source:

```json
{
  "data_type": "candles",
  "origin": "derived",
  "timeframe": "2h",
  "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"}
}
```

The data reader must resolve `market_data_refs` for the asset, `data_type`, `origin`, and `timeframe`, then read partitioned Parquet from `storage_uri`. Returned rows must be UTC, sorted, deduped, and confirmed-only where the source supports confirmation.

## Signal Packet

Signal packets are neutral market evidence. They must not contain strategy direction, order intent, sizing, leverage, TP, SL, or confidence scoring.

Canonical packet shape:

```json
{
  "schema_version": "signal_packet.v2",
  "asset": "SOL",
  "instrument": "SOL-USDT-SWAP",
  "timestamp": "2026-06-08T00:00:00Z",
  "active_timeframes": ["5m", "2h"],
  "evidence": {
    "pattern": "breakout",
    "trigger_price": "150.25",
    "features": {"range_pct": 1.2}
  }
}
```

Forbidden packet fields include `direction`, `side`, `action`, `trade_action`, `confidence`, `entry_price`, `size`, `notional_usd`, `margin`, `leverage`, `tp`, `tp_pct`, `sl`, and `sl_pct`.

## Training Signal Generation

Training generation scans historical canonical Parquet and appends research packets to the signal pool. It does not create a live order queue.

Result contract:

```json
{
  "status": "appended",
  "generated_packet_count": 12,
  "appended_packet_count": 10,
  "raw_candle_end_ts": "2026-06-08T00:00:00Z",
  "previous_signal_end_ts": "2026-06-07T00:00:00Z",
  "scan_coverage_end_ts": "2026-06-08T00:00:00Z",
  "final_signal_end_ts": "2026-06-08T00:00:00Z",
  "packet_refs": ["packets/sol-20260608T000000Z.json"]
}
```

Training dedupe belongs to research signal generation only. Live execution must not drain historical packets as a backlog.

## Outcome-First Signal Discovery

Outcome-First discovery is an alternative research entry point for engines whose event hypothesis is not known in advance. It freezes an executable fixed-R outcome target before an engine is researched, then evaluates the registered engine and paired strategy directly against that target.

The immutable target uses schema `signal_discovery_target.v1`:

```json
{
  "schema_version": "signal_discovery_target.v1",
  "target_version": 1,
  "session_id": "discovery-btc-v1",
  "config_hash": "sha256",
  "selected_target": {
    "selected_risk_pct": 1.0,
    "reward_multiple": 2.0,
    "stop_multiple": 1.0,
    "horizon_hours": 36,
    "entry_delay_minutes": 5,
    "entry_semantics": "next_5m_open",
    "fee_bps_per_side": 5.0,
    "slippage_bps_per_side": 5.0
  },
  "source_data": {
    "dataset_id": "canonical-raw-5m",
    "storage_backend": "parquet",
    "storage_uri": ".data/market-data/..."
  },
  "splits": {
    "research_start": "2025-03-01T00:00:00Z",
    "research_end": "2026-03-31T23:55:00Z",
    "walk_forward_start": "2026-04-01T00:00:00Z",
    "walk_forward_end": "2026-05-30T23:55:00Z"
  }
}
```

Target semantics are fixed:

- Entry is the next confirmed 5-minute open after the configured delay. The entry candle is not part of the barrier scan.
- `LONG` and `SHORT` require the 2R target to touch before the 1R stop in that direction.
- `NEUTRAL` means neither direction qualifies within the complete horizon.
- `AMBIGUOUS` means available candle resolution cannot establish barrier order.
- Every emitted engine timestamp is relabeled from its own executable path. Nearest-opportunity tolerance is not used.

Session artifacts live under `dev/signal_discovery_sessions/<session_id>/`:

```text
manifest.json
atlas/training_timestamp_labels.parquet
atlas/training_episodes.parquet
atlas/training_features.parquet
atlas/training_hard_negatives.parquet
atlas/r_feasibility.json
target/frozen_target.json
prompt/engine_builder_prompt.md
prompt/engine_research_rationale.md
walk_forward/walk_forward_timestamp_labels.parquet
walk_forward/walk_forward_episodes.parquet
walk_forward/walk_forward_summary.json
evaluation/engine_evaluation.json
handoff/stage0/scores/fixed_target_contract.json
handoff/stage0/scores/ground_truth_summary.json
handoff/stage0/scores/ground_truth/*.json
```

The leakage boundary is enforced by artifact role. Before target freeze, only training labels, episodes, causal features, hard negatives, and R feasibility exist. The engine-builder prompt may name those training artifacts and `target/frozen_target.json`; it must not name or embed walk-forward labels, exact opportunity timestamps, exact episode/signal ids, or outcome rows. Walk-forward artifacts are created only by the terminal after the target is frozen and are never authorized evidence for the engine-building agent.

The generated prompt requires `$signal-engine-builder`. The agent must either reject the hypothesis or register one neutral `signal_packet.v2` engine and a directional paired strategy, write `engine_research_rationale.md`, and prove point-in-time safety, cadence parity, packet-consumer compatibility, and direct training-target scoring.

Candidate evaluation resolves the registered engine, fills its canonical signal pool through the shared training runtime, loads `code_ref.base_strategy_path`, invokes the strategy with the canonical runtime wrapper, and reports training and sealed walk-forward slices. Primary metrics include opportunity precision, episode recall, LONG/SHORT/NEUTRAL/AMBIGUOUS counts, directional accuracy including natural neutral mismatches, and net R after costs from each emitted timestamp's own selected path.

Accepted evaluations materialize Stage 0 compatibility artifacts with `label_contract: fixed_r_first_touch.v1`. They do not run legacy excursion threshold calibration or the Stage 0A information gate. Stage 2 may retain travel capture as diagnostics, but its shared TP, initial SL, and horizon come from the frozen target. Stage 3 validates those values against the Stage 0 contract and may vary protection behavior only; it cannot recalibrate base TP or SL.

## Live Signal Scan

Live scanning uses freshly warmed canonical Parquet, builds the latest eligible candle state, and scans the latest confirmed candle only.

Result contract:

```json
{
  "status": "fresh_signal",
  "source": "live_parquet_snapshot",
  "signal": {
    "schema_version": "signal_packet.v2",
    "asset": "SOL",
    "timestamp": "2026-06-08T00:00:00Z",
    "evidence": {"pattern": "breakout"}
  }
}
```

No-signal result:

```json
{
  "status": "no_fresh_signal",
  "source": "live_parquet_snapshot",
  "reason": "latest_confirmed_candle_did_not_trigger"
}
```

## Strategy Module

A strategy module must expose:

```python
def decide(context: dict) -> dict:
    ...
```

It may also expose:

```python
def manage_position(context: dict) -> dict:
    ...
```

`decide(context)` returns one of:

```json
{"action": "ENTER", "direction": "LONG", "reason_code": "accepted"}
{"action": "ENTER_LONG", "direction": "LONG", "reason_code": "accepted"}
{"action": "ENTER_SHORT", "direction": "SHORT", "reason_code": "accepted"}
{"action": "SKIP", "direction": "FLAT", "reason_code": "filtered"}
{"action": "BLOCKED", "direction": "FLAT", "reason_code": "missing_context"}
```

`manage_position(context)` returns one of:

```json
{"action": "HOLD", "reason_code": "policy_ok"}
{"action": "EXIT", "reason_code": "strategy_exit"}
{"action": "REDUCE", "reason_code": "risk_reduction"}
{"action": "PYRAMID", "reason_code": "strategy_add"}
{"action": "UPDATE_PROTECTION", "reason_code": "strategy_protection"}
{"action": "BLOCKED", "reason_code": "missing_context"}
```

Strategies should not hardcode live sizing, leverage, TP/SL prices, or exchange account behavior. They can read `execution_setup` percentages and diagnostics to explain their decisions, but mechanical execution derives order prices from exchange truth.

## Execution Setup

Promotion produces an execution setup consumed by live execution:

```json
{
  "schema_version": "0.1",
  "source": "stage4_realized_expectancy",
  "account_mode": "live",
  "execution_adapter": "okx",
  "forward_hours": 24,
  "hard_exit_after_hours": 24,
  "stage4_candidate_id": "candidate-001",
  "setup": {
    "candidate_id": "candidate-001",
    "final_tp_pct": 1.2,
    "initial_sl_pct": 0.6,
    "protection_enabled": true,
    "protect_trigger_pct": 0.5,
    "trail_sl_pct": 0.1,
    "pyramid": {"max_legs": 3, "step_pct": 0.3}
  }
}
```

For fixed-SL candidates:

```json
{
  "final_tp_pct": 1.2,
  "initial_sl_pct": 0.6,
  "protection_enabled": false
}
```

Live execution derives actual TP/SL prices from OKX average entry, side, size, mark price, and this percentage policy. It must not treat derived local prices as exchange truth.

## Promotion Handoff

Stage handoff responsibilities:

- Stage 0 defines asset universe, training/walk-forward windows, significant-move threshold, and hard forward-hours gate.
- Outcome-First discovery may instead provide an accepted Stage 0 compatibility candidate with `fixed_r_first_touch.v1` labels and an immutable target contract.
- Stage 1 produces the strategy module and directional evidence.
- Stage 2 selects an exit policy from the training travel profile, except fixed-target discovery candidates whose base TP/SL/horizon are already frozen.
- Stage 3 tests fixed SL, exact protection, local variants, and pyramiding candidates using candle walk-forward semantics. Fixed-target candidates preserve base TP/SL across protection variants.
- Stage 4 runs sequential account simulation with user capital, margin allocation, leverage, fees, hard exit, protection, and pyramiding.
- Promotion freezes the latest completed Stage 4 candidate into an execution bundle with `strategy.py`, `execution_setup.json`, `manifest.json`, `evidence_refs.json`, and risk limits.

## Validation Entrypoints

Use the SDK validators before accepting new builds:

```python
from quant_terminal_sdk.engine_contracts import (
    validate_execution_bundle,
    validate_execution_bundle_contract,
    validate_signal_engine_spec,
    validate_signal_packet,
    validate_strategy_module,
)

validate_signal_engine_spec("example_breakout")
validate_signal_engine_spec("templates/engine_strategy_pair/engine_registry_entry.json")
validate_signal_packet(packet)
validate_strategy_module("strategy.py")
validate_execution_bundle_contract(bundle)
validate_execution_bundle("aave-vegas_ema-aave-vegas_ema-strategy-v01-3bee1a88652e")
```

These validators are intentionally strict for new contracts and include limited legacy parsing only so current Vegas metadata remains readable during the phase-2 runtime refactor.
