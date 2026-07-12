---
name: signal-engine-builder
description: Use when creating, registering, refactoring, or reviewing a Motis Quant Terminal signal engine, live scanner, training generator, neutral signal packet, engine registry entry, or paired base strategy template.
---

# Signal Engine Builder

## Purpose

Build Motis signal engines against the repo-owned contract, not by copying Vegas-specific runtime assumptions. A complete engine includes canonical metadata, training generation, latest-candle live scan, neutral packet output, required-data declarations, tests, and a paired base strategy template.

## Required References

Read these first:

- `docs/engine-strategy-contract.md`
- `templates/engine_strategy_pair/`
- `packages/strategy_sdk/src/quant_terminal_sdk/engine_contracts.py`
- `artifacts/signal_engine/engine_registry.json`
- `references/motis-signal-engine-consumer-contract.md` when building a new packet shape, debugging Stage 0/1/2/3/4 consumption, or adding a new evidence field convention.

For current runtime examples, read:

- `apps/worker/src/quant_terminal_worker/signal_engines/runtime.py`
- `apps/worker/src/quant_terminal_worker/signal_engines/vegas_ema.py`
- `apps/worker/src/quant_terminal_worker/signal_engines/bollinger.py`
- `tests/test_signal_engine_runtime.py`
- `tests/test_api.py` signal-engine catalog tests

## Non-Negotiable Contract Rules

- Engine metadata must validate as `SignalEngineSpec`.
- Engine id must be stable and unique, e.g. `vegas_ema_vote1`.
- `required_data` must declare canonical Parquet needs, currently candle data by `origin` and `timeframe`.
- Training generator must consume canonical Parquet through `MarketDataReader` or the engine runtime context.
- Live scanner must scan the latest eligible canonical Parquet candle state, not historical DB signal backlog.
- Signal packets must be point-in-time safe: every field, chart, feature, and higher-timeframe context value must be derivable from data available at or before the signal generation timestamp. Do not use future rows, unclosed candles presented as completed context, centered/forward-looking indicators, or derived features that were calculated with future candles.
- Signal packets must be neutral market evidence only. Do not include direction, side, confidence, sizing, leverage, TP, SL, or order intent.
- Signal packets must fit downstream Motis consumers, not only `validate_signal_packet()`. Every event packet must expose a standard reference price in `evidence.reference_price` and `evidence.trigger_candle_close`; for confirmed-5m event engines these usually equal the signal candle close. Do not patch Stage 0/2/3/4 to learn one-off engine aliases unless the user explicitly requests a system-wide compatibility change.
- Stage 1/runtime strategy invocation is the canonical interface boundary. Engines emit raw `signal_packet.v2` packets; every strategy caller must wrap those packets before calling `decide(context)`.
- Canonical `decide(context)` shape:
  - `context["signal"]["signal_id"]`
  - `context["signal"]["signal_set_key"]` when available
  - `context["signal"]["signal_engine_id"]` when available
  - `context["signal"]["asset"]`
  - `context["signal"]["instrument"]`
  - `context["signal"]["timestamp"]`
  - `context["signal"]["payload_schema"] == "signal_packet.v2"`
  - `context["signal"]["payload"] == <raw emitted packet>`
  - `context["runtime_mode"]` set to `stage1`, `backtest`, or `live`
  - `context["parameters"]` as a dict
  - `context["raw_data"]` as a dict
- Strategy direction belongs in the paired base strategy `decide(context)`, and strategies should read packet evidence through `context["signal"]["payload"]`, not by assuming the raw packet was passed directly as `context["signal"]`.
- The paired base strategy must match the engine's actual emitted packet shape. Do not copy legacy Vegas gates such as requiring two active timeframes unless the new engine really emits and requires that shape.
- Execution setup and live router own sizing, TP/SL price derivation, protection, pyramiding, and order submission.
- The engine registry entry should include `code_ref.base_strategy_path`, and that file must expose a valid strategy `decide()`.
- The engine must be visible through `GET /api/v1/signal-engines`, even before any DB signal pool exists. Repo registry entries must merge into the API catalog with zero counts when the DB has no row yet.

## Point-in-Time Data Discipline

Use this section before designing any multi-timeframe or indicator-heavy engine.

- Define the signal timestamp as the latest confirmed 5m candle close used to trigger the event.
- For higher-timeframe completed candles, select by availability/close time, not open time. A 2h candle that opens at 06:00 is unavailable to a 07:05 signal if it closes at 08:00.
- If forming higher-timeframe context is useful, compute it from confirmed lower-timeframe candles up to the signal timestamp and mark it explicitly as `complete: false`. Never mix forming candles into completed-candle arrays without an explicit completeness flag.
- Prefer one canonical `candles` structure per timeframe where each row has enough metadata for strategy code to know `timeframe`, `open_time`, `close_time` or equivalent timestamp convention, and `complete`. Avoid storing duplicate `completed_candles` plus `candles` copies unless compatibility requires it.
- Indicators must be causal. EMA/SMA/RSI style indicators are acceptable only when calculated using rows available at or before the signal timestamp. Never use centered windows, full-series normalization, future-filled features, or postcomputed higher-timeframe values attached retroactively.
- Training generation and live generation must use the same packet-building function. Live must not have a richer or poorer context shape than training.
- Dedupe/cadence state must be continuous across repeated generation calls. If training uses a 2h post-fill dedupe, extending a signal pool every 5m must look back to the latest existing canonical signal and preserve the same cadence instead of resetting the dedupe window.
- Packets should include enough market evidence for Stage 1 to reason, but not every possible candle twice. Large packets slow Stage 1 and frontend artifact handling; add context intentionally and test packet size with representative assets.
- Build per-signal context with bounded source windows. Do not rebuild HTF/chart context by scanning full prior history for every emitted packet; that can turn full-asset generation into an accidental O(N^2) job.

## Packet Shape And Consumer Contract

- Treat the engine as responsible for conforming to Motis pipeline conventions. If Stage 0 or Stage 2 cannot read a new packet, fix the engine packet first.
- Include `evidence.reference_price`, `evidence.trigger_candle_close`, `evidence.signal_available_at`, and signal candle open/close timestamps when applicable.
- Prefer compact chart rows with a `columns` header, but keep headers agent-readable enough for strategy work. Avoid opaque names for domain-specific data if the agent is expected to reason from it.
- Keep old generated DB packets in mind. If packet fields change, tell the user to regenerate the signal pool instead of hiding the mismatch with consumer fallbacks.
- Use `scripts/audit_signal_packet_contract.py --packet <packet.json>` on representative emitted packets before claiming the engine is pipeline-ready.

## Stage 0A Information Gate Feedback

Use Stage 0A as an engine-quality feedback loop, not as a strategy-profitability test.

- After generating a candidate signal pool, run the Stage 0A information gate before spending agent time on Stage 1 optimization.
- Interpret Stage 0A as: "Does this event select future excursion windows better than matched random timestamps?"
- Good engine events should show positive train median `max_abs_mfe_pct` lift, non-degraded walk-forward lift, reasonable p/q values, and monthly stability.
- If Stage 0A is worse than random with adequate sample size, revise the event definition or packet context before optimizing strategy logic. A strategy agent cannot reliably manufacture edge from timestamps that do not select informative future windows.
- Do not bake Stage 0A labels, future excursion, or random-baseline results into emitted signal packets. Stage 0A is an evaluation artifact only.
- Keep Stage 0A separate from legacy Stage 0 scoring: information can guide whether to continue, while threshold calibration and ground-truth labeling remain downstream compatibility artifacts.

## Outcome-First Discovery

Use this workflow when the task is driven by a frozen fixed-R opportunity target rather than an existing event hypothesis.

- Treat only the prompt-listed training-only target, timestamp labels, episodes, causal features, matched hard negatives, and any `signal_discovery_evidence_manifest.v1` sources as discovery evidence. Never inspect walk-forward, validation, locked OOS, live-result, or future-outcome artifacts while researching or building the engine.
- When an evidence manifest is present, every included same-asset Parquet dataset is fair game through its `research_end` authorization cutoff, including all earlier history as feature warmup. Apply the cutoff at row level even when a listed shard also contains later rows; a source path is not permission to read the whole file.
- Treat `training_features.parquet` as a convenience baseline, not the feature-search boundary. Perform arbitrary causal resampling and derived-feature research when justified, including higher-timeframe candles or OI, trends, z-scores, acceleration, interactions, and regime features.
- Do not trust a registered derived dataset merely because it is in the manifest. Prove its timestamp and availability semantics are point-in-time safe, and prefer a reproducible raw-source transformation when the derived series cannot be refreshed identically in training and live execution.
- Start from episode-level evidence. Determine whether opportunity episodes differ from matched neutral timestamps through a plausible, point-in-time market mechanism that recurs across broad periods and regimes. Timestamp-level row count alone is not independent evidence because adjacent labels belong to the same opportunity episode.
- Compare competing causal hypotheses and record their evidence, stability, failure cases, and rejection criteria in the requested `engine_research_rationale.md` before choosing thresholds or event rules.
- In the rationale, list all dataset ids and columns examined, selected lookbacks and transformations, causal availability checks, rejected hypotheses, and final production dependencies. The registry `required_data` must cover every source the engine actually needs rather than only the session's primary 5m label series.
- Score a candidate directly against the frozen fixed-R target using training labels. Report episode precision/recall, timestamp coverage, matched-hard-negative false-positive rate, monthly stability, cadence, and overlap behavior; do not substitute Stage 0A information lift or aggregate directional accuracy for direct target evaluation.
- Keep outcome rows out of packets and implementation fixtures. Do not encode exact opportunity timestamps, episode ids, signal ids, exact dates, or date-specific branches in engine rules, tests, or rationale.
- Build only a neutral engine; any directional inference learned from causal packet evidence belongs in the paired base strategy. The frozen target defines evaluation and must not be copied into live packet fields.
- Reject the candidate when there is no recurring causal mechanism, when evidence depends on a narrow calendar slice, or when causal features cannot separate episodes from hard negatives. Rejection is a successful research result and is preferable to manufacturing a permissive trigger.
- The agent may use only training evidence to propose and fit the engine. Held-out walk-forward scoring is owned by the terminal after the registered engine id is attached.

## Build Workflow

1. Pick the engine id, display name, version, default parameters, and required Parquet data.
2. Add or update the engine registry entry with canonical fields:
   - `signal_engine_id`
   - `version`
   - `name`
   - `required_data`
   - `output_envelope_version: "signal_packet.v2"`
   - `runtime_entrypoint`
   - `live_scanner_entrypoint`
   - `configuration_schema.default_parameters` when defaults differ from the adapter defaults
   - `code_ref.base_strategy_path`
3. Implement the engine adapter under `apps/worker/src/quant_terminal_worker/signal_engines/` unless an existing adapter can safely be parameterized.
4. Implement or point to the paired base strategy under `packages/strategy_modules/src/quant_terminal_strategies/`.
5. Add tests before implementation:
   - registry/spec validation
   - API engine catalog includes registry-only entries with `signal_set_count: 0` and `packet_count: 0`
   - training dispatch from Parquet
   - live scan from Parquet
   - point-in-time safety: no emitted packet may include data whose close/availability time is after the signal timestamp, especially higher-timeframe context candles and derived features
   - repeated extension/cadence parity: a second generator call must respect existing dedupe state and not append signals inside the training dedupe window
   - packet neutrality
   - paired base strategy validates
   - canonical strategy context: wrap at least one real emitted training packet and one live-scan packet as `context["signal"]["payload"]` before calling `decide(context)`
   - engine/strategy compatibility: assert the paired strategy does not skip solely because of packet-shape assumptions such as active timeframe count, wrapper path, or missing legacy fields
   - packet consumer contract: assert emitted packets include `evidence.reference_price` and `evidence.trigger_candle_close`, and run the packet audit script on a representative packet
   - Stage 1 scorer compatibility: exercise the actual Stage 1 scoring path with a representative raw emitted packet artifact and assert it produces scoreable `LONG`/`SHORT` decisions when the paired strategy has directional rules
   - Stage 0A information gate can run on the generated pool and reports interpretable pass/fail/insufficient-sample metrics
   - Stage 0 signal-pool preparation still works where relevant
6. If catalog behavior changes, update `apps/api/src/quant_terminal_api/main.py` and tests so DB rows win but repo registry entries fill missing engines.
7. Run focused tests, then `pytest -q`. Run `npm --workspace apps/web-v2 run build` if frontend/API types were touched.
8. Restart the backend and verify the live endpoint includes the new engine:
   - `curl -sS http://127.0.0.1:8000/api/v1/signal-engines`

## Common Mistakes

- Adding a legacy script path without a contract runtime adapter.
- Assuming `engine_registry.json` alone makes the engine visible in the UI. The Engines page reads the API catalog, so registry-only engines must be merged into `GET /api/v1/signal-engines`.
- Letting a packet imply `LONG` or `SHORT`.
- Emitting only engine-specific price aliases such as `evidence.close`. Stage 0/2/3/4 need a standard reference price field.
- Selecting higher-timeframe context by candle open timestamp instead of close/availability timestamp. A 2h/1d candle is not available to a 5m signal until that higher-timeframe candle has closed.
- Calling a partially formed 2h/8h/1d candle "latest completed" because its open timestamp is before the signal. If it is useful, emit it as forming context with `complete: false`.
- Letting training generation and live extension use different packet builders, different dedupe windows, or different HTF availability rules.
- Resetting a post-fill dedupe window on every generator call, which makes live append clustered signals that could never appear in the training pool.
- Emitting redundant candle arrays that double packet size without adding evidence. Prefer a single explicit candle schema with `complete` metadata.
- Using overly cryptic column headers for evidence the agent must reason from. Compact is good; unintelligible is not.
- Building chart/HTF context from the whole history inside every packet. Compute event features from rolling caches and packet context from bounded windows.
- Forgetting `code_ref.base_strategy_path`, which leaves Stage 1 to fall back to the generic starter.
- Reusing a paired base strategy that expects an older packet shape. Example: a 5m-only engine emitting `active_timeframes: ["5m"]` must not use a base strategy that requires two active timeframe votes before it can score.
- Testing `decide(context)` by passing the raw emitted packet as `context["signal"]`. This bypasses the canonical runtime wrapper and can hide Stage 1/live execution contract bugs.
- Making each strategy defensively support malformed scorer input instead of fixing the caller. Stage 1, backtests, promotion, and live execution must all call strategies with the same canonical signal wrapper.
- Using live exchange fetches inside the engine instead of canonical Parquet.
- Changing Vegas defaults while adding a variant. Add a separate engine id and spec defaults instead.

## Final Checklist

- `validate_signal_engine_spec(...)` passes.
- `validate_signal_packet(...)` passes for emitted packets.
- `validate_strategy_module(base_strategy_path)` passes.
- Tests prove all packet evidence is point-in-time safe and excludes unavailable future context at signal generation time.
- Repeated signal-pool extension preserves the same dedupe/cadence behavior as full-window training generation.
- Representative packet size is inspected; duplicate candle payloads are avoided unless explicitly needed for backward compatibility.
- A representative packet passes `scripts/audit_signal_packet_contract.py` and has standard reference price fields.
- Stage 0A information gate has been run, and weak/worse-than-random event pools are not pushed into Stage 1 without an explicit experimental reason.
- A representative emitted packet from the training generator and live scanner can be wrapped as `context["signal"]["payload"]` and passed to the paired `decide(context)` without being rejected for stale packet-shape reasons.
- The actual Stage 1 scorer path can consume a representative raw emitted packet artifact and call `decide(context)` with the canonical runtime signal wrapper.
- Strategy callers in Stage 1, backtests, promotion, and live execution use the same canonical wrapper shape.
- `GET /api/v1/signal-engines` returns the new engine after backend restart, even with zero signal sets.
- The v2 Engines tab can list/select the new engine.
- Existing engines keep their old behavior unless the user explicitly asked for a behavior change.
- New engine can be selected independently in research/trading flows by its engine id.
