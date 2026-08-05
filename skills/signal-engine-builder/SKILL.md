---
name: signal-engine-builder
description: Use when creating, training, registering, refactoring, or reviewing a Motis Quant Terminal signal engine, supervised timestamp sequence model, training-data module, live scanner, training generator, neutral signal packet, engine registry entry, or paired base strategy template.
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
- `docs/supervised-signal-engine-training-contract.md` when the handoff requests raw timestamp supervised training.
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
- The complete emission decision must be point-in-time safe, not only the packet. For a signal timestamp `T`, trigger/no-trigger, selected leaf or leaves, feature and normalization state, resampling, joins, cadence/dedupe admission, and packet construction may depend only on source observations whose `available_at <= T`.
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

**Look-ahead bias warning:** a source row timestamp at or before `T` is not sufficient when that row closes, settles, publishes, or becomes knowable after `T`. Prove an explicit availability time for every trigger input and enforce `available_at <= T` throughout research, training generation, extension, and live scanning.

- Define the signal timestamp as the latest confirmed 5m candle close used to trigger the event.
- For higher-timeframe completed candles, select by availability/close time, not open time. A 2h candle that opens at 06:00 is unavailable to a 07:05 signal if it closes at 08:00.
- If forming higher-timeframe context is useful, compute it from confirmed lower-timeframe candles up to the signal timestamp and mark it explicitly as `complete: false`. Never mix forming candles into completed-candle arrays without an explicit completeness flag.
- Prefer one canonical `candles` structure per timeframe where each row has enough metadata for strategy code to know `timeframe`, `open_time`, `close_time` or equivalent timestamp convention, and `complete`. Avoid storing duplicate `completed_candles` plus `candles` copies unless compatibility requires it.
- Indicators must be causal. EMA/SMA/RSI style indicators are acceptable only when calculated using rows available at or before the signal timestamp. Never use centered windows, full-series normalization, future-filled features, or postcomputed higher-timeframe values attached retroactively.
- Make every resample and as-of join availability-aware. Bucket membership by open time does not make a higher-timeframe aggregate available before its close, and a join must never select a later observation merely because its source timestamp sorts before `T`.
- Classify every non-candle source before implementation as either a **hard-aligned decision channel** or an **as-of/context channel**. Hard-aligned channels such as 5m OI or 5m futures metrics must have the exact signal timestamp row; never substitute a prior row for a missing required timestamp. As-of/context channels such as funding state may use the latest observation known at or before the signal timestamp, but this must be explicit in code, packet fields, and tests.
- Training generation must report `scan_coverage_end_ts` as the latest timestamp actually evaluated with all hard-aligned decision channels present, not as the requested target or raw candle end. If candles reach `06:05` but a hard-aligned side channel only reaches `06:00`, coverage is `06:00`. This prevents live extension from permanently skipping late-arriving required rows.
- Live/latest scanners for multi-source engines must scan the latest fully eligible hard-aligned timestamp and avoid stale substitution. Signal-pool extension should use recent overlap rescans for multi-source engines and rely on engine-reported actual coverage to recover delayed side-channel rows.
- Use outcome labels, brackets, and future excursions only as authorized offline training evaluation evidence. They may guide hypothesis and fixed-threshold selection inside the training window, but must never become runtime feature values, preprocessing state, leaf-routing inputs, dedupe state, packet fields, or implementation fixtures.
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

## Workflow Routing

- Follow the workflow explicitly declared by the generated handoff. Do not combine episode-event discovery with raw timestamp supervised training.
- Use **Supervised Timestamp Training** when the handoff authorizes exact timestamp labels or a `motis_supervised_training_input.v1` manifest and asks whether each decision timestamp satisfies the frozen target.
- Use **Outcome-First Discovery** when the handoff authorizes filtered brackets and asks for a sparse event mechanism scored by emitted episodes.
- If a supervised handoff does not authorize exact timestamp labels, stop and request a corrected handoff. Never infer ordinary negatives from filtered brackets.

## Supervised Timestamp Training

Use this workflow for a neutral binary sequence model that predicts the frozen fixed-R truth at every eligible decision timestamp.

- Consume prepared data through `quant_terminal_worker.signal_discovery.supervised_training_data:TimestampTrainingDataModule` and its `motis_supervised_training_input.v1` manifest. Do not create session-local feature engineering or rebuild labels from brackets.
- Use raw labels directly: map `LONG` and `SHORT` to neutral target `1`, map `NEUTRAL` to `0`, and exclude `AMBIGUOUS` or incomplete rows. Do not merge, bridge, filter, or relabel short positive runs.
- Give every eligible timestamp base weight `1`. Longer positive runs then contribute more training mass through their additional timestamps without episode construction or a second duration multiplier.
- Treat every prepared branch as an oldest-to-newest sequence. Preserve dense recent history. Do not replace native paths with a fixed small number of mean/std/last bins unless an information-preservation comparison supports that exact compression.
- Fit robust scaling or other learned preprocessing on unique feature rows from each chronological training fold only. Apply the frozen fold state to validation. Represent missing values with explicit per-channel masks rather than conflating missing with an economic zero.
- Keep outcome labels, direction-specific results, and future excursions out of model inputs and preprocessing state. Enforce `available_at <= decision_ts` for every branch row.
- Before a full run, prove the model can overfit a small chronological subset and run a shuffled-label control that returns to the natural-prevalence baseline.
- Use expanding chronological validation with target-horizon purge. Select epochs from internal validation, retain the best checkpoint, and test multiple deterministic seeds. Do not accept a fixed shallow pass count without validation evidence.
- Evaluate raw timestamp scores before dedupe. Report precision, recall, F1, PR-AUC, prevalence, calibration, the full threshold frontier, fold/seed results, monthly stability, and causal baselines on the same timestamps.
- Keep walk-forward sealed during feature, architecture, epoch, calibration, and threshold selection. Do not register an engine, build a live scanner, or advance Stage 1 unless the handoff explicitly authorizes those later steps after training acceptance.
- Treat failure to beat prevalence and simple causal baselines across chronological folds as a rejected model, not permission to inspect walk-forward or manufacture a sparse calendar pocket.

### Supervised Runner Lifecycle

Use the repo-owned runner at `quant_terminal_worker.signal_discovery.supervised_sequence_training`. Do not create a session-local model, data loader, fold builder, control script, metric implementation, or training entrypoint when this runner supports the prepared manifest.

Before the long run:

1. Use the handoff's `motis_supervised_training_input.v1` manifest and verify its hashes through the runner.
2. Run `--mode preflight` and fix any data, history-length, causality, split, purge, schema, or label failure in the repo-owned module. Regenerate the prepared manifest after a schema fix.
3. Run `--mode controls` with the intended model configuration. Require the overfit subset to contain both positive and negative timestamps; reject a trivial single-class pass. Require the shuffled-label result to return near natural prevalence.
4. Confirm `preflight.json` and `controls.json` both pass for the exact prepared-manifest and model-configuration hashes.
5. Unless the user explicitly asks the agent to launch the long run, stop and give the user the exact `--mode train` command. Keep the manifest, model configuration, seeds, and output directory identical to the passing controls.

Treat a manifest or model-configuration change after controls as a new preparation attempt: rerun preflight and both controls. Do not bypass a failed gate, hand-edit its artifact, or reuse controls from another session. An interrupted run is resumable; preserve checkpoints and rerun the identical command rather than deleting partial artifacts or changing parameters mid-run.

After the user reports completion, inspect only the authorized training artifacts: `status.json`, `training_report.json`, raw and ensemble OOF predictions, fold/seed checkpoints and metrics, causal baselines, fixed-threshold monthly stability, calibration, block-bootstrap intervals, controls, and final-refit hashes when accepted. Explain performance in timestamp terms and verify that acceptance recurs across folds and seeds rather than one calendar pocket. Keep walk-forward sealed. Do not register an engine, build a live scanner, or advance Stage 1 unless a later handoff explicitly authorizes that next phase.

## Outcome-First Discovery

Use this workflow when the task is driven by a frozen fixed-R opportunity target rather than an existing event hypothesis.

- Treat only the prompt-listed training-only target, timestamp labels, episodes, causal features, matched hard negatives, and any `signal_discovery_evidence_manifest.v1` sources as discovery evidence. Never inspect walk-forward, validation, locked OOS, live-result, or future-outcome artifacts while researching or building the engine.
- When an evidence manifest is present, every included same-asset Parquet dataset is fair game through its `research_end` authorization cutoff, including all earlier history as feature warmup. Apply the cutoff at row level even when a listed shard also contains later rows; a source path is not permission to read the whole file.
- Before choosing training channels, inventory every authorized same-asset dataset available for the ticker and research window. Do not reuse a legacy channel set by default. For each candidate source, check columns, coverage, missingness, timestamp and `available_at` semantics, backfill/live continuity, causal derivations, and whether the final selected channel set can be reproduced identically in training and live generation. Freeze the selected channel order, resolutions, transforms, masks, and required data in the session/model schema; any later channel change requires a new schema and retraining.
- Treat `training_features.parquet` as a convenience baseline, not the feature-search boundary. Perform arbitrary causal resampling and derived-feature research when justified, including higher-timeframe candles or OI, trends, z-scores, acceleration, interactions, and regime features.
- Do not trust a registered derived dataset merely because it is in the manifest. Prove its timestamp and availability semantics are point-in-time safe, and prefer a reproducible raw-source transformation when the derived series cannot be refreshed identically in training and live execution.
- Work at episode level throughout outcome-first discovery. Treat approved brackets as ground-truth opportunity episodes and construct matched neutral episodes for comparison. Timestamp rows inside one episode are repeated observations of the same opportunity, not independent samples, precision units, support, or stability evidence.
- Treat approved brackets as opportunity regions. Read any prompt-defined opportunity-precision and bracket-count-coverage objectives before choosing a sparse or broad engine. Objectives such as "around 80%" are optimization targets and desired frontier coordinates, not universal hard cutoffs; report the closest causally supported frontier when both cannot be approached together.
- Convert the final globally deduped signal stream into deterministic emitted episodes before scoring. Declare the episode-construction rule before outcome scoring and make it production-reproducible from causal emission state, such as one emission per engine episode or a fixed refractory gap since the prior emission. Do not tune the episode gap from labels, and do not use future observations to decide whether the current signal may emit.
- Match emitted episodes to approved brackets using deterministic one-to-one matching. An emitted episode is eligible to match when at least one of its canonical signal availability timestamps falls inside the bracket or within the evaluator-declared tolerance. Use the evaluator's assignment rule when declared; otherwise use maximum-cardinality matching with stable chronological tie-breaking by episode start and bracket start. The tolerance is zero unless the target contract explicitly defines one. Each emitted episode and each approved bracket may receive at most one match credit, even when they contain multiple signals or overlap multiple candidates; never invent a grace window or reuse one episode for repeated credit.
- Define engine episode precision as matched emitted episodes divided by all final emitted episodes. Define bracket-count coverage as matched approved brackets divided by all approved brackets. Optimize and report these two episode-level quantities together. Raw signal precision, timestamp coverage, and signal count are cadence diagnostics only and must not select the engine or substitute for episode precision.
- Fit and select models with episode-independent evidence. Keep complete episodes together and avoid treating timestamps inside one episode as independent support, but use the target contract's episode weighting when duration matters, such as eligible 5m bars capped by the outcome horizon. If class balancing is applied, preserve within-class duration ratios. Build internal chronological blocks from complete episodes and purge or embargo boundaries by the target outcome horizon so overlapping label windows cannot cross train/evaluation boundaries.
- Treat episode precision, bracket-count coverage, matched-hard-negative episode rate, cadence, and aggregate monthly/regime stability as the primary neutral-engine discovery metrics. Directional accuracy, chosen-path expected R, and strategy profitability belong to downstream strategy workflows and must not select engine leaves unless the prompt explicitly requests joint engine/strategy optimization.
- Treat a prompt's precision and coverage targets as a constrained frontier. Add coverage only while the final ensemble remains near the requested precision and false-positive budget. Do not raise coverage through repeated emissions, post hoc tolerance, or dense permissive triggers that miss the precision objective.
- Compare competing causal hypotheses and record their evidence, stability, failure cases, and rejection criteria in the requested `engine_research_rationale.md` before choosing thresholds or event rules.
- In the rationale, list all dataset ids and columns examined, selected lookbacks and transformations, causal availability checks, rejected hypotheses, and final production dependencies. The registry `required_data` must cover every source the engine actually needs rather than only the session's primary 5m label series.
- Score a candidate directly against the frozen fixed-R target using training episodes. Report emitted-episode count, matched-episode count, episode precision, bracket-count coverage, unmatched emitted episodes, matched-hard-negative episode rate, signals per emitted episode, raw signal count, timestamp coverage, monthly stability, cadence, drought, and overlap behavior. Report paired-strategy direction separately when required for compatibility; do not use it as a substitute for engine precision or coverage.
- Data mining and a bounded conditional tree are allowed. Do not force one universal rule when distinct causal regimes require distinct logic. The neutral engine may use OR-composed leaves to decide emit versus no event. Start with an anchor when available, then search for complementary leaves against the brackets the current final deduped ensemble still misses.
- Evaluate every candidate leaf by its marginal effect after OR composition, production-equivalent global dedupe, emitted-episode construction, and one-to-one matching: newly matched approved brackets, new matched emitted episodes, new unmatched emitted episodes, hard-negative episode hits, overlap, and complexity. Never sum standalone leaf metrics, dedupe only after leaf selection, or give repeated credit for multiple signals inside one episode.
- Apply precision, coverage, cadence, and chronological stability primarily to the complete ensemble. Each leaf must still express a coherent causal setup, use broad thresholds with perturbation checks, have minimum independent episode support outside a single unexplained calendar pocket, and add unique coverage in leave-one-leaf-out ablation. A causally known regime leaf may be absent or weak in some chronological blocks and need not independently meet the ensemble targets or be independently profitable.
- Choose tree complexity by the stability and target proximity of the final ensemble, plus each leaf's unique contribution. Track per-leaf independent-episode support, hard-negative episode rate, month/regime presence, emitted-episode overlap, threshold sensitivity, and marginal post-matching coverage. Merge redundant leaves and remove leaves whose gain disappears after global dedupe and one-to-one matching. Keep walk-forward sealed while using purged training-only chronological episode blocks for research and pruning.
- Keep outcome rows out of packets and implementation fixtures. Do not encode exact opportunity timestamps, episode ids, signal ids, exact dates, or date-specific branches in engine rules, tests, or rationale.
- Build only a neutral engine; any directional inference belongs in downstream strategy workflows and must not influence neutral trigger selection unless the prompt explicitly requests joint optimization. A paired base strategy may remain a minimal contract-compatible seed. The frozen target defines evaluation and must not be copied into live packet fields.
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
  - lagged required-data coverage: when a hard-aligned side channel is one or more bars behind raw candles, the engine must not evaluate that missing timestamp and must report `scan_coverage_end_ts` as the latest fully aligned evaluated timestamp
  - future-mutation invariance: for a representative timestamp `T`, mutate or append every source row with `available_at > T` and assert byte-identical trigger/no-trigger, selected leaves, dedupe admission, and emitted packet at `T`
   - repeated extension/cadence parity: a second generator call must respect existing dedupe state and not append signals inside the training dedupe window
   - packet neutrality
   - paired base strategy validates
   - canonical strategy context: wrap at least one real emitted training packet and one live-scan packet as `context["signal"]["payload"]` before calling `decide(context)`
   - engine/strategy compatibility: assert the paired strategy does not skip solely because of packet-shape assumptions such as active timeframe count, wrapper path, or missing legacy fields
   - packet consumer contract: assert emitted packets include `evidence.reference_price` and `evidence.trigger_candle_close`, and run the packet audit script on a representative packet
   - Stage 1 scorer compatibility: exercise the actual Stage 1 scoring path with a representative raw emitted packet artifact; treat its direction as interface verification, not an engine-quality gate
   - Stage 0A information gate can run on the generated pool and reports interpretable pass/fail/insufficient-sample metrics
   - Stage 0 signal-pool preparation still works where relevant
   - outcome-first matching deterministically constructs emitted episodes from the final globally deduped stream, performs one-to-one matching, and scores episode precision plus distinct bracket-count coverage against the prompt-defined objectives
   - episode-accounting tests prove that repeated signals inside one emitted episode or approved bracket cannot add precision or coverage credit, and that long brackets do not add independent fitting weight
   - chronological-validation tests keep complete episodes together and purge or embargo the target outcome horizon at block boundaries
   - conditional-tree tests prove deterministic OR semantics, episode-level marginal/ablation accounting, global dedupe, causal per-leaf support, threshold perturbation checks, and identical training/live behavior
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
- Proving only that packet fields exclude future rows while trigger routing, normalization, resampling, joins, or dedupe admission still use post-signal information.
- Reporting scan coverage from raw candle end or `context.end` when hard-aligned side channels lag. This falsely marks un-evaluated timestamps as scanned and can permanently miss live signals after delayed data arrives.
- Forward-filling hard-aligned 5m decision channels such as OI or futures metrics to make a missing timestamp look complete. If a source is meant to be as-of context, declare and test it as as-of context instead.
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
- Ignoring the prompt's bracket-count-coverage objective because a sparse leaf has attractive downstream directional accuracy. Direction is not neutral-engine coverage.
- Mixing units by scoring precision on emitted timestamps while scoring coverage on approved episodes. Construct emitted episodes first and use one-to-one episode/bracket matching for both primary metrics.
- Treating adjacent rows inside a long bracket as independent training samples or stability evidence. Aggregate or weight by episode and purge overlapping outcome horizons at chronological boundaries.
- Inflating bracket coverage by emitting repeatedly inside already-covered brackets, inventing tolerance, or adding permissive leaves whose unmatched signals violate the requested precision frontier.
- Requiring every regime leaf to meet the final ensemble's standalone stability or profitability targets. Evaluate aggregate stability while retaining causal support, perturbation, and unique-ablation guardrails per leaf.
- Forcing one oversized universal rule when multiple causal regimes are supported, or taking the opposite extreme of adding unsupported leaves until training labels are memorized.

## Final Checklist

- `validate_signal_engine_spec(...)` passes.
- `validate_signal_packet(...)` passes for emitted packets.
- `validate_strategy_module(base_strategy_path)` passes.
- Tests prove the entire emission decision is point-in-time safe: trigger/no-trigger, selected leaves, feature state, resampling/joins, dedupe admission, and packet evidence exclude observations unavailable at signal generation time.
- Tests prove lagged hard-aligned required data does not advance scan coverage past the latest fully aligned timestamp.
- A future-mutation test proves that changing or appending all rows with `available_at > T` cannot change the decision or packet at `T`.
- Repeated signal-pool extension preserves the same dedupe/cadence behavior as full-window training generation.
- Representative packet size is inspected; duplicate candle payloads are avoided unless explicitly needed for backward compatibility.
- A representative packet passes `scripts/audit_signal_packet_contract.py` and has standard reference price fields.
- Stage 0A information gate has been run, and weak/worse-than-random event pools are not pushed into Stage 1 without an explicit experimental reason.
- Final globally deduped signals are converted into deterministic production-reproducible emitted episodes and matched one-to-one against approved brackets with the evaluator-defined tolerance. Episode precision and distinct bracket-count coverage are the primary metrics; raw signal precision, timestamp coverage, signal count, signals per episode, hard-negative episode rate, aggregate stability, cadence, and drought are diagnostics.
- Fitting and chronological validation use episode-independent evidence: complete episodes remain in one block, duration-aware episode weighting follows the target contract when required, within-class duration ratios are preserved under class balancing, and the target outcome horizon is purged or embargoed at boundaries.
- Each engine leaf has documented causal features, independent episode support outside one unexplained calendar pocket, threshold sensitivity, unique post-dedupe and post-matching bracket coverage, emitted-episode overlap, episode-level ablation value, and training/live parity. The complete ensemble, rather than every leaf, satisfies the aggregate stability gate.
- Paired-strategy directional metrics are reported separately and are not used to excuse weak neutral-engine precision or bracket-count coverage.
- A representative emitted packet from the training generator and live scanner can be wrapped as `context["signal"]["payload"]` and passed to the paired `decide(context)` without being rejected for stale packet-shape reasons.
- The actual Stage 1 scorer path can consume a representative raw emitted packet artifact and call `decide(context)` with the canonical runtime signal wrapper.
- Strategy callers in Stage 1, backtests, promotion, and live execution use the same canonical wrapper shape.
- `GET /api/v1/signal-engines` returns the new engine after backend restart, even with zero signal sets.
- The v2 Engines tab can list/select the new engine.
- Existing engines keep their old behavior unless the user explicitly asked for a behavior change.
- New engine can be selected independently in research/trading flows by its engine id.
