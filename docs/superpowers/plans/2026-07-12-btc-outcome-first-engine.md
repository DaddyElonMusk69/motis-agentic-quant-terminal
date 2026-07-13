# BTC Episode-Level Signal Engine Research Decision

**Goal:** Determine whether authorized BTC training episodes support a causal neutral engine near 80% emitted-episode precision and 80% distinct approved-bracket coverage.

**Decision:** Research gate failed. Do not implement or register an engine for session `discovery-btc-2025-03-01-2026-05-30-mrhdyqd1`.

## Completed Work

- [x] Replaced mixed timestamp/bracket scoring with deterministic emitted episodes and one-to-one episode/bracket matching.
- [x] Fixed the episode rule at six quiet 5m intervals from the frozen bracket policy; no outcome-tuned grace window or second cooldown is used.
- [x] Verified the evaluator oracle supports 100% episode precision and 91.52% bracket coverage, so roughly 80/80 is structurally feasible.
- [x] Weighted each approved bracket and each contiguous matched hard-negative episode as one independent fitting unit.
- [x] Kept complete episodes together and purged the 36-hour outcome horizon at internal chronological boundaries.
- [x] Re-ran compression/release, OI, sweep, bounded-tree, Extra Trees, leaf-budget, capacity, sensitivity, monthly, block, cadence, and ablation diagnostics.
- [x] Preserved all walk-forward, validation, locked OOS, live outcomes, and post-cutoff rows as sealed.

## Build Gate Result

The old OI anchor rescored at 43.36% episode precision and 8.66% bracket coverage.

The best balanced purged OOF model scored 25.27% episode precision and 37.81% coverage with episode-weighted AUC 0.5191. The best balanced full-training bounded tree scored 33.73% precision and 35.34% coverage. Larger trees raised coverage only by emitting many unmatched episodes; a representative 512-leaf/minimum-25 tree scored 20.52% precision and 75.09% coverage.

The prior 81.67% precision / 79.86% coverage result is retired because its precision numerator and denominator were deduped timestamps while coverage was counted in distinct brackets. It did not measure the requested episode-level objective.

## Closed Implementation Tasks

- [x] Do not create candidate contract tests; no candidate passed the episode-level research gate.
- [x] Do not create a worker adapter or paired strategy.
- [x] Do not modify `artifacts/signal_engine/engine_registry.json` for this session.
- [x] Do not run packet, Stage 1, API catalog, or live scanner verification for a nonexistent candidate.

The complete episode contract, evidence inventory, causal proof, frontier, stability diagnostics, and rejection rationale are recorded in `dev/signal_discovery_sessions/discovery-btc-2025-03-01-2026-05-30-mrhdyqd1/prompt/engine_research_rationale.md`.
