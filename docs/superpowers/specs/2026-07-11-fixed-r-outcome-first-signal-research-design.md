# Fixed-R Outcome-First Signal Research

## Status

Approved research concept. This document preserves the research thesis and its boundaries. It intentionally does not prescribe implementation architecture, commands, storage formats, model classes, or an exact parameter grid.

## Objective

Research a BTC entry process that can identify trade opportunities approximately daily or almost daily, with a maximum holding horizon of 36 to 48 hours.

The system will allow only one open position per asset. Pyramiding is outside the scope of this research iteration. The immediate objective is entry opportunity discovery and directional entry selection, not position management.

## Motivation

The previous engine-first process begins with a hypothesized market event and then asks whether its timestamps contain useful future movement. That can produce engines which select volatility but do not offer a clean directional payoff. Maximum absolute excursion is especially insufficient because it rewards movement in either direction and does not require a target to be reached before an adverse stop.

The new process reverses the order:

1. Define the future price paths that would have constituted acceptable trades.
2. Locate and organize those paths as opportunity episodes.
3. Audit only causal, point-in-time features at those opportunities.
4. Derive a neutral signal engine that detects recurrence of the opportunity state.
5. Derive a separate strategy that predicts LONG, SHORT, or no entry.

This gives signal-engine research an explicit outcome target instead of asking an arbitrary engine to prove itself after construction.

## Fixed-R Opportunity Definition

`R` is a fixed percentage of entry price. It is not adjusted by volatility or market regime. Once selected, the same `R` applies to every evaluated entry.

For each eligible timestamp, the research process evaluates a symmetric 2:1 reward-to-risk opportunity:

- LONG target: entry price plus `2R`
- LONG stop: entry price minus `1R`
- SHORT target: entry price minus `2R`
- SHORT stop: entry price plus `1R`

The path label is determined by barrier order, not eventual maximum excursion:

- `LONG`: the LONG target is reached before the LONG stop.
- `SHORT`: the SHORT target is reached before the SHORT stop.
- `NEUTRAL`: neither direction produces a qualifying target-before-stop path within the horizon.
- `AMBIGUOUS`: available candle resolution cannot establish barrier order.

The primary horizon is 36 hours. A 48-hour horizon is used as a sensitivity check and possible timeout extension. A timestamp's reference entry must be executable using information available at that time; future MAE or MFE must never determine its TP or SL levels.

## Opportunity Episodes

Every eligible timestamp is labeled independently. Contiguous timestamps with the same qualifying direction form one opportunity episode.

Any qualifying timestamp inside an episode is an acceptable entry target. The future signal engine is not required to reproduce the earliest oracle timestamp or land within an arbitrary tolerance around it. A signal emitted at a timestamp is judged from its own executable entry and its own target-before-stop path.

Concentration of qualifying timestamps is desirable when it represents a coherent market state that a later engine can detect. The research must nevertheless distinguish a recurring causal state from one isolated historical move.

Timestamp count is not opportunity count. A long trend containing hundreds of qualifying 5-minute timestamps may still represent only one independent entry episode. Feature analysis may use the shape of the entire episode, but frequency and statistical evidence must be reported at episode level.

## Selecting R

The research will evaluate a small, predeclared family of economically plausible fixed-percentage `R` values. The exact grid belongs to the subsequent implementation design.

`R` is selected using research-training data only. The process seeks a contiguous feasible region of nearby `R` values, not the single historical optimum. A feasible region should produce enough independent episodes for the intended opportunity profile while remaining viable after fees, slippage, delayed entry, and candle ambiguity.

The selected `R` is frozen before walk-forward evaluation. Walk-forward data may confirm or reject the frozen choice but may not be used to retune it.

If no neighboring `R` values produce a credible opportunity population, the fixed-R thesis is rejected rather than rescued with regime-specific tolerances or a larger parameter search.

## Causal Feature Discovery

After `R` is frozen, the research audits market information available at each timestamp. Candidate evidence may include:

- price returns, trend, and higher-timeframe structure;
- volatility, compression, and expansion state;
- volume and participation changes;
- open-interest level, change, acceleration, and normalized extremes;
- price and open-interest interactions;
- taker-flow and positioning ratios;
- funding or basis when canonical point-in-time data is available.

The research compares opportunity episodes with hard negative timestamps drawn from comparable market conditions. Exact dates, signal identifiers, and post-event features are prohibited as engine rules.

The desired result is a small recurring causal mechanism or feature-state bucket. Uniform opportunity frequency across every month is not required; regime concentration is acceptable. The mechanism must, however, recur in multiple independent periods rather than derive its apparent strength from one continuous historical episode.

## Engine and Strategy Responsibilities

The production signal engine, if research supports one, will emit neutral evidence that an opportunity state is present. It will not emit direction, confidence, TP, SL, sizing, leverage, or order intent.

The paired entry strategy will use the packet's causal evidence to choose LONG, SHORT, or no entry. Directional performance will be evaluated only on timestamps actually emitted by the frozen engine.

The engine and strategy are separate research claims:

- Engine claim: causal information can detect outcome-qualified opportunity episodes.
- Strategy claim: conditional on a detected episode, causal information can select a direction with positive expected net `R`.

Passing the engine claim does not imply that the strategy claim passes.

## Evaluation Principles

Research evaluation must preserve these rules:

- Split training and sealed walk-forward data before selecting `R` or features.
- Use barrier order and executable entry semantics rather than hindsight MFE direction.
- Treat contiguous qualifying timestamps as correlated members of one episode.
- Report independent episodes separately from raw qualifying timestamps.
- Judge emitted signals by their own realized 2R-before-1R path; do not use nearest-oracle timestamp tolerance.
- Include fees, slippage assumptions, entry-delay sensitivity, timeout outcomes, and ambiguous candles.
- Use purging or embargo appropriate for overlapping 36-to-48-hour future windows.
- Require stability across neighboring `R` values and multiple independent periods.
- Run a final sequential replay with one position per asset and no overlapping entries.

The main economic metric is expected net return in `R`, supported by opportunity coverage, precision, direction accuracy, payoff ratio, timeout rate, and episode-level stability. Raw timestamp classification accuracy is not sufficient.

## Research Sequence

The approved sequence is:

1. Fixed-R opportunity atlas and episode feasibility research.
2. Freeze a feasible `R` using training evidence.
3. Causal feature audit and opportunity-state discovery.
4. Neutral signal-engine hypothesis and deterministic rule construction.
5. Directional entry-strategy research on frozen engine timestamps.
6. Sealed walk-forward evaluation.
7. Sequential one-position expectancy evaluation.

No production signal engine should be built before the opportunity atlas establishes that a credible fixed-R target population exists.

## Non-Goals

This concept does not cover:

- implementation architecture or task planning;
- modifications to the current 4-hour engine;
- dynamic or volatility-adjusted TP/SL;
- regime-specific TP/SL buckets;
- pyramiding or position-management rules;
- leverage, sizing, or portfolio allocation;
- order-book depth data;
- production deployment or live routing;
- changes to Stage 0, Stage 1, or execution contracts.

Those subjects may be designed later after the fixed-R opportunity thesis is tested.

## Decisions Preserved

- Research begins from desired trade outcomes rather than a preselected engine pattern.
- `R` is a fixed percentage, not volatility-adjusted.
- The target path is 2R before 1R.
- The main horizon is 36 hours, with 48 hours as sensitivity.
- Any qualifying timestamp inside an opportunity episode is an acceptable entry.
- Exact timestamp-matching tolerances are unnecessary.
- Episodes, not raw timestamps, are the independent opportunity unit.
- One position per asset is assumed.
- Pyramiding is excluded from this iteration.
- The opportunity atlas is built before causal engine discovery.
