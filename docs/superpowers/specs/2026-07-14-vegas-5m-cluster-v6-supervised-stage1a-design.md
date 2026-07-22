# Vegas 5m Cluster v6 Supervised Stage 1A Design

Status: implemented as an experimental BTC Stage 1A draft. It is not approved for promotion.

## Objective

Replace the mutable BTC v6 session's hand-authored directional rules with a supervised policy while preserving the live execution decision contract. The v6 signal engine and its causal packet builder remain unchanged.

The policy answers only whether a v6 signal should enter long, enter short, or skip. Position sizing, exits, protection, and trade management remain outside Stage 1A.

## Safety Boundary

- Fit only from `builder_training_sample.json` labels with `sample_method=training` and visible training truth.
- Never read walk-forward, validation, locked OOS, later candles, timestamps as features, signal ids, or raw price levels.
- Use the same `extract_packet_features(...)` implementation in offline fitting and runtime inference.
- Reject packet rows whose availability timestamp is later than the packet timestamp.
- Keep the existing v6 engine, base strategy, and signal packet shape unchanged.

## Feature Planes

The extractor exposes a broader stationary research plane than the initial model consumes.

The initial active set has 31 features:

- EMA-cluster vote count and six matched-period indicators.
- 5m return, range, close location, 12-row range position, and EMA stack spread.
- Completed return, causal forming return, 12-row range position, and EMA stack spread for 2h, 8h, and 12h.
- Completed/forming 1d Bollinger position, z-score, bandwidth, and position delta.

Open-interest regime fields remain extracted and visible in decision diagnostics but are not active in the first model. This separates feature availability from model membership, so OI can be evaluated later without changing signal packets or retraining data plumbing.

Excluded inputs include raw prices, raw EMA levels, dates, ids, label-derived values, and post-signal outcomes.

## Model

The artifact contains two class-balanced logistic heads:

1. Entry head: `P(ENTERABLE)` versus natural no-trade truth.
2. Direction head: `P(LONG | ENTERABLE)` versus short, fitted only on directional labels.

Runtime scores are:

```text
score_long  = p_enter * p_long_given_enter
score_short = p_enter * (1 - p_long_given_enter)
score_skip  = 1 - p_enter
```

The policy enters the larger directional score when it meets the fixed artifact threshold. Otherwise it emits `SKIP + FLAT`. It also skips when active-feature missingness exceeds the artifact limit.

The initial threshold is fixed at `0.30`; it was not selected by a training threshold sweep.

## Portable Artifact

`model_artifact.json` stores:

- model and feature schema versions;
- ordered active, observed, and diagnostic feature names;
- per-head intercepts and coefficients;
- training-only imputation medians, means, and scales;
- entry and missingness thresholds;
- training label counts and window metadata.

`sklearn` is used only by the offline trainer. Live inference is pure Python and has an exact probability-parity test against the fitted estimator.

## Runtime Contract

The strategy continues to return exactly:

- `decision_id`
- `strategy_id`
- `strategy_version`
- `signal_id`
- `trade_action`
- `action`
- `direction`
- `confidence`
- `reason_code`
- `execution_profile`
- `diagnostics`

Valid combinations remain `ENTER + LONG`, `ENTER + SHORT`, and `SKIP + FLAT`.

Diagnostics include model probabilities, combined scores, threshold, direction margin, missing active features, active feature names, and compact Bollinger/OI snapshots.

## Packaging

The shared extractor and runtime live in `quant_terminal_strategies`. The fitted model is a sidecar next to the mutable session `strategy.py`.

Stage 1 frozen snapshots already copy the full strategy directory. Execution-bundle promotion now also hashes and copies `model_artifact.json`, including through a Stage 4B timing wrapper, so research and live inference load the identical artifact.

## Initial Training Readout

The BTC training bundle contains 2,606 signals: 1,116 long, 1,115 short, and 375 natural no-trade labels.

- Full training: 681 match, 560 mismatch, 1,365 neutral; 54.8751% directional agreement on 1,241 entered decisions.
- Expanding forward months: 397 match, 504 mismatch, 906 neutral; 44.0622% directional agreement on 901 entered decisions.

The unstable forward result means this artifact is infrastructure-complete but not promotion-worthy. Walk-forward and locked OOS were not read or used.
