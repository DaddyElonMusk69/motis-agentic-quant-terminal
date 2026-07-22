# Supervised Signal Engine Training Contract

Status: canonical. This document defines the supervised-learning problem for outcome-labeled signal engines and is enforced by the signal-engine-builder skill and generated training handoff.

## Purpose

Train a causal model that answers one question at every eligible five-minute decision timestamp:

> Given all market information available at or before this timestamp, does entering from this timestamp satisfy the frozen fixed-R outcome target?

The model does not discover or recalculate the future outcome. Outcome generation is completed before training. The model learns to predict the supplied timestamp truth.

This is a timestamp-classification problem with time-series inputs. Exact timestamp labels are the complete supervised truth; do not derive episodes or brackets to replace, filter, group, or reweight them.

## Canonical Training Object

For every eligible five-minute decision timestamp `t`:

```text
X_t = ordered market history whose available_at is less than or equal to t
y_t = the frozen target truth for entering from t
```

The primary model output is:

```text
p_t = P(y_t = 1 | X_t)
```

At a fixed operating threshold `q`:

```text
prediction_t = 1 when p_t >= q
prediction_t = 0 otherwise
```

The model must produce a score for every eligible timestamp. Dedupe, refractory periods, and signal-cadence policies are downstream operational transformations and are not part of the primary supervised objective or primary model evaluation.

## Label Contract

The fixed-R target contract owns label generation. Its risk, reward, stop, entry delay, entry semantics, costs, and outcome horizon are frozen before model research.

For a neutral opportunity model:

- `y_t = 1` when at least one permitted direction reaches the frozen reward barrier before its stop barrier within the complete horizon.
- `y_t = 0` when neither permitted direction qualifies.
- `AMBIGUOUS` timestamps are excluded from loss and primary evaluation unless a later contract explicitly defines a separate class.
- Timestamps without a complete forward outcome horizon are excluded.
- LONG and SHORT may both map to neutral opportunity label `1`. Direction is a separate downstream target and must not alter the neutral opportunity label.
- There is no nearest-episode tolerance. The label belongs to that exact decision timestamp.

Every eligible timestamp must be present in the supervised dataset. Ordinary negative timestamps must not be discarded merely because they were not selected as hard negatives.

Required timestamp-label fields:

```text
decision_ts
label                 # 1 or 0 for eligible rows
label_status          # ELIGIBLE, AMBIGUOUS, or INCOMPLETE
entry_ts
horizon_end_ts
target_config_hash
```

Optional diagnostic fields may include direction-specific outcome states, but they must never become model inputs.

## Timestamp Weighting

Do not construct opportunity episodes or filtered brackets for supervised fitting. Every eligible timestamp has unit base weight, so a contiguous run with `N` positive timestamps naturally contributes `N` units of positive training mass.

Example:

```text
10-minute positive run = 2 timestamps   = 2 units of mass
10-hour positive run   = 120 timestamps = 120 units of mass
```

This is the required duration-aware behavior. Do not normalize contiguous runs to equal total weight and do not add another duration multiplier, because that would count duration twice.

If a future session wants capped, square-root, or other nonlinear duration weighting, that weighting must be frozen explicitly before training and treated as a new target/training contract version.

Hard negatives are diagnostic and sampling aids. They may be oversampled during minibatch construction, but ordinary negatives remain part of the objective. Any oversampling must use correction weights so reported probabilities and metrics reflect the natural timestamp distribution.

## Data Discovery And Selection

Before defining model inputs, inventory every authorized same-asset dataset available for the ticker. For every channel, record:

- data type, source, origin, and timeframe;
- first and last available timestamps;
- missingness and gap behavior;
- timestamp, close-time, settlement-time, and `available_at` semantics;
- backfill and live-fetch continuity;
- whether derived values can be reproduced identically in training and live execution.

Do not inherit a legacy channel list automatically. Select from all available channels, then freeze:

- dataset ids and source priority;
- channel order;
- resolutions;
- transforms and normalization;
- missingness and age masks;
- sequence lengths and pooling rules.

Changing any selected channel, ordering, transform, or sequence definition requires a new feature schema and retraining.

Current BTC candidate channels include:

- confirmed candles and volume;
- open interest and OI notional;
- top-trader account long/short ratio;
- top-trader position long/short ratio;
- global account long/short ratio;
- taker buy/sell volume ratio;
- top-trader versus global long-share gaps;
- premium-index candles;
- settled funding events and causal funding state;
- explicit source coverage, missingness, and data-age masks.

Open interest already has its own established derived features. New training may consume those features where useful, but it must not silently duplicate inconsistent OI calculations.

## Historical Span

There are three different time boundaries. They must not be conflated:

1. `data_warmup_start`: earliest market history available to construct sequences.
2. `label_start`: first timestamp allowed to contribute supervised loss.
3. `research_end`: final market-data boundary before sealed walk-forward.

Current BTC default:

```text
data_warmup_start = 2023-01-01T00:00:00Z
label_start       = 2024-01-01T00:00:00Z
research_end      = 2026-04-30T23:55:00Z
walk_forward_start = 2026-05-01T00:00:00Z
walk_forward_end   = 2026-07-10T23:55:00Z
```

The 2023 history is sequence warmup. It gives the first 2024 labeled timestamp approximately one year of market context. It is not automatically labeled training history.

If complete, point-in-time-safe channels become available earlier, move both warmup and label boundaries earlier under a new frozen session. Do not shorten warmup merely to create more labeled rows.

### Final Eligible Label

Features may extend through `research_end`, but a supervised label is eligible only when its complete outcome is contained before sealed walk-forward:

```text
horizon_end_ts < walk_forward_start
```

The terminal must compute this boundary from entry delay, entry semantics, source interval, and outcome horizon. For a 48-hour target followed by a May 1 walk-forward start, the last eligible decision is approximately April 28. April 29-30 may remain feature history but cannot contribute outcome labels that inspect May.

## Sequence Input Contract

The primary model must consume ordered causal sequences. A single engineered snapshot may be retained as a baseline, but it is not the intended final architecture.

Recommended initial logical context:

| Branch | Minimum logical history | Purpose |
|---|---:|---|
| 5m | 7 days | immediate structure, volatility, volume, OI, premium, and positioning transitions |
| 15m | 30 days | multiweek buildup, compression, and derivatives-state persistence |
| 1h | 90 days | swing structure and medium-term market regime |
| 4h | 365 days | cycle context and long derivatives regimes |
| 1d | all available history from warmup start | structural location and long-term regime |
| funding events | 365 days | settled funding sequence without repeating one event as independent 5m observations |

These are logical lookbacks. Implementations may use deterministic causal pooling or fixed temporal bins to control memory, but they must preserve ordering and document exactly which source intervals each encoded step represents.

Every sequence ends at the decision timestamp. No source observation may enter a sequence before it becomes available.

Required sequence rules:

- Higher-timeframe completed rows are available only at their close or publication time.
- Forming higher-timeframe state may be constructed only from confirmed lower-timeframe rows available at `t` and must remain distinguishable from completed state.
- No centered windows, backward fill from future observations, or full-series normalization.
- Missing values use explicit masks and source-age features. Do not silently convert missing derivatives values into economically meaningful zeroes.
- Rolling normalization must use only prior/current observations. Alternatively, scaler parameters must be fitted on the training fold only.
- Offline training and runtime inference must use the same sequence builder and frozen schema.

## Initial Model Architecture

The first serious implementation should use a multiresolution causal temporal convolutional network (TCN):

```text
5m sequence --------> causal temporal encoder ---+
15m sequence -------> causal temporal encoder ---|
1h sequence --------> causal temporal encoder ---|--> fused state --> one opportunity logit
4h sequence --------> causal temporal encoder ---|
1d sequence --------> causal temporal encoder ---|
funding events -----> causal temporal encoder ---+
```

Reasons for choosing a TCN first:

- it preserves ordered temporal information;
- dilated causal convolutions provide long receptive fields;
- it is more data-efficient than a large transformer;
- training and live inference are deterministic and practical;
- it supports independent resolution branches and missingness masks.

A transformer, recurrent model, or state-space model may be compared later, but it must beat the TCN under the same timestamp labels, splits, and evaluation contract. Do not select architecture from sealed walk-forward results.

The model has one neutral binary head. Directional prediction, if later required, uses a separate head or downstream strategy and separate evaluation.

## Training Dataset Construction

For every eligible timestamp from `label_start` through the final complete label:

1. Build the multiresolution sequence ending at `decision_ts`.
2. Attach the exact frozen timestamp label.
3. Attach optional hard-negative and regime flags for diagnostics or corrected sampling.
4. Reject the sample if required causal availability checks fail.

All eligible `0` and `1` timestamps participate. Do not train only on approved brackets plus selected hard negatives.

Because neighboring timestamps overlap heavily, storage may use indexed sequence views rather than materializing duplicate arrays. This optimization must not change the logical sample set.

## Chronological Validation

Use expanding-window chronological validation inside the research period.

Default schedule rules:

- minimum initial training history: six labeled months;
- validation block: three calendar months;
- step size: three calendar months;
- minimum purge before each validation block: target horizon plus entry delay;
- exclude any training example whose outcome window overlaps validation;
- use at least three folds; prefer five or more when history allows.

Each fold fits every learned component from its training slice only:

- model weights;
- imputation state;
- normalization state;
- class or sampling weights;
- probability calibration.

Use multiple deterministic seeds for the final candidate configuration. Report mean, median, and worst-fold results rather than selecting the luckiest seed.

After architecture, hyperparameters, epoch count, calibration, and operating threshold are frozen from internal research folds, fit one final artifact on all eligible research labels. That final refit may use the complete labeled training span. It has no new unbiased performance estimate until sealed walk-forward evaluation.

## Loss And Optimization

Default loss is binary cross-entropy over the natural eligible timestamp population.

Default base sample weight is `1` for every eligible timestamp. This automatically gives longer positive runs more total influence.

Do not automatically force positive and negative classes to equal total mass. If imbalance prevents learning, use a documented weighted loss or corrected sampler, then calibrate probabilities on natural-prevalence validation rows.

Focal loss may be compared when hard examples dominate, but it is a model hyperparameter and must be selected only through internal chronological folds.

Initial optimization guidance:

- maximum 100 epochs per fold;
- early stopping based on validation PR-AUC, with precision and recall reported at the current operating policy;
- patience of 8-12 evaluation checkpoints;
- retain the best checkpoint from training-only/internal-validation evidence;
- gradient clipping and deterministic seeds;
- at least three seeds for the chosen configuration before final refit.

These are limits, not success criteria. Training stops when validation evidence stops improving, not when a target wall-clock time is reached.

For the final full-research refit, use the robust epoch count selected across folds, such as the median best epoch. Do not monitor sealed walk-forward to stop training.

## Probability Calibration And Threshold Selection

The model must output a score for every eligible timestamp. Calibrate probabilities using only internal out-of-fold or validation predictions under natural class prevalence.

Always report the complete precision-recall frontier. Threshold selection requires a frozen policy:

- When the session declares a minimum precision, select the threshold with the highest recall that satisfies that precision and stability requirements.
- When the session declares a minimum recall, select the threshold with the highest precision that satisfies that recall and stability requirements.
- When neither is declared, use maximum timestamp-level F1 on pooled purged out-of-fold predictions as the default operating point, while still publishing the full frontier.

Minimum monthly support and broad chronological recurrence may be frozen as additional constraints. Do not select a threshold from one favorable month or from sealed walk-forward.

## Primary Evaluation

Primary evaluation is performed on raw timestamp predictions before dedupe.

Let:

```text
P  = all eligible timestamps with truth 1
S  = all eligible timestamps predicted 1
TP = intersection(P, S)
FP = S minus P
FN = P minus S
```

Primary metrics:

```text
timestamp precision = TP / (TP + FP)
timestamp recall    = TP / (TP + FN)
F1                  = 2 * precision * recall / (precision + recall)
```

Required reporting:

- eligible timestamp count;
- positive and negative prevalence;
- TP, FP, TN, and FN;
- timestamp precision and recall;
- F1 or the session-frozen F-beta;
- PR-AUC;
- calibration error and reliability curve;
- precision-recall threshold frontier;
- signals per day and positive predictions per month;
- monthly and broad-regime stability;
- block-bootstrap confidence intervals using chronological blocks.

Accuracy and ROC-AUC may be reported as diagnostics but must not replace precision, recall, or PR-AUC.

## Positive-Run Diagnostics

Primary scoring remains timestamp based. Optional diagnostics may group adjacent positive timestamps into run-length buckets for reporting only, without persisting episode identities or using those groups in fitting, splitting, threshold selection, or weighting. Timestamp recall is the required duration-weighted coverage metric:

```text
duration-weighted positive coverage = total TP timestamps / total truth-1 timestamps
```

Also report recall by positive run-length bucket and concentration by chronological block when useful. Do not replace timestamp precision or recall with one-to-one matching or one-hit-per-run credit.

## Operational Signal Policy

After the raw model is accepted, separately evaluate optional production rules such as:

- minimum spacing between emitted packets;
- one packet per model-state transition;
- hysteresis with separate enter/exit thresholds;
- maximum packet rate.

Operational-policy metrics must be clearly labeled and reported alongside, not instead of, raw timestamp metrics. A dedupe policy changes the emitted event stream; it does not retroactively change the supervised model's timestamp precision or recall.

## Baselines And Acceptance

Required baselines use the same eligible timestamps:

- natural positive prevalence;
- random predictions at the model's positive prediction rate;
- fixed-cadence sampling as an operational diagnostic;
- a simple tabular logistic or tree baseline using causal engineered features.

A sequence candidate is not accepted merely because training loss decreases. It must show:

- PR-AUC above the natural-prevalence baseline across internal chronological folds;
- useful precision and recall at a predeclared operating policy;
- positive lift over simple causal baselines;
- recurrence across months and regimes;
- no dependence on one calendar pocket or one seed;
- identical causal sequence construction in training and runtime.

Exact numeric acceptance thresholds belong in the frozen session configuration. They must be declared before sealed walk-forward is inspected.

## Sealed Walk-Forward

The walk-forward period is never used for:

- feature or channel selection;
- model architecture selection;
- hyperparameter or seed selection;
- epoch count or early stopping;
- calibration;
- threshold selection;
- operational dedupe selection.

After the full-research artifact and operating policy are frozen, the terminal evaluates walk-forward once. A failed walk-forward is a rejected model, not permission to retune against the same period.

## Reproducibility Artifacts

Every training run must persist:

- frozen target config and hash;
- authorized evidence manifest and dataset ids;
- data coverage and missingness report;
- ordered feature/channel schema;
- sequence lengths, resolution rules, and pooling specification;
- label counts and label-status counts;
- positive-run duration distribution when reported as a diagnostic;
- exact chronological fold definitions and purge;
- model architecture and hyperparameters;
- optimizer, loss, seeds, epochs, and early-stopping history;
- out-of-fold scores for every eligible research timestamp;
- precision-recall frontier and selected operating policy;
- baseline results;
- final artifact hash;
- proof that sealed walk-forward was not inspected during fitting.

The training script must be rerunnable from canonical market data and frozen labels. An ephemeral notebook or terminal experiment is not a completed training run.

## Non-Conforming Designs

The following do not implement this contract:

- training only on positive brackets and selected hard negatives;
- treating each contiguous positive run as total weight one;
- adding a second duration multiplier when all timestamps already have unit weight;
- using one tabular snapshot as the final model without testing temporal sequences;
- applying an eight-hour refractory before primary timestamp evaluation;
- reporting one-to-one episode precision instead of timestamp precision;
- selecting thresholds from the same sealed period used to claim performance;
- using labels or source observations whose outcome/availability crosses the split boundary;
- fitting normalization on the complete series before chronological splitting;
- judging success from training loss or in-sample hit rate.

## Session Creation Checklist

Before starting the next BTC supervised session, confirm:

- raw and derived channel inventory begins no later than January 2023;
- sequence warmup begins January 2023;
- supervised labels begin January 2024 unless a different warmup is justified;
- research data ends April 30, 2026;
- incomplete labels near the boundary are excluded by `horizon_end_ts < walk_forward_start`;
- May-July 2026 walk-forward remains sealed;
- full eligible timestamp labels are authorized, including ordinary negatives;
- ambiguous/incomplete masks are available;
- no episodes or filtered brackets are constructed for fitting;
- raw timestamp precision and recall are the primary metrics;
- multiresolution causal sequence construction is shared by training and runtime;
- the operating threshold policy is frozen before walk-forward evaluation.
