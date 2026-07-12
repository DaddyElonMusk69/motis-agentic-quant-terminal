# BTC Outcome-First Signal Engine Design

## Scope

Research session `discovery-btc-2025-03-01-2026-05-30-mrhdyqd1` using only its frozen training artifacts and manifest-authorized market rows through `2026-03-31T23:55:00Z`. Produce either one registered neutral engine plus a paired directional strategy, or an evidence-backed rejection. Held-out and walk-forward artifacts remain sealed.

## Research Design

Start with three causal mechanism families:

1. Compression followed by price and participation release.
2. Open-interest flush or trap reversal near a recent liquidity extreme.
3. Candle-only range expansion or sweep as a baseline.

If these families fail, broaden the search to causal trend, volatility, volume, OI level/change/acceleration, and cross-feature interactions derived from authorized raw rows. Broader search does not relax the gates: every selected leaf needs an economic interpretation, independent episode support, chronological recurrence, nearby-threshold stability, and positive out-of-leaf ablation value.

Approved brackets are opportunity regions. Candidate matching uses the final deduped availability timestamps with zero added tolerance. Selection prioritizes opportunity precision, expected net R per signal, low matched-hard-negative false positives, monthly and chronological-block stability, and acceptable droughts. Episode recall is diagnostic only.

## Causal Data Discipline

Raw confirmed OKX 5m candles and raw Binance 5m open interest are the preferred production sources. Features are trailing or backward-looking and are computed only from rows available at or before each signal timestamp. Higher-timeframe aggregates are closed by availability time. Derived registered datasets may corroborate hypotheses but become production dependencies only if their timestamp semantics and reproducibility are proven.

No outcome label, bracket boundary, episode id, signal id, exact opportunity timestamp, or date-specific branch enters production code, tests, packets, or rationale.

## Candidate Shape

If evidence survives, implement one distinct engine id with a bounded OR tree. A shared row scanner and packet builder serve historical and live generation. Packets are neutral `signal_packet.v2` evidence with standard reference price and availability fields. Directional inference is isolated in the paired `decide(context)` strategy. Dedupe state accepts the runtime seed timestamp so full generation and extension have identical cadence.

## Verification

Tests are written first for registry validation, training/live parity, point-in-time safety, repeated-extension cadence, packet neutrality and consumer fields, paired strategy validation, canonical wrapper behavior, and Stage 1 scoring compatibility. The final training stream is evaluated directly against the frozen fixed-R target and reported by leaf, month, chronological block, hard negatives, cadence, drought, overlap, ablation, and threshold perturbation.

