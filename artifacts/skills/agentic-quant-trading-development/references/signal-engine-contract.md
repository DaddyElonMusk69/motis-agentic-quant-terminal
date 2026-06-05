# Signal Engine Contract

The signal engine is deterministic infrastructure.

It may:

- normalize candles
- derive higher timeframes
- build replay and live market snapshots
- compute indicators
- detect neutral signal conditions
- emit model-facing packets
- enforce scanner dedupe and runtime safety gates
- update live candle cache needed by live scanners
- route live wakes based on deterministic runtime state

It must not:

- choose trade direction
- set entry, TP, SL, leverage, or size
- rank signals using future outcomes
- encode strategy skill judgment
- change rules to improve backtest win rate
- own historical data fetching or training data prep
- own Stage 0, Stage 2, Stage 3, or Stage 4 optimization scoring

## Packet Contract

Packets should include neutral market evidence only:

- asset
- timestamp
- active timeframes
- indicator interactions
- chart context

Packets should exclude:

- direction
- entry verdict
- confidence
- TP or SL
- leverage
- position sizing
- model score

Replay and live packet schemas should match.

## Tool Ownership

Signal generation and live runtime scripts live under `artifacts/signal_engine/scripts/`.
Historical data fetch/build scripts and optimization/scoring scripts live under this
development skill's `scripts/` directory.
