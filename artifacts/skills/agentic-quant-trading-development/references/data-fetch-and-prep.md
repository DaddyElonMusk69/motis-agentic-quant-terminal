# Data Fetch And Prep

Data quality is the first gate.

No optimization stage may start until the asset has a valid data manifest.

## Required Raw Data

Default raw data is `5m` OHLCV candles:

```text
dev/data/raw/<ASSET>/5m/candles.csv
```

Required columns:

```text
ts,open,high,low,close,volume,vol_ccy,vol_ccy_quote,confirm
```

Column rules:

- `ts`: UTC ISO timestamp ending in `Z`
- OHLC fields: numeric strings or numbers
- `confirm`: `1` for closed candles
- rows sorted by `ts`
- no duplicate timestamps

## Derived Timeframes

Rebuild derived candles from raw `5m` data:

- `5m`
- `2h`
- `4h`
- `8h`
- `12h`
- `1d`

Use UTC bucket boundaries. Do not mix exchange-native HTF candles with locally derived HTF
candles unless the data manifest explicitly records the source difference.

Bucket starts:

- `2h`: `00:00`, `02:00`, `04:00`, ...
- `4h`: `00:00`, `04:00`, `08:00`, ...
- `8h`: `00:00`, `08:00`, `16:00`
- `12h`: `00:00`, `12:00`
- `1d`: `00:00`

## Data Manifest

Every asset must have:

```text
dev/data/manifests/<ASSET>.json
```

The manifest records source, date range, row counts, gaps, derived timeframes, and validation
status.

Minimal shape:

```json
{
  "asset": "BTC",
  "schema_version": "0.1",
  "source": "okx",
  "raw_5m_path": "dev/data/raw/BTC/5m/candles.csv",
  "derived_timeframes": ["5m", "2h", "4h", "8h", "12h", "1d"],
  "start_ts": "2023-01-01T00:00:00Z",
  "end_ts": "2026-01-01T00:00:00Z",
  "row_counts": {
    "raw_5m": 0,
    "5m": 0,
    "2h": 0,
    "4h": 0,
    "8h": 0,
    "12h": 0,
    "1d": 0
  },
  "gaps": [],
  "validation_status": "valid"
}
```

## Validation

Before signal generation:

- timestamps are sorted and unique
- candles are confirmed
- no unexpected 5m gaps unless documented
- derived HTFs are rebuilt after raw updates
- history is long enough for indicator warmup

For EMA-style strategies with slow daily EMAs, confirm whether the available history can
warm the longest period. If not, record the earliest valid timestamp or explicitly exclude
under-warmed timeframes from voting.

## Canonical Tools

Historical data tooling belongs to this development skill, not the signal engine.

Fetch raw OKX `5m` history when needed:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/data/fetch_okx_5m_history_cli.py \
  --asset BTC \
  --out /tmp/BTC_5m.csv
```

Build canonical `dev/data` from a raw `5m` CSV:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/data/build_training_data.py \
  --asset BTC \
  --source okx \
  --source-file /tmp/BTC_5m.csv
```

Build or refresh manifests after data changes:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/build_data_manifests.py .
```

These tools create reusable artifacts. They do not generate trading judgments.

## Output Discipline

Raw downloads, normalized candles, and derived candles are rebuildable artifacts. Do not
edit them manually to improve results. If data is corrected, update the manifest with the
source and reason.
