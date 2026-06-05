# Signal Generation

Signal generation creates neutral packets from validated data.

Read `signal-engine-contract.md` before adding or modifying any scanner.

Signal generation scripts live in `artifacts/signal_engine/scripts/` because signal
generation is signal-engine responsibility. The development skill owns when to run them
and where their outputs belong.

## Dev Signal Sets

Write replay signal sets to:

```text
dev/signals/<SIGNAL_ENGINE_ID>/<ASSET>/<SIGNAL_SET_ID>/
  manifest.json
  packets/<TIMESTAMP>.json
```

`signal_engine_id` is the canonical producer id and also the root folder under
`dev/signals/`. Keep current engine root names such as `vegas_ema` and `bollinger`.
`signal_family` may remain in manifests as legacy descriptive metadata, but new logic must
not branch on it.

Example EMA/Vegas replay generation:

```bash
python3 artifacts/signal_engine/scripts/signals/generate_training_session.py \
  --asset BTC \
  --start 2026-01-01T00:00:00Z \
  --end 2026-05-01T00:00:00Z \
  --vote-threshold 2
```

Example Bollinger replay generation:

```bash
python3 artifacts/signal_engine/scripts/signals/generate_bollinger_training_session.py \
  --asset BTC \
  --start 2026-01-01T00:00:00Z \
  --end 2026-05-01T00:00:00Z \
  --vote-threshold 2
```

When a canonical signal set already exists and only the horizon tail is missing, extend it
instead of regenerating the whole folder. Use:

```bash
python3 artifacts/signal_engine/scripts/signals/fill_signal_set_tail.py \
  --signal-engine-id bollinger \
  --asset BTC \
  --signal-set-id 2026-BTC-2h-dedupe-vote2 \
  --target-end 2026-06-01T00:00:00Z
```

Tail-fill behavior:

- no-op if `manifest.json:end_ts` already reaches the target horizon
- use the signal set's last emitted packet timestamp as the replay overlap anchor so dedupe
  continuity is preserved across the old/new boundary
- merge generated packets by canonical timestamp filename
- rewrite `manifest.json` with updated `packet_count`, `end_ts`, and required
  `signal_engine_id`

## Signal Set Naming

Signal set folder names must use:

```text
<YEAR>-<ASSET>-<DEDUPE_WINDOW>-dedupe-vote<VOTE_THRESHOLD>
```

Examples:

```text
2026-BTC-2h-dedupe-vote2
2025-ETH-2h-dedupe-vote3
```

Rules:

- `YEAR` is the UTC year of the signal set start timestamp.
- `ASSET` is uppercase standalone asset symbol, e.g. `BTC`.
- `DEDUPE_WINDOW` is the replay dedupe/windowing interval, e.g. `30m`, `2h`, `1d`.
- `vote<VOTE_THRESHOLD>` is the deterministic scanner vote threshold.
- If two signal sets would otherwise collide, add a short suffix after the vote segment only when necessary, e.g. `2025-BTC-2h-dedupe-vote2-oos`. Avoid suffixes for normal runs.

## Packet Naming

Individual signal packet filenames must use the packet timestamp in UTC compact form:

```text
YYYYMMDDTHHMMSSZ.json
```

Example:

```text
20260101T010500Z.json
```

The filename timestamp must match the packet's top-level `timestamp` field exactly after
converting the field to compact UTC form.

The manifest should record:

- signal engine id
- optional legacy signal family
- asset
- signal engine version
- strategy-independent scanner parameters
- data manifest path
- packet count
- start and end timestamps
- dedupe/windowing policy, if applied
- packet schema version

## Live Signals

Write live signal status and packets to:

```text
live/signals/<SIGNAL_ENGINE_ID>/<ASSET>/
  latest_scan.json
  packets/<TIMESTAMP>.json
```

Live scanners must use live cache data and emit the same packet shape as replay.

## Signal Set Manifest

Minimal shape:

```json
{
  "signal_set_id": "2026-BTC-2h-dedupe-vote2",
  "schema_version": "0.1",
  "signal_engine_id": "vegas_ema",
  "signal_family": "vegas_ema",
  "asset": "BTC",
  "signal_engine_version": "0.1",
  "data_manifest": "dev/data/manifests/BTC.json",
  "parameters": {
    "proximity_threshold": "0.002",
    "vote_threshold": 2,
    "timeframes": ["2h", "4h", "8h", "12h", "1d"]
  },
  "packet_count": 0,
  "start_ts": "2026-01-01T00:00:00Z",
  "end_ts": "2026-05-01T00:00:00Z",
  "packets_path": "packets/",
  "packet_filename_format": "YYYYMMDDTHHMMSSZ.json"
}
```

## Packet Discipline

Packets must not include:

- direction
- model confidence
- entry decision
- TP or SL
- leverage or size
- position management advice
- future outcome fields

Signal scanners may compute indicators and proximity facts. They must not add hard-coded
trade-quality scoring to improve model performance.

## Replay/Live Parity

For higher timeframes, completed candles provide context and only the latest forming candle
is reconstructed from raw `5m` candles. Live scanning should match replay behavior over
confirmed 5m candles as closely as the exchange cache permits.
