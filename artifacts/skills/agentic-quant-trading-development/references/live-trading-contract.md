# Live Trading Contract

Live operation uses promoted strategy skills and deterministic runtime rails.

## Live Sources Of Truth

- Exchange account is source of truth for positions, orders, fills, and balances.
- Live candle cache is source of truth for scanner market state.
- Owner state is routing-only.
- Strategy skills are source of truth for judgment and management.
- The monthly walk-forward universe is the research-side source of truth for which
  strategy-engine candidates are current tradable candidates.

## Live Folder Rules

- `live/data/` contains cache, not training evidence.
- `live/signals/` contains live scanner status and packets.
- `live/router/` contains runtime state, logs, and prompts.
- Do not write training results into `live/`.

## Execution Agent Setup

An execution agent should look in these places before operating:

- signal engine scripts: `artifacts/signal_engine/scripts/`
- promoted repo strategy skills: `artifacts/skills/strategies/`
- monthly walk-forward universe: `dev/walk_forward/<YYYY-MM>/tradable_universe.json`
- router command templates: `live/router/prompts/current-router-commands.md`
- cron prompt templates: `live/router/prompts/`
- runtime owner state: `live/router/state/`
- live candle cache and scanner state: `live/data/`
- live emitted signal packets and latest scan status: `live/signals/`

The local OKX CLI must be available as `okx`. Market candle reads use public OKX swap
market data. Authenticated balance, position, order, and fill reads use the account mode
selected by the router:

- demo: `okx --demo ...`
- live: `okx ...`

Cron should call `artifacts/signal_engine/scripts/live/autonomous_wake_router.py`, not a
scanner script directly. Use one cron per strategy per asset on a 5-minute cadence. The
router must receive `--strategy-id`, `--signal-engine-id`, `--account-mode`, owner state
root, and position-review state root explicitly. It resolves scanner path and signal root
from `artifacts/signal_engine/engine_registry.json`; explicit scanner/root overrides remain
allowed for migration and debugging.

Important Hermes cron path rule:
- if the cron job `script` field uses a **relative** path, Hermes resolves it from
  `~/.hermes/scripts/`, not from the cron `workdir`
- therefore, the reliable setup is either:
  - place the launcher script itself in `~/.hermes/scripts/`, or
  - use an absolute script path in the cron config
- do not assume `workdir` makes `script` repo-relative

## Deployment Provisioning

Before enabling or resuming a live cron for a strategy-engine candidate, the execution
agent must complete both provisioning steps:

1. Strategy skill install:
   - read the promoted repo skill under `artifacts/skills/strategies/<STRATEGY_ID>/`
   - install that latest promoted skill into the execution agent's own skill set
   - replace any older installed copy for the same `strategy_id` when needed
2. Candle warm-up:
   - run `python3 artifacts/signal_engine/scripts/data/update_okx_live_data.py --asset <ASSET>`
   - this updater may seed `live/data/raw/<ASSET>/5m/candles.csv` from
     `dev/data/raw/<ASSET>/5m/candles.csv` when appropriate, then fetch/refresh OKX live
     candles and rebuild `live/data/derived/<ASSET>/`
3. Cron launcher provisioning:
   - if using Hermes cron `script` with a relative path, place the launcher under
     `~/.hermes/scripts/`
   - if keeping the main wrapper in the repo, the Hermes-side launcher should call it using
     an absolute workspace path
   - `workdir` should still point at the repo root, but it does not control relative cron
     script resolution

Do not hard-code an executor skill-home path in this contract. Different execution agents
may maintain different local skill directories. The requirement is only that the promoted
strategy skill be installed into that executor's own skill set before live evaluation.

## Execution Rule

The live execution agent is the executor role. It operates cron/router wakes and live
orders only; it must not run research backtests, tune strategy wording, or alter training
artifacts during live operation.

The execution agent must load the installed active strategy skill and execution reference.
It should not infer current TP/SL, leverage, or sizing from old training summaries.

Before enabling or continuing a live cron, execution agents should consult the current
monthly universe record. Rows in `tradable_universe.json` are Path A strategy-engine
candidates keyed by `strategy_id`; `signal_engine_id` tells the router which scanner and
live signal root to use. The same asset can appear multiple times under different
`strategy_id`s. Rows in `watchlist_universe.json` are research-only by default. This
contract update does not require router enforcement; the execution agent is responsible for
applying the record during operational setup. A cron is not ready to enable until the
executor has both installed the latest promoted strategy skill into its own skill set and
warmed the live candle cache for that asset.
