# Strategy Skill Contract

Use this contract for every strategy skill under:

```text
artifacts/skills/strategies/<STRATEGY_ID>/
```

## Required Shape

```text
<STRATEGY_ID>/
  SKILL.md
  references/
    execution-parameters.md
    position-management.md
    failure-patterns.md
```

## Vegas EMA Seeding Rule

New Vegas EMA asset skills must be seeded mechanically from the canonical Vegas EMA base.
Do not hand-summarize, reinterpret, or simplify the base wording.

The base itself is generated from `btc-vegas-tunnel-v01` v0.16 by exact token replacement:

- `BTC` -> `<ASSET>`
- `Btc` -> `<Asset>`
- `btc` -> `<asset>`

Every other word should remain the same as BTC v0.16. This keeps the battle-tested
directional, execution, and management constraints from drifting.

Build or refresh the base with:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/seed_vegas_ema_skill.py \
  build-base \
  --source artifacts/skills/strategies/btc-vegas-tunnel-v01 \
  --out artifacts/skills/strategy-bases/vegas-ema-base \
  --overwrite
```

Seed a new asset skill from the base with:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/seed_vegas_ema_skill.py \
  seed-asset \
  --base artifacts/skills/strategy-bases/vegas-ema-base \
  --asset WIF \
  --strategy-id wif-vegas-tunnel-v00 \
  --out artifacts/skills/strategies/wif-vegas-tunnel-v00
```

After seeding, run validation before using the skill in Stage 1. Asset-specific edits are
allowed only through the normal scored iteration and failure-audit process.

## File Responsibilities

`SKILL.md` contains the active directional and entry-evaluation logic:

- signal-family interpretation
- market structure and regime rules
- anchor timeframe selection
- directional classification
- entry decision logic after a directional bias is found
- explicit Stage 1B entry-gate logic for path-B/sparse-pool strategies
- progressive-loading instructions for references

For path-B strategies that run Stage 1B before Stage 1A, the main `SKILL.md` must contain
an explicit `Entry Gate` or equivalent section. It must define what packet evidence permits
`trade_action: ENTER`, what evidence requires `trade_action: SKIP`, and state that a
directional bias alone is not permission to trade. `SKIP` means no live entry order for
that signal. Low expected travel means skip, not reduced confidence sizing.

For these path-B strategies, the expected gate shape is default-enter with explicit skip
disqualifiers. The skill should lean `ENTER` and then clearly name the blocker conditions
that require `SKIP`. Avoid proof-heavy positive checklists that force the evaluator to infer
perfect setup quality before every entry.

The main `SKILL.md` must keep directional logic and entry logic as separate layers. A valid
strategy layout has one section or rule path that chooses `LONG`/`SHORT`, and a separate
entry-gate section that consumes that chosen direction and decides `ENTER`/`SKIP`. Do not
write rules where directional confidence, macro pressure, or "favored side" directly implies
entry permission. Computed features may be used by the strategist to derive or tighten those
rules, but they should not be handed to the evaluator as an extra decision surface unless
the user explicitly chooses that contract for both backtest and production.

`references/execution-parameters.md` contains execution source of truth:

- instrument and account mode assumptions
- leverage and margin rules
- sizing formula and contract-unit caveats
- entry order type
- TP/SL setup
- pyramiding rules
- max hold, stale-order, and no-fill handling when part of the promoted setup
- evidence path for the passing promoted Stage 4 setup
- upstream Stage 3 candidate evidence path when relevant
- owner-state write requirements after entry order submission/fill, if applicable

Execution parameters must be written as operational instructions, not research notes. If
Stage 4 chose different LONG and SHORT setups, state both explicitly. Margin rules must use
account margin language, not notional exposure language.

`references/position-management.md` contains open-position management:

- hold, exit, repair, reduce, and pyramid rules
- protection-order review
- thesis review cadence
- requirement to read `execution-parameters.md` as the setup source of truth
- exchange-truth-first rules for position age, side, size, fills, and orders

Position management must not override promoted execution parameters unless the strategy
skill is intentionally revised through a new scored iteration.

`references/failure-patterns.md` contains audit-derived notes:

- repeated failure clusters
- protected winning conditions
- edge cases to watch
- candidate observations not yet promoted into active rules

Failure-pattern notes are not active trading rules unless promoted into `SKILL.md`,
`execution-parameters.md`, or `position-management.md`.

## Skill Iteration Rules

When updating a strategy skill from a scored training iteration:

- Make the smallest possible skill change that addresses a repeated directional failure.
- Preserve conditions that were already working.
- Prefer directional reclassification over broader avoidance language.
- Prefer proof-of-control wording over action-forcing guard wording.
- Update strategy skills, not signal engine logic, when judgment improves.
- Do not encode exact training timestamps as strategy rules.
- Do not add broad skip language to hide directional weakness unless the active stage is
  explicitly opportunity screening.
- For Stage 1B/path-B updates, express the behavioral change as an entry-gate rule in the
  main `SKILL.md`, not only as confidence, caution, or travel-conviction wording.
- Stage 1A failure audits may update directional rules only. Stage 1B failure audits may
  update entry-gate rules only unless the audit separately demonstrates a directional
  mismatch. Do not fix false-positive entries by rewriting the direction layer when the
  direction was plausible but the entry gate was weak.
- For path-B iteration order, work Stage 1B first. Once Stage 1B passes, then optimize
  Stage 1A on the entered subset. If Stage 1A stalls for two iterations, use computed
  feature audits to improve the strategy wording itself, not to augment evaluator inputs.

Good updates explain both sides:

- what recurring condition the model misread
- what evidence changes the read
- what evidence preserves the previous read
- where the rule applies
- where the rule does not apply

## Validation

Run:

```bash
python3 artifacts/skills/agentic-quant-trading-development/scripts/validate_strategy_skill.py artifacts/skills/strategies/<STRATEGY_ID>
```

The validator checks required files and basic placement of directional, execution,
position-management, and failure-pattern content. It is a structural guard, not a substitute
for reviewing strategy quality.
