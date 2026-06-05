# Scoring Artifacts

Deterministic scoring output must be structured and reproducible.

## Common Fields

Every score artifact should include:

```json
{
  "schema_version": "0.1",
  "session_id": "",
  "asset": "",
  "strategy_id": "",
  "strategy_version": "",
  "signal_engine_id": "",
  "signal_family": "",
  "signal_set_id": "",
  "stage": "",
  "scoring_method": "",
  "created_at": "",
  "metrics": {},
  "records": []
}
```

## Stage 0 Ground Truth Record

```json
{
  "signal_id": "20260301T072000Z",
  "reference_price": 66976,
  "significance_threshold_pct": 0.9,
  "natural_direction": "LONG",
  "first_move_pct": 4.24,
  "max_travel_pct": 4.24,
  "opposite_max_pct": 0.82,
  "first_move_hours": 31.8,
  "reversed": false,
  "status": "triggered"
}
```

## Stage 1A Score Record

```json
{
  "signal_id": "20260301T072000Z",
  "ground_truth_direction": "LONG",
  "agent_direction": "LONG",
  "confidence": 0.75,
  "agreement": "MATCH",
  "status": "CORRECT"
}
```

## Stage 1B Score Record

```json
{
  "signal_id": "20260301T072000Z",
  "triggered": true,
  "trade_action": "ENTER",
  "expected_travel": "high",
  "entry_gate": "pass",
  "gate_reason_code": "accepted_reclaim_with_room",
  "classification": "TP",
  "direction": "LONG",
  "direction_match": true
}
```

Stage 1B scores `trade_action` as the source of truth:

- `TP`: `ENTER` and ground truth triggered
- `FP`: `ENTER` and ground truth no-trigger
- `TN`: `SKIP` and ground truth no-trigger
- `FN`: `SKIP` and ground truth triggered

`expected_travel` is retained for migration compatibility and should agree with
`trade_action`. Directional match is a secondary diagnostic for entered triggered signals,
not the Stage 1B promotion gate.

## Stage 2 Capture Curve

```json
{
  "tp_levels_pct": [0.5, 1.0, 1.5, 2.0],
  "capture_rates": {
    "0.5": 0.92,
    "1.0": 0.78,
    "1.5": 0.61,
    "2.0": 0.43
  }
}
```

## Stage 3 Grid Result

```json
{
  "tp_pct": 2.0,
  "sl_pct": 0.3,
  "total": 100,
  "tp_hits": 34,
  "sl_hits": 66,
  "no_hit": 0,
  "win_rate": 0.34,
  "profit_factor": 1.2,
  "expectancy_r": 0.08,
  "assumptions": {
    "fees_included": false,
    "slippage_included": false,
    "forward_hours": 36
  }
}
```

## Stage 4 Realized Expectancy Result

```json
{
  "candidate_id": "market_tp2.0_sl2.0",
  "total_decisions": 173,
  "executed_trades": 173,
  "tp_hits": 97,
  "sl_hits": 76,
  "no_hit": 0,
  "unfilled": 0,
  "gross_expectancy_pct": 0.24,
  "net_expectancy_pct": 0.11,
  "profit_factor": 1.18,
  "cost_assumptions": {
    "fees_bps_per_side": 5,
    "slippage_bps_per_side": 3
  },
  "slices": {
    "train": { "net_expectancy_pct": 0.08 },
    "validation": { "net_expectancy_pct": 0.14 },
    "locked_oos": { "net_expectancy_pct": 0.00, "status": "missing_slice_exception" }
  }
}
```

The companion `stage4_trade_ledger.json` should preserve one trade record per Stage 1
decision per candidate, including:

- `signal_id`
- `signal_ts`
- `agreement`
- `decision_direction`
- `entry_status`
- `exit_status`
- `entry_price`
- `exit_price`
- `gross_pnl_pct`
- `net_pnl_pct`
- `filled_legs`

## Summary Requirement

Every score artifact should have a human-readable companion summary under `summaries/`.
The summary states the decision: continue, audit failures, update skill, expand sample,
or reject.
