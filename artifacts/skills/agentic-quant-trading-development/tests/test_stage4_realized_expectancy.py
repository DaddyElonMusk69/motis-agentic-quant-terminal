from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "optimization"
    / "stage4_realized_expectancy.py"
)

SPEC = importlib.util.spec_from_file_location("stage4_realized_expectancy", SCRIPT_PATH)
assert SPEC is not None
stage4 = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(stage4)


def write_stage1_scores(root: Path, records: list[dict], ground_truth_dir: Path) -> Path:
    path = root / "scores" / "stage1a_full_cycle_scores.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "asset": "AAA",
                "strategy_id": "aaa-strategy",
                "strategy_version": "v0.1",
                "signal_engine_id": "vegas_ema",
                "signal_set_id": "2026-AAA-2h-dedupe-vote2",
                "inputs": {
                    "ground_truth_dir": str(ground_truth_dir),
                },
                "records": records,
            },
            indent=2,
        )
        + "\n"
    )
    return path


def write_ground_truth(root: Path, signal_id: str, reference_price: float) -> None:
    path = root / "ground_truth" / f"{signal_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "signal_id": signal_id,
                "reference_price": reference_price,
                "natural_direction": "LONG",
                "status": "ok",
            },
            indent=2,
        )
        + "\n"
    )


def write_candles(root: Path, rows: list[tuple[str, float, float, float, float]]) -> Path:
    path = root / "candles.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    content = ["ts,open,high,low,close"]
    for ts, open_, high, low, close in rows:
        content.append(f"{ts},{open_},{high},{low},{close}")
    path.write_text("\n".join(content) + "\n")
    return path


def write_candidates(root: Path, candidates: list[dict], *, leverage: int = 2) -> Path:
    path = root / "scores" / "stage4_candidates.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "defaults": {
                    "leverage": leverage,
                    "max_hold_hours": 2,
                    "timeout_exit_policy": "close_at_cutoff",
                },
                "candidates": candidates,
            },
            indent=2,
        )
        + "\n"
    )
    return path


def test_market_stage4_scores_full_decision_set_with_slices_and_costs(tmp_path: Path) -> None:
    ground_truth_dir = tmp_path / "ground_truth"
    records = [
        {
            "signal_id": "20260301T000000Z",
            "agent_direction": "LONG",
            "agreement": "MATCH",
        },
        {
            "signal_id": "20260301T010000Z",
            "agent_direction": "LONG",
            "agreement": "MISMATCH",
        },
        {
            "signal_id": "20260301T020000Z",
            "agent_direction": "LONG",
            "agreement": "MATCH",
        },
    ]
    for signal_id in ("20260301T000000Z", "20260301T010000Z", "20260301T020000Z"):
        write_ground_truth(tmp_path, signal_id, 100.0)

    stage1_scores = write_stage1_scores(tmp_path, records, ground_truth_dir)
    candidates = write_candidates(
        tmp_path,
        [
            {
                "candidate_id": "market_tp1_sl1",
                "entry_type": "market",
                "tp_pct": 1.0,
                "sl_pct": 1.0,
            }
        ],
    )
    candles = write_candles(
        tmp_path,
        [
            ("2026-03-01T00:05:00Z", 100.0, 101.5, 99.8, 101.2),
            ("2026-03-01T01:05:00Z", 100.0, 100.2, 98.8, 99.0),
            ("2026-03-01T02:05:00Z", 100.0, 100.4, 99.6, 100.0),
            ("2026-03-01T03:55:00Z", 100.0, 100.8, 99.9, 100.5),
        ],
    )

    result = stage4.run_stage4(
        stage1_scores_path=stage1_scores,
        candidates_path=candidates,
        candles_path=candles,
        out_dir=tmp_path / "scores" / "stage4",
        fees_bps_per_side=5.0,
        slippage_bps_per_side=0.0,
        slice_windows=[
            stage4.SliceWindow(
                name="train",
                start=stage4.parse_ts("2026-03-01T00:00:00Z"),
                end=stage4.parse_ts("2026-03-01T02:00:00Z"),
            ),
            stage4.SliceWindow(
                name="validation",
                start=stage4.parse_ts("2026-03-01T02:00:00Z"),
                end=stage4.parse_ts("2026-03-01T04:00:00Z"),
            ),
        ],
    )

    candidate = result["best_candidate"]
    assert candidate["candidate_id"] == "market_tp1_sl1"
    assert candidate["total_decisions"] == 3
    assert candidate["executed_trades"] == 3
    assert candidate["tp_hits"] == 1
    assert candidate["sl_hits"] == 1
    assert candidate["no_hit"] == 1
    assert candidate["profitable_trades"] == 2
    assert round(candidate["gross_expectancy_pct"], 4) == 0.3333
    assert round(candidate["net_expectancy_pct"], 4) == 0.1333
    assert round(candidate["mismatch_cohort"]["gross_pnl_pct"], 4) == -2.0
    assert round(candidate["slices"]["train"]["net_expectancy_pct"], 4) == -0.2
    assert round(candidate["slices"]["validation"]["net_expectancy_pct"], 4) == 0.8

    ledger_path = tmp_path / "scores" / "stage4" / "stage4_trade_ledger.json"
    payload = json.loads(ledger_path.read_text())
    trade_records = payload["candidates"][0]["trades"]
    assert [trade["exit_status"] for trade in trade_records] == ["TP", "SL", "TIMEOUT"]
    assert round(trade_records[2]["gross_pnl_pct"], 4) == 1.0
    assert round(trade_records[2]["net_pnl_pct"], 4) == 0.8


def test_limit_stage4_keeps_unfilled_decision_as_zero_pnl(tmp_path: Path) -> None:
    ground_truth_dir = tmp_path / "ground_truth"
    write_ground_truth(tmp_path, "20260301T000000Z", 100.0)
    stage1_scores = write_stage1_scores(
        tmp_path,
        [
            {
                "signal_id": "20260301T000000Z",
                "agent_direction": "LONG",
                "agreement": "MATCH",
            }
        ],
        ground_truth_dir,
    )
    candidates = write_candidates(
        tmp_path,
        [
            {
                "candidate_id": "limit_tp1_sl1_off1",
                "entry_type": "limit",
                "tp_pct": 1.0,
                "sl_pct": 1.0,
                "limit_offset_pct": 1.0,
            }
        ],
        leverage=3,
    )
    candles = write_candles(
        tmp_path,
        [
            ("2026-03-01T00:05:00Z", 100.0, 100.4, 99.2, 100.1),
            ("2026-03-01T01:55:00Z", 100.1, 100.6, 99.3, 100.0),
        ],
    )

    result = stage4.run_stage4(
        stage1_scores_path=stage1_scores,
        candidates_path=candidates,
        candles_path=candles,
        out_dir=tmp_path / "scores" / "stage4",
        fees_bps_per_side=5.0,
        slippage_bps_per_side=3.0,
    )

    candidate = result["best_candidate"]
    assert candidate["total_decisions"] == 1
    assert candidate["executed_trades"] == 0
    assert candidate["unfilled"] == 1
    assert candidate["gross_expectancy_pct"] == 0.0
    assert candidate["net_expectancy_pct"] == 0.0

    ledger = json.loads((tmp_path / "scores" / "stage4" / "stage4_trade_ledger.json").read_text())
    trade = ledger["candidates"][0]["trades"][0]
    assert trade["entry_status"] == "UNFILLED"
    assert trade["gross_pnl_pct"] == 0.0
    assert trade["net_pnl_pct"] == 0.0


def test_parse_slice_window_accepts_full_iso_timestamps() -> None:
    slice_window = stage4.parse_slice_window("train:2026-03-01T00:00:00Z:2026-05-01T00:00:00Z")
    assert slice_window.name == "train"
    assert stage4.isoformat_z(slice_window.start) == "2026-03-01T00:00:00Z"
    assert stage4.isoformat_z(slice_window.end) == "2026-05-01T00:00:00Z"
