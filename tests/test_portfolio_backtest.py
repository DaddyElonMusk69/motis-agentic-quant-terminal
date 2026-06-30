import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from quant_terminal_worker.stage4.portfolio_backtest import delete_portfolio_backtest_run
from quant_terminal_worker.stage4.portfolio_backtest import list_portfolio_backtest_runs
from quant_terminal_worker.stage4.portfolio_backtest import run_portfolio_backtest
from quant_terminal_worker.stage4.realized_expectancy import _simulate_account_position


class MockRepository:
    """Minimal repository mock that returns signals from an in-memory store."""

    def __init__(self, signals: dict[str, list[dict[str, Any]]] | None = None):
        self._signals = signals or {}

    def list_signals_for_signal_set_window(
        self, *, signal_set_key: str, window_start: str, window_end: str
    ) -> list[dict[str, Any]]:
        return self._signals.get(signal_set_key, [])


class MockMarketDataReader:
    """Mock candle reader that returns candles from an in-memory store."""

    def __init__(self, candles_by_asset: dict[str, list[dict[str, Any]]] | None = None):
        self._candles = candles_by_asset or {}

    def get_candles(self, *, asset: str, **kwargs: Any) -> list[dict[str, Any]]:
        return self._candles.get(asset.upper(), [])


def _make_candles(start_ts: str, count: int, base_price: float = 100.0, *, trend: float = 0.0) -> list[dict[str, Any]]:
    """Generate simple 5m candles. trend > 0 = uptrend, < 0 = downtrend."""
    dt = datetime.fromisoformat(start_ts.replace("Z", "+00:00"))
    candles = []
    price = base_price
    for i in range(count):
        candle = {
            "timestamp": dt,
            "open": price,
            "high": price * 1.01,
            "low": price * 0.99,
            "close": price + trend,
        }
        candles.append(candle)
        price = candle["close"]
        dt += timedelta(minutes=5)
    return candles


def _make_signal(signal_id: str, ts: str, direction: str = "LONG", ref_price: float = 100.0) -> dict[str, Any]:
    return {
        "signal_id": signal_id,
        "timestamp": ts,
        "payload": {
            "timestamp": ts,
            "evidence": {"reference_price": ref_price},
            "active_timeframes": ["5m"],
            "charts": {
                "5m": {
                    "latest_forming_candle": {"close": ref_price},
                },
            },
        },
    }


def _write_stage4_artifacts(
    tmp_path: Path,
    *,
    asset: str,
    session_id: str,
    candidate_id: str,
    train_start: str = "2026-05-01",
    train_end: str = "2026-05-01",
    walk_forward_start: str = "2026-05-01",
    walk_forward_end: str = "2026-05-01",
    tp_pct: float = 2.0,
    sl_pct: float = 1.0,
    leverage: float = 5.0,
    max_hold_hours: float = 12.0,
    pyramid_step_pct: float | None = None,
    max_legs: int = 3,
    slippage_bps_per_side: float = 0.0,
    signal_records: list[dict] | None = None,
) -> Path:
    artifact_root = tmp_path / "dev" / "training_sessions" / asset.lower() / session_id
    promotion_root = artifact_root / "promotion"
    promotion_root.mkdir(parents=True, exist_ok=True)

    candidate_setup = {
        "candidate_id": candidate_id,
        "entry_model": "market",
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "final_tp_pct": tp_pct,
        "initial_sl_pct": sl_pct,
        "protection_enabled": False,
        "timeout_policy": "close_at_cutoff",
        "max_hold_hours": max_hold_hours,
        "leverage": leverage,
    }
    if pyramid_step_pct is not None:
        candidate_setup["pyramid_step_pct"] = pyramid_step_pct
        candidate_setup["max_legs"] = max_legs
        candidate_setup["sl_breakeven"] = False

    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({
            "run_id": "stage4-run",
            "asset": asset,
            "best_candidate_id": candidate_id,
            "best_candidate": {"candidate_id": candidate_id, "setup": candidate_setup},
            "cost_assumptions": {"fees_bps_per_side": 5.0, "slippage_bps_per_side": slippage_bps_per_side},
        })
    )
    (promotion_root / "stage4_candidates.json").write_text(
        json.dumps({"candidates": [{"candidate_id": candidate_id, "setup": candidate_setup}]})
    )
    (promotion_root / "stage1a_canonical_full_cycle_scores.json").write_text(
        json.dumps({
            "records": signal_records or [
                {"signal_id": "sig-1", "decision_direction": "LONG", "agreement": "MATCH"},
            ],
        })
    )
    return artifact_root


def _write_stage4b_timing_artifacts(
    artifact_root: Path,
    *,
    candidate_id: str,
    source_stage4_run_id: str = "stage4-run",
    stage4a_wf: float = 2.0,
    stage4b_wf: float = 6.0,
    stage4a_total: float = 20.0,
    stage4b_total: float = 60.0,
    exclude_utc_hours: list[int] | None = None,
) -> None:
    promotion_root = artifact_root / "promotion"
    realized = json.loads((promotion_root / "stage4_realized_expectancy.json").read_text())
    stage4a_best = realized.get("best_candidate") or {}
    stage4a_best["account"] = {"net_pnl_usdt": stage4a_total, "ending_equity_usdt": 1000 + stage4a_total}
    stage4a_best["slices"] = {"walk_forward_test": {"net_pnl_pct": stage4a_wf, "profit_factor": 1.2}}
    realized["best_candidate"] = stage4a_best
    (promotion_root / "stage4_realized_expectancy.json").write_text(json.dumps(realized))

    timing_root = promotion_root / "stage4b_timing"
    timing_root.mkdir(parents=True, exist_ok=True)
    overlay = {
        "schema_version": "stage4b_timing_overlay.v1",
        "source_stage4_run_id": source_stage4_run_id,
        "source_stage4_candidate_id": stage4a_best.get("candidate_id"),
        "exclude_utc_hours": exclude_utc_hours or [1],
        "exclude_utc_weekdays": [],
        "applies_to": "all",
        "rationale": "Test timing window.",
    }
    stage4b_best = {
        "candidate_id": candidate_id,
        "setup": {"tp_pct": 10.0, "sl_pct": 50.0, "initial_sl_pct": 50.0, "max_hold_hours": 12.0, "leverage": 1.0},
        "account": {"net_pnl_usdt": stage4b_total, "ending_equity_usdt": 1000 + stage4b_total},
        "slices": {"walk_forward_test": {"net_pnl_pct": stage4b_wf, "profit_factor": 1.8}},
        "skipped_timing_filter": 1,
    }
    (timing_root / "timing_overlay.json").write_text(json.dumps(overlay))
    (timing_root / "timing_replay.json").write_text(
        json.dumps({"run_id": "stage4b-run", "best_candidate_id": candidate_id, "best_candidate": stage4b_best, "candidates": [stage4b_best]})
    )
    (timing_root / "timing_trade_ledger.json").write_text(json.dumps({"run_id": "stage4b-run", "candidates": []}))
    (timing_root / "timing_summary.md").write_text("# Stage 4B Timing Replay\n")


def _candidate(pool_id: str, candidate_id: str, asset: str) -> dict:
    return {
        "candidate_id": candidate_id,
        "universe_run_id": pool_id,
        "asset": asset,
        "acceptance_status": "accepted",
    }


def _session(pool_id: str, candidate_id: str, asset: str, artifact_root: Path, signal_set_key: str | None = None) -> dict:
    return {
        "session_id": f"stage1-{asset.lower()}",
        "source_universe_run_id": pool_id,
        "source_candidate_id": candidate_id,
        "asset": asset,
        "artifact_root": str(artifact_root),
        "signal_set_key": signal_set_key or f"sigset-{asset.lower()}",
        "train_start": "2026-05-01",
        "train_end": "2026-05-01",
        "walk_forward_start": "2026-05-01",
        "walk_forward_end": "2026-05-01",
    }


def test_portfolio_backtest_uses_isolated_available_cash_for_new_entries(tmp_path: Path, monkeypatch):
    pool_id = "pool-isolated"
    roots = {
        asset: _write_stage4_artifacts(
            tmp_path,
            asset=asset,
            session_id=f"stage1-{asset.lower()}",
            candidate_id=f"candidate-{asset.lower()}",
            tp_pct=50.0,
            sl_pct=50.0,
            leverage=1.0,
            max_hold_hours=12.0,
            signal_records=[{"signal_id": f"sig-{asset.lower()}-1", "decision_direction": "LONG", "agreement": "MATCH"}],
        )
        for asset in ("AAVE", "SOL", "XRP")
    }
    roots["ADA"] = _write_stage4_artifacts(
        tmp_path,
        asset="ADA",
        session_id="stage1-ada",
        candidate_id="candidate-ada",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-ada-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.0)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:05:00Z", "LONG", 100.0)],
        "sigset-xrp": [_make_signal("sig-xrp-1", "2026-05-01T00:10:00Z", "LONG", 100.0)],
        "sigset-ada": [_make_signal("sig-ada-1", "2026-05-01T00:15:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({asset: candles for asset in ("AAVE", "SOL", "XRP", "ADA")}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, f"candidate-{asset.lower()}", asset) for asset in ("AAVE", "SOL", "XRP", "ADA")],
        sessions=[
            _session(pool_id, f"candidate-{asset.lower()}", asset, roots[asset])
            for asset in ("AAVE", "SOL", "XRP", "ADA")
        ],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 60, "SOL": 30, "XRP": 10, "ADA": 10.1},
        repository=MockRepository(signals),
    )

    executed_assets = {trade["asset"] for trade in result["trade_ledger"]}
    assert {"AAVE", "SOL", "XRP"}.issubset(executed_assets)
    assert "ADA" not in executed_assets
    assert result["summary"]["skipped_insufficient_margin"] == 1


def test_portfolio_backtest_does_not_reduce_isolated_cash_by_unrealized_losses(tmp_path: Path, monkeypatch):
    pool_id = "pool-unrealized-isolated"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    sol_root = _write_stage4_artifacts(
        tmp_path,
        asset="SOL",
        session_id="stage1-sol",
        candidate_id="candidate-sol",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-sol-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    aave_candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=-1.0)
    sol_candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.0)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:10:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": aave_candles, "SOL": sol_candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE"), _candidate(pool_id, "candidate-sol", "SOL")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root), _session(pool_id, "candidate-sol", "SOL", sol_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 90, "SOL": 10},
        repository=MockRepository(signals),
    )

    assert {trade["asset"] for trade in result["trade_ledger"]} == {"AAVE", "SOL"}
    assert result["summary"]["skipped_insufficient_margin"] == 0


def test_portfolio_backtest_ignores_zero_percent_allocations(tmp_path: Path, monkeypatch):
    pool_id = "pool-zero-allocation"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    sol_root = _write_stage4_artifacts(
        tmp_path,
        asset="SOL",
        session_id="stage1-sol",
        candidate_id="candidate-sol",
        signal_records=[{"signal_id": "sig-sol-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.1)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles, "SOL": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE"), _candidate(pool_id, "candidate-sol", "SOL")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root), _session(pool_id, "candidate-sol", "SOL", sol_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 0, "SOL": 30},
        repository=MockRepository(signals),
    )

    assert result["summary"]["eligible_asset_count"] == 1
    assert result["summary"]["total_signals"] == 1
    assert {trade["asset"] for trade in result["trade_ledger"]} == {"SOL"}
    assert all(item["asset"] != "AAVE" for item in result["skipped_signals"])


def test_portfolio_backtest_reports_asset_contribution_breakdown(tmp_path: Path, monkeypatch):
    pool_id = "pool-asset-breakdown"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    sol_root = _write_stage4_artifacts(
        tmp_path,
        asset="SOL",
        session_id="stage1-sol",
        candidate_id="candidate-sol",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-sol-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    aave_candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=1.0)
    sol_candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=-1.0)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:05:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": aave_candles, "SOL": sol_candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE"), _candidate(pool_id, "candidate-sol", "SOL")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root), _session(pool_id, "candidate-sol", "SOL", sol_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30, "SOL": 30},
        repository=MockRepository(signals),
    )

    breakdown = {row["asset"]: row for row in result["asset_breakdown"]}
    assert set(breakdown) == {"AAVE", "SOL"}
    for asset, row in breakdown.items():
        trades = [trade for trade in result["trade_ledger"] if trade["asset"] == asset]
        assert row["executed_positions"] == len(trades)
        assert row["net_pnl_usdt"] == round(sum(trade["net_pnl_usdt"] for trade in trades), 4)
        assert row["total_fees_usdt"] == round(sum(trade["total_fees_usdt"] for trade in trades), 4)
    assert sum(row["executed_positions"] for row in breakdown.values()) == result["summary"]["executed_positions"]


def test_portfolio_backtest_uses_resolved_stage4b_promotion_candidate(tmp_path: Path, monkeypatch):
    pool_id = "pool-stage4b-selection"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="stage4a-candidate",
        tp_pct=1.0,
        sl_pct=50.0,
        leverage=1.0,
        signal_records=[
            {"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"},
            {"signal_id": "sig-aave-2", "decision_direction": "LONG", "agreement": "MATCH"},
        ],
    )
    promotion_root = aave_root / "promotion"
    stage4_candidates = json.loads((promotion_root / "stage4_candidates.json").read_text())
    stage4_candidates["candidates"].append(
        {
            "candidate_id": "stage4b-candidate",
            "setup": {
                "candidate_id": "stage4b-candidate",
                "entry_model": "market",
                "tp_pct": 10.0,
                "sl_pct": 50.0,
                "final_tp_pct": 10.0,
                "initial_sl_pct": 50.0,
                "protection_enabled": False,
                "max_hold_hours": 12.0,
                "leverage": 1.0,
            },
        }
    )
    (promotion_root / "stage4_candidates.json").write_text(json.dumps(stage4_candidates))
    _write_stage4b_timing_artifacts(aave_root, candidate_id="stage4b-candidate", exclude_utc_hours=[1])

    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.0)
    signals = {
        "sigset-aave": [
            _make_signal("sig-aave-1", "2026-05-01T01:00:00Z", "LONG", 100.0),
            _make_signal("sig-aave-2", "2026-05-01T02:00:00Z", "LONG", 100.0),
        ]
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )

    eligible = result["eligible_assets"][0]
    assert eligible["promotion_source"] == "stage4b_timing"
    assert eligible["stage4_candidate_id"] == "stage4b-candidate"
    assert result["summary"]["skipped_timing_filter"] == 1
    assert result["skipped_signals"][0]["skip_reason"] == "timing_filter"
    assert result["trade_ledger"][0]["candidate_id"] == "stage4b-candidate"


def test_portfolio_backtest_uses_highest_oos_protected_promotion_candidate(tmp_path: Path, monkeypatch):
    pool_id = "pool-protected-selection"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="stage4a-unprotected",
        tp_pct=1.0,
        sl_pct=50.0,
        leverage=1.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    promotion_root = aave_root / "promotion"
    stage4_candidates = json.loads((promotion_root / "stage4_candidates.json").read_text())
    protected_setup = {
        "candidate_id": "stage4a-protected",
        "entry_model": "market",
        "tp_pct": 10.0,
        "sl_pct": 50.0,
        "final_tp_pct": 10.0,
        "initial_sl_pct": 50.0,
        "protection_enabled": True,
        "protect_trigger_pct": 2.0,
        "trail_sl_pct": 0.5,
        "max_hold_hours": 12.0,
        "leverage": 1.0,
    }
    stage4_candidates["candidates"].append({"candidate_id": "stage4a-protected", "setup": protected_setup})
    (promotion_root / "stage4_candidates.json").write_text(json.dumps(stage4_candidates))
    realized = json.loads((promotion_root / "stage4_realized_expectancy.json").read_text())
    realized["best_candidate"]["slices"] = {"walk_forward_test": {"net_pnl_pct": 40, "profit_factor": 2.0}}
    realized["best_candidate"]["account"] = {"net_pnl_usdt": 900, "ending_equity_usdt": 1900}
    realized["candidates"] = [
        realized["best_candidate"],
        {
            "candidate_id": "stage4a-protected",
            "setup": protected_setup,
            "account": {"net_pnl_usdt": 500, "ending_equity_usdt": 1500},
            "slices": {"walk_forward_test": {"net_pnl_pct": 25, "profit_factor": 1.7}},
        },
    ]
    (promotion_root / "stage4_realized_expectancy.json").write_text(json.dumps(realized))
    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.0)
    signals = {"sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)]}

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )

    eligible = result["eligible_assets"][0]
    assert eligible["stage4_candidate_id"] == "stage4a-protected"
    assert eligible["promotion_selection_criterion"] == "protected_walk_forward_net_pnl_pct"
    assert result["trade_ledger"][0]["candidate_id"] == "stage4a-protected"


def test_portfolio_backtest_consumes_pyramid_margin_dynamically(tmp_path: Path, monkeypatch):
    pool_id = "pool-pyramid"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        pyramid_step_pct=1.0,
        max_legs=2,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    sol_root = _write_stage4_artifacts(
        tmp_path,
        asset="SOL",
        session_id="stage1-sol",
        candidate_id="candidate-sol",
        tp_pct=50.0,
        sl_pct=50.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-sol-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    aave_candles = [
        {"timestamp": datetime(2026, 5, 1, 0, 0, tzinfo=UTC), "open": 100.0, "high": 100.2, "low": 99.8, "close": 100.0},
        {"timestamp": datetime(2026, 5, 1, 0, 5, tzinfo=UTC), "open": 100.0, "high": 101.5, "low": 99.9, "close": 101.2},
        *_make_candles("2026-05-01T00:10:00Z", 160, base_price=101.2, trend=0.0),
    ]
    sol_candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.0)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": aave_candles, "SOL": sol_candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE"), _candidate(pool_id, "candidate-sol", "SOL")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root), _session(pool_id, "candidate-sol", "SOL", sol_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 60, "SOL": 40.1},
        repository=MockRepository(signals),
    )

    aave_trade = next(trade for trade in result["trade_ledger"] if trade["asset"] == "AAVE")
    assert aave_trade["filled_legs"] == 1
    assert result["summary"]["skipped_pyramid_margin"] >= 1


def test_portfolio_backtest_matches_stage4_reference_price_and_cost_semantics(tmp_path: Path, monkeypatch):
    pool_id = "pool-stage4-parity"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=2.0,
        sl_pct=1.0,
        leverage=5.0,
        max_hold_hours=12.0,
        slippage_bps_per_side=2.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = [
        {"timestamp": datetime(2026, 5, 1, 0, 0, tzinfo=UTC), "open": 101.0, "high": 101.2, "low": 100.8, "close": 101.0},
        *_make_candles("2026-05-01T00:05:00Z", 40, base_price=100.0, trend=0.2),
    ]
    signals = {"sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)]}
    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )
    portfolio_trade = result["trade_ledger"][0]
    stage4_trade = _simulate_account_position(
        item={
            "signal_id": "sig-aave-1",
            "signal_ts": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "reference_price": 100.0,
            "direction": "LONG",
            "slice_name": "training",
            "record": {"agreement": "MATCH"},
        },
        candidate={
            "candidate_id": "candidate-aave",
            "entry_model": "market",
            "tp_pct": 2.0,
            "sl_pct": 1.0,
            "final_tp_pct": 2.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": False,
            "timeout_policy": "close_at_cutoff",
            "max_hold_hours": 12.0,
            "leverage": 5.0,
        },
        candles=candles,
        equity_before=1000,
        margin_allocation_pct=30,
        leverage=5.0,
        fees_bps_per_side=5.0,
        slippage_bps_per_side=2.0,
    )

    assert portfolio_trade["entry_price"] == stage4_trade["entry_price"] == 100.0
    assert portfolio_trade["exit_status"] == stage4_trade["exit_status"]
    assert portfolio_trade["net_pnl_usdt"] == pytest.approx(stage4_trade["net_pnl_usdt"])


def test_portfolio_backtest_matches_stage4_dual_hit_resolution(tmp_path: Path, monkeypatch):
    pool_id = "pool-stage4-dual-hit"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=2.0,
        sl_pct=1.0,
        leverage=1.0,
        max_hold_hours=12.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = [
        {"timestamp": datetime(2026, 5, 1, 0, 0, tzinfo=UTC), "open": 100.0, "high": 100.2, "low": 99.8, "close": 100.0},
        {"timestamp": datetime(2026, 5, 1, 0, 5, tzinfo=UTC), "open": 100.0, "high": 103.0, "low": 98.0, "close": 101.0},
    ]
    signals = {"sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)]}
    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )
    stage4_trade = _simulate_account_position(
        item={
            "signal_id": "sig-aave-1",
            "signal_ts": datetime(2026, 5, 1, 0, 0, tzinfo=UTC),
            "reference_price": 100.0,
            "direction": "LONG",
            "slice_name": "training",
            "record": {"agreement": "MATCH"},
        },
        candidate={
            "candidate_id": "candidate-aave",
            "entry_model": "market",
            "tp_pct": 2.0,
            "sl_pct": 1.0,
            "final_tp_pct": 2.0,
            "initial_sl_pct": 1.0,
            "protection_enabled": False,
            "timeout_policy": "close_at_cutoff",
            "max_hold_hours": 12.0,
            "leverage": 1.0,
        },
        candles=candles,
        equity_before=1000,
        margin_allocation_pct=30,
        leverage=1.0,
        fees_bps_per_side=5.0,
        slippage_bps_per_side=0.0,
    )

    assert result["trade_ledger"][0]["exit_status"] == stage4_trade["exit_status"] == "TP"


def test_portfolio_backtest_continuous_5m_timeline_records_missing_candle_gaps(tmp_path: Path, monkeypatch):
    pool_id = "pool-gaps"
    aave_root = _write_stage4_artifacts(
        tmp_path,
        asset="AAVE",
        session_id="stage1-aave",
        candidate_id="candidate-aave",
        tp_pct=2.0,
        sl_pct=1.0,
        leverage=1.0,
        max_hold_hours=1.0,
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = [
        {"timestamp": datetime(2026, 5, 1, 0, 0, tzinfo=UTC), "open": 100.0, "high": 100.2, "low": 99.8, "close": 100.0},
        {"timestamp": datetime(2026, 5, 1, 0, 10, tzinfo=UTC), "open": 100.0, "high": 102.5, "low": 99.9, "close": 102.0},
    ]
    signals = {"sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)]}
    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )

    assert result["summary"]["continuous_5m_steps"] == 3
    assert result["summary"]["data_gap_candles"] == 1
    assert result["trade_ledger"][0]["exit_ts"] == "2026-05-01T00:10:00Z"


def test_portfolio_backtest_blocks_when_shared_margin_is_unavailable(tmp_path: Path, monkeypatch):
    """Two assets both need margin, but combined allocation exceeds capital."""
    pool_id = "pool-vegas"

    # AAVE signal at 00:00, LONG at 100
    aave_root = _write_stage4_artifacts(
        tmp_path, asset="AAVE", session_id="stage1-aave", candidate_id="candidate-aave",
        signal_records=[{"signal_id": "sig-aave-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    # SOL signal at 00:05, LONG at 100
    sol_root = _write_stage4_artifacts(
        tmp_path, asset="SOL", session_id="stage1-sol", candidate_id="candidate-sol",
        signal_records=[{"signal_id": "sig-sol-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )

    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.5)
    signals = {
        "sigset-aave": [_make_signal("sig-aave-1", "2026-05-01T00:00:00Z", "LONG", 100.0)],
        "sigset-sol": [_make_signal("sig-sol-1", "2026-05-01T00:05:00Z", "LONG", 100.0)],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles, "SOL": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE"), _candidate(pool_id, "candidate-sol", "SOL")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root), _session(pool_id, "candidate-sol", "SOL", sol_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 80, "SOL": 30},
        repository=MockRepository(signals),
    )

    assert result["summary"]["eligible_asset_count"] == 2
    # AAVE takes 80% margin (800), SOL needs 30% (300) but only 200 free → skip
    assert result["summary"]["skipped_insufficient_margin"] == 1
    assert result["summary"]["executed_positions"] >= 1
    assert (tmp_path / result["portfolio_backtest_path"]).exists()


def test_portfolio_backtest_blocks_same_asset_overlap(tmp_path: Path, monkeypatch):
    """When a position is already open for an asset, subsequent signals are skipped."""
    pool_id = "pool-vegas"

    # Two signals for same asset, second fires while first position is still open
    aave_root = _write_stage4_artifacts(
        tmp_path, asset="AAVE", session_id="stage1-aave", candidate_id="candidate-aave",
        max_hold_hours=12.0,
        signal_records=[
            {"signal_id": "sig-1", "decision_direction": "LONG", "agreement": "MATCH"},
            {"signal_id": "sig-2", "decision_direction": "LONG", "agreement": "MATCH"},
        ],
    )

    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.1)
    signals = {
        "sigset-aave": [
            _make_signal("sig-1", "2026-05-01T00:00:00Z", "LONG", 100.0),
            _make_signal("sig-2", "2026-05-01T00:05:00Z", "LONG", 100.0),
        ],
    }

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    result = run_portfolio_backtest(
        workspace_root=tmp_path,
        universe_run={"universe_run_id": pool_id},
        candidates=[_candidate(pool_id, "candidate-aave", "AAVE")],
        sessions=[_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        initial_capital_usdt=1000,
        margin_allocations_pct={"AAVE": 30},
        repository=MockRepository(signals),
    )

    assert result["summary"]["executed_positions"] == 1
    # Second signal should be skipped because asset has open position
    assert result["summary"]["total_signals"] == 2
    assert result["summary"]["skipped_asset_open"] == 1
    assert result["summary"]["skipped_signals"] == 1
    assert result["skipped_signals"][0]["skip_reason"] == "asset_position_open"


def test_portfolio_backtest_rejects_pool_without_stage4_complete_assets(tmp_path: Path):
    with pytest.raises(ValueError, match="at least one Stage 4-complete asset"):
        run_portfolio_backtest(
            workspace_root=tmp_path,
            universe_run={"universe_run_id": "pool-vegas"},
            candidates=[_candidate("pool-vegas", "candidate-aave", "AAVE")],
            sessions=[],
            initial_capital_usdt=1000,
            margin_allocations_pct={"AAVE": 30},
            repository=MockRepository(),
        )


def test_portfolio_backtest_run_history_can_delete_latest_and_promote_previous(tmp_path: Path, monkeypatch):
    pool_id = "pool-vegas"
    aave_root = _write_stage4_artifacts(
        tmp_path, asset="AAVE", session_id="stage1-aave", candidate_id="candidate-aave",
        signal_records=[{"signal_id": "sig-1", "decision_direction": "LONG", "agreement": "MATCH"}],
    )
    candles = _make_candles("2026-05-01T00:00:00Z", 200, base_price=100.0, trend=0.1)
    signals = {"sigset-aave": [_make_signal("sig-1", "2026-05-01T00:00:00Z", "LONG", 100.0)]}

    monkeypatch.setattr(
        "quant_terminal_worker.stage4.portfolio_backtest.MarketDataReader",
        lambda **kw: MockMarketDataReader({"AAVE": candles}),
    )

    common = {
        "workspace_root": tmp_path,
        "universe_run": {"universe_run_id": pool_id},
        "candidates": [_candidate(pool_id, "candidate-aave", "AAVE")],
        "sessions": [_session(pool_id, "candidate-aave", "AAVE", aave_root)],
        "repository": MockRepository(signals),
    }
    first = run_portfolio_backtest(**common, initial_capital_usdt=1000, margin_allocations_pct={"AAVE": 20})
    second = run_portfolio_backtest(**common, initial_capital_usdt=1000, margin_allocations_pct={"AAVE": 40})

    history = list_portfolio_backtest_runs(workspace_root=tmp_path, universe_run_id=pool_id)
    assert history["latest_run_id"] == second["run_id"]
    assert [item["run_id"] for item in history["runs"]] == [second["run_id"], first["run_id"]]

    deleted = delete_portfolio_backtest_run(workspace_root=tmp_path, universe_run_id=pool_id, run_id=second["run_id"])

    assert deleted["latest_run_id"] == first["run_id"]
    assert not (tmp_path / second["run_root"]).exists()
    latest = json.loads((tmp_path / "dev" / "portfolio_backtests" / pool_id / "portfolio_backtest.json").read_text())
    assert latest["run_id"] == first["run_id"]
