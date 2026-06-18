import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from quant_terminal_worker.stage4.portfolio_backtest import delete_portfolio_backtest_run
from quant_terminal_worker.stage4.portfolio_backtest import list_portfolio_backtest_runs
from quant_terminal_worker.stage4.portfolio_backtest import run_portfolio_backtest


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

    (promotion_root / "stage4_realized_expectancy.json").write_text(
        json.dumps({
            "run_id": "stage4-run",
            "asset": asset,
            "best_candidate_id": candidate_id,
            "best_candidate": {"candidate_id": candidate_id, "setup": candidate_setup},
            "cost_assumptions": {"fees_bps_per_side": 5.0, "slippage_bps_per_side": 0.0},
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
    # (cursor advances past it while position is open)
    assert result["summary"]["total_signals"] == 2


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
