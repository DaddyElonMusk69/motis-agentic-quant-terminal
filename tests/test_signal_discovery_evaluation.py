from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_sdk.engine_contracts import LiveSignalScanResult
from quant_terminal_sdk.market_data_reader import MarketDataCandle
import quant_terminal_worker.signal_discovery.evaluation as discovery_evaluation
from quant_terminal_worker.signal_discovery.evaluation import (
    evaluate_registered_engine,
    score_candidate_signals,
)


def test_signal_discovery_engine_scoring_uses_each_path_and_counts_neutral_mismatches() -> None:
    train_timestamps = [_ts("2026-01-01"), _ts("2026-01-03"), _ts("2026-01-05")]
    wf_timestamps = [_ts("2026-01-07"), _ts("2026-01-09"), _ts("2026-01-11")]
    outcomes = {
        train_timestamps[0]: "LONG",
        train_timestamps[1]: "SHORT",
        train_timestamps[2]: "NEUTRAL",
        wf_timestamps[0]: "LONG",
        wf_timestamps[1]: "SHORT",
        wf_timestamps[2]: "NEUTRAL",
    }
    signals = [
        _signal(timestamp, state_code="A" if outcome != "SHORT" else "B")
        for timestamp, outcome in outcomes.items()
    ]
    candles = _candles_with_outcomes(outcomes)
    strategy = SimpleNamespace(decide=_decide)

    result = score_candidate_signals(
        signals=signals,
        candles=candles,
        strategy_module=strategy,
        selected_target={
            "selected_risk_pct": 1.0,
            "reward_multiple": 2.0,
            "stop_multiple": 1.0,
            "horizon_hours": 36,
            "entry_delay_minutes": 5,
            "entry_semantics": "next_5m_open",
            "fee_bps_per_side": 2.5,
            "slippage_bps_per_side": 2.5,
        },
        split_windows={
            "training": (train_timestamps[0], train_timestamps[-1]),
            "walk_forward": (wf_timestamps[0], wf_timestamps[-1]),
        },
        episodes_by_split={
            "training": [
                _episode("train-long", "LONG", train_timestamps[0]),
                _episode("train-short", "SHORT", train_timestamps[1]),
            ],
            "walk_forward": [
                _episode("wf-long", "LONG", wf_timestamps[0]),
                _episode("wf-short", "SHORT", wf_timestamps[1]),
            ],
        },
        cadence={
            "training_dedupe_window_minutes": 240,
            "live_dedupe_window_minutes": 240,
        },
    )

    assert result["schema_version"] == "signal_discovery_engine_evaluation.v1"
    assert result["contracts"] == {
        "packet_neutrality": True,
        "strategy_wrapper_compatible": True,
        "cadence_metadata_parity": True,
    }
    for split in ("training", "walk_forward"):
        metrics = result["slices"][split]
        assert metrics["emitted_timestamp_count"] == 3
        assert metrics["target_label_counts"] == {
            "LONG": 1,
            "SHORT": 1,
            "NEUTRAL": 1,
            "AMBIGUOUS": 0,
        }
        assert metrics["strategy_decision_counts"] == {
            "LONG": 2,
            "SHORT": 1,
            "NEUTRAL": 0,
        }
        assert metrics["opportunity_precision"] == pytest.approx(2 / 3)
        assert metrics["episode_recall"] == 1.0
        assert metrics["directional_accuracy"] == pytest.approx(2 / 3)
        assert metrics["entered_count"] == 3
        assert metrics["chosen_path_outcome_counts"] == {
            "TP": 2,
            "SL": 0,
            "TIMEOUT": 1,
            "AMBIGUOUS": 0,
        }
        assert metrics["net_r_after_costs"] == pytest.approx(3.7)
        assert metrics["expected_net_r_per_emitted_timestamp"] == pytest.approx(3.7 / 3)
    assert result["accepted"] is True

    mismatched = score_candidate_signals(
        signals=signals,
        candles=candles,
        strategy_module=strategy,
        selected_target={
            "selected_risk_pct": 1.0,
            "reward_multiple": 2.0,
            "stop_multiple": 1.0,
            "horizon_hours": 36,
            "entry_delay_minutes": 5,
        },
        split_windows={
            "training": (train_timestamps[0], train_timestamps[-1]),
            "walk_forward": (wf_timestamps[0], wf_timestamps[-1]),
        },
        episodes_by_split={"training": [], "walk_forward": []},
        cadence={
            "training_dedupe_window_minutes": 240,
            "live_dedupe_window_minutes": 240,
            "packet_metadata_parity": False,
        },
    )
    assert mismatched["contracts"]["cadence_metadata_parity"] is False
    assert mismatched["accepted"] is False


def test_signal_discovery_engine_scoring_rejects_directional_packets() -> None:
    timestamp = _ts("2026-01-01")
    signal = _signal(timestamp, state_code="A")
    signal["payload"]["direction"] = "LONG"

    with pytest.raises(ValueError, match="packet neutrality"):
        score_candidate_signals(
            signals=[signal],
            candles=_candles_with_outcomes({timestamp: "LONG"}),
            strategy_module=SimpleNamespace(decide=_decide),
            selected_target={
                "selected_risk_pct": 1.0,
                "reward_multiple": 2.0,
                "stop_multiple": 1.0,
                "horizon_hours": 36,
                "entry_delay_minutes": 5,
            },
            split_windows={"training": (timestamp, timestamp)},
            episodes_by_split={"training": []},
            cadence={
                "training_dedupe_window_minutes": 240,
                "live_dedupe_window_minutes": 240,
            },
        )


def test_signal_discovery_engine_registered_evaluation_fills_and_persists(
    tmp_path: Path,
    monkeypatch,
) -> None:
    train_timestamps = [_ts("2026-01-01"), _ts("2026-01-03"), _ts("2026-01-05")]
    wf_timestamps = [_ts("2026-01-07"), _ts("2026-01-09"), _ts("2026-01-11")]
    outcomes = {
        train_timestamps[0]: "LONG",
        train_timestamps[1]: "SHORT",
        train_timestamps[2]: "NEUTRAL",
        wf_timestamps[0]: "LONG",
        wf_timestamps[1]: "SHORT",
        wf_timestamps[2]: "NEUTRAL",
    }
    signals = [
        _signal(timestamp, state_code="A" if outcome != "SHORT" else "B")
        for timestamp, outcome in outcomes.items()
    ]
    signal_set_key = "fixture-engine:BTC:BTC-fixture-engine-canonical"
    signal_set = {
        "signal_set_key": signal_set_key,
        "signal_engine_id": "fixture-engine",
        "signal_engine_version": "0.1",
        "manifest": {"parameters": {"dedupe_window_minutes": 240}},
    }
    artifact_root = tmp_path / "dev/signal_discovery_sessions/discovery-eval"
    _write_episode_parquet(
        artifact_root / "atlas/training_episodes.parquet",
        [
            {**_episode("train-long", "LONG", train_timestamps[0]), "risk_pct": 1.0, "entry_delay_minutes": 5, "horizon_hours": 36.0},
            {**_episode("train-short", "SHORT", train_timestamps[1]), "risk_pct": 1.0, "entry_delay_minutes": 5, "horizon_hours": 36.0},
        ],
    )
    _write_episode_parquet(
        artifact_root / "walk_forward/walk_forward_episodes.parquet",
        [
            _episode("wf-long", "LONG", wf_timestamps[0]),
            _episode("wf-short", "SHORT", wf_timestamps[1]),
        ],
    )
    strategy_path = tmp_path / "paired_strategy.py"
    strategy_path.write_text(
        "def decide(context):\n"
        "    payload = context.get('signal', {}).get('payload', {})\n"
        "    state = payload.get('evidence', {}).get('state_code')\n"
        "    if state not in {'A', 'B'}:\n"
        "        return {'trade_action': 'SKIP', 'direction': 'FLAT', 'reason_code': 'no_state'}\n"
        "    return {'trade_action': 'ENTER', 'direction': 'LONG' if state == 'A' else 'SHORT', 'reason_code': 'state'}\n"
    )
    spec = SimpleNamespace(
        signal_engine_id="fixture-engine",
        version="0.1",
        code_ref={"base_strategy_path": str(strategy_path)},
        configuration_schema={"default_parameters": {"dedupe_window_minutes": 240}},
    )
    resolved = SimpleNamespace(
        spec=spec,
        scan_live_signal=lambda context: LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest row is not an event",
        ),
    )
    repository = _EvaluationRepository(signal_set=signal_set, signals=signals)
    calls = {}

    def record_generation(**kwargs):
        calls["generation"] = kwargs
        return {"status": "extended", "appended_packet_count": len(signals)}

    monkeypatch.setattr(
        discovery_evaluation,
        "resolve_signal_engine",
        lambda signal_engine_id, **kwargs: calls.setdefault("resolved", (signal_engine_id, kwargs)) and resolved,
    )
    monkeypatch.setattr(
        discovery_evaluation,
        "extend_signal_pool_from_local_candles",
        record_generation,
    )
    monkeypatch.setattr(
        discovery_evaluation,
        "read_candles_from_ref",
        lambda *args, **kwargs: _candles_with_outcomes(outcomes),
    )
    session = {
        "session_id": "discovery-eval",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "dataset_id": "btc-raw-5m",
        "research_start": train_timestamps[0],
        "research_end": train_timestamps[-1],
        "walk_forward_start": wf_timestamps[0],
        "walk_forward_end": wf_timestamps[-1],
        "artifact_root": str(artifact_root),
        "candidate_engine_id": "fixture-engine",
        "candidate_signal_set_key": signal_set_key,
        "frozen_target": {
            "config_hash": "frozen-hash",
            "source_data": {
                "dataset_id": "btc-raw-5m",
                "storage_backend": "parquet",
                "storage_uri": "unused-in-fixture",
            },
            "selected_target": {
                "selected_risk_pct": 1.0,
                "reward_multiple": 2.0,
                "stop_multiple": 1.0,
                "horizon_hours": 36,
                "entry_delay_minutes": 5,
                "entry_semantics": "next_5m_open",
                "fee_bps_per_side": 2.5,
                "slippage_bps_per_side": 2.5,
            },
        },
    }

    result = evaluate_registered_engine(
        workspace_root=tmp_path,
        repository=repository,
        session=session,
    )

    assert calls["resolved"][0] == "fixture-engine"
    assert calls["generation"]["signal_engine_id"] == "fixture-engine"
    assert result["accepted"] is True
    assert result["target_config_hash"] == "frozen-hash"
    assert result["cadence"]["live_scan_status"] == "no_fresh_signal"
    evaluation_path = artifact_root / "evaluation/engine_evaluation.json"
    assert Path(result["evaluation_path"]) == evaluation_path
    assert json.loads(evaluation_path.read_text())["accepted"] is True


def _decide(context: dict[str, object]) -> dict[str, object]:
    payload = context["signal"]["payload"]
    state_code = payload["evidence"]["state_code"]
    return {
        "trade_action": "ENTER",
        "direction": "LONG" if state_code == "A" else "SHORT",
        "confidence": 0.7,
        "reason_code": "fixture_state",
    }


def _signal(timestamp: datetime, *, state_code: str) -> dict[str, object]:
    iso = timestamp.isoformat().replace("+00:00", "Z")
    return {
        "signal_id": f"fixture:{timestamp.strftime('%Y%m%d%H%M')}",
        "signal_set_key": "fixture-engine:BTC:BTC-fixture-engine-canonical",
        "signal_engine_id": "fixture-engine",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "timestamp": timestamp,
        "payload_schema": "signal_packet.v2",
        "payload": {
            "schema_version": "signal_packet.v2",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "timestamp": iso,
            "evidence": {
                "state_code": state_code,
                "reference_price": 100.0,
                "trigger_candle_close": 100.0,
                "dedupe_window_minutes": 240,
            },
        },
    }


def _candles_with_outcomes(outcomes: dict[datetime, str]) -> list[MarketDataCandle]:
    start = min(outcomes)
    end = max(outcomes) + timedelta(hours=37)
    outcome_candles = {
        timestamp + timedelta(minutes=10): outcome
        for timestamp, outcome in outcomes.items()
        if outcome != "NEUTRAL"
    }
    candles = []
    timestamp = start
    while timestamp <= end:
        outcome = outcome_candles.get(timestamp)
        high = Decimal("103") if outcome == "LONG" else Decimal("100.25")
        low = Decimal("97") if outcome == "SHORT" else Decimal("99.75")
        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=Decimal("100"),
                high=high,
                low=low,
                close=Decimal("100"),
                volume=Decimal("10"),
                vol_ccy=Decimal("10"),
                vol_ccy_quote=Decimal("1000"),
                confirm=1,
            )
        )
        timestamp += timedelta(minutes=5)
    return candles


def _episode(episode_id: str, direction: str, timestamp: datetime) -> dict[str, object]:
    return {
        "episode_id": episode_id,
        "direction": direction,
        "start_ts": timestamp,
        "end_ts": timestamp,
    }


def _write_episode_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)


class _EvaluationRepository:
    def __init__(self, *, signal_set: dict[str, object], signals: list[dict[str, object]]) -> None:
        self.signal_set = signal_set
        self.signals = signals

    def get_signal_set(self, signal_set_key: str):
        return self.signal_set if signal_set_key == self.signal_set["signal_set_key"] else None

    def list_signals_for_signal_set_window(self, **kwargs):
        return list(self.signals)


def _ts(day: str) -> datetime:
    return datetime.fromisoformat(f"{day}T00:00:00+00:00").astimezone(UTC)
