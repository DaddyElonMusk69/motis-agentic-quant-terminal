from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from quant_terminal_api.db.models import metadata
from quant_terminal_api.main import create_app
from quant_terminal_api.repositories.runtime import RuntimeRepository
import quant_terminal_worker.jobs as worker_jobs
from quant_terminal_worker.jobs import run_claimed_job
from quant_terminal_worker.signal_discovery.handoff import handoff_accepted_candidate


def test_signal_discovery_handoff_materializes_fixed_target_stage0_and_starts_stage1(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    repository = _repository()
    engine_id = "fixture-engine"
    signal_set_id = "BTC-fixture-engine-canonical"
    signal_set_key = f"{engine_id}:BTC:{signal_set_id}"
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
    source_path = _write_candle_parquet(tmp_path, outcomes=outcomes)
    repository.upsert_signal_set(
        {
            "signal_set_key": signal_set_key,
            "signal_set_id": signal_set_id,
            "signal_engine_id": engine_id,
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "start_ts": min(outcomes),
            "end_ts": max(outcomes),
            "packet_count": len(outcomes),
            "payload_schema": "signal_packet.v2",
            "source_path": "canonicalized:signals",
            "manifest": {"parameters": {"dedupe_window_minutes": 240}},
        }
    )
    for timestamp in outcomes:
        repository.upsert_signal(_signal(signal_set_key=signal_set_key, timestamp=timestamp))
    artifact_root = tmp_path / "dev/signal_discovery_sessions/discovery-handoff"
    session = {
        "session_id": "discovery-handoff",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "dataset_id": "btc-raw-5m",
        "research_start": train_timestamps[0],
        "research_end": train_timestamps[-1],
        "walk_forward_start": wf_timestamps[0],
        "walk_forward_end": wf_timestamps[-1],
        "artifact_root": str(artifact_root),
        "target_version": 1,
        "candidate_engine_id": engine_id,
        "candidate_signal_set_key": signal_set_key,
        "evaluation": {
            "schema_version": "signal_discovery_engine_evaluation.v1",
            "accepted": True,
            "slices": {
                "training": {"net_r_after_costs": 3.7},
                "walk_forward": {"net_r_after_costs": 3.7},
            },
        },
        "frozen_target": {
            "schema_version": "signal_discovery_target.v1",
            "target_version": 1,
            "config_hash": "frozen-target-hash",
            "source_data": {
                "dataset_id": "btc-raw-5m",
                "storage_backend": "parquet",
                "storage_uri": str(source_path),
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

    result = handoff_accepted_candidate(
        workspace_root=tmp_path,
        repository=repository,
        session=session,
    )

    candidate = repository.get_stage0_universe_candidate(result["candidate_id"])
    universe = repository.get_stage0_universe_run(result["universe_run_id"])
    assert result["candidate_id"] == candidate["candidate_id"]
    assert universe["train_start"].isoformat() == "2026-01-01"
    assert universe["walk_forward_end"].isoformat() == "2026-01-11"
    assert universe["forward_hours"] == 36
    assert candidate["acceptance_status"] == "accepted"
    assert candidate["signal_set_key"] == signal_set_key
    assert candidate["packet_count"] == 6
    assert candidate["metrics"]["label_contract"] == "fixed_r_first_touch.v1"
    assert candidate["metrics"]["target_pct"] == 2.0
    assert candidate["metrics"]["stop_pct"] == 1.0
    assert candidate["metrics"]["forward_hours"] == 36
    assert candidate["metrics"]["source_discovery_session_id"] == "discovery-handoff"
    assert candidate["metrics"]["target_config_hash"] == "frozen-target-hash"

    stage0_root = artifact_root / "handoff/stage0"
    ground_truth_files = sorted((stage0_root / "scores/ground_truth").glob("*.json"))
    packet_files = sorted((stage0_root / "scores/_scoreable_signal_subset/packets").glob("*.json"))
    assert len(ground_truth_files) == len(packet_files) == 6
    labels = {
        json.loads(path.read_text())["natural_direction"] for path in ground_truth_files
    }
    assert labels == {"LONG", "SHORT", "NEUTRAL"}
    summary = json.loads((stage0_root / "scores/ground_truth_summary.json").read_text())
    assert summary["label_contract"] == "fixed_r_first_touch.v1"
    assert summary["metrics"]["total_records"] == 6
    assert summary["metrics"]["direction_counts"] == {
        "AMBIGUOUS": 0,
        "LONG": 2,
        "NEUTRAL": 2,
        "SHORT": 2,
    }

    response = TestClient(create_app(runtime_repository=repository)).post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": result["candidate_id"],
            "strategy_id": "btc-outcome-first-v1",
            "strategy_version": "v0.1",
        },
    )
    assert response.status_code == 200
    stage1_session = response.json()["session"]
    assert stage1_session["source_candidate_id"] == result["candidate_id"]
    assert stage1_session["manifest"]["stage0_artifact_root"] == str(stage0_root)


def test_signal_discovery_handoff_job_persists_terminal_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    repository = _repository()
    repository.create_signal_discovery_session(
        {
            "session_id": "discovery-worker-handoff",
            "name": "Worker Handoff",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_id": "btc-raw-5m",
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-31T00:00:00Z",
            "walk_forward_start": "2026-02-01T00:00:00Z",
            "walk_forward_end": "2026-02-28T00:00:00Z",
            "artifact_root": str(tmp_path / "discovery-worker-handoff"),
            "config": {},
        }
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="atlas_ready",
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="target_frozen",
        frozen_target={"config_hash": "abc", "selected_target": {}},
        target_version=1,
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="candidate_attached",
        candidate_engine_id="fixture-engine",
        candidate_signal_set_key="fixture-engine:BTC:BTC-fixture-engine-canonical",
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="evaluation_running",
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="evaluated",
        evaluation={"accepted": True},
    )
    repository.update_signal_discovery_session(
        "discovery-worker-handoff",
        status="accepted",
    )
    handoff = {
        "schema_version": "signal_discovery_handoff.v1",
        "universe_run_id": "stage0-discovery-worker-v1",
        "candidate_id": "candidate-worker-v1",
    }
    monkeypatch.setattr(
        worker_jobs,
        "handoff_accepted_candidate",
        lambda **_: handoff,
    )
    repository.enqueue_job(
        job_type="signal_discovery_handoff",
        scope_key="signal_discovery:discovery-worker-handoff",
        payload={"session_id": "discovery-worker-handoff"},
    )

    completed = run_claimed_job(
        repository=repository,
        job=repository.claim_next_job(worker_id="worker-discovery"),
        workspace_root=tmp_path,
    )

    assert completed["status"] == "completed"
    session = repository.get_signal_discovery_session("discovery-worker-handoff")
    assert session["status"] == "handed_off"
    assert session["handoff"] == handoff


def _repository() -> RuntimeRepository:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    return RuntimeRepository(engine)


def _signal(*, signal_set_key: str, timestamp: datetime) -> dict[str, object]:
    suffix = timestamp.strftime("%Y%m%dT%H%M%SZ")
    iso = timestamp.isoformat().replace("+00:00", "Z")
    return {
        "signal_id": f"fixture-engine:BTC:BTC-fixture-engine-canonical:{suffix}",
        "signal_set_key": signal_set_key,
        "signal_engine_id": "fixture-engine",
        "signal_engine_version": "0.1",
        "asset": "BTC",
        "instrument": "BTC-USDT-SWAP",
        "timestamp": timestamp,
        "data_refs": ["btc-raw-5m"],
        "payload_schema": "signal_packet.v2",
        "payload": {
            "schema_version": "signal_packet.v2",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "timestamp": iso,
            "evidence": {
                "reference_price": 100.0,
                "trigger_candle_close": 100.0,
            },
        },
    }


def _write_candle_parquet(
    tmp_path: Path,
    *,
    outcomes: dict[datetime, str],
) -> Path:
    storage_root = tmp_path / ".data/market-data/btc-raw-5m"
    rows = []
    outcome_candles = {
        timestamp + timedelta(minutes=10): outcome
        for timestamp, outcome in outcomes.items()
        if outcome != "NEUTRAL"
    }
    timestamp = min(outcomes)
    end = max(outcomes) + timedelta(hours=37)
    while timestamp <= end:
        outcome = outcome_candles.get(timestamp)
        rows.append(
            {
                "timestamp": timestamp,
                "open": Decimal("100"),
                "high": Decimal("103") if outcome == "LONG" else Decimal("100.25"),
                "low": Decimal("97") if outcome == "SHORT" else Decimal("99.75"),
                "close": Decimal("100"),
                "volume": Decimal("10"),
                "vol_ccy": Decimal("10"),
                "vol_ccy_quote": Decimal("1000"),
                "confirm": 1,
            }
        )
        timestamp += timedelta(minutes=5)
    path = storage_root / "year=2026/month=01/data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    return storage_root


def _ts(day: str) -> datetime:
    return datetime.fromisoformat(f"{day}T00:00:00+00:00").astimezone(UTC)
