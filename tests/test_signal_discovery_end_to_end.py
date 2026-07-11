from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from quant_terminal_api.db.models import metadata
from quant_terminal_api.main import create_app
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_sdk.engine_contracts import LiveSignalScanResult, SignalEngineSpec
import quant_terminal_worker.signal_discovery.evaluation as discovery_evaluation
from quant_terminal_worker.jobs import run_claimed_job


def test_fresh_discovery_session_reaches_stage1_without_artifact_surgery(
    tmp_path: Path,
    monkeypatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    repository = _repository()
    dataset_id = "btc-outcome-first-e2e"
    storage_uri = _write_canonical_parquet(tmp_path)
    market_repository = _MarketDataRepository(
        {
            "dataset_id": dataset_id,
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "ingestion_version": "fixture-v1",
        }
    )
    client = TestClient(
        create_app(
            runtime_repository=repository,
            market_data_repository=market_repository,
        )
    )
    session_id = "discovery-btc-e2e"

    created = client.post(
        "/api/v1/research/signal-discovery-sessions",
        json={
            "session_id": session_id,
            "name": "BTC outcome-first E2E",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_id": dataset_id,
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-01T00:10:00Z",
            "walk_forward_start": "2026-01-04T00:00:00Z",
            "walk_forward_end": "2026-01-04T00:10:00Z",
            "risk_values": [0.75, 1.0, 1.25],
            "reward_multiple": 2.0,
            "stop_multiple": 1.0,
            "horizon_hours": [36, 48],
            "entry_delays_minutes": [5, 10],
            "fee_bps_per_side": 2.5,
            "slippage_bps_per_side": 2.5,
        },
    )
    assert created.status_code == 200
    artifact_root = Path(created.json()["session"]["artifact_root"])

    assert client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/atlas"
    ).status_code == 200
    _run_next_job(
        repository=repository,
        expected_type="signal_discovery_atlas",
        workspace_root=tmp_path,
        market_data_repository=market_repository,
    )
    atlas_session = repository.get_signal_discovery_session(session_id)
    assert atlas_session["status"] == "atlas_ready"
    feasibility = atlas_session["summary"]
    assert {row["risk_pct"] for row in feasibility["r_summaries"]} == {
        0.75,
        1.0,
        1.25,
    }
    assert feasibility["training_episode_count"] > 0
    assert not (artifact_root / "walk_forward").exists()

    frozen_response = client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/freeze",
        json={
            "selected_risk_pct": 1.0,
            "horizon_hours": 36,
            "entry_delay_minutes": 5,
        },
    )
    assert frozen_response.status_code == 200
    frozen = frozen_response.json()["target"]
    assert frozen["schema_version"] == "signal_discovery_target.v1"
    assert frozen["selected_target"]["reward_multiple"] == 2.0
    assert frozen["selected_target"]["stop_multiple"] == 1.0
    assert frozen["selected_target"]["entry_semantics"] == "next_5m_open"
    assert frozen["source_data"]["storage_backend"] == "parquet"

    prompt_response = client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/engine-builder-prompt"
    )
    assert prompt_response.status_code == 200
    prompt = prompt_response.json()["prompt"]
    assert "$signal-engine-builder" in prompt
    assert "walk_forward_timestamp_labels.parquet" not in prompt
    assert "walk_forward_episodes.parquet" not in prompt

    assert client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/walk-forward"
    ).status_code == 200
    _run_next_job(
        repository=repository,
        expected_type="signal_discovery_walk_forward",
        workspace_root=tmp_path,
        market_data_repository=market_repository,
    )
    assert repository.get_signal_discovery_session(session_id)["status"] == (
        "walk_forward_ready"
    )

    engine_id = "fixture-engine"
    signal_set_id = "BTC-fixture-engine-canonical"
    signal_set_key = f"{engine_id}:BTC:{signal_set_id}"
    decision_timestamps = [_ts("2026-01-01"), _ts("2026-01-04")]
    repository.upsert_signal_set(
        {
            "signal_set_key": signal_set_key,
            "signal_set_id": signal_set_id,
            "signal_engine_id": engine_id,
            "signal_engine_version": "0.1",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "start_ts": decision_timestamps[0],
            "end_ts": decision_timestamps[-1],
            "packet_count": len(decision_timestamps),
            "payload_schema": "signal_packet.v2",
            "source_path": "fixture-engine:canonical",
            "manifest": {"parameters": {"dedupe_window_minutes": 240}},
        }
    )
    repository.upsert_signals(
        [
            _signal(signal_set_key=signal_set_key, timestamp=timestamp)
            for timestamp in decision_timestamps
        ]
    )
    strategy_path = tmp_path / "fixture_engine_strategy.py"
    strategy_path.write_text(
        "def decide(context):\n"
        "    payload = context.get('signal', {}).get('payload', {})\n"
        "    state = payload.get('evidence', {}).get('state_code')\n"
        "    if state != 'opportunity':\n"
        "        return {'trade_action': 'SKIP', 'direction': 'FLAT', 'reason_code': 'no_state'}\n"
        "    return {'trade_action': 'ENTER', 'direction': 'LONG', 'reason_code': 'opportunity'}\n"
    )
    spec = SignalEngineSpec(
        signal_engine_id=engine_id,
        version="0.1",
        required_data=[
            {
                "data_type": "candles",
                "origin": "raw",
                "timeframe": "5m",
                "lookback_bars": 100,
            }
        ],
        output_envelope_version="signal_packet.v2",
        runtime_entrypoint="fixture:generate_training_signals",
        live_scanner_entrypoint="fixture:scan_live_signal",
        code_ref={"base_strategy_path": str(strategy_path)},
        configuration_schema={
            "default_parameters": {"dedupe_window_minutes": 240}
        },
    )
    resolved = SimpleNamespace(
        spec=spec,
        scan_live_signal=lambda _context: LiveSignalScanResult(
            status="no_fresh_signal",
            source="live_parquet_snapshot",
            reason="latest candle is not an event",
        ),
    )
    monkeypatch.setattr(
        discovery_evaluation,
        "resolve_signal_engine",
        lambda *_args, **_kwargs: resolved,
    )
    monkeypatch.setattr(
        discovery_evaluation,
        "extend_signal_pool_from_local_candles",
        lambda **_kwargs: {"status": "noop", "appended_packet_count": 0},
    )

    attached = client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/candidate",
        json={
            "signal_engine_id": engine_id,
            "signal_set_key": signal_set_key,
        },
    )
    assert attached.status_code == 200
    assert client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/evaluate"
    ).status_code == 200
    _run_next_job(
        repository=repository,
        expected_type="signal_discovery_engine_evaluation",
        workspace_root=tmp_path,
    )
    evaluated = repository.get_signal_discovery_session(session_id)
    assert evaluated["status"] == "accepted"
    assert evaluated["evaluation"]["accepted"] is True
    assert evaluated["evaluation"]["slices"]["training"]["net_r_after_costs"] > 0
    assert evaluated["evaluation"]["slices"]["walk_forward"]["net_r_after_costs"] > 0

    assert client.post(
        f"/api/v1/research/signal-discovery-sessions/{session_id}/handoff"
    ).status_code == 200
    _run_next_job(
        repository=repository,
        expected_type="signal_discovery_handoff",
        workspace_root=tmp_path,
    )
    handed_off = repository.get_signal_discovery_session(session_id)
    assert handed_off["status"] == "handed_off"
    candidate_id = handed_off["handoff"]["candidate_id"]
    candidate = repository.get_stage0_universe_candidate(candidate_id)
    assert candidate["metrics"]["label_contract"] == "fixed_r_first_touch.v1"
    assert candidate["metrics"]["fixed_target_contract_path"] == (
        handed_off["handoff"]["fixed_target_contract_path"]
    )

    stage1_response = client.post(
        "/api/v1/research/stage1-sessions",
        json={
            "source_candidate_id": candidate_id,
            "strategy_id": "btc-outcome-first-e2e",
            "strategy_version": "v0.1",
        },
    )
    assert stage1_response.status_code == 200
    stage1 = stage1_response.json()["session"]
    assert stage1["source_candidate_id"] == candidate_id
    assert stage1["manifest"]["stage0_artifact_root"] == str(
        artifact_root / "handoff/stage0"
    )
    assert Path(stage1["manifest"]["strategy_path"]).is_file()


def _run_next_job(
    *,
    repository: RuntimeRepository,
    expected_type: str,
    workspace_root: Path,
    market_data_repository: _MarketDataRepository | None = None,
) -> None:
    job = repository.claim_next_job(worker_id="signal-discovery-e2e")
    assert job is not None
    assert job["job_type"] == expected_type
    completed = run_claimed_job(
        repository=repository,
        job=job,
        workspace_root=workspace_root,
        market_data_repository=market_data_repository,
    )
    assert completed is not None
    assert completed["status"] == "completed", completed.get("error")


def _repository() -> RuntimeRepository:
    engine = create_engine(
        "sqlite+pysqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    metadata.create_all(engine)
    return RuntimeRepository(engine)


class _MarketDataRepository:
    def __init__(self, ref: dict[str, object]) -> None:
        self.ref = ref

    def get_ref(self, dataset_id: str):
        return self.ref if dataset_id == self.ref["dataset_id"] else None


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
        "data_refs": ["btc-outcome-first-e2e"],
        "payload_schema": "signal_packet.v2",
        "payload": {
            "schema_version": "signal_packet.v2",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "timestamp": iso,
            "evidence": {
                "state_code": "opportunity",
                "reference_price": 100.0,
                "trigger_candle_close": 100.0,
                "dedupe_window_minutes": 240,
            },
        },
    }


def _write_canonical_parquet(tmp_path: Path) -> Path:
    storage_root = tmp_path / ".data/market-data/btc-outcome-first-e2e"
    outcome_timestamps = {
        _ts("2026-01-01") + timedelta(minutes=10),
        _ts("2026-01-04") + timedelta(minutes=10),
    }
    rows_by_month: dict[tuple[int, int], list[dict[str, object]]] = {}
    timestamp = datetime(2025, 12, 25, tzinfo=UTC)
    end = datetime(2026, 1, 6, 1, tzinfo=UTC)
    while timestamp <= end:
        is_long_target = timestamp in outcome_timestamps
        rows_by_month.setdefault((timestamp.year, timestamp.month), []).append(
            {
                "timestamp": timestamp,
                "open": Decimal("100"),
                "high": Decimal("103") if is_long_target else Decimal("100.25"),
                "low": Decimal("99.75"),
                "close": Decimal("100"),
                "volume": Decimal("100"),
                "vol_ccy": Decimal("100"),
                "vol_ccy_quote": Decimal("10000"),
                "confirm": 1,
            }
        )
        timestamp += timedelta(minutes=5)
    for (year, month), rows in rows_by_month.items():
        path = storage_root / f"year={year}" / f"month={month:02d}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path)
    return storage_root


def _ts(day: str) -> datetime:
    return datetime.fromisoformat(f"{day}T00:00:00+00:00").astimezone(UTC)
