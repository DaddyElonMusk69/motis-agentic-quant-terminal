import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine

from quant_terminal_api.db.models import metadata
from quant_terminal_api.repositories.runtime import RuntimeRepository
import quant_terminal_worker.jobs as worker_jobs
from quant_terminal_worker.job_routing import queue_for_job
from quant_terminal_worker.jobs import run_claimed_job
from quant_terminal_worker.signal_discovery.workspace import freeze_target_contract


def test_worker_runs_training_and_frozen_walk_forward_signal_discovery(tmp_path):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    dataset_id = "okx-btc-raw-5m-discovery"
    storage_uri = _write_discovery_candles(tmp_path)
    market_repository = DiscoveryMarketDataRepository(
        {
            "dataset_id": dataset_id,
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
        }
    )
    artifact_root = tmp_path / "dev/signal_discovery_sessions/discovery-btc"
    session = repository.create_signal_discovery_session(
        {
            "session_id": "discovery-btc",
            "name": "BTC Fixed R",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_id": dataset_id,
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-01T00:10:00Z",
            "walk_forward_start": "2026-01-04T00:00:00Z",
            "walk_forward_end": "2026-01-04T00:10:00Z",
            "artifact_root": str(artifact_root),
            "status": "draft",
            "config": {
                "risk_values": [1.0],
                "reward_multiple": 2.0,
                "stop_multiple": 1.0,
                "horizon_hours": [36, 48],
                "entry_delays_minutes": [5, 10],
                "fee_bps_per_side": 5.0,
                "slippage_bps_per_side": 5.0,
            },
        }
    )
    repository.enqueue_job(
        job_type="signal_discovery_atlas",
        scope_key="signal_discovery:discovery-btc",
        payload={"session_id": "discovery-btc"},
    )

    atlas_job = repository.claim_next_job(worker_id="worker-discovery")
    completed_atlas = run_claimed_job(
        repository=repository,
        job=atlas_job,
        workspace_root=tmp_path,
        market_data_repository=market_repository,
    )

    assert completed_atlas["status"] == "completed"
    assert repository.get_signal_discovery_session("discovery-btc")["status"] == "atlas_ready"
    assert (artifact_root / "atlas/training_timestamp_labels.parquet").is_file()
    assert (artifact_root / "atlas/training_episodes.parquet").is_file()
    assert (artifact_root / "atlas/training_features.parquet").is_file()
    assert (artifact_root / "atlas/training_hard_negatives.parquet").is_file()
    assert (artifact_root / "atlas/r_feasibility.json").is_file()
    assert not (artifact_root / "walk_forward").exists()

    frozen_target = freeze_target_contract(
        artifact_root=artifact_root,
        session_id=session["session_id"],
        selected_target={
            "selected_risk_pct": 1.0,
            "reward_multiple": 2.0,
            "stop_multiple": 1.0,
            "horizon_hours": 36,
            "entry_delay_minutes": 5,
            "entry_semantics": "next_5m_open",
            "fee_bps_per_side": 5.0,
            "slippage_bps_per_side": 5.0,
        },
        source_data={
            "dataset_id": dataset_id,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "timeframe": "5m",
        },
        splits={
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-01T00:10:00Z",
            "walk_forward_start": "2026-01-04T00:00:00Z",
            "walk_forward_end": "2026-01-04T00:10:00Z",
        },
    )
    repository.update_signal_discovery_session(
        "discovery-btc",
        status="target_frozen",
        frozen_target=frozen_target,
        target_version=1,
    )
    repository.enqueue_job(
        job_type="signal_discovery_walk_forward",
        scope_key="signal_discovery:discovery-btc",
        payload={"session_id": "discovery-btc"},
    )

    wf_job = repository.claim_next_job(worker_id="worker-discovery")
    completed_wf = run_claimed_job(
        repository=repository,
        job=wf_job,
        workspace_root=tmp_path,
        market_data_repository=market_repository,
    )

    assert completed_wf["status"] == "completed"
    assert repository.get_signal_discovery_session("discovery-btc")[
        "status"
    ] == "walk_forward_ready"
    wf_rows = pq.read_table(
        artifact_root / "walk_forward/walk_forward_timestamp_labels.parquet"
    ).to_pylist()
    assert len(wf_rows) == 3
    assert {row["risk_pct"] for row in wf_rows} == {1.0}
    assert {row["horizon_hours"] for row in wf_rows} == {36.0}
    assert {row["entry_delay_minutes"] for row in wf_rows} == {5}


def test_signal_discovery_jobs_route_to_research_queue():
    assert queue_for_job("signal_discovery_atlas") == "research"
    assert queue_for_job("signal_discovery_walk_forward") == "research"
    assert queue_for_job("signal_discovery_engine_evaluation") == "research"
    assert queue_for_job("signal_discovery_handoff") == "research"


def test_signal_discovery_engine_evaluation_job_persists_accepted_result(
    tmp_path,
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    repository.create_signal_discovery_session(
        {
            "session_id": "discovery-evaluation",
            "name": "BTC Evaluation",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_id": "btc-raw-5m",
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-31T00:00:00Z",
            "walk_forward_start": "2026-02-01T00:00:00Z",
            "walk_forward_end": "2026-02-28T00:00:00Z",
            "artifact_root": str(tmp_path / "dev/signal_discovery_sessions/discovery-evaluation"),
            "config": {},
        }
    )
    repository.update_signal_discovery_session(
        "discovery-evaluation",
        status="atlas_ready",
    )
    repository.update_signal_discovery_session(
        "discovery-evaluation",
        status="target_frozen",
        frozen_target={"config_hash": "abc", "selected_target": {}},
        target_version=1,
    )
    repository.update_signal_discovery_session(
        "discovery-evaluation",
        status="candidate_attached",
        candidate_engine_id="fixture-engine",
        candidate_signal_set_key="fixture-engine:BTC:BTC-fixture-engine-canonical",
    )
    evaluation = {
        "schema_version": "signal_discovery_engine_evaluation.v1",
        "accepted": True,
        "slices": {"training": {"net_r_after_costs": 3.0}, "walk_forward": {"net_r_after_costs": 1.0}},
    }
    monkeypatch.setattr(
        worker_jobs,
        "evaluate_registered_engine",
        lambda **_: evaluation,
    )
    repository.enqueue_job(
        job_type="signal_discovery_engine_evaluation",
        scope_key="signal_discovery:discovery-evaluation",
        payload={"session_id": "discovery-evaluation"},
    )

    completed = run_claimed_job(
        repository=repository,
        job=repository.claim_next_job(worker_id="worker-discovery"),
        workspace_root=tmp_path,
    )

    assert completed["status"] == "completed"
    session = repository.get_signal_discovery_session("discovery-evaluation")
    assert session["status"] == "accepted"
    assert session["evaluation"] == evaluation


def test_signal_discovery_atlas_write_failure_never_marks_session_ready(
    tmp_path,
    monkeypatch,
):
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    dataset_id = "okx-btc-raw-5m-discovery"
    storage_uri = _write_discovery_candles(tmp_path)
    market_repository = DiscoveryMarketDataRepository(
        {
            "dataset_id": dataset_id,
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "5m",
            "data_origin": "raw",
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
        }
    )
    repository.create_signal_discovery_session(
        {
            "session_id": "discovery-write-failure",
            "name": "BTC Fixed R",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "dataset_id": dataset_id,
            "research_start": "2026-01-01T00:00:00Z",
            "research_end": "2026-01-01T00:10:00Z",
            "walk_forward_start": "2026-01-04T00:00:00Z",
            "walk_forward_end": "2026-01-04T00:10:00Z",
            "artifact_root": str(
                tmp_path / "dev/signal_discovery_sessions/discovery-write-failure"
            ),
            "config": {
                "risk_values": [1.0],
                "horizon_hours": [36],
                "entry_delays_minutes": [5],
            },
        }
    )
    repository.enqueue_job(
        job_type="signal_discovery_atlas",
        scope_key="signal_discovery:discovery-write-failure",
        payload={"session_id": "discovery-write-failure"},
    )
    monkeypatch.setattr(
        worker_jobs,
        "materialize_training_atlas",
        lambda **_: (_ for _ in ()).throw(OSError("disk full")),
    )

    failed = run_claimed_job(
        repository=repository,
        job=repository.claim_next_job(worker_id="worker-discovery"),
        workspace_root=tmp_path,
        market_data_repository=market_repository,
    )

    assert failed["status"] == "failed"
    session = repository.get_signal_discovery_session("discovery-write-failure")
    assert session["status"] == "failed"
    assert session["summary"]["last_error"]["message"] == "disk full"


class DiscoveryMarketDataRepository:
    def __init__(self, ref: dict[str, object]) -> None:
        self.ref = ref

    def get_ref(self, dataset_id: str):
        return self.ref if dataset_id == self.ref["dataset_id"] else None


def _write_discovery_candles(tmp_path: Path) -> Path:
    storage_uri = tmp_path / ".data/market-data/origin=raw/source=okx/type=candles/asset=BTC/timeframe=5m"
    rows_by_month: dict[tuple[int, int], list[dict[str, object]]] = {}
    start = datetime(2025, 12, 24, tzinfo=UTC)
    end = datetime(2026, 1, 6, tzinfo=UTC)
    timestamp = start
    while timestamp <= end:
        rows_by_month.setdefault((timestamp.year, timestamp.month), []).append(
            {
                "timestamp": timestamp,
                "open": Decimal("100"),
                "high": Decimal("100.5"),
                "low": Decimal("99.5"),
                "close": Decimal("100"),
                "volume": Decimal("100"),
                "vol_ccy": Decimal("100"),
                "vol_ccy_quote": Decimal("10000"),
                "confirm": 1,
            }
        )
        timestamp += timedelta(minutes=5)
    for (year, month), rows in rows_by_month.items():
        path = storage_uri / f"year={year}" / f"month={month:02d}" / "data.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(rows), path)
    return storage_uri


def test_worker_runs_stage1_score_job(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    artifact_root = tmp_path / "dev/training_sessions/aave-vegas-tunnel-v01/stage1-aave"
    iteration_root = artifact_root / "iterations" / "iter_001_v0.1"
    strategy_root = artifact_root / "strategy_module"
    packets_root = tmp_path / "packets"
    for path in (
        iteration_root / "decisions",
        iteration_root / "scores",
        iteration_root / "summaries",
        strategy_root,
        packets_root,
    ):
        path.mkdir(parents=True)
    (strategy_root / "strategy.py").write_text(
        """
def decide(context):
    return {
        "strategy_id": "aave-vegas-tunnel-v01",
        "strategy_version": "v0.1",
        "signal_id": context["signal"]["signal_id"],
        "trade_action": "ENTER",
        "action": "ENTER",
        "direction": "LONG",
        "confidence": 0.7,
        "reason_code": "api_test",
        "diagnostics": {},
    }
"""
    )
    (packets_root / "sig-1.json").write_text('{"signal_id":"sig-1","payload":{}}')
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "packet_path": str(packets_root / "sig-1.json")}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": "sig-1", "ground_truth": {"natural_direction": "LONG"}}]})
    )
    repository.create_stage1_research_session(
        {
            "session_id": "stage1-aave",
            "artifact_root": str(artifact_root),
            "source_candidate_id": "candidate-aave",
            "source_universe_run_id": "universe-aave",
            "signal_set_key": "vegas_ema:AAVE:2026-AAVE-2h-dedupe-vote2",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "signal_set_id": "2026-AAVE-2h-dedupe-vote2",
            "strategy_id": "aave-vegas-tunnel-v01",
            "strategy_version": "v0.1",
            "train_start": "2026-03-01",
            "train_end": "2026-04-30",
            "walk_forward_start": "2026-05-25",
            "walk_forward_end": "2026-05-31",
            "status": "draft",
            "manifest": {"session_id": "stage1-aave"},
        }
    )
    repository.enqueue_job(
        job_type="stage1_score",
        scope_key="stage1_session:stage1-aave",
        payload={"session_id": "stage1-aave", "iteration_id": "iter_001_v0.1", "sample_role": "training"},
    )
    job = repository.claim_next_job(worker_id="worker-1")

    completed = run_claimed_job(repository=repository, job=job, workspace_root=tmp_path)

    assert completed["status"] == "completed"
    assert completed["result"]["score"]["metrics"]["directional_agreement"] == 1
    assert (iteration_root / "scores" / "stage1a_directional_scores.json").exists()


def test_worker_runs_market_data_ema_refresh_job(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    storage_uri = tmp_path / "origin=derived/source=okx/type=candles/asset=BTC/timeframe=2h"
    path = storage_uri / "year=2026/month=06/data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                {"timestamp": "2026-06-01T00:00:00Z", "open": 100, "high": 100, "low": 100, "close": 100, "volume": 1},
                {"timestamp": "2026-06-01T02:00:00Z", "open": 110, "high": 110, "low": 110, "close": 110, "volume": 1},
            ]
        ),
        path,
    )
    market_repository = FakeMarketDataRepository(
        {
            "dataset_id": "btc-derived-2h",
            "source_id": "okx",
            "asset": "BTC",
            "instrument": "BTC-USDT-SWAP",
            "data_type": "candles",
            "timeframe": "2h",
            "data_origin": "derived",
            "start_ts": "2026-06-01T00:00:00Z",
            "end_ts": "2026-06-01T02:00:00Z",
            "row_count": 2,
            "storage_backend": "parquet",
            "storage_uri": str(storage_uri),
            "schema_descriptor": {},
            "quality_status": "rebuilt",
            "ingestion_version": "test",
        }
    )
    repository.enqueue_job(
        job_type="market_data_ema_refresh",
        scope_key="asset:BTC:ema",
        payload={"asset": "BTC"},
    )
    job = repository.claim_next_job(worker_id="worker-1")

    completed = run_claimed_job(repository=repository, job=job, workspace_root=tmp_path, market_data_repository=market_repository)

    assert completed["status"] == "completed"
    assert completed["result"]["enriched_count"] == 1
    refreshed = market_repository.get_ref("btc-derived-2h")
    assert refreshed["quality_status"] == "ema_enriched"
    assert refreshed["schema_descriptor"]["ema"]["periods"] == [36, 43, 144, 169, 576, 676]


class FakeMarketDataRepository:
    def __init__(self, ref):
        self.ref = ref

    def list_refs(self):
        return [self.ref]

    def update_ref(self, registration):
        self.ref = registration

    def get_ref(self, dataset_id):
        return self.ref if self.ref["dataset_id"] == dataset_id else None
