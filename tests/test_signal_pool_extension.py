from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import create_engine, insert

from quant_terminal_api.db.models import data_sources, market_data_refs, metadata
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_worker.signal_engines import vegas_ema
from quant_terminal_worker.ingestion.signal_pool_extension import _append_packets_to_signal_set, extend_signal_pool_from_local_candles


WORKSPACE_MANIFEST = {
    "workspace_name": "test-workspace",
    "schema_version": "0.1",
    "purpose": "test",
    "directories": {"dev": "Historical data", "live": "Live data", "artifacts": "Artifacts"},
}


def test_extend_signal_pool_blocks_when_target_exceeds_parquet_raw_candles(tmp_path: Path):
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(root)
    _register_candle_ref(
        repository,
        root=root,
        asset="AAVE",
        timeframe="5m",
        origin="raw",
        timestamps=["2026-05-15T00:00:00Z"],
    )
    _write_stale_csv(root, "AAVE", ["2026-06-01T00:00:00Z"])

    try:
        extend_signal_pool_from_local_candles(
            workspace_root=root,
            repository=repository,
            signal_engine_id="vegas_ema",
            asset="AAVE",
            target_end="2026-05-30T00:00:00Z",
        )
    except ValueError as exc:
        assert str(exc) == "Raw candle data only covers through 2026-05-15T00:00:00Z. Update local candle data first."
    else:
        raise AssertionError("expected parquet raw coverage blocker")


def test_append_packets_uses_bulk_repository_insert_when_available(tmp_path: Path):
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(root)
    signal_set = repository.get_signal_set("vegas_ema:AAVE:AAVE-vegas_ema-canonical")
    bulk_calls = []
    single_calls = []

    def bulk_insert(rows):
        bulk_calls.append(list(rows))
        RuntimeRepository.upsert_signals(repository, rows)

    def single_insert(row):
        single_calls.append(row)
        RuntimeRepository.upsert_signal(repository, row)

    repository.upsert_signals = bulk_insert
    repository.upsert_signal = single_insert

    result = _append_packets_to_signal_set(
        repository=repository,
        signal_set=signal_set,
        signal_set_key=signal_set["signal_set_key"],
        packets=[
            {
                "schema_version": "signal_packet.v2",
                "asset": "AAVE",
                "timestamp": "2026-05-20T00:00:00Z",
                "active_timeframes": ["2h"],
                "interactions": [],
                "charts": {},
            },
            {
                "schema_version": "signal_packet.v2",
                "asset": "AAVE",
                "timestamp": "2026-05-20T00:05:00Z",
                "active_timeframes": ["2h"],
                "interactions": [],
                "charts": {},
            },
        ],
    )

    assert result["signals"] == 2
    assert len(bulk_calls) == 1
    assert len(bulk_calls[0]) == 2
    assert single_calls == []
    assert len(repository.list_signals(signal_set_key=signal_set["signal_set_key"])) == 3


def test_extend_signal_pool_reports_packet_progress_for_streamed_chunks(tmp_path: Path, monkeypatch) -> None:
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(root)
    _register_default_refs(
        repository,
        root=root,
        asset="AAVE",
        timestamps=[
            "2026-05-10T00:00:00Z",
            "2026-05-10T00:05:00Z",
            "2026-05-10T00:10:00Z",
            "2026-05-10T00:15:00Z",
        ],
    )
    progress_steps = []

    def fake_resolve_signal_engine(*args, **kwargs):
        class FakeSpec:
            configuration_schema = {}

        class FakeResolved:
            spec = FakeSpec()

            @staticmethod
            def generate_training_signals(context):
                packets = [
                    {
                        "schema_version": "signal_packet.v2",
                        "asset": "AAVE",
                        "timestamp": "2026-05-10T00:05:00Z",
                    },
                    {
                        "schema_version": "signal_packet.v2",
                        "asset": "AAVE",
                        "timestamp": "2026-05-10T00:10:00Z",
                    },
                    {
                        "schema_version": "signal_packet.v2",
                        "asset": "AAVE",
                        "timestamp": "2026-05-10T00:15:00Z",
                    },
                ]
                context.packet_sink(packets[:2])
                context.packet_sink(packets[2:])

                class FakeOutput:
                    packets = []

                return FakeOutput()

        return FakeResolved()

    monkeypatch.setattr(
        "quant_terminal_worker.ingestion.signal_pool_extension.resolve_signal_engine",
        fake_resolve_signal_engine,
    )

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema",
        asset="AAVE",
        target_end="2026-05-10T00:15:00Z",
        progress_callback=progress_steps.append,
    )

    assert result["appended_packet_count"] == 3
    assert progress_steps == [
        "packets 2 appended",
        "packets 3 appended",
    ]


def test_extend_signal_pool_uses_parquet_candles_and_ignores_persistent_packet_folder(
    tmp_path: Path,
    monkeypatch,
):
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(root)
    _register_default_refs(
        repository,
        root=root,
        asset="AAVE",
        timestamps=["2026-05-10T00:00:00Z", "2026-05-20T00:00:00Z"],
    )
    stale_packet = root / "dev" / "signals" / "vegas_ema" / "AAVE" / "AAVE-vegas_ema-canonical" / "packets"
    stale_packet.mkdir(parents=True)
    (stale_packet / "20990101T000000Z.json").write_text(json.dumps({"timestamp": "2099-01-01T00:00:00Z"}))

    calls = []

    def fake_generator(**kwargs):
        calls.append(kwargs)
        assert [candle.timestamp.isoformat() for candle in kwargs["raw_5m"]] == [
            "2026-05-10T00:00:00+00:00",
            "2026-05-20T00:00:00+00:00",
        ]
        return [
            {
                "schema_version": "signal_packet.v2",
                "asset": "AAVE",
                "timestamp": "2026-05-20T00:00:00Z",
                "active_timeframes": ["2h"],
                "interactions": [],
                "charts": {},
            }
        ]

    monkeypatch.setattr(vegas_ema, "generate_vegas_packets", fake_generator)

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema",
        asset="AAVE",
        target_end="2026-05-20T00:00:00Z",
    )

    assert calls
    assert result["source"] == "parquet_market_data"
    assert result["raw_candle_end_ts"] == "2026-05-20T00:00:00Z"
    assert result["previous_signal_end_ts"] == "2026-05-10T00:00:00Z"
    assert result["scan_coverage_end_ts"] == "2026-05-20T00:00:00Z"
    assert result["final_signal_end_ts"] == "2026-05-20T00:00:00Z"
    assert result["appended_packet_count"] == 1
    assert result["final_packet_count"] == 2
    refreshed = repository.get_signal_set("vegas_ema:AAVE:AAVE-vegas_ema-canonical")
    assert refreshed["packet_count"] == 2
    assert refreshed["coverage_end_ts"].isoformat() == "2026-05-20T00:00:00+00:00"
    assert [_iso_z(signal["timestamp"]) for signal in repository.list_signals(signal_set_key=refreshed["signal_set_key"])] == [
        "2026-05-10T00:00:00+00:00",
        "2026-05-20T00:00:00+00:00",
    ]


def test_extend_signal_pool_reports_no_new_signals_and_advances_scan_coverage(
    tmp_path: Path,
    monkeypatch,
):
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(root)
    _register_default_refs(
        repository,
        root=root,
        asset="AAVE",
        timestamps=["2026-05-10T00:00:00Z", "2026-05-20T00:00:00Z"],
    )

    monkeypatch.setattr(vegas_ema, "generate_vegas_packets", lambda **kwargs: [])

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema",
        asset="AAVE",
        target_end="2026-05-20T00:00:00Z",
    )

    assert result["status"] == "no_new_signals"
    assert result["raw_candle_end_ts"] == "2026-05-20T00:00:00Z"
    assert result["previous_signal_end_ts"] == "2026-05-10T00:00:00Z"
    assert result["scan_coverage_end_ts"] == "2026-05-20T00:00:00Z"
    assert result["appended_packet_count"] == 0
    assert result["final_packet_count"] == 1
    refreshed = repository.get_signal_set("vegas_ema:AAVE:AAVE-vegas_ema-canonical")
    assert refreshed["packet_count"] == 1
    assert refreshed["end_ts"].isoformat() == "2026-05-10T00:00:00+00:00"
    assert refreshed["coverage_end_ts"].isoformat() == "2026-05-20T00:00:00+00:00"


def test_extend_signal_pool_resumes_after_existing_parquet_scan_coverage(
    tmp_path: Path,
    monkeypatch,
):
    root = _make_workspace(tmp_path)
    repository = _repository_with_signal_pool(
        root,
        scan_coverage={
            "start_ts": "2026-05-10T00:00:00Z",
            "end_ts": "2026-05-20T00:00:00Z",
            "source": "parquet_market_data",
        },
    )
    _register_default_refs(
        repository,
        root=root,
        asset="AAVE",
        timestamps=[
            "2026-05-10T00:00:00Z",
            "2026-05-20T00:00:00Z",
            "2026-05-20T00:05:00Z",
            "2026-05-20T00:10:00Z",
        ],
    )
    calls = []

    def fake_generator(**kwargs):
        calls.append(kwargs)
        return []

    monkeypatch.setattr(vegas_ema, "generate_vegas_packets", fake_generator)

    result = extend_signal_pool_from_local_candles(
        workspace_root=root,
        repository=repository,
        signal_engine_id="vegas_ema",
        asset="AAVE",
        target_end="2026-05-20T00:10:00Z",
    )

    assert result["status"] == "no_new_signals"
    assert calls[0]["start"].isoformat() == "2026-05-20T00:05:00+00:00"
    assert calls[0]["end"].isoformat() == "2026-05-20T00:10:00+00:00"


def _repository_with_signal_pool(
    root: Path,
    *,
    scan_coverage: dict[str, str] | None = None,
) -> RuntimeRepository:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)
    with engine.begin() as connection:
        connection.execute(
            insert(data_sources).values(source_id="okx", name="OKX", source_type="exchange", config={})
        )
    repository.register_signal_engine(
        {
            "signal_engine_id": "vegas_ema",
            "name": "Vegas EMA",
            "description": "",
            "version": "0.1",
            "code_ref": {},
            "supported_input_data_types": ["candles"],
            "required_data": [
                {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                {
                    "data_type": "candles",
                    "origin": "derived",
                    "timeframe": "2h",
                    "source": {"data_type": "candles", "origin": "raw", "timeframe": "5m"},
                },
            ],
            "output_envelope_version": "signal_packet.v2",
            "runtime_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:generate_training_signals",
            "live_scanner_entrypoint": "quant_terminal_worker.signal_engines.vegas_ema:scan_live_signal",
            "configuration_schema": {},
        }
    )
    repository.upsert_signal_set(
        {
            "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            "signal_set_id": "AAVE-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "start_ts": "2026-05-10T00:00:00Z",
            "end_ts": "2026-05-10T00:00:00Z",
            "packet_count": 1,
            "payload_schema": "signal_packet.v2",
            "source_path": str(root / "dev" / "signals" / "vegas_ema" / "AAVE"),
            "manifest": {
                "parameters": {
                    "vote_threshold": 2,
                    "dedupe_window_minutes": 120,
                },
                **({"scan_coverage": scan_coverage} if scan_coverage else {}),
            },
        }
    )
    repository.upsert_signal(
        {
            "signal_id": "vegas_ema:AAVE:AAVE-vegas_ema-canonical:20260510T000000Z",
            "signal_set_key": "vegas_ema:AAVE:AAVE-vegas_ema-canonical",
            "signal_engine_id": "vegas_ema",
            "signal_engine_version": "0.1",
            "asset": "AAVE",
            "instrument": "AAVE-USDT-SWAP",
            "timestamp": "2026-05-10T00:00:00Z",
            "data_refs": [],
            "payload_schema": "signal_packet.v2",
            "payload": {
                "schema_version": "signal_packet.v2",
                "asset": "AAVE",
                "timestamp": "2026-05-10T00:00:00Z",
            },
        }
    )
    repository.refresh_signal_set_coverage("vegas_ema:AAVE:AAVE-vegas_ema-canonical")
    return repository


def _make_workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    (root / "dev").mkdir(parents=True)
    (root / "live").mkdir()
    (root / "artifacts" / "signal_engine" / "src").mkdir(parents=True)
    (root / "workspace_manifest.json").write_text(json.dumps(WORKSPACE_MANIFEST))
    return root


def _register_default_refs(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    timestamps: list[str],
) -> None:
    _register_candle_ref(repository, root=root, asset=asset, timeframe="5m", origin="raw", timestamps=timestamps)
    for timeframe in ("2h", "4h", "8h", "12h", "1d"):
        _register_candle_ref(
            repository,
            root=root,
            asset=asset,
            timeframe=timeframe,
            origin="derived",
            timestamps=timestamps,
        )


def _register_candle_ref(
    repository: RuntimeRepository,
    *,
    root: Path,
    asset: str,
    timeframe: str,
    origin: str,
    timestamps: list[str],
) -> None:
    storage_uri = root / ".data" / "market-data" / f"origin={origin}" / "source=okx" / "type=candles" / f"asset={asset}" / f"timeframe={timeframe}"
    rows = [_candle_row(timestamp) for timestamp in timestamps]
    path = storage_uri / "year=2026" / "month=05" / "data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    with repository.engine.begin() as connection:
        connection.execute(
            insert(market_data_refs).values(
                dataset_id=f"{asset}-{origin}-{timeframe}",
                source_id="okx",
                asset=asset,
                instrument=f"{asset}-USDT-SWAP",
                data_type="candles",
                timeframe=timeframe,
                data_origin=origin,
                start_ts=datetime(2026, 5, 10, tzinfo=UTC),
                end_ts=datetime.fromisoformat(timestamps[-1].replace("Z", "+00:00")),
                row_count=len(rows),
                storage_backend="parquet",
                storage_uri=str(storage_uri),
                schema_descriptor={},
                quality_status="ingested",
                ingestion_version="test",
            )
        )


def _candle_row(timestamp: str) -> dict[str, object]:
    return {
        "timestamp": timestamp,
        "open": 1.0,
        "high": 1.0,
        "low": 1.0,
        "close": 1.0,
        "volume": 1.0,
        "vol_ccy": 1.0,
        "vol_ccy_quote": 1.0,
        "confirm": 1,
    }


def _write_stale_csv(root: Path, asset: str, timestamps: list[str]) -> None:
    raw_path = root / "dev" / "data" / "raw" / asset / "5m" / "candles.csv"
    raw_path.parent.mkdir(parents=True)
    rows = ["ts,open,high,low,close,volume,vol_ccy,vol_ccy_quote,confirm"]
    rows.extend(f"{timestamp},1,1,1,1,1,1,1,1" for timestamp in timestamps)
    raw_path.write_text("\n".join(rows) + "\n")


def _iso_z(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
