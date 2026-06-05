from __future__ import annotations

import json

from sqlalchemy import create_engine, select

from quant_terminal_api.db.models import metadata, signal_sets, signals
from quant_terminal_api.repositories.runtime import RuntimeRepository
from quant_terminal_worker.ingestion.legacy_signals import import_legacy_signal_sets


def test_import_legacy_signal_sets_preserves_manifest_and_packet_payload(tmp_path):
    root = tmp_path / "vegas_ema"
    set_root = root / "BTC" / "2026-BTC-2h-dedupe-vote2"
    packets_root = set_root / "packets"
    packets_root.mkdir(parents=True)
    manifest = {
        "schema_version": "0.1",
        "signal_set_id": "2026-BTC-2h-dedupe-vote2",
        "signal_engine_id": "vegas_ema",
        "signal_family": "vegas_ema",
        "asset": "BTC",
        "signal_engine_version": "0.1",
        "data_manifest": "dev/data/manifests/BTC.json",
        "parameters": {"vote_threshold": 2, "timeframes": ["2h", "4h"]},
        "packet_count": 1,
        "start_ts": "2026-03-01T00:00:00Z",
        "end_ts": "2026-06-01T00:00:00Z",
        "packets_path": "packets/",
        "packet_filename_format": "YYYYMMDDTHHMMSSZ.json",
    }
    packet = {
        "schema_version": "signal_packet.v2",
        "asset": "BTC",
        "timestamp": "2026-03-02T01:05:00Z",
        "active_timeframes": ["2h", "4h"],
        "interactions": [{"timeframe": "2h", "tunnel": "fast"}],
        "charts": {"2h": {"columns": ["ts", "open"], "completed_candles": []}},
    }
    (set_root / "manifest.json").write_text(json.dumps(manifest))
    (packets_root / "20260302T010500Z.json").write_text(json.dumps(packet))

    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    result = import_legacy_signal_sets(root=root, repository=repository)

    assert result == {
        "status": "imported",
        "signal_engine_id": "vegas_ema",
        "signal_sets": 1,
        "signals": 1,
    }
    with engine.connect() as connection:
        stored_set = connection.execute(select(signal_sets)).mappings().one()
        stored_signal = connection.execute(select(signals)).mappings().one()

    assert stored_set["signal_set_key"] == "vegas_ema:BTC:BTC-vegas_ema-canonical"
    assert stored_set["signal_set_id"] == "BTC-vegas_ema-canonical"
    assert stored_set["manifest"]["source_signal_set_id"] == "2026-BTC-2h-dedupe-vote2"
    assert stored_set["manifest"]["canonical_signal_set_key"] == "vegas_ema:BTC:BTC-vegas_ema-canonical"
    assert stored_set["packet_count"] == 1
    assert stored_set["source_path"].endswith("2026-BTC-2h-dedupe-vote2")
    assert stored_signal["signal_id"] == "vegas_ema:BTC:BTC-vegas_ema-canonical:20260302T010500Z"
    assert stored_signal["signal_set_key"] == "vegas_ema:BTC:BTC-vegas_ema-canonical"
    assert stored_signal["payload"] == packet
    assert stored_signal["payload_schema"] == "signal_packet.v2"


def test_import_legacy_signal_sets_extends_one_canonical_pool_per_engine_asset(tmp_path):
    root = tmp_path / "vegas_ema"
    for signal_set_id, filename, timestamp in [
        ("2026-BTC-2h-dedupe-vote2", "20260302T010500Z.json", "2026-03-02T01:05:00Z"),
        ("2026-BTC-forward-fill", "20260526T091000Z.json", "2026-05-26T09:10:00Z"),
    ]:
        set_root = root / "BTC" / signal_set_id
        packets_root = set_root / "packets"
        packets_root.mkdir(parents=True)
        manifest = {
            "schema_version": "0.1",
            "signal_set_id": signal_set_id,
            "signal_engine_id": "vegas_ema",
            "asset": "BTC",
            "signal_engine_version": "0.1",
            "parameters": {"vote_threshold": 2},
            "packet_count": 1,
            "start_ts": "2026-03-01T00:00:00Z",
            "end_ts": "2026-06-01T00:00:00Z",
            "packets_path": "packets/",
        }
        packet = {
            "schema_version": "signal_packet.v2",
            "asset": "BTC",
            "timestamp": timestamp,
            "active_timeframes": ["2h"],
            "interactions": [],
        }
        (set_root / "manifest.json").write_text(json.dumps(manifest))
        (packets_root / filename).write_text(json.dumps(packet))

    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    import_legacy_signal_sets(root=root, repository=repository)

    with engine.connect() as connection:
        stored_sets = connection.execute(select(signal_sets)).mappings().all()
        stored_signals = connection.execute(select(signals)).mappings().all()

    assert len(stored_sets) == 1
    assert stored_sets[0]["signal_set_key"] == "vegas_ema:BTC:BTC-vegas_ema-canonical"
    assert stored_sets[0]["packet_count"] == 2
    assert len(stored_signals) == 2
    assert {signal["signal_set_key"] for signal in stored_signals} == {"vegas_ema:BTC:BTC-vegas_ema-canonical"}


def test_import_legacy_signal_sets_skips_non_signal_set_manifests(tmp_path):
    root = tmp_path / "vegas_ema"
    malformed_root = root / "BTC" / "BTC-vegas_ema-canonical"
    malformed_root.mkdir(parents=True)
    (malformed_root / "manifest.json").write_text(json.dumps({"schema_version": "0.1"}))

    valid_root = root / "BTC" / "2026-BTC-2h-dedupe-vote2"
    packets_root = valid_root / "packets"
    packets_root.mkdir(parents=True)
    (valid_root / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": "0.1",
                "signal_set_id": "2026-BTC-2h-dedupe-vote2",
                "signal_engine_id": "vegas_ema",
                "asset": "BTC",
                "signal_engine_version": "0.1",
                "parameters": {"vote_threshold": 2},
                "packet_count": 1,
                "start_ts": "2026-03-01T00:00:00Z",
                "end_ts": "2026-06-01T00:00:00Z",
                "packets_path": "packets/",
            }
        )
    )
    (packets_root / "20260302T010500Z.json").write_text(
        json.dumps(
            {
                "schema_version": "signal_packet.v2",
                "asset": "BTC",
                "timestamp": "2026-03-02T01:05:00Z",
            }
        )
    )

    engine = create_engine("sqlite+pysqlite:///:memory:")
    metadata.create_all(engine)
    repository = RuntimeRepository(engine)

    result = import_legacy_signal_sets(root=root, repository=repository)

    assert result["signal_sets"] == 1
    assert result["signals"] == 1
