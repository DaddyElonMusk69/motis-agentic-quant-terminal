from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quant_terminal_worker.signal_discovery.evidence import (
    build_evidence_manifest,
    resolve_primary_label_ref,
    validate_evidence_manifest,
)


def test_primary_label_ref_is_resolved_deterministically_and_requires_full_coverage() -> None:
    refs = [
        _ref(
            "btc-late",
            start="2025-04-01T00:00:00Z",
            end="2026-06-03T00:00:00Z",
            row_count=500,
        ),
        _ref(
            "btc-smaller",
            start="2025-01-01T00:00:00Z",
            end="2026-06-03T00:00:00Z",
            row_count=600,
        ),
        _ref(
            "btc-primary",
            start="2025-01-01T00:00:00Z",
            end="2026-06-03T00:00:00Z",
            row_count=900,
        ),
    ]

    selected = resolve_primary_label_ref(
        refs=list(reversed(refs)),
        asset="btc",
        instrument="BTC-USDT-SWAP",
        research_start=_ts("2025-03-01T00:00:00Z"),
        walk_forward_end=_ts("2026-05-30T23:55:00Z"),
        horizon_hours=[48],
        entry_delays_minutes=[5, 10],
    )

    assert selected["dataset_id"] == "btc-primary"
    assert (
        resolve_primary_label_ref(
            refs=refs,
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            research_start=_ts("2025-03-01T00:00:00Z"),
            walk_forward_end=_ts("2026-05-30T23:55:00Z"),
            horizon_hours=[48],
            entry_delays_minutes=[10],
            preferred_dataset_id="btc-smaller",
        )["dataset_id"]
        == "btc-smaller"
    )

    with pytest.raises(ValueError, match="fully covers"):
        resolve_primary_label_ref(
            refs=[refs[0]],
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            research_start=_ts("2025-03-01T00:00:00Z"),
            walk_forward_end=_ts("2026-05-30T23:55:00Z"),
            horizon_hours=[48],
            entry_delays_minutes=[10],
        )


def test_evidence_manifest_references_every_same_asset_parquet_without_copying(
    tmp_path: Path,
) -> None:
    cutoff = _ts("2026-03-31T23:55:00Z")
    refs = [
        _write_ref(tmp_path, "btc-candles-5m", data_type="candles", timeframe="5m", origin="raw"),
        _write_ref(
            tmp_path, "btc-candles-8h", data_type="candles", timeframe="8h", origin="derived"
        ),
        _write_ref(
            tmp_path,
            "btc-oi-5m",
            data_type="open_interest",
            timeframe="5m",
            origin="raw",
            instrument="BTCUSDT",
        ),
        _write_ref(
            tmp_path,
            "btc-oi-4h",
            data_type="open_interest",
            timeframe="4h",
            origin="derived",
            instrument="BTCUSDT",
        ),
        _write_ref(
            tmp_path,
            "btc-regime-2h",
            data_type="feature_regime_momentum",
            timeframe="2h",
            origin="derived",
        ),
        _write_ref(tmp_path, "eth-candles-5m", asset="ETH", instrument="ETH-USDT-SWAP"),
    ]
    artifact_root = tmp_path / "dev/signal_discovery_sessions/btc-all-evidence"

    manifest = build_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
        session_id="btc-all-evidence",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        primary_dataset_id="btc-candles-5m",
        research_start=_ts("2025-03-01T00:00:00Z"),
        research_end=cutoff,
        refs=refs,
    )

    assert manifest["schema_version"] == "signal_discovery_evidence_manifest.v1"
    assert manifest["authorized_end"] == "2026-03-31T23:55:00Z"
    assert manifest["warmup_policy"] == "all_available_history"
    assert manifest["primary_label_dataset_id"] == "btc-candles-5m"
    assert len(manifest["manifest_hash"]) == 64
    included = {row["dataset_id"]: row for row in manifest["included_datasets"]}
    assert set(included) == {
        "btc-candles-5m",
        "btc-candles-8h",
        "btc-oi-5m",
        "btc-oi-4h",
        "btc-regime-2h",
    }
    assert included["btc-candles-8h"]["timeframe"] == "8h"
    assert included["btc-oi-5m"]["instrument"] == "BTCUSDT"
    assert included["btc-regime-2h"]["data_type"] == "feature_regime_momentum"
    assert all(row["authorized_row_count"] == 2 for row in included.values())
    assert all(len(row["parquet_shards"][0]["sha256"]) == 64 for row in included.values())
    assert not any(row["dataset_id"].startswith("eth-") for row in manifest["excluded_datasets"])
    assert (artifact_root / "evidence/evidence_manifest.json").is_file()
    assert not list((artifact_root / "evidence").rglob("*.parquet"))


def test_evidence_manifest_marks_partial_and_unreadable_optional_sources(
    tmp_path: Path,
) -> None:
    primary = _write_ref(tmp_path, "btc-primary")
    partial = _write_ref(tmp_path, "btc-partial")
    partial["start_ts"] = _ts("2026-01-01T00:00:00Z")
    corrupt = _ref(
        "btc-corrupt",
        data_type="open_interest",
        storage_uri=str(tmp_path / "missing"),
        start="2025-01-01T00:00:00Z",
        end="2026-03-31T23:55:00Z",
    )

    manifest = build_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=tmp_path / "session",
        session_id="session",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        primary_dataset_id="btc-primary",
        research_start=_ts("2025-03-01T00:00:00Z"),
        research_end=_ts("2026-03-31T23:55:00Z"),
        refs=[primary, partial, corrupt],
    )

    included = {row["dataset_id"]: row for row in manifest["included_datasets"]}
    assert "partial_research_coverage" in included["btc-partial"]["warnings"]
    assert manifest["excluded_datasets"] == [
        {"dataset_id": "btc-corrupt", "reason": "no_parquet_shards"}
    ]
    assert manifest["baseline_oi_dataset_id"] is None

    with pytest.raises(ValueError, match="primary label dataset"):
        build_evidence_manifest(
            workspace_root=tmp_path,
            artifact_root=tmp_path / "bad-primary",
            session_id="bad-primary",
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            primary_dataset_id="btc-corrupt",
            research_start=_ts("2025-03-01T00:00:00Z"),
            research_end=_ts("2026-03-31T23:55:00Z"),
            refs=[corrupt],
        )


def test_evidence_manifest_validation_detects_historical_source_drift(tmp_path: Path) -> None:
    source = _write_ref(tmp_path, "btc-primary")
    artifact_root = tmp_path / "session"
    build_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
        session_id="session",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        primary_dataset_id="btc-primary",
        research_start=_ts("2025-03-01T00:00:00Z"),
        research_end=_ts("2026-03-31T23:55:00Z"),
        refs=[source],
    )

    assert validate_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
    )["manifest_hash"]

    parquet_path = next(Path(source["storage_uri"]).rglob("*.parquet"))
    table = pq.ParquetFile(parquet_path).read()
    future_only_change = table.to_pylist()
    future_only_change[-1]["close"] = Decimal("999")
    pq.write_table(pa.Table.from_pylist(future_only_change, schema=table.schema), parquet_path)

    assert validate_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
    )["manifest_hash"]

    historical_table = pq.ParquetFile(parquet_path).read()
    historical_change = historical_table.to_pylist()
    historical_change[0]["close"] = Decimal("777")
    pq.write_table(
        pa.Table.from_pylist(historical_change, schema=historical_table.schema),
        parquet_path,
    )

    with pytest.raises(ValueError, match="source drift"):
        validate_evidence_manifest(workspace_root=tmp_path, artifact_root=artifact_root)


def test_evidence_manifest_validation_detects_added_historical_shards(tmp_path: Path) -> None:
    source = _write_ref(tmp_path, "btc-primary")
    artifact_root = tmp_path / "session"
    build_evidence_manifest(
        workspace_root=tmp_path,
        artifact_root=artifact_root,
        session_id="session",
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        primary_dataset_id="btc-primary",
        research_start=_ts("2025-03-01T00:00:00Z"),
        research_end=_ts("2026-03-31T23:55:00Z"),
        refs=[source],
    )
    added = Path(source["storage_uri"]) / "year=2025/month=02/data.parquet"
    added.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([_row("2025-02-01T00:00:00Z")]), added)

    with pytest.raises(ValueError, match="source drift"):
        validate_evidence_manifest(workspace_root=tmp_path, artifact_root=artifact_root)


def _write_ref(
    root: Path,
    dataset_id: str,
    *,
    asset: str = "BTC",
    instrument: str = "BTC-USDT-SWAP",
    data_type: str = "candles",
    timeframe: str = "5m",
    origin: str = "raw",
) -> dict[str, object]:
    storage_uri = root / ".data" / dataset_id
    path = storage_uri / "year=2026/month=03/data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        pa.Table.from_pylist(
            [
                _row("2025-01-01T00:00:00Z"),
                _row("2026-03-31T23:55:00Z"),
                _row("2026-04-01T00:00:00Z"),
            ]
        ),
        path,
    )
    return _ref(
        dataset_id,
        asset=asset,
        instrument=instrument,
        data_type=data_type,
        timeframe=timeframe,
        origin=origin,
        storage_uri=str(storage_uri),
        start="2025-01-01T00:00:00Z",
        end="2026-04-01T00:00:00Z",
        row_count=3,
    )


def _ref(
    dataset_id: str,
    *,
    asset: str = "BTC",
    instrument: str = "BTC-USDT-SWAP",
    data_type: str = "candles",
    timeframe: str = "5m",
    origin: str = "raw",
    storage_uri: str = ".data/unused",
    start: str = "2025-01-01T00:00:00Z",
    end: str = "2026-06-03T00:00:00Z",
    row_count: int = 1,
) -> dict[str, object]:
    return {
        "dataset_id": dataset_id,
        "source_id": "okx" if instrument.endswith("SWAP") else "binance",
        "asset": asset,
        "instrument": instrument,
        "data_type": data_type,
        "timeframe": timeframe,
        "data_origin": origin,
        "start_ts": _ts(start),
        "end_ts": _ts(end),
        "row_count": row_count,
        "storage_backend": "parquet",
        "storage_uri": storage_uri,
        "schema_descriptor": {"timestamp": "timestamp[us, tz=UTC]"},
        "quality_status": "updated",
        "ingestion_version": "fixture-v1",
    }


def _row(timestamp: str) -> dict[str, object]:
    return {
        "timestamp": _ts(timestamp),
        "open": Decimal("100"),
        "high": Decimal("101"),
        "low": Decimal("99"),
        "close": Decimal("100"),
        "volume": Decimal("10"),
        "confirm": 1,
    }


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
