from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import zipfile

import pyarrow.parquet as pq

from quant_terminal_worker.ingestion.binance_futures_metrics import (
    build_live_futures_metrics_rows,
    derive_futures_metrics_rows,
    fill_raw_futures_metrics_dataset,
    import_binance_futures_metrics_history,
    normalize_archive_futures_metrics_row,
)


class FakeRepository:
    def __init__(self) -> None:
        self.upserted_sources = []
        self.upserted = []
        self.updated = []

    def upsert_data_source(self, source_id, name, source_type):
        self.upserted_sources.append(
            {"source_id": source_id, "name": name, "source_type": source_type}
        )

    def upsert_ref(self, registration):
        self.upserted.append(registration)

    def update_ref(self, registration):
        self.updated.append(registration)

    def list_derived_refs_for_raw(self, registration):
        return [
            row
            for row in self.upserted
            if row.get("data_origin") == "derived"
            and row.get("data_type") == registration.get("data_type")
            and row.get("asset") == registration.get("asset")
        ]


def test_normalize_archive_row_uses_interval_start_and_semantic_columns() -> None:
    row = normalize_archive_futures_metrics_row(
        {
            "create_time": "2026-07-12 00:00:00",
            "symbol": "BTCUSDT",
            "sum_open_interest": "101328.727",
            "sum_open_interest_value": "6470842373.3473",
            "count_toptrader_long_short_ratio": "1.40428986",
            "sum_toptrader_long_short_ratio": "1.37836100",
            "count_long_short_ratio": "1.25625663",
            "sum_taker_long_short_vol_ratio": "1.34681800",
        }
    )

    assert row == {
        "timestamp": "2026-07-12T00:00:00Z",
        "interval_end": "2026-07-12T00:05:00Z",
        "available_at": "2026-07-12T00:05:00Z",
        "symbol": "BTCUSDT",
        "sum_open_interest": 101328.727,
        "sum_open_interest_value": 6470842373.3473,
        "top_trader_account_long_short_ratio": 1.40428986,
        "top_trader_position_long_short_ratio": 1.378361,
        "global_account_long_short_ratio": 1.25625663,
        "taker_buy_sell_volume_ratio": 1.346818,
        "complete": True,
        "confirm": 1,
        "ingest_source": "archive",
    }


def test_live_rows_align_snapshot_end_timestamps_with_taker_interval_start() -> None:
    adapter = _ExactArchiveMatchAdapter()

    rows = build_live_futures_metrics_rows(
        adapter=adapter,
        symbol="BTCUSDT",
        from_ts=datetime(2026, 7, 12, 0, 0, tzinfo=UTC),
        target=datetime(2026, 7, 12, 0, 5, tzinfo=UTC),
        limit=2,
    )

    assert [row["timestamp"] for row in rows] == [
        "2026-07-12T00:00:00Z",
        "2026-07-12T00:05:00Z",
    ]
    assert rows[0]["available_at"] == "2026-07-12T00:05:00Z"
    assert rows[0]["sum_open_interest"] == 101328.727
    assert rows[0]["top_trader_account_long_short_ratio"] == 1.4044
    assert rows[0]["top_trader_position_long_short_ratio"] == 1.3784
    assert rows[0]["global_account_long_short_ratio"] == 1.2563
    assert rows[0]["taker_buy_sell_volume_ratio"] == 1.3468
    assert rows[0]["ingest_source"] == "rest"
    assert adapter.oi_calls[0]["start_time_ms"] == 1783814700000
    assert adapter.taker_calls[0]["start_time_ms"] == 1783814400000


def test_live_rows_stop_before_first_incomplete_interval() -> None:
    adapter = _ExactArchiveMatchAdapter()
    adapter.global_rows = adapter.global_rows[:1]

    rows = build_live_futures_metrics_rows(
        adapter=adapter,
        symbol="BTCUSDT",
        from_ts=datetime(2026, 7, 12, 0, 0, tzinfo=UTC),
        target=datetime(2026, 7, 12, 0, 5, tzinfo=UTC),
        limit=2,
    )

    assert [row["timestamp"] for row in rows] == ["2026-07-12T00:00:00Z"]


def test_derive_futures_metrics_excludes_oi_and_builds_top_global_gaps() -> None:
    rows = [
        _metric_row(
            "2026-07-12T00:00:00Z",
            account=3.0,
            position=1.0,
            global_ratio=1.0,
            taker=0.8,
        ),
        _metric_row(
            "2026-07-12T00:05:00Z",
            account=1.0,
            position=1.0,
            global_ratio=3.0,
            taker=1.0,
        ),
        _metric_row(
            "2026-07-12T00:10:00Z",
            account=1.0,
            position=1.0,
            global_ratio=1.0,
            taker=1.2,
        ),
    ]

    derived = derive_futures_metrics_rows(
        raw_rows=rows,
        raw_timeframe="5m",
        derived_timeframe="15m",
    )

    assert len(derived) == 1
    row = derived[0]
    assert row["timestamp"] == "2026-07-12T00:00:00Z"
    assert row["available_at"] == "2026-07-12T00:15:00Z"
    assert row["top_trader_account_long_short_ratio_avg"] == 5.0 / 3.0
    assert row["top_trader_account_long_short_ratio_last"] == 1.0
    assert row["taker_buy_sell_volume_ratio_avg"] == 1.0
    assert row["taker_buy_sell_volume_ratio_last"] == 1.2
    assert row["top_trader_account_vs_global_long_share_gap_avg"] == 0.0
    assert row["top_trader_account_vs_global_long_share_gap_last"] == 0.0
    assert row["top_trader_position_vs_global_long_share_gap_avg"] == -1.0 / 12.0
    assert row["source_row_count"] == 3
    assert not any("open_interest" in key for key in row)


def test_derive_futures_metrics_skips_noncontiguous_bucket() -> None:
    rows = [
        _metric_row("2026-07-12T00:00:00Z"),
        _metric_row("2026-07-12T00:10:00Z"),
    ]

    assert derive_futures_metrics_rows(
        raw_rows=rows,
        raw_timeframe="5m",
        derived_timeframe="15m",
    ) == []


def test_archive_import_writes_deduped_futures_metrics_dataset(tmp_path: Path) -> None:
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-metrics-2023-01-01.zip",
        "BTCUSDT-metrics-2023-01-01.csv",
        [
            "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio",
            "2023-01-01 00:00:00,BTCUSDT,100,200,1.1,1.2,1.3,1.4",
            "2023-01-01 00:00:00,BTCUSDT,100,200,1.1,1.2,1.3,1.4",
            "2023-01-01 00:05:00,BTCUSDT,101,202,1.2,1.3,1.4,1.5",
        ],
    )
    repository = FakeRepository()

    result = import_binance_futures_metrics_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        ingestion_version="binance-futures-metrics.v1",
        min_timestamp=datetime(2023, 1, 1, tzinfo=UTC),
    )

    assert result["dataset_id"] == "btc-binance-futures_metrics-raw-5m"
    assert result["row_count"] == 2
    assert repository.upserted[0]["data_type"] == "futures_metrics"
    assert repository.upserted[0]["schema_descriptor"]["quality"] == {
        "missing_interval_count": 0,
        "incomplete_row_count": 0,
        "complete_row_count": 2,
    }
    path = (
        tmp_path
        / ".data/market-data/origin=raw/source=binance/type=futures_metrics/asset=BTC/timeframe=5m/year=2023/month=01/data.parquet"
    )
    rows = pq.read_table(path).to_pylist()
    assert rows[0]["top_trader_position_long_short_ratio"] == 1.2
    assert rows[0]["available_at"] == "2023-01-01T00:05:00Z"


def test_archive_import_registers_non_oi_derived_metrics(tmp_path: Path) -> None:
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-metrics-2023-01-01.zip",
        "BTCUSDT-metrics-2023-01-01.csv",
        [
            "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio",
            "2023-01-01 00:00:00,BTCUSDT,100,200,3.0,1.0,1.0,0.8",
            "2023-01-01 00:05:00,BTCUSDT,101,202,1.0,1.0,3.0,1.0",
            "2023-01-01 00:10:00,BTCUSDT,102,204,1.0,1.0,1.0,1.2",
        ],
    )
    repository = FakeRepository()

    result = import_binance_futures_metrics_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        derived_timeframes=("15m",),
    )

    assert [item["dataset_id"] for item in result["derived"]] == [
        "btc-binance-futures_metrics-derived-15m"
    ]
    registration = repository.upserted[-1]
    assert "retail-only" in registration["schema_descriptor"][
        "top_vs_global_semantics"
    ]
    assert not any(
        "open_interest" in column
        for column in registration["schema_descriptor"]["columns"]
    )


def test_fill_appends_complete_rest_rows(tmp_path: Path) -> None:
    source_dir = tmp_path / "downloads"
    source_dir.mkdir()
    _write_zip(
        source_dir / "BTCUSDT-metrics-2026-07-11.zip",
        "BTCUSDT-metrics-2026-07-11.csv",
        [
            "create_time,symbol,sum_open_interest,sum_open_interest_value,count_toptrader_long_short_ratio,sum_toptrader_long_short_ratio,count_long_short_ratio,sum_taker_long_short_vol_ratio",
            "2026-07-11 23:55:00,BTCUSDT,100,200,1.1,1.2,1.3,1.4",
        ],
    )
    repository = FakeRepository()
    imported = import_binance_futures_metrics_history(
        source_dir=source_dir,
        target_root=tmp_path / ".data" / "market-data",
        repository=repository,
        asset="BTC",
        symbol="BTCUSDT",
        ingestion_version="binance-futures-metrics.v1",
    )
    registration = repository.upserted[0]

    result = fill_raw_futures_metrics_dataset(
        registration=registration,
        repository=repository,
        adapter=_ExactArchiveMatchAdapter(),
        as_of=datetime(2026, 7, 12, 0, 10, tzinfo=UTC),
        limit=2,
    )

    assert imported["row_count"] == 1
    assert result["status"] == "filled"
    assert result["rows_added"] == 2
    assert result["end_ts"] == "2026-07-12T00:05:00Z"
    assert repository.updated[-1]["row_count"] == 3


class _ExactArchiveMatchAdapter:
    def __init__(self) -> None:
        self.oi_calls = []
        self.taker_calls = []
        self.oi_rows = [
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "101328.727",
                "sumOpenInterestValue": "6470842373.3473",
                "timestamp": 1783814700000,
            },
            {
                "symbol": "BTCUSDT",
                "sumOpenInterest": "101284.425",
                "sumOpenInterestValue": "6458735598.7275",
                "timestamp": 1783815000000,
            },
        ]
        self.account_rows = [
            {"longShortRatio": "1.4044", "timestamp": 1783814700000},
            {"longShortRatio": "1.4079", "timestamp": 1783815000000},
        ]
        self.position_rows = [
            {"longShortRatio": "1.3784", "timestamp": 1783814700000},
            {"longShortRatio": "1.3798", "timestamp": 1783815000000},
        ]
        self.global_rows = [
            {"longShortRatio": "1.2563", "timestamp": 1783814700000},
            {"longShortRatio": "1.2599", "timestamp": 1783815000000},
        ]
        self.taker_rows = [
            {"buySellRatio": "1.3468", "timestamp": 1783814400000},
            {"buySellRatio": "0.5611", "timestamp": 1783814700000},
        ]

    def open_interest_statistics(self, **kwargs):
        self.oi_calls.append(kwargs)
        return self.oi_rows

    def top_trader_account_ratio(self, **kwargs):
        return self.account_rows

    def top_trader_position_ratio(self, **kwargs):
        return self.position_rows

    def global_account_ratio(self, **kwargs):
        return self.global_rows

    def taker_buy_sell_volume(self, **kwargs):
        self.taker_calls.append(kwargs)
        return self.taker_rows


def _write_zip(path: Path, name: str, lines: list[str]) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(name, "\n".join(lines))


def _metric_row(
    timestamp: str,
    *,
    account: float = 1.0,
    position: float = 1.0,
    global_ratio: float = 1.0,
    taker: float = 1.0,
) -> dict[str, object]:
    start = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    interval_end = start + timedelta(minutes=5)
    return {
        "timestamp": timestamp,
        "interval_end": interval_end.isoformat().replace("+00:00", "Z"),
        "available_at": interval_end.isoformat().replace("+00:00", "Z"),
        "symbol": "BTCUSDT",
        "sum_open_interest": 100.0,
        "sum_open_interest_value": 200.0,
        "top_trader_account_long_short_ratio": account,
        "top_trader_position_long_short_ratio": position,
        "global_account_long_short_ratio": global_ratio,
        "taker_buy_sell_volume_ratio": taker,
        "complete": True,
        "confirm": 1,
        "ingest_source": "archive",
    }
