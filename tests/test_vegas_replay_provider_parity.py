from __future__ import annotations

import csv
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.market_data_reader import MarketDataReader

VEGAS_SRC = Path("artifacts/signal_engine/src").resolve()
if str(VEGAS_SRC) not in sys.path:
    sys.path.insert(0, str(VEGAS_SRC))

from vegas.replay_provider import DEFAULT_TIMEFRAMES, ReplayMarketStateProvider  # noqa: E402
from vegas.schemas import Candle  # noqa: E402


class FakeRepository:
    def __init__(self, refs):
        self.refs = refs

    def get_candle_ref(self, *, asset: str, timeframe: str, origin: str, data_type: str = "candles"):
        return self.refs[(asset, timeframe, origin)]


def test_replay_provider_from_parquet_rows_matches_csv_provider_snapshot(tmp_path: Path):
    asset = "BTC"
    raw_rows = [_row(datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=5 * index)) for index in range(36)]
    derived_rows = {
        "2h": [_row(datetime(2025, 12, 31, 20, tzinfo=UTC)), _row(datetime(2025, 12, 31, 22, tzinfo=UTC))],
        "4h": [_row(datetime(2025, 12, 31, 16, tzinfo=UTC)), _row(datetime(2025, 12, 31, 20, tzinfo=UTC))],
        "8h": [_row(datetime(2025, 12, 31, 8, tzinfo=UTC)), _row(datetime(2025, 12, 31, 16, tzinfo=UTC))],
        "12h": [_row(datetime(2025, 12, 31, 0, tzinfo=UTC)), _row(datetime(2025, 12, 31, 12, tzinfo=UTC))],
        "1d": [_row(datetime(2025, 12, 30, tzinfo=UTC)), _row(datetime(2025, 12, 31, tzinfo=UTC))],
    }
    _write_csv_candles(tmp_path / "raw" / asset / "5m" / "candles.csv", raw_rows)
    for timeframe, rows in derived_rows.items():
        _write_csv_candles(tmp_path / "derived" / asset / timeframe / "candles.csv", rows)

    refs = {}
    _write_parquet_ref(refs, tmp_path, asset=asset, timeframe="5m", origin="raw", rows=raw_rows)
    for timeframe, rows in derived_rows.items():
        _write_parquet_ref(refs, tmp_path, asset=asset, timeframe=timeframe, origin="derived", rows=rows)
    reader = MarketDataReader(repository=FakeRepository(refs), workspace_root=tmp_path)
    parquet_raw = [_to_vegas_candle(candle) for candle in reader.get_candles(asset=asset, timeframe="5m", origin="raw")]
    parquet_derived = {
        timeframe: [
            _to_vegas_candle(candle)
            for candle in reader.get_candles(asset=asset, timeframe=timeframe, origin="derived")
        ]
        for timeframe in DEFAULT_TIMEFRAMES
    }

    csv_provider = ReplayMarketStateProvider(asset=asset, training_root=tmp_path, context_bars=1)
    parquet_provider = ReplayMarketStateProvider(
        asset=asset,
        raw_5m=parquet_raw,
        derived_candles=parquet_derived,
        context_bars=1,
    )

    timestamp = datetime(2026, 1, 1, 0, 30, tzinfo=UTC)
    assert csv_provider.snapshot_at(timestamp).to_dict() == parquet_provider.snapshot_at(timestamp).to_dict()


def _write_parquet_ref(
    refs: dict[tuple[str, str, str], dict[str, str]],
    root: Path,
    *,
    asset: str,
    timeframe: str,
    origin: str,
    rows: list[dict[str, object]],
) -> None:
    storage_uri = root / ".data" / f"origin={origin}" / "source=okx" / "type=candles" / f"asset={asset}" / f"timeframe={timeframe}"
    path = storage_uri / "year=2026" / "month=01" / "data.parquet"
    path.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    refs[(asset, timeframe, origin)] = {
        "dataset_id": f"{asset}-{origin}-{timeframe}",
        "storage_backend": "parquet",
        "storage_uri": str(storage_uri),
    }


def _write_csv_candles(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["ts", "open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote", "confirm"],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({"ts": row["timestamp"], **{key: row[key] for key in writer.fieldnames if key != "ts"}})


def _row(timestamp: datetime) -> dict[str, object]:
    return {
        "timestamp": timestamp.isoformat().replace("+00:00", "Z"),
        "open": "1",
        "high": "1",
        "low": "1",
        "close": "1",
        "volume": "1",
        "vol_ccy": "1",
        "vol_ccy_quote": "1",
        "confirm": 1,
    }


def _to_vegas_candle(candle) -> Candle:
    return Candle(
        ts=candle.timestamp,
        open=candle.open,
        high=candle.high,
        low=candle.low,
        close=candle.close,
        volume=candle.volume,
        vol_ccy=candle.vol_ccy,
        vol_ccy_quote=candle.vol_ccy_quote,
        confirm=candle.confirm,
    )
