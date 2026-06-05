from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quant_terminal_sdk.market_data import MarketDataReference


def write_candles(
    *,
    root: Path,
    reference: MarketDataReference,
    year: int,
    month: int,
    rows: list[dict[str, Any]],
) -> Path:
    path = reference.parquet_path(root=root, year=year, month=month)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)
    return path


def read_candles(path: Path) -> list[dict[str, Any]]:
    partition_columns = {"source", "type", "asset", "timeframe", "year", "month"}
    rows = pq.ParquetFile(path).read().to_pylist()
    return [
        {key: value for key, value in row.items() if key not in partition_columns}
        for row in rows
    ]
