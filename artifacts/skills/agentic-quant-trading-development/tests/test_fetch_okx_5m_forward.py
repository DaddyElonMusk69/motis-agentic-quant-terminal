from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "data" / "fetch_okx_5m_forward.py"
SPEC = importlib.util.spec_from_file_location("fetch_okx_5m_forward", SCRIPT_PATH)
assert SPEC is not None
forward = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = forward
SPEC.loader.exec_module(forward)


def candle(ts_ms: int, close: str = "100", confirm: str = "1"):
    return forward.Candle(
        ts_ms=ts_ms,
        open=close,
        high=close,
        low=close,
        close=close,
        volume="1",
        vol_ccy="1",
        vol_ccy_quote="1",
        confirm=confirm,
    )


def test_append_forward_keeps_confirmed_new_candles_only() -> None:
    existing = [candle(1000), candle(2000)]
    fetched = [
        candle(2000, close="duplicate"),
        candle(3000, close="new"),
        candle(4000, close="unconfirmed", confirm="0"),
        candle(5000, close="past-target"),
    ]

    combined, added = forward.append_forward(existing, fetched, target_end_ms=3000)

    assert added == 1
    assert [(item.ts_ms, item.close) for item in combined] == [
        (1000, "100"),
        (2000, "100"),
        (3000, "new"),
    ]


def test_write_and_load_candles_preserves_sorted_canonical_csv(tmp_path: Path) -> None:
    path = tmp_path / "candles.csv"
    forward.write_candles(path, [candle(300000, "103"), candle(0, "100"), candle(300000, "latest")])

    with path.open(newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert [row["ts"] for row in rows] == ["1970-01-01T00:00:00Z", "1970-01-01T00:05:00Z"]
    assert rows[-1]["close"] == "latest"

    loaded = forward.load_existing(path)
    assert [(item.ts_ms, item.close) for item in loaded] == [(0, "100"), (300000, "latest")]


def test_has_5m_gaps_detects_missing_middle_candles() -> None:
    assert forward.has_5m_gaps([candle(0), candle(300000), candle(600000)]) is False
    assert forward.has_5m_gaps([candle(0), candle(600000)]) is True


def test_contiguous_anchor_rewinds_to_gap_before_target() -> None:
    candles = [
        candle(0),
        candle(300000),
        candle(1800000),
        candle(2100000),
    ]

    assert forward.contiguous_anchor_before_target(candles, target_end_ms=1500000) == 300000


def test_contiguous_anchor_accepts_gap_after_target() -> None:
    candles = [
        candle(0),
        candle(300000),
        candle(1800000),
        candle(2100000),
    ]

    assert forward.contiguous_anchor_before_target(candles, target_end_ms=300000) == 300000


def test_has_gap_between_requires_reaching_target() -> None:
    candles = [
        candle(0),
        candle(300000),
        candle(1800000),
    ]

    assert forward.has_gap_between(candles, start_ms=300000, end_ms=900000) is True
