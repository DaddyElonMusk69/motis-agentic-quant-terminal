from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "artifacts"
    / "skills"
    / "agentic-quant-trading-development"
    / "scripts"
    / "data"
    / "audit_repair_okx_raw_5m.py"
)


def load_module():
    spec = importlib.util.spec_from_file_location("audit_repair_okx_raw_5m", SCRIPT_PATH)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_compare_candles_reports_missing_extra_field_mismatch_and_zero_quote_span():
    module = load_module()

    local = {
        "2026-05-15T00:00:00Z": _row("2026-05-15T00:00:00Z", open="100", vol_ccy="0", vol_ccy_quote="0"),
        "2026-05-15T00:05:00Z": _row("2026-05-15T00:05:00Z", close="101"),
        "2026-05-15T00:15:00Z": _row("2026-05-15T00:15:00Z", close="103"),
    }
    okx = {
        "2026-05-15T00:00:00Z": _row("2026-05-15T00:00:00Z", open="100.5", vol_ccy_quote="10"),
        "2026-05-15T00:05:00Z": _row("2026-05-15T00:05:00Z", close="101"),
        "2026-05-15T00:10:00Z": _row("2026-05-15T00:10:00Z", close="102"),
    }

    result = module.compare_candles(
        local=local,
        okx=okx,
        start="2026-05-15T00:00:00Z",
        end="2026-05-15T00:15:00Z",
    )

    assert result.expected_slots == 4
    assert result.missing_timestamps == ["2026-05-15T00:10:00Z"]
    assert result.extra_timestamps == ["2026-05-15T00:15:00Z"]
    assert result.price_mismatches == 1
    assert result.volume_only_mismatches == 0
    assert result.zero_quote_spans == [
        {"start": "2026-05-15T00:00:00Z", "end": "2026-05-15T00:00:00Z", "rows": 1}
    ]


def test_build_repaired_rows_replaces_only_audited_range_and_preserves_outside_rows():
    module = load_module()

    existing = [
        _row("2026-05-14T23:55:00Z", close="99"),
        _row("2026-05-15T00:00:00Z", close="98"),
        _row("2026-05-15T00:05:00Z", close="101"),
        _row("2026-05-15T00:15:00Z", close="103"),
    ]
    okx = {
        "2026-05-15T00:00:00Z": _row("2026-05-15T00:00:00Z", close="100"),
        "2026-05-15T00:05:00Z": _row("2026-05-15T00:05:00Z", close="101"),
        "2026-05-15T00:10:00Z": _row("2026-05-15T00:10:00Z", close="102"),
    }

    repaired = module.build_repaired_rows(
        existing_rows=existing,
        okx_rows=okx,
        start="2026-05-15T00:00:00Z",
        end="2026-05-15T00:10:00Z",
    )

    assert [row["timestamp"] for row in repaired] == [
        "2026-05-14T23:55:00Z",
        "2026-05-15T00:00:00Z",
        "2026-05-15T00:05:00Z",
        "2026-05-15T00:10:00Z",
        "2026-05-15T00:15:00Z",
    ]
    assert repaired[0]["close"] == "99"
    assert repaired[1]["close"] == "100"
    assert repaired[3]["close"] == "102"
    assert repaired[4]["close"] == "103"


def _row(
    timestamp: str,
    *,
    open: str = "100",
    high: str = "101",
    low: str = "99",
    close: str = "100",
    volume: str = "1",
    vol_ccy: str = "0.1",
    vol_ccy_quote: str = "10",
    confirm: str = "1",
) -> dict[str, str]:
    return {
        "timestamp": timestamp,
        "open": open,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vol_ccy": vol_ccy,
        "vol_ccy_quote": vol_ccy_quote,
        "confirm": confirm,
    }
