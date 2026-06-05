from __future__ import annotations

import json
from datetime import UTC, datetime
from decimal import Decimal

from vegas.packet_format import dumps_signal_packet
from vegas.schemas import Candle


def test_dumps_signal_packet_keeps_each_candle_row_on_one_line() -> None:
    payload = {
        "schema_version": "signal_packet.v2",
        "asset": "TEST",
        "timestamp": "2026-01-01T00:00:00Z",
        "active_timeframes": ["2h"],
        "interactions": [],
        "charts": {
            "2h": {
                "timeframe": "2h",
                "columns": Candle.packet_columns(),
                "completed_candles": [
                    Candle(
                        ts=datetime(2025, 12, 31, 20, tzinfo=UTC),
                        open=Decimal("100"),
                        high=Decimal("101"),
                        low=Decimal("99"),
                        close=Decimal("100.5"),
                        volume=Decimal("10"),
                        vol_ccy=Decimal("0"),
                        vol_ccy_quote=Decimal("0"),
                        confirm=1,
                    ).to_packet_row(),
                    Candle(
                        ts=datetime(2025, 12, 31, 22, tzinfo=UTC),
                        open=Decimal("101"),
                        high=Decimal("102"),
                        low=Decimal("100"),
                        close=Decimal("101.5"),
                        volume=Decimal("11"),
                        vol_ccy=Decimal("0"),
                        vol_ccy_quote=Decimal("0"),
                        confirm=1,
                    ).to_packet_row(),
                ],
                "latest_forming_candle": Candle(
                    ts=datetime(2026, 1, 1, tzinfo=UTC),
                    open=Decimal("102"),
                    high=Decimal("103"),
                    low=Decimal("101"),
                    close=Decimal("102.5"),
                    volume=Decimal("12"),
                    vol_ccy=Decimal("0"),
                    vol_ccy_quote=Decimal("0"),
                    confirm=0,
                ).to_packet_row(),
            }
        },
    }

    rendered = dumps_signal_packet(payload)

    assert json.loads(rendered) == payload
    row_lines = [
        line
        for line in rendered.splitlines()
        if "2025-12-31T20:00:00Z" in line or "2025-12-31T22:00:00Z" in line
    ]
    assert len(row_lines) == 2
    assert all(line.strip().startswith("[") and line.rstrip().endswith((",", "]")) for line in row_lines)
