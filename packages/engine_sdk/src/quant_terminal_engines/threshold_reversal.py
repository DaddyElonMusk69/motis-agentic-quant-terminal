from __future__ import annotations

from datetime import datetime
from typing import Any


ENGINE_ID = "threshold_reversal"
ENGINE_VERSION = "0.1.0"
PAYLOAD_SCHEMA = "threshold_reversal.v1"


def generate_signals(payload: dict[str, Any]) -> dict[str, Any]:
    rows = sorted(payload.get("rows", []), key=lambda row: row["timestamp"])
    parameters = payload.get("parameters", {})
    min_move_pct = float(parameters.get("min_move_pct", 1.0))
    asset = payload["asset"]
    instrument = payload["instrument"]
    dataset_refs = list(payload.get("dataset_refs", []))

    signals: list[dict[str, Any]] = []
    if len(rows) < 2:
        return {"signals": signals}

    anchor_open = float(rows[0]["open"])
    for row in rows[1:]:
        close = float(row["close"])
        move_pct = round(((close - anchor_open) / anchor_open) * 100, 6)
        if abs(move_pct) < min_move_pct:
            continue
        timestamp = row["timestamp"]
        signals.append(
            {
                "signal_id": f"{ENGINE_ID}-{asset}-{_compact_timestamp(timestamp)}",
                "signal_engine_id": ENGINE_ID,
                "signal_engine_version": ENGINE_VERSION,
                "asset": asset,
                "instrument": instrument,
                "timestamp": timestamp,
                "data_refs": dataset_refs,
                "payload_schema": PAYLOAD_SCHEMA,
                "payload": {
                    "move_pct": move_pct,
                    "lookback_open": anchor_open,
                    "current_close": close,
                    "neutral_trigger": "lookback_move_exceeded",
                },
            }
        )
    return {"signals": signals}


def _compact_timestamp(value: str) -> str:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).strftime("%Y%m%dT%H%M%SZ")
