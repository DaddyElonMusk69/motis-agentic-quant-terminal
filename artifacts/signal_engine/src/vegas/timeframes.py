from __future__ import annotations

from datetime import UTC, datetime, timedelta


TIMEFRAME_DELTAS: dict[str, timedelta] = {
    "5m": timedelta(minutes=5),
    "2h": timedelta(hours=2),
    "4h": timedelta(hours=4),
    "8h": timedelta(hours=8),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
}


def require_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Timestamp must be timezone-aware UTC")
    return value.astimezone(UTC)


def floor_timestamp(value: datetime, timeframe: str) -> datetime:
    value = require_utc(value)
    delta = TIMEFRAME_DELTAS[timeframe]
    epoch = datetime(1970, 1, 1, tzinfo=UTC)
    seconds = int((value - epoch).total_seconds())
    bucket = int(delta.total_seconds())
    return epoch + timedelta(seconds=seconds - (seconds % bucket))

