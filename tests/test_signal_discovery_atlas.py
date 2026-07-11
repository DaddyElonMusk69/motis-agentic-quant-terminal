from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_discovery.atlas import label_fixed_r_timestamp


def test_fixed_r_label_marks_long_when_long_target_precedes_stop() -> None:
    candles = _path(
        (10, "100", "101.2", "99.5", "100.8"),
        (15, "100.8", "102.1", "100.2", "102"),
    )

    result = label_fixed_r_timestamp(
        candles=candles,
        decision_ts=_ts("2026-01-01T00:00:00Z"),
        entry_delay_minutes=5,
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=1,
    )

    assert result["label"] == "LONG"
    assert result["long"]["outcome"] == "TP"
    assert result["long"]["first_touch_ts"] == _ts("2026-01-01T00:15:00Z")
    assert result["short"]["outcome"] == "SL"
    assert result["entry_ts"] == _ts("2026-01-01T00:05:00Z")
    assert result["entry_price"] == 100.0
    assert result["entry_semantics"] == "next_5m_open"


def test_fixed_r_label_preserves_stop_first_despite_later_mfe() -> None:
    candles = _path(
        (10, "100", "100.5", "98.9", "99.2"),
        (15, "99.2", "102.5", "99.1", "102"),
    )

    result = label_fixed_r_timestamp(
        candles=candles,
        decision_ts=_ts("2026-01-01T00:00:00Z"),
        entry_delay_minutes=5,
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=1,
    )

    assert result["label"] == "NEUTRAL"
    assert result["long"]["outcome"] == "SL"
    assert result["long"]["first_touch_ts"] == _ts("2026-01-01T00:10:00Z")
    assert result["long"]["mfe_pct"] == pytest.approx(2.5)


def test_fixed_r_label_marks_short_when_short_target_precedes_stop() -> None:
    candles = _path(
        (10, "100", "100.5", "98.8", "99"),
        (15, "99", "99.4", "97.9", "98"),
    )

    result = label_fixed_r_timestamp(
        candles=candles,
        decision_ts=_ts("2026-01-01T00:00:00Z"),
        entry_delay_minutes=5,
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=1,
    )

    assert result["label"] == "SHORT"
    assert result["short"]["outcome"] == "TP"
    assert result["short"]["first_touch_ts"] == _ts("2026-01-01T00:15:00Z")
    assert result["long"]["outcome"] == "SL"


def test_fixed_r_label_marks_neutral_after_a_complete_timeout() -> None:
    candles = _path()

    result = label_fixed_r_timestamp(
        candles=candles,
        decision_ts=_ts("2026-01-01T00:00:00Z"),
        entry_delay_minutes=5,
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=1,
    )

    assert result["label"] == "NEUTRAL"
    assert result["long"]["outcome"] == "TIMEOUT"
    assert result["short"]["outcome"] == "TIMEOUT"
    assert result["horizon_end_ts"] == _ts("2026-01-01T01:05:00Z")


def test_fixed_r_label_marks_same_candle_barrier_order_ambiguous() -> None:
    candles = _path(
        (10, "100", "102.1", "98.9", "100"),
    )

    result = label_fixed_r_timestamp(
        candles=candles,
        decision_ts=_ts("2026-01-01T00:00:00Z"),
        entry_delay_minutes=5,
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=1,
    )

    assert result["label"] == "AMBIGUOUS"
    assert result["long"]["outcome"] == "AMBIGUOUS"
    assert result["long"]["first_touch_ts"] == _ts("2026-01-01T00:10:00Z")
    assert result["short"]["outcome"] == "SL"


def test_fixed_r_label_rejects_an_incomplete_horizon() -> None:
    candles = _path()[:-1]

    with pytest.raises(ValueError, match="full holding horizon"):
        label_fixed_r_timestamp(
            candles=candles,
            decision_ts=_ts("2026-01-01T00:00:00Z"),
            entry_delay_minutes=5,
            risk_pct=1.0,
            reward_multiple=2.0,
            stop_multiple=1.0,
            horizon_hours=1,
        )


def _path(*overrides: tuple[int, str, str, str, str]) -> list[MarketDataCandle]:
    start = _ts("2026-01-01T00:00:00Z")
    by_minute = {minute: values for minute, *values in overrides}
    candles: list[MarketDataCandle] = []
    for minute in range(0, 66, 5):
        values = by_minute.get(minute, ["100", "100.5", "99.5", "100"])
        candles.append(
            _candle(
                start + timedelta(minutes=minute),
                open_value=values[0],
                high=values[1],
                low=values[2],
                close=values[3],
            )
        )
    return candles


def _candle(
    timestamp: datetime,
    *,
    open_value: str,
    high: str,
    low: str,
    close: str,
) -> MarketDataCandle:
    return MarketDataCandle(
        timestamp=timestamp,
        open=Decimal(open_value),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal(close),
        volume=Decimal("100"),
        vol_ccy=Decimal("100"),
        vol_ccy_quote=Decimal("10000"),
        confirm=1,
    )


def _ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC)
