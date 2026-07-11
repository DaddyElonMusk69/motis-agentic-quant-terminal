from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Sequence

from quant_terminal_sdk.market_data_reader import MarketDataCandle

Direction = Literal["LONG", "SHORT"]
AggregateLabel = Literal["LONG", "SHORT", "NEUTRAL", "AMBIGUOUS"]
PathOutcome = Literal["TP", "SL", "TIMEOUT", "AMBIGUOUS"]


@dataclass(frozen=True, slots=True)
class DiscoveryConfig:
    risk_pct: float
    reward_multiple: float = 2.0
    stop_multiple: float = 1.0
    horizon_hours: float = 36.0
    entry_delay_minutes: int = 5

    def __post_init__(self) -> None:
        if self.risk_pct <= 0:
            raise ValueError("risk_pct must be positive")
        if self.reward_multiple <= 0:
            raise ValueError("reward_multiple must be positive")
        if self.stop_multiple <= 0:
            raise ValueError("stop_multiple must be positive")
        if self.horizon_hours <= 0:
            raise ValueError("horizon_hours must be positive")
        if self.entry_delay_minutes < 0:
            raise ValueError("entry_delay_minutes must be nonnegative")


@dataclass(frozen=True, slots=True)
class FixedRPath:
    direction: Direction
    outcome: PathOutcome
    target_price: float
    stop_price: float
    first_touch_ts: datetime | None
    mfe_pct: float
    mae_pct: float
    terminal_return_pct: float


@dataclass(frozen=True, slots=True)
class FixedRLabel:
    decision_ts: datetime
    entry_ts: datetime
    horizon_end_ts: datetime
    entry_price: float
    entry_semantics: str
    entry_delay_minutes: int
    risk_pct: float
    reward_multiple: float
    stop_multiple: float
    horizon_hours: float
    label: AggregateLabel
    long: FixedRPath
    short: FixedRPath

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


def label_fixed_r_timestamp(
    *,
    candles: Sequence[MarketDataCandle],
    decision_ts: datetime,
    entry_delay_minutes: int,
    risk_pct: float,
    reward_multiple: float,
    stop_multiple: float,
    horizon_hours: float,
) -> dict[str, Any]:
    config = DiscoveryConfig(
        risk_pct=risk_pct,
        reward_multiple=reward_multiple,
        stop_multiple=stop_multiple,
        horizon_hours=horizon_hours,
        entry_delay_minutes=entry_delay_minutes,
    )
    decision_utc = _as_utc(decision_ts)
    confirmed = sorted(
        ((_as_utc(candle.timestamp), candle) for candle in candles if candle.confirm == 1),
        key=lambda item: item[0],
    )
    eligible_entry_ts = decision_utc + timedelta(minutes=config.entry_delay_minutes)
    entry_index = next(
        (index for index, (timestamp, _) in enumerate(confirmed) if timestamp >= eligible_entry_ts),
        None,
    )
    if entry_index is None:
        raise ValueError("no confirmed candle is available for executable entry")

    entry_ts, entry_candle = confirmed[entry_index]
    horizon_end_ts = entry_ts + timedelta(hours=config.horizon_hours)
    if not confirmed or confirmed[-1][0] < horizon_end_ts:
        raise ValueError("candles do not cover the full holding horizon")

    path_candles = [
        (timestamp, candle)
        for timestamp, candle in confirmed[entry_index + 1 :]
        if timestamp <= horizon_end_ts
    ]
    if not path_candles:
        raise ValueError("candles do not cover the full holding horizon")

    entry_price = Decimal(entry_candle.open)
    risk_fraction = Decimal(str(config.risk_pct)) / Decimal("100")
    reward = Decimal(str(config.reward_multiple))
    stop = Decimal(str(config.stop_multiple))

    long_path = _label_direction(
        direction="LONG",
        path_candles=path_candles,
        entry_price=entry_price,
        target_price=entry_price * (Decimal("1") + risk_fraction * reward),
        stop_price=entry_price * (Decimal("1") - risk_fraction * stop),
    )
    short_path = _label_direction(
        direction="SHORT",
        path_candles=path_candles,
        entry_price=entry_price,
        target_price=entry_price * (Decimal("1") - risk_fraction * reward),
        stop_price=entry_price * (Decimal("1") + risk_fraction * stop),
    )
    label = _aggregate_label(long_path=long_path, short_path=short_path)

    return FixedRLabel(
        decision_ts=decision_utc,
        entry_ts=entry_ts,
        horizon_end_ts=horizon_end_ts,
        entry_price=float(entry_price),
        entry_semantics="next_5m_open",
        entry_delay_minutes=config.entry_delay_minutes,
        risk_pct=config.risk_pct,
        reward_multiple=config.reward_multiple,
        stop_multiple=config.stop_multiple,
        horizon_hours=config.horizon_hours,
        label=label,
        long=long_path,
        short=short_path,
    ).to_mapping()


def _label_direction(
    *,
    direction: Direction,
    path_candles: Sequence[tuple[datetime, MarketDataCandle]],
    entry_price: Decimal,
    target_price: Decimal,
    stop_price: Decimal,
) -> FixedRPath:
    outcome: PathOutcome | None = None
    first_touch_ts: datetime | None = None
    highest = entry_price
    lowest = entry_price

    for timestamp, candle in path_candles:
        high = Decimal(candle.high)
        low = Decimal(candle.low)
        highest = max(highest, high)
        lowest = min(lowest, low)
        if outcome is not None:
            continue

        if direction == "LONG":
            target_hit = high >= target_price
            stop_hit = low <= stop_price
        else:
            target_hit = low <= target_price
            stop_hit = high >= stop_price

        if target_hit and stop_hit:
            outcome = "AMBIGUOUS"
            first_touch_ts = timestamp
        elif target_hit:
            outcome = "TP"
            first_touch_ts = timestamp
        elif stop_hit:
            outcome = "SL"
            first_touch_ts = timestamp

    terminal_close = Decimal(path_candles[-1][1].close)
    if direction == "LONG":
        mfe_pct = _percentage(highest - entry_price, entry_price)
        mae_pct = _percentage(entry_price - lowest, entry_price)
        terminal_return_pct = _percentage(terminal_close - entry_price, entry_price)
    else:
        mfe_pct = _percentage(entry_price - lowest, entry_price)
        mae_pct = _percentage(highest - entry_price, entry_price)
        terminal_return_pct = _percentage(entry_price - terminal_close, entry_price)

    return FixedRPath(
        direction=direction,
        outcome=outcome or "TIMEOUT",
        target_price=float(target_price),
        stop_price=float(stop_price),
        first_touch_ts=first_touch_ts,
        mfe_pct=float(max(Decimal("0"), mfe_pct)),
        mae_pct=float(max(Decimal("0"), mae_pct)),
        terminal_return_pct=float(terminal_return_pct),
    )


def _aggregate_label(*, long_path: FixedRPath, short_path: FixedRPath) -> AggregateLabel:
    if "AMBIGUOUS" in {long_path.outcome, short_path.outcome}:
        return "AMBIGUOUS"
    if long_path.outcome == "TP":
        return "LONG"
    if short_path.outcome == "TP":
        return "SHORT"
    return "NEUTRAL"


def _percentage(numerator: Decimal, denominator: Decimal) -> Decimal:
    return numerator / denominator * Decimal("100")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
