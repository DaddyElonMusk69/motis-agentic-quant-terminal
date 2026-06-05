from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from vegas.schemas import BollingerBands


def ema_series(values: Iterable[Decimal], period: int) -> list[Decimal]:
    values = list(values)
    if period <= 0:
        raise ValueError("EMA period must be positive")
    if not values:
        return []

    multiplier = Decimal("2") / Decimal(period + 1)
    ema_values = [values[0]]
    for value in values[1:]:
        previous = ema_values[-1]
        ema_values.append((value - previous) * multiplier + previous)
    return ema_values


def latest_ema(values: Iterable[Decimal], period: int) -> Decimal:
    series = ema_series(values, period)
    if not series:
        raise ValueError("Cannot compute EMA without values")
    return series[-1]


def latest_bollinger_bands(
    values: Iterable[Decimal],
    period: int = 20,
    stddev_multiplier: Decimal | str = Decimal("2"),
) -> BollingerBands:
    values = list(values)
    if period <= 0:
        raise ValueError("Bollinger period must be positive")
    if len(values) < period:
        raise ValueError("Cannot compute Bollinger Bands without enough values")

    window = values[-period:]
    middle = sum(window, start=Decimal("0")) / Decimal(period)
    variance = sum(((value - middle) ** 2 for value in window), start=Decimal("0")) / Decimal(period)
    stddev = Decimal(variance.sqrt())
    multiplier = Decimal(stddev_multiplier)
    upper = middle + (stddev * multiplier)
    lower = middle - (stddev * multiplier)
    return BollingerBands(upper=upper, middle=middle, lower=lower)
