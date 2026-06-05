from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from vegas.indicators import latest_bollinger_bands
from vegas.schemas import (
    BollingerInteraction,
    BollingerSignalPacket,
    ChartSnapshot,
    MarketStateSnapshot,
)


BOLLINGER_DEFAULT_TIMEFRAMES = ("4h", "8h", "12h", "1d")
BOLLINGER_DEFAULT_WATCHED_BANDS = ("upper", "lower")


class UniversalBollingerSignalEngine:
    def __init__(
        self,
        bb_period: int = 20,
        bb_stddev: Decimal | str = Decimal("2"),
        proximity_threshold: Decimal | str = Decimal("0.002"),
        vote_threshold: int = 3,
        watched_bands: Iterable[str] = BOLLINGER_DEFAULT_WATCHED_BANDS,
    ) -> None:
        self.bb_period = bb_period
        self.bb_stddev = Decimal(bb_stddev)
        self.proximity_threshold = Decimal(proximity_threshold)
        self.vote_threshold = vote_threshold
        self.watched_bands = tuple(watched_bands)

    def scan(self, snapshot: MarketStateSnapshot) -> BollingerSignalPacket | None:
        interactions_by_timeframe: dict[str, tuple[BollingerInteraction, ...]] = {}
        active_timeframes: list[str] = []

        for timeframe, chart in snapshot.charts.items():
            interactions = self._chart_interactions(chart)
            if interactions:
                interactions_by_timeframe[timeframe] = tuple(interactions)
                active_timeframes.append(timeframe)

        total_votes = len(active_timeframes)
        if total_votes < self.vote_threshold:
            return None

        return BollingerSignalPacket(
            asset=snapshot.asset,
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            proximity_threshold=self.proximity_threshold,
            vote_threshold=self.vote_threshold,
            bb_period=self.bb_period,
            bb_stddev=self.bb_stddev,
            total_votes=total_votes,
            active_timeframes=tuple(active_timeframes),
            interactions=interactions_by_timeframe,
            charts={timeframe: snapshot.charts[timeframe] for timeframe in active_timeframes},
        )

    def _chart_interactions(self, chart: ChartSnapshot) -> list[BollingerInteraction]:
        market_price = chart.active_candle.close
        source_candles = chart.ema_source_candles or chart.chart_candles
        if len(source_candles) < self.bb_period:
            return []

        bands = latest_bollinger_bands(
            [candle.close for candle in source_candles],
            period=self.bb_period,
            stddev_multiplier=self.bb_stddev,
        )
        band_values = {
            "upper": bands.upper,
            "middle": bands.middle,
            "lower": bands.lower,
        }
        band_range = bands.upper - bands.lower
        band_width_pct = Decimal("0") if market_price == 0 else band_range / market_price
        percent_b = (
            Decimal("0")
            if band_range == 0
            else (market_price - bands.lower) / band_range
        )

        interactions: list[BollingerInteraction] = []
        for band in self.watched_bands:
            band_value = band_values[band]
            distance_pct = Decimal("0") if market_price == 0 else abs(market_price - band_value) / market_price
            if distance_pct <= self.proximity_threshold:
                interactions.append(
                    BollingerInteraction(
                        timeframe=chart.timeframe,
                        band=band,
                        band_upper_limit=bands.upper,
                        band_middle=bands.middle,
                        band_lower_limit=bands.lower,
                        market_price=market_price,
                        distance_pct=distance_pct,
                        band_width_pct=band_width_pct,
                        percent_b=percent_b,
                    )
                )
        return interactions
