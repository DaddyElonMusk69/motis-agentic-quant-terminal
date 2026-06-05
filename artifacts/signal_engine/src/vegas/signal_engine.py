from __future__ import annotations

from decimal import Decimal

from vegas.indicators import latest_ema
from vegas.schemas import ChartSnapshot, MarketStateSnapshot, SignalPacket, TunnelInteraction


EMA_TUNNELS: dict[str, tuple[int, int]] = {
    "fast": (36, 43),
    "mid": (144, 169),
    "slow": (576, 676),
}

REQUIRED_CONTEXT_TIMEFRAMES: tuple[str, ...] = ("2h", "1d")


class UniversalVegasSignalEngine:
    def __init__(
        self,
        proximity_threshold: Decimal | str = Decimal("0.002"),
        vote_threshold: int = 3,
    ) -> None:
        self.proximity_threshold = Decimal(proximity_threshold)
        self.vote_threshold = vote_threshold

    def scan(self, snapshot: MarketStateSnapshot) -> SignalPacket | None:
        interactions_by_timeframe: dict[str, tuple[TunnelInteraction, ...]] = {}
        enriched_charts: dict[str, ChartSnapshot] = {}
        voting_timeframes: list[str] = []

        for timeframe, chart in snapshot.charts.items():
            enriched_chart, interactions = self._scan_chart(chart)
            enriched_charts[timeframe] = enriched_chart
            if interactions:
                interactions_by_timeframe[timeframe] = tuple(interactions)
                voting_timeframes.append(timeframe)

        total_votes = len(voting_timeframes)
        if total_votes < self.vote_threshold:
            return None

        chart_timeframes: list[str] = []
        seen_timeframes: set[str] = set()
        for timeframe in (*voting_timeframes, *REQUIRED_CONTEXT_TIMEFRAMES):
            if timeframe in enriched_charts and timeframe not in seen_timeframes:
                chart_timeframes.append(timeframe)
                seen_timeframes.add(timeframe)

        return SignalPacket(
            asset=snapshot.asset,
            timestamp=snapshot.timestamp,
            mode=snapshot.mode,
            proximity_threshold=self.proximity_threshold,
            vote_threshold=self.vote_threshold,
            total_votes=total_votes,
            voting_timeframes=tuple(voting_timeframes),
            interactions=interactions_by_timeframe,
            charts={timeframe: enriched_charts[timeframe] for timeframe in chart_timeframes},
        )

    def _scan_chart(self, chart: ChartSnapshot) -> tuple[ChartSnapshot, list[TunnelInteraction]]:
        active_price = chart.active_candle.close
        ema_source_candles = chart.ema_source_candles or chart.chart_candles
        closes = [candle.close for candle in ema_source_candles]
        completed_warmup_count = max(0, len(ema_source_candles) - 1)
        interactions: list[TunnelInteraction] = []
        ema_values: dict[int, Decimal] = {}
        ema_distances: dict[int, Decimal] = {}
        ema_warmup_counts: dict[int, int] = {}
        ema_validity: dict[int, bool] = {}

        for tunnel, periods in EMA_TUNNELS.items():
            period_distances: list[Decimal] = []
            period_values: list[Decimal] = []
            periods_are_valid = True
            for period in periods:
                ema_value = latest_ema(closes, period)
                ema_values[period] = ema_value
                distance_pct = abs(active_price - ema_value) / active_price
                ema_distances[period] = distance_pct
                ema_warmup_counts[period] = completed_warmup_count
                ema_validity[period] = completed_warmup_count >= period
                periods_are_valid = periods_are_valid and ema_validity[period]
                period_distances.append(distance_pct)
                period_values.append(ema_value)

            nearest_distance = min(period_distances)
            if periods_are_valid and nearest_distance <= self.proximity_threshold:
                interactions.append(
                    TunnelInteraction(
                        timeframe=chart.timeframe,
                        tunnel=tunnel,
                        tunnel_upper_limit=max(period_values),
                        tunnel_lower_limit=min(period_values),
                        market_price=active_price,
                        distance_pct=nearest_distance,
                    )
                )

        enriched_chart = ChartSnapshot(
            timeframe=chart.timeframe,
            completed_context=chart.completed_context,
            active_candle=chart.active_candle,
            chart_candles=chart.chart_candles,
            active_5m_path=chart.active_5m_path,
            ema_source_candles=ema_source_candles,
            ema_values=ema_values,
            ema_distances=ema_distances,
            ema_warmup_counts=ema_warmup_counts,
            ema_validity=ema_validity,
        )
        return enriched_chart, interactions

    def _chart_interactions(self, chart: ChartSnapshot) -> list[TunnelInteraction]:
        return self._scan_chart(chart)[1]
