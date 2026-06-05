from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class Candle:
    ts: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vol_ccy: Decimal
    vol_ccy_quote: Decimal
    confirm: int = 1

    def to_dict(self) -> dict[str, object]:
        data = asdict(self)
        data["ts"] = self.ts.isoformat().replace("+00:00", "Z")
        for key in ("open", "high", "low", "close", "volume", "vol_ccy", "vol_ccy_quote"):
            data[key] = str(data[key])
        return data

    @staticmethod
    def packet_columns() -> list[str]:
        return [
            "ts",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "vol_ccy",
            "vol_ccy_quote",
            "confirm",
        ]

    def to_packet_row(self) -> list[object]:
        data = self.to_dict()
        return [data[column] for column in self.packet_columns()]


@dataclass(frozen=True)
class ChartSnapshot:
    timeframe: str
    completed_context: tuple[Candle, ...]
    active_candle: Candle
    chart_candles: tuple[Candle, ...]
    active_5m_path: tuple[Candle, ...] = ()
    ema_source_candles: tuple[Candle, ...] = ()
    ema_values: dict[int, Decimal] | None = None
    ema_distances: dict[int, Decimal] | None = None
    ema_warmup_counts: dict[int, int] | None = None
    ema_validity: dict[int, bool] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe,
            "completed_context": [candle.to_dict() for candle in self.completed_context],
            "active_candle": self.active_candle.to_dict(),
            "chart_candles": [candle.to_dict() for candle in self.chart_candles],
            "active_5m_path": [candle.to_dict() for candle in self.active_5m_path],
            "ema_source_candle_count": len(self.ema_source_candles),
            "ema_values": {
                str(period): str(value)
                for period, value in (self.ema_values or {}).items()
            },
            "ema_distances": {
                str(period): str(value)
                for period, value in (self.ema_distances or {}).items()
            },
            "ema_warmup_counts": {
                str(period): count
                for period, count in (self.ema_warmup_counts or {}).items()
            },
            "ema_validity": {
                str(period): valid
                for period, valid in (self.ema_validity or {}).items()
            },
        }

    def to_packet_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe,
            "columns": Candle.packet_columns(),
            "completed_candles": [candle.to_packet_row() for candle in self.completed_context],
            "latest_forming_candle": self.active_candle.to_packet_row(),
        }


@dataclass(frozen=True)
class MarketStateSnapshot:
    asset: str
    timestamp: datetime
    mode: str
    latest_5m_candle: Candle
    charts: dict[str, ChartSnapshot]

    def to_dict(self) -> dict[str, object]:
        return {
            "asset": self.asset,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "mode": self.mode,
            "latest_5m_candle": self.latest_5m_candle.to_dict(),
            "charts": {timeframe: chart.to_dict() for timeframe, chart in self.charts.items()},
        }


@dataclass(frozen=True)
class BollingerBands:
    upper: Decimal
    middle: Decimal
    lower: Decimal


@dataclass(frozen=True)
class TunnelInteraction:
    timeframe: str
    tunnel: str
    tunnel_upper_limit: Decimal
    tunnel_lower_limit: Decimal
    market_price: Decimal
    distance_pct: Decimal

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe,
            "tunnel": self.tunnel,
            "tunnel_upper_limit": str(self.tunnel_upper_limit),
            "tunnel_lower_limit": str(self.tunnel_lower_limit),
            "market_price": str(self.market_price),
            "distance_pct": str(self.distance_pct),
        }


@dataclass(frozen=True)
class BollingerInteraction:
    timeframe: str
    band: str
    band_upper_limit: Decimal
    band_middle: Decimal
    band_lower_limit: Decimal
    market_price: Decimal
    distance_pct: Decimal
    band_width_pct: Decimal
    percent_b: Decimal

    def to_dict(self) -> dict[str, object]:
        return {
            "timeframe": self.timeframe,
            "band": self.band,
            "band_upper_limit": str(self.band_upper_limit),
            "band_middle": str(self.band_middle),
            "band_lower_limit": str(self.band_lower_limit),
            "market_price": str(self.market_price),
            "distance_pct": str(self.distance_pct),
            "band_width_pct": str(self.band_width_pct),
            "percent_b": str(self.percent_b),
        }


@dataclass(frozen=True)
class SignalPacket:
    asset: str
    timestamp: datetime
    mode: str
    proximity_threshold: Decimal
    vote_threshold: int
    total_votes: int
    voting_timeframes: tuple[str, ...]
    interactions: dict[str, tuple[TunnelInteraction, ...]]
    charts: dict[str, ChartSnapshot]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "signal_packet.v2",
            "asset": self.asset,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "active_timeframes": list(self.voting_timeframes),
            "interactions": [
                interaction.to_dict()
                for timeframe, interactions in self.interactions.items()
                for interaction in interactions
            ],
            "charts": {
                timeframe: chart.to_packet_dict()
                for timeframe, chart in self.charts.items()
            },
        }


@dataclass(frozen=True)
class BollingerSignalPacket:
    asset: str
    timestamp: datetime
    mode: str
    proximity_threshold: Decimal
    vote_threshold: int
    bb_period: int
    bb_stddev: Decimal
    total_votes: int
    active_timeframes: tuple[str, ...]
    interactions: dict[str, tuple[BollingerInteraction, ...]]
    charts: dict[str, ChartSnapshot]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": "signal_packet.v2",
            "asset": self.asset,
            "timestamp": self.timestamp.isoformat().replace("+00:00", "Z"),
            "active_timeframes": list(self.active_timeframes),
            "interactions": [
                interaction.to_dict()
                for timeframe, interactions in self.interactions.items()
                for interaction in interactions
            ],
            "charts": {
                timeframe: chart.to_packet_dict()
                for timeframe, chart in self.charts.items()
            },
        }
