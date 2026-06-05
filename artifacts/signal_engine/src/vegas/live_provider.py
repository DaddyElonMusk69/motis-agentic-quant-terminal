from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from vegas.candle_store import derived_path, load_candles, raw_5m_path
from vegas.replay_provider import DEFAULT_TIMEFRAMES
from vegas.schemas import Candle, ChartSnapshot, MarketStateSnapshot
from vegas.timeframes import floor_timestamp, require_utc
from vegas.workspace import live_data_root


class LiveMarketStateProvider:
    def __init__(
        self,
        asset: str,
        timeframes: Iterable[str] = DEFAULT_TIMEFRAMES,
        context_bars: int = 80,
        ema_warmup_bars: int = 676,
        live_root: str | Path | None = None,
    ) -> None:
        self.asset = asset.upper()
        self.timeframes = tuple(timeframes)
        self.context_bars = context_bars
        self.ema_warmup_bars = ema_warmup_bars
        self.live_root = Path(live_root) if live_root is not None else live_data_root()

        self.raw_5m = load_candles(raw_5m_path(self.live_root, self.asset), confirmed_only=True)
        self.raw_5m_ts = [candle.ts for candle in self.raw_5m]
        self.derived = {
            timeframe: load_candles(derived_path(self.live_root, self.asset, timeframe))
            for timeframe in self.timeframes
        }
        self.derived_ts = {
            timeframe: [candle.ts for candle in candles]
            for timeframe, candles in self.derived.items()
        }

    def latest_timestamp(self) -> datetime:
        if not self.raw_5m:
            raise ValueError(f"No live 5m candles available for {self.asset}")
        return self.raw_5m[-1].ts

    def snapshot_at(self, timestamp: datetime) -> MarketStateSnapshot:
        timestamp = require_utc(timestamp)
        latest_5m = self._latest_5m_at(timestamp)
        charts = {
            timeframe: self._chart_snapshot(timeframe, timestamp)
            for timeframe in self.timeframes
        }
        return MarketStateSnapshot(
            asset=self.asset,
            timestamp=timestamp,
            mode="live",
            latest_5m_candle=latest_5m,
            charts=charts,
        )

    def _latest_5m_at(self, timestamp: datetime) -> Candle:
        index = bisect_right(self.raw_5m_ts, timestamp) - 1
        if index < 0:
            raise ValueError(f"No 5m candle available at or before {timestamp.isoformat()}")
        return self.raw_5m[index]

    def _chart_snapshot(self, timeframe: str, timestamp: datetime) -> ChartSnapshot:
        bucket_start = floor_timestamp(timestamp, timeframe)
        completed_context = self._completed_context(timeframe, bucket_start)
        ema_completed_context = self._ema_completed_context(timeframe, bucket_start)
        active_5m_path = self._active_5m_path(bucket_start, timestamp)
        active_candle = self._active_candle(bucket_start, active_5m_path)
        return ChartSnapshot(
            timeframe=timeframe,
            completed_context=completed_context,
            active_candle=active_candle,
            chart_candles=(*completed_context, active_candle),
            active_5m_path=active_5m_path,
            ema_source_candles=(*ema_completed_context, active_candle),
        )

    def _completed_context(self, timeframe: str, active_bucket_start: datetime) -> tuple[Candle, ...]:
        candles = self.derived[timeframe]
        timestamps = self.derived_ts[timeframe]
        end = bisect_left(timestamps, active_bucket_start)
        start = end - self.context_bars
        if start < 0:
            raise ValueError(
                f"Not enough completed {timeframe} candles before {active_bucket_start.isoformat()}"
            )
        return tuple(candles[start:end])

    def _ema_completed_context(
        self,
        timeframe: str,
        active_bucket_start: datetime,
    ) -> tuple[Candle, ...]:
        candles = self.derived[timeframe]
        timestamps = self.derived_ts[timeframe]
        end = bisect_left(timestamps, active_bucket_start)
        start = max(0, end - self.ema_warmup_bars)
        return tuple(candles[start:end])

    def _active_5m_path(self, bucket_start: datetime, timestamp: datetime) -> tuple[Candle, ...]:
        start = bisect_left(self.raw_5m_ts, bucket_start)
        end = bisect_right(self.raw_5m_ts, timestamp)
        candles = self.raw_5m[start:end]
        if not candles:
            raise ValueError(
                f"No 5m candles to reconstruct active candle from {bucket_start.isoformat()} "
                f"through {timestamp.isoformat()}"
            )
        return tuple(candles)

    def _active_candle(self, bucket_start: datetime, candles: tuple[Candle, ...]) -> Candle:
        return Candle(
            ts=bucket_start,
            open=candles[0].open,
            high=max(candle.high for candle in candles),
            low=min(candle.low for candle in candles),
            close=candles[-1].close,
            volume=sum((candle.volume for candle in candles), start=Decimal("0")),
            vol_ccy=sum((candle.vol_ccy for candle in candles), start=Decimal("0")),
            vol_ccy_quote=sum((candle.vol_ccy_quote for candle in candles), start=Decimal("0")),
            confirm=0,
        )
