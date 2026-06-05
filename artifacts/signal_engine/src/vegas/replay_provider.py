from __future__ import annotations

import csv
from bisect import bisect_left, bisect_right
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Iterable

from vegas.schemas import Candle, ChartSnapshot, MarketStateSnapshot
from vegas.timeframes import floor_timestamp, require_utc
from vegas.workspace import dev_data_root


DEFAULT_TIMEFRAMES = ("2h", "4h", "8h", "12h", "1d")


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def parse_decimal(value: str) -> Decimal:
    return Decimal(value)


def load_candles(path: Path) -> list[Candle]:
    candles: list[Candle] = []
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candles.append(
                Candle(
                    ts=parse_ts(row["ts"]),
                    open=parse_decimal(row["open"]),
                    high=parse_decimal(row["high"]),
                    low=parse_decimal(row["low"]),
                    close=parse_decimal(row["close"]),
                    volume=parse_decimal(row["volume"]),
                    vol_ccy=parse_decimal(row["vol_ccy"]),
                    vol_ccy_quote=parse_decimal(row["vol_ccy_quote"]),
                    confirm=int(row["confirm"]),
                )
            )
    candles.sort(key=lambda candle: candle.ts)
    return candles


class ReplayMarketStateProvider:
    def __init__(
        self,
        asset: str,
        timeframes: Iterable[str] = DEFAULT_TIMEFRAMES,
        context_bars: int = 80,
        ema_warmup_bars: int = 676,
        training_root: str | Path | None = None,
        raw_5m: Iterable[Candle] | None = None,
        derived_candles: dict[str, Iterable[Candle]] | None = None,
    ) -> None:
        self.asset = asset.upper()
        self.timeframes = tuple(timeframes)
        self.context_bars = context_bars
        self.ema_warmup_bars = ema_warmup_bars
        self.training_root = Path(training_root) if training_root is not None else dev_data_root()

        self.raw_5m = (
            _prepare_candles(raw_5m)
            if raw_5m is not None
            else load_candles(self.training_root / "raw" / self.asset / "5m" / "candles.csv")
        )
        self.raw_5m_ts = [candle.ts for candle in self.raw_5m]
        self.derived = {}
        for timeframe in self.timeframes:
            if derived_candles is not None and timeframe in derived_candles:
                self.derived[timeframe] = _prepare_candles(derived_candles[timeframe])
            else:
                self.derived[timeframe] = load_candles(
                    self.training_root / "derived" / self.asset / timeframe / "candles.csv"
                )
        self.derived_ts = {
            timeframe: [candle.ts for candle in candles]
            for timeframe, candles in self.derived.items()
        }

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
            mode="replay",
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
        active_candle = self._active_candle(bucket_start, timestamp, active_5m_path)
        chart_candles = (*completed_context, active_candle)
        ema_source_candles = (*ema_completed_context, active_candle)
        return ChartSnapshot(
            timeframe=timeframe,
            completed_context=completed_context,
            active_candle=active_candle,
            chart_candles=chart_candles,
            active_5m_path=active_5m_path,
            ema_source_candles=ema_source_candles,
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

    def _active_candle(
        self,
        bucket_start: datetime,
        timestamp: datetime,
        active_5m_path: tuple[Candle, ...] | None = None,
    ) -> Candle:
        candles = active_5m_path or self._active_5m_path(bucket_start, timestamp)
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


def _prepare_candles(candles: Iterable[Candle]) -> list[Candle]:
    return sorted(candles, key=lambda candle: candle.ts)
