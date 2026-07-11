from __future__ import annotations

from bisect import bisect_left, bisect_right
from datetime import UTC, datetime, timedelta
from math import log, sqrt
from typing import Any, Mapping, Sequence

from quant_terminal_sdk.market_data_reader import MarketDataCandle


def build_causal_feature_rows(
    *,
    candles: Sequence[MarketDataCandle],
    decision_rows: Sequence[Mapping[str, Any]],
    walk_forward_start: datetime,
    oi_rows: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    candle_index = _FeatureIndex(candles)
    oi_index = _OIIndex(oi_rows or ())
    wf_start = _as_utc(walk_forward_start)
    features: list[dict[str, Any]] = []

    for decision in sorted(decision_rows, key=lambda row: _coerce_timestamp(row["decision_ts"])):
        decision_ts = _coerce_timestamp(decision["decision_ts"])
        if decision_ts >= wf_start:
            continue
        row = candle_index.features_at(decision_ts)
        row["decision_ts"] = decision_ts
        row["label"] = str(decision.get("label") or "NEUTRAL").upper()
        for hours in (1, 4, 12):
            row[f"oi_change_{hours}h_pct"] = oi_index.change_pct(
                decision_ts=decision_ts,
                lookback_hours=hours,
            )
        features.append(row)
    return features


def select_hard_negatives(
    *,
    feature_rows: Sequence[Mapping[str, Any]],
    episodes: Sequence[Mapping[str, Any]],
    negatives_per_episode: int = 1,
) -> list[dict[str, Any]]:
    if negatives_per_episode <= 0:
        raise ValueError("negatives_per_episode must be positive")
    enriched = _with_volatility_quintiles(feature_rows)
    positives_by_timestamp: dict[datetime, list[dict[str, Any]]] = {}
    neutral_rows: list[dict[str, Any]] = []
    for row in enriched:
        timestamp = _coerce_timestamp(row["decision_ts"])
        positives_by_timestamp.setdefault(timestamp, []).append(row)
        if str(row.get("label") or "").upper() == "NEUTRAL":
            neutral_rows.append(row)
    neutral_rows.sort(key=lambda row: _coerce_timestamp(row["decision_ts"]))

    selected: list[dict[str, Any]] = []
    used_timestamps: set[datetime] = set()
    ordered_episodes = sorted(episodes, key=lambda row: _coerce_timestamp(row["start_ts"]))
    for episode in ordered_episodes:
        start_ts = _coerce_timestamp(episode["start_ts"])
        positive = next(
            (
                row
                for row in positives_by_timestamp.get(start_ts, ())
                if str(row.get("label") or "").upper() in {"LONG", "SHORT"}
            ),
            None,
        )
        if positive is None or positive.get("prior_volatility_quintile") is None:
            continue
        month = start_ts.strftime("%Y-%m")
        hour_block = start_ts.hour // 6
        quintile = positive["prior_volatility_quintile"]
        matches = [
            row
            for row in neutral_rows
            if _coerce_timestamp(row["decision_ts"]) not in used_timestamps
            and _coerce_timestamp(row["decision_ts"]).strftime("%Y-%m") == month
            and _coerce_timestamp(row["decision_ts"]).hour // 6 == hour_block
            and row.get("prior_volatility_quintile") == quintile
        ]
        for candidate in matches[:negatives_per_episode]:
            timestamp = _coerce_timestamp(candidate["decision_ts"])
            used_timestamps.add(timestamp)
            selected.append(
                {
                    **candidate,
                    "matched_episode_id": str(episode["episode_id"]),
                    "matched_positive_ts": start_ts,
                    "match_month": month,
                    "utc_hour_block": hour_block,
                }
            )
    return selected


class _FeatureIndex:
    def __init__(self, candles: Sequence[MarketDataCandle]) -> None:
        by_timestamp = {
            _as_utc(candle.timestamp): candle for candle in candles if candle.confirm == 1
        }
        self.timestamps = tuple(sorted(by_timestamp))
        self.candles = tuple(by_timestamp[timestamp] for timestamp in self.timestamps)
        if not self.candles:
            raise ValueError("causal features require confirmed candles")
        self.closes = tuple(float(candle.close) for candle in self.candles)
        self.highs = tuple(float(candle.high) for candle in self.candles)
        self.lows = tuple(float(candle.low) for candle in self.candles)
        self.volumes = tuple(float(candle.volume) for candle in self.candles)
        if any(close <= 0 for close in self.closes):
            raise ValueError("causal features require positive close prices")
        self.log_closes = tuple(log(close) for close in self.closes)
        log_returns = [0.0]
        log_returns.extend(
            self.log_closes[index] - self.log_closes[index - 1]
            for index in range(1, len(self.log_closes))
        )
        self.return_sum = _prefix(log_returns)
        self.return_square_sum = _prefix(value * value for value in log_returns)
        self.volume_sum = _prefix(self.volumes)
        self.volume_square_sum = _prefix(value * value for value in self.volumes)
        origin = self.timestamps[0]
        hours = tuple((timestamp - origin).total_seconds() / 3600 for timestamp in self.timestamps)
        self.trend_x_sum = _prefix(hours)
        self.trend_y_sum = _prefix(self.log_closes)
        self.trend_xy_sum = _prefix(x * y for x, y in zip(hours, self.log_closes, strict=True))
        self.trend_x_square_sum = _prefix(value * value for value in hours)
        self.extrema = _RangeExtrema(highs=self.highs, lows=self.lows)

    def features_at(self, decision_ts: datetime) -> dict[str, Any]:
        current = bisect_right(self.timestamps, decision_ts) - 1
        if current < 0:
            raise ValueError(f"no causal candle is available at {decision_ts.isoformat()}")
        source_ts = self.timestamps[current]
        result: dict[str, Any] = {"source_candle_ts": source_ts}
        for hours in (1, 4, 12, 24):
            result[f"return_{hours}h_pct"] = self._return_pct(
                current=current,
                decision_ts=decision_ts,
                hours=hours,
            )
        for hours in (4, 24):
            left = self._complete_window_start(decision_ts=decision_ts, hours=hours)
            if left is None or left >= current:
                volatility = None
                range_pct = None
                trend = None
            else:
                volatility = self._realized_volatility_pct(left=left, right=current)
                high = self.extrema.range_high(left, current)
                low = self.extrema.range_low(left, current)
                range_pct = (high - low) / self.closes[current] * 100
                trend = self._trend_slope_pct_per_hour(left=left, right=current)
            result[f"realized_volatility_{hours}h_pct"] = volatility
            result[f"range_{hours}h_pct"] = range_pct
            result[f"trend_slope_{hours}h_pct_per_hour"] = trend
        result["volume_zscore_7d"] = self._volume_zscore_7d(
            current=current,
            decision_ts=decision_ts,
        )
        result["history_complete_24h"] = (
            self._complete_window_start(decision_ts=decision_ts, hours=24) is not None
        )
        return result

    def _return_pct(self, *, current: int, decision_ts: datetime, hours: int) -> float | None:
        target = decision_ts - timedelta(hours=hours)
        baseline = bisect_right(self.timestamps, target) - 1
        if baseline < 0:
            return None
        return (self.closes[current] / self.closes[baseline] - 1) * 100

    def _complete_window_start(self, *, decision_ts: datetime, hours: int) -> int | None:
        target = decision_ts - timedelta(hours=hours)
        if self.timestamps[0] > target:
            return None
        return bisect_left(self.timestamps, target)

    def _realized_volatility_pct(self, *, left: int, right: int) -> float:
        square_sum = _range_sum(self.return_square_sum, left + 1, right)
        return sqrt(max(0.0, square_sum)) * 100

    def _volume_zscore_7d(self, *, current: int, decision_ts: datetime) -> float | None:
        left = self._complete_window_start(decision_ts=decision_ts, hours=7 * 24)
        right = current - 1
        if left is None or right - left + 1 < 2:
            return None
        count = right - left + 1
        total = _range_sum(self.volume_sum, left, right)
        square_total = _range_sum(self.volume_square_sum, left, right)
        mean = total / count
        variance = max(0.0, square_total / count - mean * mean)
        standard_deviation = sqrt(variance)
        return 0.0 if standard_deviation == 0 else (self.volumes[current] - mean) / standard_deviation

    def _trend_slope_pct_per_hour(self, *, left: int, right: int) -> float | None:
        count = right - left + 1
        if count < 2:
            return None
        sum_x = _range_sum(self.trend_x_sum, left, right)
        sum_y = _range_sum(self.trend_y_sum, left, right)
        sum_xy = _range_sum(self.trend_xy_sum, left, right)
        sum_x_square = _range_sum(self.trend_x_square_sum, left, right)
        denominator = count * sum_x_square - sum_x * sum_x
        if denominator == 0:
            return 0.0
        return (count * sum_xy - sum_x * sum_y) / denominator * 100


class _OIIndex:
    def __init__(self, rows: Sequence[Mapping[str, Any]]) -> None:
        by_timestamp: dict[datetime, float] = {}
        for row in rows:
            raw_value = next(
                (
                    row.get(key)
                    for key in ("open_interest", "openInterest", "oi", "oi_value")
                    if row.get(key) is not None
                ),
                None,
            )
            if raw_value is None:
                continue
            by_timestamp[_coerce_timestamp(row.get("timestamp") or row.get("ts"))] = float(
                raw_value
            )
        self.timestamps = tuple(sorted(by_timestamp))
        self.values = tuple(by_timestamp[timestamp] for timestamp in self.timestamps)

    def change_pct(self, *, decision_ts: datetime, lookback_hours: int) -> float | None:
        current = bisect_right(self.timestamps, decision_ts) - 1
        baseline = bisect_right(
            self.timestamps,
            decision_ts - timedelta(hours=lookback_hours),
        ) - 1
        if current < 0 or baseline < 0 or self.values[baseline] == 0:
            return None
        return (self.values[current] / self.values[baseline] - 1) * 100


class _RangeExtrema:
    def __init__(self, *, highs: Sequence[float], lows: Sequence[float]) -> None:
        self.size = 1
        while self.size < len(highs):
            self.size *= 2
        self.high_tree = [float("-inf")] * (2 * self.size)
        self.low_tree = [float("inf")] * (2 * self.size)
        for index, (high, low) in enumerate(zip(highs, lows, strict=True)):
            self.high_tree[self.size + index] = high
            self.low_tree[self.size + index] = low
        for node in range(self.size - 1, 0, -1):
            self.high_tree[node] = max(self.high_tree[node * 2], self.high_tree[node * 2 + 1])
            self.low_tree[node] = min(self.low_tree[node * 2], self.low_tree[node * 2 + 1])

    def range_high(self, left: int, right: int) -> float:
        return self._query(self.high_tree, left, right, maximum=True)

    def range_low(self, left: int, right: int) -> float:
        return self._query(self.low_tree, left, right, maximum=False)

    def _query(
        self,
        tree: Sequence[float],
        left: int,
        right: int,
        *,
        maximum: bool,
    ) -> float:
        result = float("-inf") if maximum else float("inf")
        left += self.size
        right += self.size
        while left <= right:
            if left % 2 == 1:
                result = max(result, tree[left]) if maximum else min(result, tree[left])
                left += 1
            if right % 2 == 0:
                result = max(result, tree[right]) if maximum else min(result, tree[right])
                right -= 1
            left //= 2
            right //= 2
        return result


def _with_volatility_quintiles(
    rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    copied = [dict(row) for row in rows]
    valid = sorted(
        (
            row
            for row in copied
            if row.get("realized_volatility_24h_pct") is not None
        ),
        key=lambda row: (
            float(row["realized_volatility_24h_pct"]),
            _coerce_timestamp(row["decision_ts"]),
        ),
    )
    for rank, row in enumerate(valid):
        row["prior_volatility_quintile"] = min(4, rank * 5 // len(valid))
    return copied


def _prefix(values: Sequence[float] | Any) -> tuple[float, ...]:
    result = [0.0]
    total = 0.0
    for value in values:
        total += float(value)
        result.append(total)
    return tuple(result)


def _range_sum(prefix: Sequence[float], left: int, right: int) -> float:
    return prefix[right + 1] - prefix[left]


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _as_utc(parsed)
    raise ValueError(f"invalid timestamp: {value!r}")
