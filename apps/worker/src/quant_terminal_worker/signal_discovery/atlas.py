from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any, Literal, Mapping, Sequence

from quant_terminal_sdk.market_data_reader import MarketDataCandle

Direction = Literal["LONG", "SHORT"]
AggregateLabel = Literal["LONG", "SHORT", "NEUTRAL", "AMBIGUOUS"]
PathOutcome = Literal["TP", "SL", "TIMEOUT", "AMBIGUOUS"]


@dataclass(frozen=True, slots=True)
class DiscoveryConfig:
    risk_values: tuple[float, ...]
    research_start: datetime
    research_end: datetime
    walk_forward_start: datetime
    reward_multiple: float = 2.0
    stop_multiple: float = 1.0
    horizon_hours: tuple[float, ...] = (36.0, 48.0)
    entry_delays_minutes: tuple[int, ...] = (5,)
    fee_bps_per_side: float = 0.0
    slippage_bps_per_side: float = 0.0

    def __post_init__(self) -> None:
        risk_values = tuple(sorted({float(value) for value in self.risk_values}))
        horizons = tuple(sorted({float(value) for value in self.horizon_hours}))
        delays = tuple(sorted({int(value) for value in self.entry_delays_minutes}))
        research_start = _as_utc(self.research_start)
        research_end = _as_utc(self.research_end)
        walk_forward_start = _as_utc(self.walk_forward_start)
        if not risk_values or any(value <= 0 for value in risk_values):
            raise ValueError("risk_values must contain positive values")
        if self.reward_multiple <= 0:
            raise ValueError("reward_multiple must be positive")
        if self.stop_multiple <= 0:
            raise ValueError("stop_multiple must be positive")
        if not horizons or any(value <= 0 for value in horizons):
            raise ValueError("horizon_hours must contain positive values")
        if not delays or any(value < 0 for value in delays):
            raise ValueError("entry_delays_minutes must contain nonnegative values")
        if self.fee_bps_per_side < 0 or self.slippage_bps_per_side < 0:
            raise ValueError("cost assumptions must be nonnegative")
        if research_start > research_end:
            raise ValueError("research_start must not follow research_end")
        if research_end >= walk_forward_start:
            raise ValueError("research_end must be strictly before walk_forward_start")
        object.__setattr__(self, "risk_values", risk_values)
        object.__setattr__(self, "horizon_hours", horizons)
        object.__setattr__(self, "entry_delays_minutes", delays)
        object.__setattr__(self, "research_start", research_start)
        object.__setattr__(self, "research_end", research_end)
        object.__setattr__(self, "walk_forward_start", walk_forward_start)


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


class _CandleIndex:
    def __init__(self, candles: Sequence[MarketDataCandle]) -> None:
        by_timestamp = {
            _as_utc(candle.timestamp): candle for candle in candles if candle.confirm == 1
        }
        self.timestamps = tuple(sorted(by_timestamp))
        self.candles = tuple(by_timestamp[timestamp] for timestamp in self.timestamps)
        self._size = 1
        while self._size < len(self.candles):
            self._size *= 2
        self._high_tree = [Decimal("-Infinity")] * (2 * self._size)
        self._low_tree = [Decimal("Infinity")] * (2 * self._size)
        for offset, candle in enumerate(self.candles):
            leaf = self._size + offset
            self._high_tree[leaf] = Decimal(candle.high)
            self._low_tree[leaf] = Decimal(candle.low)
        for node in range(self._size - 1, 0, -1):
            self._high_tree[node] = max(
                self._high_tree[node * 2], self._high_tree[node * 2 + 1]
            )
            self._low_tree[node] = min(
                self._low_tree[node * 2], self._low_tree[node * 2 + 1]
            )

    def first_index_at_or_after(self, timestamp: datetime) -> int | None:
        index = bisect_left(self.timestamps, timestamp)
        return index if index < len(self.timestamps) else None

    def last_index_at_or_before(self, timestamp: datetime) -> int | None:
        index = bisect_right(self.timestamps, timestamp) - 1
        return index if index >= 0 else None

    def first_high_at_least(
        self,
        left: int,
        right: int,
        threshold: Decimal,
    ) -> int | None:
        return self._first_match(
            tree=self._high_tree,
            left=left,
            right=right,
            threshold=threshold,
            at_least=True,
        )

    def first_low_at_most(
        self,
        left: int,
        right: int,
        threshold: Decimal,
    ) -> int | None:
        return self._first_match(
            tree=self._low_tree,
            left=left,
            right=right,
            threshold=threshold,
            at_least=False,
        )

    def range_high(self, left: int, right: int) -> Decimal:
        return self._range_extreme(self._high_tree, left, right, maximum=True)

    def range_low(self, left: int, right: int) -> Decimal:
        return self._range_extreme(self._low_tree, left, right, maximum=False)

    def _first_match(
        self,
        *,
        tree: Sequence[Decimal],
        left: int,
        right: int,
        threshold: Decimal,
        at_least: bool,
    ) -> int | None:
        def search(node: int, node_left: int, node_right: int) -> int | None:
            if node_right < left or node_left > right:
                return None
            extreme = tree[node]
            if (at_least and extreme < threshold) or (not at_least and extreme > threshold):
                return None
            if node_left == node_right:
                return node_left if node_left < len(self.candles) else None
            middle = (node_left + node_right) // 2
            first = search(node * 2, node_left, middle)
            return first if first is not None else search(node * 2 + 1, middle + 1, node_right)

        return search(1, 0, self._size - 1)

    def _range_extreme(
        self,
        tree: Sequence[Decimal],
        left: int,
        right: int,
        *,
        maximum: bool,
    ) -> Decimal:
        result = Decimal("-Infinity") if maximum else Decimal("Infinity")
        left += self._size
        right += self._size
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
    _validate_label_parameters(
        risk_pct=risk_pct,
        reward_multiple=reward_multiple,
        stop_multiple=stop_multiple,
        horizon_hours=horizon_hours,
        entry_delay_minutes=entry_delay_minutes,
    )
    index = _CandleIndex(candles)
    return _label_fixed_r_from_index(
        index=index,
        decision_ts=decision_ts,
        entry_delay_minutes=entry_delay_minutes,
        risk_pct=risk_pct,
        reward_multiple=reward_multiple,
        stop_multiple=stop_multiple,
        horizon_hours=horizon_hours,
    )


def _label_fixed_r_from_index(
    *,
    index: _CandleIndex,
    decision_ts: datetime,
    entry_delay_minutes: int,
    risk_pct: float,
    reward_multiple: float,
    stop_multiple: float,
    horizon_hours: float,
) -> dict[str, Any]:
    decision_utc = _as_utc(decision_ts)
    eligible_entry_ts = decision_utc + timedelta(minutes=entry_delay_minutes)
    entry_index = index.first_index_at_or_after(eligible_entry_ts)
    if entry_index is None:
        raise ValueError("no confirmed candle is available for executable entry")

    entry_ts = index.timestamps[entry_index]
    horizon_end_ts = entry_ts + timedelta(hours=horizon_hours)
    if not index.timestamps or index.timestamps[-1] < horizon_end_ts:
        raise ValueError("candles do not cover the full holding horizon")
    path_start = entry_index + 1
    path_end = index.last_index_at_or_before(horizon_end_ts)
    if path_end is None or path_start > path_end:
        raise ValueError("candles do not cover the full holding horizon")

    entry_price = Decimal(index.candles[entry_index].open)
    risk_fraction = Decimal(str(risk_pct)) / Decimal("100")
    reward = Decimal(str(reward_multiple))
    stop = Decimal(str(stop_multiple))

    long_path = _label_indexed_direction(
        direction="LONG",
        index=index,
        path_start=path_start,
        path_end=path_end,
        entry_price=entry_price,
        target_price=entry_price * (Decimal("1") + risk_fraction * reward),
        stop_price=entry_price * (Decimal("1") - risk_fraction * stop),
    )
    short_path = _label_indexed_direction(
        direction="SHORT",
        index=index,
        path_start=path_start,
        path_end=path_end,
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
        entry_delay_minutes=entry_delay_minutes,
        risk_pct=risk_pct,
        reward_multiple=reward_multiple,
        stop_multiple=stop_multiple,
        horizon_hours=horizon_hours,
        label=label,
        long=long_path,
        short=short_path,
    ).to_mapping()


def build_opportunity_episodes(
    labels: Sequence[Mapping[str, Any]],
    *,
    cadence_minutes: int = 5,
) -> list[dict[str, Any]]:
    if cadence_minutes <= 0:
        raise ValueError("cadence_minutes must be positive")
    ordered = sorted(labels, key=lambda row: _coerce_timestamp(row["decision_ts"]))
    episodes: list[dict[str, Any]] = []
    current: list[Mapping[str, Any]] = []

    for row in ordered:
        label = str(row.get("label") or "").upper()
        timestamp = _coerce_timestamp(row["decision_ts"])
        if label not in {"LONG", "SHORT"}:
            if current:
                episodes.append(_episode_row(current, number=len(episodes) + 1))
                current = []
            continue
        if current:
            previous_label = str(current[-1]["label"]).upper()
            previous_ts = _coerce_timestamp(current[-1]["decision_ts"])
            expected_ts = previous_ts + timedelta(minutes=cadence_minutes)
            if label != previous_label or timestamp != expected_ts:
                episodes.append(_episode_row(current, number=len(episodes) + 1))
                current = []
        current.append(row)

    if current:
        episodes.append(_episode_row(current, number=len(episodes) + 1))
    return episodes


def summarize_r_candidate(
    *,
    scenario_results: Mapping[tuple[int, float], Sequence[Mapping[str, Any]]],
    risk_pct: float,
    reward_multiple: float,
    stop_multiple: float,
    fee_bps_per_side: float,
    slippage_bps_per_side: float,
    primary_scenario: tuple[int, float],
) -> dict[str, Any]:
    if primary_scenario not in scenario_results:
        raise ValueError("primary_scenario is missing from scenario_results")
    if risk_pct <= 0:
        raise ValueError("risk_pct must be positive")

    scenario_summaries: list[dict[str, Any]] = []
    by_scenario: dict[tuple[int, float], dict[str, Any]] = {}
    for (entry_delay, horizon), labels in sorted(
        scenario_results.items(), key=lambda item: (item[0][1], item[0][0])
    ):
        stats = _scenario_summary(labels)
        row = {
            "entry_delay_minutes": entry_delay,
            "horizon_hours": float(horizon),
            **stats,
        }
        scenario_summaries.append(row)
        by_scenario[(entry_delay, float(horizon))] = row

    primary_key = (primary_scenario[0], float(primary_scenario[1]))
    primary = by_scenario[primary_key]
    primary_delay, primary_horizon = primary_key
    delay_sensitivity = [
        _with_retention(row, primary=primary)
        for key, row in by_scenario.items()
        if key[1] == primary_horizon and key[0] != primary_delay
    ]
    horizon_sensitivity = [
        _with_retention(row, primary=primary)
        for key, row in by_scenario.items()
        if key[0] == primary_delay and key[1] != primary_horizon
    ]

    round_trip_cost_pct = 2 * (fee_bps_per_side + slippage_bps_per_side) / 100
    cost_in_r = round_trip_cost_pct / risk_pct
    return {
        "risk_pct": risk_pct,
        "reward_multiple": reward_multiple,
        "stop_multiple": stop_multiple,
        "primary_scenario": {
            "entry_delay_minutes": primary_delay,
            "horizon_hours": primary_horizon,
        },
        "primary": _without_scenario_coordinates(primary),
        "scenarios": scenario_summaries,
        "delay_sensitivity": delay_sensitivity,
        "horizon_sensitivity": horizon_sensitivity,
        "cost": {
            "fee_bps_per_side": fee_bps_per_side,
            "slippage_bps_per_side": slippage_bps_per_side,
            "round_trip_cost_pct": round_trip_cost_pct,
            "cost_in_r": cost_in_r,
            "net_reward_r": reward_multiple - cost_in_r,
            "net_stop_r": -(stop_multiple + cost_in_r),
        },
    }


def run_training_atlas(
    *,
    candles: Sequence[MarketDataCandle],
    config: DiscoveryConfig,
) -> dict[str, Any]:
    index = _CandleIndex(candles)
    decisions = [
        timestamp
        for timestamp in index.timestamps
        if config.research_start <= timestamp <= config.research_end
    ]
    primary_scenario = (min(config.entry_delays_minutes), min(config.horizon_hours))
    all_labels: list[dict[str, Any]] = []
    all_episodes: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    purged_decisions: set[datetime] = set()

    for risk_pct in config.risk_values:
        scenarios: dict[tuple[int, float], list[dict[str, Any]]] = {}
        for horizon in config.horizon_hours:
            for entry_delay in config.entry_delays_minutes:
                labels: list[dict[str, Any]] = []
                for decision_ts in decisions:
                    horizon_end = decision_ts + timedelta(
                        minutes=entry_delay,
                        hours=horizon,
                    )
                    if horizon_end >= config.walk_forward_start:
                        purged_decisions.add(decision_ts)
                        continue
                    row = _label_fixed_r_from_index(
                        index=index,
                        decision_ts=decision_ts,
                        entry_delay_minutes=entry_delay,
                        risk_pct=risk_pct,
                        reward_multiple=config.reward_multiple,
                        stop_multiple=config.stop_multiple,
                        horizon_hours=horizon,
                    )
                    row["scenario_entry_delay_minutes"] = entry_delay
                    row["scenario_horizon_hours"] = horizon
                    labels.append(row)
                    all_labels.append(row)
                scenarios[(entry_delay, horizon)] = labels
                scenario_episodes = build_opportunity_episodes(labels)
                for episode in scenario_episodes:
                    episode["episode_id"] = f"episode-{len(all_episodes) + 1:06d}"
                    episode["risk_pct"] = risk_pct
                    episode["entry_delay_minutes"] = entry_delay
                    episode["horizon_hours"] = horizon
                    all_episodes.append(episode)

        summaries.append(
            summarize_r_candidate(
                scenario_results=scenarios,
                risk_pct=risk_pct,
                reward_multiple=config.reward_multiple,
                stop_multiple=config.stop_multiple,
                fee_bps_per_side=config.fee_bps_per_side,
                slippage_bps_per_side=config.slippage_bps_per_side,
                primary_scenario=primary_scenario,
            )
        )

    neighboring = []
    for lower, upper in zip(summaries, summaries[1:], strict=False):
        neighboring.append(
            {
                "lower_risk_pct": lower["risk_pct"],
                "upper_risk_pct": upper["risk_pct"],
                "primary_episode_count_delta": (
                    upper["primary"]["episode_count"] - lower["primary"]["episode_count"]
                ),
                "primary_qualifying_timestamp_count_delta": (
                    upper["primary"]["qualifying_timestamp_count"]
                    - lower["primary"]["qualifying_timestamp_count"]
                ),
            }
        )

    return {
        "timestamp_labels": all_labels,
        "episodes": all_episodes,
        "r_summaries": summaries,
        "neighboring_r_diagnostics": neighboring,
        "purged_decision_count": len(purged_decisions),
    }


def run_fixed_target_window(
    *,
    candles: Sequence[MarketDataCandle],
    window_start: datetime,
    window_end: datetime,
    selected_target: Mapping[str, Any],
) -> dict[str, Any]:
    start = _as_utc(window_start)
    end = _as_utc(window_end)
    if start > end:
        raise ValueError("fixed-target window_start must not follow window_end")
    risk_pct = float(selected_target["selected_risk_pct"])
    reward_multiple = float(selected_target["reward_multiple"])
    stop_multiple = float(selected_target["stop_multiple"])
    horizon_hours = float(selected_target["horizon_hours"])
    entry_delay_minutes = int(selected_target["entry_delay_minutes"])
    index = _CandleIndex(candles)
    decisions = [timestamp for timestamp in index.timestamps if start <= timestamp <= end]
    labels = [
        _label_fixed_r_from_index(
            index=index,
            decision_ts=decision_ts,
            entry_delay_minutes=entry_delay_minutes,
            risk_pct=risk_pct,
            reward_multiple=reward_multiple,
            stop_multiple=stop_multiple,
            horizon_hours=horizon_hours,
        )
        for decision_ts in decisions
    ]
    episodes = build_opportunity_episodes(labels)
    summary = summarize_r_candidate(
        scenario_results={(entry_delay_minutes, horizon_hours): labels},
        risk_pct=risk_pct,
        reward_multiple=reward_multiple,
        stop_multiple=stop_multiple,
        fee_bps_per_side=float(selected_target.get("fee_bps_per_side") or 0.0),
        slippage_bps_per_side=float(selected_target.get("slippage_bps_per_side") or 0.0),
        primary_scenario=(entry_delay_minutes, horizon_hours),
    )
    return {
        "timestamp_labels": labels,
        "episodes": episodes,
        "summary": summary,
    }


def _label_indexed_direction(
    *,
    direction: Direction,
    index: _CandleIndex,
    path_start: int,
    path_end: int,
    entry_price: Decimal,
    target_price: Decimal,
    stop_price: Decimal,
) -> FixedRPath:
    if direction == "LONG":
        target_index = index.first_high_at_least(path_start, path_end, target_price)
        stop_index = index.first_low_at_most(path_start, path_end, stop_price)
    else:
        target_index = index.first_low_at_most(path_start, path_end, target_price)
        stop_index = index.first_high_at_least(path_start, path_end, stop_price)
    outcome, first_touch_index = _resolve_barrier_order(
        target_index=target_index,
        stop_index=stop_index,
    )
    highest = max(entry_price, index.range_high(path_start, path_end))
    lowest = min(entry_price, index.range_low(path_start, path_end))
    terminal_close = Decimal(index.candles[path_end].close)
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
        first_touch_ts=(
            index.timestamps[first_touch_index] if first_touch_index is not None else None
        ),
        mfe_pct=float(max(Decimal("0"), mfe_pct)),
        mae_pct=float(max(Decimal("0"), mae_pct)),
        terminal_return_pct=float(terminal_return_pct),
    )


def _resolve_barrier_order(
    *,
    target_index: int | None,
    stop_index: int | None,
) -> tuple[PathOutcome, int | None]:
    if target_index is None and stop_index is None:
        return "TIMEOUT", None
    if target_index is not None and target_index == stop_index:
        return "AMBIGUOUS", target_index
    if stop_index is None or (target_index is not None and target_index < stop_index):
        return "TP", target_index
    return "SL", stop_index


def _aggregate_label(*, long_path: FixedRPath, short_path: FixedRPath) -> AggregateLabel:
    if "AMBIGUOUS" in {long_path.outcome, short_path.outcome}:
        return "AMBIGUOUS"
    if long_path.outcome == "TP":
        return "LONG"
    if short_path.outcome == "TP":
        return "SHORT"
    return "NEUTRAL"


def _episode_row(rows: Sequence[Mapping[str, Any]], *, number: int) -> dict[str, Any]:
    timestamps = [_coerce_timestamp(row["decision_ts"]) for row in rows]
    start_ts = timestamps[0]
    end_ts = timestamps[-1]
    return {
        "episode_id": f"episode-{number:06d}",
        "direction": str(rows[0]["label"]).upper(),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "timestamp_count": len(rows),
        "duration_minutes": int((end_ts - start_ts).total_seconds() // 60),
        "member_timestamps": timestamps,
    }


def _scenario_summary(labels: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    episodes = build_opportunity_episodes(labels)
    direction_counts = {
        direction: sum(1 for row in labels if str(row.get("label")).upper() == direction)
        for direction in ("LONG", "SHORT")
    }
    monthly_episode_counts: dict[str, int] = {}
    for episode in episodes:
        month = episode["start_ts"].strftime("%Y-%m")
        monthly_episode_counts[month] = monthly_episode_counts.get(month, 0) + 1
    return {
        "timestamp_count": len(labels),
        "qualifying_timestamp_count": sum(direction_counts.values()),
        "episode_count": len(episodes),
        "direction_counts": direction_counts,
        "monthly_episode_counts": monthly_episode_counts,
        "neutral_count": sum(
            1 for row in labels if str(row.get("label")).upper() == "NEUTRAL"
        ),
        "ambiguous_count": sum(
            1 for row in labels if str(row.get("label")).upper() == "AMBIGUOUS"
        ),
    }


def _with_retention(
    row: Mapping[str, Any],
    *,
    primary: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(row)
    result["qualifying_timestamp_retention"] = _ratio(
        row["qualifying_timestamp_count"], primary["qualifying_timestamp_count"]
    )
    result["episode_retention"] = _ratio(row["episode_count"], primary["episode_count"])
    return result


def _without_scenario_coordinates(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in row.items()
        if key not in {"entry_delay_minutes", "horizon_hours"}
    }


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _validate_label_parameters(
    *,
    risk_pct: float,
    reward_multiple: float,
    stop_multiple: float,
    horizon_hours: float,
    entry_delay_minutes: int,
) -> None:
    if risk_pct <= 0:
        raise ValueError("risk_pct must be positive")
    if reward_multiple <= 0:
        raise ValueError("reward_multiple must be positive")
    if stop_multiple <= 0:
        raise ValueError("stop_multiple must be positive")
    if horizon_hours <= 0:
        raise ValueError("horizon_hours must be positive")
    if entry_delay_minutes < 0:
        raise ValueError("entry_delay_minutes must be nonnegative")


def _percentage(numerator: Decimal, denominator: Decimal) -> Decimal:
    return numerator / denominator * Decimal("100")


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _coerce_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _as_utc(value)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return _as_utc(parsed)
    raise ValueError(f"invalid timestamp: {value!r}")
