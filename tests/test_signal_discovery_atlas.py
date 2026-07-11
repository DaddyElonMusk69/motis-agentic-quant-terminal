from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_discovery.atlas import (
    DiscoveryConfig,
    build_opportunity_episodes,
    label_fixed_r_timestamp,
    run_training_atlas,
    summarize_r_candidate,
)


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


def test_opportunity_episodes_require_adjacent_same_direction_rows() -> None:
    labels = [
        _label_row("2026-01-01T00:00:00Z", "LONG"),
        _label_row("2026-01-01T00:05:00Z", "LONG"),
        _label_row("2026-01-01T00:10:00Z", "NEUTRAL"),
        _label_row("2026-01-01T00:15:00Z", "LONG"),
        _label_row("2026-01-01T00:20:00Z", "SHORT"),
        _label_row("2026-01-01T00:30:00Z", "SHORT"),
    ]

    episodes = build_opportunity_episodes(labels)

    assert [(row["direction"], row["timestamp_count"]) for row in episodes] == [
        ("LONG", 2),
        ("LONG", 1),
        ("SHORT", 1),
        ("SHORT", 1),
    ]
    assert episodes[0]["start_ts"] == _ts("2026-01-01T00:00:00Z")
    assert episodes[0]["end_ts"] == _ts("2026-01-01T00:05:00Z")
    assert episodes[0]["duration_minutes"] == 5
    assert episodes[0]["episode_id"] == "episode-000001"


def test_r_feasibility_summary_reports_episodes_sensitivity_and_cost() -> None:
    primary = [
        _label_row("2026-01-01T00:00:00Z", "LONG"),
        _label_row("2026-01-01T00:05:00Z", "LONG"),
        _label_row("2026-01-01T00:10:00Z", "NEUTRAL"),
        _label_row("2026-01-01T00:15:00Z", "SHORT"),
        _label_row("2026-02-01T00:00:00Z", "AMBIGUOUS"),
    ]
    delayed = [
        _label_row("2026-01-01T00:00:00Z", "LONG"),
        _label_row("2026-01-01T00:05:00Z", "NEUTRAL"),
        _label_row("2026-01-01T00:10:00Z", "NEUTRAL"),
        _label_row("2026-01-01T00:15:00Z", "NEUTRAL"),
        _label_row("2026-02-01T00:00:00Z", "AMBIGUOUS"),
    ]
    extended = [
        *primary,
        _label_row("2026-02-01T00:05:00Z", "LONG"),
    ]

    summary = summarize_r_candidate(
        scenario_results={(5, 36.0): primary, (10, 36.0): delayed, (5, 48.0): extended},
        risk_pct=1.0,
        reward_multiple=2.0,
        stop_multiple=1.0,
        fee_bps_per_side=5.0,
        slippage_bps_per_side=5.0,
        primary_scenario=(5, 36.0),
    )

    assert summary["primary"]["qualifying_timestamp_count"] == 3
    assert summary["primary"]["episode_count"] == 2
    assert summary["primary"]["direction_counts"] == {"LONG": 2, "SHORT": 1}
    assert summary["primary"]["monthly_episode_counts"] == {"2026-01": 2}
    assert summary["primary"]["ambiguous_count"] == 1
    assert summary["delay_sensitivity"][0]["entry_delay_minutes"] == 10
    assert summary["delay_sensitivity"][0]["qualifying_timestamp_retention"] == pytest.approx(
        1 / 3
    )
    assert summary["horizon_sensitivity"][0]["horizon_hours"] == 48.0
    assert summary["horizon_sensitivity"][0]["episode_count"] == 3
    assert summary["cost"]["round_trip_cost_pct"] == pytest.approx(0.2)
    assert summary["cost"]["cost_in_r"] == pytest.approx(0.2)
    assert summary["cost"]["net_reward_r"] == pytest.approx(1.8)
    assert summary["cost"]["net_stop_r"] == pytest.approx(-1.2)


def test_training_atlas_returns_the_full_r_frontier_without_selecting_a_winner() -> None:
    config = DiscoveryConfig(
        risk_values=(1.0, 1.5),
        reward_multiple=2.0,
        stop_multiple=1.0,
        horizon_hours=(1.0, 2.0),
        entry_delays_minutes=(5, 10),
        fee_bps_per_side=5.0,
        slippage_bps_per_side=5.0,
        research_start=_ts("2026-01-01T00:00:00Z"),
        research_end=_ts("2026-01-01T00:10:00Z"),
        walk_forward_start=_ts("2026-01-02T00:00:00Z"),
    )

    atlas = run_training_atlas(candles=_flat_candles(hours=3), config=config)

    assert [row["risk_pct"] for row in atlas["r_summaries"]] == [1.0, 1.5]
    assert len(atlas["timestamp_labels"]) == 24
    assert all(
        row["decision_ts"] < _ts("2026-01-02T00:00:00Z")
        for row in atlas["timestamp_labels"]
    )
    assert atlas["episodes"] == []
    assert atlas["neighboring_r_diagnostics"] == [
        {
            "lower_risk_pct": 1.0,
            "upper_risk_pct": 1.5,
            "primary_episode_count_delta": 0,
            "primary_qualifying_timestamp_count_delta": 0,
        }
    ]
    assert "selected_risk_pct" not in atlas


def test_discovery_config_rejects_training_rows_that_reach_walk_forward() -> None:
    with pytest.raises(ValueError, match="strictly before walk_forward_start"):
        DiscoveryConfig(
            risk_values=(1.0,),
            research_start=_ts("2026-01-01T00:00:00Z"),
            research_end=_ts("2026-01-01T00:15:00Z"),
            walk_forward_start=_ts("2026-01-01T00:15:00Z"),
        )


def test_training_atlas_embargoes_labels_whose_horizon_crosses_walk_forward() -> None:
    config = DiscoveryConfig(
        risk_values=(1.0,),
        horizon_hours=(1.0,),
        entry_delays_minutes=(5,),
        research_start=_ts("2026-01-01T00:00:00Z"),
        research_end=_ts("2026-01-01T00:10:00Z"),
        walk_forward_start=_ts("2026-01-01T01:11:00Z"),
    )

    atlas = run_training_atlas(candles=_flat_candles(hours=2), config=config)

    assert [row["decision_ts"] for row in atlas["timestamp_labels"]] == [
        _ts("2026-01-01T00:00:00Z"),
        _ts("2026-01-01T00:05:00Z"),
    ]
    assert atlas["purged_decision_count"] == 1


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


def _flat_candles(*, hours: int) -> list[MarketDataCandle]:
    start = _ts("2026-01-01T00:00:00Z")
    return [
        _candle(
            start + timedelta(minutes=minute),
            open_value="100",
            high="100.5",
            low="99.5",
            close="100",
        )
        for minute in range(0, hours * 60 + 5, 5)
    ]


def _label_row(timestamp: str, label: str) -> dict[str, object]:
    return {"decision_ts": _ts(timestamp), "label": label}


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
