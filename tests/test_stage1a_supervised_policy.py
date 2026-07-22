from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta

import pytest

from quant_terminal_strategies.stage1a_supervised_features import (
    DEFAULT_ACTIVE_FEATURE_NAMES,
    OI_FEATURE_NAMES,
    STAGE1A_CONTEXT_FEATURE_NAMES,
    extract_packet_features,
    extract_signal_features,
    observed_feature_names,
)
from quant_terminal_strategies.stage1a_supervised_runtime import (
    decide_with_artifact,
    score_two_head_artifact,
)


def test_feature_extractor_is_point_in_time_safe_and_shared_by_signal_runtime() -> None:
    packet = _packet()
    expected = extract_packet_features(packet)
    wrapped = {"signal_id": "signal-1", "payload": packet}

    assert extract_signal_features(wrapped) == expected
    assert expected["2h_forming_return_pct"] == pytest.approx(2.0)
    assert expected["bb_forming_position_pct"] == pytest.approx(82.0)

    mutated = deepcopy(packet)
    future = datetime(2026, 1, 2, 2, 5, tzinfo=UTC)
    for timeframe in ("5m", "2h", "8h", "12h"):
        mutated["charts"][timeframe]["candles"].append(
            _candle_row(
                future,
                open_=100,
                high=1000,
                low=1,
                close=999,
                completed=True,
                available_at=future + timedelta(minutes=5),
            )
        )
    mutated["charts"]["bollinger_1d"]["rows"].append(
        [
            future.isoformat().replace("+00:00", "Z"),
            (future + timedelta(days=1)).isoformat().replace("+00:00", "Z"),
            True,
            288,
            999,
            100,
            120,
            80,
            999,
            999,
            999,
        ]
    )

    assert extract_packet_features(mutated) == expected


def test_oi_is_preserved_in_feature_plane_but_not_active_initially() -> None:
    features = extract_packet_features(_packet())

    assert set(OI_FEATURE_NAMES).isdisjoint(DEFAULT_ACTIVE_FEATURE_NAMES)
    assert features["oi_return_pct_2h"] == pytest.approx(0.4)
    assert features["oi_return_pct_8h"] == pytest.approx(-1.2)
    assert features["oi_change_2h_zscore_7d"] == pytest.approx(0.6)


def test_stage1a_context_v2_is_observed_but_does_not_change_existing_active_features() -> None:
    packet = _packet()
    features = extract_packet_features(packet)

    assert features["5m_return_48_pct"] == pytest.approx(1.25)
    assert features["2h_ema_36_slope_3_pct"] == pytest.approx(-0.4)
    assert set(STAGE1A_CONTEXT_FEATURE_NAMES) <= set(observed_feature_names())
    assert set(STAGE1A_CONTEXT_FEATURE_NAMES).isdisjoint(DEFAULT_ACTIVE_FEATURE_NAMES)

    unavailable = deepcopy(packet)
    block = unavailable["evidence"]["derived_features"]["stage1a_context_v2"]
    block["available_at"] = "2026-01-02T00:10:00Z"
    unavailable_features = extract_packet_features(unavailable)
    assert all(unavailable_features[name] is None for name in STAGE1A_CONTEXT_FEATURE_NAMES)

    wrong_schema = deepcopy(packet)
    wrong_schema["evidence"]["derived_features"]["stage1a_context_v2"]["schema_version"] = "unknown"
    wrong_schema_features = extract_packet_features(wrong_schema)
    assert all(wrong_schema_features[name] is None for name in STAGE1A_CONTEXT_FEATURE_NAMES)

    artifact = _artifact(active_feature_names=["2h_forming_return_pct"])
    with_context = decide_with_artifact(
        {"signal": {"signal_id": "signal-context", "payload": packet}},
        artifact=artifact,
        strategy_id="test_stage1a_supervised",
        strategy_version="v1",
    )
    without_context_packet = deepcopy(packet)
    without_context_packet["evidence"]["derived_features"].pop("stage1a_context_v2")
    without_context = decide_with_artifact(
        {"signal": {"signal_id": "signal-context", "payload": without_context_packet}},
        artifact=artifact,
        strategy_id="test_stage1a_supervised",
        strategy_version="v1",
    )
    assert (with_context["action"], with_context["direction"], with_context["confidence"]) == (
        without_context["action"],
        without_context["direction"],
        without_context["confidence"],
    )


def test_two_head_runtime_scores_exported_logistic_artifact_without_sklearn() -> None:
    artifact = _artifact()

    long_score = score_two_head_artifact({"feature": 1.0}, artifact)
    short_score = score_two_head_artifact({"feature": -1.0}, artifact)

    assert long_score["p_enter"] == pytest.approx(0.8807970779)
    assert long_score["p_long_given_enter"] == pytest.approx(0.8807970779)
    assert long_score["direction"] == "LONG"
    assert short_score["direction"] == "SHORT"
    assert long_score["missing_active_features"] == []


def test_supervised_decision_preserves_live_contract_and_diagnostics() -> None:
    packet = _packet()
    artifact = _artifact(active_feature_names=["2h_forming_return_pct"])
    decision = decide_with_artifact(
        {"signal": {"signal_id": "signal-1", "payload": packet}, "runtime_mode": "live"},
        artifact=artifact,
        strategy_id="test_stage1a_supervised",
        strategy_version="v1",
    )

    assert set(decision) == {
        "decision_id",
        "strategy_id",
        "strategy_version",
        "signal_id",
        "trade_action",
        "action",
        "direction",
        "confidence",
        "reason_code",
        "execution_profile",
        "diagnostics",
    }
    assert decision["trade_action"] == decision["action"] == "ENTER"
    assert decision["direction"] == "LONG"
    assert decision["diagnostics"]["model_version"] == "test-v1"
    assert decision["diagnostics"]["runtime_mode"] == "live"
    assert decision["diagnostics"]["features_used_by_model"] == ["2h_forming_return_pct"]
    assert decision["diagnostics"]["oi_features_snapshot"]["oi_return_pct_2h"] == pytest.approx(0.4)
    assert "oi_return_pct_2h" in decision["diagnostics"]["features_observed_not_used"]


def test_supervised_decision_skips_when_active_feature_missingness_exceeds_limit() -> None:
    artifact = _artifact(active_feature_names=["missing_a", "missing_b"])
    decision = decide_with_artifact(
        {"signal": {"signal_id": "signal-2", "payload": _packet()}},
        artifact=artifact,
        strategy_id="test_stage1a_supervised",
        strategy_version="v1",
    )

    assert decision["trade_action"] == decision["action"] == "SKIP"
    assert decision["direction"] == "FLAT"
    assert decision["reason_code"] == "supervised_active_features_missing"


def _artifact(*, active_feature_names: list[str] | None = None) -> dict[str, object]:
    names = active_feature_names or ["feature"]
    head = {
        "intercept": 0.0,
        "coefficients": [2.0] * len(names),
        "imputation_values": [0.0] * len(names),
        "means": [0.0] * len(names),
        "scales": [1.0] * len(names),
    }
    return {
        "schema_version": "stage1a_supervised_model.v1",
        "model_id": "test-model",
        "model_version": "test-v1",
        "feature_spec_version": "vegas_5m_cluster_v6.stage1a_features.v1",
        "model_family": "two_head_logistic",
        "active_feature_names": names,
        "observed_feature_names": list(names) + list(OI_FEATURE_NAMES),
        "heads": {"enter": head, "direction": head},
        "thresholds": {"enter_threshold": 0.3, "max_missing_fraction": 0.2},
    }


def _packet() -> dict[str, object]:
    as_of = datetime(2026, 1, 2, 0, 5, tzinfo=UTC)
    charts = {
        timeframe: _chart(as_of, timeframe=timeframe)
        for timeframe in ("5m", "2h", "8h", "12h")
    }
    charts["bollinger_1d"] = {
        "timeframe": "1d",
        "columns": [
            "open_ts",
            "available_at",
            "complete",
            "source_candle_count",
            "close",
            "bb_mid_20",
            "bb_upper_20_2",
            "bb_lower_20_2",
            "bb_position_pct",
            "bb_bandwidth_pct",
            "bb_zscore",
        ],
        "rows": [
            ["2025-12-31T00:00:00Z", "2026-01-01T00:00:00Z", True, 288, 100, 100, 110, 90, 50, 20, 0],
            ["2026-01-01T00:00:00Z", "2026-01-02T00:05:00Z", False, 1, 106, 100, 110, 90, 82, 20, 1.2],
        ],
    }
    return {
        "schema_version": "signal_packet.v2",
        "asset": "BTC",
        "timestamp": as_of.isoformat().replace("+00:00", "Z"),
        "charts": charts,
        "evidence": {
            "matched_ema_count": 3,
            "matched_periods": [36, 43, 576],
            "derived_features": {
                "stage1a_context_v2": {
                    "schema_version": "vegas_5m_cluster_v6.stage1a_context.v2",
                    "available_at": "2026-01-02T00:05:00Z",
                    "complete": True,
                    "source_windows": {},
                    "values": {
                        "5m_return_48_pct": "1.25",
                        "2h_ema_36_slope_3_pct": "-0.4",
                    },
                },
                "open_interest_regime": {
                    "available_at": "2026-01-02T00:00:00Z",
                    "values": {
                        "oi_return_pct_2h": "0.4",
                        "oi_return_pct_8h": "-1.2",
                        "oi_return_pct_24h": "2.1",
                        "oi_change_2h_zscore_7d": "0.6",
                        "general_long_short_ratio": "1.1",
                        "taker_long_short_ratio_avg_2h": "0.9",
                    },
                }
            },
        },
    }


def _chart(as_of: datetime, *, timeframe: str) -> dict[str, object]:
    return {
        "timeframe": timeframe,
        "columns": [
            "ts",
            "open",
            "high",
            "low",
            "close",
            "is_completed",
            "source_candle_count",
            "partial_close_timestamp",
            "expected_close_timestamp",
        ],
        "candles": [
            _candle_row(as_of - timedelta(hours=4), open_=98, high=101, low=97, close=100, completed=True, available_at=as_of - timedelta(hours=2)),
            _candle_row(as_of - timedelta(hours=2), open_=100, high=103, low=99, close=102, completed=True, available_at=as_of),
            _candle_row(as_of, open_=102, high=105, low=101, close=104.04, completed=False, available_at=as_of),
        ],
        "ema_values": {"36": "103", "43": "102", "144": "100", "169": "99", "576": "95", "676": "94"},
        "ema_distances": {"36": "0.01", "43": "0.02", "144": "0.03", "169": "0.04", "576": "0.05", "676": "0.06"},
    }


def _candle_row(
    ts: datetime,
    *,
    open_: float,
    high: float,
    low: float,
    close: float,
    completed: bool,
    available_at: datetime,
) -> list[object]:
    timestamp = ts.isoformat().replace("+00:00", "Z")
    available = available_at.isoformat().replace("+00:00", "Z")
    return [timestamp, open_, high, low, close, completed, 6, available, available]
