from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
import hashlib
import json
import runpy

import numpy as np
import pandas as pd
import pytest
import torch


pytest.skip(
    "legacy session-local multiresolution experiment was discarded",
    allow_module_level=True,
)

from quant_terminal_sdk.engine_contracts import validate_signal_packet
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_strategies.btc_multires_opportunity_v1_base import decide
from quant_terminal_worker.signal_engines import btc_multires_opportunity_v1 as engine
from quant_terminal_worker.signal_engines.multires_tcn_runtime import load_model_artifact
from quant_terminal_worker.stage1.scoring import run_stage1a_training_score


SCRIPT = runpy.run_path(
    str(
        Path(__file__).resolve().parents[1]
        / "dev"
        / "signal_discovery_sessions"
        / "discovery-btc-2025-03-01-2026-05-30-mrk5l3hl"
        / "prompt"
        / "research_multires_tcn.py"
    )
)


def test_sequence_store_excludes_rows_unavailable_at_decision_time() -> None:
    channel_count = SCRIPT["BIN_CHANNEL_COUNT"]
    row_count = SCRIPT["SEQUENCE_BINS"] + 4
    available = np.arange(1, row_count + 1, dtype=np.int64) * 300_000_000_000
    values = np.repeat(
        np.arange(row_count, dtype=np.float32)[:, None],
        channel_count,
        axis=1,
    )
    decision_ns = int(available[SCRIPT["SEQUENCE_BINS"] - 1])
    store = SCRIPT["SequenceStore"](
        timeframe="5m",
        available_ns=available,
        binned_values=values,
        bin_bars=1,
    )

    baseline = store.tensor_at(decision_ns)
    mutated = values.copy()
    mutated[SCRIPT["SEQUENCE_BINS"] :] = 999_999.0
    mutated_store = SCRIPT["SequenceStore"](
        timeframe="5m",
        available_ns=available,
        binned_values=mutated,
        bin_bars=1,
    )

    assert baseline is not None
    assert baseline.shape == (channel_count, SCRIPT["SEQUENCE_BINS"])
    assert np.array_equal(baseline, mutated_store.tensor_at(decision_ns))
    assert np.all(baseline[:, -1] == SCRIPT["SEQUENCE_BINS"] - 1)


def test_hourly_aggregation_preserves_price_volume_and_oi_semantics() -> None:
    start = pd.Timestamp("2025-01-01T00:00:00Z")
    source = pd.DataFrame(
        {
            "open": np.arange(100.0, 112.0),
            "high": np.arange(101.0, 113.0),
            "low": np.arange(99.0, 111.0),
            "close": np.arange(100.5, 112.5),
            "vol_ccy_quote": np.arange(10.0, 22.0),
            "volume": np.arange(1.0, 13.0),
            "sum_open_interest": np.arange(1000.0, 1012.0),
            "sum_open_interest_value": np.arange(100_000.0, 100_012.0),
            "sum_toptrader_long_short_ratio": np.full(12, 1.1),
            "count_long_short_ratio": np.full(12, 1.2),
            "sum_taker_long_short_vol_ratio": np.full(12, 1.3),
        },
        index=pd.date_range(start, periods=12, freq="5min"),
    )

    result = SCRIPT["aggregate_resolution"](
        source,
        rule="1h",
        expected_rows=12,
    )

    assert len(result) == 1
    row = result.iloc[0]
    assert row["open"] == 100.0
    assert row["high"] == 112.0
    assert row["low"] == 99.0
    assert row["close"] == 111.5
    assert row["vol_ccy_quote"] == sum(np.arange(10.0, 22.0))
    assert row["oi_first"] == 1000.0
    assert row["sum_open_interest"] == 1011.0
    assert row["oi_min"] == 1000.0
    assert row["oi_max"] == 1011.0
    assert row["source_coverage"] == 1.0


def test_production_alignment_consumes_semantic_futures_metrics_columns() -> None:
    timestamp = datetime(2026, 7, 12, tzinfo=UTC)
    candles = [
        MarketDataCandle(
            timestamp=timestamp,
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=Decimal("10"),
            vol_ccy=Decimal("10"),
            vol_ccy_quote=Decimal("1005"),
            confirm=1,
        )
    ]
    metrics = [
        {
            "timestamp": "2026-07-12T00:00:00Z",
            "available_at": "2026-07-12T00:05:00Z",
            "sum_open_interest": 101328.727,
            "sum_open_interest_value": 6470842373.3473,
            "top_trader_position_long_short_ratio": 1.378361,
            "global_account_long_short_ratio": 1.25625663,
            "taker_buy_sell_volume_ratio": 1.346818,
            "complete": True,
            "confirm": 1,
        }
    ]

    frame = engine.align_market_rows(raw_5m=candles, raw_oi=metrics)

    assert frame.iloc[0]["sum_toptrader_long_short_ratio"] == pytest.approx(1.378361)
    assert frame.iloc[0]["count_long_short_ratio"] == pytest.approx(1.25625663)
    assert frame.iloc[0]["sum_taker_long_short_vol_ratio"] == pytest.approx(1.346818)


def test_incomplete_higher_timeframe_bucket_is_explicitly_marked() -> None:
    start = pd.Timestamp("2025-01-01T00:00:00Z")
    rows = 13
    source = pd.DataFrame(
        {
            "open": np.full(rows, 100.0),
            "high": np.full(rows, 101.0),
            "low": np.full(rows, 99.0),
            "close": np.full(rows, 100.0),
            "vol_ccy_quote": np.full(rows, 10.0),
            "volume": np.full(rows, 1.0),
            "sum_open_interest": np.full(rows, 1000.0),
            "sum_open_interest_value": np.full(rows, 100_000.0),
            "sum_toptrader_long_short_ratio": np.full(rows, 1.1),
            "count_long_short_ratio": np.full(rows, 1.2),
            "sum_taker_long_short_vol_ratio": np.full(rows, 1.3),
        },
        index=pd.date_range(start, periods=rows, freq="5min"),
    )

    result = SCRIPT["aggregate_resolution"](
        source,
        rule="1h",
        expected_rows=12,
    )

    assert result["source_coverage"].tolist() == [1.0, 1.0 / 12.0]


def test_multiresolution_model_has_stable_output_shape_and_eval_result() -> None:
    torch.manual_seed(7)
    model = SCRIPT["MultiResolutionTCN"]().eval()
    branches = [
        torch.randn(2, SCRIPT["BIN_CHANNEL_COUNT"], SCRIPT["SEQUENCE_BINS"])
        for _ in SCRIPT["BRANCH_CONFIG"]
    ]

    with torch.inference_mode():
        first = model(branches)
        second = model(branches)

    assert first.shape == (2,)
    assert torch.equal(first, second)


def test_research_and_production_tensor_builders_are_identical() -> None:
    frame = _production_frame(periods=3000)

    research_stores, _ = SCRIPT["build_sequence_stores"](
        frame.reset_index()
    )
    production_stores = engine.build_sequence_stores(frame)

    assert research_stores.keys() == production_stores.keys()
    for timeframe in research_stores:
        research = research_stores[timeframe]
        production = production_stores[timeframe]
        np.testing.assert_array_equal(research.available_ns, production.available_ns)
        np.testing.assert_array_equal(research.binned_values, production.binned_values)
        assert research.bin_bars == production.bin_bars


def test_capped_proportional_sampling_scales_with_episode_length() -> None:
    short = SCRIPT["Episode"](
        target=1,
        direction=1,
        timestamps_ns=np.arange(12, dtype=np.int64),
    )
    medium = SCRIPT["Episode"](
        target=1,
        direction=1,
        timestamps_ns=np.arange(120, dtype=np.int64),
    )
    very_long = SCRIPT["Episode"](
        target=1,
        direction=1,
        timestamps_ns=np.arange(1000, dtype=np.int64),
    )
    dataset = SCRIPT["EpisodeDataset"](
        episodes=[short, medium, very_long],
        stores={},
        seed=1,
        weighting_mode="capped_proportional",
    )

    assert len(dataset) == 1 + 10 + 48
    assert SCRIPT["episode_training_mass"](
        very_long,
        weighting_mode="capped_proportional",
    ) == 576


def test_signal_objective_accepts_repeated_signals_inside_one_bracket() -> None:
    member_timestamps = pd.date_range("2025-01-01T00:00:00Z", periods=3, freq="5min")
    brackets = pd.DataFrame({"member_timestamps": [member_timestamps.to_pydatetime()]})
    outside = pd.Timestamp("2025-01-01T01:00:00Z")
    emissions = pd.DataFrame(
        {
            "decision_ts": [member_timestamps[0], member_timestamps[1], outside],
            "timestamp": [
                member_timestamps[0] - pd.Timedelta(minutes=5),
                member_timestamps[1] - pd.Timedelta(minutes=5),
                outside - pd.Timedelta(minutes=5),
            ],
            "score": [0.9, 0.8, 0.7],
        }
    )
    hard_negatives = pd.DataFrame({"decision_ts": [outside]})
    tabular = runpy.run_path(str(SCRIPT["TABULAR_RESEARCH_PATH"]))

    result = SCRIPT["score_signal_stream"](
        tabular=tabular,
        threshold=0.5,
        emissions=emissions,
        brackets=brackets,
        hard_negatives=hard_negatives,
    )

    assert result.raw_signal_count == 3
    assert result.in_bracket_signal_count == 2
    assert result.signal_precision == pytest.approx(2 / 3, abs=1e-6)
    assert result.timestamp_coverage == pytest.approx(2 / 3, abs=1e-6)
    assert result.matched_brackets == 1
    assert result.distinct_bracket_coverage == 1.0
    assert result.signals_per_matched_bracket == 2.0
    assert result.hard_negative_rate == pytest.approx(1 / 3, abs=1e-6)


def test_production_model_artifact_has_expected_hash_and_threshold() -> None:
    path = Path("artifacts/signal_engine/models/btc_multires_opportunity_v1.pt").resolve()

    model, artifact = load_model_artifact(path)

    assert hashlib.sha256(path.read_bytes()).hexdigest() == engine.MODEL_SHA256
    assert artifact["engine_id"] == engine.ENGINE_ID
    assert artifact["score_threshold"] == pytest.approx(0.684161)
    assert model.training is False


def test_packet_is_neutral_causal_and_strategy_compatible() -> None:
    frame = _production_frame(periods=300)
    index = 290

    packet = engine.build_packet(
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        frame=frame,
        index=index,
        context_bars=24,
    )
    mutated = frame.copy()
    mutated.loc[mutated.index[index + 1] :, ["close", "sum_open_interest"]] = 999_999_999.0
    mutated_packet = engine.build_packet(
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        frame=mutated,
        index=index,
        context_bars=24,
    )

    validate_signal_packet(packet)
    assert packet == mutated_packet
    assert "direction" not in packet
    assert "confidence" not in packet
    assert "score" not in packet["evidence"]
    decision = decide(
        {
            "signal": {
                "signal_id": "synthetic",
                "payload_schema": "signal_packet.v2",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
            "raw_data": {},
        }
    )
    assert decision["action"] == "ENTER"
    assert decision["direction"] in {"LONG", "SHORT"}


def test_generation_preserves_fixed_cadence_and_extension_seed(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = _production_frame(periods=3, freq="1h")
    frame.index = pd.DatetimeIndex(
        [
            pd.Timestamp("2025-01-01T00:00:00Z"),
            pd.Timestamp("2025-01-01T01:00:00Z"),
            pd.Timestamp("2025-01-01T09:00:00Z"),
        ],
        name="timestamp",
    )
    monkeypatch.setattr(engine, "align_market_rows", lambda **_: frame)
    monkeypatch.setattr(engine, "build_sequence_stores", lambda _: {})
    monkeypatch.setattr(engine, "ready_decision_indices", lambda **_: np.asarray([0, 1, 2]))
    monkeypatch.setattr(engine, "_load_verified_model", lambda _: (object(), {"score_threshold": 0.5}))
    monkeypatch.setattr(
        engine,
        "score_decisions",
        lambda **_: np.asarray([0.9, 0.9, 0.9], dtype=np.float32),
    )

    packets, count, _ = engine.generate_multires_packets(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=[],
        raw_oi=[],
        start=frame.index[0].to_pydatetime(),
        end=frame.index[-1].to_pydatetime(),
        parameters={"dedupe_window_minutes": 480},
    )
    extended, extended_count, _ = engine.generate_multires_packets(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=[],
        raw_oi=[],
        start=frame.index[0].to_pydatetime(),
        end=frame.index[-1].to_pydatetime(),
        parameters={
            "dedupe_window_minutes": 480,
            "_dedupe_seed_timestamp": "2025-01-01T00:00:00Z",
        },
    )

    assert count == 2
    assert [packet["timestamp"] for packet in packets] == [
        "2025-01-01T00:00:00Z",
        "2025-01-01T09:00:00Z",
    ]
    assert extended_count == 1
    assert [packet["timestamp"] for packet in extended] == ["2025-01-01T09:00:00Z"]


def test_training_and_live_use_identical_packet_builder(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    frame = _production_frame(periods=3, freq="1h")
    latest = len(frame) - 1
    monkeypatch.setattr(engine, "align_market_rows", lambda **_: frame)
    monkeypatch.setattr(engine, "build_sequence_stores", lambda _: {})
    monkeypatch.setattr(
        engine,
        "ready_decision_indices",
        lambda **_: np.asarray([latest]),
    )
    monkeypatch.setattr(
        engine,
        "_load_verified_model",
        lambda _: (object(), {"score_threshold": 0.5}),
    )
    monkeypatch.setattr(
        engine,
        "score_decisions",
        lambda **_: np.asarray([0.9], dtype=np.float32),
    )

    training_packets, count, _ = engine.generate_multires_packets(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=[],
        raw_oi=[],
        start=frame.index[latest].to_pydatetime(),
        end=frame.index[latest].to_pydatetime(),
        parameters={"dedupe_window_minutes": 480, "context_bars": 24},
    )
    live_packet = engine.scan_multires_latest(
        workspace_root=Path.cwd(),
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=[],
        raw_oi=[],
        parameters={"context_bars": 24},
    )

    assert count == 1
    assert live_packet == training_packets[0]


def test_stage1_scorer_consumes_raw_emitted_packet(tmp_path: Path) -> None:
    packet = engine.build_packet(
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        frame=_production_frame(periods=300),
        index=290,
        context_bars=24,
    )
    expected_direction = decide(
        {
            "signal": {
                "signal_id": "expected",
                "payload_schema": "signal_packet.v2",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
            "raw_data": {},
        }
    )["direction"]

    iteration_root = tmp_path / "iteration"
    packet_root = tmp_path / "packets"
    strategy_root = iteration_root / "strategy_module"
    for path in (
        packet_root,
        strategy_root,
        iteration_root / "decisions",
        iteration_root / "scores",
        iteration_root / "summaries",
    ):
        path.mkdir(parents=True)
    (strategy_root / "__init__.py").write_text("")
    strategy_path = Path(
        "packages/strategy_modules/src/quant_terminal_strategies/"
        "btc_multires_opportunity_v1_base.py"
    )
    (strategy_root / "strategy.py").write_text(strategy_path.read_text())
    packet_path = packet_root / "btc.json"
    packet_path.write_text(json.dumps(packet))
    signal_id = f"{engine.ENGINE_ID}:BTC:representative"
    (iteration_root / "signal_sample.json").write_text(
        json.dumps(
            {"signals": [{"signal_id": signal_id, "packet_path": str(packet_path)}]}
        )
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "signal_id": signal_id,
                        "ground_truth": {"natural_direction": expected_direction},
                    }
                ]
            }
        )
    )

    result = run_stage1a_training_score(iteration_root=iteration_root)

    assert result["metrics"]["matches"] == 1
    decisions = json.loads(
        (iteration_root / "decisions/stage1a_directional_decisions.json").read_text()
    )
    assert decisions["decisions"][0]["action"] == "ENTER"
    assert decisions["decisions"][0]["direction"] == expected_direction


def _production_frame(*, periods: int, freq: str = "5min") -> pd.DataFrame:
    index = pd.date_range("2025-01-01T00:00:00Z", periods=periods, freq=freq, name="timestamp")
    values = np.arange(periods, dtype=float)
    close = 100.0 + values * 0.1
    return pd.DataFrame(
        {
            "open": close - 0.05,
            "high": close + 0.2,
            "low": close - 0.2,
            "close": close,
            "volume": 10.0 + values,
            "vol_ccy_quote": 1000.0 + values * 10.0,
            "sum_open_interest": 10_000.0 + values,
            "sum_open_interest_value": 1_000_000.0 + values * 100.0,
            "sum_toptrader_long_short_ratio": np.full(periods, 1.1),
            "count_long_short_ratio": np.full(periods, 1.0),
            "sum_taker_long_short_vol_ratio": np.full(periods, 1.05),
            "source_coverage": np.ones(periods),
        },
        index=index,
    )
