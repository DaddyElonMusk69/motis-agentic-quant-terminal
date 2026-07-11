from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import importlib
import json
from pathlib import Path
import subprocess
import sys

from quant_terminal_sdk.engine_contracts import (
    validate_signal_engine_spec,
    validate_signal_packet,
    validate_strategy_module,
)
from quant_terminal_sdk.market_data_reader import MarketDataCandle
from quant_terminal_worker.signal_engines.runtime import (
    EngineLiveScanContext,
    EngineTrainingContext,
    resolve_signal_engine,
)
from quant_terminal_worker.stage1.scoring import run_stage1a_training_score


ENGINE_ID = "compression_participation_release_v1"
FOUR_HOUR_ENGINE_ID = "compression_participation_release_4h_v1"
STRATEGY_PATH = (
    "packages/strategy_modules/src/quant_terminal_strategies/"
    "compression_participation_release_v1_base.py"
)
TEST_PARAMETERS = {
    "compression_window_bars": 24,
    "stats_lookback_bars": 160,
    "min_stats_bars": 100,
    "compression_percentile_threshold": "0.35",
    "volume_zscore_threshold": "1.0",
    "range_zscore_threshold": "0.75",
    "dedupe_window_minutes": 480,
    "context_bars": 48,
    "context_timeframes": ["2h", "4h", "8h"],
}


def _engine():
    return importlib.import_module(
        "quant_terminal_worker.signal_engines.compression_participation_release_v1"
    )


def _four_hour_engine():
    return importlib.import_module(
        "quant_terminal_worker.signal_engines.compression_participation_release_4h_v1"
    )


def test_compression_participation_registry_and_strategy_validate() -> None:
    validate_signal_engine_spec(ENGINE_ID)
    validate_strategy_module(STRATEGY_PATH)


def test_four_hour_registry_declares_immutable_cadence() -> None:
    validate_signal_engine_spec(FOUR_HOUR_ENGINE_ID)
    resolved = resolve_signal_engine(
        FOUR_HOUR_ENGINE_ID,
        repository=_Repository(),
        workspace_root=Path.cwd(),
    )

    defaults = resolved.spec.configuration_schema["default_parameters"]
    fixed = resolved.spec.configuration_schema["fixed_parameters"]
    assert defaults["dedupe_window_minutes"] == 240
    assert fixed == {"dedupe_window_minutes": 240}


def test_four_hour_adapter_forces_cadence_and_preserves_runtime_packet_parity(
    tmp_path: Path,
) -> None:
    engine = _four_hour_engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp
    reader = _Reader(candles=candles, oi_rows=oi_rows)
    repository = _Repository()
    resolved = resolve_signal_engine(
        FOUR_HOUR_ENGINE_ID,
        repository=repository,
        workspace_root=Path.cwd(),
    )
    caller_parameters = {**TEST_PARAMETERS, "dedupe_window_minutes": 0}

    assert engine._enforced_parameters(caller_parameters)["dedupe_window_minutes"] == 240
    assert engine._enforced_parameters(
        {**caller_parameters, "dedupe_window_minutes": 480}
    )["dedupe_window_minutes"] == 240

    training = resolved.generate_training_signals(
        EngineTrainingContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            signal_set={},
            signal_set_key=f"{FOUR_HOUR_ENGINE_ID}:BTC:canonical",
            parameters=caller_parameters,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=tmp_path,
            repository=repository,
            start=latest_ts,
            end=latest_ts,
            raw_candle_end=latest_ts,
        )
    )
    live = resolved.scan_live_signal(
        EngineLiveScanContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            route={},
            parameters={**caller_parameters, "dedupe_window_minutes": 480},
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=tmp_path,
            repository=repository,
        )
    )

    assert training.result.generated_packet_count == 1
    assert live.status == "fresh_signal"
    assert live.signal is not None
    packet = training.packets[0]
    validate_signal_packet(packet)
    assert packet["evidence"]["engine"] == FOUR_HOUR_ENGINE_ID
    assert packet["evidence"]["dedupe_window_minutes"] == 240
    assert live.signal.evidence == packet["evidence"]


def test_four_hour_adapter_enforces_seed_boundary(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp
    repository = _Repository()
    resolved = resolve_signal_engine(
        FOUR_HOUR_ENGINE_ID,
        repository=repository,
        workspace_root=Path.cwd(),
    )

    def generated_count(seed_delta: timedelta) -> int:
        output = resolved.generate_training_signals(
            EngineTrainingContext(
                asset="BTC",
                instrument="BTC-USDT-SWAP",
                signal_set={},
                signal_set_key=f"{FOUR_HOUR_ENGINE_ID}:BTC:canonical",
                parameters={
                    **TEST_PARAMETERS,
                    "dedupe_window_minutes": 0,
                    "_dedupe_seed_timestamp": (latest_ts - seed_delta).isoformat(),
                },
                market_data_reader=_Reader(candles=candles, oi_rows=oi_rows),
                spec=resolved.spec,
                workspace_root=tmp_path,
                repository=repository,
                start=latest_ts,
                end=latest_ts,
                raw_candle_end=latest_ts,
            )
        )
        return output.result.generated_packet_count

    assert generated_count(timedelta(hours=3)) == 0
    assert generated_count(timedelta(hours=4)) == 1


def test_four_hour_packet_is_scoreable_by_paired_strategy(tmp_path: Path) -> None:
    engine = _four_hour_engine()
    strategy = importlib.import_module(
        "quant_terminal_strategies.compression_participation_release_v1_base"
    )
    candles, oi_rows = _fixture_rows(released_boundary="lower")
    packet = engine.scan_compression_participation_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={**TEST_PARAMETERS, "dedupe_window_minutes": 480},
    )

    decision = strategy.decide(
        {
            "signal": {
                "signal_id": f"{FOUR_HOUR_ENGINE_ID}:BTC:test",
                "signal_set_key": f"{FOUR_HOUR_ENGINE_ID}:BTC:canonical",
                "signal_engine_id": FOUR_HOUR_ENGINE_ID,
                "asset": "BTC",
                "instrument": "BTC-USDT-SWAP",
                "timestamp": packet["timestamp"],
                "payload_schema": "signal_packet.v2",
                "payload": packet,
            },
            "runtime_mode": "stage1",
            "parameters": {},
            "raw_data": {},
        }
    )

    assert decision["action"] == "ENTER"
    assert decision["direction"] == "SHORT"


def test_four_hour_packet_passes_consumer_contract_audit(tmp_path: Path) -> None:
    engine = _four_hour_engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    packet = engine.scan_compression_participation_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters=TEST_PARAMETERS,
    )
    packet_path = tmp_path / "compression_participation_release_4h_packet.json"
    packet_path.write_text(json.dumps(packet))
    audit_script = (
        Path.cwd()
        / "skills"
        / "signal-engine-builder"
        / "scripts"
        / "audit_signal_packet_contract.py"
    )

    result = subprocess.run(
        [sys.executable, str(audit_script), "--packet", str(packet_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    audit = json.loads(result.stdout)
    assert audit["status"] == "pass"
    assert audit["errors"] == []


def test_training_and_live_share_causal_neutral_packet_builder(tmp_path: Path) -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp

    packets, generated_count = engine.generate_compression_participation_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        start=latest_ts,
        end=latest_ts,
        parameters={**TEST_PARAMETERS, "dedupe_window_minutes": 0},
    )
    live_packet = engine.scan_compression_participation_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters={**TEST_PARAMETERS, "dedupe_window_minutes": 0},
    )

    assert generated_count == 1
    assert packets == [live_packet]
    packet = packets[0]
    validate_signal_packet(packet)
    evidence = packet["evidence"]
    assert evidence["engine"] == ENGINE_ID
    assert evidence["event_type"] == "COMPRESSION_PARTICIPATION_RELEASE"
    assert evidence["released_boundary"] == "upper"
    assert Decimal(evidence["trigger_candle_close"]) > Decimal(evidence["prior_range_high"])
    assert Decimal(evidence["compression_percentile"]) <= Decimal(
        evidence["compression_percentile_threshold"]
    )
    assert Decimal(evidence["quote_volume_zscore"]) >= Decimal(evidence["volume_zscore_threshold"])
    assert Decimal(evidence["trigger_range_zscore"]) >= Decimal(evidence["range_zscore_threshold"])
    assert evidence["reference_price"] == evidence["trigger_candle_close"]
    assert evidence["validated_information_horizons_hours"] == [2, 4]
    assert "direction" not in packet
    assert "direction" not in evidence

    signal_open = _parse_ts(evidence["signal_candle_open_ts"])
    signal_close = _parse_ts(evidence["signal_candle_close_ts"])
    assert signal_close == signal_open + timedelta(minutes=5)
    assert _parse_ts(evidence["signal_available_at"]) == signal_close
    candle_ts_index = engine.CANDLE_COLUMNS.index("ts")
    oi_ts_index = engine.OI_COLUMNS.index("ts")
    assert all(
        _parse_ts(row[candle_ts_index]) <= signal_open
        for row in packet["charts"]["5m"]["candles"]
    )
    assert all(
        _parse_ts(row[oi_ts_index]) <= signal_open
        for row in packet["charts"]["open_interest_5m"]["rows"]
    )
    for timeframe in ("2h", "4h", "8h"):
        chart = packet["charts"][timeframe]
        close_index = engine.HTF_COLUMNS.index("close_ts")
        partial_close_index = engine.HTF_COLUMNS.index("partial_close_ts")
        complete_index = engine.HTF_COLUMNS.index("complete")
        for row in chart["candles"]:
            if row[complete_index]:
                assert _parse_ts(row[close_index]) <= signal_close
                assert row[partial_close_index] == row[close_index]
            else:
                assert _parse_ts(row[close_index]) > signal_close
                assert _parse_ts(row[partial_close_index]) == signal_close


def test_shared_scanner_accepts_explicit_engine_identity(tmp_path: Path) -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp

    packets, generated_count = engine.generate_compression_participation_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        start=latest_ts,
        end=latest_ts,
        parameters={**TEST_PARAMETERS, "dedupe_window_minutes": 240},
        engine_id="compression_participation_release_4h_v1",
    )

    assert generated_count == 1
    assert packets[0]["evidence"]["engine"] == "compression_participation_release_4h_v1"
    assert packets[0]["evidence"]["dedupe_window_minutes"] == 240


def test_training_respects_eight_hour_dedupe_seed(tmp_path: Path) -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp

    packets, generated_count = engine.generate_compression_participation_packets(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        start=latest_ts,
        end=latest_ts,
        parameters={
            **TEST_PARAMETERS,
            "_dedupe_seed_timestamp": (latest_ts - timedelta(hours=7)).isoformat(),
        },
    )

    assert generated_count == 0
    assert packets == []


def test_scan_warmup_uses_aligned_row_count_when_timestamps_have_gaps() -> None:
    engine = _engine()
    start = datetime(2025, 1, 1, tzinfo=UTC)
    rows = []
    for index in range(600):
        gap = timedelta(days=2) if index >= 300 else timedelta(0)
        rows.append({"timestamp": start + timedelta(minutes=5 * index) + gap})
    scan_timestamp = rows[550]["timestamp"]

    trimmed = engine._trim_rows_for_scan(
        rows=rows,
        start=scan_timestamp,
        end=scan_timestamp,
        parameters=engine._with_defaults(
            {**TEST_PARAMETERS, "context_bars": 1, "context_timeframes": ["2h"]}
        ),
    )

    assert trimmed[-1]["timestamp"] == scan_timestamp
    assert len(trimmed) == 452


def test_alignment_preserves_confirmed_zero_oi_rows_for_price_window_continuity() -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    oi_rows[-10]["sum_open_interest"] = Decimal("0")
    oi_rows[-10]["sum_open_interest_value"] = Decimal("0")

    aligned = engine._aligned_rows(raw_5m=candles, raw_oi=oi_rows)

    assert len(aligned) == len(candles)
    assert aligned[-10]["sum_open_interest"] == 0.0
    assert aligned[-10]["sum_open_interest_value"] == 0.0


def test_live_scan_rejects_stale_oi_instead_of_replaying_old_aligned_event(
    tmp_path: Path,
) -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    previous = candles[-1]
    candles.append(
        MarketDataCandle(
            timestamp=previous.timestamp + timedelta(minutes=5),
            open=previous.close,
            high=previous.close + Decimal("0.05"),
            low=previous.close - Decimal("0.05"),
            close=previous.close,
            volume=previous.volume,
            vol_ccy=previous.vol_ccy,
            vol_ccy_quote=previous.vol_ccy_quote,
            confirm=1,
        )
    )

    packet = engine.scan_compression_participation_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters=TEST_PARAMETERS,
    )

    assert packet is None


def test_base_strategy_maps_released_boundary_to_scoreable_direction(tmp_path: Path) -> None:
    engine = _engine()
    strategy = importlib.import_module(
        "quant_terminal_strategies.compression_participation_release_v1_base"
    )

    for released_boundary, expected_direction in (("upper", "LONG"), ("lower", "SHORT")):
        candles, oi_rows = _fixture_rows(released_boundary=released_boundary)
        packet = engine.scan_compression_participation_latest(
            workspace_root=tmp_path,
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            raw_5m=candles,
            raw_oi=oi_rows,
            parameters=TEST_PARAMETERS,
        )
        assert packet is not None

        decision = strategy.decide(
            {
                "signal": {
                    "signal_id": f"{ENGINE_ID}:BTC:{released_boundary}",
                    "signal_set_key": f"{ENGINE_ID}:BTC:canonical",
                    "signal_engine_id": ENGINE_ID,
                    "asset": "BTC",
                    "instrument": "BTC-USDT-SWAP",
                    "timestamp": packet["timestamp"],
                    "payload_schema": "signal_packet.v2",
                    "payload": packet,
                },
                "runtime_mode": "stage1",
                "parameters": {},
                "raw_data": {},
            }
        )

        assert decision["action"] == "ENTER"
        assert decision["direction"] == expected_direction
        assert decision["execution_profile"] == {}


def test_runtime_entrypoints_use_canonical_reader_and_share_packet_shape(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    latest_ts = candles[-1].timestamp
    reader = _Reader(candles=candles, oi_rows=oi_rows)
    repository = _Repository()
    resolved = resolve_signal_engine(
        ENGINE_ID,
        repository=repository,
        workspace_root=Path.cwd(),
    )
    parameters = {**TEST_PARAMETERS, "dedupe_window_minutes": 0}

    training = resolved.generate_training_signals(
        EngineTrainingContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            signal_set={},
            signal_set_key=f"{ENGINE_ID}:BTC:canonical",
            parameters=parameters,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=tmp_path,
            repository=repository,
            start=latest_ts,
            end=latest_ts,
            raw_candle_end=latest_ts,
        )
    )
    live = resolved.scan_live_signal(
        EngineLiveScanContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            route={},
            parameters=parameters,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=tmp_path,
            repository=repository,
        )
    )

    assert reader.calls == ["candles", "open_interest", "candles", "open_interest"]
    assert training.result.generated_packet_count == 1
    assert live.status == "fresh_signal"
    assert live.signal is not None
    assert training.packets[0] == live.signal.to_mapping() | {"charts": training.packets[0]["charts"]}


def test_training_reports_coverage_limited_by_stale_oi(tmp_path: Path) -> None:
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    oi_end = candles[-1].timestamp
    last = candles[-1]
    candles.append(
        MarketDataCandle(
            timestamp=last.timestamp + timedelta(minutes=5),
            open=last.close,
            high=last.high,
            low=last.low,
            close=last.close,
            volume=last.volume,
            vol_ccy=last.vol_ccy,
            vol_ccy_quote=last.vol_ccy_quote,
            confirm=1,
        )
    )
    reader = _Reader(candles=candles, oi_rows=oi_rows)
    repository = _Repository()
    resolved = resolve_signal_engine(
        ENGINE_ID,
        repository=repository,
        workspace_root=Path.cwd(),
    )

    output = resolved.generate_training_signals(
        EngineTrainingContext(
            asset="BTC",
            instrument="BTC-USDT-SWAP",
            signal_set={},
            signal_set_key=f"{ENGINE_ID}:BTC:canonical",
            parameters=TEST_PARAMETERS,
            market_data_reader=reader,
            spec=resolved.spec,
            workspace_root=tmp_path,
            repository=repository,
            start=candles[0].timestamp,
            end=candles[-1].timestamp,
            raw_candle_end=candles[-1].timestamp,
        )
    )

    assert output.result.scan_coverage_end_ts == oi_end.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_stage1_scorer_consumes_raw_emitted_packet(tmp_path: Path) -> None:
    engine = _engine()
    candles, oi_rows = _fixture_rows(released_boundary="upper")
    packet = engine.scan_compression_participation_latest(
        workspace_root=tmp_path,
        asset="BTC",
        instrument="BTC-USDT-SWAP",
        raw_5m=candles,
        raw_oi=oi_rows,
        parameters=TEST_PARAMETERS,
    )
    assert packet is not None

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
    (strategy_root / "strategy.py").write_text(Path(STRATEGY_PATH).read_text())
    packet_path = packet_root / "upper.json"
    packet_path.write_text(json.dumps(packet))
    signal_id = f"{ENGINE_ID}:BTC:upper"
    (iteration_root / "signal_sample.json").write_text(
        json.dumps({"signals": [{"signal_id": signal_id, "packet_path": str(packet_path)}]})
    )
    (iteration_root / "builder_training_sample.json").write_text(
        json.dumps(
            {
                "signals": [
                    {"signal_id": signal_id, "ground_truth": {"natural_direction": "LONG"}}
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
    assert decisions["decisions"][0]["direction"] == "LONG"


def _fixture_rows(*, released_boundary: str) -> tuple[list[MarketDataCandle], list[dict[str, object]]]:
    start = datetime(2025, 1, 1, tzinfo=UTC)
    count = 260
    candles: list[MarketDataCandle] = []
    oi_rows: list[dict[str, object]] = []
    for index in range(count):
        timestamp = start + timedelta(minutes=5 * index)
        if index < count - 48:
            center = Decimal("100") + Decimal(str((index % 13) - 6)) * Decimal("0.03")
            half_range = Decimal("0.35") + Decimal(str(index % 7)) * Decimal("0.02")
            close = center
            high = center + half_range
            low = center - half_range
            quote_volume = Decimal("1000") + Decimal(str(index % 17)) * Decimal("25")
        else:
            center = Decimal("100")
            close = center + Decimal(str((index % 3) - 1)) * Decimal("0.01")
            high = Decimal("100.10")
            low = Decimal("99.90")
            quote_volume = Decimal("900") + Decimal(str(index % 11)) * Decimal("10")
        if index == count - 1:
            if released_boundary == "upper":
                close = Decimal("100.35")
                high = Decimal("101.35")
                low = Decimal("99.35")
            else:
                close = Decimal("99.65")
                high = Decimal("100.65")
                low = Decimal("98.65")
            quote_volume = Decimal("10000")
        candles.append(
            MarketDataCandle(
                timestamp=timestamp,
                open=close,
                high=high,
                low=low,
                close=close,
                volume=quote_volume / Decimal("100"),
                vol_ccy=quote_volume / Decimal("10"),
                vol_ccy_quote=quote_volume,
                confirm=1,
            )
        )
        oi = Decimal("1000") + Decimal(str(index)) * Decimal("0.4")
        oi_rows.append(
            {
                "timestamp": timestamp,
                "sum_open_interest": oi,
                "sum_open_interest_value": oi * Decimal("100"),
                "count_toptrader_long_short_ratio": Decimal("1.05") + Decimal(str(index % 5)) / Decimal("1000"),
                "sum_toptrader_long_short_ratio": Decimal("1.02"),
                "count_long_short_ratio": Decimal("0.98") + Decimal(str(index % 7)) / Decimal("1000"),
                "sum_taker_long_short_vol_ratio": Decimal("1.01") + Decimal(str(index % 3)) / Decimal("1000"),
                "confirm": 1,
            }
        )
    return candles, oi_rows


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class _Reader:
    def __init__(
        self,
        *,
        candles: list[MarketDataCandle],
        oi_rows: list[dict[str, object]],
    ) -> None:
        self.candles = candles
        self.oi_rows = oi_rows
        self.calls: list[str] = []

    def get_candles(self, **_: object) -> list[MarketDataCandle]:
        self.calls.append("candles")
        return self.candles

    def get_rows(self, **_: object) -> list[dict[str, object]]:
        self.calls.append("open_interest")
        return self.oi_rows


class _Repository:
    def list_signal_engines(self) -> list[dict[str, object]]:
        return []
