from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from vegas.candle_store import asset_to_okx_swap, rebuild_derived, write_candles
from vegas.schemas import Candle


ROOT = Path(__file__).resolve().parents[1]


def make_5m_candles(start: datetime, count: int) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        price = Decimal("100") + Decimal(index % 50)
        candles.append(
            Candle(
                ts=ts,
                open=price,
                high=price + Decimal("2"),
                low=price - Decimal("2"),
                close=price + Decimal("0.25"),
                volume=Decimal("10"),
                vol_ccy=Decimal("0.1"),
                vol_ccy_quote=Decimal("1000"),
                confirm=1,
            )
        )
    return candles


def make_one_candle_ema_signal(start: datetime, count: int) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        price = Decimal("100")
        if index == count - 1:
            price = Decimal("1000")
        candles.append(
            Candle(
                ts=ts,
                open=price,
                high=price,
                low=price,
                close=price,
                volume=Decimal("10"),
                vol_ccy=Decimal("0.1"),
                vol_ccy_quote=Decimal("1000"),
                confirm=1,
            )
        )
    return candles


def test_asset_to_okx_swap_resolution() -> None:
    assert asset_to_okx_swap("BTC") == "BTC-USDT-SWAP"
    assert asset_to_okx_swap("eth") == "ETH-USDT-SWAP"


def test_scan_okx_live_signals_writes_status_from_existing_cache(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    status_file = signals_root / asset / "latest_scan.json"
    assert status_file.exists()
    assert status["asset"] == asset
    assert status["inst_id"] == "TEST-USDT-SWAP"
    assert status["mode"] == "live"
    assert status["signal_found"] is True
    assert status["emitted"] is True

    packet_path = Path(status["packet_path"])
    packet = json.loads(packet_path.read_text())
    assert set(packet) == {
        "schema_version",
        "asset",
        "timestamp",
        "active_timeframes",
        "interactions",
        "charts",
    }
    assert packet["schema_version"] == "signal_packet.v2"
    assert "mode" not in packet


def test_scan_okx_live_signals_suppresses_recent_duplicate_by_default(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    latest_ts = candles[-1].ts.isoformat().replace("+00:00", "Z")
    state_path = live_root / "state" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({"last_emitted_at": latest_ts}) + "\n")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["dedupe_window_minutes"] == 30
    assert status["signal_found"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "dedupe_window"
    assert status["packet_path"] is None


def test_scan_okx_live_signals_suppresses_when_position_is_open(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    position_path = live_root / "state" / "positions" / f"{asset}.json"
    position_path.parent.mkdir(parents=True)
    position_path.write_text(json.dumps({"position_open": True}) + "\n")

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["signal_found"] is True
    assert status["open_position"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "open_position"
    assert status["packet_path"] is None


def test_scan_okx_live_signals_catches_intermediate_signal_since_last_scan(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_one_candle_ema_signal(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-3].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "0.002",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["scan_start_timestamp"] == candles[-2].ts.isoformat().replace("+00:00", "Z")
    assert status["scan_end_timestamp"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert status["scanned_candles"] == 2
    assert status["qualified_signals"] == 1
    assert status["signal_found"] is True
    assert status["emitted"] is True
    assert status["emitted_packet_timestamp"] == candles[-2].ts.isoformat().replace("+00:00", "Z")

    packet = json.loads(Path(status["packet_path"]).read_text())
    assert packet["timestamp"] == candles[-2].ts.isoformat().replace("+00:00", "Z")

    state = json.loads(state_path.read_text())
    assert state["last_scanned_at"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert state["last_emitted_at"] == candles[-2].ts.isoformat().replace("+00:00", "Z")


def test_scan_okx_live_signals_emits_latest_eligible_catchup_packet(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-4].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["scanned_candles"] == 3
    assert status["qualified_signals"] == 3
    assert status["emitted_packet_timestamp"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert Path(status["packet_path"]).name == candles[-1].ts.strftime("%Y%m%dT%H%M%SZ.json")


def test_scan_okx_live_signals_dedupe_still_advances_last_scanned_at(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps(
            {
                "last_scanned_at": candles[-4].ts.isoformat().replace("+00:00", "Z"),
                "last_emitted_at": candles[-2].ts.isoformat().replace("+00:00", "Z"),
            }
        )
        + "\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["signal_found"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "dedupe_window"
    assert status["emitted_packet_timestamp"] is None
    state = json.loads(state_path.read_text())
    assert state["last_scanned_at"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert state["last_emitted_at"] == candles[-2].ts.isoformat().replace("+00:00", "Z")


def test_scan_okx_live_signals_truncates_only_stale_catchup_state(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-20].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--ema-warmup-bars",
            "676",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "2h",
            "--max-catchup-minutes",
            "30",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    status = json.loads(result.stdout)
    assert status["catchup_truncated"] is True
    assert status["scan_start_timestamp"] == candles[-6].ts.isoformat().replace("+00:00", "Z")
    assert status["scanned_candles"] == 6


def test_update_okx_live_data_uses_existing_cache_when_fetch_fails(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    training_root = tmp_path / "dev" / "data"
    asset = "TEST"
    candles = make_5m_candles(datetime(2026, 1, 1, 0, 0, tzinfo=UTC), 17000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_okx = fake_bin / "okx"
    fake_okx.write_text("#!/bin/sh\nexit 1\n")
    fake_okx.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "data" / "update_okx_live_data.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--training-root",
            str(training_root),
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    status = json.loads(result.stdout)
    assert status["asset"] == asset
    assert status["fetch_status"] == "cache_fallback"
    assert status["raw_rows"] == len(candles)
    assert (live_root / "derived" / asset / "2h" / "candles.csv").exists()


def test_update_okx_live_data_skips_network_when_cache_is_fresh(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    training_root = tmp_path / "dev" / "data"
    asset = "TEST"
    candles = make_5m_candles(datetime.now(UTC) - timedelta(minutes=20), 5)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_okx = fake_bin / "okx"
    fake_okx.write_text("#!/bin/sh\necho should-not-fetch >&2\nexit 1\n")
    fake_okx.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "data" / "update_okx_live_data.py"),
            "--asset",
            asset,
            "--inst-id",
            "TEST-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--training-root",
            str(training_root),
            "--fresh-cache-minutes",
            "30",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    status = json.loads(result.stdout)
    assert status["fetch_status"] == "fresh_cache"
    assert "should-not-fetch" not in result.stderr
