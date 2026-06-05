from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from vegas.candle_store import rebuild_derived, write_candles
from vegas.schemas import Candle


ROOT = Path(__file__).resolve().parents[1]


def make_flat_5m_candles(start: datetime, count: int) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        price = Decimal("100")
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


def make_one_candle_bollinger_signal(start: datetime, count: int) -> list[Candle]:
    candles: list[Candle] = []
    for index in range(count):
        ts = start + timedelta(minutes=5 * index)
        price = Decimal("100")
        if index == count - 2:
            price = Decimal("150")
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


def run_bollinger_scanner(
    live_root: Path,
    signals_root: Path,
    asset: str,
    *extra: str,
) -> dict[str, object]:
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "signals" / "scan_okx_live_bollinger_signals.py"),
            "--asset",
            asset,
            "--inst-id",
            f"{asset}-USDT-SWAP",
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--skip-update",
            "--context-bars",
            "3",
            "--bb-period",
            "20",
            "--bb-stddev",
            "2",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
            "--timeframes",
            "4h",
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def test_scan_okx_live_bollinger_signals_writes_separate_status_and_packet(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    status = run_bollinger_scanner(live_root, signals_root, asset)

    status_file = signals_root / asset / "latest_scan.json"
    assert status_file.exists()
    assert status["asset"] == asset
    assert status["mode"] == "live"
    assert status["signal_found"] is True
    assert status["emitted"] is True

    packet_path = Path(status["packet_path"])
    assert packet_path.parent == signals_root / asset
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
    interaction = next(
        interaction
        for interaction in packet["interactions"]
        if interaction["timeframe"] == "4h"
    )
    assert "band" in interaction
    assert "tunnel" not in interaction
    assert "mode" not in packet

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
    assert state_path.exists()
    assert not (live_root / "state" / f"{asset}.json").exists()


def test_scan_okx_live_bollinger_signals_suppresses_recent_duplicate(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_emitted_at": candles[-1].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    status = run_bollinger_scanner(live_root, signals_root, asset)

    assert status["signal_found"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "dedupe_window"


def test_scan_okx_live_bollinger_signals_suppresses_when_position_is_open(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    position_path = live_root / "state" / "positions" / f"{asset}.json"
    position_path.parent.mkdir(parents=True)
    position_path.write_text(json.dumps({"position_open": True}) + "\n")

    status = run_bollinger_scanner(live_root, signals_root, asset)

    assert status["signal_found"] is True
    assert status["open_position"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "open_position"


def test_scan_okx_live_bollinger_signals_catches_intermediate_signal_since_last_scan(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_one_candle_bollinger_signal(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-3].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    status = run_bollinger_scanner(
        live_root,
        signals_root,
        asset,
        "--proximity-threshold",
        "0.01",
    )

    assert status["scan_start_timestamp"] == candles[-2].ts.isoformat().replace("+00:00", "Z")
    assert status["scan_end_timestamp"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert status["scanned_candles"] == 2
    assert status["qualified_signals"] >= 1
    assert status["signal_found"] is True
    assert status["emitted"] is True
    assert status["emitted_packet_timestamp"] in {
        candles[-2].ts.isoformat().replace("+00:00", "Z"),
        candles[-1].ts.isoformat().replace("+00:00", "Z"),
    }

    packet = json.loads(Path(status["packet_path"]).read_text())
    assert packet["timestamp"] == status["emitted_packet_timestamp"]

    state = json.loads(state_path.read_text())
    assert state["last_scanned_at"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert state["last_emitted_at"] == status["emitted_packet_timestamp"]


def test_scan_okx_live_bollinger_signals_emits_latest_eligible_catchup_packet(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-4].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    status = run_bollinger_scanner(live_root, signals_root, asset)

    assert status["scanned_candles"] == 3
    assert status["qualified_signals"] == 3
    assert status["emitted_packet_timestamp"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert Path(status["packet_path"]).name == candles[-1].ts.strftime("%Y%m%dT%H%M%SZ.json")


def test_scan_okx_live_bollinger_signals_dedupe_still_advances_last_scanned_at(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
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

    status = run_bollinger_scanner(live_root, signals_root, asset)

    assert status["signal_found"] is True
    assert status["emitted"] is False
    assert status["suppressed_reason"] == "dedupe_window"
    assert status["emitted_packet_timestamp"] is None
    state = json.loads(state_path.read_text())
    assert state["last_scanned_at"] == candles[-1].ts.isoformat().replace("+00:00", "Z")
    assert state["last_emitted_at"] == candles[-2].ts.isoformat().replace("+00:00", "Z")


def test_scan_okx_live_bollinger_signals_truncates_only_stale_catchup_state(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)

    state_path = live_root / "state" / "bollinger" / f"{asset}.json"
    state_path.parent.mkdir(parents=True)
    state_path.write_text(
        json.dumps({"last_scanned_at": candles[-20].ts.isoformat().replace("+00:00", "Z")})
        + "\n"
    )

    status = run_bollinger_scanner(
        live_root,
        signals_root,
        asset,
        "--max-catchup-minutes",
        "30",
    )

    assert status["catchup_truncated"] is True
    assert status["scan_start_timestamp"] == candles[-6].ts.isoformat().replace("+00:00", "Z")
    assert status["scanned_candles"] == 6


def test_router_can_use_bollinger_scanner_with_separate_roots(tmp_path) -> None:
    live_root = tmp_path / "live" / "data"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    router_state_root = tmp_path / "wake_router_bollinger"
    asset = "TEST"
    candles = make_flat_5m_candles(datetime(2026, 1, 1, tzinfo=UTC), 1000)
    write_candles(live_root / "raw" / asset / "5m" / "candles.csv", candles)
    rebuild_derived(live_root, asset)
    positions_path = tmp_path / "positions.json"
    positions_path.write_text("[]\n")
    orders_path = tmp_path / "orders.json"
    orders_path.write_text("[]\n")
    fake_scanner = tmp_path / "fake_bollinger_scanner.py"
    fake_scanner.write_text(
        "\n".join(
            [
                "import json, pathlib, sys",
                "args = sys.argv",
                "asset = args[args.index('--asset') + 1]",
                "signals_root = pathlib.Path(args[args.index('--signals-root') + 1])",
                "root = signals_root / asset",
                "root.mkdir(parents=True, exist_ok=True)",
                "packet_path = root / '20260101T000000Z.json'",
                "packet = {",
                "  'asset': asset,",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'active_timeframes': ['4h', '8h', '12h', '1d'],",
                "  'interactions': {'4h': [{'market_price': '100'}]},",
                "  'charts': {},",
                "}",
                "packet_path.write_text(json.dumps(packet) + '\\n')",
                "status = {",
                "  'asset': asset,",
                "  'inst_id': 'TEST-USDT-SWAP',",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'scanned_at_utc': '2026-01-01T00:05:00Z',",
                "  'signal_found': True,",
                "  'emitted': True,",
                "  'packet_path': str(packet_path),",
                "}",
                "(root / 'latest_scan.json').write_text(json.dumps(status) + '\\n')",
            ]
        )
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "live" / "autonomous_wake_router.py"),
            "--asset",
            asset,
            "--inst-id",
            f"{asset}-USDT-SWAP",
            "--positions-json-path",
            str(positions_path),
            "--orders-json-path",
            str(orders_path),
            "--live-root",
            str(live_root),
            "--signals-root",
            str(signals_root),
            "--router-state-root",
            str(router_state_root),
            "--scanner-path",
            str(fake_scanner),
            "--scanner-timeout-seconds",
            "20",
            "--proximity-threshold",
            "1",
            "--vote-threshold",
            "1",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    wake = json.loads(result.stdout)
    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "signal"
    assert wake["context"]["votes"] == 4
    assert wake["context"]["signal_packet_path"].startswith(str(signals_root / asset))
