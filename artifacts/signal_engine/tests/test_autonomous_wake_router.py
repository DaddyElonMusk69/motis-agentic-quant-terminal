from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n")


def run_router(tmp_path: Path, positions: object, *extra: str) -> dict[str, object]:
    positions_path = tmp_path / "positions.json"
    write_json(positions_path, positions)
    orders_path = tmp_path / "orders.json"
    write_json(orders_path, [])
    command = [
        sys.executable,
        str(ROOT / "scripts" / "live" / "autonomous_wake_router.py"),
        "--asset",
        "TEST",
        "--inst-id",
        "TEST-USDT-SWAP",
        "--positions-json-path",
        str(positions_path),
        "--orders-json-path",
        str(orders_path),
        "--router-state-root",
        str(tmp_path / "router_state"),
    ]
    if "--signals-root" not in extra and "--signal-engine-id" not in extra:
        command.extend(
            [
                "--signals-root",
                str(tmp_path / "live" / "signals" / "vegas_ema"),
            ]
        )
    command.extend(extra)
    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def run_router_with_fake_okx(tmp_path: Path, *extra: str) -> tuple[dict[str, object], list[str]]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    capture_path = tmp_path / "okx_args.json"
    positions_path = tmp_path / "positions.json"
    orders_path = tmp_path / "orders.json"
    write_json(positions_path, [])
    write_json(orders_path, [])
    fake_okx = fake_bin / "okx"
    fake_okx.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, pathlib, sys",
                f"capture_path = pathlib.Path({str(capture_path)!r})",
                f"positions_path = pathlib.Path({str(positions_path)!r})",
                f"orders_path = pathlib.Path({str(orders_path)!r})",
                "argv = sys.argv[1:]",
                "capture_path.write_text(json.dumps(argv) + '\\n')",
                "if len(argv) >= 3 and argv[-3:] == ['account', 'positions', '--json']:",
                "    print(positions_path.read_text())",
                "    raise SystemExit(0)",
                "if 'swap' in argv and 'orders' in argv and '--json' in argv:",
                "    print(orders_path.read_text())",
                "    raise SystemExit(0)",
                "print('[]')",
            ]
        )
    )
    fake_okx.chmod(0o755)
    fake_scanner = tmp_path / "fake_scanner.py"
    fake_scanner.write_text(
        "\n".join(
            [
                "import json, pathlib, sys",
                "args = sys.argv",
                "asset = args[args.index('--asset') + 1]",
                "signals_root = pathlib.Path(args[args.index('--signals-root') + 1]) / asset",
                "signals_root.mkdir(parents=True, exist_ok=True)",
                "status = {",
                "  'asset': asset,",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'signal_found': False,",
                "  'emitted': False,",
                "  'suppressed_reason': None,",
                "  'packet_path': None,",
                "}",
                "(signals_root / 'latest_scan.json').write_text(json.dumps(status) + '\\n')",
            ]
        )
    )

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "live" / "autonomous_wake_router.py"),
            "--asset",
            "TEST",
            "--inst-id",
            "TEST-USDT-SWAP",
            "--orders-json-path",
            str(orders_path),
            "--router-state-root",
            str(tmp_path / "router_state"),
            "--signals-root",
            str(tmp_path / "live" / "signals" / "vegas_ema"),
            "--scanner-path",
            str(fake_scanner),
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    return json.loads(result.stdout), json.loads(capture_path.read_text())


def run_router_with_fake_okx_and_scanner(
    tmp_path: Path,
    *,
    positions: object,
    working_orders: object | None = None,
    scanner_status: dict[str, object] | None = None,
    scanner_packet: dict[str, object] | None = None,
    extra: tuple[str, ...] = (),
) -> tuple[dict[str, object], dict[str, object]]:
    positions_path = tmp_path / "positions.json"
    write_json(positions_path, positions)

    working_orders_path = tmp_path / "working_orders.json"
    if working_orders is None:
        write_json(working_orders_path, [])
    else:
        write_json(working_orders_path, working_orders)

    capture_path = tmp_path / "exchange_calls.json"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_okx = fake_bin / "okx"
    fake_okx.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, pathlib, sys",
                f"capture = pathlib.Path({str(capture_path)!r})",
                f"positions_path = pathlib.Path({str(positions_path)!r})",
                f"working_orders_path = pathlib.Path({str(working_orders_path)!r})",
                "calls = []",
                "if capture.exists():",
                "    calls = json.loads(capture.read_text())",
                "argv = sys.argv[1:]",
                "calls.append(argv)",
                "capture.write_text(json.dumps(calls) + '\\n')",
                "if len(argv) >= 3 and argv[-3:] == ['account', 'positions', '--json']:",
                "    print(positions_path.read_text())",
                "    raise SystemExit(0)",
                "if 'swap' in argv and 'orders' in argv and '--json' in argv:",
                "    print(working_orders_path.read_text())",
                "    raise SystemExit(0)",
                "if 'swap' in argv and 'cancel' in argv:",
                "    print('{\"result\":\"ok\"}')",
                "    raise SystemExit(0)",
                "print('[]')",
            ]
        )
    )
    fake_okx.chmod(0o755)

    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    fake_scanner = tmp_path / "fake_scanner.py"
    fake_scanner_lines = [
        "import json, pathlib, sys",
        "args = sys.argv",
        "asset = args[args.index('--asset') + 1]",
        "signals_root = pathlib.Path(args[args.index('--signals-root') + 1]) / asset",
        "signals_root.mkdir(parents=True, exist_ok=True)",
    ]
    if scanner_packet is not None:
        fake_scanner_lines.extend(
            [
                f"packet_path = signals_root / {scanner_packet['packet_filename']!r}",
                f"packet = {json.dumps(scanner_packet['packet'])}",
                "packet_path.write_text(json.dumps(packet) + '\\n')",
            ]
        )
    status_payload = scanner_status or {
        "asset": "TEST",
        "inst_id": "TEST-USDT-SWAP",
        "timestamp": "2026-01-01T00:00:00Z",
        "scanned_at_utc": "2026-01-01T00:05:00Z",
        "signal_found": False,
        "emitted": False,
        "suppressed_reason": None,
        "packet_path": None,
    }
    fake_scanner_lines.append(f"status = json.loads({json.dumps(json.dumps(status_payload))})")
    fake_scanner_lines.append("(signals_root / 'latest_scan.json').write_text(json.dumps(status) + '\\n')")
    fake_scanner.write_text("\n".join(fake_scanner_lines))

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "live" / "autonomous_wake_router.py"),
            "--asset",
            "TEST",
            "--inst-id",
            "TEST-USDT-SWAP",
            "--positions-json-path",
            str(positions_path),
            "--orders-json-path",
            str(working_orders_path),
            "--router-state-root",
            str(tmp_path / "router_state"),
            "--signals-root",
            str(signals_root),
            "--scanner-path",
            str(fake_scanner),
            *extra,
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    calls = json.loads(capture_path.read_text()) if capture_path.exists() else []
    return json.loads(result.stdout), {"exchange_calls": calls, "signals_root": str(signals_root)}


def test_router_demo_account_mode_uses_okx_demo_for_position_reads(tmp_path) -> None:
    _wake, okx_args = run_router_with_fake_okx(tmp_path, "--account-mode", "demo")

    assert okx_args[:1] == ["--demo"]


def test_router_live_account_mode_uses_live_okx_for_position_reads(tmp_path) -> None:
    _wake, okx_args = run_router_with_fake_okx(tmp_path, "--account-mode", "live")

    assert "--demo" not in okx_args


def test_router_wakes_position_review_when_position_open_and_no_cooldown(tmp_path) -> None:
    owner_path = tmp_path / "owner" / "TEST.json"
    write_json(owner_path, {"owner_strategy": "test-strategy"})

    wake = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(tmp_path / "position_reviews"),
    )

    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "position_review"
    assert wake["context"]["asset"] == "TEST"
    assert wake["context"]["account_mode"] == "demo"
    assert "profile" not in wake["context"]
    assert wake["context"]["position_count"] == 1
    assert wake["context"]["owner_strategy"] == "test-strategy"


def test_router_suppresses_position_review_during_cooldown(tmp_path) -> None:
    owner_path = tmp_path / "owner" / "TEST.json"
    write_json(owner_path, {"owner_strategy": "test-strategy"})
    review_state_root = tmp_path / "position_reviews"
    state_path = review_state_root / "TEST.json"
    write_json(
        state_path,
        {
            "last_position_review_wake_at": (
                datetime.now(UTC) - timedelta(minutes=10)
            ).isoformat().replace("+00:00", "Z")
        },
    )

    wake = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(review_state_root),
    )

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "position_review_cooldown"


def test_router_suppresses_position_review_when_owned_by_other_strategy(tmp_path) -> None:
    owner_path = tmp_path / "owner" / "TEST.json"
    write_json(owner_path, {"owner_strategy": "other-strategy"})

    wake = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(tmp_path / "position_reviews"),
    )

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "position_owned_by_other_strategy"
    assert wake["context"]["owner_strategy"] == "other-strategy"


def test_router_fails_closed_when_position_owner_missing(tmp_path) -> None:
    wake = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(tmp_path / "position_reviews"),
    )

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "position_owner_unknown"


def test_router_uses_shared_position_review_cooldown_across_router_state_roots(tmp_path) -> None:
    owner_path = tmp_path / "owner" / "TEST.json"
    write_json(owner_path, {"owner_strategy": "test-strategy"})
    review_state_root = tmp_path / "position_reviews"

    first = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(review_state_root),
        "--router-state-root",
        str(tmp_path / "router_state_a"),
    )
    second = run_router(
        tmp_path,
        [{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        "--strategy-id",
        "test-strategy",
        "--owner-state-root",
        str(tmp_path / "owner"),
        "--position-review-state-root",
        str(review_state_root),
        "--router-state-root",
        str(tmp_path / "router_state_b"),
    )

    assert first["wakeAgent"] is True
    assert first["context"]["reason"] == "position_review"
    assert second["wakeAgent"] is False
    assert second["context"]["reason"] == "position_review_cooldown"


def test_router_does_not_scan_when_position_state_unknown(tmp_path) -> None:
    wake = run_router(tmp_path, {"unexpected": "shape"})

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "position_state_unknown"


def test_router_wakes_signal_when_flat_and_scanner_emits_packet(tmp_path) -> None:
    signals_root = tmp_path / "live" / "signals" / "vegas_ema"
    packet_path = signals_root / "TEST" / "20260101T000000Z.json"
    write_json(
        packet_path,
        {
            "asset": "TEST",
            "timestamp": "2026-01-01T00:00:00Z",
            "active_timeframes": ["2h", "4h", "8h"],
            "interactions": {"2h": [{"market_price": "100"}]},
            "charts": {},
        },
    )

    fake_scanner = tmp_path / "fake_scan_okx_live_signals.py"
    fake_scanner.write_text(
        "\n".join(
            [
                "import json, pathlib",
                f"root = pathlib.Path({str(signals_root)!r}) / 'TEST'",
                "root.mkdir(parents=True, exist_ok=True)",
                "payload = {",
                "  'asset': 'TEST',",
                "  'inst_id': 'TEST-USDT-SWAP',",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'scanned_at_utc': '2026-01-01T00:05:00Z',",
                "  'signal_found': True,",
                "  'emitted': True,",
                f"  'packet_path': {str(packet_path)!r},",
                "}",
                "(root / 'latest_scan.json').write_text(json.dumps(payload) + '\\n')",
            ]
        )
    )
    wake = run_router(tmp_path, [], "--scanner-path", str(fake_scanner))

    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "signal"
    assert wake["context"]["signal_packet_path"] == str(packet_path)
    assert wake["context"]["votes"] == 3


def test_router_skips_scan_when_flat_and_working_entry_order_is_below_ttl(tmp_path) -> None:
    recent_ctime = int((datetime.now(UTC) - timedelta(minutes=5)).timestamp() * 1000)
    wake, info = run_router_with_fake_okx_and_scanner(
        tmp_path,
        positions=[],
        working_orders=[
            {
                "instId": "TEST-USDT-SWAP",
                "ordId": "123",
                "reduceOnly": False,
                "cTime": str(recent_ctime),
            }
        ],
        extra=("--entry-order-ttl-minutes", "30"),
    )

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "resting_entry_order_active"
    assert wake["context"]["order_count"] == 1
    assert wake["context"]["entry_order_ttl_minutes"] == 30
    assert not any("cancel" in call for call in info["exchange_calls"])


def test_router_cancels_stale_entry_order_and_scans_same_tick(tmp_path) -> None:
    packet_path = tmp_path / "live" / "signals" / "vegas_ema" / "TEST" / "20260101T000000Z.json"
    wake, info = run_router_with_fake_okx_and_scanner(
        tmp_path,
        positions=[],
        working_orders=[
            {
                "instId": "TEST-USDT-SWAP",
                "ordId": "123",
                "reduceOnly": False,
                "cTime": "1767223500000",
            }
        ],
        scanner_status={
            "asset": "TEST",
            "inst_id": "TEST-USDT-SWAP",
            "timestamp": "2026-01-01T00:00:00Z",
            "scanned_at_utc": "2026-01-01T00:05:00Z",
            "signal_found": True,
            "emitted": True,
            "packet_path": str(packet_path),
        },
        scanner_packet={
            "packet_filename": "20260101T000000Z.json",
            "packet": {
                "asset": "TEST",
                "timestamp": "2026-01-01T00:00:00Z",
                "active_timeframes": ["2h", "4h"],
                "interactions": {"2h": [{"market_price": "100"}]},
                "charts": {},
            },
        },
        extra=("--entry-order-ttl-minutes", "30"),
    )

    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "signal"
    assert wake["context"]["signal_packet_path"] == str(packet_path)
    assert any("cancel" in call for call in info["exchange_calls"])


def test_router_fails_closed_when_stale_entry_order_cancel_fails(tmp_path) -> None:
    positions_path = tmp_path / "positions.json"
    write_json(positions_path, [])
    working_orders_path = tmp_path / "working_orders.json"
    write_json(
        working_orders_path,
        [
            {
                "instId": "TEST-USDT-SWAP",
                "ordId": "123",
                "reduceOnly": False,
                "cTime": "1767223500000",
            }
        ],
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_okx = fake_bin / "okx"
    fake_okx.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import pathlib, sys",
                f"positions_path = pathlib.Path({str(positions_path)!r})",
                f"working_orders_path = pathlib.Path({str(working_orders_path)!r})",
                "argv = sys.argv[1:]",
                "if len(argv) >= 3 and argv[-3:] == ['account', 'positions', '--json']:",
                "    print(positions_path.read_text())",
                "    raise SystemExit(0)",
                "if 'swap' in argv and 'orders' in argv and '--json' in argv:",
                "    print(working_orders_path.read_text())",
                "    raise SystemExit(0)",
                "if 'swap' in argv and 'cancel' in argv:",
                "    raise SystemExit(1)",
                "print('[]')",
            ]
        )
    )
    fake_okx.chmod(0o755)
    fake_scanner = tmp_path / "fake_scanner.py"
    fake_scanner.write_text("raise SystemExit('scanner should not run')\n")
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
    result = subprocess.run(
        [
            sys.executable,
            str(ROOT / "scripts" / "live" / "autonomous_wake_router.py"),
            "--asset",
            "TEST",
            "--inst-id",
            "TEST-USDT-SWAP",
            "--positions-json-path",
            str(positions_path),
            "--orders-json-path",
            str(working_orders_path),
            "--router-state-root",
            str(tmp_path / "router_state"),
            "--signals-root",
            str(tmp_path / "live" / "signals" / "vegas_ema"),
            "--scanner-path",
            str(fake_scanner),
            "--entry-order-ttl-minutes",
            "30",
        ],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    wake = json.loads(result.stdout)
    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "entry_order_state_unknown"


def test_router_cancels_leftover_entry_order_before_position_review(tmp_path) -> None:
    owner_path = tmp_path / "owner" / "TEST.json"
    write_json(owner_path, {"owner_strategy": "test-strategy"})
    wake, info = run_router_with_fake_okx_and_scanner(
        tmp_path,
        positions=[{"instId": "TEST-USDT-SWAP", "pos": "1"}],
        working_orders=[
            {
                "instId": "TEST-USDT-SWAP",
                "ordId": "123",
                "reduceOnly": False,
                "cTime": "1767223500000",
            }
        ],
        extra=(
            "--strategy-id",
            "test-strategy",
            "--owner-state-root",
            str(tmp_path / "owner"),
            "--position-review-state-root",
            str(tmp_path / "position_reviews"),
        ),
    )

    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "position_review"
    assert any("cancel" in call for call in info["exchange_calls"])


def test_router_ignores_reduce_only_working_orders_for_entry_ttl(tmp_path) -> None:
    wake, _info = run_router_with_fake_okx_and_scanner(
        tmp_path,
        positions=[],
        working_orders=[
            {
                "instId": "TEST-USDT-SWAP",
                "ordId": "123",
                "reduceOnly": True,
                "cTime": "1767223500000",
            }
        ],
        scanner_status={
            "asset": "TEST",
            "inst_id": "TEST-USDT-SWAP",
            "timestamp": "2026-01-01T00:00:00Z",
            "scanned_at_utc": "2026-01-01T00:05:00Z",
            "signal_found": False,
            "emitted": False,
            "suppressed_reason": None,
        },
    )

    assert wake["wakeAgent"] is False
    assert wake["context"]["reason"] == "no_signal"


def test_router_loads_scanner_and_signal_root_from_engine_registry(tmp_path) -> None:
    engine_registry = tmp_path / "engine_registry.json"
    signals_root = tmp_path / "live" / "signals" / "bollinger"
    fake_scanner = tmp_path / "bollinger_scanner.py"
    packet_path = signals_root / "TEST" / "20260101T000000Z.json"
    fake_scanner.write_text(
        "\n".join(
            [
                "import json, pathlib, sys",
                "args = sys.argv",
                "asset = args[args.index('--asset') + 1]",
                "signals_root = pathlib.Path(args[args.index('--signals-root') + 1]) / asset",
                "signals_root.mkdir(parents=True, exist_ok=True)",
                f"packet_path = pathlib.Path({str(packet_path)!r})",
                "packet_path.write_text(json.dumps({",
                "  'asset': asset,",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'active_timeframes': ['2h'],",
                "  'interactions': {'2h': [{'market_price': '100'}]},",
                "  'charts': {},",
                "}) + '\\n')",
                "status = {",
                "  'asset': asset,",
                "  'inst_id': 'TEST-USDT-SWAP',",
                "  'timestamp': '2026-01-01T00:00:00Z',",
                "  'scanned_at_utc': '2026-01-01T00:05:00Z',",
                "  'signal_found': True,",
                "  'emitted': True,",
                f"  'packet_path': {str(packet_path)!r},",
                "}",
                "(signals_root / 'latest_scan.json').write_text(json.dumps(status) + '\\n')",
            ]
        )
    )
    engine_registry.write_text(
        json.dumps(
            {
                "bollinger": {
                    "signal_engine_id": "bollinger",
                    "live_scanner_path": str(fake_scanner),
                    "live_signals_root": str(signals_root),
                }
            }
        )
        + "\n"
    )

    wake = run_router(
        tmp_path,
        [],
        "--signal-engine-id",
        "bollinger",
        "--engine-registry-path",
        str(engine_registry),
    )

    assert wake["wakeAgent"] is True
    assert wake["context"]["reason"] == "signal"
    assert wake["context"]["signal_packet_path"] == str(packet_path)
