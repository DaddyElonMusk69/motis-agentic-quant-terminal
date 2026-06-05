#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.candle_store import asset_to_okx_swap, format_ts
from vegas.live_provider import LiveMarketStateProvider
from vegas.packet_format import write_signal_packet
from vegas.replay_provider import DEFAULT_TIMEFRAMES
from vegas.signal_engine import UniversalVegasSignalEngine
from vegas.workspace import find_workspace_root, live_data_root, live_signals_root


WORKSPACE_ROOT = find_workspace_root(SIGNAL_ENGINE_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scan OKX live candles for neutral Vegas signals.")
    parser.add_argument("--asset", required=True, help="Standalone asset ticker, e.g. BTC")
    parser.add_argument("--inst-id", help="OKX instrument override, e.g. BTC-USDT-SWAP")
    parser.add_argument("--live-root", default=str(live_data_root(WORKSPACE_ROOT)))
    parser.add_argument("--signals-root", default=str(live_signals_root(WORKSPACE_ROOT) / "vegas_ema"))
    parser.add_argument("--context-bars", type=int, default=80)
    parser.add_argument("--ema-warmup-bars", type=int, default=676)
    parser.add_argument("--proximity-threshold", default="0.002")
    parser.add_argument("--vote-threshold", type=int, default=3)
    parser.add_argument("--dedupe-window-minutes", type=int, default=30)
    parser.add_argument("--max-catchup-minutes", type=int, default=1440)
    parser.add_argument(
        "--position-state-path",
        help=(
            "Optional JSON state file owned by the execution agent. "
            "If it contains position_open/has_open_position/open_position=true, "
            "signal packets are suppressed for this scan."
        ),
    )
    parser.add_argument(
        "--ignore-position-state",
        action="store_true",
        help="Do not suppress signal packets from local position state; use when a router already checked exchange position truth.",
    )
    parser.add_argument("--timeframes", nargs="*", default=list(DEFAULT_TIMEFRAMES))
    parser.add_argument("--skip-update", action="store_true", help="Scan existing live cache only.")
    parser.add_argument("--skip-fetch", action="store_true", help="Pass --skip-fetch to updater.")
    return parser.parse_args()


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def run_updater(args: argparse.Namespace, inst_id: str) -> None:
    cmd = [
        sys.executable,
        str(SIGNAL_ENGINE_ROOT / "scripts" / "data" / "update_okx_live_data.py"),
        "--asset",
        args.asset.upper(),
        "--inst-id",
        inst_id,
        "--live-root",
        args.live_root,
    ]
    if args.skip_fetch:
        cmd.append("--skip-fetch")
    subprocess.run(cmd, check=True)


def load_state(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def position_state_path(args: argparse.Namespace, asset: str, live_root: Path) -> Path:
    if args.position_state_path:
        return Path(args.position_state_path)
    return live_root / "state" / "positions" / f"{asset}.json"


def has_open_position(path: Path) -> bool:
    if not path.exists():
        return False
    state = json.loads(path.read_text())
    if not isinstance(state, dict):
        raise ValueError(f"Position state must be a JSON object: {path}")
    for key in ("position_open", "has_open_position", "open_position"):
        value = state.get(key)
        if isinstance(value, bool):
            return value
    return False


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def should_emit(
    packet_timestamp: datetime,
    state: dict[str, object],
    window: timedelta,
) -> bool:
    last_value = state.get("last_emitted_at")
    if not isinstance(last_value, str):
        return True
    last_emitted_at = parse_ts(last_value)
    return packet_timestamp - last_emitted_at >= window


def scan_candles(provider: LiveMarketStateProvider, state: dict[str, object], max_catchup_minutes: int):
    latest_timestamp = provider.latest_timestamp()
    last_scanned_value = state.get("last_scanned_at")
    catchup_truncated = False

    if isinstance(last_scanned_value, str):
        last_scanned_at = parse_ts(last_scanned_value)
        floor = latest_timestamp - timedelta(minutes=max_catchup_minutes)
        if last_scanned_at < floor:
            last_scanned_at = floor
            catchup_truncated = True
        candles = [
            candle
            for candle in provider.raw_5m
            if last_scanned_at < candle.ts <= latest_timestamp
        ]
    else:
        candles = [provider._latest_5m_at(latest_timestamp)]

    return candles, latest_timestamp, catchup_truncated


def main() -> int:
    args = parse_args()
    asset = args.asset.upper()
    inst_id = args.inst_id or asset_to_okx_swap(asset)
    live_root = Path(args.live_root)
    signals_root = Path(args.signals_root)
    state_path = live_root / "state" / f"{asset}.json"
    pos_state_path = position_state_path(args, asset, live_root)
    status_path = signals_root / asset / "latest_scan.json"

    if not args.skip_update:
        run_updater(args, inst_id)

    provider = LiveMarketStateProvider(
        asset=asset,
        timeframes=args.timeframes,
        context_bars=args.context_bars,
        ema_warmup_bars=args.ema_warmup_bars,
        live_root=live_root,
    )
    state = load_state(state_path)
    candles_to_scan, timestamp, catchup_truncated = scan_candles(
        provider,
        state,
        args.max_catchup_minutes,
    )
    engine = UniversalVegasSignalEngine(
        proximity_threshold=Decimal(args.proximity_threshold),
        vote_threshold=args.vote_threshold,
    )
    qualified_packets = []
    for candle in candles_to_scan:
        snapshot = provider.snapshot_at(candle.ts)
        packet = engine.scan(snapshot)
        if packet is not None:
            qualified_packets.append(packet)

    packet = qualified_packets[-1] if qualified_packets else None
    open_position = False if args.ignore_position_state else has_open_position(pos_state_path)
    scan_start = candles_to_scan[0].ts if candles_to_scan else None
    scan_end = candles_to_scan[-1].ts if candles_to_scan else None

    status: dict[str, object] = {
        "asset": asset,
        "inst_id": inst_id,
        "timestamp": format_ts(timestamp),
        "scan_start_timestamp": format_ts(scan_start) if scan_start else None,
        "scan_end_timestamp": format_ts(scan_end) if scan_end else None,
        "scanned_candles": len(candles_to_scan),
        "qualified_signals": len(qualified_packets),
        "emitted_packet_timestamp": None,
        "catchup_truncated": catchup_truncated,
        "mode": "live",
        "proximity_threshold": args.proximity_threshold,
        "vote_threshold": args.vote_threshold,
        "dedupe_window_minutes": args.dedupe_window_minutes,
        "position_state_path": str(pos_state_path),
        "open_position": open_position,
        "signal_found": packet is not None,
        "emitted": False,
        "suppressed_reason": None,
        "packet_path": None,
        "scanned_at_utc": format_ts(datetime.now(UTC)),
    }

    if packet is not None:
        if open_position:
            status["suppressed_reason"] = "open_position"
        else:
            window = timedelta(minutes=args.dedupe_window_minutes)
            if not should_emit(packet.timestamp, state, window):
                status["suppressed_reason"] = "dedupe_window"
            else:
                signal_id = packet.timestamp.strftime("%Y%m%dT%H%M%SZ")
                packet_path = signals_root / asset / f"{signal_id}.json"
                write_signal_packet(packet_path, packet.to_dict())
                state["last_emitted_at"] = format_ts(packet.timestamp)
                state["last_packet_path"] = str(packet_path)
                status["emitted"] = True
                status["packet_path"] = str(packet_path)
                status["emitted_packet_timestamp"] = format_ts(packet.timestamp)

    if scan_end is not None:
        state.update(
            {
                "asset": asset,
                "inst_id": inst_id,
                "last_scanned_at": format_ts(scan_end),
            }
        )
        write_json(state_path, state)

    write_json(status_path, status)
    print(json.dumps(status, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
