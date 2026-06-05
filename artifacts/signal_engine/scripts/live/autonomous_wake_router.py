#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

SIGNAL_ENGINE_ROOT = Path(__file__).resolve().parents[2]
SRC = SIGNAL_ENGINE_ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from vegas.candle_store import asset_to_okx_swap, format_ts
from vegas.engine_registry import infer_signal_engine_id, load_engine_registry
from vegas.workspace import find_workspace_root, live_data_root, live_router_root, live_signals_root


WORKSPACE_ROOT = find_workspace_root(SIGNAL_ENGINE_ROOT)


DEFAULT_MANAGEMENT_COOLDOWN_MINUTES = 30
POSITION_QUERY_TIMEOUT_SECONDS = 15
ORDER_QUERY_TIMEOUT_SECONDS = 15
ORDER_CANCEL_TIMEOUT_SECONDS = 15
SCANNER_TIMEOUT_SECONDS = 90
DEFAULT_ENTRY_ORDER_TTL_MINUTES = 30


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Route Vegas cron ticks to signal or position-review wakes.")
    parser.add_argument("--asset", required=True, help="Standalone asset ticker, e.g. BTC")
    parser.add_argument("--inst-id", help="OKX instrument override, e.g. BTC-USDT-SWAP")
    parser.add_argument(
        "--account-mode",
        choices=("demo", "live"),
        default="demo",
        help="Authenticated OKX account mode for position reads. Defaults to demo.",
    )
    parser.add_argument("--profile", default="demo", help="OKX profile for authenticated position reads")
    parser.add_argument("--live-root", default=str(live_data_root(WORKSPACE_ROOT)))
    parser.add_argument("--signals-root", default=str(live_signals_root(WORKSPACE_ROOT) / "vegas_ema"))
    parser.add_argument("--signal-engine-id", default="", help="Canonical signal engine id, e.g. vegas_ema")
    parser.add_argument(
        "--engine-registry-path",
        default=str(SIGNAL_ENGINE_ROOT / "engine_registry.json"),
        help="Signal engine registry used to resolve scanner path and signal roots by signal_engine_id.",
    )
    parser.add_argument("--router-state-root", default=str(live_router_root(WORKSPACE_ROOT) / "state" / "wake_router"))
    parser.add_argument("--strategy-id", help="Strategy id that owns this router invocation.")
    parser.add_argument(
        "--owner-state-root",
        default=str(live_router_root(WORKSPACE_ROOT) / "state" / "open_position_owner"),
        help="Directory containing <ASSET>.json owner files written after confirmed entry fills.",
    )
    parser.add_argument(
        "--position-review-state-root",
        default=str(live_router_root(WORKSPACE_ROOT) / "state" / "position_reviews"),
        help="Shared position-review cooldown state directory across strategy routers.",
    )
    parser.add_argument(
        "--scanner-path",
        default=str(SIGNAL_ENGINE_ROOT / "scripts" / "signals" / "scan_okx_live_signals.py"),
    )
    parser.add_argument("--management-cooldown-minutes", type=int, default=DEFAULT_MANAGEMENT_COOLDOWN_MINUTES)
    parser.add_argument("--entry-order-ttl-minutes", type=int, default=DEFAULT_ENTRY_ORDER_TTL_MINUTES)
    parser.add_argument("--proximity-threshold", default="0.002")
    parser.add_argument("--vote-threshold", type=int, default=3)
    parser.add_argument("--dedupe-window-minutes", type=int, default=30)
    parser.add_argument(
        "--positions-json-path",
        help="Optional fixture/file containing OKX positions JSON; bypasses live OKX account query.",
    )
    parser.add_argument(
        "--orders-json-path",
        help="Optional fixture/file containing OKX working orders JSON; bypasses live OKX order query.",
    )
    parser.add_argument(
        "--position-query-timeout-seconds",
        type=int,
        default=POSITION_QUERY_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--order-query-timeout-seconds",
        type=int,
        default=ORDER_QUERY_TIMEOUT_SECONDS,
    )
    parser.add_argument(
        "--order-cancel-timeout-seconds",
        type=int,
        default=ORDER_CANCEL_TIMEOUT_SECONDS,
    )
    parser.add_argument("--scanner-timeout-seconds", type=int, default=SCANNER_TIMEOUT_SECONDS)
    return parser.parse_args()


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def okx_account_cmd(args: argparse.Namespace, *parts: str) -> list[str]:
    cmd = ["okx"]
    if args.account_mode == "demo":
        cmd.append("--demo")
    cmd.extend(parts)
    return cmd


def load_positions(args: argparse.Namespace) -> tuple[list[dict[str, Any]], str | None]:
    if args.positions_json_path:
        payload = json.loads(Path(args.positions_json_path).read_text())
    else:
        cmd = okx_account_cmd(args, "account", "positions", "--json")
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.position_query_timeout_seconds,
        )
        payload = json.loads(result.stdout)

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], None
    if isinstance(payload, dict):
        data = payload.get("data", payload.get("positions", payload.get("result", payload)))
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)], None
    return [], "unexpected_positions_payload"


def load_working_orders(args: argparse.Namespace, inst_id: str) -> tuple[list[dict[str, Any]], str | None]:
    if args.orders_json_path:
        payload = json.loads(Path(args.orders_json_path).read_text())
    else:
        cmd = okx_account_cmd(args, "swap", "orders", "--instId", inst_id, "--json")
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.order_query_timeout_seconds,
        )
        payload = json.loads(result.stdout)

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], None
    if isinstance(payload, dict):
        data = payload.get("data", payload.get("orders", payload.get("result", payload)))
        if isinstance(data, list):
            return [item for item in data if isinstance(item, dict)], None
    return [], "unexpected_orders_payload"


def position_size(position: dict[str, Any]) -> float:
    for key in ("pos", "position", "size", "sz"):
        value = position.get(key)
        if value not in (None, ""):
            try:
                return abs(float(value))
            except (TypeError, ValueError):
                return 0.0
    return 0.0


def matching_open_positions(positions: list[dict[str, Any]], inst_id: str) -> list[dict[str, Any]]:
    matches = []
    for position in positions:
        if position.get("instId") != inst_id:
            continue
        if position_size(position) > 0:
            matches.append(position)
    return matches


def is_reduce_only(order: dict[str, Any]) -> bool:
    value = order.get("reduceOnly")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return False


def matching_working_entry_orders(orders: list[dict[str, Any]], inst_id: str) -> list[dict[str, Any]]:
    matches = []
    for order in orders:
        if order.get("instId") != inst_id:
            continue
        if is_reduce_only(order):
            continue
        matches.append(order)
    return matches


def order_created_at(order: dict[str, Any]) -> datetime | None:
    value = order.get("cTime")
    if value in (None, ""):
        return None
    try:
        millis = int(str(value))
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(millis / 1000, UTC)


def oldest_order_age_minutes(orders: list[dict[str, Any]], now: datetime) -> float | None:
    created = [order_created_at(order) for order in orders]
    created = [value for value in created if value is not None]
    if not created:
        return None
    return round((now - min(created)).total_seconds() / 60, 2)


def cancel_working_order(args: argparse.Namespace, inst_id: str, order: dict[str, Any]) -> str | None:
    ord_id = order.get("ordId")
    cl_ord_id = order.get("clOrdId")
    cmd = okx_account_cmd(args, "swap", "cancel", inst_id)
    if isinstance(ord_id, str) and ord_id:
        cmd.extend(["--ordId", ord_id])
    elif isinstance(cl_ord_id, str) and cl_ord_id:
        cmd.extend(["--clOrdId", cl_ord_id])
    else:
        return "cancel_failed: missing ordId/clOrdId"

    try:
        subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=args.order_cancel_timeout_seconds,
        )
    except (subprocess.SubprocessError, OSError) as error:
        return f"cancel_failed: {error}"
    return None


def cancel_orders(
    args: argparse.Namespace,
    inst_id: str,
    orders: list[dict[str, Any]],
) -> tuple[int, str | None]:
    canceled = 0
    for order in orders:
        error = cancel_working_order(args, inst_id, order)
        if error is not None:
            return canceled, error
        canceled += 1
    return canceled, None


def should_management_wake(state: dict[str, Any], cooldown: timedelta, now: datetime) -> bool:
    last_value = state.get("last_position_review_wake_at")
    if not isinstance(last_value, str):
        return True
    return now - parse_ts(last_value) >= cooldown


def owner_state_path(args: argparse.Namespace, asset: str) -> Path:
    return Path(args.owner_state_root) / f"{asset}.json"


def position_review_state_path(args: argparse.Namespace, asset: str) -> Path:
    return Path(args.position_review_state_root) / f"{asset}.json"


def load_owner_state(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    if not path.exists():
        return None, "missing_owner_state"
    try:
        state = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as error:
        return None, f"invalid_owner_state: {error}"
    if not isinstance(state, dict):
        return None, "invalid_owner_state: expected JSON object"
    owner_strategy = state.get("owner_strategy")
    if not isinstance(owner_strategy, str) or not owner_strategy:
        return None, "invalid_owner_state: missing owner_strategy"
    return state, None


def configure_engine_paths(args: argparse.Namespace) -> tuple[str, str]:
    explicit_signals_root = "--signals-root" in sys.argv
    explicit_scanner_path = "--scanner-path" in sys.argv
    signal_engine_id = infer_signal_engine_id(args.signal_engine_id, signals_root=args.signals_root)
    if not signal_engine_id:
        return "", ""
    if explicit_signals_root and explicit_scanner_path:
        return signal_engine_id, ""

    registry = load_engine_registry(Path(args.engine_registry_path))
    entry = registry.get(signal_engine_id)
    if entry is None:
        raise ValueError(f"unknown signal_engine_id: {signal_engine_id}")

    if not explicit_signals_root:
        live_signals_root_value = entry.get("live_signals_root")
        if not isinstance(live_signals_root_value, str) or not live_signals_root_value:
            raise ValueError(f"engine registry entry missing live_signals_root: {signal_engine_id}")
        resolved_root = Path(live_signals_root_value)
        if not resolved_root.is_absolute():
            resolved_root = WORKSPACE_ROOT / resolved_root
        args.signals_root = str(resolved_root)

    if not explicit_scanner_path:
        live_scanner_path = entry.get("live_scanner_path")
        if not isinstance(live_scanner_path, str) or not live_scanner_path:
            raise ValueError(f"engine registry entry missing live_scanner_path: {signal_engine_id}")
        resolved_scanner = Path(live_scanner_path)
        if not resolved_scanner.is_absolute():
            resolved_scanner = WORKSPACE_ROOT / resolved_scanner
        args.scanner_path = str(resolved_scanner)

    signal_family = entry.get("signal_family")
    return signal_engine_id, str(signal_family) if isinstance(signal_family, str) else ""


def signal_context_from_packet(packet_path: Path) -> dict[str, Any]:
    packet = json.loads(packet_path.read_text())
    active_timeframes = packet.get("active_timeframes", [])
    price = "?"
    interactions = packet.get("interactions", {})
    if isinstance(interactions, dict) and interactions:
        first_tf = next(iter(interactions))
        first_items = interactions.get(first_tf) or []
        if first_items:
            price = first_items[0].get("market_price", "?")
    return {
        "votes": len(active_timeframes) if isinstance(active_timeframes, list) else 0,
        "active_timeframes": active_timeframes if isinstance(active_timeframes, list) else [],
        "price": price,
    }


def run_scanner(args: argparse.Namespace, inst_id: str) -> tuple[dict[str, Any] | None, str | None]:
    cmd = [
        sys.executable,
        args.scanner_path,
        "--asset",
        args.asset.upper(),
        "--inst-id",
        inst_id,
        "--live-root",
        args.live_root,
        "--signals-root",
        args.signals_root,
        "--proximity-threshold",
        args.proximity_threshold,
        "--vote-threshold",
        str(args.vote_threshold),
        "--dedupe-window-minutes",
        str(args.dedupe_window_minutes),
        "--ignore-position-state",
    ]
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=args.scanner_timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        return None, f"scanner_timeout_{int(error.timeout)}s"

    if result.stderr:
        sys.stderr.write(result.stderr)
    if result.returncode != 0:
        return None, f"scanner_failed_rc_{result.returncode}"

    status_path = Path(args.signals_root) / args.asset.upper() / "latest_scan.json"
    if not status_path.exists():
        return None, "no_scan_json"
    return json.loads(status_path.read_text()), None


def quiet(reason: str, context: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"wakeAgent": False, "context": {"reason": reason, **(context or {})}}


def main() -> int:
    args = parse_args()
    asset = args.asset.upper()
    try:
        signal_engine_id, signal_family = configure_engine_paths(args)
    except ValueError as exc:
        wake = quiet("engine_registry_error", {"asset": asset, "error": str(exc)})
        print(json.dumps(wake))
        return 0
    inst_id = args.inst_id or asset_to_okx_swap(asset)
    now = datetime.now(UTC)
    state_path = Path(args.router_state_root) / f"{asset}.json"
    latest_wake_path = Path(args.router_state_root) / f"{asset}_latest_wake.json"
    state = load_json(state_path)
    review_state_path = position_review_state_path(args, asset)
    review_state = load_json(review_state_path)

    try:
        positions, positions_error = load_positions(args)
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as error:
        positions = []
        positions_error = f"position_query_failed: {error}"

    base_context: dict[str, Any] = {
        "asset": asset,
        "inst_id": inst_id,
        "account_mode": args.account_mode,
        "router_checked_at": format_ts(now),
        "signal_engine_id": signal_engine_id,
    }
    if signal_family:
        base_context["signal_family"] = signal_family

    if positions_error:
        wake = quiet("position_state_unknown", {**base_context, "error": positions_error})
        write_json(latest_wake_path, wake)
        print(json.dumps(wake))
        return 0

    try:
        working_orders, orders_error = load_working_orders(args, inst_id)
    except (subprocess.SubprocessError, json.JSONDecodeError, OSError) as error:
        working_orders = []
        orders_error = f"order_query_failed: {error}"

    if orders_error:
        wake = quiet("entry_order_state_unknown", {**base_context, "error": orders_error})
        write_json(latest_wake_path, wake)
        print(json.dumps(wake))
        return 0

    working_entry_orders = matching_working_entry_orders(working_orders, inst_id)

    open_positions = matching_open_positions(positions, inst_id)
    if open_positions:
        canceled_order_count = 0
        if working_entry_orders:
            canceled_order_count, cancel_error = cancel_orders(args, inst_id, working_entry_orders)
            if cancel_error:
                wake = quiet(
                    "entry_order_state_unknown",
                    {
                        **base_context,
                        "error": cancel_error,
                        "order_count": len(working_entry_orders),
                        "canceled_order_count": canceled_order_count,
                    },
                )
                write_json(latest_wake_path, wake)
                print(json.dumps(wake))
                return 0

        if not args.strategy_id:
            wake = quiet(
                "position_owner_unknown",
                {
                    **base_context,
                    "error": "missing_strategy_id",
                    "owner_state_path": str(owner_state_path(args, asset)),
                    "canceled_order_count": canceled_order_count,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

        owner_path = owner_state_path(args, asset)
        owner_state, owner_error = load_owner_state(owner_path)
        if owner_error or owner_state is None:
            wake = quiet(
                "position_owner_unknown",
                {
                    **base_context,
                    "error": owner_error,
                    "owner_state_path": str(owner_path),
                    "strategy_id": args.strategy_id,
                    "canceled_order_count": canceled_order_count,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

        owner_strategy = str(owner_state["owner_strategy"])
        owner_signal_engine_id = infer_signal_engine_id(
            owner_state.get("signal_engine_id"),
            owner_state.get("signal_family"),
        )
        if owner_signal_engine_id and signal_engine_id and owner_signal_engine_id != signal_engine_id:
            wake = quiet(
                "position_owned_by_other_engine",
                {
                    **base_context,
                    "owner_strategy": owner_strategy,
                    "owner_signal_engine_id": owner_signal_engine_id,
                    "strategy_id": args.strategy_id,
                    "owner_state_path": str(owner_path),
                    "canceled_order_count": canceled_order_count,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0
        if owner_strategy != args.strategy_id:
            wake = quiet(
                "position_owned_by_other_strategy",
                {
                    **base_context,
                    "owner_strategy": owner_strategy,
                    "strategy_id": args.strategy_id,
                    "owner_state_path": str(owner_path),
                    "canceled_order_count": canceled_order_count,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

        cooldown = timedelta(minutes=args.management_cooldown_minutes)
        if should_management_wake(review_state, cooldown, now):
            wake = {
                "wakeAgent": True,
                "context": {
                    **base_context,
                    "reason": "position_review",
                    "position_count": len(open_positions),
                    "owner_strategy": owner_strategy,
                    "owner_state_path": str(owner_path),
                    "canceled_order_count": canceled_order_count,
                },
            }
            review_state.update(
                {
                    "asset": asset,
                    "inst_id": inst_id,
                    "owner_strategy": owner_strategy,
                    "signal_engine_id": signal_engine_id,
                    "last_position_review_wake_at": format_ts(now),
                    "last_wake_reason": "position_review",
                }
            )
            write_json(review_state_path, review_state)
        else:
            wake = quiet(
                "position_review_cooldown",
                {
                    **base_context,
                    "owner_strategy": owner_strategy,
                    "last_position_review_wake_at": review_state.get("last_position_review_wake_at"),
                    "management_cooldown_minutes": args.management_cooldown_minutes,
                    "canceled_order_count": canceled_order_count,
                },
            )
        write_json(latest_wake_path, wake)
        print(json.dumps(wake))
        return 0

    canceled_order_count = 0
    if working_entry_orders:
        age_minutes = oldest_order_age_minutes(working_entry_orders, now)
        if age_minutes is None:
            wake = quiet(
                "entry_order_state_unknown",
                {
                    **base_context,
                    "error": "missing_order_ctime",
                    "order_count": len(working_entry_orders),
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

        if age_minutes < args.entry_order_ttl_minutes:
            wake = quiet(
                "resting_entry_order_active",
                {
                    **base_context,
                    "order_count": len(working_entry_orders),
                    "oldest_order_age_minutes": age_minutes,
                    "entry_order_ttl_minutes": args.entry_order_ttl_minutes,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

        canceled_order_count, cancel_error = cancel_orders(args, inst_id, working_entry_orders)
        if cancel_error:
            wake = quiet(
                "entry_order_state_unknown",
                {
                    **base_context,
                    "error": cancel_error,
                    "order_count": len(working_entry_orders),
                    "oldest_order_age_minutes": age_minutes,
                    "entry_order_ttl_minutes": args.entry_order_ttl_minutes,
                    "canceled_order_count": canceled_order_count,
                },
            )
            write_json(latest_wake_path, wake)
            print(json.dumps(wake))
            return 0

    scan, scan_error = run_scanner(args, inst_id)
    if scan_error:
        wake = quiet(scan_error, base_context)
        write_json(latest_wake_path, wake)
        print(json.dumps(wake))
        return 0

    assert scan is not None
    if scan.get("emitted") and scan.get("packet_path"):
        packet_path = Path(str(scan["packet_path"]))
        packet_context = signal_context_from_packet(packet_path) if packet_path.exists() else {}
        wake = {
            "wakeAgent": True,
            "context": {
                **base_context,
                "reason": "signal",
                "timestamp": scan.get("timestamp"),
                "scanned_at": scan.get("scanned_at_utc"),
                "signal_packet_path": str(packet_path),
                "packet_path": str(packet_path),
                "signal_found": True,
                "emitted": True,
                **packet_context,
            },
        }
        state.update(
            {
                "asset": asset,
                "inst_id": inst_id,
                "last_signal_wake_at": format_ts(now),
                "last_wake_reason": "signal",
                "last_packet_path": str(packet_path),
            }
        )
        write_json(state_path, state)
    else:
        wake = quiet(
            "no_signal",
            {
                **base_context,
                "scan_timestamp": scan.get("timestamp"),
                "signal_found": bool(scan.get("signal_found")),
                "suppressed_reason": scan.get("suppressed_reason"),
                "canceled_order_count": canceled_order_count,
            },
        )

    write_json(latest_wake_path, wake)
    print(json.dumps(wake))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
