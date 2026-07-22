from __future__ import annotations

from dataclasses import dataclass, replace
import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess
from typing import Any

from quant_terminal_worker.adapters.exchange import ExchangeAdapterError, SwapOrderRequest, SwapProtectionRequest


class OKXCLIError(ExchangeAdapterError):
    pass


@dataclass(frozen=True, slots=True)
class OKXAdapter:
    config: dict[str, Any]
    adapter_id: str = "okx"

    def readiness_blockers(self) -> list[str]:
        backend = self.config.get("backend", "okx_cli")
        if backend == "okx_cli":
            blockers: list[str] = []
            if self._cli_path() is None:
                blockers.append("missing_okx_cli")
            if self.config.get("mode", "demo") not in {"demo", "live"}:
                blockers.append("invalid_okx_mode")
            return blockers

        required = {
            "api_key": "missing_okx_api_key",
            "api_secret": "missing_okx_api_secret",
            "passphrase": "missing_okx_passphrase",
        }
        return [
            blocker
            for key, blocker in required.items()
            if not self.config.get(key)
        ]

    def build_command(self, module: str, action: str, args: list[str] | None = None) -> list[str]:
        cli_path = self._cli_path()
        if cli_path is None:
            raise OKXCLIError("missing OKX CLI executable")

        command = [cli_path]
        profile = self.config.get("profile")
        if profile:
            command.extend(["--profile", str(profile)])

        mode = self.config.get("mode", "demo")
        if mode not in {"demo", "live"}:
            raise OKXCLIError(f"invalid OKX mode: {mode}")
        command.append(f"--{mode}")
        command.append("--json")
        command.extend([module, action])
        command.extend(args or [])
        return command

    def run_json_command(
        self,
        module: str,
        action: str,
        args: list[str] | None = None,
        timeout_seconds: int = 30,
    ) -> Any:
        command = self.build_command(module, action, args)
        run_command = command
        if self.config.get("use_login_shell", True):
            run_command = [
                str(self.config.get("login_shell") or "/bin/zsh"),
                "-lic",
                shlex.join(command),
            ]
        completed = subprocess.run(
            run_command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
        if completed.returncode != 0:
            details = "\n".join(
                part.strip()
                for part in (completed.stderr, completed.stdout)
                if part.strip()
            )
            raise OKXCLIError(details or "OKX CLI command failed")

        try:
            parsed = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise OKXCLIError("OKX CLI returned non-JSON output") from exc

        return parsed

    def market_candles(
        self,
        inst_id: str,
        *,
        bar: str,
        limit: int,
        after: str | None = None,
    ) -> dict[str, Any]:
        args = [inst_id, "--bar", bar, "--limit", str(limit)]
        if after:
            args.extend(["--after", after])
        market_mode = str(self.config.get("market_mode") or "live")
        market_adapter = self
        if market_mode != self.config.get("mode", "demo"):
            market_adapter = OKXAdapter(config={**self.config, "mode": market_mode})
        parsed = market_adapter.run_json_command(
            "market",
            "candles",
            args,
        )
        if isinstance(parsed, list):
            return {"code": "0", "data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned unsupported candle JSON")
        return parsed

    def snapshot(self, instrument: str) -> dict[str, Any]:
        positions = self._extract_list(
            self.run_json_command(
                "account",
                "positions",
                [],
                timeout_seconds=int(self.config.get("position_query_timeout_seconds", 15)),
            ),
            keys=("data", "positions", "result"),
        )
        open_orders = self._extract_list(
            self.run_json_command(
                "swap",
                "orders",
                ["--instId", instrument],
                timeout_seconds=int(self.config.get("order_query_timeout_seconds", 15)),
            ),
            keys=("data", "orders", "result"),
        )
        protection_orders = self.list_swap_algo_orders(instrument, order_type="oco")
        balance = self._extract_object_or_list(
            self.run_json_command(
                "account",
                "balance",
                [],
                timeout_seconds=int(self.config.get("balance_query_timeout_seconds", 15)),
            )
        )
        recent_fills = self.list_swap_recent_fills(instrument)
        return {
            "instrument": instrument,
            "positions": self._filter_instrument_rows(positions, instrument),
            "open_orders": self._filter_instrument_rows(open_orders, instrument),
            "protection_orders": self._filter_instrument_rows(protection_orders, instrument),
            "balance": balance,
            "recent_fills": recent_fills,
        }

    def cancel_order(
        self,
        *,
        instrument: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]:
        args = [instrument]
        if order_id:
            args.extend(["--ordId", order_id])
        elif client_order_id:
            args.extend(["--clOrdId", client_order_id])
        else:
            raise OKXCLIError("cancel_order requires order_id or client_order_id")
        parsed = self.run_json_command(
            "swap",
            "cancel",
            args,
            timeout_seconds=int(self.config.get("order_cancel_timeout_seconds", 15)),
        )
        if not isinstance(parsed, dict):
            return {"instrument": instrument, "order_id": order_id, "client_order_id": client_order_id, "result": parsed}
        return {
            "instrument": instrument,
            "order_id": order_id,
            "client_order_id": client_order_id,
            "status": "cancel_requested",
            "result": parsed,
        }

    def list_swap_recent_fills(self, inst_id: str) -> list[dict[str, Any]]:
        try:
            parsed = self.run_json_command(
                "swap",
                "fills",
                ["--instId", inst_id],
                timeout_seconds=int(self.config.get("fill_query_timeout_seconds", 15)),
            )
        except OKXCLIError:
            return []
        return self._filter_instrument_rows(
            self._extract_list(parsed, keys=("data", "fills", "orders", "result")),
            inst_id,
        )

    def list_swap_positions(self, inst_id: str) -> list[dict[str, Any]]:
        positions = self._extract_list(
            self.run_json_command(
                "account",
                "positions",
                [],
                timeout_seconds=int(self.config.get("position_query_timeout_seconds", 15)),
            ),
            keys=("data", "positions", "result"),
        )
        return [
            position
            for position in self._filter_instrument_rows(positions, inst_id)
            if abs(_numeric(position.get("pos") or position.get("size") or position.get("sz"))) > 0
        ]

    def list_swap_algo_orders(self, inst_id: str, *, order_type: str | None = None) -> list[dict[str, Any]]:
        args = ["--instId", inst_id]
        if order_type:
            args.extend(["--ordType", order_type])
        parsed = self.run_json_command(
            "swap",
            "algo",
            ["orders", *args],
            timeout_seconds=int(self.config.get("algo_order_query_timeout_seconds", 15)),
        )
        return self._extract_list(parsed, keys=("data", "orders", "result"))

    def cancel_swap_algo_order(self, *, inst_id: str, algo_id: str) -> dict[str, Any]:
        parsed = self.run_json_command(
            "swap",
            "algo",
            ["cancel", "--instId", inst_id, "--algoId", algo_id],
            timeout_seconds=int(self.config.get("algo_order_cancel_timeout_seconds", 15)),
        )
        if isinstance(parsed, list):
            return {"data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return parsed

    def cancel_swap_protection_orders(self, inst_id: str) -> dict[str, Any]:
        orders = self.list_swap_algo_orders(inst_id, order_type="oco")
        cancelled = []
        for order in orders:
            algo_id = str(order.get("algoId") or order.get("algo_id") or "")
            if algo_id:
                cancelled.append(self.cancel_swap_algo_order(inst_id=inst_id, algo_id=algo_id))
        return {
            "status": "cancelled" if cancelled else "noop",
            "instrument": inst_id,
            "cancelled_count": len(cancelled),
            "cancelled": cancelled,
        }

    def place_swap_protection_order(self, request: SwapProtectionRequest) -> dict[str, Any]:
        args = [
            "place",
            "--instId",
            request.inst_id,
            "--side",
            request.side,
            "--sz",
            request.size,
            "--ordType",
            "oco",
            "--tpTriggerPx",
            request.tp_trigger_price,
            "--tpOrdPx=-1",
            "--slTriggerPx",
            request.sl_trigger_price,
            "--slOrdPx=-1",
            "--tdMode",
            request.trade_mode,
            "--reduceOnly",
        ]
        position_side = _cli_position_side(request.position_side)
        if position_side:
            args.extend(["--posSide", position_side])
        parsed = self.run_json_command("swap", "algo", args)
        if isinstance(parsed, list):
            return {"data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return parsed

    def amend_swap_protection_order(
        self,
        *,
        inst_id: str,
        algo_id: str,
        tp_trigger_price: str,
        sl_trigger_price: str,
        size: str | None = None,
    ) -> dict[str, Any]:
        args = [
            "amend",
            "--instId",
            inst_id,
            "--algoId",
            algo_id,
            "--newTpTriggerPx",
            tp_trigger_price,
            "--newTpOrdPx=-1",
            "--newSlTriggerPx",
            sl_trigger_price,
            "--newSlOrdPx=-1",
        ]
        if size:
            args.extend(["--newSz", size])
        parsed = self.run_json_command("swap", "algo", args)
        if isinstance(parsed, list):
            return {"data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return parsed

    def ensure_swap_protection(self, request: SwapProtectionRequest) -> dict[str, Any]:
        positions = self.list_swap_positions(request.inst_id)
        position = _select_exchange_position(positions, request=request)
        orders = self.list_swap_algo_orders(request.inst_id, order_type="oco")
        all_live_orders = [order for order in orders if str(order.get("state") or "").lower() in {"", "live"}]
        live_orders = _protection_orders_for_position(
            all_live_orders,
            request=request,
            positions=positions,
        )
        if position is None:
            cancelled = self._cancel_swap_algo_orders(request.inst_id, live_orders)
            return {
                "status": "cancelled" if cancelled else "noop",
                "reason": "exchange_position_flat",
                "cancelled_count": len(cancelled),
                "cancelled": cancelled,
            }

        effective_request = _protection_request_for_exchange_position(
            request,
            position=position,
            live_orders=live_orders,
        )
        unmanageable_orders = [
            order
            for order in live_orders
            if not str(order.get("algoId") or order.get("algo_id") or "")
        ]
        if unmanageable_orders:
            raise OKXCLIError(
                f"live protection order for {request.inst_id} has no algoId; refusing to create duplicate protection"
            )
        if len(live_orders) == 1:
            order = live_orders[0]
            closes_entire_position = _closes_entire_position(order)
            algo_id = str(order.get("algoId") or "")
            order_belongs_to_position = not _protection_order_predates_position(order, position=position)
            if algo_id and order_belongs_to_position and _protection_matches(order, effective_request):
                return {
                    "status": "noop",
                    "reason": "protection_already_matches_exchange_position",
                    "position": _position_summary(position),
                    "order": order,
                }
            if algo_id and order_belongs_to_position and _protection_order_can_be_amended(order, request=effective_request):
                try:
                    amended = self.amend_swap_protection_order(
                        inst_id=request.inst_id,
                        algo_id=algo_id,
                        tp_trigger_price=effective_request.tp_trigger_price,
                        sl_trigger_price=effective_request.sl_trigger_price,
                        size=None if closes_entire_position else effective_request.size,
                    )
                except OKXCLIError as exc:
                    if not _is_missing_attached_protection_error(exc):
                        raise
                else:
                    return {
                        "status": "amended",
                        "reason": "protection_reconciled_to_exchange_position",
                        "position": _position_summary(position),
                        "previous_order": order,
                        "result": amended,
                    }

        cancelled = self._cancel_swap_algo_orders(request.inst_id, live_orders)
        latest_position = _select_exchange_position(self.list_swap_positions(request.inst_id), request=request)
        if latest_position is None:
            return {
                "status": "cancelled" if cancelled else "noop",
                "reason": "exchange_position_flat_after_cleanup",
                "cancelled_count": len(cancelled),
                "cancelled": cancelled,
            }
        effective_request = _protection_request_for_exchange_position(
            request,
            position=latest_position,
            live_orders=[],
        )
        return {
            "status": "placed",
            "reason": "protection_recreated_from_exchange_position",
            "position": _position_summary(latest_position),
            "cancelled_count": len(cancelled),
            "cancelled": cancelled,
            "result": self.place_swap_protection_order(effective_request),
        }

    def _cancel_swap_algo_orders(self, inst_id: str, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        cancelled = []
        for order in orders:
            algo_id = str(order.get("algoId") or order.get("algo_id") or "")
            if algo_id:
                try:
                    cancelled.append(self.cancel_swap_algo_order(inst_id=inst_id, algo_id=algo_id))
                except OKXCLIError:
                    refreshed_ids = {
                        str(item.get("algoId") or item.get("algo_id") or "")
                        for item in self.list_swap_algo_orders(inst_id, order_type="oco")
                        if str(item.get("state") or "").lower() in {"", "live"}
                    }
                    if algo_id in refreshed_ids:
                        raise
                    cancelled.append({"status": "already_gone", "algo_id": algo_id})
        return cancelled

    def place_swap_order(self, request: SwapOrderRequest) -> dict[str, Any]:
        exchange_client_order_id = _okx_client_order_id(request.client_order_id)
        args = [
            "--instId",
            request.inst_id,
            "--side",
            request.side,
            "--ordType",
            request.order_type,
            "--sz",
            request.size,
            "--tdMode",
            request.trade_mode,
            "--clOrdId",
            exchange_client_order_id,
        ]
        position_side = _cli_position_side(request.position_side)
        if position_side:
            args.extend(["--posSide", position_side])
        if request.price:
            args.extend(["--px", request.price])
        if request.target_currency:
            args.extend(["--tgtCcy", request.target_currency])
        if request.tp_trigger_price:
            args.extend(["--tpTriggerPx", request.tp_trigger_price, "--tpOrdPx=-1"])
        if request.sl_trigger_price:
            args.extend(["--slTriggerPx", request.sl_trigger_price, "--slOrdPx=-1"])
        if request.reduce_only:
            args.append("--reduceOnly")
        parsed = self.run_json_command("swap", "place", args)
        if isinstance(parsed, list):
            return {
                "data": parsed,
                "client_order_id": request.client_order_id,
                "exchange_client_order_id": exchange_client_order_id,
            }
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return {
            **parsed,
            "client_order_id": request.client_order_id,
            "exchange_client_order_id": exchange_client_order_id,
        }

    def set_swap_leverage(
        self,
        *,
        inst_id: str,
        leverage: str,
        margin_mode: str,
        position_side: str | None = None,
    ) -> dict[str, Any]:
        args = [
            "--instId",
            inst_id,
            "--lever",
            leverage,
            "--mgnMode",
            margin_mode,
        ]
        cli_position_side = _cli_position_side(position_side)
        if cli_position_side:
            args.extend(["--posSide", cli_position_side])
        parsed = self.run_json_command("swap", "leverage", args)
        if isinstance(parsed, list):
            return {"data": parsed}
        if not isinstance(parsed, dict):
            raise OKXCLIError("OKX CLI returned JSON that was not an object")
        return parsed

    def _extract_list(self, payload: Any, *, keys: tuple[str, ...]) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]
        if isinstance(payload, dict):
            for key in keys:
                data = payload.get(key)
                if isinstance(data, list):
                    return [item for item in data if isinstance(item, dict)]
            if all(isinstance(value, (str, int, float, bool, type(None))) for value in payload.values()):
                return [payload]
        return []

    def _extract_object_or_list(self, payload: Any) -> dict[str, Any] | list[Any]:
        if isinstance(payload, dict):
            return payload
        if isinstance(payload, list):
            return payload
        return {}

    def _filter_instrument_rows(self, rows: list[dict[str, Any]], instrument: str) -> list[dict[str, Any]]:
        filtered = [
            row
            for row in rows
            if (row.get("instId") or row.get("instrument") or row.get("inst_id")) in {None, "", instrument}
        ]
        return filtered

    def _cli_path(self) -> str | None:
        configured = self.config.get("cli_path")
        if configured:
            path = Path(str(configured))
            if path.exists():
                return str(path)
            return shutil.which(str(configured))
        return shutil.which("okx")


def _okx_client_order_id(client_order_id: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9]", "", client_order_id)
    if cleaned == client_order_id and 1 <= len(cleaned) <= 32:
        return client_order_id
    digest = hashlib.sha1(client_order_id.encode("utf-8")).hexdigest()[:10]
    prefix = cleaned[: max(1, 32 - len(digest))]
    return f"{prefix}{digest}"[:32]


def _protection_matches(order: dict[str, Any], request: SwapProtectionRequest) -> bool:
    return (
        _same_decimal(order.get("tpTriggerPx"), request.tp_trigger_price)
        and _same_decimal(order.get("slTriggerPx"), request.sl_trigger_price)
        and (_closes_entire_position(order) or _same_decimal(order.get("sz"), request.size))
        and str(order.get("side") or "").lower() == request.side
        and _position_side_matches(order, request=request)
    )


def _protection_order_can_be_amended(order: dict[str, Any], *, request: SwapProtectionRequest) -> bool:
    if str(order.get("side") or "").lower() != request.side:
        return False
    if not _position_side_matches(order, request=request):
        return False
    return all(order.get(key) not in (None, "") for key in ("tpTriggerPx", "slTriggerPx"))


def _position_side_matches(order: dict[str, Any], *, request: SwapProtectionRequest) -> bool:
    order_position_side = str(order.get("posSide") or order.get("position_side") or "").lower()
    request_position_side = str(request.position_side or "").lower()
    return not order_position_side or not request_position_side or order_position_side == request_position_side


def _protection_orders_for_position(
    orders: list[dict[str, Any]],
    *,
    request: SwapProtectionRequest,
    positions: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    requested_position_side = str(request.position_side or "").lower()
    if requested_position_side not in {"long", "short"}:
        return orders
    scoped = [
        order
        for order in orders
        if str(order.get("posSide") or order.get("position_side") or "").lower() == requested_position_side
    ]
    unknown = [order for order in orders if not str(order.get("posSide") or order.get("position_side") or "").strip()]
    active_sides = {
        str(position.get("posSide") or position.get("position_side") or "").lower()
        for position in positions
        if abs(_numeric(position.get("pos") or position.get("size") or position.get("sz"))) > 0
    }
    if unknown and len(active_sides.intersection({"long", "short"})) > 1:
        raise OKXCLIError(
            f"live protection orders for {request.inst_id} have no position side in hedge mode; refusing ambiguous reconciliation"
        )
    return scoped


def _select_exchange_position(
    positions: list[dict[str, Any]],
    *,
    request: SwapProtectionRequest,
) -> dict[str, Any] | None:
    active = [
        position
        for position in positions
        if abs(_numeric(position.get("pos") or position.get("size") or position.get("sz"))) > 0
    ]
    requested_position_side = str(request.position_side or "").lower()
    if requested_position_side in {"long", "short"}:
        matching = [
            position
            for position in active
            if str(position.get("posSide") or position.get("position_side") or "").lower() == requested_position_side
        ]
        if len(matching) == 1:
            return matching[0]
        if not matching:
            return None
        active = matching
    if not active:
        return None
    if len(active) != 1:
        raise OKXCLIError(f"multiple active exchange positions found for {request.inst_id}; position_side is required")
    return active[0]


def _protection_request_for_exchange_position(
    request: SwapProtectionRequest,
    *,
    position: dict[str, Any],
    live_orders: list[dict[str, Any]],
) -> SwapProtectionRequest:
    raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
    if raw_size == 0:
        raise OKXCLIError(f"exchange position is flat for {request.inst_id}")
    position_side = str(position.get("posSide") or position.get("position_side") or "").lower()
    direction = "SHORT" if position_side in {"short", "sell"} or (position_side not in {"long", "buy"} and raw_size < 0) else "LONG"
    entry_price = _numeric(position.get("avgPx") or position.get("avg_price") or position.get("entry_price"))
    tp = request.tp_trigger_price
    sl = request.sl_trigger_price
    phase = _protection_phase_for_exchange_position(
        request,
        position=position,
        direction=direction,
        entry_price=entry_price,
        live_orders=live_orders,
    )
    sl_pct = request.sl_pct
    if phase == "protected" and request.trail_sl_pct:
        sl_pct = request.trail_sl_pct
    elif phase == "initial" and request.initial_sl_pct:
        sl_pct = request.initial_sl_pct
    if request.tp_pct and request.sl_pct:
        if entry_price <= 0:
            raise OKXCLIError(f"exchange position has no usable entry price for {request.inst_id}")
        tp, sl = _protection_prices_from_position(
            entry_price=entry_price,
            direction=direction,
            tp_pct=float(request.tp_pct),
            sl_pct=float(sl_pct or request.sl_pct),
            phase=phase,
        )
    _validate_protection_prices(entry_price=entry_price, direction=direction, tp=tp, sl=sl, phase=phase)
    exchange_position_side = str(position.get("posSide") or position.get("position_side") or request.position_side or "") or None
    trade_mode = str(position.get("mgnMode") or position.get("margin_mode") or request.trade_mode)
    return replace(
        request,
        side="sell" if direction == "LONG" else "buy",
        size=_format_decimal(abs(raw_size)),
        trade_mode=trade_mode,
        tp_trigger_price=tp,
        sl_trigger_price=sl,
        position_side=exchange_position_side,
        sl_pct=sl_pct,
        protection_phase=phase,
    )


def _protection_phase_for_exchange_position(
    request: SwapProtectionRequest,
    *,
    position: dict[str, Any],
    direction: str,
    entry_price: float,
    live_orders: list[dict[str, Any]],
) -> str:
    if not request.protection_enabled or not request.protect_trigger_pct or not request.trail_sl_pct:
        return "initial"
    expected_side = "sell" if direction == "LONG" else "buy"
    matching_order = next(
        (
            order
            for order in live_orders
            if str(order.get("side") or "").lower() == expected_side
            and not _protection_order_predates_position(order, position=position)
        ),
        None,
    )
    live_sl = _numeric((matching_order or {}).get("slTriggerPx"))
    if _live_stop_is_protected(entry_price=entry_price, live_sl=live_sl, direction=direction):
        return "protected"
    mark_price = _numeric(
        position.get("markPx")
        or position.get("mark_price")
        or position.get("last")
        or position.get("lastPx")
        or position.get("last_price")
    )
    favorable_move = _favorable_move_pct(entry_price=entry_price, mark_price=mark_price, direction=direction)
    return "protected" if favorable_move is not None and favorable_move >= float(request.protect_trigger_pct) else "initial"


def _protection_prices_from_position(
    *,
    entry_price: float,
    direction: str,
    tp_pct: float,
    sl_pct: float,
    phase: str,
) -> tuple[str, str]:
    if tp_pct <= 0 or sl_pct <= 0:
        raise OKXCLIError("protection percentages must be positive")
    protected = phase == "protected"
    if direction == "SHORT":
        tp = entry_price * (1 - tp_pct / 100)
        sl = entry_price * (1 - sl_pct / 100) if protected else entry_price * (1 + sl_pct / 100)
    else:
        tp = entry_price * (1 + tp_pct / 100)
        sl = entry_price * (1 + sl_pct / 100) if protected else entry_price * (1 - sl_pct / 100)
    return _format_decimal(tp), _format_decimal(sl)


def _favorable_move_pct(*, entry_price: float, mark_price: float, direction: str) -> float | None:
    if entry_price <= 0 or mark_price <= 0:
        return None
    if direction == "SHORT":
        return (entry_price - mark_price) / entry_price * 100
    return (mark_price - entry_price) / entry_price * 100


def _live_stop_is_protected(*, entry_price: float, live_sl: float, direction: str) -> bool:
    if entry_price <= 0 or live_sl <= 0:
        return False
    return live_sl < entry_price if direction == "SHORT" else live_sl > entry_price


def _protection_order_predates_position(order: dict[str, Any], *, position: dict[str, Any]) -> bool:
    order_created_at = _numeric(order.get("cTime") or order.get("created_at"))
    position_opened_at = _numeric(position.get("cTime") or position.get("opened_at") or position.get("open_time"))
    return order_created_at > 0 and position_opened_at > 0 and order_created_at < position_opened_at


def _validate_protection_prices(*, entry_price: float, direction: str, tp: str, sl: str, phase: str) -> None:
    tp_value = _numeric(tp)
    sl_value = _numeric(sl)
    if tp_value <= 0 or sl_value <= 0:
        raise OKXCLIError("protection trigger prices must be positive")
    if entry_price <= 0:
        return
    if direction == "SHORT" and tp_value >= entry_price:
        raise OKXCLIError("short take-profit must be below the exchange entry price")
    if direction == "LONG" and tp_value <= entry_price:
        raise OKXCLIError("long take-profit must be above the exchange entry price")
    if phase == "protected":
        if direction == "SHORT" and sl_value >= entry_price:
            raise OKXCLIError("protected short stop-loss must be below the exchange entry price")
        if direction == "LONG" and sl_value <= entry_price:
            raise OKXCLIError("protected long stop-loss must be above the exchange entry price")
    else:
        if direction == "SHORT" and sl_value <= entry_price:
            raise OKXCLIError("initial short stop-loss must be above the exchange entry price")
        if direction == "LONG" and sl_value >= entry_price:
            raise OKXCLIError("initial long stop-loss must be below the exchange entry price")


def _position_summary(position: dict[str, Any]) -> dict[str, Any]:
    raw_size = _numeric(position.get("pos") or position.get("size") or position.get("sz"))
    position_side = str(position.get("posSide") or position.get("position_side") or "").lower()
    direction = "SHORT" if position_side in {"short", "sell"} or (position_side not in {"long", "buy"} and raw_size < 0) else "LONG"
    return {
        "position_id": position.get("posId") or position.get("position_id"),
        "direction": direction,
        "size": _format_decimal(abs(raw_size)),
        "entry_price": position.get("avgPx") or position.get("avg_price") or position.get("entry_price"),
        "position_side": position.get("posSide") or position.get("position_side"),
    }


def _is_missing_attached_protection_error(exc: OKXCLIError) -> bool:
    return "51527" in str(exc)


def _closes_entire_position(order: dict[str, Any]) -> bool:
    try:
        if float(order.get("closeFraction") or 0) == 1:
            return True
    except (TypeError, ValueError):
        pass
    return order.get("sz") in (None, "")


def _same_decimal(left: Any, right: Any, *, tolerance: float = 1e-8) -> bool:
    try:
        return abs(float(left) - float(right)) <= tolerance
    except (TypeError, ValueError):
        return str(left) == str(right)


def _numeric(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _cli_position_side(value: Any) -> str | None:
    position_side = str(value or "").strip().lower()
    return position_side if position_side in {"long", "short"} else None


def _format_decimal(value: Any) -> str:
    number = _numeric(value)
    if number.is_integer():
        return str(int(number))
    return f"{number:.12f}".rstrip("0").rstrip(".")
