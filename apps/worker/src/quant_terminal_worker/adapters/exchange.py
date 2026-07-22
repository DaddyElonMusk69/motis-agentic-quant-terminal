from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Protocol


class ExchangeAdapterError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SwapOrderRequest:
    inst_id: str
    side: str
    order_type: str
    size: str
    trade_mode: str
    client_order_id: str
    position_side: str | None = None
    price: str | None = None
    target_currency: str | None = None
    tp_trigger_price: str | None = None
    sl_trigger_price: str | None = None
    reduce_only: bool = False

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if not self.client_order_id:
            raise ValueError("client_order_id is required for idempotent live orders")
        if self.target_currency not in {None, "base_ccy", "quote_ccy", "margin"}:
            raise ValueError("target_currency must be base_ccy, quote_ccy, or margin")


@dataclass(frozen=True, slots=True)
class SwapProtectionRequest:
    inst_id: str
    side: str
    size: str
    trade_mode: str
    tp_trigger_price: str
    sl_trigger_price: str
    position_side: str | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None
    protection_phase: str | None = None
    initial_sl_pct: float | None = None
    trail_sl_pct: float | None = None
    protect_trigger_pct: float | None = None
    protection_enabled: bool | None = None

    def __post_init__(self) -> None:
        if self.side not in {"buy", "sell"}:
            raise ValueError("side must be buy or sell")
        if not self.tp_trigger_price or not self.sl_trigger_price:
            raise ValueError("tp_trigger_price and sl_trigger_price are required")


class ExchangeExecutionAdapter(Protocol):
    adapter_id: str

    def readiness_blockers(self) -> list[str]: ...

    def market_candles(self, inst_id: str, *, bar: str, limit: int, after: str | None = None) -> dict[str, Any]: ...

    def snapshot(self, instrument: str) -> dict[str, Any]: ...

    def cancel_order(
        self,
        *,
        instrument: str,
        order_id: str | None = None,
        client_order_id: str | None = None,
    ) -> dict[str, Any]: ...

    def cancel_swap_protection_orders(self, inst_id: str) -> dict[str, Any]: ...

    def place_swap_order(self, request: SwapOrderRequest) -> dict[str, Any]: ...

    def ensure_swap_protection(self, request: SwapProtectionRequest) -> dict[str, Any]: ...

    def set_swap_leverage(
        self,
        *,
        inst_id: str,
        leverage: str,
        margin_mode: str,
        position_side: str | None = None,
    ) -> dict[str, Any]: ...


def build_exchange_adapter(route: dict[str, Any]) -> ExchangeExecutionAdapter:
    adapter_tag = str(route.get("execution_adapter") or "okx").strip().lower()
    if adapter_tag == "okx":
        from quant_terminal_worker.adapters.okx import OKXAdapter

        account_mode = route.get("account_mode")
        profile = route.get("exchange_account")
        return OKXAdapter(
            {
                "backend": "okx_cli",
                "mode": account_mode if account_mode in {"demo", "live"} else os.environ.get("OKX_MODE", "demo"),
                "profile": profile if profile not in {None, "", "default"} else None,
            }
        )
    raise ExchangeAdapterError(f"unsupported execution adapter: {adapter_tag}")
