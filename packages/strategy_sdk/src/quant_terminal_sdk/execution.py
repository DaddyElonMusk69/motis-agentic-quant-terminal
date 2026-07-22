from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

TradeDirection = Literal["LONG", "SHORT", "FLAT"]
OrderAction = Literal[
    "ENTER",
    "ENTER_LONG",
    "ENTER_SHORT",
    "SKIP",
    "HOLD",
    "EXIT",
    "REDUCE",
    "PYRAMID",
    "UPDATE_PROTECTION",
    "BLOCKED",
]
SchedulerStatus = Literal["stopped", "running"]


@dataclass(frozen=True, slots=True)
class RiskLimits:
    max_notional_usd: float
    max_daily_loss_usd: float

    def __post_init__(self) -> None:
        if self.max_notional_usd <= 0:
            raise ValueError("max_notional_usd must be positive")
        if self.max_daily_loss_usd <= 0:
            raise ValueError("max_daily_loss_usd must be positive")


@dataclass(frozen=True, slots=True)
class ExecutionSetup:
    schema_version: str
    source: str
    account_mode: str
    execution_adapter: str
    forward_hours: int
    hard_exit_after_hours: int
    stage4_candidate_id: str | None = None
    setup: dict[str, Any] | None = None
    cost_assumptions: dict[str, Any] | None = None
    slice_windows: list[dict[str, Any]] | None = None

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ExecutionSetup":
        nested = value.get("setup") if isinstance(value.get("setup"), dict) else {}
        forward_hours = int(value.get("forward_hours") or nested.get("forward_hours"))
        hard_exit_after_hours = int(
            value.get("hard_exit_after_hours")
            or nested.get("hard_exit_after_hours")
            or forward_hours
        )
        return cls(
            schema_version=str(value.get("schema_version") or "0.1"),
            source=str(value.get("source") or "unknown"),
            account_mode=str(value.get("account_mode") or "live"),
            execution_adapter=str(value.get("execution_adapter") or "okx"),
            forward_hours=forward_hours,
            hard_exit_after_hours=hard_exit_after_hours,
            stage4_candidate_id=value.get("stage4_candidate_id"),
            setup=value.get("setup") if isinstance(value.get("setup"), dict) else {},
            cost_assumptions=value.get("cost_assumptions") if isinstance(value.get("cost_assumptions"), dict) else {},
            slice_windows=value.get("slice_windows") if isinstance(value.get("slice_windows"), list) else [],
        )


@dataclass(frozen=True, slots=True)
class PositionContext:
    instrument: str | None
    direction: Literal["LONG", "SHORT"]
    side: Literal["long", "short"]
    size: str
    raw_size: str
    entry_price: str | None
    opened_at: str | None
    age_hours: float | None
    hard_exit_after_hours: float | None

    def is_hard_time_gate_expired(self) -> bool:
        if self.age_hours is None or self.hard_exit_after_hours is None:
            return False
        return self.age_hours >= self.hard_exit_after_hours


@dataclass(frozen=True, slots=True)
class ExecutionStrategyBundle:
    bundle_id: str
    asset: str
    instrument: str
    signal_engine_id: str
    signal_engine_version: str
    strategy_id: str
    strategy_version: str
    source_stage1_session_id: str
    bundle_uri: str
    strategy_module_ref: str
    execution_setup: dict[str, Any]
    risk_limits: RiskLimits
    evidence_refs: dict[str, Any]
    content_hash: str
    status: Literal["draft", "promoted", "retired", "revoked"] = "promoted"


@dataclass(frozen=True, slots=True)
class DeploymentRoute:
    route_id: str
    strategy_id: str
    strategy_version: str
    signal_engine_id: str
    signal_engine_version: str
    asset: str
    instrument: str
    account_mode: str
    execution_adapter: str
    risk_limits: RiskLimits
    promoted: bool
    data_warmed: bool
    manually_armed: bool
    enabled: bool
    exchange_account: str = "default"
    cron_interval_minutes: int = 15
    margin_allocation_pct: float = 10.0
    leverage: float = 1.0
    scheduler_status: SchedulerStatus = "stopped"
    auto_submit_enabled: bool = False
    last_wake_at: str | None = None
    last_wake_id: str | None = None
    next_wake_at: str | None = None
    last_lifecycle_error: dict[str, Any] | None = None
    active_bundle_id: str | None = None

    def blockers(self) -> list[str]:
        blockers: list[str] = []
        if not self.enabled:
            blockers.append("route_disabled")
        if self.account_mode == "live":
            if not self.promoted:
                blockers.append("route_not_promoted")
            if not self.data_warmed:
                blockers.append("data_not_warmed")
            if not self.manually_armed:
                blockers.append("route_not_manually_armed")
        return blockers

    def can_execute_live(self) -> bool:
        return self.account_mode == "live" and not self.blockers()


@dataclass(frozen=True, slots=True)
class OrderIntent:
    intent_id: str
    route_id: str
    asset: str
    instrument: str
    side: Literal["buy", "sell"]
    order_type: str
    quantity: str
    reduce_only: bool
    client_order_id: str
    action: OrderAction | None = None
    direction: TradeDirection | None = None
    signal_id: str | None = None
    notional_usd: float | None = None
    trade_mode: str | None = None
    target_currency: Literal["base_ccy", "quote_ccy", "margin"] | None = None
    leverage: float | int | None = None
    price: str | None = None
    tp: str | None = None
    sl: str | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None
    position_side: str | None = None
    status: Literal["intent_only", "submitted", "submission_failed", "cancelled", "blocked"] = "intent_only"

    def __post_init__(self) -> None:
        if not self.client_order_id:
            raise ValueError("client_order_id is required for idempotent execution")


@dataclass(frozen=True, slots=True)
class ExecutionDecision:
    decision_id: str
    route_id: str
    signal_id: str | None
    action: Literal["ENTER", "ENTER_LONG", "ENTER_SHORT", "SKIP", "BLOCKED"]
    reason_code: str
    order_intents: list[OrderIntent]
    diagnostics: dict[str, Any]
    direction: TradeDirection | None = None
    quantity: str | None = None
    notional_usd: float | None = None
    order_type: str | None = None
    price: str | None = None
    tp: str | None = None
    sl: str | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None


@dataclass(frozen=True, slots=True)
class PositionManagementDecision:
    decision_id: str
    route_id: str
    action: Literal["HOLD", "EXIT", "REDUCE", "PYRAMID", "UPDATE_PROTECTION", "BLOCKED"]
    reason_code: str
    order_intents: list[OrderIntent]
    diagnostics: dict[str, Any]
    direction: TradeDirection | None = None
    quantity: str | None = None
    notional_usd: float | None = None
    order_type: str | None = None
    price: str | None = None
    tp: str | None = None
    sl: str | None = None
    tp_pct: float | None = None
    sl_pct: float | None = None
    reduce_only: bool | None = None


@dataclass(frozen=True, slots=True)
class OwnerState:
    owner_state_id: str
    route_id: str
    asset: str
    instrument: str
    account_mode: str
    owner_strategy_id: str
    owner_strategy_version: str
    bundle_id: str
    position_instance_id: str | None
    opened_from_signal_id: str | None
    position_state: dict[str, Any]
    status: Literal["open", "closed", "unknown"] = "open"


@dataclass(frozen=True, slots=True)
class WakeRun:
    wake_id: str
    route_id: str
    status: Literal["blocked", "ready", "completed", "error"]
    branch: Literal["route_gate", "position_management", "entry_scan", "idle", "error"]
    blockers: list[str]
    summary: dict[str, Any]
