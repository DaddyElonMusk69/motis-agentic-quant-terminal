from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass(frozen=True, slots=True)
class SignalEnvelope:
    signal_id: str
    signal_engine_id: str
    signal_engine_version: str
    asset: str
    instrument: str
    timestamp: str
    data_refs: list[str]
    payload_schema: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StrategyContext:
    signal: SignalEnvelope
    runtime_mode: Literal["backtest", "paper", "live"]
    parameters: dict[str, Any] = field(default_factory=dict)
    raw_data: dict[str, Any] = field(default_factory=dict)
    derived_features: dict[str, Any] = field(default_factory=dict)
    portfolio_state: dict[str, Any] = field(default_factory=dict)
    prior_strategy_state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    decision_id: str
    strategy_id: str
    strategy_version: str
    signal_id: str
    trade_action: Literal["ENTER", "SKIP"]
    direction: Literal["LONG", "SHORT", "FLAT"]
    confidence: float
    reason_code: str
    execution_profile: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not 0 <= self.confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")
        validate_strategy_decision(self)

    @property
    def action(self) -> Literal["ENTER", "SKIP"]:
        return self.trade_action


def validate_strategy_decision(decision: StrategyDecision) -> StrategyDecision:
    if decision.trade_action == "SKIP" and decision.direction != "FLAT":
        raise ValueError("SKIP decisions must use FLAT direction")
    if decision.trade_action == "ENTER" and decision.direction not in {"LONG", "SHORT"}:
        raise ValueError("ENTER decisions must use LONG or SHORT direction")
    return decision
