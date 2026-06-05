import pytest

from quant_terminal_sdk.contracts import StrategyDecision, validate_strategy_decision


def test_strategy_decision_contract_requires_skip_to_be_flat():
    decision = StrategyDecision(
        decision_id="decision-1",
        strategy_id="test-strategy",
        strategy_version="v0.1",
        signal_id="signal-1",
        trade_action="SKIP",
        direction="FLAT",
        confidence=0.4,
        reason_code="unreadable_packet",
    )

    assert validate_strategy_decision(decision) is decision


def test_strategy_decision_contract_rejects_enter_without_direction():
    with pytest.raises(ValueError, match="ENTER decisions must use LONG or SHORT"):
        StrategyDecision(
            decision_id="decision-1",
            strategy_id="test-strategy",
            strategy_version="v0.1",
            signal_id="signal-1",
            trade_action="ENTER",
            direction="FLAT",
            confidence=0.4,
            reason_code="bad_direction",
        )


def test_strategy_decision_contract_keeps_action_alias_for_existing_runners():
    decision = StrategyDecision(
        decision_id="decision-1",
        strategy_id="test-strategy",
        strategy_version="v0.1",
        signal_id="signal-1",
        trade_action="ENTER",
        direction="LONG",
        confidence=0.7,
        reason_code="trend_continuation",
    )

    assert decision.action == "ENTER"
