"""Execution contract for the exclusive native 15-minute SuperTrend system."""
import pytest

from execution.price_model import PricePreset
from strategies.types import Signal, SignalType


def _signal(**metadata):
    return Signal(
        signal_type=SignalType.LONG,
        instrument="GOLDM",
        strategy_id="gold_02",
        timestamp=1.0,
        trigger_price=100.0,
        stop_price=0.0,
        quantity=1,
        metadata=metadata,
    )


def test_completed_supertrend_flip_uses_market_order_at_signal_close():
    plan = PricePreset().plan_for(
        _signal(strategy_type="supertrend", market_on_signal_close=True), "BUY")

    assert plan.order_type == "MARKET"
    assert plan.kind == "signal_close_entry_market"
    assert plan.price is None
    assert plan.trigger_price is None


def test_opposite_flip_uses_market_exit_and_does_not_create_stop_order():
    plan = PricePreset().plan_for(
        _signal(strategy_type="supertrend", market_on_signal_close=True,
                exit=True, is_reversal=True), "SELL")

    assert plan.order_type == "MARKET"
    assert plan.kind == "signal_close_exit_market"


@pytest.mark.parametrize("metadata", [
    {},
    {"strategy_type": "supertrend"},
    {"strategy_type": "unsupported", "market_on_signal_close": True},
])
def test_no_other_strategy_or_unclosed_signal_can_reach_execution(metadata):
    with pytest.raises(ValueError):
        PricePreset().plan_for(_signal(**metadata), "BUY")
