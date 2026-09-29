"""The controlled live signal must stop at the armed-trigger boundary."""
from strategies.instance import StrategyInstance
from strategies.types import PendingEntry, Signal, SignalType, StrategyState


def test_controlled_long_waits_for_threshold_crossing_before_firing():
    strategy = StrategyInstance(
        "gold_01", "GOLDPETAL", "571306", "5m", quantity=1, multiplier=1.0)
    strategy._trigger_generation = 1
    signal = Signal(
        signal_type=SignalType.LONG,
        instrument="GOLDPETAL",
        strategy_id="gold_01",
        timestamp=100.0,
        trigger_price=14909.0,
        stop_price=14859.0,
        quantity=1,
        side="LONG",
        metadata={
            "pending": True,
            "triggered": False,
            "trigger_state": "ARMED",
            "trigger_generation": 1,
            "trigger_source": "market_websocket_ltp",
            "test_mode": True,
            "test_run_id": "validation-run",
        },
    )
    strategy.pending_entry = PendingEntry(
        signal=signal, trigger_price=14909.0, side="LONG",
        created_at=100.0, status="pending")
    strategy.state = StrategyState.PENDING_LONG

    assert strategy.on_tick(14908.0, 101.0) is None
    assert signal.metadata["trigger_state"] == "ARMED"

    fired = strategy.on_tick(14909.0, 102.0)

    assert fired is signal
    assert fired.metadata["pending"] is False
    assert fired.metadata["triggered"] is True
    assert fired.metadata["trigger_state"] == "FIRED"
    assert fired.metadata["trigger_source"] == "market_websocket_ltp"
    assert fired.metadata["trigger_ltp"] == 14909.0
    assert signal.signal_id in strategy._fired_trigger_signal_ids
