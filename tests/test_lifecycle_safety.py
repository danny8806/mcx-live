from types import SimpleNamespace

from execution.live.engine import LiveExecutionEngine
from execution.models import Fill, OrderState
from portfolio.position_manager import PositionManager
from strategies.types import Signal, SignalType
from trading_engine import TradingEngine


class CountingBroker:
    def __init__(self):
        self.placed = []

    def place_market_order(self, **order):
        self.placed.append(order)
        return {
            "broker_order_id": f"B{len(self.placed)}",
            "status": "filled",
            "quantity": order["quantity"],
            "price": 100.0,
        }

    def update_price(self, instrument, price):
        pass


def _signal(side, strategy="s1", *, lifecycle=None, position=None,
            generation=None, exit=False, reason=None, signal_id=None):
    sig = Signal(
        signal_type=SignalType.LONG if side == "LONG" else SignalType.SHORT,
        instrument="GOLDM", strategy_id=strategy, timestamp=1,
        trigger_price=100, stop_price=95 if side == "LONG" else 105,
        quantity=1, metadata={},
    )
    if signal_id:
        sig.signal_id = signal_id
    sig.lifecycle_id = lifecycle
    sig.parent_position_id = position
    sig.position_generation = generation
    if exit:
        sig.metadata.update(exit=True, exit_reason=reason or "signal_exit")
    return sig


def test_old_long_stop_cannot_close_new_short_and_current_stop_submits_once():
    broker = CountingBroker()
    execution = LiveExecutionEngine(broker)
    positions = PositionManager()
    env = SimpleNamespace(
        name="live", mode="LIVE", safe_mode=None, position_manager=positions,
    )
    guard = object.__new__(TradingEngine)
    execution.submission_guard = lambda order: guard._validate_live_order_ownership(
        env, order)

    # LONG entry and its first position generation.
    long_signal = _signal("LONG", lifecycle="trade-long")
    long_entry = execution.create_order(long_signal, trade_id="trade-long")
    long_entry.order_role = "ENTRY"
    execution.submit_order(long_entry)
    assert long_entry.state == OrderState.FILLED
    long_fill = Fill("f-long", long_entry.order_id, "GOLDM", "BUY", 1,
                     100, 2, "s1", trade_id="trade-long")
    long_position = positions.open_position(
        long_fill, trade_id="trade-long", position_generation=1)

    # Reversal exit is tied to the exact LONG position and fully closes it.
    reversal_exit_signal = _signal(
        "SHORT", lifecycle="trade-long", position=long_position.position_id,
        generation=1, exit=True, reason="reversal_exit")
    long_exit = execution.create_order(
        reversal_exit_signal, trade_id="trade-long", side="SELL")
    long_exit.order_role = "REVERSAL_EXIT"
    execution.submit_order(long_exit)
    assert long_exit.state == OrderState.FILLED
    positions.close_position(
        long_position.position_id,
        Fill("f-long-exit", long_exit.order_id, "GOLDM", "SELL", 1,
             100, 3, "s1", trade_id="trade-long"), "reversal")

    # Confirmed flat permits a new, separately owned SHORT lifecycle.
    short_signal = _signal("SHORT", lifecycle="trade-short")
    short_entry = execution.create_order(short_signal, trade_id="trade-short")
    short_entry.order_role = "REVERSAL_ENTRY"
    execution.submit_order(short_entry)
    assert short_entry.state == OrderState.FILLED
    short_fill = Fill("f-short", short_entry.order_id, "GOLDM", "SELL", 1,
                      100, 4, "s1", trade_id="trade-short")
    short_position = positions.open_position(
        short_fill, trade_id="trade-short", position_generation=2)

    # A delayed LONG stop retains the old lifecycle and generation. It must
    # be rejected before it reaches the broker.
    old_stop_signal = _signal(
        "LONG", lifecycle="trade-long", position=long_position.position_id,
        generation=1, exit=True, reason="stop_loss_hit", signal_id="old-sl")
    old_stop = execution.create_order(
        old_stop_signal, trade_id="trade-long", side="SELL")
    old_stop.order_role = "STOP_LOSS"
    execution.submit_order(old_stop)
    assert old_stop.state == OrderState.REJECTED
    assert "STALE_LIFECYCLE_TRIGGER_REJECTED" in old_stop.reason
    assert sum(o["side"] == "SELL" and o["instrument"] == "GOLDM"
               for o in broker.placed) == 2  # LONG entry + SHORT entry only

    # The current SHORT stop owns the new position and can close it once.
    current_stop_signal = _signal(
        "LONG", lifecycle="trade-short", position=short_position.position_id,
        generation=2, exit=True, reason="stop_loss_hit", signal_id="short-sl")
    current_stop = execution.create_order(
        current_stop_signal, trade_id="trade-short", side="BUY")
    current_stop.order_role = "STOP_LOSS"
    execution.submit_order(current_stop)
    assert current_stop.state == OrderState.FILLED

    duplicate = execution.create_order(
        current_stop_signal, trade_id="trade-short", side="BUY")
    duplicate.order_role = "STOP_LOSS"
    execution.submit_order(duplicate)
    assert duplicate.state == OrderState.REJECTED
    assert len(broker.placed) == 4  # exactly one current SHORT exit
