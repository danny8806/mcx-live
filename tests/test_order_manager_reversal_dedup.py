from types import SimpleNamespace

from execution.models import Order, OrderState
from execution.order_manager import OrderManager
from strategies.types import Signal, SignalType, resolve_order_role


class _Execution:
    def __init__(self):
        self.orders = {}

    def create_order(self, signal, multiplier=1.0, trade_id="", side=None):
        role = resolve_order_role(signal)
        order = Order(
            order_id=f"O-{len(self.orders) + 1}",
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=side or ("SELL" if signal.side == "SHORT" else "BUY"),
            quantity=signal.quantity,
            order_role=role,
            state=OrderState.CREATED,
            trade_id=trade_id,
            multiplier=multiplier,
        )
        self.orders[order.order_id] = order
        return order

    def submit_order(self, order):
        order.state = OrderState.SUBMITTED
        return order

    def get_fills(self, strategy_id=None):
        return []


def _signal(*, exit_leg=False):
    metadata = ({"exit": True, "is_reversal": True}
                if exit_leg else {"is_reversal": True, "is_reversal_entry": True})
    return Signal(
        signal_type=SignalType.SHORT,
        instrument="SILVERM",
        strategy_id="silver_01",
        timestamp=1790757900.0,
        trigger_price=228621.0,
        stop_price=229305.0,
        quantity=1,
        side="SHORT",
        metadata=metadata,
    )


def test_reversal_exit_and_paired_entry_share_candle_but_not_dedup_key():
    manager = OrderManager(_Execution())
    exit_signal = _signal(exit_leg=True)
    entry_signal = _signal()

    exit_order = manager.submit_signal(exit_signal, trade_id="old-trade")
    entry_order = manager.submit_signal(entry_signal, trade_id="new-trade")

    assert exit_order is not None and exit_order.order_role == "REVERSAL_EXIT"
    assert entry_order is not None and entry_order.order_role == "REVERSAL_ENTRY"
    assert manager.submit_signal(_signal(), trade_id="duplicate-new-trade") is None


def test_remove_pending_uses_same_role_aware_dedup_key():
    manager = OrderManager(_Execution())
    exit_signal = _signal(exit_leg=True)
    assert manager.submit_signal(exit_signal, trade_id="old-trade") is not None
    manager.remove_pending(exit_signal)

    # Clearing the EXIT cannot accidentally clear or collide with the new
    # REVERSAL_ENTRY, even though both legs use the same candle timestamp.
    assert manager.submit_signal(_signal(), trade_id="new-trade") is not None


def test_old_reversal_signal_stays_deduplicated_while_broker_order_is_pending():
    manager = OrderManager(_Execution())
    signal = _signal()

    order = manager.submit_signal(signal, trade_id="new-trade")
    assert order is not None and order.state == OrderState.SUBMITTED

    # The fixture candle is intentionally older than the old one-hour cleanup
    # threshold. Age must not release a live broker order's dedup key.
    assert manager.submit_signal(_signal(), trade_id="duplicate-trade") is None
