"""Fresh adversarial coverage for same-candle reversal order ownership.

The reversal's exit and opposite entry may share a candle timestamp, but each
logical leg may reach the broker only once. The entry is permitted as a
distinct role after the exit leg has been acknowledged by Dhan; production
signal flow separately waits for broker-confirmed flat before this handoff.
"""
from __future__ import annotations

from threading import Event, Thread

from execution.live.dhan_transport import DhanRestTransport
from execution.live.engine import LiveExecutionEngine
from execution.models import OrderState
from execution.order_manager import OrderManager
from strategies.types import Signal, SignalType


class SlowAckHttp:
    def __init__(self):
        self.entered = Event()
        self.release = Event()
        self.posts = []

    def _post(self, path, payload, retry_network=False):
        self.posts.append((path, dict(payload)))
        if len(self.posts) == 1:
            self.entered.set()
            assert self.release.wait(timeout=3)
        return {"orderId": f"DHAN-{len(self.posts)}", "orderStatus": "PENDING"}

    def _get(self, path, params=None):
        raise AssertionError(f"unexpected lookup during accepted order: {path}")


def _reversal_signals():
    candle_ts = 1_800_000_900.0
    common = {
        "strategy_type": "supertrend",
        "market_on_signal_close": True,
        "no_separate_stop": True,
        "trigger_state": "FIRED",
        "trigger_source": "completed_signal_candle_close",
    }
    exit_signal = Signal(
        signal_type=SignalType.SHORT, instrument="SILVERM",
        strategy_id="silver_01", timestamp=candle_ts,
        trigger_price=228_500, stop_price=0, quantity=1, side="SHORT",
        metadata={**common, "exit": True, "is_reversal": True},
        lifecycle_id="old-trade", parent_position_id="old-position",
        position_generation=7,
    )
    entry_signal = Signal(
        signal_type=SignalType.SHORT, instrument="SILVERM",
        strategy_id="silver_01", timestamp=candle_ts,
        trigger_price=228_500, stop_price=0, quantity=1, side="SHORT",
        metadata={**common, "is_reversal": True, "is_reversal_entry": True,
                  "reversal_parent_signal_id": exit_signal.signal_id},
    )
    return exit_signal, entry_signal


def test_same_candle_reversal_allows_exit_plus_entry_but_deduplicates_racing_exit():
    http = SlowAckHttp()
    broker = DhanRestTransport(
        client_id="TEST", http=http, gate_enabled=True, product_type="MARGIN",
        instruments={"SILVERM": {
            "security_id": "483080", "exchange_segment": "MCX_COMM",
        }},
    )
    execution = LiveExecutionEngine(broker, clock=lambda: 1_800_000_999.0)
    manager = OrderManager(execution)
    exit_signal, entry_signal = _reversal_signals()
    exit_results = []

    first = Thread(target=lambda: exit_results.append(
        manager.submit_signal(exit_signal, multiplier=5, trade_id="old-trade")))
    second = Thread(target=lambda: exit_results.append(
        manager.submit_signal(exit_signal, multiplier=5, trade_id="old-trade")))
    first.start()
    assert http.entered.wait(timeout=2)
    second.start()
    http.release.set()
    first.join(timeout=3)
    second.join(timeout=3)

    assert not first.is_alive() and not second.is_alive()
    assert sorted(result is None for result in exit_results) == [False, True]
    accepted_exit = next(result for result in exit_results if result is not None)
    assert accepted_exit.order_role == "REVERSAL_EXIT"
    assert accepted_exit.state == OrderState.SUBMITTED
    assert len(http.posts) == 1

    # Simulate Dhan confirming the exit fill/terminal state and the separate
    # broker-flat check succeeding before the paired entry handoff.
    accepted_exit.state = OrderState.FILLED

    # Same candle and direction, but this is the paired entry leg. Its role is
    # distinct, so it is allowed once after the old exit is terminal, not
    # mistaken for a duplicate of the exit.
    entry_order = manager.submit_signal(
        entry_signal, multiplier=5, trade_id="new-trade")
    assert entry_order is not None
    assert entry_order.order_role == "REVERSAL_ENTRY"
    assert entry_order.state == OrderState.SUBMITTED
    assert len(http.posts) == 2
    assert [body["transactionType"] for _, body in http.posts] == ["SELL", "SELL"]
    assert len({body["correlationId"] for _, body in http.posts}) == 2


def test_different_signal_ids_cannot_race_two_working_orders_for_one_instrument():
    http = SlowAckHttp()
    broker = DhanRestTransport(
        client_id="TEST", http=http, gate_enabled=True, product_type="MARGIN",
        instruments={"SILVERM": {
            "security_id": "483080", "exchange_segment": "MCX_COMM",
        }},
    )
    execution = LiveExecutionEngine(broker, clock=lambda: 1_800_000_999.0)
    metadata = {
        "strategy_type": "supertrend",
        "market_on_signal_close": True,
        "trigger_state": "FIRED",
    }
    first_signal = Signal(
        signal_type=SignalType.LONG, instrument="SILVERM",
        strategy_id="silver_01", timestamp=1_800_000_900,
        trigger_price=228_500, stop_price=0, quantity=1, side="LONG",
        metadata=dict(metadata),
    )
    competing_signal = Signal(
        signal_type=SignalType.SHORT, instrument="SILVERM",
        strategy_id="silver_01", timestamp=1_800_001_800,
        trigger_price=228_400, stop_price=0, quantity=1, side="SHORT",
        metadata=dict(metadata),
    )
    first_order = execution.create_order(
        first_signal, multiplier=5, trade_id="trade-first")
    competing_order = execution.create_order(
        competing_signal, multiplier=5, trade_id="trade-competing")
    results = []

    first = Thread(target=lambda: results.append(
        execution.submit_order(first_order)))
    first.start()
    assert http.entered.wait(timeout=2)
    second = Thread(target=lambda: results.append(
        execution.submit_order(competing_order)))
    second.start()
    second.join(timeout=2)
    http.release.set()
    first.join(timeout=3)

    assert not first.is_alive() and not second.is_alive()
    assert len(http.posts) == 1
    assert first_order.state == OrderState.SUBMITTED
    assert competing_order.state == OrderState.REJECTED
    assert competing_order.reason.startswith("INSTRUMENT_ORDER_ALREADY_WORKING:")
