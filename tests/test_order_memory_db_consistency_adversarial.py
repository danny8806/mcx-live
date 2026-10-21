"""Fresh failure-oriented tests for memory/SQLite live-order divergence."""
from __future__ import annotations

import threading
from types import SimpleNamespace

from execution.live.engine import LiveExecutionEngine
from execution.models import Order, OrderState
from persistence.manager import PersistenceManager
from execution.live.poller import LiveBrokerPoller
from execution.order_manager import OrderManager
from strategies.types import Signal, SignalType


def _persistence(tmp_path):
    persistence = PersistenceManager(
        state_path=str(tmp_path / "state.json"),
        db_path=str(tmp_path / "live.sqlite"), execution_mode="LIVE")
    persistence.save_signal({
        "signal_id": "S-1", "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "LONG", "signal_type": "LONG",
        "timestamp": 1,
    })
    persistence.save_trade({
        "trade_id": "T-1", "strategy_id": "silver_01",
        "instrument": "SILVERM", "side": "LONG",
        "entry_signal_id": "S-1", "status": "open",
    })
    return persistence


def _order_row(state, updated_at, *, filled=0, average=0.0):
    return {
        "order_id": "O-1", "trade_id": "T-1", "signal_id": "S-1",
        "strategy_id": "silver_01", "instrument": "SILVERM",
        "side": "BUY", "quantity": 1, "order_type": "MARKET",
        "state": state, "filled_quantity": filled,
        "average_fill_price": average,
        "created_at": "2026-10-21T09:00:00+00:00",
        "updated_at": updated_at,
    }


def test_older_order_write_cannot_regress_filled_database_state(tmp_path):
    persistence = _persistence(tmp_path)
    try:
        persistence.save_order(_order_row(
            "filled", "2026-10-21T09:00:02+00:00", filled=1, average=100.0))
        persistence.save_order(_order_row(
            "submitted", "2026-10-21T09:00:01+00:00"))

        [row] = persistence.get_orders("O-1")
        assert row["state"] == "filled"
        assert row["filled_quantity"] == 1
        assert row["average_fill_price"] == 100.0
        assert row["updated_at"] == "2026-10-21T09:00:02+00:00"
    finally:
        persistence.close()


def test_order_and_fill_are_one_sqlite_commit(tmp_path):
    persistence = _persistence(tmp_path)
    try:
        with persistence._tx() as conn:
            conn.execute("""
                CREATE TRIGGER reject_test_fill BEFORE INSERT ON fills
                BEGIN SELECT RAISE(ABORT, 'injected fill write failure'); END
            """)
        fill = {
            "fill_id": "F-ATOMIC", "order_id": "O-1",
            "strategy_id": "silver_01", "instrument": "SILVERM",
            "side": "BUY", "quantity": 1, "price": 100.0,
            "timestamp": "2026-10-21T09:00:02+00:00", "trade_id": "T-1",
        }

        try:
            persistence.save_order_and_fill(
                _order_row("filled", "2026-10-21T09:00:02+00:00", filled=1,
                           average=100.0), fill)
        except Exception as exc:
            assert "injected fill write failure" in str(exc)
        else:
            raise AssertionError("expected injected fill write failure")

        assert persistence.get_orders("O-1") == []
        assert persistence.get_fills("F-ATOMIC") == []
    finally:
        persistence.close()


def _engine_with_order(state, updated_at, *, filled=0):
    engine = LiveExecutionEngine.__new__(LiveExecutionEngine)
    engine._orders = {
        "O-1": Order(
            order_id="O-1", strategy_id="silver_01", instrument="SILVERM",
            side="BUY", quantity=1, state=state, filled_quantity=filled,
            updated_at=updated_at),
    }
    engine._lock = threading.RLock()
    engine.broker_router = None
    engine._broker_fill_map = {}
    return engine


def test_newer_filled_database_row_repairs_stale_snapshot_order():
    engine = _engine_with_order(OrderState.SUBMITTED, 1_792_570_000.0)
    count = engine.restore_orders_from_persistence([{
        **_order_row("filled", "2026-10-21T09:00:02+00:00", filled=1,
                     average=100.0),
        "execution_mode": "LIVE", "broker_order_id": "DHAN-1",
    }])

    assert count == 1
    order = engine.get_order("O-1")
    assert order.state == OrderState.FILLED
    assert order.filled_quantity == 1
    assert order._broker_order_id == "DHAN-1"


def test_newer_memory_order_is_not_overwritten_by_older_database_copy():
    engine = _engine_with_order(OrderState.SUBMITTED, 1_792_580_000.0)
    count = engine.restore_orders_from_persistence([{
        **_order_row("filled", "2026-10-21T09:00:02+00:00", filled=1,
                     average=100.0),
        "execution_mode": "LIVE",
    }])

    assert count == 0
    assert engine.get_order("O-1").state == OrderState.SUBMITTED
    assert engine.get_order("O-1").filled_quantity == 0


def test_broker_fill_id_is_restored_as_restart_dedup_key():
    engine = _engine_with_order(OrderState.SUBMITTED, 1_792_580_000.0)
    restored = engine.restore_broker_fill_ids_from_persistence([{
        "execution_mode": "LIVE", "broker_fill_id": "BF-1",
        "fill_id": "LIVE-FILL-1",
    }])

    assert restored == 1
    assert engine._broker_fill_map == {"BF-1": "LIVE-FILL-1"}


def test_failed_fill_persistence_is_retried_without_a_new_broker_event():
    class Persistence:
        def __init__(self):
            self.fail_once = True
            self.fills = []

        def save_order(self, _row):
            if self.fail_once:
                self.fail_once = False
                raise OSError("injected disk-full failure")

        def save_fill(self, row):
            self.fills.append(row)

    order = SimpleNamespace(
        order_id="O-1", strategy_id="silver_01", instrument="SILVERM",
        side="BUY", quantity=1, order_type="MARKET", price=100.0,
        trigger_price=None, planned_entry_price=100.0, planned_sl=None,
        planned_order_type="MARKET", order_role="ENTRY", protected_order_id=None,
        correlation_id="MCX-1", state=OrderState.FILLED, filled_quantity=1,
        average_fill_price=100.0, created_at=1.0, updated_at=2.0,
        entry_signal_id="S-1", trade_id="T-1", _broker_order_id="D-1",
        lifecycle_id="T-1", parent_position_id=None, position_id="P-1",
        position_generation=1,
    )
    fill = SimpleNamespace(
        fill_id="F-1", order_id="O-1", strategy_id="silver_01",
        instrument="SILVERM", side="BUY", quantity=1, price=100.0,
        timestamp=2.0, trade_id="T-1", entry_signal_id="S-1",
        broker_fill_id="BF-1", broker_order_id="D-1", broker_trade_id="BT-1",
        cumulative_filled_quantity=1, position_id="P-1",
        lifecycle_id="T-1", position_generation=1,
    )
    persistence = Persistence()
    engine = SimpleNamespace(get_order=lambda _oid: order)
    env = SimpleNamespace(name="live", persistence=persistence,
                          execution_engine=engine,
                          broker=SimpleNamespace(order_statuses=lambda: {}))
    poller = LiveBrokerPoller(env)
    poller._fill_persist_retry[fill.fill_id] = fill

    poller._retry_fill_persistence()
    assert fill.fill_id in poller._fill_persist_retry
    assert poller.stats()["cycle"]["order_persist_errors"] == 1

    # A normal poll cycle retries the retained fill even with no new broker
    # status event.
    poller.poll_orders()
    assert poller._fill_persist_retry == {}
    assert poller.stats()["cycle"]["fill_persist_pending"] == 0
    assert [row["fill_id"] for row in persistence.fills] == ["F-1"]


def _signal():
    return Signal(
        signal_type=SignalType.LONG, instrument="SILVERM",
        strategy_id="silver_01", timestamp=1_800.0,
        trigger_price=100.0, stop_price=0.0, quantity=1, side="LONG",
        metadata={"strategy_type": "supertrend", "trigger_state": "FIRED",
                  "trigger_source": "completed_signal_candle_close"},
    )


class _SubmissionDouble:
    mode = "LIVE"

    def __init__(self):
        self._orders = {}
        self.broker_calls = 0

    def create_order(self, signal, multiplier=1.0, trade_id="", side=None):
        order = Order(
            order_id="O-1", strategy_id=signal.strategy_id,
            instrument=signal.instrument, side="BUY", quantity=signal.quantity,
            state=OrderState.CREATED, trade_id=trade_id,
            entry_signal_id=signal.signal_id, lifecycle_id=trade_id,
            parent_signal_id=signal.signal_id, order_role="ENTRY",
            trigger_state="FIRED",
        )
        self._orders[order.order_id] = order
        return order

    def submit_order(self, order):
        self.broker_calls += 1
        order.state = OrderState.SUBMITTED
        return order


def test_live_order_intent_is_persisted_before_broker_submission():
    execution = _SubmissionDouble()
    observations = []

    def persist_before_submit(order, _signal):
        observations.append((order.state, execution.broker_calls))

    manager = OrderManager(execution, before_submit=persist_before_submit)
    order = manager.submit_signal(_signal(), multiplier=5, trade_id="T-1")

    assert observations == [(OrderState.CREATED, 0)]
    assert execution.broker_calls == 1
    assert order.state == OrderState.SUBMITTED


def test_live_order_is_not_sent_when_durable_intent_write_fails():
    execution = _SubmissionDouble()

    def fail_persistence(_order, _signal):
        raise OSError("injected database outage")

    manager = OrderManager(execution, before_submit=fail_persistence)
    order = manager.submit_signal(_signal(), multiplier=5, trade_id="T-1")

    assert execution.broker_calls == 0
    assert order.state == OrderState.REJECTED
    assert order.submission_outcome == "NOT_SENT"
    assert order.pre_submit_persistence_failed is True
    assert not manager.get_active_orders()
