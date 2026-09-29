"""An opposite candle supersedes an untriggered local entry without an order."""
from types import SimpleNamespace

import pytest

from core.timeframe_engine import Bar
from strategies.instance import StrategyInstance
from strategies.types import StrategyState
from trading_engine import TradingEngine
from execution.price_model import PricePreset
from strategies.types import Signal, SignalType
from execution.live.engine import LiveExecutionEngine
from execution.models import Order, OrderState
from execution.live.dhan_transport import DhanRestTransport
from core.candle_fetcher import CandleFetcher, IST
from datetime import datetime


@pytest.mark.parametrize("timeout_bars", [0, 50])
def test_opposite_signal_replaces_untriggered_entry(timeout_bars):
    strategy = StrategyInstance("s1", "GOLDM", "123", "5m",
                                pending_timeout_bars=timeout_bars)
    old = strategy._create_triggered_entry_signal(
        "LONG", 100.0, 101.0, 99.0, 1.0, 100.0, 98.0)
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._prev_fast_high = 101.0
    strategy._prev_fast_low = 99.0
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: True
    bar = Bar("GOLDM", "5m", 2.0, 3.0, 100.0, 101.0, 97.0,
              99.0, 1.0)

    new = strategy.on_bar(bar, SimpleNamespace(htf_value=100.0), 100.0)

    assert new is not None
    assert new.signal_id != old.signal_id
    assert old.metadata["trigger_state"] == "CANCELLED"
    assert new.metadata["trigger_state"] == "ARMED"
    assert new.metadata["cancel_inflight"] is True
    assert new.metadata["old_pending_id"] == old.signal_id
    if timeout_bars == 0:
        assert new.metadata["pending_termination"] == "expired"
    assert strategy.pending_entry.signal is new
    assert strategy.state == StrategyState.PENDING_SHORT
    assert strategy.on_tick(old.trigger_price, 4.0) is None
    assert strategy.on_tick(new.trigger_price, 5.0) is new
    assert new.metadata["trigger_state"] == "FIRED"
    assert strategy.on_tick(new.trigger_price, 5.0) is None


def test_expired_trigger_without_replacement_emits_cancel_only_event():
    strategy = StrategyInstance("s1", "GOLDM", "123", "5m",
                                pending_timeout_bars=0)
    old = strategy._create_triggered_entry_signal(
        "LONG", 100.0, 101.0, 99.0, 1.0, 100.0, 98.0)
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: False
    bar = Bar("GOLDM", "5m", 2.0, 3.0, 100.0, 101.0, 97.0,
              99.0, 1.0)

    control = strategy.on_bar(bar, SimpleNamespace(htf_value=100.0), 100.0)

    assert control.metadata["cancel_only"] is True
    assert control.metadata["pending_termination"] == "expired"
    assert control.metadata["old_pending_id"] == old.signal_id
    assert strategy.pending_entry is None
    assert strategy.on_tick(old.trigger_price, 4.0) is None


def test_entry_flat_gate_fails_closed_on_unknown_or_offsetting_broker_rows():
    engine = object.__new__(TradingEngine)
    signal = SimpleNamespace(instrument="GOLDM")
    for broker in (None, SimpleNamespace(positions=lambda: None),
                   SimpleNamespace(positions=lambda: [
                       {"instrument": "GOLDM", "side": "LONG", "quantity": 1},
                       {"instrument": "GOLDM", "side": "SHORT", "quantity": 1},
                   ])):
        flat, _ = engine._broker_flat_for_entry(
            SimpleNamespace(broker=broker), signal)
        assert flat is False

    flat, _ = engine._broker_flat_for_entry(
        SimpleNamespace(broker=SimpleNamespace(positions=lambda: [])), signal)
    assert flat is True


@pytest.mark.parametrize("side,ltp,expected", [
    ("BUY", 100.1, 101.0), ("SELL", 99.9, 99.0),
])
def test_fired_entry_limit_uses_current_tick_not_old_trigger(side, ltp, expected):
    signal = Signal(
        signal_type=SignalType.LONG if side == "BUY" else SignalType.SHORT,
        instrument="GOLDM", strategy_id="s1", timestamp=1.0,
        trigger_price=100.0, stop_price=95.0, quantity=1,
        metadata={"triggered": True, "trigger_state": "FIRED",
                  "trigger_ltp": ltp},
    )
    plan = PricePreset(tick_size=1.0).plan_for(signal, side)
    assert plan.order_type == "LIMIT"
    assert plan.price == expected


@pytest.mark.parametrize("side,ltp,expected", [
    ("SELL", 94.2, 94.0), ("BUY", 105.2, 106.0),
])
def test_stop_exit_limit_is_marketable_after_gap(side, ltp, expected):
    signal = Signal(
        signal_type=SignalType.SHORT if side == "SELL" else SignalType.LONG,
        instrument="GOLDM", strategy_id="s1", timestamp=1.0,
        trigger_price=ltp, stop_price=95.0 if side == "SELL" else 105.0,
        quantity=1,
        metadata={"exit": True, "exit_reason": "stop_loss_hit",
                  "trigger_state": "FIRED", "trigger_ltp": ltp},
    )
    plan = PricePreset(tick_size=1.0).plan_for(signal, side)
    assert plan.price == expected


def test_live_opposite_pending_signal_terminalizes_old_without_broker_order():
    strategy = StrategyInstance("s1", "GOLDM", "123", "5m")
    old = strategy._create_triggered_entry_signal(
        "LONG", 100.0, 101.0, 99.0, 1.0, 100.0, 98.0)
    strategy._prev_fast_close = 101.0
    strategy._prev_htf_value = 100.0
    strategy._check_long_cross = lambda *args: False
    strategy._check_short_cross = lambda *args: True
    new = strategy.on_bar(
        Bar("GOLDM", "5m", 2.0, 3.0, 100.0, 101.0, 97.0, 99.0, 1.0),
        SimpleNamespace(htf_value=100.0), 100.0)
    terminalized = []
    armed = []
    runtime = SimpleNamespace(lifecycle=None, order_manager=SimpleNamespace(
        submit_signal=lambda *a, **kw: pytest.fail("pending signal sent an order")),
        position_manager=None, current_trade_id=None)
    env = SimpleNamespace(
        name="live", mode="LIVE", is_live=True,
        strategies={"s1": strategy},
        runtimes=SimpleNamespace(require=lambda sid: runtime,
                                 get=lambda sid: runtime),
        persistence=SimpleNamespace(terminalize_pending_order=lambda *a, **kw:
                                    terminalized.append((a, kw))),
        execution_engine=SimpleNamespace(_orders={}),
    )
    engine = object.__new__(TradingEngine)
    engine._env_for = lambda name: env
    engine._gate_for = lambda sid: SimpleNamespace(entries_allowed=True)
    engine._validate_strategy_risk_gate = lambda *a: (True, "")
    engine._rollover_blocked = set()
    engine._persist_signal = lambda *a: None
    engine.publish_event = lambda *a, **kw: None
    engine._notify_signal = lambda *a: None
    engine._arm_live_pending = lambda sig, _env: armed.append(sig.signal_id)

    engine._process_signal(new, "live")

    assert terminalized == [((old.signal_id,), {
        "status": "cancelled_by_reversal",
        "reason": "opposite_crossover_superseded",
    })]
    assert armed == [new.signal_id]
    assert strategy.pending_entry.signal is new


@pytest.mark.parametrize("status,should_cancel", [
    ("filled", False), ("unknown", False), ("cancelled", True),
])
def test_cancel_race_does_not_mark_filled_order_cancelled(status, should_cancel):
    broker = SimpleNamespace(cancel_order=lambda bid: {
        "ok": status != "unknown", "status": status})
    engine = LiveExecutionEngine(broker)
    order = Order("o1", "s1", "GOLDM", "BUY", 1,
                  state=OrderState.SUBMITTED)
    order._broker_order_id = "b1"
    engine._orders[order.order_id] = order

    assert engine.cancel_order(order.order_id) is should_cancel
    assert (order.state == OrderState.CANCELED) is should_cancel


def test_dhan_positions_query_failure_is_not_treated_as_flat():
    transport = DhanRestTransport.__new__(DhanRestTransport)
    import threading
    transport._lock = threading.RLock()
    transport._http = SimpleNamespace(_get=lambda path: (_ for _ in ()).throw(
        ConnectionError("offline")))
    transport._audit = lambda *a, **kw: None

    with pytest.raises(RuntimeError, match="positions query failed"):
        transport.positions()


@pytest.mark.parametrize("broker_status,expected_ok", [
    ("CANCELLED", True), ("TRADED", False), ("PENDING", False),
])
def test_dhan_cancel_requires_verified_broker_status(broker_status, expected_ok):
    transport = DhanRestTransport.__new__(DhanRestTransport)
    import threading
    transport._lock = threading.RLock()
    transport._orders = {"b1": {"raw_status": "PENDING", "status": "submitted"}}
    transport._http_delete = lambda path: {"orderId": "b1"}
    transport._http = SimpleNamespace(_get=lambda path: {
        "orderStatus": broker_status})
    transport._audit = lambda *a, **kw: None

    result = transport.cancel_order("b1")

    assert result["ok"] is expected_ok
    assert result["status"] == ("cancelled" if expected_ok else
                                ("filled" if broker_status == "TRADED" else
                                 "submitted"))


def test_next_candle_close_rolls_into_next_day():
    fetcher = CandleFetcher(None, {}, lambda bar: None)
    now = datetime(2026, 9, 29, 23, 59, 30, tzinfo=IST)
    next_close = fetcher._next_minute_with_residue(now, 5, 0)

    assert next_close == datetime(2026, 9, 30, 0, 0, tzinfo=IST)
    assert fetcher._seconds_until_next_close(now) == 30.0
