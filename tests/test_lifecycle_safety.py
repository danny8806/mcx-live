from types import SimpleNamespace
import tempfile
from pathlib import Path
import pytest

from execution.live.engine import LiveExecutionEngine
from execution.models import Fill, OrderState
from execution.price_model import PricePreset
from portfolio.position_manager import PositionManager
from persistence.manager import PersistenceManager
from strategies.gold import create_gold_5m
from strategies.types import Signal, SignalType
from strategies.intent import (
    entry_levels, long_crossover, reversal_levels, short_crossover,
)
from execution.live.order_watcher import OrderWatchRecord, OrderWatcher
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
        quantity=1, metadata={
            "pending": False, "triggered": True, "trigger_state": "FIRED",
            "trigger_generation": generation or 1,
            "trigger_source": "market_websocket_ltp",
        },
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
    guard._gate_for = lambda strategy_id: SimpleNamespace(
        entries_allowed=True, reversal_enabled=True, sl_enabled=True,
        exit_enabled=True)
    strategy_state = SimpleNamespace(
        enabled=True, _trigger_generation=1,
        _last_fired_trigger_signal_id=None)
    execution.submission_guard = lambda order: guard._validate_live_order_ownership(
        env, order)
    env.gate_enabled = True
    env.strategies = {"s1": strategy_state}

    # LONG entry and its first position generation.
    long_signal = _signal("LONG", lifecycle="trade-long")
    strategy_state._last_fired_trigger_signal_id = long_signal.signal_id
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
    strategy_state._last_fired_trigger_signal_id = reversal_exit_signal.signal_id
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
    strategy_state._last_fired_trigger_signal_id = short_signal.signal_id
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
    strategy_state._last_fired_trigger_signal_id = old_stop_signal.signal_id
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
    strategy_state._trigger_generation = 2
    strategy_state._last_fired_trigger_signal_id = current_stop_signal.signal_id
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


def test_dema_atr_strategy_intent_stays_explicit_and_shared():
    assert long_crossover(101, 99, 100, 98)
    assert not long_crossover(101, 99, 100, 100)
    assert short_crossover(99, 101, 100, 102)
    assert not short_crossover(99, 101, 100, 100)

    assert entry_levels("LONG", high=105, low=99,
                        previous_high=104, previous_low=98) == (105, 98)
    assert entry_levels("SHORT", high=105, low=99,
                        previous_high=106, previous_low=100) == (99, 106)
    assert reversal_levels("SHORT", high=105, low=99,
                           previous_high=106, previous_low=100,
                           gap=2) == (99, 97, 106)


def test_live_entry_waits_for_ltp_cross_and_trigger_fires_once():
    strategy = create_gold_5m()
    entry = strategy._entry_signal(
        "LONG", close=101, high=105, low=99, timestamp=1,
        prev_high=104, prev_low=98)

    broker = CountingBroker()
    execution = LiveExecutionEngine(broker)
    assert strategy.execution_model == "local_trigger_limit"
    assert entry.metadata["pending"] is True
    assert entry.metadata["triggered"] is False
    assert entry.metadata["trigger_state"] == "ARMED"
    assert strategy.on_tick(104.99, timestamp=2) is None
    assert broker.placed == []

    fired = strategy.on_tick(105, timestamp=3)
    assert fired is entry
    assert fired.metadata["trigger_state"] == "FIRED"
    restarted = create_gold_5m()
    restarted.restore(strategy.snapshot())
    assert restarted.pending_entry is None
    assert restarted.on_tick(105, timestamp=4) is None
    assert strategy.on_tick(106, timestamp=4) is None
    order = execution.create_order(fired, trade_id="entry-plan")
    assert order.order_type == "LIMIT"
    assert order.planned_sl == 98
    execution.submit_order(order)
    assert len(broker.placed) == 1
    assert broker.placed[0]["order_type"] == "LIMIT"
    assert strategy.on_tick(106, timestamp=5) is None
    assert len(broker.placed) == 1


def test_short_trigger_cross_and_snapshot_restore_fire_once():
    strategy = create_gold_5m()
    signal = strategy._entry_signal(
        "SHORT", close=99, high=105, low=95, timestamp=10,
        prev_high=106, prev_low=96)
    snapshot = strategy.snapshot()
    restored = create_gold_5m()
    restored.restore(snapshot)

    assert restored.pending_entry.signal.signal_id == signal.signal_id
    assert restored.on_tick(95.01, timestamp=11) is None
    fired = restored.on_tick(95, timestamp=12)
    assert fired.signal_id == signal.signal_id
    assert fired.metadata["trigger_state"] == "FIRED"
    assert restored.on_tick(94, timestamp=13) is None


def test_local_stop_loss_fires_once_and_uses_limit_exit_plan():
    strategy = create_gold_5m()
    strategy.position_side = "LONG"
    strategy.stop_price = 98
    strategy.position_quantity = 1
    stop_signal = strategy.on_tick(97, timestamp=2)
    assert stop_signal is not None
    assert stop_signal.metadata["exit_reason"] == "stop_loss_hit"
    assert strategy.on_tick(96, timestamp=3) is None

    stop_exit = _signal(
        "SHORT", lifecycle="trade-long", exit=True, reason="stop_loss_hit")
    stop_exit.trigger_price = 94
    stop_exit.stop_price = 95
    stop_plan = PricePreset(tick_size=1).plan_for(stop_exit, "SELL")
    assert (stop_plan.order_type, stop_plan.kind, stop_plan.price) == (
        "LIMIT", "system_sl_exit", 95)

    untriggered = _signal("LONG")
    untriggered.metadata = {"pending": True, "triggered": False}
    with pytest.raises(ValueError, match="triggered"):
        PricePreset().plan_for(untriggered, "BUY")

    missing_trigger_state = _signal("LONG")
    missing_trigger_state.metadata = {"triggered": True}
    with pytest.raises(ValueError, match="FIRED"):
        PricePreset().plan_for(missing_trigger_state, "BUY")


def test_strategy_triggers_are_isolated_and_live_gate_rejects_stale_generation():
    first = create_gold_5m()
    second = create_gold_5m()
    first_signal = first._entry_signal(
        "LONG", 101, 105, 99, 1, 104, 98)
    second_signal = second._entry_signal(
        "SHORT", 99, 105, 95, 1, 106, 96)

    assert first.on_tick(105, timestamp=2) is first_signal
    assert second.pending_entry.signal.signal_id == second_signal.signal_id
    assert second.on_tick(95, timestamp=2) is second_signal

    broker = CountingBroker()
    execution = LiveExecutionEngine(broker)
    guard = object.__new__(TradingEngine)
    guard._gate_for = lambda _strategy_id: SimpleNamespace(
        entries_allowed=True, reversal_enabled=True, sl_enabled=True,
        exit_enabled=True)
    env = SimpleNamespace(
        name="live", mode="LIVE", safe_mode=None, gate_enabled=True,
        position_manager=PositionManager(), strategies={
            first.strategy_id: SimpleNamespace(
                enabled=True, _trigger_generation=first._trigger_generation,
                _last_fired_trigger_signal_id=first_signal.signal_id),
        })
    execution.submission_guard = lambda order: guard._validate_live_order_ownership(
        env, order)
    stale = execution.create_order(first_signal, trade_id="stale-trade")
    stale.trigger_generation -= 1
    execution.submit_order(stale)
    assert stale.state == OrderState.REJECTED
    assert stale.reason == "ORDER_TRIGGER_GENERATION_STALE"
    assert broker.placed == []


def test_pending_trigger_identity_persists_in_sqlite():
    with tempfile.TemporaryDirectory() as temp_dir:
        persistence = PersistenceManager(
            state_path=str(Path(temp_dir) / "state.json"),
            db_path=str(Path(temp_dir) / "trading.db"),
            execution_mode="LIVE")
        persistence.save_pending_order({
            "pending_order_id": "signal-1", "signal_id": "signal-1",
            "strategy_id": "gold_01", "instrument": "GOLDM",
            "side": "LONG", "direction": "LONG", "order_type": "LIMIT",
            "trigger_price": 105, "quantity": 1, "status": "ARMED",
            "trigger_state": "ARMED", "trigger_generation": 7,
            "trigger_source": "market_websocket_ltp", "signal_timestamp": 12.0,
        })
        row = persistence.get_pending_orders(execution_mode="LIVE")[0]
        assert row["strategy_id"] == "gold_01"
        assert row["instrument"] == "GOLDM"
        assert row["trigger_state"] == "ARMED"
        assert row["trigger_generation"] == 7
        persistence.close()


def test_stale_generation_is_cancelled_and_reversal_waits_for_flat_then_entry_cross():
    strategy = create_gold_5m()
    pending = strategy._entry_signal(
        "LONG", close=101, high=105, low=99, timestamp=1,
        prev_high=104, prev_low=98)
    strategy._trigger_generation += 1
    assert strategy.on_tick(105, timestamp=2) is None
    assert pending.metadata["trigger_state"] == "CANCELLED"

    strategy.position_side = "LONG"
    strategy.position_quantity = 1
    strategy.stop_price = 90
    strategy.current_position_id = "pos-long"
    strategy.reversal_entry_gap_points = 2
    reversal = strategy._create_reversal_signal(
        "SHORT", close=101, high=104, low=100, timestamp=3,
        prev_high=103, prev_low=99)
    assert reversal.metadata["pending"] is True
    assert strategy.pending_entry.status == "waiting_for_flat"
    entry_signal = strategy.pending_entry.signal
    snapshot = strategy.snapshot()
    restarted = create_gold_5m()
    restarted.restore(snapshot)
    restored_exit = restarted.on_tick(100, timestamp=3.5)
    assert restored_exit.signal_id == reversal.signal_id
    assert restarted.on_tick(99, timestamp=3.6) is None
    assert strategy.on_tick(101, timestamp=4) is None
    fired_exit = strategy.on_tick(100, timestamp=5)
    assert fired_exit is reversal
    assert fired_exit.metadata["trigger_state"] == "FIRED"
    assert strategy.on_tick(98, timestamp=6) is None  # exit order still in flight
    exiting_restart = create_gold_5m()
    exiting_restart.restore(strategy.snapshot())
    assert exiting_restart.on_tick(100, timestamp=6.5) is None
    assert exiting_restart.pending_entry.status == "waiting_for_flat"

    # A confirmed flat transition activates the opposite entry trigger.
    strategy.position_side = None
    strategy.pending_entry.status = "pending"
    assert strategy.on_tick(99, timestamp=7) is None
    fired_entry = strategy.on_tick(98, timestamp=8)
    assert fired_entry is entry_signal
    assert fired_entry.metadata["trigger_state"] == "FIRED"


def test_limit_fallback_uses_only_remaining_quantity_and_unknown_blocks_market():
    class FakeEngine:
        def __init__(self):
            self._orders = {}
            self.submitted = []

        def cancel_order(self, order_id):
            return True

        def create_order(self, signal, multiplier=1.0, trade_id=""):
            return SimpleNamespace(
                quantity=signal.quantity, metadata=signal.metadata,
                order_id="market-fallback", correlation_id="fallback-corr")

        def submit_order(self, order):
            self.submitted.append(order)

    engine = FakeEngine()
    watcher = OrderWatcher(engine=engine, config={"live": {"order_watcher": {
        "market_fallback_enabled": True, "market_fallback_timeout_ms": 1,
    }}})
    rec = OrderWatchRecord(
        internal_order_id="limit-1", broker_order_id="broker-1",
        strategy_id="s1", trade_id="trade-1", lifecycle_id="trade-1",
        signal_id="signal-1", instrument="GOLDM", side="BUY",
        order_role="ENTRY", requested_quantity=10, filled_quantity=3,
        remaining_quantity=7,
        status="PARTIALLY_FILLED", extra={
            "trigger_state": "FIRED", "trigger_generation": 1,
            "trigger_source": "market_websocket_ltp",
        })
    watcher.verify_order = lambda *_a, **_k: setattr(rec, "status", "CANCELED") or {}
    result = watcher._do_market_fallback(rec, now=100)
    assert result["ok"] is True
    assert engine.submitted[0].quantity == 7
    assert engine.submitted[0].metadata["trigger_state"] == "FIRED"

    engine.submitted.clear()
    rec.status = "SUBMITTED"
    watcher.verify_order = lambda *_a, **_k: setattr(rec, "status", "UNKNOWN") or {}
    result = watcher._do_market_fallback(rec, now=101)
    assert result["ok"] is False
    assert engine.submitted == []

    rec.broker_order_id = None
    result = watcher._do_market_fallback(rec, now=102)
    assert result["ok"] is False
    assert result["error"] == "missing_broker_order_id_blocks_market"
    assert engine.submitted == []
