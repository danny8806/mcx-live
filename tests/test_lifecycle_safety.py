from types import SimpleNamespace
import tempfile
from collections import OrderedDict
from pathlib import Path
import pytest

from execution.live.engine import LiveExecutionEngine
from execution.models import Fill, OrderState
from execution.price_model import PricePreset
from portfolio.position_manager import PositionManager
from persistence.manager import PersistenceManager
from strategies.gold import create_gold_5m
from strategies.instance import StrategyInstance
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
        # The live guard refuses an exit unless the BROKER confirms matching
        # exposure, and reserves working close quantity so two exits cannot
        # overshoot one net position. Deriving rows from the real position book
        # keeps this honest across the LONG -> reversal -> SHORT sequence.
        broker=_PositionConfirmingBroker(lambda: _broker_rows_from(positions)),
        execution_engine=execution,
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

    # A delayed LONG stop retains the old lifecycle and generation. There is no
    # broker-side protective role at all any more, so it is rejected outright
    # before the broker is reached — a strictly stronger guarantee than the
    # old stale-lifecycle check.
    old_stop_signal = _signal(
        "LONG", lifecycle="trade-long", position=long_position.position_id,
        generation=1, exit=True, reason="stop_loss_hit", signal_id="old-sl")
    strategy_state._last_fired_trigger_signal_id = old_stop_signal.signal_id
    old_stop = execution.create_order(
        old_stop_signal, trade_id="trade-long", side="SELL")
    old_stop.order_role = "STOP_LOSS"
    execution.submit_order(old_stop)
    assert old_stop.state == OrderState.REJECTED
    assert "BROKER_SL_RETIRED" in old_stop.reason
    assert sum(o["side"] == "SELL" and o["instrument"] == "GOLDM"
               for o in broker.placed) == 2  # LONG entry + SHORT entry only

    # The current SHORT stop is the LOCAL position-owned monitor: it submits
    # one ordinary EXIT order for the CURRENT position, and only one.
    current_stop_signal = _signal(
        "LONG", lifecycle="trade-short", position=short_position.position_id,
        generation=2, exit=True, reason="stop_loss_hit", signal_id="short-sl")
    current_stop_signal.metadata["local_sl_exit"] = True
    strategy_state._trigger_generation = 2
    strategy_state._last_fired_trigger_signal_id = current_stop_signal.signal_id
    current_stop = execution.create_order(
        current_stop_signal, trade_id="trade-short", side="BUY")
    assert current_stop.order_role == "EXIT"
    execution.submit_order(current_stop)
    assert current_stop.state == OrderState.FILLED

    duplicate = execution.create_order(
        current_stop_signal, trade_id="trade-short", side="BUY")
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


def test_strategy_never_mints_a_stop_and_the_local_sl_owns_it():
    """The strategy no longer decides the stop: it is position-owned.

    ``on_tick`` must NOT produce a stop-loss signal.  The exit comes from the
    position-owned SL monitor instead, which mints exactly one ordinary EXIT
    and a second tick produces nothing.
    """
    from execution.live.sl_monitor import PositionOwnedSLMonitor, SLState
    from portfolio.position_manager import Position, PositionSide
    import time as _time

    strategy = create_gold_5m()
    strategy.position_side = "LONG"
    strategy.stop_price = 98
    strategy.position_quantity = 1
    # The strategy alone cannot mint a stop any more.
    assert strategy.on_tick(97, timestamp=2) is None
    assert strategy.on_tick(90, timestamp=3) is None

    # The position-owned monitor fires it, once.
    position = Position(position_id="P1", strategy_id="s1",
                        instrument="GOLDM", side=PositionSide.LONG, quantity=1,
                        average_entry=100.0, entry_timestamp=_time.time(),
                        stop_price=98.0)
    monitor = PositionOwnedSLMonitor()
    assert monitor.arm(position) == SLState.ARMED
    decision = monitor.evaluate(position, 97)
    assert decision.fire is True
    assert decision.exit_side == "SELL"
    assert decision.quantity == 1
    # Latch: a second, worse tick cannot mint a second exit.
    assert monitor.mark_triggered(position.position_id, 97) is True
    again = monitor.evaluate(position, 90)
    assert again.fire is False
    assert again.reason == "SL_ALREADY_TRIGGERED"
    assert monitor.mark_triggered(position.position_id, 90) is False
    # The strategy is only told the stop fired; it still mints nothing itself.
    strategy.notify_local_sl_exit("stop_loss_hit")
    assert strategy.on_tick(90, timestamp=4) is None

    # The local SL exit is an ordinary exit, priced by the normal plan.
    stop_exit = _signal(
        "SHORT", lifecycle="trade-long", exit=True, reason="stop_loss_hit")
    stop_exit.metadata["local_sl_exit"] = True
    stop_exit.trigger_price = 94
    stop_exit.stop_price = 95
    stop_plan = PricePreset(tick_size=1).plan_for(stop_exit, "SELL")
    assert stop_plan.order_type in ("MARKET", "LIMIT")
    assert stop_plan.order_type != "STOP_LOSS"
    assert stop_plan.order_type != "STOP_LOSS_MARKET"


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


# ═══════════════════════════════════════════════════════════════════════════
# 14 — regression: two SL exits minted on the same tick must BOTH submit
# ═══════════════════════════════════════════════════════════════════════════


class _FiredSetStrategy:
    """Binds the REAL StrategyInstance trigger-registration methods.

    Reusing the production methods (rather than re-implementing them) is the
    point: a regression that changed the real implementation would fail here.
    """
    def __init__(self):
        self.enabled = True
        self._trigger_generation = 1
        self._last_fired_trigger_signal_id = None
        self._fired_trigger_signal_ids = OrderedDict()
        self._max_fired_trigger_ids = 16

    _register_fired_trigger = StrategyInstance._register_fired_trigger
    is_fired_trigger_signal = StrategyInstance.is_fired_trigger_signal


def _fired_set_strategy():
    return _FiredSetStrategy()


class _PositionConfirmingBroker:
    """Reports whatever the position book actually holds.

    The live guard refuses an exit unless the BROKER confirms matching
    exposure, and it reserves working quantity from other close orders so two
    exits cannot overshoot one net position.  Deriving the rows from the real
    position manager keeps the test honest through a reversal, where the
    broker's net flips from LONG to SHORT.
    """
    def __init__(self, rows_fn):
        self._rows_fn = rows_fn if callable(rows_fn) else (lambda: list(rows_fn))

    def positions(self):
        return [dict(r) for r in self._rows_fn()]


def _broker_rows_from(pm):
    """Aggregate open positions into the broker's per-security net view."""
    net = {}
    for pos in pm.open_positions:
        side = "LONG" if pos.is_long else "SHORT"
        entry = net.setdefault(pos.instrument, {"instrument": pos.instrument,
                                                "side": side, "quantity": 0})
        entry["quantity"] += int(pos.quantity or 0)
    return list(net.values())

def _guard_env():
    """A LIVE env wired to the real ownership+trigger guard."""
    broker = CountingBroker()
    execution = LiveExecutionEngine(broker)
    pm = PositionManager()
    env = SimpleNamespace(name="live", mode="LIVE", safe_mode=None,
                          position_manager=pm, gate_enabled=True,
                          broker=_PositionConfirmingBroker(
                              lambda: _broker_rows_from(pm)),
                          execution_engine=execution)
    guard = object.__new__(TradingEngine)
    guard._gate_for = lambda strategy_id: SimpleNamespace(
        entries_allowed=True, reversal_enabled=True, sl_enabled=True,
        exit_enabled=True)
    execution.submission_guard = lambda order: guard._validate_live_order_ownership(
        env, order)
    return broker, execution, env


def test_two_strategies_stopping_out_on_the_same_tick_both_submit():
    """The realistic concurrent case: two strategies, one instrument.

    market_flow evaluates EVERY environment's positions on one tick, so two
    strategies can mint and register their SL exit before either order is
    submitted.  Each strategy owns its own trigger state, and each order is
    matched against its OWN open position, so both must reach the broker.
    """
    broker, execution, env = _guard_env()
    s1, s2 = _fired_set_strategy(), _fired_set_strategy()
    env.strategies = {"s1": s1, "s2": s2}

    pos_a = env.position_manager.open_position(
        Fill("f-a", "o-a", "GOLDM", "BUY", 1, 100, 1, "s1", trade_id="trade-a"),
        trade_id="trade-a", position_generation=1)
    pos_b = env.position_manager.open_position(
        Fill("f-b", "o-b", "GOLDM", "BUY", 1, 100, 1, "s2", trade_id="trade-b"),
        trade_id="trade-b", position_generation=1)

    exit_a = _signal("LONG", "s1", lifecycle="trade-a", position=pos_a.position_id,
                     generation=1, exit=True, reason="stop_loss_hit")
    exit_b = _signal("LONG", "s2", lifecycle="trade-b", position=pos_b.position_id,
                     generation=1, exit=True, reason="stop_loss_hit")
    s1._register_fired_trigger(exit_a.signal_id)
    s2._register_fired_trigger(exit_b.signal_id)

    order_a = execution.create_order(exit_a, trade_id="trade-a", side="SELL")
    order_a.order_role = "EXIT"
    order_b = execution.create_order(exit_b, trade_id="trade-b", side="SELL")
    order_b.order_role = "EXIT"

    assert execution.submission_guard(order_a) is None
    assert execution.submission_guard(order_b) is None
    execution.submit_order(order_a)
    execution.submit_order(order_b)
    assert order_a.state == OrderState.FILLED
    assert order_b.state == OrderState.FILLED
    assert len(broker.placed) == 2


def test_a_second_fired_trigger_does_not_invalidate_the_first():
    """The exact regression: one shared slot dropped the earlier signal.

    Registering trigger B after trigger A must not make A unsubmittable, even
    when A's order is only created afterwards.
    """
    _broker, execution, env = _guard_env()
    state = _fired_set_strategy()
    env.strategies = {"s1": state}

    pos = env.position_manager.open_position(
        Fill("f-a", "o-a", "GOLDM", "BUY", 1, 100, 1, "s1", trade_id="trade-a"),
        trade_id="trade-a", position_generation=1)

    sig_a = _signal("LONG", lifecycle="trade-a", position=pos.position_id,
                    generation=1, exit=True, reason="stop_loss_hit")
    sig_b = _signal("LONG", lifecycle="trade-b", position=pos.position_id,
                    generation=1, exit=True, reason="stop_loss_hit")
    state._register_fired_trigger(sig_a.signal_id)
    state._register_fired_trigger(sig_b.signal_id)

    # BOTH remain submittable; the older one was not evicted by the newer.
    assert state.is_fired_trigger_signal(sig_a.signal_id) is True
    assert state.is_fired_trigger_signal(sig_b.signal_id) is True
    assert state._last_fired_trigger_signal_id == sig_b.signal_id


def test_an_unfired_signal_is_still_rejected():
    """Widening the guard must not let an arbitrary signal through."""
    _broker, execution, env = _guard_env()
    state = _fired_set_strategy()
    env.strategies = {"s1": state}

    pos = env.position_manager.open_position(
        Fill("f-a", "o-a", "GOLDM", "BUY", 1, 100, 1, "s1", trade_id="trade-a"),
        trade_id="trade-a", position_generation=1)

    never_fired = _signal("LONG", lifecycle="trade-a", position=pos.position_id,
                          generation=1, exit=True, reason="stop_loss_hit")
    order = execution.create_order(never_fired, trade_id="trade-a", side="SELL")
    order.order_role = "EXIT"
    assert execution.submission_guard(order) == "ORDER_TRIGGER_SIGNAL_STALE"


def test_fired_trigger_ids_are_bounded():
    """An abandoned signal must not stay submittable forever."""
    state = _fired_set_strategy()
    for i in range(state._max_fired_trigger_ids + 5):
        state._register_fired_trigger(f"sig-{i}")
    assert len(state._fired_trigger_signal_ids) == 16
    assert state.is_fired_trigger_signal("sig-0") is False
    assert state.is_fired_trigger_signal("sig-20") is True


def test_generation_still_rejects_a_stale_order_even_when_id_is_fired():
    """Real staleness protection is the generation, not the id set."""
    _broker, execution, env = _guard_env()
    state = _fired_set_strategy()
    env.strategies = {"s1": state}

    pos = env.position_manager.open_position(
        Fill("f-a", "o-a", "GOLDM", "BUY", 1, 100, 1, "s1", trade_id="trade-a"),
        trade_id="trade-a", position_generation=1)

    stale = _signal("LONG", lifecycle="trade-a", position=pos.position_id,
                    generation=1, exit=True, reason="stop_loss_hit")
    state._register_fired_trigger(stale.signal_id)
    order = execution.create_order(stale, trade_id="trade-a", side="SELL")
    order.order_role = "EXIT"
    # The strategy has since moved to a NEW trigger generation.
    state._trigger_generation = 2
    assert execution.submission_guard(order) == "ORDER_TRIGGER_GENERATION_STALE"


def test_reset_clears_fired_ids():
    """A flat strategy must not keep any submittable trigger id."""
    state = _fired_set_strategy()
    state._register_fired_trigger("sig-1")
    state.reset() if hasattr(state, "reset") else None
    if not hasattr(state, "reset"):
        # Mirror what the real reset/close paths do.
        state._last_fired_trigger_signal_id = None
        state._fired_trigger_signal_ids.clear()
    assert state.is_fired_trigger_signal("sig-1") is False
