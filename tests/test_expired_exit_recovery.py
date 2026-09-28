"""Recovery from a terminal exit.

Two guarantees are pinned here:

1. An exit order the broker ends REJECTED / CANCELLED / EXPIRED while its
   position is still open must release the latched local SL, so the next tick
   re-evaluates that position's own stop.  There is no broker-side stop behind
   it any more, so a stuck latch means a permanently unprotected position.
2. A MARKET order may only ever REDUCE exposure.  The engine admits a MARKET
   exit; the entry-side fallback is still refused.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest  # noqa: E402

from execution.live.sl_monitor import SLState  # noqa: E402
from execution.models import OrderState  # noqa: E402
from strategies.types import Signal, SignalType  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
from test_position_owned_sl import (  # noqa: E402
    FakeBroker,
    FakeEnv,
    FakeStrategy,
    Harness,
    make_fill,
    make_pm,
    make_position,
)


class _FakePersistence:
    def __init__(self):
        self.saved: list[str] = []

    def save_position(self, position):
        self.saved.append(getattr(position, "position_id", "?"))


def _exit_order(position, state="expired", role="EXIT", **over):
    """A terminal exit order bound to ``position``'s real lifecycle identity."""
    order = SimpleNamespace(
        order_id="ORD-1",
        order_role=role,
        state=SimpleNamespace(value=state),
        parent_position_id=position.position_id,
        lifecycle_id=position.trade_id,
        trade_id=position.trade_id,
        position_generation=position.position_generation,
        strategy_id=position.strategy_id,
    )
    for key, value in over.items():
        setattr(order, key, value)
    return order


def _poller(env, order, monitor):
    """A poller-shaped object bound to ``env`` with a single terminal order."""
    poller_env = SimpleNamespace(
        name=env.name,
        mode="LIVE",
        strategies=env.strategies,
        position_manager=env.position_manager,
        sl_monitor=monitor,
        persistence=_FakePersistence(),
        execution_engine=SimpleNamespace(_orders={order.order_id: order}),
        broker=env.broker,
    )
    from execution.live.poller import LiveBrokerPoller
    return SimpleNamespace(
        env=poller_env, _stats={},
        _release_terminal_exits=LiveBrokerPoller._release_terminal_exits,
    ), poller_env


def _armed_position(stop=90.0, side="LONG"):
    """A position open in the book with its SL armed and then latched EXITING."""
    h = Harness()
    position = make_position(stop=stop)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([position]), FakeBroker()))
    monitor = h._sl_monitor(env)
    assert monitor.arm(position) == SLState.ARMED
    return h, env, position, monitor


# ═══════════════════════════════════════════════════════════════════════════
# 11 — a terminal exit re-arms the local SL
# ═══════════════════════════════════════════════════════════════════════════


def test_expired_exit_re_arms_the_sl_so_the_open_position_is_protected_again():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    # The stop crosses and the exit is minted, latching the monitor EXITING.
    assert monitor.evaluate(position, 89.0).fire is True
    assert monitor.mark_exiting(position.position_id, "ORD-1") is True
    position.exit_started = True
    position.exit_order_id = "ORD-1"
    assert monitor.state_of(position.position_id) == SLState.EXITING
    # While the exit is in flight the monitor can never mint a second one.
    assert monitor.evaluate(position, 80.0).fire is False

    poller, poller_env = _poller(env, _exit_order(position, "expired"), monitor)
    released = LiveBrokerPoller._release_terminal_exits(poller)

    assert released == 1
    assert poller._stats["sl_rearmed"] == 1
    assert monitor.state_of(position.position_id) == SLState.ARMED
    assert position.exit_started is False
    assert position.exit_order_id is None
    assert position.sl_state == "ARMED"
    assert env.strategies["S1"].stop_exit_submitted is False
    assert poller_env.persistence.saved == [position.position_id]
    # The position is genuinely protectable again: the next tick re-fires
    # against its own stop instead of sitting open and unprotected.
    assert monitor.evaluate(position, 80.0).fire is True


@pytest.mark.parametrize("state", ["expired", "canceled", "cancelled",
                                   "rejected"])
def test_every_terminal_exit_state_re_arms_the_sl(state):
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    position.exit_started = True
    poller, _ = _poller(env, _exit_order(position, state), monitor)
    assert LiveBrokerPoller._release_terminal_exits(poller) == 1
    assert monitor.state_of(position.position_id) == SLState.ARMED


def test_a_triggered_sl_that_never_reached_the_broker_is_also_released():
    """TRIGGERED (stop crossed, no order ever existed) was unreleasable too."""
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    assert monitor.mark_triggered(position.position_id, 89.0) is True
    assert monitor.state_of(position.position_id) == SLState.TRIGGERED
    assert monitor.evaluate(position, 80.0).reason == "SL_ALREADY_TRIGGERED"
    poller, _ = _poller(env, _exit_order(position, "expired"), monitor)
    assert LiveBrokerPoller._release_terminal_exits(poller) == 1
    assert monitor.state_of(position.position_id) == SLState.ARMED
    assert monitor.evaluate(position, 80.0).fire is True


def test_release_is_one_shot_per_latch_so_the_poll_cannot_churn_the_book():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    poller, _ = _poller(env, _exit_order(position, "expired"), monitor)
    assert LiveBrokerPoller._release_terminal_exits(poller) == 1
    # Every subsequent 2 s pass is a no-op.
    assert LiveBrokerPoller._release_terminal_exits(poller) == 0
    assert LiveBrokerPoller._release_terminal_exits(poller) == 0
    assert poller._stats["sl_rearmed"] == 1


def test_a_position_the_exit_really_closed_is_never_resurrected():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    poller, _ = _poller(env, _exit_order(position, "expired"), monitor)
    # The position closes: it leaves open_positions entirely.
    env.position_manager.close_position(
        position.position_id, make_fill(position), "stop_loss_hit")
    assert position.is_open is False
    assert position.sl_state == "CLOSED"
    # The sweep must not touch a closed position's latch: nothing re-arms,
    # because there is no longer any exposure to protect.
    assert LiveBrokerPoller._release_terminal_exits(poller) == 0
    assert monitor.state_of(position.position_id) == SLState.EXITING


def test_release_requires_the_exact_lifecycle_and_generation():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    mismatched = (
        _exit_order(position, "expired", lifecycle_id="some-other-trade"),
        _exit_order(position, "expired", position_generation=999),
        _exit_order(position, "expired", parent_position_id="other-position"),
        _exit_order(position, "expired", parent_position_id=None),
    )
    for order in mismatched:
        poller, _ = _poller(env, order, monitor)
        assert LiveBrokerPoller._release_terminal_exits(poller) == 0
        assert monitor.state_of(position.position_id) == SLState.EXITING


def test_release_ignores_entry_side_orders_and_untouched_positions():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    for role in ("ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"):
        poller, _ = _poller(env, _exit_order(position, "expired", role), monitor)
        assert LiveBrokerPoller._release_terminal_exits(poller) == 0
        assert monitor.state_of(position.position_id) == SLState.EXITING

    # A position whose SL was never armed (no stop available) is left alone
    # rather than being falsely reported as ARMED.
    h2 = Harness()
    naked = make_position(stop=None)
    env2 = h2.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                              make_pm([naked]), FakeBroker()))
    naked_monitor = h2._sl_monitor(env2)
    poller, _ = _poller(env2, _exit_order(naked, "expired"), naked_monitor)
    assert LiveBrokerPoller._release_terminal_exits(poller) == 0
    assert naked_monitor.state_of(naked.position_id) == SLState.NONE


def test_release_never_acts_on_a_live_order():
    from execution.live.poller import LiveBrokerPoller

    h, env, position, monitor = _armed_position()
    monitor.mark_exiting(position.position_id, "ORD-1")
    for state in ("submitted", "acknowledged", "partially_filled", "filled"):
        poller, _ = _poller(env, _exit_order(position, state), monitor)
        assert LiveBrokerPoller._release_terminal_exits(poller) == 0
        assert monitor.state_of(position.position_id) == SLState.EXITING


# ═══════════════════════════════════════════════════════════════════════════
# 12 — a MARKET order may only reduce exposure
# ═══════════════════════════════════════════════════════════════════════════


def _watcher_record(**over):
    from execution.live.order_watcher import OrderWatchRecord

    rec = OrderWatchRecord(
        internal_order_id="ORD-1", broker_order_id="BRK-1",
        strategy_id="S1", instrument="NIFTY", side="SELL",
        order_type="LIMIT", submitted_price=90.0, requested_price=90.0,
        requested_quantity=10, filled_quantity=0, remaining_quantity=10,
        status="SUBMITTED",
    )
    rec.order_role = over.pop("order_role", "EXIT")
    rec.trade_id = "T1"
    rec.lifecycle_id = "T1"
    rec.position_id = "P1"
    rec.position_generation = 0
    rec.signal_id = "SIG-1"
    rec.submitted_at = 0.0
    rec.extra = {"trigger_state": "FIRED", "trigger_generation": 0,
                 "stop_price": 90.0}
    for key, value in over.items():
        setattr(rec, key, value)
    return rec


def _watcher():
    from execution.live.order_watcher import OrderWatcher

    w = OrderWatcher.__new__(OrderWatcher)
    w._clock = lambda: 1000.0
    return w


def test_exit_fallback_keeps_the_exit_side_and_never_crosses_the_position():
    w = _watcher()
    # The record is a SELL (closing a LONG), so the fallback must stay a SELL.
    signal = w._fresh_market_entry(_watcher_record(), quantity=10)
    assert signal.signal_type == SignalType.SHORT
    assert signal.metadata["exit"] is True
    assert signal.metadata["exit_reason"]
    # The trigger is inherited from the exit order being completed, not invented.
    assert signal.metadata["trigger_state"] == "FIRED"
    assert signal.metadata["market_fallback"] is True
    assert signal.metadata["prev_order_id"] == "ORD-1"


def test_entry_fallback_is_not_dressed_up_as_an_exit():
    w = _watcher()
    signal = w._fresh_market_entry(
        _watcher_record(order_role="ENTRY", side="BUY"), quantity=10)
    assert signal.signal_type == SignalType.LONG
    assert "exit" not in signal.metadata
    assert signal.metadata["market_fallback"] is True


def test_fallback_always_uses_the_remaining_quantity():
    w = _watcher()
    rec = _watcher_record(filled_quantity=4)
    assert w._fresh_market_entry(rec).quantity == 6
    assert w._fresh_market_entry(rec, quantity=6).quantity == 6


def test_engine_admits_a_market_exit_but_still_bans_a_market_entry():
    from execution.live.engine import LiveExecutionEngine

    class _B:
        def __init__(self):
            self.placed: list[dict] = []

        def place_market_order(self, **o):
            self.placed.append(o)
            return {"broker_order_id": "B1", "status": "submitted"}

        def update_price(self, instrument, price):
            pass

    def _signal(is_exit, strategy_tracking=True):
        s = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
                   strategy_id="S1", timestamp=1, trigger_price=90.0,
                   stop_price=90.0, quantity=10,
                   metadata={"exit": is_exit, "triggered": True,
                             "trigger_state": "FIRED",
                             "trigger_generation": 0})
        s.lifecycle_id = "T1" if strategy_tracking else "T2"
        s.parent_position_id = "P1"
        s.position_generation = 0
        return s

    broker = _B()
    engine = LiveExecutionEngine(broker)

    # An exit-family MARKET is admitted: it only reduces exposure.
    exit_order = engine.create_order(_signal(True), trade_id="T1", side="SELL")
    exit_order.order_type = "MARKET"
    exit_order.order_role = "EXIT"
    engine.submit_order(exit_order)
    assert exit_order.state == OrderState.SUBMITTED
    assert exit_order.reason is None
    assert [o["order_type"] for o in broker.placed] == ["MARKET"]

    # The entry-side fallback is still refused, so a MARKET can never open.
    entry_order = engine.create_order(_signal(False, False), trade_id="T2")
    entry_order.order_type = "MARKET"
    entry_order.order_role = "FALLBACK_MARKET"
    engine.submit_order(entry_order)
    assert entry_order.state == OrderState.REJECTED
    assert entry_order.reason == "LIVE_MARKET_ENTRY_DISABLED"
    assert len(broker.placed) == 1


# ═══════════════════════════════════════════════════════════════════════════
# 13 — proof: STOP_LOSS is unreachable from ANY signal, and no resting
#      broker-side stop can be created by ANY transport
# ═══════════════════════════════════════════════════════════════════════════


def test_resolve_order_role_can_never_return_stop_loss():
    """A stop-loss reason must classify as an ORDINARY exit.

    If it ever returned "STOP_LOSS" the engine would reject the order and a
    genuine protective exit would be silently DISCARDED, leaving the position
    unprotected — a worse failure than the broker-side stop it replaced.
    """
    from strategies.types import Signal, SignalType, resolve_order_role

    reasons = ["stop_loss_hit", "stop_loss", "sl_triggered", "SL",
               "local_sl_exit", "stop_loss_partial", "sl_exit_failed"]
    for reason in reasons:
        for is_exit in (True, False):
            s = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
                       strategy_id="S1", timestamp=1, trigger_price=1.0,
                       stop_price=1.0, quantity=1,
                       metadata={"exit": is_exit, "exit_reason": reason})
            assert resolve_order_role(s) != "STOP_LOSS", (reason, is_exit)
        # Also with the reason passed explicitly, bypassing metadata.
        s = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
                   strategy_id="S1", timestamp=1, trigger_price=1.0,
                   stop_price=1.0, quantity=1, metadata={})
        assert resolve_order_role(s, exit_reason=reason) != "STOP_LOSS", reason


def test_a_local_sl_exit_resolves_to_ordinary_exit():
    from strategies.types import Signal, SignalType, resolve_order_role

    s = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
               strategy_id="S1", timestamp=1, trigger_price=90.0,
               stop_price=90.0, quantity=10,
               metadata={"exit": True, "local_sl_exit": True,
                         "exit_reason": "stop_loss_hit"})
    assert resolve_order_role(s) == "EXIT"


def test_stub_transport_refuses_a_broker_side_stop_too():
    """A stub that rests a stop would let a retired path pass every local
    test and then place a real resting stop in production."""
    from execution.live.broker_client import StubLiveBroker

    stub = StubLiveBroker(gate_enabled=True)
    for order_type in ("STOP_LOSS", "STOP_LOSS_MARKET"):
        with pytest.raises(ValueError, match="BROKER_SL_RETIRED"):
            stub.place_market_order(side="SELL", quantity=1,
                                    instrument="NIFTY",
                                    order_type=order_type, trigger_price=90.0)
    assert list(stub._orders) == []
    assert stub.fills() == []


def test_watcher_priority_still_orders_a_local_sl_exit_as_an_exit():
    """The retired P1 priority must not be reachable by a live signal."""
    from execution.live.order_watcher import _PRIORITY_RANK

    assert _PRIORITY_RANK["EXIT"] == 2
    # STOP_LOSS keeps a priority slot only for legacy records read back from
    # the DB; resolve_order_role can no longer produce one.
    assert _PRIORITY_RANK["STOP_LOSS"] == 1
