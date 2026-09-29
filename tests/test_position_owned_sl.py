"""Local-mock tests for the position-owned SL architecture.

NO network, NO broker, NO live order.  Every test drives the real
``PositionOwnedSLMonitor`` plus the real ``SLFlowMixin`` plumbing against
in-memory fakes only.

Covered invariants (the hard rules from the spec):
  1  no open position        -> no SL can submit
  2  one open position       -> at most one active SL
  3  stop crossed            -> exactly ONE exit signal
  4  duplicate tick          -> no second exit
  5  position closed         -> old SL can never fire
  6  new position            -> old SL can never fire
  7  SL bound to position_id + generation (no cross-strategy / cross-symbol)
  8  exit qty <= open qty
  9  stale DB position       -> cannot arm an SL
  10 no broker-side protective order is ever created or submitted
  11 no stop invention
  12 partial exit re-arms on the remaining quantity only
  13 reversal cannot leave the old SL able to fire
  14 never more than one authoritative exit order
  15 no stop_price on the position -> SL_UNAVAILABLE, position stays open
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from application.sl_flow import SLFlowMixin  # noqa: E402
from execution.live.sl_monitor import (  # noqa: E402
    PositionOwnedSLMonitor,
    SLReject,
    SLState,
)
from execution.models import Fill, OrderState  # noqa: E402
from portfolio.position_manager import (  # noqa: E402
    Position,
    PositionManager,
    PositionManagerFacade,
    PositionSide,
    PositionStatus,
)
from strategies.types import SignalType, resolve_order_role  # noqa: E402


# ── fakes ────────────────────────────────────────────────────────────────────

class FakeStrategy:
    """Minimal stand-in for StrategyInstance as the SL flow sees it."""

    def __init__(self, strategy_id: str, instrument: str):
        self.strategy_id = strategy_id
        self.instrument = instrument
        self._trigger_generation = 7
        self._last_fired_trigger_signal_id = None
        self.sl_exit_notifications: list[str] = []

    def notify_local_sl_exit(self, reason: str = "stop_loss_hit") -> None:
        self.sl_exit_notifications.append(reason)


class FakeExecutionEngine:
    def __init__(self, orders=None):
        self.orders = dict(orders or {})

    def get_order(self, order_id):
        return self.orders.get(order_id)


class FakeBroker:
    """In-memory broker.  Records every submitted order it is handed."""

    def __init__(self, positions=None):
        self._positions = list(positions or [])
        self.submitted: list[dict] = []

    def positions(self):
        return [dict(p) for p in self._positions]

    def submit(self, order: dict) -> str:
        self.submitted.append(order)
        return f"BRK-{len(self.submitted)}"


class FakeEnv:
    def __init__(self, name, strategies, pm, broker, execution_engine=None):
        self.name = name
        self.mode = "LIVE"
        self.strategies = strategies
        self.position_manager = pm
        self.broker = broker
        self.execution_engine = execution_engine
        self.sl_monitor = None


class Harness(SLFlowMixin):
    """Real SLFlowMixin + the few collaborators it needs, all in memory."""

    def __init__(self):
        self.events: list[tuple] = []
        self.persisted: list[str] = []
        self.processed: list = []
        self.envs: dict = {}
        self._order_seq = 0

    # SLFlowMixin collaborators
    def _env_for(self, name=None):
        return self.envs.get(name or next(iter(self.envs), None))

    def _persist_position(self, position, env_name=None):
        self.persisted.append(getattr(position, "position_id", "?"))

    def publish_event(self, event_type, data, env_name=None):
        self.events.append((event_type, data, env_name))

    def _process_signal(self, signal, env_name=None):
        """Stands in for the real execution path: submits an ordinary exit."""
        self.processed.append(signal)
        env = self._env_for(env_name)
        self._order_seq += 1
        order = {
            "order_id": f"ORD-{self._order_seq}",
            "role": resolve_order_role(signal),
            "order_type": "MARKET",
            # The exit side is the OPPOSITE of the position, so the signal
            # type alone determines it.
            "side": "SELL" if signal.signal_type == SignalType.SHORT else "BUY",
            "quantity": signal.quantity,
            "instrument": signal.instrument,
        }
        pid = signal.metadata["position_id"]
        pos = env.position_manager.get_position(pid)
        if pos is not None:
            self._mark_sl_exiting(env, pos, order["order_id"])
        env.broker.submit(order)
        return order

    # helpers
    def events_of(self, event_type):
        return [d for t, d, _ in self.events if t == event_type]

    def add_env(self, env):
        self.envs[env.name] = env
        return env


def make_position(pid="P1", strategy_id="S1", instrument="NIFTY",
                  side=PositionSide.LONG, qty=10, stop=90.0,
                  generation=0, trade_id="T1"):
    return Position(
        position_id=pid, strategy_id=strategy_id, instrument=instrument,
        side=side, quantity=qty, average_entry=100.0,
        entry_timestamp=time.time(), stop_price=stop,
        trade_id=trade_id, position_generation=generation,
    )


_FILL_SEQ = [0]


def make_fill(position, price=None):
    _FILL_SEQ[0] += 1
    return Fill(
        fill_id=f"F-{_FILL_SEQ[0]}", order_id=f"E-{position.position_id}",
        instrument=position.instrument,
        side="BUY" if position.is_long else "SELL",
        quantity=position.quantity,
        price=position.average_entry if price is None else price,
        timestamp=position.entry_timestamp,
        strategy_id=position.strategy_id,
        trade_id=position.trade_id or position.position_id,
    )


def make_pm(positions):
    """Open each position through the REAL facade, so position_id,
    position_generation and entry_order_id are produced by production code.

    The only post-hoc tweak is the position_id: the production code mints a
    UUID, and the tests want stable names.  Re-keying the manager's own dict
    keeps the book and the test objects identical (one object, one id).
    """
    pm = PositionManagerFacade()
    for sid in {p.strategy_id for p in positions}:
        mgr = PositionManager()
        pm.register(sid, mgr)
    for p in positions:
        real = pm.open_position(
            make_fill(p), stop_price=p.stop_price,
            trade_id=p.trade_id or p.position_id,
            position_generation=p.position_generation or 0,
            entry_order_id=f"E-{p.position_id}",
        )
        mgr = pm._managers[p.strategy_id]
        mgr._positions.pop(real.position_id)
        # Keep the production-assigned identity, but let the test's object be
        # the one the book owns so quantity/stop mutations are observed.
        p.position_generation = real.position_generation
        p.entry_order_id = real.entry_order_id
        mgr._positions[p.position_id] = p
    return pm


# ═══════════════════════════════════════════════════════════════════════════
# 1/2/3/4 — arming, crossing, exactly one exit, duplicate suppression
# ═══════════════════════════════════════════════════════════════════════════

def test_arm_requires_open_position_with_stop():
    mon = PositionOwnedSLMonitor()
    p = make_position()
    assert mon.arm(p) == SLState.ARMED
    assert mon.state_of(p.position_id) == SLState.ARMED
    assert mon.active_for(p.position_id) is True
    # A second arm for the SAME position must not create a second record.
    assert mon.arm(p) == SLState.ARMED
    assert mon.armed_ids() == [p.position_id]


def test_zero_quantity_never_arms():
    mon = PositionOwnedSLMonitor()
    p = make_position(qty=0)
    assert mon.arm(p) == SLState.NONE
    assert mon.armed_ids() == []


def test_missing_stop_is_unavailable_and_fires_nothing():
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=None)
    assert mon.arm(p) == SLState.UNAVAILABLE
    d = mon.evaluate(p, 1.0)
    assert d.fire is False
    assert d.reason in (SLReject.NOT_ARMED, SLReject.STOP_MISSING)


def test_long_fires_only_on_or_below_stop():
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=90.0)
    mon.arm(p)
    assert mon.evaluate(p, 90.01).fire is False
    assert mon.evaluate(p, 90.0).fire is True   # exactly at the stop
    assert mon.evaluate(p, 89.5).fire is True


def test_short_fires_only_on_or_above_stop():
    mon = PositionOwnedSLMonitor()
    p = make_position(side=PositionSide.SHORT, stop=110.0)
    mon.arm(p)
    assert mon.evaluate(p, 109.99).fire is False
    assert mon.evaluate(p, 110.0).fire is True
    assert mon.evaluate(p, 111.0).fire is True


def test_minted_signal_is_an_ordinary_exit_not_a_broker_stop():
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=90.0)
    mon.arm(p)
    d = mon.evaluate(p, 88.0)
    assert d.fire is True
    assert d.exit_side == "SELL"
    assert d.quantity == 10
    assert d.stop_price == 90.0


def test_duplicate_tick_mints_exactly_one_exit_signal():
    h = Harness()
    p = make_position(stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, env.position_manager.get_position(p.position_id))
    assert p.sl_state == SLState.ARMED.value

    first = h._evaluate_position_sl(env, p, 88.0, "LIVE")
    assert first is not None
    # Three more ticks at a worse price must not mint anything.
    for _ in range(3):
        assert h._evaluate_position_sl(env, p, 87.0, "LIVE") is None
    assert len(h.processed) == 1
    assert len(env.broker.submitted) == 1
    # And exactly one order exists, with the ordinary EXIT role.
    assert env.broker.submitted[0]["role"] == "EXIT"
    assert env.broker.submitted[0]["order_type"] == "MARKET"
    # No broker-side protective order was ever created.
    assert all(o["order_type"] != "STOP_LOSS" for o in env.broker.submitted)
    assert all(o["order_type"] != "STOP_LOSS_MARKET" for o in env.broker.submitted)


def test_exit_state_is_latched_through_exit_submission():
    h = Harness()
    p = make_position(stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    h._evaluate_position_sl(env, p, 88.0, "LIVE")
    mon = env.sl_monitor
    assert mon.state_of(p.position_id) == SLState.EXITING
    assert p.sl_state == SLState.EXITING.value
    assert p.exit_order_id == "ORD-1"
    # While EXITING the monitor refuses to fire again.
    d = mon.evaluate(p, 80.0)
    assert d.fire is False
    assert d.reason == SLReject.ALREADY_EXITING


def test_failed_exit_releases_and_allows_retry():
    h = Harness()
    p = make_position(stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    h._evaluate_position_sl(env, p, 88.0, "LIVE")
    h._release_sl_after_failed_exit(env, p, "broker_rejected")
    assert p.sl_state == SLState.ARMED.value
    assert p.exit_started is False
    # A later tick may now retry exactly once.
    assert h._evaluate_position_sl(env, p, 87.0, "LIVE") is not None
    assert h._evaluate_position_sl(env, p, 86.0, "LIVE") is None
    assert len(h.processed) == 2


# ═══════════════════════════════════════════════════════════════════════════
# 5/6 — the old SL can never fire against a later position
# ═══════════════════════════════════════════════════════════════════════════

def test_close_position_makes_the_sl_unreachable_forever():
    h = Harness()
    p = make_position(stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    assert h._evaluate_position_sl(env, p, 88.0, "LIVE") is not None
    h._clear_position_sl(env, p, reason="exit_filled")
    assert p.sl_state == SLState.CLOSED.value
    mon = env.sl_monitor
    assert mon.state_of(p.position_id) == SLState.NONE
    # No amount of further price action can resurrect it.
    for px in (50.0, 0.5, 200.0):
        assert h._evaluate_position_sl(env, p, px, "LIVE") is None
    assert len(h.processed) == 1


def test_new_position_id_cannot_inherit_the_old_sl():
    h = Harness()
    old = make_position(pid="P-OLD", stop=90.0, generation=0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([old]), FakeBroker()))
    h._arm_position_sl(env, old)
    h._clear_position_sl(env, old, reason="reversal_flat")

    new = make_position(pid="P-NEW", side=PositionSide.SHORT, stop=110.0,
                        generation=1)
    h._arm_position_sl(env, new)
    # The old position's extreme tick must not fire the NEW position's SL.
    assert h._evaluate_position_sl(env, old, 1.0, "LIVE") is None
    assert h._evaluate_position_sl(env, new, 1.0, "LIVE") is None  # wrong side
    assert h._evaluate_position_sl(env, new, 111.0, "LIVE") is not None
    assert len(h.processed) == 1
    assert h.processed[0].metadata["position_id"] == "P-NEW"


def test_generation_mismatch_kills_the_arm():
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=90.0, generation=1)
    mon.arm(p)
    p.position_generation = 2          # a new reversal generation
    d = mon.evaluate(p, 80.0)
    assert d.fire is False
    assert d.reason == SLReject.GENERATION_MISMATCH
    assert mon.state_of(p.position_id) == SLState.NONE


# ═══════════════════════════════════════════════════════════════════════════
# 7 — no cross-strategy / cross-symbol borrowing
# ═══════════════════════════════════════════════════════════════════════════

def test_evaluate_rejects_a_foreign_strategy_and_symbol():
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=90.0)
    mon.arm(p)
    assert mon.evaluate(p, 80.0, strategy_id="S2").reason == SLReject.STRATEGY_MISMATCH
    assert mon.evaluate(p, 80.0, instrument="BANKNIFTY").reason == SLReject.INSTRUMENT_MISMATCH
    # A mismatch must not consume the arm.
    assert mon.evaluate(p, 80.0).fire is True


def test_arms_are_per_position_not_per_instrument():
    mon = PositionOwnedSLMonitor()
    a = make_position(pid="P-A", instrument="NIFTY", stop=90.0)
    b = make_position(pid="P-B", instrument="NIFTY", side=PositionSide.SHORT,
                      stop=110.0)
    mon.arm(a)
    mon.arm(b)
    assert mon.armed_ids() == ["P-A", "P-B"]
    # Closing one must not disarm the other.
    mon.close("P-A")
    assert mon.armed_ids() == ["P-B"]
    assert mon.evaluate(b, 111.0).fire is True


# ═══════════════════════════════════════════════════════════════════════════
# 8 — quantity can never exceed the open quantity
# ═══════════════════════════════════════════════════════════════════════════

def test_exit_quantity_never_exceeds_open_quantity():
    mon = PositionOwnedSLMonitor()
    p = make_position(qty=7, stop=90.0)
    mon.arm(p)
    d = mon.evaluate(p, 80.0)
    assert d.fire is True
    assert d.quantity == 7
    p.quantity = 0
    d2 = mon.evaluate(p, 80.0)
    assert d2.fire is False
    assert d2.reason == SLReject.QUANTITY_INVALID


# ═══════════════════════════════════════════════════════════════════════════
# 9 — startup: the broker is the authority; a stale DB row cannot arm
# ═══════════════════════════════════════════════════════════════════════════

def test_startup_arms_only_broker_confirmed_positions():
    h = Harness()
    real = make_position(pid="P-REAL", stop=90.0)
    # The classic stale row: DB says OPEN, the broker is flat on that
    # securityId.  It must be closed, not monitored.
    stale = make_position(pid="P-STALE", instrument="SENSEX", stop=95.0)
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "LONG"}])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([real, stale]), broker))
    summary = h.sync_sl_from_broker("LIVE")

    assert [a["position_id"] for a in summary["armed"]] == ["P-REAL"]
    assert [d["position_id"] for d in summary["dropped_local"]] == ["P-STALE"]
    # The stale row is CLOSED in the book, not merely hidden from the SL.
    stale_now = env.position_manager.get_position("P-STALE")
    assert stale_now.is_open is False
    assert stale_now.status == PositionStatus.CLOSED
    assert stale_now.sl_state == SLState.CLOSED.value
    assert env.position_manager.get_position("P-REAL").is_open is True
    # A deep tick on the stale position must now do nothing.
    assert h._evaluate_position_sl(env, stale_now, 1.0, "LIVE") is None
    assert h.processed == []


def test_startup_treats_the_transports_dhan_fan_out_as_one_net():
    """Dhan reports ONE signed netQty per securityId, with no strategy id.

    ``DhanRestTransport.positions()`` fans that single row out to every
    strategy configured on the instrument.  The sync must collapse those
    duplicates instead of believing the broker holds a position per strategy.
    """
    h = Harness()
    a = make_position(pid="P-A", strategy_id="S1", instrument="NIFTY", stop=90.0)
    b = make_position(pid="P-B", strategy_id="S2", instrument="NIFTY",
                      side=PositionSide.SHORT, stop=95.0)
    # Identical rows, exactly as the transport emits them: the broker holds a
    # single NET LONG 10 on NIFTY.
    broker = FakeBroker([
        {"instrument": "NIFTY", "quantity": 10, "side": "LONG",
         "strategy_id": "S1"},
        {"instrument": "NIFTY", "quantity": 10, "side": "LONG",
         "strategy_id": "S2"},
    ])
    env = h.add_env(FakeEnv(
        "LIVE",
        {"S1": FakeStrategy("S1", "NIFTY"), "S2": FakeStrategy("S2", "NIFTY")},
        make_pm([a, b]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    # Only the local LONG agrees with the broker's net direction, so only it
    # is armed.  The local SHORT is contradicted by the broker and is closed.
    assert [u["position_id"] for u in summary["armed"]] == ["P-A"]
    assert [d["position_id"] for d in summary["dropped_local"]] == ["P-B"]
    assert env.position_manager.get_position("P-B").is_open is False
    # The duplicates were collapsed, not counted twice.
    assert summary["broker_only"] == []


def test_startup_never_arms_a_position_the_broker_holds_the_other_side_of():
    h = Harness()
    p = make_position(side=PositionSide.SHORT, stop=110.0)
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "LONG"}])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["armed"] == []
    assert [d["reason"] for d in summary["dropped_local"]] == [
        "DB_OPEN_BROKER_NOT_CONFIRMED"]
    assert env.position_manager.open_positions == []
    # Nothing is monitored, so no tick can ever mint an exit for it.
    assert env.sl_monitor.armed_ids() == []


def test_startup_agrees_on_direction_across_strategies():
    h = Harness()
    a = make_position(pid="P-A", strategy_id="S1", instrument="NIFTY", stop=90.0)
    b = make_position(pid="P-B", strategy_id="S2", instrument="NIFTY", stop=95.0)
    broker = FakeBroker([
        {"instrument": "NIFTY", "quantity": 14, "side": "LONG",
         "strategy_id": "S1"},
        {"instrument": "NIFTY", "quantity": 14, "side": "LONG",
         "strategy_id": "S2"},
    ])
    env = h.add_env(FakeEnv(
        "LIVE",
        {"S1": FakeStrategy("S1", "NIFTY"), "S2": FakeStrategy("S2", "NIFTY")},
        make_pm([a, b]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    # Both rows agree the instrument is long, so BOTH local positions are
    # broker-confirmed and each arms from its OWN stop.
    assert sorted(u["position_id"] for u in summary["armed"]) == ["P-A", "P-B"]
    assert summary["dropped_local"] == []
    assert env.position_manager.get_position("P-A").stop_price == 90.0
    assert env.position_manager.get_position("P-B").stop_price == 95.0


def test_startup_never_invents_a_stop():
    h = Harness()
    p = make_position(stop=None)
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "BUY"}])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["armed"] == []
    assert [u["reason"] for u in summary["unavailable"]] == [SLReject.STOP_MISSING]
    live = env.position_manager.get_position(p.position_id)
    assert live is not None and live.is_open is True      # still open, unmonitored
    assert live.sl_state == SLState.UNAVAILABLE.value
    assert live.stop_price is None                        # nothing fabricated
    assert h._evaluate_position_sl(env, live, 1.0, "LIVE") is None


def test_startup_stop_resolver_may_only_use_the_positions_own_stop():
    h = Harness()
    p = make_position(stop=None)
    env_pm = None
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "BUY"}])
    env_pm = make_pm([p])
    # The entry order is the ONLY other legitimate source of this position's
    # own stop — the production-minted entry_order_id, not a hand-written one.
    orders = {p.entry_order_id: type("O", (), {"planned_sl": 88.0})()}
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            env_pm, broker,
                            execution_engine=FakeExecutionEngine(orders)))
    summary = h.sync_sl_from_broker("LIVE")
    assert [a["position_id"] for a in summary["armed"]] == [p.position_id]
    assert env.position_manager.get_position(p.position_id).stop_price == 88.0


def test_startup_ignores_an_unrelated_strategys_stop():
    h = Harness()
    p = make_position(stop=None)
    env_pm = make_pm([p])
    other = make_position(strategy_id="S2", instrument="BANKNIFTY", stop=50.0)
    orders = {p.entry_order_id: type("O", (), {"planned_sl": 88.0})(),
              other.entry_order_id: type("O", (), {"planned_sl": 50.0})()}
    env_pm.register("S2", PositionManager())
    broker = FakeBroker([{"instrument": "NIFTY", "quantity": 10, "side": "BUY"}])
    env = h.add_env(FakeEnv(
        "LIVE",
        {"S1": FakeStrategy("S1", "NIFTY"), "S2": FakeStrategy("S2", "BANKNIFTY")},
        env_pm, broker, execution_engine=FakeExecutionEngine(orders)))
    summary = h.sync_sl_from_broker("LIVE")
    assert [a["position_id"] for a in summary["armed"]] == [p.position_id]
    assert env.position_manager.get_position(p.position_id).stop_price == 88.0


def test_startup_broker_only_position_is_surfaced_not_opened():
    h = Harness()
    broker = FakeBroker([{"instrument": "BANKNIFTY", "quantity": 5, "side": "SELL"}])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    assert [b["instrument"] for b in summary["broker_only"]] == ["BANKNIFTY"]
    assert env.position_manager.open_positions == []


def test_startup_conflicting_broker_nets_are_treated_as_flat():
    """Two different nets for one securityId cannot both be true."""
    h = Harness()
    a = make_position(pid="P-A", strategy_id="S1", instrument="NIFTY", stop=90.0)
    broker = FakeBroker([
        {"instrument": "NIFTY", "quantity": 10, "side": "LONG"},
        {"instrument": "NIFTY", "quantity": 4, "side": "SHORT"},
    ])
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([a]), broker))
    summary = h.sync_sl_from_broker("LIVE")
    assert summary["armed"] == []
    assert [d["position_id"] for d in summary["dropped_local"]] == ["P-A"]
    assert env.position_manager.open_positions == []


# ═══════════════════════════════════════════════════════════════════════════
# 9b — the transport parses the REAL documented Dhan GET /positions payload
# ═══════════════════════════════════════════════════════════════════════════

# Verbatim field set from Dhan v2 docs (GET /positions), LONG row.
DHAN_LONG_ROW = {
    "dhanClientId": "1000000009",
    "tradingSymbol": "GOLDM27SEP4700CE",
    "securityId": "54321",
    "positionType": "LONG",
    "exchangeSegment": "MCX_COMM",
    "productType": "MARGIN",
    "buyAvg": 3345.8,
    "buyQty": 40,
    "costPrice": 3215.0,
    "sellAvg": 0.0,
    "sellQty": 0,
    "netQty": 40,
    "realizedProfit": 0.0,
    "unrealizedProfit": 6122.0,
    "rbiReferenceRate": 1.0,
    "multiplier": 10,
    "carryForwardBuyQty": 0,
    "carryForwardSellQty": 0,
    "dayBuyQty": 40,
    "daySellQty": 0,
    "crossCurrency": False,
}


def test_transport_parses_the_real_dhan_positions_payload():
    from execution.live.dhan_transport import DhanRestTransport

    tr = DhanRestTransport.__new__(DhanRestTransport)
    tr._lock = threading.RLock()
    tr.instruments = {"GOLDM": {"security_id": "54321",
                                "exchange_segment": "MCX_COMM"}}
    tr.instrument_strategies = {"GOLDM": ["s1", "s2"]}

    class _Http:
        def _get(self, path, *a, **k):
            assert path == "/positions"
            return [dict(DHAN_LONG_ROW)]
    tr._http = _Http()
    tr._audit = lambda *a, **k: None
    tr.circuit_quote = lambda instrument: {"ltp": 3498.0}

    rows = tr.positions()
    # Dhan holds ONE net position; it is fanned out to both strategies on the
    # instrument, and the sync collapses them again (see the resync tests).
    assert len(rows) == 2
    for row in rows:
        assert row["instrument"] == "GOLDM"
        assert row["side"] == "LONG"
        assert row["quantity"] == 40
        assert row["average_entry_price"] == 3345.8
        assert row["unrealized_profit"] == 6122.0
        assert row["net_profit"] == 6122.0
    assert {r["strategy_id"] for r in rows} == {"s1", "s2"}
    # No LTP in the positions payload -> the market quote is used, not 0.0.
    assert rows[0]["ltp"] == 3498.0


def test_positions_releases_order_book_lock_before_quote_fallback():
    """A missing Dhan LTP must not deadlock positions -> circuit_quote."""
    from execution.live.dhan_transport import DhanRestTransport

    tr = DhanRestTransport.__new__(DhanRestTransport)
    tr._lock = threading.Lock()
    tr.instruments = {"GOLDM": {"security_id": "54321",
                                "exchange_segment": "MCX_COMM"}}
    tr.instrument_strategies = {"GOLDM": ["s1"]}

    class _Http:
        def _get(self, path, *a, **k):
            assert path == "/positions"
            return [dict(DHAN_LONG_ROW)]

    tr._http = _Http()
    tr._audit = lambda *a, **k: None

    def _quote(instrument):
        # circuit_quote uses this same lock to update/read its cache.
        with tr._lock:
            return {"ltp": 3498.0}

    tr.circuit_quote = _quote
    done = threading.Event()
    result = []

    def _read_positions():
        result.extend(tr.positions())
        done.set()

    worker = threading.Thread(target=_read_positions, daemon=True)
    worker.start()
    assert done.wait(1.0), "positions deadlocked while fetching fallback quote"
    assert result[0]["ltp"] == 3498.0


def test_transport_skips_flat_and_unmapped_dhan_rows():
    from execution.live.dhan_transport import DhanRestTransport

    tr = DhanRestTransport.__new__(DhanRestTransport)
    tr._lock = threading.RLock()
    tr.instruments = {"GOLDM": {"security_id": "54321"}}
    tr.instrument_strategies = {"GOLDM": ["s1"]}

    class _Http:
        def _get(self, path, *a, **k):
            return [
                dict(DHAN_LONG_ROW, netQty=0, positionType="CLOSED"),
                dict(DHAN_LONG_ROW, securityId="99999",
                     tradingSymbol="BANKNIFTY", netQty=-25),
                dict(DHAN_LONG_ROW),
            ]
    tr._http = _Http()
    tr._audit = lambda *a, **k: None
    tr.circuit_quote = lambda instrument: {}

    rows = tr.positions()
    # CLOSED/flat dropped, unmapped securityId dropped, the mapped LONG kept.
    assert len(rows) == 1
    assert rows[0]["instrument"] == "GOLDM"
    assert rows[0]["quantity"] == 40


def test_transport_reads_a_negative_net_as_short():
    from execution.live.dhan_transport import DhanRestTransport

    tr = DhanRestTransport.__new__(DhanRestTransport)
    tr._lock = threading.RLock()
    tr.instruments = {"GOLDM": {"security_id": "54321"}}
    tr.instrument_strategies = {"GOLDM": ["s1"]}

    class _Http:
        def _get(self, path, *a, **k):
            return [dict(DHAN_LONG_ROW, positionType="SHORT", netQty=-30,
                         buyAvg=0.0, sellAvg=3500.0)]
    tr._http = _Http()
    tr._audit = lambda *a, **k: None
    tr.circuit_quote = lambda instrument: {}

    rows = tr.positions()
    assert rows[0]["side"] == "SHORT"
    assert rows[0]["quantity"] == 30
    assert rows[0]["average_entry_price"] == 3500.0

# ═══════════════════════════════════════════════════════════════════════════
# 10 — nothing broker-side, anywhere
# ═══════════════════════════════════════════════════════════════════════════


def test_local_sl_exit_never_resolves_to_a_broker_stop_role():
    from strategies.types import Signal
    sig = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
                 strategy_id="S1", timestamp=time.time(), trigger_price=88.0,
                 stop_price=90.0, quantity=10)
    sig.metadata = {"exit": True, "exit_reason": "stop_loss_hit",
                    "local_sl_exit": True}
    assert resolve_order_role(sig) == "EXIT"


def test_engine_and_transport_hard_reject_broker_stop_orders():
    from execution.live.engine import LiveExecutionEngine
    from execution.live.dhan_transport import DhanRestTransport
    from strategies.types import Signal

    eng = LiveExecutionEngine.__new__(LiveExecutionEngine)
    eng._plan_by_signal = {}
    eng._plans = {}
    eng._orders = {}
    eng._price_preset = type("P", (), {"tick_size": 1.0, "sl_offset": 0.0})()
    sig = Signal(signal_type=SignalType.SHORT, instrument="NIFTY",
                 strategy_id="S1", timestamp=time.time(), trigger_price=88.0,
                 stop_price=90.0, quantity=10)
    sig.metadata = {"exit": True, "exit_reason": "stop_loss_legacy",
                    "trigger_state": "FIRED"}

    def _order(role, order_type):
        return type("O", (), {"order_role": role, "order_id": "X",
                              "signal": sig, "instrument": "NIFTY",
                              "side": "SELL", "quantity": 10,
                              "order_type": order_type,
                              "state": OrderState.CREATED, "reason": None,
                              "updated_at": None,
                              "trigger_price": 90.0, "price": 0.0,
                              "requested_price": 0.0,
                              "planned_order_type": order_type,
                              "planned_entry_price": 0.0,
                              "planned_sl": 90.0})()

    eng._now = lambda: time.time()
    for role, order_type in (("STOP_LOSS", "STOP_LOSS"),
                             ("STOP_LOSS", "STOP_LOSS_MARKET"),
                             ("EXIT", "STOP_LOSS_MARKET"),
                             ("EXIT", "STOP_LOSS")):
        out = eng.submit_order(_order(role, order_type))
        assert out.state == OrderState.REJECTED
        assert out.reason == "BROKER_SL_RETIRED_POSITION_OWNED_SL_ONLY"

    # A modify cannot create one either.
    tr = DhanRestTransport.__new__(DhanRestTransport)
    for order_type in ("STOP_LOSS", "STOP_LOSS_MARKET"):
        with pytest.raises(ValueError) as exc2:
            tr.modify_order("BRK-1", order_type=order_type, price=95.0,
                            trigger_price=90.0)
        assert "BROKER_SL_RETIRED" in str(exc2.value)

    # place_market_order refuses it at the wire boundary too.
    tr.gate_enabled = False        # gate is OFF: the SL guard must still win
    for order_type in ("STOP_LOSS", "STOP_LOSS_MARKET"):
        with pytest.raises(ValueError) as exc3:
            tr.place_market_order("SELL", 10, "NIFTY", order_type=order_type,
                                  price=95.0, trigger_price=90.0)
        assert "BROKER_SL_RETIRED" in str(exc3.value)


# ═══════════════════════════════════════════════════════════════════════════
# 11 — no stop invention
# ═══════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize("bad", [None, 0.0, -5.0, float("nan"), float("inf"),
                                 "abc", ""])
def test_invalid_stop_values_never_arm(bad):
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=bad)
    assert mon.arm(p) in (SLState.UNAVAILABLE, SLState.NONE)
    d = mon.evaluate(p, 1.0)
    assert d.fire is False


@pytest.mark.parametrize("bad_ltp", [None, 0.0, -1.0, float("nan")])
def test_invalid_market_price_never_fires(bad_ltp):
    mon = PositionOwnedSLMonitor()
    p = make_position(stop=90.0)
    mon.arm(p)
    d = mon.evaluate(p, bad_ltp)
    assert d.fire is False
    assert d.reason == SLReject.MARKET_INVALID


def test_no_stop_is_never_borrowed_from_a_previous_position():
    h = Harness()
    first = make_position(pid="P-1", stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([first]), FakeBroker()))
    h._arm_position_sl(env, first)
    h._clear_position_sl(env, first, reason="closed")
    second = make_position(pid="P-2", side=PositionSide.SHORT, stop=None)
    h._arm_position_sl(env, second)
    assert second.sl_state == SLState.UNAVAILABLE.value
    assert second.stop_price is None
    assert env.sl_monitor.armed_ids() == []
    # The OLD level is not reused: a tick at the old 90 does not fire.
    assert h._evaluate_position_sl(env, second, 90.0, "LIVE") is None


# ═══════════════════════════════════════════════════════════════════════════
# 12 — partial exit
# ═══════════════════════════════════════════════════════════════════════════

def test_partial_exit_rearms_only_when_the_exit_is_done():
    h = Harness()
    p = make_position(qty=10, stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    h._evaluate_position_sl(env, p, 88.0, "LIVE")

    # Exit order still has working quantity -> stay EXITING, no second exit.
    state = h._rearm_sl_after_partial_exit(env, p, exit_still_working=True)
    assert state == SLState.EXITING.value
    assert h._evaluate_position_sl(env, p, 80.0, "LIVE") is None

    # Exit done, position reduced to 3 -> re-arm on the remainder only.
    p.quantity = 3
    state = h._rearm_sl_after_partial_exit(env, p, exit_still_working=False)
    assert state == SLState.ARMED.value
    assert env.sl_monitor.snapshot(p.position_id)["quantity"] == 3
    d = env.sl_monitor.evaluate(p, 80.0)
    assert d.fire is True and d.quantity == 3


def test_partial_exit_never_marks_the_position_closed():
    h = Harness()
    p = make_position(qty=10, stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    h._evaluate_position_sl(env, p, 88.0, "LIVE")
    p.quantity = 6
    h._rearm_sl_after_partial_exit(env, p, exit_still_working=False)
    live = env.position_manager.get_position(p.position_id)
    assert live is not None
    assert live.is_open is True
    assert live.status == PositionStatus.OPEN


# ═══════════════════════════════════════════════════════════════════════════
# 14 — concurrency: exactly one exit under simultaneous ticks
# ═══════════════════════════════════════════════════════════════════════════

def test_concurrent_ticks_mint_exactly_one_exit():
    h = Harness()
    p = make_position(qty=10, stop=90.0)
    env = h.add_env("LIVE" and FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                                       make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    results = []
    barrier = threading.Barrier(8)

    def worker():
        barrier.wait()
        results.append(h._evaluate_position_sl(env, p, 85.0, "LIVE"))

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(h.processed) == 1
    assert len(env.broker.submitted) == 1


# ═══════════════════════════════════════════════════════════════════════════
# 15 — SL_UNAVAILABLE keeps the position, it does not close or invent
# ═══════════════════════════════════════════════════════════════════════════

def test_sl_unavailable_publishes_and_leaves_the_position_open():
    h = Harness()
    p = make_position(stop=None)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    state = h._arm_position_sl(env, p, source="entry_fill")
    assert state == SLState.UNAVAILABLE.value
    assert p.sl_state == SLState.UNAVAILABLE.value
    assert env.position_manager.get_position(p.position_id).is_open is True
    assert h.events_of("sl_unavailable")
    assert h.processed == []


# ═══════════════════════════════════════════════════════════════════════════
# 13 — reversal end-to-end through the SL flow
# ═══════════════════════════════════════════════════════════════════════════

def test_reversal_clears_the_old_sl_before_the_new_one_arms():
    h = Harness()
    old = make_position(pid="P-OLD", stop=90.0, generation=0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([old]), FakeBroker()))
    h._arm_position_sl(env, old)
    assert env.sl_monitor.armed_ids() == ["P-OLD"]

    # Reversal: old position goes flat first, THEN the new one arms.
    h._clear_position_sl(env, old, reason="reversal_exit_filled")
    assert env.sl_monitor.armed_ids() == []
    new = make_position(pid="P-NEW", side=PositionSide.SHORT, stop=110.0,
                        generation=1)
    h._arm_position_sl(env, new)
    assert env.sl_monitor.armed_ids() == ["P-NEW"]
    # The old SL cannot fire the new position even on a violent tick.
    assert h._evaluate_position_sl(env, old, 1.0, "LIVE") is None
    assert len(h.processed) == 0
    # Only the new, opposite stop is live.
    assert h._evaluate_position_sl(env, new, 115.0, "LIVE") is not None
    assert h.processed[0].metadata["position_id"] == "P-NEW"
    assert h.processed[0].signal_type == SignalType.LONG


# ═══════════════════════════════════════════════════════════════════════════
# observability
# ═══════════════════════════════════════════════════════════════════════════

def test_arm_and_clear_publish_the_lifecycle_events():
    h = Harness()
    p = make_position(stop=90.0)
    env = h.add_env(FakeEnv("LIVE", {"S1": FakeStrategy("S1", "NIFTY")},
                            make_pm([p]), FakeBroker()))
    h._arm_position_sl(env, p)
    h._evaluate_position_sl(env, p, 88.0, "LIVE")
    h._clear_position_sl(env, p, reason="exit_filled")
    names = [t for t, _, _ in h.events]
    for expected in ("sl_armed", "sl_triggered", "sl_exit_submitted",
                     "sl_cleared"):
        assert expected in names
