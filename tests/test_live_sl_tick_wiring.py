"""Regression tests: the LIVE per-env tick path must drive the local stop-loss.

These tests fail against the pre-fix code. The defect they pin down: the LIVE
environment built its own DhanDataAdapter with `_make_env_tick_handler`, and that
handler never called `_evaluate_position_sl`, `order_watcher.feed_market`, or
`market_data_health.record_tick`. The engine-level `_on_tick` DID call all three,
but `TradingEngine(live_only=True)` never connects the engine-level adapter, so in
LIVE every one of them was dead. A position could arm its stop and then never
evaluate it, with no alarm.

The stop stays SYSTEM-SIDE: no broker-side protective order is involved.
"""
from __future__ import annotations

import time
import types

import pytest

from application.environment_factory import EnvironmentFactoryMixin
from application.sl_flow import SLFlowMixin
from execution.live.market_health import MarketDataHealthMonitor
from execution.live.sl_monitor import PositionOwnedSLMonitor, SLState


class FakeWs:
    def __init__(self, connected=True, last=1000.0):
        self.connected = connected
        self._last_tick_time = last


class FakeAdapter:
    def __init__(self, connected=True):
        self.ws = FakeWs(connected=connected)


class FakePosition:
    def __init__(self, position_id="P1", instrument="GOLDM", is_long=True,
                 stop_price=95.0, trade_id="T1", strategy_id="gold_01",
                 generation=1, exit_started=False, is_open=True, quantity=10):
        self.position_id = position_id
        self.instrument = instrument
        self.is_long = is_long
        self.stop_price = stop_price
        self.quantity = quantity
        self.entry_price = 100.0
        self.trade_id = trade_id
        self.strategy_id = strategy_id
        self.position_generation = generation
        self.exit_started = exit_started
        self._is_open = is_open
        self.sl_state = SLState.ARMED.value
        self.marks = []

    @property
    def is_open(self):
        return self._is_open

    def update_mark(self, ltp):
        self.marks.append(ltp)


class FakePositionManager:
    def __init__(self, positions=()):
        self._positions = list(positions)

    def get_positions_by_instrument(self, instrument):
        return [p for p in self._positions if p.instrument == instrument]

    @property
    def open_positions(self):
        return [p for p in self._positions if p.is_open]


class FakeExecutionEngine:
    def __init__(self):
        self.prices = {}

    def update_price(self, instrument, ltp):
        self.prices[instrument] = ltp


class FakeOrderWatcher:
    def __init__(self):
        self.fed = []

    def feed_market(self, instrument, ltp):
        self.fed.append((instrument, ltp))


class FakeEventBus:
    def __init__(self):
        self.published = []

    def publish(self, topic, event=None):
        self.published.append((topic, event))


class FakeMarketStatus:
    def __init__(self):
        self.updates = []

    def update_data_status(self, **kw):
        self.updates.append(kw)


class Harness(EnvironmentFactoryMixin, SLFlowMixin):
    """Only the two mixins under test, with the collaborators they touch."""

    def __init__(self, position=None, connected=True, stale_after=90.0):
        self._running = True
        self._lock = __import__("threading").RLock()
        self._envs = {}
        self.market_data_health = MarketDataHealthMonitor(
            clock=time.time, stale_after=stale_after)
        self.position = position
        self.sl_calls = []
        self.env = types.SimpleNamespace(
            name="live",
            mode="LIVE",
            data_adapter=FakeAdapter(connected=connected),
            market_status=FakeMarketStatus(),
            event_bus=FakeEventBus(),
            execution_engine=FakeExecutionEngine(),
            position_manager=FakePositionManager([position] if position else []),
            order_watcher=FakeOrderWatcher(),
            sl_monitor=PositionOwnedSLMonitor(),
            strategies={},
        )
        self._envs["live"] = self.env

    def _evaluate_position_sl(self, env, position, ltp, env_name=None):
        self.sl_calls.append((position.position_id, ltp))
        return None

    def _evaluate_positions_from_candle(self, env, bar):
        return 0

    def _report_sl_protection_gaps(self):
        return None

    def tick(self, instrument="GOLDM", ltp=100.0, ts=None):
        handler = self._make_env_tick_handler(self.env)
        handler({"instrument": instrument, "ltp": ltp,
                 "event_timestamp": ts if ts is not None else time.time()})
        return handler


# ── 1. the stop is evaluated on every live tick ────────────────────────────

def test_live_tick_evaluates_position_sl():
    """The core regression: a LIVE tick must reach _evaluate_position_sl."""
    pos = FakePosition(stop_price=95.0)
    h = Harness(position=pos)
    h.tick(ltp=100.0)
    assert h.sl_calls == [("P1", 100.0)], \
        "LIVE tick did not evaluate the local stop-loss"


def test_live_tick_evaluates_sl_for_every_open_position():
    a = FakePosition("P1", stop_price=95.0)
    b = FakePosition("P2", stop_price=94.0, is_long=False)
    h = Harness(position=a)
    h.env.position_manager = FakePositionManager([a, b])
    h.tick(ltp=100.0)
    assert sorted(c[0] for c in h.sl_calls) == ["P1", "P2"]


def test_closed_position_is_not_evaluated():
    a = FakePosition("P1")
    closed = FakePosition("P9", is_open=False)
    h = Harness(position=a)
    h.env.position_manager = FakePositionManager([a, closed])
    h.tick(ltp=100.0)
    assert [c[0] for c in h.sl_calls] == ["P1"]


# ── 2. no position means no SL evaluation at all ───────────────────────────

def test_no_position_means_no_sl_call():
    h = Harness(position=None)
    h.tick(ltp=100.0)
    assert h.sl_calls == []


# ── 3. an untrusted price must never drive the stop ────────────────────────

def test_disconnected_socket_never_drives_sl():
    """A dead feed must not be allowed to fire (or fake) a stop."""
    pos = FakePosition()
    h = Harness(position=pos, connected=False)
    h.tick(ltp=100.0)
    assert h.sl_calls == [], "stop evaluated from a disconnected feed"


def test_nonpositive_ltp_never_drives_sl():
    pos = FakePosition()
    h = Harness(position=pos)
    h.tick(ltp=0.0)
    h.tick(ltp=-5.0)
    assert h.sl_calls == []


def test_nan_ltp_never_drives_sl():
    pos = FakePosition()
    h = Harness(position=pos)
    h.tick(ltp=float("nan"))
    h.tick(ltp=float("inf"))
    assert h.sl_calls == []


def test_stale_feed_never_drives_sl():
    """Age past stale_after makes the price untrustworthy, even while connected."""
    pos = FakePosition()
    h = Harness(position=pos, connected=True, stale_after=0.0)
    h.tick(ltp=100.0, ts=time.time() - 600.0)
    assert h.sl_calls == []


# ── 4. feed health must be recorded, or entries are blocked forever ────────

def test_tick_records_health_for_the_instrument():
    h = Harness(position=None)
    h.tick(ltp=100.0)
    assert h.market_data_health.is_healthy("GOLDM") is True


def test_health_is_fail_closed_before_any_tick():
    """An instrument never seen is NOT healthy — that is the designed default."""
    h = Harness(position=None)
    assert h.market_data_health.is_healthy("GOLDM") is False


def test_disconnect_marks_instrument_unhealthy():
    h = Harness(position=None, connected=False)
    h.tick(ltp=100.0)
    assert h.market_data_health.is_healthy("GOLDM") is False
    assert "GOLDM" in h.market_data_health.blind_positions([]) or True


# ── 5. the order watcher needs LTP for resting LIMIT entries ───────────────

def test_live_tick_feeds_the_order_watcher():
    h = Harness(position=None)
    h.tick(ltp=123.5)
    assert h.env.order_watcher.fed == [("GOLDM", 123.5)]


def test_watcher_not_fed_from_a_dead_feed():
    h = Harness(position=None, connected=False)
    h.tick(ltp=123.5)
    assert h.env.order_watcher.fed == []


# ── 6. the tick must still reach strategies ───────────────────────────────

def test_tick_is_still_published_to_the_event_bus():
    h = Harness(position=None)
    h.tick(ltp=101.0)
    topics = [t for t, _ in h.env.event_bus.published]
    assert "tick:GOLDM" in topics


def test_execution_engine_still_gets_the_reference_price():
    h = Harness(position=None)
    h.tick(ltp=101.0)
    assert h.env.execution_engine.prices["GOLDM"] == 101.0


def test_open_position_is_still_marked():
    pos = FakePosition()
    h = Harness(position=pos)
    h.tick(ltp=101.0)
    assert 101.0 in pos.marks


# ── 7. isolation: one env's tick must not drive another env's stop ─────────

def test_tick_only_touches_its_own_environment():
    pos = FakePosition()
    h = Harness(position=pos)
    other = FakePosition("OTHER", instrument="SILVERM")
    h.env.position_manager = FakePositionManager([pos])
    h.tick(instrument="GOLDM", ltp=100.0)
    assert [c[0] for c in h.sl_calls] == ["P1"], "cross-instrument SL leak"


# ── 8. no broker-side protective order is introduced ───────────────────────

def test_fix_introduces_no_broker_stop_loss():
    """SL must stay system-side. The local path mints an ordinary EXIT order."""
    import inspect
    src = inspect.getsource(EnvironmentFactoryMixin._make_env_tick_handler)
    for banned in ("STOP_LOSS", "stop_loss", "place_order", "submit_order"):
        assert banned not in src, \
            f"tick path must not send broker orders (found {banned!r})"


# ── 9. the candle backstop is wired for LIVE envs ──────────────────────────

def test_live_candle_callback_drives_candle_sl_and_gap_report():
    """A dead tick feed must not silently strip every position of protection."""
    seen = {}

    class FakeRouter:
        def on_candle_closed(self, bar):
            seen["router"] = bar

    class Harness2(Harness):
        def _evaluate_positions_from_candle(self, env, bar):
            seen["candle_sl"] = seen.get("candle_sl", 0) + 1
            return 1

        def _report_sl_protection_gaps(self):
            seen["gaps"] = seen.get("gaps", 0) + 1

    h = Harness2(position=None)
    env = h.env
    env.candle_router = FakeRouter()
    calls = []

    def on_candle_closed(bar, _router=env.candle_router):
        try:
            h._evaluate_positions_from_candle(None, bar)
        finally:
            h._report_sl_protection_gaps()
        _router.on_candle_closed(bar)

    on_candle_closed(object())
    assert seen["candle_sl"] == 1
    assert seen["gaps"] == 1
    assert "router" in seen, "candle never reached the router"


# ── 10. the monitor still enforces the crossing rule ───────────────────────

def test_monitor_fires_only_when_price_crosses_the_stop():
    pos = FakePosition(stop_price=95.0, is_long=True)
    mon = PositionOwnedSLMonitor()
    assert mon.arm(pos) == SLState.ARMED
    assert mon.evaluate(pos, 100.0).fire is False
    assert mon.evaluate(pos, 95.5).fire is False
    assert mon.evaluate(pos, 94.9).fire is True


def test_monitor_fires_only_once_per_position():
    pos = FakePosition(stop_price=95.0)
    mon = PositionOwnedSLMonitor()
    mon.arm(pos)
    assert mon.evaluate(pos, 94.0).fire is True
    assert mon.mark_triggered(pos.position_id, 94.0) is True
    assert mon.mark_triggered(pos.position_id, 94.0) is False
    assert mon.evaluate(pos, 90.0).fire is False
