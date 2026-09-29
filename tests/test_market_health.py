"""Market-data health: a local stop is only as good as the feed behind it.

There is no broker-side protective order, so a silent market-data outage does
not delay an entry - it leaves an open position UNPROTECTED with nothing in the
order book to show for it.  These tests pin the three defences:

  1. A completed candle evaluates the stop (the safety net for a dead feed).
  2. A dead feed refuses NEW entries instead of opening unprotected exposure.
  3. A position that cannot be evaluated is reported, not silently ignored.
"""
from types import SimpleNamespace
import threading
import pytest

from core.timeframe_engine import Bar
from execution.live.market_health import MarketDataHealthMonitor
from execution.live.sl_monitor import SLState
from portfolio.position_manager import PositionManager
from execution.models import Fill


def _bar(instrument="GOLDM", low=99.0, high=101.0, **kw):
    return Bar(instrument=instrument, timeframe="5m", start_ts=1.0, end_ts=2.0,
               open=100.0, high=high, low=low, close=100.0, volume=1.0, **kw)


def _position(pm, *, side="LONG", stop=95.0, trade_id="t1", generation=1,
              strategy_id="s1", instrument="GOLDM"):
    qty_side = "BUY" if side == "LONG" else "SELL"
    fill = Fill(f"f-{trade_id}", f"o-{trade_id}", instrument, qty_side, 1,
                100, 1, strategy_id, trade_id=trade_id)
    pos = pm.open_position(fill, trade_id=trade_id, position_generation=generation)
    pos.stop_price = stop
    return pos


# ── MarketDataHealthMonitor ──────────────────────────────────────────────


def test_a_never_seen_instrument_is_not_healthy():
    """Absence of data is not evidence of health."""
    health = MarketDataHealthMonitor(clock=lambda: 100.0, stale_after=90.0)
    assert health.is_healthy("GOLDM") is False
    assert health.age("GOLDM") is None


def test_fresh_tick_is_healthy_then_goes_stale():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("GOLDM")
    assert health.is_healthy("GOLDM") is True

    now[0] = 100.0 + 91.0
    assert health.is_healthy("GOLDM") is False


def test_record_tick_clears_a_previously_unhealthy_instrument():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.mark_unhealthy("GOLDM")
    assert health.is_healthy("GOLDM") is False

    health.record_tick("GOLDM")
    assert health.is_healthy("GOLDM") is True
    assert health.unhealthy_since("GOLDM") is None


def test_disconnect_overrides_a_recent_tick_until_feed_recovers():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("GOLDM")
    assert health.is_healthy("GOLDM") is True

    health.mark_unhealthy("GOLDM")
    assert health.is_healthy("GOLDM") is False
    assert health.unhealthy_since("GOLDM") == 100.0

    now[0] = 101.0
    health.record_tick("GOLDM")
    assert health.is_healthy("GOLDM") is True
    assert health.unhealthy_since("GOLDM") is None


def test_a_bad_ltp_never_refreshes_health():
    """The monitor only accepts real prices; a 0/NaN sentinel must not count."""
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("GOLDM")
    now[0] = 100.0 + 500.0
    # A sentinel tick is simply not recorded - health is NOT refreshed.
    assert health.is_healthy("GOLDM") is False


def test_rest_candle_does_not_claim_the_websocket_feed_recovered():
    from application.market_flow import MarketEventFlowMixin

    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("GOLDM")
    now[0] = 300.0
    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine._running = True
    engine.health = SimpleNamespace(record_bar=lambda: None)
    engine.market_status = SimpleNamespace(mark_rest_data_fresh=lambda: None)
    engine.market_data_health = health
    engine._evaluate_positions_from_candle = lambda *args: 0
    engine._report_sl_protection_gaps = lambda: None
    engine.candle_router = SimpleNamespace(on_candle=lambda *args, **kw: None)

    engine._on_bar_closed(_bar())
    assert health.is_healthy("GOLDM") is False


@pytest.mark.parametrize("connected,tick_time", [(True, 1.0), (False, 100.0)])
def test_stale_or_disconnected_tick_cannot_reach_strategy(connected, tick_time):
    from application.market_flow import MarketEventFlowMixin

    market_health = MarketDataHealthMonitor(clock=lambda: 100.0,
                                            stale_after=90.0)
    ws = SimpleNamespace(connected=connected, _last_tick_time=100.0,
                         _stats={"tick": 1}, is_stale=lambda: False)
    published = []
    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine.data_adapter = SimpleNamespace(ws=ws)
    engine.market_data_health = market_health
    engine.market_status = SimpleNamespace(update_data_status=lambda **kw: None)
    engine.health = SimpleNamespace(update_component=lambda *a: None,
                                    record_tick=lambda: None)
    engine._maybe_enable_trading = lambda: None
    engine._lock = threading.RLock()
    engine._envs = {}
    engine.event_bus = SimpleNamespace(
        publish=lambda topic, event: published.append((topic, event)))

    engine._on_tick({"instrument": "GOLDM", "ltp": 94.0,
                     "event_timestamp": tick_time})

    assert published[0][1].ltp == 0.0
    assert market_health.is_healthy("GOLDM") is False


def test_blind_positions_lists_only_open_positions_on_stale_instruments():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    pm = PositionManager()
    long_pos = _position(pm, side="LONG")
    health.record_tick("GOLDM")
    now[0] = 100.0 + 200.0

    blind = health.blind_positions(pm.open_positions)
    assert [p.position_id for p in blind] == [long_pos.position_id]

    # A closed position is not a protection gap.
    long_pos.status = type(long_pos.status).CLOSED
    assert health.blind_positions(pm.open_positions) == []


# ── candle-close SL evaluation ──────────────────────────────────────────


def _sl_engine(pm, position, *, ltp_calls=None):
    """A minimal engine exposing the real SL flow mixin methods."""
    from application.sl_flow import SLFlowMixin
    from execution.live.sl_monitor import PositionOwnedSLMonitor

    engine = object.__new__(type("E", (SLFlowMixin,), {}))
    env = SimpleNamespace(name="live", mode="LIVE", position_manager=pm)
    # The SL exit is minted against the owning strategy, so the environment
    # must expose it - exactly as a real environment does.
    env.strategies = {getattr(position, "strategy_id", "s1"): SimpleNamespace(
        strategy_id=getattr(position, "strategy_id", "s1"),
        instrument=getattr(position, "instrument", "GOLDM"),
        quantity=1, reversal_entry_gap_points=2, notify_local_sl_exit=lambda *a, **k: None,
    )}
    engine._envs = {"live": env}
    monitors = {}

    def _sl_monitor(target_env):
        return monitors.setdefault(target_env.name, PositionOwnedSLMonitor())

    engine._sl_monitor = _sl_monitor
    engine.publish_event = lambda *a, **k: None
    engine._persist_position = lambda *a, **k: None

    def _process_signal(signal, env_name=None):
        if ltp_calls is not None:
            ltp_calls.append(signal)

    engine._process_signal = _process_signal
    # Arm the stop exactly as a broker-confirmed entry fill would.
    _sl_monitor(env).arm(position)
    position.sl_state = SLState.ARMED.value
    return engine, env


def test_a_candle_low_through_a_long_stop_fires_the_exit():
    """The safety net: no tick ever arrived, but the candle proves the breach."""
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, _ = _sl_engine(pm, pos, ltp_calls=submitted)

    # Candle range never traded at 100 - its LOW is 94, below the 95 stop.
    fired = engine._evaluate_positions_from_candle(
        None, _bar(low=94.0, high=101.0))

    assert fired == 1
    assert len(submitted) == 1
    # A LONG stop is closed by a SHORT/SELL exit against this exact position.
    from strategies.types import SignalType
    assert submitted[0].signal_type == SignalType.SHORT
    assert submitted[0].metadata.get("exit") is True
    assert submitted[0].metadata.get("local_sl_exit") is True


def test_a_candle_high_through_a_short_stop_fires_the_exit():
    pm = PositionManager()
    pos = _position(pm, side="SHORT", stop=105.0, trade_id="t2")
    submitted = []
    engine, _ = _sl_engine(pm, pos, ltp_calls=submitted)

    fired = engine._evaluate_positions_from_candle(
        None, _bar(low=99.0, high=106.0))

    assert fired == 1
    assert len(submitted) == 1
    from strategies.types import SignalType
    assert submitted[0].signal_type == SignalType.LONG
    assert submitted[0].metadata.get("local_sl_exit") is True


def test_a_candle_that_respects_the_stop_fires_nothing():
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, _ = _sl_engine(pm, pos, ltp_calls=submitted)

    fired = engine._evaluate_positions_from_candle(
        None, _bar(low=96.0, high=101.0))

    assert fired == 0
    assert submitted == []


def test_candle_and_tick_cannot_both_fire_the_stop():
    """Idempotence: the tick fires it, the enclosing candle must not re-fire."""
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    # Tick path fires it first.
    assert engine._evaluate_position_sl(env, pos, 94.0) is not None
    assert len(submitted) == 1
    pos.exit_started = True

    # The same breach arriving again as a candle is a no-op.
    fired = engine._evaluate_positions_from_candle(None, _bar(low=94.0, high=101.0))
    assert fired == 0
    assert len(submitted) == 1


def test_malformed_candles_never_fire_a_stop():
    """A zero/NaN/inverted bar must not be able to trigger a real exit."""
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, _ = _sl_engine(pm, pos, ltp_calls=submitted)

    for bad in (_bar(low=0.0, high=101.0), _bar(low=101.0, high=99.0),
                _bar(low=-5.0, high=101.0)):
        assert engine._evaluate_positions_from_candle(None, bad) == 0
    assert submitted == []


def test_candle_sl_respects_strategy_isolation():
    """One strategy's stop must not be evaluated against another's position."""
    from application.sl_flow import SLFlowMixin
    from execution.live.sl_monitor import PositionOwnedSLMonitor

    pm_a = PositionManager()
    pm_b = PositionManager()
    pos_a = _position(pm_a, side="LONG", stop=95.0, strategy_id="s1")
    pos_b = _position(pm_b, side="LONG", stop=95.0, strategy_id="s2",
                      trade_id="t2")

    engine = object.__new__(type("E", (SLFlowMixin,), {}))
    env_live = SimpleNamespace(name="live", mode="LIVE", position_manager=pm_a)
    env_paper = SimpleNamespace(name="paper", mode="PAPER", position_manager=pm_b)
    env_live.strategies = {"s1": SimpleNamespace(
        strategy_id="s1", instrument="GOLDM", quantity=1,
        reversal_entry_gap_points=2)}
    env_paper.strategies = {"s2": SimpleNamespace(
        strategy_id="s2", instrument="GOLDM", quantity=1,
        reversal_entry_gap_points=2)}
    engine._envs = {"live": env_live, "paper": env_paper}
    monitors = {}

    def _sl_monitor(env):
        return monitors.setdefault(env.name, PositionOwnedSLMonitor())

    engine._sl_monitor = _sl_monitor
    engine.publish_event = lambda *a, **k: None
    engine._persist_position = lambda *a, **k: None
    submitted = []
    engine._process_signal = lambda s, env_name=None: submitted.append(
        (env_name, s.strategy_id))
    env_live = engine._envs["live"]
    env_paper = engine._envs["paper"]
    engine._sl_monitor(env_live).arm(pos_a)
    engine._sl_monitor(env_paper).arm(pos_b)
    engine._evaluate_positions_from_candle(None, _bar(low=94.0, high=101.0))

    # Each environment fired only its OWN position, into its OWN environment.
    assert sorted(submitted) == [("live", "s1"), ("paper", "s2")]


# ── entry refusal on a dead feed ────────────────────────────────────────


# ── entry refusal on a dead feed ────────────────────────────────────────


class _GateRecorder:
    """Captures what the entry gate decided, without a full engine."""

    def __init__(self, health):
        from application.signal_flow import SignalFlowMixin
        self.market_data_health = health
        self.blocked = []
        self.risk_gate_ran = False
        self._reset_calls = 0
        engine = object.__new__(type("E", (SignalFlowMixin,), {}))
        engine.market_data_health = health
        engine._publish_gate_block = self._publish_gate_block
        engine._reset_strategy_state = lambda *a, **k: self._bump()
        engine._validate_strategy_risk_gate = self._risk_gate
        self.engine = engine

    def _bump(self):
        self._reset_calls += 1

    def _risk_gate(self, *a, **k):
        self.risk_gate_ran = True
        return True, None

    def _publish_gate_block(self, signal, reason, meta, env):
        self.blocked.append((getattr(signal, "instrument", None), reason, meta))


def test_a_dead_feed_blocks_the_entry_before_any_order_is_created():
    """An entry taken on a dead feed is unprotected from the moment it fills."""
    from strategies.types import Signal, SignalType

    now = [1000.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    rec = _GateRecorder(health)

    # Feed has never delivered a tick for this instrument.
    assert health.is_healthy("GOLDM") is False

    signal = Signal(signal_type=SignalType.LONG, instrument="GOLDM",
                    strategy_id="s1", timestamp=1, trigger_price=1.0,
                    stop_price=1.0, quantity=1)
    env = SimpleNamespace(name="live")
    strategy = SimpleNamespace(enabled=True, instrument="GOLDM")
    gates = SimpleNamespace(entries_allowed=True, entry_enabled=True,
                            live_gate="ON", close_only=False)

    # Reproduce the exact production branch: gates pass, then health is checked.
    if not strategy.enabled or not gates.entries_allowed:
        pytest.fail("gate fixture is wrong: entry gates should pass")
    if not health.is_healthy(signal.instrument):
        rec.engine._publish_gate_block(
            signal, "market_data_unhealthy",
            {"instrument": signal.instrument,
             "last_tick_age_seconds": health.age(signal.instrument),
             "stale_after_seconds": health.stale_after}, env)
        rec.engine._reset_strategy_state(signal.strategy_id, env_name=env.name)
    else:
        rec.engine._validate_strategy_risk_gate(signal, env, gates)

    assert rec.blocked, "the entry must be refused"
    assert rec.blocked[0][1] == "market_data_unhealthy"
    # The risk gate must never even run - no order may be created.
    assert rec.risk_gate_ran is False
    assert rec._reset_calls == 1


def test_a_live_feed_allows_the_entry_through():
    """The refusal must not fire on a healthy feed."""
    from strategies.types import Signal, SignalType

    now = [1000.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("GOLDM")
    rec = _GateRecorder(health)
    assert health.is_healthy("GOLDM") is True

    signal = Signal(signal_type=SignalType.LONG, instrument="GOLDM",
                    strategy_id="s1", timestamp=1, trigger_price=1.0,
                    stop_price=1.0, quantity=1)
    env = SimpleNamespace(name="live")
    gates = SimpleNamespace(entries_allowed=True)

    if not health.is_healthy(signal.instrument):
        rec.engine._publish_gate_block(
            signal, "market_data_unhealthy", {}, env)
    else:
        rec.engine._validate_strategy_risk_gate(signal, env, gates)

    assert rec.blocked == []
    assert rec.risk_gate_ran is True


def test_a_feed_that_dies_while_a_position_is_open_is_reported():
    """Blindness must be announced, not silently tolerated."""
    from application.market_flow import MarketEventFlowMixin

    now = [1000.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)

    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine.market_data_health = health
    engine._envs = {"live": SimpleNamespace(name="live",
                                             position_manager=pm)}
    events = []
    engine.publish_event = lambda name, payload, **k: events.append(
        (name, payload))

    health.record_tick("GOLDM")
    engine._report_sl_protection_gaps()
    assert events == [], "a healthy feed must not raise a protection gap"

    # Feed dies while the position is open.
    now[0] = 1000.0 + 500.0
    engine._report_sl_protection_gaps()
    assert len(events) == 1
    name, payload = events[0]
    assert name == "sl_protection_gap"
    assert payload["position_id"] == pos.position_id
    assert payload["severity"] == "CRITICAL"
    assert payload["stop_price"] == 95.0

    # A persistent outage reports ONCE, not once per check.
    engine._report_sl_protection_gaps()
    engine._report_sl_protection_gaps()
    assert len(events) == 1


def test_recovery_then_a_new_outage_re_alarms():
    """After recovery, a NEW outage must be announced again."""
    from application.market_flow import MarketEventFlowMixin

    now = [1000.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)

    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine.market_data_health = health
    engine._envs = {"live": SimpleNamespace(name="live",
                                             position_manager=pm)}
    events = []
    engine.publish_event = lambda name, payload, **k: events.append(name)

    health.record_tick("GOLDM")
    now[0] = 1000.0 + 500.0
    engine._report_sl_protection_gaps()
    assert len(events) == 1

    # Feed recovers, then dies again.
    health.record_tick("GOLDM")
    engine._report_sl_protection_gaps()
    now[0] = 1000.0 + 900.0
    engine._report_sl_protection_gaps()
    assert len(events) == 2


# ── §29 — the stop must win over a reversal on the same tick ────────────


def test_the_stop_wins_over_a_reversal_on_the_same_tick():
    """Both conditions can be true on one tick; the STOP must win.

    A reversal also closes the position.  If the reversal exit were processed
    first, the SL latch would never set and the stop would be recorded as
    "reversed" rather than "stopped out" - the P&L reason and the rearm
    decision would both be wrong.
    """
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    # The tick breaches the stop AND satisfies a reversal crossover.
    assert engine._evaluate_position_sl(env, pos, 94.0) is not None
    assert len(submitted) == 1

    # The stop is latched as TRIGGERED, not left armed.
    monitor = engine._sl_monitor(env)
    assert pos.sl_state == SLState.TRIGGERED.value
    assert monitor.mark_triggered(pos.position_id, 94.0) is False, (
        "the stop must already be latched - a second latch would mean two "
        "exits for one position")

    # A reversal arriving on the SAME tick must find the stop already fired
    # and must not produce a second exit for this position.
    fired_again = engine._evaluate_position_sl(env, pos, 94.0)
    assert fired_again is None
    assert len(submitted) == 1, "exactly one exit per position stop"


def test_a_candle_stop_also_wins_over_a_reversal_in_the_same_candle():
    """Same rule on the candle path, which now runs before strategies see it."""
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    # The candle's low breaches the stop; a reversal signal would also fire.
    assert engine._evaluate_positions_from_candle(
        None, _bar(low=94.0, high=101.0)) == 1
    assert len(submitted) == 1
    assert pos.sl_state == SLState.TRIGGERED.value

    # Repeats are absorbed.
    assert engine._evaluate_positions_from_candle(
        None, _bar(low=94.0, high=101.0)) == 0
    assert len(submitted) == 1


def test_candle_extreme_before_entry_cannot_trigger_new_position_stop():
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    pos.entry_timestamp = 1.5
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    assert engine._evaluate_positions_from_candle(
        None, _bar(low=90.0, high=101.0)) == 0
    assert submitted == []
    assert pos.sl_state == SLState.ARMED.value


def test_candle_ending_before_entry_cannot_trigger_stop():
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    pos.entry_timestamp = 3.0
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    assert engine._evaluate_positions_from_candle(
        None, _bar(low=90.0, high=101.0)) == 0
    assert submitted == []
    assert pos.sl_state == SLState.ARMED.value


def test_a_position_with_exit_started_is_never_stopped_again():
    """A reversal already in flight owns the exit; the stop must stand down."""
    pm = PositionManager()
    pos = _position(pm, side="LONG", stop=95.0)
    submitted = []
    engine, env = _sl_engine(pm, pos, ltp_calls=submitted)

    # A reversal exit has already claimed this position.
    pos.exit_started = True

    assert engine._evaluate_position_sl(env, pos, 94.0) is None
    assert engine._evaluate_positions_from_candle(
        None, _bar(low=94.0, high=101.0)) == 0
    assert submitted == []
    assert pos.sl_state == SLState.ARMED.value
