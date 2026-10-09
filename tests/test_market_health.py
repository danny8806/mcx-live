"""Market-data freshness checks used by the 15-minute SuperTrend runtime."""
from types import SimpleNamespace
import threading

from execution.live.market_health import MarketDataHealthMonitor


def test_unknown_and_stale_feed_are_not_healthy():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)

    assert not health.is_healthy("GOLDM")
    assert health.age("GOLDM") is None

    health.record_tick("GOLDM")
    assert health.is_healthy("GOLDM")
    now[0] += 91.0
    assert not health.is_healthy("GOLDM")


def test_disconnect_overrides_recent_tick_until_feed_recovers():
    now = [100.0]
    health = MarketDataHealthMonitor(clock=lambda: now[0], stale_after=90.0)
    health.record_tick("SILVERM")
    health.mark_unhealthy("SILVERM")

    assert not health.is_healthy("SILVERM")
    health.record_tick("SILVERM")
    assert health.is_healthy("SILVERM")
    assert health.unhealthy_since("SILVERM") is None


def test_rest_candle_does_not_claim_websocket_feed_recovered():
    from application.market_flow import MarketEventFlowMixin
    from core.timeframe_engine import Bar

    health = MarketDataHealthMonitor(clock=lambda: 100.0, stale_after=90.0)
    health.record_tick("GOLDM")
    health.mark_unhealthy("GOLDM")
    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine._running = True
    engine.health = SimpleNamespace(record_bar=lambda: None)
    engine.market_status = SimpleNamespace(mark_rest_data_fresh=lambda: None)
    engine.market_data_health = health
    engine.candle_router = SimpleNamespace(on_candle=lambda *args, **kwargs: None)
    bar = Bar(instrument="GOLDM", timeframe="15m", start_ts=1, end_ts=2,
              open=100, high=102, low=99, close=101, volume=1)

    engine._on_bar_closed(bar)

    assert not health.is_healthy("GOLDM")


def test_disconnected_websocket_publishes_invalid_tick_without_strategy_data():
    from application.market_flow import MarketEventFlowMixin

    health = MarketDataHealthMonitor(clock=lambda: 100.0, stale_after=90.0)
    ws = SimpleNamespace(connected=False, _last_tick_time=100.0,
                         _stats={"tick": 0}, is_stale=lambda: False)
    events = []
    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine.data_adapter = SimpleNamespace(ws=ws)
    engine.market_data_health = health
    engine.market_status = SimpleNamespace(update_data_status=lambda **kwargs: None)
    engine.health = SimpleNamespace(update_component=lambda *args: None,
                                    record_tick=lambda: None)
    engine._maybe_enable_trading = lambda: None
    engine._lock = threading.RLock()
    engine._envs = {}
    engine.event_bus = SimpleNamespace(
        publish=lambda topic, event: events.append((topic, event)))

    engine._on_tick({"instrument": "GOLDM", "ltp": 100.0,
                     "event_timestamp": 99.0, "receive_timestamp": 100.0})

    assert len(events) == 1
    assert events[0][1].ltp == 0.0
    assert not health.is_healthy("GOLDM")


def test_completed_15m_strategy_signal_is_sent_to_execution_handler():
    from application.market_flow import MarketEventFlowMixin

    signal = object()
    strategy = SimpleNamespace(
        signal_timeframe="15m", on_candle=lambda event: signal)
    engine = object.__new__(type("E", (MarketEventFlowMixin,), {}))
    engine._running = True
    engine._lock = threading.RLock()
    engine.health = SimpleNamespace(record_bar=lambda: None)
    engine.market_status = SimpleNamespace(mark_rest_data_fresh=lambda: None)
    engine._maybe_enable_trading = lambda: None
    engine._env_for = lambda _name: SimpleNamespace(pending_triggers=None)
    sent = []
    engine._bind_signal_position = lambda *args: None
    engine._process_signal = lambda value, name: sent.append((value, name))

    handler = engine._make_candle_handler(strategy, "live")
    handler(SimpleNamespace(timeframe="15m"))

    assert sent == [(signal, "live")]
