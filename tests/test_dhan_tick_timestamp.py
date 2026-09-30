"""Dhan market-feed timestamps must retain their documented epoch value."""
from types import SimpleNamespace

from data.dhan.adapter import DhanDataAdapter
from execution.live.market_health import MarketDataHealthMonitor


def test_dhan_ltt_epoch_remains_fresh_after_normalization():
    adapter = DhanDataAdapter.__new__(DhanDataAdapter)
    adapter._security_to_symbol = {"123": "GOLDM"}
    adapter._instruments = {
        "GOLDM": SimpleNamespace(exchange_segment="MCX_COMM")}
    ltt = 1_800_000_000

    tick = adapter._normalize_tick({"security_id": "123", "ltt": ltt,
                                    "ltp": 72400.0})

    assert tick["event_timestamp"] == float(ltt)
    health = MarketDataHealthMonitor(clock=lambda: tick["receive_timestamp"] + 1,
                                     stale_after=90.0)
    health.record_tick(tick["instrument"], tick["receive_timestamp"])
    assert health.is_healthy("GOLDM") is True


def test_tick_health_uses_receive_time_while_event_ltt_is_preserved():
    from application.market_flow import MarketEventFlowMixin
    import threading

    now = 1_800_000_000.0
    health = MarketDataHealthMonitor(clock=lambda: now + 1, stale_after=90.0)
    ws = SimpleNamespace(connected=True, _last_tick_time=now,
                         _stats={"tick": 1}, is_stale=lambda: False)
    emitted = []
    engine = object.__new__(type("Engine", (MarketEventFlowMixin,), {}))
    engine._running = True
    engine._lock = threading.RLock()
    engine.data_adapter = SimpleNamespace(ws=ws)
    engine.market_data_health = health
    engine.market_status = SimpleNamespace(update_data_status=lambda **kw: None)
    engine.health = SimpleNamespace(update_component=lambda *a: None,
                                    record_tick=lambda: None)
    engine._envs = {}
    engine.event_bus = SimpleNamespace(
        publish=lambda topic, event: emitted.append((topic, event)))
    engine._maybe_enable_trading = lambda: None
    engine._report_sl_protection_gaps = lambda: None

    # Dhan event LTT may differ from host clock; receipt time determines feed
    # freshness, while the event's source timestamp is kept intact.
    engine._on_tick({"instrument": "GOLDM", "ltp": 72_400.0,
                     "event_timestamp": now + 19_800,
                     "receive_timestamp": now})

    assert health.is_healthy("GOLDM") is True
    assert emitted[0][1].timestamp == now + 19_800
