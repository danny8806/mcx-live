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
    health = MarketDataHealthMonitor(clock=lambda: ltt + 1, stale_after=90.0)
    health.record_tick(tick["instrument"], tick["event_timestamp"])
    assert health.is_healthy("GOLDM") is True
