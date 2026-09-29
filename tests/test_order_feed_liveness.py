"""Order-feed liveness: a stale socket must never be reported as healthy.

The defect: ``_health_tick_once`` computed ``self._healthy = worker_alive`` and
never consulted the order WebSocket, while ``stats``/``snapshot`` published only
``ws_connected``. Dhan's socket keeps ``connected=True`` after its heartbeat
times out, so the service reported ``service_healthy: true`` and
``ws_connected: true`` for an order feed that was dead. That is the worst kind
of fault report: the operator's dashboard is green while order events are not
arriving.

Liveness is BOTH conditions: connected AND not stale. Fills still arrive over
REST, so a dead order feed degrades the service rather than stopping it.
"""
from __future__ import annotations

import threading

import pytest

from execution.live.broker_sync import BrokerSyncService


class FakeWs:
    def __init__(self, connected=True, stale=False):
        self.connected = connected
        self._stale = stale

    def is_stale(self):
        return self._stale


class FakePoller:
    def __init__(self, running=True):
        self.running = running
        self._lock = threading.RLock()

    def stats(self):
        return {}

    def snapshot(self):
        return {}

    def start(self):
        pass

    def stop(self, timeout=3.0):
        pass

    def is_stale(self, task, since=None):
        return False

    def last_run(self, task):
        return 0.0


class FakeEnv:
    name = "LIVE"
    is_live = True


def _service(ws, poller_running=True):
    svc = BrokerSyncService.__new__(BrokerSyncService)
    svc.env = FakeEnv()
    svc._poller = FakePoller(running=poller_running)
    svc._ws_enabled = True
    svc._ws_feed = ws
    svc._ws_stale_active = False
    svc._ws_stale_warned_at = 0.0
    svc._running = True
    svc._stop_event = threading.Event()
    svc._lock = threading.RLock()
    svc._healthy = True
    svc._stats = {"ws_records_ingested": 0, "ws_records_applied": 0,
                  "health_checks": 0, "worker_deaths": 0}
    svc._last_cycle = {}
    svc._consecutive_errors = {}
    svc._clock = lambda: 10000.0
    return svc


# ── the core contradiction ─────────────────────────────────────────────────

def test_stale_but_connected_ws_is_not_healthy():
    svc = _service(FakeWs(connected=True, stale=True))
    svc._health_tick_once()
    assert svc.healthy is False, \
        "connected=True with a timed-out heartbeat must not read as healthy"


def test_disconnected_ws_is_not_healthy():
    svc = _service(FakeWs(connected=False, stale=False))
    svc._health_tick_once()
    assert svc.healthy is False


def test_connected_and_fresh_ws_is_healthy():
    svc = _service(FakeWs(connected=True, stale=False))
    svc._health_tick_once()
    assert svc.healthy is True


def test_live_ws_does_not_depend_on_a_dead_poller():
    svc = _service(FakeWs(connected=True, stale=False), poller_running=False)
    svc._health_tick_once()
    assert svc.healthy is False


# ── the contradiction must be VISIBLE, not just internally correct ─────────

def test_stats_expose_stale_alongside_connected():
    """Publishing only ws_connected is what hid the fault."""
    svc = _service(FakeWs(connected=True, stale=True))
    svc._health_tick_once()
    s = svc.stats()
    assert s["ws_connected"] is True
    assert s["ws_stale"] is True
    assert s["ws_live"] is False
    assert s["service_healthy"] is False


def test_snapshot_exposes_the_same_liveness_triple():
    svc = _service(FakeWs(connected=True, stale=True))
    svc._health_tick_once()
    s = svc.snapshot()["service"]
    assert (s["ws_connected"], s["ws_stale"], s["ws_live"]) == (True, True, False)
    assert s["healthy"] is False


def test_stats_are_green_when_the_feed_is_actually_live():
    svc = _service(FakeWs(connected=True, stale=False))
    svc._health_tick_once()
    s = svc.stats()
    assert s["ws_live"] is True
    assert s["service_healthy"] is True


def test_disabled_ws_does_not_degrade_health():
    svc = _service(FakeWs(connected=False, stale=True), poller_running=True)
    svc._ws_enabled = False
    svc._health_tick_once()
    assert svc.healthy is True


def test_missing_ws_feed_does_not_degrade_health():
    svc = _service(None)
    svc._ws_feed = None
    svc._health_tick_once()
    assert svc.healthy is True


# ── state bookkeeping must not thrash ──────────────────────────────────────

def test_stale_state_is_recorded_and_cleared_on_recovery():
    svc = _service(FakeWs(connected=True, stale=True))
    svc._health_tick_once()
    assert svc._ws_stale_active is True
    svc._ws_feed = FakeWs(connected=True, stale=False)
    svc._health_tick_once()
    assert svc._ws_stale_active is False
    assert svc.healthy is True


def test_repeated_stale_ticks_count_the_degraded_window():
    svc = _service(FakeWs(connected=True, stale=True))
    for _ in range(3):
        svc._health_tick_once()
    assert svc._stats["health_checks"] == 3
    assert svc._stats["ws_unhealthy_ticks"] == 3


# ── packet parsing: depth/OHLC are not data loss ───────────────────────────

def test_market_depth_packet_is_ignored_not_unknown():
    from data.dhan.websocket_client import DhanWebSocketClient
    ws = DhanWebSocketClient.__new__(DhanWebSocketClient)
    ws._stats = {"recv": 1}
    frame = bytearray(40)
    frame[0] = 5  # market depth
    assert ws._parse_packet(bytes(frame)) is None
    assert ws._stats.get("ignored_packets") == 1
    assert "unknown_packets" not in ws._stats, \
        "market depth must not be reported as unrecognised data loss"


def test_ohlc_packet_is_ignored_not_unknown():
    from data.dhan.websocket_client import DhanWebSocketClient
    ws = DhanWebSocketClient.__new__(DhanWebSocketClient)
    ws._stats = {"recv": 1}
    frame = bytearray(40)
    frame[0] = 8
    assert ws._parse_packet(bytes(frame)) is None
    assert ws._stats.get("ignored_packets") == 1


def test_a_genuinely_unknown_code_is_still_flagged():
    """Silently dropping real data is how a stop goes blind unnoticed."""
    from data.dhan.websocket_client import DhanWebSocketClient
    ws = DhanWebSocketClient.__new__(DhanWebSocketClient)
    ws._stats = {"recv": 1}
    frame = bytearray(40)
    frame[0] = 99
    assert ws._parse_packet(bytes(frame)) is None
    assert ws._stats.get("unknown_packets") == 1


def test_quote_packet_still_parses():
    """The fix must not break the packets that actually carry LTP."""
    import struct
    from data.dhan.websocket_client import DhanWebSocketClient
    ws = DhanWebSocketClient.__new__(DhanWebSocketClient)
    ws._stats = {"recv": 1}
    frame = bytearray(64)
    frame[0] = 4
    struct.pack_into("<i", frame, 4, 12345)
    struct.pack_into("<f", frame, 8, 4567.5)
    out = ws._parse_packet(bytes(frame))
    assert out is not None
    assert out["security_id"] == "12345"
    assert out["ltp"] == pytest.approx(4567.5)
