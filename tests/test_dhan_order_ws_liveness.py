from types import SimpleNamespace

from execution.live import dhan_order_ws


def test_protocol_pong_keeps_quiet_order_socket_fresh(monkeypatch):
    now = [100.0]
    monkeypatch.setattr(dhan_order_ws.time, "time", lambda: now[0])
    feed = dhan_order_ws.DhanOrderUpdateFeed(
        "client", token="token", stale_threshold=30.0)
    socket = SimpleNamespace(send=lambda _message: None)
    feed._on_open(socket)
    assert feed.is_stale() is False

    # No order alerts arrive during this interval, but websocket-client receives
    # protocol pongs in response to its configured ping heartbeat.
    now[0] += 25.0
    feed._on_pong(socket, b"")
    assert feed.is_stale() is False
    assert feed._last_message_time == 100.0  # pong is transport, not an alert

    now[0] += 31.0
    assert feed.is_stale() is True


def test_order_socket_registers_pong_callback(monkeypatch):
    captured = {}

    class FakeSocket:
        def __init__(self, _url, **kwargs):
            captured.update(kwargs)

        def run_forever(self, **_kwargs):
            return None

    feed = dhan_order_ws.DhanOrderUpdateFeed(
        "client", token="token", socket_factory=FakeSocket)
    feed._connect_once()
    assert callable(captured["on_pong"])
