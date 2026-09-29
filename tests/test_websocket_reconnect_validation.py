"""Reconnects restore subscriptions without replaying the same LTP twice."""
import json
import struct

from data.dhan.websocket_client import DhanWebSocketClient


class FakeSocket:
    def __init__(self):
        self.sent = []

    def send(self, message):
        self.sent.append(json.loads(message))


def _quote_packet(sid=571306, ltp=14908.0, ltt=1_800_000_000):
    packet = bytearray(64)
    packet[0] = 4
    struct.pack_into("<i", packet, 4, sid)
    struct.pack_into("<f", packet, 8, ltp)
    struct.pack_into("<h", packet, 12, 1)
    struct.pack_into("<i", packet, 14, ltt)
    return bytes(packet)


def test_duplicate_ltp_redelivery_after_reconnect_is_ignored_and_resubscribed():
    ticks = []
    statuses = []
    client = DhanWebSocketClient("client", "token", on_tick=ticks.append,
                                 on_status=statuses.append)
    client.add_instrument("GOLDPETAL", "571306", "MCX_COMM", "FUTCOM")
    first_socket = FakeSocket()
    client._on_open(first_socket)
    packet = _quote_packet()
    client._on_message(first_socket, packet)
    client._on_message(first_socket, packet)

    # Simulate a new Dhan socket session and Dhan replaying its last quote.
    second_socket = FakeSocket()
    client._on_open(second_socket)
    client._on_message(second_socket, packet)

    assert len(ticks) == 1
    assert first_socket.sent == second_socket.sent
    assert second_socket.sent[0]["RequestCode"] == 17
    assert second_socket.sent[0]["InstrumentList"] == [
        {"ExchangeSegment": "MCX_COMM", "SecurityId": "571306"}]
    assert client.stats["sub"] == 2
    assert statuses == ["connected", "connected"]


def test_reconnect_reloads_token_before_each_connection_attempt():
    loaded = []
    attempts = []
    client = DhanWebSocketClient(
        "client", "stale", token_loader=lambda: loaded.append("fresh") or "fresh",
        reconnect_delay=0,
    )

    def connect_once():
        attempts.append(client.token)
        if len(attempts) == 2:
            client._stop.set()

    client._connect_once = connect_once
    client._run_loop()

    assert loaded == ["fresh", "fresh"]
    assert attempts == ["fresh", "fresh"]
