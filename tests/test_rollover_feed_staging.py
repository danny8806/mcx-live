import json

from data.dhan.adapter import DhanDataAdapter
from data.dhan.websocket_client import DhanWebSocketClient


class _Socket:
    def __init__(self):
        self.sent = []

    def send(self, raw):
        self.sent.append(json.loads(raw))


def test_live_market_socket_subscribes_and_unsubscribes_staged_contract():
    client = DhanWebSocketClient(client_id="client", token="token")
    socket = _Socket()
    client._ws = socket
    client._connected = True

    client.add_instrument("MCX:GOLDM202611", "571445", "MCX_COMM", "FUTCOM")
    client.remove_instrument("MCX:GOLDM202611")

    assert [frame["RequestCode"] for frame in socket.sent] == [17, 18]
    assert socket.sent[0]["InstrumentList"] == [
        {"ExchangeSegment": "MCX_COMM", "SecurityId": "571445"}]


def test_candidate_ticks_stay_aliased_until_promotion():
    adapter = DhanDataAdapter.__new__(DhanDataAdapter)
    adapter._instruments = {}
    adapter._security_to_symbol = {}
    adapter._live_ltp = {}
    import threading
    adapter._ltp_lock = threading.Lock()

    class _Ws:
        def __init__(self):
            self.added = []
            self.removed = []

        def add_instrument(self, *args):
            self.added.append(args)

        def remove_instrument(self, *args):
            self.removed.append(args)

    adapter.ws = _Ws()
    adapter.register_instruments({"GOLDM": {
        "symbol": "MCX:GOLDM202610", "security_id": "569003",
        "exchange_segment": "MCX_COMM", "instrument": "FUTCOM",
        "multiplier": 10, "tick_size": 1, "lot_size": 1,
    }})
    adapter.register_rollover_candidate("GOLDM__NEXT", {
        "symbol": "MCX:GOLDM202611", "security_id": "571445",
        "exchange_segment": "MCX_COMM", "instrument": "FUTCOM",
        "multiplier": 10, "tick_size": 1, "lot_size": 1,
    })

    assert adapter._security_to_symbol["571445"] == "GOLDM__NEXT"
    adapter.promote_rollover_candidate("GOLDM__NEXT", "GOLDM")
    assert adapter._security_to_symbol["571445"] == "GOLDM"
    assert "569003" not in adapter._security_to_symbol
