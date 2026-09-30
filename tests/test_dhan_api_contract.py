"""Contract regressions for shapes and HTTP semantics in DhanHQ v2 docs.

All tests use local fakes; no credentials or network access are involved.
"""
import json
from types import SimpleNamespace

from data.dhan.rest_client import DhanRESTClient
from execution.live.dhan_transport import DhanRestTransport
from execution.live.dhan_order_ws import parse_order_alert


class _Response:
    def __init__(self, status_code, body=None, text=None):
        self.status_code = status_code
        self._body = body
        self.text = (json.dumps(body) if body is not None else "") if text is None else text

    def json(self):
        if not self.text:
            raise ValueError("empty response body")
        return self._body


class _Session:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def delete(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def test_documented_empty_202_cancel_ack_is_not_retried_or_treated_as_error():
    client = DhanRESTClient(max_retries=3)
    session = _Session(_Response(202, text=""))
    client._session = session
    client._headers = lambda: {"access-token": "test"}
    client.limiter = SimpleNamespace(acquire=lambda: None)

    assert client._delete("/orders/BROKER-1") == {}
    assert len(session.calls) == 1
    assert session.calls[0][0].endswith("/orders/BROKER-1")
    assert "json" not in session.calls[0][1]


def test_transport_confirms_order_state_after_documented_202_cancel_ack():
    class Http:
        def __init__(self):
            self.calls = []

        def _delete(self, path):
            self.calls.append(("DELETE", path))
            return {}  # documented 202 response may have no JSON body

        def _get(self, path):
            self.calls.append(("GET", path))
            return [{"orderId": "B-CANCEL", "orderStatus": "CANCELLED"}]

    http = Http()
    broker = DhanRestTransport(
        client_id="TEST", http=http,
        instruments={"GOLDM": {"security_id": "123", "exchange_segment": "MCX_COMM"}},
        gate_enabled=True,
    )
    broker._orders["B-CANCEL"] = {
        "status": "submitted", "raw_status": "PENDING", "quantity": 1,
        "filled_quantity": 0, "last_accounted_qty": 0,
        "correlation_id": "MCX-cancel-1",
    }

    result = broker.cancel_order("B-CANCEL")

    assert http.calls == [("DELETE", "/orders/B-CANCEL"),
                          ("GET", "/orders/B-CANCEL")]
    assert result["ok"] is True
    assert result["raw_status"] == "CANCELLED"


class _DhanHttp:
    def __init__(self):
        self.puts = []

    def _put(self, path, payload):
        self.puts.append((path, dict(payload)))
        return {"orderId": "B-1", "orderStatus": "TRANSIT"}


def test_regular_order_modify_uses_only_documented_fields_not_bracket_leg_enum():
    http = _DhanHttp()
    broker = DhanRestTransport(
        client_id="TEST", http=http,
        instruments={"GOLDM": {"security_id": "123", "exchange_segment": "MCX_COMM"}},
        gate_enabled=True,
    )
    broker._orders["B-1"] = {"quantity": 1, "correlation_id": "C-1"}

    result = broker.modify_order("B-1", order_type="LIMIT", price=100.0)

    assert result["ok"] is True
    path, payload = http.puts[0]
    assert path == "/orders/B-1"
    assert payload == {
        "dhanClientId": "TEST", "orderId": "B-1", "orderType": "LIMIT",
        "quantity": 1, "price": 100.0, "triggerPrice": 0.0,
        "validity": "DAY",
    }


def test_documented_dhan_order_update_frame_fields_normalize_for_internal_use():
    # Field names/status casing follow Dhan's published order-update example.
    frame = {
        "Data": {
            "Source": "P", "OrderNo": "1124091136546",
            "Status": "Traded", "TradedQty": 1,
            "RemainingQuantity": 0, "AvgTradedPrice": 146803.0,
            "CorrelationId": "MCX-test-123", "ReasonDescription": "CONFIRMED",
            "OrderType": "LMT",
        },
        "Type": "order_alert",
    }

    parsed = parse_order_alert(json.dumps(frame))

    assert parsed == {
        "broker_order_id": "1124091136546", "status": "filled",
        "raw_status": "TRADED", "filled_quantity": 1,
        "average_fill_price": 146803.0, "remaining_quantity": 0,
        "correlation_id": "MCX-test-123", "reason": "CONFIRMED",
        "order_type": "LMT",
    }


def test_official_rest_order_array_and_cumulative_fill_fields_are_idempotent():
    class Http:
        def __init__(self):
            self.rows = [{
                "orderId": "B-2", "orderStatus": "PART_TRADED",
                "quantity": 3, "filledQty": 1,
                "averageTradedPrice": 146803.0, "remainingQuantity": 2,
            }]

        def _get(self, path):
            assert path == "/orders/B-2"
            return list(self.rows)

    broker = DhanRestTransport(
        client_id="TEST", http=Http(),
        instruments={"GOLDM": {"security_id": "123", "exchange_segment": "MCX_COMM"}},
        gate_enabled=True,
    )
    broker._orders["B-2"] = {
        "broker_order_id": "B-2", "status": "submitted", "raw_status": "PENDING",
        "quantity": 3, "filled_quantity": 0, "last_accounted_qty": 0,
        "average_fill_price": 0.0, "side": "BUY", "instrument": "GOLDM",
        "correlation_id": "MCX-test-456",
    }

    first = broker.order_statuses()["B-2"]
    second = broker.order_statuses()["B-2"]

    assert first["raw_status"] == "PART_TRADED"
    assert first["status"] == "filled"  # normalized; raw partial remains preserved
    assert first["filled_quantity"] == 1
    assert first["fills"][0]["quantity"] == 1
    assert first["fills"][0]["price"] == 146803.0
    assert len(second["fills"]) == 1  # repeated REST reads do not duplicate fill
