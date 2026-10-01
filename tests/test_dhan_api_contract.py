"""Contract regressions for shapes and HTTP semantics in DhanHQ v2 docs.

All tests use local fakes; no credentials or network access are involved.
"""
import json
from types import SimpleNamespace

import pytest

from data.dhan.rest_client import DhanHTTPError, DhanRESTClient
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


def test_get_404_returns_immediately_for_higher_level_bounded_retry():
    class NotFoundSession:
        def __init__(self):
            self.calls = []

        def get(self, url, **kwargs):
            self.calls.append(url)
            return _Response(404, {"error": "not found"})

    client = DhanRESTClient(max_retries=3)
    session = NotFoundSession()
    client._session = session
    client._headers = lambda: {"access-token": "test"}
    client.limiter = SimpleNamespace(acquire=lambda: None)

    with pytest.raises(DhanHTTPError) as exc:
        client._get("/orders/external/CORR")

    assert exc.value.status == 404
    assert len(session.calls) == 1


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


def test_cumulative_average_is_converted_to_incremental_fill_price():
    class Http:
        rows = [
            {"orderId": "B-PARTIAL", "orderStatus": "PART_TRADED",
             "quantity": 3, "filledQty": 1, "averageTradedPrice": 100.0},
            {"orderId": "B-PARTIAL", "orderStatus": "PART_TRADED",
             "quantity": 3, "filledQty": 2, "averageTradedPrice": 101.0},
        ]

        def _get(self, path):
            assert path == "/orders/B-PARTIAL"
            return self.rows.pop(0)

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-PARTIAL"] = {
        "broker_order_id": "B-PARTIAL", "status": "submitted",
        "raw_status": "PENDING", "quantity": 3, "filled_quantity": 0,
        "last_accounted_qty": 0, "average_fill_price": 0.0,
        "side": "BUY", "instrument": "GOLDM",
    }

    first = broker.order_statuses()["B-PARTIAL"]
    second = broker.order_statuses()["B-PARTIAL"]

    assert [(f["quantity"], f["price"]) for f in first["fills"]] == [(1, 100.0)]
    # Two contracts average 101, so the second contract must be 102, not 101.
    assert [(f["quantity"], f["price"]) for f in second["fills"]] == [
        (1, 100.0), (1, 102.0)]
    assert sum(f["quantity"] * f["price"] for f in second["fills"]) / 2 == 101.0


def test_bad_fill_numbers_do_not_abort_polling_other_orders():
    class Http:
        def _get(self, path):
            if path == "/orders/B-BAD":
                return {"orderId": "B-BAD", "orderStatus": "PART_TRADED",
                        "filledQty": "not-a-number", "averageTradedPrice": 10}
            if path == "/orders/B-GOOD":
                return {"orderId": "B-GOOD", "orderStatus": "TRADED",
                        "filledQty": 1, "averageTradedPrice": 20}
            raise AssertionError(path)

    broker = DhanRestTransport(client_id="TEST", http=Http())
    for bid in ("B-BAD", "B-GOOD"):
        broker._orders[bid] = {
            "broker_order_id": bid, "status": "submitted", "raw_status": "PENDING",
            "quantity": 1, "filled_quantity": 0, "last_accounted_qty": 0,
            "average_fill_price": 0.0, "side": "BUY", "instrument": "GOLDM",
        }

    statuses = broker.order_statuses()

    assert "B-BAD" not in statuses
    assert statuses["B-GOOD"]["filled_quantity"] == 1
    assert len(statuses["B-GOOD"]["fills"]) == 1


def test_stale_lower_cumulative_quantity_does_not_regress_fill_truth():
    class Http:
        rows = [
            {"orderId": "B-STALE", "orderStatus": "PART_TRADED",
             "quantity": 3, "filledQty": 2, "averageTradedPrice": 101.0},
            {"orderId": "B-STALE", "orderStatus": "PENDING",
             "quantity": 3, "filledQty": 1, "averageTradedPrice": 90.0},
        ]

        def _get(self, path):
            assert path == "/orders/B-STALE"
            return self.rows.pop(0)

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-STALE"] = {
        "broker_order_id": "B-STALE", "status": "submitted", "raw_status": "PENDING",
        "quantity": 3, "filled_quantity": 0, "last_accounted_qty": 0,
        "average_fill_price": 0.0, "side": "BUY", "instrument": "GOLDM",
    }

    first = broker.order_statuses()["B-STALE"]
    second = broker.order_statuses()["B-STALE"]

    assert first["filled_quantity"] == 2
    assert second["filled_quantity"] == 2
    assert second["average_fill_price"] == 101.0
    assert len(second["fills"]) == 1


@pytest.mark.parametrize("trade_response", [
    {"orderId": "B-TRADE", "exchangeTradeId": "T-1",
     "tradedQuantity": 1, "tradedPrice": 146803.0},
    [{"orderId": "B-TRADE", "exchangeTradeId": "T-1",
      "tradedQuantity": 1, "tradedPrice": 146803.0}],
])
def test_order_trade_decoder_accepts_documented_object_and_multi_trade_list(
        trade_response):
    class Http:
        def _get(self, path):
            assert path == "/trades/B-TRADE"
            return trade_response

    broker = DhanRestTransport(client_id="TEST", http=Http())

    rows = broker.order_trades("B-TRADE")

    assert len(rows) == 1
    assert rows[0]["exchangeTradeId"] == "T-1"
    assert rows[0]["tradedQuantity"] == 1


def test_account_pnl_keeps_dhan_realized_profit_for_flat_positions():
    class Http:
        def _get(self, path):
            if path == "/fundlimit":
                return {"availabelBalance": 1000, "utilizedAmount": 0}
            if path == "/positions":
                return [
                    {"netQty": 0, "realizedProfit": -9190,
                     "unrealizedProfit": 0},
                    {"netQty": 1, "realizedProfit": -25,
                     "unrealizedProfit": 75},
                ]
            raise AssertionError(path)

    broker = DhanRestTransport(client_id="TEST", http=Http())

    account = broker.account_status()

    assert account["realized_pnl"] == -9215
    assert account["unrealized_pnl"] == 75
    assert account["net_pnl"] == -9140
    assert account["realized_pnl_source"] == "dhan_positions_realized_profit"
