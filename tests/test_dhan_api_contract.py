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


def test_dhan_candle_parser_rejects_mismatched_ohlc_arrays_instead_of_truncating():
    client = DhanRESTClient()
    with pytest.raises(ValueError, match="mismatched array lengths"):
        client._to_candles({
            "timestamp": [1, 2], "open": [10, 11], "high": [12, 13],
            "low": [9], "close": [11, 12], "volume": [4, 5],
        })


def test_dhan_candle_parser_accepts_missing_optional_volume_as_zeroes():
    client = DhanRESTClient()
    assert client._to_candles({
        "timestamp": [1], "open": [10], "high": [12], "low": [9],
        "close": [11],
    }) == [[1, 10, 12, 9, 11, 0]]


def test_market_data_adapter_disconnect_stops_rest_token_scheduler():
    from data.dhan.adapter import DhanDataAdapter

    calls = []
    adapter = DhanDataAdapter.__new__(DhanDataAdapter)
    adapter.ws = SimpleNamespace(disconnect=lambda: calls.append("ws"))
    adapter.rest = SimpleNamespace(stop_scheduler=lambda: calls.append("scheduler"))
    adapter._connected = True

    adapter.disconnect()

    assert calls == ["ws", "scheduler"]
    assert adapter._connected is False


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


def test_rest_post_retry_acquires_rate_limit_token_for_each_http_attempt(monkeypatch):
    import data.dhan.rest_client as rest_client_module

    class RetryingSession:
        def __init__(self):
            self.calls = 0

        def post(self, *_args, **_kwargs):
            self.calls += 1
            return _Response(503, {"error": "temporary"})

    class CountingLimiter:
        def __init__(self):
            self.calls = 0

        def acquire(self):
            self.calls += 1

    monkeypatch.setattr(rest_client_module.time, "sleep", lambda _seconds: None)
    client = DhanRESTClient(max_retries=3)
    session = RetryingSession()
    limiter = CountingLimiter()
    client._session = session
    client._headers = lambda: {"access-token": "test"}
    client.limiter = limiter

    with pytest.raises(DhanHTTPError) as exc:
        client._post("/charts/intraday", {"securityId": "123"})

    assert exc.value.status == 503
    assert session.calls == 3
    assert limiter.calls == 3


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


def test_stale_cancel_verification_does_not_regress_partial_fill_state():
    class Http:
        def _delete(self, _path):
            return {}

        def _get(self, _path):
            return [{"orderId": "B-PART", "orderStatus": "PENDING",
                     "filledQty": 1, "averageTradedPrice": 100}]

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-PART"] = {
        "broker_order_id": "B-PART", "status": "filled", "raw_status": "PART_TRADED",
        "quantity": 2, "filled_quantity": 1, "last_accounted_qty": 1,
        "average_fill_price": 100.0,
    }

    result = broker.cancel_order("B-PART")

    assert result["ok"] is False
    assert result["raw_status"] == "PART_TRADED"


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

    assert statuses["B-BAD"]["status"] == "submitted"
    assert statuses["B-GOOD"]["filled_quantity"] == 1
    assert len(statuses["B-GOOD"]["fills"]) == 1
    assert statuses.poll_errors[0]["broker_order_id"] == "B-BAD"


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


def test_broker_read_failures_are_not_reported_as_empty_books_or_zero_account():
    class Http:
        def _get(self, path):
            raise OSError(f"offline: {path}")

    broker = DhanRestTransport(client_id="TEST", http=Http())

    with pytest.raises(RuntimeError, match="day order book"):
        broker.day_order_book()
    with pytest.raises(RuntimeError, match="tradebook"):
        broker.tradebook()
    with pytest.raises(RuntimeError, match="fund-limit"):
        broker.account_status()


def test_malformed_position_payload_does_not_turn_into_flat_or_zero_pnl():
    class Http:
        def _get(self, path):
            if path == "/positions":
                return [{"securityId": "123", "netQty": "unknown"}]
            if path == "/fundlimit":
                return {"availabelBalance": 1000, "utilizedAmount": 0}
            raise AssertionError(path)

    broker = DhanRestTransport(
        client_id="TEST", http=Http(),
        instruments={"GOLDM": {"security_id": "123"}},
    )

    with pytest.raises(ValueError, match="quantity is malformed"):
        broker.positions()
    with pytest.raises(ValueError, match="quantity is malformed"):
        broker.account_status()


def test_unknown_order_status_is_reported_and_does_not_mutate_known_state():
    class Http:
        def _get(self, path):
            return {"orderId": "B-UNKNOWN", "orderStatus": "MYSTERY",
                    "filledQty": 0}

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-UNKNOWN"] = {
        "broker_order_id": "B-UNKNOWN", "status": "submitted", "raw_status": "PENDING",
        "quantity": 1, "filled_quantity": 0, "last_accounted_qty": 0,
        "average_fill_price": 0.0,
    }

    statuses = broker.order_statuses()

    assert statuses["B-UNKNOWN"]["raw_status"] == "PENDING"
    assert "unknown or missing" in statuses.poll_errors[0]["error"]


def test_order_status_poll_preserves_good_rows_and_exposes_failed_status_reads():
    class Http:
        def _get(self, path):
            if path == "/orders/B-FAIL":
                raise TimeoutError("slow Dhan response")
            if path == "/orders/B-OK":
                return {"orderId": "B-OK", "orderStatus": "TRADED",
                        "filledQty": 1, "averageTradedPrice": 99}
            raise AssertionError(path)

    broker = DhanRestTransport(client_id="TEST", http=Http())
    for bid in ("B-FAIL", "B-OK"):
        broker._orders[bid] = {
            "broker_order_id": bid, "status": "submitted", "raw_status": "PENDING",
            "quantity": 1, "filled_quantity": 0, "last_accounted_qty": 0,
            "average_fill_price": 0.0, "side": "BUY", "instrument": "GOLDM",
        }

    statuses = broker.order_statuses()

    assert statuses["B-FAIL"]["status"] == "submitted"  # last known state retained
    assert statuses["B-OK"]["filled_quantity"] == 1
    assert len(statuses["B-OK"]["fills"]) == 1
    assert statuses.poll_errors == [{
        "broker_order_id": "B-FAIL", "error": "slow Dhan response"}]


def test_order_status_rest_wait_does_not_hold_transport_state_lock():
    import threading

    started = threading.Event()
    release = threading.Event()

    class Http:
        def _get(self, path):
            started.set()
            assert release.wait(timeout=2)
            return {"orderId": "B-SLOW", "orderStatus": "PENDING",
                    "filledQty": 0, "averageTradedPrice": 0}

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-SLOW"] = {
        "broker_order_id": "B-SLOW", "status": "submitted", "raw_status": "PENDING",
        "quantity": 1, "filled_quantity": 0, "last_accounted_qty": 0,
        "average_fill_price": 0.0, "side": "BUY", "instrument": "GOLDM",
    }
    worker = threading.Thread(target=broker.order_statuses)
    worker.start()
    assert started.wait(timeout=1)
    # This takes the transport state lock and must return while HTTP is blocked.
    assert broker.fills() == []
    release.set()
    worker.join(timeout=2)
    assert not worker.is_alive()


def test_late_pending_rest_response_cannot_regress_a_broker_confirmed_fill():
    class Http:
        def _get(self, path):
            assert path == "/orders/B-MONO"
            return {"orderId": "B-MONO", "orderStatus": "PENDING",
                    "filledQty": 0, "averageTradedPrice": 0}

    broker = DhanRestTransport(client_id="TEST", http=Http())
    broker._orders["B-MONO"] = {
        "broker_order_id": "B-MONO", "status": "filled", "raw_status": "TRADED",
        "quantity": 1, "filled_quantity": 1, "last_accounted_qty": 1,
        "average_fill_price": 100.0, "side": "BUY", "instrument": "GOLDM",
    }

    status = broker.order_statuses()["B-MONO"]

    assert status["status"] == "filled"
    assert status["raw_status"] == "TRADED"
    assert status["filled_quantity"] == 1


def test_static_ip_helper_matches_documented_primary_and_secondary_fields():
    class Http:
        def _get(self, path):
            assert path == "/ip/getIP"
            return {"primaryIP": "203.0.113.10", "secondaryIP": "203.0.113.11"}

    broker = DhanRestTransport(client_id="TEST", http=Http())

    assert broker.verify_static_ip("203.0.113.11")["whitelisted"] is True
    assert broker.verify_static_ip("198.51.100.5")["whitelisted"] is False
    assert broker.verify_static_ip()["whitelisted"] is None
