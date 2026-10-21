"""Fresh contract tests for Dhan's single-order status recovery lookup."""

import json
from pathlib import Path

import pytest

from execution.live.dhan_transport import DhanRestTransport


class _Http:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def _get(self, path):
        self.calls.append(path)
        return self.response


@pytest.mark.parametrize(
    ("raw_status", "normalized"),
    [
        ("PENDING", "submitted"),
        ("TRANSIT", "submitted"),
        # Transport's status vocabulary folds PART_TRADED into ``filled``;
        # callers must inspect the preserved raw status and fill quantities.
        ("PART_TRADED", "filled"),
        ("TRADED", "filled"),
        ("REJECTED", "rejected"),
        ("CANCELLED", "cancelled"),
        ("EXPIRED", "expired"),
    ],
)
def test_single_order_status_decodes_documented_dhan_statuses(raw_status, normalized):
    http = _Http({
        "orderId": "DHAN-123",
        "orderStatus": raw_status,
        "omsErrorDescription": "broker detail",
    })
    transport = DhanRestTransport(client_id="TEST", http=http)

    result = transport.order_status("DHAN-123")

    assert http.calls == ["/orders/DHAN-123"]
    assert result["broker_order_id"] == "DHAN-123"
    assert result["raw_status"] == raw_status
    assert result["status"] == normalized
    assert result["reason"] == "broker detail"


def test_single_order_status_rejects_unknown_status_without_poisoning_recovery():
    transport = DhanRestTransport(
        client_id="TEST", http=_Http({"orderId": "DHAN-123", "orderStatus": "???"})
    )

    with pytest.raises(ValueError, match="unknown or missing Dhan order status"):
        transport.order_status("DHAN-123")


def test_single_order_status_rejects_empty_status_body():
    transport = DhanRestTransport(client_id="TEST", http=_Http({}))

    with pytest.raises(ValueError, match="response empty"):
        transport.order_status("DHAN-123")


@pytest.mark.parametrize("instrument", ["GOLDM", "SILVERM"])
def test_market_payload_uses_current_instrument_and_product_settings(instrument):
    config_path = Path(__file__).resolve().parents[1] / "config" / "live_settings.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))

    class PlacementHttp:
        def __init__(self):
            self.payloads = []

        def _post(self, path, payload, **_kwargs):
            assert path == "/orders"
            self.payloads.append(dict(payload))
            return {"orderId": "DHAN-TEST", "orderStatus": "PENDING"}

    http = PlacementHttp()
    transport = DhanRestTransport(
        client_id="CLIENT-TEST",
        http=http,
        gate_enabled=True,
        product_type=config["dhan"]["product_type"],
        instruments=config["instruments"],
    )
    correlation_id = "MCX-CURRENT-TEST"

    transport.place_market_order(
        side="BUY", quantity=1, instrument=instrument,
        order_type="MARKET", correlation_id=correlation_id,
    )

    assert http.payloads == [{
        "dhanClientId": "CLIENT-TEST",
        "transactionType": "BUY",
        "exchangeSegment": config["instruments"][instrument]["exchange_segment"],
        "productType": config["dhan"]["product_type"],
        "orderType": "MARKET",
        "validity": "DAY",
        "securityId": config["instruments"][instrument]["security_id"],
        "quantity": 1,
        "disclosedQuantity": 0,
        "price": "",
        "triggerPrice": "",
        "afterMarketOrder": False,
        "correlationId": correlation_id,
    }]
