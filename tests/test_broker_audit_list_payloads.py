import json

from persistence.broker_audit import BrokerAuditStore


def test_audit_store_preserves_and_sanitizes_list_responses(tmp_path):
    store = BrokerAuditStore(str(tmp_path / "broker-audit.db"))
    try:
        store.record(
            action="TRADEBOOK", endpoint="/trades", http_method="GET",
            response=[
                {"orderId": "B-1", "tradedQuantity": 1,
                 "tradedPrice": 123.0, "accessToken": "must-not-persist"},
                {"orderId": "B-2", "tradedQuantity": 2,
                 "tradedPrice": 124.0},
            ], http_status=200)

        row = store.query(action="TRADEBOOK", limit=1)[0]
        payload = json.loads(row["response_payload"])

        assert len(payload) == 2
        assert payload[0]["orderId"] == "B-1"
        assert payload[0]["accessToken"] == "***"
        assert payload[1]["tradedQuantity"] == 2
        assert row["http_status"] == 200
    finally:
        store._db.close()
