from types import SimpleNamespace

from execution.live.poller import LiveBrokerPoller
from execution.live.tradebook_reconciliation import compare_tradebook


def test_tradebook_comparison_flags_missing_system_fills_but_not_manual_trades():
    report = compare_tradebook(
        broker_orders=[
            {"broker_order_id": "B-1", "correlation_id": "MCX-entry",
             "instrument": "SILVERM", "side": "BUY", "filled_quantity": 1},
            {"broker_order_id": "B-MANUAL", "correlation_id": "TV_OrderWindow",
             "instrument": "SILVERM", "side": "SELL", "filled_quantity": 1},
        ],
        broker_trades=[
            {"orderId": "B-1", "tradedQuantity": 1,
             "tradedPrice": 229144.0, "tradingSymbol": "SILVERM"},
            {"orderId": "B-MANUAL", "tradedQuantity": 1,
             "tradedPrice": 228595.0, "tradingSymbol": "SILVERM"},
        ],
        local_orders=[],
        local_fills=[],
    )

    assert report["status"] == "MISMATCH"
    assert report["mismatch_count"] == 1
    assert report["mismatches"][0]["broker_order_id"] == "B-1"
    assert report["mismatches"][0]["type"] == "BROKER_FILL_MISSING_LOCAL"
    assert report["mismatches"][0]["broker_average_price"] == 229144.0
    assert report["manual_or_unclassified_execution_orders"] == 1


def test_tradebook_comparison_uses_import_order_id_and_aggregates_partial_fills():
    report = compare_tradebook(
        broker_orders=[{"broker_order_id": "B-2", "correlation_id": "MCX-exit",
                        "filled_quantity": 2, "side": "SELL"}],
        broker_trades=[
            {"orderId": "B-2", "tradedQuantity": 1, "tradedPrice": 100},
            {"orderId": "B-2", "tradedQuantity": 1, "tradedPrice": 102},
        ],
        local_orders=[],
        local_fills=[{"order_id": "IMPORT-B-2", "quantity": 2, "price": 101}],
    )

    assert report["status"] == "MATCHED"
    assert report["mismatch_count"] == 0
    assert report["checked_system_orders"] == 1


def test_live_poller_publishes_broker_tradebook_mismatch_without_mutation():
    broker = SimpleNamespace(
        day_order_book=lambda: [{"broker_order_id": "B-3",
                                 "correlation_id": "MCX-order",
                                 "instrument": "SILVERM", "side": "BUY",
                                 "filled_quantity": 1}],
        tradebook=lambda: [{"orderId": "B-3", "tradedQuantity": 1,
                            "tradedPrice": 200}],
    )
    env = SimpleNamespace(name="LIVE", broker=broker,
                          persistence=SimpleNamespace(get_orders=lambda: [],
                                                      get_fills=lambda: []))
    poller = LiveBrokerPoller(env, config={})

    report = poller.poll_tradebook_reconciliation()

    assert report["mismatch_count"] == 1
    assert poller.stats()["tradebook_reconciliation"]["status"] == "MISMATCH"
