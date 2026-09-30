"""Stress characterization of in-memory order/fill history retention.

No network or credentials are used. This intentionally records current cache
sizes after a completed-order burst so unbounded history retention stays visible
until an acknowledgement-safe compaction policy is implemented.
"""
from types import SimpleNamespace

from execution.live.dhan_transport import DhanRestTransport
from execution.live.order_watcher import OrderWatcher
from execution.models import OrderState


class _CompletedOrderHttp:
    def __init__(self):
        self.next_id = 0

    def _post(self, path, payload, retry_network=False):
        assert path == "/orders"
        self.next_id += 1
        return {"orderId": f"B-{self.next_id}", "orderStatus": "TRANSIT"}

    def _get(self, path):
        order_id = path.rsplit("/", 1)[-1]
        return [{
            "orderId": order_id,
            "orderStatus": "TRADED",
            "filledQty": 1,
            "averageTradedPrice": 100.0,
        }]


def test_completed_order_burst_characterizes_live_cache_retention():
    count = 1200
    broker = DhanRestTransport(
        client_id="TEST", http=_CompletedOrderHttp(), gate_enabled=True,
        instruments={"GOLDM": {"security_id": "123", "exchange_segment": "MCX_COMM"}},
    )
    watcher = OrderWatcher()

    internal_ids = []
    for i in range(count):
        placed = broker.place_market_order(
            side="BUY", quantity=1, instrument="GOLDM", order_type="LIMIT",
            price=100.0, correlation_id=f"MCX-STRESS-{i}")
        assert placed["status"] == "submitted"
        internal_id = f"internal-{i}"
        internal_ids.append(internal_id)
        watcher.register_from_order(SimpleNamespace(
            order_id=internal_id,
            state=OrderState.SUBMITTED,
            strategy_id="s1",
            trade_id=f"trade-{i}",
            lifecycle_id=f"trade-{i}",
            instrument="GOLDM",
            side="BUY",
            order_type="LIMIT",
            quantity=1,
            created_at=float(i),
            order_role="ENTRY",
        ))

    statuses = broker.order_statuses()
    assert len(statuses) == count
    assert sum(len(row["fills"]) for row in statuses.values()) == count

    # Characterization assertions: these three historical stores currently
    # retain the whole burst. Replace with fixed-cap assertions when safe
    # compaction is implemented without losing fill dedupe or net exposure.
    assert len(broker._orders) == count
    assert len(broker._fills) == count
    assert len(broker._fill_seq) == count
    assert len(watcher._records) == count
    assert len(internal_ids) == count

