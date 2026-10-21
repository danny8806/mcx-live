"""Fresh end-to-end regression for an accepted order with delayed visibility."""
from __future__ import annotations

from types import SimpleNamespace

from execution.broker_router import BrokerEventRouter
from execution.live.dhan_transport import DhanRestTransport
from execution.live.engine import LiveExecutionEngine
from execution.live.poller import LiveBrokerPoller
from execution.models import OrderState
from strategies.types import Signal, SignalType


class DelayedCorrelationHttp:
    def __init__(self):
        self.posts = []
        self.lookups = 0
        self.accepted = {
            "orderId": "DHAN-RECOVERED",
            "orderStatus": "PENDING",
            "securityId": "483080",
            "transactionType": "BUY",
            "quantity": 1,
            "orderType": "MARKET",
        }

    def _post(self, path, payload, retry_network=False):
        self.posts.append((path, dict(payload)))
        # Simulate Dhan accepting the order while the HTTP response is lost.
        raise TimeoutError("response lost after broker accepted the order")

    def _get(self, path, params=None):
        if path == "/orders/external/MCX-RECOVER-01":
            self.lookups += 1
            if self.lookups <= 6:
                raise DhanNotFound("correlation index not visible yet")
            return dict(self.accepted)
        if path == "/orders/DHAN-RECOVERED":
            return {
                **self.accepted,
                "orderStatus": "TRADED",
                "filledQty": 1,
                "averageTradedPrice": 228_501,
            }
        raise AssertionError(f"unexpected Dhan GET {path}")


class DhanNotFound(RuntimeError):
    status = 404


class Persistence:
    def __init__(self):
        self.pending = []
        self.orders = []
        self.mappings = []

    def save_pending_order(self, row):
        self.pending.append(dict(row))

    def save_order(self, row):
        self.orders.append(dict(row))

    def save_broker_order_mapping(self, row):
        self.mappings.append(dict(row))


def test_delayed_correlation_recovery_registers_fill_lineage_without_repost(monkeypatch):
    monkeypatch.setattr("execution.live.dhan_transport.time.sleep", lambda _s: None)
    http = DelayedCorrelationHttp()
    persistence = Persistence()
    broker = DhanRestTransport(
        client_id="TEST",
        http=http,
        gate_enabled=True,
        product_type="MARGIN",
        clock=lambda: 1_800_000_999.0,
        instruments={"SILVERM": {
            "security_id": "483080", "exchange_segment": "MCX_COMM",
        }},
    )
    router = BrokerEventRouter(persistence=persistence)
    engine = LiveExecutionEngine(broker, clock=lambda: 1_800_000_999.0)
    engine.broker_router = router
    signal = Signal(
        signal_type=SignalType.LONG,
        instrument="SILVERM",
        strategy_id="silver_01",
        timestamp=1_800_000_900.0,
        trigger_price=228_500,
        stop_price=0,
        quantity=1,
        side="LONG",
        metadata={
            "strategy_type": "supertrend",
            "market_on_signal_close": True,
            "no_separate_stop": True,
            "trigger_state": "FIRED",
        },
    )
    order = engine.create_order(signal, multiplier=5, trade_id="TRADE-1")
    order.correlation_id = "MCX-RECOVER-01"
    engine.submit_order(order)

    assert order.state == OrderState.SUBMITTED
    assert order.submission_outcome == "OUTCOME_UNKNOWN"
    assert getattr(order, "_broker_order_id", None) is None
    assert len(http.posts) == 1

    poller = LiveBrokerPoller.__new__(LiveBrokerPoller)
    poller.env = SimpleNamespace(
        name="live", broker=broker, persistence=persistence,
        execution_engine=engine, broker_router=router, runtimes={},
        strategies={},
    )
    poller._stats = {"pending_terminalized": 0, "order_persist_errors": 0}
    poller._errors = {}
    poller._reset_strategy_fn = None
    row = {
        "pending_order_id": signal.signal_id,
        "signal_id": signal.signal_id,
        "trade_id": "TRADE-1",
        "strategy_id": "silver_01",
        "instrument": "SILVERM",
        "correlation_id": order.correlation_id,
        "status": "entry_sent",
    }

    # Repeated 404s must leave the intent unresolved and must not free it.
    assert not poller._self_heal_by_correlation(
        row, ("rejected", "cancelled", "canceled", "expired"))
    assert order.state == OrderState.SUBMITTED
    assert getattr(order, "_broker_order_id", None) is None
    assert router.resolve("DHAN-RECOVERED") is None
    assert len(http.posts) == 1

    # Dhan's delayed lookup now returns the original order. It must be linked
    # to the live Order and router so the later status/fill can reach the trade.
    assert not poller._self_heal_by_correlation(
        row, ("rejected", "cancelled", "canceled", "expired"))
    assert len(http.posts) == 1
    assert getattr(order, "_broker_order_id", None) == "DHAN-RECOVERED"
    mapping = router.resolve("DHAN-RECOVERED")
    assert mapping is not None
    assert mapping.order_id == order.order_id
    assert mapping.strategy_id == "silver_01"
    assert mapping.trade_id == "TRADE-1"
    assert persistence.pending[-1]["broker_order_id"] == "DHAN-RECOVERED"
    assert persistence.orders[-1]["broker_order_id"] == "DHAN-RECOVERED"

    # Use the real Dhan status decoder after recovery: it reads TRADED plus
    # cumulative fill quantity, creates the fill delta, and routes by mapping.
    statuses = broker.order_statuses()
    fills = engine.apply_broker_statuses(statuses)
    assert len(fills) == 1, (dict(statuses), router.resolve("DHAN-RECOVERED"),
                             order.state, order.order_type)
    assert fills[0].broker_order_id == "DHAN-RECOVERED"
    assert fills[0].broker_fill_id == "DHAN-RECOVERED:fill:1"
    assert fills[0].price == 228_501
    assert order.state == OrderState.FILLED
    assert engine.apply_broker_statuses(broker.order_statuses()) == []
    assert len(engine.get_fills(strategy_id="silver_01")) == 1
    assert len(http.posts) == 1
