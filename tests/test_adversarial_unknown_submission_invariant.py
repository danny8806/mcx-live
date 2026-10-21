"""Adversarial submission tests defined by safety invariants, not snapshots.

The injected transport outcomes model a connection failing after a POST may
have crossed the network boundary. Such an outcome must remain unresolved and
deduplicated until broker reconciliation proves whether an order exists.
"""
from __future__ import annotations

import pytest
from threading import Barrier, Event, Thread

from execution.broker_router import BrokerEventRouter
from execution.live.engine import LiveExecutionEngine
from execution.live.broker_client import BrokerGateClosed, OrderPlacementUnresolved
from execution.live.dhan_transport import DhanRestTransport
from execution.models import OrderState
from execution.order_manager import OrderManager
from strategies.types import Signal, SignalType


class _WriteThenTimeout:
    mode = "LIVE"

    def __init__(self, outcome):
        self.outcome = outcome
        self.post_attempts = 0

    def place_market_order(self, **_kwargs):
        self.post_attempts += 1
        if self.outcome == "exception":
            raise TimeoutError("connection lost after request write")
        return None  # malformed/empty response: broker acceptance is unknown

    def update_price(self, *_args):
        pass


def _signal():
    return Signal(
        signal_type=SignalType.LONG,
        instrument="SILVERM",
        strategy_id="silver_01",
        timestamp=1_800.0,
        trigger_price=100.0,
        stop_price=0.0,
        quantity=1,
        side="LONG",
        metadata={
            "strategy_type": "supertrend",
            "market_on_signal_close": True,
            "no_separate_stop": True,
            "trigger_state": "FIRED",
            "trigger_source": "completed_signal_candle_close",
        },
    )


@pytest.mark.parametrize("outcome", ["exception", "empty_response"])
def test_ambiguous_submission_cannot_become_terminal_or_release_dedup(outcome):
    """An unknown POST result may not authorize a second economic order."""
    broker = _WriteThenTimeout(outcome)
    execution = LiveExecutionEngine(broker, clock=lambda: 10_000.0)
    orders = OrderManager(execution)
    signal = _signal()

    first = orders.submit_signal(signal, multiplier=5, trade_id="unknown-trade")
    assert first is not None
    assert first.submission_outcome == "OUTCOME_UNKNOWN"
    assert first.state == OrderState.SUBMITTED
    assert first in orders.get_active_orders("silver_01")

    # Until an authoritative broker lookup resolves the correlation id, a
    # repeated signal must be deduplicated instead of issuing another POST.
    second = orders.submit_signal(signal, multiplier=5, trade_id="unknown-trade")
    assert second is None
    assert broker.post_attempts == 1


class _LostResponseHttp:
    """Models POST accepted or unknown, followed by correlation lookup."""

    def __init__(self, lookup):
        self.lookup = lookup
        self.posts = []
        self.lookups = []

    def _post(self, path, payload, retry_network=True):
        self.posts.append((path, payload, retry_network))
        raise TimeoutError("lost response after request write")

    def _get(self, path):
        self.lookups.append(path)
        if isinstance(self.lookup, Exception):
            raise self.lookup
        return self.lookup


def _dhan(http):
    return DhanRestTransport(
        client_id="TEST-CLIENT",
        http=http,
        instruments={"SILVERM": {
            "security_id": "483080",
            "exchange_segment": "MCX_COMM",
        }},
        gate_enabled=True,
    )


def test_lost_post_response_adopts_correlated_order_without_reposting():
    http = _LostResponseHttp({"orderId": "DHAN-FOUND", "orderStatus": "PENDING"})
    result = _dhan(http).place_market_order(
        "BUY", 1, "SILVERM", correlation_id="MCX-LOST-RESPONSE")
    assert result["broker_order_id"] == "DHAN-FOUND"
    assert result["note"] == "resolved_via_correlation_lookup"
    assert len(http.posts) == 1
    assert http.posts[0][2] is False
    assert http.lookups == ["/orders/external/MCX-LOST-RESPONSE"]


def test_lost_post_and_failed_lookups_are_unresolved_without_retry(monkeypatch):
    import execution.live.dhan_transport as transport_module

    monkeypatch.setattr(transport_module.time, "sleep", lambda _seconds: None)
    http = _LostResponseHttp(ConnectionError("lookup feed unavailable"))
    with pytest.raises(OrderPlacementUnresolved):
        _dhan(http).place_market_order(
            "SELL", 1, "SILVERM", correlation_id="MCX-AMBIGUOUS")
    assert len(http.posts) == 1
    assert len(http.lookups) == 3


def test_explicit_pre_send_gate_does_not_call_http():
    http = _LostResponseHttp({})
    broker = DhanRestTransport(
        client_id="TEST-CLIENT",
        http=http,
        instruments={"SILVERM": {"security_id": "483080"}},
        gate_enabled=False,
    )
    with pytest.raises(BrokerGateClosed, match="LIVE_GATE_CLOSED"):
        broker.place_market_order("BUY", 1, "SILVERM")
    assert http.posts == []
    assert http.lookups == []


def test_two_workers_cannot_submit_the_same_logical_order_leg():
    broker_entered = Event()
    release_broker = Event()

    class SlowAckBroker(_WriteThenTimeout):
        def place_market_order(self, **_kwargs):
            self.post_attempts += 1
            broker_entered.set()
            assert release_broker.wait(timeout=2)
            return {
                "broker_order_id": "ONE-ECONOMIC-ORDER",
                "status": "submitted",
                "correlation_id": "MCX-RACE",
            }

    broker = SlowAckBroker("exception")
    execution = LiveExecutionEngine(broker, clock=lambda: 10_000.0)
    signal = _signal()
    first = execution.create_order(signal, multiplier=5, trade_id="same-trade")
    second = execution.create_order(signal, multiplier=5, trade_id="same-trade")
    errors = []

    def submit(order):
        try:
            execution.submit_order(order)
        except Exception as exc:
            errors.append(exc)

    worker_a = Thread(target=submit, args=(first,))
    worker_a.start()
    assert broker_entered.wait(timeout=1)
    worker_b = Thread(target=submit, args=(second,))
    worker_b.start()
    worker_b.join(timeout=1)
    release_broker.set()
    worker_a.join(timeout=2)
    worker_b.join(timeout=2)

    assert not worker_a.is_alive() and not worker_b.is_alive()
    assert errors == []
    assert broker.post_attempts == 1
    assert first.state == OrderState.SUBMITTED
    assert second.state == OrderState.REJECTED
    assert second.reason.startswith("DUPLICATE_EXECUTION_KEY:")


def test_concurrent_repoll_of_same_broker_fill_creates_one_local_fill():
    broker = _WriteThenTimeout("exception")

    # Override placement to produce a normal async broker acknowledgement.
    def acknowledge(**kwargs):
        broker.post_attempts += 1
        return {"broker_order_id": "BROKER-ONE", "status": "submitted",
                "correlation_id": kwargs["correlation_id"]}

    broker.place_market_order = acknowledge
    execution = LiveExecutionEngine(broker, clock=lambda: 10_000.0)
    execution.broker_router = BrokerEventRouter()
    signal = _signal()
    order = execution.submit_order(
        execution.create_order(signal, multiplier=5, trade_id="fill-race-trade"))
    assert order.state == OrderState.SUBMITTED
    status = {"BROKER-ONE": {
        "status": "filled",
        "fills": [{"broker_fill_id": "UNIQUE-FILL-1", "quantity": 1,
                   "price": 101.0, "timestamp": 10_001, "side": "BUY"}],
    }}
    workers = 12
    rendezvous = Barrier(workers)
    returned_batches = []
    failures = []

    def poll_same_snapshot():
        try:
            rendezvous.wait(timeout=2)
            returned_batches.append(execution.apply_broker_statuses(status))
        except Exception as exc:
            failures.append(exc)

    threads = [Thread(target=poll_same_snapshot) for _ in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)
    assert all(not thread.is_alive() for thread in threads)
    assert failures == []
    assert sum(len(batch) for batch in returned_batches) == 1
    assert len(execution.get_fills()) == 1
    assert order.state == OrderState.FILLED
