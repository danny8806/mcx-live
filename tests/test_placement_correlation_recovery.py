"""Regression tests: an unresolved placement must NEVER become a rejection.

The defect: when the ``POST /orders`` response was lost, the transport resolved
the order by correlation id. If THAT lookup also failed, the code fell through
to the same "correlation lookup found no order" error used for a successful
lookup that genuinely found nothing. The engine caught it generically and marked
the order REJECTED — leaving the local book flat while the exchange could hold a
live order. Worse, a REJECTED entry leaves the durable pending row ARMED, so the
entry "may be retried", risking a DUPLICATE order against a real position.

Broker truth, not local optimism:
  * lookup succeeded, no order  -> definitive non-execution -> REJECTED is safe
  * lookup FAILED               -> UNRESOLVED -> must stay pending & reconcilable
"""
from __future__ import annotations

import pytest

from execution.live.broker_client import OrderPlacementUnresolved
from execution.live.dhan_transport import DhanRestTransport


class Http:
    """Minimal transport HTTP double."""

    def __init__(self, post_exc=None, post_resp=None, lookup_exc=None,
                 lookup_body=None):
        self._post_exc = post_exc
        self._post_resp = post_resp or {"orderId": "B1", "orderStatus": "NEW"}
        self._lookup_exc = lookup_exc
        self._lookup_body = lookup_body
        self.posts = []
        self.gets = []

    def _post(self, path, payload, retry_network=True):
        self.posts.append((path, dict(payload)))
        if self._post_exc is not None:
            raise self._post_exc
        return self._post_resp

    def _get(self, path):
        self.gets.append(path)
        if "external" in path:
            if self._lookup_exc is not None:
                raise self._lookup_exc
            return self._lookup_body or {}
        return {}

    def _delete(self, path):
        return {}

    def _put(self, path, payload):
        return {}


def _transport(http, **kw):
    inst = DhanRestTransport(
        client_id="C1",
        http=http,
        instruments={"GOLDM": {"security_id": "1",
                               "exchange_segment": "MCX_COMM",
                               "symbol": "GOLDM"}},
        instrument_strategies={"GOLDM": ["gold_01"]},
        gate_enabled=True,
        clock=lambda: 1000.0,
        product_type="MARGIN",
    )
    inst._audit = lambda *a, **k: None
    for k, v in kw.items():
        setattr(inst, k, v)
    return inst


def _place(inst, correlation_id="CORR1"):
    return inst.place_market_order(
        side="BUY", quantity=10, instrument="GOLDM", order_type="LIMIT",
        price=100.0, correlation_id=correlation_id)


# ── 1. a FAILED lookup is UNRESOLVED, not a rejection ──────────────────────

def test_failed_lookup_raises_unresolved_not_rejection():
    http = Http(post_exc=TimeoutError("read timeout"),
                lookup_exc=ConnectionError("lookup down"))
    inst = _transport(http)
    with pytest.raises(OrderPlacementUnresolved):
        _place(inst)


def test_unresolved_error_carries_the_correlation_id():
    http = Http(post_exc=TimeoutError("t"), lookup_exc=ConnectionError("down"))
    inst = _transport(http)
    with pytest.raises(OrderPlacementUnresolved) as ei:
        _place(inst, correlation_id="CORR-XYZ")
    assert "CORR-XYZ" in str(ei.value)


def test_unresolved_is_not_the_definitive_not_found_error():
    """The two outcomes must not be the same exception type."""
    http_ok = Http(post_exc=TimeoutError("t"), lookup_body={})
    with pytest.raises(RuntimeError) as ei:
        _place(_transport(http_ok))
    assert not isinstance(ei.value, OrderPlacementUnresolved)
    assert "found no order" in str(ei.value)


# ── 2. a successful lookup that FOUND the order adopts it ──────────────────

def test_successful_lookup_adopts_the_broker_order():
    http = Http(post_exc=TimeoutError("t"),
                lookup_body={"orderId": "B77", "orderStatus": "NEW"})
    inst = _transport(http)
    res = _place(inst)
    assert res["broker_order_id"] == "B77"
    assert res["note"] == "resolved_via_correlation_lookup"
    assert "B77" in inst._orders


def test_successful_lookup_adopts_a_filled_order():
    """A placement whose response was lost but which filled must NOT be rejected."""
    http = Http(post_exc=TimeoutError("t"),
                lookup_body={"orderId": "B78", "orderStatus": "TRADED"})
    inst = _transport(http)
    res = _place(inst)
    assert res["broker_order_id"] == "B78"
    assert res["raw_status"] == "TRADED"


def test_exactly_one_adoption_for_a_duplicate_lookup():
    http = Http(post_exc=TimeoutError("t"),
                lookup_body={"orderId": "B79", "orderStatus": "NEW"})
    inst = _transport(http)
    a = _place(inst)
    b = _place(inst)
    assert a["broker_order_id"] == b["broker_order_id"] == "B79"
    assert len([k for k in inst._orders if k == "B79"]) == 1


# ── 3. the public correlation resolver used by the poller ──────────────────

def test_resolver_reports_a_broker_order():
    http = Http(lookup_body={"orderId": "B80", "orderStatus": "NEW"})
    rec = _transport(http).order_by_correlation_id("CORR2")
    assert rec["broker_order_id"] == "B80"


def test_resolver_distinguishes_not_found_from_unresolved():
    http = Http(lookup_body={})
    assert _transport(http).order_by_correlation_id("C")["status"] == "not_found"
    http2 = Http(lookup_exc=ConnectionError("down"))
    assert _transport(http2).order_by_correlation_id("C")["status"] == "unresolved"


def test_explicit_dhan_order_error_is_rejected_without_correlation_lookup():
    from data.dhan.rest_client import DhanHTTPError

    err = DhanHTTPError(400, "Order rejected", {
        "errorType": "Order_Error", "errorCode": "DH-906",
        "errorMessage": "transactions blocked by RMS",
    })
    http = Http(post_exc=err, lookup_exc=ConnectionError("should not be called"))
    rec = _place(_transport(http))
    assert rec["status"] == "rejected"
    assert rec["raw_status"] == "REJECTED"
    assert "Order rejected" in rec["reason"]
    assert http.gets == []


def test_correlation_http_404_is_authoritative_not_found():
    class NotFound(RuntimeError):
        status = 404

    http = Http(lookup_exc=NotFound("no such correlation"))
    rec = _transport(http).order_by_correlation_id("MISSING")
    assert rec["status"] == "not_found"


def test_placement_timeout_plus_correlation_http_404_is_definitive_absence():
    class NotFound(RuntimeError):
        status = 404

    http = Http(post_exc=TimeoutError("response lost"),
                lookup_exc=NotFound("no such correlation"))
    with pytest.raises(RuntimeError, match="found no order") as exc:
        _place(_transport(http))
    assert not isinstance(exc.value, OrderPlacementUnresolved)


def test_resolver_without_a_correlation_id_is_unresolved_not_not_found():
    """A missing handle is not proof the order is absent."""
    rec = _transport(Http(lookup_body={})).order_by_correlation_id("")
    assert rec["status"] == "unresolved"


# ── 4. the ENGINE must park an unresolved order, not reject it ─────────────

def test_engine_parks_unresolved_placement_as_submitted():
    from core.lifecycle import OrderRole
    from execution.live.engine import LiveExecutionEngine
    from execution.models import Order, OrderState

    class RaisingBroker:
        def place_market_order(self, **kw):
            raise OrderPlacementUnresolved("broker outcome UNRESOLVED")

    eng = LiveExecutionEngine.__new__(LiveExecutionEngine)
    eng._lock = __import__("threading").RLock()
    eng._orders = {}
    eng.broker = RaisingBroker()
    eng.broker_router = None
    eng.submission_guard = None
    eng._now = lambda: 1000.0

    order = Order(order_id="O1", strategy_id="gold_01", instrument="GOLDM",
                  side="BUY", quantity=10, order_type="LIMIT", price=100.0,
                  order_role="ENTRY", trade_id="T1",
                  lifecycle_id="L1", parent_signal_id="S1",
                  trigger_state="FIRED")
    order.correlation_id = "CORR1"
    out = eng.submit_order(order)

    assert out.state == OrderState.SUBMITTED, \
        "unresolved placement must stay pending, never be rejected"
    assert "UNRESOLVED" in (out.reason or "")


def test_engine_still_rejects_a_definitive_broker_rejection():
    """The fix must not weaken genuine broker rejections."""
    from core.lifecycle import OrderRole
    from execution.live.engine import LiveExecutionEngine
    from execution.models import Order, OrderState

    class RejectingBroker:
        def place_market_order(self, **kw):
            raise RuntimeError("RMS: insufficient funds")

    eng = LiveExecutionEngine.__new__(LiveExecutionEngine)
    eng._lock = __import__("threading").RLock()
    eng._orders = {}
    eng.broker = RejectingBroker()
    eng.broker_router = None
    eng.submission_guard = None
    eng._now = lambda: 1000.0

    order = Order(order_id="O2", strategy_id="gold_01", instrument="GOLDM",
                  side="BUY", quantity=10, order_type="LIMIT", price=100.0,
                  order_role="ENTRY", trade_id="T1",
                  lifecycle_id="L1", parent_signal_id="S2",
                  trigger_state="FIRED")
    order.correlation_id = "CORR2"
    out = eng.submit_order(order)
    assert out.state == OrderState.REJECTED
    assert "insufficient funds" in (out.reason or "")


# ── 5. the durable row stays ENTRY_SENT so it remains reconcilable ─────────

def test_unresolved_order_keeps_the_pending_row_entry_sent():
    """parked SUBMITTED must be persisted as ENTRY_SENT, not left ARMED.

    An ARMED row is "may be retried", which is exactly how a duplicate order
    gets placed against a position the broker already holds.
    """
    from application.persistence_flow import PersistenceFlowMixin
    from application.live_position_flow import LivePositionFlowMixin
    from execution.models import OrderState

    class Row(dict):
        status = "armed"
        pending_order_id = "P1"

    Row_ = Row()
    Row_.update({"pending_order_id": "P1", "status": "armed"})

    class Persist:
        def __init__(self):
            self.saved = []

        def save_pending_order(self, row):
            self.saved.append(dict(row))

    class Env:
        name = "live"
        mode = "LIVE"
        persistence = Persist()

    class H(PersistenceFlowMixin, LivePositionFlowMixin):
        def __init__(self):
            self._envs = {"live": Env()}
            self._live_rows = {"P1": Row_}

        def _live_pending_row(self, env, signal):
            return self._live_rows.get("P1")

        def publish_event(self, *a, **k):
            pass

    h = H()

    class Sig:
        signal_id = "P1"
        strategy_id = "gold_01"
        instrument = "GOLDM"
        side = "BUY"
        trigger_price = 100.0
        timestamp = 1000.0
        metadata = {"trigger_state": "FIRED", "trigger_generation": 1,
                    "trigger_source": "market_websocket_ltp"}

    class Trade:
        trade_id = "T1"

    class Ord:
        state = OrderState.SUBMITTED
        order_id = "O1"
        correlation_id = "CORR1"
        _broker_order_id = None

    h._mark_live_pending_entry_sent(Env(), Sig(), Trade(), Ord())
    saved = Env.persistence.saved
    assert saved, "pending row was not persisted"
    assert saved[-1]["status"] == "entry_sent", \
        "unresolved placement must stay ENTRY_SENT so it is reconcilable"
    assert saved[-1]["correlation_id"] == "CORR1"
