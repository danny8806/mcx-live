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


class DhanNotFound(RuntimeError):
    status = 404


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
    # An ambiguous lookup must never trigger a second placement POST.
    assert len(http.posts) == 1
    assert len(http.gets) == 3


def test_unresolved_error_carries_the_correlation_id():
    http = Http(post_exc=TimeoutError("t"), lookup_exc=ConnectionError("down"))
    inst = _transport(http)
    with pytest.raises(OrderPlacementUnresolved) as ei:
        _place(inst, correlation_id="CORR-XYZ")
    assert "CORR-XYZ" in str(ei.value)


def test_empty_lookup_body_retries_lookup_and_stays_unresolved():
    """An empty 200 body is ambiguous, not proof that placement never landed."""
    http_ok = Http(post_exc=TimeoutError("t"), lookup_body={})
    with pytest.raises(OrderPlacementUnresolved) as ei:
        _place(_transport(http_ok))
    assert "UNRESOLVED" in str(ei.value)
    assert len(http_ok.posts) == 1
    assert len(http_ok.gets) == 3


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


def test_lookup_retries_transient_errors_until_the_order_is_found():
    class RecoveringLookupHttp(Http):
        def __init__(self):
            super().__init__(post_exc=TimeoutError("response lost"))
            self.lookup_count = 0

        def _get(self, path):
            self.gets.append(path)
            self.lookup_count += 1
            if self.lookup_count < 3:
                raise ConnectionError("temporary lookup outage")
            return {"orderId": "B-FOUND-AFTER-RETRY",
                    "orderStatus": "PENDING"}

    http = RecoveringLookupHttp()
    result = _place(_transport(http), correlation_id="CORR-RECOVER")

    assert result["broker_order_id"] == "B-FOUND-AFTER-RETRY"
    assert result["note"] == "resolved_via_correlation_lookup"
    assert len(http.posts) == 1  # ambiguous lookups never repost
    assert len(http.gets) == 3


def test_internal_typeerror_after_post_attempt_does_not_send_second_post():
    class TypeErrorAfterAttempt(Http):
        def __init__(self):
            super().__init__(lookup_body={
                "orderId": "B-TYPEERROR", "orderStatus": "PENDING"})

        def _post(self, path, payload, retry_network=True):
            self.posts.append((path, dict(payload)))
            raise TypeError("transport decoder failed after request")

    http = TypeErrorAfterAttempt()
    result = _place(_transport(http), correlation_id="CORR-TYPEERROR")

    assert result["broker_order_id"] == "B-TYPEERROR"
    assert len(http.posts) == 1
    assert len(http.gets) == 1


@pytest.mark.parametrize(
    ("raw_status", "expected"),
    [("PENDING", "submitted"), ("TRANSIT", "submitted"),
     ("REJECTED", "rejected"), ("CANCELLED", "cancelled"),
     ("EXPIRED", "expired")],
)
def test_initial_dhan_placement_response_drives_transport_status(raw_status, expected):
    http = Http(post_resp={"orderId": "B-STATUS", "orderStatus": raw_status,
                           "omsErrorDescription": "broker final reason"})
    rec = _place(_transport(http))
    assert rec["status"] == expected
    assert rec["raw_status"] == raw_status


def test_correlation_adoption_preserves_terminal_dhan_status():
    http = Http(post_exc=TimeoutError("response lost"), lookup_body={
        "orderId": "B-REJECTED", "orderStatus": "REJECTED",
        "omsErrorDescription": "RMS rejected",
    })
    rec = _place(_transport(http))
    assert rec["broker_order_id"] == "B-REJECTED"
    assert rec["status"] == "rejected"
    assert rec["reason"] == "RMS rejected"


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
    # Empty 200 responses are ambiguous; only the broker's 404 is definitive.
    http = Http(lookup_body={})
    assert _transport(http).order_by_correlation_id("C")["status"] == "unresolved"
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
    assert len(http.posts) == 4  # initial attempt plus exactly three retries
    assert len({payload["correlationId"] for _, payload in http.posts}) == 4
    assert rec["submission_attempt_count"] == 4
    assert rec["rejection_retry_count"] == 3
    assert [attempt["status"] for attempt in rec["submission_attempts"]] == [
        "rejected", "rejected", "rejected", "rejected"]


def test_bounded_rejection_retries_stop_after_verified_acceptance():
    class RejectionThenAccepted(Http):
        def __init__(self):
            super().__init__()

        def _post(self, path, payload, retry_network=True):
            self.posts.append((path, dict(payload)))
            n = len(self.posts)
            if n < 3:
                return {"orderId": f"B-REJ-{n}", "orderStatus": "REJECTED",
                        "omsErrorDescription": f"temporary rejection {n}"}
            return {"orderId": "B-ACCEPTED", "orderStatus": "PENDING"}

    http = RejectionThenAccepted()
    result = _place(_transport(http), correlation_id="REJECT-RETRY")

    assert result["status"] == "submitted"
    assert result["broker_order_id"] == "B-ACCEPTED"
    assert result["submission_attempt_count"] == 3
    assert result["rejection_retry_count"] == 2
    assert [attempt["status"] for attempt in result["submission_attempts"]] == [
        "rejected", "rejected", "submitted"]
    assert len({payload["correlationId"] for _, payload in http.posts}) == 3


def test_correlation_http_404_is_authoritative_not_found():
    class NotFound(RuntimeError):
        status = 404

    http = Http(lookup_exc=NotFound("no such correlation"))
    rec = _transport(http).order_by_correlation_id("MISSING")
    assert rec["status"] == "not_found"


def test_poller_correlation_resolver_retries_transient_404_then_adopts_order():
    class DelayedIndexHttp(Http):
        def __init__(self):
            super().__init__()
            self.lookup_count = 0

        def _get(self, path):
            self.gets.append(path)
            self.lookup_count += 1
            if self.lookup_count == 1:
                raise DhanNotFound("correlation index lag")
            return [{"orderId": "B-POLLER-RECOVERED",
                     "orderStatus": "PENDING", "quantity": 1}]

    http = DelayedIndexHttp()
    result = _transport(http).order_by_correlation_id("CORR-POLLER")

    assert result["broker_order_id"] == "B-POLLER-RECOVERED"
    assert result["status"] == "submitted"
    assert len(http.gets) == 2


def test_poller_resolver_reports_not_found_only_after_every_lookup_is_404():
    http = Http(lookup_exc=DhanNotFound("no correlation"))

    result = _transport(http).order_by_correlation_id("CORR-ABSENT")

    assert result["status"] == "not_found"
    assert len(http.gets) == 3


def test_resolver_coerces_empty_list_to_unresolved_not_not_found():
    http = Http(lookup_body=[])
    rec = _transport(http).order_by_correlation_id("EMPTY")
    assert rec["status"] == "unresolved"
    assert "no usable order record" in rec["reason"]
    assert len(http.gets) == 3


def test_repeated_correlation_http_404_is_bounded_confirmed_absence():
    class NotFound(RuntimeError):
        status = 404

    http = Http(post_exc=TimeoutError("response lost"),
                lookup_exc=NotFound("no such correlation"))
    with pytest.raises(RuntimeError, match="confirmed no order") as exc:
        _place(_transport(http))
    assert not isinstance(exc.value, OrderPlacementUnresolved)
    # Each placement attempt requires the full bounded lookup sequence to
    # return 404 before another POST is eligible.
    assert len(http.posts) == 3
    assert len(http.gets) == 9


def test_transient_correlation_404_then_found_does_not_repost():
    class DelayedIndexHttp(Http):
        def __init__(self):
            super().__init__(post_exc=TimeoutError("response lost"))
            self.lookup_count = 0

        def _get(self, path):
            self.gets.append(path)
            self.lookup_count += 1
            if self.lookup_count == 1:
                raise DhanNotFound("index not ready")
            return {"orderId": "B-DELAYED-INDEX", "orderStatus": "PENDING"}

    http = DelayedIndexHttp()
    result = _place(_transport(http), correlation_id="CORR-DELAYED")

    assert result["broker_order_id"] == "B-DELAYED-INDEX"
    assert len(http.posts) == 1
    assert len(http.gets) == 2


def test_confirmed_absence_retries_post_and_sends_the_intent_to_dhan():
    class RetryHttp(Http):
        def __init__(self):
            super().__init__(lookup_exc=DhanNotFound("no order"))
            self._post_count = 0

        def _post(self, path, payload, retry_network=True):
            self.posts.append((path, dict(payload)))
            self._post_count += 1
            if self._post_count == 1:
                raise TimeoutError("first response lost")
            return {"orderId": "B-RETRY", "orderStatus": "PENDING"}

    http = RetryHttp()
    result = _place(_transport(http), correlation_id="CORR-RETRY")

    assert result["broker_order_id"] == "B-RETRY"
    assert result["status"] == "submitted"
    assert len(http.posts) == 2
    assert len(http.gets) == 3
    assert {payload["correlationId"] for _, payload in http.posts} == {
        "CORR-RETRY"}


def test_retries_after_each_confirmed_absence_but_stops_when_order_lands():
    class RetryHttp(Http):
        def __init__(self):
            super().__init__(lookup_exc=DhanNotFound("no order"))
            self._post_count = 0

        def _post(self, path, payload, retry_network=True):
            self.posts.append((path, dict(payload)))
            self._post_count += 1
            if self._post_count < 3:
                raise TimeoutError("response lost")
            return {"orderId": "B-THIRD", "orderStatus": "PENDING"}

    http = RetryHttp()
    result = _place(_transport(http), correlation_id="CORR-THIRD")

    assert result["broker_order_id"] == "B-THIRD"
    assert len(http.posts) == 3
    assert len(http.gets) == 6


def test_market_fallback_submission_uses_same_safe_delivery_retry():
    class RetryHttp(Http):
        def __init__(self):
            super().__init__(lookup_exc=DhanNotFound("no order"))
            self._post_count = 0

        def _post(self, path, payload, retry_network=True):
            self.posts.append((path, dict(payload)))
            self._post_count += 1
            if self._post_count == 1:
                raise TimeoutError("fallback response lost")
            return {"orderId": "B-MARKET", "orderStatus": "PENDING"}

    http = RetryHttp()
    result = _transport(http).place_market_order(
        side="SELL", quantity=1, instrument="GOLDM", order_type="MARKET",
        correlation_id="CORR-MARKET")

    assert result["broker_order_id"] == "B-MARKET"
    assert len(http.posts) == 2
    assert len(http.gets) == 3
    assert all(payload["orderType"] == "MARKET" for _, payload in http.posts)
    assert all(payload["price"] == payload["triggerPrice"] == 0.0
               for _, payload in http.posts)


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
    assert out.submission_outcome == "OUTCOME_UNKNOWN"
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
    assert out.submission_outcome == "OUTCOME_UNKNOWN"
    assert "insufficient funds" in (out.reason or "")


@pytest.mark.parametrize(
    ("broker_result", "expected_state"),
    [
        ({"broker_order_id": "B-OK", "status": "submitted",
          "raw_status": "PENDING"}, "submitted"),
        ({"broker_order_id": "B-NO", "status": "rejected",
          "raw_status": "REJECTED", "reason": "RMS rejected"}, "rejected"),
        ({"broker_order_id": "B-CANCEL", "status": "cancelled",
          "raw_status": "CANCELLED"}, "canceled"),
    ],
)
def test_engine_order_state_follows_dhan_placement_result(broker_result, expected_state):
    from execution.live.engine import LiveExecutionEngine
    from execution.models import Order, OrderState

    class RespondingBroker:
        def place_market_order(self, **_kwargs):
            return dict(broker_result)

    eng = LiveExecutionEngine.__new__(LiveExecutionEngine)
    eng._lock = __import__("threading").RLock()
    eng._orders = {}
    eng.broker = RespondingBroker()
    eng.broker_router = None
    eng.submission_guard = None
    eng._now = lambda: 1000.0
    order = Order(order_id="O-RESPONSE", strategy_id="gold_01", instrument="GOLDM",
                  side="BUY", quantity=1, order_type="LIMIT", price=100.0,
                  order_role="ENTRY", trade_id="T-RESPONSE",
                  lifecycle_id="T-RESPONSE", parent_signal_id="S-RESPONSE",
                  trigger_state="FIRED")
    out = eng.submit_order(order)
    assert out.state == OrderState(expected_state)
    assert out.submission_outcome == "BROKER_RESPONSE_RECEIVED"
    assert getattr(out, "_broker_order_id", None) == broker_result["broker_order_id"]
    if expected_state == "rejected":
        assert out.reason == "RMS rejected"


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
