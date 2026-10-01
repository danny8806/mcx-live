"""Phase 2 — Dhan v2 REST live broker transport.

:class:`DhanRestTransport` implements the :class:`LiveBrokerClient` contract
(``place_market_order`` / ``order_statuses`` / ``fills`` / ``positions`` /
``account_status``) with Dhan's v2 REST API as the LIVE account authority::

    place order        POST  /orders          (MARKET / INTRADAY / DAY / MCX_COMM)
    order status       GET   /orders/{id}     (PENDING/TRANSIT/TRADED/PART_TRADED/...)
    positions          GET   /positions       (netQty per securityId)
    funds              GET   /fundlimit       (availabelBalance / utilizedAmount)

Broker-native ids are preserved and stable (``broker_order_id``,
``broker_fill_id``); a broker event is never double-applied.  The transport
tracks cumulative traded quantity per order and emits exactly one fill per
previously-unaccounted quantity delta, so repeated status polls are
idempotent.  The engine's ``apply_broker_statuses`` additionally dedups by
``broker_fill_id``.

Fills are ALWAYS derived from the order-status poll (with the exchange-reported
``averageTradedPrice``) — never from the placement response — so every live
fill carries exchange truth, matching the async-poll architecture of
:class:`execution.live.poller.LiveBrokerPoller`.

HTTP dependency: the transport only needs an object with ``_post(path,
payload)`` and ``_get(path, params=None)`` — the repo's
:class:`data.dhan.rest_client.DhanRESTClient` provides both (token renewal,
rate limiting, retries) and tests inject a fake.
"""
from __future__ import annotations

import math
import threading
import time
import uuid
from typing import Optional

from execution.live.broker_client import (
    BrokerGateClosed,
    LiveBrokerClient,
    OrderPlacementUnresolved,
    PreTradeGateBlocked,
)

# Dhan v2 order-status values that mean SOME quantity traded.
_FILLED_RAW = {"TRADED", "PART_TRADED"}

_AUDIT_ACTION = {
    "MARKET": "PLACE_MARKET",
    "LIMIT": "PLACE_LIMIT",
    "STOP_LOSS": "PLACE_STOP_LIMIT",
    "STOP_LOSS_MARKET": "PLACE_SL",
}


def _coerce_status_body(body: object) -> dict:
    """Dhan answers ``GET /orders/{id}`` with a JSON ARRAY containing the one
    order object (live-verified 2026-09-11); a dict body is tolerated too."""
    if isinstance(body, list):
        body = body[0] if body else {}
    return body if isinstance(body, dict) else {}


def _normalize_status(raw_status: str) -> str:
    """Map a Dhan orderStatus to the transport-neutral status vocabulary
    (submitted / filled / rejected / cancelled / expired)."""
    upper = str(raw_status or "").strip().upper()
    if upper in ("TRADED", "PART_TRADED"):
        return "filled"
    if upper in ("REJECTED",):
        return "rejected"
    if upper in ("CANCELLED", "CANCELED"):
        return "cancelled"
    if upper in ("EXPIRED",):
        return "expired"
    if upper in ("PENDING", "TRANSIT", "NEW", "OPEN", "PART_TRADED"):
        return "submitted"
    return "submitted"


def _placement_state(status: object) -> str:
    """Map placement/correlation truth to an immediate engine state.

    A fill is still reconciled from the order-status poll; only broker-side
    terminal failures may bypass SUBMITTED at placement time.
    """
    normalized = str(status or "").strip().lower()
    if normalized in ("rejected", "cancelled", "canceled", "expired"):
        return "cancelled" if normalized == "canceled" else normalized
    return "submitted"


def _is_raw_terminal(raw_status: object) -> bool:
    """True only when the RAW Dhan status is truly settled.

    PART_TRADED normalizes to 'filled' but still has a live working remainder,
    so it must NEVER short-circuit a cancel (C9): a DELETE is required to stop
    the remainder from continuing to fill.
    """
    return str(raw_status or "").strip().upper() in (
        "TRADED", "CANCELLED", "CANCELED", "REJECTED", "EXPIRED",
    )


# Monotonicity ranking for status ingest: a stale/late order-alert frame must
# never regress the transport book (TRADED -> PENDING).  Filled is the highest
# stage; the abnormal-terminal stages sit above plain submitted.
_STATUS_RANK = {"submitted": 0, "rejected": 1, "cancelled": 1, "expired": 1, "filled": 2}

# A lost POST response is retried only after Dhan's correlation endpoint has
# authoritatively confirmed that no order exists. Ambiguous lookups never
# reach this retry path. The bound prevents a broker outage from holding the
# strategy tick indefinitely or generating an unbounded stream of requests.
_MAX_CONFIRMED_ABSENCE_RETRIES = 2
_MAX_CORRELATION_LOOKUP_RETRIES = 2
_MAX_DEFINITIVE_REJECTION_RETRIES = 3


class DhanRestTransport(LiveBrokerClient):
    """Live account authority backed by Dhan v2 REST (orders/positions/funds)."""

    mode = "LIVE"

    def __init__(
        self,
        client_id: str = "",
        http: Optional[object] = None,
        instruments: Optional[dict] = None,
        instrument_strategies: Optional[dict] = None,
        gate_enabled: bool = False,
        clock: Optional[callable] = None,
        dhan_config: Optional[dict] = None,
        product_type: str = "INTRADAY",
        audit_store: Optional[object] = None,
    ):
        """Build the transport.

        ``http`` must implement ``_post(path, payload)`` / ``_get(path)``; when
        omitted the transport builds a :class:`DhanRESTClient` from
        ``dhan_config`` (rest_base / token_file / client_id / pin / totp_secret).
        """
        self.client_id = client_id
        self.gate_enabled = bool(gate_enabled)
        self.product_type = product_type
        self.instruments: dict = {
            inst: {
                "security_id": str(cfg.get("security_id", "")),
                "exchange_segment": cfg.get("exchange_segment", "MCX_COMM"),
                "symbol": cfg.get("symbol", ""),
            }
            for inst, cfg in (instruments or {}).items()
        }
        self.instrument_strategies: dict[str, list[str]] = {
            inst: list(sids)
            for inst, sids in (instrument_strategies or {}).items()
        }
        self._clock = clock or time.time
        self._owns_http = http is None
        if http is None:
            http = self._build_default_http(dhan_config or {})
        self._http = http
        # Appendix §L — optional COMPLETE broker response audit store.  When
        # wired, every material Dhan call is recorded (sanitized).  Never
        # required: a stock transport (fakes, tests) records nothing.
        self.audit = audit_store

        self._lock = threading.Lock()
        self._orders: dict[str, dict] = {}   # broker_order_id -> book record
        self._fills: dict[str, dict] = {}    # broker_fill_id -> fill record
        self._fill_seq: dict[str, int] = {}  # broker_order_id -> fill counter
        # §9.11 — pre-trade circuit-limit gate (POST /marketfeed/quote).  OFF by
        # default so a transport built without a dhan_config block is a pure
        # wire; the LIVE config opts in.
        cg = (dhan_config or {}).get("circuit_gate") or {}
        self._circuit_gate_enabled = bool(cg.get("enabled", False))
        self._circuit_gate_fail_closed = bool(cg.get("fail_closed", False))

    # ── construction helpers ───────────────────────────────────────────

    @classmethod
    def from_config(cls, dhan_config: Optional[dict], instruments: Optional[dict],
                    instrument_strategies: Optional[dict],
                    gate_enabled: bool, http: Optional[object] = None,
                    clock: Optional[callable] = None,
                    audit_store: Optional[object] = None) -> "DhanRestTransport":
        """Build a transport from the engine's ``dhan`` config block."""
        return cls(
            client_id=(dhan_config or {}).get("client_id", ""),
            http=http,
            instruments=instruments or {},
            instrument_strategies=instrument_strategies or {},
            gate_enabled=gate_enabled,
            clock=clock,
            dhan_config=dhan_config or {},
            product_type=(dhan_config or {}).get("product_type", "INTRADAY"),
            audit_store=audit_store,
        )

    # ── appendix §L broker audit hook ────────────────────────────────────

    def _audit(self, action: str, endpoint: str, method: str,
               payload: Optional[dict] = None, response: object = None,
               http_status: Optional[int] = None,
               error: Optional[BaseException] = None,
               correlation_id: Optional[str] = None, **extra) -> None:
        """Record one wire call into the audit store when configured.

        The audit is best-effort: any failure is swallowed so broker execution
        never depends on audit persistence.
        """
        if self.audit is None:
            return
        try:
            self.audit.record(
                action=action, endpoint=endpoint, http_method=method,
                request_payload=payload, response=response,
                http_status=http_status, error=error,
                correlation_id=correlation_id, **extra,
            )
        except Exception:
            pass

    def _build_default_http(self, dhan_config: dict):
        from data.dhan.rest_client import DhanRESTClient
        return DhanRESTClient(
            base_url=dhan_config.get("rest_base", "https://api.dhan.co/v2"),
            token_file=dhan_config.get("token_file", "data/db/dhan_token.json"),
            client_id=self.client_id,
            pin=dhan_config.get("pin", ""),
            totp_secret=dhan_config.get("totp_secret", ""),
        )

    # ── LiveBrokerClient ───────────────────────────────────────────────

    def place_market_order(self, side: str, quantity: int, instrument: str,
                           order_type: str = "MARKET", price: Optional[float] = None,
                           trigger_price: Optional[float] = None,
                           correlation_id: Optional[str] = None,
                           _confirmed_absence_retries: int = 0,
                           _rejection_retries: int = 0,
                           _attempt_history: Optional[list] = None) -> dict:
        if _attempt_history is None:
            _attempt_history = []
        side_u = str(side).upper()
        # INVARIANT 10 — the last gate before the wire, checked FIRST.  This
        # system has NO broker-side protective stop: a stop-loss is the local
        # position-owned monitor minting an ordinary LIMIT/MARKET exit.  A
        # resting stop-limit / SLM must never reach Dhan from any caller.
        order_type_u = (order_type or "MARKET").upper()
        if order_type_u in ("STOP_LOSS", "STOP_LOSS_MARKET"):
            raise ValueError(
                "BROKER_SL_RETIRED: broker-side protective stop orders are "
                "disabled; the stop is the local position-owned SL monitor")
        if not self.gate_enabled:
            raise BrokerGateClosed("LIVE_GATE_CLOSED: live trading master gate is OFF")
        if side_u not in ("BUY", "SELL"):
            raise ValueError(f"invalid transaction side: {side}")
        quantity = int(quantity)
        if quantity <= 0:
            raise ValueError("quantity must be positive")
        # §9.4 — NaN/Inf values must never reach the Dhan payload (a NaN JSON
        # breaks the order request); also we never leak a nonzero price onto a
        # MARKET order or a nonzero trigger onto a non-SLM one.
        if price is not None and not math.isfinite(float(price)):
            raise ValueError("price must be finite (got NaN/Inf)")
        if trigger_price is not None and not math.isfinite(float(trigger_price)):
            raise ValueError("trigger_price must be finite (got NaN/Inf)")
        sec = self.instruments.get(instrument)
        if sec is None or not sec.get("security_id"):
            raise RuntimeError(
                f"no Dhan security mapping for instrument {instrument}; "
                "configure instruments.<name>.security_id")

        if order_type_u not in ("MARKET", "LIMIT"):
            raise ValueError(f"unsupported Dhan orderType: {order_type_u}")
        if order_type_u == "LIMIT" and not (price is not None and float(price) > 0):
            raise ValueError("LIMIT orders require a positive price")
        # §9.11 — pre-trade circuit-limit validation: only priced order types
        # are checked against the exchange band.  A MARKET order has no price
        # to validate.
        if self._circuit_gate_enabled and order_type_u == "LIMIT":
            self._enforce_circuit_gate(instrument, float(price or 0.0),
                                       order_type_u)
        # §9.4 — exact payload: exactly ONE of price/trigger is nonzero per
        # order type; MARKET carries both zero (never leaks caller junk).
        if order_type_u == "MARKET":
            limit_price, trigger = 0.0, 0.0
        else:  # LIMIT
            limit_price, trigger = float(price), 0.0
        # §9.4 — the engine-owned correlation id; minted here only when the
        # caller didn't provide one (keeps signal->order->broker lineage).
        correlation_id = correlation_id or f"MCX-{uuid.uuid4().hex[:12]}"
        payload = {
            "dhanClientId": self.client_id,
            "transactionType": side_u,
            "exchangeSegment": sec["exchange_segment"],
            "productType": self.product_type,
            "orderType": order_type_u,
            "validity": "DAY",
            "securityId": sec["security_id"],
            "quantity": quantity,
            "disclosedQuantity": 0,
            "price": limit_price,
            "triggerPrice": trigger,
            "afterMarketOrder": False,
            "correlationId": correlation_id,
        }
        # §8/§55 — the POST is OUTSIDE the lock and fired with
        # retry_network=False: a network exception leaves the broker outcome
        # UNKNOWN, and re-POSTing could create a duplicate order.  Any failure
        # is recovered via the broker correlation lookup (single economic
        # order), never by a blind retry.
        resp = None
        placement_error = None
        try:
            resp = self._http._post("/orders", payload, retry_network=False)
        except TypeError as exc:
            # Compatibility for older injected HTTP adapters that do not
            # accept the retry_network keyword. Catch only Python's exact
            # signature-mismatch case: an internal TypeError may occur after
            # a request was sent and must never cause a second POST.
            signature_mismatch = (
                "retry_network" in str(exc)
                and "unexpected keyword argument" in str(exc))
            if signature_mismatch:
                try:
                    resp = self._http._post("/orders", payload)
                except Exception as fallback_exc:
                    placement_error = fallback_exc
            else:
                placement_error = exc
        except Exception as exc:
            placement_error = exc

        if placement_error is not None:
            exc = placement_error
            place_status = getattr(exc, "status", None)
            self._audit(_AUDIT_ACTION.get(order_type_u, "PLACE_ORDER"),
                        "/orders", "POST", payload, error=exc,
                        http_status=place_status,
                        correlation_id=correlation_id)
            # Dhan's structured 4xx Order_Error is an explicit broker
            # rejection. A correlation lookup after this response is both
            # unnecessary and harmful: Dhan may return 404 for an order that
            # it already rejected, leaving local state unresolved forever.
            if (place_status is not None and 400 <= int(place_status) < 500
                    and getattr(exc, "dhan_error_type", None) == "Order_Error"):
                reason = (getattr(exc, "text", None)
                          or getattr(exc, "body", None) or str(exc))
                _attempt_history.append({
                    "attempt": _rejection_retries + 1,
                    "correlation_id": correlation_id,
                    "http_status": int(place_status),
                    "status": "rejected",
                    "reason": str(reason),
                })
                rejected = {
                    "broker_order_id": None,
                    "status": "rejected",
                    "raw_status": "REJECTED",
                    "reason": str(reason),
                    "side": side_u,
                    "quantity": quantity,
                    "instrument": instrument,
                    "requested_order_type": order_type_u,
                    "requested_price": limit_price,
                    "requested_trigger_price": trigger,
                    "correlation_id": correlation_id,
                    "timestamp": self._clock(),
                    "filled_quantity": 0,
                    "average_fill_price": 0.0,
                    "last_accounted_qty": 0,
                }
                return self._retry_definitive_rejection(
                    rejected, side_u, quantity, instrument, order_type_u,
                    limit_price, trigger, correlation_id,
                    _confirmed_absence_retries, _rejection_retries,
                    _attempt_history)
            return self._resolve_unknown_placement(
                correlation_id, side_u, quantity, instrument,
                order_type_u, limit_price, trigger, exc,
                confirmed_absence_retries=_confirmed_absence_retries)
        self._audit(_AUDIT_ACTION.get(order_type_u, "PLACE_ORDER"),
                    "/orders", "POST", payload, response=resp, http_status=200,
                    correlation_id=correlation_id)
        if not isinstance(resp, (dict, list)):
            return self._resolve_unknown_placement(
                correlation_id, side_u, quantity, instrument,
                order_type_u, limit_price, trigger,
                RuntimeError(f"Dhan returned malformed placement body: {resp!r}"),
                confirmed_absence_retries=_confirmed_absence_retries)
        resp = _coerce_status_body(resp)
        broker_order_id = resp.get("orderId") or resp.get("order_id")
        raw_status = resp.get("orderStatus") or resp.get("order_status") or "PENDING"
        reason = (resp.get("omsErrorDescription")
                  or resp.get("errorMessage")
                  or resp.get("reason") or None)
        normalized_status = _normalize_status(raw_status)
        _attempt_history.append({
            "attempt": _rejection_retries + 1,
            "correlation_id": correlation_id,
            "http_status": 200,
            "broker_order_id": broker_order_id,
            "raw_status": str(raw_status).upper(),
            "status": normalized_status,
            "reason": str(reason) if reason else None,
        })
        if (not broker_order_id
                and normalized_status in ("rejected", "cancelled", "expired")):
            # Dhan can report a terminal error without an order id. This
            # is still explicit placement truth and must settle locally.
            terminal = {
                "broker_order_id": None, "status": _placement_state(normalized_status),
                "raw_status": str(raw_status).upper(),
                "reason": reason or f"Dhan placement status: {raw_status}",
                "side": side_u, "quantity": quantity,
                "instrument": instrument,
                "requested_order_type": order_type_u,
                "requested_price": limit_price,
                "requested_trigger_price": trigger,
                "correlation_id": correlation_id, "timestamp": self._clock(),
                "filled_quantity": 0, "average_fill_price": 0.0,
                "last_accounted_qty": 0,
            }
            if normalized_status == "rejected":
                return self._retry_definitive_rejection(
                    terminal, side_u, quantity, instrument, order_type_u,
                    limit_price, trigger, correlation_id,
                    _confirmed_absence_retries, _rejection_retries,
                    _attempt_history)
            return self._placement_attempt_metadata(
                terminal, _rejection_retries + 1, _rejection_retries,
                _attempt_history)
        if not broker_order_id:
            # HTTP success without an order id is not proof of rejection:
            # recover by correlation id instead of making the engine mark
            # a possibly live broker order rejected.
            return self._resolve_unknown_placement(
                correlation_id, side_u, quantity, instrument,
                order_type_u, limit_price, trigger,
                RuntimeError(f"Dhan placement response has no order id: {resp}"),
                confirmed_absence_retries=_confirmed_absence_retries)
        with self._lock:
            existing = self._orders.get(str(broker_order_id))
            if existing is None:
                now = self._clock()
                rec = {
                    "broker_order_id": str(broker_order_id),
                    "status": normalized_status,
                    "raw_status": str(raw_status).upper(),
                    "reason": reason,
                    "side": side_u,
                    "quantity": quantity,
                    "instrument": instrument,
                    "price": None,
                    "requested_order_type": order_type_u,
                    "requested_price": limit_price,
                    "requested_trigger_price": trigger,
                    "correlation_id": correlation_id,
                    "timestamp": now,
                    "filled_quantity": 0,
                    "average_fill_price": 0.0,
                    "last_accounted_qty": 0,
                }
                self._orders[broker_order_id] = rec
            else:
                rec = dict(existing)
        if existing is not None:
            prior = dict(rec, status=_placement_state(rec.get("status")))
            if prior["status"] == "rejected":
                return self._retry_definitive_rejection(
                    prior, side_u, quantity, instrument, order_type_u,
                    limit_price, trigger, correlation_id,
                    _confirmed_absence_retries, _rejection_retries,
                    _attempt_history)
            return self._placement_attempt_metadata(
                prior, _rejection_retries + 1, _rejection_retries,
                _attempt_history)
        # A FILLED status is never taken from the placement response - the
        # fill always flows through the status poll with the exchange price. A
        # terminal failure is immediate broker truth and must not be presented
        # as a working order.
        if rec["status"] in ("rejected", "cancelled", "expired"):
            terminal = dict(rec, status=rec["status"])
            if rec["status"] == "rejected":
                return self._retry_definitive_rejection(
                    terminal, side_u, quantity, instrument, order_type_u,
                    limit_price, trigger, correlation_id,
                    _confirmed_absence_retries, _rejection_retries,
                    _attempt_history)
            return self._placement_attempt_metadata(
                terminal, _rejection_retries + 1, _rejection_retries,
                _attempt_history)
        return self._placement_attempt_metadata(
            dict(rec, status="submitted"), _rejection_retries + 1,
            _rejection_retries, _attempt_history)

    @staticmethod
    def _placement_attempt_metadata(result: dict, attempts: int,
                                    retries: int, history: list) -> dict:
        result = dict(result)
        result["submission_attempt_count"] = int(attempts)
        result["rejection_retry_count"] = int(retries)
        result["submission_attempts"] = list(history)
        return result

    def _retry_definitive_rejection(
            self, rejected: dict, side_u: str, quantity: int,
            instrument: str, order_type_u: str, limit_price: float,
            trigger: float, correlation_id: str,
            confirmed_absence_retries: int, rejection_retries: int,
            history: list) -> dict:
        """Retry only a broker-confirmed rejection, at most three times.

        Each retry has a distinct Dhan correlation id and re-enters the full
        validation/payload path. Unknown transport outcomes are handled by the
        separate correlation-resolution logic and are never retried here.
        Every response summary is returned for persistence/diagnostics.
        """
        if rejection_retries >= _MAX_DEFINITIVE_REJECTION_RETRIES:
            return self._placement_attempt_metadata(
                rejected, rejection_retries + 1, rejection_retries, history)
        retry_number = rejection_retries + 1
        suffix = f"-R{retry_number}"
        next_correlation_id = f"{str(correlation_id)[:30-len(suffix)]}{suffix}"
        return self.place_market_order(
            side=side_u, quantity=quantity, instrument=instrument,
            order_type=order_type_u,
            price=None if order_type_u == "MARKET" else limit_price,
            trigger_price=None if order_type_u == "MARKET" else trigger,
            correlation_id=next_correlation_id,
            _confirmed_absence_retries=confirmed_absence_retries,
            _rejection_retries=retry_number,
            _attempt_history=history,
        )

    def _resolve_unknown_placement(self, correlation_id, side_u, quantity,
                                   instrument, order_type_u, limit_price,
                                   trigger, origin: BaseException,
                                   confirmed_absence_retries: int = 0) -> dict:
        """Recover a placement whose broker outcome is unknown.

        Called when the ``POST /orders`` network response was lost.  The order
        MAY have reached Dhan, so we never blindly re-POST.  Instead we resolve
        the broker by correlation id:

        * an existing broker order -> adopted into the transport book exactly
          once (``note="resolved_via_correlation_lookup"``), so downstream
          reconciliation sees a single economic order;
        * authoritative no-order response -> retry the same intent with the
          same correlation id, bounded to two retries. The same id lets a late
          first response be resolved as one economic order; ambiguous lookups
          never retry.
        """
        lookup_body = None
        lookup_error = None
        all_lookups_confirmed_absent = True
        lookup_path = f"/orders/external/{correlation_id}"
        for attempt in range(_MAX_CORRELATION_LOOKUP_RETRIES + 1):
            try:
                response = self._http._get(lookup_path)
            except Exception as exc:
                self._audit("ORDER_BY_CORRELATION", lookup_path, "GET",
                            error=exc,
                            http_status=getattr(exc, "status", None) or 400,
                            correlation_id=correlation_id)
                # Retry even a 404: correlation indexing can race the order
                # POST. A single not-found response is not enough to justify
                # another order submission. Any ambiguous response prevents
                # us from proving absence.
                if getattr(exc, "status", None) == 404:
                    lookup_error = exc
                else:
                    all_lookups_confirmed_absent = False
                    lookup_error = exc
            else:
                self._audit("ORDER_BY_CORRELATION", lookup_path, "GET",
                            response=response, http_status=200,
                            correlation_id=correlation_id)
                lookup_body = _coerce_status_body(response)
                if lookup_body.get("orderId") or lookup_body.get("order_id"):
                    break
                # An empty/malformed 200 body does not prove that the POST
                # failed to land. Retry the lookup and remain unresolved if
                # Dhan still gives no usable order record.
                all_lookups_confirmed_absent = False
                lookup_error = RuntimeError(
                    f"correlation lookup returned no order record: {response!r}")

            if attempt < _MAX_CORRELATION_LOOKUP_RETRIES:
                time.sleep(0.1 * (2 ** attempt))

        confirmed_absence = all_lookups_confirmed_absent

        oid = ((lookup_body or {}).get("orderId")
               or (lookup_body or {}).get("order_id"))
        if oid:
            oid = str(oid)
            with self._lock:
                known = self._orders.get(oid)
                if known is not None:
                    return dict(known, status=_placement_state(known.get("status")),
                                note="resolved_via_correlation_lookup")
                raw_status = ((lookup_body or {}).get("orderStatus")
                              or (lookup_body or {}).get("order_status") or "PENDING")
                rec = {
                    "broker_order_id": oid,
                    "status": _normalize_status(raw_status),
                    "raw_status": str(raw_status).upper(),
                    "reason": ((lookup_body or {}).get("omsErrorDescription")
                               or (lookup_body or {}).get("errorMessage") or None),
                    "side": side_u,
                    "quantity": quantity,
                    "instrument": instrument,
                    "price": None,
                    "requested_order_type": order_type_u,
                    "requested_price": limit_price,
                    "requested_trigger_price": trigger,
                    "correlation_id": correlation_id,
                    "timestamp": self._clock(),
                    "filled_quantity": 0,
                    "average_fill_price": 0.0,
                    "last_accounted_qty": 0,
                }
                self._orders[oid] = rec
                return dict(rec, status=_placement_state(rec.get("status")),
                            note="resolved_via_correlation_lookup")
        if not confirmed_absence:
            # A failed lookup is UNRESOLVED, never "rejected". Raising the
            # generic error here used to let the engine mark the order
            # REJECTED while the exchange may hold a live order, leaving the
            # local book flat against real broker exposure.
            raise OrderPlacementUnresolved(
                f"place_order outcome UNRESOLVED after "
                f"{_MAX_CORRELATION_LOOKUP_RETRIES + 1} correlation lookups "
                f"({correlation_id}); the order may have reached Dhan: "
                f"{lookup_error or origin}")
        if confirmed_absence_retries < _MAX_CONFIRMED_ABSENCE_RETRIES:
            # A successful lookup with no matching broker order (including
            # Dhan's explicit external-order 404) proves this attempt did not
            # land. Retry only this confirmed-absence case. Reuse the
            # correlation id so an unexpectedly late first result can still
            # be resolved without minting another logical order identity.
            delay = 0.1 * (2 ** confirmed_absence_retries)
            time.sleep(delay)
            return self.place_market_order(
                side=side_u, quantity=quantity, instrument=instrument,
                order_type=order_type_u, price=limit_price,
                trigger_price=trigger, correlation_id=correlation_id,
                _confirmed_absence_retries=confirmed_absence_retries + 1)
        raise RuntimeError(
            f"place_order not delivered after "
            f"{_MAX_CONFIRMED_ABSENCE_RETRIES + 1} attempts; Dhan confirmed no "
            f"order for correlation {correlation_id}: {origin}")

    # ── Phase 5 wire primitives ─────────────────────────────────────────

    def order_by_correlation_id(self, correlation_id: str) -> dict:
        """Resolve a placement by its correlation id (``GET /orders/external/{id}``).

        The recovery path for an order whose ``POST /orders`` response was lost
        AND whose own correlation lookup failed at placement time: such an order
        has no ``broker_order_id``, so it can never be self-healed by
        :meth:`order_status`. This is the only way to learn whether it reached
        the exchange.

        Returns a status dict, ``not_found`` only for Dhan's explicit 404, or
        ``unresolved`` when the lookup failed or returned no usable order
        record. An empty 200 response is not proof of non-execution.
        """
        cid = str(correlation_id or "").strip()
        if not cid:
            return {"status": "unresolved", "reason": "missing_correlation_id"}
        path = f"/orders/external/{cid}"
        body = None
        all_404 = True
        last_error = None
        for attempt in range(_MAX_CORRELATION_LOOKUP_RETRIES + 1):
            try:
                response = self._http._get(path)
            except Exception as exc:
                self._audit("ORDER_BY_CORRELATION", path, "GET", error=exc,
                            http_status=getattr(exc, "status", None) or 400,
                            correlation_id=cid)
                last_error = exc
                if getattr(exc, "status", None) != 404:
                    all_404 = False
            else:
                self._audit("ORDER_BY_CORRELATION", path, "GET",
                            response=response, http_status=200,
                            correlation_id=cid)
                candidate = _coerce_status_body(response)
                if candidate.get("orderId") or candidate.get("order_id"):
                    body = candidate
                    break
                all_404 = False
                last_error = RuntimeError(
                    "correlation lookup returned no usable order record")
            if attempt < _MAX_CORRELATION_LOOKUP_RETRIES:
                time.sleep(0.1 * (2 ** attempt))
        if body is None and all_404:
            return {"status": "not_found", "correlation_id": cid,
                    "reason": f"broker_no_such_order after retries: {last_error}"}
        if body is None:
            return {"status": "unresolved",
                    "reason": f"correlation_lookup_unresolved: {last_error}",
                    "correlation_id": cid}
        oid = body.get("orderId") or body.get("order_id")
        oid = str(oid)
        with self._lock:
            rec = self._orders.get(oid)
        if rec is not None:
            return dict(rec)
        raw_status = (body.get("orderStatus") or body.get("order_status")
                      or "PENDING")
        return {
            "broker_order_id": oid,
            "status": _normalize_status(raw_status),
            "raw_status": str(raw_status).upper(),
            "reason": (body.get("omsErrorDescription")
                       or body.get("errorMessage") or None),
            "correlation_id": cid,
            "quantity": body.get("quantity"),
            "tradedQuantity": body.get("tradedQuantity"),
            "averageTradedPrice": body.get("averageTradedPrice"),
            "timestamp": self._clock(),
        }

    def _http_delete(self, path: str) -> dict:
        deleter = getattr(self._http, "_delete", None)
        if not callable(deleter):
            raise RuntimeError(
                "broker HTTP layer does not support DELETE; "
                "order cancel unavailable")
        return deleter(path)

    def _http_put(self, path: str, payload: dict) -> dict:
        putter = getattr(self._http, "_put", None)
        if not callable(putter):
            raise RuntimeError(
                "broker HTTP layer does not support PUT; "
                "order modify unavailable")
        return putter(path, payload)

    def cancel_order(self, broker_order_id: str) -> dict:
        """Cancel a live Dhan order (``DELETE /orders/{order-id}``).

        Idempotent: a settled/cancelled order answers without a network call.
        """
        bid = str(broker_order_id)
        with self._lock:
            rec = self._orders.get(bid)
            # C9 — settle-short-circuit must key on the RAW terminal status.
            # Before this fix the normalized 'filled' from PART_TRADED skipped
            # the DELETE while a live remainder kept filling at Dhan.
            if rec is not None and _is_raw_terminal(rec.get("raw_status")):
                return {
                    "broker_order_id": bid,
                    "ok": True,
                    "status": rec.get("status"),
                    "raw_status": rec.get("raw_status"),
                }
            try:
                resp = self._http_delete(f"/orders/{bid}")
            except Exception as exc:
                # Dhan answers a cancel on an already-settled order with an
                # order-level 400 (DH-906 "Order Is Cancelled..."); the token
                # renew+retry loop in the REST client turns that into a raised
                # error.  When the poll already knows the order is terminal the
                # settlement is authoritative — answer ok, never chain an auth
                # renewal on an order-error body.
                inner = str(exc)
                if rec is not None and _is_raw_terminal(rec.get("raw_status")):
                    return {
                        "broker_order_id": bid,
                        "ok": True,
                        "status": rec.get("status"),
                        "raw_status": rec.get("raw_status"),
                        "note": "already settled (broker answered order error)",
                    }
                self._audit("CANCEL", f"/orders/{bid}", "DELETE", error=exc,
                            correlation_id=(rec or {}).get("correlation_id"))
                raise
            self._audit("CANCEL", f"/orders/{bid}", "DELETE", response=resp,
                        http_status=200,
                        correlation_id=(rec or {}).get("correlation_id"))
            # DELETE acknowledgement is not proof that the order stopped
            # working. Re-read broker state before releasing its replacement.
            try:
                status_body = _coerce_status_body(self._http._get(f"/orders/{bid}"))
            except Exception as exc:
                self._audit("CANCEL_VERIFY", f"/orders/{bid}", "GET", error=exc,
                            correlation_id=(rec or {}).get("correlation_id"))
                raise RuntimeError("Dhan cancel status could not be verified") from exc
            self._audit("CANCEL_VERIFY", f"/orders/{bid}", "GET",
                        response=status_body, http_status=200,
                        correlation_id=(rec or {}).get("correlation_id"))
            raw_status = status_body.get("orderStatus") or status_body.get("order_status")
            if not raw_status:
                raise RuntimeError("Dhan cancel status missing from verification")
            new_status = _normalize_status(raw_status)
            if rec is not None:
                rec["status"] = new_status
                rec["raw_status"] = str(raw_status).upper()
            return {
                "broker_order_id": resp.get("orderId") or bid,
                "ok": new_status in ("cancelled", "rejected", "expired"),
                "status": new_status,
                "raw_status": str(raw_status).upper(),
            }

    # ── Emergency exit-all: strictly OUR OWN orders/positions ───────────

    def cancel_all_orders(self) -> list:
        """Cancel every still-resting order THIS SYSTEM placed.

        Only orders in the transport's own book are considered (raw_status
        PENDING / TRANSIT / PART_TRADED); anything this system never placed —
        and anything already settled — is never touched.
        """
        with self._lock:
            pending = [
                bid for bid, rec in self._orders.items()
                if str(rec.get("raw_status") or "PENDING").upper()
                in ("PENDING", "TRANSIT", "PART_TRADED")
            ]
        results: list[dict] = []
        for bid in pending:
            try:
                results.append(self.cancel_order(bid))
            except Exception as e:  # one failure never aborts the sweep
                results.append({
                    "broker_order_id": bid, "ok": False, "error": str(e),
                })
        return results

    def cancel_instrument_orders(self, instrument: str) -> list:
        """Cancel every still-resting order THIS SYSTEM placed for ONE
        instrument (contract-rollover window).  Other instruments are never
        touched."""
        with self._lock:
            pending = [
                bid for bid, rec in self._orders.items()
                if rec.get("instrument") == instrument
                and str(rec.get("raw_status") or "PENDING").upper()
                in ("PENDING", "TRANSIT", "PART_TRADED")
            ]
        results: list[dict] = []
        for bid in pending:
            try:
                results.append(self.cancel_order(bid))
            except Exception as e:
                results.append({
                    "broker_order_id": bid, "ok": False, "error": str(e),
                })
        return results

    def _own_net_positions(self) -> list[dict]:
        """Net exposure per instrument from OUR OWN fills only.

        The full Dhan position book (which may include manual broker-side
        trades) is never consulted: a position this system did not open is
        never squared off.
        """
        with self._lock:
            net: dict[str, int] = {}
            for rec in self._orders.values():
                inst = rec.get("instrument")
                qty = int(rec.get("filled_quantity") or 0)
                if not inst or qty <= 0:
                    continue
                side = str(rec.get("side") or "").upper()
                net[inst] = net.get(inst, 0) + (qty if side == "BUY" else -qty)
        return [
            {"instrument": inst, "side": "LONG" if n > 0 else "SHORT",
             "quantity": abs(n)}
            for inst, n in net.items() if n != 0
        ]

    def close_all_positions(self) -> dict:
        """Direct flattening is disabled; exits require a position lifecycle."""
        return {"closed": [], "errors": [{
            "error": "DIRECT_FLATTEN_DISABLED: use TradingEngine.emergency_exit_all"
        }]}

    def close_instrument(self, instrument: str) -> dict:
        """Direct flattening is disabled; use a lifecycle-owned engine exit."""
        return {"closed": [], "errors": [{
            "instrument": instrument,
            "error": "DIRECT_FLATTEN_DISABLED: use TradingEngine.emergency_exit_all"
        }]}

    # ── §9.11 pre-trade circuit-limit gate (POST /marketfeed/quote) ─────

    def circuit_quote(self, instrument: str) -> dict:
        """Fetch the live quote / circuit band for a mapped instrument
        (``POST /marketfeed/quote``).

        Documented contract (live-verified 2026-09-15): the request body is the
        batch map ``{exchangeSegment: [securityId, ...]}`` and the endpoint is
        authenticated with BOTH the ``access-token`` and ``client-id`` headers
        (a ``dhanClientId`` inside the body is ignored and Dhan answers 401
        "ClientId is invalid", error 810).  The response nests the band fields
        under ``data[exchangeSegment][securityId]`` with snake_case names
        (``lower_circuit_limit`` / ``upper_circuit_limit`` / ``last_price``).
        Defensive parse: any spelling of the band fields is accepted; returns
        {} when the fetch or parse fails."""
        sec = self.instruments.get(instrument)
        if sec is None or not sec.get("security_id"):
            return {}
        segment = sec.get("exchange_segment", "MCX_COMM")
        secid = str(sec["security_id"])
        payload = {segment: [int(secid)]}
        with self._lock:
            try:
                body = self._http._post(
                    "/marketfeed/quote", payload,
                    extra_headers={"client-id": self.client_id},
                ) or {}
                origin_status = 200
            except TypeError:
                # HTTP layers that don't expose the per-request header knob
                # (fakes) fall back to the plain call.
                try:
                    body = self._http._post("/marketfeed/quote", payload) or {}
                    origin_status = 200
                except Exception as exc:
                    self._audit("CIRCUIT_QUOTE", "/marketfeed/quote", "POST",
                                payload, error=exc)
                    return {}
            except Exception as exc:
                self._audit("CIRCUIT_QUOTE", "/marketfeed/quote", "POST",
                            payload, error=exc)
                return {}
            self._audit("CIRCUIT_QUOTE", "/marketfeed/quote", "POST", payload,
                        response=body, http_status=origin_status)
            if not isinstance(body, dict):
                return {}
            # Nested documented shape: data[segment][securityId] = row.
            row = None
            data = body.get("data") if isinstance(body, dict) else None
            if isinstance(data, dict):
                seg_block = data.get(segment)
                if isinstance(seg_block, dict):
                    row = seg_block.get(secid) or seg_block.get(int(secid))
            if not isinstance(row, dict):
                # Tolerate a flattened response keyed by securityId.
                row = (body.get(secid) if isinstance(body, dict)
                       else None) or {}
            if not isinstance(row, dict):
                row = {}
            upper = (row.get("upper_circuit_limit")
                     or row.get("upperCircuitLimit")
                     or row.get("upper_ckt_limit") or row.get("upperCircuit"))
            lower = (row.get("lower_circuit_limit")
                     or row.get("lowerCircuitLimit")
                     or row.get("lower_ckt_limit") or row.get("lowerCircuit"))
            ltp = (row.get("last_price") or row.get("lastTradedPrice")
                   or row.get("ltp") or row.get("closePrice")
                   or row.get("lastPrice"))
            oi = row.get("oi") or row.get("openInterest") or row.get("open_interest")
            return {
                "security_id": secid,
                "ltp": float(ltp) if ltp not in (None, "") else None,
                "upper_circuit_limit": float(upper) if upper not in (None, "") else None,
                "lower_circuit_limit": float(lower) if lower not in (None, "") else None,
                "open_interest": float(oi) if oi not in (None, "") else None,
                "raw": body,
            }

    def _enforce_circuit_gate(self, instrument: str, price: float,
                              order_type: str) -> None:
        """Reject a priced order whose price/trigger lies outside the live
        exchange circuit band.  Fail-open by default (a quote hiccup never
        blocks trading); ``fail_closed`` config makes a missing band reject."""
        quote = self.circuit_quote(instrument)
        upper = quote.get("upper_circuit_limit")
        lower = quote.get("lower_circuit_limit")
        if upper is None or lower is None:
            if self._circuit_gate_fail_closed:
                raise PreTradeGateBlocked(
                    f"PRETRADE_CIRCUIT_GATE: no circuit band for {instrument} "
                    f"(order not sent)")
            return
        if not (lower <= price <= upper):
            raise PreTradeGateBlocked(
                f"PRETRADE_CIRCUIT_GATE: {order_type} price {price} for "
                f"{instrument} outside circuit band [{lower}, {upper}] "
                f"(order not sent)")
        return

    # ── §9.11 day-order-book reconciliation (GET /orders) ─────────────

    def day_order_book(self) -> list:
        """Today's full broker order book (``GET /orders``), expanded to the
        transport's neutral row shape.  All orders the account placed today —
        including ones this system never created — are returned for
        reconciliation; only orders carrying OUR correlation id are ever
        adopted into the book."""
        with self._lock:
            try:
                rows = self._http._get("/orders") or []
            except Exception as exc:
                self._audit("DAY_ORDER_BOOK", "/orders", "GET", error=exc)
                return []
            self._audit("DAY_ORDER_BOOK", "/orders", "GET", response=rows,
                        http_status=200)
            if not isinstance(rows, list):
                rows = [rows]
            out = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                raw = (row.get("orderStatus") or row.get("order_status") or "PENDING")
                sec_id = str(row.get("securityId") or "")
                out.append({
                    "broker_order_id": str(row.get("orderId")
                                           or row.get("order_id") or ""),
                    "status": _normalize_status(raw),
                    "raw_status": str(raw).upper(),
                    "side": (row.get("transactionType")
                             or row.get("transaction_type") or "").upper(),
                    "quantity": int(row.get("quantity") or 0),
                    "filled_quantity": int(row.get("filledQty")
                                           or row.get("filled_quantity") or 0),
                    "average_fill_price": float(row.get("averageTradedPrice")
                                                or row.get("average_fill_price") or 0.0),
                    "price": float(row.get("price") or 0.0),
                    "trigger_price": float(row.get("triggerPrice") or 0.0),
                    "order_type": (row.get("orderType")
                                   or row.get("order_type") or "").upper(),
                    "correlation_id": row.get("correlationId")
                                      or row.get("correlation_id") or "",
                    "security_id": sec_id,
                    "reason": (row.get("omsErrorDescription")
                               or row.get("errorMessage") or row.get("reason") or None),
                })
            return out

    def adopt_order(self, row: dict) -> Optional[dict]:
        """Seed the transport book from a broker day-order-book row that THIS
        system placed in a previous process run.

        Only rows carrying our correlation prefix are accepted; the row must
        resolve to a configured instrument; already-known orders are left
        untouched.  ``last_accounted_qty`` is seeded from the reported fill so
        the next :meth:`order_statuses` poll emits only genuinely NEW deltas.
        """
        bid = str(row.get("broker_order_id") or "")
        cid = str(row.get("correlation_id") or "")
        if not bid or not cid:
            return None
        if not (cid.startswith("MCX-") or cid.startswith("EMER-")):
            return None
        sec_id = str(row.get("security_id") or "")
        instrument = None
        for inst, cfg in self.instruments.items():
            if sec_id and str(cfg.get("security_id", "")) == sec_id:
                instrument = inst
                break
        if instrument is None:
            return None
        with self._lock:
            if bid in self._orders:
                return dict(self._orders[bid])
            rec = {
                "broker_order_id": bid,
                "status": row.get("status") or "submitted",
                "raw_status": str(row.get("raw_status") or "PENDING").upper(),
                "reason": row.get("reason"),
                "side": row.get("side") or "BUY",
                "quantity": int(row.get("quantity") or 0),
                "instrument": instrument,
                "price": row.get("average_fill_price") or None,
                "requested_order_type": row.get("order_type") or "MARKET",
                "requested_price": float(row.get("price") or 0.0),
                "requested_trigger_price": float(row.get("trigger_price") or 0.0),
                "correlation_id": cid,
                "timestamp": self._clock(),
                "filled_quantity": int(row.get("filled_quantity") or 0),
                "average_fill_price": float(row.get("average_fill_price") or 0.0),
                "last_accounted_qty": int(row.get("filled_quantity") or 0),
            }
            self._orders[bid] = rec
            return dict(rec)

    def modify_order(self, broker_order_id: str, order_type: str = "MARKET",
                     price: Optional[float] = None,
                     trigger_price: Optional[float] = None, quantity: int = 0,
                     validity: str = "DAY") -> dict:
        """Modify a live Dhan order (``PUT /orders/{order-id}``)."""
        order_type_u = (order_type or "MARKET").upper()
        # A modify must not be able to CREATE a resting broker-side stop
        # either (INVARIANT 10).
        if order_type_u in ("STOP_LOSS", "STOP_LOSS_MARKET"):
            raise ValueError(
                "BROKER_SL_RETIRED: broker-side protective stop orders are "
                "disabled; the stop is the local position-owned SL monitor")
        if order_type_u not in ("MARKET", "LIMIT"):
            raise ValueError(f"unsupported Dhan orderType: {order_type_u}")
        if order_type_u == "LIMIT" and not (price is not None and float(price) > 0):
            raise ValueError("LIMIT orders require a positive price")
        limit_price = float(price or 0.0) if order_type_u == "LIMIT" else 0.0
        bid = str(broker_order_id)
        with self._lock:
            rec = self._orders.get(bid)
            payload = {
                "dhanClientId": self.client_id,
                "orderId": bid,
                "orderType": order_type_u,
                "quantity": int(quantity) if quantity else int(rec.get("quantity") or 0),
                "price": limit_price,
                "triggerPrice": float(trigger_price or 0.0),
                "validity": validity,
            }
            resp = self._http_put(f"/orders/{bid}", payload)
            self._audit("MODIFY", f"/orders/{bid}", "PUT", payload,
                        response=resp, http_status=200,
                        correlation_id=(rec or {}).get("correlation_id"))
            raw_status = resp.get("orderStatus") or resp.get("order_status") or "PENDING"
            new_status = _normalize_status(raw_status)
            if rec is not None:
                rec["status"] = new_status
                rec["raw_status"] = str(raw_status).upper()
                rec["requested_order_type"] = order_type_u
                rec["requested_price"] = limit_price
                rec["requested_trigger_price"] = float(trigger_price or 0.0)
            return {
                "broker_order_id": resp.get("orderId") or bid,
                "ok": new_status not in ("cancelled", "rejected", "expired"),
                "status": new_status,
                "raw_status": str(raw_status).upper(),
            }

    def tradebook(self) -> list:
        """Today's exchange tradebook (``GET /trades``)."""
        with self._lock:
            try:
                rows = self._http._get("/trades") or []
            except Exception as exc:
                self._audit("TRADEBOOK", "/trades", "GET", error=exc)
                return []
            self._audit("TRADEBOOK", "/trades", "GET", response=rows,
                        http_status=200)
            if not isinstance(rows, list):
                return []
            return [dict(r) for r in rows]

    def order_trades(self, order_id: str) -> list:
        """Exchange trades for one order (``GET /trades/{order-id}``)."""
        with self._lock:
            try:
                rows = self._http._get(f"/trades/{str(order_id)}") or []
            except Exception as exc:
                self._audit("ORDER_TRADES", f"/trades/{order_id}", "GET",
                            error=exc)
                return []
            self._audit("ORDER_TRADES", f"/trades/{order_id}", "GET",
                        response=rows, http_status=200)
            # Dhan's documented response example for /trades/{order-id} is a
            # single trade object, while deployments may return a list when an
            # order has multiple partial executions. Normalize both shapes so
            # the audit/reconciliation path does not silently drop the
            # documented object form.
            if isinstance(rows, dict):
                rows = [rows]
            if not isinstance(rows, list):
                return []
            return [dict(r) for r in rows if isinstance(r, dict)]

    def order_by_correlation(self, correlation_id: str) -> Optional[dict]:
        """Resolve a broker order by our correlation id
        (``GET /orders/external/{correlation-id}``)."""
        with self._lock:
            try:
                body = self._http._get(f"/orders/external/{correlation_id}") or {}
            except Exception as exc:
                self._audit("ORDER_BY_CORRELATION",
                            f"/orders/external/{correlation_id}", "GET",
                            error=exc, correlation_id=correlation_id)
                return None
            self._audit("ORDER_BY_CORRELATION",
                        f"/orders/external/{correlation_id}", "GET",
                        response=body, http_status=200,
                        correlation_id=correlation_id)
            return {
                "broker_order_id": body.get("orderId") or body.get("order_id"),
                "status": _normalize_status(
                    body.get("orderStatus") or body.get("order_status") or "PENDING"),
                "raw_status": str(body.get("orderStatus") or body.get("order_status")
                                  or "PENDING").upper(),
                "side": (body.get("transactionType") or body.get("transaction_type")
                         or "").upper(),
                "quantity": int(body.get("quantity") or 0),
                "correlation_id": correlation_id,
            }

    def verify_static_ip(self) -> dict:
        """Static-IP whitelist readiness (``GET /ip/getIP``).

        Dhan requires the request IP to be static-IP whitelisted for the
        order APIs.  Field names on this endpoint are not yet verified
        against a real account, so the parse is defensive: any key that
        *looks* like an IP / an enable-flag is accepted, and the raw body is
        preserved for forensics.  ``whitelisted`` is only True when an
        explicit enabled flag or an active status is observed; it is never
        assumed from a bare IP echo.
        """
        with self._lock:
            try:
                body = self._http._get("/ip/getIP") or {}
            except Exception as exc:
                self._audit("STATIC_IP", "/ip/getIP", "GET", error=exc)
                return {"available": False, "whitelisted": False, "ip": None,
                        "reason": "getIP error"}
            self._audit("STATIC_IP", "/ip/getIP", "GET", response=body,
                        http_status=200)
            if not isinstance(body, dict):
                body = {}
            ip = (body.get("ip") or body.get("IP") or body.get("ipAddress")
                  or body.get("requestIp") or body.get("detectedIP") or None)
            enabled_raw = (body.get("isEnabled") if "isEnabled" in body
                           else body.get("enabled")
                           if "enabled" in body else body.get("isActive")
                           if "isActive" in body else body.get("status")
                           if "status" in body else body.get("ordersAllowed")
                           if "ordersAllowed" in body else body.get("ipMatchStatus"))
            status_u = str(enabled_raw or "").upper()
            if isinstance(enabled_raw, bool):
                observed = enabled_raw
            else:
                observed = (status_u in ("TRUE", "1", "ACTIVE", "ACTIVATED",
                                         "ENABLED", "WHITELISTED", "PRIMARY_MATCH"))
            return {
                "available": True,
                "whitelisted": bool(observed),
                "ip": ip,
                "raw": body,
            }

    def kill_switch_status(self) -> dict:
        """Broker kill-switch (``GET /killswitch``) — ACTIVATE blocks new
        orders broker-side."""
        with self._lock:
            try:
                body = self._http._get("/killswitch") or {}
            except Exception as exc:
                self._audit("KILLSWITCH", "/killswitch", "GET", error=exc)
                return {"available": False, "active": False, "reason": "killswitch error"}
            self._audit("KILLSWITCH", "/killswitch", "GET", response=body,
                        http_status=200)
            raw = (body.get("killSwitchStatus") or body.get("kill_switch_status")
                   or body.get("status") or "")
            active = str(raw).upper() in ("ACTIVATE", "ACTIVE", "TRIGGERED", "ON")
            return {"available": True, "active": active, "raw": raw}

    def update_price(self, instrument: str, price: float) -> None:
        pass

    def order_statuses(self) -> dict:
        """Poll Dhan for the latest status of every in-flight order.

        Emits exactly one fill per newly-traded quantity delta, at the
        exchange-reported average fill price.  Idempotent under repeated polls.
        """
        with self._lock:
            out: dict = {}
            for bid in list(self._orders.keys()):
                rec = self._orders.get(bid)
                if rec is None:
                    continue
                # Cached-terminal skip: rejected/cancelled/expired orders were
                # already confirmed by the broker; the cached record is
                # authoritative and re-polling them forever is pure waste.
                # Every poll previously fired one GET per such order, so as
                # the day's flow settles the poll count collapses.
                cached_status = str(rec.get("status") or "").lower()
                if cached_status in ("rejected", "cancelled", "expired"):
                    out[bid] = self._record_for(bid)
                    continue
                if (cached_status == "filled"
                        and str(rec.get("raw_status") or "").upper() == "TRADED"
                        and int(rec.get("quantity") or 0) > 0
                        and int(rec.get("last_accounted_qty") or 0)
                        >= int(rec.get("quantity") or 0)):
                    # Canonically-terminal fully-accounted fill (raw TRADED):
                    # every traded delta (with a known price) was already
                    # emitted, so the network call cannot teach us anything
                    # new.  PART_TRADED stays polled — it is still in-flight
                    # (a multi-lot remainder can fill).  Kept in the book so
                    # _own_net_positions (EMERGENCY sizing) still sees it.
                    out[bid] = self._record_for(bid)
                    continue
                try:
                    body = self._http._get(f"/orders/{bid}")
                except Exception as exc:
                    self._audit("ORDER_STATUS", f"/orders/{bid}", "GET",
                                error=exc,
                                correlation_id=rec.get("correlation_id"))
                    # A broker hiccup must not kill the poll: keep the last
                    # known status; the next poll re-attempts.
                    continue
                self._audit("ORDER_STATUS", f"/orders/{bid}", "GET",
                            response=body, http_status=200,
                            correlation_id=rec.get("correlation_id"))
                body = _coerce_status_body(body)
                rec = self._orders[bid]
                raw_status = body.get("orderStatus") or rec.get("raw_status") or "PENDING"
                # Real Dhan field is "filledQty" (live-verified); the other
                # spellings are tolerated defensively.
                traded_raw = next((body[name] for name in
                                   ("filledQty", "tradedQuantity", "tradedQty")
                                   if body.get(name) is not None), 0)
                avg_raw = next((body[name] for name in
                                ("averageTradedPrice", "averageFillPrice")
                                if body.get(name) is not None), 0.0)
                try:
                    traded_qty = int(traded_raw)
                    avg_price = float(avg_raw)
                    if (traded_qty < 0 or avg_price < 0
                            or not math.isfinite(avg_price)):
                        raise ValueError("negative/non-finite fill data")
                except (TypeError, ValueError, OverflowError) as exc:
                    # One malformed status row must not abort polling every
                    # other live order. Keep this order's last known truth and
                    # retry it on the next cycle.
                    self._audit("ORDER_STATUS_DECODE", f"/orders/{bid}",
                                "GET", response=body, error=exc,
                                correlation_id=rec.get("correlation_id"))
                    continue
                rec["raw_status"] = str(raw_status).upper()
                rec["status"] = _normalize_status(raw_status)
                # The broker's order-level detail (rejection reason: funds,
                # settlement, product, rate-limit) stays with the order so the
                # book and the engine's reason string carry the real message.
                rec["reason"] = (body.get("omsErrorDescription")
                                 or body.get("errorMessage")
                                 or body.get("reason")
                                 or rec.get("reason") or None)
                previously_reported = int(rec.get("filled_quantity") or 0)
                previous_accounted = int(rec.get("last_accounted_qty") or 0)
                previous_average = float(rec.get("average_fill_price") or 0.0)
                # REST snapshots can arrive out of order around a partial
                # fill. Never regress cumulative quantity or apply a stale
                # average to a later fill delta.
                if traded_qty < previously_reported:
                    traded_qty = previously_reported
                    avg_price = previous_average
                rec["filled_quantity"] = traded_qty
                delta = traded_qty - previous_accounted
                # Dhan's averageTradedPrice is the cumulative average for the
                # order, not the price of the latest delta. Recover the
                # weighted average price of the newly observed delta so
                # applying multiple PART_TRADED updates preserves the broker's
                # cumulative average instead of reusing it as every fill price.
                delta_average = avg_price
                if delta > 0 and previous_accounted > 0 and avg_price > 0:
                    delta_average = (
                        avg_price * traded_qty
                        - previous_average * previous_accounted
                    ) / delta
                if avg_price > 0:
                    rec["average_fill_price"] = avg_price
                if delta > 0 and delta_average > 0 and math.isfinite(delta_average):
                    seq = self._fill_seq.get(bid, 0) + 1
                    self._fill_seq[bid] = seq
                    broker_fill_id = f"{bid}:fill:{seq}"
                    self._fills[broker_fill_id] = {
                        "broker_fill_id": broker_fill_id,
                        "broker_order_id": bid,
                        "status": "filled",
                        "side": rec.get("side"),
                        "quantity": delta,
                        "price": delta_average,
                        "timestamp": self._clock(),
                        "instrument": rec.get("instrument"),
                    }
                    rec["last_accounted_qty"] = traded_qty
                # A filled poll without a known price advances nothing: the
                # delta stays pending and the next poll re-attempts with a
                # price, so a late averageTradedPrice never slips a fill.
                out[bid] = self._record_for(bid)
            return out

    def order_status(self, broker_order_id: str) -> dict:
        """Single-order Dhan status (``GET /orders/{id}``).

        Read-only probe used by the poller's F2 self-heal to resolve
        ENTRY_SENT pending rows whose order is no longer in the transport's
        in-memory book (post-restart).  Mirrors the per-order rules in
        :meth:`order_statuses` without touching the native order book: it
        never manufactures fills or deltas, it only reports the broker's word.
        """
        with self._lock:
            try:
                body = self._http._get(f"/orders/{broker_order_id}")
            except Exception as exc:
                self._audit("ORDER_STATUS", f"/orders/{broker_order_id}", "GET",
                            error=exc, correlation_id=None)
                return {"broker_order_id": broker_order_id, "status": "unknown"}
            self._audit("ORDER_STATUS", f"/orders/{broker_order_id}", "GET",
                        response=body, http_status=200)
            body = _coerce_status_body(body)
            raw_status = str(body.get("orderStatus") or "PENDING").upper()
            rec = {
                "broker_order_id": broker_order_id,
                "status": _normalize_status(raw_status),
                "raw_status": raw_status,
                "reason": (body.get("omsErrorDescription")
                           or body.get("errorMessage") or body.get("reason") or None),
            }
            return rec

    def _record_for(self, bid: str) -> dict:
        rec = self._orders[bid]
        return {
            "broker_order_id": bid,
            "status": rec.get("status"),
            "raw_status": rec.get("raw_status"),
            "reason": rec.get("reason"),
            "side": rec.get("side"),
            "quantity": rec.get("quantity"),
            "instrument": rec.get("instrument"),
            "filled_quantity": rec.get("filled_quantity", 0),
            "average_fill_price": rec.get("average_fill_price", 0.0),
            "price": rec.get("average_fill_price") or None,
            "timestamp": rec.get("timestamp"),
            "requested_order_type": rec.get("requested_order_type"),
            "requested_price": rec.get("requested_price"),
            "requested_trigger_price": rec.get("requested_trigger_price"),
            "correlation_id": rec.get("correlation_id"),
            "fills": [
                dict(f) for f in self._fills.values()
                if f.get("broker_order_id") == bid
            ],
        }

    def fills(self) -> list:
        with self._lock:
            return [dict(f) for f in self._fills.values()]

    def positions(self) -> list[dict]:
        """Dhan :class:`GET /positions` expanded to one row per mapped strategy.

        Rows are keyed by securityId (matched against the configured
        instruments).  netQty>0 -> LONG, netQty<0 -> SHORT.  Positions are
        instrument-level on the exchange; the transport expands each to every
        strategy configured on that instrument so the poller can surface
        internal-vs-broker mismatches per strategy.
        """
        # Do not hold the in-memory order-book lock across a blocking broker
        # request.  order_statuses() and other poll paths also use this lock;
        # holding it during HTTP I/O can stall emergency exits behind an
        # unrelated Dhan request even though order placement itself is safe.
        try:
            rows = self._http._get("/positions")
        except Exception as exc:
            self._audit("POSITIONS", "/positions", "GET", error=exc)
            raise RuntimeError("Dhan positions query failed") from exc
        self._audit("POSITIONS", "/positions", "GET", response=rows,
                    http_status=200)
        if not isinstance(rows, list):
            raise ValueError("Dhan positions response is not a list")
        out: list[dict] = []
        for row in rows:
            sec_id = str(row.get("securityId") or "")
            instrument = None
            for inst, cfg in self.instruments.items():
                if sec_id and str(cfg.get("security_id", "")) == sec_id:
                    instrument = inst
                    break
            if instrument is None:
                continue
            net = int(row.get("netQty") or row.get("netQuantity") or 0)
            if net == 0:
                continue
            side = "LONG" if net > 0 else "SHORT"
            qty = abs(net)
            avg = float(row.get("buyAvg" if side == "LONG" else "sellAvg") or 0.0)
            # Broker-reported P&L (live field names verified capture) — kept
            # so the live dashboard can render broker truth, not re-derived.
            realized = float(row.get("realizedProfit")
                             or row.get("realized_profit") or 0.0)
            unrealized = float(row.get("unrealizedProfit")
                               or row.get("unrealized_profit")
                               or row.get("unRealizedProfit") or 0.0)
            # Dhan's GET /positions carries NO last-traded price.  Use the
            # authoritative LTP from the market-quote cache when the instrument
            # has one; leave it None rather than inventing 0.0.
            ltp_raw = (row.get("ltp") or row.get("LTP")
                       or row.get("last_price") or row.get("lastTradedPrice"))
            try:
                ltp = float(ltp_raw) if ltp_raw not in (None, "") else None
            except (TypeError, ValueError):
                ltp = None
            if ltp is None:
                try:
                    cached = self.circuit_quote(instrument) or {}
                except Exception:
                    cached = {}
                try:
                    ltp = float(cached.get("ltp")) if cached.get("ltp") else None
                except (TypeError, ValueError):
                    ltp = None
            sids = self.instrument_strategies.get(instrument) or [""]
            for sid in sids:
                out.append({
                    "strategy_id": sid,
                    "instrument": instrument,
                    "side": side,
                    "quantity": qty,
                    "average_entry_price": avg,
                    "realized_profit": realized,
                    "unrealized_profit": unrealized,
                    "net_profit": realized + unrealized,
                    "ltp": ltp,
                })
        return out

    def ingest_status(self, record: dict) -> bool:
        """Accelerator-only WS ingest: update the transport book from a parsed
        order alert so the next REST poll sees fresh status/filled_quantity.

        This method never mints fills; delta accounting remains exclusively
        inside :meth:`order_statuses` (the REST authority).
        """
        bid = str(record.get("broker_order_id") or "")
        if not bid:
            return False
        with self._lock:
            rec = self._orders.get(bid)
            if rec is None:
                return False
            raw = str(record.get("raw_status") or record.get("status") or "").upper()
            if raw:
                new_normal = _normalize_status(raw)
                cur_normal = rec.get("status") or "submitted"
                # Monotonic ingest: a reordered/stale alert (e.g. an old
                # PENDING redelivered after TRADED) never regresses the book.
                if _STATUS_RANK.get(new_normal, 0) >= _STATUS_RANK.get(cur_normal, 0):
                    rec["raw_status"] = raw
                    rec["status"] = new_normal
            traded_qty = record.get("filled_quantity")
            if traded_qty is not None:
                new_qty = int(traded_qty)
                if new_qty >= int(rec.get("filled_quantity") or 0):
                    rec["filled_quantity"] = new_qty
            avg_price = record.get("average_fill_price") or 0.0
            if avg_price and avg_price > 0:
                rec["average_fill_price"] = float(avg_price)
            if record.get("reason"):
                rec["reason"] = record.get("reason")
            rec["timestamp"] = record.get("timestamp") or self._clock()
            return True

    def account_status(self) -> dict:
        with self._lock:
            try:
                funds = self._http._get("/fundlimit") or {}
            except Exception as exc:
                self._audit("FUNDLIMIT", "/fundlimit", "GET", error=exc)
                return {"mode": "LIVE", "equity": 0.0, "available_margin": 0.0,
                        "source": "fundlimit(error)"}
            self._audit("FUNDLIMIT", "/fundlimit", "GET", response=funds,
                        http_status=200)
            # The Dhan API ships an (upstream-typo'd) "availabelBalance" field.
            avail = float(
                funds.get("availabelBalance")
                or funds.get("availableBalance")
                or 0.0)
            utilized = float(funds.get("utilizedAmount") or 0.0)
            # Dhan includes realizedProfit on flat (netQty=0) rows.  Keep
            # those realized values; only unrealized P&L is position-scoped.
            realized = 0.0
            unrealized = 0.0
            try:
                pos_rows = self._http._get("/positions") or []
            except Exception as exc:
                pos_rows = []
                self._audit("POSITIONS", "/positions", "GET", error=exc)
            else:
                self._audit("POSITIONS", "/positions", "GET",
                            response=pos_rows, http_status=200)
            for row in pos_rows if isinstance(pos_rows, list) else []:
                if not isinstance(row, dict):
                    continue
                try:
                    realized += float(row.get("realizedProfit")
                                      or row.get("realized_profit") or 0.0)
                except (TypeError, ValueError):
                    pass
                try:
                    net_qty = int(row.get("netQty") or row.get("netQuantity") or 0)
                except (TypeError, ValueError):
                    net_qty = 0
                if net_qty:
                    try:
                        unrealized += float(row.get("unrealizedProfit")
                                            or row.get("unrealized_profit")
                                            or row.get("unRealizedProfit") or 0.0)
                    except (TypeError, ValueError):
                        pass
            return {
                "mode": "LIVE",
                "equity": avail + utilized,
                "available_margin": avail,
                "used_margin": utilized,
                "realized_pnl": round(realized, 2),
                "unrealized_pnl": round(unrealized, 2),
                "net_pnl": round(realized + unrealized, 2),
                "realized_pnl_source": "dhan_positions_realized_profit",
                "dhan_client_id": self.client_id,
                "source": "fundlimit+positions",
            }

    def disconnect(self) -> None:
        if self._owns_http:
            try:
                stopper = getattr(self._http, "stop_scheduler", None)
                if callable(stopper):
                    stopper()
            except Exception:
                pass
