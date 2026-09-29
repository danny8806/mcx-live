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
                           correlation_id: Optional[str] = None) -> dict:
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
        try:
            resp = self._http._post("/orders", payload, retry_network=False)
        except TypeError:
            # HTTP layers that don't expose the placement-retry knob (fakes).
            resp = self._http._post("/orders", payload)
        except Exception as exc:
            place_status = getattr(exc, "status", None)
            self._audit(_AUDIT_ACTION.get(order_type_u, "PLACE_ORDER"),
                        "/orders", "POST", payload, error=exc,
                        http_status=place_status,
                        correlation_id=correlation_id)
            return self._resolve_unknown_placement(
                correlation_id, side_u, quantity, instrument,
                order_type_u, limit_price, trigger, exc)
        self._audit(_AUDIT_ACTION.get(order_type_u, "PLACE_ORDER"),
                    "/orders", "POST", payload, response=resp, http_status=200,
                    correlation_id=correlation_id)
        with self._lock:
            broker_order_id = resp.get("orderId") or resp.get("order_id")
            if not broker_order_id:
                raise RuntimeError(f"Dhan place order returned no order id: {resp}")
            existing = self._orders.get(broker_order_id)
            if existing is not None:
                return dict(existing)
            raw_status = resp.get("orderStatus") or resp.get("order_status") or "PENDING"
            reason = (resp.get("omsErrorDescription")
                      or resp.get("errorMessage")
                      or resp.get("reason") or None)
            now = self._clock()
            rec = {
                "broker_order_id": broker_order_id,
                "status": _normalize_status(raw_status),
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
            # A FILLED status is never taken from the placement response - the
            # fill always flows through the status poll with the exchange
            # price.  A TERMINAL FAILURE is different: Dhan has already settled
            # the order (e.g. a 100-qty margin rejection), so reporting
            # "submitted" would leave the engine believing a dead order is
            # working until the next poll - holding the entry slot and, worse,
            # counting it as exposure.
            if rec["status"] in ("rejected", "cancelled", "expired"):
                return dict(rec, status=rec["status"])
            return dict(rec, status="submitted")

    def _resolve_unknown_placement(self, correlation_id, side_u, quantity,
                                   instrument, order_type_u, limit_price,
                                   trigger, origin: BaseException) -> dict:
        """Recover a placement whose broker outcome is unknown.

        Called when the ``POST /orders`` network response was lost.  The order
        MAY have reached Dhan, so we never blindly re-POST.  Instead we resolve
        the broker by correlation id:

        * an existing broker order -> adopted into the transport book exactly
          once (``note="resolved_via_correlation_lookup"``), so downstream
          reconciliation sees a single economic order;
        * no broker order -> the placement definitively did NOT execute, raised
          so the engine marks the signal REJECTED with the explicit origin.
        """
        with self._lock:
            try:
                body = self._http._get(f"/orders/external/{correlation_id}") or {}
            except Exception as exc:
                self._audit("ORDER_BY_CORRELATION",
                            f"/orders/external/{correlation_id}", "GET",
                            error=exc,
                            http_status=getattr(exc, "status", None) or 400,
                            correlation_id=correlation_id)
                body = {}
            else:
                self._audit("ORDER_BY_CORRELATION",
                            f"/orders/external/{correlation_id}", "GET",
                            response=body, http_status=200,
                            correlation_id=correlation_id)
            oid = body.get("orderId") or body.get("order_id")
            if oid:
                oid = str(oid)
                known = self._orders.get(oid)
                if known is not None:
                    return dict(known, status="submitted",
                                note="resolved_via_correlation_lookup")
                raw_status = (body.get("orderStatus")
                              or body.get("order_status") or "PENDING")
                rec = {
                    "broker_order_id": oid,
                    "status": _normalize_status(raw_status),
                    "raw_status": str(raw_status).upper(),
                    "reason": (body.get("omsErrorDescription")
                               or body.get("errorMessage") or None),
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
                return dict(rec, status="submitted",
                            note="resolved_via_correlation_lookup")
        raise RuntimeError(
            f"place_order broker outcome UNKNOWN and correlation lookup found "
            f"no order ({correlation_id}): {origin}")

    # ── Phase 5 wire primitives ─────────────────────────────────────────

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
                "legName": 0,
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
            if not isinstance(rows, list):
                return []
            return [dict(r) for r in rows]

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
                rec["raw_status"] = str(raw_status).upper()
                rec["status"] = _normalize_status(raw_status)
                # Real Dhan field is "filledQty" (live-verified); the other
                # spellings are tolerated defensively.
                traded_qty = int(body.get("filledQty")
                                 or body.get("tradedQuantity")
                                 or body.get("tradedQty")
                                 or 0)
                avg_price = float(
                    body.get("averageTradedPrice")
                    or body.get("averageFillPrice")
                    or 0.0)
                # The broker's order-level detail (rejection reason: funds,
                # settlement, product, rate-limit) stays with the order so the
                # book and the engine's reason string carry the real message.
                rec["reason"] = (body.get("omsErrorDescription")
                                 or body.get("errorMessage")
                                 or body.get("reason")
                                 or rec.get("reason") or None)
                rec["filled_quantity"] = traded_qty
                if avg_price > 0:
                    rec["average_fill_price"] = avg_price
                delta = traded_qty - int(rec.get("last_accounted_qty") or 0)
                if delta > 0 and avg_price > 0:
                    seq = self._fill_seq.get(bid, 0) + 1
                    self._fill_seq[bid] = seq
                    broker_fill_id = f"{bid}:fill:{seq}"
                    self._fills[broker_fill_id] = {
                        "broker_fill_id": broker_fill_id,
                        "broker_order_id": bid,
                        "status": "filled",
                        "side": rec.get("side"),
                        "quantity": delta,
                        "price": avg_price,
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
        with self._lock:
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
                # authoritative LTP from the market-quote cache when the
                # instrument has one; leave it None rather than inventing 0.0,
                # which would render as a real (wrong) price downstream.
                ltp_raw = (row.get("ltp") or row.get("LTP")
                           or row.get("last_price") or row.get("lastTradedPrice"))
                try:
                    ltp = float(ltp_raw) if ltp_raw not in (None, "") else None
                except (TypeError, ValueError):
                    ltp = None
                if ltp is None:
                    # Fall back to the live market quote the transport already
                    # keeps for the circuit gate.
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
            # Live broker P&L: aggregate over /positions rows so the account
            # card shows broker truth, not locally re-derived numbers.
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
            try:
                for row in pos_rows:
                    if int(row.get("netQty") or 0) == 0:
                        continue
                    realized += float(row.get("realizedProfit")
                                      or row.get("realized_profit") or 0.0)
                    unrealized += float(row.get("unrealizedProfit")
                                        or row.get("unrealized_profit")
                                        or row.get("unRealizedProfit") or 0.0)
            except Exception:
                pass  # positions poll failure must not kill the funds read
            return {
                "mode": "LIVE",
                "equity": avail + utilized,
                "available_margin": avail,
                "used_margin": utilized,
                "realized_pnl": round(realized, 2),
                "unrealized_pnl": round(unrealized, 2),
                "net_pnl": round(realized + unrealized, 2),
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
