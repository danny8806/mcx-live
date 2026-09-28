"""Live broker client abstraction — the LIVE environment's sole trade authority.

Architecture: the broker account is the authority for the LIVE environment.
The engine never trusts its own simulated prices for live fills; every live
order goes through a :class:`LiveBrokerClient` and every live fill comes from
the broker's order/fill records.

  * :class:`LiveBrokerClient` — interface contract.
  * :class:`StubLiveBroker` — deterministic, idempotent broker used while the
    LIVE master gate is OFF (the default) and by tests with the gate ON.
    Honors the gate: any order placed while ``live_trading_enabled=false`` is
    REJECTED with reason ``LIVE_GATE_CLOSED``.
  * :class:`execution.live.dhan_transport.DhanRestTransport` — the real Dhan
    v2 REST account authority (place order / order status / positions /
    funds). Selected with ``live.broker: "dhan"``; order placement enforces
    the master gate and requires valid Dhan credentials.

Idempotency: every broker-native id (``broker_order_id`` / ``broker_fill_id``)
is preserved and stable; re-submitting the same broker_order_id must return the
same result with no side effects on the simulated account.
"""
from __future__ import annotations

import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional


class BrokerGateClosed(Exception):
    """Raised by a live broker while the master LIVE gate is OFF."""


class PreTradeGateBlocked(Exception):
    """Raised by a live broker when a pre-trade safety gate rejects an order.

    Distinct from :class:`BrokerGateClosed`: the master gate is ON, but a
    pre-trade validation (price outside the exchange circuit band, etc.)
    forbids the specific order.  The engine surfaces the message verbatim as
    the order's rejection reason.
    """


class LiveBrokerClient(ABC):
    """Interface contract every live broker transport must satisfy."""

    mode = "LIVE"

    @abstractmethod
    def place_market_order(self, side: str, quantity: int, instrument: str,
                           order_type: str = "MARKET", price: Optional[float] = None,
                           trigger_price: Optional[float] = None,
                           correlation_id: Optional[str] = None) -> dict:
        """Submit a market order. Returns a broker-native result dict with at
        least: broker_order_id, broker_fill_id, status, price, quantity,
        timestamp. Raises BrokerGateClosed when the gate forbids orders.

        Phase 5 — price/order-type primitives:
          * ``MARKET``          -> execute at market (price/trigger ignored)
          * ``LIMIT``           -> price is required; fill only through the limit
          * ``STOP_LOSS_MARKET``-> trigger_price is required (SLM trigger); fills at market after trigger
        """

    @abstractmethod
    def update_price(self, instrument: str, price: float) -> None:
        """Feed an execution reference price for the broker."""

    @abstractmethod
    def positions(self) -> list[dict]:
        """Current broker positions (authoritative for the LIVE account)."""

    @abstractmethod
    def account_status(self) -> dict:
        """Equity / available / used margin snapshot from the broker."""

    def order_statuses(self) -> dict:
        """Latest broker status for every live order.

        Returns ``{broker_order_id: {status, filled_quantity,
        average_fill_price, fills: [{broker_fill_id, side, quantity, price,
        timestamp}]}}``.  The poller consumes this to turn completed broker
        orders into engine fills.  Default no-op for transports that are
        synchronously filled (safe additive contract)."""
        return {}

    def order_status(self, broker_order_id: str) -> dict:
        """Single-order broker status.

        Used by the poller's F2 self-heal to resolve ENTRY_SENT pending rows
        whose order is unknown to the transport's in-memory book (e.g. after a
        restart).  Default: unresolvable without a native order book."""
        return {"broker_order_id": broker_order_id, "status": "unknown"}

    def fills(self) -> list:
        """Every fill the broker has produced so far (idempotent, keyed by
        broker_fill_id).  Default no-op for synchronously-filled transports."""
        return []

    # ── Phase 5 primitives (additive, safe defaults) ─────────────────────

    def cancel_order(self, broker_order_id: str) -> dict:
        """Cancel a broker-native order. Default: unsupported (no-op)."""
        return {"broker_order_id": broker_order_id, "ok": False,
                "status": "unsupported"}

    def modify_order(self, broker_order_id: str, order_type: str = "MARKET",
                     price: Optional[float] = None,
                     trigger_price: Optional[float] = None, quantity: int = 0,
                     validity: str = "DAY") -> dict:
        """Modify a live broker order (price/type/quantity). Default no-op."""
        return {"broker_order_id": broker_order_id, "ok": False,
                "status": "unsupported"}

    def tradebook(self) -> list:
        """Exchange tradebook (today's trades). Default no-op (no tradebook)."""
        return []

    def order_trades(self, order_id: str) -> list:
        """Exchange trades for one order. Default no-op."""
        return []

    def cancel_all_orders(self) -> list:
        """Cancel every PENDING order THIS SYSTEM placed (never the whole
        broker order book).  Returns per-order cancel results. Default: none."""
        return []

    def close_all_positions(self) -> dict:
        """Square off the OPEN positions THIS SYSTEM opened.

        Scope is strictly the system's own orders/fills — a manual broker-side
        position that this system never traded is NEVER touched.  Returns
        ``{"closed": [..], "errors": [...]}``.  Default: no-op."""
        return {"closed": [], "errors": []}

    def emergency_exit_all(self) -> dict:
        """Emergency flatten: cancel every pending order this system placed AND
        square off every open position this system opened.  Idempotent; never
        raises — per-item failures are collected in ``errors``.  Default: empty
        summary (transports without exits are safe no-ops)."""
        cancelled = self.cancel_all_orders()
        closed = self.close_all_positions()
        return {
            "cancelled": cancelled,
            "closed": closed.get("closed", []),
            "errors": closed.get("errors", []),
        }

    def cancel_instrument_orders(self, instrument: str) -> list:
        """Cancel every resting order THIS SYSTEM placed for ONE instrument
        (contract-rollover window).  Never touches other instruments.  Default:
        no-op."""
        return []

    def close_instrument(self, instrument: str) -> dict:
        """Square off the open position THIS SYSTEM opened for ONE instrument
        with opposite MARKET orders (contract-rollover final-session fallback).
        Returns ``{"closed": [...], "errors": [...]}``.  Default: no-op."""
        return {"closed": [], "errors": []}

    def order_by_correlation(self, correlation_id: str) -> Optional[dict]:
        """Resolve a broker order via our own correlation id. Default no-op."""
        return None

    def verify_static_ip(self) -> dict:
        """Static-IP whitelist readiness (Dhan requires it for order APIs).
        Default: unavailable."""
        return {"available": False, "whitelisted": False}

    def kill_switch_status(self) -> dict:
        """Broker kill-switch status. Default: unavailable/inactive."""
        return {"available": False, "active": False}

    def disconnect(self) -> None:
        pass


class StubLiveBroker(LiveBrokerClient):
    """Deterministic live broker stub.

    * gate OFF  -> every order is REJECTED (reason LIVE_GATE_CLOSED).
    * gate ON   -> market orders fill at the current reference price with zero
      random slippage (deterministic; the caller feeds prices via
      :meth:`update_price`), producing a broker_order_id / broker_fill_id pair.
    * ``defer_fills=True`` simulates an ASYNC broker (Dhan REST style): place
      returns a SUBMITTED order with no fill; the fill materializes only when
      :meth:`ack_order` is called (or the poller's status query sees it).

    Idempotent: re-placing the same broker_order_id returns the recorded result
    without mutating the account again.
    """

    def __init__(self, gate_enabled: bool = False, clock: Optional[callable] = None,
                 defer_fills: bool = False):
        self.gate_enabled = bool(gate_enabled)
        self.defer_fills = bool(defer_fills)
        self._clock = clock or time.time
        self._lock = threading.Lock()
        self._orders: dict[str, dict] = {}   # broker_order_id -> order record
        self._fills: dict[str, dict] = {}    # broker_fill_id -> fill record
        self._prices: dict[str, float] = {}
        self._positions: dict[tuple[str, str], dict] = {}  # (strategy, instrument)

    # ── LiveBrokerClient ──────────────────────────────────────────────

    def place_market_order(self, side: str, quantity: int, instrument: str,
                           order_type: str = "MARKET", price: Optional[float] = None,
                           trigger_price: Optional[float] = None,
                           correlation_id: Optional[str] = None) -> dict:
        side_u = str(side).upper()
        with self._lock:
            if not self.gate_enabled:
                raise BrokerGateClosed("LIVE_GATE_CLOSED: live trading master gate is OFF")
            broker_order_id = f"BROKER-LIVE-{uuid.uuid4()}"
            now = self._clock()
            common = {
                "broker_order_id": broker_order_id,
                "status": "filled",
                "side": side_u,
                "quantity": int(quantity),
                "price": None,
                "timestamp": now,
                "instrument": instrument,
                "correlation_id": correlation_id,
            }
            ot = str(order_type or "").upper()
            if ot in ("STOP_LOSS", "STOP_LOSS_MARKET"):
                # Spec §22-24 — a protective/trigger stop is a RESTING order:
                # it is accepted by the broker, never filled at placement.  Its
                # fill materializes only when the trigger is crossed (ack_order
                # with a price past the trigger, or a broker status poll
                # reporting the fill).  trigger_price is recorded on the book.
                rec = dict(common, status="submitted", price=None,
                           filled_quantity=0, average_fill_price=0.0,
                           order_type=ot,
                           trigger_price=float(trigger_price or 0.0))
                self._orders[broker_order_id] = dict(rec)
                return dict(rec)
            if self.defer_fills:
                # Async broker simulation: the order is accepted; the fill is
                # materialized later by ack_order() / stale-status polling.
                rec = dict(common, status="submitted", price=None,
                           filled_quantity=0, average_fill_price=0.0)
                self._orders[broker_order_id] = dict(rec)
                return dict(rec)
            ref_price = self._prices.get(instrument)
            if ref_price is None or ref_price <= 0:
                raise RuntimeError("no reference price for live stub fill")
            broker_fill_id = f"BROKER-FILL-LIVE-{uuid.uuid4()}"
            fill_price = float(ref_price)
            rec = dict(common, broker_fill_id=broker_fill_id, price=fill_price,
                       filled_quantity=int(quantity), average_fill_price=fill_price)
            self._orders[broker_order_id] = dict(rec)
            self._fills[broker_fill_id] = dict(
                common, broker_fill_id=broker_fill_id, price=fill_price,
                filled_quantity=int(quantity), average_fill_price=fill_price)
            return dict(rec)

    def ack_order(self, broker_order_id: str, price: Optional[float] = None) -> bool:
        """Async-only: complete a previously SUBMITTED order into a FILLED one.

        For resting STOP_LOSS_MARKET orders the fill is produced only when the
        given price has crossed the order's trigger (LONG-protection SELL SLM:
        price <= trigger; SHORT-protection BUY SLM: price >= trigger).  Returns
        False for unknown orders / triggers not crossed; idempotent for orders
        already filled (a repeated ack never double-fills).
        """
        with self._lock:
            rec = self._orders.get(broker_order_id)
            if rec is None:
                return False
            if rec.get("status") == "filled":
                return True
            if rec.get("status") in ("cancelled", "canceled", "rejected", "expired"):
                return False
            ref = price if price is not None else self._prices.get(rec.get("instrument"))
            if ref is None or ref <= 0:
                return False
            if (rec.get("order_type") or "").upper() in ("STOP_LOSS",
                                                          "STOP_LOSS_MARKET"):
                trig = float(rec.get("trigger_price") or 0.0)
                if trig > 0:
                    crossed = (ref <= trig) if str(rec.get("side") or "").upper() == "SELL" \
                        else (ref >= trig)
                    if not crossed:
                        return False
            broker_fill_id = f"BROKER-FILL-LIVE-{uuid.uuid4()}"
            rec["status"] = "filled"
            rec["filled_quantity"] = rec.get("quantity", 0)
            rec["average_fill_price"] = float(ref)
            rec["price"] = float(ref)
            self._fills[broker_fill_id] = {
                "broker_fill_id": broker_fill_id,
                "broker_order_id": broker_order_id,
                "status": "filled",
                "side": rec.get("side"),
                "quantity": rec.get("quantity"),
                "price": float(ref),
                "timestamp": self._clock(),
                "instrument": rec.get("instrument"),
            }
            return True

    def cancel_order(self, broker_order_id: str) -> dict:
        """Cancel a resting (unfilled) broker order.  MARKET-fills and unknown
        orders answer ok=False — the engine keeps its own CANCELED mark."""
        with self._lock:
            rec = self._orders.get(broker_order_id)
            if rec is None or rec.get("status") == "filled":
                return {"ok": False, "status": "unsupported"}
            rec["status"] = "cancelled"
            rec["average_fill_price"] = 0.0
            return {"ok": True, "status": "cancelled"}

    def modify_order(self, broker_order_id: str, order_type: str = "MARKET",
                     price: Optional[float] = None,
                     trigger_price: Optional[float] = None, quantity: int = 0,
                     validity: str = "DAY") -> dict:
        """Modify a resting (unfilled) order's price.  FILLED/unknown orders
        answer ok=False so the engine keeps its authoritative state."""
        with self._lock:
            rec = self._orders.get(broker_order_id)
            if rec is None or rec.get("status") == "filled":
                return {"ok": False, "status": "unsupported"}
            if price is not None:
                rec["price"] = float(price)
                rec["requested_price"] = float(price)
            if quantity and quantity > 0:
                rec["quantity"] = int(quantity)
            if trigger_price is not None:
                rec["trigger_price"] = float(trigger_price)
            rec["order_type"] = str(order_type or "").upper() or rec.get("order_type")
            return {"ok": True, "status": rec.get("status"), "price": rec.get("price")}

    # ── Emergency exit-all: strictly OUR OWN orders/positions ──────────

    def cancel_all_orders(self) -> list:
        """Cancel every resting (not yet filled) order THIS stub placed."""
        with self._lock:
            pending = [bid for bid, rec in self._orders.items()
                       if str(rec.get("status") or "").lower() == "submitted"]
        results: list[dict] = []
        for bid in pending:
            try:
                results.append(self.cancel_order(bid))
            except Exception as e:
                results.append({"broker_order_id": bid, "ok": False, "error": str(e)})
        return results

    def cancel_instrument_orders(self, instrument: str) -> list:
        """Cancel every resting (unfilled) order THIS stub placed for ONE
        instrument (contract-rollover window)."""
        with self._lock:
            pending = [bid for bid, rec in self._orders.items()
                       if str(rec.get("status") or "").lower() == "submitted"
                       and rec.get("instrument") == instrument]
        results: list[dict] = []
        for bid in pending:
            try:
                results.append(self.cancel_order(bid))
            except Exception as e:
                results.append({"broker_order_id": bid, "ok": False, "error": str(e)})
        return results

    def close_all_positions(self) -> dict:
        """Square off the positions recorded for THIS system (record_position).

        A manual broker-side position this system never recorded is never
        touched: only instruments in the stub's own position map are closed.
        """
        with self._lock:
            net: dict[str, int] = {}
            avg: dict[str, float] = {}
            for (_sid, inst), pos in self._positions.items():
                q = int(pos.get("quantity") or 0)
                side = str(pos.get("side") or "").upper()
                net[inst] = net.get(inst, 0) + (q if side == "LONG" else -q)
                avg[inst] = float(pos.get("average_entry_price") or avg.get(inst, 0.0))
        closed: list[dict] = []
        errors: list[dict] = []
        for inst, n in net.items():
            if n == 0:
                continue
            side = "SELL" if n > 0 else "BUY"
            try:
                if avg.get(inst, 0.0) > 0:
                    self.update_price(inst, avg[inst])
                res = self.place_market_order(
                    side=side, quantity=abs(n), instrument=inst,
                    order_type="MARKET",
                    correlation_id=f"EMER-{uuid.uuid4().hex[:8]}")
                closed.append({
                    "instrument": inst,
                    "side": "LONG" if n > 0 else "SHORT",
                    "quantity": abs(n),
                    "broker_order_id": res.get("broker_order_id"),
                    "status": res.get("status"),
                })
            except Exception as e:
                errors.append({"instrument": inst, "error": str(e)})
        return {"closed": closed, "errors": errors}

    def close_instrument(self, instrument: str) -> dict:
        """Square off the stub's OWN cumulative net position in ONE instrument
        (contract-rollover final-session fallback)."""
        with self._lock:
            net = 0
            avg_price = 0.0
            for (_sid, inst), pos in self._positions.items():
                if inst != instrument:
                    continue
                q = int(pos.get("quantity") or 0)
                side = str(pos.get("side") or "").upper()
                net += q if side == "LONG" else -q
                avg_price = float(pos.get("average_entry_price") or avg_price)
        if net == 0:
            return {"closed": [], "errors": []}
        side = "SELL" if net > 0 else "BUY"
        try:
            if avg_price > 0:
                self.update_price(instrument, avg_price)
            res = self.place_market_order(
                side=side, quantity=abs(net), instrument=instrument,
                order_type="MARKET",
                correlation_id=f"EMER-{uuid.uuid4().hex[:8]}")
            return {"closed": [{
                "instrument": instrument,
                "side": "LONG" if net > 0 else "SHORT",
                "quantity": abs(net),
                "broker_order_id": res.get("broker_order_id"),
                "status": res.get("status"),
            }], "errors": []}
        except Exception as e:
            return {"closed": [], "errors": [{"instrument": instrument, "error": str(e)}]}

    def order_statuses(self) -> dict:
        with self._lock:
            out = {}
            for bid, rec in self._orders.items():
                status = dict(rec)
                status["broker_order_id"] = bid
                status["fills"] = [
                    dict(f) for f in self._fills.values()
                    if f.get("broker_order_id") == bid
                ]
                out[bid] = status
            return out

    def fills(self) -> list:
        with self._lock:
            return [dict(f) for f in self._fills.values()]

    def update_price(self, instrument: str, price: float) -> None:
        with self._lock:
            self._prices[instrument] = float(price)

    def positions(self) -> list[dict]:
        with self._lock:
            out = []
            for (strategy_id, instrument), pos in self._positions.items():
                p = dict(pos)
                p["strategy_id"] = strategy_id
                p["instrument"] = instrument
                out.append(p)
            return out

    def account_status(self) -> dict:
        return {"mode": "LIVE", "equity": 0.0, "available_margin": 0.0}

    # ── stub-only helpers (tests / boot) ──────────────────────────────

    def record_position(self, strategy_id: str, instrument: str, side: str,
                        quantity: int, avg_price: float) -> None:
        with self._lock:
            self._positions[(strategy_id, instrument)] = {
                "side": str(side).upper(), "quantity": int(quantity),
                "average_entry_price": float(avg_price),
            }

    def clear_position(self, strategy_id: str, instrument: str) -> None:
        with self._lock:
            self._positions.pop((strategy_id, instrument), None)