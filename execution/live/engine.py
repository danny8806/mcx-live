"""LIVE execution engine — broker-authoritative, interface-compatible with the
paper engine.

:class:`LiveExecutionEngine` exposes the same surface
(``create_order`` / ``submit_order`` / ``update_price`` / ``get_order`` /
``get_fills`` / ``cancel_order`` / ``snapshot`` / ``restore``) so the engine
and runtime layer can treat transports identically.  The difference is where
fills come from: a simulation engine synthesizes fills from its own price
feed, the live engine hands every order to a :class:`LiveBrokerClient` and
derives fills from the broker's result.

Live fills/orders carry ``LIVE-`` id prefixes so a live record is never
confusable with simulated ones even before it hits the (separate) live DB.
"""
from __future__ import annotations

import math
import threading
import time
import uuid
from typing import Optional

from execution.broker_router import BrokerEventRouter
from execution.live.broker_client import BrokerGateClosed, LiveBrokerClient
from execution.models import Fill, Order, OrderState
from execution.price_model import (
    ExecutionPricePlan, PricePreset,
    calculate_long_sl_price, calculate_short_sl_price,
)
from strategies.types import Signal, SignalType, resolve_order_role


def _broker_state_reason(prefix: str, status: dict) -> str:
    """Build the engine reason string for a broker terminal-state transition,
    keeping the broker's own message (rejection detail such as an
    "insufficient funds" RMS notice) when the transport reported it."""
    base = f"{prefix}: {status.get('raw_status', '')}"
    detail = status.get("reason") or ""
    return f"{base} ({detail})" if detail else base


class LiveExecutionEngine:
    """Execute live orders through the configured broker client."""

    mode = "LIVE"

    def __init__(
        self,
        broker: LiveBrokerClient,
        clock: Optional[callable] = None,
        price_preset: Optional[PricePreset] = None,
    ):
        self.broker = broker
        self._clock = clock or time.time
        self._orders: dict[str, Order] = {}
        self._fills: list[Fill] = []
        self._current_prices: dict[str, float] = {}
        self._price_lock = threading.Lock()
        self._lock = threading.RLock()
        self._max_fills = 500
        # broker_fill_id -> engine fill_id (idempotency / reconcile map)
        self._broker_fill_map: dict[str, str] = {}
        self.broker_router: Optional[BrokerEventRouter] = None
        # One LIVE execution plan is always active. No preset means zero
        # offsets on the standard instrument tick grid, never a MARKET-mode
        # fallback for ordinary entries.
        self._price_preset = price_preset or PricePreset()
        self._plans: dict[str, ExecutionPricePlan] = {}
        # §9.5 — price plans are IMMUTABLE PER SIGNAL: the plan computed for a
        # signal_id is stored once and every recompute must equal it exactly.
        self._plan_by_signal: dict[str, ExecutionPricePlan] = {}
        # Tick grid used to derive the mandatory minimum separation between the
        # LIMIT and TRIGGER legs of STOP_LOSS orders (Dhan DH-906); defaults to
        # 1.0 and is refined by the PricePreset when one is configured.
        self._tick_size = float(self._price_preset.tick_size)
        # Installed by TradingEngine. This is the single LIVE submission
        # boundary used by normal orders, recovery orders, and protection.
        self.submission_guard = None

    def _now(self) -> float:
        return self._clock()

    def update_price(self, instrument: str, price: float) -> None:
        with self._price_lock:
            self._current_prices[instrument] = price
        try:
            self.broker.update_price(instrument, price)
        except Exception:
            pass

    def create_order(
        self,
        signal: Signal,
        multiplier: float = 1.0,
        trade_id: str = "",
        side: Optional[str] = None,
    ) -> Order:
        if not trade_id:
            raise ValueError("trade_id is required to create an order")
        if side is not None:
            order_side = side.upper()
        elif signal.signal_type == SignalType.REVERSAL and signal.side:
            order_side = "BUY" if signal.side == "LONG" else "SELL"
        elif signal.signal_type in (SignalType.LONG, SignalType.REVERSAL):
            order_side = "BUY"
        else:
            order_side = "SELL"
        order = Order(
            order_id=f"LIVE-{uuid.uuid4()}",
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=order_side,
            quantity=signal.quantity,
            state=OrderState.CREATED,
            multiplier=multiplier,
            created_at=self._now(),
            updated_at=self._now(),
            entry_signal_id=signal.signal_id,
            parent_signal_id=signal.signal_id,
            parent_position_id=getattr(signal, "parent_position_id", None),
            position_id=getattr(signal, "parent_position_id", None),
            lifecycle_id=getattr(signal, "lifecycle_id", None) or trade_id,
            position_generation=getattr(signal, "position_generation", None),
            trade_id=trade_id,
            order_role=resolve_order_role(signal),
        )
        if (signal.metadata or {}).get("reversal_parent_signal_id"):
            order.reversal_parent_signal_id = signal.metadata["reversal_parent_signal_id"]
        # §9.4 — the correlation id is minted ONCE at order creation so the
        # signal -> order -> broker -> status -> fill carrier is stable and the
        # same id ends up in WS/REST records and the persisted order row.
        order.correlation_id = f"MCX-{uuid.uuid4().hex[:12]}"
        # LIVE uses one immediate-limit entry plan plus system-triggered exits.
        # PAPER is untouched: only this LIVE engine consults the price preset.
        if self._price_preset is not None:
            plan = self._price_preset.plan_for(signal, order_side)
            # §9.5 — immutability: the first plan for a signal_id is canonical;
            # any later recompute for the SAME signal MUST be byte-equal.
            # C3 — the plan is keyed by (signal_id, role), NOT signal_id alone:
            # a reversal intentionally reuses ONE signal_id for the old trade's
            # exit leg and the new opposite-side entry leg, and those two legs
            # legitimately carry different plans (system exit vs.
            # immediate-limit entry). Immutability is preserved per-leg.
            plan_key = (signal.signal_id, order.order_role)
            prior = self._plan_by_signal.get(plan_key)
            if prior is not None and prior != plan:
                raise RuntimeError(
                    f"immutable price plan for signal {signal.signal_id} "
                    f"role {order.order_role} violated: {prior!r} != {plan!r}")
            self._plan_by_signal[plan_key] = plan
            order.order_type = plan.order_type
            order.price = plan.price
            order.trigger_price = plan.trigger_price
            order.requested_price = plan.price
            order.planned_order_type = plan.order_type
            order.planned_entry_price = plan.price
            if plan.order_type == "STOP_LOSS_MARKET":
                order.planned_sl = plan.trigger_price
            elif plan.kind == "long_entry":
                order.planned_sl = calculate_long_sl_price(
                    signal.stop_price, self._price_preset.sl_offset)
            elif plan.kind == "short_entry":
                order.planned_sl = calculate_short_sl_price(
                    signal.stop_price, self._price_preset.sl_offset)
            self._plans[order.order_id] = plan
        self._orders[order.order_id] = order
        if self.broker_router is not None:
            self.broker_router.register_from_order(order)
        return order

    def price_plan(self, order_id: str) -> Optional[ExecutionPricePlan]:
        """The execution plan resolved for a created order (Phase 3 forensics)."""
        return self._plans.get(order_id)

    def create_protective_sl(self, *, strategy_id: str, instrument: str,
                             side: str, quantity: int, trigger_price: float,
                             trade_id: str, entry_order_id: str,
                             limit_price: Optional[float] = None,
                             position_id: Optional[str] = None,
                             position_generation: Optional[int] = None,
                             signal_id: Optional[str] = None) -> Order:
        """Create (not submit) a broker-side protective STOP_LOSS order.

        Spec §22-24: a resting stop-limit protects an OPEN live position and is
        placed as soon as the entry fill opens the position, carrying the
        explicit protected_order_id lineage and order_role=STOP_LOSS.  The
        order is NOT filled on placement — it rests at the broker until the
        trigger fires (verified live on Dhan MCX 2026-09-16: STOP_LOSS carries
        BOTH a limit price and a trigger; a BUY requires limit > trigger and a
        SELL requires limit < trigger).
        """
        tick = self._tick_size
        if limit_price is None:
            limit_price, trigger_price = _stop_limit_legs(
                trigger=float(trigger_price), side=str(side).upper(),
                tick_size=tick, direction="same")
        # SAFETY: enforce Dhan DH-906 invariant regardless of how the caller
        # computed limit/trigger.  BUY STOP_LOSS requires limit > trigger;
        # SELL STOP_LOSS requires limit < trigger.  If violated, nudge the
        # limit by one tick in the correct direction.
        side_u = str(side).upper()
        if side_u == "BUY" and float(limit_price) <= float(trigger_price):
            limit_price = float(trigger_price) + tick
        elif side_u == "SELL" and float(limit_price) >= float(trigger_price):
            limit_price = float(trigger_price) - tick
        order = Order(
            order_id=f"LIVE-{uuid.uuid4()}",
            strategy_id=strategy_id,
            instrument=instrument,
            side=side,
            quantity=quantity,
            order_type="STOP_LOSS",
            price=float(limit_price),
            trigger_price=float(trigger_price),
            correlation_id=f"MCX-{uuid.uuid4().hex[:12]}",
            order_role="STOP_LOSS",
            lifecycle_id=trade_id,
            parent_signal_id=signal_id or entry_order_id,
            parent_position_id=position_id,
            position_generation=position_generation,
            protected_order_id=entry_order_id,
            trade_id=trade_id,
            state=OrderState.CREATED,
            created_at=self._now(),
            updated_at=self._now(),
        )
        self._orders[order.order_id] = order
        if self.broker_router is not None:
            self.broker_router.register_from_order(order)
        return order

    def submit_order(self, order: Order) -> Order:
        if order.state != OrderState.CREATED:
            raise ValueError(f"Cannot submit order in state {order.state}")

        order.lifecycle_id = order.lifecycle_id or order.trade_id
        order.parent_signal_id = order.parent_signal_id or order.entry_signal_id
        order.parent_position_id = (order.parent_position_id or
                                    getattr(order, "position_id", None))
        order.position_id = order.position_id or order.parent_position_id
        role = str(order.order_role or "").upper()
        if role not in {"ENTRY", "EXIT", "STOP_LOSS", "REVERSAL_EXIT",
                        "REVERSAL_ENTRY", "FALLBACK_MARKET", "EMERGENCY_EXIT"}:
            order.state = OrderState.REJECTED
            order.reason = "ORDER_ROLE_INVALID"
            order.updated_at = self._now()
            return order
        if not order.trade_id or not order.lifecycle_id or not order.parent_signal_id:
            order.state = OrderState.REJECTED
            order.reason = "ORDER_MISSING_LIFECYCLE_OWNERSHIP"
            order.updated_at = self._now()
            return order
        if self.submission_guard is not None:
            try:
                reason = self.submission_guard(order)
            except Exception as exc:
                reason = f"submission_guard_error:{exc}"
            if reason:
                order.state = OrderState.REJECTED
                order.reason = str(reason)
                order.updated_at = self._now()
                return order

        # Execution-key idempotency covers callers that bypass OrderManager.
        key = (order.lifecycle_id, order.parent_position_id,
               order.parent_signal_id, role, order.position_generation)
        with self._lock:
            for prior in self._orders.values():
                prior_key = (getattr(prior, "lifecycle_id", None) or prior.trade_id,
                             getattr(prior, "parent_position_id", None),
                             getattr(prior, "parent_signal_id", None) or prior.entry_signal_id,
                             str(prior.order_role or "").upper(),
                             getattr(prior, "position_generation", None))
                if prior is not order and prior_key == key and prior.state in (
                        OrderState.SUBMITTED, OrderState.ACKNOWLEDGED,
                        OrderState.PARTIALLY_FILLED, OrderState.FILLED):
                    order.state = OrderState.REJECTED
                    order.reason = f"DUPLICATE_EXECUTION_KEY:{prior.order_id}"
                    order.updated_at = self._now()
                    return order

        order.state = OrderState.SUBMITTED
        order.updated_at = self._now()
        # §9.4 — record the REQUESTED price/type on the book before placement;
        # the placement response is never trusted as the fill price (the fill
        # always flows from the broker's own order records).
        if order.requested_price is None:
            order.requested_price = order.price

        try:
            result = self.broker.place_market_order(
                side=order.side, quantity=order.quantity, instrument=order.instrument,
                order_type=order.order_type, price=order.price,
                trigger_price=getattr(order, "trigger_price", None),
                correlation_id=order.correlation_id,
            )
        except BrokerGateClosed as e:
            order.state = OrderState.REJECTED
            order.reason = str(e)
            order.updated_at = self._now()
            return order
        except Exception as e:
            # Broker-side placement rejection (DH-905/DH-906 input errors, RMS
            # funds) and transport failures land here.  Preserve the real
            # message so the book/UI show the actual reason; the generic
            # sentinel is only used when the exception carries no usable detail.
            order.state = OrderState.REJECTED
            order.reason = str(e) or "LIVE_BROKER_UNAVAILABLE"
            order.updated_at = self._now()
            return order

        if not result:
            order.state = OrderState.REJECTED
            order.reason = "LIVE_ORDER_NO_STATUS"
            order.updated_at = self._now()
            return order

        # §40 — the REAL broker order id (BROKER-*) must resolve back to this
        # order. register_from_order() keys the mapping by the internal
        # LIVE- id; register the broker-native id too so late broker events
        # (status polls / fills) route through the explicit mapping.
        broker_order_id = result.get("broker_order_id")
        if broker_order_id and self.broker_router is not None:
            try:
                self.broker_router.register_from_kwargs(
                    broker_order_id=broker_order_id,
                    order_id=order.order_id,
                    trade_id=getattr(order, "trade_id", None) or "",
                    strategy_id=order.strategy_id,
                    instrument=order.instrument,
                )
            except Exception:
                pass

        status = str(result.get("status") or "").lower()
        # Terminal broker statuses returned by the transport are applied now
        # (not on the next poll).  This covers the adopted-order resume path
        # where the SAME broker order id is already known settled, and any
        # transport that returns an already-final status at placement time.
        if status == "rejected":
            order.state = OrderState.REJECTED
            order.reason = result.get("reason") or _broker_state_reason(
                "BROKER_REJECTED", result)
            order.updated_at = self._now()
            try:
                order._broker_order_id = broker_order_id
            except Exception:
                pass
            return order
        if status in ("cancelled", "canceled", "expired"):
            order.state = OrderState.CANCELED
            order.reason = result.get("reason") or _broker_state_reason(
                "BROKER_CANCELLED", result)
            order.updated_at = self._now()
            try:
                order._broker_order_id = broker_order_id
            except Exception:
                pass
            return order
        if status not in ("filled", "complete", "completed", "executed"):
            # Async broker: the exchange accepted the order; the fill arrives
            # later.  The order stays SUBMITTED and the Phase-2 broker poller
            # (LiveBrokerPoller) reconciles the broker status -> fills.
            order.state = OrderState.SUBMITTED
            order.filled_quantity = 0
            order.updated_at = self._now()
            try:
                order._broker_order_id = broker_order_id
            except Exception:
                pass
            return order

        if result.get("quantity") is None or result.get("price") is None:
            order.state = OrderState.REJECTED
            order.reason = "LIVE_ORDER_INVALID_FILL"
            order.updated_at = self._now()
            return order

        # §9.6 — the broker order id belongs to the order regardless of how
        # quickly the fill arrives; keep it on the book so the durable pending
        # ENTRY_SENT tie-back and reconciliation always see it.
        try:
            order._broker_order_id = broker_order_id
        except Exception:
            pass

        fill = Fill(
            fill_id=f"LIVE-{uuid.uuid4()}",
            order_id=order.order_id,
            instrument=order.instrument,
            side=order.side,
            quantity=int(result["quantity"]),
            price=float(result["price"]),
            timestamp=float(result.get("timestamp", self._now())),
            strategy_id=order.strategy_id,
            multiplier=order.multiplier,
            entry_signal_id=order.entry_signal_id,
            trade_id=order.trade_id,
            lifecycle_id=order.lifecycle_id,
            position_id=order.parent_position_id,
            position_generation=order.position_generation,
        )
        fill.broker_order_id = broker_order_id
        fill.broker_fill_id = result.get("broker_fill_id")
        fill.broker_trade_id = result.get("broker_trade_id") or ""
        # §9.7 — the broker's cumulative traded quantity is authoritative: even
        # on an instantaneous fill the delta equals the full quantity.
        fill.cumulative_filled_quantity = int(result.get("quantity") or order.quantity)
        order.state = OrderState.FILLED
        order.filled_quantity = order.quantity
        order.average_fill_price = fill.price
        order.fill_ids.append(fill.fill_id)
        self._fills.append(fill)
        bfid = result.get("broker_fill_id")
        if bfid:
            self._broker_fill_map[bfid] = fill.fill_id
        if len(self._fills) > self._max_fills:
            self._fills = self._fills[-self._max_fills:]
        if len(self._orders) > self._max_fills:
            stale_ids = [oid for oid, o in self._orders.items()
                         if o.state in (OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELED)]
            for oid in stale_ids[:len(stale_ids) // 2]:
                del self._orders[oid]
        return order

    def get_order(self, order_id: str) -> Optional[Order]:
        return self._orders.get(order_id)

    def pending_orders(self) -> list[Order]:
        """Orders in flight (submitted to an async broker, not yet filled)."""
        return [o for o in self._orders.values()
                if o.state in (OrderState.CREATED, OrderState.SUBMITTED)]

    def apply_broker_statuses(self, statuses: dict) -> list[Fill]:
        """Reconcile the broker's order statuses into engine fills.

        Consumed by the Phase-2 :class:`LiveBrokerPoller`.  Each broker order id
        is resolved through the explicit broker router mapping; every broker
        fill is applied exactly once (dedup via ``broker_fill_id``), turning the
        owning order FILLED and producing an engine :class:`Fill` ready to route
        into the strategy lifecycle.  Returns the list of NEW engine fills.
        """
        new_fills: list[Fill] = []
        # C1 — the whole reconcile runs under one RLock so the broker-fill
        # dedup check-and-insert is atomic against the WS thread's
        # verify_order/apply_broker_statuses and the poller thread.  Two
        # interleaved callers previously could both mint the same broker fill.
        with self._lock:
            for broker_order_id, status in (statuses or {}).items():
                if broker_order_id is None:
                    continue
                mapping = (self.broker_router.resolve(broker_order_id)
                           if self.broker_router is not None else None)
                if mapping is None:
                    # Unmappable broker event — never guessed (§39). The router
                    # itself quarantines routed events; here the status is simply
                    # not acted upon.
                    continue
                order = self._orders.get(mapping.order_id)
                if order is None or order.state == OrderState.FILLED:
                    continue
                st = str(status.get("status") or "").lower()
                if st not in ("filled", "complete", "completed", "executed"):
                    # Phase 6 — propagate broker REJECTED/CANCELLED into the
                    # engine order state so the poller can persist it and the
                    # strategy lifecycle can react (FLAT on rejection, etc.).
                    if st == "rejected" and order.state not in (
                            OrderState.REJECTED, OrderState.CANCELED):
                        order.state = OrderState.REJECTED
                        order.reason = _broker_state_reason(
                            "BROKER_REJECTED", status)
                        order.updated_at = self._now()
                    elif st in ("cancelled", "canceled") and order.state not in (
                            OrderState.REJECTED, OrderState.CANCELED):
                        order.state = OrderState.CANCELED
                        order.reason = _broker_state_reason(
                            "BROKER_CANCELLED", status)
                        order.updated_at = self._now()
                    elif st == "expired" and order.state not in (
                            OrderState.REJECTED, OrderState.CANCELED):
                        # Phase 9.6 — broker EXPIRED is a terminal state like
                        # CANCELLED; the raw status is kept in the reason string so
                        # the book never loses it.
                        order.state = OrderState.CANCELED
                        order.reason = _broker_state_reason(
                            "BROKER_EXPIRED", status)
                        order.updated_at = self._now()
                    continue
                carry = getattr(order, "filled_quantity", 0) or 0
                for f in status.get("fills", []) or []:
                    bfid = f.get("broker_fill_id") or f.get("fill_id")
                    if not bfid or bfid in self._broker_fill_map:
                        continue
                    qty = int(f.get("quantity") or order.quantity)
                    cum = carry + qty
                    carry = cum
                    fill = Fill(
                        fill_id=f"LIVE-{uuid.uuid4()}",
                        order_id=order.order_id,
                        instrument=order.instrument,
                        side=str(f.get("side") or order.side).upper(),
                        quantity=qty,
                        price=float(f.get("price") or 0.0),
                        timestamp=float(f.get("timestamp") or self._now()),
                        strategy_id=order.strategy_id,
                        multiplier=order.multiplier,
                        entry_signal_id=order.entry_signal_id,
                        trade_id=order.trade_id,
                        lifecycle_id=order.lifecycle_id,
                        position_id=order.parent_position_id,
                        position_generation=order.position_generation,
                    )
                    fill.broker_order_id = broker_order_id
                    fill.broker_fill_id = bfid
                    # §9.7 — keep the broker's execution identity and the broker-
                    # cumulative quantity at the moment of this delta so the
                    # reconciliation ledger proves local cumulative == broker.
                    fill.broker_trade_id = f.get("broker_trade_id") or ""
                    fill.cumulative_filled_quantity = cum
                    self._broker_fill_map[bfid] = fill.fill_id
                    self._fills.append(fill)
                    order.fill_ids.append(fill.fill_id)
                    order.filled_quantity = cum
                    # C1 — volume-weighted average across legs, not the last
                    # leg's price.
                    prev_qty = cum - qty
                    prev_avg = float(getattr(order, "average_fill_price", 0.0) or 0.0)
                    order.average_fill_price = (
                        (prev_avg * prev_qty + float(fill.price) * qty) / cum
                        if cum > 0 else float(fill.price))
                    order.state = (OrderState.FILLED if cum >= order.quantity
                                   else OrderState.PARTIALLY_FILLED)
                    order.updated_at = self._now()
                    new_fills.append(fill)
            if len(self._fills) > self._max_fills:
                self._fills = self._fills[-self._max_fills:]
        return new_fills

    def get_fills(self, strategy_id: Optional[str] = None,
                  instrument: Optional[str] = None) -> list[Fill]:
        fills = self._fills
        if strategy_id:
            fills = [f for f in fills if f.strategy_id == strategy_id]
        if instrument:
            fills = [f for f in fills if f.instrument == instrument]
        return fills

    def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        # PARTIALLY_FILLED is cancellable: cancelling stops the REMAINING
        # quantity still working at the broker (partial-remainder completion)
        # while the already-filled portion stays booked to the position.
        if not order or order.state not in (OrderState.CREATED, OrderState.SUBMITTED,
                                            OrderState.PARTIALLY_FILLED):
            return False
        # Phase 5 — route a live cancel to the broker when the order already
        # has a real broker order id and the broker supports IRREVOCABLE
        # cancels (Dhan DELETE /orders/{id}).  A broker failure leaves the
        # order SUBMITTED and propagates the reason; the next cancel attempt
        # re-routes.  Non-Dhan transports answer ok=False/unsupported and the
        # engine proceeds with the internal CANCELED mark as before.
        broker_order_id = getattr(order, "_broker_order_id", None)
        if broker_order_id and self.broker is not None:
            try:
                outcome = self.broker.cancel_order(broker_order_id)
            except Exception as exc:
                order.reason = f"BROKER_CANCEL_FAILED: {exc}"
                order.updated_at = self._now()
                return False
            if not outcome.get("ok") and outcome.get("status") not in (None, "unsupported"):
                order.reason = (
                    f"BROKER_CANCEL_REJECTED: {outcome.get('status')} "
                    f"{outcome.get('raw_status', '')}".strip())
                order.updated_at = self._now()
                return False
        order.state = OrderState.CANCELED
        order.updated_at = self._now()
        return True

    def snapshot(self) -> dict:
        return {
            "mode": "LIVE",
            "orders_count": len(self._orders),
            "fills_count": len(self._fills),
            "current_prices": dict(self._current_prices),
            "broker_gate_enabled": bool(getattr(self.broker, "gate_enabled", False)),
            "orders": [
                {
                    "order_id": o.order_id,
                    "strategy_id": o.strategy_id,
                    "instrument": o.instrument,
                    "side": o.side,
                    "quantity": o.quantity,
                    "order_type": o.order_type,
                    "multiplier": o.multiplier,
                    "state": o.state.value,
                    "filled_quantity": o.filled_quantity,
                    "average_fill_price": o.average_fill_price,
                    "fill_ids": list(o.fill_ids),
                    "created_at": o.created_at,
                    "updated_at": o.updated_at,
                    "reason": o.reason,
                    "entry_signal_id": o.entry_signal_id,
                    "trade_id": o.trade_id,
                    "trigger_price": o.trigger_price,
                    "correlation_id": o.correlation_id,
                    "requested_price": o.requested_price,
                    "planned_entry_price": o.planned_entry_price,
                    "planned_sl": o.planned_sl,
                    "planned_order_type": o.planned_order_type,
                    "order_role": o.order_role,
                    "_broker_order_id": getattr(o, "_broker_order_id", None),
                }
                for o in self._orders.values()
            ],
            "fills": [
                {
                    "fill_id": f.fill_id,
                    "order_id": f.order_id,
                    "strategy_id": f.strategy_id,
                    "instrument": f.instrument,
                    "side": f.side,
                    "price": f.price,
                    "quantity": f.quantity,
                    "multiplier": f.multiplier,
                    "timestamp": f.timestamp,
                    "gross_value": f.gross_value,
                    "entry_signal_id": f.entry_signal_id,
                    "trade_id": f.trade_id,
                }
                for f in self._fills
            ],
        }

    def restore(self, data: dict) -> None:
        if not data:
            return
        restored_prices = data.get("current_prices", {})
        self._current_prices = {
            k: v for k, v in restored_prices.items()
            if v is not None and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) and v > 0.0
        }
        for inst, price in self._current_prices.items():
            try:
                self.broker.update_price(inst, price)
            except Exception:
                pass
        self._fills.clear()
        self._orders.clear()
        for f_data in data.get("fills", []):
            fill = Fill(
                fill_id=f_data["fill_id"],
                order_id=f_data["order_id"],
                strategy_id=f_data.get("strategy_id", ""),
                instrument=f_data["instrument"],
                side=f_data["side"],
                price=f_data["price"],
                quantity=f_data["quantity"],
                multiplier=f_data.get("multiplier", 1),
                timestamp=f_data["timestamp"],
                entry_signal_id=f_data.get("entry_signal_id"),
                trade_id=f_data.get("trade_id"),
                lifecycle_id=f_data.get("lifecycle_id"),
                position_id=f_data.get("position_id"),
                position_generation=f_data.get("position_generation"),
            )
            self._fills.append(fill)
        for o_data in data.get("orders", []):
            order = Order(
                order_id=o_data["order_id"],
                strategy_id=o_data["strategy_id"],
                instrument=o_data["instrument"],
                side=o_data["side"],
                quantity=o_data["quantity"],
                order_type=o_data.get("order_type", "MARKET"),
                multiplier=o_data.get("multiplier", 1),
                filled_quantity=o_data.get("filled_quantity", 0),
                average_fill_price=o_data.get("average_fill_price", 0.0),
                fill_ids=list(o_data.get("fill_ids", [])),
                reason=o_data.get("reason"),
                entry_signal_id=o_data.get("entry_signal_id"),
                trade_id=o_data.get("trade_id"),
            )
            order.state = OrderState(o_data["state"])
            order.created_at = o_data.get("created_at", 0)
            order.updated_at = o_data.get("updated_at", 0)
            order.trigger_price = o_data.get("trigger_price")
            order.correlation_id = o_data.get("correlation_id")
            order.requested_price = o_data.get("requested_price")
            order.planned_entry_price = o_data.get("planned_entry_price")
            order.planned_sl = o_data.get("planned_sl")
            order.planned_order_type = o_data.get("planned_order_type")
            order.order_role = o_data.get("order_role")
            if o_data.get("_broker_order_id"):
                order._broker_order_id = o_data["_broker_order_id"]
            self._orders[order.order_id] = order
