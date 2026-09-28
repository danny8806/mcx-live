"""Paper execution engine for realistic paper trading simulation.

TEST-ONLY module. Imported ONLY by the crash-replay/legacy paper harness and
test fixtures — never by the LIVE runtime (LIVE resolves to
:class:`execution.live.engine.LiveExecutionEngine` over a real broker
transport). The shared :class:`Order` / :class:`OrderState` / :class:`Fill`
data structures now live in ``execution.models``.
"""
from __future__ import annotations

import math
import time
import threading
import uuid
from typing import Callable, Optional

from execution.models import Fill, Order, OrderState
from strategies.types import Signal, SignalType, resolve_order_role


class PaperExecutionEngine:
    """Simulates realistic paper trading execution.

    Handles:
    - Order creation and validation
    - Slippage modeling
    - Latency simulation
    - Partial fills
    - Fill generation
    - Fee calculation
    """

    mode = "PAPER"

    def __init__(
        self,
        slippage_ticks: int = 1,
        latency_ms: float = 100.0,
        partial_fill_probability: float = 0.0,
        clock: Optional[Callable[[], float]] = None,
    ):
        self.slippage_ticks = slippage_ticks
        self.latency_ms = latency_ms
        # Optional replay clock: when set, order/fill timestamps come from it
        # instead of wall-clock time.  Production leaves this None so every
        # timestamp is time.time() exactly as before; the replay drivers set it
        # to the candle's end_ts so persisted history carries the day's times.
        self._clock: Optional[Callable[[], float]] = clock
        if partial_fill_probability != 0:
            raise ValueError(
                "Partial fills are not supported by the position ledger; "
                "set partial_fill_probability to 0 until partial-close accounting exists."
            )
        self.partial_fill_probability = partial_fill_probability

        self._orders: dict[str, Order] = {}
        self._fills: list[Fill] = []
        self._current_prices: dict[str, float] = {}
        self._price_lock = threading.Lock()
        self._max_fills = 500  # Keep only last 500 fills in memory
        # §40 — optional BrokerEventRouter: every created order registers its
        # explicit broker_order_id -> strategy/trade mapping here.
        self.broker_router = None

    def _now(self) -> float:
        return self._clock() if self._clock is not None else time.time()

    def update_price(self, instrument: str, price: float) -> None:
        """Update current market price for an instrument."""
        with self._price_lock:
            self._current_prices[instrument] = price

    def create_order(
        self,
        signal: Signal,
        multiplier: float = 1.0,
        trade_id: str = "",
        side: Optional[str] = None,
    ) -> Order:
        """Create a new order from a signal."""
        if not trade_id:
            raise ValueError("trade_id is required to create an order")
        # Determine order side: REVERSAL signals carry explicit side ("LONG"/"SHORT")
        if side is not None:
            order_side = side.upper()
        elif signal.signal_type == SignalType.REVERSAL and signal.side:
            order_side = "BUY" if signal.side == "LONG" else "SELL"
        elif signal.signal_type in (SignalType.LONG, SignalType.REVERSAL):
            order_side = "BUY"
        else:
            order_side = "SELL"
        order = Order(
            order_id=str(uuid.uuid4()),
            strategy_id=signal.strategy_id,
            instrument=signal.instrument,
            side=order_side,
            quantity=signal.quantity,
            state=OrderState.CREATED,
            multiplier=multiplier,
            created_at=self._now(),
            updated_at=self._now(),
            entry_signal_id=signal.signal_id,
            trade_id=trade_id,
            order_role=resolve_order_role(signal),
        )
        self._orders[order.order_id] = order
        if self.broker_router is not None:
            self.broker_router.register_from_order(order)
        return order

    def submit_order(self, order: Order) -> Order:
        """Submit order for execution."""
        if order.state != OrderState.CREATED:
            raise ValueError(f"Cannot submit order in state {order.state}")

        order.state = OrderState.SUBMITTED
        order.updated_at = self._now()

        # Simulate execution
        fill = self._execute_order(order)
        if fill:
            order.state = OrderState.FILLED
            order.filled_quantity = order.quantity
            order.average_fill_price = fill.price
            order.fill_ids.append(fill.fill_id)
        else:
            order.state = OrderState.REJECTED
            order.reason = "No market data available"

        return order

    def _execute_order(self, order: Order) -> Optional[Fill]:
        """Execute order with slippage and latency simulation."""
        if self.latency_ms > 0:
            time.sleep(self.latency_ms / 1000.0)
        with self._price_lock:
            current_price = self._current_prices.get(order.instrument)
        if current_price is None or current_price <= 0.0:
            return None

        # Apply slippage
        slippage = self.slippage_ticks * 1.0  # tick_size = 1.0 for MCX
        if order.side == "BUY":
            fill_price = current_price + slippage
        else:
            fill_price = current_price - slippage
        if fill_price is None or fill_price <= 0.0 or math.isnan(fill_price) or math.isinf(fill_price):
            return None

        # Create fill
        fill = Fill(
            fill_id=str(uuid.uuid4()),
            order_id=order.order_id,
            instrument=order.instrument,
            side=order.side,
            quantity=order.quantity,
            price=fill_price,
            timestamp=self._now(),
            strategy_id=order.strategy_id,
            multiplier=order.multiplier,
            entry_signal_id=order.entry_signal_id,
            trade_id=order.trade_id,
        )
        self._fills.append(fill)
        # Prune old fills to prevent unbounded growth
        if len(self._fills) > self._max_fills:
            self._fills = self._fills[-self._max_fills:]
        # Prune old completed orders
        if len(self._orders) > self._max_fills:
            stale_ids = [oid for oid, o in self._orders.items()
                         if o.state in (OrderState.FILLED, OrderState.REJECTED, OrderState.CANCELED)]
            for oid in stale_ids[:len(stale_ids)//2]:
                del self._orders[oid]
        return fill

    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID."""
        return self._orders.get(order_id)

    def get_fills(
        self,
        strategy_id: Optional[str] = None,
        instrument: Optional[str] = None,
    ) -> list[Fill]:
        """Get fills with optional filtering."""
        fills = self._fills
        if strategy_id:
            fills = [f for f in fills if f.strategy_id == strategy_id]
        if instrument:
            fills = [f for f in fills if f.instrument == instrument]
        return fills

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        order = self._orders.get(order_id)
        if order and order.state in (OrderState.CREATED, OrderState.SUBMITTED):
            order.state = OrderState.CANCELED
            order.updated_at = self._now()
            return True
        return False

    def snapshot(self) -> dict:
        """Get execution state for persistence."""
        return {
            "orders_count": len(self._orders),
            "fills_count": len(self._fills),
            "current_prices": dict(self._current_prices),
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
        """Restore execution state from persistence."""
        if not data:
            return
        # Restore current prices
        restored_prices = data.get("current_prices", {})
        # Drop non-positive / non-finite prices (e.g. a stale `-1` sentinel)
        # so a poisoned price can't survive restart and be used for fills.
        self._current_prices = {
            k: v for k, v in restored_prices.items()
            if v is not None and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))) and v > 0.0
        }
        # Clear before restoring to avoid duplicates
        self._fills.clear()
        self._orders.clear()
        # Restore fills
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
            )
            self._fills.append(fill)
        # Restore orders
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
            self._orders[order.order_id] = order
