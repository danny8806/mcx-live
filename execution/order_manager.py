"""Order manager for centralized order handling."""
from __future__ import annotations

import threading
from typing import Any, Optional

from execution.models import Order, OrderState, Fill


class OrderManager:
    """Centralized order manager.
    
    Responsibilities:
    - Receive order intents from strategies
    - Validate orders
    - Assign client order IDs
    - Track order state
    - Process fills
    - Prevent duplicate orders
    """

    def __init__(
        self,
        execution_engine: Any,
    ):
        self.execution_engine = execution_engine

        self._pending_signals: dict[str, Any] = {}
        self._active_orders: dict[str, Order] = {}
        self._fills_to_notify: list[Fill] = []
        self._lock = threading.Lock()

    def submit_signal(
        self,
        signal: Any,
        multiplier: float = 1.0,
        trade_id: str = "",
        side: Optional[str] = None,
    ) -> Optional[Order]:
        """Submit a trading signal for execution.
        
        Args:
            signal: Strategy signal (must have strategy_id, instrument, timestamp attributes)
            multiplier: Contract multiplier for the instrument
            
        Returns:
            Order if created, None if rejected.
            Fills produced by the order are collected and MUST be drained (and
            the order persisted BEFORE them) by the caller via drain_fills().
        """
        if not trade_id:
            raise ValueError("trade_id is required to submit a signal")
        collect = []
        with self._lock:
            # Check for duplicate signals
            key = f"{signal.strategy_id}:{signal.instrument}:{signal.timestamp}"
            if key in self._pending_signals:
                return None
            self._pending_signals[key] = signal

            # Cleanup old pending signals (> 1 hour old) to prevent memory leak
            import time
            now = time.time()
            stale_keys = [k for k, v in self._pending_signals.items()
                          if hasattr(v, 'timestamp') and now - v.timestamp > 3600]
            for k in stale_keys:
                del self._pending_signals[k]

            # Create order
            order = self.execution_engine.create_order(
                signal, multiplier=multiplier, trade_id=trade_id, side=side
            )
            self._active_orders[order.order_id] = order

            # Execute
            order = self.execution_engine.submit_order(order)

            # Process fill if successful
            if order.state == OrderState.FILLED and order.fill_ids:
                for fill_id in order.fill_ids:
                    fills = self.execution_engine.get_fills(strategy_id=signal.strategy_id)
                    for fill in fills:
                        if fill.fill_id == fill_id:
                            collect.append(fill)
                            break
            elif order.state == OrderState.REJECTED:
                # Order rejected — clean up memory
                self._pending_signals.pop(key, None)
                self._active_orders.pop(order.order_id, None)
                return None

            # Prune entries for terminal orders (FILLED / CANCELED) to
            # avoid unbounded memory growth over a long session.
            if order.state in (OrderState.FILLED, OrderState.CANCELED):
                self._pending_signals.pop(key, None)
                self._active_orders.pop(order.order_id, None)

            # Append (never overwrite): a caller that submits two signals
            # before draining must not lose the first signal's fills.
            self._fills_to_notify.extend(collect)

        return order

    def drain_fills(self) -> list[Fill]:
        """Return and clear the fills produced by the last submitted signal.

        The caller is responsible for persisting the order row BEFORE dispatching
        these fills, so the DB invariant "every fill references an existing
        order" is never violated (even on a crash between the two).
        """
        with self._lock:
            fills = self._fills_to_notify
            self._fills_to_notify = []
            return fills

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        with self._lock:
            return self.execution_engine.cancel_order(order_id)

    def remove_pending(self, signal) -> None:
        """Remove a signal from the pending-dedup set.

        Called by the engine after a reversal EXIT order has been submitted
        and its fills drained, so the subsequent REVERSAL_ENTRY (which shares
        the same strategy_id:instrument:timestamp key) is not falsely
        deduplicated.
        """
        with self._lock:
            key = f"{signal.strategy_id}:{signal.instrument}:{signal.timestamp}"
            self._pending_signals.pop(key, None)

    def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID."""
        return self.execution_engine.get_order(order_id)

    def get_active_orders(
        self,
        strategy_id: Optional[str] = None,
    ) -> list[Order]:
        """Get active orders."""
        orders = [
            o for o in self.execution_engine._orders.values()
            if o.state in (OrderState.CREATED, OrderState.SUBMITTED, OrderState.PARTIALLY_FILLED)
        ]
        if strategy_id:
            orders = [o for o in orders if o.strategy_id == strategy_id]
        return orders

    def snapshot(self) -> dict:
        """Get order manager state."""
        return {
            "pending_signals": len(self._pending_signals),
            "active_orders": len(self._active_orders),
        }


class OrderManagerFacade:
    """Shared read/routing layer over per-strategy OrderManagers.

    Each StrategyRuntime owns its own OrderManager (its own pending-signal
    dedup set, active-order cache and fill-notification slot). The shared
    execution engine is the single broker transport underneath. The facade
    keeps external consumers (reconciliation, dashboard, scripts) stable:
    reads aggregate across runtimes; submit/drain route by strategy.
    """

    def __init__(self, execution_engine: Any):
        self.execution_engine = execution_engine
        self._managers: dict[str, OrderManager] = {}

    def register(self, strategy_id: str, manager: OrderManager) -> None:
        self._managers[strategy_id] = manager

    def submit_signal(self, signal, multiplier: float = 1.0, trade_id: str = "", side: Optional[str] = None):
        mgr = self._managers.get(getattr(signal, "strategy_id", None))
        if mgr is None:
            return None
        return mgr.submit_signal(signal, multiplier=multiplier, trade_id=trade_id, side=side)

    def remove_pending(self, signal) -> None:
        """Remove a signal from the pending-dedup set across all runtimes."""
        mgr = self._managers.get(getattr(signal, "strategy_id", None))
        if mgr is not None:
            mgr.remove_pending(signal)

    def drain_fills(self) -> list[Fill]:
        """Aggregate drains across all runtimes (compat only).

        The engine core drains its own strategy manager directly.
        """
        fills: list[Fill] = []
        for mgr in self._managers.values():
            fills.extend(mgr.drain_fills())
        return fills

    def cancel_order(self, order_id: str) -> bool:
        return self.execution_engine.cancel_order(order_id)

    def get_order(self, order_id: str) -> Optional[Order]:
        return self.execution_engine.get_order(order_id)

    def get_active_orders(self, strategy_id: Optional[str] = None) -> list[Order]:
        orders = [
            o for o in self.execution_engine._orders.values()
            if o.state in (OrderState.CREATED, OrderState.SUBMITTED, OrderState.PARTIALLY_FILLED)
        ]
        if strategy_id:
            orders = [o for o in orders if o.strategy_id == strategy_id]
        return orders

    def snapshot(self) -> dict:
        """Aggregated order-manager state (all strategies + shared broker)."""
        orders = {
            oid: {
                "order_id": o.order_id,
                "strategy_id": o.strategy_id,
                "instrument": o.instrument,
                "side": o.side,
                "quantity": o.quantity,
                "state": o.state.value,
            }
            for oid, o in self.execution_engine._orders.items()
        }
        return {
            "orders": orders,
            "active_orders": len(self.get_active_orders()),
            "pending_signals": sum(
                len(m._pending_signals) for m in self._managers.values()
            ),
        }
