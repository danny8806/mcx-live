"""Execution engine package.

LIVE-only runtime — the paper execution engine is isolated in
``execution.paper_broker`` (test fixtures / legacy replay harness) and is NOT
imported here so no LIVE import chain ever loads it.  The shared
:class:`Order` / :class:`OrderState` / :class:`Fill` types come from
``execution.models`` and are re-exported for convenience.
"""
from .models import Order, OrderState, Fill
from .fee_model import MCXFeeModel, FeeBreakdown
from .order_manager import OrderManager

__all__ = [
    "Order",
    "OrderState",
    "Fill",
    "MCXFeeModel",
    "FeeBreakdown",
    "OrderManager",
]