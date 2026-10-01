"""Shared order/fill data structures used by both LIVE and PAPER runtimes.

These dataclasses/enums are broker-agnostic: the LIVE execution engine, order
manager, position manager, P&L engine, trade-close manager and reconciliation
all use them, and they carry LIVE- broker-native attributes at runtime without
the shell of PaperExecutionEngine (which is isolated in
``execution.paper_broker`` and never reachable from the LIVE runtime).
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class OrderState(Enum):
    """Order lifecycle states."""
    CREATED = "created"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"
    CANCELED = "canceled"


@dataclass
class Order:
    """Represents an order."""
    order_id: str
    strategy_id: str
    instrument: str
    side: str  # "BUY" or "SELL"
    quantity: int
    order_type: str = "MARKET"
    price: Optional[float] = None
    trigger_price: Optional[float] = None
    correlation_id: Optional[str] = None
    requested_price: Optional[float] = None
    planned_entry_price: Optional[float] = None
    planned_sl: Optional[float] = None
    planned_order_type: Optional[str] = None
    order_role: Optional[str] = None  # ENTRY/EXIT/STOP_LOSS/REVERSAL_EXIT/REVERSAL_ENTRY/EMERGENCY_EXIT
    # LEGACY, always None: the broker-side protective SL was retired.  Kept so
    # historical order rows remain readable; nothing populates it any more.
    protected_order_id: Optional[str] = None
    state: OrderState = OrderState.CREATED
    filled_quantity: int = 0
    average_fill_price: float = 0.0
    fill_ids: list[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    reason: Optional[str] = None
    multiplier: float = 1.0
    entry_signal_id: Optional[str] = None
    trade_id: Optional[str] = None
    # Canonical lifecycle lineage.  lifecycle_id intentionally aliases the
    # immutable trade_id; position ownership is attached after entry fill.
    lifecycle_id: Optional[str] = None
    parent_signal_id: Optional[str] = None
    parent_position_id: Optional[str] = None
    position_id: Optional[str] = None
    position_generation: Optional[int] = None
    original_order_id: Optional[str] = None
    fallback_cancel_confirmed: bool = False
    reversal_parent_signal_id: Optional[str] = None
    trigger_state: Optional[str] = None
    trigger_generation: Optional[int] = None
    trigger_source: Optional[str] = None
    # Execution handoff evidence. NOT_SENT means a local validation/gate
    # stopped the order; BROKER_RESPONSE_RECEIVED means the broker adapter
    # returned a response; OUTCOME_UNKNOWN means a request may be in flight.
    submission_outcome: str = "NOT_SENT"
    submission_attempt_count: int = 0
    rejection_retry_count: int = 0
    submission_attempts: list[dict] = field(default_factory=list)


@dataclass
class Fill:
    """Represents an order fill."""
    fill_id: str
    order_id: str
    instrument: str
    side: str
    quantity: int
    price: float
    timestamp: float
    strategy_id: str
    multiplier: float = 1.0
    entry_signal_id: Optional[str] = None
    trade_id: Optional[str] = None
    lifecycle_id: Optional[str] = None
    position_id: Optional[str] = None
    position_generation: Optional[int] = None

    @property
    def gross_value(self) -> float:
        return self.quantity * self.price * self.multiplier
