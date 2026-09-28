"""Shared type definitions for strategies and execution to avoid circular imports.

This module MUST NOT import anything from core, execution, htf, or strategies modules.
It only uses Python standard library types.
"""
from __future__ import annotations

import uuid as _uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class SignalType(Enum):
    """Signal types."""
    LONG = "LONG"
    SHORT = "SHORT"
    FLAT = "FLAT"
    REVERSAL = "REVERSAL"


class StrategyState(Enum):
    """Strategy state machine states."""
    FLAT = "flat"
    SIGNAL_LONG = "signal_long"
    SIGNAL_SHORT = "signal_short"
    PENDING_LONG = "pending_long"
    PENDING_SHORT = "pending_short"
    ENTRY_TRIGGERED = "entry_triggered"
    LONG_POSITION = "long_position"
    SHORT_POSITION = "short_position"
    STOP_ACTIVE = "stop_active"
    EXIT_PENDING = "exit_pending"
    EXIT_ORDER_SUBMITTED = "exit_order_submitted"


class OrderState(Enum):
    """Order lifecycle states."""
    CREATED = "created"
    SUBMITTED = "submitted"
    ACKNOWLEDGED = "acknowledged"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    REJECTED = "rejected"


@dataclass
class Signal:
    """Strategy signal output.

    Every signal gets a unique signal_id (UUID) that travels through the
    entire lifecycle: Signal -> Order -> Fill -> Position -> Trade.
    This provides complete signal lineage for audit and reconciliation.
    """
    signal_type: 'SignalType'
    instrument: str
    strategy_id: str
    timestamp: float
    trigger_price: float
    stop_price: float
    quantity: int
    side: Optional[str] = None  # "LONG" or "SHORT" — used for REVERSAL to determine order direction
    metadata: Optional[dict] = None
    signal_id: str = field(default_factory=lambda: str(_uuid.uuid4()))
    # Execution ownership is copied from the active position at signal time.
    # It must never be inferred later from whichever position is current.
    lifecycle_id: Optional[str] = None
    parent_position_id: Optional[str] = None
    position_generation: Optional[int] = None
    context: Optional['SignalExecutionContext'] = None  # Phase 4 — frozen inputs


@dataclass(frozen=True)
class SignalExecutionContext:
    """Immutable snapshot of the inputs that produced a signal (Phase 4).

    Frozen at signal creation so execution and reconciliation always read the
    signal's OWN candle/indicator/position state — never mutable strategy state
    that may have advanced.  Freezing is purely additive: no execution path
    reads the context yet, so attaching it never changes behavior.  Persisting
    the context across restarts lands with the persistence convergence phase.
    """
    timestamp: float = 0.0
    open: Optional[float] = None
    high: Optional[float] = None
    low: Optional[float] = None
    close: Optional[float] = None
    dema: Optional[float] = None      # fast DEMA-ATR line value at signal time
    atr: Optional[float] = None       # fast ATR value at signal time
    htf_value: Optional[float] = None # 1H line value at signal time
    mid_value: Optional[float] = None # 15m value at signal time (when fast != mid)
    position_side: Optional[str] = None   # held side when the signal was created
    position_stop: Optional[float] = None # live stop when the signal was created


def freeze_signal_context(
    signal: 'Signal',
    *,
    bar: Any = None,
    open_: Optional[float] = None,
    high: Optional[float] = None,
    low: Optional[float] = None,
    close: Optional[float] = None,
    dema: Optional[float] = None,
    atr: Optional[float] = None,
    htf_value: Optional[float] = None,
    mid_value: Optional[float] = None,
    position_side: Optional[str] = None,
    position_stop: Optional[float] = None,
    timestamp: Optional[float] = None,
) -> 'Signal':
    """Attach the frozen execution context to a signal (first write wins).

    ``bar`` is optional — tick exits have no bar — and explicit scalar values
    override bar-derived ones.  Idempotent: a signal already carrying a context
    (e.g. a pending entry armed on an earlier candle) keeps its original
    snapshot, which is exactly the immutability the phase demands.
    """
    if signal is None or getattr(signal, "context", None) is not None:
        return signal
    if bar is not None:
        high = bar.high if high is None else high
        low = bar.low if low is None else low
        close = bar.close if close is None else close
        open_ = getattr(bar, "open", None) if open_ is None else open_
        if timestamp is None:
            timestamp = getattr(bar, "start_ts", 0.0)
    if timestamp is None:
        timestamp = signal.timestamp
    signal.context = SignalExecutionContext(
        timestamp=float(timestamp),
        open=float(open_) if open_ is not None else None,
        high=float(high) if high is not None else None,
        low=float(low) if low is not None else None,
        close=float(close) if close is not None else None,
        dema=float(dema) if dema is not None else None,
        atr=float(atr) if atr is not None else None,
        htf_value=float(htf_value) if htf_value is not None else None,
        mid_value=float(mid_value) if mid_value is not None else None,
        position_side=position_side,
        position_stop=float(position_stop) if position_stop is not None else None,
    )
    return signal


@dataclass
class PendingEntry:
    """Pending entry order."""
    signal: 'Signal'
    trigger_price: float
    side: str  # "LONG" or "SHORT"
    status: str = "pending"
    created_at: float = 0.0  # timestamp when pending entry was created
    bars_pending: int = 0    # number of bars since creation
    immediate: bool = False  # direct-market re-entry: engine flips right after a reversal exit


def resolve_order_role(signal: Optional['Signal'], *, exit_reason: Optional[str] = None) -> str:
    """Resolve the canonical order_role for an order created from a signal.

    Spec §12-14 order roles: ENTRY, EXIT, STOP_LOSS, REVERSAL_EXIT,
    REVERSAL_ENTRY, EMERGENCY_EXIT.  Pure, shared resolution so paper and
    live engines and persistence all agree on the same classification.
    """
    reason = exit_reason
    if signal is not None:
        md = signal.metadata or {}
        if md.get("market_fallback"):
            return "FALLBACK_MARKET"
        if reason is None:
            reason = md.get("exit_reason") or md.get("reason")
        if signal.signal_type == SignalType.REVERSAL:
            if md.get("exit"):  # pragma: no cover - reversal is dual-purposed
                return "REVERSAL_EXIT"
            return "REVERSAL_ENTRY"
        if md.get("is_reversal") and not md.get("exit"):
            return "REVERSAL_ENTRY"
        if md.get("exit") or md.get("is_exit"):
            if signal.signal_type == SignalType.REVERSAL or md.get("is_reversal"):
                return "REVERSAL_EXIT"
            reason_l = str(reason or "").lower()
            if "stop_loss" in reason_l or "sl_" in reason_l or reason_l.startswith("sl"):
                return "STOP_LOSS"
            if "reversal" in reason_l:
                return "REVERSAL_EXIT"
            return "EXIT"
    if reason is not None:
        reason_l = str(reason).lower()
        if "stop_loss" in reason_l or reason_l.startswith("sl"):
            return "STOP_LOSS"
        if "reversal" in reason_l:
            return "REVERSAL_EXIT"
    if signal is not None and signal.metadata is not None and signal.metadata.get("is_reversal"):
        return "REVERSAL_ENTRY"
    return "ENTRY"


@dataclass
class StrategyInput:
    """Input data for strategy decision."""
    instrument: str
    timestamp: float
    fast_bar: 'Bar'  # Forward reference
    fast_close: float
    fast_high: float
    fast_low: float
    previous_fast_close: float
    fast_dema_atr: float
    htf_dema_atr: Optional[float]
    previous_htf_dema_atr: Optional[float]
    htf_confirmed: bool
    htf_source_timestamp: Optional[float]


# Forward reference for type hints - Bar is imported at runtime in base_dema_strategy
Bar = Any  # Placeholder, actual import happens in base_dema_strategy
