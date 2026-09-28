"""Fail-closed live order ownership rules.

This module is intentionally independent of TradingEngine. Every live order
must cross this check before the broker adapter is called.

There is no broker-side protective SL: a stop-loss is the local,
position-owned SL monitor minting an ordinary ``EXIT`` order bound to the open
position.  ``STOP_LOSS`` is therefore NOT an exit role here.
"""
from __future__ import annotations

from typing import Optional


_ENTRY_ROLES = {"ENTRY", "REVERSAL_ENTRY", "FALLBACK_MARKET"}
_EXIT_ROLES = {"EXIT", "REVERSAL_EXIT", "EMERGENCY_EXIT"}


def validate_live_order_ownership(env, order) -> Optional[str]:
    """Return a rejection reason unless ``order`` belongs to current exposure."""
    role = str(getattr(order, "order_role", "") or "").upper()
    safe_mode = getattr(env, "safe_mode", None)
    if safe_mode is not None and safe_mode.is_active and role in _ENTRY_ROLES:
        return "SAFE_MODE_ACTIVE"

    lifecycle_id = getattr(order, "lifecycle_id", None) or getattr(order, "trade_id", None)
    signal_id = getattr(order, "parent_signal_id", None) or getattr(order, "entry_signal_id", None)
    if not lifecycle_id or lifecycle_id != getattr(order, "trade_id", None):
        return "ORDER_LIFECYCLE_MISMATCH"
    if not signal_id:
        return "ORDER_SIGNAL_OWNERSHIP_MISSING"

    positions = env.position_manager.get_positions_by_strategy(order.strategy_id)
    current = next((p for p in positions
                    if p.is_open and p.instrument == order.instrument), None)
    if role in _ENTRY_ROLES:
        if role == "FALLBACK_MARKET":
            # A verified fallback may finish a zero/partial-fill entry. With
            # no fill yet the instrument must still be flat; after a partial
            # fill it must belong to this exact lifecycle and position.
            if current is None:
                if getattr(order, "parent_position_id", None) is not None:
                    return "FALLBACK_POSITION_MISSING"
                return None
            if (current.trade_id == lifecycle_id
                    and current.position_id == getattr(order, "parent_position_id", None)
                    and current.position_generation == getattr(order, "position_generation", None)):
                return None
            return "ENTRY_BLOCKED_POSITION_NOT_FLAT"
        if current is None:
            return None
        return "ENTRY_BLOCKED_POSITION_NOT_FLAT"
    if role not in _EXIT_ROLES:
        return "ORDER_ROLE_INVALID"

    owner_position_id = (getattr(order, "parent_position_id", None)
                         or getattr(order, "position_id", None))
    if not owner_position_id or current is None:
        return "EXIT_POSITION_OWNERSHIP_MISSING"
    if current.position_id != owner_position_id:
        return "STALE_LIFECYCLE_TRIGGER_REJECTED:position_mismatch"
    if current.trade_id != lifecycle_id:
        return "STALE_LIFECYCLE_TRIGGER_REJECTED:lifecycle_mismatch"
    if (getattr(order, "position_generation", None) is None
            or current.position_generation != order.position_generation):
        return "STALE_LIFECYCLE_TRIGGER_REJECTED:generation_mismatch"
    if (current.strategy_id != order.strategy_id
            or current.instrument != order.instrument
            or current.quantity <= 0):
        return "STALE_LIFECYCLE_TRIGGER_REJECTED:position_invalid"
    if current.exit_started:
        return "POSITION_EXIT_ALREADY_STARTED"
    if str(order.side).upper() != ("SELL" if current.is_long else "BUY"):
        return "STALE_LIFECYCLE_TRIGGER_REJECTED:side_mismatch"
    if int(order.quantity or 0) <= 0 or int(order.quantity) > int(current.quantity):
        return "ORDER_QUANTITY_EXCEEDS_POSITION"
    return None
