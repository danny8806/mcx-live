"""Safe mode manager for the trading system.

Manages safe/degraded mode transitions with structured reason tracking.
Integrates with MarketStatus to enforce engine state machine transitions.

Usage:
    safe_mode = SafeModeManager(market_status)
    safe_mode.enter_safe_mode("position_mismatch", "Gold long 3 != 1")

    # Before order submission
    if not safe_mode.should_allow_trading():
        return

    # Exit when conditions clear
    safe_mode.clear_reason("position_mismatch")
    safe_mode.exit_safe_mode()
"""
from __future__ import annotations

import time
import threading
from typing import Optional


class SafeModeManager:
    """Manages safe/degraded mode transitions."""

    REASONS = {
        "position_mismatch": "Position state inconsistent after reconciliation",
        "fill_ambiguity": "Duplicate or ambiguous fill detected",
        "database_failure": "Critical persistence operation failed",
        "state_restore_failure": "State restoration produced invalid state",
        "market_data_uncertain": "Market data source uncertain",
        "persistence_failure": "Trade persistence failed",
        "order_state_uncertain": "Order state inconsistent",
        "reconciliation_failed": "Reconciliation found critical errors",
    }

    def __init__(self, market_status):
        self.market_status = market_status
        self._active_reasons: dict[str, float] = {}
        self._lock = threading.Lock()
        self._last_exit_attempt: float = 0.0
        self._exit_cooldown: float = 5.0  # seconds between exit attempts

    def enter_safe_mode(self, reason: str, details: str = "") -> None:
        """Enter safe mode with a specific reason.

        If already in safe mode, adds the new reason to the set.
        Logs the transition and updates MarketStatus engine state.
        """
        with self._lock:
            was_safe = self.is_active
            self._active_reasons[reason] = time.time()

            if not was_safe:
                description = self.REASONS.get(reason, reason)
                msg = f"[SafeMode] ENTERED: {reason} — {description}"
                if details:
                    msg += f" | {details}"
                print(msg, flush=True)
                self.market_status.enter_safe_mode(reason=f"{reason}: {description}")
            else:
                msg = f"[SafeMode] Added reason: {reason}"
                if details:
                    msg += f" | {details}"
                print(msg, flush=True)

    def clear_reason(self, reason: str) -> bool:
        """Clear a specific safe mode reason.

        Returns True if the reason was active and is now cleared.
        """
        with self._lock:
            if reason in self._active_reasons:
                del self._active_reasons[reason]
                print(f"[SafeMode] Cleared reason: {reason}", flush=True)
                return True
            return False

    def exit_safe_mode(self) -> bool:
        """Exit safe mode only if all conditions are clear.

        Returns True if safe mode was exited successfully.
        Returns False if reasons are still active or cooldown hasn't elapsed.
        """
        with self._lock:
            now = time.time()

            if now - self._last_exit_attempt < self._exit_cooldown:
                return False
            self._last_exit_attempt = now

            if self._active_reasons:
                remaining = list(self._active_reasons.keys())
                print(f"[SafeMode] Cannot exit - active reasons: {remaining}", flush=True)
                return False

            if self.market_status.is_safe:
                print("[SafeMode] EXITED — all reasons cleared", flush=True)
                self.market_status.exit_safe_mode()
                return True

            return True  # Already not in safe mode

    def should_allow_trading(self) -> bool:
        """Check if trading is allowed.

        Returns False if:
        - Safe mode is active (has any reasons)
        - MarketStatus engine is in SAFE_MODE or HALTED
        - MarketStatus is_trading_allowed is False
        """
        if self._active_reasons:
            return False
        if self.market_status.is_safe:
            return False
        if not self.market_status.is_trading_allowed:
            return False
        return True

    def get_status(self) -> dict:
        """Get current safe mode status."""
        with self._lock:
            reasons_detail = {}
            for reason, entry_time in self._active_reasons.items():
                reasons_detail[reason] = {
                    "description": self.REASONS.get(reason, reason),
                    "entered_at": entry_time,
                    "duration_s": round(time.time() - entry_time, 1),
                }
            return {
                "active": bool(self._active_reasons),
                "reason_count": len(self._active_reasons),
                "reasons": reasons_detail,
                "trading_allowed": self.should_allow_trading(),
                "engine_status": self.market_status.engine_status.value,
                "market_state": self.market_status.state.value,
            }

    def has_reason(self, reason: str) -> bool:
        """Check if a specific reason is currently active."""
        return reason in self._active_reasons

    @property
    def is_active(self) -> bool:
        """True if safe mode is currently active."""
        return bool(self._active_reasons)

    @property
    def active_reasons(self) -> list[str]:
        """List of currently active reason keys."""
        return list(self._active_reasons.keys())

    def __repr__(self) -> str:
        reasons = list(self._active_reasons.keys())
        return f"SafeModeManager(active={self.is_active}, reasons={reasons})"
