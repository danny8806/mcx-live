"""Risk engine for portfolio-level risk management."""
from __future__ import annotations

import threading
from datetime import datetime, timezone, timedelta
from typing import Any, Optional


class RiskEngine:
    """Portfolio-level risk management engine.
    
    Enforces:
    - Per-strategy position limits
    - Per-instrument exposure limits
    - Total portfolio exposure limits
    - Margin requirements
    - Daily loss limits
    - Maximum drawdown
    - Kill switch
    """

    def __init__(
        self,
        max_positions_per_strategy: int = 1,
        max_positions_total: int = 8,
        max_daily_loss: float = 999_999_999.0,
        max_drawdown_pct: float = 100.0,
        kill_switch_enabled: bool = False,
        kill_switch_callback: Optional[Any] = None,
    ):
        self.max_positions_per_strategy = max_positions_per_strategy
        self.max_positions_total = max_positions_total
        self.max_daily_loss = max_daily_loss
        self.max_drawdown_pct = max_drawdown_pct
        self.kill_switch_enabled = kill_switch_enabled
        self._kill_switch_callback = kill_switch_callback
        self._lock = threading.RLock()

        self._kill_switch_active = False
        self._last_kill_switch_reason: str = ""
        # ### OBSERVATION (_daily_pnl vs _open_unrealized)
        # `_daily_pnl` was realized-only, so the dashboard "Today's P&L" and the
        # daily-loss kill switch both ignored every floating (unrealized) move.
        # A session that lost ₹50k on paper yet booked nothing was shown as ₹0.
        # The engine also never fed open-unrealized P&L into its daily loss
        # limit, so the kill switch could not fire on a drawdown that had not
        # been realized yet. We now track the realized session figure
        # (`_daily_pnl`) plus the live open unrealized P&L (`_open_unrealized`,
        # refreshed from the engine snapshot every tick) and expose the
        # economically-correct combined session P&L as `daily_pnl`.
        self._daily_pnl: float = 0.0
        self._open_unrealized: float = 0.0
        self._peak_equity: float = 0.0
        self._ist = timezone(timedelta(hours=5, minutes=30))
        self._last_reset_date: str = datetime.now(self._ist).strftime("%Y-%m-%d")

    def _reset_if_new_day_locked(self) -> None:
        """Reset session-risk counters using the exchange's IST trading date."""
        today = datetime.now(self._ist).strftime("%Y-%m-%d")
        if today != self._last_reset_date:
            self._daily_pnl = 0.0
            self._open_unrealized = 0.0
            self._peak_equity = 0.0
            self._last_reset_date = today
            print(f"[Risk] Daily reset for {today}", flush=True)

    def check_order(
        self,
        signal: Any,
        current_positions: int,
        strategy_positions: int,
        available_margin: float,
        margin_required: float,
        current_equity: float,
    ) -> tuple[bool, Optional[str]]:
        """Check if order passes risk checks.
        
        Returns:
            (allowed, reason) - reason is None if allowed
        """
        with self._lock:
            self._reset_if_new_day_locked()
            # Kill switch check
            if self._kill_switch_active:
                return False, "kill_switch_active"

            # Max positions per strategy
            if strategy_positions >= self.max_positions_per_strategy:
                return False, "max_positions_per_strategy_reached"

            # Max total positions
            if current_positions >= self.max_positions_total:
                return False, "max_positions_total_reached"

            # Margin check
            if margin_required > available_margin:
                return False, "insufficient_margin"

            # Daily loss check — realized AND unrealized losses combined so the
            # kill switch reacts to a live drawdown, not just booked losses.
            if self._daily_pnl + self._open_unrealized <= -self.max_daily_loss:
                self._activate_kill_switch("daily_loss_limit_reached")
                return False, "daily_loss_limit_reached"

            # Max drawdown check
            if self._peak_equity > 0:
                drawdown_pct = (self._peak_equity - current_equity) / self._peak_equity * 100
                if drawdown_pct >= self.max_drawdown_pct:
                    self._activate_kill_switch("max_drawdown_reached")
                    return False, "max_drawdown_reached"

            return True, None

    def update_daily_pnl(self, pnl: float) -> None:
        """Update running daily realized P&L. Auto-resets at start of new trading day."""
        with self._lock:
            self._reset_if_new_day_locked()
            self._daily_pnl += pnl

    def set_open_unrealized(self, pnl: float) -> None:
        """Feed the current open-position unrealized session P&L.

        Called from the engine snapshot path each mark cycle so the session
        figure tracked by this engine always includes floating loss/profit.
        """
        with self._lock:
            self._reset_if_new_day_locked()
            self._open_unrealized = float(pnl)

    def update_peak_equity(self, equity: float) -> None:
        """Update peak equity for drawdown calculation."""
        with self._lock:
            self._peak_equity = max(self._peak_equity, equity)

    def _activate_kill_switch(self, reason: str = "") -> None:
        """Activate kill switch to stop all trading."""
        if self.kill_switch_enabled:
            with self._lock:
                self._kill_switch_active = True
                self._last_kill_switch_reason = reason
            print("[RISK] KILL SWITCH ACTIVATED", flush=True)
            cb = self._kill_switch_callback
            if cb is not None:
                try:
                    cb(self._last_kill_switch_reason)
                except Exception:
                    pass

    def deactivate_kill_switch(self) -> None:
        """Manually deactivate kill switch."""
        with self._lock:
            self._kill_switch_active = False

    @property
    def kill_switch_active(self) -> bool:
        return self._kill_switch_active

    @property
    def daily_pnl(self) -> float:
        """Combined session P&L (realized + open unrealized)."""
        return self._daily_pnl + self._open_unrealized

    @property
    def realized_daily_pnl(self) -> float:
        """Realized-only session P&L (used for booked-loss bookkeeping)."""
        return self._daily_pnl

    def reset_daily(self) -> None:
        """Reset daily P&L (call at start of new trading day)."""
        with self._lock:
            self._daily_pnl = 0.0
            self._open_unrealized = 0.0

    def snapshot(self) -> dict:
        """Get risk engine state."""
        with self._lock:
            return {
                "kill_switch_active": self._kill_switch_active,
                "daily_pnl": self.daily_pnl,
                "realized_daily_pnl": self._daily_pnl,
                "open_unrealized": self._open_unrealized,
                "peak_equity": self._peak_equity,
                "last_reset_date": self._last_reset_date,
            }

    def restore(self, data: dict) -> None:
        """Restore risk engine state.

        ### OBSERVATION (restore)
        The kill switch, daily P&L and peak equity were never persisted, so a
        restart wiped the risk posture: an auto-triggered kill switch stayed
        dead until manually re-triggered, and the daily-loss budget silently
        reset. Loading them back from the saved engine snapshot keeps risk
        enforcement continuous across restarts.
        """
        with self._lock:
            self._kill_switch_active = data.get("kill_switch_active", False)
            self._daily_pnl = data.get("realized_daily_pnl", data.get("daily_pnl", 0.0))
            # Back-compat: pre-fix snapshots only stored the total; if it
            # contained unrealized we cannot split it, so treat it as realized.
            self._open_unrealized = data.get("open_unrealized", 0.0)
            self._peak_equity = data.get("peak_equity", 0.0)
            self._last_reset_date = data.get("last_reset_date", self._last_reset_date)
