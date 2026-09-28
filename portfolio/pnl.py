"""P&L (Profit and Loss) engine for calculating realized and unrealized P&L."""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Optional

from execution.models import Fill
from execution.fee_model import MCXFeeModel, FeeBreakdown
from .position_manager import Position


@dataclass
class PnLSnapshot:
    """Point-in-time P&L snapshot."""
    realized_gross: float
    realized_charges: float
    realized_net: float
    unrealized_gross: float
    gross_pnl: float
    net_pnl: float
    timestamp: float


class PNLEngine:
    """P&L calculation engine.
    
    Calculates:
    - Realized P&L from fills (LONG: exit - entry, SHORT: entry - exit)
    - Unrealized P&L from current market price
    - Gross P&L
    - Charges (estimated)
    - Net P&L
    
    Source of truth:
    - Realized P&L: fills only
    - Unrealized P&L: current executable market price
    """

    def __init__(self, fee_model: MCXFeeModel):
        self.fee_model = fee_model
        self._lock = threading.Lock()
        self._realized_gross: float = 0.0
        self._realized_charges: float = 0.0
        self._realized_net: float = 0.0
        self._unrealized_gross: float = 0.0
        self._trade_count: int = 0
        self._wins: int = 0
        self._losses: int = 0

    def calculate_realized_pnl(
        self,
        entry_fill: Fill,
        exit_fill: Fill,
        multiplier: float = 1.0,
    ) -> tuple[float, float, float]:
        """Calculate realized P&L for a completed trade (PURE — no side effects).
        
        Returns:
            (gross_pnl, charges, net_pnl)
        """
        # Gross P&L
        if entry_fill.side == "BUY":  # LONG position
            gross = (exit_fill.price - entry_fill.price) * entry_fill.quantity * multiplier
        else:  # SHORT position
            gross = (entry_fill.price - exit_fill.price) * entry_fill.quantity * multiplier

        # Charges
        position_side = "LONG" if entry_fill.side == "BUY" else "SHORT"
        fees = self.fee_model.calculate(
            entry_fill.price, exit_fill.price,
            entry_fill.quantity, multiplier,
            side=position_side,
        )

        net = gross - fees.total
        return gross, fees.total, net

    def record_trade(self, gross: float, charges: float, net: float) -> None:
        """Record a completed trade in running totals. Call AFTER calculate_realized_pnl."""
        with self._lock:
            self._realized_gross += gross
            self._realized_charges += charges
            self._realized_net += net
            self._trade_count += 1
            if net >= 0:
                self._wins += 1
            else:
                self._losses += 1

    def calculate_unrealized_pnl(
        self,
        position: Position,
        current_price: float,
    ) -> float:
        """Calculate unrealized P&L for an open position.
        
        Uses:
        - BID for LONG positions (liquidation value)
        - ASK for SHORT positions (liquidation value)
        """
        position.update_mark(current_price)
        self._unrealized_gross = position.unrealized_pnl
        return position.unrealized_pnl

    def update_unrealized_pnl(self, gross: float) -> None:
        """Set current unrealized gross P&L from open-position marks.

        Called on every tick from the engine's mark loop so the P&L snapshot
        reports a live per-strategy unrealized figure instead of zero.
        """
        with self._lock:
            self._unrealized_gross = gross

    def get_snapshot(self) -> PnLSnapshot:
        """Get current P&L snapshot."""
        with self._lock:
            return PnLSnapshot(
                realized_gross=self._realized_gross,
                realized_charges=self._realized_charges,
                realized_net=self._realized_net,
                unrealized_gross=self._unrealized_gross,
                gross_pnl=self._realized_gross + self._unrealized_gross,
                net_pnl=self._realized_net + self._unrealized_gross,
                timestamp=time.time(),
            )

    @property
    def realized_gross(self) -> float:
        with self._lock:
            return self._realized_gross

    @property
    def realized_net(self) -> float:
        with self._lock:
            return self._realized_net

    @property
    def trade_count(self) -> int:
        with self._lock:
            return self._trade_count

    @property
    def win_rate(self) -> float:
        with self._lock:
            if self._trade_count == 0:
                return 0.0
            return self._wins / self._trade_count * 100

    def snapshot(self) -> dict:
        """Get P&L state for persistence."""
        with self._lock:
            return {
                "realized_gross": self._realized_gross,
                "realized_charges": self._realized_charges,
                "realized_net": self._realized_net,
                "trade_count": self._trade_count,
                "wins": self._wins,
                "losses": self._losses,
                "win_rate": (self._wins / self._trade_count * 100) if self._trade_count > 0 else 0.0,
            }

    def restore(self, data: dict) -> None:
        """Restore P&L state from persistence."""
        with self._lock:
            self._realized_gross = data.get("realized_gross", 0.0)
            self._realized_charges = data.get("realized_charges", 0.0)
            self._realized_net = data.get("realized_net", 0.0)
            self._trade_count = data.get("trade_count", 0)
            self._wins = data.get("wins", 0)
            self._losses = data.get("losses", 0)

    def rebuild_from_ledger(self, trades: list[dict]) -> None:
        """Rebuild the realized/trade-count state from persisted closed trades.

        ### OBSERVATION (rebuild_from_ledger)
        PNLEngine is memory-only: its realized totals/win/loss counters are
        never persisted, so after a restart /api/overview, /api/pnl and
        /api/strategies all reported realized=0 and reconciliation raised
        "P&L mismatch" errors against the still-correct DB trade sum.
        Feeding it the closed DB trades makes every derived endpoint and the
        reconciliation check correct again (the code now produces the right
        numbers rather than the UI being patched around a stale zero).
        """
        with self._lock:
            self._realized_gross = 0.0
            self._realized_charges = 0.0
            self._realized_net = 0.0
            self._trade_count = 0
            self._wins = 0
            self._losses = 0
            for t in trades:
                if (t.get("status") or "").lower() != "closed":
                    continue
                if t.get("net_pnl") is None:
                    continue
                gross = float(t.get("gross_pnl") or 0.0)
                charges = float(t.get("charges") or 0.0)
                net = float(t.get("net_pnl") or 0.0)
                self._realized_gross += gross
                self._realized_charges += charges
                self._realized_net += net
                self._trade_count += 1
                if net >= 0.0:
                    self._wins += 1
                else:
                    self._losses += 1
