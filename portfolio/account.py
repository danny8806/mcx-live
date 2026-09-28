"""Account engine for managing capital, equity, and margin."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class AccountSnapshot:
    """Point-in-time account state."""
    starting_capital: float
    cash: float
    realized_pnl: float
    unrealized_pnl: float
    gross_pnl: float
    charges: float
    net_pnl: float
    equity: float
    used_margin: float
    available_margin: float


class AccountEngine:
    """Account management engine.
    
    Maintains:
    - Starting capital
    - Cash balance
    - Realized P&L
    - Unrealized P&L
    - Gross P&L
    - Charges
    - Net P&L
    - Equity
    - Used margin
    - Available margin
    
    Formula:
        equity = starting_capital + realized_net_pnl + current_unrealized_pnl
    """

    def __init__(
        self,
        starting_capital: float = 1_000_000.0,
        margin_per_trade_pct: float = 6.5,
    ):
        self.starting_capital = starting_capital
        self.margin_per_trade_pct = margin_per_trade_pct

        self.cash = starting_capital
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.charges = 0.0
        self.used_margin = 0.0
        # Broker-reported totals (LIVE only). When present, equity / used /
        # available mirror the broker's own numbers so persisted account
        # snapshots show exchange truth instead of the locally derived base.
        # P&L attribution always stays locally computed; only the equity BASE
        # and margin view adopt broker truth.
        self._broker_equity: Optional[float] = None
        self._broker_used_margin: Optional[float] = None
        self._broker_available_margin: Optional[float] = None

    def set_broker_reported(self, equity: Optional[float] = None,
                            used_margin: Optional[float] = None,
                            available_margin: Optional[float] = None) -> None:
        """Adopt broker-reported totals (LIVE mode).

        Each non-None argument overrides that figure; an all-None call clears
        the override and reverts to locally derived accounting.  The override
        is intentionally not persisted — the broker is re-polled every cycle.
        """
        if equity is None and used_margin is None and available_margin is None:
            self.clear_broker_reported()
            return
        if equity is not None:
            self._broker_equity = float(equity)
        if used_margin is not None:
            self._broker_used_margin = float(used_margin)
        if available_margin is not None:
            self._broker_available_margin = float(available_margin)

    def clear_broker_reported(self) -> None:
        """Drop the broker override and return to local accounting."""
        self._broker_equity = None
        self._broker_used_margin = None
        self._broker_available_margin = None

    @property
    def broker_reported(self) -> bool:
        """True when broker-reported totals are currently adopted."""
        return (self._broker_equity is not None
                or self._broker_available_margin is not None)

    @property
    def equity(self) -> float:
        """Current equity = starting capital + realized net + unrealized."""
        if self._broker_equity is not None:
            return self._broker_equity
        return self.starting_capital + self.realized_pnl + self.unrealized_pnl

    @property
    def net_pnl(self) -> float:
        """Net P&L = realized net + unrealized."""
        return self.realized_pnl + self.unrealized_pnl

    @property
    def gross_pnl(self) -> float:
        """Gross P&L (before charges)."""
        return self.realized_pnl + self.unrealized_pnl + self.charges

    @property
    def available_margin(self) -> float:
        """Available margin for new trades."""
        if self._broker_available_margin is not None:
            return self._broker_available_margin
        return self.equity - self.used_margin

    def calculate_margin_required(self, price: float, quantity: int, multiplier: float) -> float:
        """Calculate margin required for a position."""
        return price * quantity * multiplier * self.margin_per_trade_pct / 100.0

    def update_realized_pnl(self, pnl: float, charges: float) -> None:
        """Update realized P&L and charges from a closed trade.
        
        Args:
            pnl: NET P&L (gross - charges already deducted by PNLEngine)
            charges: Total charges for this trade (tracked separately for reporting)
        """
        self.realized_pnl += pnl
        self.charges += charges
        self.cash += pnl  # pnl is already NET (charges already subtracted)

    def update_unrealized_pnl(self, pnl: float) -> None:
        """Update unrealized P&L from position mark."""
        self.unrealized_pnl = pnl

    def block_margin(self, margin: float, force: bool = False) -> bool:
        """Block margin for a new position.

        Returns True if margin available, False otherwise.

        ``force=True`` over-allocates even when the margin exceeds what is
        available (used by the LIVE entry path when the broker has ALREADY
        confirmed the fill: the real position exists regardless, so the
        accounting ledger must reflect it and never orphan the broker side).
        """
        if margin > self.available_margin and not force:
            return False
        self.used_margin += margin
        return True

    def release_margin(self, margin: float) -> None:
        """Release margin when position closes."""
        self.used_margin = max(0, self.used_margin - margin)

    def can_open_position(self, margin_required: float) -> bool:
        """Check if account can support a new position."""
        return margin_required <= self.available_margin

    def get_snapshot(self) -> AccountSnapshot:
        """Get current account state."""
        used = (self._broker_used_margin if self._broker_used_margin is not None
                else self.used_margin)
        return AccountSnapshot(
            starting_capital=self.starting_capital,
            cash=self.cash,
            realized_pnl=self.realized_pnl,
            unrealized_pnl=self.unrealized_pnl,
            gross_pnl=self.gross_pnl,
            charges=self.charges,
            net_pnl=self.net_pnl,
            equity=self.equity,
            used_margin=used,
            available_margin=self.available_margin,
        )

    def snapshot(self) -> dict:
        """Get account state for persistence."""
        used = (self._broker_used_margin if self._broker_used_margin is not None
                else self.used_margin)
        return {
            "starting_capital": self.starting_capital,
            "cash": self.cash,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "charges": self.charges,
            "used_margin": used,
            "equity": self.equity,
            "available_margin": self.available_margin,
            "net_pnl": self.net_pnl,
            "equity_source": "broker" if self.broker_reported else "local",
        }

    def restore(self, data: dict) -> None:
        """Restore account state from persistence.
        
        NOTE: starting_capital is NEVER restored from saved state.
        It always comes from config (set during __init__).
        This prevents stale capital values from overwriting config.
        """
        self.realized_pnl = data.get("realized_pnl", 0.0)
        self.unrealized_pnl = data.get("unrealized_pnl", 0.0)
        self.charges = data.get("charges", 0.0)
        self.used_margin = data.get("used_margin", 0.0)
        self.cash = data.get("cash", self.starting_capital)
