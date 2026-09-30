"""Position manager for tracking open positions."""
from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

from execution.models import Fill


class PositionSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


class PositionStatus(Enum):
    OPEN = "open"
    CLOSED = "closed"


@dataclass
class Position:
    """Represents a trading position.

    position_id and trade_id are separate immutable identities.
    entry_signal_id links to the Signal that triggered this trade.
    exit_signal_id links to the Signal that closed this trade (if signal-based exit).
    """
    position_id: str
    strategy_id: str
    instrument: str
    side: PositionSide
    quantity: int
    average_entry: float
    entry_timestamp: float
    entry_fill_ids: list[str] = field(default_factory=list)
    stop_price: Optional[float] = None
    current_mark: Optional[float] = None
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    margin: float = 0.0
    trade_id: Optional[str] = None
    exit_reason: Optional[str] = None
    exit_fills: list[Fill] = field(default_factory=list)
    status: PositionStatus = PositionStatus.OPEN
    multiplier: float = 1.0
    entry_signal_id: Optional[str] = None
    exit_signal_id: Optional[str] = None
    # ── Position-owned SL (the only stop-loss mechanism) ──────────────────
    # ``stop_price`` above is the ONE authority for the stop level.  ``sl_state``
    # is the local monitor lifecycle only — it never means "a broker order is
    # resting".  There is deliberately no broker SL order id: no broker-side
    # protective order exists in this system.
    #   NONE | ARMED | TRIGGERED | EXITING | CLOSED | SL_UNAVAILABLE
    sl_state: Optional[str] = None
    sl_trigger_price: Optional[float] = None  # market price that crossed stop
    sl_protected_at: Optional[float] = None  # engine clock when the SL was armed
    entry_order_id: Optional[str] = None     # broker entry order id
    exit_order_id: Optional[str] = None      # broker exit order id, once exiting
    position_generation: int = 0
    exit_started: bool = False
    lifecycle_id: Optional[str] = None

    @property
    def is_long(self) -> bool:
        return self.side == PositionSide.LONG

    @property
    def is_short(self) -> bool:
        return self.side == PositionSide.SHORT

    @property
    def is_open(self) -> bool:
        return self.status == PositionStatus.OPEN

    def update_mark(self, price: float) -> None:
        """Update current mark price and recalculate unrealized P&L."""
        self.current_mark = price
        if self.is_long:
            self.unrealized_pnl = (price - self.average_entry) * self.quantity * self.multiplier
        else:
            self.unrealized_pnl = (self.average_entry - price) * self.quantity * self.multiplier

    def snapshot(self) -> dict:
        """Get position state for persistence."""
        return {
            "position_id": self.position_id,
            "strategy_id": self.strategy_id,
            "instrument": self.instrument,
            "side": self.side.value,
            "quantity": self.quantity,
            "average_entry": self.average_entry,
            "entry_timestamp": self.entry_timestamp,
            "entry_fill_ids": self.entry_fill_ids,
            "stop_price": self.stop_price,
            "current_mark": self.current_mark,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": self.unrealized_pnl,
            "margin": self.margin,
            "trade_id": self.trade_id,
            "exit_reason": self.exit_reason,
            "exit_fills": [
                {
                    "fill_id": f.fill_id,
                    "order_id": f.order_id,
                    "instrument": f.instrument,
                    "side": f.side,
                    "quantity": f.quantity,
                    "price": f.price,
                    "timestamp": f.timestamp,
                    "strategy_id": f.strategy_id,
                    "multiplier": f.multiplier,
                }
                for f in self.exit_fills
            ],
            "status": self.status.value,
            "is_open": self.is_open,
            "multiplier": self.multiplier,
            "entry_signal_id": self.entry_signal_id,
            "exit_signal_id": self.exit_signal_id,
            "sl_state": self.sl_state,
            "sl_trigger_price": self.sl_trigger_price,
            "sl_protected_at": self.sl_protected_at,
            "entry_order_id": self.entry_order_id,
            "exit_order_id": self.exit_order_id,
            "position_generation": self.position_generation,
            "exit_started": self.exit_started,
            "lifecycle_id": self.lifecycle_id or self.trade_id,
        }


class PositionManager:
    """Manages open positions for all strategies.
    
    Supports multiple strategies independently.
    Each strategy has its own position.
    """

    def __init__(self):
        self._positions: dict[str, Position] = {}
        self._closed_positions: list[Position] = []
        self._lock = threading.Lock()
        self._generation_counters: dict[tuple[str, str], int] = {}

    def allocate_generation(self, strategy_id: str, instrument: str) -> int:
        key = (str(strategy_id), str(instrument))
        with self._lock:
            generation = self._generation_counters.get(key, 0) + 1
            self._generation_counters[key] = generation
            return generation

    def open_position(
        self,
        fill: Fill,
        multiplier: float = 1.0,
        stop_price: Optional[float] = None,
        margin: float = 0.0,
        entry_signal_id: Optional[str] = None,
        trade_id: Optional[str] = None,
        position_generation: int = 0,
        entry_order_id: Optional[str] = None,
    ) -> Position:
        """Open a new position from an entry fill."""
        trade_id = trade_id or fill.trade_id
        if not trade_id:
            raise ValueError("trade_id is required to open a position")
        if not position_generation:
            position_generation = self.allocate_generation(fill.strategy_id, fill.instrument)
        position = Position(
            position_id=str(uuid.uuid4()),
            strategy_id=fill.strategy_id,
            instrument=fill.instrument,
            side=PositionSide.LONG if fill.side == "BUY" else PositionSide.SHORT,
            quantity=fill.quantity,
            average_entry=fill.price,
            entry_timestamp=fill.timestamp,
            entry_fill_ids=[fill.fill_id],
            stop_price=stop_price,
            current_mark=fill.price,
            margin=margin,
            trade_id=trade_id,
            multiplier=multiplier,
            entry_signal_id=entry_signal_id or fill.entry_signal_id,
            position_generation=int(position_generation or 0),
            lifecycle_id=trade_id,
            entry_order_id=entry_order_id or fill.order_id,
        )
        with self._lock:
            self._positions[position.position_id] = position
        return position

    def close_position(
        self,
        position_id: str,
        fill: Fill,
        reason: str,
        exit_signal_id: Optional[str] = None,
    ) -> Position:
        """Close an existing position with an exit fill."""
        with self._lock:
            position = self._positions.get(position_id)
            if not position:
                raise ValueError(f"Position {position_id} not found")
            if position.exit_started and position.quantity <= 0:
                raise ValueError(f"Position {position_id} is already flat")

            position.exit_fills.append(fill)
            position.exit_reason = reason
            position.status = PositionStatus.CLOSED
            # INVARIANT 5 — a closed position's SL is terminal at the data
            # layer too, so no code path can leave it looking ARMED.
            position.sl_state = "CLOSED"
            if exit_signal_id:
                position.exit_signal_id = exit_signal_id

            # Calculate realized P&L
            if position.is_long:
                position.realized_pnl = (
                    (fill.price - position.average_entry)
                    * position.quantity
                    * position.multiplier
                )
            else:
                position.realized_pnl = (
                    (position.average_entry - fill.price)
                    * position.quantity
                    * position.multiplier
                )

            self._closed_positions.append(position)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-250:]
            del self._positions[position_id]
            return position

    def abandon_stale_position(self, position_id: str) -> Optional[Position]:
        """Close a local OPEN row the broker does not confirm (startup sync).

        No synthetic fill is recorded: the broker says this position does not
        exist, so there was no exit to price.  ``sl_state`` goes terminal in
        the same step (INVARIANT 5).
        """
        with self._lock:
            position = self._positions.get(position_id)
            if position is None:
                return None
            position.status = PositionStatus.CLOSED
            position.sl_state = "CLOSED"
            position.sl_trigger_price = None
            position.exit_reason = "startup_broker_flat"
            position.quantity = 0
            self._closed_positions.append(position)
            if len(self._closed_positions) > 500:
                self._closed_positions = self._closed_positions[-250:]
            del self._positions[position_id]
            return position

    def restore_open_position(self, position: Position) -> Position:
        """Reinstate a canonical open row for narrowly validated fill recovery.

        A stale broker-flat reconciliation can remove the in-memory owner
        before its already-filled exit is routed.  Recovery code supplies the
        position reconstructed from the durable open-position row; this method
        replaces any stale closed snapshot with that exact identity.
        """
        if not position or not position.position_id or not position.is_open:
            raise ValueError("restore_open_position requires an OPEN position")
        with self._lock:
            conflict = next((p for p in self._positions.values()
                             if p.instrument == position.instrument
                             and p.is_open
                             and p.position_id != position.position_id), None)
            if conflict is not None:
                raise ValueError("another open position already owns this instrument")
            self._closed_positions = [
                p for p in self._closed_positions
                if p.position_id != position.position_id
            ]
            self._positions[position.position_id] = position
            key = (position.strategy_id, position.instrument)
            self._generation_counters[key] = max(
                self._generation_counters.get(key, 0),
                int(position.position_generation or 0))
            return position

    def reduce_position(
        self,
        position_id: str,
        fill: Fill,
        reason: str,
        exit_signal_id: Optional[str] = None,
    ) -> int:
        """C2 — apply a PARTIAL exit: reduce an OPEN position by the exit
        fill's quantity, release margin proportionally, and keep it OPEN for
        the remaining quantity.

        The entry average is untouched (entry legs keep their own average);
        only the held quantity and the margin backing it shrink.  Returns the
        remaining quantity after the leg (caller closes the position when it
        reaches 0).
        """
        with self._lock:
            position = self._positions.get(position_id)
            if not position:
                raise ValueError(f"Position {position_id} not found")
            exit_qty = int(fill.quantity or 0)
            if exit_qty <= 0 or exit_qty >= int(position.quantity or 0):
                raise ValueError(
                    f"reduce_position requires 0 < exit_qty < position qty "
                    f"({exit_qty} vs {position.quantity})")
            position.exit_fills.append(fill)
            position.exit_reason = reason
            if exit_signal_id:
                position.exit_signal_id = exit_signal_id
            remaining = int(position.quantity) - exit_qty
            # Release margin proportional to the quantity that left.
            if position.margin:
                position.margin = position.margin * remaining / (remaining + exit_qty)
            position.quantity = remaining
            return remaining

    def update_marks(self, prices: dict[str, float]) -> None:
        """Update mark prices for all open positions."""
        with self._lock:
            for pos in self._positions.values():
                price = prices.get(pos.instrument)
                if price is not None:
                    pos.update_mark(price)

    def get_position(self, position_id: str) -> Optional[Position]:
        """Get position by ID (open or recently closed)."""
        with self._lock:
            if position_id in self._positions:
                return self._positions[position_id]
            for p in self._closed_positions:
                if p.position_id == position_id:
                    return p
            return None

    def get_positions_by_strategy(self, strategy_id: str) -> list[Position]:
        """Get all positions for a strategy."""
        with self._lock:
            return [
                p for p in self._positions.values()
                if p.strategy_id == strategy_id
            ]

    def get_positions_by_instrument(self, instrument: str) -> list[Position]:
        """Get all positions for an instrument."""
        with self._lock:
            return [
                p for p in self._positions.values()
                if p.instrument == instrument
            ]

    @property
    def open_positions(self) -> list[Position]:
        """All open positions."""
        with self._lock:
            return list(self._positions.values())

    @property
    def closed_positions(self) -> list[Position]:
        """All closed positions."""
        with self._lock:
            return list(self._closed_positions)

    def snapshot(self) -> dict:
        """Get position manager state for persistence."""
        with self._lock:
            return {
                "open_positions": {
                    pos.position_id: pos.snapshot()
                    for pos in self._positions.values()
                },
                "closed_positions": [pos.snapshot() for pos in self._closed_positions],
                "generation_counters": {
                    f"{sid}|{instrument}": generation
                    for (sid, instrument), generation in self._generation_counters.items()
                },
            }

    def restore(self, data: dict) -> None:
        """Restore position manager state from persistence."""
        with self._lock:
            # Clear existing state before restoring — prevents phantom
            # positions accumulating on double-restore (A12).
            self._positions.clear()
            self._closed_positions.clear()
            self._generation_counters = {}
            for key, generation in data.get("generation_counters", {}).items():
                sid, sep, instrument = str(key).partition("|")
                if sep:
                    self._generation_counters[(sid, instrument)] = int(generation or 0)

            # Restore open positions.
            # BUG FIX: Always key by pos.position_id (the UUID), NOT by
            # the dict key (pid) from the state file.  Older state files
            # used strategy names (e.g. "silver_02") as dict keys, but
            # close_position() looks up by position_id (UUID).  Using the
            # wrong key causes "Position not found" in heal/reconciliation.
            for pid, pos_data in data.get("open_positions", {}).items():
                exit_fills = []
                for f_data in pos_data.get("exit_fills", []):
                    exit_fills.append(Fill(
                        fill_id=f_data["fill_id"],
                        order_id=f_data.get("order_id", ""),
                        instrument=f_data["instrument"],
                        side=f_data["side"],
                        quantity=f_data["quantity"],
                        price=f_data["price"],
                        timestamp=f_data["timestamp"],
                        strategy_id=f_data.get("strategy_id", ""),
                        multiplier=f_data.get("multiplier", 1.0),
                    ))
                pos = Position(
                    position_id=pos_data["position_id"],
                    strategy_id=pos_data["strategy_id"],
                    instrument=pos_data["instrument"],
                    side=PositionSide(pos_data["side"]),
                    quantity=pos_data["quantity"],
                    average_entry=pos_data["average_entry"],
                    entry_timestamp=pos_data["entry_timestamp"],
                    entry_fill_ids=pos_data.get("entry_fill_ids", []),
                    stop_price=pos_data.get("stop_price"),
                    current_mark=pos_data.get("current_mark"),
                    realized_pnl=pos_data.get("realized_pnl", 0.0),
                    unrealized_pnl=pos_data.get("unrealized_pnl", 0.0),
                    margin=pos_data.get("margin", 0.0),
                    trade_id=pos_data.get("trade_id"),
                    exit_reason=pos_data.get("exit_reason"),
                    exit_fills=exit_fills,
                    status=PositionStatus(pos_data.get("status", "open")),
                    multiplier=pos_data.get("multiplier", 1.0),
                    entry_signal_id=pos_data.get("entry_signal_id"),
                    exit_signal_id=pos_data.get("exit_signal_id"),
                    sl_state=pos_data.get("sl_state"),
                    sl_trigger_price=pos_data.get("sl_trigger_price"),
                    sl_protected_at=pos_data.get("sl_protected_at"),
                    entry_order_id=pos_data.get("entry_order_id"),
                    exit_order_id=pos_data.get("exit_order_id"),
                    position_generation=int(pos_data.get("position_generation", 0) or 0),
                    exit_started=bool(pos_data.get("exit_started", False)),
                    lifecycle_id=pos_data.get("lifecycle_id") or pos_data.get("trade_id"),
                )
                self._positions[pos.position_id] = pos
                key = (pos.strategy_id, pos.instrument)
                self._generation_counters[key] = max(
                    self._generation_counters.get(key, 0), pos.position_generation)

            # Restore closed positions (A11) — ensures reconciliation has
            # exit-fill linkage after restart, eliminating the spurious
            # "N fill(s) in DB not linked" warning.
            for cp_data in data.get("closed_positions", []):
                exit_fills = []
                for f_data in cp_data.get("exit_fills", []):
                    exit_fills.append(Fill(
                        fill_id=f_data["fill_id"],
                        order_id=f_data.get("order_id", ""),
                        instrument=f_data["instrument"],
                        side=f_data["side"],
                        quantity=f_data["quantity"],
                        price=f_data["price"],
                        timestamp=f_data["timestamp"],
                        strategy_id=f_data.get("strategy_id", ""),
                        multiplier=f_data.get("multiplier", 1.0),
                    ))
                pos = Position(
                    position_id=cp_data["position_id"],
                    strategy_id=cp_data["strategy_id"],
                    instrument=cp_data["instrument"],
                    side=PositionSide(cp_data["side"]),
                    quantity=cp_data["quantity"],
                    average_entry=cp_data["average_entry"],
                    entry_timestamp=cp_data["entry_timestamp"],
                    entry_fill_ids=cp_data.get("entry_fill_ids", []),
                    stop_price=cp_data.get("stop_price"),
                    current_mark=cp_data.get("current_mark"),
                    realized_pnl=cp_data.get("realized_pnl", 0.0),
                    unrealized_pnl=cp_data.get("unrealized_pnl", 0.0),
                    margin=cp_data.get("margin", 0.0),
                    trade_id=cp_data.get("trade_id"),
                    exit_reason=cp_data.get("exit_reason"),
                    exit_fills=exit_fills,
                    status=PositionStatus(cp_data.get("status", "closed")),
                    multiplier=cp_data.get("multiplier", 1.0),
                    entry_signal_id=cp_data.get("entry_signal_id"),
                    exit_signal_id=cp_data.get("exit_signal_id"),
                    sl_state=cp_data.get("sl_state"),
                    sl_trigger_price=cp_data.get("sl_trigger_price"),
                    sl_protected_at=cp_data.get("sl_protected_at"),
                    entry_order_id=cp_data.get("entry_order_id"),
                    exit_order_id=cp_data.get("exit_order_id"),
                    position_generation=int(cp_data.get("position_generation", 0) or 0),
                    exit_started=bool(cp_data.get("exit_started", False)),
                    lifecycle_id=cp_data.get("lifecycle_id") or cp_data.get("trade_id"),
                )
                self._closed_positions.append(pos)
                key = (pos.strategy_id, pos.instrument)
                self._generation_counters[key] = max(
                    self._generation_counters.get(key, 0), pos.position_generation)


class PositionManagerFacade:
    """Shared routing layer over per-strategy PositionManagers.

    Each StrategyRuntime owns its own PositionManager (its own Position
    objects, margins and P&L). The facade keeps the engine/dashboard/
    reconciliation surface stable: reads aggregate across all runtimes and
    every write routes to the position's owning strategy manager. No mutable
    position state lives on the facade itself.
    """

    def __init__(self, managers: Optional[dict[str, PositionManager]] = None):
        self._managers: dict[str, PositionManager] = dict(managers or {})

    def register(self, strategy_id: str, manager: PositionManager) -> None:
        self._managers[strategy_id] = manager

    def _owner(self, strategy_id: Optional[str]) -> PositionManager:
        if not strategy_id or strategy_id not in self._managers:
            raise ValueError(f"no position manager for strategy {strategy_id!r}")
        return self._managers[strategy_id]

    def _find_owner(self, position_id: str):
        for mgr in self._managers.values():
            pos = next((p for p in mgr._positions.values() if p.position_id == position_id),
                       next((p for p in mgr._closed_positions if p.position_id == position_id), None))
            if pos is not None:
                return mgr, pos
        return None, None

    def open_position(self, fill, multiplier=1.0, stop_price=None, margin=0.0,
                      entry_signal_id=None, trade_id=None,
                      position_generation=0, entry_order_id=None) -> Position:
        """Forward the FULL position identity to the owning strategy manager.

        ``position_generation`` and ``entry_order_id`` MUST be forwarded: the
        SL monitor binds to ``(position_id, position_generation)`` and the
        startup stop resolver reads ``entry_order_id``.  Dropping either here
        silently breaks INVARIANT 7.
        """
        return self._owner(getattr(fill, "strategy_id", None)).open_position(
            fill, multiplier=multiplier, stop_price=stop_price, margin=margin,
            entry_signal_id=entry_signal_id, trade_id=trade_id,
            position_generation=position_generation or 0,
            entry_order_id=entry_order_id,
        )

    def close_position(self, position_id, fill, reason, exit_signal_id=None) -> Position:
        mgr, pos = self._find_owner(position_id)
        if mgr is None:
            raise ValueError(f"Position {position_id} not found")
        return mgr.close_position(position_id, fill, reason, exit_signal_id)

    def abandon_stale_position(self, position_id: str) -> Optional[Position]:
        """Drop a local OPEN row the broker does not confirm (startup sync).

        The broker is the only authority on whether a position exists.  A row
        the broker reports flat cannot have been filled, so it is closed
        WITHOUT a synthetic fill: no exit fill, no fabricated P&L.  Its SL is
        terminal at the same moment (INVARIANT 5).
        """
        mgr, _ = self._find_owner(position_id)
        if mgr is None:
            return None
        return mgr.abandon_stale_position(position_id)

    def restore_open_position(self, position: Position) -> Position:
        """Restore an exact owner through the strategy-scoped manager."""
        return self._owner(getattr(position, "strategy_id", None)).restore_open_position(
            position)

    def get_position(self, position_id: str) -> Optional[Position]:
        _, pos = self._find_owner(position_id)
        return pos

    def get_positions_by_strategy(self, strategy_id: str) -> list[Position]:
        mgr = self._managers.get(strategy_id)
        if mgr is None:
            return []
        return mgr.get_positions_by_strategy(strategy_id)

    def get_positions_by_instrument(self, instrument: str) -> list[Position]:
        out: list[Position] = []
        for mgr in self._managers.values():
            out.extend(mgr.get_positions_by_instrument(instrument))
        return out

    @property
    def open_positions(self) -> list[Position]:
        out: list[Position] = []
        for mgr in self._managers.values():
            out.extend(mgr.open_positions)
        return out

    @property
    def closed_positions(self) -> list[Position]:
        out: list[Position] = []
        for mgr in self._managers.values():
            out.extend(mgr.closed_positions)
        return out

    def update_marks(self, prices: dict[str, float]) -> None:
        for mgr in self._managers.values():
            mgr.update_marks(prices)

    def snapshot(self) -> dict:
        """Aggregated snapshot (same shape as PositionManager.snapshot)."""
        open_positions: dict[str, dict] = {}
        closed_positions: list[dict] = []
        for mgr in self._managers.values():
            data = mgr.snapshot()
            for pid, pos_data in data.get("open_positions", {}).items():
                open_positions[pid] = pos_data
            closed_positions.extend(data.get("closed_positions", []))
        return {
            "open_positions": open_positions,
            "closed_positions": closed_positions,
        }

    def restore(self, data: dict) -> None:
        """Restore per-strategy position managers from a snapshot.

        Accepts either the aggregated engine shape (strategy_id -> manager
        snapshot) or the flat PositionManager shape (keys open_positions/
        closed_positions); rows are routed to the owning strategy manager.
        """
        if not data:
            return
        if "open_positions" in data or "closed_positions" in data:
            # Flat shape: group rows by owning strategy and restore each
            # strategy's manager ONCE. Two passes over the per-row restore
            # (open then closed) would clear the freshly restored open
            # positions again because PositionManager.restore clears first
            # (A12 fix) — a snapshot carrying any closed history silently
            # dropped open positions. Aggregating keeps clear-once semantics.
            by_sid: dict[str, dict] = {}
            for pid, pos_data in (data.get("open_positions") or {}).items():
                sid = (pos_data or {}).get("strategy_id")
                if sid is None:
                    continue
                slot = by_sid.setdefault(sid, {"open_positions": {}, "closed_positions": []})
                slot["open_positions"][pid] = pos_data
            seen_closed: set[str] = set()
            for cp_data in (data.get("closed_positions") or []):
                sid = (cp_data or {}).get("strategy_id")
                if sid is None:
                    continue
                cid = cp_data.get("position_id")
                if cid in seen_closed:
                    continue
                seen_closed.add(cid)
                by_sid.setdefault(sid, {"open_positions": {}, "closed_positions": []})["closed_positions"].append(cp_data)
            for sid, mgr_data in by_sid.items():
                mgr = self._managers.get(sid)
                if mgr is not None:
                    mgr.restore(mgr_data)
            return
        # Per-strategy shape: strategy_id -> manager snapshot.
        for sid, mgr_data in data.items():
            mgr = self._managers.get(sid)
            if mgr is not None:
                mgr.restore(mgr_data)
