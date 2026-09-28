"""Trade Ledger - derived projection of canonical trades in trading.db.

The ledger reads/writes derived ``trades_analytics`` / ``trade_legs`` tables
through the central :class:`Database` connection so that all modules share
one process-wide SQLite connection and write lock — eliminating
``database is locked`` errors under concurrent load.
"""
from __future__ import annotations
import json
import threading
import time
import uuid
from typing import Optional, Any
from dataclasses import dataclass, field, asdict
from enum import Enum

from persistence.database import Database


class TradeStatus(Enum):
    OPEN = "OPEN"
    PARTIALLY_CLOSED = "PARTIALLY_CLOSED"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"
    REJECTED = "REJECTED"


class TradeSide(Enum):
    LONG = "LONG"
    SHORT = "SHORT"


@dataclass
class TradeRecord:
    """The authoritative record for a single trade."""
    trade_id: str = ""
    strategy_id: str = ""
    strategy_version: str = "v1"
    parameter_hash: Optional[str] = None
    instrument: str = ""
    side: str = "LONG"
    status: str = "OPEN"
    signal_time: Optional[float] = None
    trigger_time: Optional[float] = None
    order_time: Optional[float] = None
    first_fill_time: Optional[float] = None
    last_exit_fill_time: Optional[float] = None
    entry_quantity: int = 0
    filled_quantity: int = 0
    remaining_quantity: int = 0
    entry_price: Optional[float] = None
    average_entry_price: Optional[float] = None
    initial_stop: Optional[float] = None
    initial_risk: Optional[float] = None
    exit_price: Optional[float] = None
    average_exit_price: Optional[float] = None
    exit_quantity: int = 0
    exit_reason: Optional[str] = None
    entry_reason: Optional[str] = None
    entry_dema: Optional[float] = None
    entry_atr: Optional[float] = None
    entry_dema_atr: Optional[float] = None
    entry_htf_value: Optional[float] = None
    entry_bid: Optional[float] = None
    entry_ask: Optional[float] = None
    entry_spread: Optional[float] = None
    entry_slippage: Optional[float] = None
    entry_order_id: Optional[str] = None
    exit_order_id: Optional[str] = None
    position_id: Optional[str] = None
    session_id: Optional[str] = None
    replay_id: Optional[str] = None
    gross_pnl: Optional[float] = None
    fees: Optional[float] = None
    slippage_cost: Optional[float] = None
    net_pnl: Optional[float] = None
    return_pct: Optional[float] = None
    r_multiple: Optional[float] = None
    r_status: str = "UNDEFINED"
    multiplier: float = 1.0
    mfe: Optional[float] = None
    mae: Optional[float] = None
    max_favorable_price: Optional[float] = None
    max_adverse_price: Optional[float] = None
    duration_seconds: Optional[float] = None
    duration_minutes: Optional[float] = None
    closed_at: Optional[float] = None
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


@dataclass
class TradeLeg:
    """Individual fill leg within a trade."""
    leg_id: str
    trade_id: str
    fill_id: str
    order_id: str
    side: str
    quantity: int
    price: float
    timestamp: float
    slippage: float = 0.0
    spread: float = 0.0
    is_entry: bool = True


class TradeLedger:
    """Authoritative trade lifecycle management."""

    def __init__(self, db_path: str = "trading.db"):
        self._db = Database(db_path)
        self._lock = threading.Lock()
        self._open_trades: dict[str, TradeRecord] = {}
        self._load_open_trades()

    def _get_conn(self):
        """Get the shared process-wide connection from the central Database."""
        return self._db.get_conn()

    def _load_open_trades(self):
        """Load open trades from database on startup."""
        try:
            rows = self._db.query(
                "SELECT * FROM trades_analytics WHERE status IN ('OPEN', 'PARTIALLY_CLOSED')"
            )
            for row in rows:
                trade = TradeRecord(**{k: k_val for k, k_val in row.items() if hasattr(TradeRecord, k)})
                self._open_trades[trade.trade_id] = trade
        except Exception:
            pass

    def create_trade(self, strategy_id: str, instrument: str, side: str,
                     entry_quantity: int, signal_time: float,
                     trigger_price: float, stop_price: float,
                     entry_reason: str = "signal",
                     strategy_version: str = "v1",
                     parameter_hash: Optional[str] = None,
                     session_id: Optional[str] = None,
                     replay_id: Optional[str] = None,
                     trade_id: Optional[str] = None,
                     position_id: Optional[str] = None,
                     **kwargs) -> TradeRecord:
        """Create a new trade record on signal/entry.

        ``trade_id`` must be supplied by the canonical lifecycle owner. The
        ledger is only a derived projection and never invents trade identity.
        """
        if trade_id is None:
            raise ValueError("trade_id is required for a ledger projection")
        initial_risk = abs(trigger_price - stop_price) if stop_price else None

        trade = TradeRecord(
            trade_id=trade_id,
            strategy_id=strategy_id,
            strategy_version=strategy_version,
            parameter_hash=parameter_hash,
            instrument=instrument,
            side=side,
            status="OPEN",
            signal_time=signal_time,
            entry_quantity=entry_quantity,
            filled_quantity=0,
            remaining_quantity=entry_quantity,
            initial_stop=stop_price,
            initial_risk=initial_risk,
            entry_reason=entry_reason,
            position_id=position_id,
            session_id=session_id,
            replay_id=replay_id,
            created_at=time.time(),
            updated_at=time.time(),
            **{k: v for k, v in kwargs.items() if k in TradeRecord.__dataclass_fields__}
        )

        with self._lock:
            self._open_trades[trade_id] = trade
            self._save_trade(trade)

        return trade

    def record_fill(self, trade_id: str, fill_id: str, order_id: str,
                    side: str, quantity: int, price: float, timestamp: float,
                    is_entry: bool = True, slippage: float = 0.0,
                    spread: float = 0.0) -> TradeLeg:
        """Record an individual fill leg.

        Idempotent on ``fill_id``: if a leg for this fill_id already exists
        (e.g. the same fill is replayed after a crash before the engine's
        durable dedup mark), no second leg is written and the financial
        effects are NOT re-applied (would otherwise double filled_quantity /
        recompute P&L).
        """
        with self._lock:
            existing = self._get_leg_fill_id(fill_id)
            if existing is not None:
                return existing
            leg_id = str(uuid.uuid4())
            leg = TradeLeg(
                leg_id=leg_id,
                trade_id=trade_id,
                fill_id=fill_id,
                order_id=order_id,
                side=side,
                quantity=quantity,
                price=price,
                timestamp=timestamp,
                slippage=slippage,
                spread=spread,
                is_entry=is_entry,
            )
            self._save_leg(leg)
            # The in-memory cache is NEVER authoritative. If this trade is not
            # cached (e.g. lost on restart, heal, or a projection that was never
            # loaded), load the canonical aggregate from the DB and continue so
            # the exit accounting / P&L is applied to the persistent copy rather
            # than silently dropped (the baa04bef divergence pattern).
            trade = self._open_trades.get(trade_id)
            if trade is None:
                trade = self._get_db_trade(trade_id)
            if trade is not None:
                if is_entry:
                    self._update_entry_fill(trade, leg)
                else:
                    self._update_exit_fill(trade, leg)
                trade.updated_at = time.time()
                self._save_trade(trade)
                # Keep the open-trades cache faithful to the DB aggregate
                # (OPEN/PARTIALLY_CLOSED stay cached; CLOSED is evicted).
                if trade.status == "CLOSED":
                    self._open_trades.pop(trade_id, None)
                else:
                    self._open_trades[trade_id] = trade
            return leg

    def _get_leg_fill_id(self, fill_id: str) -> TradeLeg | None:
        """Return the existing leg for a fill_id, or None if not recorded."""
        try:
            row = self._db.query_one(
                "SELECT * FROM trade_legs WHERE fill_id=?", (fill_id,)
            )
        except Exception:
            return None
        if row is None:
            return None
        fields = set(TradeLeg.__dataclass_fields__.keys())
        return TradeLeg(**{k: v for k, v in row.items() if k in fields})

    def _update_entry_fill(self, trade: TradeRecord, leg: TradeLeg):
        """Update trade with entry fill."""
        if trade.filled_quantity == 0:
            trade.first_fill_time = leg.timestamp
            trade.entry_price = leg.price
            trade.entry_order_id = leg.order_id

        # Weighted average entry
        total_cost = (trade.average_entry_price or 0) * trade.filled_quantity + leg.price * leg.quantity
        trade.filled_quantity += leg.quantity
        trade.remaining_quantity = trade.entry_quantity - trade.filled_quantity
        trade.average_entry_price = total_cost / trade.filled_quantity if trade.filled_quantity > 0 else 0

    def _update_exit_fill(self, trade: TradeRecord, leg: TradeLeg):
        """Update trade with exit fill."""
        trade.last_exit_fill_time = leg.timestamp
        trade.exit_order_id = leg.order_id

        # Weighted average exit
        prev_exit_value = (trade.average_exit_price or 0) * trade.exit_quantity
        trade.exit_quantity += leg.quantity
        trade.remaining_quantity = trade.filled_quantity - trade.exit_quantity

        if trade.exit_quantity > 0:
            trade.average_exit_price = (prev_exit_value + leg.price * leg.quantity) / trade.exit_quantity

        # Check if fully closed
        if trade.remaining_quantity <= 0:
            trade.remaining_quantity = 0
            trade.status = "CLOSED"
            trade.closed_at = trade.last_exit_fill_time or time.time()
            trade.exit_price = trade.average_exit_price

            # Calculate P&L (use filled_quantity = total position size closed)
            if trade.side == "LONG":
                gross = (trade.exit_price - trade.average_entry_price) * trade.filled_quantity * trade.multiplier
            else:
                gross = (trade.average_entry_price - trade.exit_price) * trade.filled_quantity * trade.multiplier

            trade.gross_pnl = gross
            trade.net_pnl = gross - (trade.fees or 0)

            # Duration
            if trade.first_fill_time and trade.closed_at:
                trade.duration_seconds = trade.closed_at - trade.first_fill_time
                trade.duration_minutes = trade.duration_seconds / 60.0

            # R-multiple
            if trade.initial_risk and trade.initial_risk > 0:
                trade.r_multiple = trade.net_pnl / (trade.initial_risk * trade.entry_quantity)
                trade.r_status = "DEFINED"

            # A fully-closed trade must be removed from the open-trades cache so
            # get_open_trades() never reports a CLOSED round trip as OPEN
            # (mirrors close_trade).  Without this, an SL/reversal exit fill
            # marks status=CLOSED in the DB but leaves a stale OPEN entry in
            # _open_trades, desyncing /api/analytics/open-trades from the
            # ledger rows.
            if trade.trade_id in self._open_trades:
                del self._open_trades[trade.trade_id]

        elif trade.exit_quantity > 0 and trade.remaining_quantity > 0:
            trade.status = "PARTIALLY_CLOSED"

    def close_trade(self, trade_id: str, exit_reason: str = "exit",
                    gross_pnl: Optional[float] = None,
                    net_pnl: Optional[float] = None,
                    fees: Optional[float] = None) -> Optional[TradeRecord]:
        """Manually close a trade and apply the authoritative P&L override.

        The trade may already be CLOSED by an exit fill (which removes it from
        the open-trades cache).  In that case fall back to the DB copy so the
        fee-model-based authoritative P&L/gross/fees still get stamped on the
        ledger row (TradeCloseManager calls record_fill(exit) then close_trade).
        """
        with self._lock:
            trade = self._open_trades.get(trade_id)
            if not trade:
                trade = self._get_db_trade(trade_id)
            if trade:
                trade.status = "CLOSED"
                trade.exit_reason = exit_reason
                # Preserve the fill-derived close time (set in _update_exit_fill
                # from last_exit_fill_time); only fall back to wall clock when
                # the trade was closed without an exit fill (replay/history then
                # carries the real trading-day time, not the run time).
                trade.closed_at = trade.closed_at or time.time()
                trade.updated_at = time.time()
                if gross_pnl is not None:
                    trade.gross_pnl = gross_pnl
                if net_pnl is not None:
                    trade.net_pnl = net_pnl
                if fees is not None:
                    trade.fees = fees
                if trade.first_fill_time and trade.closed_at:
                    trade.duration_seconds = trade.closed_at - trade.first_fill_time
                    trade.duration_minutes = trade.duration_seconds / 60.0
                self._save_trade(trade)
                if trade.trade_id in self._open_trades:
                    del self._open_trades[trade.trade_id]
        return trade

    def update_mfe_mae(self, trade_id: str, current_price: float) -> None:
        """Update MFE/MAE for an open trade."""
        with self._lock:
            trade = self._open_trades.get(trade_id)
            if not trade or not trade.average_entry_price:
                return

            if trade.side == "LONG":
                favorable = current_price - trade.average_entry_price
                adverse = trade.average_entry_price - current_price
            else:
                favorable = trade.average_entry_price - current_price
                adverse = current_price - trade.average_entry_price

            changed = False
            if trade.mfe is None or favorable > trade.mfe:
                trade.mfe = favorable
                trade.max_favorable_price = current_price
                changed = True

            if trade.mae is None or adverse > trade.mae:
                trade.mae = adverse
                trade.max_adverse_price = current_price
                changed = True

            # Only persist when MFE/MAE actually moved — avoids a DB write (and
            # commit) on every tick for every open position.
            if changed:
                trade.updated_at = time.time()
                self._save_trade(trade)

    def get_trade(self, trade_id: str) -> Optional[TradeRecord]:
        """Get a trade by ID."""
        if trade_id in self._open_trades:
            return self._open_trades[trade_id]
        return self._get_db_trade(trade_id)

    def _get_db_trade(self, trade_id: str) -> Optional[TradeRecord]:
        """Load a trade directly from the DB (skipping the open-trades cache).

        Used by get_trade and close_trade so CLOSED trades that have been
        purged from the in-memory open cache remain queryable/overridable.
        """
        try:
            row = self._db.query_one(
                "SELECT * FROM trades_analytics WHERE trade_id = ?", (trade_id,)
            )
        except Exception:
            return None
        if row:
            return TradeRecord(**{k: v for k, v in row.items() if hasattr(TradeRecord, k)})
        return None

    def get_open_trades(self, strategy_id: Optional[str] = None,
                        instrument: Optional[str] = None) -> list[TradeRecord]:
        """Get all open trades."""
        trades = list(self._open_trades.values())
        if strategy_id:
            trades = [t for t in trades if t.strategy_id == strategy_id]
        if instrument:
            trades = [t for t in trades if t.instrument == instrument]
        return trades

    def get_closed_trades(self, strategy_id: Optional[str] = None,
                          instrument: Optional[str] = None,
                          limit: int = 1000) -> list[TradeRecord]:
        """Get closed trades."""
        query = "SELECT * FROM trades_analytics WHERE status = 'CLOSED'"
        params: list[Any] = []
        if strategy_id:
            query += " AND strategy_id = ?"
            params.append(strategy_id)
        if instrument:
            query += " AND instrument = ?"
            params.append(instrument)
        query += " ORDER BY closed_at DESC LIMIT ?"
        params.append(limit)
        rows = self._db.query(query, params)
        fields = set(TradeRecord.__dataclass_fields__.keys())
        return [TradeRecord(**{k: v for k, v in row.items() if k in fields}) for row in rows]

    def get_trades_for_strategy(self, strategy_id: str,
                                status: Optional[str] = None) -> list[TradeRecord]:
        """Get all trades for a strategy."""
        if status:
            rows = self._db.query(
                "SELECT * FROM trades_analytics WHERE strategy_id = ? AND status = ? ORDER BY created_at DESC",
                (strategy_id, status)
            )
        else:
            rows = self._db.query(
                "SELECT * FROM trades_analytics WHERE strategy_id = ? ORDER BY created_at DESC",
                (strategy_id,)
            )
        fields = set(TradeRecord.__dataclass_fields__.keys())
        return [TradeRecord(**{k: v for k, v in row.items() if k in fields}) for row in rows]

    def get_legs_for_trade(self, trade_id: str) -> list[TradeLeg]:
        """Get all fill legs for a trade."""
        rows = self._db.query(
            "SELECT * FROM trade_legs WHERE trade_id = ? ORDER BY timestamp",
            (trade_id,)
        )
        fields = set(TradeLeg.__dataclass_fields__.keys())
        return [TradeLeg(**{k: v for k, v in row.items() if k in fields}) for row in rows]

    def count_trades(self, strategy_id: Optional[str] = None,
                     status: Optional[str] = None) -> int:
        """Count trades."""
        query = "SELECT COUNT(*) FROM trades_analytics WHERE 1=1"
        params: list[Any] = []
        if strategy_id:
            query += " AND strategy_id = ?"
            params.append(strategy_id)
        if status:
            query += " AND status = ?"
            params.append(status)
        return self._db.scalar(query, params)

    def _save_trade(self, trade: TradeRecord) -> None:
        """Save trade to database (idempotent UPSERT via shared transaction)."""
        d = trade.__dict__.copy()
        columns = list(d.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)
        updates = ", ".join([f"{c} = excluded.{c}" for c in columns if c != "trade_id"])

        with self._db.transaction() as conn:
            conn.execute(
                f"""INSERT INTO trades_analytics ({col_names}) VALUES ({placeholders})
                    ON CONFLICT(trade_id) DO UPDATE SET {updates}""",
                list(d.values())
            )

    def _save_leg(self, leg: TradeLeg) -> None:
        """Save trade leg to database via shared transaction."""
        d = leg.__dict__.copy()
        d["is_entry"] = 1 if d["is_entry"] else 0
        columns = list(d.keys())
        placeholders = ", ".join(["?"] * len(columns))
        col_names = ", ".join(columns)

        with self._db.transaction() as conn:
            conn.execute(
                f"INSERT OR IGNORE INTO trade_legs ({col_names}) VALUES ({placeholders})",
                list(d.values())
            )
