"""Persistence layer for system state and trade logging."""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Optional

from .database import Database, shared_connection, shared_path_lock


class PersistenceManager:
    """Manages persistence of system state and trade data.
    
    Uses:
    - JSON for system state (fast, atomic writes)
    - SQLite for trade history (queryable, auditable)
    
    Thread safety: All DB operations are serialized via _lock.
    Single persistent connection avoids connection churn.
    """

    def __init__(
        self,
        state_path: str = "system_state.json",
        db_path: str = "trading.db",
        execution_mode: str = "PAPER",
    ):
        self.state_path = Path(state_path)
        self.db_path = Path(db_path)
        self.execution_mode = str(execution_mode).upper()
        # Public write methods call _get_conn while holding this lock.  It must
        # therefore be re-entrant; a plain Lock deadlocks on the first write.
        # This is the process-wide lock SHARED with every other Database
        # instance writing the same file, so all writers serialize together.
        self._lock = shared_path_lock(self.db_path)

        # Initialize the canonical schema eagerly (legacy behavior: existing
        # databases get their tables/migrations on construction) but do not
        # hold a connection afterwards.  Runtime writes share the
        # process-wide connection created lazily on the first DB write.
        Database(self.db_path).close()

        # Runtime writes open the schema via the shared Database, created
        # lazily on the first DB write.
        self._db: Optional[Database] = None

        # Persistent connection (created lazily through the shared Database)
        self._conn: Optional[sqlite3.Connection] = None

    def _ensure_db(self) -> Database:
        """Return the shared Database handle, opening it if needed.

        Read helpers (``query_one``/``query_all``/``scalar``) live on
        ``Database``, not on this manager, so callers must not reach for
        ``persistence.query_one`` directly.
        """
        if self._db is None:
            self._db = Database(self.db_path)
            self._db.init_schema()
        return self._db

    def query_one(self, sql: str, params: tuple | list = ()) -> Optional[dict]:
        """Lock-protected single-row read (delegates to Database)."""
        return self._ensure_db().query_one(sql, params)

    def scalar(self, sql: str, params: tuple | list = ()):
        """Lock-protected single-value read (delegates to Database)."""
        return self._ensure_db().scalar(sql, params)

    def _get_conn(self) -> sqlite3.Connection:
        """Return the process-wide shared connection (thread-safe).

        The canonical schema is (re-)applied on every lazy connection
        acquisition.  ``Database.__init__`` already idempotently creates every
        table (``CREATE TABLE IF NOT EXISTS`` + guarded ``ALTER`` migrations),
        so even a stale DB file missing tables (e.g. the historical ``no such
        table: signals`` crash) self-heals before any write.

        When the shared connection is STUCK (leaked transaction) the database
        layer recreates it; a cached stale handle would stay closed, so every
        acquisition re-validates identity against the CURRENT registry handle
        before trusting the cache.
        """
        if self._conn is not None:
            try:
                if self._conn is shared_connection(self.db_path):
                    return self._conn
            except Exception:
                pass
            self._conn = None
        with self._lock:
            if self._conn is not None:
                return self._conn
            if self._db is None:
                self._db = Database(self.db_path)
            self._db.init_schema()
            self._conn = self._db.get_conn()
            return self._conn

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        """Run a DB write inside an EXPLICIT transaction on the process-wide
        shared connection.

        Every writer in the process (Database, TradeLedger, EventStore and this
        manager) shares ONE connection and ONE write lock.  Write statements
        MUST go through an explicit BEGIN/COMMIT (rollback on error); a bare
        ``execute()`` would open a pysqlite implicit transaction and, if it
        ever failed, leak an open transaction that would then break the next
        ``BEGIN IMMEDIATE`` from any other component on the same connection.
        """
        with self._lock:
            if self._db is None:
                self._db = Database(self.db_path)
            with self._db.transaction() as conn:
                yield conn

    def _init_db(self) -> None:
        """Initialize SQLite database schema."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path))
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT UNIQUE,
                    strategy_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_timestamp TEXT,
                    entry_price REAL,
                    exit_timestamp TEXT,
                    exit_price REAL,
                    quantity INTEGER,
                    multiplier REAL,
                    gross_pnl REAL,
                    charges REAL,
                    net_pnl REAL,
                    exit_reason TEXT,
                    status TEXT,
                    created_at TEXT DEFAULT (datetime('now')),
                    entry_signal_id TEXT,
                    exit_signal_id TEXT
                );

                CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT UNIQUE,
                    strategy_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity INTEGER,
                    order_type TEXT,
                    price REAL,
                    state TEXT,
                    filled_quantity INTEGER,
                    average_fill_price REAL,
                    created_at TEXT,
                    updated_at TEXT,
                    entry_signal_id TEXT,
                    trade_id TEXT
                );

                CREATE TABLE IF NOT EXISTS fills (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    fill_id TEXT UNIQUE,
                    order_id TEXT,
                    strategy_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity INTEGER,
                    price REAL,
                    timestamp TEXT,
                    entry_signal_id TEXT,
                    trade_id TEXT
                );

                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    signal_id TEXT UNIQUE,
                    strategy_id TEXT NOT NULL,
                    instrument TEXT NOT NULL,
                    side TEXT,
                    signal_type TEXT,
                    timestamp REAL,
                    trigger_price REAL,
                    stop_price REAL,
                    quantity INTEGER,
                    candle_data TEXT,
                    indicator_data TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                );

                CREATE TABLE IF NOT EXISTS trade_signal_link (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id TEXT NOT NULL,
                    signal_id TEXT NOT NULL,
                    relationship_type TEXT NOT NULL,
                    created_at TEXT DEFAULT (datetime('now')),
                    UNIQUE(trade_id, signal_id, relationship_type)
                );

                CREATE TABLE IF NOT EXISTS account_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    equity REAL,
                    realized_pnl REAL,
                    unrealized_pnl REAL,
                    used_margin REAL,
                    available_margin REAL
                );

                CREATE TABLE IF NOT EXISTS events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    event_type TEXT,
                    strategy_id TEXT,
                    instrument TEXT,
                    details TEXT
                );
            """)
            # Run migrations for existing databases
            self._migrate_db(conn)
        finally:
            conn.close()

    def _migrate_db(self, conn: sqlite3.Connection) -> None:
        """Add missing columns to existing tables (idempotent)."""
        cursor = conn.cursor()
        # Check which columns exist on each table
        existing = {}
        for table in ("trades", "orders", "fills"):
            cursor.execute(f"PRAGMA table_info({table})")
            existing[table] = {row[1] for row in cursor.fetchall()}

        # trades table
        if "entry_signal_id" not in existing.get("trades", set()):
            cursor.execute("ALTER TABLE trades ADD COLUMN entry_signal_id TEXT")
        if "exit_signal_id" not in existing.get("trades", set()):
            cursor.execute("ALTER TABLE trades ADD COLUMN exit_signal_id TEXT")

        # orders table
        if "entry_signal_id" not in existing.get("orders", set()):
            cursor.execute("ALTER TABLE orders ADD COLUMN entry_signal_id TEXT")
        if "trade_id" not in existing.get("orders", set()):
            cursor.execute("ALTER TABLE orders ADD COLUMN trade_id TEXT")

        # fills table
        if "entry_signal_id" not in existing.get("fills", set()):
            cursor.execute("ALTER TABLE fills ADD COLUMN entry_signal_id TEXT")
        if "trade_id" not in existing.get("fills", set()):
            cursor.execute("ALTER TABLE fills ADD COLUMN trade_id TEXT")

        conn.commit()

    def save_state(self, state: dict) -> None:
        """Save system state to JSON file (atomic write, thread-safe)."""
        with self._lock:
            self.state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.state_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump(state, f, indent=2, default=str)
            tmp.replace(self.state_path)

    def load_state(self) -> Optional[dict]:
        """Load system state from JSON file."""
        if not self.state_path.exists():
            return None
        try:
            with open(self.state_path) as f:
                return json.load(f)
        except Exception:
            return None

    def save_trade(self, trade: dict) -> None:
        """Save a completed (or in-flight) trade to database.

        Lineage fields (entry/exit order ids, position id, entry/exit fill ids)
        are stored with COALESCE semantics on repeat writes so a later partial
        update (e.g. the exit fill id arriving after the trade row was first
        written at entry time) cannot null-out earlier lineage.
        """
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO trades (
                    trade_id, strategy_id, instrument, side,
                    entry_timestamp, entry_price, exit_timestamp, exit_price,
                    quantity, multiplier, gross_pnl, charges, net_pnl,
                    exit_reason, status, entry_signal_id, exit_signal_id,
                    entry_fill_id, execution_mode,
                    entry_order_id, exit_order_id, position_id, exit_fill_id,
                    realized_pnl, exit_type, stop_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    strategy_id=excluded.strategy_id, instrument=excluded.instrument,
                    side=excluded.side, entry_timestamp=excluded.entry_timestamp,
                    entry_price=excluded.entry_price, exit_timestamp=excluded.exit_timestamp,
                    exit_price=excluded.exit_price, quantity=excluded.quantity,
                    multiplier=excluded.multiplier, gross_pnl=excluded.gross_pnl,
                    charges=excluded.charges, net_pnl=excluded.net_pnl,
                    exit_reason=excluded.exit_reason, status=excluded.status,
                    entry_signal_id=excluded.entry_signal_id,
                        exit_signal_id=excluded.exit_signal_id,
                    entry_fill_id=excluded.entry_fill_id,
                    execution_mode=excluded.execution_mode,
                    realized_pnl=COALESCE(excluded.realized_pnl, trades.realized_pnl),
                    exit_type=COALESCE(excluded.exit_type, trades.exit_type),
                    entry_order_id=COALESCE(excluded.entry_order_id,
                                            trades.entry_order_id),
                    exit_order_id=COALESCE(excluded.exit_order_id,
                                           trades.exit_order_id),
                    position_id=COALESCE(excluded.position_id,
                                         trades.position_id),
                    exit_fill_id=COALESCE(excluded.exit_fill_id,
                                           trades.exit_fill_id),
                    stop_price=COALESCE(excluded.stop_price, trades.stop_price)
                    """, (
                trade.get("trade_id"),
                trade.get("strategy_id"),
                trade.get("instrument"),
                trade.get("side"),
                trade.get("entry_timestamp"),
                trade.get("entry_price"),
                trade.get("exit_timestamp"),
                trade.get("exit_price"),
                trade.get("quantity"),
                trade.get("multiplier"),
                trade.get("gross_pnl"),
                trade.get("charges"),
                trade.get("net_pnl"),
                trade.get("exit_reason"),
                trade.get("status", "closed"),
                trade.get("entry_signal_id"),
                trade.get("exit_signal_id"),
                trade.get("entry_fill_id"),
                self.execution_mode,
                trade.get("entry_order_id"),
                trade.get("exit_order_id"),
                trade.get("position_id"),
                trade.get("exit_fill_id"),
                trade.get("realized_pnl"),
                trade.get("exit_type"),
                trade.get("stop_price"),
            ))

    def save_order(self, order: dict) -> None:
        """Save order to database."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO orders (
                    order_id, strategy_id, instrument, side,
                    quantity, order_type, price, state,
                    filled_quantity, average_fill_price,
                    created_at, updated_at,
                    signal_id, trade_id, execution_mode,
                    broker_order_id, planned_entry_price, planned_sl,
                    planned_order_type, order_role, trigger_price,
                    protected_order_id, correlation_id, lifecycle_id,
                    parent_signal_id, position_id, parent_position_id,
                    position_generation, original_order_id, trigger_state,
                    trigger_generation, trigger_source
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(order_id) DO UPDATE SET
                    state=excluded.state, filled_quantity=excluded.filled_quantity,
                    average_fill_price=excluded.average_fill_price,
                        updated_at=excluded.updated_at,
                        broker_order_id=COALESCE(excluded.broker_order_id,
                                                 orders.broker_order_id),
                        planned_entry_price=COALESCE(excluded.planned_entry_price,
                                                     orders.planned_entry_price),
                        planned_sl=COALESCE(excluded.planned_sl,
                                            orders.planned_sl),
                        planned_order_type=COALESCE(excluded.planned_order_type,
                                                    orders.planned_order_type),
                        order_role=COALESCE(excluded.order_role,
                                            orders.order_role),
                        trigger_price=COALESCE(excluded.trigger_price,
                                               orders.trigger_price),
                        protected_order_id=COALESCE(excluded.protected_order_id,
                                                    orders.protected_order_id),
                        correlation_id=COALESCE(excluded.correlation_id,
                                                orders.correlation_id),
                        lifecycle_id=COALESCE(excluded.lifecycle_id, orders.lifecycle_id),
                        parent_signal_id=COALESCE(excluded.parent_signal_id, orders.parent_signal_id),
                        position_id=COALESCE(excluded.position_id, orders.position_id),
                        parent_position_id=COALESCE(excluded.parent_position_id, orders.parent_position_id),
                        position_generation=COALESCE(excluded.position_generation, orders.position_generation),
                        original_order_id=COALESCE(excluded.original_order_id, orders.original_order_id),
                        trigger_state=COALESCE(excluded.trigger_state, orders.trigger_state),
                        trigger_generation=COALESCE(excluded.trigger_generation, orders.trigger_generation),
                        trigger_source=COALESCE(excluded.trigger_source, orders.trigger_source)
                    """, (
                order.get("order_id"),
                order.get("strategy_id"),
                order.get("instrument"),
                order.get("side"),
                order.get("quantity"),
                order.get("order_type"),
                order.get("price"),
                order.get("state"),
                order.get("filled_quantity"),
                order.get("average_fill_price"),
                order.get("created_at"),
                order.get("updated_at"),
                order.get("signal_id", order.get("entry_signal_id")),
                order.get("trade_id"),
                self.execution_mode,
                order.get("broker_order_id"),
                order.get("planned_entry_price"),
                order.get("planned_sl"),
                order.get("planned_order_type"),
                order.get("order_role"),
                order.get("trigger_price"),
                order.get("protected_order_id"),
                order.get("correlation_id"),
                order.get("lifecycle_id") or order.get("trade_id"),
                order.get("parent_signal_id") or order.get("signal_id", order.get("entry_signal_id")),
                order.get("position_id") or order.get("parent_position_id"),
                order.get("parent_position_id"),
                order.get("position_generation"),
                order.get("original_order_id"),
                order.get("trigger_state"),
                order.get("trigger_generation"),
                order.get("trigger_source"),
            ))

    def save_fill(self, fill: dict) -> None:
        """Save fill to database."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO fills (
                    fill_id, order_id, strategy_id, instrument,
                    side, quantity, price, timestamp, trade_id, entry_signal_id,
                    execution_mode, broker_fill_id, broker_order_id,
                    broker_trade_id, cumulative_filled_quantity,
                    position_id, lifecycle_id, position_generation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fill_id) DO NOTHING
            """, (
                fill.get("fill_id"),
                fill.get("order_id"),
                fill.get("strategy_id"),
                fill.get("instrument"),
                fill.get("side"),
                fill.get("quantity"),
                fill.get("price"),
                fill.get("timestamp"),
                fill.get("trade_id"),
                fill.get("entry_signal_id"),
                self.execution_mode,
                fill.get("broker_fill_id"),
                fill.get("broker_order_id"),
                fill.get("broker_trade_id"),
                fill.get("cumulative_filled_quantity"),
                fill.get("position_id"),
                fill.get("lifecycle_id") or fill.get("trade_id"),
                fill.get("position_generation"),
            ))

    def save_signal(self, signal_data: dict) -> None:
        """Save signal to the signals table for audit trail (UPSERT).

        The FIRST write for a signal_id wins for every snapshot column
        (candle/indicator data) so the engine's full Phase-4 freeze at signal
        creation is never overwritten by a later partial lifecycle write.
        Identity columns follow the latest write.
        """
        candle = signal_data.get("candle_data")
        indicator = signal_data.get("indicator_data")
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO signals (
                    signal_id, strategy_id, instrument, side, signal_type,
                    signal_timestamp, trigger_price, stop_price, quantity,
                    candle_timestamp, open, high, low, close, volume,
                    htf_value, mid_value, fast_dema, fast_atr, signal_reason,
                    candle_data, indicator_data, signal_metadata, execution_mode
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(signal_id) DO UPDATE SET
                    strategy_id=excluded.strategy_id,
                    instrument=excluded.instrument,
                    side=COALESCE(excluded.side, signals.side),
                    signal_type=COALESCE(excluded.signal_type, signals.signal_type),
                    signal_timestamp=COALESCE(excluded.signal_timestamp,
                                              signals.signal_timestamp),
                    trigger_price=COALESCE(excluded.trigger_price,
                                           signals.trigger_price),
                    stop_price=COALESCE(excluded.stop_price, signals.stop_price),
                    quantity=COALESCE(excluded.quantity, signals.quantity),
                    candle_timestamp=COALESCE(excluded.candle_timestamp,
                                              signals.candle_timestamp),
                    open=COALESCE(excluded.open, signals.open),
                    high=COALESCE(excluded.high, signals.high),
                    low=COALESCE(excluded.low, signals.low),
                    close=COALESCE(excluded.close, signals.close),
                    volume=COALESCE(excluded.volume, signals.volume),
                    htf_value=COALESCE(excluded.htf_value, signals.htf_value),
                    mid_value=COALESCE(excluded.mid_value, signals.mid_value),
                    fast_dema=COALESCE(excluded.fast_dema, signals.fast_dema),
                    fast_atr=COALESCE(excluded.fast_atr, signals.fast_atr),
                    signal_reason=COALESCE(excluded.signal_reason,
                                           signals.signal_reason),
                    candle_data=COALESCE(excluded.candle_data,
                                         signals.candle_data),
                    indicator_data=COALESCE(excluded.indicator_data,
                                            signals.indicator_data),
                    signal_metadata=COALESCE(excluded.signal_metadata,
                                             signals.signal_metadata)
            """, (
                signal_data.get("signal_id"),
                signal_data.get("strategy_id"),
                signal_data.get("instrument"),
                signal_data.get("side"),
                signal_data.get("signal_type"),
                signal_data.get("timestamp"),
                signal_data.get("trigger_price"),
                signal_data.get("stop_price"),
                signal_data.get("quantity"),
                signal_data.get("candle_timestamp"),
                signal_data.get("open"),
                signal_data.get("high"),
                signal_data.get("low"),
                signal_data.get("close"),
                signal_data.get("volume"),
                signal_data.get("htf_value"),
                signal_data.get("mid_value"),
                signal_data.get("fast_dema"),
                signal_data.get("fast_atr"),
                signal_data.get("signal_reason"),
                json.dumps(candle) if candle else None,
                json.dumps(indicator) if indicator else None,
                json.dumps(signal_data.get("signal_metadata"))
                if signal_data.get("signal_metadata") is not None else None,
                self.execution_mode,
            ))

    def save_trade_signal_link(self, trade_id: str, signal_id: str, relationship_type: str) -> None:
        """Save a trade-signal relationship link."""
        with self._tx() as conn:
            conn.execute("""
                INSERT OR IGNORE INTO trade_signal_link (
                    trade_id, signal_id, relationship_type, execution_mode
                ) VALUES (?, ?, ?, ?)
            """, (trade_id, signal_id, relationship_type, self.execution_mode))

    def get_fill(self, fill_id: str) -> Optional[dict]:
        """Fetch a single fill row by fill_id (DB-backed idempotency guard)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM fills WHERE fill_id = ?", (fill_id,)
            ).fetchone()
            return dict(row) if row else None

    def get_signal(self, signal_id: str) -> Optional[dict]:
        """Fetch a single signal row by signal_id for pending-entry enrichment."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM signals WHERE signal_id = ?", (signal_id,)
            ).fetchone()
            return dict(row) if row else None

    def save_broker_order_mapping(self, mapping: dict) -> None:
        """Persist the explicit broker_order_id -> strategy identity mapping
        (mission §40). Survives restart through canonical persistence."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO broker_order_mapping (
                    broker_order_id, order_id, trade_id, strategy_id, instrument,
                    execution_mode
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(broker_order_id) DO UPDATE SET
                    order_id=excluded.order_id, trade_id=excluded.trade_id,
                    strategy_id=excluded.strategy_id, instrument=excluded.instrument
            """, (
                mapping.get("broker_order_id"),
                mapping.get("order_id"),
                mapping.get("trade_id"),
                mapping.get("strategy_id"),
                mapping.get("instrument"),
                self.execution_mode,
            ))

    def get_broker_order_mappings(self) -> list[dict]:
        """Load all durable broker order mappings (restore on restart)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM broker_order_mapping ORDER BY broker_order_id"
            ).fetchall()
            return [dict(r) for r in rows]

    def save_pending_order(self, pending: dict) -> None:
        """Upsert one durable pending-order lifecycle row (Phase 9.6).

        State writes are additive and idempotent: correlation_id,
        broker_order_id and armed_at use first-write-wins so a later state
        update (e.g. EXPIRED) can never erase the broker tie-back recorded at
        ENTRY_SENT.
        """
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO pending_orders (
                    pending_order_id, trade_id, signal_id, side, order_type,
                    trigger_price, quantity, status, execution_mode,
                    created_at, updated_at, broker_order_id, correlation_id,
                    expired_reason, armed_at, strategy_id, instrument, direction,
                    trigger_state, trigger_generation, trigger_source, signal_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(pending_order_id) DO UPDATE SET
                    trade_id=excluded.trade_id,
                    status=excluded.status,
                    updated_at=excluded.updated_at,
                    broker_order_id=COALESCE(excluded.broker_order_id,
                                             pending_orders.broker_order_id),
                    correlation_id=COALESCE(excluded.correlation_id,
                                            pending_orders.correlation_id),
                    expired_reason=COALESCE(excluded.expired_reason,
                                            pending_orders.expired_reason),
                    armed_at=COALESCE(excluded.armed_at, pending_orders.armed_at),
                    strategy_id=COALESCE(excluded.strategy_id, pending_orders.strategy_id),
                    instrument=COALESCE(excluded.instrument, pending_orders.instrument),
                    direction=COALESCE(excluded.direction, pending_orders.direction),
                    trigger_state=COALESCE(excluded.trigger_state, pending_orders.trigger_state),
                    trigger_generation=COALESCE(excluded.trigger_generation, pending_orders.trigger_generation),
                    trigger_source=COALESCE(excluded.trigger_source, pending_orders.trigger_source),
                    signal_timestamp=COALESCE(excluded.signal_timestamp, pending_orders.signal_timestamp)
            """, (
                pending.get("pending_order_id"),
                pending.get("trade_id"),
                pending.get("signal_id", pending.get("pending_order_id")),
                pending.get("side"),
                pending.get("order_type"),
                pending.get("trigger_price"),
                pending.get("quantity"),
                pending.get("status"),
                self.execution_mode,
                pending.get("created_at"),
                pending.get("updated_at"),
                pending.get("broker_order_id"),
                pending.get("correlation_id"),
                pending.get("expired_reason"),
                pending.get("armed_at"),
                pending.get("strategy_id"),
                pending.get("instrument"),
                pending.get("direction", pending.get("side")),
                pending.get("trigger_state"),
                pending.get("trigger_generation"),
                pending.get("trigger_source"),
                pending.get("signal_timestamp"),
            ))

    def get_pending_orders(
        self,
        status: Optional[str] = None,
        execution_mode: Optional[str] = None,
    ) -> list[dict]:
        """Load durable pending-order rows (restore on restart).

        Optional filters keep callers cheap: a status filter lets the engine
        find still-ARMED pending orders without dumping the whole table.
        """
        sql = "SELECT * FROM pending_orders"
        where: list[str] = []
        params: list[Any] = []
        if status is not None:
            where.append("status = ?")
            params.append(status)
        if execution_mode is not None:
            where.append("execution_mode = ?")
            params.append(str(execution_mode).upper())
        if where:
            sql += " WHERE " + " AND ".join(where)
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql + " ORDER BY created_at", params).fetchall()
            return [dict(r) for r in rows]

    def get_pending_order(self, signal_id: str,
                          execution_mode: Optional[str] = None) -> Optional[dict]:
        """Fetch only the newest pending row for one signal.

        Startup uses this to reject a stale in-memory trigger without loading
        the entire historical pending_orders table into RAM.
        """
        sql = ("SELECT * FROM pending_orders WHERE "
               "(signal_id = ? OR pending_order_id = ?)")
        params: list[Any] = [str(signal_id), str(signal_id)]
        if execution_mode is not None:
            sql += " AND execution_mode = ?"
            params.append(str(execution_mode).upper())
        sql += " ORDER BY created_at DESC LIMIT 1"
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(sql, params).fetchone()
            return dict(row) if row else None

    def get_order_by_broker_order_id(self, broker_order_id: str) -> Optional[dict]:
        """Resolve the newest local order row by broker_order_id (F2 tier-3)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM orders WHERE broker_order_id = ? "
                "ORDER BY created_at DESC LIMIT 1", (broker_order_id,)
            ).fetchall()
            return dict(rows[0]) if rows else None

    def terminalize_pending_order(self, signal_id: str,
                                  status: str = "resolved",
                                  reason: str = "") -> bool:
        """Terminalize a durable pending-order row (Phase 9.6 / F2).

        Moves the newest pending row for ``signal_id`` to an absorbing
        terminal state (default ``resolved``) once the broker confirms the
        entry ended without a fill (rejected / cancelled).  Safe to call from
        the poller every cycle for every terminal broker order: rows already
        terminal (resolved / expired / cancelled_by_reversal) are left
        untouched.  Returns True when a row was actually terminalized.
        """
        with self._lock, self._tx() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM pending_orders WHERE signal_id = ? "
                "ORDER BY created_at DESC LIMIT 1", (signal_id,)
            ).fetchone()
            if row is None:
                return False
            current = (row["status"] or "pending").lower()
            terminal = {"resolved", "expired", "cancelled_by_reversal"}
            if current in terminal:
                return False
            if current != status:
                try:
                    from core.lifecycle import transition_pending_state
                    status = transition_pending_state(current, status,
                                                      row["pending_order_id"])
                except ValueError:
                    return False
            conn.execute(
                "UPDATE pending_orders SET status = ?, expired_reason = ?, "
                "updated_at = ? WHERE pending_order_id = ?",
                (status, reason or (row["expired_reason"] or ""),
                 datetime.now(timezone.utc).isoformat(), row["pending_order_id"]),
            )
            return True

    def save_quarantine_record(self, record: dict) -> None:
        """Persist a rejected/quarantined event (mission §34)."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO quarantine_records (
                    original_type, original_id, reason, payload, execution_mode
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                record.get("original_type", "event"),
                record.get("original_id", ""),
                record.get("reason", ""),
                json.dumps(record.get("payload", {})),
                record.get("execution_mode", self.execution_mode),
            ))

    def get_quarantine_records(self, limit: int = 100) -> list[dict]:
        """Read recent quarantine records (diagnostics/audit)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM quarantine_records ORDER BY record_id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_trade_and_fill(self, trade: dict, fill: dict) -> None:
        """Persist a closed trade and its exit fill in one transaction."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO trades (
                    trade_id, strategy_id, instrument, side,
                    entry_timestamp, entry_price, exit_timestamp, exit_price,
                    quantity, multiplier, gross_pnl, charges, net_pnl,
                    exit_reason, status, entry_signal_id, exit_signal_id,
                    entry_fill_id, execution_mode,
                    entry_order_id, exit_order_id, position_id, exit_fill_id,
                    realized_pnl, exit_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_id) DO UPDATE SET
                    exit_timestamp=excluded.exit_timestamp,
                    exit_price=excluded.exit_price,
                    gross_pnl=excluded.gross_pnl,
                    charges=excluded.charges,
                    net_pnl=excluded.net_pnl,
                    exit_reason=excluded.exit_reason,
                    status=excluded.status,
                    exit_signal_id=excluded.exit_signal_id,
                    entry_fill_id=COALESCE(excluded.entry_fill_id, trades.entry_fill_id),
                    realized_pnl=COALESCE(excluded.realized_pnl, trades.realized_pnl),
                    exit_type=COALESCE(excluded.exit_type, trades.exit_type),
                    entry_order_id=COALESCE(excluded.entry_order_id, trades.entry_order_id),
                    exit_order_id=COALESCE(excluded.exit_order_id, trades.exit_order_id),
                    position_id=COALESCE(excluded.position_id, trades.position_id),
                    exit_fill_id=COALESCE(excluded.exit_fill_id, trades.exit_fill_id)
            """, (
                trade.get("trade_id"), trade.get("strategy_id"), trade.get("instrument"),
                trade.get("side"), trade.get("entry_timestamp"), trade.get("entry_price"),
                trade.get("exit_timestamp"), trade.get("exit_price"), trade.get("quantity"),
                trade.get("multiplier"), trade.get("gross_pnl"), trade.get("charges"),
                trade.get("net_pnl"), trade.get("exit_reason"), trade.get("status", "closed"),
                trade.get("entry_signal_id"), trade.get("exit_signal_id"),
                trade.get("entry_fill_id"), trade.get("execution_mode", self.execution_mode),
                trade.get("entry_order_id"), trade.get("exit_order_id"),
                trade.get("position_id"), trade.get("exit_fill_id"),
                trade.get("realized_pnl"), trade.get("exit_type"),
            ))
            conn.execute("""
                INSERT INTO fills (
                    fill_id, order_id, strategy_id, instrument,
                    side, quantity, price, timestamp, trade_id, entry_signal_id,
                    broker_order_id, broker_fill_id, broker_trade_id,
                    cumulative_filled_quantity, execution_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fill_id) DO NOTHING
            """, (
                fill.get("fill_id"), fill.get("order_id"), fill.get("strategy_id"),
                fill.get("instrument"), fill.get("side"), fill.get("quantity"),
                fill.get("price"), fill.get("timestamp"), fill.get("trade_id"),
                fill.get("entry_signal_id"),
                fill.get("broker_order_id"), fill.get("broker_fill_id"),
                fill.get("broker_trade_id"), fill.get("cumulative_filled_quantity"),
                self.execution_mode,
            ))

    def save_event(self, event: dict) -> None:
        """Save event to audit log (stamped with the environment mode)."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO events (
                    timestamp, event_type, strategy_id, instrument, details,
                    execution_mode
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                event.get("timestamp", datetime.now(timezone.utc).isoformat()),
                event.get("event_type"),
                event.get("strategy_id"),
                event.get("instrument"),
                json.dumps(event.get("details", {})),
                event.get("execution_mode", self.execution_mode),
            ))

    def save_execution_failure_event(self, event: dict) -> None:
        """Persist a durable execution-failure / protection audit event (spec §53).

        Records the full identity lineage (strategy/trade/order/broker/signal),
        the error, the action taken and the resulting state for broker
        submit/modify/cancel/SL/reversal step failures.
        """
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO execution_failure_events (
                    event_id, event_type, strategy_id, trade_id, order_id,
                    broker_order_id, signal_id, instrument, error, action,
                    final_state, details, execution_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO NOTHING
            """, (
                event.get("event_id"),
                event.get("event_type"),
                event.get("strategy_id"),
                event.get("trade_id"),
                event.get("order_id"),
                event.get("broker_order_id"),
                event.get("signal_id"),
                event.get("instrument"),
                event.get("error"),
                event.get("action"),
                event.get("final_state"),
                json.dumps(event.get("details", {})),
                self.execution_mode,
            ))

    def get_execution_failure_events(
        self,
        strategy_id: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """Read recent execution-failure audit events (diagnostics/audit)."""
        sql = "SELECT * FROM execution_failure_events"
        params: list[Any] = []
        if strategy_id:
            sql += " WHERE strategy_id = ?"
            params.append(strategy_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(sql, params).fetchall()
            out = []
            for r in rows:
                row = dict(r)
                try:
                    row["details"] = json.loads(row.get("details") or "{}")
                except (TypeError, ValueError):
                    row["details"] = {}
                out.append(row)
            return out

    def get_trades(self, strategy_id: Optional[str] = None) -> list[dict]:
        """Get trades from database."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if strategy_id:
                rows = conn.execute(
                    "SELECT * FROM trades WHERE strategy_id=? ORDER BY id DESC",
                    (strategy_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM trades ORDER BY id DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_orders(self, order_id: Optional[str] = None) -> list[dict]:
        """Fetch orders from the canonical orders table.

        ### OBSERVATION (get_orders/get_fills)
        The dashboard route previously served orders/fills from the in-memory
        paper broker only, so after a restart the Orders and Fills pages went
        empty even though the data was in trading.db.  These readers let the
        routes merge memory + DB so history survives a restart.  Rows are
        returned newest-first to match the memory list ordering.
        """
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if order_id:
                rows = conn.execute(
                    "SELECT * FROM orders WHERE order_id=? ORDER BY rowid DESC",
                    (order_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM orders ORDER BY rowid DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_fills(self, fill_id: Optional[str] = None) -> list[dict]:
        """Fetch fills from the canonical fills table (see get_orders note)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if fill_id:
                rows = conn.execute(
                    "SELECT * FROM fills WHERE fill_id=? ORDER BY rowid DESC",
                    (fill_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM fills ORDER BY rowid DESC"
                ).fetchall()
            return [dict(r) for r in rows]

    def fill_by_broker_fill_id(self, broker_fill_id: str) -> Optional[dict]:
        """Return the persisted fill for a broker-native execution identity, if
        any (Phase 9.7 duplicate suppression across restart/sources)."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM fills WHERE broker_fill_id=? LIMIT 1",
                (broker_fill_id,),
            ).fetchone()
            return dict(row) if row else None

    def order_cumulative_filled(
        self,
        broker_order_id: str,
        execution_mode: Optional[str] = None,
    ) -> int:
        """Local cumulative filled quantity already persisted for a broker
        order (Phase 9.7 ledger).  Uses the latest persisted cumulative value;
        falls back to the sum of persisted deltas for legacy rows."""
        mode = execution_mode or self.execution_mode
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT MAX(cumulative_filled_quantity) AS cum, "
                "       COALESCE(SUM(quantity), 0) AS total "
                "FROM fills WHERE broker_order_id=? AND execution_mode=?",
                (broker_order_id, mode),
            ).fetchone()
            if row is None:
                return 0
            cum = row["cum"]
            return int(cum if cum is not None else row["total"] or 0)

    def get_fill_reconciliation(
        self,
        broker_order_id: Optional[str] = None,
        execution_mode: Optional[str] = None,
    ) -> list[dict] | dict | None:
        """Fetch the Phase 9.7 per-order fill reconciliation ledger rows."""
        mode = execution_mode or self.execution_mode
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if broker_order_id:
                row = conn.execute(
                    "SELECT * FROM fill_reconciliation "
                    "WHERE broker_order_id=? AND execution_mode=?",
                    (broker_order_id, mode),
                ).fetchone()
                return dict(row) if row else None
            rows = conn.execute(
                "SELECT * FROM fill_reconciliation WHERE execution_mode=? "
                "ORDER BY broker_order_id",
                (mode,),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_fill_reconciliation(self, rec: dict) -> None:
        """Upsert one Phase 9.7 per-order fill reconciliation ledger row.

        broker_cumulative_qty moves monotonically with the broker's own
        cumulative traded quantity; local_cumulative_qty mirrors the persisted
        fills; status is 'synced' when the two agree and 'divergence' when the
        local copy disagrees with the broker (the broker wins — a mismatch is
        surfaced, never invented away).
        """
        broker_cum = int(rec.get("broker_cumulative_qty") or 0)
        local_cum = int(rec.get("local_cumulative_qty") or 0)
        status = rec.get("status")
        if status is None:
            status = "synced" if broker_cum == local_cum else "divergence"
        gap = abs(broker_cum - local_cum)
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO fill_reconciliation (
                    broker_order_id, execution_mode, strategy_id, instrument,
                    order_id, side, broker_cumulative_qty, local_cumulative_qty,
                    gap_qty, status, last_broker_fill_id, broker_average_price,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(broker_order_id, execution_mode) DO UPDATE SET
                    strategy_id=excluded.strategy_id,
                    instrument=excluded.instrument,
                    order_id=excluded.order_id,
                    side=excluded.side,
                    broker_cumulative_qty=excluded.broker_cumulative_qty,
                    local_cumulative_qty=excluded.local_cumulative_qty,
                    gap_qty=excluded.gap_qty,
                    status=excluded.status,
                    last_broker_fill_id=excluded.last_broker_fill_id,
                    broker_average_price=excluded.broker_average_price,
                    updated_at=excluded.updated_at
            """, (
                rec.get("broker_order_id"),
                rec.get("execution_mode", self.execution_mode),
                rec.get("strategy_id"),
                rec.get("instrument"),
                rec.get("order_id"),
                rec.get("side"),
                broker_cum,
                local_cum,
                gap,
                status,
                rec.get("last_broker_fill_id"),
                rec.get("broker_average_price"),
                rec.get("updated_at") or datetime.now(timezone.utc).isoformat(),
            ))

    def save_position(self, position) -> None:
        """Persist a position to the canonical positions table.

        The position_id is a SEPARATE identity from the trade_id (enforced by
        the canonical uniqueness trigger). status='open' on entry; the row is
        flipped to 'closed' by close_position_record() on exit.
        """
        entry_time = position.entry_timestamp
        if isinstance(entry_time, (int, float)) and entry_time:
            entry_time = datetime.fromtimestamp(entry_time, tz=timezone.utc).isoformat()
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO positions (
                    position_id, trade_id, strategy_id, instrument, side,
                    quantity, average_entry_price, status, entry_time,
                    realized_pnl, updated_at, execution_mode,
                    sl_state, sl_trigger_price, sl_protected_at,
                    stop_price, position_generation, exit_started, lifecycle_id,
                    entry_order_id, exit_order_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(position_id) DO UPDATE SET
                    strategy_id=excluded.strategy_id, instrument=excluded.instrument,
                    side=excluded.side, quantity=excluded.quantity,
                    average_entry_price=excluded.average_entry_price,
                    status=excluded.status, realized_pnl=excluded.realized_pnl,
                    updated_at=excluded.updated_at,
                    sl_state=excluded.sl_state,
                    sl_trigger_price=excluded.sl_trigger_price,
                    sl_protected_at=COALESCE(excluded.sl_protected_at, positions.sl_protected_at),
                    stop_price=COALESCE(excluded.stop_price, positions.stop_price)
                    ,position_generation=excluded.position_generation
                    ,exit_started=excluded.exit_started
                    ,lifecycle_id=COALESCE(excluded.lifecycle_id, positions.lifecycle_id)
                    ,entry_order_id=COALESCE(excluded.entry_order_id, positions.entry_order_id)
                    ,exit_order_id=COALESCE(excluded.exit_order_id, positions.exit_order_id)
            """, (
                position.position_id,
                position.trade_id,
                position.strategy_id,
                position.instrument,
                position.side.value if hasattr(position.side, "value") else str(position.side),
                position.quantity,
                position.average_entry,
                position.status.value if hasattr(position.status, "value") else str(position.status),
                entry_time,
                position.realized_pnl,
                datetime.now(timezone.utc).isoformat(),
                self.execution_mode,
                getattr(position, "sl_state", None),
                getattr(position, "sl_trigger_price", None),
                getattr(position, "sl_protected_at", None),
                getattr(position, "stop_price", None),
                getattr(position, "position_generation", 0),
                int(bool(getattr(position, "exit_started", False))),
                getattr(position, "lifecycle_id", None) or getattr(position, "trade_id", None),
                getattr(position, "entry_order_id", None),
                getattr(position, "exit_order_id", None),
            ))

    def close_position_record(self, position) -> None:
        """Flip a position row to closed with exit price/timestamp/realized P&L."""
        exit_price = None
        exit_time = None
        if getattr(position, "exit_fills", None):
            last_exit = position.exit_fills[-1]
            exit_price = last_exit.price
            if last_exit.timestamp:
                exit_time = datetime.fromtimestamp(
                    last_exit.timestamp, tz=timezone.utc
                ).isoformat()
        with self._tx() as conn:
            conn.execute("""
                UPDATE positions SET
                    status='closed',
                    average_exit_price=?,
                    exit_time=?,
                    realized_pnl=?,
                    updated_at=?
                WHERE position_id=?
            """, (
                exit_price,
                exit_time,
                position.realized_pnl,
                datetime.now(timezone.utc).isoformat(),
                position.position_id,
            ))

    def get_open_positions(self, strategy_id: Optional[str] = None) -> list[dict]:
        """Get open position rows from the canonical positions table."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if strategy_id:
                rows = conn.execute(
                    "SELECT * FROM positions WHERE status='open' AND strategy_id=?",
                    (strategy_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM positions WHERE status='open'"
                ).fetchall()
            return [dict(r) for r in rows]

    def get_account_snapshots(self, limit: int = 100) -> list[dict]:
        """Get recent account snapshots."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM account_snapshots ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]

    def save_account_snapshot(self, snapshot: dict) -> None:
        """Save account snapshot."""
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO account_snapshots (
                    timestamp, equity, realized_pnl, unrealized_pnl,
                    used_margin, available_margin, execution_mode
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot.get("timestamp", datetime.now(timezone.utc).isoformat()),
                snapshot.get("equity"),
                snapshot.get("realized_pnl"),
                snapshot.get("unrealized_pnl"),
                snapshot.get("used_margin"),
                snapshot.get("available_margin"),
                self.execution_mode,
            ))

    def save_account_snapshot_from_state(self, state: dict) -> None:
        """Derive and persist an account-snapshot row from an engine snapshot.

        LIVE snapshots are persisted ONLY when they carry broker-verified
        totals (``equity_source == 'broker'``), so a stale locally-derived
        starting-capital default can never masquerade as the account balance in
        the persistent ledger (F1 invariant).  PAPER/other modes persist as-is.
        """
        acct = (state or {}).get("account")
        if not acct:
            return
        if str(self.execution_mode).upper() == "LIVE" and \
                acct.get("equity_source") != "broker":
            return
        self.save_account_snapshot({
            "timestamp": state.get("timestamp") or datetime.now(timezone.utc).isoformat(),
            "equity": acct.get("equity"),
            "realized_pnl": acct.get("realized_pnl"),
            "unrealized_pnl": acct.get("unrealized_pnl"),
            "used_margin": acct.get("used_margin"),
            "available_margin": acct.get("available_margin"),
        })

    # ── reversal lifecycle records ──────────────────────────────────

    def save_reversal(self, reversal: dict) -> None:
        """Upsert a durable reversal lifecycle record keyed by signal_id.

        On the first call (REVERSAL_EXIT submission) the row is inserted with
        status=PENDING_EXIT; subsequent updates (exit fill, new entry submit,
        entry fill, SL placement) merge the additional broker/fill evidence.
        The unique signal_id ensures at most one reversal row per SIG-X
        lifecycle (exit and entry share the same signal id).
        """
        now = datetime.now(timezone.utc).isoformat()
        with self._tx() as conn:
            conn.execute("""
                INSERT INTO reversals (
                    reversal_id, signal_id, strategy_id, instrument,
                    old_trade_id, old_position_id, old_exit_order_id,
                    old_broker_order_id, old_sl_order_id, old_sl_state,
                    new_trade_id, new_position_id, new_entry_order_id,
                    new_broker_order_id, new_sl_order_id, new_sl_state,
                    reversal_trigger_price,
                    old_exit_fill_price, new_entry_fill_price,
                    old_exit_filled_quantity, new_entry_filled_quantity,
                    old_exit_broker_status, new_entry_broker_status,
                    fallback_used, fallback_status,
                    exit_verified_at, entry_fill_confirmed_at,
                    status, created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(reversal_id) DO UPDATE SET
                    signal_id=COALESCE(excluded.signal_id, signal_id),
                    strategy_id=COALESCE(excluded.strategy_id, strategy_id),
                    instrument=COALESCE(excluded.instrument, instrument),
                    old_trade_id=COALESCE(excluded.old_trade_id, old_trade_id),
                    old_position_id=COALESCE(excluded.old_position_id, old_position_id),
                    old_exit_order_id=COALESCE(excluded.old_exit_order_id, old_exit_order_id),
                    old_broker_order_id=COALESCE(excluded.old_broker_order_id, old_broker_order_id),
                    old_sl_order_id=COALESCE(excluded.old_sl_order_id, old_sl_order_id),
                    old_sl_state=COALESCE(excluded.old_sl_state, old_sl_state),
                    new_trade_id=COALESCE(excluded.new_trade_id, new_trade_id),
                    new_position_id=COALESCE(excluded.new_position_id, new_position_id),
                    new_entry_order_id=COALESCE(excluded.new_entry_order_id, new_entry_order_id),
                    new_broker_order_id=COALESCE(excluded.new_broker_order_id, new_broker_order_id),
                    new_sl_order_id=COALESCE(excluded.new_sl_order_id, new_sl_order_id),
                    new_sl_state=COALESCE(excluded.new_sl_state, new_sl_state),
                    reversal_trigger_price=COALESCE(excluded.reversal_trigger_price, reversal_trigger_price),
                    old_exit_fill_price=COALESCE(excluded.old_exit_fill_price, old_exit_fill_price),
                    new_entry_fill_price=COALESCE(excluded.new_entry_fill_price, new_entry_fill_price),
                    old_exit_filled_quantity=COALESCE(excluded.old_exit_filled_quantity, old_exit_filled_quantity),
                    new_entry_filled_quantity=COALESCE(excluded.new_entry_filled_quantity, new_entry_filled_quantity),
                    old_exit_broker_status=COALESCE(excluded.old_exit_broker_status, old_exit_broker_status),
                    new_entry_broker_status=COALESCE(excluded.new_entry_broker_status, new_entry_broker_status),
                    fallback_used=COALESCE(excluded.fallback_used, fallback_used),
                    fallback_status=COALESCE(excluded.fallback_status, fallback_status),
                    exit_verified_at=COALESCE(excluded.exit_verified_at, exit_verified_at),
                    entry_fill_confirmed_at=COALESCE(excluded.entry_fill_confirmed_at, entry_fill_confirmed_at),
                    status=COALESCE(excluded.status, status),
                    updated_at=?
            """, (
                reversal.get("reversal_id"),
                reversal.get("signal_id"),
                reversal.get("strategy_id"),
                reversal.get("instrument"),
                reversal.get("old_trade_id"),
                reversal.get("old_position_id"),
                reversal.get("old_exit_order_id"),
                reversal.get("old_broker_order_id"),
                reversal.get("old_sl_order_id"),
                reversal.get("old_sl_state"),
                reversal.get("new_trade_id"),
                reversal.get("new_position_id"),
                reversal.get("new_entry_order_id"),
                reversal.get("new_broker_order_id"),
                reversal.get("new_sl_order_id"),
                reversal.get("new_sl_state"),
                reversal.get("reversal_trigger_price"),
                reversal.get("old_exit_fill_price"),
                reversal.get("new_entry_fill_price"),
                reversal.get("old_exit_filled_quantity"),
                reversal.get("new_entry_filled_quantity"),
                reversal.get("old_exit_broker_status"),
                reversal.get("new_entry_broker_status"),
                reversal.get("fallback_used"),
                reversal.get("fallback_status"),
                reversal.get("exit_verified_at"),
                reversal.get("entry_fill_confirmed_at"),
                reversal.get("status"),
                now,
                now,
                now,
            ))

    def update_reversal(self, reversal_id: str, fields: dict) -> None:
        """Targeted update on an existing reversal lifecycle record."""
        if not fields or not reversal_id:
            return
        fields["updated_at"] = datetime.now(timezone.utc).isoformat()
        cols = ", ".join(f"{k}=?" for k in fields.keys())
        vals = list(fields.values()) + [reversal_id]
        with self._tx() as conn:
            conn.execute(f"UPDATE reversals SET {cols} WHERE reversal_id=?", vals)

    def get_reversal(self, reversal_id: str) -> Optional[dict]:
        """Fetch one reversal lifecycle record by reversal_id."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM reversals WHERE reversal_id=? LIMIT 1",
                (reversal_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_reversal_by_signal_id(self, signal_id: str) -> Optional[dict]:
        """Fetch one reversal lifecycle record by its shared signal id."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM reversals WHERE signal_id=? LIMIT 1",
                (signal_id,),
            ).fetchone()
            return dict(row) if row else None

    def get_reversals(self, strategy_id: Optional[str] = None,
                      limit: int = 50) -> list[dict]:
        """Fetch reversal lifecycle records, newest first."""
        with self._lock:
            conn = self._get_conn()
            conn.row_factory = sqlite3.Row
            if strategy_id:
                rows = conn.execute(
                    "SELECT * FROM reversals WHERE strategy_id=? "
                    "ORDER BY id DESC LIMIT ?",
                    (strategy_id, limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM reversals ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            return [dict(r) for r in rows]

    def close(self) -> None:
        """Close the persistent database connection."""
        with self._lock:
            self._conn = None
            if self._db is not None:
                try:
                    self._db.close()
                except Exception:
                    pass
            self._db = None

    def __del__(self) -> None:
        """Auto-close connection on garbage collection."""
        try:
            self.close()
        except Exception:
            pass
