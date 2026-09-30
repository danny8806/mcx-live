"""Central database access layer — ONE canonical SQLite database (trading.db).

This is the ONLY sanctioned path to the canonical database.  Every module that
needs durable persistence must go through :class:`Database` (or the repository
layer built on it).  This makes the "single source of truth" enforceable:

* ONE database file (``trading.db``).
* ``PRAGMA foreign_keys = ON`` on every connection.
* WAL journal, sane busy timeout, serialized writes.
* Real transactions via :meth:`Database.transaction`.
* Versioned schema with idempotent migrations.

The old ``analytics.db`` is no longer a runtime database.  Its tables
(``trades_analytics``, ``trade_legs``, ``trade_snapshots``, ``strategy_*``)
live *inside* trading.db as DERIVED tables — they are a rebuildable read-model
and are never authoritative (see ``rebuild_analytics()``).
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
import logging
from typing import Any, Iterator, Optional

from config import Config

log = logging.getLogger(__name__)

# Current schema version of the canonical database.  Bump when adding tables/
# columns; the migrator applies step-wise idempotent DDL.
SCHEMA_VERSION = 6

# Tables whose rows are CANONICAL — the durable source of truth.  Never deleted
# or rebuilt from memory; only mutated by lifecycle/domain persistence code.
CANONICAL_TABLES = frozenset({
    "signals",
    "trades",
    "trade_signal_link",
    "pending_orders",
    "orders",
    "fills",
    "positions",
    "trade_events",
    "processed_fills",
    "account_snapshots",
    "events",
    "execution_failure_events",
    "quarantine_records",
    "broker_order_mapping",
    "fill_reconciliation",
    "system_metadata",
    "broker_api_events",
    "alert_events",
    "reversals",
})

# Tables whose rows are DERIVED — a rebuildable read-model computed from the
# canonical tables.  Safe to DELETE and rebuild via rebuild_analytics().
DERIVED_TABLES = frozenset({
    "trades_analytics",
    "trade_legs",
    "trade_snapshots",
    "strategy_daily_performance",
    "strategy_monthly_performance",
    "strategy_parameter_results",
    "strategy_performance_snapshots",
})
# Order matters for FK creation (parents before children).  Not every column
# carries an actual FOREIGN KEY constraint (SQLite cannot add them to an existing
# table via ALTER), so enforcement is dual:
#   1. new tables (positions, pending_orders, trade_events, quarantine_records)
#      declare real FK constraints;
#   2. legacy tables (orders, fills, trade_signal_link) get FK *indexes* plus
#      repository-level validation + `PRAGMA foreign_key_check` tooling.
_SCHEMA_DDL: list[str] = [
    # ── canonical: signals (full signal candle snapshot) ──
    """
    CREATE TABLE IF NOT EXISTS signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signal_id TEXT UNIQUE NOT NULL,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        security_id TEXT,
        timeframe TEXT,
        side TEXT,
        signal_type TEXT,
        signal_timestamp REAL,
        candle_timestamp REAL,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        volume REAL,
        trigger_price REAL,
        stop_price REAL,
        quantity INTEGER,
        htf_value REAL,
        mid_value REAL,
        fast_dema REAL,
        fast_atr REAL,
        signal_reason TEXT,
        candle_data TEXT,
        indicator_data TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: trades ──
    """
    CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id TEXT UNIQUE NOT NULL,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        security_id TEXT,
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
        updated_at TEXT,
        entry_signal_id TEXT NOT NULL,
        exit_signal_id TEXT,
        entry_order_id TEXT,
        exit_order_id TEXT,
        position_id TEXT,
        -- OBSERVATION (entry_fill_id)
        -- Prior schema had no way to persist the trade->entry-fill linkage even
        -- though core/lifecycle.py:register_entry_fill maintains it in memory.
        -- After a restart the restored trade had entry_fill_id=None, which the
        -- legacy reconciliation engine counted as a hard ERROR every startup
        -- ("TRADE_NO_ENTRY_FILL").  The column makes the linkage durable and
        -- lets the reconciliation/F4 fixes store and restore it.
        entry_fill_id TEXT,
        exit_type TEXT,
        realized_pnl REAL,
        exit_fill_id TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER'
    )
    """,
    # ── canonical: trade <-> signal links ──
    """
    CREATE TABLE IF NOT EXISTS trade_signal_link (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id),
        signal_id TEXT NOT NULL REFERENCES signals(signal_id),
        relationship_type TEXT NOT NULL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(trade_id, signal_id, relationship_type)
    )
    """,
    # ── canonical: pending orders ──
    """
    CREATE TABLE IF NOT EXISTS pending_orders (
        pending_order_id TEXT PRIMARY KEY,
        trade_id TEXT REFERENCES trades(trade_id),
        signal_id TEXT,
        side TEXT,
        order_type TEXT,
        trigger_price REAL,
        quantity INTEGER,
        status TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT,
        broker_order_id TEXT,
        correlation_id TEXT,
         expired_reason TEXT,
         armed_at TEXT,
         strategy_id TEXT,
         instrument TEXT,
         direction TEXT,
         trigger_state TEXT,
         trigger_generation INTEGER,
         trigger_source TEXT,
         signal_timestamp REAL
    )""",
    # ── canonical: orders ──
    """
    CREATE TABLE IF NOT EXISTS orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id TEXT UNIQUE NOT NULL,
        trade_id TEXT REFERENCES trades(trade_id),
        pending_order_id TEXT,
        signal_id TEXT,
        broker_order_id TEXT,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity INTEGER,
        order_type TEXT,
        price REAL,
        planned_entry_price REAL,
        planned_sl REAL,
        planned_order_type TEXT,
        order_intent TEXT,
        lifecycle_id TEXT,
        parent_signal_id TEXT,
        position_id TEXT,
        parent_position_id TEXT,
        position_generation INTEGER,
        original_order_id TEXT,
        reversal_parent_signal_id TEXT,
        trigger_state TEXT,
        trigger_generation INTEGER,
        trigger_source TEXT,
        order_role TEXT,
        state TEXT,
        filled_quantity INTEGER,
        average_fill_price REAL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT,
        updated_at TEXT
    )
    """,
    # ── canonical: fills ──
    """
    CREATE TABLE IF NOT EXISTS fills (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fill_id TEXT UNIQUE NOT NULL,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id),
        order_id TEXT REFERENCES orders(order_id),
        broker_fill_id TEXT,
        broker_order_id TEXT,
        broker_trade_id TEXT,
        cumulative_filled_quantity INTEGER,
        position_id TEXT,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity INTEGER,
        price REAL,
        timestamp TEXT,
        fill_type TEXT,
        entry_signal_id TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        lifecycle_id TEXT,
        position_generation INTEGER
    )
    """,
    # ── canonical: positions (separate identity from trade) ──
    """
    CREATE TABLE IF NOT EXISTS positions (
        position_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id),
        strategy_id TEXT,
        instrument TEXT,
        side TEXT,
        quantity INTEGER,
        average_entry_price REAL,
        average_exit_price REAL,
        status TEXT,
        entry_time TEXT,
        exit_time TEXT,
        realized_pnl REAL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT,
        position_generation INTEGER,
        exit_started INTEGER NOT NULL DEFAULT 0,
        lifecycle_id TEXT
    )
    """,
    # ── canonical: append-only lifecycle event log ──
    """
    CREATE TABLE IF NOT EXISTS trade_events (
        event_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL REFERENCES trades(trade_id),
        sequence_no INTEGER NOT NULL,
        event_type TEXT NOT NULL,
        event_version INTEGER DEFAULT 1,
        idempotency_key TEXT UNIQUE,
        payload_json TEXT,
        strategy_id TEXT,
        instrument TEXT,
        timestamp REAL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(trade_id, sequence_no)
    )
    """,
    # ── canonical: fill idempotency marks ──
    """
    CREATE TABLE IF NOT EXISTS processed_fills (
        fill_id TEXT PRIMARY KEY,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        processed_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: account snapshots ──
    """
    CREATE TABLE IF NOT EXISTS account_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        equity REAL,
        realized_pnl REAL,
        unrealized_pnl REAL,
        used_margin REAL,
        available_margin REAL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER'
    )
    """,
    # ── canonical: generic audit events ──
    """
    CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        event_type TEXT,
        strategy_id TEXT,
        instrument TEXT,
        details TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER'
    )
    """,
    # ── canonical: execution-failure / protection audit events ──
    # Durable per-step record of broker submit/modify/cancel/SL/reversal step
    # failures with the full identity lineage (strategy/trade/order/broker/
    # signal), the error, the action taken and the resulting state (spec §53).
    """
    CREATE TABLE IF NOT EXISTS execution_failure_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE,
        event_type TEXT NOT NULL,
        strategy_id TEXT,
        trade_id TEXT,
        order_id TEXT,
        broker_order_id TEXT,
        signal_id TEXT,
        instrument TEXT,
        error TEXT,
        action TEXT,
        final_state TEXT,
        details TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: quarantine storage (never silently discard bad records) ──
    """
    CREATE TABLE IF NOT EXISTS quarantine_records (
        record_id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_type TEXT,
        original_id TEXT,
        reason TEXT,
        payload TEXT,
        detected_at TEXT DEFAULT (datetime('now')),
        resolution_status TEXT DEFAULT 'OPEN',
        resolved_trade_id TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER'
    )
    """,
    # ── canonical: broker order -> strategy identity mapping (mission §40) ──
    """
    CREATE TABLE IF NOT EXISTS broker_order_mapping (
        broker_order_id TEXT PRIMARY KEY,
        order_id TEXT,
        trade_id TEXT,
        strategy_id TEXT,
        instrument TEXT,
        registered_at REAL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        updated_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: per-order fill reconciliation ledger (Phase 9.7) ──
    # Broker is the authority for cumulative traded quantity: for every broker
    # order we mirror broker_cumulative_qty and the local cumulative quantity
    # actually persisted in `fills`.  gap_qty > 0 -> local behind broker
    # (unaccounted broker quantity); status 'divergence' -> local ahead of or
    # offset from broker (never silently ignored).  One row per broker order.
    """
    CREATE TABLE IF NOT EXISTS fill_reconciliation (
        broker_order_id TEXT NOT NULL,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        strategy_id TEXT,
        instrument TEXT,
        order_id TEXT,
        side TEXT,
        broker_cumulative_qty INTEGER NOT NULL DEFAULT 0,
        local_cumulative_qty INTEGER NOT NULL DEFAULT 0,
        gap_qty INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'synced',
        last_broker_fill_id TEXT,
        broker_average_price REAL,
        updated_at TEXT,
        PRIMARY KEY (broker_order_id, execution_mode)
    )
    """,
    # ── canonical: COMPLETE broker API request/response storage (spec §L) ──
    # Every material Dhan order action (LIMIT/MARKET/SL entry, cancel, modify,
    # exit, reversal, status polls, funds, positions, tradebook, circuit quote)
    # persists the sanitized request payload + response/error so the full
    # broker evidence is reconstructable without the exchange.  Credential
    # fields are masked at write time by the broker audit store.
    """
    CREATE TABLE IF NOT EXISTS broker_api_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE NOT NULL,
        action TEXT,
        endpoint TEXT,
        http_method TEXT,
        request_timestamp REAL,
        response_timestamp REAL,
        http_status INTEGER,
        request_payload TEXT,
        response_payload TEXT,
        error_type TEXT,
        error_code TEXT,
        error_message TEXT,
        broker_order_id TEXT,
        exchange_order_id TEXT,
        correlation_id TEXT,
        order_status TEXT,
        order_type TEXT,
        transaction_type TEXT,
        security_id TEXT,
        quantity INTEGER,
        remaining_quantity INTEGER,
        price REAL,
        trigger_price REAL,
        validity TEXT,
        product_type TEXT,
        exchange_segment TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: durable alert/event ledger (spec §AG) ──
    # One logical alert per configured state transition, persisted with the
    # full identity lineage and the Telegram delivery outcome.  The ledger is
    # the canonical chronological view; Telegram is only the presentation
    # channel and can be fully reconstructed from this table after a restart.
    """
    CREATE TABLE IF NOT EXISTS alert_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        event_id TEXT UNIQUE NOT NULL,
        event_type TEXT,
        event_source TEXT,
        event_timestamp REAL,
        received_timestamp REAL,
        processed_timestamp REAL,
        strategy_id TEXT,
        signal_id TEXT,
        execution_intent_id TEXT,
        trade_id TEXT,
        local_order_id TEXT,
        broker_order_id TEXT,
        correlation_id TEXT,
        security_id TEXT,
        side TEXT,
        quantity INTEGER,
        price REAL,
        trigger_price REAL,
        status_before TEXT,
        status_after TEXT,
        payload_sanitized TEXT,
        processing_status TEXT,
        error TEXT,
        telegram_status TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # ── canonical: schema / system metadata ──
    """
    CREATE TABLE IF NOT EXISTS system_metadata (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    )
    """,
    # Canonical reversal lifecycle: this is operational state, not a rebuildable
    # analytics projection. Create it before column migrations run.
    """
    CREATE TABLE IF NOT EXISTS reversals (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        reversal_id TEXT UNIQUE NOT NULL,
        signal_id TEXT UNIQUE,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        old_trade_id TEXT,
        old_position_id TEXT,
        old_exit_order_id TEXT,
        old_broker_order_id TEXT,
        old_sl_order_id TEXT,
        old_sl_state TEXT,
        new_trade_id TEXT,
        new_position_id TEXT,
        new_entry_order_id TEXT,
        new_broker_order_id TEXT,
        new_sl_order_id TEXT,
        new_sl_state TEXT,
        reversal_trigger_price REAL,
        old_exit_fill_price REAL,
        new_entry_fill_price REAL,
        old_exit_filled_quantity INTEGER,
        new_entry_filled_quantity INTEGER,
        old_exit_broker_status TEXT,
        new_entry_broker_status TEXT,
        fallback_used INTEGER DEFAULT 0,
        fallback_status TEXT,
        exit_verified_at TEXT,
        entry_fill_confirmed_at TEXT,
        status TEXT,
        failure_reason TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )
    """,
]
# ── DERIVED tables (rebuildable read-model inside the SAME database) ──
_DERIVED_DDL: list[str] = [
    # ── derived: analytics trade projection (canonical source = trades) ──
    """
    CREATE TABLE IF NOT EXISTS trades_analytics (
        trade_id TEXT PRIMARY KEY,
        strategy_id TEXT NOT NULL,
        strategy_version TEXT DEFAULT 'v1',
        parameter_hash TEXT,
        instrument TEXT NOT NULL,
        side TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'OPEN',
        signal_time REAL,
        trigger_time REAL,
        order_time REAL,
        first_fill_time REAL,
        last_exit_fill_time REAL,
        entry_quantity INTEGER NOT NULL,
        filled_quantity INTEGER NOT NULL DEFAULT 0,
        remaining_quantity INTEGER NOT NULL DEFAULT 0,
        entry_price REAL,
        average_entry_price REAL,
        initial_stop REAL,
        initial_risk REAL,
        exit_price REAL,
        average_exit_price REAL,
        exit_quantity INTEGER DEFAULT 0,
        exit_reason TEXT,
        entry_reason TEXT,
        entry_dema REAL,
        entry_atr REAL,
        entry_dema_atr REAL,
        entry_htf_value REAL,
        entry_bid REAL,
        entry_ask REAL,
        entry_spread REAL,
        entry_slippage REAL,
        entry_order_id TEXT,
        exit_order_id TEXT,
        position_id TEXT,
        session_id TEXT,
        replay_id TEXT,
        gross_pnl REAL,
        fees REAL,
        slippage_cost REAL,
        net_pnl REAL,
        return_pct REAL,
        r_multiple REAL,
        r_status TEXT,
        multiplier REAL DEFAULT 1.0,
        mfe REAL,
        mae REAL,
        max_favorable_price REAL,
        max_adverse_price REAL,
        duration_seconds REAL,
        duration_minutes REAL,
        closed_at REAL,
        created_at REAL,
        updated_at REAL
    )
    """,
    # ── derived: trade legs (one per fill; unique fill_id prevents double-count) ──
    """
    CREATE TABLE IF NOT EXISTS trade_legs (
        leg_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL,
        fill_id TEXT NOT NULL UNIQUE,
        order_id TEXT NOT NULL,
        side TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price REAL NOT NULL,
        timestamp REAL NOT NULL,
        slippage REAL,
        spread REAL,
        is_entry INTEGER NOT NULL DEFAULT 1
    )
    """,
    # ── derived: trade snapshots ──
    """
    CREATE TABLE IF NOT EXISTS trade_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        market_price REAL,
        bid REAL,
        ask REAL,
        unrealized_pnl REAL,
        payload TEXT
    )
    """,
    # ── derived: strategy daily / monthly performance ──
    """
    CREATE TABLE IF NOT EXISTS strategy_daily_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        date TEXT NOT NULL,
        trade_count INTEGER DEFAULT 0,
        net_pnl REAL,
        UNIQUE(strategy_id, date)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_monthly_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        month TEXT NOT NULL,
        trade_count INTEGER DEFAULT 0,
        net_pnl REAL,
        UNIQUE(strategy_id, month)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_parameter_results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        parameter_hash TEXT NOT NULL,
        dema_period INTEGER,
        atr_period INTEGER,
        atr_factor REAL,
        fast_timeframe TEXT,
        htf_timeframe TEXT,
        trade_count INTEGER DEFAULT 0,
        win_rate REAL,
        profit_factor REAL,
        net_pnl REAL,
        max_drawdown REAL,
        sharpe REAL,
        UNIQUE(strategy_id, parameter_hash)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        instrument TEXT,
        trade_count INTEGER DEFAULT 0,
        net_pnl REAL,
        timestamp REAL
    )
    """,
]
_SCHEMA_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_signals_strategy_id ON signals(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_strategy_id ON trades(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_status ON trades(status)",
    "CREATE INDEX IF NOT EXISTS idx_trades_instrument ON trades(instrument)",
    "CREATE INDEX IF NOT EXISTS idx_trades_created_at ON trades(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_trade_signal_link_trade_id ON trade_signal_link(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_signal_link_signal_id ON trade_signal_link(signal_id)",
    "CREATE INDEX IF NOT EXISTS idx_pending_orders_trade_id ON pending_orders(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_trade_id ON orders(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_signal_id ON orders(signal_id)",
    "CREATE INDEX IF NOT EXISTS idx_orders_role ON orders(order_role)",
    "CREATE INDEX IF NOT EXISTS idx_orders_broker_order_id ON orders(broker_order_id)",
    "CREATE INDEX IF NOT EXISTS idx_failure_events_strategy ON execution_failure_events(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_failure_events_trade ON execution_failure_events(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_positions_sl_state ON positions(sl_state)",
    "CREATE INDEX IF NOT EXISTS idx_fills_trade_id ON fills(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id)",
    "CREATE INDEX IF NOT EXISTS idx_fills_fill_id ON fills(fill_id)",
    "CREATE INDEX IF NOT EXISTS idx_positions_trade_id ON positions(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_events_seq ON trade_events(trade_id, sequence_no)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_strategy ON trades_analytics(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_status ON trades_analytics(status)",
    "CREATE INDEX IF NOT EXISTS idx_trade_legs_trade_id ON trade_legs(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_broker_api_events_order ON broker_api_events(broker_order_id)",
    "CREATE INDEX IF NOT EXISTS idx_broker_api_events_corr ON broker_api_events(correlation_id)",
    "CREATE INDEX IF NOT EXISTS idx_broker_api_events_action ON broker_api_events(action)",
    "CREATE INDEX IF NOT EXISTS idx_broker_api_events_ts ON broker_api_events(request_timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_alert_events_type ON alert_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_alert_events_order ON alert_events(broker_order_id)",
    "CREATE INDEX IF NOT EXISTS idx_alert_events_corr ON alert_events(correlation_id)",
    "CREATE INDEX IF NOT EXISTS idx_alert_events_trade ON alert_events(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_alert_events_ts ON alert_events(event_timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_reversals_strategy_id ON reversals(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_reversals_signal_id ON reversals(signal_id)",
    "CREATE INDEX IF NOT EXISTS idx_reversals_status ON reversals(status)",
]

# Legacy tables predate the canonical schema and cannot receive foreign keys
# through ALTER TABLE. These triggers provide equivalent insert/update
# enforcement without rebuilding production tables during startup.
_INTEGRITY_TRIGGERS: list[str] = [
    """
    CREATE TRIGGER IF NOT EXISTS trg_trades_entry_signal_required
    BEFORE INSERT ON trades
    WHEN NEW.entry_signal_id IS NULL OR NEW.entry_signal_id = ''
    BEGIN SELECT RAISE(ABORT, 'trade requires entry_signal_id'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_trades_entry_signal_exists
    BEFORE INSERT ON trades
    WHEN NOT EXISTS (SELECT 1 FROM signals WHERE signal_id = NEW.entry_signal_id)
    BEGIN SELECT RAISE(ABORT, 'trade references missing entry signal'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_orders_trade_required
    BEFORE INSERT ON orders
    WHEN NEW.trade_id IS NULL OR NEW.trade_id = ''
    BEGIN SELECT RAISE(ABORT, 'order requires trade_id'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_orders_trade_exists
    BEFORE INSERT ON orders
    WHEN NOT EXISTS (SELECT 1 FROM trades WHERE trade_id = NEW.trade_id)
    BEGIN SELECT RAISE(ABORT, 'order references missing trade'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_fills_lineage_required
    BEFORE INSERT ON fills
    WHEN NEW.trade_id IS NULL OR NEW.trade_id = '' OR NEW.order_id IS NULL OR NEW.order_id = ''
    BEGIN SELECT RAISE(ABORT, 'fill requires trade_id and order_id'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_fills_trade_exists
    BEFORE INSERT ON fills
    WHEN NOT EXISTS (SELECT 1 FROM trades WHERE trade_id = NEW.trade_id)
    BEGIN SELECT RAISE(ABORT, 'fill references missing trade'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_fills_order_exists
    BEFORE INSERT ON fills
    WHEN NOT EXISTS (SELECT 1 FROM orders WHERE order_id = NEW.order_id)
    BEGIN SELECT RAISE(ABORT, 'fill references missing order'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_trade_signal_link_trade_exists
    BEFORE INSERT ON trade_signal_link
    WHEN NOT EXISTS (SELECT 1 FROM trades WHERE trade_id = NEW.trade_id)
    BEGIN SELECT RAISE(ABORT, 'trade signal link references missing trade'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_trade_signal_link_signal_exists
    BEFORE INSERT ON trade_signal_link
    WHEN NOT EXISTS (SELECT 1 FROM signals WHERE signal_id = NEW.signal_id)
    BEGIN SELECT RAISE(ABORT, 'trade signal link references missing signal'); END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS trg_positions_identity_separate
    BEFORE INSERT ON positions
    WHEN NEW.position_id = NEW.trade_id
    BEGIN SELECT RAISE(ABORT, 'position_id must differ from trade_id'); END
    """,
]

# Idempotent ALTER TABLE column additions for legacy tables (trading.db tables
# created by the old PersistenceManager schema lacked these columns).
_ALTER_MIGRATIONS: list[tuple[str, str, str]] = [
    ("trades", "realized_pnl", "REAL"),
    ("trades", "exit_type", "TEXT"),
    ("trades", "updated_at", "TEXT"),
    ("trades", "entry_order_id", "TEXT"),
    ("trades", "exit_order_id", "TEXT"),
    ("trades", "position_id", "TEXT"),
    ("trades", "entry_fill_id", "TEXT"),
    ("trades", "exit_fill_id", "TEXT"),
    ("trades", "security_id", "TEXT"),
    # Base columns referenced by triggers/indexes (idx_trades_created_at,
    # idx_trades_status, idx_trades_instrument, idx_trades_strategy_id) that a
    # bare legacy `trades` table may predate.  Upgraded here so a restored
    # pre-fix DB cannot abort schema init with `no such column`.
    ("trades", "created_at", "TEXT"),
    ("trades", "status", "TEXT"),
    ("trades", "instrument", "TEXT"),
    ("trades", "strategy_id", "TEXT"),
    # Signal lineage columns used by trades-row writes + integrity triggers;
    # an old-format trades table may lack them entirely.
    ("trades", "entry_signal_id", "TEXT"),
    ("trades", "exit_signal_id", "TEXT"),
    ("orders", "trade_id", "TEXT"),
    ("orders", "pending_order_id", "TEXT"),
    ("orders", "signal_id", "TEXT"),
    ("orders", "broker_order_id", "TEXT"),
    ("events", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("quarantine_records", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("orders", "order_intent", "TEXT"),
    ("orders", "planned_entry_price", "REAL"),
    ("orders", "planned_sl", "REAL"),
    ("orders", "planned_order_type", "TEXT"),
    ("orders", "lifecycle_id", "TEXT"),
    ("orders", "parent_signal_id", "TEXT"),
    ("orders", "position_id", "TEXT"),
    ("orders", "parent_position_id", "TEXT"),
    ("orders", "position_generation", "INTEGER"),
    ("orders", "original_order_id", "TEXT"),
    ("orders", "reversal_parent_signal_id", "TEXT"),
    ("reversals", "failure_reason", "TEXT"),
    ("orders", "trigger_state", "TEXT"),
    ("orders", "trigger_generation", "INTEGER"),
    ("orders", "trigger_source", "TEXT"),
    ("pending_orders", "strategy_id", "TEXT"),
    ("pending_orders", "instrument", "TEXT"),
    ("pending_orders", "direction", "TEXT"),
    ("pending_orders", "trigger_state", "TEXT"),
    ("pending_orders", "trigger_generation", "INTEGER"),
    ("pending_orders", "trigger_source", "TEXT"),
    ("pending_orders", "signal_timestamp", "REAL"),
    ("orders", "order_role", "TEXT"),
    ("fills", "trade_id", "TEXT"),
    ("fills", "broker_fill_id", "TEXT"),
    ("fills", "position_id", "TEXT"),
    ("fills", "lifecycle_id", "TEXT"),
    ("fills", "position_generation", "INTEGER"),
    ("fills", "fill_type", "TEXT"),
    ("positions", "position_generation", "INTEGER"),
    ("positions", "exit_started", "INTEGER NOT NULL DEFAULT 0"),
    ("positions", "lifecycle_id", "TEXT"),
    ("signals", "security_id", "TEXT"),
    ("signals", "timeframe", "TEXT"),
    ("signals", "signal_timestamp", "REAL"),
    ("signals", "candle_timestamp", "REAL"),
    ("signals", "open", "REAL"),
    ("signals", "high", "REAL"),
    ("signals", "low", "REAL"),
    ("signals", "close", "REAL"),
    ("signals", "volume", "REAL"),
    ("signals", "htf_value", "REAL"),
    ("signals", "mid_value", "REAL"),
    ("signals", "fast_dema", "REAL"),
    ("signals", "fast_atr", "REAL"),
    ("signals", "signal_reason", "TEXT"),
    ("signals", "signal_metadata", "TEXT"),
    ("trade_events", "event_version", "INTEGER"),
    ("trade_events", "payload_json", "TEXT"),
    ("trade_events", "sequence_no", "INTEGER"),
    # ── LIVE/PAPER environment tagging (schema v3). Every execution record
    #    carries execution_mode ('PAPER'/'LIVE'); legacy files get the column
    #    idempotently with DEFAULT 'PAPER' so existing rows remain valid.
    ("signals", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("trades", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("trade_signal_link", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("pending_orders", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    # ── Phase 9.6 — durable LIVE pending-order lifecycle columns.  A pending
    #    order row is durably PENDING/ARMED before any trade exists (trades are
    #    born only at trigger) and records the broker correlation the moment the
    #    entry is placed (ENTRY_SENT).
    ("pending_orders", "broker_order_id", "TEXT"),
    ("pending_orders", "correlation_id", "TEXT"),
    ("pending_orders", "expired_reason", "TEXT"),
    ("pending_orders", "armed_at", "TEXT"),
    ("orders", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("fills", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    # ── Schema v4 — order identity model (spec §12-14): explicit order_role
    #    (ENTRY/EXIT/REVERSAL_EXIT/REVERSAL_ENTRY/EMERGENCY_EXIT), plus the
    #    trigger_price / correlation_id lineage columns.
    #    NOTE: the broker-side protective SL was retired.  ``protected_order_id``
    #    and ``positions.sl_order_id`` are LEGACY columns kept read-only so old
    #    audit rows stay intelligible; nothing writes them any more.
    ("orders", "order_role", "TEXT"),
    ("orders", "trigger_price", "REAL"),
    ("orders", "protected_order_id", "TEXT"),
    ("orders", "correlation_id", "TEXT"),
    # ── Position-owned SL state (local monitor lifecycle only:
    #    NONE/ARMED/TRIGGERED/EXITING/CLOSED/SL_UNAVAILABLE).  NEVER means
    #    "a broker order is resting".
    ("positions", "sl_state", "TEXT"),
    ("positions", "sl_order_id", "TEXT"),
    ("positions", "sl_trigger_price", "REAL"),
    ("positions", "sl_protected_at", "TEXT"),
    # ── Broker entry/exit order ids for the position (audit lineage + the
    #    startup SL stop resolver).
    ("positions", "entry_order_id", "TEXT"),
    ("positions", "exit_order_id", "TEXT"),
    # ── Schema v5 — the position's OWN stop level.  This is the single
    #    authority for the stop; it is never derived from an old signal, an old
    #    position, or a previous reversal.
    ("positions", "stop_price", "REAL"),
    ("trades", "stop_price", "REAL"),
    # ── Phase 9.7 — fill reconciliation ledger columns: the broker-native
    #    execution identity (broker order / broker trade) and the cumulative
    #    traded quantity at the moment of the delta, so local cumulative per
    #    broker order can be proven == broker cumulative across restarts.
    ("fills", "broker_order_id", "TEXT"),
    ("fills", "broker_trade_id", "TEXT"),
    ("fills", "cumulative_filled_quantity", "INTEGER"),
    ("positions", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("trade_events", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("processed_fills", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("account_snapshots", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
    ("broker_order_mapping", "execution_mode", "TEXT NOT NULL DEFAULT 'PAPER'"),
]
# ────────────────────────────────────────────────────────────────
# Process-wide shared connection registry, keyed by resolved db path.
#
# Every component (EventStore, TradeLedger, FillDeduplicator, recovery and
# PersistenceManager) opens a `Database` on the SAME trading.db file.  Keeping
# per-instance thread-local connections meant up to four independent SQLite
# connections (each with its own write lock) wrote to one file concurrently,
# which produced real `database is locked` errors under trade load.  Sharing a
# single connection and a single write lock per path enforces the intended
# single-writer model process-wide.
# ────────────────────────────────────────────────────────────────
_shared_conns: dict[str, sqlite3.Connection] = {}
_shared_locks: dict[str, threading.RLock] = {}
_shared_refs: dict[str, int] = {}
_shared_registry_lock = threading.RLock()
# Ultra-low-latency: the resolved key is identical for every transaction on a
# db file; Path.resolve()/realpath() hits the filesystem EVERY call (measured
# ~0.06ms per _db_key on the hot order path). Memoize the resolved key.
_db_key_cache: dict[str, str] = {}
_DB_KEY_CACHE_MAX = 512


def _db_key(db_path: str | Path) -> str:
    raw = str(db_path)
    key = _db_key_cache.get(raw)
    if key is None:
        key = str(Path(raw).resolve())
        if len(_db_key_cache) >= _DB_KEY_CACHE_MAX:
            _db_key_cache.clear()
        _db_key_cache[raw] = key
    return key


def shared_path_lock(db_path: str | Path) -> threading.RLock:
    """Return the process-wide write lock for a db file path."""
    key = _db_key(db_path)
    with _shared_registry_lock:
        return _shared_locks.setdefault(key, threading.RLock())


def _open_connection(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    # THE critical invariant — enforced on every connection:
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def shared_connection(db_path: str | Path) -> sqlite3.Connection:
    """Return the process-wide connection for a db file path (created once)."""
    key = _db_key(db_path)
    with _shared_registry_lock:
        conn = _shared_conns.get(key)
        if conn is None:
            Path(str(db_path)).parent.mkdir(parents=True, exist_ok=True)
            conn = _open_connection(db_path)
            _shared_conns[key] = conn
        return conn


def _release_shared_connection(db_path: str | Path) -> None:
    """Close the shared connection for a path and drop its registry state."""
    global_key = _db_key(db_path)
    with _shared_registry_lock:
        conn = _shared_conns.pop(global_key, None)
        _shared_locks.pop(global_key, None)
        _shared_refs.pop(global_key, None)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


def reset_shared_connection(db_path: str | Path) -> sqlite3.Connection:
    """Close the process-wide connection for a path and open a FRESH one.

    Used when the shared connection cannot be un-stuck (e.g. rollback fails).
    The lock and reference-count registry entries are intentionally preserved
    so every live :class:`Database`/manager handle keeps working; cached
    sqlite3.Connection handles must re-resolve via :func:`shared_connection`.
    """
    key = _db_key(db_path)
    with _shared_registry_lock:
        conn = _shared_conns.pop(key, None)
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        fresh = _open_connection(db_path)
        _shared_conns[key] = fresh
        return fresh


# ── Phase 9.6 — pending_orders table rebuild ─────────────────────────
# SQLite cannot drop a NOT NULL constraint via ALTER TABLE.  Legacy
# `pending_orders` tables declared `trade_id TEXT NOT NULL REFERENCES...`,
# which makes it impossible to persist a pending order BEFORE its trade is
# born (trades are created only at trigger).  The rebuild (copy -> drop ->
# rename) relaxes trade_id to nullable and gains the durable lifecycle
# columns (broker_order_id / correlation_id / expired_reason / armed_at) in
# one pass.  Idempotent: skipped as soon as trade_id is already nullable.
_PENDING_ORDERS_V96_DDL = """
    CREATE TABLE pending_orders_v96 (
        pending_order_id TEXT PRIMARY KEY,
        trade_id TEXT REFERENCES trades(trade_id),
        signal_id TEXT,
        side TEXT,
        order_type TEXT,
        trigger_price REAL,
        quantity INTEGER,
        status TEXT,
        execution_mode TEXT NOT NULL DEFAULT 'PAPER',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT,
        broker_order_id TEXT,
        correlation_id TEXT,
        expired_reason TEXT,
        armed_at TEXT
    )
    """


def _rebuild_pending_orders_trade_id(conn: "sqlite3.Connection") -> None:
    """Relax `pending_orders.trade_id` to nullable (legacy DBs only)."""
    cols = {
        r["name"]: r for r in conn.execute("PRAGMA table_info(pending_orders)")
    }
    trade_id_col = cols.get("trade_id")
    if trade_id_col is None or trade_id_col["notnull"] == 0:
        return
    conn.execute(_PENDING_ORDERS_V96_DDL)
    conn.execute("""
        INSERT INTO pending_orders_v96 (
            pending_order_id, trade_id, signal_id, side, order_type,
            trigger_price, quantity, status, execution_mode, created_at,
            updated_at
        ) SELECT
            pending_order_id, trade_id, signal_id, side, order_type,
            trigger_price, quantity, status, execution_mode, created_at,
            updated_at
        FROM pending_orders
    """)
    conn.execute("DROP TABLE pending_orders")
    conn.execute("ALTER TABLE pending_orders_v96 RENAME TO pending_orders")


class Database:
    """Central access to the canonical SQLite database (trading.db).

    Thread safety: all instances on the same db file SHARE one connection and
    one process-wide write lock (module-level registry), so concurrent writers
    can never contend on the file.  Every connection enables
    ``foreign_keys=ON``.  Write transactions are serialized by the shared lock.
    """

    def __init__(self, db_path: str | Path | None = None):
        if db_path is None:
            cfg_path = Config.get("system.db_path", "trading.db")
            db_path = Config.resolve_path(cfg_path)
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._key = _db_key(self.db_path)
        # Shared process-wide write lock for THIS db file.
        self._write_lock = shared_path_lock(self.db_path)
        with _shared_registry_lock:
            _shared_refs[self._key] = _shared_refs.get(self._key, 0) + 1
        self.init_schema()

    # ── connection management ────────────────────────────────────

    def _new_connection(self) -> sqlite3.Connection:
        return _open_connection(self.db_path)

    def get_conn(self) -> sqlite3.Connection:
        return shared_connection(self.db_path)

    def close(self) -> None:
        """Release this instance's reference to the shared connection.

        The shared connection is actually closed only when the LAST live
        Database instance for the path is closed.
        """
        with _shared_registry_lock:
            ref = _shared_refs.get(self._key, 0) - 1
            if ref <= 0:
                _shared_refs.pop(self._key, None)
                conn = _shared_conns.pop(self._key, None)
                _shared_locks.pop(self._key, None)
            else:
                _shared_refs[self._key] = ref
                conn = None
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    # ── transactions ─────────────────────────────────────────────

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Real DB transaction: BEGIN IMMEDIATE / COMMIT / ROLLBACK.

        Self-healing and leak-proof:

        * while holding the process-wide write lock the connection must not be
          mid-transaction — if one is open it was LEAKED by a writer that
          raised BaseException (``except Exception`` cannot see those, so the
          old code left the tx open forever and every later ``BEGIN IMMEDIATE``
          on the shared connection died with "cannot start a transaction
          within a transaction").  Roll it back / recreate so the process
          keeps writing.
        * ``except BaseException`` guarantees a rollback even for
          KeyboardInterrupt/SystemExit/GeneratorExit so a transaction can
          never be leaked by this context manager again.
        """
        conn = self.get_conn()
        with self._write_lock:
            if getattr(conn, "in_transaction", False):
                try:
                    conn.rollback()  # heal a leaked (abandoned) transaction
                except Exception:
                    reset_shared_connection(self.db_path)
                    conn = self.get_conn()
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
                if getattr(conn, "in_transaction", False):
                    conn.commit()
            except BaseException:
                try:
                    if getattr(conn, "in_transaction", False):
                        conn.rollback()
                finally:
                    raise

    # ── query helpers ────────────────────────────────────────────
    # All queries go through the shared write lock to prevent
    # "database is locked" errors when multiple threads access the
    # single shared connection concurrently.

    def execute(self, sql: str, params: tuple | list = ()) -> sqlite3.Cursor:
        """Run a standalone statement on the shared connection and COMMIT.

        C11 — a DML statement must never be left sitting in an implicit
        transaction on the shared connection: the next ``transaction()``
        self-heal would roll it back (the leaked-tx heal discards committed
        intent).  ``execute()`` takes the write lock, runs the statement and
        commits immediately, so standalone INSERT/UPDATE/DELETE callers can
        never lose their write to a later heal-rollback.
        """
        with self._write_lock:
            conn = self.get_conn()
            cursor = conn.execute(sql, params)
            # Commit leaked-work too: if a crashed writer left an open
            # transaction, committing what it already wrote is safer than the
            # alternative silent discard.
            if getattr(conn, "in_transaction", False):
                try:
                    conn.commit()
                except Exception:
                    reset_shared_connection(self.db_path)
            return cursor

    def query(self, sql: str, params: tuple | list = ()) -> list[dict]:
        with self._write_lock:
            rows = self.get_conn().execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def query_one(self, sql: str, params: tuple | list = ()) -> Optional[dict]:
        with self._write_lock:
            row = self.get_conn().execute(sql, params).fetchone()
        return dict(row) if row else None

    def scalar(self, sql: str, params: tuple | list = ()):
        with self._write_lock:
            row = self.get_conn().execute(sql, params).fetchone()
        return row[0] if row else None

    # ── schema / migration ───────────────────────────────────────

    def init_schema(self) -> None:
        """Create canonical + derived tables, indexes, run idempotent ALTERs.

        Order is deliberate: column migrations run IMMEDIATELY after the base
        DDL and BEFORE any trigger/index DDL that references those columns.
        This makes startup self-healing for stale/legacy DB files — a restored
        pre-fix trading.db (or any table missing newer columns) is upgraded
        before anything can fail with ``no such column``/``no such table``.
        """
        with self.transaction() as conn:
            for ddl in _SCHEMA_DDL:
                conn.execute(ddl)
            for table, column, decl in _ALTER_MIGRATIONS:
                cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
                if column not in cols:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
            _rebuild_pending_orders_trade_id(conn)
            for ddl in _DERIVED_DDL:
                conn.execute(ddl)
            for trigger in _INTEGRITY_TRIGGERS:
                conn.execute(trigger)
            for idx in _SCHEMA_INDEXES:
                try:
                    conn.execute(idx)
                except Exception as e:  # keep startup alive on pathological tables
                    log.warning("[Schema] skipping index (self-heal continues): %s", e)
            conn.execute(
                "INSERT OR REPLACE INTO system_metadata (key, value, updated_at) "
                "VALUES ('schema_version', ?, datetime('now'))",
                (str(SCHEMA_VERSION),),
            )

    def table_exists(self, table: str) -> bool:
        row = self.scalar(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        return row is not None

    def table_columns(self, table: str) -> list[str]:
        return [r["name"] for r in self.query(f"PRAGMA table_info({table})")]

    # ── integrity ────────────────────────────────────────────────

    def integrity_check(self) -> list[str]:
        return [r[0] for r in self.get_conn().execute("PRAGMA integrity_check")]

    def foreign_key_check(self) -> list[dict]:
        return [dict(r) for r in self.get_conn().execute("PRAGMA foreign_key_check")]

    def foreign_keys_enabled(self) -> bool:
        return bool(self.scalar("PRAGMA foreign_keys"))


# ────────────────────────────────────────────────────────────────
# Process-wide singleton — every component shares the same canonical
# database, so there is exactly ONE source of truth per process.
# ────────────────────────────────────────────────────────────────
_database: Optional[Database] = None
_database_lock = threading.Lock()


def get_database(db_path: str | Path | None = None) -> Database:
    """Return the process-wide canonical Database (created lazily once)."""
    global _database
    if _database is not None and db_path is None:
        return _database
    with _database_lock:
        if _database is None:
            _database = Database(db_path)
    return _database


def reset_database_for_tests(db_path: str | Path | None = None) -> Database:
    """Replace the canonical Database (used by tests to point at a tmp DB file)."""
    global _database
    with _database_lock:
        if _database is not None:
            _database.close()
        if db_path is not None:
            # Drop any shared connection already parked on this path so the
            # test starts from a clean, freshly-opened connection/file.
            _release_shared_connection(db_path)
        _database = Database(db_path)
    return _database


def close_database() -> None:
    """Close the canonical Database (call on application shutdown)."""
    global _database
    with _database_lock:
        if _database is not None:
            _database.close()
            _database = None
        with _shared_registry_lock:
            for key in list(_shared_conns):
                _release_shared_connection(key)
