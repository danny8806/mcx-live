"""Database schema definitions for the strategy analytics system.

Provides SQL statements for creating all analytics tables and indexes,
along with an initialization function to set up the database.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

# ---------------------------------------------------------------------------
# Table creation SQL
# ---------------------------------------------------------------------------

CREATE_TABLES: list[str] = [
    # Canonical event store (immutable)
    """
    CREATE TABLE IF NOT EXISTS trade_events (
        event_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        timestamp REAL NOT NULL,
        event_type TEXT NOT NULL,
        source TEXT DEFAULT 'system',
        payload TEXT,
        sequence_number INTEGER
    )
    """,
    # Trade ledger (the authoritative trade record)
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
    # Trade legs (individual fills)
    """
    CREATE TABLE IF NOT EXISTS trade_legs (
        leg_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL,
        fill_id TEXT NOT NULL,
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
    # Trade snapshots (periodic open trade state)
    """
    CREATE TABLE IF NOT EXISTS trade_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        trade_id TEXT NOT NULL,
        timestamp REAL NOT NULL,
        market_price REAL,
        bid REAL,
        ask REAL,
        spread REAL,
        quantity INTEGER,
        average_entry REAL,
        unrealized_pnl REAL,
        realized_pnl REAL,
        equity REAL,
        margin REAL,
        drawdown REAL,
        mfe REAL,
        mae REAL,
        price_distance_to_stop REAL,
        price_distance_to_trigger REAL,
        strategy_state TEXT
    )
    """,
    # Strategy daily performance (derived)
    """
    CREATE TABLE IF NOT EXISTS strategy_daily_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        trade_count INTEGER DEFAULT 0,
        winning_trades INTEGER DEFAULT 0,
        losing_trades INTEGER DEFAULT 0,
        gross_profit REAL DEFAULT 0,
        gross_loss REAL DEFAULT 0,
        net_pnl REAL DEFAULT 0,
        fees REAL DEFAULT 0,
        win_rate REAL DEFAULT 0,
        profit_factor REAL,
        max_drawdown REAL DEFAULT 0,
        UNIQUE(date, strategy_id)
    )
    """,
    # Strategy monthly performance (derived)
    """
    CREATE TABLE IF NOT EXISTS strategy_monthly_performance (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        month TEXT NOT NULL,
        strategy_id TEXT NOT NULL,
        instrument TEXT NOT NULL,
        trades INTEGER DEFAULT 0,
        wins INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        gross_pnl REAL DEFAULT 0,
        fees REAL DEFAULT 0,
        net_pnl REAL DEFAULT 0,
        profit_factor REAL,
        win_rate REAL DEFAULT 0,
        max_drawdown REAL DEFAULT 0,
        UNIQUE(month, strategy_id)
    )
    """,
    # Strategy performance snapshots (cached aggregates)
    """
    CREATE TABLE IF NOT EXISTS strategy_performance_snapshots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        strategy_id TEXT NOT NULL,
        snapshot_time REAL NOT NULL,
        trade_count INTEGER,
        win_rate REAL,
        profit_factor REAL,
        net_pnl REAL,
        max_drawdown REAL,
        sharpe REAL,
        sortino REAL,
        expectancy REAL,
        avg_trade REAL,
        avg_win REAL,
        avg_loss REAL,
        max_consecutive_wins INTEGER,
        max_consecutive_losses INTEGER,
        UNIQUE(strategy_id, snapshot_time)
    )
    """,
    # Strategy parameter results
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
]

# ---------------------------------------------------------------------------
# Index creation SQL
# ---------------------------------------------------------------------------

CREATE_INDEXES: list[str] = [
    "CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_events_strategy_id ON trade_events(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_events_timestamp ON trade_events(timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_trade_events_type ON trade_events(event_type)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_strategy ON trades_analytics(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_status ON trades_analytics(status)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_instrument ON trades_analytics(instrument)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_signal_time ON trades_analytics(signal_time)",
    "CREATE INDEX IF NOT EXISTS idx_trades_analytics_closed_at ON trades_analytics(closed_at)",
    "CREATE INDEX IF NOT EXISTS idx_trade_legs_trade_id ON trade_legs(trade_id)",
    # One fill must map to exactly ONE leg anywhere in the ledger.  Enforce at
    # the schema level so a duplicated/replayed fill can never multiply a leg
    # (defense-in-depth behind the engine's fill_dedup/get_fill guards).
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_legs_fill_id ON trade_legs(fill_id)",
    "CREATE INDEX IF NOT EXISTS idx_trade_snapshots_trade_id ON trade_snapshots(trade_id)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_daily_strategy ON strategy_daily_performance(strategy_id)",
    "CREATE INDEX IF NOT EXISTS idx_strategy_monthly_strategy ON strategy_monthly_performance(strategy_id)",
]


def init_analytics_db(db_path: str | Path) -> None:
    """Initialize the analytics database.

    Creates all required tables and indexes if they do not already exist.
    Uses WAL journal mode for better concurrent read performance.

    Args:
        db_path: Path to the SQLite database file. Parent directories are
                 created automatically if they do not exist.

    Raises:
        sqlite3.Error: If any SQL statement fails to execute.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")

        for ddl in CREATE_TABLES:
            conn.execute(ddl)

        for idx in CREATE_INDEXES:
            conn.execute(idx)

        # Migration: add multiplier column to trades_analytics if missing
        try:
            conn.execute("ALTER TABLE trades_analytics ADD COLUMN multiplier REAL DEFAULT 1.0")
        except sqlite3.OperationalError:
            pass  # Column already exists

        conn.commit()
    finally:
        conn.close()
