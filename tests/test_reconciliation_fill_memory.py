import sqlite3
import threading

from core.lifecycle import TradeContext, TradeLifecycleManager
from persistence.manager import PersistenceManager
from reconciliation.engine import ReconciliationEngine, ReconciliationResult


def _check_fills(db_fills, mem_fills, db_trades):
    engine = object.__new__(ReconciliationEngine)
    result = ReconciliationResult()
    engine._check_db_vs_memory_fills(db_fills, mem_fills, db_trades, result)
    return result


def test_closed_trade_history_missing_from_bounded_memory_is_not_a_mismatch():
    result = _check_fills(
        [{"fill_id": "entry"}, {"fill_id": "exit"}],
        {},
        [{"status": "closed", "entry_fill_id": "entry", "exit_fill_id": "exit"}],
    )

    assert result.warnings == []
    assert result.errors == []


def test_open_trade_fill_missing_from_memory_still_warns():
    result = _check_fills(
        [{"fill_id": "entry"}],
        {},
        [{"status": "open", "entry_fill_id": "entry", "exit_fill_id": None}],
    )

    assert len(result.warnings) == 1
    assert "entry" in result.warnings[0]


def test_trade_fill_linking_repairs_only_exact_null_or_same_trade_identity():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE fills (fill_id TEXT, strategy_id TEXT, trade_id TEXT, lifecycle_id TEXT)"
    )
    conn.executemany(
        "INSERT INTO fills VALUES (?, ?, ?, ?)",
        [
            ("entry", "silver_01", None, None),
            ("exit", "silver_01", "trade-1", None),
            ("wrong-owner", "gold_02", None, None),
            ("claimed", "silver_01", "trade-other", None),
        ],
    )

    repaired = PersistenceManager._link_trade_fill_ids(
        conn,
        {
            "trade_id": "trade-1",
            "strategy_id": "silver_01",
            "entry_fill_id": "entry",
            "exit_fill_id": "exit",
        },
    )

    assert repaired == 2
    rows = dict(conn.execute("SELECT fill_id, trade_id FROM fills"))
    assert rows == {
        "entry": "trade-1",
        "exit": "trade-1",
        "wrong-owner": None,
        "claimed": "trade-other",
    }
    conn.close()


def test_stale_runtime_snapshot_cannot_erase_database_fill_ownership():
    manager = object.__new__(TradeLifecycleManager)
    manager._lock = threading.RLock()
    manager._trades = {
        "trade-1": TradeContext(
            trade_id="trade-1", entry_fill_id="durable-entry",
            status="CLOSED",
        )
    }
    manager._signal_to_trade = {}
    manager._order_to_trade = {}
    manager._fill_to_trade = {"durable-entry": "trade-1"}
    manager._position_to_trade = {}
    manager._pending_to_trade = {}
    stale = TradeContext(trade_id="trade-1", status="OPEN").snapshot()

    manager.restore({"trades": {"trade-1": stale}, "fill_to_trade": {}})

    assert manager._trades["trade-1"].status == "CLOSED"
    assert manager._trades["trade-1"].entry_fill_id == "durable-entry"
    assert manager._fill_to_trade["durable-entry"] == "trade-1"


def test_strategy_lifecycle_reconciliation_ignores_other_strategy_fills():
    manager = object.__new__(TradeLifecycleManager)
    manager._lock = threading.RLock()
    manager._trades = {}
    manager._strategy_id = "silver_01"
    manager._fill_to_trade = {}
    manager._persistence = type("Persistence", (), {
        "get_fills": lambda self: [
            {"fill_id": "silver-fill", "strategy_id": "silver_01"},
            {"fill_id": "gold-fill", "strategy_id": "gold_02"},
        ]
    })()

    result = manager.reconcile()

    assert result["warnings"] == [
        {"type": "ORPHAN_FILL", "fill_id": "silver-fill", "order_id": ""}
    ]
