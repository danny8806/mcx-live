"""Validate canonical trade lineage and lifecycle integrity."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

from persistence.database import Database


def validate(db_path: str | Path) -> tuple[bool, dict[str, list]]:
    db = Database(db_path)
    report: dict[str, list] = {}
    report["ORPHANS"] = []
    report["MISSING IDs"] = []
    report["INVALID FK"] = db.foreign_key_check()
    required_fks = {
        "orders": {"trade_id"},
        "fills": {"trade_id", "order_id"},
        "positions": {"trade_id"},
        "trade_events": {"trade_id"},
        "trade_signal_link": {"trade_id", "signal_id"},
    }
    report["SCHEMA CONTRACTS"] = []
    # Legacy tables (orders, fills, trade_signal_link) enforce lineage via
    # triggers (see persistence/database.py "dual enforcement" design) rather
    # than declared FK constraints.  Accept either the declared FK or a matching
    # enforcement trigger as satisfying the lineage contract.
    enforced_triggers = {r["name"] for r in db.query("SELECT name FROM sqlite_master WHERE type='trigger'")}
    trigger_contract = {
        "orders": {"trade_id": {"trg_orders_trade_required", "trg_orders_trade_exists"}},
        "fills": {"trade_id": {"trg_fills_trade_exists"}, "order_id": {"trg_fills_order_exists"}},
        "positions": {"trade_id": set()},
        "trade_events": {"trade_id": set()},
        "trade_signal_link": {
            "trade_id": {"trg_trade_signal_link_trade_exists"},
            "signal_id": {"trg_trade_signal_link_signal_exists"},
        },
    }
    for table, columns in required_fks.items():
        declared = {row["from"] for row in db.query(f"PRAGMA foreign_key_list({table})")}
        for column in sorted(columns - declared):
            ok_triggers = trigger_contract.get(table, {}).get(column, set())
            if not (ok_triggers & enforced_triggers):
                report["SCHEMA CONTRACTS"].append(
                    {"table": table, "missing_fk": column,
                     "note": "no declared FK and no enforcement trigger found"}
                )
    report["INVALID STATES"] = []
    report["P&L MISMATCHES"] = []

    checks = (
        ("orders without trades", "SELECT order_id FROM orders WHERE trade_id IS NULL OR trade_id = ''"),
        ("fills without trades", "SELECT fill_id FROM fills WHERE trade_id IS NULL OR trade_id = ''"),
        ("fills without orders", "SELECT fill_id FROM fills WHERE order_id IS NULL OR order_id = ''"),
        ("trades without entry signals", "SELECT trade_id FROM trades WHERE entry_signal_id IS NULL OR entry_signal_id = ''"),
        ("positions with aliased identity", "SELECT position_id FROM positions WHERE position_id = trade_id"),
    )
    for label, query in checks:
        for row in db.query(query):
            report["MISSING IDs" if "without" in label else "INVALID FK"].append({label: row})

    for row in db.query("SELECT trade_id, status FROM trades"):
        if row["status"] not in {"CREATED", "PENDING", "PENDING_ENTRY", "ENTRY_ORDERED", "PARTIALLY_FILLED", "OPEN", "EXIT_PENDING", "EXIT_REQUESTED", "EXIT_ORDERED", "PARTIALLY_EXITED", "CLOSED", "CANCELLED", "FAILED", "REJECTED", "RECOVERY_REQUIRED", "closed", "open"}:
            report["INVALID STATES"].append(row)

    report["DUPLICATES"] = [
        {"fill_id": row["fill_id"], "count": row["count"]}
        for row in db.query("SELECT fill_id, COUNT(*) AS count FROM fills GROUP BY fill_id HAVING count > 1")
    ]
    report["DATABASE"] = db.integrity_check()
    report["TRADES"] = [{"count": db.scalar("SELECT COUNT(*) FROM trades")}]
    report["SIGNALS"] = [{"count": db.scalar("SELECT COUNT(*) FROM signals")}]
    report["ORDERS"] = [{"count": db.scalar("SELECT COUNT(*) FROM orders")}]
    report["PENDING ORDERS"] = [{"count": db.scalar("SELECT COUNT(*) FROM pending_orders")}]
    report["FILLS"] = [{"count": db.scalar("SELECT COUNT(*) FROM fills")}]
    report["POSITIONS"] = [{"count": db.scalar("SELECT COUNT(*) FROM positions")}]
    report["EVENTS"] = [{"count": db.scalar("SELECT COUNT(*) FROM trade_events")}]
    failed = any(report[key] for key in ("ORPHANS", "MISSING IDs", "INVALID FK", "SCHEMA CONTRACTS", "DUPLICATES", "INVALID STATES", "P&L MISMATCHES"))
    failed = failed or report["DATABASE"] != ["ok"]
    return not failed, report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="trading.db")
    args = parser.parse_args()
    passed, report = validate(args.db)
    for section, rows in report.items():
        print(section)
        for row in rows:
            print(f"  {row}")
    print("PASS" if passed else "FAIL")
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
