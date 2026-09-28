"""Order lineage trace / audit tool for the live system.

Reconstructs the full identity lineage for any order, from any handle:

    python -m live.audit_order <order_id>
    python -m live.audit_order --broker <broker_order_id>
    python -m live.audit_order --json <id>

Accepts a local order id, broker order id, correlation id, trade id, signal
id, or fill id and walks: pending lifecycle -> order row -> broker mapping ->
fills -> position -> SL / exit -> trade P&L -> fill reconciliation ->
lifecycle/failure events.  Optional ``--broker`` performs a READ-ONLY Dhan
status/trades lookup (offline by default).

Exit codes: 0 clean, 1 identity not found, 2 anomalies detected, 3 cannot
open the database.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from persistence.database import Database

COLS = {
    "orders": [
        "order_id", "trade_id", "pending_order_id", "signal_id",
        "broker_order_id", "strategy_id", "instrument", "side", "quantity",
        "order_type", "price", "order_intent", "state", "filled_quantity",
        "average_fill_price", "created_at", "updated_at", "execution_mode",
        "planned_entry_price", "planned_sl", "planned_order_type",
        "order_role", "trigger_price", "protected_order_id", "correlation_id",
        "status", "entry_price", "entry_time", "kind", "limit_price",
    ],
    "pending_orders": [
        "pending_order_id", "trade_id", "signal_id", "side", "order_type",
        "trigger_price", "quantity", "status", "execution_mode", "created_at",
        "updated_at", "broker_order_id", "correlation_id", "expired_reason",
        "armed_at", "strategy", "instrument", "signal_time", "entry_price",
    ],
    "fills": [
        "fill_id", "trade_id", "order_id", "broker_fill_id", "position_id",
        "strategy_id", "instrument", "side", "quantity", "price", "timestamp",
        "fill_type", "execution_mode", "broker_order_id", "broker_trade_id",
        "cumulative_filled_quantity", "fill_time", "commission",
    ],
    "positions": [
        "position_id", "trade_id", "strategy_id", "instrument", "side",
        "quantity", "average_entry_price", "average_exit_price", "status",
        "entry_time", "exit_time", "realized_pnl", "created_at", "updated_at",
        "execution_mode", "sl_state", "sl_order_id", "sl_trigger_price",
        "sl_protected_at", "avg_entry_price", "order_id", "liquidation_price",
    ],
    "trades": [
        "trade_id", "strategy_id", "instrument", "side", "entry_timestamp",
        "entry_price", "exit_timestamp", "exit_price", "quantity",
        "multiplier", "gross_pnl", "charges", "net_pnl", "exit_reason",
        "status", "created_at", "updated_at", "entry_signal_id",
        "exit_signal_id", "entry_order_id", "exit_order_id", "position_id",
        "exit_type", "realized_pnl", "entry_fill_id", "execution_mode",
        "exit_fill_id", "entry_time", "exit_time",
    ],
    "broker_order_mapping": [
        "broker_order_id", "order_id", "trade_id", "strategy_id",
        "instrument", "registered_at", "updated_at", "execution_mode",
        "mapping_id",
    ],
    "fill_reconciliation": [
        "broker_order_id", "execution_mode", "strategy_id", "instrument",
        "order_id", "side", "broker_cumulative_qty", "local_cumulative_qty",
        "gap_qty", "status", "last_broker_fill_id", "broker_average_price",
        "updated_at", "checked_at",
    ],
    "trade_events": [
        "event_id", "trade_id", "sequence_no", "event_type", "event_version",
        "idempotency_key", "payload_json", "strategy_id", "instrument",
        "timestamp", "created_at", "execution_mode", "order_id", "status",
    ],
    "execution_failure_events": [
        "event_id", "trade_id", "order_id", "broker_order_id", "signal_id",
        "instrument", "error", "action", "final_state", "details",
        "execution_mode", "created_at",
    ],
    "quarantine_records": [
        "record_id", "original_type", "original_id", "reason", "payload",
        "detected_at", "resolution_status", "resolved_trade_id",
        "execution_mode", "strategy", "order_id", "trade_id",
    ],
    "signals": [
        "signal_id", "strategy_id", "instrument", "trigger_price",
        "stop_price", "quantity", "signal_timestamp",
    ],
    "processed_fills": ["fill_id", "execution_mode", "processed_at"],
}


def resolve_db(db_arg: Optional[str]) -> str:
    if db_arg:
        return str(Path(db_arg).resolve())
    cfg_path = Config.get("system.db_path", "trading.db")
    return str(Config.resolve_path(cfg_path))


def _table_cols(db: Database, table: str) -> list[str]:
    try:
        return [str(r["name"]) for r in db.query(f"PRAGMA table_info({table})")]
    except Exception:
        return []


def _avail(cols: list[str], table: str) -> list[str]:
    desired = COLS.get(table, [])
    return [c for c in desired if c in cols]


def _pick(row: Any, pick_list: list[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set(row.keys()) if hasattr(row, "keys") else set()
    for c in pick_list:
        if c in keys:
            try:
                v = row[c]
            except (KeyError, IndexError):
                v = None
            if v is not None:
                out[c] = v
    return out


def _fmt(v: Any) -> str:
    if v is None:
        return "∅"
    if isinstance(v, float) and v != v:
        return "NaN!"
    return str(v)


def _safe_query(db: Database, sql: str, params: tuple = ()) -> list[dict]:
    try:
        return db.query(sql, params)
    except sqlite3.OperationalError:
        return []


def _print_section(title: str, rows: list[dict[str, Any]]) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("  (none)")
        return
    for i, row in enumerate(rows, 1):
        for k, v in row.items():
            print(f"  {i}. {k}: {_fmt(v)}")
        print()


def _find_identity(db: Database, handle: str):
    hits: list[dict[str, Any]] = []
    table_cols = {t: _table_cols(db, t) for t in COLS}
    _order = [
        ("orders", "order_id"),
        ("orders", "broker_order_id"),
        ("orders", "correlation_id"),
        ("pending_orders", "pending_order_id"),
        ("pending_orders", "broker_order_id"),
        ("pending_orders", "correlation_id"),
        ("broker_order_mapping", "broker_order_id"),
        ("broker_order_mapping", "order_id"),
        ("fills", "fill_id"),
        ("fills", "broker_fill_id"),
        ("fills", "broker_order_id"),
        ("fills", "order_id"),
        ("trades", "trade_id"),
        ("signals", "signal_id"),
        ("quarantine_records", "original_id"),
    ]
    for table, col in _order:
        if col not in table_cols.get(table, []):
            continue
        rows = _safe_query(db,
                           f"SELECT * FROM {table} WHERE {col} = ?",
                           (handle,))
        for r in rows:
            hits.append({"table": table, "column": col, "row": dict(r)})
    return hits, table_cols


def _broker_lookup_best_effort(broker_order_id: str) -> Optional[dict]:
    dhan = Config.get("dhan") or {}
    try:
        from data.dhan.rest_client import DhanRESTClient
        client = DhanRESTClient(
            base_url=dhan.get("rest_base", "https://api.dhan.co/v2"),
            token_file=dhan.get("token_file", "data/db/dhan_token.json"),
            client_id=str(dhan.get("client_id", "")),
            pin=str(dhan.get("pin", "")),
            totp_secret=str(dhan.get("totp_secret", "")),
        )
        status = client._get(f"/orders/{broker_order_id}")
        trades = client._get(f"/trades/{broker_order_id}")
        return {"status": status, "trades": trades}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


def _trace(handle: str, db_path: str, wants_broker: bool) -> int:
    db = Database(db_path)
    db.init_schema()
    hits, real_cols = _find_identity(db, handle)
    if not hits:
        print(f"[trace] no identity matched '{handle}' in {db_path}")
        db.close()
        return 1

    order_ids: set[str] = set()
    trade_ids: set[str] = set()
    fill_ids: set[str] = set()
    for h in hits:
        r = h["row"]
        if h["table"] == "orders":
            order_ids.add(r.get("order_id", ""))
            if r.get("trade_id"):
                trade_ids.add(r["trade_id"])
        if h["table"] == "trades":
            if r.get("entry_order_id"):
                order_ids.add(r["entry_order_id"])
            if r.get("exit_order_id"):
                order_ids.add(r["exit_order_id"])
        if h["table"] == "pending_orders":
            if r.get("order_id"):
                order_ids.add(r["order_id"])
            if r.get("trade_id"):
                trade_ids.add(r["trade_id"])
        if r.get("trade_id"):
            trade_ids.add(r["trade_id"])
        if h["table"] in ("broker_order_mapping", "fills"):
            if r.get("order_id"):
                order_ids.add(r["order_id"])
        if h["table"] == "fills":
            fill_ids.add(r.get("fill_id", ""))
    order_id = next(iter(order_ids - {""}), None)
    trade_id = next(iter(trade_ids - {""}), None)

    print(f"[trace] database : {db_path}")
    print(f"[trace] identity : {handle}")
    if order_id:
        print(f"[trace] order_id : {order_id}")
    if trade_id:
        print(f"[trace] trade_id : {trade_id}")

    _print_section(
        "I.  IDENTITY MATCHES",
        [{"table": h["table"], "column": h["column"]}
         for h in hits])

    if order_id:
        rows = _safe_query(db,
                           "SELECT * FROM orders WHERE order_id = ?",
                           (order_id,))
        _print_section("II.  ORDER ROW",
                       [_pick(r, _avail(real_cols["orders"], "orders"))
                        for r in rows])
    else:
        rows = _safe_query(db,
                           "SELECT * FROM pending_orders WHERE pending_order_id = ?",
                           (handle,))
        _print_section("II.  PENDING ROW",
                       [_pick(r, _avail(real_cols["pending_orders"],
                                        "pending_orders"))
                        for r in rows])

    _print_section(
        "III.  PENDING LIFECYCLE",
        [_pick(r, _avail(real_cols["pending_orders"], "pending_orders"))
         for r in _safe_query(
             db,
             "SELECT * FROM pending_orders "
             "WHERE pending_order_id = ? OR broker_order_id = ? "
             "OR correlation_id = ? ORDER BY created_at",
             (order_id or "", handle, handle))])

    _print_section(
        "IV.  BROKER ORDER MAPPING",
        [_pick(r, _avail(real_cols["broker_order_mapping"],
                         "broker_order_mapping"))
         for r in _safe_query(
             db,
             "SELECT * FROM broker_order_mapping "
             "WHERE order_id = ? OR broker_order_id = ? "
             "ORDER BY registered_at",
             (order_id or "", handle))])

    _print_section(
        "V.  FILLS",
        [_pick(r, _avail(real_cols["fills"], "fills"))
         for r in _safe_query(
             db,
             "SELECT * FROM fills WHERE order_id = ? OR trade_id = ? "
             "OR broker_order_id = ? ORDER BY timestamp",
             (order_id or "", trade_id or "", handle))])

    _print_section(
        "VI.  PROCESSED FILLS",
        [dict(r) for r in _safe_query(
            db,
            "SELECT * FROM processed_fills WHERE fill_id IN "
            "(SELECT fill_id FROM fills WHERE order_id = ?)",
            (order_id or "",))])

    _print_section(
        "VII.  POSITION",
        [_pick(r, _avail(real_cols["positions"], "positions"))
         for r in _safe_query(
             db,
             "SELECT * FROM positions WHERE trade_id = ? "
             "ORDER BY created_at",
             (trade_id or "",))])

    _print_section(
        "VIII.  TRADE / P&L",
        [_pick(r, _avail(real_cols["trades"], "trades"))
         for r in _safe_query(
             db,
             "SELECT * FROM trades WHERE trade_id = ?",
             (trade_id or "",))])

    _print_section(
        "IX.  FILL RECONCILIATION",
        [_pick(r, _avail(real_cols["fill_reconciliation"],
                         "fill_reconciliation"))
         for r in _safe_query(
             db,
             "SELECT * FROM fill_reconciliation "
             "WHERE broker_order_id = ? OR order_id = ? "
             "ORDER BY updated_at",
             (handle, order_id or ""))])

    _print_section(
        "X.  LIFECYCLE EVENTS",
        [_pick(r, _avail(real_cols["trade_events"], "trade_events"))
         for r in _safe_query(
             db,
             "SELECT * FROM trade_events WHERE trade_id = ? "
             "ORDER BY sequence_no",
             (trade_id or "",))])

    _print_section(
        "XI.  EXECUTION FAILURES",
        [dict(r) for r in _safe_query(
            db,
            "SELECT * FROM execution_failure_events WHERE trade_id = ? "
            "OR order_id = ? ORDER BY created_at",
            (trade_id or "", order_id or ""))])

    _print_section(
        "XII.  QUARANTINE / REJECTED EVENTS",
        [dict(r) for r in _safe_query(
            db,
            "SELECT * FROM quarantine_records WHERE original_id = ? "
            "OR resolved_trade_id = ? ORDER BY detected_at",
            (handle, trade_id or ""))])

    if wants_broker:
        oid = handle
        if order_id:
            bro = _safe_query(
                db,
                "SELECT broker_order_id FROM orders WHERE order_id = ?",
                (order_id,))
            oid = bro[0]["broker_order_id"] if bro else oid
        result = _broker_lookup_best_effort(str(oid))
        _print_section("XIII.  BROKER (LIVE READ-ONLY)",
                       [{"source": k, "payload": json.dumps(v, default=str)}
                        for k, v in (result or {}).items()])

    anomalies: list[str] = []
    for order in _safe_query(
            db,
            "SELECT order_id, order_role, state FROM orders WHERE order_id = ?",
            (order_id or "",)):
        st = str(order.get("state") or "").upper()
        if st in ("REJECTED", "CANCELLED", "EXPIRED"):
            anomalies.append(
                f"ORDER {order['order_id']} state={st} "
                f"role={order.get('order_role')}")
        elif st not in (
                "FILLED", "PARTIALLY_FILLED", "SUBMITTED", "PENDING",
                "OPEN", "ACTIVE", "COMPLETED", "CLOSED", "NEW",
                "TRADED", "PART_TRADED"):
            anomalies.append(
                f"ORDER {order['order_id']} unrecognised state '{st}'")
    for r in _safe_query(
            db,
            "SELECT broker_order_id, order_id, status, gap_qty "
            "FROM fill_reconciliation WHERE broker_order_id = ? OR order_id = ?",
            (handle, order_id or "")):
        if str(r.get("status") or "").lower() in ("divergent", "mismatch"):
            anomalies.append(
                f"RECONCILIATION DIVERGENT {r['broker_order_id']} "
                f"gap={r.get('gap_qty')}")
        elif int(r.get("gap_qty") or 0) != 0:
            anomalies.append(
                f"RECONCILIATION GAP {r['broker_order_id']} "
                f"status={r.get('status')} gap={r.get('gap_qty')}")

    print("\n=== XIV.  VERDICT ===")
    if not anomalies:
        print("  CLEAN — no anomalies on this lineage.")
    else:
        for a in anomalies:
            print(f"  ! {a}")
    db.close()
    return 2 if anomalies else 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "order_id",
        help="order id / broker id / correlation id / trade id / signal id")
    parser.add_argument(
        "--db", help="path to trading.db (default: configured)")
    parser.add_argument(
        "--broker", action="store_true",
        help="also query Dhan (READ-ONLY) for broker status")
    args = parser.parse_args(argv)
    return _trace(args.order_id, resolve_db(args.db), args.broker)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(main())