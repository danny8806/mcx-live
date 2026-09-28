"""Audit log export / import for the canonical trading.db.

The audit log lives in the database (``events``, ``trade_events`` and every
other canonical table).  This tool gives you the option to:

  * export  — save the ENTIRE audit log to CSV (one file per table) + manifest
  * import  — restore a saved audit log from CSV back into a database
              (idempotent upsert by primary key, never duplicates rows)
  * stats   — show what the audit log currently contains

Usage:
    python tools/audit_log.py export [--db PATH] [--out DIR]
    python tools/audit_log.py import [--db PATH] [--dir DIR | --csv FILE]
    python tools/audit_log.py stats  [--db PATH]

Notes:
  * NULL values use the PostgreSQL-style ``\\N`` sentinel inside the CSVs.
  * Import is idempotent: a row whose primary key already exists is updated
    (never duplicated); re-running an import is always safe.
  * Import runs one transaction per table (all-or-nothing per table) and
    reports per-table insert/update/failure counts.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import Config
from persistence.database import Database

NULL = "\\N"

# Export order (parents before children so imports honour foreign keys) and
# the PRIMARY KEY used for idempotent upsert.
TABLES: list[tuple[str, tuple[str, ...]]] = [
    ("system_metadata", ("key",)),
    ("events", ("id",)),
    ("account_snapshots", ("id",)),
    ("signals", ("signal_id",)),
    ("trades", ("trade_id",)),
    ("trade_signal_link", ("trade_id", "signal_id", "relationship_type")),
    ("pending_orders", ("pending_order_id",)),
    ("orders", ("order_id",)),
    ("fills", ("fill_id",)),
    ("positions", ("position_id",)),
    ("trade_events", ("event_id",)),
    ("processed_fills", ("fill_id",)),
    ("quarantine_records", ("record_id",)),
]


def resolve_db(db_arg: Optional[str]) -> str:
    if db_arg:
        return str(Path(db_arg).resolve())
    cfg_path = Config.get("system.db_path", "trading.db")
    return str(Config.resolve_path(cfg_path))


def _table_columns(db: Database, table: str) -> list[str]:
    return [r["name"] for r in db.query(f"PRAGMA table_info({table})")]


def _dump_row(values: list) -> list[str]:
    return [NULL if v is None else "" if isinstance(v, float) and str(v) == "nan" else str(v)
            for v in values]


def cmd_export(args) -> int:
    db_path = resolve_db(args.db)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    db = Database(db_path)
    manifest: dict = {
        "tool": "audit_log",
        "version": 1,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "source_db": db_path,
        "tables": {},
    }
    print(f"[audit] exporting {db_path} -> {out}")
    for table, _pk in TABLES:
        cols = _table_columns(db, table)
        if not cols:
            print(f"[audit]    SKIP {table} (table missing)")
            continue
        rows = db.query(f"SELECT * FROM {table} ORDER BY 1")
        dest = out / f"{table}.csv"
        with open(dest, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cols)
            for row in rows:
                writer.writerow(_dump_row([row[c] for c in cols]))
        manifest["tables"][table] = {
            "columns": cols,
            "rows": len(rows),
            "pk": list(_pk),
        }
        print(f"[audit]    {table}: {len(rows)} rows -> {dest.name}")
    with open(out / "manifest.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, default=str)
    db.close()
    print(f"[audit] done. {len(manifest['tables'])} tables saved under {out}")
    return 0


def _import_file(db: Database, table: str, path: Path) -> None:
    """Upsert one CSV file into `table`. Atomic per file (one transaction)."""
    pk = dict(TABLES)[table]
    target_cols = _table_columns(db, table)
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
    cols = [c for c in header if c in target_cols]
    missing = [c for c in header if c not in target_cols]
    if missing:
        print(f"[audit]    {table}: ignoring unknown columns {missing}")
    if not cols:
        print(f"[audit]    {table}: no usable columns — skipping")
        return
    if any(c not in cols for c in pk):
        print(f"[audit]    {table}: primary key {pk} not present — skipping")
        return
    placeholders = ", ".join("?" for _ in cols)
    conflict = ", ".join(pk)
    updates = ", ".join(f"{c}=excluded.{c}" for c in cols if c not in pk)
    sql = (f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({placeholders}) "
           f"ON CONFLICT({conflict}) DO UPDATE SET {updates}")
    processed = failed = 0
    with db.transaction() as conn:
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                values = [None if v == NULL else v for v in
                          (row.get(c, "") for c in cols)]
                try:
                    conn.execute(sql, values)
                    processed += 1
                except Exception as e:
                    failed += 1
                    if failed <= 5:
                        print(f"[audit]    {table}: row {row.get(list(pk)[0], '?')} "
                              f"failed: {e}")
    print(f"[audit]    {table}: {processed} rows upserted, {failed} failed")
    if failed:
        print(f"[audit]    WARNING {table}: {failed} rows not imported")


def cmd_import(args) -> int:
    db_path = resolve_db(args.db)
    db = Database(db_path)
    sources: list[tuple[str, Path]] = []
    if args.csv:
        p = Path(args.csv).resolve()
        table = p.stem
        if table not in dict(TABLES):
            print(f"[audit] {p.name}: unknown table '{table}' "
                  f"(expected one of {', '.join(dict(TABLES))})")
            db.close()
            return 2
        sources.append((table, p))
    else:
        d = Path(args.dir).resolve()
        for table, _pk in TABLES:
            p = d / f"{table}.csv"
            if p.exists():
                sources.append((table, p))
    if not sources:
        print(f"[audit] no CSV files found in {args.dir or args.csv}")
        db.close()
        return 2
    print(f"[audit] importing {len(sources)} table(s) -> {db_path}")
    for table, path in sources:
        _import_file(db, table, path)
    db.close()
    print("[audit] done.")
    return 0


def cmd_stats(args) -> int:
    db_path = resolve_db(args.db)
    db = Database(db_path)
    size = Path(db_path).stat().st_size if Path(db_path).exists() else 0
    print(f"[audit] database: {db_path} ({size:,} bytes)")
    for table, _pk in TABLES:
        if not db.table_exists(table):
            print(f"[audit]    {table}: MISSING")
            continue
        n = db.scalar(f"SELECT COUNT(*) FROM {table}")
        print(f"[audit]    {table}: {n}")
    db.close()
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("export", help="save the audit log to CSV files")
    p.add_argument("--db", help="path to trading.db (default: configured)")
    p.add_argument("--out", default="audit_export",
                   help="output directory (default: audit_export)")
    p.set_defaults(func=cmd_export)

    p = sub.add_parser("import", help="restore an audit log from CSV")
    p.add_argument("--db", help="path to trading.db (default: configured)")
    p.add_argument("--dir", help="directory containing <table>.csv files")
    p.add_argument("--csv", help="single <table>.csv file to import")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("stats", help="show audit log contents")
    p.add_argument("--db", help="path to trading.db (default: configured)")
    p.set_defaults(func=cmd_stats)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    raise SystemExit(main())