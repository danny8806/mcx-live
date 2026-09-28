import sqlite3, json

db = sqlite3.connect("/app/live/data/db/live_trading.db")
db.row_factory = sqlite3.Row

print("=== TABLES ===")
for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
    print(r[0])

print()
print("=== EVENTS SCHEMA ===")
for r in db.execute("SELECT sql FROM sqlite_master WHERE name='events'"):
    print(r[0])

print()
print("=== ALL ORDER-LIKE TABLES ROW COUNTS ===")
for t in ["orders", "ordered", "order_status_events", "trades", "signals"]:
    try:
        n = db.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(t, "->", n)
    except Exception as e:
        print(t, "ERR", e)