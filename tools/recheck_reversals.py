"""In-container LIVE verification of the reversals feature (run via docker exec)."""
import json
import sqlite3
import urllib.request

# ── 1) reversals table ──
db = "/app/live/data/db/live_trading.db"
con = sqlite3.connect(db)
tables = [r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
print("reversals-table:", "reversals" in tables)

cols = [r[1] for r in con.execute("PRAGMA table_info(reversals)")]
print("reversals-cols:%d" % len(cols))
print("reversals-col-names:", ",".join(cols[:6]), "...")

idx = [r[1] for r in con.execute("PRAGMA index_list(reversals)")]
print("reversals-indexes:", idx)

count = con.execute("SELECT COUNT(*) FROM reversals").fetchone()[0]
print("reversals-count:", count)
con.close()

# ── 2) HTTP routing through the live gateway ──
def http(path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{path}", timeout=5) as r:
            body = r.read(600).decode("utf-8", errors="replace")
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read(400).decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

s, b = http("/api/reversals")
print(f"GET /api/reversals -> {s} : {b[:300]}")

s2, b2 = http("/api/live/reversals")
print(f"GET /api/live/reversals -> {s2} : {b2[:300]}")

s3, b3 = http("/api/reversals/nonexistent-xyz")
print(f"GET /api/reversals/nonexistent-xyz -> {s3} : {b3[:300]}")