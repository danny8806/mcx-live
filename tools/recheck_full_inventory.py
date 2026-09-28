"""In-container LIVE full-stack inventory probe (run via docker exec)."""
import json
import sqlite3
import urllib.request

db = "/app/live/data/db/live_trading.db"
con = sqlite3.connect(db)
tables = [r[0] for r in con.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
print("DB-TABLES:", json.dumps(tables))

for t in tables:
    try:
        c = con.execute(f"SELECT COUNT(*) FROM '{t}'").fetchone()[0]
        cols = [r[1] for r in con.execute(f"PRAGMA table_info('{t}')")]
        print(f"TABLE {t}: rows={c} cols={len(cols)}")
    except Exception as e:
        print(f"TABLE {t}: ERROR {e}")
con.close()


def http(path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{path}", timeout=6) as r:
            return r.status, r.read(800).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read(400).decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)


def print_route(path):
    s, b = http(path)
    print(f"GET {path} -> {s}")

for p in [
    "/health", "/api/health", "/openapi.json",
    "/api/live/health", "/api/live/account", "/api/live/profile",
    "/api/live/funds", "/api/live/margin",
    "/api/live/orders", "/api/live/trades", "/api/live/positions",
    "/api/live/strategies", "/api/live/reconciliation", "/api/live/broker-sync",
    "/api/live/events", "/api/reversals", "/api/live/reversals",
]:
    print_route(p)

s, b = http("/openapi.json")
if s == 200:
    try:
        paths = json.loads(b[:1_000_000] if False else b)["paths"]
        print("OPENAPI-PATHS:", json.dumps(sorted(paths.keys())))
    except Exception as e:
        print("OPENAPI-PARSE-ERROR:", e, b[:200])
s, b = http("/api/health")
print("API-HEALTH-BODY:", b[:200])