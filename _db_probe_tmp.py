import json, os, sqlite3

paths = [
    "/app/live/data/db/live_trading.db",
    "/app/live/data/db",
    "/app/data/db/live_trading.db",
    "/app/data/db",
]
for p in paths:
    print(p, "exists=", os.path.exists(p),
          "dir=", os.path.isdir(p),
          "size=", os.path.getsize(p) if (os.path.exists(p) and os.path.isfile(p)) else None)
print("--- find live*.db ---")
for root, dirs, files in os.walk("/app"):
    dirs[:] = [d for d in dirs if d not in ("site-packages", "__pycache__", ".git", "node_modules")]
    if root.count(os.sep) > 4:
        continue
    for f in files:
        if f.endswith(".db") or f.endswith(".sqlite") or "token" in f:
            fp = os.path.join(root, f)
            print(fp, os.path.getsize(fp))

print("--- try live db ---")
con = sqlite3.connect("/app/live/data/db/live_trading.db")
tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
print("tables:", tables)
for t in ("signals", "orders", "fills", "positions", "trades", "candles", "pending_orders"):
    if t in tables:
        n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(t, n)
con.close()