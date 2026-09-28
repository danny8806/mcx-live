#!/usr/bin/env python3
"""FINAL LIVE EXECUTION — Sections 9-20: Order chain, SL, fill, P&L, DB lineage."""
import paramiko, time, json

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"

def ssh(cmd, timeout=30):
    t = paramiko.Transport((VPS, 22))
    t.connect(username=USER, password=PASS)
    ch = t.open_session()
    ch.settimeout(timeout)
    ch.exec_command(cmd)
    out = b""
    while not ch.exit_status_ready():
        if ch.recv_ready(): out += ch.recv(65536)
        time.sleep(0.1)
    while ch.recv_ready(): out += ch.recv(65536)
    ch.close(); t.close()
    return out.decode(errors="replace")

# ====================================================================
# DB LINEAGE FOR THE LIVE TRADE
# ====================================================================
print("=" * 80)
print("DB LINEAGE: LIVE TRADE")
print("=" * 80)

queries = [
    ("TRADE", "SELECT * FROM trades WHERE trade_id = '2bf9a9ee-2e10-43c2-b225-37fa7ee9ac5c'"),
    ("ORDER (entry)", "SELECT * FROM orders WHERE order_id = 'LIVE-557bbf3e-f389-4f9b-965c-063d8ce95881'"),
    ("FILL", "SELECT * FROM fills WHERE fill_id = 'LIVE-b844c652-910d-49f0-bbc3-6ae2df33d3ff'"),
    ("POSITION", "SELECT * FROM positions WHERE strategy_id = 'silver_01' ORDER BY created_at DESC LIMIT 3"),
    ("SIGNAL", "SELECT * FROM signals WHERE signal_id = '306c55c8-7eb1-4356-b918-eee497866703'"),
    ("ALL ORDERS for silver_01", "SELECT order_id, strategy_id, side, order_type, state, price, trigger_price, broker_order_id, filled_quantity, quantity, order_role, created_at FROM orders WHERE strategy_id = 'silver_01' ORDER BY created_at DESC LIMIT 10"),
    ("ALL FILLS for silver_01", "SELECT fill_id, order_id, broker_order_id, side, quantity, price, strategy_id, timestamp FROM fills WHERE strategy_id = 'silver_01' ORDER BY timestamp DESC LIMIT 5"),
    ("TRADE EVENTS for trade", "SELECT * FROM trade_events WHERE trade_id = '2bf9a9ee-2e10-43c2-b225-37fa7ee9ac5c' ORDER BY created_at LIMIT 20"),
    ("BROKER ORDER MAPPING", "SELECT * FROM broker_order_mapping ORDER BY created_at DESC LIMIT 10"),
    ("PENDING ORDERS", "SELECT * FROM pending_orders ORDER BY created_at DESC LIMIT 10"),
    ("FILL RECONCILIATION", "SELECT * FROM fill_reconciliation ORDER BY created_at DESC LIMIT 10"),
    ("PROCESSED FILLS", "SELECT * FROM processed_fills ORDER BY created_at DESC LIMIT 10"),
]
for label, q in queries:
    r = ssh(f"docker exec mcx-live python3 -c \"import sqlite3,json; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('{q}'); cols=[d[0] for d in c.description]; [print(json.dumps(dict(zip(cols,row)),default=str)) for row in c.fetchall()]\" 2>/dev/null")
    print(f"\n--- {label} ---")
    if r.strip():
        print(r.strip()[:800])
    else:
        print("  (empty)")

# ====================================================================
# SL STATUS CHECK
# ====================================================================
print("\n" + "=" * 80)
print("SL PROTECTION CHECK")
print("=" * 80)

sl_queries = [
    ("SL orders", "SELECT order_id, strategy_id, side, order_type, state, price, trigger_price, broker_order_id, filled_quantity, quantity, order_role FROM orders WHERE order_role = 'STOP_LOSS' ORDER BY created_at DESC LIMIT 5"),
    ("STOP_LOSS orders (all)", "SELECT order_id, strategy_id, side, order_type, state, price, trigger_price, broker_order_id, filled_quantity, quantity, order_role FROM orders WHERE order_type = 'STOP_LOSS' ORDER BY created_at DESC LIMIT 10"),
    ("Position stop_price", "SELECT position_id, strategy_id, instrument, side, quantity, average_entry, stop_price, status FROM positions WHERE strategy_id = 'silver_01' ORDER BY created_at DESC LIMIT 3"),
]
for label, q in sl_queries:
    r = ssh(f"docker exec mcx-live python3 -c \"import sqlite3,json; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('{q}'); cols=[d[0] for d in c.description]; [print(json.dumps(dict(zip(cols,row)),default=str)) for row in c.fetchall()]\" 2>/dev/null")
    print(f"\n--- {label} ---")
    if r.strip():
        print(r.strip()[:800])
    else:
        print("  (empty)")

# ====================================================================
# CONTAINER LOGS — FULL TRADE LIFECYCLE
# ====================================================================
print("\n" + "=" * 80)
print("CONTAINER LOGS: FULL TRADE LIFECYCLE")
print("=" * 80)

r = ssh("docker logs mcx-live 2>&1 | grep -iE 'silver_01|SILVERM|2bf9a9ee|306c55c8|557bbf3e|b844c652|f0551468|SHORT|ENTRY|EXIT|SL|FILL|CANCEL|MARKET|REVERSAL|signal_created|order_created|position_opened|TRADE'" )
print(r.strip()[:2000] if r.strip() else "  No matching logs")

# ====================================================================
# DB ALL ORDERS (full chain)
# ====================================================================
print("\n" + "=" * 80)
print("ALL ORDERS (full history)")
print("=" * 80)

r = ssh("docker exec mcx-live python3 -c \"import sqlite3,json; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('SELECT order_id, strategy_id, side, order_type, state, price, trigger_price, broker_order_id, filled_quantity, quantity, order_role, created_at FROM orders ORDER BY created_at DESC LIMIT 25'); cols=[d[0] for d in c.description]; [print(json.dumps(dict(zip(cols,row)),default=str)) for row in c.fetchall()]\" 2>/dev/null")
print(r.strip()[:2000] if r.strip() else "  (empty)")

# ====================================================================
# DB ALL TRADES
# ====================================================================
print("\n" + "=" * 80)
print("ALL TRADES")
print("=" * 80)

r = ssh("docker exec mcx-live python3 -c \"import sqlite3,json; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('SELECT trade_id, strategy_id, instrument, side, entry_price, exit_price, net_pnl, status, exit_reason, stop_price, signal_id, created_at FROM trades ORDER BY created_at DESC LIMIT 15'); cols=[d[0] for d in c.description]; [print(json.dumps(dict(zip(cols,row)),default=str)) for row in c.fetchall()]\" 2>/dev/null")
print(r.strip()[:1500] if r.strip() else "  (empty)")

# ====================================================================
# DB ALL FILLS
# ====================================================================
print("\n" + "=" * 80)
print("ALL FILLS")
print("=" * 80)

r = ssh("docker exec mcx-live python3 -c \"import sqlite3,json; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('SELECT fill_id, order_id, broker_order_id, instrument, side, quantity, price, strategy_id, timestamp FROM fills ORDER BY timestamp DESC LIMIT 10'); cols=[d[0] for d in c.description]; [print(json.dumps(dict(zip(cols,row)),default=str)) for row in c.fetchall()]\" 2>/dev/null")
print(r.strip()[:1000] if r.strip() else "  (empty)")

# ====================================================================
# P&L
# ====================================================================
print("\n" + "=" * 80)
print("P&L")
print("=" * 80)

import urllib.request
def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

pnl = api("/api/pnl")
print(f"  execution_mode: {pnl.get('execution_mode', '?')}")
print(f"  portfolio: {json.dumps(pnl.get('portfolio', {}))[:200]}")
print(f"  by_instrument: {json.dumps(pnl.get('by_instrument', {}))[:300]}")

# ====================================================================
# RECONCILIATION DETAIL
# ====================================================================
print("\n" + "=" * 80)
print("RECONCILIATION DETAIL")
print("=" * 80)

recon = api("/api/reconciliation")
print(f"  is_consistent: {recon.get('is_consistent', '?')}")
print(f"  summary: {json.dumps(recon.get('summary', {}))}")
if "checks" in recon:
    for c in recon["checks"]:
        name = c.get("name", "?")
        consistent = c.get("is_consistent", "?")
        errs = c.get("errors", [])
        warns = c.get("warnings", [])
        print(f"\n  {name}: consistent={consistent}")
        for e in errs[:3]:
            print(f"    ERROR: {str(e)[:200]}")
        for w in warns[:3]:
            print(f"    WARN: {str(w)[:200]}")

# ====================================================================
# TELEGRAM ALERTS DETAIL
# ====================================================================
print("\n" + "=" * 80)
print("TELEGRAM ALERTS DETAIL")
print("=" * 80)

alerts = api("/api/alerts")
print(f"  count: {alerts.get('count', '?')}")
for a in alerts.get("alerts", []):
    print(f"  type={a.get('type','?')} severity={a.get('severity','?')}")
    print(f"    data: {json.dumps(a.get('data',{}), default=str)[:300]}")
