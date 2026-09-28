#!/usr/bin/env python3
"""SL failure deep investigation + live state."""
import paramiko, time, json, urllib.request

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

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

# ====================================================================
# SL FAILURE DETAIL
# ====================================================================
print("=" * 80)
print("SL FAILURE DETAIL")
print("=" * 80)

# Full SL-related logs
r = ssh("docker logs mcx-live 2>&1 | grep -iE 'sl|stop.loss|protect|SL_|SL |sl_protection|blocked_retry|BUY STOP_LOSS|price.*trigger|trigger.*price'")
print("SL logs:\n" + r.strip()[:1500])

# All STOP_LOSS orders in API
print("\n--- ALL STOP_LOSS ORDERS ---")
orders = api("/api/orders")
for o in orders.get("orders", []):
    if o.get("order_type") == "STOP_LOSS":
        oid = o.get("order_id", "?")[:20]
        state = o.get("state", "?")
        side = o.get("side", "?")
        price = o.get("price", "?")
        trigger = o.get("trigger_price", "?")
        broker = o.get("broker_order_id", "?")
        filled = o.get("filled_quantity", "?")
        role = o.get("order_role", "?")
        print(f"  {oid}... | {side} | state={state} | price={price} trigger={trigger} | filled={filled} | broker={broker} | role={role}")

# ====================================================================
# POSITION DETAIL
# ====================================================================
print("\n" + "=" * 80)
print("POSITION DETAIL")
print("=" * 80)

positions = api("/api/positions")
for p in positions.get("positions", []):
    print(f"  position_id: {p.get('position_id', '?')}")
    print(f"  strategy_id: {p.get('strategy_id', '?')}")
    print(f"  instrument: {p.get('instrument', '?')}")
    print(f"  side: {p.get('side', '?')}")
    print(f"  quantity: {p.get('quantity', '?')}")
    print(f"  average_entry: {p.get('average_entry', '?')}")
    print(f"  stop_price: {p.get('stop_price', '?')}")
    print(f"  entry_fill_ids: {p.get('entry_fill_ids', '?')}")
    print(f"  status: {p.get('status', '?')}")
    print(f"  current_sl_order_id: {p.get('current_sl_order_id', '?')}")
    for k in p:
        if k not in ['position_id','strategy_id','instrument','side','quantity','average_entry','stop_price','entry_fill_ids','status','current_sl_order_id']:
            print(f"  {k}: {p[k]}")

# ====================================================================
# TRADE DETAIL
# ====================================================================
print("\n" + "=" * 80)
print("TRADE DETAIL")
print("=" * 80)

trades = api("/api/trades")
for t in trades.get("trades", []):
    if t.get("status") != "PENDING" or t.get("entry_price", 0) > 0:
        print(f"  trade_id: {t.get('trade_id', '?')}")
        for k, v in t.items():
            print(f"    {k}: {v}")
        print()

# ====================================================================
# FILL DETAIL
# ====================================================================
print("\n" + "=" * 80)
print("FILL DETAIL")
print("=" * 80)

fills = api("/api/fills")
for f_item in fills.get("fills", []):
    print(f"  fill_id: {f_item.get('fill_id', '?')}")
    for k, v in f_item.items():
        print(f"    {k}: {v}")
    print()

# ====================================================================
# CONTAINER LOGS — COMPLETE SL ATTEMPT
# ====================================================================
print("\n" + "=" * 80)
print("CONTAINER LOGS — SL ATTEMPT (full)")
print("=" * 80)

r = ssh("docker logs mcx-live 2>&1 | grep -A2 -B2 -iE 'sl_protection|protective|SL|stop.loss|price.*trigger|trigger.*price|blocked|retry'")
print(r.strip()[:2000] if r.strip() else "  No SL logs found")

# ====================================================================
# CONTAINER LOGS — COMPLETE ENTRY SEQUENCE
# ====================================================================
print("\n" + "=" * 80)
print("CONTAINER LOGS — ENTRY SEQUENCE")
print("=" * 80)

r = ssh("docker logs mcx-live 2>&1 | grep -iE 'signal|entry|order|fill|position|SL|cancel|market|fill' | grep -v 'CandleFetcher\|dhan_adapter.*TICK\|dhan_ws.*DEDUP\|GET /api' | tail -40")
print(r.strip()[:1500] if r.strip() else "  No entry logs")

# ====================================================================
# MARKET DATA CHECK
# ====================================================================
print("\n" + "=" * 80)
print("MARKET DATA (current)")
print("=" * 80)

mkt = api("/api/market-data")
for inst, data in mkt.get("instruments", {}).items():
    print(f"  {inst}: LTP={data.get('ltp')} ticks={data.get('tick_count')}")

# ====================================================================
# RISK + P&L
# ====================================================================
print("\n" + "=" * 80)
print("RISK + P&L")
print("=" * 80)

risk = api("/api/risk")
print(f"  equity: {risk.get('equity')}")
print(f"  available_margin: {risk.get('available_margin')}")
print(f"  used_margin: {risk.get('used_margin')}")
print(f"  open_positions: {risk.get('open_positions')}")
print(f"  daily_pnl: {risk.get('daily_pnl')}")
print(f"  unrealized_pnl (from P&L): {api('/api/pnl').get('portfolio',{}).get('unrealized_pnl')}")

# ====================================================================
# STRATEGY STATE
# ====================================================================
print("\n" + "=" * 80)
print("STRATEGY STATE (silver_01)")
print("=" * 80)

strats = api("/api/strategies")
for s in strats.get("strategies", []):
    if s.get("strategy_id") == "silver_01":
        for k, v in s.items():
            print(f"  {k}: {v}")

# ====================================================================
# DB CHECK — separate live DB
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB (separate path)")
print("=" * 80)

r = ssh("docker exec mcx-live ls -la /app/live/data/db/ 2>/dev/null || echo 'dir not found'")
print(r.strip())

r = ssh("docker exec mcx-live python3 -c \"import sqlite3; c=sqlite3.connect('/app/live/data/db/live_trading.db').cursor(); c.execute('SELECT name FROM sqlite_master WHERE type=\\\"table\\\"'); print('\\n'.join([r[0] for r in c.fetchall()]))\" 2>/dev/null || echo 'live db not found'")
print(f"  Live DB tables: {r.strip()[:300]}")

# ====================================================================
# CHECK MAIN DB vs API DISCREPANCY
# ====================================================================
print("\n" + "=" * 80)
print("DB vs API COMPARISON")
print("=" * 80)

# Main DB counts
r = ssh("docker exec mcx-live python3 -c \"import sqlite3; c=sqlite3.connect('/app/data/db/trading.db').cursor(); print(f'main_db: trades={c.execute(\\\"SELECT COUNT(*) FROM trades\\\").fetchone()[0]} orders={c.execute(\\\"SELECT COUNT(*) FROM orders\\\").fetchone()[0]} fills={c.execute(\\\"SELECT COUNT(*) FROM fills\\\").fetchone()[0]} positions={c.execute(\\\"SELECT COUNT(*) FROM positions\\\").fetchone()[0]}')\"")
print(f"  {r.strip()}")

# API counts
print(f"  api: trades={api('/api/trades').get('count','?')} orders={api('/api/orders').get('count','?')} fills={api('/api/fills').get('count','?')} positions={api('/api/positions').get('count','?')}")
