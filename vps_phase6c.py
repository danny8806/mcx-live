#!/usr/bin/env python3
"""Phase 6 — remaining verification after hash mismatch discovery."""
import paramiko, time, urllib.request, json
from vps_credentials import load_vps_password

VPS_HOST = "200.234.44.93"
VPS_USER = "root"
VPS_PASS = load_vps_password()
def ssh(cmd, timeout=20):
    t = paramiko.Transport((VPS_HOST, 22))
    t.connect(username=VPS_USER, password=VPS_PASS)
    ch = t.open_session()
    ch.settimeout(timeout)
    ch.exec_command(cmd)
    out = b""
    while not ch.exit_status_ready():
        if ch.recv_ready(): out += ch.recv(65536)
        time.sleep(0.1)
    while ch.recv_ready(): out += ch.recv(65536)
    ch.close()
    t.close()
    return out.decode(errors="replace")

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

# 1. All API endpoints
print("=== ALL API ENDPOINTS ===")
endpoints = [
    "/health", "/api/positions", "/api/orders", "/api/strategies",
    "/api/risk", "/api/market-data", "/api/trades", "/api/pnl",
    "/api/reconciliation", "/api/overview", "/api/fills", "/api/events",
    "/api/indicators", "/api/pending-orders", "/api/settings",
    "/api/alerts", "/api/reversals", "/api/account",
]
for ep in endpoints:
    r = api(ep)
    status = "OK" if "error" not in r else f"404"
    keys = list(r.keys())[:4] if isinstance(r, dict) else []
    print(f"  {ep}: {status} keys={keys}")

# 2. Strategy details
print("\n=== STRATEGY DETAILS ===")
strat = api("/api/strategies")
if "strategies" in strat:
    for s in strat["strategies"]:
        sid = s.get("strategy_id", "?")
        inst = s.get("instrument", "?")
        state = s.get("state", "?")
        enabled = s.get("enabled", "?")
        pos = s.get("position_side", "?")
        bars = s.get("bars_processed", 0)
        tc = s.get("trade_count", 0)
        print(f"  {sid}: {inst} state={state} enabled={enabled} pos={pos} bars={bars} trades={tc}")

# 3. Order detail (all 15)
print("\n=== ALL ORDERS ===")
ords = api("/api/orders")
if "orders" in ords:
    for o in ords["orders"]:
        oid = o.get("order_id", "?")[:24]
        strat_id = o.get("strategy_id", "?")
        side = o.get("side", "?")
        state = o.get("state", "?")
        otype = o.get("order_type", "?")
        price = o.get("price", "?")
        broker = o.get("broker_order_id", "?")
        role = o.get("order_role", "?")
        print(f"  {oid} | {strat_id} | {side} {otype} | {state} | role={role} | broker={broker}")

# 4. Trade detail (all 15)
print("\n=== ALL TRADES ===")
trades = api("/api/trades")
if "trades" in trades:
    for t in trades["trades"][:10]:
        tid = t.get("trade_id", "?")[:24]
        strat = t.get("strategy_id", "?")
        side = t.get("side", "?")
        entry = t.get("entry_price", "?")
        exit_p = t.get("exit_price", "?")
        pnl = t.get("net_pnl", "?")
        status = t.get("status", "?")
        reason = t.get("exit_reason", "?")
        print(f"  {tid} | {strat} {side} | entry={entry} exit={exit_p} | pnl={pnl} | {status} | {reason}")

# 5. Market data
print("\n=== MARKET DATA ===")
mkt = api("/api/market-data")
if "instruments" in mkt:
    for inst, data in mkt["instruments"].items():
        print(f"  {inst}: LTP={data.get('ltp')} tick_count={data.get('tick_count')}")
ws = mkt.get("ws_connected", "?")
print(f"  WS connected: {ws}")

# 6. Reconciliation
print("\n=== RECONCILIATION ===")
recon = api("/api/reconciliation")
if "checks" in recon:
    for c in recon["checks"]:
        name = c.get("name", "?")
        consistent = c.get("is_consistent", "?")
        errs = c.get("errors", [])
        print(f"  {name}: consistent={consistent}")
        for e in errs[:2]:
            print(f"    - {str(e)[:150]}")

# 7. Risk detail
print("\n=== RISK DETAIL ===")
risk = api("/api/risk")
for k, v in risk.items():
    if k != "strategies":
        print(f"  {k}: {v}")
if "strategies" in risk:
    for sid, rv in risk["strategies"].items():
        print(f"  strategy {sid}: {rv}")

# 8. Overview
print("\n=== OVERVIEW ===")
ov = api("/api/overview")
for k, v in ov.items():
    print(f"  {k}: {v}")

# 9. Settings
print("\n=== SETTINGS ===")
settings = api("/api/settings")
if "system" in settings:
    for k, v in settings["system"].items():
        print(f"  {k}: {v}")

# 10. Container errors (wider search)
print("\n=== CONTAINER ERRORS (wider) ===")
errs = ssh("docker logs mcx-live 2>&1 | grep -iE 'error|exception|traceback|fail|reject|auth' | tail -15")
print(errs.strip()[:600] if errs.strip() else "  No errors found")

# 11. Dhan token age
print("\n=== DHAN TOKEN ===")
token_info = ssh("docker exec mcx-live python -c \"import json,time; d=json.load(open('/app/data/db/dhan_token.json')); exp=d.get('expires_at',0); age=time.time()-d.get('created_at',0); print(f'age_s={age:.0f} expires_at={exp} now={time.time()} remaining={exp-time.time():.0f}s')\" 2>/dev/null || echo token_check_failed")
print(f"  {token_info.strip()}")

# 12. Live DB trade count (from within container)
print("\n=== LIVE DB STATUS ===")
db_status = ssh("docker exec mcx-live python -c \"\nimport sqlite3\nconn = sqlite3.connect('/app/data/db/trading.db')\nc = conn.cursor()\nfor t in ['signals','trades','orders','fills','positions','pending_orders','reversals']:\n    try:\n        c.execute(f'SELECT COUNT(*) FROM {t}')\n        print(f'{t}: {c.fetchone()[0]}')\n    except: print(f'{t}: N/A')\n\"")
print(db_status.strip())
