#!/usr/bin/env python3
"""Phase 6 Sections 5-9: LIVE/PAPER isolation + Dhan connectivity."""
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
    ch.close()
    t.close()
    return out.decode(errors="replace")

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

print("=" * 80)
print("SECTION 5: LIVE/PAPER ISOLATION")
print("=" * 80)

isolation_cmds = [
    ("Paper references in config", "docker exec mcx-live grep -rni 'paper\\|demo\\|mock\\|simulator\\|sandbox\\|fake\\|test_broker\\|test broker' /app/config/ 2>/dev/null | grep -v '.pyc' | head -30"),
    ("Paper references in execution", "docker exec mcx-live grep -rni 'paper\\|demo\\|mock\\|simulator\\|sandbox\\|fake\\|test_broker' /app/execution/ 2>/dev/null | grep -v '.pyc' | grep -v '__pycache__' | head -30"),
    ("Paper references in live", "docker exec mcx-live grep -rni 'paper\\|demo\\|mock\\|simulator\\|sandbox\\|fake\\|test_broker' /app/live/ 2>/dev/null | grep -v '.pyc' | grep -v '__pycache__' | head -30"),
    ("Paper references in strategies", "docker exec mcx-live grep -rni 'paper\\|demo\\|mock\\|simulator\\|sandbox\\|fake\\|test_broker' /app/strategies/ 2>/dev/null | grep -v '.pyc' | grep -v '__pycache__' | head -30"),
    ("paper_broker.py exists?", "docker exec mcx-live ls /app/execution/paper_broker.py 2>/dev/null && echo 'EXISTS - classified' || echo 'NOT FOUND'"),
    ("paper_broker import check", "docker exec mcx-live grep -rn 'paper_broker\\|PaperBroker' /app/execution/live/ /app/live/ /app/trading_engine.py 2>/dev/null | grep -v '.pyc' | head -20"),
    ("TRADING_MODE env", "docker exec mcx-live env | grep TRADING_MODE"),
    ("Config environment", "docker exec mcx-live python3 -c \"import json; print(json.load(open('/app/config/live_settings.json'))['system']['environment'])\" 2>/dev/null"),
    ("Config live_enabled", "docker exec mcx-live python3 -c \"import json; print(json.load(open('/app/config/live_settings.json'))['system']['live_enabled'])\" 2>/dev/null"),
    ("Execution mode from API", "curl -s http://200.234.44.93:8001/api/positions 2>/dev/null | python3 -c \"import sys,json; print(json.load(sys.stdin).get('execution_mode','UNKNOWN'))\" 2>/dev/null || echo 'API check failed'"),
    ("Replay references", "docker exec mcx-live grep -rni 'replay' /app/live/ /app/execution/ 2>/dev/null | grep -v '.pyc' | grep -v '__pycache__' | head -10"),
]
for label, cmd in isolation_cmds:
    r = ssh(cmd)
    print(f"\n  {label}:")
    val = r.strip()[:400] if r.strip() else "(empty)"
    print(f"    {val}")

print("\n" + "=" * 80)
print("SECTION 6-8: DHAN CONNECTION + REST + MARKET WS")
print("=" * 80)

dhan_checks = [
    ("WS connected", "/api/market-data"),
    ("Positions", "/api/positions"),
    ("Orders", "/api/orders"),
    ("Strategies", "/api/strategies"),
    ("Risk", "/api/risk"),
    ("Overview", "/api/overview"),
    ("P&L", "/api/pnl"),
    ("Trades", "/api/trades"),
    ("Fills", "/api/fills"),
    ("Reconciliation", "/api/reconciliation"),
    ("Settings", "/api/settings"),
]
for label, ep in dhan_checks:
    r = api(ep)
    print(f"\n--- {label} ({ep}) ---")
    if isinstance(r, dict):
        if "error" in r:
            print(f"  ERROR: {r['error'][:100]}")
        else:
            print(f"  OK. Keys: {list(r.keys())}")
            if "execution_mode" in r: print(f"  execution_mode: {r['execution_mode']}")
            if "count" in r: print(f"  count: {r['count']}")
            if "instruments" in r:
                for inst, d in r["instruments"].items():
                    print(f"  {inst}: LTP={d.get('ltp')} ticks={d.get('tick_count')} ws={r.get('ws_connected')}")
            if "equity" in r: print(f"  equity: {r['equity']}")
            if "total_equity" in r: print(f"  total_equity: {r['total_equity']}")
            if "kill_switch_active" in r: print(f"  kill_switch: {r['kill_switch_active']}")
    else:
        print(f"  {str(r)[:200]}")

print("\n--- Dhan REST from container ---")
rest_checks = [
    ("Fund limits", "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/fundlimit',headers=h),timeout=10); print(json.loads(r.read()))"),
    ("Positions", "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/positions',headers=h),timeout=10); print(json.loads(r.read()))"),
    ("Orders", "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/orders',headers=h),timeout=10); d=json.loads(r.read()); print(f'type={type(d).__name__} len={len(d) if isinstance(d,list) else \"N/A\"}')"),
]
for label, py in rest_checks:
    r = ssh(f"docker exec mcx-live python3 -c \"{py}\"")
    print(f"\n  {label}: {r.strip()[:300]}")

print("\n--- Auth status from container ---")
auth_logs = ssh("docker logs mcx-live 2>&1 | grep -iE 'auth|token|totp|renew|rate' | tail -15")
print(auth_logs.strip()[:500])

print("\n--- Token file ---")
token = ssh("docker exec mcx-live python3 -c \"import json,time; d=json.load(open('/app/data/db/dhan_token.json')); print(f'keys={list(d.keys())} expires_at={d.get(\\\"expires_at\\\",\\\"?\\\")} now={time.time():.0f}')\" 2>/dev/null || echo 'token file not found'")
print(token.strip()[:300])

print("\n--- Market WS detail ---")
mkt = api("/api/market-data")
if "instruments" in mkt:
    for inst, data in mkt["instruments"].items():
        print(f"  {inst}: LTP={data.get('ltp')} spread={data.get('spread')} ticks={data.get('tick_count')} ts={data.get('timestamp')}")
    print(f"  ws_connected: {mkt.get('ws_connected')}")
    print(f"  adapter_stats: {mkt.get('adapter_stats',{})}")

print("\n" + "=" * 80)
print("SECTION 9: DHAN ORDER WEBSOCKET")
print("=" * 80)
ow_logs = ssh("docker logs mcx-live 2>&1 | grep -iE 'order.*ws|order.*watch|order.*event|broker.*order|order.*connect' | tail -20")
print(ow_logs.strip()[:500] if ow_logs.strip() else "  No order WS logs found in recent output")

ow_status = api("/api/orders")
if "orders" in ow_status:
    for o in ow_status.get("orders", []):
        broker_id = o.get("broker_order_id", "none")
        state = o.get("state", "?")
        otype = o.get("order_type", "?")
        oid = o.get("order_id", "?")[:24]
        print(f"  Order {oid}: type={otype} state={state} broker_id={broker_id}")
