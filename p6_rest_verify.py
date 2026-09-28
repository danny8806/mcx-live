#!/usr/bin/env python3
"""Verify Dhan REST after token renewal."""
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

# 1. Dhan REST from container (simple approach)
print("=== DHAN REST VERIFICATION ===")

rest_tests = {
    "Fund Limits": "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/fundlimit',headers=h),timeout=10); print(json.dumps(json.loads(r.read()),indent=2))",
    "Positions": "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/positions',headers=h),timeout=10); print(json.dumps(json.loads(r.read()),indent=2))",
    "Orders": "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/orders',headers=h),timeout=10); d=json.loads(r.read()); print(f'type={type(d).__name__} count={len(d) if isinstance(d,list) else \"N/A\"}')",
    "Trades": "import urllib.request,json,os; t=os.environ.get('DHAN_ACCESS_TOKEN',''); c=os.environ.get('DHAN_CLIENT_ID',''); h={'access-token':t,'client-id':c}; r=urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/trades',headers=h),timeout=10); d=json.loads(r.read()); print(f'type={type(d).__name__} count={len(d) if isinstance(d,list) else \"N/A\"}')",
}

for label, cmd in rest_tests.items():
    r = ssh(f"docker exec mcx-live python3 -c \"{cmd}\"")
    print(f"\n--- {label} ---")
    print(r.strip()[:500])

# 2. Token info
print("\n=== TOKEN INFO ===")
r = ssh("docker exec mcx-live python3 -c \"import json,time; d=json.load(open('/app/data/db/dhan_token.json')); print(f'expires={d.get(chr(101)+chr(120)+chr(112)+chr(105)+chr(114)+chr(101)+chr(115)+chr(95)+chr(97)+chr(116),chr(63))} now={time.time():.0f}')\"")
print(r.strip())

# 3. All API endpoints
print("\n=== API ENDPOINTS ===")
for ep in ["/health", "/api/positions", "/api/orders", "/api/market-data", "/api/risk", "/api/overview", "/api/reconciliation", "/api/trades", "/api/pnl", "/api/fills", "/api/strategies", "/api/settings"]:
    r = api(ep)
    ok = "error" not in r
    if ok:
        extra = ""
        if "execution_mode" in r: extra += f" mode={r['execution_mode']}"
        if "count" in r: extra += f" count={r['count']}"
        if "ws_connected" in r: extra += f" ws={r['ws_connected']}"
        if "equity" in r: extra += f" equity={r['equity']}"
        print(f"  {ep}: OK{extra}")
    else:
        print(f"  {ep}: ERR {r['error'][:60]}")

# 4. Container status
print("\n=== CONTAINER STATUS ===")
r = ssh("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(f"  {r.strip()}")

r = ssh("docker logs mcx-live 2>&1 | grep -iE 'auth|token|renew|totp' | tail -10")
print(f"  Auth: {r.strip()[:300]}")

r = ssh("docker logs mcx-live 2>&1 | grep -iE 'ready|trading|signal|order|fill' | tail -10")
print(f"  Trading: {r.strip()[:300]}")
