#!/usr/bin/env python3
"""Phase 6 VPS deep verification."""
import paramiko
from vps_credentials import load_vps_password
import time
import urllib.request
import json

VPS_HOST = "200.234.44.93"
VPS_USER = "root"
VPS_PASS = load_vps_password()
def ssh_run(cmd, timeout=20):
    transport = paramiko.Transport((VPS_HOST, 22))
    transport.connect(username=VPS_USER, password=VPS_PASS)
    channel = transport.open_session()
    channel.settimeout(timeout)
    channel.exec_command(cmd)
    out = b""
    while not channel.exit_status_ready():
        if channel.recv_ready():
            out += channel.recv(65536)
        time.sleep(0.1)
    while channel.recv_ready():
        out += channel.recv(65536)
    channel.close()
    transport.close()
    return out.decode(errors="replace")

def api_get(path):
    """Fetch from live API."""
    url = f"http://200.234.44.93:8001{path}"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        return {"error": str(e)}

print("="*60)
print("  PHASE 6: DEPLOYED SYSTEM VERIFICATION")
print("="*60)

# 1. Source hash verification
print("\n--- SOURCE HASH VERIFICATION ---")
files_to_check = [
    "trading_engine.py",
    "execution/price_model.py",
    "execution/live/engine.py",
    "execution/live/dhan_transport.py",
    "execution/live/order_watcher.py",
    "execution/live/poller.py",
    "execution/live/broker_sync.py",
    "strategies/instance.py",
    "persistence/database.py",
    "live/api.py",
    "notifications/telegram_client.py",
]
for f in files_to_check:
    remote = ssh_run(f"md5sum /app/{f} 2>/dev/null || echo MISSING")
    print(f"  {f}: {remote.strip()}")

# 2. API health
print("\n--- API HEALTH ---")
health = api_get("/health")
print(f"  /health: {json.dumps(health, indent=2)[:200]}")

# 3. API system status
print("\n--- SYSTEM STATUS ---")
status = api_get("/api/status")
print(f"  /api/status: {json.dumps(status, indent=2)[:300]}")

# 4. Trading engine status
print("\n--- TRADING ENGINE ---")
engine = api_get("/api/engine/status")
print(f"  /api/engine/status: {json.dumps(engine, indent=2)[:500]}")

# 5. Positions
print("\n--- POSITIONS ---")
positions = api_get("/api/positions")
print(f"  /api/positions: {json.dumps(positions, indent=2)[:300]}")

# 6. Orders
print("\n--- ORDERS ---")
orders = api_get("/api/orders")
print(f"  /api/orders: {json.dumps(orders, indent=2)[:300]}")

# 7. Strategies
print("\n--- STRATEGIES ---")
strategies = api_get("/api/strategies")
print(f"  /api/strategies: {json.dumps(strategies, indent=2)[:500]}")

# 8. Risk
print("\n--- RISK ---")
risk = api_get("/api/risk")
print(f"  /api/risk: {json.dumps(risk, indent=2)[:300]}")

# 9. Market data
print("\n--- MARKET DATA ---")
market = api_get("/api/market-data")
print(f"  /api/market-data: {json.dumps(market, indent=2)[:300]}")

# 10. DB verification
print("\n--- DATABASE ---")
db = ssh_run("docker exec mcx-live python -c \"import sqlite3; conn=sqlite3.connect('/app/data/db/trading.db'); c=conn.cursor(); c.execute('SELECT name FROM sqlite_master WHERE type=\\\"table\\\"'); print('\\n'.join([r[0] for r in c.fetchall()]))\"")
print(f"  Tables: {db.strip()}")

# 11. DB trade count
trades = ssh_run("docker exec mcx-live python -c \"import sqlite3; conn=sqlite3.connect('/app/data/db/trading.db'); c=conn.cursor(); c.execute('SELECT COUNT(*) FROM trades'); print(c.fetchone()[0])\"")
print(f"  Trade count: {trades.strip()}")

# 12. DB signal count
signals = ssh_run("docker exec mcx-live python -c \"import sqlite3; conn=sqlite3.connect('/app/data/db/trading.db'); c=conn.cursor(); c.execute('SELECT COUNT(*) FROM signals'); print(c.fetchone()[0])\"")
print(f"  Signal count: {signals.strip()}")

# 13. DB order count
order_count = ssh_run("docker exec mcx-live python -c \"import sqlite3; conn=sqlite3.connect('/app/data/db/trading.db'); c=conn.cursor(); c.execute('SELECT COUNT(*) FROM orders'); print(c.fetchone()[0])\"")
print(f"  Order count: {order_count.strip()}")

# 14. Dhan connectivity check
print("\n--- DHAN CONNECTIVITY ---")
dhan_check = ssh_run("docker exec mcx-live python -c \"\nimport urllib.request, json, os\ntoken = os.environ.get('DHAN_ACCESS_TOKEN', '')\nclient_id = os.environ.get('DHAN_CLIENT_ID', '')\nheaders = {'access-token': token, 'client-id': client_id}\nreq = urllib.request.Request('https://api.dhan.co/v2/fundlimit', headers=headers)\ntry:\n    resp = urllib.request.urlopen(req, timeout=10)\n    data = json.loads(resp.read())\n    print(json.dumps(data, indent=2))\nexcept Exception as e:\n    print(f'ERROR: {e}')\n\"")
print(f"  Dhan funds: {dhan_check.strip()[:300]}")

# 15. Dhan positions
dhan_pos = ssh_run("docker exec mcx-live python -c \"\nimport urllib.request, json, os\ntoken = os.environ.get('DHAN_ACCESS_TOKEN', '')\nclient_id = os.environ.get('DHAN_CLIENT_ID', '')\nheaders = {'access-token': token, 'client-id': client_id}\nreq = urllib.request.Request('https://api.dhan.co/v2/positions', headers=headers)\ntry:\n    resp = urllib.request.urlopen(req, timeout=10)\n    data = json.loads(resp.read())\n    print(json.dumps(data, indent=2))\nexcept Exception as e:\n    print(f'ERROR: {e}')\n\"")
print(f"  Dhan positions: {dhan_pos.strip()[:300]}")

# 16. Dhan orders today
dhan_orders = ssh_run("docker exec mcx-live python -c \"\nimport urllib.request, json, os\ntoken = os.environ.get('DHAN_ACCESS_TOKEN', '')\nclient_id = os.environ.get('DHAN_CLIENT_ID', '')\nheaders = {'access-token': token, 'client-id': client_id}\nreq = urllib.request.Request('https://api.dhan.co/v2/orders', headers=headers)\ntry:\n    resp = urllib.request.urlopen(req, timeout=10)\n    data = json.loads(resp.read())\n    if isinstance(data, list):\n        print(f'Order count: {len(data)}')\n        for o in data[:5]:\n            print(f'  {o.get(\"orderId\",\"?\")} | {o.get(\"orderStatus\",\"?\")} | {o.get(\"transactionType\",\"?\")} | {o.get(\"orderType\",\"?\")} | qty={o.get(\"quantity\",\"?\")}')\n    else:\n        print(json.dumps(data, indent=2)[:300])\nexcept Exception as e:\n    print(f'ERROR: {e}')\n\"")
print(f"  Dhan orders: {dhan_orders.strip()[:500]}")

# 17. Telegram check
print("\n--- TELEGRAM ---")
tg = api_get("/api/telegram/stats")
print(f"  /api/telegram/stats: {json.dumps(tg, indent=2)[:200]}")

# 18. WebSocket status
print("\n--- WEBSOCKET STATUS ---")
ws = api_get("/api/websocket/status")
print(f"  /api/websocket/status: {json.dumps(ws, indent=2)[:300]}")

# 19. Reconciliation
print("\n--- RECONCILIATION ---")
recon = api_get("/api/reconciliation")
print(f"  /api/reconciliation: {json.dumps(recon, indent=2)[:500]}")

# 20. Container logs (errors only)
print("\n--- CONTAINER ERRORS ---")
errors = ssh_run("docker logs mcx-live 2>&1 | grep -iE 'error|exception|traceback|fail|reject' | tail -20")
print(f"  Errors: {errors.strip()[:500]}")

# 21. Risk config verification
print("\n--- RISK CONFIG ---")
risk_cfg = ssh_run("docker exec mcx-live python -c \"\nimport json\nwith open('/app/config/live_settings.json') as f:\n    cfg = json.load(f)\nrisk = cfg.get('risk', {})\nprint(f'max_daily_loss: {risk.get(\\\"max_daily_loss\\\", \\\"NOT SET\\\")}')\nprint(f'max_drawdown_pct: {risk.get(\\\"max_drawdown_pct\\\", \\\"NOT SET\\\")}')\nbroker_sl = cfg.get('live', {}).get('broker_sl', {})\nprint(f'broker_sl.enabled: {broker_sl.get(\\\"enabled\\\", \\\"NOT SET\\\")}')\nprint(f'broker_sl.fail_closed: {broker_sl.get(\\\"fail_closed\\\", \\\"NOT SET\\\")}')\now = cfg.get('live', {}).get('order_watcher', {})\nprint(f'order_watcher.market_fallback_enabled: {ow.get(\\\"market_fallback_enabled\\\", \\\"NOT SET\\\")}')\nprint(f'order_watcher.limit_skip_policy.enabled: {ow.get(\\\"limit_skip_policy\", {}).get(\\\"enabled\\\", \\\"NOT SET\\\")}')\n\"")
print(f"  Config: {risk_cfg.strip()}")
