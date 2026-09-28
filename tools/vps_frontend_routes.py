"""Check what the frontend actually fetches vs what nginx routes."""
import paramiko

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=15):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# 1. Check what APP_API_BASE and APP_WS_BASE the frontend is injecting
print("=== FRONTEND index.html (served) ===")
print(run("curl -sk https://deltacapitals.systems/ 2>/dev/null"))

# 2. Check the JS bundle for fetch/API calls
print("\n=== JS BUNDLE API CALLS ===")
print(run("docker exec mcx-live grep -oE 'fetch\\([\"'\\''/][^)]+' /app/dashboard-ui/dist/assets/index-6-YdCe5Z.js 2>/dev/null | head -30"))

# 3. Check what window.APP_API_BASE and window.APP_WS_BASE are set to
print("\n=== JS BUNDLE: APP_API_BASE / APP_WS_BASE ===")
print(run("docker exec mcx-live grep -oE 'APP_(API|WS)_BASE[^;]*' /app/dashboard-ui/dist/assets/index-6-YdCe5Z.js 2>/dev/null | head -10"))

# 4. Test each specific route the frontend would use
print("\n=== ROUTE TESTS ===")
routes = [
    "/api/overview",
    "/api/health",
    "/api/strategies",
    "/api/positions",
    "/api/orders",
    "/api/trades",
    "/api/pnl",
    "/api/risk",
    "/api/market-data",
    "/api/indicators",
    "/api/alerts",
    "/api/settings",
    "/api/audit",
    "/api/reconciliation",
    "/api/envs",
    "/api/broker-events",
    "/api/alert-ledger",
    "/api/equity-curve",
    "/api/fills",
    "/api/replay/status",
    "/api/health/system",
    "/api/live/profile",
    "/api/live/funds",
    "/api/live/sync",
    "/api/live/candles",
    "/api/live/signals",
    "/api/live/orders",
    "/api/live/positions",
    "/api/live/pnl",
    "/api/live/recon",
    "/api/live/telegram",
    "/api/live/dashboard",
    "/api/live/order/test",
    "/api/live/timeline",
    "/api/reversals",
    "/api/overview/GOLDM",
    "/api/strategies/gold_01",
    "/api/strategies/gold_01/control",
    "/api/strategies/gold_01/parameters",
    "/api/pnl/GOLDM",
    "/api/pnl/GOLDM/strategy/gold_01",
    "/api/positions/test",
    "/api/orders/test",
    "/api/trades/test",
    "/api/trades/orphan-scan",
    "/api/trades/lifecycle-reconcile",
    "/api/market-data/GOLDM",
    "/api/indicators/GOLDM",
    "/api/htf/GOLDM",
    "/api/reversals/test",
    "/api/equity-curve/GOLDM",
    "/api/pnl/GOLDM/strategy/gold_01",
    "/api/settings/refresh",
    "/api/broker-events/actions",
    "/api/broker-events/test",
    "/api/alert-ledger/stats",
    "/api/alert-ledger/test",
]

for r in routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    status = "OK" if code == "200" else f"FAIL({code})"
    print(f"  {r:<50} -> {status}")

# 5. Check nginx error log for recent 502s or misses
print("\n=== NGINX ERROR LOG (last 10 lines) ===")
print(run("tail -10 /var/log/nginx/error.log 2>/dev/null"))

print("\n=== NGINX ACCESS LOG (last 20 lines, mcx-live routes) ===")
print(run("tail -50 /var/log/nginx/access.log 2>/dev/null | grep -E '/api/(overview|strategies|live|health|positions|orders)' | tail -20"))

ssh.close()
