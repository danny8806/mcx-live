"""Fix nginx using python3 on the VPS."""
import paramiko, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")

# Use python3 on the VPS to fix the file
fix_script = r'''
with open("/etc/nginx/sites-available/deltacapitals.systems") as f:
    content = f.read()
old = "overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health"
new = "overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health|reversals"
if new not in content:
    content = content.replace(old, new)
    with open("/etc/nginx/sites-available/deltacapitals.systems", "w") as f:
        f.write(content)
    print("Fixed: added reversals to regex")
else:
    print("Already contains reversals")
'''

print(run(f'python3 -c "{fix_script}"'))

# Verify
print("\n=== GREP reversals ===")
print(run("grep reversals /etc/nginx/sites-available/deltacapitals.systems"))

# Test nginx
print("\n=== NGINX TEST ===")
print(run("nginx -t 2>&1"))

# Reload
print("\n=== RELOAD ===")
print(run("systemctl reload nginx 2>&1"))

# Test
import time
time.sleep(1)
print("\n=== TEST /api/reversals ===")
code = run("curl -sk -o /dev/null -w '%{http_code}' 'https://deltacapitals.systems/api/reversals'")
print(f"Status: {code}")
body = run("curl -sk 'https://deltacapitals.systems/api/reversals' 2>/dev/null | head -c 200")
print(f"Body: {body}")

# Recheck all routes
print("\n=== RECHECK ALL 34 ROUTES ===")
routes = ["/api/overview", "/api/health", "/api/strategies", "/api/positions", "/api/orders",
          "/api/trades", "/api/pnl", "/api/risk", "/api/market-data", "/api/indicators",
          "/api/alerts", "/api/settings", "/api/audit", "/api/reconciliation",
          "/api/equity-curve", "/api/fills", "/api/htf", "/api/envs",
          "/api/broker-events", "/api/alert-ledger",
          "/api/live/dashboard", "/api/live/orders", "/api/live/positions",
          "/api/live/pnl", "/api/live/funds", "/api/live/profile",
          "/api/live/signals", "/api/live/candles", "/api/live/recon",
          "/api/live/telegram", "/api/live/sync", "/api/live/timeline",
          "/api/analytics/strategies", "/api/reversals"]
fail = 0
for r in routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    if code != "200":
        print(f"  FAIL: {r} -> {code}")
        fail += 1
print(f"\n{len(routes)-fail}/{len(routes)} routes PASS")

ssh.close()
