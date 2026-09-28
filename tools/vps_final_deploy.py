"""Final deploy: config, overview fix, CORS, gates check."""
import paramiko, time

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# 1. Copy overview.py
print("DEPLOYING overview.py...")
sftp = ssh.open_sftp()
sftp.put("dashboard/routes/overview.py", "/tmp/overview.py")
sftp.put("config/live_settings.json", "/tmp/live_settings.json")
sftp.close()
print(run("docker cp /tmp/overview.py mcx-live:/app/dashboard/routes/overview.py"))
print(run("docker cp /tmp/live_settings.json mcx-live:/app/config/live_settings.json"))

# 2. Restart
print("\nRESTARTING...")
print(run("docker restart mcx-live", timeout=60))

# 3. Wait
print("Waiting for healthy...")
for i in range(15):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    print(f"  [{i*3}s] {health}")
    if health == "healthy":
        break

# 4. Verify
print("\n" + "=" * 60)
print("FINAL VERIFICATION")
print("=" * 60)

print("\n--- GATES ---")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); print(l.get(chr(103)+chr(97)+chr(116)+chr(101)))"'))

print("\n--- CORS ---")
print(run("docker exec mcx-live printenv CORS_ORIGINS"))

print("\n--- ALL ROUTES (unique per service) ---")
tests = [
    # LIVE routes
    ("LIVE /api/overview", "/api/overview"),
    ("LIVE /api/health", "/api/health"),
    ("LIVE /api/strategies", "/api/strategies"),
    ("LIVE /api/positions", "/api/positions"),
    ("LIVE /api/orders", "/api/orders"),
    ("LIVE /api/trades", "/api/trades"),
    ("LIVE /api/pnl", "/api/pnl"),
    ("LIVE /api/risk", "/api/risk"),
    ("LIVE /api/market-data", "/api/market-data"),
    ("LIVE /api/indicators", "/api/indicators"),
    ("LIVE /api/alerts", "/api/alerts"),
    ("LIVE /api/settings", "/api/settings"),
    ("LIVE /api/audit", "/api/audit"),
    ("LIVE /api/reconciliation", "/api/reconciliation"),
    ("LIVE /api/equity-curve", "/api/equity-curve"),
    ("LIVE /api/fills", "/api/fills"),
    ("LIVE /api/htf", "/api/htf"),
    ("LIVE /api/envs", "/api/envs"),
    ("LIVE /api/broker-events", "/api/broker-events"),
    ("LIVE /api/alert-ledger", "/api/alert-ledger"),
    ("LIVE /api/analytics/strategies", "/api/analytics/strategies"),
    ("LIVE /api/replay/status", "/api/replay/status"),
    ("LIVE /api/live/dashboard", "/api/live/dashboard"),
    ("LIVE /api/live/orders", "/api/live/orders"),
    ("LIVE /api/live/positions", "/api/live/positions"),
    ("LIVE /api/live/pnl", "/api/live/pnl"),
    ("LIVE /api/live/funds", "/api/live/funds"),
    ("LIVE /api/live/profile", "/api/live/profile"),
    ("LIVE /api/live/signals", "/api/live/signals"),
    ("LIVE /api/live/candles", "/api/live/candles"),
    ("LIVE /api/live/recon", "/api/live/recon"),
    ("LIVE /api/live/telegram", "/api/live/telegram"),
    ("LIVE /api/live/sync", "/api/live/sync"),
    ("LIVE /api/live/timeline", "/api/live/timeline"),
    ("LIVE /api/reversals", "/api/reversals"),
    # OPTION routes
    ("OPTION /option/", "/option/"),
    ("OPTION /api/options/status", "/api/options/status"),
    # SCREENER routes
    ("SCREENER /screener/", "/screener/"),
]
for name, path in tests:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{path}'")
    ok = "OK" if code in ("200", "308") else f"FAIL({code})"
    print(f"  {name:<45} {code:<6} {ok}")

# Verify no cross-contamination
print("\n--- /api/health should be mcx-live (has 'engine' field) ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 100"))

print("\n--- /api/options/overview should be option-demo (has 'open_count') ---")
print(run("curl -sk https://deltacapitals.systems/api/options/overview 2>/dev/null | head -c 100"))

print("\n--- OVERVIEW ---")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('total_equity:', d.get('total_equity',{}).get('value')); print('starting_capital:', d.get('starting_capital',{}).get('value'))\""))

ssh.close()
