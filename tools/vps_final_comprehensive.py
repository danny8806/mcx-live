"""Final comprehensive verification - all routes, all services."""
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

print("=" * 70)
print("FINAL COMPREHENSIVE CHECK - ALL SERVICES")
print("=" * 70)

# SECTION 1: MCX-LIVE (34 routes)
print("\n--- MCX-LIVE ROUTES ---")
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
print(f"  {len(routes)-fail}/{len(routes)} PASS")

# SECTION 2: OPTION DEMO
print("\n--- OPTION DEMO ROUTES ---")
opt_routes = ["/option/", "/api/options/status", "/api/options/overview",
              "/api/options/dashboard", "/api/options/config", "/api/options/trades"]
fail = 0
for r in opt_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    if code != "200":
        print(f"  FAIL: {r} -> {code}")
        fail += 1
print(f"  {len(opt_routes)-fail}/{len(opt_routes)} PASS")

# SECTION 3: SCREENER
print("\n--- SCREENER ---")
code = run("curl -sk -o /dev/null -w '%{http_code}' 'https://deltacapitals.systems/screener/'")
print(f"  /screener/ -> {code}")

# SECTION 4: FRONTEND SPAs
print("\n--- FRONTEND SPAS ---")
for path, expect in [("/", "Trading"), ("/option/", "Option"), ("/screener/", "Scanner")]:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{path}'")
    print(f"  {path:<20} -> {code}")

# SECTION 5: HEALTH + DATA
print("\n--- HEALTH + KEY DATA ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 120"))
overview = run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | head -c 200")
print(f"Overview: {overview}")
funds = run("curl -sk https://deltacapitals.systems/api/live/funds 2>/dev/null | head -c 150")
print(f"Funds: {funds}")

# SECTION 6: CONTAINER HEALTH
print("\n--- CONTAINER STATUS ---")
print(f"Health: {run('docker inspect mcx-live --format \"{{.State.Health.Status}}\"')}")
print(f"Image: {run('docker inspect mcx-live --format \"{{.Config.Image}}\"')}")
print(f"Restart count: {run('docker inspect mcx-live --format \"{{.RestartCount}}\"')}")
print(f"Engine: {run('docker inspect mcx-live --format \"{{.Config.Image}}\" | sed \"s/.*:/f/\"')}")

# SECTION 7: GATES
print("\n--- GATES (all should be OFF) ---")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); bs=l.get(chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(115)+chr(108),{}); print(f\"live_trading={l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))}, gate={l.get(chr(103)+chr(97)+chr(116)+chr(101))}, broker_sl={bs.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))}\")" 2>/dev/null'))

# SECTION 8: POSITIONS + ORDERS
print("\n--- POSITIONS + ORDERS ---")
print(run("curl -sk https://deltacapitals.systems/api/positions 2>/dev/null | head -c 80"))
print(run("curl -sk https://deltacapitals.systems/api/orders 2>/dev/null | head -c 120"))

# SECTION 9: CORS
print("\n--- CORS ---")
print(run("docker exec mcx-live printenv CORS_ORIGINS 2>/dev/null"))
cors_header = run("curl -sk -I -H 'Origin: https://deltacapitals.systems' https://deltacapitals.systems/api/overview 2>/dev/null | grep -i 'access-control'")
print(cors_header)

# SECTION 10: CONTAINER LOGS (errors)
print("\n--- RECENT ERRORS ---")
errors = run("docker logs mcx-live --tail 50 2>&1 | grep -iE 'error|traceback|exception' | tail -5")
print(errors if errors else "  None!")

print("\n" + "=" * 70)
print("CHECK COMPLETE")
print("=" * 70)

ssh.close()
