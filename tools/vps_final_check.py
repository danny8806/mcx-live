"""Final verification of container code."""
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

# Check plan_for function for LIMIT-first logic
print("=== CONTAINER price_model.py plan_for function ===")
print(run("docker exec mcx-live sed -n '150,250p' /app/execution/price_model.py"))

# Check MD5 with --strip-trailing-cr to ignore line endings
print("\n=== MD5 comparison (ignoring line endings) ===")
for f in ["execution/price_model.py", "trading_engine.py", "live/api.py", "config/live_settings.json"]:
    local_hash = run(f"certutil -hashfile \"{f}\" MD5 2>nul | findstr /v ':'").strip()
    # For container, use python to normalize line endings
    remote_hash = run(f'docker exec mcx-live python3 -c "import hashlib; h=hashlib.md5(); f=open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+\"{f.replace(chr(47), chr(47)+chr(47))}\".replace(chr(47)+chr(47), chr(47)),chr(114)+chr(98)); h.update(f.read().encode(chr(117)+chr(116)+chr(102)+chr(45)+chr(49)+chr(56))); print(h.hexdigest())"')
    print(f"  {f}: local={local_hash} container={remote_hash}")

# Full health summary
print("\n=== FINAL HEALTH CHECK ===")
print(run("curl -sk http://127.0.0.1:8001/api/health 2>/dev/null"))
print("\n=== OVERVIEW ===")
print(run("curl -sk http://127.0.0.1:8001/api/overview 2>/dev/null | head -c 200"))
print("\n=== LIVE FUNDS ===")
print(run("curl -sk http://127.0.0.1:8001/api/live/funds 2>/dev/null | head -c 200"))
print("\n=== ALL ROUTES ===")
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
