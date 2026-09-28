"""Check why /api/live/ routes return 404 from backend."""
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

# Test directly against backend (bypass nginx)
print("=== DIRECT BACKEND TESTS (port 8001) ===")
tests = [
    "/api/overview",
    "/api/health",
    "/api/live/dashboard",
    "/api/live/funds",
    "/api/live/profile",
    "/api/live/signals",
    "/api/live/candles",
    "/api/live/orders",
    "/api/live/positions",
    "/api/live/pnl",
    "/api/live/recon",
    "/api/live/telegram",
    "/api/live/sync",
    "/api/live/timeline",
    "/api/reversals",
    "/api/replay/status",
]
for path in tests:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'http://127.0.0.1:8001{path}'")
    print(f"  {path:<40} {code}")

# Check backend logs for errors
print("\n=== BACKEND ERRORS (last 50 lines) ===")
print(run("docker logs mcx-live --tail 50 2>&1 | grep -iE 'error|traceback|exception|404|not found'"))

# Check if live_ops routes are registered
print("\n=== OPENAPI ROUTES containing 'live' ===")
print(run("curl -sk http://127.0.0.1:8001/openapi.json 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(p) for p in sorted(d.get('paths',{}).keys()) if 'live' in p or 'reversal' in p or 'replay' in p]\""))

# Check if there's a _live_env issue
print("\n=== DIRECT /api/live/funds response ===")
print(run("curl -sk http://127.0.0.1:8001/api/live/funds 2>/dev/null | head -c 300"))

print("\n=== DIRECT /api/live/dashboard response ===")
print(run("curl -sk http://127.0.0.1:8001/api/live/dashboard 2>/dev/null | head -c 300"))

print("\n=== DIRECT /api/reversals response ===")
print(run("curl -sk http://127.0.0.1:8001/api/reversals 2>/dev/null | head -c 300"))

ssh.close()
