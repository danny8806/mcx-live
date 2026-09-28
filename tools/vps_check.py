"""Quick VPS health check - run locally."""
import paramiko, json, os

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
    try:
        _, o, e = ssh.exec_command(cmd, timeout=timeout)
        out = o.read().decode("utf-8", "replace").strip()
        err = e.read().decode("utf-8", "replace").strip()
        return out or err or "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

print("=" * 60)
print("1. CONTAINER STATUS")
print("=" * 60)
print(run('docker ps --format "table {{"Names"}}\\t{{"Status"}}\\t{{"Ports"}}" | grep mcx'))

print("\n" + "=" * 60)
print("2. GATES ON VPS (must all be OFF)")
print("=" * 60)
# Write a small script to the VPS to avoid escaping hell
run('cat > /tmp/check_gates.py << \'PYEOF\'\nimport json\nc = json.load(open("/app/config/live_settings.json"))\nl = c.get("live", {})\ns = c.get("strategies", {})\nprint("live_trading_enabled:", l.get("live_trading_enabled"))\nprint("gate:", l.get("gate"))\nprint("broker_sl.enabled:", l.get("broker_sl", {}).get("enabled"))\nfor k, v in s.items():\n    print(f"{k}: enabled={v.get(\'enabled\')}, gate={v.get(\'live_gate\')}, entry={v.get(\'entry_enabled\')}, exit={v.get(\'exit_enabled\')}, rev={v.get(\'reversal_enabled\')}, sl={v.get(\'sl_enabled\')}\")\nPYEOF')
print(run("docker exec mcx-live python3 /tmp/check_gates.py 2>/dev/null || docker exec mcx-live cat /app/config/live_settings.json | python3 -c \"import sys,json; c=json.load(sys.stdin); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); s=c.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115),{}); print(l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(108)+chr(98)+chr(108)+chr(101)+chr(100))); print(l.get(chr(103)+chr(97)+chr(116)+chr(101))); [print(k, v.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)), v.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(103)+chr(97)+chr(116)+chr(101))) for k,v in s.items()]\""))

print("\n" + "=" * 60)
print("3. API HEALTH")
print("=" * 60)
print(run("curl -s http://localhost:8001/api/health"))

print("\n" + "=" * 60)
print("4. OVERVIEW (first 500 chars)")
print("=" * 60)
print(run('curl -s http://localhost:8001/api/overview | head -c 500'))

print("\n" + "=" * 60)
print("5. RECENT LOGS (last 50 lines)")
print("=" * 60)
print(run("docker logs mcx-live --tail 50 2>&1"))

print("\n" + "=" * 60)
print("6. FRONTEND DIST EXISTS")
print("=" * 60)
print(run("docker exec mcx-live ls -la /app/dashboard-ui/dist/index.html"))

print("\n" + "=" * 60)
print("7. CORS ENV")
print("=" * 60)
print(run("docker exec mcx-live printenv CORS_ORIGINS 2>/dev/null; docker exec mcx-live printenv APP_API_BASE 2>/dev/null; docker exec mcx-live printenv APP_WS_BASE 2>/dev/null"))

ssh.close()
