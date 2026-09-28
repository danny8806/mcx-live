"""Verify VPS config and diagnose frontend."""
import paramiko, json

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
        return out or "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

# Write a check script to VPS first
sftp = ssh.open_sftp()
with sftp.open("/tmp/gate_check.py", "w") as f:
    f.write('''
import json
c = json.load(open("/app/config/live_settings.json"))
l = c.get("live", {})
s = c.get("strategies", {})
bs = l.get("broker_sl", {})
print("live_trading_enabled:", repr(l.get("live_trading_enabled")))
print("gate:", repr(l.get("gate")))
print("broker_sl.enabled:", repr(bs.get("enabled")))
for k, v in s.items():
    print(f"  {k}: enabled={v.get('enabled')}, gate={v.get('live_gate')}, entry={v.get('entry_enabled')}, exit={v.get('exit_enabled')}, rev={v.get('reversal_enabled')}, sl={v.get('sl_enabled')}")
''')
sftp.close()

print("=== GATES ON VPS ===")
print(run("docker cp /tmp/gate_check.py mcx-live:/tmp/gate_check.py && docker exec mcx-live python3 /tmp/gate_check.py"))

print("\n=== CONTAINER ENV ===")
env_out = run('docker inspect mcx-live --format "{{range .Config.Env}}{{println .}}{{end}}"')
for line in sorted(env_out.split("\n")):
    if line.strip():
        print(f"  {line}")

print("\n=== FRONTEND ASSETS ===")
print(run("docker exec mcx-live ls -la /app/dashboard-ui/dist/assets/"))

print("\n=== CORS CHECK ===")
print(run("docker exec mcx-live python3 -c \"import os; print('CORS_ORIGINS:', repr(os.environ.get('CORS_ORIGINS', 'NOT SET')))\""))

print("\n=== WS CONNECTIONS ===")
print(run("curl -s http://localhost:8001/api/health | python3 -c 'import sys,json; d=json.load(sys.stdin); print(\"ws_connections:\", d.get(\"ws_connections\"))'"))

print("\n=== LIVE OPERATIONS (any open orders?) ===")
print(run("curl -s http://localhost:8001/api/live/orders 2>/dev/null | head -c 300"))

print("\n=== OVERVIEW (equity + positions) ===")
print(run("curl -s http://localhost:8001/api/overview | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps({k:d[k] for k in ['equity_source','total_equity','available_margin','today_pnl','positions','strategies'] if k in d}, indent=2, default=str)[:600])\""))

ssh.close()
