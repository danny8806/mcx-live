"""Check container code content."""
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
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

print("=== CONTAINER price_model.py head ===")
print(run("docker exec mcx-live head -80 /app/execution/price_model.py"))
print("\n=== limit_first search ===")
print(run('docker exec mcx-live grep -n "limit_first" /app/execution/price_model.py'))
print(run('docker exec mcx-live grep -n "plan_for" /app/execution/price_model.py'))
print(run('docker exec mcx-live grep -n "MARKET_FALLBACK" /app/execution/price_model.py'))
print(run('docker exec mcx-live grep -n "STOP_LIMIT" /app/execution/price_model.py'))
print("\n=== live_settings.json gates ===")
print(run('docker exec mcx-live python3 -c "import json; print(json.dumps(json.load(open(\"/app/config/live_settings.json\")), indent=2))"'))
print("\n=== CONTAINER trading_engine.py line 1145 ===")
print(run("docker exec mcx-live sed -n '1143,1148p' /app/trading_engine.py"))
ssh.close()
