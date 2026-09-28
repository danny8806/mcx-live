"""Verify key code changes are in the running container."""
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

print("=== CONTAINER IMAGE INFO ===")
print("Image:", run("docker inspect mcx-live --format '{{.Config.Image}}'"))
print("Created:", run("docker inspect mcx-live --format '{{.Created}}'"))

print("\n=== KEY CODE CHECKS ===")

# 1. price_model.py - should have LIMIT-first logic
print("\n1. price_model.py - LIMIT-first check:")
lim = run("docker exec mcx-live grep -n 'limit_first' /app/execution/price_model.py 2>/dev/null | head -5")
print(f"  {lim}")
fallback = run("docker exec mcx-live grep -n 'MARKET_FALLBACK' /app/execution/price_model.py 2>/dev/null | head -5")
print(f"  {fallback}")

# 2. trading_engine.py - logger fix
print("\n2. trading_engine.py - logger fix:")
logger_check = run("docker exec mcx-live grep -n 'logger\\.' /app/trading_engine.py 2>/dev/null | head -5")
print(f"  'logger.' references: {logger_check if logger_check else 'NONE (fixed!)'}")
log_check = run("docker exec mcx-live grep -n 'log\\.warning.*Telegram alert ledger' /app/trading_engine.py 2>/dev/null")
print(f"  'log.warning' fix: {log_check}")

# 3. live/api.py - reversals route
print("\n3. live/api.py - reversals route:")
reversal = run("docker exec mcx-live grep -n 'reversal' /app/live/api.py 2>/dev/null | head -5")
print(f"  {reversal}")

# 4. live_ops.py - not empty
print("\n4. live_ops.py:")
lops_size = run("docker exec mcx-live wc -l /app/dashboard/routes/live_ops.py 2>/dev/null")
print(f"  Lines: {lops_size}")
lops_routes = run("docker exec mcx-live grep -c '@router' /app/dashboard/routes/live_ops.py 2>/dev/null")
print(f"  Routes: {lops_routes}")

# 5. reversals.py - not empty
print("\n5. reversals.py:")
rev_size = run("docker exec mcx-live wc -l /app/dashboard/routes/reversals.py 2>/dev/null")
print(f"  Lines: {rev_size}")

# 6. live_settings.json - all gates OFF
print("\n6. live_settings.json - gates:")
gates = run('docker exec mcx-live python3 -c "import json; c=json.load(open(\'/app/config/live_settings.json\')); l=c.get(\'live\',{}); s=c.get(\'strategies\',{}); bs=l.get(\'broker_sl\',{}); print(f\'live_trading={l.get(\"live_trading_enabled\")}, gate={l.get(\"gate\")}, broker_sl={bs.get(\"enabled\")}\'); [print(f\'  {k}: enabled={v.get(\"enabled\")}, gate={v.get(\"live_gate\")}, entry={v.get(\"entry_enabled\")}\') for k,v in s.items()]" 2>/dev/null')
print(f"  {gates}")

# 7. Docker image layers
print("\n7. Docker image layers:")
print(run("docker history mcx-trader-live:remedy-f29 --no-trunc 2>/dev/null | head -10"))

# 8. Running container is from the image?
print("\n8. Container image ID vs image ID:")
print("  Container:", run("docker inspect mcx-live --format '{{.Image}}'"))
print("  Image:", run("docker inspect mcx-trader-live:remedy-f29 --format '{{.Id}}'"))

# 9. Quick functional test
print("\n9. FUNCTIONAL TESTS:")
print("  Health:", run("curl -sk http://127.0.0.1:8001/api/health 2>/dev/null | head -c 80"))
print("  Overview:", run("curl -sk http://127.0.0.1:8001/api/overview 2>/dev/null | head -c 120"))
print("  Live funds:", run("curl -sk http://127.0.0.1:8001/api/live/funds 2>/dev/null | head -c 120"))
print("  Strategies:", run("curl -sk http://127.0.0.1:8001/api/strategies 2>/dev/null | head -c 120"))

ssh.close()
