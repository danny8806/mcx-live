"""Force reload overview.py in the running container."""
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

# Check how the app runs
print("=== PROCESSES IN CONTAINER ===")
print(run("docker exec mcx-live ps aux"))

# Check if uvicorn has --reload
print("\n=== CHECKING ENTRYPOINT ===")
print(run("docker exec mcx-live cat /app/live/run.py 2>/dev/null | head -30"))

# Try to find uvicorn master process
print("\n=== MAIN PROCESS ===")
print(run("docker exec mcx-live cat /proc/1/cmdline | tr '\\0' ' '"))

# Check if we can touch the file to trigger reload
print("\n=== TOUCH FILE ===")
print(run("docker exec mcx-live touch /app/dashboard/routes/overview.py"))

import time
time.sleep(2)

# Test again
print("\n=== AFTER TOUCH - /api/overview starting_capital ===")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('total_equity:', d.get('total_equity',{}).get('value')); print('starting_capital:', d.get('starting_capital',{}).get('value')); print('net_pnl:', d.get('total_net_pnl',{}).get('value'))\""))

ssh.close()
