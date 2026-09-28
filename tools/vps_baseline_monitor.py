import paramiko, sys, io, time
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
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
def py(cmd):
    return run(f"python3 -c \"{cmd}\"")

print("=== BASELINE (trading enabled) ===")
print("--- market data ---")
print(run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null | head -c 400"))
print("--- funds ---")
print(run("curl -s http://127.0.0.1:8001/api/live/funds 2>/dev/null | head -c 400"))
print("--- positions ---")
print(run("curl -s http://127.0.0.1:8001/api/live/positions 2>/dev/null | head -c 400"))
print("--- orders ---")
print(run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null | head -c 400"))
print("--- live signals ---")
print(run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null | head -c 400"))
print("--- pnl ---")
print(run("curl -s http://127.0.0.1:8001/api/live/pnl 2>/dev/null | head -c 400"))
print("--- timeline/recent events ---")
print(run("curl -s http://127.0.0.1:8001/api/live/timeline 2>/dev/null | head -c 600"))
print("--- reversal route ---")
print(run("curl -s http://127.0.0.1:8001/api/reversals 2>/dev/null | head -c 300"))
print("--- reconnect/ws state ---")
print(run("docker logs mcx-live --tail 12 2>&1 | tail -12"))
ssh.close()
