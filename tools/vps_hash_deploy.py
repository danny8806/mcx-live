import paramiko, io, sys, hashlib
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
def run(cmd, t=60):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

print("=== DOCKER PS ===")
print(run("docker ps --format '{{.ID}}  {{.Image}}  {{.Names}}  {{.Status}}'"))
print()
print("=== IMAGE INFO ===")
print(run("docker images | head -20"))
print()
print("=== CONTAINER WORKDIR / APP FILES ===")
print(run("docker exec mcx-live sh -c 'pwd; ls' | head -60"))
print()
print("=== CONTAINER PROC (python entry) ===")
print(run("docker exec mcx-live sh -c 'ps aux | grep -i python | grep -v grep'"))
print()
print("=== key file hashes INSIDE container ===")
keys = [
    "/app/trading_engine.py",
    "/app/execution/price_model.py",
    "/app/execution/live/engine.py",
    "/app/execution/live/order_watcher.py",
    "/app/execution/live/dhan_transport.py",
    "/app/execution/live/dhan_order_ws.py",
    "/app/execution/live/broker_sync.py",
    "/app/execution/live/broker_client.py",
    "/app/execution/order_manager.py",
    "/app/execution/broker_router.py",
    "/app/execution/rejection_classifier.py",
    "/app/strategies/runtime.py",
    "/app/strategies/instance.py",
    "/app/strategies/htf_state.py",
    "/app/config/live_settings.json",
    "/app/dashboard/frontend.py",
]
print(run("docker exec mcx-live sh -c 'cd /app " + " && ".join("sha256sum %s" % k for k in keys) + "'"))
ssh.close()