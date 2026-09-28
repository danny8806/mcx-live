import paramiko, io, sys, json
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
def run(cmd, t=45):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

def api(path):
    return run("docker exec mcx-live curl -s -m 10 http://127.0.0.1:8001%s" % path)

print("=== GET /api/health ===")
print(api("/api/health")[:1500])
print()
print("=== GET /api/live/dashboard (truncated to 4000) ===")
print(api("/api/live/dashboard")[:4000])
print()
print("=== GET /api/live/orders ===")
print(api("/api/live/orders")[:3000])
print()
print("=== GET /api/live/positions ===")
print(api("/api/live/positions")[:2000])
print()
print("=== GET /api/positions ===")
print(api("/api/positions")[:1500])
print()
print("=== GET /api/trades ===")
print(api("/api/trades")[:1500])
print()
print("=== GET /api/reconciliation ===")
print(api("/api/reconciliation")[:3000])
print()
print("=== GET /api/live/recon ===")
print(api("/api/live/recon")[:2500])
print()
ssh.close()