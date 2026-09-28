"""Debug why live_ops routes are missing."""
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

# Check live_ops.py in container
print("=== LIVE_OPS.PY in container ===")
print(run("docker exec mcx-live head -20 /app/dashboard/routes/live_ops.py"))

# Check if the module can be imported
print("\n=== IMPORT TEST ===")
print(run("docker exec mcx-live python3 -c \"from dashboard.routes import live_ops; print(dir(live_ops))\" 2>&1 | head -20"))

# Check ROUTE_MODULES in live/api.py
print("\n=== ROUTE_MODULES in live/api.py ===")
print(run("docker exec mcx-live grep -A 15 'ROUTE_MODULES' /app/live/api.py | head -20"))

# Check if reversals module exists
print("\n=== REVERSALS MODULE ===")
print(run("docker exec mcx-live head -20 /app/dashboard/routes/reversals.py"))

# Check if live_ops router has routes
print("\n=== LIVE_OPS ROUTER ROUTES ===")
print(run("docker exec mcx-live grep '@router' /app/dashboard/routes/live_ops.py | head -20"))

# Check all registered routes in the running app
print("\n=== ALL REGISTERED ROUTES ===")
print(run("curl -sk http://127.0.0.1:8001/openapi.json 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(p) for p in sorted(d.get('paths',{}).keys())]\""))

ssh.close()
