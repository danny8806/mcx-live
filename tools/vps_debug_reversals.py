"""Debug reversals 404."""
import paramiko, sys, io
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
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")

# Check container logs for reversals init errors
print("=== CONTAINER LOGS - reversal errors ===")
print(run("docker logs mcx-live 2>&1 | grep -i 'reversal' | head -20"))

# Check if reversals init was called
print("\n=== reversals.init calls in container ===")
print(run("docker exec mcx-live grep -rn 'reversals.init' /app/live/ 2>/dev/null"))
print(run("docker exec mcx-live grep -rn 'reversals.init' /app/dashboard/ 2>/dev/null"))

# List all registered routes on the running app
print("\n=== ALL FASTAPI ROUTES ===")
print(run('docker exec mcx-live python3 -c "import json,sys; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from live.api import app; routes=[{chr(109)+chr(101)+chr(116)+chr(104)+chr(111)+chr(100)+chr(115)+chr(115):r.methods,chr(112)+chr(97)+chr(116)+chr(104):r.path} for r in app.routes if hasattr(r,chr(109)+chr(101)+chr(116)+chr(104)+chr(111)+chr(100)+chr(115))]; print(json.dumps(routes,indent=2))" 2>/dev/null | head -c 3000'))

# Direct test of the reversals endpoint
print("\n=== DIRECT REVERSALS TEST ===")
print(run("docker exec mcx-live curl -s http://127.0.0.1:8001/api/reversals 2>/dev/null"))

# Check the reversals route is in the container's reversals.py
print("\n=== reversals.py routes ===")
print(run("docker exec mcx-live grep -n '@router' /app/dashboard/routes/reversals.py 2>/dev/null"))

ssh.close()
