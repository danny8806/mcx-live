"""Test reversals from inside the container."""
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

# Internal test via Python requests
print("=== INTERNAL REVERSALS TEST ===")
print(run('docker exec mcx-live python3 -c "import urllib.request; r=urllib.request.urlopen(chr(104)+chr(116)+chr(116)+chr(112)+chr(58)+chr(47)+chr(47)+chr(49)+chr(50)+chr(55)+chr(46)+chr(48)+chr(46)+chr(48)+chr(46)+chr(49)+chr(58)+chr(56)+chr(48)+chr(48)+chr(49)+chr(47)+chr(97)+chr(112)+chr(105)+chr(47)+chr(114)+chr(101)+chr(118)+chr(101)+chr(114)+chr(115)+chr(97)+chr(108)+chr(115),timeout=5); print(r.status, r.read().decode()[:200])" 2>/dev/null'))

# Check via nginx to see if it's a routing issue
print("\n=== VIA NGINX ===")
print(run("curl -sk 'https://deltacapitals.systems/api/reversals' -v 2>&1 | tail -20"))

# Check the catch-all route ordering
print("\n=== ROUTE ORDER CHECK ===")
print(run('docker exec mcx-live python3 -c "import sys; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from live.api import app; [print(type(r).__name__, getattr(r,chr(112)+chr(97)+chr(116)+chr(104),chr(63))) for r in app.routes[-10:]]" 2>/dev/null'))

# Check if reversals is in the OpenAPI routes
print("\n=== OPENAPI ROUTES CONTAINING reversal ===")
print(run('docker exec mcx-live python3 -c "import sys,json; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from live.api import app; spec=app.openapi(); routes=[p for p in spec.get(chr(112)+chr(97)+chr(116)+chr(104)+chr(115),{}).keys() if chr(114)+chr(101)+chr(118) in p]; print(chr(10).join(routes))" 2>/dev/null'))

ssh.close()
