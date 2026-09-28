"""Check container code content - encoding safe."""
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

print("=== CONTAINER price_model.py - grep key terms ===")
print(run('docker exec mcx-live grep -n "limit_first" /app/execution/price_model.py 2>/dev/null'))
print(run('docker exec mcx-live grep -n "plan_for" /app/execution/price_model.py 2>/dev/null'))
print(run('docker exec mcx-live grep -n "MARKET_FALLBACK" /app/execution/price_model.py 2>/dev/null'))
print(run('docker exec mcx-live grep -n "STOP_LIMIT" /app/execution/price_model.py 2>/dev/null'))

print("\n=== CONTAINER price_model.py first 100 lines ===")
out = run("docker exec mcx-live head -100 /app/execution/price_model.py 2>/dev/null")
print(out)

print("\n=== LOCAL price_model.py first 100 lines ===")
with open("execution/price_model.py", encoding="utf-8") as f:
    lines = f.readlines()[:100]
    for i, line in enumerate(lines, 1):
        safe = line.encode("ascii", "replace").decode("ascii").rstrip()
        print(f"{i}: {safe}")

print("\n=== live_settings.json ===")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); print(json.dumps(c,indent=2))" 2>/dev/null'))

ssh.close()
