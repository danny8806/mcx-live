"""Fix nginx: add reversals to the explicit API route regex."""
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

# Fix: add reversals to the regex
old_line = "location ~ ^/api/(overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health) {"
new_line = "location ~ ^/api/(overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health|reversals) {"

# Use sed to replace
result = run(f"sed -i 's|{old_line}|{new_line}|' /etc/nginx/sites-available/deltacapitals.systems")
print(f"sed result: {result}")

# Verify the change
print("\n=== VERIFY ===")
print(run("grep 'reversals' /etc/nginx/sites-available/deltacapitals.systems"))

# Test nginx config
print("\n=== NGINX TEST ===")
print(run("nginx -t"))

# Reload nginx
print("\n=== RELOAD ===")
print(run("systemctl reload nginx"))

# Test the route
import time
time.sleep(1)
print("\n=== TEST /api/reversals ===")
print(run("curl -sk -o /dev/null -w '%{http_code}' 'https://deltacapitals.systems/api/reversals'"))
print()
print(run("curl -sk 'https://deltacapitals.systems/api/reversals' | head -c 100"))

ssh.close()
