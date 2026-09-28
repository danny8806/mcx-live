"""Write the nginx config fix directly."""
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
    return o.read().decode("utf-8", "replace").strip()

# Read current content
current = run("cat /etc/nginx/sites-available/deltacapitals.systems")
print(f"Current length: {len(current)}")
print(f"Has reversals: {'reversals' in current}")

# Write the fix using SFTP
sftp = ssh.open_sftp()
with sftp.open("/etc/nginx/sites-available/deltacapitals.systems", "r") as f:
    content = f.read().decode("utf-8")

old = "overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health"
new = "overview|strategies|positions|orders|trades|pnl|market-data|risk|reconciliation|alerts|settings|audit|indicators|htf|envs|broker-events|alert-ledger|equity-curve|fills|health|reversals"

if "reversals" in content:
    print("Already has reversals!")
else:
    content = content.replace(old, new)
    with sftp.open("/etc/nginx/sites-available/deltacapitals.systems", "w") as f:
        f.write(content.encode("utf-8"))
    print("Written fix!")

sftp.close()

# Verify
print(f"\nHas reversals now: {'reversals' in run('cat /etc/nginx/sites-available/deltacapitals.systems')}")

# Test and reload
print(run("nginx -t 2>&1"))
print(run("systemctl reload nginx 2>&1"))

# Test route
import time; time.sleep(1)
code = run("curl -sk -o /dev/null -w '%{http_code}' 'https://deltacapitals.systems/api/reversals'")
print(f"\n/api/reversals -> {code}")
body = run("curl -sk 'https://deltacapitals.systems/api/reversals' 2>/dev/null | head -c 150")
print(f"Body: {body}")

ssh.close()
