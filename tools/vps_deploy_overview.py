"""Deploy single file to mcx-live container."""
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

# Upload overview.py to VPS
sftp = ssh.open_sftp()
sftp.put("dashboard/routes/overview.py", "/tmp/overview.py")
sftp.close()

# Copy into container
print(run("docker cp /tmp/overview.py mcx-live:/app/dashboard/routes/overview.py"))
print("deployed overview.py")

# Verify starting_capital fix
import time
time.sleep(1)
print("\n--- /api/overview starting_capital check ---")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('total_equity:', d.get('total_equity',{}).get('value')); print('starting_capital:', d.get('starting_capital',{}).get('value')); print('net_pnl:', d.get('total_net_pnl',{}).get('value'))\""))

ssh.close()
