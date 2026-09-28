"""Restart mcx-live container and verify."""
import paramiko, time

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# 1. Upload overview.py to VPS build context
print("UPLOADING overview.py to build context...")
sftp = ssh.open_sftp()
sftp.put("dashboard/routes/overview.py", "/home/jadhavdnyaneshwar701/mcx-trader-live/dashboard/routes/overview.py")
sftp.close()
print("  uploaded to build context OK")

# 2. Copy into container
print("COPYING into container...")
print(run("docker cp dashboard/routes/overview.py mcx-live:/app/dashboard/routes/overview.py"))
# Fix: need to upload locally first
sftp = ssh.open_sftp()
sftp.put("dashboard/routes/overview.py", "/tmp/overview.py")
sftp.close()
print(run("docker cp /tmp/overview.py mcx-live:/app/dashboard/routes/overview.py"))

# 3. Restart container (docker restart preserves the writable layer)
print("\nRESTARTING mcx-live container...")
print(run("docker restart mcx-live", timeout=120))

# 4. Wait for startup
print("Waiting for container to start...")
for i in range(20):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    running = run("docker inspect mcx-live --format '{{.State.Running}}' 2>/dev/null")
    print(f"  [{i*3}s] running={running} health={health}")
    if running == "true" and health == "healthy":
        break

# 5. Verify
print("\n" + "=" * 60)
print("VERIFYING")
print("=" * 60)

print("\n--- API HEALTH ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 200"))

print("\n--- OVERVIEW (equity + starting_capital) ---")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('total_equity:', d.get('total_equity',{}).get('value')); print('starting_capital:', d.get('starting_capital',{}).get('value')); print('net_pnl:', d.get('total_net_pnl',{}).get('value'))\""))

print("\n--- GATES (must be OFF) ---")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); print(chr(103)+chr(97)+chr(116)+chr(101)+chr(58), l.get(chr(103)+chr(97)+chr(116)+chr(101)))"'))

print("\n--- FRONTEND ---")
print(run("curl -sk https://deltacapitals.systems/ 2>/dev/null | head -c 200"))

ssh.close()
