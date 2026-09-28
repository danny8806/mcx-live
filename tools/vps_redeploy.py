"""Re-deploy overview.py fix into the new container."""
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

def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# 1. Copy overview.py into container
print("COPYING overview.py into container...")
sftp = ssh.open_sftp()
sftp.put("dashboard/routes/overview.py", "/tmp/overview.py")
sftp.close()
print(run("docker cp /tmp/overview.py mcx-live:/app/dashboard/routes/overview.py"))

# 2. Also copy the gate config
print("COPYING live_settings.json...")
sftp = ssh.open_sftp()
sftp.put("config/live_settings.json", "/tmp/live_settings.json")
sftp.close()
print(run("docker cp /tmp/live_settings.json mcx-live:/app/config/live_settings.json"))

# 3. Restart container
print("\nRESTARTING...")
print(run("docker restart mcx-live", timeout=60))

# 4. Wait for healthy
print("Waiting for healthy...")
for i in range(15):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    print(f"  [{i*3}s] {health}")
    if health == "healthy":
        break

# 5. Verify
print("\n=== VERIFICATION ===")
print("CORS:", run("docker exec mcx-live printenv CORS_ORIGINS"))
print("GATE:", run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); print(c.get(chr(108)+chr(105)+chr(118)+chr(101),{}).get(chr(103)+chr(97)+chr(116)+chr(101)))"'))

print("\n--- /api/overview ---")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('total_equity:', d.get('total_equity',{}).get('value')); print('starting_capital:', d.get('starting_capital',{}).get('value'))\""))

print("\n--- /api/health ---")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 200"))

print("\n--- FRONTEND ---")
print(run("curl -sk https://deltacapitals.systems/ 2>/dev/null | head -c 200"))

# CORS header check
print("\n--- CORS HEADER CHECK ---")
print(run("curl -sk -I -H 'Origin: https://deltacapitals.systems' https://deltacapitals.systems/api/overview 2>/dev/null | grep -i 'access-control'"))

ssh.close()
