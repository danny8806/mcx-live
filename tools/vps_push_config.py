"""Push config to VPS and verify gates are OFF."""
import paramiko, json

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
    try:
        _, o, e = ssh.exec_command(cmd, timeout=timeout)
        return o.read().decode("utf-8", "replace").strip()
    except Exception as ex:
        return f"ERROR: {ex}"

# 1. Upload config file
print("UPLOADING config/live_settings.json to VPS...")
sftp = ssh.open_sftp()
sftp.put("config/live_settings.json", "/tmp/live_settings.json")
sftp.close()
print("  uploaded OK")

# 2. Copy into container
print("COPYING into mcx-live container...")
print(run("docker cp /tmp/live_settings.json mcx-live:/app/config/live_settings.json"))
print("  copied OK")

# 3. Verify gates on VPS
print("\nVERIFYING GATES:")
gates_out = run('docker exec mcx-live python3 /app/tools/check_gates.py 2>/dev/null || docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); s=c.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115),{}); print(chr(76)+chr(73)+chr(86)+chr(69)+chr(95)+chr(84)+chr(82)+chr(65)+chr(68)+chr(73)+chr(78)+chr(71)+chr(95)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(108)+chr(98)+chr(108)+chr(101)+chr(100))); print(chr(71)+chr(65)+chr(84)+chr(69)+chr(58), l.get(chr(103)+chr(97)+chr(116)+chr(101))); print(chr(66)+chr(82)+chr(79)+chr(75)+chr(69)+chr(82)+chr(95)+chr(83)+chr(76)+chr(46)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), l.get(chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(115)+chr(108),{}).get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))); [print(k, v.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)), v.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(103)+chr(97)+chr(116)+chr(101))) for k,v in s.items()]"')
print(gates_out)

# 4. Check if gates are OFF
if "False" in gates_out and "OFF" in gates_out and "True" not in gates_out:
    print("\n*** ALL GATES CONFIRMED OFF ON VPS ***")
else:
    print("\n*** WARNING: SOME GATES MAY STILL BE ON ***")

# 5. Check frontend
print("\nFRONTEND DIST:")
print(run("docker exec mcx-live ls -la /app/dashboard-ui/dist/"))
print(run("docker exec mcx-live head -c 500 /app/dashboard-ui/dist/index.html"))

# 6. CORS
print("\nCORS ENV:")
print(run("docker exec mcx-live printenv CORS_ORIGINS 2>/dev/null || echo 'NOT SET'"))

# 7. Health
print("\nAPI HEALTH:")
print(run("curl -s http://localhost:8001/api/health"))

ssh.close()
