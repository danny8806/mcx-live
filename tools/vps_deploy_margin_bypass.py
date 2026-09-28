"""Deploy the allow_broker_margin_reject change: sync trading_engine.py +
live_settings.json, restart the container, verify health + config."""
import paramiko
import time

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, t=60):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

print("=== syncing files ===")
sftp = ssh.open_sftp()
sftp.put("trading_engine.py", "/tmp/trading_engine.py")
sftp.put("config/live_settings.json", "/tmp/live_settings.json")
sftp.close()
print(run("docker cp /tmp/trading_engine.py mcx-live:/app/trading_engine.py"))
print(run("docker cp /tmp/live_settings.json mcx-live:/app/config/live_settings.json"))

print("=== restarting ===")
print(run("docker restart mcx-live", t=120))

print("=== waiting for health ===")
for i in range(20):
    time.sleep(3)
    h = run("curl -s http://127.0.0.1:8001/api/health 2>/dev/null")
    if "engine\":true" in h.replace(" ", ""):
        print(f"  [{i*3}s] READY")
        print(h[:200])
        break
    print(f"  [{i*3}s] waiting...")
else:
    print("  NOT HEALTHY after 60s")

print("=== risk config in container ===")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); print(c.get(chr(114)+chr(105)+chr(115)+chr(107), {}).get(chr(97)+chr(108)+chr(108)+chr(111)+chr(119)+chr(95)+chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(109)+chr(97)+chr(114)+chr(103)+chr(105)+chr(110)+chr(95)+chr(114)+chr(101)+chr(106)+chr(101)+chr(99)+chr(116)))"'))

print("=== trading_engine.py contains bypass ===")
print(run("docker exec mcx-live grep -c 'allow_broker_margin_reject' /app/trading_engine.py"))

print("=== state restore check ===")
print(run("docker logs mcx-live --since 3m 2>&1 | grep -iE 'engine_restored|engine_started|restored|gate' | tail -10"))
ssh.close()