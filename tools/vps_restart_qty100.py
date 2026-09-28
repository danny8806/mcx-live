import paramiko, sys, io, time
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
def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
print("restarting mcx-live...")
print(run("docker restart mcx-live", timeout=60))
time.sleep(12)
print("container status:")
print(run("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'"))
print("in-container config quantity:")
print(run("docker exec mcx-live python3 -c \"import json; c=json.load(open('/app/config/live_settings.json')); print(json.dumps({k:{'quantity':v.get('quantity'),'lots':v.get('lots')} for k,v in c['strategies'].items()}))\" 2>&1"))
print("container health:")
print(run("docker inspect mcx-live --format '{{.State.Health.Status}} restarts={{.RestartCount}}' 2>&1"))
ssh.close()
