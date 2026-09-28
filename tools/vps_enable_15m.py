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
sftp = ssh.open_sftp()
sftp.put("config/live_settings.json", "/home/jadhavdnyaneshwar701/mcx-trader-live/config/live_settings.json")
sftp.close()
print("-> VPS build context: done")
run("docker cp /home/jadhavdnyaneshwar701/mcx-trader-live/config/live_settings.json mcx-live:/app/config/live_settings.json")
print("-> running container: done")
print("restarting...")
print(run("docker restart mcx-live", timeout=60))
time.sleep(14)
print("status:", run("docker ps --filter name=mcx-live --format '{{.Status}}'"), "|", run("docker inspect mcx-live --format 'health={{.State.Health.Status}} restarts={{.RestartCount}}'"))
print("running engine strategies count (health):")
print(run("curl -s http://127.0.0.1:8001/api/live/health 2>&1 | python3 -c \"import sys,json; d=json.load(sys.stdin); print('strategies=', d.get('strategies'), 'gate=', d.get('gate_enabled'), 'broker=', d.get('broker'))\" 2>&1"))
print("strategy list from /api/strategies:")
print(run("curl -s http://127.0.0.1:8001/api/strategies 2>/dev/null | head -c 600"))
ssh.close()
