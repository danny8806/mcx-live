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
def run(cmd, t=20):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

print("=== health ===")
print(run("curl -s http://127.0.0.1:8001/api/health 2>/dev/null"))
print("=== container status ===")
print(run('docker ps --filter name=mcx-live --format "{{.Status}}"'))
print("=== engine log tail ===")
print(run("docker logs mcx-live --tail 15 2>&1 | tail -15"))
ssh.close()