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
print("=== ports in container ===")
print(run("docker exec mcx-live sh -c 'env | grep -i port; ss -tlnp 2>/dev/null | head -20' 2>&1"))
print("=== live_ops routes (quantity source) ===")
print(run("docker exec mcx-live sh -c 'grep -rn \"lots\" /app/dashboard/routes/live_ops.py | head -10' 2>&1"))
print("=== API entry config ===")
print(run("docker exec mcx-live sh -c 'grep -rn \"8000\\|8082\\|uvicorn\\|run(\" /app/live/run.py /app/live/main.py | head -20' 2>&1"))
ssh.close()
