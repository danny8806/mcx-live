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
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print("=== what does the f31 IMAGE have at live/data/db? (run a throwaway container from image) ===")
print(run("docker run --rm mcx-trader-live:remedy-f31 ls -la /app/live/data/db/ 2>&1 | head -20"))
print("=== does the image have a baked live_system_state.json? ===")
print(run("docker run --rm mcx-trader-live:remedy-f31 cat /app/live/data/db/live_system_state.json 2>&1 | head -c 400"))
ssh.close()
