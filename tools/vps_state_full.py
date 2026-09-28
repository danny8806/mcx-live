import paramiko, sys, io, json
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
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
print("=== FULL strategy snapshot section ===")
print(run("""docker exec mcx-live python3 -c "
import json
d = json.load(open('/app/live/data/db/live_system_state.json'))
print(json.dumps(d['strategies']['gold_02'], indent=1, default=str)[:2500])
" 2>&1"""))
print("=== 1h native closes logged? last 3h ===")
print(run("docker logs mcx-live --since 3h 2>&1 | grep -iE '1h native|1H native|GOLDM 1h|SILVERM 1h' | tail -8"))
ssh.close()
