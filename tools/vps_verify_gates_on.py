import paramiko, sys, io, json
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
script = r'''
import json
st = json.load(open("/app/live/data/db/live_system_state.json"))
print("strategy_gates:")
print(json.dumps(st.get("strategy_gates"), indent=1))
'''
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
sf = ssh.open_sftp()
with sf.open("/tmp/dbg4.py", "w") as f:
    f.write(script)
sf.close()
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print(run("docker cp /tmp/dbg4.py mcx-live:/tmp/dbg4.py && docker exec mcx-live python3 /tmp/dbg4.py"))
print("--- new events since start ---")
print(run("docker logs mcx-live --since 2m 2>&1 | grep -iE 'strategy_control|gate|signal|error' | grep -viE 'HTTP/1.1' | tail -8"))
ssh.close()
