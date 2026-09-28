import paramiko, sys, io
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
print("keys:", list(st.keys()))
print("strategy_gates:", json.dumps(st.get("strategy_gates"), indent=1))
print("strategies full:", json.dumps(st.get("strategies"), indent=1)[:2000])
'''
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
sf = ssh.open_sftp()
with sf.open("/tmp/dbg3.py", "w") as f:
    f.write(script)
sf.close()
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print(run("docker cp /tmp/dbg3.py mcx-live:/tmp/dbg3.py && docker exec mcx-live python3 /tmp/dbg3.py"))
ssh.close()
