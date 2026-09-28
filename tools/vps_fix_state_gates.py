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
p = "/app/live/data/db/live_system_state.json"
st = json.load(open(p))
gates = st.setdefault("strategy_gates", {})
for sid in ("gold_02", "silver_01"):
    g = gates.setdefault(sid, {})
    g["live_gate"] = "ON"
    g["entry_enabled"] = True
    g["exit_enabled"] = True
    g["reversal_enabled"] = True
    g["sl_enabled"] = True
    g["close_only"] = False
json.dump(st, open(p, "w"), indent=1, default=str)
print("updated. strategy_gates now:")
print(json.dumps(st.get("strategy_gates"), indent=1))
'''
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
sf = ssh.open_sftp()
with sf.open("/tmp/fixgates.py", "w") as f:
    f.write(script)
sf.close()
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print(run("docker cp /tmp/fixgates.py mcx-live:/tmp/fixgates.py && docker exec mcx-live python3 /tmp/fixgates.py"))
ssh.close()
