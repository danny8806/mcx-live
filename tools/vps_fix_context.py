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
sf = ssh.open_sftp()
# 1) sync new .dockerignore
sf.put(".dockerignore", "/home/jadhavdnyaneshwar701/mcx-trader-live/.dockerignore")
print("-> .dockerignore synced")
# 2) patch build-context state file gates to ON (belt & suspenders)
script = r'''
import json
p = "/home/jadhavdnyaneshwar701/mcx-trader-live/live/data/db/live_system_state.json"
try:
    st = json.load(open(p))
except Exception as e:
    print("no context state:", e)
    raise SystemExit
for sid in ("gold_02", "silver_01"):
    g = st.setdefault("strategy_gates", {}).setdefault(sid, {})
    g["live_gate"] = "ON"; g["entry_enabled"] = True
    g["exit_enabled"] = True; g["reversal_enabled"] = True
    g["sl_enabled"] = True; g["close_only"] = False
json.dump(st, open(p, "w"), indent=1, default=str)
print("context state gates patched ->", json.dumps(st["strategy_gates"]["gold_02"]))
'''
s = ssh.open_sftp()
with s.open("/tmp/patch_ctx.py", "w") as f:
    f.write(script)
s.close()
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print(run("python3 /tmp/patch_ctx.py"))
# 3) confirm context gates now
print(run("cat /home/jadhavdnyaneshwar701/mcx-trader-live/live/data/db/live_system_state.json | python3 -c \"import sys,json; d=json.load(sys.stdin); print('g02:', json.dumps(d['strategy_gates']['gold_02'])); print('s01:', json.dumps(d['strategy_gates']['silver_01']))\""))
ssh.close()
