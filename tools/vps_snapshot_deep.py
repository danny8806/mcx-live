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
def run(cmd, t=30):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

script = r'''
import json
d = json.load(open("/app/live/data/db/live_system_state.json"))
print("TOP KEYS:", list(d.keys()))
st = d.get("strategies", {})
print("STRATEGIES:", list(st.keys()))
for k, v in st.items():
    print("---", k)
    print(json.dumps({kk: v.get(kk) for kk in ("state","bars_processed","signals_generated","fast_indicator_count","slow_htf_value","mid_htf_value","prev_fast_close","prev_htf_value","prev_mid_value","enabled","live_gate","close_only","has_pending","last_armed_pending_id")}, indent=1))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/deep_probe.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/deep_probe.py mcx-live:/tmp/deep_probe.py && docker exec mcx-live python3 /tmp/deep_probe.py"))
ssh.close()