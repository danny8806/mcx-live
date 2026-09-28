import paramiko, io, sys
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
_, o, e = ssh.exec_command(
    "docker exec mcx-live python3 -c \"import json; d = json.load(open('/app/live/data/db/live_system_state.json')); "
    "[print(k, '| state=', d['strategies'][k]['state'], '| bars=', d['strategies'][k]['bars_processed'], "
    "'| sg=', d['strategies'][k]['signals_generated'], '| fast_count=', d['strategies'][k]['fast_indicator_count'], "
    "'| slow_htf=', round(d['strategies'][k]['slow_htf_value'], 2), '| prev_htf=', round(d['strategies'][k]['prev_htf_value'], 2), "
    "'| mid=', round(d['strategies'][k]['mid_htf_value'], 2), '| prev_mid=', round(d['strategies'][k]['prev_mid_value'], 2), "
    "'| prev_fast_close=', d['strategies'][k]['prev_fast_close'], "
    "'| pending=', d['strategies'][k]['pending_entry'], '| last_armed=', d['strategies'][k]['last_armed_pending_id']) "
    "for k in ['gold_02','silver_01']]\"",
    timeout=30,
)
try:
    out = o.read().decode("utf-8", "replace").strip()
except Exception:
    out = ""
print(out)
print("=== DEMA values visible in snapshot node? raw keys ===")
_, o2, _ = ssh.exec_command(
    "docker exec mcx-live python3 -c \"import json; d = json.load(open('/app/live/data/db/live_system_state.json')); "
    "print(list(d['strategies']['gold_02'].keys()))\"",
    timeout=30,
)
print(o2.read().decode("utf-8", "replace").strip())
ssh.close()