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
script = r'''
import json
d = json.load(open("/app/live/data/db/live_system_state.json"))
for sid, s in d.get("strategies", {}).items():
    if not s.get("enabled"):
        continue
    fast = s.get("fast_indicator_count")
    mid = s.get("mid_indicator_count") or s.get("mid_htf_state", {}).get("indicator_count")
    slow = s.get("slow_indicator_count") or s.get("slow_htf_state", {}).get("indicator_count")
    print(sid, "state=", s.get("state"), "bars=", s.get("bars_processed"),
          "fast_count=", fast, "mid_count=", mid, "slow_count=", slow,
          "htf_val=", s.get("htf_value"), "prev_htf=", s.get("prev_htf_value"))
    print("   htf_state keys:", {k: v for k, v in s.get("slow_htf_state", {}).items() if k != "aggregator"})
'''
sf = ssh.open_sftp()
with sf.open("/tmp/probe.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/probe.py mcx-live:/tmp/probe.py && docker exec mcx-live python3 /tmp/probe.py"))
print("=== htf/indicator/eval log lines last 45m ===")
print(run("docker logs mcx-live --since 45m 2>&1 | grep -iE 'htf|indicator|warmup|evaluat|skip|cross' | grep -viE 'HTTP/1.1' | tail -15"))
ssh.close()
