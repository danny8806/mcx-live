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
print("=== live API port 8001 - health/snapshot ===")
print(run("curl -s http://127.0.0.1:8001/api/live/health 2>&1 | head -c 400"))
print("")
print(run("""curl -s http://127.0.0.1:8001/api/live/snapshot 2>/dev/null | python3 -c "
import sys, json
raw = sys.stdin.read()
try:
    d = json.loads(raw)
except Exception as e:
    print('parse error:', e, '| raw head:', raw[:200]); sys.exit(1)
st = d.get('strategies') or {}
print('strategy entries:', len(st) if isinstance(st, dict) else len(st) if isinstance(st, list) else '?')
if isinstance(st, dict):
    for sid, s in st.items():
        print(sid, '| lots=', (s.get('config') or {}).get('lots') if isinstance(s.get('config'), dict) else s.get('lots'), '| qty=', s.get('quantity'))
elif isinstance(st, list):
    for s in st:
        print(s.get('strategy_id'), '| lots=', s.get('lots'), '| qty=', s.get('quantity'))
else:
    print(json.dumps(st)[:600])
" 2>&1"""))
ssh.close()
