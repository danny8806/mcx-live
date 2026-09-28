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
print("=== /api/strategies (running system) ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
rows = d if isinstance(d, list) else d.get('strategies') or d.get('items') or []
for s in rows:
    if isinstance(s, dict):
        print(s.get('strategy_id') or s.get('id'), '| instrument=', s.get('instrument'), '| lots=', s.get('lots'), '| qty=', s.get('quantity'))
else:
    print('count:', len(rows) if isinstance(rows,list) else rows)
" 2>&1"""))
print("")
print("=== /api/live/dashboard strategies section ===")
print(run("""curl -s http://127.0.0.1:8001/api/live/dashboard 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
st = d.get('strategies') or {}
if isinstance(st, dict):
    for sid, s in st.items():
        print(sid, '| lots=', (s.get('lots')), '| qty=', (s.get('quantity')), '| state=', s.get('state'))
elif isinstance(st, list):
    for s in st:
        print(s.get('strategy_id'), '| lots=', s.get('lots'), '| qty=', s.get('quantity'))
else:
    print('strategies type:', type(st).__name__)
" 2>&1"""))
ssh.close()
