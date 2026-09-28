import paramiko, sys, io
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
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
print("=== DB tables + counts ===")
print(run(r"""docker exec mcx-live python3 - <<'PY'
import sqlite3
con = sqlite3.connect('/app/live/data/db/live_trading.db')
cur = con.cursor()
tabs = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print('tables:', tabs)
for t in tabs:
    try:
        n = cur.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
        print(f'  {t}: {n}')
    except Exception as e:
        print(f'  {t}: err {e}')
con.close()
PY"""))
print("=== model snapshot / state ===")
print(run(r"""docker exec mcx-live python3 - <<'PY'
import json
st = json.load(open('/app/live/data/db/live_system_state.json'))
s = json.dumps(st, indent=1, default=str)
print(s[:1500])
PY"""))
ssh.close()
