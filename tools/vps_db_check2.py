import paramiko, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
script = r'''
import sqlite3, json
con = sqlite3.connect("/app/live/data/db/live_trading.db")
cur = con.cursor()
tabs = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
print("tables:", tabs)
for t in tabs:
    try:
        n = cur.execute("SELECT COUNT(*) FROM " + t).fetchone()[0]
        print("  ", t, ":", n)
    except Exception as e:
        print("  ", t, "err:", e)
con.close()
try:
    st = json.load(open("/app/live/data/db/live_system_state.json"))
    print("STATE:", json.dumps(st, indent=1, default=str)[:1200])
except Exception as e:
    print("state err:", e)
'''
sftp = paramiko.SSHClient()
sftp.set_missing_host_key_policy(paramiko.AutoAddPolicy())
sftp.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
def run(cmd, timeout=45):
    _, o, e = sftp.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
import paramiko as pk
sf = sftp.open_sftp()
with sf.open("/tmp/dbcheck.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/dbcheck.py mcx-live:/tmp/dbcheck.py && docker exec mcx-live python3 /tmp/dbcheck.py"))
sftp.close()
