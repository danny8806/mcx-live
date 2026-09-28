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
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
script = r'''
import sqlite3
con = sqlite3.connect("/app/live/data/db/live_trading.db")
cols = [r[1] for r in con.execute("PRAGMA table_info(events)").fetchall()]
print("events cols:", cols)
print("--- events recent ---")
rows = con.execute("SELECT rowid, timestamp, event_type, strategy_id FROM events ORDER BY rowid DESC LIMIT 12").fetchall()
for r in rows:
    print(r)
con.close()
'''
sf = ssh.open_sftp()
with sf.open("/tmp/ev2.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/ev2.py mcx-live:/tmp/ev2.py && docker exec mcx-live python3 /tmp/ev2.py"))
ssh.close()
