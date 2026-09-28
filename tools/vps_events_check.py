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
print("=== events ===")
for r in cur.execute("SELECT * FROM events ORDER BY rowid DESC LIMIT 15").fetchall():
    print(r)
print("=== alert_events ===")
for r in cur.execute("SELECT * FROM alert_events ORDER BY rowid DESC LIMIT 10").fetchall():
    print(r)
print("=== account_snapshots last 3 ===")
for r in cur.execute("SELECT * FROM account_snapshots ORDER BY rowid DESC LIMIT 3").fetchall():
    print(r)
con.close()
'''
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
sf = ssh.open_sftp()
with sf.open("/tmp/dbg2.py", "w") as f:
    f.write(script)
sf.close()
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print(run("docker cp /tmp/dbg2.py mcx-live:/tmp/dbg2.py && docker exec mcx-live python3 /tmp/dbg2.py"))
ssh.close()
