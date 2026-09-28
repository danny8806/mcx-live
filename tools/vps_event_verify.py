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
for et in ("strategy_gate_blocked", "strategy_control", "strategy_gate_changed"):
    rows = con.execute("SELECT rowid, timestamp, event_type, strategy_id, detail FROM events WHERE event_type=? ORDER BY rowid DESC LIMIT 8", (et,)).fetchall()
    print("##", et)
    for r in rows:
        print("  ", r[0], r[1], r[3], str(r[4])[:110])
con.close()
'''
sf = ssh.open_sftp()
with sf.open("/tmp/ev.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/ev.py mcx-live:/tmp/ev.py && docker exec mcx-live python3 /tmp/ev.py"))
print("=== container logs recent signal lines ===")
print(run("docker logs mcx-live --since 15m 2>&1 | grep -iE 'signal|SHORT|LONG|gate|entry|order' | grep -viE 'HTTP/1.1|DEDUP' | tail -15"))
ssh.close()
