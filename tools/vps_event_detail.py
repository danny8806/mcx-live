import paramiko, io, sys, base64
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

script = r'''
import sqlite3
c = sqlite3.connect("/app/live/data/db/live_trading.db")
print("=== events id>=17 ===")
rows = c.execute("SELECT id,timestamp,event_type,strategy_id,instrument,details,execution_mode FROM events WHERE id>=17 ORDER BY id").fetchall()
for r in rows:
    print(r[0], "|", r[1], "|", r[2], "|", r[3], "|", r[4], "|", r[5], "|", r[6])
print("=== orders ===")
try:
    rows = c.execute("SELECT * FROM orders ORDER BY id DESC LIMIT 5").fetchall()
    for r in rows:
        print(r)
except Exception as ex:
    print("orders err", ex)
print("=== signals ===")
try:
    cols = [d[0] for d in c.execute("SELECT * FROM signals LIMIT 0").description]
    print("signal cols:", cols)
    rows = c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 5").fetchall()
    for r in rows:
        print(r)
except Exception as ex:
    print("signals err", ex)
'''
b64 = base64.b64encode(script.encode()).decode()
_, o, e = ssh.exec_command(
    "docker exec mcx-live python3 -c \"import base64; exec(base64.b64decode('{}'))\"".format(b64),
    timeout=30,
)
try:
    print(o.read().decode("utf-8", "replace").strip())
except Exception as ex:
    print("out err", ex)
try:
    err = e.read().decode("utf-8", "replace").strip()
    if err:
        print("STDERR:", err)
except Exception as ex:
    print("err", ex)
ssh.close()