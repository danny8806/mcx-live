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

print("=== signals (recent) ===")
cols = [d[1] for d in c.execute("PRAGMA table_info(signals)").description]
print("cols:", cols)
for r in c.execute("SELECT * FROM signals ORDER BY id DESC LIMIT 4"):
    print(r)

print()
print("=== orders detail ===")
cols = [d[1] for d in c.execute("PRAGMA table_info(orders)").description]
print("cols:", cols)
for r in c.execute("SELECT * FROM orders ORDER BY rowid DESC LIMIT 2"):
    d = dict(zip(cols, r))
    print({k: d[k] for k in cols})

print()
print("=== broker_api_events place/order POST (request_timestamp 1790267416ish) ===")
rows = c.execute("SELECT id,action,endpoint,http_method,request_timestamp,response_timestamp,http_status,request_payload,response_payload,error_message,broker_order_id,execution_mode,created_at FROM broker_api_events WHERE (action LIKE 'PLACE%' OR action LIKE 'ORDER%' OR endpoint LIKE '%/orders%') AND request_timestamp BETWEEN 1790267408 AND 1790267440 ORDER BY request_timestamp").fetchall()
for r in rows:
    print(r)
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