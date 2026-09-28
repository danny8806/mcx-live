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

print("=== events id>=26 ===")
for r in c.execute("SELECT id,timestamp,event_type,strategy_id,instrument,details,execution_mode FROM events WHERE id>=26 ORDER BY id"):
    print(r)

print("=== broker_api_events ORDER actions (last 15) ===")
try:
    rows = c.execute("SELECT id,action,endpoint,http_method,request_timestamp,http_status,error_type,error_code,error_message,quantity,price,trigger_price,transaction_type,product_type,security_id,execution_mode,created_at FROM broker_api_events WHERE action LIKE '%ORDER%' AND request_timestamp > 1790260000 ORDER BY request_timestamp DESC LIMIT 15").fetchall()
    for r in rows:
        print(r)
except Exception as ex:
    print("err", ex)

print("=== orders table (all) ===")
try:
    cols = [d[1] for d in c.execute("PRAGMA table_info(orders)").description]
    print("cols:", cols)
    for r in c.execute("SELECT * FROM orders ORDER BY rowid DESC LIMIT 10"):
        print(r)
except Exception as ex:
    print("err", ex)

print("=== signals (recent) ===")
try:
    rows = c.execute("SELECT signal_id,strategy_id,instrument,side,signal_type,close,high,timestamp,trigger_price,stop_price,quantity,created_at FROM signals WHERE id > 1 ORDER BY id").fetchall()
    for r in rows:
        print(r)
except Exception as ex:
    print("err", ex)
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