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

print("=== broker_api_events around signal time 12:15:07 UTC (t 1790251207.5) ===")
rows = c.execute("SELECT id,action,endpoint,http_method,request_timestamp,response_timestamp,http_status,error_type,error_code,error_message,quantity,price,trigger_price,transaction_type,product_type,security_id,execution_mode,created_at FROM broker_api_events WHERE request_timestamp BETWEEN 1790251205 AND 1790251215 ORDER BY request_timestamp").fetchall()
for r in rows:
    print(r)
print("=== any ORDER actions since 09:00 UTC ===")
rows = c.execute("SELECT id,action,endpoint,http_method,request_timestamp,http_status,error_message,quantity,price,security_id,execution_mode FROM broker_api_events WHERE action LIKE '%ORDER%' AND request_timestamp > 1790200000 ORDER BY request_timestamp DESC LIMIT 20").fetchall()
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