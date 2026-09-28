import paramiko, io, sys
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
def run(cmd, t=60):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

probe = r'''
import sqlite3, json
c = sqlite3.connect("/app/live/data/db/live_trading.db")
print("=== ALL broker_api_events (action, ts, http, broker_id, err) ===")
for r in c.execute("SELECT created_at, action, endpoint, http_status, broker_order_id, error_code, substring(error_message,1,60) FROM broker_api_events ORDER BY request_timestamp").fetchall():
    print(r)
print()
print("=== PLACE_* action breakdown ===")
for r in c.execute("SELECT action, count(*) FROM broker_api_events GROUP BY action").fetchall():
    print(r)
print()
print("=== per-order timing: signal_created -> order_created -> broker place (Sep 25 orders) ===")
rows = c.execute("""
SELECT e.strategy_id, e.created_at, o.order_id, o.created_at, b.created_at
  FROM (SELECT strategy_id, signal_id, MIN(created_at) as created_at FROM events
        WHERE event_type='signal_created' GROUP BY strategy_id, signal_id) e
  LEFT JOIN orders o ON o.signal_id = e.signal_id
  LEFT JOIN (SELECT correlation_id, MIN(created_at) as created_at FROM broker_api_events
             WHERE action LIKE 'PLACE%' GROUP BY correlation_id) b
    ON b.correlation_id = o.correlation_id
 WHERE o.created_at >= '2026-09-25'
""").fetchall()
cols = ["strategy", "signal_created_at", "order_id", "order_created_at", "broker_place_at"]
for r in rows:
    print(dict(zip(cols, r)))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/perf_probe.py", "w") as f:
    f.write(probe)
sf.close()
print(run("docker cp /tmp/perf_probe.py mcx-live:/tmp/perf_probe.py && docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); exec(open('/tmp/perf_probe.py').read())\"", 60))
ssh.close()