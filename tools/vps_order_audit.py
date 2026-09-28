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
def run(cmd, t=30):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

script = r'''
import sqlite3, json
c = sqlite3.connect("/app/live/data/db/live_trading.db")

print("=== ALL ORDERS (orders table) ===")
cols = [d[1] for d in c.execute("PRAGMA table_info(orders)").description]
rows = c.execute("SELECT * FROM orders ORDER BY created_at").fetchall()
for r in rows:
    d = dict(zip(cols, r))
    print("order_id=%s broker=%s strat=%s inst=%s side=%s qty=%s type=%s price=%s state=%s reason=%r created=%s" % (
        d.get("order_id"), d.get("broker_order_id"), d.get("strategy_id"),
        d.get("instrument"), d.get("side"), d.get("quantity"),
        d.get("order_type"), d.get("price"), d.get("status") or d.get("state"),
        str(d.get("reason"))[:110], d.get("created_at")))

print()
print("=== broker_api_events: every PLACE_* POST /orders ===")
rows = c.execute("SELECT request_timestamp,action,endpoint,http_status,broker_order_id,error_code,error_message,request_payload,response_payload,created_at FROM broker_api_events WHERE action LIKE 'PLACE%' ORDER BY request_timestamp").fetchall()
for r in rows:
    ts, action, ep, status, boid, ecode, emsg, req, resp, cat = r
    reqd = {}
    respd = {}
    try:
        reqd = json.loads(req) if req and req != "null" else {}
    except Exception:
        pass
    try:
        respd = json.loads(resp) if resp and resp != "null" else {}
    except Exception:
        pass
    print("ts=%s action=%s http=%s broker_id=%s err=%s:%s | req{qty=%s price=%s type=%s side=%s sec=%s} resp=%s" % (
        cat, action, status, boid, ecode, str(emsg)[:60],
        reqd.get("quantity"), reqd.get("price"), reqd.get("orderType"),
        reqd.get("transactionType"), reqd.get("securityId"),
        json.dumps(respd)[:150]))

print()
print("=== recent ORDER_STATUS polls with terminal statuses (last 12 per broker id) ===")
rows = c.execute("SELECT created_at,action,broker_order_id,response_payload,error_message FROM broker_api_events WHERE action='ORDER_STATUS' AND broker_order_id != '' ORDER BY request_timestamp DESC LIMIT 20").fetchall()
seen = set()
for r in rows[:20]:
    k = (r[2], r[4])
    if k in seen:
        continue
    seen.add(k)
    print("ts=%s boid=%s msg=%r resp=%s" % (r[0], r[2], str(r[4])[:60], str(r[3])[:120]))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/order_audit.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/order_audit.py mcx-live:/tmp/order_audit.py && docker exec mcx-live python3 /tmp/order_audit.py"))
ssh.close()