import paramiko, sys, io, time, json
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
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
script = r"""
import sqlite3, json
con = sqlite3.connect("/app/live/data/db/live_trading.db")
rows = con.execute("SELECT event_type, strategy_id, COUNT(*) FROM events WHERE rowid > ? GROUP BY event_type, strategy_id", (int(OPEN("__lastid__","r").read().strip() or 0),)).fetchall() if False else []
# simpler: report max id
print(json.dumps({"max_event_id": con.execute("SELECT MAX(rowid) FROM events").fetchone()[0]}))
"""
lastid = 0
print("FIRE-WATCH4 (gates-ON verification) START", time.strftime("%H:%M:%S"))
for i in range(90):
    t = time.strftime("%H:%M:%S")
    try:
        sig = run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null")
        sigj = json.loads(sig) if sig else {}
        ords = run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null")
        ordj = json.loads(ords) if ords else {}
        pos = run("curl -s http://127.0.0.1:8001/api/live/positions 2>/dev/null")
        posj = json.loads(pos) if pos else {}
        ns = len(sigj.get("signals") or []); no = len(ordj.get("orders") or []); np_ = posj.get("count", 0)
        evid = run("docker exec mcx-live python3 -c \x22import sqlite3,json; print(json.dumps({'max': int(sqlite3.connect('/app/live/data/db/live_trading.db').execute('SELECT COALESCE(MAX(rowid),0) FROM events').fetchone()[0])}))\x22")
        evj = json.loads(evid) if evid else {}
        m = evj.get("max", lastid)
        if m > lastid:
            print(f"[{t}] *** NEW EVENTS (id range {lastid+1}..{m}) — checking...")
            new = run("docker exec mcx-live python3 -c \x22import sqlite3,json; con=sqlite3.connect('/app/live/data/db/live_trading.db'); print(json.dumps([dict(zip(['id','timestamp','event_type','strategy_id'], r)) for r in con.execute('SELECT id,timestamp,event_type,strategy_id FROM events WHERE id>%d ORDER BY id' % %d)]))\" % (%lastid%... )" )
        lastid = max(lastid, m)
        md = run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null")
        mdj = json.loads(md) if md else {}
        ltp = {k: round((v or {}).get('ltp',0),1) for k,v in mdj.get('instruments',{}).items()} if isinstance(mdj.get('instruments'), dict) else {}
        print(f"[{t}] ltp={ltp} sig={ns} ord={no} pos={np_} events_max={m}", flush=True)
        if ns+no+np_ > 0:
            print(f"*** TRADE ACTION DETECTED. sig={json.dumps(sigj)[:800]} ord={json.dumps(ordj)[:800]} pos={json.dumps(posj)[:800]}")
            break
    except Exception as ex:
        print(f"[{t}] poll error: {ex}")
    time.sleep(20)
print("FIRE-WATCH4 END", time.strftime("%H:%M:%S"))
ssh.close()
