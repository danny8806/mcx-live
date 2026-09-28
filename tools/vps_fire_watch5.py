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
def run(cmd, timeout=25):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
def snap_counters():
    raw = run("""docker exec mcx-live python3 -c "
import json
d = json.load(open('/app/live/data/db/live_system_state.json'))
print(json.dumps({s: {'sg': v.get('signals_generated'), 'bars': v.get('bars_processed')} for s, v in d.get('strategies', {}).items() if v.get('enabled')}))
" 2>&1""")
    try:
        return json.loads(raw)
    except Exception:
        return None
baseline = None
seen_ids = set()
print("FIRE-WATCH5 START", time.strftime("%H:%M:%S"))
try:
    init = run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null")
    initj = json.loads(init) if init else {}
    for s in (initj.get('signals') or []):
        if s.get('signal_id'):
            seen_ids.add(s.get('signal_id'))
    if initj.get('signals'):
        print(f"  known pre-existing signals: {[s.get('signal_id') for s in initj['signals']]}")
    inito = run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null")
    initoj = json.loads(inito) if inito else {}
    for o in (initoj.get('orders') or []):
        seen_ids.add(str(o.get('order_id') or o.get('id') or o))
    if initoj.get('orders'):
        print(f"  known pre-existing orders: {[o.get('order_id') for o in initoj['orders']]}")
except Exception as ex:
    print(f"  init poll ignored: {ex}")
for i in range(90):
    t = time.strftime("%H:%M:%S")
    try:
        c = snap_counters()
        if c:
            if baseline is None:
                baseline = c
            for sid in c:
                if c[sid]['sg'] != baseline[sid]['sg']:
                    print(f"[{t}] *** SIGNALS GENERATED delta {sid}: {baseline[sid]['sg']} -> {c[sid]['sg']}")
                    baseline = c
        md = run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null")
        mdj = json.loads(md) if md else {}
        ltp = {k: round((v or {}).get('ltp',0),1) for k,v in mdj.get('instruments',{}).items()} if isinstance(mdj.get('instruments'), dict) else {}
        sig = run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null")
        sigj = json.loads(sig) if sig else {}
        ords = run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null")
        ordj = json.loads(ords) if ords else {}
        pos = run("curl -s http://127.0.0.1:8001/api/live/positions 2>/dev/null")
        posj = json.loads(pos) if pos else {}
        ns = len(sigj.get('signals') or []); no = len(ordj.get('orders') or []); np_ = posj.get('count', 0)
        new_ids = set()
        for s in (sigj.get('signals') or []):
            sid = s.get('signal_id')
            if sid and sid not in seen_ids:
                new_ids.add(sid)
        for o in (ordj.get('orders') or []):
            oid = str(o.get('order_id') or o.get('id') or o)
            if oid and oid not in seen_ids:
                new_ids.add(oid)
        if new_ids:
            print(f"[{t}] *** NEW TRADE ACTION: sig={json.dumps(sigj)[:600]} ord={json.dumps(ordj)[:600]} pos={json.dumps(posj)[:600]}", flush=True)
            break
        elif ns > 0:
            print(f"[{t}] ltp={ltp} sig={ns}(stale) ord={no} pos={np_} counters={json.dumps(c)}", flush=True)
        else:
            print(f"[{t}] ltp={ltp} sig={ns} ord={no} pos={np_} counters={json.dumps(c)}", flush=True)
    except Exception as ex:
        print(f"[{t}] poll error: {ex}")
    time.sleep(20)
print("=== recent 15m/1h candle closes & any signal lines ===")
print(run("docker logs mcx-live --since 2h 2>&1 | grep -iE '15m native closed|1h native closed' | tail -12"))
print("=== NEW event rows since 14:25 ===")
script = r"""
import sqlite3, json
con = sqlite3.connect('/app/live/data/db/live_trading.db')
rows = con.execute("SELECT id,timestamp,event_type,strategy_id,details FROM events WHERE id>17 ORDER BY id").fetchall()
for r in rows:
    print(r[0], r[1], r[2], r[3], str(r[4])[:120])
"""
sf = ssh.open_sftp()
with sf.open("/tmp/ev3.py","w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/ev3.py mcx-live:/tmp/ev3.py && docker exec mcx-live python3 /tmp/ev3.py"))
print("FIRE-WATCH5 END", time.strftime("%H:%M:%S"))
ssh.close()
