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

seen_orders = set()
seen_signals = set()
fired = False
start = time.time()
DURATION = 1800
print("FIRE-WATCH START", time.strftime("%H:%M:%S"), "| window", DURATION, "s | until signal fires")
while time.time() - start < DURATION:
    t = time.strftime("%H:%M:%S")
    try:
        md = run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null")
        mdj = json.loads(md) if md else {}
        ltp = {k: (round(v.get('ltp',0),1) if isinstance(v, dict) else None) for k, v in mdj.get("instruments", {}).items()}
        sig = run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null")
        sigj = json.loads(sig) if sig else {}
        slist = sigj.get("signals") or []
        for s in slist:
            key = str(s.get("id") or s.get("timestamp") or json.dumps(s)[:40])
            if key not in seen_signals:
                print(f"[{t}] *** NEW SIGNAL: {json.dumps(s)[:300]}")
                seen_signals.add(key)
        ords = run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null")
        ordj = json.loads(ords) if ords else {}
        olist = ordj.get("orders") or []
        for o in olist:
            oid = str(o.get("order_id") or o.get("orderId") or json.dumps(o)[:40])
            if oid not in seen_orders:
                print(f"[{t}] *** NEW ORDER: {json.dumps(o)[:300]}")
                seen_orders.add(oid)
        pos = run("curl -s http://127.0.0.1:8001/api/live/positions 2>/dev/null")
        posj = json.loads(pos) if pos else {}
        npos = posj.get("count", 0)
        if len(slist) > 0 or len(olist) > 0 or npos > 0:
            fired = True
            print(f"[{t}] *** TRADE ACTION: sig={len(slist)} ord={len(olist)} pos={npos} ltp={ltp}")
            print("--- positions ---")
            print(json.dumps(posj)[:600])
            print("--- orders ---")
            print(json.dumps(ordj)[:600])
        else:
            print(f"[{t}] ltp={ltp} sig=0 ord=0 pos=0", flush=True)
    except Exception as ex:
        print(f"[{t}] poll error: {ex}")
    if fired:
        break
    time.sleep(20)
print("=== strategy state ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies | python3 -c "
import sys,json
d=json.load(sys.stdin)
for s in d.get('strategies',[]):
    print(s.get('strategy_id'),'|',s.get('instrument'),'| tf=',s.get('fast_timeframe'),'| qty=',s.get('quantity'),'| gate=',s.get('live_gate'),'| state=',s.get('state'),'| last_signal=',str(s.get('last_signal'))[:120])
" 2>&1"""))
print("=== log tail ===")
print(run("docker logs mcx-live --since 30m 2>&1 | grep -iE 'signal|order|entry|SL|exit|reject|margin|DH-906|auth|warmup' | grep -viE 'HTTP/1.1\" 200' | tail -20"))
print("FIRE-WATCH END", time.strftime("%H:%M:%S"), "| fired =", fired)
ssh.close()
