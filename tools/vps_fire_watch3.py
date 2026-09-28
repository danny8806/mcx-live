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
seen_orders = set(); seen_signals = set(); fired = False
start = time.time(); DURATION = 1800
print("FIRE-WATCH3 START", time.strftime("%H:%M:%S"), "| gates ON | until signal fires")
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
            print("pos:", json.dumps(posj)[:700])
            print("ord:", json.dumps(ordj)[:700])
        else:
            print(f"[{t}] ltp={ltp} sig=0 ord=0 pos=0", flush=True)
    except Exception as ex:
        print(f"[{t}] poll error: {ex}")
    if fired:
        break
    time.sleep(20)
print("=== events tail (gate/order/signal) ===")
print(run("""docker exec mcx-live python3 /tmp/ev2.py 2>/dev/null || docker cp /tmp/ev2.py mcx-live:/tmp/ev2.py >/dev/null 2>&1 && docker exec mcx-live python3 /tmp/ev2.py"""))
print("FIRE-WATCH3 END", time.strftime("%H:%M:%S"), "| fired =", fired)
ssh.close()
