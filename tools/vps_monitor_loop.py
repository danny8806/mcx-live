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

DURATION = 600  # seconds
STEP = 20
seen_orders = set()
seen_signals = set()
start = time.time()
print("MONITOR START", time.strftime("%H:%M:%S"), "| dur", DURATION, "s")
while time.time() - start < DURATION:
    t = time.strftime("%H:%M:%S")
    try:
        md = run("curl -s http://127.0.0.1:8001/api/market-data 2>/dev/null")
        mdj = json.loads(md) if md else {}
        ltp = {k: (v.get("ltp") if isinstance(v, dict) else None) for k, v in mdj.get("instruments", {}).items()}
        ws = mdj.get("ws_connected")
        sig = run("curl -s http://127.0.0.1:8001/api/live/signals 2>/dev/null")
        sigj = json.loads(sig) if sig else {}
        slist = sigj.get("signals") or []
        for s in slist:
            key = str(s.get("id") or s.get("timestamp") or json.dumps(s)[:40])
            if key not in seen_signals:
                print(f"[{t}] NEW SIGNAL: {json.dumps(s)[:220]}")
                seen_signals.add(key)
        ords = run("curl -s http://127.0.0.1:8001/api/live/orders 2>/dev/null")
        ordj = json.loads(ords) if ords else {}
        olist = ordj.get("orders") or []
        for o in olist:
            oid = str(o.get("order_id") or o.get("orderId") or json.dumps(o)[:40])
            if oid not in seen_orders:
                print(f"[{t}] NEW ORDER: {json.dumps(o)[:220]}")
                seen_orders.add(oid)
        pos = run("curl -s http://127.0.0.1:8001/api/live/positions 2>/dev/null")
        posj = json.loads(pos) if pos else {}
        hlth = run("curl -s http://127.0.0.1:8001/api/live/health 2>/dev/null")
        hltj = json.loads(hlth) if hlth else {}
        funds = run("curl -s http://127.0.0.1:8001/api/live/funds 2>/dev/null")
        fnd = json.loads(funds) if funds else {}
        sids = [s.get("strategy_id") for s in olist]
        print(f"[{t}] ws={ws} ltp={ltp} | signals={len(slist)} orders={len(olist)} positions={posj.get('count')} | gate={hltj.get('gate_enabled')} | equity={fnd.get('equity')}")
    except Exception as ex:
        print(f"[{t}] poll error: {ex}")
    time.sleep(STEP)
# final err tail
print("=== recent log errors ===")
print(run("docker logs mcx-live --since 12m 2>&1 | grep -iE 'auth|DH-906|reject|error|signal|order|SL|entry|exit' | tail -20"))
print("MONITOR END")
ssh.close()
