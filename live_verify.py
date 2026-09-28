#!/usr/bin/env python3
"""FINAL LIVE EXECUTION VERIFICATION — Sections 1-8: Deployment + Auth + Account + Market + Signals + Orders."""
import paramiko, time, json, urllib.request, os, hashlib

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"
LOCAL = r"C:\Users\pc\Desktop\MCX-TRADER-LIVE"

def ssh(cmd, timeout=30):
    t = paramiko.Transport((VPS, 22))
    t.connect(username=USER, password=PASS)
    ch = t.open_session()
    ch.settimeout(timeout)
    ch.exec_command(cmd)
    out = b""
    while not ch.exit_status_ready():
        if ch.recv_ready(): out += ch.recv(65536)
        time.sleep(0.1)
    while ch.recv_ready(): out += ch.recv(65536)
    ch.close(); t.close()
    return out.decode(errors="replace")

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

# ====================================================================
# SECTION 1: DEPLOYMENT IDENTITY
# ====================================================================
print("=" * 80)
print("SECTION 1: DEPLOYMENT IDENTITY")
print("=" * 80)

container = ssh("docker inspect mcx-live --format 'ID={{.Id}} Image={{.Config.Image}} Created={{.Created}} Started={{.State.StartedAt}} RestartCount={{.RestartCount}} Status={{.State.Status}} Health={{.State.Health.Status}}'")
print(f"Container: {container.strip()}")

status = ssh("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(f"Status: {status.strip()}")

img = ssh("docker inspect mcx-live --format '{{.Config.Image}}'")
print(f"Image: {img.strip()}")

# Source hash check on critical files
files = ["execution/live/order_watcher.py", "execution/live/engine.py", "execution/price_model.py",
         "trading_engine.py", "execution/live/dhan_transport.py", "strategies/instance.py"]
for f in files:
    lp = os.path.join(LOCAL, f)
    try:
        with open(lp, "rb") as fh: lh = hashlib.md5(fh.read()).hexdigest()[:10]
    except: lh = "LOCAL_MISSING"
    ro = ssh(f"docker exec mcx-live md5sum /app/{f} 2>/dev/null || echo MISSING")
    rh = ro.strip().split()[0][:10] if ro.strip() else "MISSING"
    m = "MATCH" if lh == rh else "MISMATCH"
    print(f"  {f}: L={lh} R={rh} [{m}]")

# ====================================================================
# SECTION 2: DHAN AUTHENTICATION
# ====================================================================
print("\n" + "=" * 80)
print("SECTION 2: DHAN AUTHENTICATION")
print("=" * 80)

auth_logs = ssh("docker logs mcx-live 2>&1 | grep -iE 'auth|token|renew|totp' | tail -10")
print(f"Auth logs:\n{auth_logs.strip()[:500]}")

# Token file check
token = ssh("docker exec mcx-live python3 -c \"import json,time; d=json.load(open('/app/data/db/dhan_token.json')); print(f'expires={d.get(chr(101)+chr(120)+chr(112)+chr(105)+chr(114)+chr(101)+chr(115)+chr(95)+chr(97)+chr(116),chr(63))}')\"")
print(f"Token: {token.strip()}")

# ====================================================================
# SECTION 3: LIVE ACCOUNT
# ====================================================================
print("\n" + "=" * 80)
print("SECTION 3: LIVE ACCOUNT (from backend API)")
print("=" * 80)

risk = api("/api/risk")
print(f"\n--- /api/risk ---")
for k in ["equity", "available_margin", "used_margin", "open_positions", "daily_pnl",
          "peak_equity", "kill_switch_active", "margin_utilization"]:
    print(f"  {k}: {risk.get(k, '?')}")

overview = api("/api/overview")
print(f"\n--- /api/overview ---")
for k in ["execution_mode", "equity_source", "total_equity", "starting_capital",
          "today_pnl", "realized_pnl", "unrealized_pnl", "margin_used",
          "open_positions_count", "active_orders_count"]:
    v = overview.get(k, "?")
    if isinstance(v, dict): v = v.get("value", "?")
    print(f"  {k}: {v}")

# ====================================================================
# SECTION 4: MARKET DATA
# ====================================================================
print("\n" + "=" * 80)
print("SECTION 4: MARKET DATA")
print("=" * 80)

mkt = api("/api/market-data")
print(f"WS connected: {mkt.get('ws_connected', '?')}")
if "instruments" in mkt:
    for inst, data in mkt["instruments"].items():
        print(f"  {inst}: LTP={data.get('ltp')} spread={data.get('spread')} ticks={data.get('tick_count')} ts={data.get('timestamp')}")
print(f"adapter_stats: {json.dumps(mkt.get('adapter_stats', {}))[:200]}")

# Candle status
candles = ssh("docker logs mcx-live 2>&1 | grep -iE 'CandleFetcher.*closed' | tail -10")
print(f"\nCandle logs:\n{candles.strip()[:500]}")

# ====================================================================
# SECTION 5: SIGNALS
# ====================================================================
print("\n" + "=" * 80)
print("SECTION 5: SIGNALS")
print("=" * 80)

signals_api = api("/api/signals")
print(f"/api/signals: {json.dumps(signals_api)[:200]}")

# Check for signals in trades
trades = api("/api/trades")
if "trades" in trades:
    print(f"\nTrades (from API): {trades.get('count', '?')}")
    for t in trades.get("trades", [])[:10]:
        tid = t.get("trade_id", "?")[:12]
        strat = t.get("strategy_id", "?")
        side = t.get("side", "?")
        inst = t.get("instrument", "?")
        status = t.get("status", "?")
        entry = t.get("entry_price", "?")
        exit_p = t.get("exit_price", "?")
        pnl = t.get("net_pnl", "?")
        sl = t.get("stop_price", "?")
        sig = t.get("signal_id", "?")
        print(f"  {tid}... | {strat} {side} {inst} | status={status} | entry={entry} exit={exit_p} sl={sl} pnl={pnl} | sig={sig}")

# ====================================================================
# SECTION 6-8: ORDERS + DHAN BROKER IDs
# ====================================================================
print("\n" + "=" * 80)
print("SECTION 6-8: ORDERS + DHAN STATUS")
print("=" * 80)

orders = api("/api/orders")
if "orders" in orders:
    print(f"\nOrders: {orders.get('count', '?')}")
    for o in orders.get("orders", [])[:20]:
        oid = o.get("order_id", "?")[:16]
        strat = o.get("strategy_id", "?")
        side = o.get("side", "?")
        otype = o.get("order_type", "?")
        state = o.get("state", "?")
        price = o.get("price", "?")
        trigger = o.get("trigger_price", "?")
        broker = o.get("broker_order_id", "?")
        filled = o.get("filled_quantity", "?")
        qty = o.get("quantity", "?")
        role = o.get("order_role", "?")
        print(f"  {oid}... | {strat} {side} {otype} | state={state} | price={price} trigger={trigger} | qty={qty} filled={filled} | broker={broker} | role={role}")

# Positions
print("\n--- POSITIONS ---")
positions = api("/api/positions")
print(f"  execution_mode: {positions.get('execution_mode', '?')}")
print(f"  count: {positions.get('count', '?')}")
for p in positions.get("positions", []):
    print(f"  {json.dumps(p)[:300]}")

# Fills
print("\n--- FILLS ---")
fills = api("/api/fills")
print(f"  count: {fills.get('count', '?')}")
for f_item in fills.get("fills", []):
    print(f"  {json.dumps(f_item)[:300]}")

# ====================================================================
# SECTION: DHAN REST FROM CONTAINER (actual Dhan API)
# ====================================================================
print("\n" + "=" * 80)
print("DHAN REST FROM CONTAINER (actual Dhan API responses)")
print("=" * 80)

rest_cmds = [
    ("Fund Limits", "https://api.dhan.co/v2/fundlimit"),
    ("Positions", "https://api.dhan.co/v2/positions"),
    ("Orders", "https://api.dhan.co/v2/orders"),
    ("Trades", "https://api.dhan.co/v2/trades"),
]
for label, url in rest_cmds:
    cmd = (
        "import urllib.request,json,os;"
        "t=os.environ.get('DHAN_ACCESS_TOKEN','');"
        "c=os.environ.get('DHAN_CLIENT_ID','');"
        f"h={{'access-token':t,'client-id':c}};"
        f"r=urllib.request.urlopen(urllib.request.Request('{url}',headers=h),timeout=10);"
        "d=json.loads(r.read());"
        "print(json.dumps(d,indent=2)[:500])"
    )
    r = ssh(f"docker exec mcx-live python3 -c \"{cmd}\"")
    print(f"\n--- {label} ---")
    print(r.strip()[:600])

# ====================================================================
# SECTION: CONTAINER LOGS (order events, fills, signals)
# ====================================================================
print("\n" + "=" * 80)
print("CONTAINER LOGS — ORDER + TRADE EVENTS")
print("=" * 80)

log_queries = [
    ("Trade events", "docker logs mcx-live 2>&1 | grep -iE 'TRADE|FILL|POSITION|ENTRY|EXIT|SL|REVERSAL|CANCEL|ORDER' | tail -40"),
    ("Signal events", "docker logs mcx-live 2>&1 | grep -iE 'SIGNAL|CandleFetcher.*closed|entry_triggered|pending_long|flat' | tail -20"),
    ("Errors", "docker logs mcx-live 2>&1 | grep -iE 'error|exception|traceback|reject' | tail -10"),
]
for label, cmd in log_queries:
    r = ssh(cmd)
    print(f"\n--- {label} ---")
    print(r.strip()[:800])

# ====================================================================
# SECTION: STRATEGY STATE
# ====================================================================
print("\n" + "=" * 80)
print("STRATEGY STATE")
print("=" * 80)

strats = api("/api/strategies")
for s in strats.get("strategies", []):
    sid = s.get("strategy_id", "?")
    inst = s.get("instrument", "?")
    state = s.get("state", "?")
    enabled = s.get("enabled", "?")
    pos = s.get("position_side", "?")
    bars = s.get("bars_processed", 0)
    trade_id = s.get("current_trade_id", "none")
    pending = s.get("pending_entry")
    last_exit = s.get("last_exit_reason", "?")
    armed = s.get("last_armed_pending_id", "none")
    print(f"  {sid}: {inst} state={state} enabled={enabled} pos={pos} bars={bars} exit={last_exit} trade={str(trade_id)[:16] if trade_id else 'none'} armed={armed}")

# Reconciliation
print("\n--- RECONCILIATION ---")
recon = api("/api/reconciliation")
print(f"  is_consistent: {recon.get('is_consistent', '?')}")
if "checks" in recon:
    for c in recon["checks"]:
        print(f"  {c.get('name','?')}: consistent={c.get('is_consistent','?')}")
        for e in c.get("errors", [])[:2]:
            print(f"    ERROR: {str(e)[:120]}")
        for w in c.get("warnings", [])[:2]:
            print(f"    WARN: {str(w)[:120]}")

# Telegram alerts
print("\n--- TELEGRAM ALERTS ---")
alerts = api("/api/alerts")
print(f"  count: {alerts.get('count', '?')}")
for a in alerts.get("alerts", [])[:5]:
    print(f"  type={a.get('type','?')} severity={a.get('severity','?')} data={json.dumps(a.get('data',{}))[:100]}")

print("\n" + "=" * 80)
print("SECTION 1-8 COMPLETE")
print("=" * 80)
