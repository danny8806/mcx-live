#!/usr/bin/env python3
"""Phase 6 Sections 10-27: Order lifecycle + restart + signals."""
import paramiko, time, json, urllib.request

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"

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
    ch.close()
    t.close()
    return out.decode(errors="replace")

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

print("=" * 80)
print("SECTION 14-16: SIGNAL + ENTRY ORDER + SUBMISSION")
print("=" * 80)

signals = api("/api/signals")
print(f"\n--- Signals ---")
if "error" in signals:
    print(f"  /api/signals: {signals['error']}")
else:
    print(f"  count: {signals.get('count', '?')}")
    for s in signals.get("signals", [])[:5]:
        print(f"  {json.dumps(s, indent=2)[:200]}")

print("\n--- Strategy state (from /api/strategies) ---")
strats = api("/api/strategies")
if "strategies" in strats:
    for s in strats["strategies"]:
        sid = s.get("strategy_id", "?")
        state = s.get("state", "?")
        pending = s.get("pending_entry")
        last_exit = s.get("last_exit_reason", "?")
        current_trade = s.get("current_trade_id", "none")
        last_armed = s.get("last_armed_pending_id", "none")
        print(f"  {sid}: state={state} pending={pending is not None} exit={last_exit} trade={current_trade[:20] if current_trade else 'none'} armed={last_armed}")

print("\n--- Price model (STOP-LIMIT entry) ---")
pm_code = ssh("docker exec mcx-live python3 -c \"import inspect; from execution.price_model import PriceModel; src=inspect.getsource(PriceModel._stop_limit_legs); print(src)\" 2>/dev/null")
print(pm_code.strip()[:600] if pm_code.strip() else "  Could not inspect")

print("\n--- immediate_limit in instance ---")
im_code = ssh("docker exec mcx-live python3 -c \"import inspect; from strategies.instance import StrategyInstance; src=inspect.getsource(StrategyInstance._create_immediate_limit_signal); print(src)\" 2>/dev/null")
print(im_code.strip()[:600] if im_code.strip() else "  Could not inspect")

print("\n" + "=" * 80)
print("SECTION 17-22: PENDING TRACKING + TRIGGER + CANCEL + MARKET FALLBACK")
print("=" * 80)

ow_code = ssh("docker exec mcx-live python3 -c \"import inspect; from execution.live.order_watcher import OrderWatcher; src=inspect.getsource(OrderWatcher._decide); print(src)\" 2>/dev/null")
print("\n--- OrderWatcher._decide (full decision tree) ---")
print(ow_code.strip()[:1200] if ow_code.strip() else "  Could not inspect")

print("\n--- market_fallback logic ---")
mf_code = ssh("docker exec mcx-live grep -n 'market_fallback\\|MARKET_FALLBACK\\|cancel.*verify\\|remaining.*quantity' /app/execution/live/order_watcher.py 2>/dev/null | head -20")
print(mf_code.strip()[:400] if mf_code.strip() else "  not found")

print("\n--- Pending orders ---")
pend = api("/api/pending-orders")
print(f"  /api/pending-orders: {json.dumps(pend)[:200]}")

print("\n--- Cancel-inflight logic ---")
ci_code = ssh("docker exec mcx-live grep -n 'cancel_inflight\\|cancel.*inflight\\|CANCELLED\\|inflight' /app/trading_engine.py 2>/dev/null | head -20")
print(ci_code.strip()[:400] if ci_code.strip() else "  not found")

print("\n--- Trigger detection ---")
trig_code = ssh("docker exec mcx-live grep -n 'TRIGGER_CROSSED\\|trigger_crossed\\|trigger.*cross' /app/execution/live/order_watcher.py 2>/dev/null | head -15")
print(trig_code.strip()[:400] if trig_code.strip() else "  not found")

print("\n" + "=" * 80)
print("SECTION 23: SL CREATION")
print("=" * 80)

sl_code = ssh("docker exec mcx-live python3 -c \"import inspect; from execution.live.engine import LiveExecutionEngine; src=inspect.getsource(LiveExecutionEngine.create_protective_sl); print(src)\" 2>/dev/null")
print(sl_code.strip()[:800] if sl_code.strip() else "  Could not inspect")

print("\n--- SL config ---")
sl_cfg = ssh("docker exec mcx-live python3 -c \"import json; c=json.load(open('/app/config/live_settings.json')); sl=c.get('live',{}).get('broker_sl',{}); print(json.dumps(sl,indent=2))\" 2>/dev/null")
print(sl_cfg.strip()[:300] if sl_cfg.strip() else "  not found")

print("\n" + "=" * 80)
print("SECTION 24-25: EXIT + REVERSAL")
print("=" * 80)

reversal_code = ssh("docker exec mcx-live python3 -c \"import inspect; from strategies.instance import StrategyInstance; src=inspect.getsource(StrategyInstance._create_reversal_signal); print(src)\" 2>/dev/null")
print(reversal_code.strip()[:800] if reversal_code.strip() else "  Could not inspect")

print("\n--- Reversals from API ---")
rev = api("/api/reversals")
print(f"  count: {rev.get('count', '?')}")
if "reversals" in rev:
    for r_item in rev.get("reversals", [])[:3]:
        print(f"  {json.dumps(r_item)[:200]}")

print("\n--- Exit handling ---")
exit_code = ssh("docker exec mcx-live grep -n 'def.*exit\\|def.*close\\|def.*flatten\\|NORMAL_EXIT\\|exit_signal\\|exit_order' /app/trading_engine.py /app/strategies/instance.py 2>/dev/null | head -15")
print(exit_code.strip()[:400] if exit_code.strip() else "  not found")

print("\n" + "=" * 80)
print("SECTION 26: STRATEGY ISOLATION")
print("=" * 80)

print("\n--- Strategies from API ---")
if "strategies" in strats:
    for s in strats["strategies"]:
        sid = s.get("strategy_id", "?")
        inst = s.get("instrument", "?")
        tf = s.get("fast_timeframe", "?")
        enabled = s.get("enabled", "?")
        qty = s.get("quantity", "?")
        print(f"  {sid}: inst={inst} tf={tf} enabled={enabled} qty={qty}")

print("\n--- Strategy isolation code ---")
iso_code = ssh("docker exec mcx-live grep -n 'strategy_id\\|isolation\\|per_strategy\\|_guard_live' /app/trading_engine.py 2>/dev/null | head -15")
print(iso_code.strip()[:500] if iso_code.strip() else "  not found")

print("\n" + "=" * 80)
print("SECTION 27: DATABASE LINEAGE")
print("=" * 80)

db_queries = [
    ("Trade lineage", "SELECT t.trade_id, t.strategy_id, t.instrument, t.entry_price, t.exit_price, t.net_pnl, t.status, t.exit_reason FROM trades t ORDER BY t.created_at DESC LIMIT 10"),
    ("Order lineage", "SELECT o.order_id, o.strategy_id, o.side, o.order_type, o.state, o.price, o.trigger_price, o.broker_order_id FROM orders o ORDER BY o.created_at DESC LIMIT 15"),
    ("Signal lineage", "SELECT s.signal_id, s.strategy_id, s.instrument, s.direction, s.trigger_price, s.sl_price, s.candle_timestamp FROM signals s ORDER BY s.created_at DESC LIMIT 10"),
    ("Fill lineage", "SELECT f.fill_id, f.order_id, f.broker_order_id, f.quantity, f.price, f.timestamp FROM fills f ORDER BY f.timestamp DESC LIMIT 10"),
    ("Position lineage", "SELECT p.position_id, p.strategy_id, p.instrument, p.side, p.quantity, p.entry_price, p.status FROM positions p ORDER BY p.created_at DESC LIMIT 10"),
    ("Broker mapping", "SELECT * FROM broker_order_mapping ORDER BY created_at DESC LIMIT 10"),
    ("Pending orders", "SELECT * FROM pending_orders ORDER BY created_at DESC LIMIT 10"),
    ("Trade events", "SELECT * FROM trade_events ORDER BY created_at DESC LIMIT 10"),
    ("System metadata", "SELECT * FROM system_metadata LIMIT 10"),
]
for label, q in db_queries:
    r = ssh(f"docker exec mcx-live python3 -c \"import sqlite3; c=sqlite3.connect('/app/data/db/trading.db').cursor(); c.execute('{q}'); [print(dict(zip([d[0] for d in c.description],row))) for row in c.fetchall()]\" 2>/dev/null")
    print(f"\n--- {label} ---")
    print(r.strip()[:500] if r.strip() else "  (empty or error)")
