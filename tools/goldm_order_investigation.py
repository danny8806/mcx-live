"""GOLDM buy order investigation."""
import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file
import paramiko

seed = load_env_file(Path(__file__).resolve().parent.parent / "mcx-trader.env")
vps_pass = seed.get("VPS_PASS", "")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

def run(cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode("utf-8", errors="replace").strip()

db = VPS_BASE + "/live/data/db/live_trading.db"

print("=" * 70)
print("GOLDM BUY ORDER INVESTIGATION")
print("=" * 70)

# 1. ALL GOLDM SIGNALS
print("\n[1] ALL GOLDM SIGNALS")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT signal_id, strategy_id, instrument, side, signal_type, signal_timestamp, candle_timestamp, trigger_price, stop_price, quantity, execution_mode FROM signals WHERE instrument=\'GOLDM\' ORDER BY signal_timestamp DESC")\n'
'rows = c.fetchall()\n'
'print(f"  Total GOLDM signals: {len(rows)}")\n'
'for r in rows:\n'
'    import datetime\n'
'    ts = datetime.datetime.fromtimestamp(r[5]/1000).strftime("%Y-%m-%d %H:%M") if r[5] else "?"\n'
'    print(f"  {r[0][:25]}... | {r[1]} | {r[2]} | side={r[3]} | type={r[4]} | ts={ts} | trigger={r[7]} | stop={r[8]} | qty={r[9]} | mode={r[10]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe1.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe1.py"))

# 2. ALL GOLDM ORDERS
print("\n[2] ALL GOLDM ORDERS")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT order_id, strategy_id, instrument, side, order_type, state, broker_order_id, correlation_id, order_role, price, trigger_price, quantity, created_at FROM orders WHERE instrument=\'GOLDM\' ORDER BY created_at DESC")\n'
'rows = c.fetchall()\n'
'print(f"  Total GOLDM orders: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0][:30]}... | {r[1]} | {r[2]} | {r[3]} | {r[4]} | state={r[5]} | broker={r[6]} | role={r[8]} | price={r[9]} | trigger={r[10]} | qty={r[11]} | {r[12]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe2.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe2.py"))

# 3. ALL GOLDM TRADES
print("\n[3] ALL GOLDM TRADES")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT trade_id, strategy_id, instrument, side, status, entry_price, exit_price, exit_reason, entry_timestamp FROM trades WHERE instrument=\'GOLDM\' ORDER BY entry_timestamp DESC")\n'
'rows = c.fetchall()\n'
'print(f"  Total GOLDM trades: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0][:25]}... | {r[1]} | {r[2]} | {r[3]} | status={r[4]} | entry={r[5]} | exit={r[6]} | reason={r[7]} | {r[8]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe3.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe3.py"))

# 4. GOLDM BROKER API EVENTS (rejection reasons)
print("\n[4] GOLDM BROKER API EVENTS (rejection/submit)")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT action, http_status, broker_order_id, order_status, order_type, transaction_type, security_id, error_type, error_code, error_message, created_at FROM broker_api_events WHERE security_id=\'569003\' OR security_id=\'569003\' ORDER BY created_at DESC LIMIT 20")\n'
'rows = c.fetchall()\n'
'print(f"  GOLDM broker events: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0]} | HTTP={r[1]} | broker={r[2]} | status={r[3]} | type={r[4]} | txn={r[5]} | sid={r[6]} | err={r[7]} code={r[8]} msg={r[9]} | {r[10]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe4.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe4.py"))

# 5. GOLDM FAILURE EVENTS
print("\n[5] GOLDM FAILURE EVENTS")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT event_type, strategy_id, trade_id, order_id, broker_order_id, error, action, final_state, details, created_at FROM execution_failure_events WHERE instrument=\'GOLDM\' ORDER BY created_at DESC LIMIT 20")\n'
'rows = c.fetchall()\n'
'print(f"  GOLDM failure events: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0]} | {r[1]} | trade={r[2][:20] if r[2] else \'?\'}... | order={r[3][:20] if r[3] else \'?\'}... | broker={r[4]} | err={r[5]} | action={r[6]} | final={r[7]} | {r[8]} | {r[9]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe5.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe5.py"))

# 6. STRATEGY GATE STATE
print("\n[6] STRATEGY GATE STATE (live)")
strat_raw = run("curl -sk 'http://127.0.0.1:8001/api/strategies' 2>/dev/null")
try:
    strats = json.loads(strat_raw)
    items = strats.get("strategies", strats) if isinstance(strats, dict) else strats
    if isinstance(items, list):
        for s in items:
            sid = s.get("strategy_id", s.get("id", "?"))
            if "gold" in str(sid).lower():
                print(f"  {json.dumps(s, indent=2)[:500]}")
    elif isinstance(items, dict):
        for sid, s in items.items():
            if "gold" in str(sid).lower():
                print(f"  {sid}: {json.dumps(s, indent=2)[:500]}")
except Exception as e:
    print(f"  ERROR: {e}")
    print(f"  Raw: {strat_raw[:500]}")

# 7. LIVE ENGINE STATE
print("\n[7] ENGINE STATE (live)")
hs = run("curl -sk 'http://127.0.0.1:8001/api/health/system' 2>/dev/null")
print(f"  {hs[:500]}")

# 8. RECENT EVENTS (all strategies)
print("\n[8] RECENT EVENTS (last 10)")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT timestamp, event_type, strategy_id, instrument, details FROM events ORDER BY id DESC LIMIT 10")\n'
'rows = c.fetchall()\n'
'for r in rows:\n'
'    print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]} | {r[4][:100] if r[4] else \'\'}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe8.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe8.py"))

# 9. CHECK IF GATE IS BLOCKING ENTRIES
print("\n[9] GATE / RISK / PENDING ORDER CHECK")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT * FROM pending_orders ORDER BY created_at DESC LIMIT 10")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  PENDING ORDERS: {len(rows)}")\n'
'for r in rows:\n'
'    d = dict(zip(cols, r))\n'
'    print(f"  {d.get(\'pending_order_id\',\'?\')[:25]} | {d.get(\'strategy_id\',\'?\')} | {d.get(\'side\',\'?\')} | status={d.get(\'status\',\'?\')} | armed={d.get(\'armed_at\',\'?\')} | expired={d.get(\'expired_reason\',\'?\')}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/goldm_probe9.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/goldm_probe9.py"))

# 10. MARKET STATUS + TRADING ALLOWED
print("\n[10] MARKET STATUS + TRADING ALLOWED")
dash_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/dashboard' 2>/dev/null")
try:
    d = json.loads(dash_raw)
    p = d.get("profile", {})
    sm = d.get("safe_mode", {})
    print(f"  market_status: {d.get('market_status', p.get('market_status', '?'))}")
    print(f"  engine_status: {d.get('engine_status', '?')}")
    print(f"  safe_mode.active: {sm.get('active')}")
    print(f"  safe_mode.trading_allowed: {sm.get('trading_allowed')}")
    print(f"  gate: {p.get('gate')}")
    print(f"  execution_model: {p.get('execution_model')}")
except Exception as e:
    print(f"  ERROR: {e}")

ssh.close()
