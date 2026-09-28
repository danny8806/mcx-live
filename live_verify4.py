#!/usr/bin/env python3
"""SL failure investigation + live DB data."""
import paramiko, time, json

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
    ch.close(); t.close()
    return out.decode(errors="replace")

# ====================================================================
# LIVE DB TRADES
# ====================================================================
print("=" * 80)
print("LIVE DB: TRADES")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT trade_id, strategy_id, instrument, side, entry_price, exit_price, net_pnl, status, exit_reason, stop_price, signal_id, created_at FROM trades ORDER BY created_at DESC LIMIT 5')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:1500])

# ====================================================================
# LIVE DB ORDERS
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: ORDERS (recent)")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT order_id, strategy_id, side, order_type, state, price, trigger_price, broker_order_id, filled_quantity, quantity, order_role, created_at FROM orders ORDER BY created_at DESC LIMIT 15')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:2000])

# ====================================================================
# LIVE DB FILLS
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: FILLS")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT fill_id, order_id, broker_order_id, instrument, side, quantity, price, strategy_id, timestamp FROM fills ORDER BY timestamp DESC LIMIT 10')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:1000])

# ====================================================================
# LIVE DB POSITIONS
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: POSITIONS")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT position_id, strategy_id, instrument, side, quantity, average_entry, stop_price, status, created_at FROM positions ORDER BY created_at DESC LIMIT 5')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:500])

# ====================================================================
# LIVE DB SIGNALS
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: SIGNALS")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT signal_id, strategy_id, instrument, direction, trigger_price, sl_price, candle_timestamp, created_at FROM signals ORDER BY created_at DESC LIMIT 5')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:800])

# ====================================================================
# LIVE DB TRADE EVENTS
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: TRADE EVENTS (for silver_01 trade)")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute(\"SELECT event_type, trade_id, order_id, strategy_id, data, created_at FROM trade_events WHERE trade_id = '2bf9a9ee-2e10-43c2-b225-37fa7ee9ac5c' ORDER BY created_at LIMIT 20\")
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str, indent=2))
" 2>/dev/null""")
print(r.strip()[:2000])

# ====================================================================
# LIVE DB BROKER ORDER MAPPING
# ====================================================================
print("\n" + "=" * 80)
print("LIVE DB: BROKER ORDER MAPPING")
print("=" * 80)

r = ssh("""docker exec mcx-live python3 -c "
import sqlite3, json
c = sqlite3.connect('/app/live/data/db/live_trading.db').cursor()
c.execute('SELECT * FROM broker_order_mapping ORDER BY created_at DESC LIMIT 10')
cols = [d[0] for d in c.description]
for row in c.fetchall():
    print(json.dumps(dict(zip(cols, row)), default=str))
" 2>/dev/null""")
print(r.strip()[:800])

# ====================================================================
# FULL CONTAINER LOGS — SL + ENTRY SEQUENCE
# ====================================================================
print("\n" + "=" * 80)
print("CONTAINER LOGS — COMPLETE ENTRY + SL SEQUENCE")
print("=" * 80)

r = ssh("docker logs mcx-live 2>&1 | grep -iE 'Lifecycle|SL|stop_loss|protect|blocked|price.*trigger|trigger.*price|BUY STOP|order_created|position_opened|fill|reject|Dhan|submitted|broker'")
print(r.strip()[:2000] if r.strip() else "  No logs found")

# ====================================================================
# SL CONFIG + CODE
# ====================================================================
print("\n" + "=" * 80)
print("SL CONFIG")
print("=" * 80)

r = ssh("docker exec mcx-live python3 -c \"import json; c=json.load(open('/app/config/live_settings.json')); sl=c.get('live',{}).get('broker_sl',{}); print(json.dumps(sl,indent=2))\"")
print(r.strip()[:300])

print("\n" + "=" * 80)
print("LOCAL POSITION-OWNED SL")
print("=" * 80)

# The broker-side protective SL is RETIRED.  Verify the retired method is gone
# and that the local position-owned monitor is the only stop mechanism.
r = ssh("docker exec mcx-live python3 -c \"import inspect; "
        "from execution.live.engine import LiveExecutionEngine; "
        "from execution.live.sl_monitor import PositionOwnedSLMonitor, SLState; "
        "print('create_protective_sl present:', hasattr(LiveExecutionEngine, 'create_protective_sl')); "
        "print('local monitor:', PositionOwnedSLMonitor.__name__); "
        "print('local states:', [s.value for s in SLState])\"")
print(r.strip()[:1000])
