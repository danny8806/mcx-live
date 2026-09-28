"""Deep past order log investigation — full lifecycle trace."""
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

print("=" * 80)
print("DEEP PAST ORDER LOG — FULL LIFECYCLE TRACE")
print("=" * 80)

# ═══════════════════════════════════════════════════════════════
# 1. ALL ORDERS (EVER) — full history
# ═══════════════════════════════════════════════════════════════
print("\n[1] ALL ORDERS — COMPLETE HISTORY")
probe = (
'import sqlite3, datetime\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT order_id, strategy_id, instrument, side, order_type, state,\n'
'    broker_order_id, correlation_id, order_role, price, trigger_price,\n'
'    quantity, filled_quantity, average_fill_price, created_at, updated_at\n'
'    FROM orders ORDER BY created_at ASC""")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL ORDERS EVER: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    d = dict(zip(cols, r))\n'
'    state_icon = "✓FILLED" if d["state"]=="filled" else "✗REJECTED" if d["state"]=="rejected" else "?"+d["state"]\n'
'    print(f"  [{i:2d}] {d[\'order_id\'][:35]}")\n'
'    print(f"       strategy: {d[\'strategy_id\']}  instrument: {d[\'instrument\"]}")\n'
'    print(f"       side: {d[\'side\']}  type: {d[\'order_type\']}  role: {d[\'order_role\"]}")\n'
'    print(f"       price: {d[\'price\']}  trigger: {d[\'trigger_price\']}  qty: {d[\'quantity\']}")\n'
'    print(f"       filled_qty: {d[\'filled_quantity\']}  avg_fill: {d[\'average_fill_price\']}")\n'
'    print(f"       broker_id: {d[\'broker_order_id\']}  correlation: {d[\'correlation_id\"]}")\n'
'    print(f"       state: {state_icon}")\n'
'    print(f"       created: {d[\'created_at\']}  updated: {d[\'updated_at\']}")\n'
'    print()\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders1.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders1.py"))

# ═══════════════════════════════════════════════════════════════
# 2. ALL TRADES (EVER) — full history
# ═══════════════════════════════════════════════════════════════
print("\n[2] ALL TRADES — COMPLETE HISTORY")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT trade_id, strategy_id, instrument, side, status,\n'
'    entry_price, exit_price, entry_signal_id, exit_signal_id,\n'
'    entry_order_id, exit_order_id, entry_fill_id, exit_fill_id,\n'
'    gross_pnl, charges, net_pnl, exit_reason, realized_pnl,\n'
'    execution_mode, created_at, updated_at\n'
'    FROM trades ORDER BY created_at ASC""")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL TRADES EVER: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    d = dict(zip(cols, r))\n'
'    print(f"  [{i:2d}] {d[\'trade_id\'][:35]}")\n'
'    print(f"       strategy: {d[\'strategy_id\']}  instrument: {d[\'instrument\"]}")\n'
'    print(f"       side: {d[\'side\']}  status: {d[\'status\"]}")\n'
'    print(f"       entry_price: {d[\'entry_price\']}  exit_price: {d[\'exit_price\"]}")\n'
'    print(f"       entry_signal: {str(d[\'entry_signal_id\'])[:25] if d[\'entry_signal_id\'] else \'None\'}")\n'
'    print(f"       entry_order: {str(d[\'entry_order_id\'])[:25] if d[\'entry_order_id\'] else \'None\'}")\n'
'    print(f"       exit_order: {str(d[\'exit_order_id\'])[:25] if d[\'exit_order_id\'] else \'None\'}")\n'
'    print(f"       entry_fill: {str(d[\'entry_fill_id\'])[:25] if d[\'entry_fill_id\'] else \'None\'}")\n'
'    print(f"       exit_fill: {str(d[\'exit_fill_id\'])[:25] if d[\'exit_fill_id\'] else \'None\'}")\n'
'    print(f"       pnl: gross={d[\'gross_pnl\']} charges={d[\'charges\']} net={d[\'net_pnl\"]}")\n'
'    print(f"       exit_reason: {d[\'exit_reason\']}  realized: {d[\'realized_pnl\"]}")\n'
'    print(f"       mode: {d[\'execution_mode\']}")\n'
'    print(f"       created: {d[\'created_at\']}  updated: {d[\'updated_at\']}")\n'
'    print()\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders2.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders2.py"))

# ═══════════════════════════════════════════════════════════════
# 3. ALL FILLS (EVER)
# ═══════════════════════════════════════════════════════════════
print("\n[3] ALL FILLS — COMPLETE HISTORY")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT COUNT(*) FROM fills")\n'
'cnt = c.fetchone()[0]\n'
'print(f"  TOTAL FILLS: {cnt}")\n'
'if cnt > 0:\n'
'    c.execute("""SELECT fill_id, trade_id, order_id, broker_fill_id, broker_order_id,\n'
'        cumulative_filled_quantity, strategy_id, instrument, side, quantity, price,\n'
'        timestamp, fill_type, execution_mode FROM fills ORDER BY timestamp ASC""")\n'
'    for r in c.fetchall():\n'
'        print(f"  {r[0][:25]}... | trade={r[1][:15] if r[1] else \'?\'} | broker_fill={r[3]} | broker_order={r[4]} | qty={r[5]} | {r[7]} {r[8]} @ {r[10]} | mode={r[13]}")\n'
'else:\n'
'    print("  NO FILLS — system has never executed a live fill")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders3.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders3.py"))

# ═══════════════════════════════════════════════════════════════
# 4. ALL SIGNALS (EVER) — full history
# ═══════════════════════════════════════════════════════════════
print("\n[4] ALL SIGNALS — COMPLETE HISTORY")
probe = (
'import sqlite3, datetime\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT signal_id, strategy_id, instrument, side, signal_type,\n'
'    signal_timestamp, trigger_price, stop_price, quantity, execution_mode,\n'
'    created_at FROM signals ORDER BY created_at ASC""")\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL SIGNALS: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    ts = r[5]\n'
'    ts_str = "?"\n'
'    if ts:\n'
'        try: ts_str = datetime.datetime.fromtimestamp(ts/1000).strftime("%Y-%m-%d %H:%M")\n'
'        except: ts_str = str(ts)\n'
'    print(f"  [{i:2d}] {r[0][:30]}... | {r[1]} | {r[2]} | side={r[3]} | type={r[4]} | ts={ts_str} | trigger={r[6]} | stop={r[7]} | qty={r[8]} | mode={r[9]} | {r[10]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders4.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders4.py"))

# ═══════════════════════════════════════════════════════════════
# 5. ALL BROKER API EVENTS (EVER) — full Dhan interaction log
# ═══════════════════════════════════════════════════════════════
print("\n[5] ALL BROKER API EVENTS — DHAN INTERACTION LOG")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT action, http_status, broker_order_id, exchange_order_id,\n'
'    order_status, order_type, transaction_type, security_id, quantity,\n'
'    price, trigger_price, error_type, error_code, error_message,\n'
'    correlation_id, request_timestamp, response_timestamp, created_at\n'
'    FROM broker_api_events ORDER BY created_at ASC""")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL BROKER API EVENTS: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    d = dict(zip(cols, r))\n'
'    status_icon = "✓" if d["http_status"]==200 else "✗"\n'
'    print(f"  [{i:3d}] {status_icon} {d[\'action\']} | HTTP={d[\'http_status\']} | broker={d[\'broker_order_id\']} | exchange={d[\'exchange_order_id\"]}")\n'
'    print(f"         status={d[\'order_status\']} | type={d[\'order_type\']} | txn={d[\'transaction_type\']} | sid={d[\'security_id\"]}")\n'
'    print(f"         qty={d[\'quantity\']} | price={d[\'price\']} | trigger={d[\'trigger_price\"]}")\n'
'    if d[\'error_type\'] or d[\'error_message\']:\n'
'        print(f"         ERROR: {d[\'error_type\']} code={d[\'error_code\']} msg={d[\'error_message\"]}")\n'
'    print(f"         corr={d[\'correlation_id\']}")\n'
'    print(f"         req_ts={d[\'request_timestamp\']} resp_ts={d[\'response_timestamp\']} | {d[\'created_at\']}")\n'
'    print()\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders5.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders5.py"))

# ═══════════════════════════════════════════════════════════════
# 6. ALL ALERT EVENTS (EVER)
# ═══════════════════════════════════════════════════════════════
print("\n[6] ALL ALERT EVENTS — COMPLETE HISTORY")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT event_id, event_type, event_source, strategy_id,\n'
'    signal_id, trade_id, broker_order_id, security_id, side,\n'
'    status_before, status_after, processing_status, error,\n'
'    created_at FROM alert_events ORDER BY created_at ASC""")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL ALERT EVENTS: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    d = dict(zip(cols, r))\n'
'    print(f"  [{i:2d}] {d[\'event_type\']} | source={d[\'event_source\']} | {d[\'strategy_id\']} | {d[\'security_id\']} {d[\'side\"]}")\n'
'    print(f"       trade={str(d[\'trade_id\'])[:20] if d[\'trade_id\'] else \'None\'} | broker={d[\'broker_order_id\"]}")\n'
'    print(f"       before={d[\'status_before\']} → after={d[\'status_after\']} | status={d[\'processing_status\']} | err={d[\'error\"]}")\n'
'    print(f"       {d[\'created_at\']}")\n'
'    print()\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders6.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders6.py"))

# ═══════════════════════════════════════════════════════════════
# 7. ALL TRADE EVENTS (EVER) — lifecycle audit trail
# ═══════════════════════════════════════════════════════════════
print("\n[7] ALL TRADE EVENTS — LIFECYCLE AUDIT TRAIL")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT trade_id, sequence_no, event_type, event_version,\n'
'    idempotency_key, strategy_id, instrument, timestamp,\n'
'    execution_mode, created_at FROM trade_events ORDER BY trade_id, sequence_no ASC""")\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL TRADE EVENTS: {len(rows)}")\n'
'print()\n'
'for r in rows:\n'
'    print(f"  {r[0][:20]}... | seq={r[1]} | {r[2]} v{r[3]} | idempotent={r[4][:20] if r[4] else \'?\'} | {r[5]} {r[6]} | mode={r[8]} | {r[9]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders7.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders7.py"))

# ═══════════════════════════════════════════════════════════════
# 8. PENDING ORDERS — current state
# ═══════════════════════════════════════════════════════════════
print("\n[8] PENDING ORDERS — CURRENT STATE")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT pending_order_id, trade_id, signal_id, side, order_type,\n'
'    trigger_price, quantity, status, broker_order_id, correlation_id,\n'
'    expired_reason, armed_at, created_at, updated_at\n'
'    FROM pending_orders ORDER BY created_at ASC""")\n'
'cols = [d[0] for d in c.description]\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL PENDING ORDERS: {len(rows)}")\n'
'print()\n'
'for i, r in enumerate(rows, 1):\n'
'    d = dict(zip(cols, r))\n'
'    print(f"  [{i:2d}] {d[\'pending_order_id\'][:30]}...")\n'
'    print(f"       trade={str(d[\'trade_id\'])[:20] if d[\'trade_id\'] else \'None\'} | signal={str(d[\'signal_id\'])[:20] if d[\'signal_id\'] else \'None\'}")\n'
'    print(f"       side={d[\'side\']} | type={d[\'order_type\']} | trigger={d[\'trigger_price\']} | qty={d[\'quantity\"]}")\n'
'    print(f"       status={d[\'status\']} | broker={d[\'broker_order_id\']} | corr={d[\'correlation_id\"]}")\n'
'    print(f"       expired_reason={d[\'expired_reason\']}")\n'
'    print(f"       armed={d[\'armed_at\']} | created={d[\'created_at\']} | updated={d[\'updated_at\']}")\n'
'    print()\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders8.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders8.py"))

# ═══════════════════════════════════════════════════════════════
# 9. REVERSALS — full history
# ═══════════════════════════════════════════════════════════════
print("\n[9] REVERSALS — FULL HISTORY")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT COUNT(*) FROM reversals")\n'
'cnt = c.fetchone()[0]\n'
'print(f"  TOTAL REVERSALS: {cnt}")\n'
'if cnt > 0:\n'
'    c.execute("""SELECT reversal_id, signal_id, strategy_id, instrument,\n'
'        old_trade_id, new_trade_id, status, fallback_used,\n'
'        old_exit_fill_price, new_entry_fill_price, created_at\n'
'        FROM reversals ORDER BY created_at ASC""")\n'
'    for r in c.fetchall():\n'
'        print(f"  {r[0]} | {r[1][:20] if r[1] else \'?\'} | {r[2]} {r[3]} | old={r[4][:15] if r[4] else \'None\'} new={r[5][:15] if r[5] else \'None\'} | status={r[6]} | fallback={r[7]} | old_exit={r[8]} new_entry={r[9]} | {r[10]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders9.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders9.py"))

# ═══════════════════════════════════════════════════════════════
# 10. ORDER WATCHER STATE — current live
# ═══════════════════════════════════════════════════════════════
print("\n[10] ORDER WATCHER STATE (live)")
orders_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/orders' 2>/dev/null")
try:
    data = json.loads(orders_raw)
    orders = data.get("orders", []) if isinstance(data, dict) else data
    print(f"  Live orders: {len(orders)}")
    for o in orders:
        print(f"  {o.get('order_id','?')[:35]} | {o.get('strategy_id','?')} | {o.get('instrument','?')} | {o.get('side','?')} | {o.get('order_type','?')} | state={o.get('state','?')} | broker={o.get('broker_order_id','?')} | role={o.get('order_role','?')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ═══════════════════════════════════════════════════════════════
# 11. ENGINE EVENTS — chronological
# ═══════════════════════════════════════════════════════════════
print("\n[11] ENGINE EVENTS — CHRONOLOGICAL")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT timestamp, event_type, strategy_id, instrument, details\n'
'    FROM events ORDER BY id ASC""")\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL ENGINE EVENTS: {len(rows)}")\n'
'print()\n'
'for r in rows:\n'
'    details = r[4][:120] if r[4] else ""\n'
'    print(f"  {r[0]} | {r[1]:20s} | {r[2] or \'\':12s} | {r[3] or \'\':10s} | {details}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders11.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders11.py"))

# ═══════════════════════════════════════════════════════════════
# 12. ACCOUNT SNAPSHOTS — equity history
# ═══════════════════════════════════════════════════════════════
print("\n[12] ACCOUNT SNAPSHOTS — EQUITY HISTORY (first + last 5)")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT COUNT(*) FROM account_snapshots")\n'
'total = c.fetchone()[0]\n'
'print(f"  TOTAL SNAPSHOTS: {total}")\n'
'c.execute("SELECT timestamp, equity, realized_pnl, unrealized_pnl, used_margin, available_margin FROM account_snapshots ORDER BY timestamp ASC LIMIT 5")\n'
'print("  FIRST 5:")\n'
'for r in c.fetchall():\n'
'    print(f"    {r[0]} | equity={r[1]} realized={r[2]} unrealized={r[3]} used={r[4]} avail={r[5]}")\n'
'c.execute("SELECT timestamp, equity, realized_pnl, unrealized_pnl, used_margin, available_margin FROM account_snapshots ORDER BY timestamp DESC LIMIT 5")\n'
'print("  LAST 5:")\n'
'for r in c.fetchall():\n'
'    print(f"    {r[0]} | equity={r[1]} realized={r[2]} unrealized={r[3]} used={r[4]} avail={r[5]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders12.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders12.py"))

# ═══════════════════════════════════════════════════════════════
# 13. BROKER ORDER MAPPING — full
# ═══════════════════════════════════════════════════════════════
print("\n[13] BROKER ORDER MAPPING — FULL")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("""SELECT broker_order_id, order_id, trade_id, strategy_id,\n'
'    instrument, registered_at, updated_at\n'
'    FROM broker_order_mapping ORDER BY registered_at ASC""")\n'
'rows = c.fetchall()\n'
'print(f"  TOTAL MAPPINGS: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  broker={r[0]} | order={r[1][:25] if r[1] else \'None\'} | trade={r[2][:15] if r[2] else \'None\'} | {r[3]} {r[4]} | registered={r[5]} | updated={r[6]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders13.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders13.py"))

# ═══════════════════════════════════════════════════════════════
# 14. FILLED RECONCILIATION
# ═══════════════════════════════════════════════════════════════
print("\n[14] FILL RECONCILIATION")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT COUNT(*) FROM fill_reconciliation")\n'
'cnt = c.fetchone()[0]\n'
'print(f"  TOTAL RECONCILIATION ROWS: {cnt}")\n'
'if cnt > 0:\n'
'    c.execute("""SELECT broker_order_id, strategy_id, instrument, side,\n'
'        broker_cumulative_qty, local_cumulative_qty, gap_qty, status,\n'
'        broker_average_price, updated_at FROM fill_reconciliation ORDER BY updated_at DESC LIMIT 20""")\n'
'    for r in c.fetchall():\n'
'        print(f"  {r[0]} | {r[1]} {r[2]} {r[3]} | broker_qty={r[4]} local_qty={r[5]} gap={r[6]} | status={r[7]} | avg_price={r[8]} | {r[9]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders14.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders14.py"))

# ═══════════════════════════════════════════════════════════════
# 15. SYSTEM METADATA
# ═══════════════════════════════════════════════════════════════
print("\n[15] SYSTEM METADATA")
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db + '")\n'
'c = conn.cursor()\n'
'c.execute("SELECT key, value, updated_at FROM system_metadata ORDER BY key")\n'
'for r in c.fetchall():\n'
'    print(f"  {r[0]}: {r[1]} (updated: {r[2]})")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/deep_orders15.py", "w") as f:
    f.write(probe)
sftp.close()
print(run("python3 /tmp/deep_orders15.py"))

ssh.close()
