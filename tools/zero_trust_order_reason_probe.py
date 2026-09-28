"""Order rejection reason probe."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file
import paramiko

seed = load_env_file(Path(__file__).resolve().parent.parent / "mcx-trader.env")
vps_pass = seed.get("VPS_PASS", "")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

db_path = VPS_BASE + "/live/data/db/live_trading.db"
probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + db_path + '")\n'
'c = conn.cursor()\n'
'\n'
'# Failure events\n'
'c.execute("SELECT event_type, strategy_id, instrument, error, action, final_state, created_at FROM execution_failure_events ORDER BY created_at DESC LIMIT 20")\n'
'rows = c.fetchall()\n'
'print("FAILURE_EVENTS:", len(rows))\n'
'for r in rows:\n'
'    print(f"  {r[0]} | {r[1]} | {r[2]} | err={r[3]} | action={r[4]} | final={r[5]} | {r[6]}")\n'
'\n'
'# Trades\n'
'c.execute("SELECT trade_id, strategy_id, instrument, side, status, entry_price, exit_price, exit_reason, entry_timestamp FROM trades ORDER BY entry_timestamp DESC LIMIT 11")\n'
'rows = c.fetchall()\n'
'print(f"\\nTRADES: {len(rows)}")\n'
'for r in rows:\n'
'    tid = r[0][:20] if r[0] else "?"\n'
'    print(f"  {tid}... | {r[1]} | {r[2]} | {r[3]} | status={r[4]} | entry={r[5]} | exit={r[6]} | reason={r[7]} | {r[8]}")\n'
'\n'
'# Broker API events\n'
'c.execute("SELECT action, http_status, broker_order_id, order_status, error_type, error_message, created_at FROM broker_api_events ORDER BY created_at DESC LIMIT 10")\n'
'rows = c.fetchall()\n'
'print(f"\\nBROKER_API_EVENTS: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0]} | HTTP={r[1]} | broker={r[2]} | status={r[3]} | err={r[4]} | {r[5]} | {r[6]}")\n'
'\n'
'# Alert events\n'
'c.execute("SELECT event_type, strategy_id, broker_order_id, status_after, error, created_at FROM alert_events ORDER BY created_at DESC LIMIT 10")\n'
'rows = c.fetchall()\n'
'print(f"\\nALERT_EVENTS: {len(rows)}")\n'
'for r in rows:\n'
'    print(f"  {r[0]} | {r[1]} | broker={r[2]} | status={r[3]} | err={r[4]} | {r[5]}")\n'
'\n'
'# Account\n'
'c.execute("SELECT equity, realized_pnl, unrealized_pnl, available_margin FROM account_snapshots ORDER BY timestamp DESC LIMIT 1")\n'
'row = c.fetchone()\n'
'if row:\n'
'    print(f"\\nACCOUNT: equity={row[0]} realized={row[1]} unrealized={row[2]} available={row[3]}")\n'
'\n'
'# Trade legs, processed fills, fill reconciliation\n'
'c.execute("SELECT COUNT(*) FROM trade_legs")\n'
'print(f"TRADE_LEGS: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM processed_fills")\n'
'print(f"PROCESSED_FILLS: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM fill_reconciliation")\n'
'print(f"FILL_RECONCILIATION: {c.fetchone()[0]}")\n'
'\n'
'conn.close()\n'
)

sftp = ssh.open_sftp()
with sftp.open("/tmp/order_reason_probe.py", "w") as f:
    f.write(probe)
sftp.close()
_, stdout, _ = ssh.exec_command("python3 /tmp/order_reason_probe.py", timeout=30)
print(stdout.read().decode())
ssh.close()
