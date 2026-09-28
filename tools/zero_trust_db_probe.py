"""DB probe via Python (sqlite3 module inside container)."""
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
    out = stdout.read().decode("utf-8", errors="replace")
    rc = stdout.channel.recv_exit_status()
    return out.strip(), rc

print("=" * 70)
print("DB DEEP PROBE")
print("=" * 70)

# Use Python sqlite3 inside container
db_path = f"{VPS_BASE}/live/data/db/live_trading.db"
probe_script = f'''
import sqlite3, json
conn = sqlite3.connect("{db_path}")
conn.row_factory = sqlite3.Row
c = conn.cursor()

# Schema version
c.execute("SELECT value FROM system_metadata WHERE key='schema_version'")
row = c.fetchone()
print(f"SCHEMA_VERSION: {{row[0] if row else 'NONE'}}")

# Foreign keys
c.execute("PRAGMA foreign_keys")
print(f"FOREIGN_KEYS: {{c.fetchone()[0]}}")

# Table list
c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in c.fetchall()]
print(f"TABLE_COUNT: {{len(tables)}}")

# Row counts for non-empty tables
for t in tables:
    c.execute(f"SELECT COUNT(*) FROM {{t}}")
    cnt = c.fetchone()[0]
    if cnt > 0:
        print(f"TABLE {{t}}: {{cnt}} rows")

# Lineage checks
c.execute("SELECT COUNT(*) FROM orders WHERE broker_order_id IS NOT NULL")
print(f"ORDERS_WITH_BROKER_ID: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM fills WHERE broker_fill_id IS NOT NULL")
print(f"FILLS_WITH_BROKER_FILL_ID: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM orders WHERE trade_id IS NULL")
print(f"ORDERS_WITH_NULL_TRADE_ID: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM fills WHERE trade_id IS NULL OR order_id IS NULL")
print(f"FILLS_WITH_NULL_LINEAGE: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM trades WHERE entry_signal_id IS NULL OR entry_signal_id=''")
print(f"TRADES_WITH_NULL_SIGNAL: {{c.fetchone()[0]}}")

# Duplicate checks
c.execute("SELECT COUNT(*) FROM (SELECT fill_id, COUNT(*) c FROM fills GROUP BY fill_id HAVING c>1)")
print(f"DUPLICATE_FILL_IDS: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM (SELECT order_id, COUNT(*) c FROM orders GROUP BY order_id HAVING c>1)")
print(f"DUPLICATE_ORDER_IDS: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM (SELECT trade_id, COUNT(*) c FROM trades GROUP BY trade_id HAVING c>1)")
print(f"DUPLICATE_TRADE_IDS: {{c.fetchone()[0]}}")

# Orphan check
c.execute("""SELECT COUNT(*) FROM fills f
    LEFT JOIN trades t ON f.trade_id = t.trade_id
    WHERE t.trade_id IS NULL""")
print(f"ORPHAN_FILLS: {{c.fetchone()[0]}}")
c.execute("""SELECT COUNT(*) FROM orders o
    LEFT JOIN trades t ON o.trade_id = t.trade_id
    WHERE o.trade_id IS NULL AND o.trade_id != ''""")
print(f"ORPHAN_ORDERS: {{c.fetchone()[0]}}")

# Reversals
c.execute("SELECT COUNT(*) FROM reversals")
print(f"REVERSALS_TOTAL: {{c.fetchone()[0]}}")
c.execute("SELECT COUNT(*) FROM reversals WHERE status='COMPLETE'")
print(f"REVERSALS_COMPLETE: {{c.fetchone()[0]}}")

# Execution mode distribution
c.execute("SELECT execution_mode, COUNT(*) FROM trades GROUP BY execution_mode")
for row in c.fetchall():
    print(f"TRADES_MODE_{{row[0]}}: {{row[1]}}")

conn.close()
'''

# Write probe script to VPS and execute
sftp = ssh.open_sftp()
sftp.open(f"/tmp/db_probe.py", "w").write(probe_script)
sftp.close()
out, rc = run(f"python3 /tmp/db_probe.py")
print(out)
if rc != 0:
    print(f"ERROR: rc={rc}")

# Also check orders detail
print("\n--- RECENT ORDERS ---")
order_probe = f'''
import sqlite3
conn = sqlite3.connect("{db_path}")
c = conn.cursor()
c.execute("""SELECT order_id, strategy_id, instrument, side, order_type, state, 
    broker_order_id, correlation_id, order_role, created_at 
    FROM orders ORDER BY created_at DESC LIMIT 11""")
for row in c.fetchall():
    print(f"  {{row[0]}} | {{row[1]}} | {{row[2]}} | {{row[3]}} | {{row[4]}} | {{row[5]}} | broker={{row[6]}} | corr={{row[7]}} | role={{row[8]}} | {{row[9]}}")
conn.close()
'''
sftp = ssh.open_sftp()
sftp.open(f"/tmp/order_probe.py", "w").write(order_probe)
sftp.close()
out, rc = run(f"python3 /tmp/order_probe.py")
print(out)

# Check Dhan connection status
print("\n--- DHAN CONNECTION STATUS ---")
out, rc = run(f"curl -sk 'http://127.0.0.1:8001/api/health/system' 2>/dev/null")
print(out[:500] if out else "No response")

# Check market data
print("\n--- MARKET DATA ---")
out, rc = run(f"curl -sk 'http://127.0.0.1:8001/api/market-data' 2>/dev/null")
print(out[:500] if out else "No response")

# Check live dashboard
print("\n--- LIVE DASHBOARD ---")
out, rc = run(f"curl -sk 'http://127.0.0.1:8001/api/live/dashboard' 2>/dev/null")
print(out[:800] if out else "No response")

ssh.close()
