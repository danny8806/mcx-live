import paramiko, os
from dotenv import load_dotenv
load_dotenv('mcx-trader.env')
host = os.getenv('VPS_HOST')
user = os.getenv('VPS_USER')
pw = os.getenv('VPS_PASS')
k = paramiko.RSAKey.from_private_key_file(os.path.expanduser('~/.ssh/id_rsa'))
c = paramiko.SSHClient()
c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
c.connect(host, 22, username=user, pkey=k, timeout=15)

sql = """SELECT pending_order_id, strategy_id, signal_id, status, expired_reason, created_at FROM pending_orders ORDER BY created_at DESC;"""
cmd = f'docker exec mcx-live sqlite3 /app/data/db/trading.db "{sql}"'
_, out, err = c.exec_command(cmd, timeout=30)
print("=== PENDING ORDERS ===")
print(out.read().decode())
e = err.read().decode()
if e: print("ERR:", e)

sql2 = """SELECT strategy_id, state, position_side, pending_entry_side, pending_entry_trigger, enabled FROM strategies_live;"""
cmd2 = f'docker exec mcx-live sqlite3 /app/data/db/trading.db "{sql2}"'
_, out2, err2 = c.exec_command(cmd2, timeout=30)
print("\n=== STRATEGIES (if table exists) ===")
print(out2.read().decode())

c.close()
