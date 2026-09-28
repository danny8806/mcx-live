"""Trace reversal entry rejection: exact error, order details, price calculation."""
import paramiko, sys, json
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=20):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

print("=" * 70)
print("1. EXACT REVERSAL REJECTION ERROR")
print("=" * 70)
r = run("docker logs mcx-live --tail 5000 2>&1 | findstr /i 'reversal rejected reject entry'", timeout=30)
print(r)

print("\n" + "=" * 70)
print("2. FULL ENGINE LOGS AROUND REVERSAL EVENT")
print("=" * 70)
r = run("docker logs mcx-live --tail 5000 2>&1", timeout=30)
lines = r.split('\n')
# Find the reversal event line and print 20 lines before/after
for i, l in enumerate(lines):
    if 'reversal' in l.lower() or 'TRADE CLOSED' in l or 'TRADE CREATED' in l:
        start = max(0, i-10)
        end = min(len(lines), i+15)
        for j in range(start, end):
            print(f"  {lines[j]}")
        print("  ---")

print("\n" + "=" * 70)
print("3. REVERSAL ENTRY ORDER DETAILS (from API)")
print("=" * 70)
sftp = ssh.open_sftp()
ord_script = """import urllib.request, json
r = urllib.request.urlopen("http://127.0.0.1:8001/api/orders", timeout=5)
d = json.loads(r.read())
for o in d.get("orders", []):
    role = o.get("order_role", "")
    if "REVERSAL" in role.upper() or "ENTRY" in role.upper():
        print(json.dumps(o, indent=2))
"""
with sftp.open('/tmp/rev_orders.py', 'w') as f:
    f.write(ord_script)
sftp.close()
ssh.exec_command('docker cp /tmp/rev_orders.py mcx-live:/tmp/rev_orders.py')
r = run('docker exec mcx-live python3 /tmp/rev_orders.py', timeout=15)
print(r if r else "(no reversal orders found via API)")

print("\n" + "=" * 70)
print("4. ALL ORDERS WITH DETAILS")
print("=" * 70)
sftp = ssh.open_sftp()
all_ord = """import urllib.request, json
r = urllib.request.urlopen("http://127.0.0.1:8001/api/orders", timeout=5)
d = json.loads(r.read())
orders = d.get("orders", [])
print(f"Total orders: {len(orders)}")
for o in orders[-10:]:
    print(json.dumps({
        "order_id": o.get("order_id", "")[:25],
        "broker_order_id": str(o.get("broker_order_id", ""))[:25],
        "order_type": o.get("order_type"),
        "side": o.get("side"),
        "state": o.get("state"),
        "price": o.get("price"),
        "trigger_price": o.get("trigger_price"),
        "order_role": o.get("order_role"),
        "strategy_id": o.get("strategy_id"),
        "instrument": o.get("instrument"),
        "rejection_reason": o.get("rejection_reason"),
    }, indent=2))
"""
with sftp.open('/tmp/all_orders.py', 'w') as f:
    f.write(all_ord)
sftp.close()
ssh.exec_command('docker cp /tmp/all_orders.py mcx-live:/tmp/all_orders.py')
r = run('docker exec mcx-live python3 /tmp/all_orders.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("5. TRADE LIFECYCLE REVERSAL RECORDS")
print("=" * 70)
sftp = ssh.open_sftp()
trade_script = """import urllib.request, json
r = urllib.request.urlopen("http://127.0.0.1:8001/api/trades", timeout=5)
d = json.loads(r.read())
trades = d.get("trades", [])
print(f"Total trades: {len(trades)}")
for t in trades[-5:]:
    print(json.dumps({k: v for k, v in t.items() if v is not None and k not in ("raw",)}, indent=2)[:600])
    print()
"""
with sftp.open('/tmp/trades.py', 'w') as f:
    f.write(trade_script)
sftp.close()
ssh.exec_command('docker cp /tmp/trades.py mcx-live:/tmp/trades.py')
r = run('docker exec mcx-live python3 /tmp/trades.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("6. ENGINE REVERSAL ENTRY CODE (lines 1775-1930)")
print("=" * 70)
sftp = ssh.open_sftp()
with sftp.open('/app/trading_engine.py') as f:
    content = f.read().decode()
    lines = content.split('\n')
    for i in range(1774, min(1930, len(lines))):
        print(f"  {i+1}: {lines[i]}")
sftp.close()

ssh.close()
print("\nREVERSAL REJECTION TRACE COMPLETE")
