"""Verify F24 deployed config."""
import json, paramiko, sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=30)
    return stdout.read().decode('utf-8', errors='replace').strip()

# 1. Container image + health
print("=" * 60)
print("[1] CONTAINER STATUS")
print(run("docker ps --filter name=mcx-live --format '{{.Repository}}:{{.Tag}} {{.Status}}'"))
print(run("docker inspect mcx-live --format '{{.Config.Image}} Created={{.Created}}'"))

# 2. reversal config path: live.reversal
print("\n" + "=" * 60)
print("[2] live.reversal SECTION")
print(run("""docker exec mcx-live python3 -c "import json; c=json.load(open('/app/config/live_settings.json')); rev=c.get('live',{}).get('reversal',{}); print(json.dumps(rev, indent=2))" """))

# 3. top-level keys (should NOT have reversal at root)
print("\n" + "=" * 60)
print("[3] ROOT-LEVEL KEYS (reversal should NOT be here)")
print(run("""docker exec mcx-live python3 -c "import json; c=json.load(open('/app/config/live_settings.json')); print([k for k in c.keys()])" """))

# 4. trading_engine.py hash match
print("\n" + "=" * 60)
print("[4] FILE HASHES")
print("Container:", run("docker exec mcx-live md5sum /app/trading_engine.py /app/config/live_settings.json"))

import subprocess
r1 = subprocess.run(['certutil', '-hashfile', r'C:\Users\pc\Desktop\MCX-TRADER-LIVE\trading_engine.py', 'MD5'], capture_output=True, text=True)
r2 = subprocess.run(['certutil', '-hashfile', r'C:\Users\pc\Desktop\MCX-TRADER-LIVE\config\live_settings.json', 'MD5'], capture_output=True, text=True)
lines1 = [l.strip() for l in r1.stdout.strip().split('\n') if l.strip() and 'CertUtil' not in l and 'hash' not in l.lower()]
lines2 = [l.strip() for l in r2.stdout.strip().split('\n') if l.strip() and 'CertUtil' not in l and 'hash' not in l.lower()]
print(f"Local trading_engine.py: {lines1[0] if lines1 else 'N/A'}")
print(f"Local live_settings.json: {lines2[0] if lines2 else 'N/A'}")

# 5. Strategy gap wiring
print("\n" + "=" * 60)
print("[5] STRATEGY GAP WIRING IN CONTAINER")
print(run("docker exec mcx-live grep -n 'reversal_entry_gap_points' /app/strategies/instance.py"))
print(run("docker exec mcx-live grep -n 'reversal_entry_gap_points' /app/strategies/base_dema_strategy.py"))
print(run("docker exec mcx-live grep -n 'reversal_entry_gap_points' /app/trading_engine.py"))

# 6. Engine reads from live.reversal (not root.reversal)
print("\n" + "=" * 60)
print("[6] ENGINE CONFIG PATH")
print(run("""docker exec mcx-live grep -n "entry_gap_points" /app/trading_engine.py"""))

# 7. Current positions + SL state
print("\n" + "=" * 60)
print("[7] POSITIONS + SL STATE")
print(run("""docker exec mcx-live python3 -c "
import sqlite3
conn = sqlite3.connect('/app/live/data/db/live_trading.db')
conn.row_factory = sqlite3.Row
print('OPEN POSITIONS:')
for row in conn.execute('SELECT position_id, strategy_id, instrument, side, quantity, is_open, sl_order_id, sl_state FROM positions WHERE is_open=1'):
    print(dict(row))
if not conn.execute('SELECT 1 FROM positions WHERE is_open=1').fetchone():
    print('  (none)')
print()
print('SL ORDERS:')
for row in conn.execute('SELECT order_id, strategy_id, side, order_type, state, order_role FROM orders WHERE order_role=\"STOP_LOSS\" AND state=\"submitted\" ORDER BY updated_at DESC LIMIT 5'):
    print(dict(row))
conn.close()
" """))

# 8. Market data
print("\n" + "=" * 60)
print("[8] MARKET DATA + DHAN WS")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json
r = urllib.request.urlopen('http://127.0.0.1:8001/api/market-data')
d = json.loads(r.read())
for k,v in d.items():
    if isinstance(v, dict):
        print(f'{k}: ltp={v.get(\"ltp\",\"?\")} connected={v.get(\"ws_connected\",\"?\")}')
" """))

# 9. Funds
print("\n" + "=" * 60)
print("[9] DHAN FUNDS")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json
r = urllib.request.urlopen('http://127.0.0.1:8001/api/live/funds')
d = json.loads(r.read())
print(json.dumps(d, indent=2)[:500])
" """))

# 10. Engine state
print("\n" + "=" * 60)
print("[10] ENGINE STATE")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json
r = urllib.request.urlopen('http://127.0.0.1:8001/api/health')
d = json.loads(r.read())
print(json.dumps({k:d[k] for k in ['market_status','engine_status','safe_mode'] if k in d}, indent=2))
" """))

ssh.close()
print("\n\nVERIFICATION COMPLETE")
