"""Deep diagnostic: Query Dhan directly, trace dashboard issue."""
import json
import paramiko
import sys
import time
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    return out, err

def run_json(cmd, timeout=30):
    out, err = run(cmd, timeout)
    try:
        return json.loads(out)
    except:
        return out

print("=" * 70)
print("PART 1: DHAN POSITIONS (via container API)")
print("=" * 70)

# Query positions via the live backend which calls Dhan
out, err = run("""docker exec mcx-live python3 -c "
import urllib.request, json

# 1. Positions from backend (which calls Dhan)
try:
    r = urllib.request.urlopen('http://127.0.0.1:8001/api/positions')
    d = json.loads(r.read())
    print('=== BACKEND /api/positions ===')
    print(json.dumps(d, indent=2))
except Exception as e:
    print(f'ERROR /api/positions: {e}')
" """)
print(out)

# 2. Funds from backend
out, err = run("""docker exec mcx-live python3 -c "
import urllib.request, json
try:
    r = urllib.request.urlopen('http://127.0.0.1:8001/api/live/funds')
    d = json.loads(r.read())
    print('=== BACKEND /api/live/funds ===')
    print(json.dumps(d, indent=2)[:800])
except Exception as e:
    print(f'ERROR /api/live/funds: {e}')
" """)
print(out)

# 3. Orders from backend
out, err = run("""docker exec mcx-live python3 -c "
import urllib.request, json
try:
    r = urllib.request.urlopen('http://127.0.0.1:8001/api/orders')
    d = json.loads(r.read())
    print('=== BACKEND /api/orders ===')
    if isinstance(d, list):
        for o in d:
            print(json.dumps(o, indent=2)[:400])
            print('---')
    else:
        print(json.dumps(d, indent=2)[:2000])
except Exception as e:
    print(f'ERROR /api/orders: {e}')
" """)
print(out)

print("\n" + "=" * 70)
print("PART 1b: DHAN DIRECT REST CALLS (from container)")
print("=" * 70)

out, err = run("""docker exec mcx-live python3 -c "
import json, sys
sys.path.insert(0, '/app')
from config import Config
c = Config()
c.load()
dhan_cfg = c.get('dhan', {})

# Build the token from file
import os
token_file = dhan_cfg.get('token_file', '/app/live/data/db/dhan_token.json')
client_id = dhan_cfg.get('client_id', '')

# Read the access token from the resolved config or env
import subprocess
env_file = '/app/.env'
env_data = {}
if os.path.exists(env_file):
    with open(env_file) as f:
        for line in f:
            if '=' in line and not line.startswith('#'):
                k, v = line.strip().split('=', 1)
                env_data[k] = v.strip('\"')

access_token = env_data.get('DHAN_ACCESS_TOKEN', '')
print(f'client_id={client_id}')
print(f'token_file exists={os.path.exists(token_file)}')

if access_token and client_id:
    import urllib.request
    base = 'https://api.dhan.co/v2'
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }

    # Positions
    try:
        req = urllib.request.Request(f'{base}/positions', headers=headers)
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        print()
        print('=== DHAN /v2/positions ===')
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f'DHAN positions error: {e}')

    # Orders
    try:
        req = urllib.request.Request(f'{base}/orders', headers=headers)
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        print()
        print('=== DHAN /v2/orders ===')
        if isinstance(data, list):
            for o in data:
                if isinstance(o, dict):
                    oid = o.get('orderId', o.get('dhanOrderId', ''))
                    st = o.get('orderStatus', '')
                    ot = o.get('orderType', '')
                    side = o.get('transactionType', '')
                    trig = o.get('triggerPrice', '')
                    px = o.get('price', '')
                    qty = o.get('quantity', '')
                    print(f'  orderId={oid} type={ot} side={side} status={st} trigger={trig} price={px} qty={qty}')
        else:
            print(json.dumps(data, indent=2)[:2000])
    except Exception as e:
        print(f'DHAN orders error: {e}')

    # Funds
    try:
        req = urllib.request.Request(f'{base}/fundlimit', headers=headers)
        resp = urllib.request.urlopen(req, timeout=10)
        data = json.loads(resp.read())
        print()
        print('=== DHAN /v2/fundlimit ===')
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f'DHAN funds error: {e}')
else:
    print('Cannot call Dhan directly: missing access_token or client_id')
" """)
print(out)

ssh.close()
print("\n\nDIAGNOSTIC STEP 1 COMPLETE")
