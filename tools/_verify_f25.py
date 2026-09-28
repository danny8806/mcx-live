"""Final verification of f25: dashboard fix + reversal config."""
import paramiko, sys, json
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# 1. Verify frontend build hash
print("=== 1. FRONTEND BUILD ===")
r = run("docker exec mcx-live md5sum /app/dashboard-ui/dist/assets/index-6-YdCe5Z.js 2>/dev/null || docker exec mcx-live ls /app/dashboard-ui/dist/assets/")
print(r)

# 2. Verify DataProvider has the fix
print("\n=== 2. DATA PROVIDER FIX ===")
r = run('docker exec mcx-live grep -c "const base = prev" /app/dashboard-ui/src/store/DataProvider.tsx 2>/dev/null || docker exec mcx-live python3 -c "import hashlib; print(\'DataProvider hash:\', hashlib.md5(open(\'/app/dashboard-ui/dist/assets/index-6-YdCe5Z.js\',\'rb\').read()).hexdigest()[:12])"')
print(r)

# Check the built JS for the fix pattern
r2 = run('docker exec mcx-live grep -c "execution_mode:null,total_equity:0" /app/dashboard-ui/dist/assets/index-6-YdCe5Z.js 2>/dev/null || echo "pattern not found in minified"')
print(f"Minified check: {r2}")

# 3. Verify reversal config
print("\n=== 3. REVERSAL CONFIG ===")
r = run('docker exec mcx-live python3 -c "import json; c=json.load(open(\'/app/config/live_settings.json\')); print(json.dumps(c.get(\'live\',{}).get(\'reversal\',{}), indent=2))"')
print(r)

# 4. Verify engine hash
print("\n=== 4. ENGINE HASH ===")
r = run('docker exec mcx-live python3 -c "import hashlib; print(hashlib.md5(open(\'/app/trading_engine.py\',\'rb\').read()).hexdigest()[:12])"')
print(r)

# 5. Verify container health
print("\n=== 5. CONTAINER STATUS ===")
r = run("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(r)

# 6. Test all endpoints
print("\n=== 6. ALL ENDPOINTS ===")
endpoints = ["/api/overview", "/api/strategies", "/api/positions", "/api/orders", "/api/pnl", "/api/risk", "/api/indicators", "/api/health/system", "/api/market-data", "/api/live/dashboard"]
for ep in endpoints:
    r = run(f'docker exec mcx-live python3 -c "import urllib.request; r=urllib.request.urlopen(\'http://127.0.0.1:8001{ep}\', timeout=5); print(f\'HTTP {{r.status}}\')"')
    status = "OK" if "200" in r else "FAIL"
    print(f"  {status} {ep:30s} -> {r}")

# 7. Test WS
print("\n=== 7. WEBSOCKET ===")
r = run('''docker exec mcx-live python3 -c "
import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({'action':'subscribe','channels':['all']}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        print(f'OK type={d.get(\"type\",\"?\")} keys={list(d.get(\"data\",{}).keys())[:5]}')
asyncio.run(t())
"''', timeout=15)
print(r)

# 8. Verify live SHORT position still active
print("\n=== 8. LIVE POSITION ===")
r = run('docker exec mcx-live python3 -c "import json; d=json.load(open(\'/app/live/data/db/live_trading.db\',\'rb\')) if False else None" 2>&1 || true')
# Use API instead
r2 = run('docker exec mcx-live python3 -c "import urllib.request,json; r=urllib.request.urlopen(\'http://127.0.0.1:8001/api/positions\', timeout=5); d=json.loads(r.read()); pos=d.get(\'positions\',[]); print(json.dumps([{\"instrument\":p.get(\"instrument\"),\"side\":p.get(\"side\"),\"qty\":p.get(\"quantity\"),\"entry\":p.get(\"entry_price\"),\"sl_state\":p.get(\"sl_state\"),\"sl_trigger\":p.get(\"sl_trigger_price\")} for p in pos if p.get(\"is_open\')], indent=2))"')
print(r2)

ssh.close()
print("\nVERIFICATION COMPLETE")
