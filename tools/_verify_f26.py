"""Verify f26 fix and monitor live signal flow."""
import paramiko, sys, json, time
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=20):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# 1. Verify fix is in deployed code
print("=" * 60)
print("1. DEPLOYED CODE VERIFICATION")
print("=" * 60)
r = run('docker exec mcx-live python3 -c "import hashlib; print(\'trading_engine:\', hashlib.md5(open(\'/app/trading_engine.py\',\'rb\').read()).hexdigest()[:12]); print(\'order_manager:\', hashlib.md5(open(\'/app/execution/order_manager.py\',\'rb\').read()).hexdigest()[:12])"')
print(r)
r = run('docker exec mcx-live grep -n "remove_pending" /app/execution/order_manager.py /app/trading_engine.py 2>/dev/null')
print(r)

# 2. Engine hashes match local
print("\n" + "=" * 60)
print("2. LOCAL vs DEPLOYED HASH COMPARISON")
print("=" * 60)
import hashlib
for fname in ['trading_engine.py', 'execution/order_manager.py']:
    local = hashlib.md5(open(fname, 'rb').read()).hexdigest()[:12]
    r = run(f'docker exec mcx-live python3 -c "import hashlib; print(hashlib.md5(open(\'/app/{fname}\',\'rb\').read()).hexdigest()[:12])"')
    match = "MATCH" if local == r else "MISMATCH"
    print(f"  {fname}: local={local} deployed={r} [{match}]")

# 3. Current live state
print("\n" + "=" * 60)
print("3. LIVE ENGINE STATE")
print("=" * 60)
sftp = ssh.open_sftp()
ws_script = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        print("running:", s.get("running"))
        print("mode:", s.get("execution_mode"))
        for name, strat in s.get("strategies", {}).items():
            print(f"  {name}: state={strat.get('state')} side={strat.get('position_side')} enabled={strat.get('enabled')} signals={strat.get('signals_generated')} bars={strat.get('bars_processed')}")
        pos = s.get("positions", {}).get("open_positions", {})
        for pid, p in pos.items():
            print(f"  POSITION: {p.get('strategy_id')} {p.get('side')} qty={p.get('quantity')} entry={p.get('average_entry')} sl={p.get('sl_trigger_price')} sl_state={p.get('sl_state')}")
        acct = s.get("account", {})
        print(f"  equity={acct.get('equity')} pnl={acct.get('net_pnl')} margin={acct.get('used_margin')}")
asyncio.run(t())
"""
with sftp.open('/tmp/verify_f26.py', 'w') as f:
    f.write(ws_script)
sftp.close()
ssh.exec_command('docker cp /tmp/verify_f26.py mcx-live:/tmp/verify_f26.py')
r = run('docker exec mcx-live python3 /tmp/verify_f26.py', timeout=15)
print(r)

# 4. Recent logs (any new signals?)
print("\n" + "=" * 60)
print("4. RECENT ENGINE LOGS")
print("=" * 60)
r = run("docker logs mcx-live --tail 50 2>&1 | findstr /i 'signal reversal reject fill close entry lifecycle'")
print(r if r else "(no signal events yet)")

# 5. Container health
print("\n" + "=" * 60)
print("5. CONTAINER HEALTH")
print("=" * 60)
r = run("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(r)

ssh.close()
print("\nVERIFICATION COMPLETE")
