"""Full signal flow trace: candle -> indicator -> signal -> gate -> order -> Dhan."""
import paramiko, sys, json, time
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=20):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    return out

# 1. Get ALL logs (not filtered)
print("=" * 70)
print("1. ALL ENGINE LOGS (last 500)")
print("=" * 70)
r = run("docker logs mcx-live --tail 500 2>&1", timeout=30)
lines = r.split('\n')
# Show everything - let user see the full picture
for l in lines[-100:]:
    print(l)

print("\n" + "=" * 70)
print("2. ALL LOGS - GREP SIGNAL/ORDER/BAR/FILL/GATE/SUBMIT/REJECT")
print("=" * 70)
r = run("docker logs mcx-live 2>&1 | findstr /i 'signal bar closed order submit fill gate reject cancel sl reversal entry exit position pending armed candle tick indicator generate emit'", timeout=30)
lines = r.split('\n')
for l in lines[-60:]:
    print(l)

print("\n" + "=" * 70)
print("3. CHECK IF MARKET IS OPEN NOW")
print("=" * 70)
sftp = ssh.open_sftp()
check_market = """import urllib.request, json
try:
    r = urllib.request.urlopen("http://127.0.0.1:8001/api/market-data", timeout=5)
    d = json.loads(r.read())
    print(json.dumps(d, indent=2)[:1500])
except Exception as e:
    print(f"Error: {e}")
"""
with sftp.open('/tmp/check_market.py', 'w') as f:
    f.write(check_market)
sftp.close()
ssh.exec_command('docker cp /tmp/check_market.py mcx-live:/tmp/check_market.py')
r = run('docker exec mcx-live python3 /tmp/check_market.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("4. LIVE CANDLE FEED (last 10 websocket ticks)")
print("=" * 70)
r = run("docker logs mcx-live --tail 2000 2>&1 | findstr /i 'CandleFetcher DEDUP tick ltp'", timeout=30)
lines = r.split('\n')
for l in lines[-20:]:
    print(l)

print("\n" + "=" * 70)
print("5. STRATEGY INDICATOR VALUES (from WS)")
print("=" * 70)
sftp = ssh.open_sftp()
ws_script = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        for name, strat in s.get("strategies", {}).items():
            print(f"=== {name} ({strat.get('instrument')}) state={strat.get('state')} ===")
            print(f"  enabled={strat.get('enabled')} bars={strat.get('bars_processed')} signals={strat.get('signals_generated')}")
            print(f"  fast_close={strat.get('prev_fast_close')} htf_value={strat.get('slow_htf_value')}")
            print(f"  mid_htf={strat.get('mid_htf_value')} prev_htf={strat.get('prev_htf_value')}")
            print(f"  position_side={strat.get('position_side')} stop_price={strat.get('stop_price')}")
            print(f"  pending_entry={strat.get('pending_entry')} has_pending={strat.get('has_pending')}")
            print(f"  last_exit={strat.get('last_exit_reason')} trade_id={strat.get('current_trade_id')}")
        print()
        # Also show indicators endpoint
asyncio.run(t())
"""
with sftp.open('/tmp/ws_strats2.py', 'w') as f:
    f.write(ws_script)
sftp.close()
ssh.exec_command('docker cp /tmp/ws_strats2.py mcx-live:/tmp/ws_strats2.py')
r = run('docker exec mcx-live python3 /tmp/ws_strats2.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("6. INDICATORS (SILVERM)")
print("=" * 70)
sftp = ssh.open_sftp()
ind_script = """import urllib.request, json
r = urllib.request.urlopen("http://127.0.0.1:8001/api/indicators", timeout=5)
d = json.loads(r.read())
inds = d.get("indicators", {})
for key, val in inds.items():
    if "SILVER" in key.upper() or "silver" in key.lower():
        print(f"{key}: {json.dumps(val, indent=2)[:300]}")
# Also show all keys
print("\\nAll indicator keys:", list(inds.keys())[:20])
"""
with sftp.open('/tmp/ind_check.py', 'w') as f:
    f.write(ind_script)
sftp.close()
ssh.exec_command('docker cp /tmp/ind_check.py mcx-live:/tmp/ind_check.py')
r = run('docker exec mcx-live python3 /tmp/ind_check.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("7. ENGINE PROCESSED BAR COUNT + POLLER STATE")
print("=" * 70)
sftp = ssh.open_sftp()
poll_script = """import urllib.request, json
try:
    r = urllib.request.urlopen("http://127.0.0.1:8001/api/health/system", timeout=5)
    d = json.loads(r.read())
    print("Health:", json.dumps(d, indent=2)[:800])
except Exception as e:
    print(f"Error: {e}")
"""
with sftp.open('/tmp/poll_check.py', 'w') as f:
    f.write(poll_script)
sftp.close()
ssh.exec_command('docker cp /tmp/poll_check.py mcx-live:/tmp/poll_check.py')
r = run('docker exec mcx-live python3 /tmp/poll_check.py', timeout=15)
print(r)

ssh.close()
print("\nFULL FLOW TRACE COMPLETE")
