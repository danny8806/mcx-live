"""Deep signal flow analysis: full logs, strategy internals, error check."""
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
print("1. FULL ENGINE LOGS (last 500 lines)")
print("=" * 70)
r = run("docker logs mcx-live --tail 500 2>&1", timeout=30)
# Filter for important lines
lines = r.split('\n')
important = [l for l in lines if any(k in l.lower() for k in ['signal', 'order', 'fill', 'sl ', 'sl_', 'reversal', 'entry', 'exit', 'error', 'warning', 'gate', 'position', 'pending', 'cancel', 'strategy', 'bar ', 'candle'])]
for l in important[-80:]:
    print(l)
if not important:
    print("(no important lines found in 500)")
    print("Last 30 lines:")
    for l in lines[-30:]:
        print(l)

print("\n" + "=" * 70)
print("2. STRATEGY FULL STATE (WS)")
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
            print(f"--- {name} ---")
            print(json.dumps(strat, indent=2))
asyncio.run(t())
"""
with sftp.open('/tmp/ws_strats.py', 'w') as f:
    f.write(ws_script)
sftp.close()
ssh.exec_command('docker cp /tmp/ws_strats.py mcx-live:/tmp/ws_strats.py')
r = run('docker exec mcx-live python3 /tmp/ws_strats.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("3. ENGINE STATE SNAPSHOT")
print("=" * 70)
sftp = ssh.open_sftp()
ws_script2 = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        print("running:", s.get("running"))
        print("execution_mode:", s.get("execution_mode"))
        print("strategy_gates:", json.dumps(s.get("strategy_gates", {}), indent=2)[:500])
        risk = s.get("risk", {})
        print("risk:", json.dumps(risk, indent=2)[:300])
        acct = s.get("account", {})
        print("account:", json.dumps(acct, indent=2)[:400])
asyncio.run(t())
"""
with sftp.open('/tmp/ws_engine.py', 'w') as f:
    f.write(ws_script2)
sftp.close()
ssh.exec_command('docker cp /tmp/ws_engine.py mcx-live:/tmp/ws_engine.py')
r = run('docker exec mcx-live python3 /tmp/ws_engine.py', timeout=15)
print(r)

print("\n" + "=" * 70)
print("4. ERRORS IN LOGS")
print("=" * 70)
r = run("docker logs mcx-live --tail 1000 2>&1 | findstr /i 'error exception traceback failed timeout reject'")
if r:
    for l in r.split('\n')[-20:]:
        print(l)
else:
    print("(no errors found)")

print("\n" + "=" * 70)
print("5. POSITION DETAILS (from API)")
print("=" * 70)
r = run('''docker exec mcx-live python3 -c "import urllib.request,json; r=urllib.request.urlopen('http://127.0.0.1:8001/api/positions', timeout=5); d=json.loads(r.read()); print(json.dumps(d, indent=2)[:2000])"''')
print(r)

ssh.close()
print("\nSIGNAL FLOW ANALYSIS COMPLETE")
