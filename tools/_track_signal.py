"""Track live signal flow: engine logs, signals, orders, WS events."""
import paramiko, sys, json, time
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=15):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

print("=" * 70)
print("1. RECENT ENGINE LOGS (signals, orders, fills)")
print("=" * 70)
r = run("docker logs mcx-live --tail 200 2>&1 | findstr /i 'signal Signal SIGNAL order Order ORDER fill Fill FILL reversal Reversal entry Entry exit Exit SL sl stop'")
print(r if r else "(no signal-related logs found)")

print("\n" + "=" * 70)
print("2. ENGINE STATE (strategies + positions)")
print("=" * 70)
r = run('''docker exec mcx-live python3 -c "import urllib.request,json; r=urllib.request.urlopen('http://127.0.0.1:8001/api/strategies', timeout=5); d=json.loads(r.read()); [print(f'{s.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(121)+chr(95)+chr(105)+chr(100),chr(63)):20s} inst={s.get(chr(105)+chr(110)+chr(115)+chr(116)+chr(114)+chr(117)+chr(109)+chr(101)+chr(110)+chr(116),chr(63)):10s} side={s.get(chr(112)+chr(111)+chr(115)+chr(105)+chr(116)+chr(105)+chr(111)+chr(110)+chr(95)+chr(115)+chr(105)+chr(100)+chr(101),chr(63)):6s} state={s.get(chr(115)+chr(116)+chr(97)+chr(116)+chr(101),chr(63)):15s} stop={s.get(chr(115)+chr(116)+chr(111)+chr(112)+chr(95)+chr(112)+chr(114)+chr(105)+chr(99)+chr(101),chr(8216))} pending={s.get(chr(112)+chr(101)+chr(110)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(116)+chr(114)+chr(121),chr(8216))}') for s in d.get('strategies',[])]"''')
print(r)

print("\n" + "=" * 70)
print("3. OPEN ORDERS")
print("=" * 70)
r = run('''docker exec mcx-live python3 -c "import urllib.request,json; r=urllib.request.urlopen('http://127.0.0.1:8001/api/orders', timeout=5); d=json.loads(r.read()); [print(f'{o.get(chr(111)+chr(114)+chr(100)+chr(101)+chr(114)+chr(95)+chr(116)+chr(121)+chr(112)+chr(101),chr(63)):15s} {o.get(chr(115)+chr(105)+chr(100)+chr(101),chr(63)):6s} {o.get(chr(115)+chr(116)+chr(97)+chr(116)+chr(101),chr(63)):15s} trigger={o.get(chr(116)+chr(114)+chr(105)+chr(103)+chr(103)+chr(101)+chr(114)+chr(95)+chr(112)+chr(114)+chr(105)+chr(99)+chr(101),chr(63))} price={o.get(chr(112)+chr(114)+chr(105)+chr(99)+chr(101),chr(63))} role={o.get(chr(111)+chr(114)+chr(100)+chr(101)+chr(114)+chr(95)+chr(114)+chr(111)+chr(108)+chr(101),chr(63))}') for o in d.get('orders',[])]"''')
print(r if r else "(no orders)")

print("\n" + "=" * 70)
print("4. LIVE EVENTS (last 20)")
print("=" * 70)
r = run('''docker exec mcx-live python3 -c "import urllib.request,json; r=urllib.request.urlopen('http://127.0.0.1:8001/api/audit', timeout=5); d=json.loads(r.read()); [print(f'{e.get(chr(116)+chr(105)+chr(109)+chr(101)+chr(115)+chr(116)+chr(97)+chr(109)+chr(112),chr(63))[-8:]} {e.get(chr(101)+chr(118)+chr(101)+chr(110)+chr(116)+chr(95)+chr(116)+chr(121)+chr(112)+chr(101),chr(63)):30s} {e.get(chr(109)+chr(101)+chr(115)+chr(115)+chr(97)+chr(103)+chr(101),chr(63))[:60]}') for e in d.get('entries',[])[-20:]]"''')
print(r if r else "(no events)")

print("\n" + "=" * 70)
print("5. WS ENGINE STATE (first engine_state snapshot)")
print("=" * 70)
sftp = ssh.open_sftp()
ws_script = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        print("strategies:", json.dumps(s.get("strategies", {}), indent=2)[:500])
        print("positions:", json.dumps(s.get("positions", {}), indent=2)[:500])
        print("account:", json.dumps(s.get("account", {}), indent=2)[:500])
asyncio.run(t())
"""
with sftp.open('/tmp/ws_snapshot.py', 'w') as f:
    f.write(ws_script)
sftp.close()
ssh.exec_command('docker cp /tmp/ws_snapshot.py mcx-live:/tmp/ws_snapshot.py')
r = run('docker exec mcx-live python3 /tmp/ws_snapshot.py', timeout=15)
print(r)

ssh.close()
print("\nLIVE FLOW TRACKING COMPLETE")
