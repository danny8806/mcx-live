"""Disable all strategies - set gates to CLOSE_ONLY."""
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

# Check current state first
print("=== CURRENT STRATEGY STATE ===")
sftp = ssh.open_sftp()
ws_script = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        for name, strat in s.get("strategies", {}).items():
            print(f"  {name}: enabled={strat.get('enabled')} state={strat.get('state')} side={strat.get('position_side')}")
        print("  gates:", json.dumps(s.get("strategy_gates", {}), indent=2)[:500])
asyncio.run(t())
"""
with sftp.open('/tmp/check_state.py', 'w') as f:
    f.write(ws_script)
sftp.close()
ssh.exec_command('docker cp /tmp/check_state.py mcx-live:/tmp/check_state.py')
r = run('docker exec mcx-live python3 /tmp/check_state.py', timeout=15)
print(r)

# Set all strategy gates to CLOSE_ONLY via the API
print("\n=== SETTING ALL GATES TO CLOSE_ONLY ===")
sftp = ssh.open_sftp()
disable_script = """import urllib.request, json

# Get current settings
r = urllib.request.urlopen("http://127.0.0.1:8001/api/settings", timeout=5)
settings = json.loads(r.read())

# Set all strategies to enabled=false in the config
# This disables signal generation at the strategy level
strategies = settings.get("strategies", {})
for sid, scfg in strategies.items():
    print(f"  {sid}: currently enabled={scfg.get('enabled', True)}")

print()
print("To disable ALL strategies, we need to set the live_gate to CLOSE_ONLY for each.")
print("Use the control API...")
"""
with sftp.open('/tmp/disable_all.py', 'w') as f:
    f.write(disable_script)
sftp.close()
ssh.exec_command('docker cp /tmp/disable_all.py mcx-live:/tmp/disable_all.py')
r = run('docker exec mcx-live python3 /tmp/disable_all.py', timeout=15)
print(r)

# Use the engine's control API to set gates
print("\n=== DISABLING VIA ENGINE GATES ===")
sftp = ssh.open_sftp()
gate_script = """import urllib.request, json

strategies = ["gold_01", "gold_02", "silver_01", "silver_02"]

for sid in strategies:
    try:
        data = json.dumps({"action": "close_only", "strategy_id": sid}).encode()
        req = urllib.request.Request(
            "http://127.0.0.1:8001/api/live/control",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        r = urllib.request.urlopen(req, timeout=5)
        result = json.loads(r.read())
        print(f"  {sid}: {result}")
    except Exception as e:
        print(f"  {sid}: ERROR - {e}")
"""
with sftp.open('/tmp/gate_close.py', 'w') as f:
    f.write(gate_script)
sftp.close()
ssh.exec_command('docker cp /tmp/gate_close.py mcx-live:/tmp/gate_close.py')
r = run('docker exec mcx-live python3 /tmp/gate_close.py', timeout=15)
print(r)

# If that didn't work, try the strategies API
print("\n=== TRYING STRATEGY CONTROL API ===")
sftp = ssh.open_sftp()
ctrl_script = """import urllib.request, json

strategies = ["gold_01", "gold_02", "silver_01", "silver_02"]

for sid in strategies:
    for endpoint in [f"/api/strategies/{sid}/close_only", f"/api/live/strategies/{sid}/close_only"]:
        try:
            data = json.dumps({}).encode()
            req = urllib.request.Request(
                f"http://127.0.0.1:8001{endpoint}",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            r = urllib.request.urlopen(req, timeout=5)
            result = json.loads(r.read())
            print(f"  {sid} via {endpoint}: {result}")
            break
        except urllib.error.HTTPError as e:
            pass
        except Exception as e:
            print(f"  {sid} via {endpoint}: ERROR - {e}")
"""
with sftp.open('/tmp/ctrl_strat.py', 'w') as f:
    f.write(ctrl_script)
sftp.close()
ssh.exec_command('docker cp /tmp/ctrl_strat.py mcx-live:/tmp/ctrl_strat.py')
r = run('docker exec mcx-live python3 /tmp/ctrl_strat.py', timeout=15)
print(r)

# Check available API endpoints
print("\n=== AVAILABLE API ROUTES ===")
sftp = ssh.open_sftp()
routes_script = """import urllib.request, json
try:
    r = urllib.request.urlopen("http://127.0.0.1:8001/openapi.json", timeout=5)
    spec = json.loads(r.read())
    paths = list(spec.get("paths", {}).keys())
    control_paths = [p for p in paths if "control" in p.lower() or "strateg" in p.lower() or "gate" in p.lower() or "disable" in p.lower()]
    print("Control-related endpoints:")
    for p in control_paths:
        methods = list(spec["paths"][p].keys())
        print(f"  {p} [{', '.join(methods)}]")
except Exception as e:
    print(f"Error: {e}")
"""
with sftp.open('/tmp/routes.py', 'w') as f:
    f.write(routes_script)
sftp.close()
ssh.exec_command('docker cp /tmp/routes.py mcx-live:/tmp/routes.py')
r = run('docker exec mcx-live python3 /tmp/routes.py', timeout=15)
print(r)

ssh.close()
