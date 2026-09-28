"""Disable all strategies via /api/strategies/{id}/control endpoint."""
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

# First check what control options are available
print("=== CHECKING CONTROL ENDPOINT SCHEMA ===")
sftp = ssh.open_sftp()
schema_script = """import urllib.request, json
try:
    r = urllib.request.urlopen("http://127.0.0.1:8001/openapi.json", timeout=5)
    spec = json.loads(r.read())
    control = spec.get("paths", {}).get("/api/strategies/{strategy_id}/control", {})
    post = control.get("post", {})
    print("Summary:", post.get("summary", ""))
    print("Description:", post.get("description", ""))
    rb = post.get("requestBody", {}).get("content", {}).get("application/json", {}).get("schema", {})
    print("Request body schema:", json.dumps(rb, indent=2)[:500])
except Exception as e:
    print(f"Error: {e}")
"""
with sftp.open('/tmp/schema_check.py', 'w') as f:
    f.write(schema_script)
sftp.close()
ssh.exec_command('docker cp /tmp/schema_check.py mcx-live:/tmp/schema_check.py')
r = run('docker exec mcx-live python3 /tmp/schema_check.py', timeout=15)
print(r)

# Now disable all strategies - try close_only action
print("\n=== DISABLING ALL STRATEGIES ===")
sftp = ssh.open_sftp()
disable_script = """import urllib.request, json

strategies = ["gold_01", "gold_02", "silver_01", "silver_02"]

for sid in strategies:
    for action in ["close_only", "disable"]:
        try:
            data = json.dumps({"action": action}).encode()
            req = urllib.request.Request(
                f"http://127.0.0.1:8001/api/strategies/{sid}/control",
                data=data,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            r = urllib.request.urlopen(req, timeout=5)
            result = json.loads(r.read())
            print(f"  {sid}: action={action} -> {json.dumps(result)[:200]}")
            break
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode()[:200]
            except:
                pass
            print(f"  {sid}: action={action} -> HTTP {e.code} {body}")
        except Exception as e:
            print(f"  {sid}: action={action} -> ERROR {e}")
"""
with sftp.open('/tmp/disable_v2.py', 'w') as f:
    f.write(disable_script)
sftp.close()
ssh.exec_command('docker cp /tmp/disable_v2.py mcx-live:/tmp/disable_v2.py')
r = run('docker exec mcx-live python3 /tmp/disable_v2.py', timeout=15)
print(r)

# Verify final state
print("\n=== VERIFY FINAL STATE ===")
sftp = ssh.open_sftp()
verify_script = """import asyncio, json, websockets
async def t():
    async with websockets.connect('ws://127.0.0.1:8001/ws') as ws:
        await ws.send(json.dumps({"action":"subscribe","channels":["all"]}))
        msg = await asyncio.wait_for(ws.recv(), timeout=5)
        d = json.loads(msg)
        s = d.get("data", {})
        for name, strat in s.get("strategies", {}).items():
            print(f"  {name}: enabled={strat.get('enabled')} state={strat.get('state')} side={strat.get('position_side')}")
        gates = s.get("strategy_gates", {})
        for name, gate in gates.items():
            print(f"  GATE {name}: live_gate={gate.get('live_gate')} entry={gate.get('entry_enabled')} exit={gate.get('exit_enabled')} reversal={gate.get('reversal_enabled')} sl={gate.get('sl_enabled')}")
asyncio.run(t())
"""
with sftp.open('/tmp/verify_state.py', 'w') as f:
    f.write(verify_script)
sftp.close()
ssh.exec_command('docker cp /tmp/verify_state.py mcx-live:/tmp/verify_state.py')
r = run('docker exec mcx-live python3 /tmp/verify_state.py', timeout=15)
print(r)

ssh.close()
