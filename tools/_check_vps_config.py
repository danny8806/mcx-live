"""Check live_settings.json reversal config and CORS."""
import json, sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
import paramiko
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

# Read config directly from the host filesystem
_, stdout, _ = ssh.exec_command('cat /root/mcx-trader/data/live_settings.json', timeout=10)
raw = stdout.read().decode()
if not raw.strip():
    # Try alternate path
    _, stdout, _ = ssh.exec_command('docker exec mcx-live cat /app/live_settings.json', timeout=10)
    raw = stdout.read().decode()

if raw.strip():
    config = json.loads(raw)
    print("=== REVERSAL CONFIG ===")
    print("root.reversal:", repr(config.get('reversal', 'NOT_FOUND')))
    print("live.reversal:", json.dumps(config.get('live', {}).get('reversal', 'NOT_FOUND'), indent=2))
    print()
    print("=== ALL TOP-LEVEL KEYS ===")
    print(list(config.keys()))
else:
    print("ERROR: Could not read config")

print()
print("=== CORS CHECK ===")
_, stdout, _ = ssh.exec_command('docker exec mcx-live grep -n "CORS" /app/live/api.py', timeout=10)
print(stdout.read().decode())

print()
print("=== ENV CORS ===")
_, stdout, _ = ssh.exec_command('docker exec mcx-live env | grep CORS', timeout=10)
cors = stdout.read().decode().strip()
print(cors if cors else "(no CORS env var set)")

ssh.close()
