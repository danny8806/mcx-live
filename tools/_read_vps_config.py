"""Read config and engine from inside docker container."""
import paramiko, sys, json
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

# Write helper script inside container, then run it
helper = """
import json, hashlib, os

print("=" * 60)
print("1. CONFIG FILE")
print("=" * 60)

cfg_path = '/app/config/live_settings.json'
print(f"Path: {cfg_path}")
print(f"Exists: {os.path.exists(cfg_path)}")

if os.path.exists(cfg_path):
    with open(cfg_path) as f:
        config = json.load(f)
    print(f"Keys: {list(config.keys())}")
    print()
    print("live.reversal:", json.dumps(config.get('live', {}).get('reversal', 'NOT_FOUND'), indent=2))
    print("root.reversal:", repr(config.get('reversal', 'NOT_FOUND')))
else:
    # Search for it
    import subprocess
    r = subprocess.run(['find', '/', '-name', 'live_settings.json', '-not', '-path', '*/node_modules/*'], capture_output=True, text=True, timeout=10)
    print("Found:", r.stdout)

print()
print("=" * 60)
print("2. ENGINE REVERSAL LINES")
print("=" * 60)

eng_path = '/app/trading_engine.py'
if os.path.exists(eng_path):
    with open(eng_path) as f:
        lines = f.readlines()
    h = hashlib.md5(open(eng_path, 'rb').read()).hexdigest()[:12]
    print(f"Hash: {h}")
    for i, line in enumerate(lines):
        if 'reversal' in line.lower():
            print(f"  {i+1}: {line.rstrip()}")
else:
    print(f"NOT FOUND: {eng_path}")

print()
print("=" * 60)
print("3. CORS + API ROUTES")
print("=" * 60)

api_path = '/app/live/api.py'
if os.path.exists(api_path):
    with open(api_path) as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if 'CORS' in line or 'cors' in line or 'allow_origins' in line:
            print(f"  api.py:{i+1}: {line.rstrip()}")
else:
    print(f"NOT FOUND: {api_path}")

print()
print("=" * 60)
print("4. Config class path")
print("=" * 60)

for root, dirs, files in os.walk('/app'):
    dirs[:] = [d for d in dirs if d not in ('node_modules', '.git', '__pycache__')]
    for f in files:
        if f == 'config.py':
            fp = os.path.join(root, f)
            with open(fp) as fh:
                content = fh.read()
            if 'live_settings' in content or 'def load' in content:
                print(f"  {fp}")
                for i, line in enumerate(content.split(chr(10))):
                    if 'live_settings' in line or 'def load' in line or 'def get' in line:
                        print(f"    {i+1}: {line.rstrip()}")
"""

# Upload script to container
sftp = ssh.open_sftp()
sftp.put = sftp.put  # noop
with sftp.open('/tmp/_diag_helper.py', 'w') as f:
    f.write(helper)
sftp.close()

# Copy and run
out, err = run('docker cp /tmp/_diag_helper.py mcx-live:/tmp/_diag_helper.py', timeout=10)
print(f"cp: {out} {err}")

out, err = run('docker exec mcx-live python3 /tmp/_diag_helper.py', timeout=30)
print(out)
if err and 'Warning' not in err:
    print("STDERR:", err[:500])

ssh.close()
