"""Verify F23 deployed reversal config + strategy gap attribute."""
import json
import paramiko
import sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, label=""):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=30)
    out = stdout.read().decode('utf-8', errors='replace').strip()
    err = stderr.read().decode('utf-8', errors='replace').strip()
    if label:
        print(f"\n{'='*60}")
        print(f"[{label}]")
    if out:
        print(out)
    if err and 'warning' not in err.lower() and 'deprecat' not in err.lower():
        print(f"STDERR: {err}")
    return out

# 1. Full reversal config
run('docker exec mcx-live python3 -c "import json; c=json.load(open(\\\"/app/config/live_settings.json\\\")); print(json.dumps(c.get(\\\"reversal\\\", {}), indent=2))"',
    "1. REVERSAL CONFIG IN CONTAINER")

# 2. Top-level keys
run('docker exec mcx-live python3 -c "import json; c=json.load(open(\\\"/app/config/live_settings.json\\\")); print(list(c.keys()))"',
    "2. TOP-LEVEL CONFIG KEYS")

# 3. Verify strategy gap via engine introspection
run('docker exec mcx-live python3 -c "import json; f=open(\\\"/app/config/live_settings.json\\\"\'); c=json.load(f); print(\\\"live section:\\\", json.dumps(c.get(\\\"live\\\",{}), indent=2)[:500])"',
    "3. LIVE SECTION IN CONFIG")

# 4. Container image
run('docker inspect mcx-live --format \\\"{{.Config.Image}}\\\"',
    "4. CONTAINER IMAGE")

# 5. Container create time
run('docker inspect mcx-live --format \\\"{{.Created}}\\\"',
    "5. CONTAINER CREATED")

# 6. Check if there's a volume mount overriding config
run('docker inspect mcx-live --format \\\"{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}\\\"',
    "6. VOLUME MOUNTS")

# 7. Check the resolved config if any
run('docker exec mcx-live ls -la /app/config/live_settings*.json',
    "7. CONFIG FILES IN CONTAINER")

# 8. Verify the ACTUAL file content (first 100 chars of reversal section)
run('docker exec mcx-live python3 -c "import json; c=json.load(open(\\\"/app/config/live_settings.json\\\")); print(json.dumps({k:c[k] for k in list(c.keys())[-5:]}, indent=2)[:600])"',
    "8. LAST 5 KEYS OF CONFIG")

# 9. Container hash
run('docker exec mcx-live md5sum /app/trading_engine.py /app/strategies/instance.py /app/strategies/base_dema_strategy.py /app/config/live_settings.json',
    "9. CONTAINER FILE HASHES")

# 10. Local hashes
import subprocess
result = subprocess.run(['certutil', '-hashfile', 'C:\\Users\\pc\\Desktop\\MCX-TRADER-LIVE\\trading_engine.py', 'MD5'],
                       capture_output=True, text=True)
print(f"\n{'='*60}")
print("[10. LOCAL trading_engine.py HASH]")
print(result.stdout.strip())

result = subprocess.run(['certutil', '-hashfile', 'C:\\Users\\pc\\Desktop\\MCX-TRADER-LIVE\\config\\live_settings.json', 'MD5'],
                       capture_output=True, text=True)
print(f"\n[10b. LOCAL live_settings.json HASH]")
print(result.stdout.strip())

ssh.close()
print("\n\nDONE")
