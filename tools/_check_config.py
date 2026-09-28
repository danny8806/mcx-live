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

print("=" * 60)
print("1. REVERSAL CONFIG")
print("=" * 60)
r = run("""docker exec mcx-live python3 -c "import json; c=json.load(open('/app/live_settings.json')); print(json.dumps(c.get('live',{}).get('reversal',{}), indent=2))" """)
print(r)

print("\n" + "=" * 60)
print("2. ROOT-LEVEL reversal KEY (should NOT exist)")
print("=" * 60)
r = run("""docker exec mcx-live python3 -c "import json; c=json.load(open('/app/live_settings.json')); print(json.dumps(c.get('reversal', 'NOT_FOUND'), indent=2))" """)
print(r)

print("\n" + "=" * 60)
print("3. CORS CONFIG")
print("=" * 60)
r = run("""docker exec mcx-live python3 -c "import os; print('CORS_ORIGINS=' + repr(os.getenv('CORS_ORIGINS', '(unset)')))" """)
print(r)

print("\n" + "=" * 60)
print("4. TRADING ENGINE CONFIG PATH CHECK")
print("=" * 60)
r = run("""docker exec mcx-live python3 -c "
import json
c = json.load(open('/app/live_settings.json'))
rev = ((c.get('live') or {}).get('reversal') or {})
print('live.reversal =', json.dumps(rev, indent=2))
print('root.reversal =', repr(c.get('reversal', 'NOT_FOUND')))
" """)
print(r)

print("\n" + "=" * 60)
print("5. ENGINE VERSION")
print("=" * 60)
r = run("docker exec mcx-live python3 -c \"import hashlib; print('trading_engine hash:', hashlib.md5(open('/app/trading_engine.py','rb').read()).hexdigest()[:12])\"")
print(r)

print("\n" + "=" * 60)
print("6. CORS MIDDLEWARE IN api.py")
print("=" * 60)
r = run("docker exec mcx-live python3 -c \"import ast,inspect; import live.api as m; lines=inspect.getsource(m).split(chr(10)); [print(f'{i+1}: {l}') for i,l in enumerate(lines) if 'CORS' in l or 'cors' in l or 'allow_origins' in l]\"")
print(r)

ssh.close()
