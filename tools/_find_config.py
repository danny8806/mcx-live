"""Find config file and check key values."""
import paramiko, sys
sys.path.insert(0, str(__import__('pathlib').Path(__file__).resolve().parent.parent))
from tools.remedy_rebuild import load_env_file, MCX_TRADER_DIR

seed = load_env_file(MCX_TRADER_DIR / 'mcx-trader.env')
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('200.234.44.93', username='root', password=seed.get('VPS_PASS', ''), timeout=15)

def run(cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode('utf-8', errors='replace').strip()

# Find config file
print("=== FIND CONFIG ===")
print(run('docker exec mcx-live find / -name "live_settings.json" -not -path "*/node_modules/*" 2>/dev/null'))

# Find where config is loaded in engine
print("\n=== ENGINE CONFIG LOADING ===")
print(run('docker exec mcx-live grep -n "live_settings" /app/live/trading_engine.py 2>/dev/null || echo "not in live/trading_engine.py"'))
print(run('docker exec mcx-live grep -n "live_settings" /app/trading_engine.py 2>/dev/null | head -5'))

# Check engine_type and config_path
print("\n=== ENGINE INIT CONFIG PATH ===")
print(run('docker exec mcx-live grep -n "config_path\\|config_file\\|settings_file\\|Settings" /app/live/trading_engine.py 2>/dev/null | head -10'))
print(run('docker exec mcx-live grep -n "config_path\\|config_file\\|settings_file\\|Settings" /app/trading_engine.py 2>/dev/null | head -10'))

# Check the mounted volume
print("\n=== VOLUME MOUNTS ===")
print(run('docker inspect mcx-live --format="{{range .Mounts}}{{.Source}} -> {{.Destination}}{{println}}{{end}}"'))

# Check CORS middleware lines
print("\n=== CORS FULL BLOCK ===")
print(run('docker exec mcx-live sed -n "293,310p" /app/live/api.py'))

# Check how engine loads config
print("\n=== ENGINE CONFIG READ ===")
print(run('docker exec mcx-live grep -n "config\[" /app/trading_engine.py 2>/dev/null | head -5'))
print(run('docker exec mcx-live grep -n "self.config" /app/trading_engine.py 2>/dev/null | head -10'))

ssh.close()
