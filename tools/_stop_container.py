"""Stop mcx-live container immediately."""
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

print("=== STOPPING mcx-live ===")
r = run("docker stop mcx-live && docker rm mcx-live")
print(r)

print("\n=== VERIFY CONTAINER STOPPED ===")
r = run("docker ps -a --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(r if r else "(no container found)")

ssh.close()
print("\nCONTAINER STOPPED")
