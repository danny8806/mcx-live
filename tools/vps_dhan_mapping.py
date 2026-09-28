import paramiko, io, sys
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)
def run(cmd, t=60):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

probe = r'''
import sys, os, json
sys.path.insert(0, "/app")
os.chdir("/app")
from config import Config
cfg = Config.load()
dhan = dict(cfg.get("dhan") or {})
rets = cfg["instruments"]
inst_str = {"GOLDM": ["gold_02"], "SILVERM": ["silver_01"]}
from execution.live.dhan_transport import DhanRestTransport
tr = DhanRestTransport.from_config(
    dhan_config=dhan, instruments={inst: rets[inst] for inst in ("GOLDM", "SILVERM")},
    instrument_strategies=inst_str, gate_enabled=True)
print("=== day_order_book ===")
ob = tr.day_order_book()
print(json.dumps(ob)[:5000])
print()
print("=== positions ===")
try:
    print(json.dumps(tr.positions())[:2000])
except Exception as ex:
    print("ERR", ex)
print()
print("=== account_status ===")
try:
    print(json.dumps(tr.account_status())[:1500])
except Exception as ex:
    print("ERR", ex)
print()
print("=== order_statuses() (per-order) ===")
try:
    print(json.dumps(tr.order_statuses())[:3000])
except Exception as ex:
    print("ERR", ex)
'''
sf = ssh.open_sftp()
with sf.open("/tmp/dhan_probe.py", "w") as f:
    f.write(probe)
sf.close()
print(run("docker cp /tmp/dhan_probe.py mcx-live:/tmp/dhan_probe.py && docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); exec(open('/tmp/dhan_probe.py').read())\"", 180))
ssh.close()