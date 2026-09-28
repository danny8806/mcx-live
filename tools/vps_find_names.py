"""Find actual class names in the container."""
import paramiko, sys, io
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
def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")

checks = [
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live import engine; print([x for x in dir(engine) if not x.startswith('_')])\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live import dhan_transport; print([x for x in dir(dhan_transport) if not x.startswith('_')])\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live import order_watcher; print([x for x in dir(order_watcher) if not x.startswith('_')])\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from strategies.types import StrategyState; print([s.name for s in StrategyState])\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); import risk; print(dir(risk))\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.price_model import PricePreset; import inspect; sig = inspect.signature(PricePreset.__init__); print(sig)\" 2>&1",
    "docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.price_model import PricePreset; pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0); print('PricePreset OK')\" 2>&1",
]

for c in checks:
    print(run(c))
    print()

ssh.close()
