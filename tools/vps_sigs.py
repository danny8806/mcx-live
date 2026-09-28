"""Find constructor signatures."""
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

# Check __init__ signatures
checks = [
    'docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,\'/app\'); from execution.live.engine import LiveExecutionEngine; print(inspect.signature(LiveExecutionEngine.__init__))"',
    'docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,\'/app\'); from execution.live.order_watcher import OrderWatcher; print(inspect.signature(OrderWatcher.__init__))"',
    'docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,\'/app\'); from execution.live.order_watcher import OrderWatcher; print(inspect.signature(OrderWatcher._fresh_market_entry))"',
    'docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,\'/app\'); from execution.live.dhan_transport import LiveBrokerClient; print(inspect.signature(LiveBrokerClient.__init__))"',
]
for c in checks:
    print(run(c))

ssh.close()
