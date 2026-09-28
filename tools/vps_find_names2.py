"""Find OrderWatcher methods + risk engine."""
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

# OrderWatcher methods
print("=== OrderWatcher methods ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live.order_watcher import OrderWatcher; print([m for m in dir(OrderWatcher) if not m.startswith('__')])\" 2>&1"))

# LiveExecutionEngine methods
print("\n=== LiveExecutionEngine methods ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live.engine import LiveExecutionEngine; print([m for m in dir(LiveExecutionEngine) if not m.startswith('__')])\" 2>&1"))

# LiveBrokerClient (Dhan transport)
print("\n=== LiveBrokerClient methods ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live.dhan_transport import LiveBrokerClient; print([m for m in dir(LiveBrokerClient) if not m.startswith('__')])\" 2>&1"))

# Risk engine search
print("\n=== Risk engine module ===")
print(run("docker exec mcx-live find /app -name 'risk*' -type f 2>/dev/null | head -10"))

ssh.close()
