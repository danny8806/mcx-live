"""Find SignalType values + OrderWatchRecord fields + DhanRestTransport."""
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

print("=== SignalType ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from strategies.types import SignalType; print([s.name for s in SignalType])\" 2>&1"))

print("\n=== OrderWatchRecord fields ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live.order_watcher import OrderWatchRecord; import dataclasses; print([f.name for f in dataclasses.fields(OrderWatchRecord)])\" 2>&1"))

print("\n=== DhanRestTransport ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0,'/app'); from execution.live.dhan_transport import DhanRestTransport; import inspect; print(inspect.signature(DhanRestTransport.__init__))\" 2>&1"))

print("\n=== Signal constructor ===")
print(run("docker exec mcx-live python3 -c \"import sys; sys.path.insert(0, '/app'); from strategies.types import Signal, SignalType; s = Signal(SignalType.LONG, 'GOLDM', strategy_id='g1', quantity=1, timestamp=1.0, trigger_price=72500.0, stop_price=72400.0); print(f'signal_type={s.signal_type}, name={s.signal_type.name}')\" 2>&1"))

ssh.close()
