"""Fix cancel_order mock + market_fallback time check."""
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

# Check cancel_order return format
print("=== cancel_order return ===")
print(run("""docker exec mcx-live python3 -c "
import sys; sys.path.insert(0, '/app')
from unittest.mock import MagicMock
from execution.live.engine import LiveExecutionEngine
from execution.price_model import PricePreset
from strategies.types import Signal, SignalType

mock = MagicMock()
mock.place_market_order.return_value = {'broker_order_id':'B-CAN','status':'pending'}
mock.cancel_order.return_value = {'status':'cancelled'}
pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=mock, price_preset=pm)

sig = Signal(SignalType.LONG,'GOLDM',strategy_id='gold_01',quantity=1,timestamp=1000.0,
             trigger_price=72500.0,stop_price=72400.0,metadata={'triggered':True})
o = engine.create_order(sig, multiplier=10.0, trade_id='T-CAN')
engine.submit_order(o)
result = engine.cancel_order(o.order_id)
print(f'cancel_result: {result}, type={type(result).__name__}')
" 2>&1"""))

# Check _market_fallback_eligible internals
print("\n=== _market_fallback_eligible ===")
print(run("""docker exec mcx-live python3 -c "
import sys; sys.path.insert(0, '/app')
import inspect
from execution.live.order_watcher import OrderWatcher
src = inspect.getsource(OrderWatcher._market_fallback_eligible)
print(src[:500])
" 2>&1"""))

ssh.close()
