"""Quick fix: check Signal constructor, OrderWatchRecord.record(), DhanRestTransport."""
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

# 1. Signal constructor
print("=== Signal ===")
print(run('docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from strategies.types import Signal; print(inspect.signature(Signal))" 2>&1'))

# 2. OrderWatchRecord.record or watcher.record method
print("\n=== Watcher.record ===")
print(run('docker exec mcx-live python3 -c "import sys,inspect; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from execution.live.order_watcher import OrderWatcher; print(inspect.signature(OrderWatcher.record))" 2>&1'))

# 3. DhanRestTransport._http vs _session
print("\n=== DhanRestTransport attrs ===")
print(run('docker exec mcx-live python3 -c "import sys; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from execution.live.dhan_transport import DhanRestTransport; t=DhanRestTransport(client_id=chr(84)+chr(69)+chr(83)+chr(84)); print([a for a in dir(t) if not a.startswith(chr(95)+chr(95)) and not a.startswith(chr(95))])" 2>&1'))

# 4. Test LIMIT trigger_price assertion 
print("\n=== LIMIT trigger_price check ===")
print(run('docker exec mcx-live python3 -c "import sys; sys.path.insert(0,chr(47)+chr(97)+chr(112)+chr(112)); from execution.price_model import PricePreset; pm=PricePreset(entry_offset=0.0,sl_offset=0.0,tick_size=1.0); class S:\n def __init__(s,a,b,**kw):\n  for k,v in kw.items(): setattr(s,k,v)\n  s.signal_type=a;s.instrument=b\n p=pm.plan_for(S(chr(76)+chr(79)+chr(78)+chr(71),chr(71)+chr(79)+chr(76)+chr(68)+chr(77),tp=72500.0,sp=72400.0,md={chr(116)+chr(114)+chr(105)+chr(103)+chr(103)+chr(101)+chr(114)+chr(101)+chr(100):True}),chr(66)+chr(85)+chr(89)); print(f\"order_type={p.order_type} price={p.price} tp={p.trigger_price}\")" 2>&1'))

# 5. Test submit call_args format
print("\n=== submit call args format ===")
print(run("""docker exec mcx-live python3 -c "
import sys; sys.path.insert(0, '/app')
from unittest.mock import MagicMock
from execution.live.engine import LiveExecutionEngine
from execution.price_model import PricePreset
from strategies.types import Signal, SignalType

mock = MagicMock()
mock.place_market_order.return_value = {'broker_order_id':'B-TEST','status':'pending'}
pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=mock, price_preset=pm)

sig = Signal(SignalType.LONG, 'GOLDM', trigger_price=72500.0, stop_price=72400.0,
             strategy_id='gold_01', quantity=1, timestamp=1000.0, metadata={'triggered':True})
engine.submit_order(engine.create_order(sig, multiplier=10.0, trade_id='T-TEST'))

c = mock.place_market_order
print('call_args:', c.call_args)
print('args:', c.call_args[0])
print('kwargs:', c.call_args[1])
" 2>&1"""))

ssh.close()
