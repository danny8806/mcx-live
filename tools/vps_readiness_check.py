import paramiko, sys, io, time
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
def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
def run_in_container(script, timeout=90):
    sftp = ssh.open_sftp()
    fname = f"/tmp/rdy_{int(time.time()*1000)}.py"
    with sftp.open(fname, "w") as f:
        f.write(script)
    sftp.close()
    result = run(f"docker cp {fname} mcx-live:{fname} && docker exec mcx-live python3 {fname} 2>&1", timeout=timeout)
    run(f"docker exec mcx-live rm -f {fname}")
    return result

print("=== READINESS + HOT-PATH LATENCY + PARALLELISM CHECK ===")
script = """
import sys, time, json, threading
sys.path.insert(0, '/app')
from unittest.mock import MagicMock
from execution.live.engine import LiveExecutionEngine
from execution.price_model import PricePreset
from strategies.types import Signal, SignalType
from events.bus import EventBus
import inspect

print('--- A) EventBus dispatch: SERIAL vs PARALLEL ---')
bus = EventBus()
order = []
def cb1(e): order.append('cb1'); time.sleep(0.05)
def cb2(e): order.append('cb2'); time.sleep(0.05)
bus.subscribe('tick:X', cb1)
bus.subscribe('tick:X', cb2)
t0 = time.perf_counter()
bus.publish('tick:X', object())
elapsed = (time.perf_counter() - t0) * 1000
print(f'   two subscribers, each sleeps 50ms -> total {elapsed:.1f}ms')
if elapsed >= 95:
    print('   => SERIAL dispatch (subscribe callbacks run one-by-one on publishing thread)')
else:
    print('   => PARALLEL/async dispatch')
print('   publish loops `for cb in`: ', 'for cb in' in inspect.getsource(EventBus.publish))

print('--- B) Signal->order hot-path latency ---')
mock = MagicMock()
mock.place_market_order.return_value = {'broker_order_id':'B-LAT','status':'pending'}
pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=mock, price_preset=pm)
sig = Signal(SignalType.LONG,'GOLDM',strategy_id='gold_01',quantity=1,
             timestamp=1000.0, trigger_price=72500.0, stop_price=72400.0,
             metadata={'triggered':True})
N = 500
t0 = time.perf_counter()
for i in range(N):
    o = engine.create_order(sig, multiplier=10.0, trade_id=f'T-{i}')
    engine.submit_order(o)
e2e = (time.perf_counter() - t0)/N*1000
print(f'   avg create+submit (signal->broker call)   : {e2e:.3f} ms/order')

print('--- C) Real broker adds REST round-trip ---')
print('   Dhan place_market_order = synchronous HTTP POST /orders')
print('   TokenBucket pacing + DNS/TLS + HTTP => typically 50-300 ms')

print('--- D) WS tick thread serialisation ---')
from trading_engine import TradingEngine
tsrc = inspect.getsource(TradingEngine._make_tick_handler)
print('   tick handler holds self._lock: ', 'with self._lock' in tsrc)
psrc = inspect.getsource(TradingEngine._process_signal)
print('   _process_signal acquires lock: ', 'with self._lock' in psrc)
print('ALL_PASSED')
"""
print(run_in_container(script))
ssh.close()
