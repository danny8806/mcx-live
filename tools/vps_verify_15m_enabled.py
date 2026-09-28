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
    fname = f"/tmp/verify_{int(time.time()*1000)}.py"
    with sftp.open(fname, "w") as f:
        f.write(script)
    sftp.close()
    result = run(f"docker cp {fname} mcx-live:{fname} && docker exec mcx-live python3 {fname} 2>&1", timeout=timeout)
    run(f"docker exec mcx-live rm -f {fname}")
    return result

script = """
import sys, json, time
sys.path.insert(0, '/app')
from execution.live.engine import LiveExecutionEngine
from execution.price_model import PricePreset
from strategies.types import Signal, SignalType
from execution.live.dhan_transport import DhanRestTransport
import importlib

cfg = json.load(open('/app/config/live_settings.json'))
enabled = {k: v for k, v in cfg['strategies'].items() if v.get('enabled')}
disabled = {k: v for k, v in cfg['strategies'].items() if not v.get('enabled')}
print('=== ENABLED (15m) ===')
print('  ', ', '.join(f'{k}(qty={v.get("quantity")},lots={v.get("lots")},tf={v.get("fast_timeframe")})' for k, v in enabled.items()))
print('=== DISABLED (5m) ===')
print('  ', ', '.join(f'{k}(qty={v.get("quantity")},tf={v.get("fast_timeframe")})' for k, v in disabled.items()))

assert len(enabled) == 2, f'want 2 enabled, got {list(enabled)}'
for k, v in enabled.items():
    assert v.get('fast_timeframe') == '15m', f'{k} is not 15m'
    assert v.get('quantity') == 100 and v.get('lots') == 100, f'{k} qty/lots != 100'

# Fire through the real Dhan transport (gate OFF -> rejected/cancelled) for enabled strategies
settings = json.load(open('/app/config/settings.json'))
client = (settings.get('dhan') or {}).get('client_id') or 'TEST'
transport = DhanRestTransport(client_id=client, gate_enabled=False)
pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=transport, price_preset=pm)

all_ok = True
for sid, s in enabled.items():
    qty = int(s.get('quantity') or 1)
    inst = s.get('instrument', 'GOLDM')
    sig = Signal(SignalType.LONG, inst, strategy_id=sid, quantity=qty,
                 timestamp=time.time(), trigger_price=72500.0, stop_price=72400.0,
                 metadata={'triggered': True})
    order = engine.create_order(sig, multiplier=10.0, trade_id=f'T-{sid}')
    sub = engine.submit_order(order)
    ok = (sub.quantity == 100 and sub.state.value == 'rejected' and 'GATE' in (sub.reason or '').upper())
    print(f'[{sid}] ENTRY qty={sub.quantity} state={sub.state.value} -> 100&cancelled={ok}')
    all_ok = all_ok and ok
    sl = engine.create_protective_sl(strategy_id=sid, instrument=inst, side='SELL',
        quantity=sub.quantity, trigger_price=72400.0, trade_id=f'T-{sid}', entry_order_id=order.order_id)
    sls = engine.submit_order(sl)
    sok = (sl.quantity == 100 and sls.state.value == 'rejected')
    print(f'[{sid}] SL    qty={sl.quantity} state={sls.state.value} -> 100&cancelled={sok}')
    all_ok = all_ok and sok
    ex = Signal(SignalType.FLAT, inst, strategy_id=sid, quantity=qty,
                timestamp=time.time(), trigger_price=72600.0, stop_price=0.0,
                metadata={'exit': True, 'exit_reason': 'signal'})
    exo = engine.create_order(ex, multiplier=10.0, trade_id=f'T-{sid}-X', side='SELL')
    xok = (exo.quantity == 100)
    print(f'[{sid}] EXIT  qty={exo.quantity} -> 100={xok}')
    all_ok = all_ok and xok
    print('')

print('RESULT:', 'ALL_PASSED (2 enabled 15m strategies, qty=100 everywhere, cancelled via gate)' if all_ok else 'FAILED')
"""
print(run_in_container(script))
ssh.close()
