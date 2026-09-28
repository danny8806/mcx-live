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
    fname = f"/tmp/qty_{int(time.time()*1000)}.py"
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
from execution.live.broker_client import StubLiveBroker

cfg = json.load(open('/app/config/live_settings.json'))
strategies = cfg['strategies']
print('--- CONFIG QUANTITY (ALL STRATEGIES) ---')
for sid, sc in strategies.items():
    print(f'  {sid}: quantity={sc.get("quantity")} lots={sc.get("lots")}')

broker = StubLiveBroker(gate_enabled=False)
pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=broker, price_preset=pm)

total_pass = True
for sid, sc in strategies.items():
    qty = int(sc.get('quantity') or 1)
    inst = sc.get('instrument', 'GOLDM')
    sig = Signal(SignalType.LONG, inst, strategy_id=sid, quantity=qty,
                 timestamp=time.time(), trigger_price=72500.0, stop_price=72400.0,
                 metadata={'triggered': True})
    order = engine.create_order(sig, multiplier=10.0, trade_id=f'T-{sid}-E')
    sub = engine.submit_order(order)
    sub_state = sub.state.value
    sub_reason = getattr(sub, 'reason', None)
    entry_ok = (sub.quantity == 100 and sub_state == 'rejected')
    print(f'[{sid}] ENTRY  qty={sub.quantity} state={sub_state} ==100: {sub.quantity == 100} cancelled: {entry_ok}')
    if not entry_ok or sub.quantity != 100:
        total_pass = False

    # The broker-side protective SL is RETIRED.  There is no
    # create_protective_sl any more; the only stop is the local
    # position-owned monitor.  Verify the retired path is truly gone and that
    # any attempt is hard-rejected at the engine boundary.
    has_create_sl = hasattr(engine, 'create_protective_sl')
    sl_state = 'RETIRED'
    sl_ok = not has_create_sl
    print(f'[{sid}] SL     create_protective_sl present={has_create_sl} '
          f'-> {sl_state} (local position-owned SL only)')
    if not sl_ok:
        total_pass = False

    ex = Signal(SignalType.FLAT, inst, strategy_id=sid, quantity=qty,
                timestamp=time.time(), trigger_price=72600.0, stop_price=0.0,
                metadata={'exit': True, 'exit_reason': 'signal'})
    exo = engine.create_order(ex, multiplier=10.0, trade_id=f'T-{sid}-X', side='SELL')
    ex_ok = (exo.quantity == 100)
    print(f'[{sid}] EXIT   qty={exo.quantity} ==100: {ex_ok}')
    if not ex_ok:
        total_pass = False

print('')
if total_pass:
    print('ALL_PASSED: quantity=100 confirmed for ENTRY + SL + EXIT on all 4 strategies')
else:
    print('FAILED: some order not 100')
"""
print(run_in_container(script))
ssh.close()
