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
    fname = f"/tmp/real_{int(time.time()*1000)}.py"
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

cfg = json.load(open('/app/config/live_settings.json'))
sc = cfg['strategies']
print('=== CONFIG: quantity/lots per strategy ===')
for k, v in sc.items():
    print(f'  {k}: quantity={v.get("quantity")} lots={v.get("lots")} enabled={v.get("enabled")}')

# Real Dhan transport, master gate OFF (same as production right now)
settings = json.load(open('/app/config/settings.json'))
dhan_client = (settings.get('dhan') or {}).get('client_id') or 'TEST'
transport = DhanRestTransport(client_id=dhan_client, gate_enabled=False)

pm = PricePreset(entry_offset=0.0, sl_offset=0.0, tick_size=1.0)
engine = LiveExecutionEngine(broker=transport, price_preset=pm)

all_ok = True
for sid, s in sc.items():
    qty = int(s.get('quantity') or 1)
    inst = s.get('instrument', 'GOLDM')

    # ENTRY (triggered -> LIMIT)
    sig = Signal(SignalType.LONG, inst, strategy_id=sid, quantity=qty,
                 timestamp=time.time(), trigger_price=72500.0, stop_price=72400.0,
                 metadata={'triggered': True})
    order = engine.create_order(sig, multiplier=10.0, trade_id=f'T-{sid}-E')
    sub = engine.submit_order(order)
    e_ok = (sub.quantity == 100 and sub.state.value == 'rejected'
            and 'GATE' in (sub.reason or '').upper())
    print(f'[{sid}] ENTRY  qty={sub.quantity} state={sub.state.value} reason={sub.reason} -> 100&cancelled={e_ok}')
    all_ok = all_ok and e_ok and sub.quantity == 100

    # Protective SL is RETIRED: the only stop is the local position-owned
    # monitor.  Assert the broker-side path no longer exists at all.
    s_ok = not hasattr(engine, 'create_protective_sl')
    print(f'[{sid}] SL     create_protective_sl present='
          f'{not s_ok} -> retired (local position-owned SL only)')
    all_ok = all_ok and s_ok

    # EXIT
    ex = Signal(SignalType.FLAT, inst, strategy_id=sid, quantity=qty,
                timestamp=time.time(), trigger_price=72600.0, stop_price=0.0,
                metadata={'exit': True, 'exit_reason': 'signal'})
    exo = engine.create_order(ex, multiplier=10.0, trade_id=f'T-{sid}-X', side='SELL')
    x_ok = (exo.quantity == 100)
    print(f'[{sid}] EXIT   qty={exo.quantity} -> 100={x_ok}')
    all_ok = all_ok and x_ok and exo.quantity == 100

print('')
print('RESULT:', 'ALL_PASSED qty=100 everywhere + all cancelled/rejected (gate OFF)'
      if all_ok else 'FAILED')
"""
print(run_in_container(script))
ssh.close()
