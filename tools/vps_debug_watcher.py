"""Quick debug for watcher eligibility."""
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

print(run("""docker exec mcx-live python3 -c "
import sys; sys.path.insert(0, '/app')
from execution.live.order_watcher import OrderWatcher, OrderWatchRecord
import time
cfg = {'market_fallback_enabled':True,'market_fallback_timeout_ms':30000,
    'limit_skip_policy':{'enabled':True,'max_age_ms':15000,'max_deviation_pct':0.5,
        'trigger_crossed_unfilled_ms':3000,'cancel_check_ms':2000},
    'max_reprices':2,'reprice_interval_ms':5000,'max_order_age_ms':60000,'max_price_deviation_pct':0.5}
w = OrderWatcher(config=cfg)
print('_tick_cfg keys:', list(w._tick_cfg.keys()))
print('_tick_cfg:', w._tick_cfg)
rec = OrderWatchRecord(
    internal_order_id='O-001', broker_order_id='B-001',
    correlation_id='MCX-001', strategy_id='gold_01',
    trade_id='T-001', signal_id='S-001',
    order_role='ENTRY', instrument='GOLDM', side='BUY',
    order_type='LIMIT', submitted_at=time.time()-31,
    last_event_at=time.time()-31, requested_price=72500.0,
    submitted_price=72500.0, requested_quantity=1,
    status='SUBMITTED',
)
now = time.time()
age_ms = (now - rec.submitted_at) * 1000.0
enabled = w._tick_cfg.get('market_fallback_enabled')
bo = rec.broker_order_id
timeout = w._tick_cfg.get('market_fallback_timeout_ms')
condition = age_ms >= timeout
print(f'enabled={enabled}, bo={bool(bo)}, age_ms={age_ms:.0f}, timeout={timeout}, condition={condition}')
result = enabled and bo and condition
print(f'Manual result: {result}')
actual = w._market_fallback_eligible(rec, now)
print(f'Actual result: {actual}')
" 2>&1"""))

ssh.close()
