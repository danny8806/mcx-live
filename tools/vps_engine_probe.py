import paramiko, sys, io, json
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
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
print("=== health ===")
print(run("curl -s http://127.0.0.1:8001/api/live/health 2>/dev/null | head -c 600"))
print()
print("=== strategies (bars/state) ===")
print(run("curl -s http://127.0.0.1:8001/api/strategies 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(s['strategy_id'], 'state=',s['state'],'bars=',s['bars_processed'],'trades=',s['trade_count'],'pend=',s.get('pending_entry')) for s in d['strategies']]\" 2>&1"))
print("=== snapshot signals_generated ===")
print(run("""docker exec mcx-live python3 -c "
import json
d = json.load(open('/app/live/data/db/live_system_state.json'))
for sid, s in d.get('strategies', {}).items():
    if s.get('enabled'):
        print(sid, 'state=', s.get('state'), 'bars_processed=', s.get('bars_processed'), 'signals_generated=', s.get('signals_generated'), 'fast_count=', s.get('fast_indicator_count'))
" 2>&1"""))
print("=== recent candle-close log lines (15m engine) ===")
print(run("docker logs mcx-live --since 25m 2>&1 | grep -iE 'candle|cross|15m|signal|evaluat' | grep -viE 'HTTP/1.1|GET /favicon' | tail -12"))
ssh.close()
