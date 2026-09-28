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
for i in range(20):
    time.sleep(6)
    st = run("docker inspect mcx-live --format '{{.State.Status}} health={{.State.Health.Status}} restarts={{.RestartCount}}'")
    if 'healthy' in st:
        print('container:', st)
        break
    print('waiting...', st)
print("image:", run("docker inspect mcx-live --format '{{.Config.Image}}'"))
print("=== engine health ===")
print(run("curl -s http://127.0.0.1:8001/api/live/health 2>&1 | python3 -c \"import sys,json; d=json.load(sys.stdin); print('mode=',d.get('execution_mode'),'strategies=',d.get('strategies'),'gate=',d.get('gate_enabled'),'broker=',d.get('broker'),'timestamp=',d.get('timestamp'))\""))
print("=== strategies gated ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
for s in d.get('strategies',[]):
    print(s.get('strategy_id'),'|',s.get('instrument'),'| tf=',s.get('fast_timeframe'),'| qty=',s.get('quantity'),'| enabled=',s.get('enabled'),'| state=',s.get('state'))
" 2>&1"""))
print("=== gate/config from settings ===")
print(run("""curl -s http://127.0.0.1:8001/api/settings 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
live=(d.get('live') or {})
print('master gate=',live.get('gate'),'live_trading_enabled=',live.get('live_trading_enabled'))
for sid,s in (d.get('strategies') or {}).items():
    print(sid, 'live_gate=', s.get('live_gate'), 'entry=', s.get('entry_enabled'), 'exit=', s.get('exit_enabled'))
" 2>&1"""))
ssh.close()
