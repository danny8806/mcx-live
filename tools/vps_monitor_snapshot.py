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
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace").strip()
    return out.encode("ascii", "replace").decode("ascii")
def py(cmd):
    return run(f"python3 -c \"{cmd}\"")
print("=== SNAPSHOT t=0 ===")
print("--- market data ---")
print(run("curl -s http://127.0.0.1:8001/api/market-data | python3 -c \"import sys,json; d=json.load(sys.stdin); print('ws=',d.get('ws_connected'),'ticks=',d['adapter_stats'].get('tick_count'),'ltp=',d.get('instruments'))\" 2>&1"))
print("--- strategies ---")
print(run("""curl -s http://127.0.0.1:8001/api/strategies | python3 -c "
import sys,json
d=json.load(sys.stdin)
for s in d.get('strategies',[]):
    print(s.get('strategy_id'),'|',s.get('instrument'),'| tf=',s.get('fast_timeframe'),'| qty=',s.get('quantity'),'| gate=',s.get('live_gate'),'| state=',s.get('state'),'| last_signal=',str(s.get('last_signal'))[:80])
" 2>&1"""))
print("--- candle/last-bar health (native 15m bringup) ---")
print(run("""curl -s http://127.0.0.1:8001/api/candles?symbol=GOLDM&interval=15m 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); bars=d.get('bars') or d.get('candles') or []; print('gold 15m bars:', len(bars), 'last:', bars[-1] if bars else None)" 2>&1 || echo 'candles route N/A'"""))
print("--- funds ---")
print(run("curl -s http://127.0.0.1:8001/api/live/funds | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity=',d.get('equity'),'avail_margin=',d.get('available_margin'))\" 2>&1"))
print("--- positions (engine) ---")
print(run("curl -s http://127.0.0.1:8001/api/live/positions | python3 -c \"import sys,json; d=json.load(sys.stdin); print('count=',d.get('count'))\" 2>&1"))
print("--- engine health ---")
print(run("curl -s http://127.0.0.1:8001/api/live/health | python3 -c \"import sys,json; d=json.load(sys.stdin); print('strategies=',d.get('strategies'),'gate=',d.get('gate_enabled'))\" 2>&1"))
print("--- recent errors in logs ---")
print(run("docker logs mcx-live 2>&1 | grep -iE 'error|DH-906|Invalid Token|auth error' | tail -8"))
ssh.close()
