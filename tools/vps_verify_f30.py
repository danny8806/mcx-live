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
# wait for health
for i in range(20):
    time.sleep(6)
    st = run("docker inspect mcx-live --format '{{.State.Status}} health={{.State.Health.Status}} restarts={{.RestartCount}}'")
    if 'healthy' in st:
        print('container:', st)
        break
    print('waiting...', st)
print("image:", run("docker inspect mcx-live --format '{{.Config.Image}}'"))
print("=== engine health ===")
print(run("curl -s http://127.0.0.1:8001/api/live/health 2>&1 | python3 -c \"import sys,json; d=json.load(sys.stdin); print('mode=',d.get('execution_mode'),'strategies=',d.get('strategies'),'gate=',d.get('gate_enabled'),'broker=',d.get('broker'))\""))
print("=== strategies ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies 2>/dev/null | python3 -c "
import sys,json
d=json.load(sys.stdin)
for s in d.get('strategies',[]):
    print(s.get('strategy_id'),'|',s.get('instrument'),'| tf=',s.get('fast_timeframe'),'| qty=',s.get('quantity'),'| enabled=',s.get('enabled'),'| state=',s.get('state'))
" 2>&1"""))
print("=== full route check ===")
print(run("""python3 - <<'PY'
import urllib.request, json
routes = ["/health", "/api/health", "/api/live/health", "/api/strategies", "/api/live/orders", "/api/live/positions", "/api/live/funds", "/api/reversals", "/api/live/dashboard", "/api/live/signals"]
ok = 0
for p in routes:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{p}", timeout=5) as r:
            code = r.status; ok += 1 if code == 200 else 0
        print(f"  {p} -> {code}")
    except Exception as e:
        print(f"  {p} -> ERR {e}")
print("OK:", ok, "/", len(routes))
PY"""))
ssh.close()
