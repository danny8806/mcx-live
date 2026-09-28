import paramiko, sys, io, time, json
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
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
for sid in ["gold_02", "silver_01"]:
    print(f"=== start {sid} ===")
    print(run(f"""curl -s -X POST http://127.0.0.1:8001/api/strategies/{sid}/control -H 'Content-Type: application/json' -d '{{"action": "start"}}' """))
    print()
print("=== verify gates via API (settings endpoint reveals gates?) ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies | python3 -c "
import sys,json
d=json.load(sys.stdin)
for s in d.get('strategies',[]):
    print(s.get('strategy_id'),'| state=',s.get('state'))
" 2>&1"""))
ssh.close()
