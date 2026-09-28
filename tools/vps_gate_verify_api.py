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
def run(cmd, timeout=45):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    out = o.read().decode("utf-8", "replace")
    err = e.read().decode("utf-8", "replace")
    return (out or err).encode("ascii", "replace").decode("ascii")
print("=== find snapshot/gates route ===")
print(run("curl -s http://127.0.0.1:8001/openapi.json | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(p) for p in d.get('paths',{}).keys() if 'gate' in p or 'snapshot' in p or 'strategy' in p]\" 2>&1"))
print("=== get_snapshot via ws? try direct engine snapshot route ===")
print(run("curl -s http://127.0.0.1:8001/api/live/snapshot 2>/dev/null | head -c 300"))
print("=== /api/strategies full json (gate included?) ===")
print(run("""curl -s http://127.0.0.1:8001/api/strategies | python3 -c "
import sys,json
d=json.load(sys.stdin)
print(json.dumps(d, indent=1)[:1400])
" 2>&1"""))
ssh.close()
