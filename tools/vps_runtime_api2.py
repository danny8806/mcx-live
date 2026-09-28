import paramiko, io, sys
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
def run(cmd, t=45):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()

probe = r'''
import urllib.request, json, traceback
def api(path, t=12):
    try:
        with urllib.request.urlopen("http://127.0.0.1:8001%s" % path, timeout=t) as r:
            return r.status, r.read().decode("utf-8", "replace")
    except Exception as ex:
        return "ERR", str(ex)[:200]

for path in ["/api/health", "/api/strategies", "/api/positions", "/api/trades", "/api/orders",
             "/api/reconciliation", "/api/live/dashboard", "/api/live/orders",
             "/api/live/positions", "/api/live/recon", "/api/live/funds", "/api/live/pnl",
             "/api/risk", "/api/settings", "/api/market-data", "/api/live/sync", "/api/live/signals"]:
    st, body = api(path)
    if st == 200:
        try:
            d = json.loads(body)
            if isinstance(d, dict):
                summ = {k: (v if not hasattr(v, "__len__") or isinstance(v, str) or len(v) < 60 else "<len %d>" % len(v)) if not isinstance(v, dict) else "<dict %d keys>" % len(v) for k, v in list(d.items())[:12]}
                print("OK  %-26s %s" % (path, json.dumps(summ)[:400]))
            else:
                print("OK  %-26s <type %s>" % (path, type(d).__name__))
        except Exception:
            print("OK  %-26s body[:300]=%r" % (path, body[:300]))
    else:
        print("ERR %-26s %s" % (path, st))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/api_probe.py", "w") as f:
    f.write(probe)
sf.close()
print(run("docker cp /tmp/api_probe.py mcx-live:/tmp/api_probe.py && docker exec mcx-live python3 /tmp/api_probe.py"))
ssh.close()