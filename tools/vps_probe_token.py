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
print("=== current UTC ===")
print(run("date -u +%s"))
print("=== market-data now ===")
print(run("curl -s http://127.0.0.1:8001/api/market-data | python3 -c \"import sys,json; d=json.load(sys.stdin); print('ws_connected=',d.get('ws_connected'),'tick_count=',d.get('tick_count')); print(json.dumps(d.get('instruments'))); print('adapter=',json.dumps(d.get('adapter_stats')))\""))
print("=== REST probe with disk token (fundlimit) ===")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json
tok = json.load(open('/app/live/data/db/dhan_token.json')).get('access_token','')
import os
cid = os.environ.get('DHAN_CLIENT_ID','')
h = {'access-token': tok, 'client-id': cid}
try:
    r = urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/fundlimit', headers=h), timeout=10)
    print('FUNDLIMIT 200:', json.dumps(json.loads(r.read()))[:400])
except urllib.error.HTTPError as e:
    print('FUNDLIMIT', e.code, ':', e.read().decode()[:300])
except Exception as e:
    print('ERR:', e)
" 2>&1"""))
print("=== trades/positions probe ===")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json, os
tok = json.load(open('/app/live/data/db/dhan_token.json')).get('access_token','')
h = {'access-token': tok, 'client-id': os.environ.get('DHAN_CLIENT_ID','')}
for name, p in [('positions','positions'), ('orders','orders')]:
    try:
        r = urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/'+p, headers=h), timeout=10)
        d = json.loads(r.read())
        n = len(d) if isinstance(d, list) else str(d)[:200]
        print(name, '->', n)
    except urllib.error.HTTPError as e:
        print(name, '->', e.code, e.read().decode()[:200])
    except Exception as e:
        print(name, 'ERR:', e)
" 2>&1"""))
print("=== recent ws/status logs ===")
print(run("docker logs mcx-live 2>&1 | grep -iE 'dhan_ws|watchdog|stale|closed|status' | tail -15"))
ssh.close()
