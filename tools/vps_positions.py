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
print("=== raw positions ===")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json, os
tok = json.load(open('/app/live/data/db/dhan_token.json')).get('access_token','')
h = {'access-token': tok, 'client-id': os.environ.get('DHAN_CLIENT_ID','')}
r = urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/positions', headers=h), timeout=10)
d = json.loads(r.read())
print(json.dumps(d, indent=1))
" 2>&1"""))
print("=== raw orders (recent) ===")
print(run("""docker exec mcx-live python3 -c "
import urllib.request, json, os
tok = json.load(open('/app/live/data/db/dhan_token.json')).get('access_token','')
h = {'access-token': tok, 'client-id': os.environ.get('DHAN_CLIENT_ID','')}
r = urllib.request.urlopen(urllib.request.Request('https://api.dhan.co/v2/orders', headers=h), timeout=10)
d = json.loads(r.read())
if isinstance(d, list):
    print('count:', len(d))
    for o in d[-8:]:
        print(json.dumps({k: o.get(k) for k in ('orderId','orderStatus','transactionType','tradingSymbol','securityId','quantity','filledQuantity','orderType','legName','productType')}, indent=0))
    d2 = json.dumps(d[-1], indent=1)
else:
    print(json.dumps(d, indent=1))
" 2>&1"""))
ssh.close()
