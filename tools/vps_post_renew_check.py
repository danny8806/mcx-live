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
print("=== logs since token renewal (tail 30) ===")
print(run("docker logs mcx-live --since 18m 2>&1 | tail -30"))
print("=== any auth errors in last 3 min? ===")
print(run("docker logs mcx-live --since 3m 2>&1 | grep -iE 'auth|DH-906|Invalid Token|dhan_ws error|CandleFetcher Error' | wc -l"))
print("=== ws/status in last 3 min ===")
print(run("docker logs mcx-live --since 3m 2>&1 | grep -iE 'dhan_ws|status|watchdog|stale' | tail -10"))
print("=== instrument candle state via engine probe ===")
print(run("""docker exec mcx-live python3 -c "
import sys
sys.path.insert(0, '/app')
from data.dhan.adapter import DhanDataAdapter
import os, json
tok = json.load(open('/app/live/data/db/dhan_token.json')).get('access_token','')
a = DhanDataAdapter(client_id=os.environ.get('DHAN_CLIENT_ID',''), token_file='/app/live/data/db/dhan_token.json')
a.register_instruments({k: v for k, v in json.load(open('/app/config/live_settings.json'))['instruments'].items()})
for sym in ['GOLDM','SILVERM']:
    st = a.fetch_candle_state(sym, '15')
    cl = len(st.get('closed') or [])
    f = st.get('forming')
    print(f'{sym} 15m: closed={cl} forming_last={f[0] if f else None} forming_close={f[4] if f else None}')
" 2>&1"""))
ssh.close()
