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
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    return out.encode("ascii", "replace").decode("ascii")
print("=== candle fetcher + strategy activity in logs (1h) ===")
print(run("docker logs mcx-live --since 60m 2>&1 | grep -viE 'HTTP/1.1\" 200|GET /api' | tail -40"))
print("=== any 'strategy' / 'evaluate' / 'bar' / 'candle' log lines ===")
print(run("docker logs mcx-live --since 120m 2>&1 | grep -iE 'candle|bar|evaluat|strategy|signal|threshold|cross' | grep -viE 'HTTP/1.1|DEDUP' | tail -30"))
print("=== DB signal/trade counts ===")
print(run("""docker exec mcx-live python3 -c "
import sqlite3
db = '/app/live/data/db/live_trading.db'
con = sqlite3.connect(db)
cur = con.cursor()
tabs = [r[0] for r in cur.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
print('tables:', tabs)
for t in tabs:
    if 'signal' in t.lower() or 'trade' in t.lower() or 'bar' in t.lower() or 'candle' in t.lower():
        try:
            n = cur.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
            print(f'  {t}: {n} rows')
        except Exception as e:
            print(f'  {t}: err {e}')
con.close()
" 2>&1"""))
ssh.close()
