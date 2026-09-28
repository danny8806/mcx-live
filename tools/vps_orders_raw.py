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
def run(cmd, t=30):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return o.read().decode("utf-8", "replace").strip()

script = r'''
import sqlite3
c = sqlite3.connect("/app/live/data/db/live_trading.db")
rows = c.execute("SELECT * FROM orders ORDER BY rowid").fetchall()
print("num order rows:", len(rows))
for i, r in enumerate(rows):
    print("--- row", i, "len", len(r))
    for j, v in enumerate(r):
        print("   [%d] %r" % (j, str(v)[:120]))
'''
sf = ssh.open_sftp()
with sf.open("/tmp/orders_raw.py", "w") as f:
    f.write(script)
sf.close()
print(run("docker cp /tmp/orders_raw.py mcx-live:/tmp/orders_raw.py && docker exec mcx-live python3 /tmp/orders_raw.py"))
ssh.close()