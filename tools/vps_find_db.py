import paramiko, io, sys, base64
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

script = r'''
import sqlite3, glob
hits = []
for base in glob.glob("/app/live/data/db/*.db") + glob.glob("/app/data/*.db") + glob.glob("/app/*.db"):
    try:
        c = sqlite3.connect(base)
        tabs = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type=\"table\"").fetchall()]
        if tabs:
            hits.append((base, tabs))
    except Exception:
        pass
print(hits)
'''
b64 = base64.b64encode(script.encode()).decode()
_, o, e = ssh.exec_command(
    "docker exec mcx-live python3 -c \"import base64; exec(base64.b64decode('{}'))\"".format(b64),
    timeout=30,
)
try:
    print(o.read().decode("utf-8", "replace").strip())
except Exception as ex:
    print("out err", ex)
try:
    err = e.read().decode("utf-8", "replace").strip()
    if err:
        print("STDERR:", err)
except Exception as ex:
    print("err", ex)
ssh.close()