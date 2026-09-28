#!/usr/bin/env python3
"""Quick hash verify after rebuild."""
import paramiko, hashlib, os, time

VPS='200.234.44.93'; USER='root'; PASS='Deltacapitals@123'
LOCAL=r'C:\Users\pc\Desktop\MCX-TRADER-LIVE'

def ssh(cmd, timeout=30):
    t = paramiko.Transport((VPS, 22))
    t.connect(username=USER, password=PASS)
    ch = t.open_session()
    ch.settimeout(timeout)
    ch.exec_command(cmd)
    out = b""
    while not ch.exit_status_ready():
        if ch.recv_ready(): out += ch.recv(65536)
        time.sleep(0.1)
    while ch.recv_ready(): out += ch.recv(65536)
    ch.close()
    t.close()
    return out.decode(errors="replace")

# Check container is up
print("--- Container status ---")
status = ssh("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
print(status.strip())

# Try md5sum
print("\n--- Hash verification ---")
files = [
    'execution/live/order_watcher.py', 'live/api.py',
    'notifications/telegram_formatter.py', 'notifications/telegram_router.py',
    'analytics/routes.py', 'execution/price_model.py', 'trading_engine.py',
    'execution/live/engine.py', 'strategies/instance.py', 'persistence/database.py',
]
for f in files:
    lp = os.path.join(LOCAL, f)
    try:
        with open(lp, 'rb') as fh:
            lh = hashlib.md5(fh.read()).hexdigest()
    except:
        lh = 'LOCAL_MISSING'
    ro = ssh(f"docker exec mcx-live md5sum /app/{f} 2>/dev/null || echo MISSING")
    rh = ro.strip().split()[0] if ro.strip() else "MISSING"
    m = "MATCH" if lh == rh else "MISMATCH"
    print(f"  {f}: L={lh[:10]} R={rh[:10]} [{m}]")
