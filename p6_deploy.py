#!/usr/bin/env python3
"""Phase 6 Sections 2-4: Deployment map + Docker + Source hash."""
import paramiko, time, hashlib, os, json

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"
LOCAL = r"C:\Users\pc\Desktop\MCX-TRADER-LIVE"

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

print("=" * 80)
print("SECTION 2: DEPLOYMENT MAP")
print("=" * 80)

cmds = [
    ("Container brief", "docker inspect mcx-live --format 'ID={{.Id}} Name={{.Name}} Image={{.Config.Image}} Created={{.Created}} Started={{.State.StartedAt}} RestartCount={{.RestartCount}} Status={{.State.Status}} Health={{.State.Health.Status}} OOMKilled={{.State.OOMKilled}} Pid={{.State.Pid}}'"),
    ("Restart policy", "docker inspect mcx-live --format 'Policy={{.HostConfig.RestartPolicy.Name}} MaxRetry={{.HostConfig.RestartPolicy.MaximumRetryCount}}'"),
    ("Mounts", "docker inspect mcx-live --format '{{range .Mounts}}{{.Type}} {{.Source}} -> {{.Destination}}{{println}}{{end}}'"),
    ("Network mode", "docker inspect mcx-live --format 'NetMode={{.HostConfig.NetworkMode}}'"),
    ("All containers", "docker ps -a --format 'table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}\t{{.Names}}'"),
    ("All images", "docker images --format 'table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}'"),
    ("Networks", "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}'"),
    ("Volumes", "docker volume ls"),
    ("Resources", "docker stats mcx-live --no-stream --format 'CPU={{.CPUPerc}} MEM={{.MemUsage}} MEM%={{.MemPerc}} NET={{.NetIO}} BLOCK={{.BlockIO}} PIDs={{.PIDs}}'"),
    ("Config env check", "docker exec mcx-live cat /app/config/live_settings.json 2>/dev/null | python3 -c \"import sys,json; c=json.load(sys.stdin); print(f'env={c[chr(115)+chr(121)+chr(115)+chr(116)+chr(101)+chr(109)][chr(101)+chr(110)+chr(118)+chr(105)+chr(114)+chr(111)+chr(110)+chr(109)+chr(101)+chr(110)+chr(116)]} live={c[chr(115)+chr(121)+chr(115)+chr(116)+chr(101)+chr(109)][chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)]}')\""),
]
for label, cmd in cmds:
    r = ssh(cmd)
    print(f"\n--- {label} ---")
    print(r.strip()[:600])

print("\n" + "=" * 80)
print("SECTION 3: SOURCE -> CONTAINER HASH VERIFICATION")
print("=" * 80)

files = [
    "execution/price_model.py", "execution/live/engine.py",
    "execution/live/dhan_transport.py", "execution/live/order_watcher.py",
    "execution/live/poller.py", "execution/live/broker_sync.py",
    "strategies/instance.py", "strategies/base_dema_strategy.py",
    "strategies/runtime.py", "persistence/database.py",
    "persistence/manager.py", "live/api.py", "live/engine.py",
    "live/run.py", "notifications/telegram_client.py",
    "notifications/telegram_formatter.py", "notifications/telegram_router.py",
    "trading_engine.py", "portfolio/position_manager.py", "analytics/routes.py",
]

mismatches = []
for f in files:
    lp = os.path.join(LOCAL, f)
    try:
        with open(lp, "rb") as fh:
            lh = hashlib.md5(fh.read()).hexdigest()
    except:
        lh = "LOCAL_MISSING"
    ro = ssh(f"docker exec mcx-live md5sum /app/{f} 2>/dev/null || echo MISSING")
    rh = ro.strip().split()[0] if ro.strip() else "MISSING"
    m = "MATCH" if lh == rh else "MISMATCH"
    if m == "MISMATCH":
        mismatches.append(f)
    print(f"  {f}: L={lh[:10]} R={rh[:10]} [{m}]")

print(f"\n  RESULT: {len(files)-len(mismatches)}/{len(files)} MATCH, {len(mismatches)} MISMATCH")
if mismatches:
    print(f"  MISMATCHED: {mismatches}")

print("\n" + "=" * 80)
print("SECTION 4: DOCKER VERIFICATION")
print("=" * 80)

cmds2 = [
    ("Startup logs", "docker logs mcx-live 2>&1 | head -80"),
    ("Error logs", "docker logs mcx-live 2>&1 | grep -iE 'error|exception|traceback|fail|crash|oom|kill' | tail -30"),
    ("Auth logs", "docker logs mcx-live 2>&1 | grep -iE 'auth|token|totp|renew|rate' | tail -20"),
    ("WS logs", "docker logs mcx-live 2>&1 | grep -iE 'ws|websocket|connect|disconnect|reconnect|subscribe' | tail -20"),
    ("Order logs", "docker logs mcx-live 2>&1 | grep -iE 'order|fill|position|signal|entry|exit|sl|cancel|reversal' | tail -30"),
    ("Risk logs", "docker logs mcx-live 2>&1 | grep -iE 'risk|daily|reset' | tail -10"),
    ("Health hits", "docker logs mcx-live 2>&1 | grep -c 'GET /health'"),
    ("DB files", "docker exec mcx-live ls -la /app/data/db/"),
    ("DB table counts", "docker exec mcx-live python3 -c \"import sqlite3; c=sqlite3.connect('/app/data/db/trading.db').cursor(); [print(f'{t}: {c.execute(f\\\"SELECT COUNT(*) FROM {t}\\\").fetchone()[0]}') for t in ['signals','trades','orders','fills','positions','pending_orders','processed_fills','broker_order_mapping','trade_events','account_snapshots','events','system_metadata']]\" 2>/dev/null"),
]
for label, cmd in cmds2:
    r = ssh(cmd)
    print(f"\n--- {label} ---")
    print(r.strip()[:800])
