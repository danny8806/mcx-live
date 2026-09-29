#!/usr/bin/env python3
"""Phase 6 — verify source in container + Dhan REST + live endpoints."""
import paramiko
from vps_credentials import load_vps_password
import time
import urllib.request
import json

VPS_HOST = "200.234.44.93"
VPS_USER = "root"
VPS_PASS = load_vps_password()
def ssh(cmd, timeout=20):
    t = paramiko.Transport((VPS_HOST, 22))
    t.connect(username=VPS_USER, password=VPS_PASS)
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

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

# 1. Container filesystem structure
print("=== CONTAINER FILE STRUCTURE ===")
for d in ["", "live/", "execution/", "execution/live/", "strategies/", "notifications/", "persistence/"]:
    r = ssh(f"docker exec mcx-live ls /app/{d} 2>/dev/null | head -20")
    print(f"  /app/{d}: {r.strip()}")

# 2. Source hash comparison
print("\n=== SOURCE HASH COMPARISON ===")
import hashlib
files = [
    "execution/live/order_watcher.py",
    "execution/live/poller.py",
    "execution/live/broker_sync.py",
    "execution/price_model.py",
    "execution/live/engine.py",
    "execution/live/dhan_transport.py",
    "strategies/instance.py",
    "persistence/database.py",
    "live/api.py",
    "notifications/telegram_formatter.py",
    "notifications/telegram_router.py",
]
for f in files:
    try:
        with open(f, "rb") as fh:
            local_hash = hashlib.md5(fh.read()).hexdigest()
    except:
        local_hash = "LOCAL_MISSING"
    remote = ssh(f"docker exec mcx-live md5sum /app/{f} 2>/dev/null || echo CONTAINER_MISSING")
    remote_hash = remote.strip().split()[0] if remote.strip() else "MISSING"
    match = "MATCH" if local_hash == remote_hash else "MISMATCH"
    print(f"  {f}: local={local_hash[:8]} remote={remote_hash[:8]} {match}")

# 3. Live API endpoints
print("\n=== LIVE API ENDPOINTS ===")
endpoints = [
    "/health",
    "/api/positions",
    "/api/orders",
    "/api/strategies",
    "/api/risk",
    "/api/market-data",
    "/api/signals",
    "/api/trades",
    "/api/pnl",
    "/api/reconciliation",
    "/api/overview",
    "/api/fills",
    "/api/events",
    "/api/indicators",
    "/api/pending-orders",
    "/api/settings",
]
for ep in endpoints:
    r = api(ep)
    status = "OK" if "error" not in r else f"ERR: {r['error'][:40]}"
    extra = ""
    if isinstance(r, dict):
        if "count" in r: extra = f" count={r['count']}"
        if "orders" in r: extra = f" orders={len(r['orders'])}"
        if "strategies" in r: extra = f" strategies={len(r['strategies'])}"
        if "trades" in r: extra = f" trades={len(r['trades'])}"
        if "signals" in r: extra = f" signals={len(r['signals'])}"
        if "status" in r: extra = f" status={r['status']}"
        if "equity" in r: extra = f" equity={r.get('equity',0)}"
    print(f"  {ep}: {status}{extra}")

# 4. Strategy details
print("\n=== STRATEGY DETAILS ===")
strat = api("/api/strategies")
if "strategies" in strat:
    for s in strat["strategies"]:
        sid = s.get("strategy_id", "?")
        inst = s.get("instrument", "?")
        state = s.get("state", "?")
        enabled = s.get("enabled", "?")
        pos = s.get("position_side", "?")
        pending = s.get("pending_entry")
        bars = s.get("bars_processed", 0)
        print(f"  {sid}: {inst} | state={state} | enabled={enabled} | pos={pos} | pending={pending is not None} | bars={bars}")

# 5. Market data detail
print("\n=== MARKET DATA ===")
mkt = api("/api/market-data")
if "instruments" in mkt:
    for inst, data in mkt["instruments"].items():
        print(f"  {inst}: LTP={data.get('ltp')} tick_count={data.get('tick_count')} ts={data.get('timestamp')}")

# 6. Order detail
print("\n=== ORDER DETAIL ===")
ords = api("/api/orders")
if "orders" in ords:
    for o in ords["orders"][:5]:
        oid = o.get("order_id", "?")[:20]
        strat_id = o.get("strategy_id", "?")
        side = o.get("side", "?")
        state = o.get("state", "?")
        otype = o.get("order_type", "?")
        price = o.get("price", "?")
        trigger = o.get("trigger_price", "?")
        print(f"  {oid}... | {strat_id} | {side} {otype} | state={state} | price={price} trigger={trigger}")

# 7. Reconciliation detail
print("\n=== RECONCILIATION DETAIL ===")
recon = api("/api/reconciliation")
if "checks" in recon:
    for c in recon["checks"]:
        name = c.get("name", "?")
        consistent = c.get("is_consistent", "?")
        errs = c.get("errors", [])
        print(f"  {name}: consistent={consistent}")
        for e in errs[:3]:
            print(f"    - {str(e)[:120]}")

# 8. Settings (version check)
print("\n=== SETTINGS ===")
settings = api("/api/settings")
if "system" in settings:
    sys_cfg = settings["system"]
    print(f"  version: {sys_cfg.get('version', '?')}")
    print(f"  environment: {sys_cfg.get('environment', '?')}")
    print(f"  live_enabled: {sys_cfg.get('live_enabled', '?')}")

# 9. Container process
print("\n=== CONTAINER PROCESS ===")
proc = ssh("docker top mcx-live -eo pid,args 2>/dev/null || docker exec mcx-live cat /proc/1/cmdline 2>/dev/null | tr '\\0' ' '")
print(f"  {proc.strip()[:200]}")

# 10. Container uptime + restart count
print("\n=== CONTAINER LIFECYCLE ===")
info = ssh("docker inspect mcx-live --format 'StartedAt={{.State.StartedAt}} RestartCount={{.RestartCount}} OOMKilled={{.State.OOMKilled}}' 2>/dev/null")
print(f"  {info.strip()}")
