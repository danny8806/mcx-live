"""Zero-trust VPS probe: single SSH session, all read-only checks."""
import sys, json, time, hashlib
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file
import paramiko

seed = load_env_file(Path(__file__).resolve().parent.parent / "mcx-trader.env")
vps_pass = seed.get("VPS_PASS", "")
if not vps_pass:
    sys.exit("VPS_PASS not found")

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

def run(cmd, timeout=60):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode("utf-8", errors="replace")
    err = stderr.read().decode("utf-8", errors="replace")
    rc = stdout.channel.recv_exit_status()
    return out.strip(), err.strip(), rc

print("=" * 70)
print("ZERO-TRUST VPS PROBE —", time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 70)

# 1. CONTAINER STATUS
print("\n[1] CONTAINER STATUS")
out, _, _ = run("docker ps -a --filter name=mcx-live --format '{{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'")
print(out)

# 2. IMAGE INSPECT
print("\n[2] IMAGE INSPECT")
out, _, _ = run("docker inspect mcx-live --format '{{.Config.Image}} {{.Image}} {{.Created}}'")
print(out)
out, _, _ = run("docker inspect mcx-live --format '{{.RestartCount}} {{.State.Status}} {{.State.Health.Status}}'")
print(f"  RESTARTS: {out.split()[0]}  STATUS: {out.split()[1]}  HEALTH: {out.split()[2] if len(out.split())>2 else 'N/A'}")

# 3. MOUNTS
print("\n[3] MOUNTS")
out, _, _ = run("docker inspect mcx-live --format '{{range .Mounts}}{{.Source}} -> {{.Destination}} ({{.Mode}})\n{{end}}'")
print(out)

# 4. ENV (masked)
print("\n[4] ENV (masked)")
out, _, _ = run("docker inspect mcx-live --format '{{range .Config.Env}}{{println .}}{{end}}'")
for line in out.split("\n"):
    if any(k in line.upper() for k in ["TOKEN", "SECRET", "PASS", "KEY", "PIN", "TOTP"]):
        parts = line.split("=", 1)
        if len(parts) == 2:
            val = parts[1]
            masked = val[:4] + "****" + val[-4:] if len(val) > 8 else "****"
            print(f"  {parts[0]}={masked}")
    elif line.strip():
        print(f"  {line}")

# 5. SOURCE HASHES (compare with local)
print("\n[5] SOURCE HASHES (container vs local)")
local_hashes = {}
hash_file = Path(__file__).resolve().parent.parent / "tools" / "local_hashes.txt"
if hash_file.exists():
    for line in hash_file.read_text().splitlines():
        parts = line.split("  ", 2)
        if len(parts) == 3:
            local_hashes[parts[2]] = parts[0]

runtime_files = [
    "trading_engine.py", "persistence/database.py", "persistence/manager.py",
    "execution/live/dhan_transport.py", "execution/live/poller.py",
    "execution/live/broker_sync.py", "execution/live/order_watcher.py",
    "execution/live/engine.py", "execution/live/dhan_order_ws.py",
    "core/stoploss.py", "core/trade_close.py",
    "dashboard/routes/reversals.py", "dashboard/routes/live_ops.py",
    "live/api.py", "live/run.py", "config/live_settings.json",
    "strategies/types.py", "strategies/instance.py",
]
for rf in runtime_files:
    out, _, rc = run(f"sha256sum {VPS_BASE}/{rf} 2>/dev/null | cut -d' ' -f1")
    remote_hash = out if rc == 0 else "MISSING"
    local_hash = local_hashes.get(rf, "UNKNOWN")
    match = "MATCH" if remote_hash == local_hash else "MISMATCH"
    if remote_hash != "MISSING":
        remote_hash = remote_hash[:16]
    if local_hash != "UNKNOWN":
        local_hash = local_hash[:16]
    print(f"  {rf}: local={local_hash} remote={remote_hash} [{match}]")

# 6. LIVE HEALTH ENDPOINTS
print("\n[6] LIVE HEALTH ENDPOINTS")
for path in ("/health", "/api/health", "/api/live/dashboard", "/api/overview", "/api/positions", "/api/orders", "/api/live/funds"):
    out, _, rc = run(f"curl -sk -o /dev/null -w '%{{http_code}} %{{time_total}}' 'http://127.0.0.1:8001{path}' 2>/dev/null")
    print(f"  {path}: HTTP {out}")

# 7. LIVE FUNDS (masked)
print("\n[7] LIVE FUNDS")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/api/live/funds' 2>/dev/null")
if rc == 0:
    try:
        d = json.loads(out)
        bal = d.get("available_balance") or d.get("availabel_balance") or d.get("availableMargin", "N/A")
        print(f"  Available: {bal}")
        print(f"  Source: {d.get('source', 'N/A')}")
        print(f"  Mode: {d.get('mode', 'N/A')}")
    except:
        print(f"  Raw: {out[:200]}")
else:
    print(f"  ERROR: rc={rc}")

# 8. LIVE POSITIONS (count)
print("\n[8] LIVE POSITIONS")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/api/live/positions' 2>/dev/null")
if rc == 0:
    try:
        d = json.loads(out)
        positions = d if isinstance(d, list) else d.get("positions", [])
        print(f"  Count: {len(positions)}")
        for p in positions[:5]:
            print(f"  {p.get('security_id','?')} side={p.get('side','?')} qty={p.get('quantity',0)} avg={p.get('average_entry_price',0)} sl={p.get('sl_state','?')}")
    except:
        print(f"  Raw: {out[:200]}")

# 9. LIVE ORDERS (count)
print("\n[9] LIVE ORDERS")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/api/live/orders' 2>/dev/null")
if rc == 0:
    try:
        d = json.loads(out)
        orders = d if isinstance(d, list) else d.get("orders", [])
        print(f"  Count: {len(orders)}")
        for o in orders[:5]:
            print(f"  {o.get('order_id','?')} type={o.get('order_type','?')} state={o.get('state','?')} broker={o.get('broker_order_id','?')}")
    except:
        print(f"  Raw: {out[:200]}")

# 10. OPENAPI ROUTES
print("\n[10] OPENAPI ROUTES (sample)")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/openapi.json' 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); paths=list(d.get('paths',{}).keys()); print(f'Total: {len(paths)}'); [print(f'  {p}') for p in paths[:30]]\" 2>/dev/null")
print(out)

# 11. DB SCHEMA + TABLE COUNTS
print("\n[11] DATABASE (live_trading.db)")
out, _, rc = run(f"sqlite3 {VPS_BASE}/live/data/db/live_trading.db \"SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;\" 2>/dev/null")
if rc == 0:
    tables = [t for t in out.split("\n") if t.strip()]
    print(f"  Tables: {len(tables)}")
    for t in tables:
        cnt, _, _ = run(f"sqlite3 {VPS_BASE}/live/data/db/live_trading.db \"SELECT COUNT(*) FROM {t};\" 2>/dev/null")
        if cnt.isdigit() and int(cnt) > 0:
            print(f"  {t}: {cnt} rows")

# 12. DB LINEAGE CHECK
print("\n[12] DB LINEAGE CHECK")
for q in [
    "SELECT COUNT(*) FROM orders WHERE broker_order_id IS NOT NULL;",
    "SELECT COUNT(*) FROM fills WHERE broker_fill_id IS NOT NULL;",
    "SELECT COUNT(*) FROM orders WHERE trade_id IS NULL;",
    "SELECT COUNT(*) FROM fills WHERE trade_id IS NULL OR order_id IS NULL;",
    "SELECT COUNT(*) FROM trades WHERE entry_signal_id IS NULL OR entry_signal_id='';",
]:
    out, _, rc = run(f"sqlite3 {VPS_BASE}/live/data/db/live_trading.db \"{q}\" 2>/dev/null")
    print(f"  {q[:60]}... = {out}")

# 13. DB SCHEMA VERSION
print("\n[13] DB SCHEMA VERSION")
out, _, rc = run(f"sqlite3 {VPS_BASE}/live/data/db/live_trading.db \"SELECT value FROM system_metadata WHERE key='schema_version';\" 2>/dev/null")
print(f"  Schema version: {out}")

# 14. DB FOREIGN KEYS
print("\n[14] DB FOREIGN KEY STATUS")
out, _, rc = run(f"sqlite3 {VPS_BASE}/live/data/db/live_trading.db \"PRAGMA foreign_keys;\" 2>/dev/null")
print(f"  foreign_keys: {out}")

# 15. LOG SCAN
print("\n[15] LOG SCAN (last 500 lines)")
for pattern in ["ERROR", "EXCEPTION", "TRACEBACK", "REJECT", "TIMEOUT", "STALE", "FILL", "CANCEL", "REVERSAL"]:
    out, _, rc = run(f"grep -c -i '{pattern}' {VPS_BASE}/logs/live-mcx/*.log 2>/dev/null || echo 0")
    total = sum(int(x) for x in out.split("\n") if x.strip().isdigit())
    if total > 0:
        print(f"  {pattern}: {total}")

# 16. DHAN WS STATUS (check logs for WS activity)
print("\n[16] DHAN WS STATUS")
out, _, rc = run(f"grep -i 'websocket\\|ws.*connect\\|ws.*error\\|ws.*close\\|DEDUP' {VPS_BASE}/logs/live-mcx/*.log 2>/dev/null | tail -10")
print(out[:500] if out else "  No WS lines found")

# 17. ENGINE STATE
print("\n[17] ENGINE STATE")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/api/health/system' 2>/dev/null")
if rc == 0:
    try:
        d = json.loads(out)
        print(f"  live_only: {d.get('live_only')}")
        print(f"  gate: {d.get('gate')}")
        print(f"  execution_model: {d.get('execution_model')}")
        print(f"  positions: {d.get('positions_count', 'N/A')}")
        print(f"  strategies: {d.get('strategies_count', 'N/A')}")
    except:
        print(f"  Raw: {out[:300]}")

# 18. MARKET DATA (current LTP via market WS or REST)
print("\n[18] MARKET DATA (REST quote)")
out, _, rc = run("curl -sk 'http://127.0.0.1:8001/api/market-data' 2>/dev/null")
if rc == 0:
    try:
        d = json.loads(out)
        for inst in (d if isinstance(d, dict) else {}):
            item = d[inst] if isinstance(d[inst], dict) else {}
            print(f"  {inst}: ltp={item.get('ltp','N/A')} source={item.get('source','N/A')}")
    except:
        print(f"  Raw: {out[:200]}")

print("\n" + "=" * 70)
print("PROBE COMPLETE")
print("=" * 70)
ssh.close()
