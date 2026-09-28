"""Post-deploy verification: hashes, health, Dhan, market, DB."""
import sys, json, time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file
import paramiko

seed = load_env_file(Path(__file__).resolve().parent.parent / "mcx-trader.env")
vps_pass = seed.get("VPS_PASS", "")
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

def run(cmd, timeout=30):
    _, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    return stdout.read().decode("utf-8", errors="replace").strip()

print("=" * 70)
print("POST-DEPLOY VERIFICATION —", time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 70)

# 1. Container status
print("\n[1] CONTAINER")
print(run("docker ps --filter name=mcx-live --format '{{.ID}} {{.Image}} {{.Status}} {{.Ports}}'"))
print(run("docker inspect mcx-live --format 'RESTARTS={{.RestartCount}} HEALTH={{.State.Health.Status}} CREATED={{.Created}}'"))

# 2. Image hash
print("\n[2] IMAGE HASH")
print(run("docker inspect mcx-live --format '{{.Image}}'"))

# 3. Source hashes (compare with local)
print("\n[3] SOURCE HASHES (post-deploy)")
local_hashes = {
    "trading_engine.py": "d8f9284fb67f42a5",
    "persistence/database.py": "9ab30cf2faa196c4",
    "persistence/manager.py": "023453647ff75560",
    "execution/live/dhan_transport.py": "08f3f9952f5b80f0",
    "execution/live/poller.py": "79c115c312d30220",
    "execution/live/broker_sync.py": "56a3633af4a4302b",
    "execution/live/order_watcher.py": "134b2bfaee2e329b",
    "execution/live/engine.py": "0cc37e681678ddf9",
    "dashboard/routes/reversals.py": "aee87bdbc46591c2",
    "config/live_settings.json": "1a2a363831aaa9ef",
}
match_count = 0
for f, local_h in local_hashes.items():
    remote_h = run(f"sha256sum {VPS_BASE}/{f} 2>/dev/null | cut -d' ' -f1")[:16]
    status = "MATCH" if remote_h == local_h else "MISMATCH"
    if status == "MATCH": match_count += 1
    print(f"  {f}: {status}")
print(f"  RESULT: {match_count}/{len(local_hashes)} MATCH")

# 4. Health endpoints
print("\n[4] HEALTH ENDPOINTS")
for path in ("/health", "/api/health", "/api/live/dashboard", "/api/overview", "/api/positions", "/api/orders", "/api/live/funds", "/api/market-data"):
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'http://127.0.0.1:8001{path}' 2>/dev/null")
    print(f"  {path}: HTTP {code}")

# 5. Dhan connection
print("\n[5] DHAN CONNECTION")
dashboard = run("curl -sk 'http://127.0.0.1:8001/api/live/dashboard' 2>/dev/null")
try:
    d = json.loads(dashboard)
    p = d.get("profile", {})
    print(f"  client_id: {p.get('client_id')}")
    print(f"  broker: {p.get('broker')}")
    print(f"  gate: {p.get('gate')}")
    print(f"  execution_model: {p.get('execution_model')}")
    print(f"  product_type: {p.get('product_type')}")
    print(f"  data_ws connected: {p.get('data_ws', {}).get('connected')}")
    print(f"  order_ws connected: {p.get('order_ws', {}).get('connected')}")
except: print(f"  Raw: {dashboard[:200]}")

# 6. Market data
print("\n[6] MARKET DATA")
mdata = run("curl -sk 'http://127.0.0.1:8001/api/market-data' 2>/dev/null")
try:
    d = json.loads(mdata)
    for inst, v in d.get("instruments", {}).items():
        print(f"  {inst}: ltp={v.get('ltp')} ticks={v.get('tick_count')}")
    print(f"  ws_connected: {d.get('ws_connected')}")
    stats = d.get("adapter_stats", {})
    print(f"  total_ticks: {stats.get('tick_count')} errors: {stats.get('error_count')}")
except: print(f"  Raw: {mdata[:200]}")

# 7. Funds
print("\n[7] FUNDS")
funds = run("curl -sk 'http://127.0.0.1:8001/api/live/funds' 2>/dev/null")
print(f"  {funds[:200]}")

# 8. Positions
print("\n[8] POSITIONS")
pos = run("curl -sk 'http://127.0.0.1:8001/api/live/positions' 2>/dev/null")
print(f"  {pos[:200]}")

# 9. DB
print("\n[9] DATABASE")
db_probe = (
'import sqlite3\n'
'conn = sqlite3.connect("' + VPS_BASE + '/live/data/db/live_trading.db")\n'
'c = conn.cursor()\n'
'c.execute("SELECT value FROM system_metadata WHERE key=\'schema_version\'")\n'
'r = c.fetchone()\n'
'print(f"  schema_version: {r[0] if r else \'NONE\'}")\n'
'c.execute("SELECT COUNT(*) FROM trades")\n'
'print(f"  trades: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM orders")\n'
'print(f"  orders: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM fills")\n'
'print(f"  fills: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM reversals")\n'
'print(f"  reversals: {c.fetchone()[0]}")\n'
'c.execute("SELECT COUNT(*) FROM signals")\n'
'print(f"  signals: {c.fetchone()[0]}")\n'
'c.execute("SELECT equity, available_margin FROM account_snapshots ORDER BY timestamp DESC LIMIT 1")\n'
'r = c.fetchone()\n'
'if r: print(f"  account: equity={r[0]} available={r[1]}")\n'
'conn.close()\n'
)
sftp = ssh.open_sftp()
with sftp.open("/tmp/post_deploy_db.py", "w") as f:
    f.write(db_probe)
sftp.close()
print(run("python3 /tmp/post_deploy_db.py"))

# 10. Engine state
print("\n[10] ENGINE STATE")
hs = run("curl -sk 'http://127.0.0.1:8001/api/health/system' 2>/dev/null")
print(f"  {hs[:300]}")

# 11. OpenAPI route count
print("\n[11] OPENAPI")
routes = run("curl -sk 'http://127.0.0.1:8001/openapi.json' 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(len(d.get(\'paths\',{})))\" 2>/dev/null")
print(f"  Total routes: {routes}")

# 12. SPA routes
print("\n[12] SPA ROUTES")
for route in ("/", "/live", "/live-ops", "/strategies", "/positions", "/orders", "/trades", "/reversals", "/settings", "/health"):
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'http://127.0.0.1:8001{route}' 2>/dev/null")
    print(f"  {route}: HTTP {code}")

print("\n" + "=" * 70)
print("POST-DEPLOY VERIFICATION COMPLETE")
print("=" * 70)
ssh.close()
