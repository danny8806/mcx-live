#!/usr/bin/env python3
"""Phase 6 Sections 28-35: API + Frontend + WS + Telegram + Performance + Security."""
import paramiko, time, json, urllib.request

VPS = "200.234.44.93"
USER = "root"
PASS = "Deltacapitals@123"

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

def api(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        return {"error": str(e)}

def api_code(path):
    try:
        with urllib.request.urlopen(f"http://200.234.44.93:8001{path}", timeout=10):
            return 200
    except urllib.error.HTTPError as e:
        return e.code
    except:
        return 0

print("=" * 80)
print("SECTION 28: API ENDPOINT MAP")
print("=" * 80)

endpoints = [
    "/health", "/api/positions", "/api/orders", "/api/strategies",
    "/api/risk", "/api/market-data", "/api/trades", "/api/pnl",
    "/api/reconciliation", "/api/overview", "/api/fills",
    "/api/indicators", "/api/settings", "/api/alerts", "/api/reversals",
    "/api/signals", "/api/events", "/api/pending-orders", "/api/account",
    "/api/ws", "/api/telegram/stats", "/api/websocket/status",
    "/api/engine/status", "/api/status",
]

for ep in endpoints:
    code = api_code(ep)
    r = api(ep)
    status_str = "OK" if code == 200 else f"{code}"
    extra = ""
    if isinstance(r, dict) and "error" not in r:
        keys = list(r.keys())[:3]
        extra = f" keys={keys}"
        if "count" in r: extra += f" count={r['count']}"
        if "execution_mode" in r: extra += f" mode={r['execution_mode']}"
    print(f"  {ep}: {status_str}{extra}")

print("\n" + "=" * 80)
print("SECTION 29-30: FRONTEND")
print("=" * 80)

print("\n--- Frontend HTML ---")
html = ssh("curl -s http://localhost:8001/ 2>/dev/null | head -20")
print(html.strip()[:500])

print("\n--- Frontend assets ---")
assets = ssh("curl -s http://localhost:8001/ 2>/dev/null | grep -oE 'src=\"[^\"]+\"|href=\"[^\"]+\"'")
print(assets.strip()[:300])

print("\n--- Static assets check ---")
for asset_path in ["/assets/index-CLC1qbuE.js", "/assets/index-hjl7d4ur.css", "/favicon.svg"]:
    code = api_code(asset_path)
    print(f"  {asset_path}: {code}")

print("\n--- Frontend API base config ---")
config = ssh("docker exec mcx-live cat /app/dashboard-ui/dist/index.html 2>/dev/null | grep -oE 'APP_[A-Z_]+=\"[^\"]*\"' || echo 'checking built frontend'")
print(config.strip()[:200])

print("\n" + "=" * 80)
print("SECTION 31: WEBSOCKET DASHBOARD")
print("=" * 80)

ws_config = ssh("docker exec mcx-live grep -rn 'WebSocket\\|websocket\\|ws://' /app/live/api.py 2>/dev/null | head -10")
print(f"\n--- Backend WS routes ---")
print(ws_config.strip()[:400] if ws_config.strip() else "  not found in api.py")

ws_routes = ssh("docker exec mcx-live grep -rn '/ws\\|@.*ws\\|WebSocket' /app/live/api.py /app/live/_api_patched.py 2>/dev/null | head -15")
print(f"\n--- WS endpoint definitions ---")
print(ws_routes.strip()[:400] if ws_routes.strip() else "  not found")

ws_handler = ssh("docker exec mcx-live grep -rn 'ws_manager\\|WSManager\\|broadcast\\|send_json\\|ws_send' /app/live/ 2>/dev/null | grep -v '.pyc' | head -15")
print(f"\n--- WS broadcast ---")
print(ws_handler.strip()[:400] if ws_handler.strip() else "  not found")

print("\n" + "=" * 80)
print("SECTION 32: TELEGRAM")
print("=" * 80)

tg_client = ssh("docker exec mcx-live python3 -c \"import inspect; from notifications.telegram_client import TelegramClient; src=inspect.getsource(TelegramClient.send_sync); print(src[:500])\" 2>/dev/null")
print(f"\n--- TelegramClient.send_sync ---")
print(tg_client.strip()[:500] if tg_client.strip() else "  Could not inspect")

tg_router = ssh("docker exec mcx-live python3 -c \"import inspect; from notifications.telegram_router import TelegramRouter; src=inspect.getsource(TelegramRouter); print(src[:800])\" 2>/dev/null")
print(f"\n--- TelegramRouter methods ---")
print(tg_router.strip()[:600] if tg_router.strip() else "  Could not inspect")

tg_formatter = ssh("docker exec mcx-live python3 -c \"from notifications.telegram_formatter import *; import notifications.telegram_formatter as m; print([x for x in dir(m) if not x.startswith('_')])\" 2>/dev/null")
print(f"\n--- Telegram formatters ---")
print(tg_formatter.strip()[:300] if tg_formatter.strip() else "  Could not inspect")

tg_logs = ssh("docker logs mcx-live 2>&1 | grep -iE 'telegram\\|tg\\|bot\\|send.*alert' | tail -15")
print(f"\n--- Telegram logs ---")
print(tg_logs.strip()[:400] if tg_logs.strip() else "  No telegram logs")

tg_stats = api("/api/telegram/stats")
print(f"\n--- /api/telegram/stats ---")
print(json.dumps(tg_stats)[:300])

print("\n" + "=" * 80)
print("SECTION 33: PERFORMANCE")
print("=" * 80)

perf = ssh("docker stats mcx-live --no-stream --format 'CPU={{.CPUPerc}} MEM={{.MemUsage}} MEM%={{.MemPerc}} NET={{.NetIO}} BLOCK={{.BlockIO}} PIDs={{.PIDs}}'")
print(f"\n--- Container resources ---")
print(perf.strip())

db_size = ssh("docker exec mcx-live ls -la /app/data/db/trading.db 2>/dev/null")
print(f"\n--- DB size ---")
print(db_size.strip())

disk = ssh("df -h / 2>/dev/null")
print(f"\n--- VPS disk ---")
print(disk.strip())

uptime = ssh("uptime 2>/dev/null")
print(f"\n--- VPS uptime ---")
print(uptime.strip())

api_latency = ssh("time curl -s http://localhost:8001/health > /dev/null 2>&1; echo 'done'")
print(f"\n--- API latency ---")
print(api_latency.strip()[:200])

print("\n" + "=" * 80)
print("SECTION 34: PARALLEL TESTING (local)")
print("=" * 80)
print("  Running locally on Windows; VPS SSH serial for safety.")
print("  No real-money tests executed during verification.")

print("\n" + "=" * 80)
print("SECTION 35: SECURITY")
print("=" * 80)

sec_checks = [
    ("Secrets in logs", "docker logs mcx-live 2>&1 | grep -iE 'access_token|secret|password|DHAN_ACCESS_TOKEN|DHAN_CLIENT_ID' | head -5"),
    ("Secrets in env (printed?)", "docker exec mcx-live env | grep -c 'TOKEN\\|SECRET\\|PASS'"),
    ("CORS config", "docker exec mcx-live grep -n 'CORSMiddleware\\|allow_origins\\|cors' /app/live/api.py /app/live/_api_patched.py 2>/dev/null | head -10"),
    ("Exposed ports", "docker ps --format '{{.Ports}}' | grep mcx-live"),
    ("TLS/HTTPS", "docker exec mcx-live grep -n 'ssl\\|https\\|tls\\|cert' /app/live/api.py /app/live/run.py 2>/dev/null | head -5"),
    ("API auth", "docker exec mcx-live grep -n 'auth\\|bearer\\|api_key\\|middleware.*auth' /app/live/api.py 2>/dev/null | head -10"),
    ("Client ID masking", "docker exec mcx-live grep -n 'mask\\|redact\\|\\*\\*\\*' /app/live/api.py /app/dashboard/routes/settings.py 2>/dev/null | head -10"),
]
for label, cmd in sec_checks:
    r = ssh(cmd)
    print(f"\n  {label}:")
    print(f"    {r.strip()[:300] if r.strip() else '(empty)'}")
