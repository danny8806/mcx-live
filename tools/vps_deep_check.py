"""Deep full system recheck."""
import paramiko, json, time

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=30):
    try:
        _, o, e = ssh.exec_command(cmd, timeout=timeout)
        out = o.read().decode("utf-8", "replace").strip()
        return out or "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

# =====================================================================
# SECTION 1: INFRASTRUCTURE
# =====================================================================
print("=" * 70)
print("SECTION 1: INFRASTRUCTURE")
print("=" * 70)

print("\n1.1 CONTAINERS")
print(run("docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}'"))

print("\n1.2 DOCKER IMAGES")
print(run("docker images --format 'table {{.Repository}}:{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}' | grep -E 'mcx|screener|option'"))

print("\n1.3 LISTENING PORTS")
print(run("ss -tlnp 2>/dev/null | grep -E 'LISTEN' | awk '{print $4, $6}' | sort"))

print("\n1.4 DOCKER NETWORKS")
print(run("docker network ls --format 'table {{.Name}}\t{{.Driver}}'"))

print("\n1.5 NGINX STATUS")
print(run("nginx -t 2>&1"))
print(run("systemctl status nginx 2>/dev/null | head -5 || service nginx status 2>/dev/null | head -5"))

# =====================================================================
# SECTION 2: NGINX ROUTING
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 2: NGINX ROUTING")
print("=" * 70)

print("\n2.1 NGINX CONFIG (all locations)")
print(run("grep -E '^\s*(location|server_name|listen)' /etc/nginx/sites-available/deltacapitals.systems"))

print("\n2.2 NGINX ERROR LOG (last 20)")
print(run("tail -20 /var/log/nginx/error.log 2>/dev/null"))

print("\n2.3 ROUTE TEST - MCX-LIVE (should all be 200)")
live_routes = [
    "/", "/api/overview", "/api/health", "/api/strategies", "/api/positions",
    "/api/orders", "/api/trades", "/api/pnl", "/api/risk", "/api/market-data",
    "/api/indicators", "/api/alerts", "/api/settings", "/api/audit",
    "/api/reconciliation", "/api/equity-curve", "/api/fills", "/api/htf",
    "/api/envs", "/api/broker-events", "/api/alert-ledger",
    "/api/analytics/strategies", "/api/live/dashboard", "/api/live/orders",
    "/api/live/positions", "/api/live/pnl", "/api/live/funds", "/api/live/profile",
    "/api/live/signals", "/api/live/candles", "/api/live/recon",
    "/api/live/telegram", "/api/live/sync", "/api/live/timeline",
    "/api/overview/GOLDM", "/api/strategies/gold_01",
    "/api/pnl/GOLDM", "/api/positions/test", "/api/orders/test",
    "/api/trades/test", "/api/market-data/GOLDM", "/api/indicators/GOLDM",
    "/api/htf/GOLDM", "/api/equity-curve/GOLDM",
]
for r in live_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    ok = "OK" if code == "200" else f"FAIL({code})"
    print(f"  {r:<50} {code} {ok}")

print("\n2.4 ROUTE TEST - OPTION DEMO (should be 200)")
option_routes = ["/option/", "/api/options/status", "/api/options/overview",
                 "/api/options/dashboard", "/api/options/config", "/api/options/trades"]
for r in option_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    ok = "OK" if code == "200" else f"FAIL({code})"
    print(f"  {r:<50} {code} {ok}")

print("\n2.5 ROUTE TEST - SCREENER (should be 200)")
screener_routes = ["/screener/", "/_next/"]
for r in screener_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    ok = "OK" if code in ("200", "308") else f"FAIL({code})"
    print(f"  {r:<50} {code} {ok}")

print("\n2.6 CROSS-CONTAMINATION CHECK")
print("  /api/health -> must be mcx-live:")
print("  ", run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 100"))
print("  /api/options/overview -> must be option-demo:")
print("  ", run("curl -sk https://deltacapitals.systems/api/options/overview 2>/dev/null | head -c 100"))
print("  /api/options/status -> must be option-demo:")
print("  ", run("curl -sk https://deltacapitals.systems/api/options/status 2>/dev/null | head -c 100"))
print("  / -> must be mcx-live frontend:")
print("  ", run("curl -sk https://deltacapitals.systems/ 2>/dev/null | head -c 100"))
print("  /option/ -> must be option-demo frontend:")
print("  ", run("curl -sk https://deltacapitals.systems/option/ 2>/dev/null | head -c 100"))
print("  /screener/ -> must be screener frontend:")
print("  ", run("curl -sk https://deltacapitals.systems/screener/ 2>/dev/null | head -c 100"))

# =====================================================================
# SECTION 3: MCX-LIVE ENGINE
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 3: MCX-LIVE ENGINE")
print("=" * 70)

print("\n3.1 HEALTH")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.2 OVERVIEW")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'{k}: {v}') for k,v in d.items() if k not in ('strategies','positions','account','failure_states')]\""))

print("\n3.3 STRATEGIES")
print(run("curl -sk https://deltacapitals.systems/api/strategies 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'  {s[\\\"strategy_id\\\"]}: enabled={s[\\\"enabled\\\"]}, gate={s.get(\\\"live_gate\\\",\\\"?\\\")}, state={s[\\\"state\\\"]}, pos={s[\\\"position_side\\\"]}') for s in d.get('strategies',[])]\""))

print("\n3.4 GATES (from config)")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); s=c.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115),{}); bs=l.get(chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(115)+chr(108),{}); print(chr(76)+chr(73)+chr(86)+chr(69)+chr(95)+chr(84)+chr(82)+chr(65)+chr(68)+chr(73)+chr(78)+chr(71)+chr(95)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(108)+chr(98)+chr(108)+chr(101)+chr(100))); print(chr(71)+chr(65)+chr(84)+chr(69)+chr(58), l.get(chr(103)+chr(97)+chr(116)+chr(101))); print(chr(66)+chr(82)+chr(79)+chr(75)+chr(69)+chr(82)+chr(95)+chr(83)+chr(76)+chr(46)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), bs.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))); [print(k+chr(58), chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)+chr(61)+str(v.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(103)+chr(97)+chr(116)+chr(101)+chr(61)+str(v.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(103)+chr(97)+chr(116)+chr(101)))+chr(44)+chr(32)+chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(61)+str(v.get(chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(101)+chr(120)+chr(105)+chr(116)+chr(61)+str(v.get(chr(101)+chr(120)+chr(105)+chr(116)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(114)+chr(101)+chr(118)+chr(61)+str(v.get(chr(114)+chr(101)+chr(118)+chr(101)+chr(114)+chr(115)+chr(97)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(115)+chr(108)+chr(61)+str(v.get(chr(115)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))) for k,v in s.items()]"'))

print("\n3.5 POSITIONS")
print(run("curl -sk https://deltacapitals.systems/api/positions 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.6 ORDERS (all)")
print(run("curl -sk https://deltacapitals.systems/api/orders 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); orders=d.get('orders',[]); print(f'Total: {len(orders)}'); states={}; [states.__setitem__(o['state'], states.get(o['state'],0)+1) for o in orders]; print('States:', states); [print(f'  {o[\\\"order_id\\\"][:24]} | {o[\\\"strategy_id\\\"]} | {o[\\\"side\\\"]} | {o[\\\"order_type\\\"]} | {o[\\\"state\\\"]} | filled={o[\\\"filled_quantity\\\"]}') for o in orders[:15]]\""))

print("\n3.7 RISK")
print(run("curl -sk https://deltacapitals.systems/api/risk 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.8 LIVE DASHBOARD")
print(run("curl -sk https://deltacapitals.systems/api/live/dashboard 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps({k:v for k,v in d.items() if k != 'raw_strategies'}, indent=2, default=str)[:800])\""))

print("\n3.9 LIVE FUNDS")
print(run("curl -sk https://deltacapitals.systems/api/live/funds 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.10 LIVE PROFILE")
print(run("curl -sk https://deltacapitals.systems/api/live/profile 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.11 LIVE SYNC")
print(run("curl -sk https://deltacapitals.systems/api/live/sync 2>/dev/null | python3 -m json.tool 2>/dev/null"))

# =====================================================================
# SECTION 4: OPTION DEMO
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 4: OPTION DEMO")
print("=" * 70)

print("\n4.1 OPTION DEMO STATUS")
print(run("curl -sk https://deltacapitals.systems/api/options/status 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n4.2 OPTION DEMO OVERVIEW")
print(run("curl -sk https://deltacapitals.systems/api/options/overview 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n4.3 OPTION DEMO HEALTH")
print(run("curl -sk http://127.0.0.1:8002/api/health 2>/dev/null | python3 -m json.tool 2>/dev/null"))

# =====================================================================
# SECTION 5: SCREENER
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 5: SCREENER")
print("=" * 70)

print("\n5.1 SCREENER FRONTEND")
code = run("curl -sk -o /dev/null -w '%{http_code}' 'https://deltacapitals.systems/screener/'")
print(f"  /screener/ -> HTTP {code}")

print("\n5.2 SCREENER BACKEND")
print(run("curl -sk http://127.0.0.1:5000/ 2>/dev/null | head -c 200 || echo 'not reachable'"))

# =====================================================================
# SECTION 6: WS CONNECTIONS
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 6: WEBSOCKET")
print("=" * 70)

print("\n6.1 WS HEALTH")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('ws_connections:', d.get('ws_connections'))\""))

# =====================================================================
# SECTION 7: DCHAN WS TICKS
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 7: DHAN MARKET DATA")
print("=" * 70)

print("\n7.1 MARKET DATA")
print(run("curl -sk https://deltacapitals.systems/api/market-data 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'  {k}: ltp={v.get(\\\"ltp\\\")}, change={v.get(\\\"change\\\")}, ts={v.get(\\\"timestamp\\\")}') for k,v in d.items() if isinstance(v,dict) and 'ltp' in v]\""))

print("\n7.2 LAST 10 TICKS (from logs)")
print(run("docker logs mcx-live --tail 50 2>&1 | grep 'dhan_ws.*ltp' | tail -10"))

# =====================================================================
# SECTION 8: CORS
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 8: CORS")
print("=" * 70)

print("\n8.1 CORS ENV")
print(run("docker exec mcx-live printenv CORS_ORIGINS"))

print("\n8.2 CORS HEADER")
print(run("curl -sk -I -H 'Origin: https://deltacapitals.systems' https://deltacapitals.systems/api/overview 2>/dev/null | grep -i 'access-control'"))

# =====================================================================
# SECTION 9: RECENT ERRORS
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 9: RECENT ERRORS")
print("=" * 70)

print("\n9.1 CONTAINER ERRORS (last 100 lines)")
print(run("docker logs mcx-live --tail 100 2>&1 | grep -iE 'error|traceback|exception|critical|fatal' | tail -20"))

print("\n9.2 NGINX ERRORS (last 20)")
print(run("tail -20 /var/log/nginx/error.log 2>/dev/null"))

print("\n9.3 CONTAINER RESTARTS")
print(run("docker inspect mcx-live --format 'RestartCount={{.RestartCount}} StartedAt={{.State.StartedAt}}'"))

# =====================================================================
# SECTION 10: DB STATUS
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 10: DATABASE")
print("=" * 70)

print("\n10.1 DB FILES")
print(run("docker exec mcx-live ls -la /app/live/data/db/ 2>/dev/null"))
print(run("docker exec mcx-live ls -la /app/data/db/ 2>/dev/null"))

print("\n10.2 TRADE COUNT")
print(run("docker exec mcx-live python3 -c \"import sqlite3; conn=sqlite3.connect('/app/live/data/db/live_trading.db'); cur=conn.cursor(); cur.execute('SELECT count(*) FROM trades'); print('trades:', cur.fetchone()[0]); cur.execute('SELECT count(*) FROM orders'); print('orders:', cur.fetchone()[0]); conn.close()\" 2>/dev/null || echo 'DB query failed'"))

# =====================================================================
# SECTION 11: DISK SPACE
# =====================================================================
print("\n" + "=" * 70)
print("SECTION 11: SYSTEM RESOURCES")
print("=" * 70)

print("\n11.1 DISK")
print(run("df -h / | tail -1"))

print("\n11.2 MEMORY")
print(run("free -h | head -2"))

print("\n11.3 DOCKER DISK USAGE")
print(run("docker system df 2>/dev/null"))

ssh.close()
print("\n" + "=" * 70)
print("DEEP CHECK COMPLETE")
print("=" * 70)
