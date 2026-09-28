"""Deep check continued - fixed encoding."""
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
        return out.encode("ascii", "replace").decode("ascii") if out else "(empty)"
    except Exception as ex:
        return f"ERROR: {ex}"

# SECTION 2: NGINX ROUTING
print("=" * 70)
print("SECTION 2: NGINX ROUTING")
print("=" * 70)

print("\n2.1 ROUTE TEST - MCX-LIVE (all should be 200)")
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
fails = 0
for r in live_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    ok = "OK" if code == "200" else f"FAIL({code})"
    if code != "200": fails += 1
    print(f"  {r:<50} {code} {ok}")
print(f"\n  TOTAL: {len(live_routes)-fails}/{len(live_routes)} PASS")

print("\n2.2 ROUTE TEST - OPTION DEMO")
option_routes = ["/option/", "/api/options/status", "/api/options/overview",
                 "/api/options/dashboard", "/api/options/config", "/api/options/trades"]
fails = 0
for r in option_routes:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    ok = "OK" if code == "200" else f"FAIL({code})"
    if code != "200": fails += 1
    print(f"  {r:<50} {code} {ok}")
print(f"\n  TOTAL: {len(option_routes)-fails}/{len(option_routes)} PASS")

print("\n2.3 ROUTE TEST - SCREENER")
for r in ["/screener/"]:
    code = run(f"curl -sk -o /dev/null -w '%{{http_code}}' 'https://deltacapitals.systems{r}'")
    print(f"  {r:<50} {code}")

print("\n2.4 CROSS-CONTAMINATION CHECK")
print("  /api/health -> mcx-live?", "engine" in run("curl -sk https://deltacapitals.systems/api/health"))
print("  /api/options/overview -> option-demo?", "open_count" in run("curl -sk https://deltacapitals.systems/api/options/overview"))
print("  / -> mcx-live SPA?", "dashboard-ui" in run("curl -sk https://deltacapitals.systems/"))
print("  /option/ -> option-demo SPA?", "Option" in run("curl -sk https://deltacapitals.systems/option/"))
print("  /screener/ -> screener SPA?", "screener" in run("curl -sk https://deltacapitals.systems/screener/").lower() or "dark" in run("curl -sk https://deltacapitals.systems/screener/").lower())

# SECTION 3: MCX-LIVE ENGINE
print("\n" + "=" * 70)
print("SECTION 3: MCX-LIVE ENGINE")
print("=" * 70)

print("\n3.1 HEALTH")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.2 OVERVIEW")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'{k}: {v}') for k,v in d.items() if k not in ('strategies','positions','account','failure_states')]\""))

print("\n3.3 GATES")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(chr(47)+chr(97)+chr(112)+chr(112)+chr(47)+chr(99)+chr(111)+chr(110)+chr(102)+chr(105)+chr(103)+chr(47)+chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(115)+chr(101)+chr(116)+chr(116)+chr(105)+chr(110)+chr(103)+chr(115)+chr(46)+chr(106)+chr(115)+chr(111)+chr(110))); l=c.get(chr(108)+chr(105)+chr(118)+chr(101),{}); s=c.get(chr(115)+chr(116)+chr(114)+chr(97)+chr(116)+chr(101)+chr(103)+chr(105)+chr(101)+chr(115),{}); bs=l.get(chr(98)+chr(114)+chr(111)+chr(107)+chr(101)+chr(114)+chr(95)+chr(115)+chr(108),{}); print(chr(76)+chr(73)+chr(86)+chr(69)+chr(95)+chr(84)+chr(82)+chr(65)+chr(68)+chr(73)+chr(78)+chr(71)+chr(95)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), l.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(116)+chr(114)+chr(97)+chr(100)+chr(105)+chr(110)+chr(103)+chr(95)+chr(101)+chr(110)+chr(97)+chr(108)+chr(98)+chr(108)+chr(101)+chr(100))); print(chr(71)+chr(65)+chr(84)+chr(69)+chr(58), l.get(chr(103)+chr(97)+chr(116)+chr(101))); print(chr(66)+chr(82)+chr(79)+chr(75)+chr(69)+chr(82)+chr(95)+chr(83)+chr(76)+chr(46)+chr(69)+chr(78)+chr(65)+chr(66)+chr(76)+chr(69)+chr(68)+chr(58), bs.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100))); [print(k+chr(58), chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)+chr(61)+str(v.get(chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(103)+chr(97)+chr(116)+chr(101)+chr(61)+str(v.get(chr(108)+chr(105)+chr(118)+chr(101)+chr(95)+chr(103)+chr(97)+chr(116)+chr(101)))+chr(44)+chr(32)+chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(61)+str(v.get(chr(101)+chr(110)+chr(116)+chr(114)+chr(121)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(101)+chr(120)+chr(105)+chr(116)+chr(61)+str(v.get(chr(101)+chr(120)+chr(105)+chr(116)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(114)+chr(101)+chr(118)+chr(61)+str(v.get(chr(114)+chr(101)+chr(118)+chr(101)+chr(114)+chr(115)+chr(97)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))+chr(44)+chr(32)+chr(115)+chr(108)+chr(61)+str(v.get(chr(115)+chr(108)+chr(95)+chr(101)+chr(110)+chr(97)+chr(98)+chr(108)+chr(101)+chr(100)))) for k,v in s.items()]"'))

print("\n3.4 POSITIONS")
print(run("curl -sk https://deltacapitals.systems/api/positions 2>/dev/null | python3 -m json.tool 2>/dev/null"))

print("\n3.5 ORDERS SUMMARY")
print(run("curl -sk https://deltacapitals.systems/api/orders 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); orders=d.get('orders',[]); states={}; [states.__setitem__(o['state'], states.get(o['state'],0)+1) for o in orders]; print(f'Total: {len(orders)}, States: {states}')\""))

print("\n3.6 RISK")
print(run("curl -sk https://deltacapitals.systems/api/risk 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps({k:v for k,v in d.items() if k != 'risk_config'}, indent=2, default=str))\""))

print("\n3.7 LIVE FUNDS")
print(run("curl -sk https://deltacapitals.systems/api/live/funds 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2, default=str)[:500])\""))

print("\n3.8 LIVE PROFILE")
print(run("curl -sk https://deltacapitals.systems/api/live/profile 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2, default=str)[:500])\""))

print("\n3.9 LIVE SYNC")
print(run("curl -sk https://deltacapitals.systems/api/live/sync 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2, default=str)[:500])\""))

print("\n3.10 MARKET DATA (Dhan ticks)")
print(run("curl -sk https://deltacapitals.systems/api/market-data 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); [print(f'  {k}: ltp={v.get(\\\"ltp\\\")}, change={v.get(\\\"change\\\")}') for k,v in d.items() if isinstance(v,dict) and 'ltp' in v]\""))

# SECTION 4: OPTION DEMO
print("\n" + "=" * 70)
print("SECTION 4: OPTION DEMO")
print("=" * 70)
print(run("curl -sk https://deltacapitals.systems/api/options/status 2>/dev/null | python3 -m json.tool 2>/dev/null"))
print(run("curl -sk https://deltacapitals.systems/api/options/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print(json.dumps(d, indent=2, default=str)[:500])\""))

# SECTION 5: CORS
print("\n" + "=" * 70)
print("SECTION 5: CORS + WS")
print("=" * 70)
print("CORS:", run("docker exec mcx-live printenv CORS_ORIGINS"))
print("CORS header:", run("curl -sk -I -H 'Origin: https://deltacapitals.systems' https://deltacapitals.systems/api/overview 2>/dev/null | grep -i 'access-control'"))
print("WS connections:", run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | python3 -c \"import sys,json; print(json.load(sys.stdin).get('ws_connections'))\""))

# SECTION 6: ERRORS
print("\n" + "=" * 70)
print("SECTION 6: RECENT ERRORS")
print("=" * 70)
print(run("docker logs mcx-live --tail 100 2>&1 | grep -iE 'error|traceback|exception' | tail -10"))
print(run("tail -5 /var/log/nginx/error.log 2>/dev/null"))

# SECTION 7: DB
print("\n" + "=" * 70)
print("SECTION 7: DATABASE")
print("=" * 70)
print(run("docker exec mcx-live ls -la /app/live/data/db/ 2>/dev/null"))
print(run("docker exec mcx-live python3 -c \"import sqlite3; conn=sqlite3.connect('/app/live/data/db/live_trading.db'); cur=conn.cursor(); cur.execute('SELECT count(*) FROM trades'); print('trades:', cur.fetchone()[0]); cur.execute('SELECT count(*) FROM orders'); print('orders:', cur.fetchone()[0]); conn.close()\" 2>/dev/null || echo 'DB query failed'"))

# SECTION 8: RESOURCES
print("\n" + "=" * 70)
print("SECTION 8: SYSTEM RESOURCES")
print("=" * 70)
print(run("df -h / | tail -1"))
print(run("free -h | head -2"))
print(run("docker system df 2>/dev/null"))
print("Container restarts:", run("docker inspect mcx-live --format 'RestartCount={{.RestartCount}}'"))

ssh.close()
print("\nDEEP CHECK COMPLETE")
