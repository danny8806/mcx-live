"""Deep live monitoring: discover ALL routes, hit every one, monitor freshness."""
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
    out = stdout.read().decode("utf-8", errors="replace")
    return out.strip()

def curl(path, timeout=15):
    """Hit endpoint, return (status_code, latency_ms, response_size, first_200_chars)."""
    out = run(f"curl -sk -o /tmp/ep_body -w '%{{http_code}}|{{time_total}}' 'http://127.0.0.1:8001{path}' 2>/dev/null; echo; wc -c < /tmp/ep_body 2>/dev/null; head -c 200 /tmp/ep_body 2>/dev/null", timeout=timeout)
    lines = out.split("\n")
    meta = lines[0] if lines else "0|0"
    parts = meta.split("|")
    code = parts[0] if len(parts) > 0 else "0"
    latency = parts[1] if len(parts) > 1 else "0"
    size = lines[1].strip() if len(lines) > 1 else "0"
    body = lines[2] if len(lines) > 2 else ""
    return code, latency, size, body

BASE = "http://127.0.0.1:8001"

print("=" * 80)
print("DEEP LIVE SYSTEM MONITORING —", time.strftime("%Y-%m-%d %H:%M:%S"))
print("=" * 80)

# ── PHASE 1: DISCOVER ALL ROUTES FROM OPENAPI ──
print("\n[PHASE 1] DISCOVERING ALL ROUTES FROM OPENAPI SPEC...")
openapi_raw = run("curl -sk 'http://127.0.0.1:8001/openapi.json' 2>/dev/null")
try:
    openapi = json.loads(openapi_raw)
    all_paths = sorted(openapi.get("paths", {}).keys())
    print(f"  Total routes in OpenAPI: {len(all_paths)}")
except:
    all_paths = []
    print("  ERROR: Could not parse OpenAPI spec")

# ── PHASE 2: HIT EVERY OPENAPI ROUTE ──
print(f"\n[PHASE 2] TESTING ALL {len(all_paths)} OPENAPI ROUTES...")
results = []
for i, path in enumerate(all_paths):
    # Skip parameterized paths (can't test without real IDs)
    if "{" in path:
        results.append((path, "SKIP", "0", "0", "0", "parameterized"))
        continue
    code, latency, size, body = curl(path)
    status = "OK" if code == "200" else f"FAIL({code})"
    results.append((path, code, latency, size, body[:80], status))
    icon = "✓" if code == "200" else "✗"
    print(f"  [{i+1:3d}/{len(all_paths)}] {icon} {path}: HTTP {code} ({latency}s, {size}B)")

# ── PHASE 3: ALL /api/live/* ENDPOINTS (DEEP) ──
print(f"\n[PHASE 3] DEEP /api/live/* ENDPOINTS...")
live_endpoints = [p for p in all_paths if "/api/live" in p]
print(f"  Found {len(live_endpoints)} live endpoints")
for path in live_endpoints:
    if "{" in path:
        continue
    code, latency, size, body = curl(path)
    icon = "✓" if code == "200" else "✗"
    print(f"  {icon} {path}: HTTP {code} ({latency}s)")
    if code == "200" and body:
        try:
            d = json.loads(run(f"cat /tmp/ep_body 2>/dev/null"))
            if isinstance(d, dict):
                keys = list(d.keys())[:5]
                print(f"    keys: {keys}")
            elif isinstance(d, list):
                print(f"    list: {len(d)} items")
        except:
            pass

# ── PHASE 4: SPA ROUTES ──
print(f"\n[PHASE 4] SPA ROUTES...")
spa_routes = ["/", "/live", "/live-ops", "/strategies", "/matrix", "/positions",
              "/orders", "/trades", "/pnl", "/risk", "/market-data", "/indicators",
              "/reconciliation", "/reversals", "/alerts", "/health", "/settings", "/audit"]
for route in spa_routes:
    code, latency, size, _ = curl(route)
    icon = "✓" if code == "200" else "✗"
    print(f"  {icon} {route}: HTTP {code} ({latency}s, {size}B)")

# ── PHASE 5: HEALTH / SYSTEM ──
print(f"\n[PHASE 5] HEALTH & SYSTEM STATUS...")
for ep in ["/health", "/api/health", "/ready", "/metrics", "/api/health/system"]:
    code, latency, size, body = curl(ep)
    print(f"  {ep}: HTTP {code}")
    if code == "200" and body:
        print(f"    {body[:150]}")

# ── PHASE 6: LIVE DATA FRESHNESS ──
print(f"\n[PHASE 6] LIVE DATA FRESHNESS CHECK...")
for ep, name in [
    ("/api/live/dashboard", "Dashboard"),
    ("/api/live/funds", "Funds"),
    ("/api/live/positions", "Positions"),
    ("/api/live/orders", "Orders"),
    ("/api/live/signals", "Signals"),
    ("/api/live/candles", "Candles"),
    ("/api/live/recon", "Reconciliation"),
    ("/api/live/sync", "BrokerSync"),
    ("/api/live/timeline", "Timeline"),
    ("/api/market-data", "MarketData"),
    ("/api/overview", "Overview"),
    ("/api/strategies", "Strategies"),
    ("/api/positions", "Positions(legacy)"),
    ("/api/orders", "Orders(legacy)"),
    ("/api/trades", "Trades"),
    ("/api/pnl", "PnL"),
    ("/api/risk", "Risk"),
    ("/api/indicators", "Indicators"),
    ("/api/htf", "HTF"),
    ("/api/alerts", "Alerts"),
    ("/api/audit", "Audit"),
    ("/api/equity-curve", "EquityCurve"),
]:
    code, latency, size, body = curl(ep)
    if code == "200":
        try:
            d = json.loads(run(f"cat /tmp/ep_body 2>/dev/null"))
            if isinstance(d, dict):
                # Check for timestamp/freshness indicators
                ts = d.get("timestamp") or d.get("generated_at") or d.get("ts")
                count = len(d) if not isinstance(d, dict) else sum(1 for v in d.values() if v is not None)
                extra = f" ts={ts}" if ts else f" keys={count}"
            elif isinstance(d, list):
                extra = f" count={len(d)}"
            else:
                extra = ""
            print(f"  ✓ {name}: HTTP 200 ({latency}s, {size}B){extra}")
        except:
            print(f"  ✓ {name}: HTTP 200 ({latency}s, {size}B)")
    else:
        print(f"  ✗ {name}: HTTP {code}")

# ── PHASE 7: ENGINE COMPONENTS ──
print(f"\n[PHASE 7] ENGINE COMPONENTS...")
try:
    dashboard = json.loads(run(f"cat /tmp/ep_body 2>/dev/null")) if False else None
    dash_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/dashboard' 2>/dev/null")
    dashboard = json.loads(dash_raw)
    profile = dashboard.get("profile", {})
    print(f"  execution_mode: {profile.get('execution_mode')}")
    print(f"  broker: {profile.get('broker')}")
    print(f"  client_id: {profile.get('client_id')}")
    print(f"  gate: {profile.get('gate')}")
    print(f"  execution_model: {profile.get('execution_model')}")
    print(f"  product_type: {profile.get('product_type')}")
    print(f"  data_ws connected: {profile.get('data_ws', {}).get('connected')}")
    print(f"  order_ws connected: {profile.get('order_ws', {}).get('connected')}")
    ow = profile.get("order_watcher", {})
    print(f"  order_watcher.fallback: {ow.get('market_fallback_enabled')}")
    print(f"  order_watcher.skip_policy: {ow.get('limit_skip_policy')}")
    instruments = profile.get("instruments", {})
    for inst, info in instruments.items():
        print(f"  instrument {inst}: sid={info.get('security_id')} symbol={info.get('symbol')} xseg={info.get('exchange_segment')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 8: STRATEGY STATUS ──
print(f"\n[PHASE 8] STRATEGY STATUS...")
try:
    strats_raw = run("curl -sk 'http://127.0.0.1:8001/api/strategies' 2>/dev/null")
    strats = json.loads(strats_raw)
    if isinstance(strats, dict) and "strategies" in strats:
        strats = strats["strategies"]
    if isinstance(strats, list):
        for s in strats:
            sid = s.get("strategy_id") or s.get("id") or "?"
            enabled = s.get("enabled", "?")
            gate = s.get("live_gate") or s.get("gate", "?")
            print(f"  {sid}: enabled={enabled} gate={gate}")
    elif isinstance(strats, dict):
        for sid, s in strats.items():
            if isinstance(s, dict):
                enabled = s.get("enabled", "?")
                gate = s.get("live_gate") or s.get("gate", "?")
                print(f"  {sid}: enabled={enabled} gate={gate}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 9: MARKET DATA DETAIL ──
print(f"\n[PHASE 9] MARKET DATA DETAIL...")
try:
    md_raw = run("curl -sk 'http://127.0.0.1:8001/api/market-data' 2>/dev/null")
    md = json.loads(md_raw)
    print(f"  ws_connected: {md.get('ws_connected')}")
    instruments = md.get("instruments", {})
    for inst, v in instruments.items():
        print(f"  {inst}: ltp={v.get('ltp')} spread={v.get('spread')} ticks={v.get('tick_count')} ts={v.get('timestamp')}")
    stats = md.get("adapter_stats", {})
    print(f"  adapter_stats.ws.recv: {stats.get('ws', {}).get('recv')}")
    print(f"  adapter_stats.ws.tick: {stats.get('ws', {}).get('tick')}")
    print(f"  adapter_stats.ws.parse_err: {stats.get('ws', {}).get('parse_err')}")
    print(f"  adapter_stats.rest.ok: {stats.get('rest', {}).get('ok')}")
    print(f"  adapter_stats.rest.retry: {stats.get('rest', {}).get('retry')}")
    print(f"  adapter_stats.error_count: {stats.get('error_count')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 10: FUNDS DETAIL ──
print(f"\n[PHASE 10] FUNDS DETAIL...")
try:
    funds_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/funds' 2>/dev/null")
    funds = json.loads(funds_raw)
    fields = funds.get("fields", funds)
    print(f"  source: {funds.get('source')}")
    print(f"  mode: {fields.get('mode')}")
    print(f"  equity: {fields.get('equity')}")
    print(f"  available_margin: {fields.get('available_margin')}")
    print(f"  used_margin: {fields.get('used_margin')}")
    print(f"  realized_pnl: {fields.get('realized_pnl')}")
    print(f"  unrealized_pnl: {fields.get('unrealized_pnl')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 11: POSITIONS DETAIL ──
print(f"\n[PHASE 11] POSITIONS DETAIL...")
try:
    pos_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/positions' 2>/dev/null")
    pos = json.loads(pos_raw)
    positions = pos.get("positions", pos) if isinstance(pos, dict) else pos
    print(f"  count: {len(positions) if isinstance(positions, list) else 'N/A'}")
    print(f"  execution_mode: {pos.get('execution_mode')}")
    if isinstance(positions, list):
        for p in positions[:5]:
            print(f"  {p.get('security_id','?')} side={p.get('side','?')} qty={p.get('quantity',0)} avg={p.get('average_entry_price',0)} sl={p.get('sl_state','?')}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 12: ORDERS DETAIL ──
print(f"\n[PHASE 12] ORDERS DETAIL...")
try:
    orders_raw = run("curl -sk 'http://127.0.0.1:8001/api/live/orders' 2>/dev/null")
    orders = json.loads(orders_raw)
    if isinstance(orders, dict):
        order_list = orders.get("orders", [])
        print(f"  count: {len(order_list)}")
        for o in order_list[:5]:
            print(f"  {o.get('order_id','?')[:30]} type={o.get('order_type','?')} state={o.get('state','?')} broker={o.get('broker_order_id','?')}")
    elif isinstance(orders, list):
        print(f"  count: {len(orders)}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 13: TRADES DETAIL ──
print(f"\n[PHASE 13] TRADES DETAIL...")
try:
    trades_raw = run("curl -sk 'http://127.0.0.1:8001/api/trades' 2>/dev/null")
    trades = json.loads(trades_raw)
    if isinstance(trades, dict):
        trade_list = trades.get("trades", [])
        print(f"  count: {len(trade_list)}")
        for t in trade_list[:5]:
            print(f"  {t.get('trade_id','?')[:25]} strat={t.get('strategy_id','?')} inst={t.get('instrument','?')} status={t.get('status','?')}")
    elif isinstance(trades, list):
        print(f"  count: {len(trades)}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 14: REVERSALS DETAIL ──
print(f"\n[PHASE 14] REVERSALS DETAIL...")
try:
    rev_raw = run("curl -sk 'http://127.0.0.1:8001/api/reversals' 2>/dev/null")
    rev = json.loads(rev_raw)
    if isinstance(rev, dict):
        rev_list = rev.get("reversals", [])
        print(f"  count: {len(rev_list)}")
        for r in rev_list[:5]:
            print(f"  {r.get('reversal_id','?')} status={r.get('status','?')} strat={r.get('strategy_id','?')}")
    elif isinstance(rev, list):
        print(f"  count: {len(rev)}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 15: AUDIT LOG ──
print(f"\n[PHASE 15] AUDIT LOG...")
try:
    audit_raw = run("curl -sk 'http://127.0.0.1:8001/api/audit' 2>/dev/null")
    audit = json.loads(audit_raw)
    if isinstance(audit, list):
        print(f"  entries: {len(audit)}")
        for a in audit[:3]:
            print(f"  {a.get('timestamp','?')} {a.get('event_type','?')} {a.get('strategy_id','?')}")
    elif isinstance(audit, dict):
        events = audit.get("events", audit.get("entries", []))
        print(f"  entries: {len(events)}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── PHASE 16: ALERTS ──
print(f"\n[PHASE 16] ALERTS...")
try:
    alert_raw = run("curl -sk 'http://127.0.0.1:8001/api/alerts' 2>/dev/null")
    alerts = json.loads(alert_raw)
    if isinstance(alerts, list):
        print(f"  count: {len(alerts)}")
        for a in alerts[:3]:
            print(f"  {a.get('event_type','?')} {a.get('strategy_id','?')} {a.get('created_at','?')}")
    elif isinstance(alerts, dict):
        items = alerts.get("alerts", alerts.get("events", []))
        print(f"  count: {len(items)}")
except Exception as e:
    print(f"  ERROR: {e}")

# ── SUMMARY ──
ok_count = sum(1 for r in results if r[1] == "200")
skip_count = sum(1 for r in results if r[5] == "parameterized")
fail_count = sum(1 for r in results if r[1] not in ("200",) and r[5] != "parameterized")

print(f"\n{'=' * 80}")
print(f"MONITORING SUMMARY")
print(f"{'=' * 80}")
print(f"  OpenAPI routes discovered: {len(all_paths)}")
print(f"  Routes tested (no params): {ok_count + fail_count}")
print(f"  HTTP 200:                  {ok_count}")
print(f"  HTTP non-200:              {fail_count}")
print(f"  Parameterized (skipped):   {skip_count}")
print(f"  SPA routes tested:         {len(spa_routes)}")
if fail_count > 0:
    print(f"\n  FAILURES:")
    for r in results:
        if r[1] not in ("200",) and r[5] != "parameterized":
            print(f"    {r[0]}: HTTP {r[1]}")
print(f"{'=' * 80}")

ssh.close()
