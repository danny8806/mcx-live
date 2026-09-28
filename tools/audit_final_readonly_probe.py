"""FINAL AUDIT read-only probe. NO order placement, NO code changes.
Runs DB forensics + broker cross-check + API/CORS + perf + security probes.
Remote python is base64-encoded to avoid shell/quote pitfalls."""
import base64, paramiko, sys, io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=20)

def run(cmd, timeout=120):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    try:
        out = o.read().decode("utf-8", "replace").strip()
    except Exception:
        out = ""
    try:
        err = e.read().decode("utf-8", "replace").strip()
    except Exception:
        err = ""
    return out, err

def run_py(code, timeout=120):
    b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
    return run("echo '%s' | base64 -d | docker exec -i mcx-live python3 -" % b64, timeout=timeout)

print("=" * 70)
print("A. CONTAINER / PROCESS")
out, err = run("docker ps | grep mcx-live || docker ps -a | grep mcx-live")
print(out)
out, err = run("docker exec mcx-live ps aux | grep -E 'live.run|uvicorn|gunicorn' | grep -v grep")
print(out or "(no live.process found)")
out, err = run("docker exec mcx-live sh -c 'date -u +%Y-%m-%dT%H:%M:%SZ; cat /etc/hostname'")
print(out)

print("=" * 70)
print("B. DB FORENSICS (live_trading.db)")
out, err = run_py("""
import sqlite3
con = sqlite3.connect('/app/live/data/db/live_trading.db')
con.row_factory = sqlite3.Row
cur = con.cursor()
tabs = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()]
print("TABLES:", ", ".join(tabs))
for t in tabs:
    try:
        n = cur.execute('SELECT COUNT(*) FROM "%s"' % t).fetchone()[0]
        cols = [r[1] for r in cur.execute('PRAGMA table_info("%s")' % t).fetchall()]
        print("  %-28s rows=%-6d cols=%s" % (t, n, ",".join(cols[:14])))
    except Exception as ex:
        print("  %s ERR %r" % (t, ex))
if "broker_api_events" in tabs:
    print("-- broker_api_events action x http_status --")
    try:
        for r in cur.execute("SELECT action, http_status, COUNT(*) n FROM broker_api_events GROUP BY action, http_status ORDER BY n DESC").fetchall():
            print("   %-32s http=%-5s n=%d" % (r["action"], r["http_status"], r["n"]))
    except Exception as e:
        print("  err", e)
    print("-- latest 20 rows (truncated, token-free) --")
    try:
        for r in cur.execute("SELECT rowid, action, http_status, COALESCE(error_type,''), COALESCE(error_code,''), substr(COALESCE(error_message,''),1,40), substr(COALESCE(response_payload,''),1,60), created_at FROM broker_api_events ORDER BY rowid DESC LIMIT 20").fetchall():
            print(str(tuple(r))[:170])
    except Exception as e:
        print("  err", e)
    print("-- bad ORDER_BY_CORRELATION rows (status!=200) --")
    try:
        n = cur.execute("SELECT COUNT(*) FROM broker_api_events WHERE action LIKE '%ORDER_BY_CORRELATION%' AND COALESCE(http_status,0)!=200").fetchone()[0]
        print("   bad rows total:", n)
    except Exception as e:
        print("  err", e)
for t in tabs:
    tl = t.lower()
    if any(k in tl for k in ("order", "fill", "trade", "lifecycle", "event", "day", "ledger", "position", "state")):
        try:
            rows = cur.execute('SELECT * FROM "%s" ORDER BY rowid DESC LIMIT 8' % t).fetchall()
            print("-- %s (%d rows shown) --" % (t, len(rows)))
            for r in rows:
                d = {k: (str(v)[:55]) for k, v in dict(r).items()}
                print("   ", str(d)[:200])
        except Exception as e:
            print("-- %s ERR %r" % (t, e))
for t in tabs:
    cols = [r[1] for r in cur.execute('PRAGMA table_info("%s")' % t).fetchall()]
    fid = [c for c in cols if "fill_id" in c.lower()]
    for c in fid:
        try:
            dups = cur.execute('SELECT "%s", COUNT(*) FROM "%s" GROUP BY "%s" HAVING COUNT(*)>1' % (c, t, c)).fetchall()
            print("FILL-DUP %s.%s: %d groups" % (t, c, len(dups)))
        except Exception as e:
            print("FILL-DUP %s.%s err %r" % (t, c, e))
con.close()
""")
print(out)
if err:
    print("STDERR:", err[:400])

print("=" * 70)
print("C. BROKER CROSS-CHECK (read-only)")
out, err = run_py("""
import json, urllib.request, urllib.error, datetime, collections
tok = json.load(open('/app/live/data/db/dhan_token.json'))['access_token']
H = {"access-token": tok, "Content-Type": "application/json"}
def get(path):
    req = urllib.request.Request("https://api.dhan.co/v2" + path, headers=H)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", "replace")[:100000]
    except urllib.error.HTTPError as e:
        return e.code, ""
st, b = get("/fundlimit")
print("FUNDLIMIT http", st)
try:
    f = json.loads(b)
    print("  fund keys:", ", ".join(list(f.keys())[:10]) if isinstance(f, dict) else type(f).__name__)
except Exception:
    print("  raw:", b[:150])
st, b = get("/positions")
print("POSITIONS http", st)
try:
    net = json.loads(b).get("netPositions") or []
    g = [p for p in net if str(p.get("tradingSymbol","")).upper().startswith(("GOLDM","SILVERM"))]
    print("  net positions:", len(net), " GOLDM/SILVERM:", len(g))
    for p in g:
        print("    %s %s qty=%s avg=%s sl=%s" % (p.get("tradingSymbol"), p.get("positionType"),
              p.get("sellQuantity") or p.get("buyQuantity"), p.get("averagePrice"), p.get("stopLoss")))
except Exception as e:
    print("  err", e, b[:120])
st, b = get("/orders")
print("ORDERS http", st)
try:
    ords = json.loads(b).get("data") or []
    today = datetime.date.today().isoformat()
    cnt = sum(1 for o in ords if str(o.get("createTime","")).startswith(today))
    stb = dict(collections.Counter(str(o.get("orderStatus","")).upper() for o in ords))
    print("  window:", len(ords), " today:", cnt, " statuses:", stb)
except Exception as e:
    print("  err", e, b[:120])
st, b = get("/tradebook")
print("TRADEBOOK http", st)
try:
    tr = json.loads(b).get("data") or []
    print("  trades:", len(tr), " today:", sum(1 for t in tr if str(t.get("createTime","")).startswith(datetime.date.today().isoformat())))
except Exception as e:
    print("  err", e, b[:120])
print("done")
""")
print(out)
if err:
    print("STDERR:", err[:300])

print("=" * 70)
print("D. API / CORS / FRONTEND")
out, err = run_py("""
import os
c = {k: ("SET" if os.environ.get(k) else "empty") for k in sorted(os.environ)
     if any(x in k for x in ("DHAN", "CORS", "APP_", "TOKEN", "VPS"))}
print("env:", c)
""")
print(out or err)
out, err = run("""for rt in overview orders positions pnl signals strategies reconciliation funds health; do
  printf "%s " "$rt"; curl -sk -o /dev/null -w '%{http_code} %{time_total}s\n' "https://deltacapitals.systems/api/$rt";
done; echo '--- CORS preflight/header with Origin ---' ; curl -sk -D - -o /dev/null -H 'Origin: https://deltacapitals.systems' https://deltacapitals.systems/api/overview | grep -i 'HTTP/\\|access-control'""")
print(out or err)
out, err = run("curl -sk -o /dev/null -w 'front_root %{http_code} %{time_total}s\\n' https://deltacapitals.systems/; docker exec mcx-live sh -c 'ls /app/dashboard | head -6; ls /app/static 2>/dev/null | head -6'")
print(out or err)

print("=" * 70)
print("E. PERFORMANCE")
out, err = run_py("""
import json, time, urllib.request, urllib.error
tok = json.load(open('/app/live/data/db/dhan_token.json'))['access_token']
for p in ("/fundlimit", "/positions", "/orders", "/tradebook"):
    req = urllib.request.Request("https://api.dhan.co/v2" + p, headers={"access-token": tok})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
        print("REST %-12s %.0fms" % (p, (time.time()-t0)*1000))
    except urllib.error.HTTPError as e:
        print("REST %-12s HTTP%d %.0fms" % (p, e.code, (time.time()-t0)*1000))
    except Exception as e:
        print("REST %-12s ERR %r" % (p, e))
""")
print(out or err)
out, err = run("docker exec mcx-live sh -c 'ps -o pid,etime,pcpu,pmem,cmd -p 1 | tail -1; ss -tln 2>/dev/null | grep -E \":80|:8000|:8001\" | head -6'")
print(out or "(port probe NA)")
out, err = run("docker exec mcx-live sh -c 'ls -la /app/live/data/db/*.json 2>/dev/null | head -8; echo ---; find /app -name \"*.log\" -mmin -120 2>/dev/null | head -3'")
print(out or "(no recent logs)")

print("=" * 70)
print("F. SECURITY")
out, err = run_py("""
import os
ks = ["DHAN_ACCESS_TOKEN","DHAN_CLIENT_ID","APP_API_BASE","APP_WS_BASE","CORS_ORIGINS","VPS_PASS"]
for k in ks:
    v = os.environ.get(k)
    print(k, "->", ("SET len %d" % len(v)) if v else "(empty)")
""")
print(out or err)
out, err = run(r"""docker exec mcx-live sh -c '
echo "--- token files ---";
ls -la /app/live/data/db/*token* 2>/dev/null;
echo "--- hardcoded long secrets scan (py) ---";
grep -rlnE "[A-Za-z0-9]{40,}" /app --include="*.py" 2>/dev/null | head -5 || echo none;
echo "--- logs showing access-token header? ---";
grep -rl "access-token" /app --include="*.log" 2>/dev/null | head -3 || echo none;
echo "--- listeners ---";
ss -tln 2>/dev/null | grep -E ":80 |:8000 |:8001 |:6379 |:3306 " | head -6 || echo none;
'""")
print(out or err)
out, err = run(r"""docker exec mcx-live sh -c 'grep -rn "HTTPBearer\|Depends(get_current\|oauth2\|Authorization" /app/live/api.py 2>/dev/null | head -4 || echo "no auth deps found"'""")
print(out or err)
ssh.close()
print("=" * 70)
print("PROBE COMPLETE")