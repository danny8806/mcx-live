"""Phase 1c — READ-ONLY VPS->Dhan latency probe (no orders placed).

Runs inside the LIVE container:
  1. Layered network latencies to api.dhan.co  (DNS / TCP / TLS / authed HTTP)
     - GET  /orders            (day order book — the reconciliation read)
     - POST /marketfeed/quote  (read-only market quote for GOLDM + SILVERM)
  2. In-app WS tick cadence from the RUNNING trading process, sampled via the
     live API /api/market-data (adapter_stats.ws.tick + per-instrument counts).
     No second market WS is opened (would risk kicking the live feed).

All percentiles: P50 / P95 / P99 / MAX / mean. Times in ms.
"""
import base64
import io
import sys

import paramiko

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8",
                              errors="replace")

ENT = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        ENT[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=ENT["VPS_PASS"],
            timeout=20)


def run_py(code, timeout=180):
    b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
    _, o, e = ssh.exec_command(
        "echo '%s' | base64 -d | docker exec -i mcx-live python3 -" % b64,
        timeout=timeout)
    return o.read().decode("utf-8", "replace").strip(), \
        e.read().decode("utf-8", "replace").strip()


LAYER = r"""
import sys, os, time, json, socket, ssl, statistics, collections
sys.path.insert(0, "/app")
base = "/app/config/live_settings.resolved.json"
cfg = json.load(open(base))
dhan = cfg.get("dhan") or {}
rest_base = dhan.get("rest_base", "https://api.dhan.co/v2")
host = rest_base.replace("https://", "").replace("http://", "").rstrip("/")
if "/" in host:
    host = host.split("/", 1)[0]

def pct(sorted_v, p):
    if not sorted_v: return 0.0
    i = (len(sorted_v) - 1) * p
    lo = int(i); hi = min(lo + 1, len(sorted_v) - 1)
    return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (i - lo)

def report(name, ms):
    if not ms: print("%-22s no samples" % name); return
    s = sorted(ms)
    print("%-22s n=%3d mean=%7.2fms p50=%7.2f p95=%7.2f p99=%7.2f max=%7.2f"
          % (name, len(s), sum(ms)/len(ms),
             pct(s,0.50), pct(s,0.95), pct(s,0.99), max(ms)))

RES = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
ip = RES[0][4][0]

# --- DNS ---
dns_ms = []
for _ in range(20):
    t0 = time.perf_counter()
    socket.getaddrinfo(host, 443)
    dns_ms.append((time.perf_counter() - t0) * 1000.0)
report("dns resolve", dns_ms)

# --- TCP connect ---
tcp_ms = []
for _ in range(20):
    t0 = time.perf_counter()
    s = socket.create_connection((ip, 443), timeout=10)
    tcp_ms.append((time.perf_counter() - t0) * 1000.0)
    s.close()
report("tcp connect", tcp_ms)

# --- TLS handshake (no app data) ---
ctx = ssl.create_default_context()
tls_ms = []
for _ in range(20):
    try:
        s = socket.create_connection((ip, 443), timeout=10)
        t0 = time.perf_counter()
        with ctx.wrap_socket(s, server_hostname=host) as ss:
            tls_ms.append((time.perf_counter() - t0) * 1000.0)
    except Exception as e:
        print("tls error:", e); break
report("tls handshake", tls_ms)

# --- authed HTTP reads via the real transport's REST client ---
from data.dhan.rest_client import DhanRESTClient
token_file = dhan.get("token_file", "data/db/dhan_token.json")
if not os.path.isabs(token_file):
    token_file = os.path.join("/app", token_file)
http = DhanRESTClient(
    base_url=rest_base,
    token_file=token_file,
    client_id=dhan.get("client_id", ""),
    pin=dhan.get("pin", ""),
    totp_secret=dhan.get("totp_secret", ""),
)
inst = cfg.get("instruments") or {}

ord_ms = []
try:
    for _ in range(15):
        t0 = time.perf_counter()
        http._get("/orders")
        ord_ms.append((time.perf_counter() - t0) * 1000.0)
except Exception as e:
    print("GET /orders error:", str(e)[:200])
report("http GET /orders", ord_ms)

quote_ms = []
try:
    payload = {}
    for name, ic in inst.items():
        seg = ic.get("exchange_segment", "MCX_COMM")
        sid = ic.get("security_id")
        if sid:
            payload.setdefault(seg, []).append(int(sid))
    for _ in range(15):
        t0 = time.perf_counter()
        http._post("/marketfeed/quote", payload,
                   extra_headers={"client-id": dhan.get("client_id", "")})
        quote_ms.append((time.perf_counter() - t0) * 1000.0)
except Exception as e:
    print("POST /marketfeed/quote error:", str(e)[:200])
report("http POST quote", quote_ms)

# stable quote p99 of the last ltp seen (sanity: legit quote handler)
try:
    body = http._post("/marketfeed/quote", payload,
                      extra_headers={"client-id": dhan.get("client_id", "")})
    def _find_ltp(o):
        if isinstance(o, dict):
            for k in ("last_price","lastPrice","ltp"):
                if k in o: return o[k]
            for v in o.values():
                r = _find_ltp(v)
                if r: return r
        return None
    print("quote ltp sample:", _find_ltp(body))
except Exception as e:
    print("quote ltp error:", str(e)[:120])
print("DONE_LAYER")
"""


CADENCE = r"""
import sys, time, json, urllib.request, statistics, collections
base = "/app/config/live_settings.resolved.json"
cfg = json.load(open(base))
inst = {k: (v.get("symbol") or k) for k, v in (cfg.get("instruments") or {}).items()}
URL = "http://127.0.0.1:8001/api/market-data"
seen = {k: 0 for k in inst}
gaps = collections.defaultdict(list)
last = {}
times = []
t_end = time.time() + 20.0
with urllib.request.urlopen(URL, timeout=5) as r:
    d0 = json.loads(r.read().decode())
    for k in inst:
        rows = (d0.get("instruments") or {})
        prev = (rows.get(k) or {}).get("tick_count")
        if prev is not None:
            seen[k] = prev
            last[k] = time.perf_counter()
    ws0 = (d0.get("adapter_stats") or {}).get("ws", {}).get("tick", 0)
while time.time() < t_end:
    try:
        with urllib.request.urlopen(URL, timeout=5) as r:
            d = json.loads(r.read().decode())
    except Exception:
        time.sleep(0.25); continue
    now = time.perf_counter()
    for k in inst:
        cnt = None
        row = (d.get("instruments") or {}).get(k)
        if isinstance(row, dict):
            cnt = row.get("tick_count")
        if cnt is None:
            continue
        if cnt > seen[k]:
            if k in last:
                gaps[k].append((now - last[k]) * 1000.0)
            seen[k] = cnt
            last[k] = now
    ws_now = (d.get("adapter_stats") or {}).get("ws", {}).get("tick", 0)
    times.append((now, ws_now))
    time.sleep(0.2)

def pct(sorted_v, p):
    if not sorted_v: return 0.0
    i = (len(sorted_v) - 1) * p
    lo = int(i); hi = min(lo + 1, len(sorted_v) - 1)
    return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (i - lo)

print("== market-data sample window 20s ==")
print("ws_connected:", d.get("ws_connected"))
print("per-instrument cadence (inter-tick gaps):")
for k in inst:
    if gaps[k]:
        g = sorted(gaps[k])
        print("  %-8s n=%3d ticks  gap_p50=%7.1fms p95=%7.1f p99=%7.1f max=%7.1f"
              % (k, len(g), pct(g,0.50), pct(g,0.95), pct(g,0.99), max(g)))
    else:
        print("  %-8s n=0 ticks received in window" % k)
ws_ticks = [w for _, w in times]
if len(ws_ticks) > 1:
    first, lastw = ws_ticks[0], ws_ticks[-1]
    print("adapter ws.tick total: %d -> %d" % (first, lastw))
print("DONE_CADENCE")
"""

print("=== LAYER PROBE (VPS -> Dhan) ===")
out, err = run_py(LAYER, timeout=180)
print(out)
if err:
    print("STDERR:", err[:500])

print("\n=== IN-APP WS TICK CADENCE (running live process, read-only) ===")
out2, err2 = run_py(CADENCE, timeout=60)
print(out2)
if err2:
    print("STDERR:", err2[:500])

ssh.close()
print("\nprobe complete (no orders placed).")