"""Phase 1c — READ-ONLY VPS->Dhan WebSocket host connect tiers (TCP/TLS only).

Connect + TLS handshake to the market feed and order-update hosts. NO data
session, NO auth — pure socket/ssl handshake probes so the live feed is never
touched. Market is closed (weekend), so this proxies the WS connect latency.
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


def run_py(code, timeout=120):
    b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
    _, o, e = ssh.exec_command(
        "echo '%s' | base64 -d | docker exec -i mcx-live python3 -" % b64,
        timeout=timeout)
    return o.read().decode("utf-8", "replace").strip(), \
        e.read().decode("utf-8", "replace").strip()


CODE = r"""
import sys, socket, ssl, time
def pct(sorted_v, p):
    i = (len(sorted_v) - 1) * p
    lo = int(i); hi = min(lo + 1, len(sorted_v) - 1)
    return sorted_v[lo] + (sorted_v[hi] - sorted_v[lo]) * (i - lo)
def probe(host):
    res = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)
    ip = res[0][4][0]
    tcp, tls = [], []
    for _ in range(15):
        try:
            s = socket.create_connection((ip, 443), timeout=10)
            t0 = time.perf_counter()
            s.close()
            tcp.append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            print("tcp err", host, e); break
    ctx = ssl.create_default_context()
    for _ in range(15):
        try:
            s = socket.create_connection((ip, 443), timeout=10)
            t0 = time.perf_counter()
            with ctx.wrap_socket(s, server_hostname=host):
                tls.append((time.perf_counter() - t0) * 1000.0)
        except Exception as e:
            print("tls err", host, e); break
    def rep(nm, ms):
        if not ms: print("%-40s no samples" % nm); return
        s = sorted(ms)
        print("%-40s tcp_p50=%6.2fms p95=%6.2f p99=%6.2f max=%6.2f (n=%d)"
              % (nm, pct(s,0.50), pct(s,0.95), pct(s,0.99), max(ms), len(s)))
    rep(host + " tcp-connect", tcp)
    rep(host + " tls-handshake", tls)
    return host, tcp, tls

import json
cfg = json.load(open("/app/config/live_settings.resolved.json"))
dhan = cfg.get("dhan") or {}
market_ws = (dhan.get("ws_url") or "wss://api-feed.dhan.co") \
    .replace("wss://", "").split("/")[0]
order_ws = (cfg.get("live", {}).get("order_ws", {}) or {}) \
    .get("url", "wss://api-order-update.dhan.co") \
    .replace("wss://", "").split("/")[0]
for host in (market_ws, order_ws):
    probe(host)
print("DONE")
"""

out, err = run_py(CODE, timeout=90)
print(out)
if err:
    print("STDERR:", err[:300])
ssh.close()