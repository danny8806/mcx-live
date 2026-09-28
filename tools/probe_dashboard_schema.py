"""In-container: dump LIVE dashboard/API response shapes for frontend cross-check."""
import json
import urllib.request


def http(path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{path}", timeout=8) as r:
            return r.status, r.read(400_000).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read(500).decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)


def keys(o):
    return sorted(o.keys()) if isinstance(o, dict) else f"<list[{len(o)}]>"


def brief(v, depth=0):
    if isinstance(v, dict):
        return {k: brief(x, depth + 1) for k, x in list(v.items())[:6]}
    if isinstance(v, list):
        return [brief(x, depth + 1) for x in v[:2]]
    if isinstance(v, (int, float)):
        return v
    s = str(v)
    return s[:60]


s, b = http("/api/live/dashboard")
print("DASHBOARD", s)
d = json.loads(b)
print("TOP:", keys(d))
for k in d:
    if isinstance(d[k], dict):
        print(f"  .{k}: {keys(d[k])}")
    elif isinstance(d[k], list):
        print(f"  .{k}: list[{len(d[k])}]")

for sub in ("funds", "profile", "positions", "pnl", "recon", "signals", "orders", "candles", "sync"):
    if isinstance(d.get(sub), dict):
        print(f"\n== .{sub} ==")
        print(json.dumps(brief(d[sub]), indent=1)[:1200])

for path in [
    "/api/positions", "/api/orders", "/api/trades", "/api/fills",
    "/api/strategies", "/api/reconciliation", "/api/analytics/reconciliation",
    "/api/reversals", "/api/live/funds", "/api/health/system", "/api/live/sync",
]:
    s, b = http(path)
    print(f"\n== {path} -> {s} ==")
    try:
        j = json.loads(b)
        print("KEYS:", keys(j))
        if isinstance(j, dict):
            for k, v in j.items():
                if isinstance(v, list) and v:
                    print(f"  .{k}[0]:", json.dumps(v[0])[:400])
                elif isinstance(v, dict):
                    print(f"  .{k}:", json.dumps(brief(v))[:400])
        elif isinstance(j, list) and j:
            print("[0]:", json.dumps(j[0])[:400])
    except Exception as e:
        print("RAW:", b[:300])