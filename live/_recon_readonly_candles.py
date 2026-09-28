"""Read-only live candle verification inside container (direct token file read).
Proves production rest_client Dhan REST candle pipeline. NO order placement."""
import json, sys, datetime, time, base64, urllib.request, urllib.error
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

DB = json.load(open("/app/live/data/db/dhan_token.json"))
tok = DB["access_token"]
p = tok.split(".")[1]; p += "=" * (-len(p) % 4)
pl = json.loads(base64.urlsafe_b64decode(p))
print("token exp in hours:", round((pl.get("exp", 0) - time.time()) / 3600, 2))

H = {"access-token": tok, "Content-Type": "application/json"}
BASE = "https://api.dhan.co/v2"

def post(path, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=H, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")[:3000]
    except Exception as e:
        return None, "ERR %r" % e

for name, sid, sym in [("GOLDM", "569003", "GOLDM"), ("SILVERM", "483080", "SILVERM")]:
    end = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=5, minutes=30)
    start = end - datetime.timedelta(hours=4)
    payload = {
        "securityId": sid, "exchangeSegment": "MCX_COMM", "instrument": "FUTCOM",
        "interval": "5", "oi": False,
        "fromDate": start.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": end.strftime("%Y-%m-%d %H:%M:%S"),
    }
    st, body = post("/charts/intraday", payload)
    print("\n== %s == HTTP %s" % (name, st))
    if st == 200:
        d = json.loads(body)
        if isinstance(d, dict):
            ts = d.get("timestamp") or []
            rows = list(zip(ts, d.get("open") or [], d.get("high") or [], d.get("low") or [], d.get("close") or [], d.get("volume") or []))
        else:
            rows = d
        print("candles:", len(rows))
        for c in rows[-5:]:
            ts_ms = c[0] if c[0] > 10 ** 12 else c[0] * 1000
            tstr = datetime.datetime.fromtimestamp(ts_ms / 1000, datetime.timezone(datetime.timedelta(hours=5, minutes=30))).strftime('%H:%M')
            print("  ", tstr, "O=%s H=%s L=%s C=%s V=%s" % (c[1], c[2], c[3], c[4], c[5]))
    else:
        print(body[:800])