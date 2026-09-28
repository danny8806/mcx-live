"""Appendix A — candle-close availability latency (READ-ONLY). Runs INSIDE container.

Real-timestamp measurement: for the next 5-minute IST boundary, poll the
production Dhan REST endpoint (POST /charts/intraday) with a 1-second cadence
and record exactly when the NEW candle (start == boundary) first appears and
how its OHLC matures.  NO token printed.  NO orders placed.
"""
import json
import sys
import time
import datetime
import urllib.request
import urllib.error

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOKEN = json.load(open("/app/live/data/db/dhan_token.json"))["access_token"]
H = {"access-token": TOKEN, "Content-Type": "application/json"}
BASE = "https://api.dhan.co/v2"

IST = datetime.timezone(datetime.timedelta(hours=5, minutes=30))
IST_OFFSET = 5 * 3600 + 30 * 60

INSTR = [("GOLDM", "569003"), ("SILVERM", "483080")]


def fetch_intraday(sid):
    now = time.time()
    end = datetime.datetime.fromtimestamp(now, tz=IST)
    start = end - datetime.timedelta(hours=2)
    payload = {
        "securityId": sid, "exchangeSegment": "MCX_COMM", "instrument": "FUTCOM",
        "interval": "5", "oi": False,
        "fromDate": start.strftime("%Y-%m-%d %H:%M:%S"),
        "toDate": end.strftime("%Y-%m-%d %H:%M:%S"),
    }
    data = json.dumps(payload).encode()
    req = urllib.request.Request(BASE + "/charts/intraday", data=data,
                                 headers=H, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            j = json.loads(r.read().decode("utf-8", errors="replace"))
    except Exception:
        return None
    ts = j.get("timestamp") or []
    if not ts:
        return None
    rows = list(zip(ts, j.get("open") or [], j.get("high") or [],
                    j.get("low") or [], j.get("close") or [],
                    j.get("volume") or []))
    return rows


def next_boundary(from_ts):
    aligned = int(from_ts) - IST_OFFSET
    nb = ((aligned // 300) + 1) * 300 + IST_OFFSET
    return nb


def rows_start_ts(rows):
    return max(int(r[0] for r in rows) if False else r[0] for r in rows)


boundary = next_boundary(time.time())
print("boundary epoch:", boundary,
      "=", datetime.datetime.fromtimestamp(boundary, tz=IST).isoformat())
wait_s = boundary - time.time()
print("waiting %.1fs to boundary" % max(0.0, wait_s))
if wait_s > 0:
    time.sleep(wait_s)
t0 = time.time()

# Newest candle start BEFORE the boundary becomes visible.
rows = fetch_intraday(INSTR[0][1])
if rows is not None:
    print("pre-boundary newest start:",
          datetime.datetime.fromtimestamp(rows_start_ts(rows), tz=IST).isoformat(),
          "candles:", len(rows))

trace = {"GOLDM": [], "SILVERM": []}
seen = {}

deadline = t0 + 45.0
poll_i = 0
while time.time() < deadline:
    poll_i += 1
    since = int(round((time.time() - t0) * 1000))
    for name, sid in INSTR:
        rows = fetch_intraday(sid)
        if rows is None:
            trace[name].append((since, "ERR"))
            continue
        newest = rows_start_ts(rows)
        # normalise to epoch seconds
        new_ms = int(newest) if newest > 10 ** 12 else int(newest) * 1000
        new_s = new_ms // 1000
        entry = {
            "since": since, "newest_s": new_s,
            "new_candle_seen": new_s == boundary,
        }
        if not seen.get(name) and new_s == boundary:
            seen[name] = since
            entry["first"] = since
        # maturing OHLC of the forming candle at start==boundary
        for r in rows:
            ts_ms = int(r[0] if r[0] > 10 ** 12 else r[0] * 1000)
            if ts_ms // 1000 == boundary:
                entry["o"], entry["h"], entry["l"], entry["c"], entry["v"] = r[1], r[2], r[3], r[4], r[5]
                break
        trace[name].append(entry)
    time.sleep(1.0)

print("\n== latency summary (all times ms since boundary) ==")
for name, _ in INSTR:
    first = seen.get(name)
    print(f"{name}: first_seen_since_boundary_ms = {first if first is not None else 'NOT SEEN in 45s'}")
    n = 0
    for e in trace[name]:
        if e is not None and isinstance(e, dict) and e.get("new_candle_seen"):
            n += 1
    print(f"{name}: polls showing new candle: {n}/{len(trace[name])}")

print("\n== trace (first 40 polls, GOLDM) ==")
for e in trace["GOLDM"][:40]:
    if isinstance(e, dict):
        print(e)

print("\n== trace (first 40 polls, SILVERM) ==")
for e in trace["SILVERM"][:40]:
    if isinstance(e, dict):
        print(e)

print("\n== candle latency done ==")