"""Recheck baseline (READ-ONLY). Runs INSIDE container.

Fresh broker-side truth for the recheck session:
  * /fundlimit  -> available / used margin (upstream-typo "availabelBalance")
  * /positions  -> net broker positions (expect [])
  * /holdings   -> holdings (expect [])
  * /orders     -> day order book (all rows, incl. today's rejected)
  * /ip/getIP   -> static-IP whitelist status
  * /killswitch -> kill-switch status
  * /marketfeed/quote -> LTP + circuit band for GOLDM/SILVERM
  * /margincalculator -> real margin required for qty=1 GOLDM/SILVERM BUY MARGIN

NO token is ever printed. NO order is placed.
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

CFG = json.load(open("/app/config/live_settings.resolved.json"))
from config import Config  # noqa: E402  (same env-var substitution the engine uses)
CFG = Config._resolve_env_vars(CFG)
CLIENT_ID = str((CFG.get("dhan", {}) or {}).get("client_id") or "").strip()


def get(path):
    req = urllib.request.Request(BASE + path, headers=H)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")[:3000]
    except Exception as e:
        return None, "ERR %r" % e


def post(path, payload, headers=None):
    h = dict(H)
    if headers:
        h.update(headers)
    data = json.dumps(payload).encode()
    req = urllib.request.Request(BASE + path, data=data, headers=h, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")[:3000]
    except Exception as e:
        return None, "ERR %r" % e


now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
print("== recheck baseline ==")
print("IST now:", now_ist.isoformat())
print("UTC now:", datetime.datetime.now(datetime.timezone.utc).isoformat())

print("\n==== GET /fundlimit ====")
st, body = get("/fundlimit")
print("HTTP", st)
print(body[:2000])

print("\n==== GET /positions ====")
st, body = get("/positions")
print("HTTP", st)
print(body[:2000])

print("\n==== GET /holdings ====")
st, body = get("/holdings")
print("HTTP", st)
print(body[:2000])

print("\n==== GET /orders (day order book) ====")
st, body = get("/orders")
print("HTTP", st)
try:
    rows = json.loads(body) if isinstance(body, str) else body
    if not isinstance(rows, list):
        rows = [rows]
    print("rows:", len(rows))
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        print(" ", i,
              "orderId=", row.get("orderId") or row.get("order_id"),
              "orderStatus=", row.get("orderStatus") or row.get("order_status"),
              "txn=", row.get("transactionType") or row.get("transaction_type"),
              "sec=", row.get("securityId") or row.get("security_id"),
              "qty=", row.get("quantity"), "filled=", row.get("filledQty"),
              "price=", row.get("price"), "trig=", row.get("triggerPrice"),
              "avgFill=", row.get("averageTradedPrice"),
              "orderType=", row.get("orderType") or row.get("order_type"),
              "corr=", row.get("correlationId") or row.get("correlation_id"),
              "reason=", (row.get("omsErrorDescription") or row.get("errorMessage") or "")[:120])
except Exception as e:
    print("parse err", e, body[:500])

print("\n==== GET /ip/getIP ====")
st, body = get("/ip/getIP")
print("HTTP", st)
print(body[:1000])

print("\n==== GET /killswitch ====")
st, body = get("/killswitch")
print("HTTP", st)
print(body[:1000])

for name, sid in [("GOLDM", "569003"), ("SILVERM", "483080")]:
    print("\n==== POST /marketfeed/quote %s ====" % name)
    st, body = post("/marketfeed/quote", {"MCX_COMM": [int(sid)]},
                    headers={"client-id": CLIENT_ID})
    print("HTTP", st)
    print(body[:1200])
    try:
        d = json.loads(body) if isinstance(body, str) else {}
        row = ((d.get("data") or {}).get("MCX_COMM") or {}).get(sid) or {}
        if not row:
            row = d
        ltp = (row.get("last_price") or row.get("lastTradedPrice")
               or row.get("ltp") or row.get("closePrice")
               or row.get("lastPrice"))
    except Exception:
        ltp = None

    print("\n==== POST /margincalculator %s qty=1 BUY MARGIN ====" % name)
    if isinstance(ltp, (int, float)):
        m = post("/margincalculator", {
            "dhanClientId": CLIENT_ID, "exchangeSegment": "MCX_COMM",
            "transactionType": "BUY", "quantity": 1,
            "productType": "MARGIN", "securityId": sid, "price": float(ltp)})
    else:
        m = post("/margincalculator", {
            "dhanClientId": CLIENT_ID, "exchangeSegment": "MCX_COMM",
            "transactionType": "BUY", "quantity": 1,
            "productType": "MARGIN", "securityId": sid, "price": 150000.0})
    print("HTTP", m[0])
    print(m[1][:1200])

print("\n== baseline done ==")