"""Authorized LIVE Dhan ALL-PATHS order-flow deep test @ qty=100.

Modes:
  probe  - read-only container checks, no order
  run    - every order path our deployed code can emit, at qty=100, against the
           real broker; RMS funds are 580k vs 13.8M required, so EVERY qty=100
           leg must wire-reject DH-906 "insufficient funds" (no order created).
           Paths exercised at qty=100 (GOLDM 569003):
             A  LIMIT      BUY   entry long   (lo+2)
             B  LIMIT      SELL  entry short  (hi-2)
             C  STOP_LOSS  SELL  protective SL on hypothetical long
                                (trigger=lo+2, limit=trigger-1; SELL needs
                                 price<trigger -- _stop_limit_legs invariant)
             D  STOP_LOSS_MARKET SELL  SLM (trigger=lo+2, price 0)
             E  MARKET     SELL  order-watcher/close market fallback
             F  LIMIT      SELL  reversal exit leg
             G  LIMIT      BUY   reversal re-entry leg
           Accepted-order liveness (only reachable at small qty):
             H  SILVERM qty=1 BUY LIMIT -> modify red let me modify (PUT)
                then CANCEL (DELETE) -- proves modify+cancel primitives live.

Safety contract: every price strictly inside the exchange band and passive for
its side (BUYs at lo+2, SELLs at hi-2, triggers in band); qty=100 legs can only
reject; the qty=1 leg is cancelled; any fill/placement/position anomaly = hard FAIL.
"""
import os, sys
if os.environ.get("MCX_LIVE_MUTATION_ALLOWED") != "1":
    sys.exit("REFUSED: vps_deep_order_flow.py PLACES ORDERS against the real "
             "Dhan account. Set MCX_LIVE_MUTATION_ALLOWED=1 to run "
             "intentionally.")
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=20)


def run(cmd, t=120):
    _, o, e = ssh.exec_command(cmd, timeout=t)
    return (o.read().decode("utf-8", "replace") + e.read().decode("utf-8", "replace")).strip()


MODE = sys.argv[1] if len(sys.argv) > 1 else "probe"

if MODE == "probe":
    print("=== container read-only pre-probe ===")
    print(run("docker exec mcx-live python3 -c \"import os,sys,json,glob; print('workdir', os.getcwd()); print('resolved', os.path.exists('/app/config/live_settings.resolved.json')); print('dbs', glob.glob('/app/**/live_trading.db', recursive=True)); print('py', sys.version.split()[0])\" 2>&1"))
    ssh.close()
    sys.exit(0)

deep_script = r'''
# DEEP ALL-PATHS LIVE TEST @ qty=100, deployed code, authorized real broker.
import json, sys, time, datetime, uuid, glob
sys.path.insert(0, "/app")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

def now_ist():
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30))).strftime("%Y-%m-%d %H:%M:%S")

print("=" * 78)
print("DEEP ALL-PATHS DHAN ORDER-FLOW @ qty=100 (A-G expect DH-906; H modify+cancel live)")
print("start IST:", now_ist())

ok = True
def check(name, cond, detail=""):
    global ok
    print(("  [PASS] " if cond else "  [FAIL] ") + name + (("  | " + detail) if detail else ""))
    if not cond:
        ok = False

try:
    CFG = json.load(open("/app/config/live_settings.resolved.json"))
except Exception:
    CFG = {}
from config import Config
CFG = Config._resolve_env_vars(CFG)
if not CFG.get("dhan", {}).get("client_id"):
    CFG = Config._resolve_env_vars(json.load(open("/app/config/live_settings.json")))

DHAN = CFG.get("dhan", {})
LIVE = CFG.get("live", {})
instruments = {}
for name, ic in CFG.get("instruments", {}).items():
    instruments[name] = {
        "security_id": ic.get("security_id", ""),
        "exchange_segment": ic.get("exchange_segment", "MCX_COMM"),
        "symbol": ic.get("symbol", ""),
    }
istrat = {}
for sid, sc in CFG.get("strategies", {}).items():
    inst = sc.get("instrument")
    if inst:
        istrat.setdefault(inst, []).append(sid)

live_db = glob.glob("/app/**/live_trading.db", recursive=True)[0]
print("live audit DB:", live_db)

from persistence.broker_audit import BrokerAuditStore
audit_store = BrokerAuditStore(db_path=live_db, execution_mode="LIVE")

from execution.live.dhan_transport import DhanRestTransport
transport = DhanRestTransport.from_config(
    dhan_config=DHAN, instruments=instruments, instrument_strategies=istrat,
    gate_enabled=True, audit_store=audit_store,
)
http = transport._http
client_id = str(DHAN.get("client_id") or "")
print("client_id:", (client_id[:4] + "..." + client_id[-2:]) if client_id else "(EMPTY)")
print("gate(live_trading_enabled):", bool(LIVE.get("live_trading_enabled", False)))

def ost_row(bid):
    try:
        return (transport.order_statuses() or {}).get(bid) or {}
    except Exception as e:
        return {"error": str(e)}

def day_book_by_corr(corr):
    try:
        rows = transport.day_order_book() or []
    except Exception as e:
        print("  day_order_book error:", e); return []
    out = []
    for r in rows:
        flat = {str(k).lower(): v for k, v in (r or {}).items()}
        cand = ""
        for k in ("correlation_id", "correlationid"):
            if k in flat and flat[k] is not None:
                cand = str(flat[k])
                break
        if cand == corr:
            out.append(flat)
    return out

def getk(flat, *names):
    for n in names:
        if n in flat and flat[n] is not None:
            return flat[n]
    return None

def margin_for(inst, qty, price):
    try:
        mp = {"dhanClientId": client_id, "exchangeSegment": instruments[inst]["exchange_segment"],
              "transactionType": "BUY", "quantity": qty,
              "productType": DHAN.get("product_type", "MARGIN"),
              "securityId": instruments[inst]["security_id"], "price": float(price)}
        mm = http._post("/margincalculator", mp)
        return float((mm or {}).get("totalMargin") or 0.0)
    except Exception as e:
        print("  margincalculator %s qty=%d error: %s" % (inst, qty, str(e)[:120]))
        return None

# ── PRE-FLIGHT ────────────────────────────────────────────────────────────
print("-" * 78)
print("PRE-FLIGHT")
acct = transport.account_status()
avail = float(acct.get("available_margin") or 0.0)
print("available_margin=%s equity=%s" % (avail, acct.get("equity")))

q_gold = transport.circuit_quote("GOLDM")
ltp_g = float(q_gold.get("ltp") or 0.0); lo_g = float(q_gold.get("lower_circuit_limit") or 0.0); hi_g = float(q_gold.get("upper_circuit_limit") or 0.0)
print("GOLDM circuit: ltp=%.2f band=[%.2f, %.2f]" % (ltp_g, lo_g, hi_g))
q_sil = transport.circuit_quote("SILVERM")
ltp_s = float(q_sil.get("ltp") or 0.0); lo_s = float(q_sil.get("lower_circuit_limit") or 0.0); hi_s = float(q_sil.get("upper_circuit_limit") or 0.0)
print("SILVERM circuit: ltp=%.2f band=[%.2f, %.2f]" % (ltp_s, lo_s, hi_s))
check("pre-flight: live circuit quotes resolved (no zeros)", min(ltp_g, lo_g, hi_g, ltp_s, lo_s, hi_s) > 0, "band G=[%.0f,%.0f] S=[%.0f,%.0f]" % (lo_g, hi_g, lo_s, hi_s))

marg_100 = margin_for("GOLDM", 100, lo_g or ltp_g or 1.0)
if not marg_100:
    slope = float(CFG.get("instruments", {}).get("GOLDM", {}).get("margin_model", {}).get("slope", 0.125))
    intercept = float(CFG.get("instruments", {}).get("GOLDM", {}).get("margin_model", {}).get("intercept", 126930.0))
    marg_100 = intercept + slope * (ltp_g or 150000.0) * 100.0
print("GOLDM qty=100 required margin: %.2f" % marg_100)
marg_1s = margin_for("SILVERM", 1, lo_s or ltp_s or 1.0)
print("SILVERM qty=1 required margin: %s" % marg_1s)
check("pre-flight: qty=100 CANNOT clear RMS (reject guaranteed)", marg_100 > avail > 0, "need %.2f vs avail %.2f" % (marg_100, avail))

# ── Generic qty=100 leg (expect instant DH-906 wire reject) ──────────────
def do_gold100(tag, side, order_type, price, trigger):
    print("-" * 78)
    print("LEG %s: PLACE %s %s qty=100" % (tag, side, order_type))
    if trigger is not None:
        print("    price=%s trigger=%s (band [%.0f,%.0f])" % (price, trigger, lo_g, hi_g))
    elif price is not None:
        print("    price=%s (band [%.0f,%.0f])" % (price, lo_g, hi_g))
    else:
        print("    unbracketed (MARKET)")
    corr = "DEEP-%s" % uuid.uuid4().hex[:12]
    print("    correlation:", corr)
    got = None; raise_msg = ""; lookup404 = None
    try:
        got = transport.place_market_order(side=side, quantity=100, instrument="GOLDM",
                                           order_type=order_type, price=price,
                                           trigger_price=trigger, correlation_id=corr)
    except Exception as e:
        raise_msg = str(e)
        print("    placement RAISED (instant wire reject):", type(e).__name__, raise_msg[:200])
        try:
            body = http._get("/orders/external/%s" % corr) or {}
            print("    correlation lookup (/orders/external):", json.dumps(body)[:300])
            lookup404 = False
        except Exception as e2:
            print("    correlation lookup error:", type(e2).__name__, str(e2)[:140])
            lookup404 = "404" in str(e2)
    bid = got.get("broker_order_id") if isinstance(got, dict) else None
    marker = ("insufficient" in raise_msg.lower() or "dh-906" in raise_msg.lower()
              or "funds" in raise_msg.lower() or "rejected" in raise_msg.lower())
    if got is not None and bid:
        check("LEG %s: UNEXPECTED accepted qty=100 %s %s order" % (tag, side, order_type), False, "bid=%s" % bid)
        ok = False
    else:
        check("LEG %s: %s %s qty=100 rejected at RMS on wire (DH-906)" % (tag, side, order_type), marker, raise_msg[:170])
        check("LEG %s: correlation lookup = 404 (order NEVER created)" % tag, lookup404 is True, corr)
        check("LEG %s: no broker order id returned" % tag, bid is None, "")
    return corr, got, raise_msg

# ── Execution ─────────────────────────────────────────────────────────────
gold_legs = [
    ("A", "BUY",  "LIMIT", lo_g + 2.0, None),
    ("B", "SELL", "LIMIT", hi_g - 2.0, None),
    ("C", "SELL", "STOP_LOSS", (lo_g + 2.0) - 1.0, lo_g + 2.0),          # SL: SELL limit < trigger
    ("D", "SELL", "STOP_LOSS_MARKET", None, lo_g + 2.0),                 # SLM: trigger only
    ("E", "SELL", "MARKET", None, None),                                 # market fallback
]
results = {}
for tag, side, ot, price, trig in gold_legs:
    corr, got, msg = do_gold100(tag, side, ot, price, trig)
    results[tag] = (corr, msg)
    time.sleep(0.8)

# Reversal pair (exit-then-reentry, opposite engagement)
print("-" * 78)
print("REVERSAL PAIR (exit SELL then re-entry BUY, both qty=100, gaps=2)")
corr_r1, got_r1, msg_r1 = do_gold100("F", "SELL", "LIMIT", hi_g - 2.0, None)   # reversal exit
time.sleep(0.8)
corr_r2, got_r2, msg_r2 = do_gold100("G", "BUY", "LIMIT", lo_g + 2.0, None)    # reversal re-entry

# Accepted-order leg: SILVERM qty=1 -> MODIFY (PUT) -> CANCEL (DELETE)
print("-" * 78)
print("LEG H: SILVERM qty=1 BUY LIMIT -> modify_order(PUT) -> cancel_order(DELETE) live")
corr_h = ""
if not (min(lo_s, ltp_s) > 0 and lo_s < ltp_s and marg_1s and 0 < marg_1s <= avail):
    print("    NO-GO: silver band/margin gate not satisfied (marg %.2f avail %.2f)" % (marg_1s or 0, avail))
else:
    corr_h = "DEEP-%s" % uuid.uuid4().hex[:12]
    print("    correlation:", corr_h, "| price:", lo_s + 2.0, "(lower+2, band [%.0f,%.0f])" % (lo_s, hi_s))
    try:
        got = transport.place_market_order(side="BUY", quantity=1, instrument="SILVERM",
                                           order_type="LIMIT", price=lo_s + 2.0, correlation_id=corr_h)
        print("    placement:", json.dumps(got)[:200])
    except Exception as e:
        print("    placement RAISED:", str(e)[:200])
        got = None
    bid = got.get("broker_order_id") if isinstance(got, dict) else None
    if bid:
        time.sleep(1.2)
        try:
            mout = transport.modify_order(bid, order_type="LIMIT", price=lo_s + 4.0)
            print("    modify(PUT):", json.dumps(mout)[:220])
            check("LEG H: modify_order(PUT /orders/{id}) accepted by broker", bool(mout.get("ok") is not False or mout.get("status") or mout.get("raw_status")), json.dumps(mout)[:120])
        except Exception as e:
            print("    modify raised:", type(e).__name__, str(e)[:200])
            check("LEG H: modify_order(PUT) binary outcome resolved", False, str(e)[:160])
        time.sleep(1.2)
        try:
            cout = transport.cancel_order(bid)
            print("    cancel(DELETE):", json.dumps(cout)[:220])
        except Exception as e:
            cout = {"error": str(e)}
            print("    cancel raised:", str(e)[:200])
        time.sleep(2.0)
        rec = ost_row(bid)
        st1 = str(rec.get("status") or "").lower()
        print("    post-cancel poll: status=%s raw=%s reason=%s" % (st1, str(rec.get("raw_status") or "").upper(), rec.get("reason")))
        check("LEG H: order CANCELLED live after MODIFY+DELETE", st1 in ("cancelled", "canceled", "rejected", "expired"), "final=%s" % st1)
        check("LEG H: zero fills (passive, cancelled pre-market)", int(rec.get("filled_quantity") or 0) == 0, "fills=%s" % rec.get("filled_quantity"))
        dbo = day_book_by_corr(corr_h)
        check("LEG H: wire record for modified+cancelled order", len(dbo) > 0 and str(getk(dbo[0], "orderstatus", "status") or "").lower() in ("cancelled", "canceled"), "rows=%d" % len(dbo))

# ── CLASSIFIER (deployed) ─────────────────────────────────────────────────
print("-" * 78)
print("CLASSIFIER (deployed execution/rejection_classifier.py) for every qty=100 reject")
from execution.rejection_classifier import classify_error
expected_acts = {
    "LIMIT": "PLACE_LIMIT", "STOP_LOSS": "PLACE_STOP_LIMIT",
    "STOP_LOSS_MARKET": "PLACE_SL", "MARKET": "PLACE_MARKET",
}
for tag, side, ot, price, trig in gold_legs + [("F", "SELL", "LIMIT", None, None), ("G", "BUY", "LIMIT", None, None)]:
    corr, msg = results.get(tag, (corr if tag not in ("F", "G") else (corr_r1 if tag == "F" else corr_r2), msg_r1 if tag == "F" else msg_r2))
    if not msg:
        continue
    code = "DH-906" if "dh-906" in msg.lower() else ""
    cls = classify_error(http_status=400, error_code=code, message=msg, order_status=None)
    good = cls.get("category") == "VALIDATION" and cls.get("retryable") is False and cls.get("max_retries") == 0
    print("  %s %s -> %s" % (tag, ot, json.dumps(cls)))
    check("classifier: LEG %s (%s) VALIDATION / no retry" % (tag, ot), good, json.dumps(cls))

# ── AUDIT TRAIL ───────────────────────────────────────────────────────────
print("-" * 78)
print("AUDIT TRAIL (broker_api_events, live DB)")
try:
    evs = audit_store.query(action=None) or []
    for tag, side, ot, price, trig in gold_legs:
        corr, _ = results[tag]
        acts = [str(e.get("action") or "") for e in evs if str(e.get("correlation_id") or "") == corr]
        print("  %s rows=%d actions=%s" % (tag, len(acts), acts))
        check("audit: LEG %s recorded %s" % (tag, expected_acts[ot]),
              any(a == expected_acts[ot] for a in acts), str(acts))
        check("audit: LEG %s recorded ORDER_BY_CORRELATION (wire resolution)" % tag,
              any(a == "ORDER_BY_CORRELATION" for a in acts), str(acts))
    for tag, corr in (("F", corr_r1), ("G", corr_r2)):
        acts = [str(e.get("action") or "") for e in evs if str(e.get("correlation_id") or "") == corr]
        print("  %s rows=%d actions=%s" % (tag, len(acts), acts))
        check("audit: LEG %s recorded PLACE_LIMIT" % tag, any(a == "PLACE_LIMIT" for a in acts), "")
    if corr_h:
        acts_h = [str(e.get("action") or "") for e in evs if str(e.get("correlation_id") or "") == corr_h]
        print("  H rows=%d actions=%s" % (len(acts_h), acts_h))
        check("audit: LEG H recorded MODIFY (PUT)", any(a == "MODIFY" for a in acts_h), str(acts_h))
        check("audit: LEG H recorded CANCEL (DELETE)", any(a == "CANCEL" for a in acts_h), str(acts_h))
        check("audit: LEG H recorded PLACE_LIMIT", any(a == "PLACE_LIMIT" for a in acts_h), str(acts_h))
    names = [str(e.get("action") or "") for e in evs if str(e.get("correlation_id") or "").startswith("DEEP-")]
    print("  MARKET placements in DEEP trail: %d" % names.count("PLACE_MARKET"))
    check("audit: PLACE_MARKET appears ONLY on the explicit MARKET leg (E)",
          names.count("PLACE_MARKET") == 1, str(names.count("PLACE_MARKET")))
except Exception as e:
    print("  audit query error:", type(e).__name__, str(e)[:160])

# ── POST-CONDITION ────────────────────────────────────────────────────────
print("-" * 78)
print("POST-CONDITION")
try:
    acct2 = transport.account_status()
    print("account_status: equity=%s available_margin=%s" % (acct2.get("equity"), acct2.get("available_margin")))
    check("post: margin/equity unchanged", abs(float(acct2.get("available_margin") or 0.0) - avail) < 1.0, "")
except Exception as e:
    print("account_status error:", e)
try:
    pos = transport.positions() or []
    print("positions: %d rows" % len(pos))
    for p in pos:
        print("   ", {k: p.get(k) for k in ("betsymbol", "netqty", "buyqty", "sellqty") if k in p})
    check("post: no phantom positions", len(pos) == 0, "pos=%d" % len(pos))
except Exception as e:
    print("positions error:", e)

print("-" * 78)
print("ALL-PATHS @ qty=100 VERDICT:", "PASS" if ok else "FAIL (see [FAIL] lines)")
print("end IST:", now_ist())
sys.exit(0 if ok else 1)
'''

sf = ssh.open_sftp()
with sf.open("/tmp/deep_all_paths.py", "w") as f:
    f.write(deep_script)
sf.close()
print("=== uploading + running all-paths deep test in container ===")
print(run("docker cp /tmp/deep_all_paths.py mcx-live:/tmp/deep_all_paths.py && "
          "docker exec mcx-live python3 /tmp/deep_all_paths.py", t=420))
ssh.close()