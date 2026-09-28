"""Appendix B — ONE controlled real LIMIT order through the PRODUCTION transport.

Runs INSIDE container.  Builds DhanRestTransport exactly like the live engine
(trading_engine.py:_resolve_live_broker), gate ON, MARGIN product.

Safety contract:
  1. Read-only pre-checks: /fundlimit (available) + /margincalculator (real
     margin needed for qty=1 GOLDM BUY MARGIN) + /marketfeed/quote (circuit band).
  2. GO only if required margin > available (order CANNOT clear RMS funds).
  3. Limit price is set at circuit lower + 2 ticks -> strictly INSIDE the band
     yet far below market (unmarketable even if RMS accepted it without funds).
  4. Capture the full lifecycle: placement response, REST status poll, reason,
     and (if the order were accepted) an immediate cancel + re-poll.
  5. EXPECTED outcome: REJECTED "insufficient funds".  Any ACCEPTED/PENDING
     outcome is cancelled at once and reported.  No fill is physically possible.

Never prints the token.  No strategy / engine / DB involvement.
"""
import json
import sys
import time
import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")

CFG = json.load(open("/app/config/live_settings.resolved.json"))
from config import Config  # noqa: E402  (same env-var substitution the engine uses)
CFG = Config._resolve_env_vars(CFG)
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

gate = bool(LIVE.get("live_trading_enabled", False))
client_id = str(DHAN.get("client_id") or "").strip()

print("gate(live.live_trading_enabled) =", gate)
print("client_id configured =", (client_id[:4] + "..." + client_id[-2:]) if client_id else "(EMPTY)")
print("product_type =", DHAN.get("product_type", "MARGIN"))
print("circuit_gate =", DHAN.get("circuit_gate"))
print("instrument_strategies =", json.dumps(istrat))
print("instruments =", json.dumps(instruments))

if not gate:
    print("ABORT: gate is OFF (no real order would be sent anyway) — nothing to test.")
    sys.exit(0)
if not client_id or not client_id.isdigit():
    print("ABORT: client_id not resolved in container config (no order sent).")
    sys.exit(0)

from execution.live.dhan_transport import DhanRestTransport  # noqa: E402

transport = DhanRestTransport.from_config(
    dhan_config=DHAN,
    instruments=instruments,
    instrument_strategies=istrat,
    gate_enabled=True,
)
http = transport._http

now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
print("\nIST now:", now_ist.isoformat())

# ── pre-checks (read-only) ────────────────────────────────────────────────
acct = transport.account_status()
print("\n== account_status (pre) ==")
print(json.dumps(acct, indent=2))
available = float(acct.get("available_margin") or 0.0)

quote = transport.circuit_quote("GOLDM")
print("\n== circuit_quote GOLDM (pre) ==")
print(json.dumps({k: v for k, v in quote.items() if k != "raw"}, indent=2))
ltp = quote.get("ltp")
upper = quote.get("upper_circuit_limit")
lower = quote.get("lower_circuit_limit")

margin_payload = {
    "dhanClientId": client_id, "exchangeSegment": "MCX_COMM",
    "transactionType": "BUY", "quantity": 1,
    "productType": DHAN.get("product_type", "MARGIN"),
    "securityId": instruments["GOLDM"]["security_id"],
    "price": float(ltp or 150000.0),
}
try:
    margin = http._post("/margincalculator", margin_payload)
except Exception as e:
    margin = {"error": str(e)}
print("\n== margincalculator GOLDM qty=1 BUY %s ==" % DHAN.get("product_type", "MARGIN"))
print(json.dumps(margin, indent=2))
total_margin = float((margin or {}).get("totalMargin") or 0.0)

print("\n== SAFETY ==")
print("available:", available, "| totalMargin required:", total_margin)
enough = (total_margin > 0) and (available >= total_margin)
print("can_execute =", bool(enough))
if enough or total_margin <= 0:
    print("NO-GO: either margin unknown or funds COULD clear -> do NOT place. STOP.")
    sys.exit(2)

print("GO: required margin (%.2f) > available (%.2f) -> order cannot execute." % (
    total_margin, available))

# ── controlled limit price: strictly inside band, far below market ───────
if lower is None or upper is None:
    print("NO-GO: no circuit band -> cannot pick a safe unmarketable price. STOP.")
    sys.exit(2)
limit_price = float(lower) + 2.0  # 2 ticks inside the lower band edge
corr = "RECHECK-%d" % int(time.time())
print("\n== PLANNED CONTROLLED ORDER ==")
print(json.dumps({
    "transactionType": "BUY", "quantity": 1, "instrument": "GOLDM",
    "securityId": instruments["GOLDM"]["security_id"],
    "exchangeSegment": "MCX_COMM",
    "productType": DHAN.get("product_type", "MARGIN"),
    "orderType": "LIMIT",
    "price": limit_price, "triggerPrice": 0.0,
    "validity": "DAY", "afterMarketOrder": False,
    "correlationId": corr,
    "ltp": ltp, "circuit_band": [lower, upper],
}, indent=2))

print("\n== PLACING ONE REAL ORDER (transport.place_market_order) ==")
try:
    result = transport.place_market_order(
        side="BUY", quantity=1, instrument="GOLDM",
        order_type="LIMIT", price=limit_price,
        correlation_id=corr,
    )
    print(json.dumps(result, indent=2))
except Exception as e:
    print("placement raised:", type(e).__name__, str(e))
    sys.exit(3)

bid = result.get("broker_order_id")
print("\nbroker_order_id:", bid)

# ── lifecycle capture ─────────────────────────────────────────────────────
print("\n== poll 1 (immediate transport.order_statuses) ==")
st1 = transport.order_statuses()
print(json.dumps(st1, indent=2))
rec = (st1 or {}).get(bid) or {}

status = str(rec.get("status") or "").lower()
raw = str(rec.get("raw_status") or "").upper()

if status in ("rejected", "cancelled", "canceled", "expired", "filled"):
    print("\n== terminal already; no cancel needed ==")
    print("status:", status, "| raw:", raw, "| reason:", rec.get("reason"))
else:
    # accepted/resting => IMMEDIATELY cancel through the transport endpoint.
    print("\n== NOT terminal (status=%s raw=%s) -> IMMEDIATE CANCEL ==" % (
        status, raw))
    try:
        c = transport.cancel_order(bid)
        print(json.dumps(c, indent=2))
    except Exception as e:
        print("cancel raised:", type(e).__name__, str(e))
    time.sleep(1.5)
    print("\n== poll 2 (after cancel) ==")
    st2 = transport.order_statuses()
    print(json.dumps((st2 or {}).get(bid), indent=2))

acct2 = transport.account_status()
print("\n== account_status (post) ==")
print(json.dumps({k: v for k, v in acct2.items() if k != "dhan_client_id"}, indent=2))

print("\n== controlled order test done ==")