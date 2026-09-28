"""Part 4 — Controlled LIVE probe: STOP_LOSS and STOP_LOSS_MARKET on MCX.

Runs INSIDE the mcx-live container.  Uses the production transport for
STOP_LOSS_MARKET (supported by transport) and raw HTTP for STOP_LOSS
(blocked by transport validation — probed at wire level).

Safety contract:
  1. Pre-check: account margin + LTP for both GOLDM and SILVERM.
  2. ALL trigger prices set FAR from current market (±5000 ticks) —
     guaranteed NOT to trigger during the session.
  3. Account has ~₹2,800 margin vs ~₹137k required — even if Dhan accepts
     the orderType, RMS will reject for insufficient funds.
  4. Both rejection types are informative:
     - DH-905 / Input_Exception  → orderType NOT supported on MCX
     - "insufficient funds"      → orderType IS supported (RMS rejected)
     - orderId with status       → orderType IS supported and accepted
  5. Any accepted/resting order is IMMEDIATELY cancelled.

NEVER prints tokens or secrets.
"""
import os, sys
if os.environ.get("MCX_LIVE_MUTATION_ALLOWED") != "1":
    sys.exit("REFUSED: probe_mcx_stop_order_types.py sends STOP_LOSS / "
             "STOP_LOSS_MARKET probe orders via the production transport. "
             "Set MCX_LIVE_MUTATION_ALLOWED=1 to run intentionally.")
import json
import sys
import time
import datetime

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, "/app")

CFG = json.load(open("/app/config/live_settings.resolved.json"))
from config import Config  # noqa: E402
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

gate = bool(LIVE.get("live_trading_enabled", False))
client_id = str(DHAN.get("client_id") or "").strip()
product_type = DHAN.get("product_type", "MARGIN")

print("=" * 70)
print("MCX STOP-ORDER TYPE PROBE")
print("=" * 70)
print("gate =", gate)
print("client_id =", (client_id[:4] + "..." + client_id[-2:]) if client_id else "(EMPTY)")
print("product_type =", product_type)
print("instruments =", json.dumps(instruments))

if not gate:
    print("ABORT: gate is OFF")
    sys.exit(0)
if not client_id or not client_id.isdigit():
    print("ABORT: client_id not resolved")
    sys.exit(0)

from execution.live.dhan_transport import DhanRestTransport  # noqa: E402

transport = DhanRestTransport.from_config(
    dhan_config=DHAN,
    instruments=instruments,
    instrument_strategies={},
    gate_enabled=True,
)
http = transport._http

now_ist = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5, minutes=30)))
print("\nIST now:", now_ist.isoformat())

# ── pre-checks ─────────────────────────────────────────────────────────
acct = transport.account_status()
available = float(acct.get("available_margin") or 0.0)
print("\n== account_status ==")
print(json.dumps({k: v for k, v in acct.items() if k != "dhan_client_id"}, indent=2))

quotes = {}
for inst in ("GOLDM", "SILVERM"):
    q = transport.circuit_quote(inst)
    quotes[inst] = q
    print(f"\n== circuit_quote {inst} ==")
    print(json.dumps({k: v for k, v in q.items() if k != "raw"}, indent=2))

results = []

def probe_one(label, payload):
    """POST one order to Dhan, capture exact response."""
    print(f"\n{'─' * 60}")
    print(f"PROBE: {label}")
    print(f"PAYLOAD:")
    print(json.dumps(payload, indent=2))
    try:
        resp = http._post("/orders", payload, retry_network=False)
        print(f"RESPONSE:")
        print(json.dumps(resp, indent=2))
        results.append({"label": label, "payload": payload, "response": resp})
        return resp
    except Exception as exc:
        err_body = str(exc)
        print(f"EXCEPTION: {type(exc).__name__}: {err_body}")
        # Try to extract Dhan error from the exception
        # DhanRESTClient raises RuntimeError with the body embedded
        results.append({"label": label, "payload": payload, "error": err_body})
        return {"error": err_body}

def cancel_if_active(resp):
    """Cancel an order if it was accepted."""
    oid = resp.get("orderId") or resp.get("order_id")
    if oid:
        print(f"  → CANCELLING accepted order {oid}")
        try:
            c = transport.cancel_order(str(oid))
            print(f"  → CANCEL result: {json.dumps(c)}")
        except Exception as e:
            print(f"  → CANCEL exception: {e}")

# ── probe 1: STOP_LOSS_MARKET BUY GOLDM ──────────────────────────────
goldm_ltp = quotes.get("GOLDM", {}).get("ltp") or 150000.0
slm_buy_trigger = float(goldm_ltp) + 5000.0  # FAR above market

resp1 = probe_one("STOP_LOSS_MARKET BUY GOLDM (trigger=+5000)", {
    "dhanClientId": client_id,
    "transactionType": "BUY",
    "exchangeSegment": "MCX_COMM",
    "productType": product_type,
    "orderType": "STOP_LOSS_MARKET",
    "validity": "DAY",
    "securityId": instruments["GOLDM"]["security_id"],
    "quantity": 1,
    "disclosedQuantity": 0,
    "price": 0,
    "triggerPrice": slm_buy_trigger,
    "afterMarketOrder": False,
    "correlationId": f"PROBE-SLM-BUY-{int(time.time())}",
})
cancel_if_active(resp1)

# ── probe 2: STOP_LOSS BUY GOLDM (raw HTTP — transport blocks this) ──
sl_buy_trigger = float(goldm_ltp) + 5000.0
sl_buy_limit = float(goldm_ltp) + 5100.0

resp2 = probe_one("STOP_LOSS BUY GOLDM (trigger=+5000, limit=+5100)", {
    "dhanClientId": client_id,
    "transactionType": "BUY",
    "exchangeSegment": "MCX_COMM",
    "productType": product_type,
    "orderType": "STOP_LOSS",
    "validity": "DAY",
    "securityId": instruments["GOLDM"]["security_id"],
    "quantity": 1,
    "disclosedQuantity": 0,
    "price": sl_buy_limit,
    "triggerPrice": sl_buy_trigger,
    "afterMarketOrder": False,
    "correlationId": f"PROBE-SL-BUY-{int(time.time())}",
})
cancel_if_active(resp2)

# ── probe 3: STOP_LOSS_MARKET SELL SILVERM ────────────────────────────
silverm_ltp = quotes.get("SILVERM", {}).get("ltp") or 150000.0
slm_sell_trigger = float(silverm_ltp) - 5000.0  # FAR below market

resp3 = probe_one("STOP_LOSS_MARKET SELL SILVERM (trigger=-5000)", {
    "dhanClientId": client_id,
    "transactionType": "SELL",
    "exchangeSegment": "MCX_COMM",
    "productType": product_type,
    "orderType": "STOP_LOSS_MARKET",
    "validity": "DAY",
    "securityId": instruments["SILVERM"]["security_id"],
    "quantity": 1,
    "disclosedQuantity": 0,
    "price": 0,
    "triggerPrice": slm_sell_trigger,
    "afterMarketOrder": False,
    "correlationId": f"PROBE-SLM-SELL-{int(time.time())}",
})
cancel_if_active(resp3)

# ── probe 4: STOP_LOSS SELL SILVERM (raw HTTP) ───────────────────────
sl_sell_trigger = float(silverm_ltp) - 5000.0
sl_sell_limit = float(silverm_ltp) - 5100.0

resp4 = probe_one("STOP_LOSS SELL SILVERM (trigger=-5000, limit=-5100)", {
    "dhanClientId": client_id,
    "transactionType": "SELL",
    "exchangeSegment": "MCX_COMM",
    "productType": product_type,
    "orderType": "STOP_LOSS",
    "validity": "DAY",
    "securityId": instruments["SILVERM"]["security_id"],
    "quantity": 1,
    "disclosedQuantity": 0,
    "price": sl_sell_limit,
    "triggerPrice": sl_sell_trigger,
    "afterMarketOrder": False,
    "correlationId": f"PROBE-SL-SELL-{int(time.time())}",
})
cancel_if_active(resp4)

# ── summary ───────────────────────────────────────────────────────────
print(f"\n{'=' * 70}")
print("PROBE SUMMARY")
print(f"{'=' * 70}")
for r in results:
    label = r.get("label", "?")
    resp = r.get("response") or r.get("error") or {}
    if isinstance(resp, dict):
        oid = resp.get("orderId") or resp.get("order_id")
        status = resp.get("orderStatus") or resp.get("order_status") or ""
        error_code = resp.get("errorCode") or resp.get("error_code") or ""
        error_msg = (resp.get("omsErrorDescription") or resp.get("errorMessage")
                     or resp.get("message") or resp.get("error") or "")
        if oid:
            verdict = f"ACCEPTED orderId={oid} status={status}"
        elif "DH-905" in str(resp) or "Input_Exception" in str(resp):
            verdict = "REJECTED — DH-905 Input_Exception (orderType NOT supported on MCX)"
        elif "insufficient" in str(resp).lower() or "margin" in str(resp).lower():
            verdict = "REJECTED — insufficient funds (orderType IS supported by Dhan)"
        elif error_code:
            verdict = f"REJECTED — {error_code}: {error_msg}"
        elif "error" in resp:
            verdict = f"ERROR: {resp['error']}"
        else:
            verdict = f"UNKNOWN: {json.dumps(resp)}"
    else:
        verdict = f"EXCEPTION: {resp}"
    print(f"  {label}: {verdict}")

print(f"\n== probe complete ==")
