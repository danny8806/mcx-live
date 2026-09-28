"""Final confirmation: STOP_LOSS BUY GOLDM with BOTH price and trigger INSIDE the circuit band.

Expected: format ACCEPTED, then RMS rejects for insufficient funds — proving a
real in-band BUY breakout stop would rest at the broker until triggered.
"""
import json
import sys
import time

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, "/app")

CFG = json.load(open("/app/config/live_settings.resolved.json"))
from config import Config  # noqa: E402
CFG = Config._resolve_env_vars(CFG)
DHAN = CFG.get("dhan", {})

from execution.live.dhan_transport import DhanRestTransport  # noqa: E402

instruments = {}
for name, ic in CFG.get("instruments", {}).items():
    instruments[name] = {
        "security_id": ic.get("security_id", ""),
        "exchange_segment": ic.get("exchange_segment", "MCX_COMM"),
        "symbol": ic.get("symbol", ""),
    }

transport = DhanRestTransport.from_config(
    dhan_config=DHAN, instruments=instruments, instrument_strategies={},
    gate_enabled=True,
)
http = transport._http
client_id = str(DHAN.get("client_id") or "").strip()

quote = transport.circuit_quote("GOLDM")
ltp = quote.get("ltp") or 152330.0
lower = quote.get("lower_circuit_limit")
upper = quote.get("upper_circuit_limit")
print(f"GOLDM LTP={ltp} band=[{lower}, {upper}]")

# In-band trigger: LTP + 500 (well inside the ~146k-155k band)
trigger = round(float(ltp) + 500.0, 0)
limitp = round(trigger + 100.0, 0)   # limit > trigger for BUY stop
print(f"Planned STOP_LOSS BUY: trigger={trigger} price={limitp}")

payload = {
    "dhanClientId": client_id,
    "transactionType": "BUY",
    "exchangeSegment": "MCX_COMM",
    "productType": DHAN.get("product_type", "MARGIN"),
    "orderType": "STOP_LOSS",
    "validity": "DAY",
    "securityId": instruments["GOLDM"]["security_id"],
    "quantity": 1,
    "disclosedQuantity": 0,
    "price": limitp,
    "triggerPrice": trigger,
    "afterMarketOrder": False,
    "correlationId": f"PROBE-INBAND-BUY-{int(time.time())}",
}
print("PAYLOAD:", json.dumps(payload, indent=2))

resp = http._post("/orders", payload, retry_network=False)
print("PLACEMENT RESPONSE:", json.dumps(resp, indent=2))
oid = resp.get("orderId")

# Poll until terminal or a few seconds
import time as _t
for i in range(6):
    _t.sleep(1.5)
    body = http._get(f"/orders/{oid}")
    if isinstance(body, list):
        body = body[0] if body else {}
    st = (body or {}).get("orderStatus")
    print(f"poll {i}: orderStatus={st}")
    if isinstance(body, dict) and body.get("orderStatus") in (
            "REJECTED", "TRADED", "CANCELLED", "EXPIRED"):
        print("FINAL:", json.dumps(body, indent=2))
        break

print("\n== done ==")