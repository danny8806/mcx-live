"""Read-only: verify lifecycle of the 3 accepted STOP_LOSS orders from the probe."""
import json
import sys

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, "/app")

CFG = json.load(open("/app/config/live_settings.resolved.json"))
from config import Config
CFG = Config._resolve_env_vars(CFG)
DHAN = CFG.get("dhan", {})

from execution.live.dhan_transport import DhanRestTransport

instruments = {}
for name, ic in CFG.get("instruments", {}).items():
    instruments[name] = {
        "security_id": ic.get("security_id", ""),
        "exchange_segment": ic.get("exchange_segment", "MCX_COMM"),
        "symbol": ic.get("symbol", ""),
    }

transport = DhanRestTransport.from_config(
    dhan_config=DHAN,
    instruments=instruments,
    instrument_strategies={},
    gate_enabled=True,
)
http = transport._http

TARGETS = [
    ("34826091620804", "STOP_LOSS BUY GOLDM"),
    ("34826091621204", "STOP_LOSS_MARKET SELL SILVERM"),
    ("34826091621304", "STOP_LOSS SELL SILVERM"),
]

for oid, label in TARGETS:
    print(f"\n{'='*60}")
    print(f"ORDER {oid} — {label}")
    try:
        body = http._get(f"/orders/{oid}")
        print(json.dumps(body, indent=2))
    except Exception as e:
        print(f"ERROR: {e}")

print("\n== DAY ORDER BOOK (today) ==")
try:
    rows = http._get("/orders") or []
    if isinstance(rows, list):
        for row in rows:
            print(json.dumps(row, indent=2))
    else:
        print(json.dumps(rows, indent=2))
except Exception as e:
    print(f"ERROR: {e}")

print("\n== done ==")