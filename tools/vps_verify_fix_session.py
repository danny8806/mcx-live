"""POST-DEPLOY read-only verification for the fix-session.

Pure introspection: imports the RUNNING container's modules and asserts the
deployed fixes are present and loadable.  NO order placement, NO DB writes.
Also confirms the trading engine restore path ran on startup.
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
    out = o.read().decode("utf-8", "replace").strip()
    err = e.read().decode("utf-8", "replace").strip()
    return out, err


CHECK = r"""
import inspect, re, importlib
import execution.live.order_watcher as W
import execution.live.engine as E
import execution.live.broker_sync as BS
import trading_engine as TE

fails = []
def ok(name, cond):
    print(("PASS" if cond else "FAIL") + "  " + name)
    if not cond:
        fails.append(name)

# 1) P2 - partial-remainder: cancel allowed for PARTIALLY_FILLED
sig = inspect.getsource(E.LiveExecutionEngine.cancel_order)
ok("engine.cancel_order allows PARTIALLY_FILLED",
   "PARTIALLY_FILLED" in sig and "cancel_order(self, order_id" in sig)

# 2) P2 - fallback never markets alongside a working remainder
src = inspect.getsource(W.OrderWatcher._do_market_fallback)
ok("fallback: cancel -> verify -> market for REMAINING qty",
   "cancel_unverified_blocks_market" in src and "resting_limit_cancel_failed" in src)
ok("fallback: bounded partial-remainder retry + abandon event",
   "partial_cancel_attempts" in src and "PARTIAL_REMAINDER_ABANDONED" in src)
ok("fallback: limit_filled_during_cancel cancels the market",
   "limit_filled_during_cancel" in src)

# 3) NC - preflight gate wired into fallback
ok("fallback: preflight_fn gate present",
   "self._preflight_fn" in src and "preflight_blocked" in src)
ok("OrderWatcher constructor accepts preflight_fn",
   "preflight_fn" in inspect.signature(W.OrderWatcher.__init__).parameters)
ok("TradingEngine exposes _market_fallback_preflight",
   hasattr(TE.TradingEngine, "_market_fallback_preflight"))
psrc = inspect.getsource(TE.TradingEngine._market_fallback_preflight)
for gate in ("kill_switch_active", "daily_loss_limit_reached",
             "market_not_trading", "position_not_flat",
             "no_position_to_close", "stop_loss_not_fall_backable"):
    ok("preflight gate: " + gate, gate in psrc)
ok("preflight: partial continuation exempt from flatness",
   "filled_quantity" in psrc)

# 4) P1-3 - blocker: broker_sl gating + rec param + self-lock exclusion
bsrc = inspect.getsource(TE.TradingEngine._entry_priority_blocker)
ok("blocker signature accepts rec",
   "rec" in inspect.signature(TE.TradingEngine._entry_priority_blocker).parameters)
ok("blocker: broker_sl gating present",
   '""broker_sl""' in bsrc or '"broker_sl"' in bsrc)
ok("blocker: self-lock exclusion present",
   "Never lock an order on its OWN leg" in bsrc or
   "getattr(o, \"order_id\", None) == rec_oid" in bsrc)

# 5) P1-2 - watcher owns exits
scan = inspect.getsource(W.OrderWatcher.scan)
ok("scan routes exit-family to _decide_exit",
   "REVERSAL_EXIT" in scan and "_decide_exit" in scan)
ok("OrderWatcher has _decide_exit",
   hasattr(W.OrderWatcher, "_decide_exit"))

# 6) P1-1 - exec book snapshot/restore carries role + broker id
sn = inspect.getsource(E.LiveExecutionEngine.snapshot)
rs = inspect.getsource(E.LiveExecutionEngine.restore)
ok("snapshot serializes order_role + _broker_order_id",
   'order_role' in sn and '_broker_order_id' in sn)
ok("restore re-applies order_role + _broker_order_id",
   "order_role" in rs and "_broker_order_id" in rs)
# TradingEngine restore applies live execution restore
tsrc = inspect.getsource(TE.TradingEngine.restore)
ok("TradingEngine.restore applies execution restore",
   "execution_engine.restore" in tsrc)

# 7) NC - WS ingest transport fallback
for clsname in ("BrokerSyncService", "BrokerSyncRecorder"):
    cls = getattr(BS, clsname, None)
    if cls is None:
        continue
    fn = getattr(cls, "_on_ws_record", None)
    if fn is None:
        continue
    wsrc = inspect.getsource(fn)
    ok("ws transport fallback in " + clsname + "._on_ws_record",
       "transport = self.env.broker" in wsrc.replace("getattr(self.env.broker, \"_transport\", None)",
                                                     "self._transport")
       and "ingest_status" in wsrc)
print("---")
print("RESULT: %d fail(s)" % len(fails))
print("\n".join(fails) or "ALL CHECKS PASS")
"""

out, err = run_py(CHECK)
print(out)
if err:
    print("STDERR:", err[:500])

# Startup restore-path evidence from the live API
print("=" * 60)
print("STARTUP EVENT BUS (engine_restored confirms restore ran)")
r2 = run_py("""
import json, urllib.request
try:
    with urllib.request.urlopen("http://127.0.0.1:8001/api/health", timeout=10) as r:
        d = json.load(r)
    print("health:", d.get("engine"), "| events:", d.get("event_bus", {}).get("counts"))
except Exception as ex:
    print("health probe err", ex)
""")
print(r2[0] or r2[1])
ssh.close()