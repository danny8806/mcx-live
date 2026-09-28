"""Read-only: report the live strategy quantity config from the container."""
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


def run_py(code, timeout=60):
    b64 = base64.b64encode(code.encode("utf-8")).decode("ascii")
    _, o, e = ssh.exec_command(
        "echo '%s' | base64 -d | docker exec -i mcx-live python3 -" % b64,
        timeout=timeout)
    return o.read().decode("utf-8", "replace").strip(), \
        e.read().decode("utf-8", "replace").strip()


CHECK = r"""
import json, glob, os
cands = ["/app/config/live_settings.resolved.json",
         "/app/live/config/live_settings.resolved.json",
         "/app/config/live_settings.json"]
found = [c for c in cands if os.path.exists(c)]
print("config files found:", found)
if found:
    cfg = json.load(open(found[0]))
    live = cfg.get("live") or {}
    strategies = cfg.get("strategies") or {}
    print("-- live keys sample:", [k for k in live.keys()])
    print("-- strategies --")
    for sid, s in strategies.items():
        print("   %-10s instrument=%-8s quantity=%s enabled=%s capital=%s"
              % (sid, s.get("instrument"), s.get("quantity"),
                 s.get("enabled"), s.get("capital")))
    print("-- order_watcher --")
    ow = live.get("order_watcher") or {}
    print("   market_fallback_enabled=%s timeout_ms=%s" % (
        ow.get("market_fallback_enabled"), ow.get("market_fallback_timeout_ms")))
    print("   limit_skip:", ow.get("limit_skip_policy"))
    print("-- broker_sl --", live.get("broker_sl"))
    print("-- exit_first --", live.get("exit_first"))
"""

out, err = run_py(CHECK)
print(out)
if err:
    print("STDERR:", err[:400])
ssh.close()