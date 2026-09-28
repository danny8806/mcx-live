"""Phase 10 probe: verify the enriched /api/reversals contract on the live
container (mcx-live, remedy-f11). Runs a payload-check script inside the
container and prints the live response.

Run:  python tools/probe_reversals_phase10.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

INNER = r'''
import json
import urllib.parse
import urllib.request

def http(path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{path}", timeout=8) as r:
            return r.status, r.read(400_000).decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read(500).decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

s, b = http("/api/reversals")
print("GET /api/reversals ->", s)
try:
    d = json.loads(b)
except Exception as e:
    print("RAW:", b[:300])
    raise SystemExit(0)
print("execution_mode:", d.get("execution_mode"))
print("count:", d.get("count"), "listed:", len(d.get("reversals") or []))
if d.get("error"):
    print("ERROR:", d["error"])

fields = [
    "reversal_id", "signal_id", "strategy_id", "strategy_name",
    "instrument", "security_id", "signal_timestamp", "side",
    "trigger_price",
    "old_trade_id", "old_position_id", "old_exit_order_id",
    "old_broker_order_id", "old_exit_status", "old_exit_fill_price",
    "old_exit_filled_quantity", "old_sl_order_id", "old_sl_status",
    "new_trade_id", "new_position_id", "new_entry_order_id",
    "new_broker_order_id", "new_entry_status", "new_entry_fill_price",
    "new_entry_filled_quantity", "new_sl_order_id", "new_sl_status",
    "exit_verified_at", "entry_fill_confirmed_at",
    "fallback_used", "fallback_status",
    "status", "complete", "chain", "created_at", "updated_at",
]

rows = d.get("reversals") or []
if not rows:
    print("NO LIVE REVERSAL ROWS (panel will show empty state)")
else:
    r0 = rows[0]
    missing = [f for f in fields if f not in r0]
    print("row0 missing fields:", missing if missing else "NONE — full contract present")
    print("row0 chain steps:", [c["step"] for c in r0.get("chain", [])])
    print("row0:", json.dumps({k: r0.get(k) for k in (
        "reversal_id", "signal_id", "strategy_name", "instrument",
        "security_id", "trigger_price", "status", "complete",
        "old_trade_id", "new_trade_id", "old_broker_order_id",
        "new_broker_order_id", "old_exit_status", "new_entry_status",
        "old_sl_order_id", "new_sl_order_id"
    )}, indent=1))
    rid = urllib.parse.quote(r0.get("reversal_id", ""))
    s2, b2 = http(f"/api/reversals/{rid}")
    print("GET /api/reversals/{id} ->", s2)

s3, b3 = http("/openapi.json")
ok_reg = False
try:
    paths = json.loads(b3).get("paths", {})
    ok_reg = "/api/reversals" in paths and "/api/reversals/{reversal_id}" in paths
except Exception:
    pass
print("openapi registers /api/reversals + /api/reversals/{reversal_id}:", ok_reg)

# ---- live state re-verification (Phase 10 §2 / §20 / §31) ----
dash_s, dash_b = http("/api/live/dashboard")
print("\n== LIVE STATE ==")
try:
    dd = json.loads(dash_b)
    prof = dd.get("profile") or {}
    print("profile.client_id (masked):", prof.get("client_id"))
    print("profile.execution_model:", prof.get("execution_model"))
    print("profile.product_type:", prof.get("product_type"))
    print("profile.live_gate:", (dd.get("profile") or {}).get("gate_enabled", (dd.get("profile") or {}).get("gate")))
    funds = dd.get("funds") or {}
    print("funds:", json.dumps({k: funds.get(k) for k in (
        "source", "equity", "available_margin", "used_margin", "balance",
        "holding_value", "live_margin"
    )}))
    pos = dd.get("positions") or {}
    print("positions counts:", json.dumps(pos.get("counts")))
    if isinstance(pos.get("positions"), list):
        for p in pos["positions"][:6]:
            print("  pos:", json.dumps({k: p.get(k) for k in (
                "position_id", "strategy_id", "instrument", "security_id",
                "side", "quantity", "average_price", "pnl"
            )}))
    pnl = dd.get("pnl") or {}
    print("pnl:", json.dumps({k: pnl.get(k) for k in (
        "dhan_pnl", "local_pnl", "difference", "unrealized_dhan",
        "unrealized_local", "realized"
    )}))
    rec = dd.get("recon") or {}
    print("recon:", json.dumps({k: rec.get(k) for k in (
        "status", "is_consistent", "last_run", "checked_at"
    )}))
    sync = dd.get("sync") or {}
    print("sync keys:", sorted(sync.keys())[:10])
except Exception as e:
    print("dashboard parse error:", e, "raw:", dash_b[:200])

h_s, h_b = http("/api/health/system")
print("\nGET /api/health/system ->", h_s)
try:
    h = json.loads(h_b)
    for k in ("dhan_api", "dhan_ws", "market_ws", "database", "broker_sync",
              "trading_engine", "broker_sync_service", "engine", "db"):
        if k in h:
            print(f"  {k}:", json.dumps(h[k])[:200])
    print("  uptime/keys:", sorted(h.keys())[:20])
except Exception as e:
    print("  parse error:", e, "raw:", h_b[:200])

# ---- frontend bundle check: new Reversals panel baked in ----
print("\nINNER PROBE DONE")
'''


def main() -> None:
    seed = load_env_file(ROOT / "mcx-trader.env")
    vps_pass = seed.get("VPS_PASS") or ""
    if not vps_pass:
        sys.exit("Aborted: VPS_PASS not found in mcx-trader.env")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"Connecting to {VPS_BASE} ...")
    ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

    def run(cmd: str, timeout: int = 180) -> tuple[str, int]:
        print(f"\n>>> {cmd}")
        stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        rc = stdout.channel.recv_exit_status()
        if out.strip():
            print(out)
        if err.strip():
            print(f"STDERR: {err[:2000]}")
        return out, rc

    try:
        remote = f"{VPS_BASE}/probe_reversals_phase10_inner.py"
        sftp = ssh.open_sftp()
        with sftp.open(remote, "w") as f:
            f.write(INNER)
        sftp.close()
        run(f"docker cp {remote} mcx-live:/tmp/probe10.py")
        run("docker exec mcx-live python /tmp/probe10.py")
        bundle = r'''import glob
hits = []
for f in glob.glob("/app/dashboard-ui/dist/assets/*.js"):
    with open(f, "r", errors="replace") as fh:
        data = fh.read()
    for marker in ("LIVE REVERSALS", "REVERSAL SIGNAL", "OLD SL CANCEL",
                   "NEW FILL", "reversal_id", "/api/reversals"):
        if marker in data:
            hits.append(marker)
print("bundle markers found:", sorted(set(hits)))
'''
        remote_b = f"{VPS_BASE}/probe10_bundle.py"
        sftp = ssh.open_sftp()
        with sftp.open(remote_b, "w") as f:
            f.write(bundle)
        sftp.close()
        run(f"docker cp {remote_b} mcx-live:/tmp/probe10b.py")
        run("docker exec mcx-live python /tmp/probe10b.py")
    finally:
        ssh.close()


if __name__ == "__main__":
    main()