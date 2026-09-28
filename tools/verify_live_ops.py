"""Tier 4.4 — LIVE Ops dashboard verification tool.

Standalone (stdlib-only) schema + freshness assertions for the Live Ops
consolidated API.  Run against the LIVE container:

    python tools/verify_live_ops.py --base-url http://127.0.0.1:8001
    python tools/verify_live_ops.py --uptime           # poll every 30s, exit on first failure

Assertions cover every panel of the LIVE Ops page so a broken panel becomes
a non-zero exit before the UI is ever trusted.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.request
from typing import Any, Callable, Optional

PASS, FAIL = "PASS", "FAIL"
_results: list[tuple[str, str]] = []


def check(name: str, cond: bool, ctx: str = "") -> None:
    _results.append((PASS if cond else FAIL, f"{name}" + (f" [{ctx}]" if ctx else "")))


def required(data: dict, *keys: str) -> bool:
    return all(k in data for k in keys)


def get_json(url: str, timeout: float = 20.0) -> dict:
    with urllib.request.urlopen(f"{url}/api/live/dashboard", timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def epoch(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) / 1000.0 if float(v) > 1e12 else float(v)
    try:
        return float(v)
    except Exception:
        try:
            import datetime
            s = str(v).strip()
            for idx in range(len(s) - 1, 10, -1):
                if s[idx] in ("+", "-") and s[idx - 1].isdigit():
                    s = s[:idx] + "Z"
                    break
            for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S"):
                dt = datetime.datetime.strptime(s, fmt)
                return dt.replace(tzinfo=datetime.timezone.utc).timestamp()
        except Exception:
            return None
    return None


def verify(base_url: str) -> None:
    now = time.time()
    try:
        d = get_json(base_url)
    except Exception as e:
        check("dashboard reachable", False, str(e))
        return

    check("dashboard payload", isinstance(d, dict) and d.get("execution_mode") == "LIVE",
          str(d.get("error") or d.get("execution_mode")))
    secs = ("profile", "funds", "sync", "candles", "signals",
            "orders", "positions", "pnl", "recon", "telegram", "timeline")
    for s in secs:
        check(f"section {s}", s in d and isinstance(d[s], dict if s != "timeline" else list),
              "missing" if s not in d else "")

    # funds
    f = d.get("funds") or {}
    check("funds equity numeric", isinstance(f.get("equity"), (int, float)), "equity=%r" % f.get("equity"))
    check("funds masked client id", isinstance(f.get("dhan_client_id"), str) and "*" in (f.get("dhan_client_id") or ""))
    fu = f.get("last_updated")
    fudge = epoch(fu)
    check("funds age finite", fudge is not None and (now - fudge) <= 600, "age=%.1fs" % ((now - fudge) if fudge else -1))

    # sync
    s = (d.get("sync") or {}).get("sync") or {}
    svc = s.get("service") or {}
    check("sync worker_alive", isinstance(svc.get("worker_alive"), bool), "worker_alive=%r" % svc.get("worker_alive"))
    check("sync healthy", isinstance(svc.get("healthy"), bool))
    st = s.get("stale_tasks") or {}
    for task in ("orders", "positions", "account", "reconcile"):
        t = st.get(task) or {}
        check(f"sync stale {task}", isinstance(t.get("is_stale"), bool), "is_stale=%r" % t.get("is_stale"))
    check("sync data_ws", isinstance(((d.get("sync") or {}).get("data_ws") or {}).get("connected"), (bool, type(None))))

    # candles
    cs = d.get("candles") or {}
    insts = cs.get("instruments") or {}
    check("candles instruments", len(insts) > 0, "count=%d" % len(insts))
    for iname, ix in insts.items():
        ltp = (ix.get("ltp") or {}).get("ltp")
        check(f"candle {iname} ltp", ltp is not None, "ltp=%r" % ltp)
        for tf, c in (ix.get("candles") or {}).items():
            cert = (c or {}).get("closed")
            off = (c or {}).get("forming")
            if cert:
                end = epoch(cert.get("end_ts"))
                check(f"candle {iname} {tf} closed end", end is not None and end <= now + 15,
                      "end=%r now=%.0f" % (end, now))
            if off:
                stts = epoch(off.get("start_ts"))
                check(f"candle {iname} {tf} forming start", stts is not None and stts <= now,
                      "start=%r" % stts)

    # signals / orders
    sig = d.get("signals") or {}
    check("signals latest_at", epoch(sig.get("latest_at")) is not None or (sig.get("signals") or []) == [],
          "latest_at=%r" % sig.get("latest_at"))
    ods = d.get("orders") or {}
    check("orders latest_at", epoch(ods.get("latest_at")) is not None or not (ods.get("orders") or []),
          "latest_at=%r" % ods.get("latest_at"))

    # telegram
    tg = (d.get("telegram") or {}).get("stats") or {}
    check("telegram sent_count", "sent_count" in tg, "keys=%r" % list(tg.keys()))
    check("telegram error_count", "error_count" in tg)

    # timeline
    tl = d.get("timeline") or []
    check("timeline list", isinstance(tl, list))


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-url", default="http://127.0.0.1:8001")
    ap.add_argument("--uptime", action="store_true",
                    help="poll until first failure; intended for a supervisor loop")
    ap.add_argument("--interval", type=float, default=30.0)
    args = ap.parse_args()

    loop = 0
    while True:
        global _results
        _results = []
        loop += 1
        verify(args.base_url)
        nfail = sum(1 for s, _ in _results if s == FAIL)
        stamp = time.strftime("%H:%M:%S")
        print(f"--- verify pass {loop} @ {stamp}: {len(_results) - nfail}/{len(_results)}")
        for status, name in _results:
            print(f"  [{status}] {name}")
        sys.stdout.flush()
        if nfail:
            return 1
        if not args.uptime:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    raise SystemExit(main())