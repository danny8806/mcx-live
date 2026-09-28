"""Phase 10: scan live container stdout logs for errors / non-200 HTTP.
Run: python tools/scan_live_logs.py"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent


def main() -> None:
    seed = load_env_file(ROOT / "mcx-trader.env")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect("200.234.44.93", username="root",
                password=seed.get("VPS_PASS") or "", timeout=15)
    cmd = "docker logs mcx-live --since 24h 2>&1 | tail -4000"
    _i, o, e = ssh.exec_command(cmd, timeout=120)
    out = o.read().decode("utf-8", "replace")
    _err = e.read().decode("utf-8", "replace")
    lines = out.splitlines()

    pat = re.compile(r"\b(ERROR|CRITICAL|Traceback|Status: 5\d\d|REJECTED)\b")
    bad = [ln[:200] for ln in lines if pat.search(ln)]
    http = [ln for ln in lines if "HTTP/1.1" in ln]
    non200 = [ln[:140] for ln in http if " 200 " not in ln and " 304 " not in ln]
    ws_lines = sum(1 for ln in lines if "dhan_ws" in ln)
    dedup = sum(1 for ln in lines if "DEDUP" in ln)

    print("total log lines tail :", len(lines))
    print("http request logs     :", len(http))
    print("non-200/non-304 http  :", len(non200))
    for x in non200[:12]:
        print("  ", x)
    print("dhan_ws lines         :", ws_lines)
    print("dhan_ws DEDUP lines   :", dedup)
    print("error-ish lines       :", len(bad))
    for x in bad[:20]:
        print("  ", x)
    ssh.close()


if __name__ == "__main__":
    main()