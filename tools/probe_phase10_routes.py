"""Phase 10: verify SPA routes served, frontend navigable, no server errors
in live logs. Run: python tools/probe_phase10_routes.py"""
from __future__ import annotations

import sys
from pathlib import Path

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

INNER = r'''
import urllib.request

def http(path):
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:8001{path}", timeout=8) as r:
            body = r.read(2_000_000).decode("utf-8", errors="replace")
            return r.status, body
    except urllib.error.HTTPError as e:
        return e.code, e.read(500).decode("utf-8", errors="replace")
    except Exception as e:
        return -1, str(e)

for path in ("/", "/reversals", "/live-ops", "/live", "/reconciliation", "/health"):
    s, b = http(path)
    frag = ""
    if "index.html" in b[:80].lower() or "<!doctype html>" in b[:200].lower() or "root" in b[:400].lower():
        frag = "SPA-INDEX"
    print(f"GET {path} -> {s} {frag}")
    if s <= 0 or s >= 500:
        print("  RAW:", b[:200])

s, b = http("/index.html")
print("GET /index.html ->", s, "bytes:", len(b), "has-root:", "root" in b)
'''

LOGSCAN = r'''
import glob
import os
import re

files = []
for base in ("/app/logs", "/logs"):
    if os.path.isdir(base):
        files += glob.glob(base + "/*.log")
print("log files:", files)
if not files:
    print("no logs found")
else:
    lines = []
    for f in files:
        lines += open(f, "r", errors="replace").read().splitlines()[-400:]
    bad = []
    pat = re.compile(r"\b(ERROR|CRITICAL|Traceback|Status: 5\d\d)\b")
    for ln in lines[-1200:]:
        if pat.search(ln):
            bad.append(ln[:220])
    print("log lines scanned:", len(lines))
    print("error-ish lines:", len(bad))
    for x in bad[:15]:
        print("  ", x)
'''


def main() -> None:
    seed = load_env_file(ROOT / "mcx-trader.env")
    vps_pass = seed.get("VPS_PASS") or ""
    if not vps_pass:
        sys.exit("Aborted: VPS_PASS not found in mcx-trader.env")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
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
        base = f"{VPS_BASE}/probe10_routes.py"
        sftp = ssh.open_sftp()
        with sftp.open(base, "w") as f:
            f.write(INNER)
        sftp.close()
        run(f"docker cp {base} mcx-live:/tmp/probe10r.py")
        run("docker exec mcx-live python /tmp/probe10r.py")

        base2 = f"{VPS_BASE}/probe10_logscan.py"
        sftp = ssh.open_sftp()
        with sftp.open(base2, "w") as f:
            f.write(LOGSCAN)
        sftp.close()
        run(f"docker cp {base2} mcx-live:/tmp/probe10l.py")
        run("docker exec mcx-live python /tmp/probe10l.py")
    finally:
        ssh.close()


if __name__ == "__main__":
    main()