"""Sanity: compare TCP connect tiers api-feed vs api.dhan.co (read-only)."""
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

CODE = r"""
import socket, time
def tcp(host, n=8):
    ip = socket.getaddrinfo(host, 443, socket.AF_INET, socket.SOCK_STREAM)[0][4][0]
    out = []
    for _ in range(n):
        t0 = time.perf_counter()
        s = socket.create_connection((ip, 443), timeout=10)
        out.append((time.perf_counter() - t0) * 1000.0)
        s.close()
    return ip, out
for host in ("api-feed.dhan.co", "api-order-update.dhan.co", "api.dhan.co", "api-order.dhan.co"):
    ip, out = tcp(host)
    out.sort()
    print("%-26s ip=%-16s min=%6.3fms p50=%6.3f p95=%6.3f max=%6.3f"
          % (host, ip, out[0], out[len(out)//2], out[int(len(out)*0.95)], out[-1]))
print("DONE")
"""

b64 = base64.b64encode(CODE.encode("utf-8")).decode("ascii")
_, o, e = ssh.exec_command(
    "echo '%s' | base64 -d | docker exec -i mcx-live python3 -" % b64,
    timeout=90)
print(o.read().decode("utf-8", "replace").strip())
err = e.read().decode("utf-8", "replace").strip()
if err:
    print("STDERR:", err[:300])
ssh.close()