#!/usr/bin/env python3
"""VPS Phase 6 verification script."""
import paramiko
import sys
import time

VPS_HOST = "200.234.44.93"
VPS_USER = "root"
VPS_PASS = "Deltacapitals@123"

def run(cmd, timeout=15):
    """Run a command on VPS and return output."""
    transport = paramiko.Transport((VPS_HOST, 22))
    transport.connect(username=VPS_USER, password=VPS_PASS)
    channel = transport.open_session()
    channel.settimeout(timeout)
    channel.exec_command(cmd)
    out = b""
    err = b""
    while not channel.exit_status_ready():
        if channel.recv_ready():
            out += channel.recv(65536)
        if channel.recv_stderr_ready():
            err += channel.recv_stderr(65536)
        time.sleep(0.1)
    while channel.recv_ready():
        out += channel.recv(65536)
    while channel.recv_stderr_ready():
        err += channel.recv_stderr(65536)
    channel.close()
    transport.close()
    return out.decode(errors="replace"), err.decode(errors="replace")

sections = [
    ("1. DOCKER PS", "docker ps -a --format 'table {{.ID}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}\t{{.Names}}'"),
    ("2. DOCKER INSPECT (mcx-live)", "docker inspect mcx-live --format 'Image: {{.Config.Image}} | Created: {{.Created}} | RestartCount: {{.RestartCount}} | Status: {{.State.Status}} | Health: {{.State.Health.Status}} | Ports: {{range $k,$v := .NetworkSettings.Ports}}{{$k}}->{{$v}} {{end}}' 2>/dev/null || echo 'container not found'"),
    ("3. DOCKER IMAGES", "docker images --format 'table {{.Repository}}\t{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}'"),
    ("4. CONTAINER LOGS (last 50 lines)", "docker logs mcx-live --tail 50 2>&1"),
    ("5. CONTAINER HEALTH", "docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null || echo 'no healthcheck'"),
    ("6. CONTAINER UPTIME", "docker inspect mcx-live --format '{{.State.StartedAt}}' 2>/dev/null || echo 'not running'"),
    ("7. DHAN CONFIG CHECK", "docker exec mcx-live cat /app/config/live_settings.json 2>/dev/null | head -50 || echo 'config not found'"),
    ("8. ENVIRONMENT VARIABLES", "docker exec mcx-live env 2>/dev/null | grep -E 'DHAN|TELEGRAM|LIVE' || echo 'env check failed'"),
    ("9. RUNNING PROCESSES", "docker exec mcx-live ps aux 2>/dev/null || docker top mcx-live 2>/dev/null || echo 'process check failed'"),
    ("10. DATABASE FILE", "docker exec mcx-live ls -la /app/data/db/ 2>/dev/null || echo 'db dir not found'"),
    ("11. SOURCE HASH CHECK", "docker exec mcx-live md5sum /app/trading_engine.py 2>/dev/null || echo 'hash check failed'"),
    ("12. NETWORK", "docker network ls --format 'table {{.Name}}\t{{.Driver}}\t{{.Scope}}'"),
    ("13. CONTAINER RESOURCES", "docker stats mcx-live --no-stream --format 'CPU: {{.CPUPerc}} | MEM: {{.MemUsage}} | NET: {{.NetIO}} | DISK: {{.BlockIO}}' 2>/dev/null || echo 'stats failed'"),
]

for title, cmd in sections:
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")
    out, err = run(cmd)
    if out.strip():
        print(out.strip())
    if err.strip() and not out.strip():
        print(f"STDERR: {err.strip()}")
