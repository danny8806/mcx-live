"""Iterative LIVE deploy — sync source, rebuild image (remedy-fN), recreate
the `mcx-live` container exactly as it runs today, verify health.

Credentials: read ONLY from the gitignored mcx-trader.env (VPS_PASS).  The
existing VPS .env.live is reused verbatim (never rewritten, secrets untouched).

Run:  python tools/remedy_deploy.py   (from repo root)
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file, should_skip, sync_tree  # noqa: E402

MCX_TRADER_DIR = str(Path(__file__).resolve().parent.parent)


def main() -> None:
    seed = load_env_file(Path(MCX_TRADER_DIR) / "mcx-trader.env")
    vps_pass = seed.get("VPS_PASS") or ""
    if not vps_pass:
        sys.exit("Aborted: VPS_PASS not found in mcx-trader.env")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print(f"Connecting to {VPS_BASE} ...")
    ssh.connect("200.234.44.93", username="root", password=vps_pass, timeout=15)

    def run(cmd: str, timeout: int = 1800) -> tuple[str, int]:
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
        # ── 1) sync source (same skip rules as deploy_vps.py) ──
        print("=" * 60)
        print("STEP 1: Syncing source to VPS (no secrets/dbs/tests)")
        print("=" * 60)
        run(f"mkdir -p {VPS_BASE}")
        sftp = ssh.open_sftp()
        sync_tree(sftp, MCX_TRADER_DIR, VPS_BASE)
        sftp.close()

        # ── 2) .env.live must already exist → sanity check only ──
        out, rc = run(f"test -f {VPS_BASE}/.env.live && echo OK")
        if rc != 0 or "OK" not in out:
            sys.exit("Aborted: .env.live missing on VPS; run deploy_vps.py first")

        # ── 3) next remedy tag ──
        out, _ = run("docker images --format '{{.Repository}}:{{.Tag}}' "
                     "| grep '^mcx-trader-live:remedy-f' "
                     "| sed 's/.*remedy-f//' | sort -n | tail -1")
        n = int(out.strip()) if out.strip() else 8
        tag = f"mcx-trader-live:remedy-f{n + 1}"
        print(f"\nNew tag: {tag}")

        # ── 4) rebuild image ──
        print("=" * 60)
        print("STEP 2: docker compose-less image build")
        print("=" * 60)
        run(f"cd {VPS_BASE} && docker build -t {tag} .", timeout=1800)
        rc = run(f"docker image inspect {tag} >/dev/null 2>&1 && echo POLISHED")[1]
        if rc != 0:
            sys.exit("Aborted: image build did not produce the tag")

        # ── 5) recreate the container exactly as configured today ──
        print("=" * 60)
        print("STEP 3: recreate mcx-live (brief downtime, restart unless-stopped)")
        print("=" * 60)
        run("docker stop mcx-live || true", timeout=120)
        run("docker rm mcx-live || true", timeout=120)
        run(
            "docker run -d --name mcx-live --restart unless-stopped "
            "-e TZ=Asia/Kolkata "
            f"--env-file {VPS_BASE}/.env.live "
            "-p 8001:8001 "
            f"-v {VPS_BASE}/live/data/db:/app/live/data/db "
            f"-v {VPS_BASE}/logs/live-mcx:/app/logs "
            f"-v {VPS_BASE}/data/db:/app/data/db "
            f"{tag}",
            timeout=120,
        )
        time.sleep(20)

        # ── 6) health verification ──
        print("=" * 60)
        print("STEP 4: health verification")
        print("=" * 60)
        for path in ("/health", "/api/health"):
            out, _ = run(
                f"curl -sk -o /dev/null -w '%{{http_code}}' "
                f"'https://127.0.0.1{path}' || true")
            print(f"  {path} -> HTTP {out}")
        run("docker ps --filter name=mcx-live --format '{{.Names}} {{.Status}}'")
        run("docker logs mcx-live --tail 15 2>&1")
    finally:
        ssh.close()

    print("=" * 60)
    print("DEPLOY COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()