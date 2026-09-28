"""Continue a remedy deploy after a mid-build fix: sync only the files that
changed since the full sync, rebuild the image, recreate mcx-live, verify.

Run:  python tools/remedy_rebuild.py <file-or-dir> [more paths...]
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import paramiko

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from deploy_vps import VPS_BASE, load_env_file  # noqa: E402

MCX_TRADER_DIR = Path(__file__).resolve().parent.parent


def main() -> None:
    targets = sys.argv[1:]
    if not targets:
        sys.exit("usage: remedy_rebuild.py <path> [path...]")

    seed = load_env_file(MCX_TRADER_DIR / "mcx-trader.env")
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
        # ── 1) upload only the changed paths ──
        print("=" * 60)
        print("STEP 1: uploading changed paths")
        print("=" * 60)
        sftp = ssh.open_sftp()
        for t in targets:
            p = MCX_TRADER_DIR / t
            if not p.exists():
                sys.exit(f"Aborted: {p} does not exist")
            rel = p.relative_to(MCX_TRADER_DIR).as_posix()
            remote_dir = f"{VPS_BASE}/{Path(rel).parent.as_posix()}"
            stdin, stdout, stderr = ssh.exec_command(
                f"mkdir -p {remote_dir}")
            stdout.channel.recv_exit_status()
            sftp.put(str(p), f"{VPS_BASE}/{rel}")
            print(f"  uploaded {rel}")
            remote_stat = sftp.stat(f"{VPS_BASE}/{rel}")
            print(f"    -> {remote_stat.st_size} bytes on VPS")
        sftp.close()

        # ── 2) next tag ──
        out, _ = run("docker images --format '{{.Repository}}:{{.Tag}}' "
                     "| grep '^mcx-trader-live:remedy-f' "
                     "| sed 's/.*remedy-f//' | sort -n | tail -1")
        n = int(out.strip()) if out.strip() else 9
        tag = f"mcx-trader-live:remedy-f{n + 1}"
        print(f"\nNew tag: {tag}")

        # ── 3) rebuild ──
        print("=" * 60)
        print("STEP 2: image build")
        print("=" * 60)
        run(f"cd {VPS_BASE} && docker build -t {tag} .", timeout=1800)
        _, rc = run(f"docker image inspect {tag} >/dev/null 2>&1 && echo POLISHED")
        if rc != 0:
            sys.exit("Aborted: image build did not produce the tag")

        # ── 4) recreate container ──
        print("=" * 60)
        print("STEP 3: recreate mcx-live")
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

        # ── 5) health ──
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
    print("REBUILD COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()