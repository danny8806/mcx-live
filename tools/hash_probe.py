"""Phase 10: capture image/container hashes for the deploy verification."""
from __future__ import annotations

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
    cmds = [
        'docker image inspect mcx-trader-live:remedy-f11 '
        '--format "IMG={{.Id}} SIZE={{.Size}}"',
        'docker inspect mcx-live --format "CID={{.Id}} IMG={{.Image}} '
        'RESTARTS={{.RestartCount}} STATE={{.State.Status}} '
        'RUNNING={{.State.Running}} PORT={{json .NetworkSettings.Ports}}"',
        'docker exec mcx-live python -c "import live; print(\'app live OK\')"',
    ]
    for cmd in cmds:
        _i, o, e = ssh.exec_command(cmd, timeout=90)
        out = o.read().decode("utf-8", "replace").strip()
        err = e.read().decode("utf-8", "replace").strip()
        print(">>> " + cmd)
        print(out or "(no stdout)")
        if err:
            print("STDERR:", err[:300])
    ssh.close()


if __name__ == "__main__":
    main()