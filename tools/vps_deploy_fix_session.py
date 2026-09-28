"""Deploy the fix-session files to the live container with byte parity.

1. SFTP the 4 modified production modules to /tmp on the VPS.
2. docker cp them into mcx-live:/app.
3. py_compile them INSIDE the container (fast failure before restart).
4. sha256-parity check local vs container for each file.
5. docker restart mcx-live and wait for health.
"""
import hashlib
import os
import time

import paramiko

FILES = [
    ("trading_engine.py", "trading_engine.py"),
    ("execution/live/order_watcher.py", "execution/live/order_watcher.py"),
    ("execution/live/engine.py", "execution/live/engine.py"),
    ("execution/live/broker_sync.py", "execution/live/broker_sync.py"),
]

ENT = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        ENT[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=ENT["VPS_PASS"], timeout=15)


def run(cmd, timeout=120):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    err = e.read().decode("utf-8", "replace").strip()
    out = o.read().decode("utf-8", "replace").strip()
    if err:
        print("  [stderr]", err)
    return out


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


local = {dst: sha256(src) for src, dst in FILES}

# 1) upload
print("UPLOADING to /tmp ...")
sftp = ssh.open_sftp()
for src, dst in FILES:
    sftp.put(src, "/tmp/" + os.path.basename(dst))
    print("  /tmp/" + os.path.basename(dst))
sftp.close()

# 2) copy into container
print("\nCOPYING into mcx-live ...")
for src, dst in FILES:
    remote_tmp = "/tmp/" + os.path.basename(dst)
    print(run(f"docker cp {remote_tmp} mcx-live:/app/{dst}"))

# 3) in-container compile check (fail fast, BEFORE restart)
print("\nCOMPILING in container (pre-flight) ...")
compile_cmds = " ".join(
    f"/app/{dst}" for _src, dst in FILES)
print(run(f"docker exec mcx-live python3 -m py_compile {compile_cmds} && "
          "echo COMPILE_OK"))

# 4) byte parity check
print("\nSHA256 PARITY:")
ok = True
for _src, dst in FILES:
    remote_hash = run(
        f"docker exec mcx-live sha256sum /app/{dst} | cut -d' ' -f1")
    match = remote_hash == local[dst]
    ok = ok and match
    print(f"  {'MATCH' if match else 'MISMATCH'} {dst}")
    if not match:
        print(f"    local : {local[dst]}\n    remote: {remote_hash}")

if not ok:
    print("\nPARITY FAILED - aborting restart. Inspect before proceeding.")
    ssh.close()
    raise SystemExit(1)

# 5) restart
print("\nRESTARTING mcx-live ...")
print(run("docker restart mcx-live", timeout=180))

print("Waiting for health ...")
healthy = False
for i in range(30):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' "
                 "2>/dev/null")
    running = run("docker inspect mcx-live --format '{{.State.Running}}' "
                  "2>/dev/null")
    print(f"  [{i*3+3}s] running={running} health={health}")
    if running == "true" and health == "healthy":
        healthy = True
        break
    if running != "true":
        print("  container not running - check logs")
        print(run("docker logs --tail 30 mcx-live 2>&1"))
        break

print("\nAPI HEALTH:")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null "
          "| head -c 200"))
print("\nPOST-RESTART FILE HASHES (re-verified against git-less source):")
match2 = True
for _src, dst in FILES:
    remote_hash = run(
        f"docker exec mcx-live sha256sum /app/{dst} | cut -d' ' -f1")
    ok2 = remote_hash == local[dst]
    match2 = match2 and ok2
    print(f"  {'MATCH' if ok2 else 'MISMATCH'} {dst}")
print("\nDEPLOY DONE. healthy=%s parity_after_restart=%s"
      % (healthy, match2))
ssh.close()