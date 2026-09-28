"""Fix DB corruption and redeploy with logger fix."""
import paramiko, time

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=300):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

VPS = "/home/jadhavdnyaneshwar701/mcx-trader-live"

# Step 1: Fix DB corruption
print("STEP 1: FIX DB CORRUPTION")
print("=" * 60)

# Stop container first to release DB lock
run("docker stop mcx-live || true", timeout=30)
time.sleep(2)

# Try to recover the DB
print("Attempting DB recovery...")
recovery = run(
    f"docker exec mcx-live python3 -c \""
    f"import sqlite3, shutil; "
    f"db='/app/live/data/db/live_trading.db'; "
    f"bak=db+'.bak'; "
    f"shutil.copy2(db,bak); "
    f"conn=sqlite3.connect(db); "
    f"r=conn.execute('PRAGMA integrity_check').fetchone(); "
    f"print('Integrity:', r[0]); "
    f"conn.close()\" 2>&1",
    timeout=30
)
print(f"  Integrity check: {recovery}")

# If corrupt, try .recover
if "ok" not in recovery.lower():
    print("\nDB may be corrupted. Trying sqlite3 .recover...")
    # Stop container, copy DB out, recover, put back
    run(f"cp {VPS}/live/data/db/live_trading.db {VPS}/live/data/db/live_trading.db.corrupt", timeout=60)
    
    # Try recovery with sqlite3 command line
    recover_result = run(
        f"sqlite3 {VPS}/live/data/db/live_trading.db '.recover' | sqlite3 {VPS}/live/data/db/live_trading_recovered.db 2>&1",
        timeout=120
    )
    print(f"  Recover: {recover_result}")
    
    # Check recovered DB
    check = run(f"sqlite3 {VPS}/live/data/db/live_trading_recovered.db 'PRAGMA integrity_check' 2>&1")
    print(f"  Recovered DB integrity: {check}")
    
    if "ok" in check.lower():
        # Replace corrupt DB
        run(f"mv {VPS}/live/data/db/live_trading.db {VPS}/live/data/db/live_trading.db.corrupt2")
        run(f"mv {VPS}/live/data/db/live_trading_recovered.db {VPS}/live/data/db/live_trading.db")
        # Also remove WAL/SHM
        run(f"rm -f {VPS}/live/data/db/live_trading.db-wal {VPS}/live/data/db/live_trading.db-shm")
        print("  DB recovered and replaced!")
    else:
        print("  Recovery failed. Starting fresh DB...")
        run(f"mv {VPS}/live/data/db/live_trading.db {VPS}/live/data/db/live_trading.db.corrupt2")
        run(f"rm -f {VPS}/live/data/db/live_trading.db-wal {VPS}/live/data/db/live_trading.db-shm")

# Step 2: Sync the logger fix
print("\nSTEP 2: SYNC LOGGER FIX")
print("=" * 60)

sftp = ssh.open_sftp()
sftp.put("trading_engine.py", f"{VPS}/trading_engine.py")
sftp.close()
print("  trading_engine.py synced")

# Step 3: Rebuild image
print("\nSTEP 3: REBUILD IMAGE")
print("=" * 60)

result = run(f"cd {VPS} && docker build --no-cache -t mcx-trader-live:remedy-f29 .", timeout=1800)
for line in result.split("\n")[-5:]:
    print(f"  {line}")

if "error" in result.lower() and "successfully" not in result.lower():
    print("  BUILD FAILED!")
    print(result[-500:])
else:
    print("  Build OK")

# Step 4: Recreate container
print("\nSTEP 4: RECREATE CONTAINER")
print("=" * 60)

run("docker stop mcx-live || true", timeout=30)
run("docker rm mcx-live || true", timeout=30)
time.sleep(2)

create = (
    f"docker run -d --name mcx-live --restart unless-stopped "
    f"-e TZ=Asia/Kolkata "
    f"--env-file {VPS}/.env.live "
    f"-p 8001:8001 "
    f"-v {VPS}/live/data/db:/app/live/data/db "
    f"-v {VPS}/logs/live-mcx:/app/logs "
    f"-v {VPS}/data/db:/app/data/db "
    f"mcx-trader-live:remedy-f29"
)
print(f"  {create}")
run(create, timeout=60)

print("\n  Waiting for healthy...")
for i in range(30):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    logs = run("docker logs mcx-live --tail 3 2>&1 | tr '\\n' ' '")
    print(f"  [{i*3}s] {health} | {logs[:100]}")
    if health == "healthy":
        break

# Step 5: Verify
print("\nSTEP 5: VERIFY")
print("=" * 60)

import hashlib
def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()
def remote_md5(path):
    return run(f"md5sum {path} 2>/dev/null | cut -d' ' -f1")

for f in ["execution/price_model.py", "trading_engine.py", "live/api.py", "config/live_settings.json"]:
    local_hash = md5(f)
    remote_hash = remote_md5(f"/app/{f}")
    status = "OK" if local_hash == remote_hash else "MISMATCH"
    print(f"  {f:<50} {status}")

health = run("curl -sk http://127.0.0.1:8001/api/health 2>/dev/null")
print(f"\n  Health: {health[:200]}")

# Check gates
print("\n  Gates:")
print(run('docker exec mcx-live python3 -c "import json; c=json.load(open(\'/app/config/live_settings.json\')); l=c.get(\'live\',{}); bs=l.get(\'broker_sl\',{}); print(f\'LIVE_TRADING={l.get(\"live_trading_enabled\")}, GATE={l.get(\"gate\")}, BROKER_SL={bs.get(\"enabled\")}\')" 2>/dev/null'))

ssh.close()
