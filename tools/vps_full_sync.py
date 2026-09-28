"""Full sync local codebase to VPS, rebuild and redeploy mcx-live."""
import paramiko, os, time

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

VPS_BASE = "/home/jadhavdnyaneshwar701/mcx-trader-live"
LOCAL_BASE = "."

# Files to sync (source code, config, frontend)
SYNC_PATTERNS = [
    "execution/",
    "live/",
    "strategies/",
    "portfolio/",
    "analytics/",
    "dashboard/routes/",
    "dashboard/envs.py",
    "dashboard/event_bus.py",
    "dashboard/ws_manager.py",
    "dashboard/frontend.py",
    "trading_engine.py",
    "paths.py",
    "config/live_settings.json",
]

print("STEP 1: SYNCING CODEBASE TO VPS")
print("=" * 60)

sftp = ssh.open_sftp()
uploaded = 0

for pattern in SYNC_PATTERNS:
    if os.path.isfile(pattern):
        # Single file
        local = pattern
        remote = f"{VPS_BASE}/{pattern}"
        # Ensure remote dir exists
        rdir = os.path.dirname(remote).replace("\\", "/")
        ssh.exec_command(f"mkdir -p {rdir}")
        sftp.put(local, remote)
        uploaded += 1
        print(f"  {local}")
    elif os.path.isdir(pattern):
        # Directory - walk and upload all files
        for root, dirs, files in os.walk(pattern):
            # Skip __pycache__ and .pyc
            dirs[:] = [d for d in dirs if d != "__pycache__"]
            for fname in files:
                if fname.endswith(".pyc") or fname.endswith(".pyo"):
                    continue
                local = os.path.join(root, fname).replace("\\", "/")
                remote = f"{VPS_BASE}/{local}"
                rdir = os.path.dirname(remote).replace("\\", "/")
                ssh.exec_command(f"mkdir -p {rdir}")
                try:
                    sftp.put(local, remote)
                    uploaded += 1
                    if uploaded % 20 == 0:
                        print(f"  ... {uploaded} files uploaded")
                except Exception as ex:
                    print(f"  FAILED: {local} -> {ex}")

sftp.close()
print(f"\n  Total files uploaded: {uploaded}")

# Also sync dashboard-ui dist (frontend build)
print("\n  Syncing dashboard-ui/dist/...")
dist_local = "dashboard-ui/dist"
if os.path.isdir(dist_local):
    sftp = ssh.open_sftp()
    for root, dirs, files in os.walk(dist_local):
        dirs[:] = [d for d in dirs if d != "node_modules"]
        for fname in files:
            local = os.path.join(root, fname).replace("\\", "/")
            remote = f"{VPS_BASE}/{local}"
            rdir = os.path.dirname(remote).replace("\\", "/")
            ssh.exec_command(f"mkdir -p {rdir}")
            try:
                sftp.put(local, remote)
            except:
                pass
    sftp.close()
    print("  dist synced")

print("\n" + "=" * 60)
print("STEP 2: REBUILD IMAGE")
print("=" * 60)

# Get next tag
out = run("docker images --format '{{.Repository}}:{{.Tag}}' | grep '^mcx-trader-live:remedy-f' | sed 's/.*remedy-f//' | sort -n | tail -1")
n = int(out.strip()) if out.strip() else 28
tag = f"mcx-trader-live:remedy-f{n + 1}"
print(f"  New tag: {tag}")

result = run(f"cd {VPS_BASE} && docker build -t {tag} .", timeout=1800)
# Check last few lines for success
for line in result.split("\n")[-5:]:
    print(f"  {line}")

if "ERROR" in result or "error" in result.lower():
    print("\n  BUILD FAILED!")
    print(result[-500:])
else:
    print(f"\n  Build OK: {tag}")
    
    # Step 3: Recreate container
    print("\n" + "=" * 60)
    print("STEP 3: RECREATE CONTAINER")
    print("=" * 60)
    
    run("docker stop mcx-live || true", timeout=30)
    run("docker rm mcx-live || true", timeout=30)
    
    create = (
        f"docker run -d --name mcx-live --restart unless-stopped "
        f"-e TZ=Asia/Kolkata "
        f"--env-file {VPS_BASE}/.env.live "
        f"-p 8001:8001 "
        f"-v {VPS_BASE}/live/data/db:/app/live/data/db "
        f"-v {VPS_BASE}/logs/live-mcx:/app/logs "
        f"-v {VPS_BASE}/data/db:/app/data/db "
        f"{tag}"
    )
    print(f"  {create}")
    run(create, timeout=60)
    
    # Wait for healthy
    print("\n  Waiting for healthy...")
    for i in range(20):
        time.sleep(3)
        health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
        print(f"  [{i*3}s] {health}")
        if health == "healthy":
            break
    
    # Step 4: Deploy overrides
    print("\n" + "=" * 60)
    print("STEP 4: DEPLOY OVERRIDES")
    print("=" * 60)
    
    sftp = ssh.open_sftp()
    sftp.put("dashboard/routes/overview.py", "/tmp/overview.py")
    sftp.put("config/live_settings.json", "/tmp/live_settings.json")
    sftp.close()
    print(run("docker cp /tmp/overview.py mcx-live:/app/dashboard/routes/overview.py"))
    print(run("docker cp /tmp/live_settings.json mcx-live:/app/config/live_settings.json"))
    
    # Step 5: Verify
    print("\n" + "=" * 60)
    print("STEP 5: VERIFY CODE MATCH")
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
    
    critical_files = [
        "execution/price_model.py",
        "trading_engine.py",
        "live/api.py",
        "dashboard/routes/overview.py",
        "strategies/instance.py",
        "execution/live/order_watcher.py",
        "dashboard/routes/live_ops.py",
        "dashboard/routes/reversals.py",
        "portfolio/account.py",
        "config/live_settings.json",
    ]
    
    all_ok = True
    for f in critical_files:
        local_hash = md5(f) if os.path.exists(f) else "?"
        remote_hash = remote_md5(f"/app/{f}")
        status = "OK" if local_hash == remote_hash else "MISMATCH"
        if status == "MISMATCH":
            all_ok = False
        print(f"  {f:<50} {status}")
    
    print(f"\n  Code sync: {'ALL OK' if all_ok else 'SOME MISMATCH'}")
    
    # Health
    print("\n  Health:", run("curl -sk http://127.0.0.1:8001/api/health 2>/dev/null | head -c 100"))

ssh.close()
