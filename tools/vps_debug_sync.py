"""Debug: check what's actually on VPS vs local."""
import paramiko, hashlib, os

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=15):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()

def remote_md5(path):
    return run(f"md5sum {path} 2>/dev/null | cut -d' ' -f1")

VPS = "/home/jadhavdnyaneshwar701/mcx-trader-live"

# Check if files ended up in the right place
print("=== VPS BUILD CONTEXT FILES ===")
for f in ["execution/price_model.py", "trading_engine.py", "live/api.py", "config/live_settings.json"]:
    vps_path = f"{VPS}/{f}"
    exists = run(f"test -f {vps_path} && echo EXISTS || echo MISSING")
    if exists == "EXISTS":
        size = run(f"stat -c%s {vps_path}")
        vps_hash = remote_md5(vps_path)
        local_hash = md5(f)
        match = "OK" if vps_hash == local_hash else "MISMATCH"
        print(f"  {f}: exists, size={size}, {match}")
    else:
        print(f"  {f}: MISSING")

# Check if files ended up in parent dir (wrong path)
print("\n=== CHECK FOR WRONG PATHS ===")
for f in ["execution/price_model.py", "trading_engine.py", "live/api.py"]:
    # Check if uploaded to MCX-TRADER-LIVE (Windows workspace)
    wrong = run(f"find /home/jadhavdnyaneshwar701/ -name '{os.path.basename(f)}' -path '*/{f}' 2>/dev/null | head -3")
    print(f"  {f}: {wrong}")

# Check what the container has
print("\n=== CONTAINER CODE (from remedy-f29) ===")
print(run("docker exec mcx-live md5sum /app/execution/price_model.py 2>/dev/null | cut -d' ' -f1"))
print(run("docker exec mcx-live md5sum /app/trading_engine.py 2>/dev/null | cut -d' ' -f1"))
print(run("docker exec mcx-live md5sum /app/live/api.py 2>/dev/null | cut -d' ' -f1"))

# The container is unhealthy - check why
print("\n=== CONTAINER HEALTH ===")
print(run("docker inspect mcx-live --format '{{.State.Health.Status}}'"))
print(run("docker logs mcx-live --tail 30 2>&1"))

# Check Dockerfile on VPS
print("\n=== DOCKERFILE ===")
print(run(f"cat {VPS}/Dockerfile"))

# Check if the COPY step in Dockerfile copies from the right place
print("\n=== DOCKER BUILD CONTEXT ===")
print(run(f"ls -la {VPS}/execution/price_model.py 2>/dev/null || echo 'NOT FOUND'"))
print(run(f"ls -la {VPS}/live/api.py 2>/dev/null || echo 'NOT FOUND'"))
print(run(f"ls -la {VPS}/trading_engine.py 2>/dev/null || echo 'NOT FOUND'"))

ssh.close()
