"""Add CORS_ORIGINS to .env.live and recreate mcx-live container."""
import os, sys
if os.environ.get("MCX_LIVE_MUTATION_ALLOWED") != "1":
    sys.exit("REFUSED: vps_add_cors.py STOPS/RECREATES the live mcx-live "
             "container. Set MCX_LIVE_MUTATION_ALLOWED=1 to run intentionally.")
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

def run(cmd, timeout=120):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

VPS_BASE = "/home/jadhavdnyaneshwar701/mcx-trader-live"

# 1. Check current .env.live
print("=== CURRENT .env.live ===")
print(run(f"cat {VPS_BASE}/.env.live"))

# 2. Add CORS_ORIGINS to .env.live
print("\n=== ADDING CORS_ORIGINS ===")

# Read the file content, check if CORS already exists
env_content = run(f"cat {VPS_BASE}/.env.live")
if "CORS_ORIGINS" not in env_content:
    # Append CORS_ORIGINS
    cors_line = "CORS_ORIGINS=https://deltacapitals.systems,http://200.234.44.93,http://200.234.44.93:8001"
    
    # Upload via SFTP
    sftp = ssh.open_sftp()
    with sftp.open(f"{VPS_BASE}/.env.live", "a") as f:
        f.write(f"\n{cors_line}\n")
    sftp.close()
    print(f"  Added: {cors_line}")
else:
    print("  CORS_ORIGINS already present")

# Also add to the build context .env.live
sftp = ssh.open_sftp()
# Write the full .env.live with CORS
full_content = run(f"cat {VPS_BASE}/.env.live")
print("\n=== UPDATED .env.live ===")
print(full_content)

# 3. Stop and recreate container with new env
print("\n=== STOPPING CONTAINER ===")
print(run("docker stop mcx-live", timeout=30))
print(run("docker rm mcx-live", timeout=30))

# 4. Recreate with the same command as remedy_rebuild but with CORS
print("\n=== RECREATING CONTAINER ===")
tag = run("docker images --format '{{.Repository}}:{{.Tag}}' | grep mcx-trader-live:remedy | tail -1")
print(f"Using image: {tag}")

create_cmd = (
    f"docker run -d --name mcx-live --restart unless-stopped "
    f"-e TZ=Asia/Kolkata "
    f"--env-file {VPS_BASE}/.env.live "
    f"-p 8001:8001 "
    f"-v {VPS_BASE}/live/data/db:/app/live/data/db "
    f"-v {VPS_BASE}/logs/live-mcx:/app/logs "
    f"-v {VPS_BASE}/data/db:/app/data/db "
    f"{tag}"
)
print(f"\n{create_cmd}")
print(run(create_cmd, timeout=60))

# 5. Wait for healthy
print("\nWaiting for healthy...")
for i in range(20):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    running = run("docker inspect mcx-live --format '{{.State.Running}}' 2>/dev/null")
    print(f"  [{i*3}s] running={running} health={health}")
    if running == "true" and health == "healthy":
        break

# 6. Verify CORS env
print("\n=== CORS ENV IN CONTAINER ===")
print(run("docker exec mcx-live printenv CORS_ORIGINS 2>/dev/null || echo 'NOT SET'"))

# 7. Test from browser perspective
print("\n=== CORS PREFLIGHT TEST ===")
print(run("curl -sk -H 'Origin: https://deltacapitals.systems' -H 'Access-Control-Request-Method: GET' -X OPTIONS -o /dev/null -w '%{http_code}' https://deltacapitals.systems/api/overview"))

print("\n=== OVERVIEW TEST ===")
print(run("curl -sk https://deltacapitals.systems/api/overview 2>/dev/null | python3 -c \"import sys,json; d=json.load(sys.stdin); print('equity_source:', d.get('equity_source')); print('starting_capital:', d.get('starting_capital',{}).get('value'))\""))

print("\n=== HEALTH TEST ===")
print(run("curl -sk https://deltacapitals.systems/api/health 2>/dev/null | head -c 200"))

ssh.close()
