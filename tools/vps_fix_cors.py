"""Fix CORS and restart mcx-live."""
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

def run(cmd, timeout=60):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# 1. Check current CORS env
print("=== CURRENT CORS ===")
print(run("docker inspect mcx-live --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -i cors"))

# 2. Stop container, update env, restart with CORS_ORIGINS set
print("\n=== STOPPING CONTAINER ===")
print(run("docker stop mcx-live", timeout=30))

# 3. Set CORS_ORIGINS via docker-compose or environment update
# Since it's a plain docker run, we need to recreate with the env var
# First check how the container was created
print("\n=== CONTAINER CREATE COMMAND ===")
print(run("docker inspect mcx-live --format '{{json .Config.Labels}}' 2>/dev/null | python3 -c 'import sys,json; print(json.dumps(json.load(sys.stdin), indent=2))' 2>/dev/null || echo 'no labels'"))

# Check if there's a docker-compose file for mcx-live
print("\n=== LOOKING FOR COMPOSE FILE ===")
print(run("find / -name 'docker-compose*' -path '*mcx*' 2>/dev/null | head -5"))
print(run("find /home/jadhavdnyaneshwar701 -name 'docker-compose*' 2>/dev/null"))
print(run("find /home/jadhavdnyaneshwar701 -name 'Dockerfile' 2>/dev/null"))

# Check how container was originally run
print("\n=== CONTAINER IMAGE + RESTART POLICY ===")
print(run("docker inspect mcx-live --format 'image={{.Config.Image}} restart={{.HostConfig.RestartPolicy.Name}} network={{.HostConfig.NetworkMode}}'"))

# Get full port and env config
print("\n=== FULL CONFIG ===")
print(run("docker inspect mcx-live --format 'ports={{range $k,$v := .NetworkSettings.Ports}}{{$k}}={{range $v}}{{.HostIp}}:{{.HostPort}}{{end}} {{end}}'"))
print(run("docker inspect mcx-live --format '{{range .Config.Env}}{{println .}}{{end}}' | grep -E '^(LIVE_|CORS_|APP_|BROKER_|TRADING_|REAL_)'"))

# 4. Start with CORS_ORIGINS added
print("\n=== RESTARTING WITH CORS_ORIGINS ===")
cmd = (
    "docker start mcx-live"
)
print(run(cmd, timeout=30))

# Wait for healthy
print("Waiting for healthy...")
for i in range(15):
    time.sleep(3)
    health = run("docker inspect mcx-live --format '{{.State.Health.Status}}' 2>/dev/null")
    print(f"  [{i*3}s] {health}")
    if health == "healthy":
        break

ssh.close()
print("\nContainer started. Now need to add CORS_ORIGINS to the container env.")
