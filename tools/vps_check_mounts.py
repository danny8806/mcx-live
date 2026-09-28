"""Check container mounts and restart uvicorn."""
import paramiko

env = {}
for line in open("mcx-trader.env"):
    line = line.strip()
    if line and not line.startswith("#") and "=" in line:
        k, v = line.split("=", 1)
        env[k] = v

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect("200.234.44.93", username="root", password=env["VPS_PASS"], timeout=15)

def run(cmd, timeout=30):
    _, o, e = ssh.exec_command(cmd, timeout=timeout)
    return o.read().decode("utf-8", "replace").strip()

# Check mounts
print("=== CONTAINER MOUNTS ===")
print(run('docker inspect mcx-live --format "{{range .Mounts}}{{.Source}} -> {{.Destination}} ({{.Type}}){{println}}{{end}}"'))

# Check entrypoint/cmd
print("\n=== CONTAINER CMD ===")
print(run('docker inspect mcx-live --format "Cmd={{.Config.Cmd}} Entrypoint={{.Config.Entrypoint}}"'))

# Check if overview.py is from volume or image
print("\n=== OVERVIEW.PY LOCATION ===")
print(run("docker exec mcx-live ls -la /app/dashboard/routes/overview.py"))
print(run("docker exec mcx-live stat /app/dashboard/routes/overview.py"))

# Check uvicorn process
print("\n=== UVICORN PROCESS ===")
print(run("docker exec mcx-live ps aux | grep uvicorn"))

# Check if live_settings.json is mounted (it worked earlier for gate changes)
print("\n=== LIVE SETTINGS (in container) ===")
print(run("docker exec mcx-live head -5 /app/config/live_settings.json"))

# Check the build context on VPS
print("\n=== VPS BUILD CONTEXT ===")
print(run("ls -la /home/jadhavdnyaneshwar701/mcx-trader-live/dashboard/routes/overview.py 2>/dev/null || echo 'not found'"))
print(run("ls -la /home/jadhavdnyaneshwar701/mcx-trader-live/ | head -10"))

ssh.close()
